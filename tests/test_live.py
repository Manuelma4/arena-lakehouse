import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from arena_lakehouse.live import LiveConfig, LiveFailure, LiveWorker, _writer_lock
from arena_lakehouse.provider_publication import current_publication

NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
TOKEN = "test-only-credential-never-real"
FOOTBALL = dict(id=100, status="FINISHED", utcDate="2026-10-05T18:00:00Z",
                competition={"name": "Example League"}, homeTeam={"id": 1, "name": "Club One"},
                awayTeam={"id": 2, "name": "Club Two"}, score={"fullTime": {"home": 2, "away": 1}})
VALORANT = dict(id=200, status="finished", begin_at="2026-10-05T18:00:00Z", end_at="2026-10-05T21:00:00Z",
                tournament={"name": "Example VALORANT Circuit"}, number_of_games=3,
                opponents=[{"opponent": {"id": 1, "name": "Team One"}}, {"opponent": {"id": 2, "name": "Team Two"}}],
                results=[{"team_id": 1, "score": 2}, {"team_id": 2, "score": 1}])


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


@pytest.fixture(autouse=True)
def credentials(monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", TOKEN)
    monkeypatch.setenv("PANDASCORE_TOKEN", TOKEN)
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _seconds: None)


def client_for(payload):
    return httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload)))


def status_file(worker):
    return json.loads((worker.snapshots / "LIVE_STATUS.json").read_text(encoding="utf-8"))


def requests_made(worker):
    with worker.ledger.connect() as db:
        return db.execute("SELECT COUNT(*) FROM requests").fetchone()[0]


def test_missing_credentials_is_explicit_without_requests_or_snapshot(tmp_path, monkeypatch):
    monkeypatch.delenv("FOOTBALL_DATA_TOKEN")
    clock = Clock()
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        status = worker.poll_once()
    assert status_file(worker) == status
    assert status["schema_version"] == "arena-live-status-1.0"
    assert status["provider"] == "football-data" and status["data_kind"] == "api"
    assert status["status"] == status["last_error_code"] == "awaiting_credentials"
    assert status["latest_snapshot"] is status["last_success_at"] is status["last_change_at"] is None
    assert status["poll_counts"]["awaiting_credentials"] == 1
    assert status["last_poll"]["requests"] == status["request_budget"]["used"] == 0
    assert not worker.cursor.exists() and worker.pipeline.events() == []


def test_success_and_unchanged_check_preserve_snapshot_and_availability(tmp_path):
    clock = Clock()
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        first = worker.poll_once()
        events, cursor = worker.pipeline.events(), worker.cursor.read_bytes()
        snapshot = worker.snapshots / first["latest_snapshot"]
        assert json.loads((snapshot / "model_report.json").read_text()) == {"status": "not_run"}
        assert json.loads((snapshot / "predictions.json").read_text()) == []
        manifest = (snapshot / "manifest.json").read_bytes()
        clock.advance(60)
        second = worker.poll_once()
    assert first["status"] == second["status"] == "ready"
    assert first["latest_snapshot"] == second["latest_snapshot"]
    assert second["last_success_at"] > first["last_success_at"]
    assert first["last_change_at"] == second["last_change_at"]
    assert second["last_poll"] == dict(source_rows=1, changed_rows=0, unsupported_rows=0, requests=1)
    assert second["poll_counts"]["succeeded"] == 2
    assert worker.pipeline.events() == events and worker.cursor.read_bytes() == cursor
    assert (snapshot / "manifest.json").read_bytes() == manifest
    assert len(list(worker.snapshots.glob("snapshot-*"))) == 1
    assert events[0]["observed_at"] == first["last_success_at"]


def test_changed_score_versions_history_and_atomically_changes_pointer(tmp_path):
    clock, scores = Clock(), [2]
    def respond(_request):
        payload = dict(FOOTBALL, score={"fullTime": {"home": scores[0], "away": 1}})
        return httpx.Response(200, json={"matches": [payload]})
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        first = worker.poll_once()
        scores[0] = 3
        clock.advance(60)
        second = worker.poll_once()
    assert second["latest_snapshot"] != first["latest_snapshot"]
    assert second["last_change_at"] > first["last_change_at"]
    assert second["last_poll"]["changed_rows"] == 1
    assert [e["revision"] for e in worker.pipeline.events()] == [1, 2]
    assert [e["score_a"] for e in worker.pipeline.events()] == [2, 3]
    assert status_file(worker)["latest_snapshot"] == second["latest_snapshot"]
    assert (worker.snapshots / first["latest_snapshot"] / "manifest.json").is_file()


def test_publication_failure_after_ingest_recovers_without_cursor_or_revision_inflation(tmp_path, monkeypatch):
    import arena_lakehouse.live as module
    original, clock = module.export_snapshot, Clock()
    def crash(*_args, **_kwargs):
        raise RuntimeError(TOKEN)
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        monkeypatch.setattr(module, "export_snapshot", crash)
        failed = worker.poll_once()
        first_observation = worker.pipeline.events()[0]["observed_at"]
        assert failed["status"] == "degraded" and failed["last_error_code"] == "publication_failed"
        assert not worker.cursor.exists() and failed["latest_snapshot"] is None
        assert TOKEN not in json.dumps(failed)
        monkeypatch.setattr(module, "export_snapshot", original)
        clock.advance(60)
        restarted = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        success = restarted.poll_once()
    assert success["status"] == "ready" and success["last_poll"]["changed_rows"] == 0
    assert restarted.cursor.exists() and len(restarted.pipeline.events()) == 1
    assert restarted.pipeline.events()[0]["observed_at"] == first_observation
    assert success["poll_counts"]["failed"] == success["poll_counts"]["succeeded"] == 1


def test_interruption_after_ingest_replays_durably_on_new_worker(tmp_path, monkeypatch):
    clock = Clock()
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        original = worker.pipeline.ingest
        def stop_after_commit(records):
            original(records)
            raise KeyboardInterrupt("simulated process interruption after committed ingest")
        monkeypatch.setattr(worker.pipeline, "ingest", stop_after_commit)
        with pytest.raises(KeyboardInterrupt):
            worker.poll_once()
        observation = worker.pipeline.events()[0]["observed_at"]
        assert not worker.cursor.exists()
        clock.advance(60)
        restarted = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        status = restarted.poll_once()
    assert status["status"] == "ready"
    assert status["poll_counts"]["interrupted"] == status["poll_counts"]["succeeded"] == 1
    assert len(restarted.pipeline.events()) == 1
    assert restarted.pipeline.events()[0]["observed_at"] == observation
    assert status["last_poll"]["changed_rows"] == 0


def test_cursor_commit_failure_reuses_immutable_candidate_and_preserves_last_good(tmp_path, monkeypatch):
    import arena_lakehouse.live as module
    original, clock, scores = module.commit_state, Clock(), [2]
    def respond(_request):
        return httpx.Response(200, json={"matches": [dict(FOOTBALL, score={"fullTime": {"home": scores[0], "away": 1}})]})
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        first = worker.poll_once()
        cursor = worker.cursor.read_bytes()
        scores[0] = 4
        clock.advance(60)
        monkeypatch.setattr(module, "commit_state", lambda *_args: (_ for _ in ()).throw(OSError(TOKEN)))
        failed = worker.poll_once()
        assert failed["latest_snapshot"] == first["latest_snapshot"]
        assert failed["last_success_at"] == first["last_success_at"]
        assert failed["last_error_code"] == "cursor_commit_failed" and worker.cursor.read_bytes() == cursor
        assert len(list(worker.snapshots.glob("snapshot-*"))) == 2
        pending = worker.ledger.prepared()
        intended = pending["intent"]["candidate"]
        request_count = requests_made(worker)
        monkeypatch.setattr(module, "commit_state", original)
        clock.advance(60)
        recovered = worker.poll_once()
    assert recovered["latest_snapshot"] != first["latest_snapshot"]
    assert len(list(worker.snapshots.glob("snapshot-*"))) == 2
    assert len(worker.pipeline.events()) == 2
    assert recovered["last_poll"]["changed_rows"] == 1
    selected = current_publication(worker.root, now=clock())
    assert selected["poll_id"] == pending["poll_id"] == worker.ledger.summary()["last"]["id"]
    assert all(selected[key] == value for key, value in intended.items())
    assert requests_made(worker) == request_count


def test_retries_consume_durable_request_budget_and_denied_requests_never_send(tmp_path):
    clock, attempts = Clock(), []
    def unavailable(request):
        attempts.append(request)
        return httpx.Response(503, json={"error": TOKEN})
    with httpx.Client(transport=httpx.MockTransport(unavailable)) as client:
        config = LiveConfig("football-data", request_limit=2)
        worker = LiveWorker(tmp_path, config, client=client, clock=clock)
        first = worker.poll_once()
        assert first["status"] == "quota_limited" and first["last_error_code"] == "quota_exhausted"
        assert len(attempts) == first["last_poll"]["requests"] == first["request_budget"]["used"] == 2
        restarted = LiveWorker(tmp_path, config, client=client, clock=clock)
        second = restarted.poll_once()
        assert second["last_poll"]["requests"] == 0 and len(attempts) == 2
        clock.advance(61)
        third = restarted.poll_once()
        assert third["last_poll"]["requests"] == 2 and len(attempts) == 4
    assert TOKEN not in json.dumps(first)


def test_server_retry_then_success_counts_both_requests(tmp_path):
    attempts = []
    def respond(request):
        attempts.append(request)
        return httpx.Response(429, headers={"Retry-After": "0"}) if len(attempts) == 1 else httpx.Response(200, json={"matches": []})
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    assert status["status"] == "empty" and status["last_error_code"] is None
    assert status["last_poll"]["requests"] == status["request_budget"]["used"] == 2


@pytest.mark.parametrize("response_code,error_code", [(401, "authentication_denied"), (403, "authentication_denied"),
                                                       (404, "provider_http_error"), (500, "provider_unavailable")])
def test_http_errors_are_typed_redacted_and_keep_cursor_unchanged(tmp_path, response_code, error_code):
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(response_code, json={"error": TOKEN}))) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    assert status["status"] == "degraded" and status["last_error_code"] == error_code
    assert status["last_poll"]["requests"] == (3 if response_code == 500 else 1)
    assert not worker.cursor.exists() and worker.pipeline.events() == []
    assert TOKEN not in json.dumps(status)


def test_network_retry_errors_never_persist_exception_or_credentials(tmp_path):
    def timeout(request):
        raise httpx.ReadTimeout(TOKEN, request=request)
    with httpx.Client(transport=httpx.MockTransport(timeout)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    assert status["last_error_code"] == "network_timeout" and status["last_poll"]["requests"] == 3
    assert TOKEN not in json.dumps(status)
    with worker.ledger.connect() as db:
        assert all(TOKEN not in str(tuple(row)) for row in db.execute("SELECT * FROM requests"))


def test_invalid_payload_is_archived_but_does_not_commit_cursor_or_publish(tmp_path):
    invalid = dict(FOOTBALL, score={"fullTime": {"home": -1, "away": 1}})
    with client_for({"matches": [invalid]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    assert status["last_error_code"] == "invalid_provider_payload"
    assert not worker.cursor.exists() and status["latest_snapshot"] is None
    assert len(list((worker.root / "provider_raw").rglob("*.json"))) == 1


def test_empty_and_unsupported_sources_have_explicit_success_counts(tmp_path):
    with client_for({"matches": [dict(FOOTBALL, status="UNKNOWN_STATUS")]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    assert status["status"] == "empty" and status["last_success_at"]
    assert status["latest_snapshot"] is status["last_change_at"] is None
    assert status["last_poll"] == dict(source_rows=1, changed_rows=0, unsupported_rows=1, requests=1)


def test_valorant_counts_every_phase_request_and_separates_provider_workspaces(tmp_path):
    phases = []
    def respond(request):
        phases.append(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(200, json=[VALORANT] if phases[-1] == "past" else [])
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=Clock())
        status = worker.poll_once()
    assert phases == ["past", "upcoming", "running"]
    assert status["status"] == "ready" and status["last_poll"]["requests"] == 3
    assert status["request_budget"] == dict(limit=1000, window_seconds=3600, used=3, remaining=997)
    assert status["capabilities"]["in_game_round_feed"] is False
    assert status["capabilities"]["scores_delayed"] is None
    assert worker.root == tmp_path / "pandascore"
    assert not (tmp_path / "football-data").exists()


def test_durable_interrupted_poll_recovers_and_lock_prevents_another_writer(tmp_path):
    clock = Clock()
    with client_for({"matches": []}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        worker.ledger.begin("2026-10-06T11:59:00.000000Z")
        with _writer_lock(worker.root / ".writer.lock"):
            with pytest.raises(LiveFailure, match="worker_error"):
                worker.poll_once()
        status = worker.poll_once()
    assert status["poll_counts"]["interrupted"] == 1
    assert status["poll_counts"]["attempted"] == 2 and status["poll_counts"]["succeeded"] == 1


def test_reused_snapshot_checksum_tamper_is_not_relabelled_successful(tmp_path):
    clock = Clock()
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        first = worker.poll_once()
        (worker.snapshots / first["latest_snapshot"] / "predictions.json").write_text("[{}]")
        clock.advance(60)
        failed = worker.poll_once()
    assert failed["status"] == "degraded" and failed["last_error_code"] == "publication_failed"
    assert failed["last_success_at"] == first["last_success_at"]


def test_redirects_never_forward_credentials_or_bypass_request_ledger(tmp_path):
    attempts = []
    def redirect(request):
        attempts.append(request)
        return httpx.Response(302, headers={"Location": "https://untrusted.example/collect"})
    with httpx.Client(transport=httpx.MockTransport(redirect), follow_redirects=True) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    assert len(attempts) == status["last_poll"]["requests"] == 1
    assert attempts[0].url.host == "api.football-data.org"
    assert status["last_error_code"] == "provider_http_error"


def test_config_interval_budget_and_scope_are_explicit(tmp_path):
    with pytest.raises(ValueError, match="interval"):
        LiveConfig("football-data", interval=29)
    with pytest.raises(ValueError, match="lower"):
        LiveConfig("football-data", request_limit=11)
    worker = LiveWorker(tmp_path, LiveConfig("football-data"), clock=Clock())
    with pytest.raises(ValueError, match="source scope"):
        LiveWorker(tmp_path, LiveConfig("football-data", competition="PD"), clock=Clock())
    with pytest.raises(ValueError, match="cycles"):
        list(worker.run(0, sleep=lambda _: None))


def test_run_sleeps_only_between_bounded_cycles(tmp_path):
    clock, delays = Clock(), []
    with client_for({"matches": []}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data", interval=60), client=client, clock=clock)
        results = list(worker.run(3, sleep=lambda seconds: (delays.append(seconds), clock.advance(seconds))))
    assert delays == [60, 60]
    assert len(results) == results[-1]["poll_counts"]["succeeded"] == 3


CODES = ("PL", "PD", "BL1", "SA", "FL1", "BSA")


def competition_response(request, score=2):
    code = request.url.path.split("/")[-2]
    payload = dict(FOOTBALL, id=100 + CODES.index(code), competition={"name": code},
                   score={"fullTime": {"home": score, "away": 1}})
    return httpx.Response(200, json={"matches": [payload]})


def test_all_six_competitions_are_checked_before_one_ingest_and_publication(tmp_path, monkeypatch):
    import arena_lakehouse.live as module
    original, exports, requested = module.export_snapshot, [], []
    def record_export(*args, **kwargs):
        exports.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(module, "export_snapshot", record_export)
    def respond(request):
        requested.append(request.url.path.split("/")[-2])
        return competition_response(request)
    clock = Clock()
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data", competitions=CODES), client=client, clock=clock)
        first = worker.poll_once()
        assert len(worker.pipeline.events()) == 6 and len(exports) == 1
        assert all(cursor.exists() for cursor in worker.cursors.values())
        clock.advance(60)
        second = worker.poll_once()
    assert requested == list(CODES) * 2
    assert first["status"] == second["status"] == "ready"
    assert second["last_poll"] == dict(source_rows=6, changed_rows=0, unsupported_rows=0, requests=6)
    assert second["source_scope"] == dict(kind="competition_list", competition_codes=list(CODES), phases=[], pages=1)
    assert first["latest_snapshot"] == second["latest_snapshot"] and len(exports) == 1
    assert second["request_budget"]["used"] == 6
    with worker.pipeline.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_partial_fetch_failure_keeps_all_cursors_and_previous_snapshot(tmp_path):
    clock, fail, score = Clock(), [False], [2]
    def respond(request):
        code = request.url.path.split("/")[-2]
        if fail[0] and code == "PD":
            return httpx.Response(403, json={"error": TOKEN})
        return competition_response(request, score[0])
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data", competitions=CODES), client=client, clock=clock)
        first = worker.poll_once()
        cursor_bytes = {code: path.read_bytes() for code, path in worker.cursors.items()}
        events = worker.pipeline.events()
        fail[0], score[0] = True, 3
        clock.advance(60)
        failed = worker.poll_once()
    assert failed["status"] == "degraded" and failed["last_error_code"] == "authentication_denied"
    assert failed["latest_snapshot"] == first["latest_snapshot"]
    assert failed["last_success_at"] == first["last_success_at"]
    assert failed["last_poll"]["requests"] == 2 and failed["last_poll"]["source_rows"] == 1
    assert worker.pipeline.events() == events
    assert {code: path.read_bytes() for code, path in worker.cursors.items()} == cursor_bytes


def test_partial_cursor_commit_failure_recovers_without_revisions_or_extra_snapshot(tmp_path, monkeypatch):
    import arena_lakehouse.live as module
    original, clock = module.commit_state, Clock()
    def partial_commit(path, state):
        if path.name == "PD-cursor.json":
            raise OSError(TOKEN)
        original(path, state)
    with httpx.Client(transport=httpx.MockTransport(competition_response)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data", competitions=CODES), client=client, clock=clock)
        monkeypatch.setattr(module, "commit_state", partial_commit)
        failed = worker.poll_once()
        original_observations = worker.pipeline.events()
        assert failed["latest_snapshot"] is None and failed["last_error_code"] == "cursor_commit_failed"
        assert worker.cursors["PL"].exists() and not worker.cursors["PD"].exists()
        assert len(original_observations) == 6 and len(list(worker.snapshots.glob("snapshot-*"))) == 1
        pending = worker.ledger.prepared()
        intended = pending["intent"]["candidate"]
        request_count = requests_made(worker)
        monkeypatch.setattr(module, "commit_state", original)
        clock.advance(60)
        restarted = LiveWorker(tmp_path, LiveConfig("football-data", competitions=CODES), client=client, clock=clock)
        recovered = restarted.poll_once()
    assert recovered["status"] == "ready" and recovered["last_poll"]["changed_rows"] == 6
    assert recovered["last_poll"]["requests"] == 6
    assert restarted.pipeline.events() == original_observations
    assert len(list(restarted.snapshots.glob("snapshot-*"))) == 1
    assert all(path.exists() for path in restarted.cursors.values())
    selected = current_publication(restarted.root, now=clock())
    assert selected["poll_id"] == pending["poll_id"] == restarted.ledger.summary()["last"]["id"]
    assert all(selected[key] == value for key, value in intended.items())
    assert requests_made(restarted) == request_count


def test_multiscope_cadence_and_scope_changes_are_rejected_before_poll(tmp_path):
    with pytest.raises(ValueError, match="cadence"):
        LiveConfig("football-data", competitions=CODES, interval=30)
    with pytest.raises(ValueError, match="unique tuple"):
        LiveConfig("football-data", competitions=("PL", "PL"))
    with pytest.raises(ValueError, match="unique tuple"):
        LiveConfig("pandascore", competitions=CODES)
    with pytest.raises(ValueError, match="pagination"):
        LiveConfig("football-data", pages=2)
    LiveWorker(tmp_path, LiveConfig("football-data", competitions=CODES), clock=Clock())
    with pytest.raises(ValueError, match="source scope"):
        LiveWorker(tmp_path, LiveConfig("football-data"), clock=Clock())


def test_foreground_forever_has_bounded_cadence_and_clean_interrupt_status(tmp_path):
    clock = Clock()
    def stop_service(_seconds):
        clock.advance(60)
        raise KeyboardInterrupt
    with client_for({"matches": []}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        service = worker.run(forever=True, sleep=stop_service)
        assert next(service)["status"] == "empty"
        with pytest.raises(KeyboardInterrupt):
            next(service)
        worker.ledger.begin("2026-10-06T12:01:00.000000Z")
        status = worker.stop()
    assert status["status"] == "degraded" and status["last_error_code"] == "worker_interrupted"
    assert status["poll_counts"]["interrupted"] == 1
    assert status["last_success_at"] == "2026-10-06T12:00:00.000000Z"
