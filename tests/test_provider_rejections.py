"""Failed candidates retain private row provenance without partial publication."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3

import httpx
import pytest

from arena_lakehouse.connectors import collect
from arena_lakehouse.live import LiveConfig, LiveLedger, LiveWorker
from arena_lakehouse.provider_publication import current_publication
from arena_lakehouse.provider_rejections import RejectedProviderRow, RowRejection

NOW = datetime(2026, 10, 13, 12, tzinfo=timezone.utc)
TOKEN = "fixture-only-private-token"
FOOTBALL = dict(id=100, status="FINISHED", utcDate="2026-10-12T18:00:00Z",
    competition={"name": "Private League"}, homeTeam={"id": 1, "name": "Private Home"},
    awayTeam={"id": 2, "name": "Private Away"}, score={"fullTime": {"home": 2, "away": 1}})


@pytest.fixture(autouse=True)
def credentials(monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", TOKEN)
    monkeypatch.setenv("PANDASCORE_TOKEN", TOKEN)
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _: None)


@pytest.mark.parametrize("score", [None, {"home": -1, "away": 1}, {"home": 2}])
def test_collector_identifies_exact_archived_row_and_suppresses_validation_input(tmp_path, score):
    invalid = dict(FOOTBALL, id=101, score={"fullTime": score or {}})
    with httpx.Client(transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"matches": [FOOTBALL, invalid]}))) as client:
        with pytest.raises(RejectedProviderRow) as captured:
            collect("football-data", tmp_path / "cursor.json", client=client, clock=lambda: NOW)
    error = captured.value
    record = error.rejection.record()
    archive = tmp_path / record["archive_path"]
    assert record["row_index"] == 1
    assert record["provider"] == "football-data" and record["endpoint"] == "competitions/PL/matches"
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == record["archive_sha256"]
    assert json.loads(archive.read_bytes())["payload"]["matches"][1] == invalid
    assert str(error) == "invalid_provider_payload" and error.__suppress_context__
    assert not (tmp_path / "cursor.json").exists()
    assert all(secret not in json.dumps(record) + str(error)
               for secret in (TOKEN, "Private Home", "Private Away", "Private League", "Finished matches"))


def test_second_competition_failure_aborts_entire_candidate_and_survives_restart(tmp_path):
    clock, failed = [NOW], [False]
    def respond(request):
        code = request.url.path.split("/")[-2]
        row = dict(FOOTBALL, id=100 if code == "PL" else 200,
                   score={"fullTime": {"home": 3 if failed[0] else 2, "away": 1}})
        rows = [row]
        if failed[0] and code == "BSA":
            rows.append(dict(row, id=201, score={"fullTime": {"home": None, "away": None}}))
        return httpx.Response(200, json={"matches": rows})
    config = LiveConfig("football-data", competitions=("PL", "BSA"))
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, config, client=client, clock=lambda: clock[0])
        first = worker.poll_once()
        assert first["status"] == "ready" and len(worker.pipeline.events()) == 2
        selection = current_publication(worker.root, now=clock[0])
        cursors = {code: path.read_bytes() for code, path in worker.cursors.items()}
        events = worker.pipeline.events()
        failed[0], clock[0] = True, NOW + timedelta(seconds=60)
        status = worker.poll_once()
        assert status["status"] == "degraded" and status["last_error_code"] == "invalid_provider_payload"
        assert status["latest_snapshot"] == first["latest_snapshot"]
        assert status["last_success_at"] == first["last_success_at"]
        assert status["last_poll"]["changed_rows"] == 0
        assert current_publication(worker.root, now=clock[0]) == selection
        assert {code: path.read_bytes() for code, path in worker.cursors.items()} == cursors
        assert worker.pipeline.events() == events and worker.ledger.prepared() is None
        incident = worker.ledger.rejected_row(status["poll_id"])
        assert incident["row_index"] == 1 and incident["endpoint"] == "competitions/BSA/matches"
        assert incident["recorded_at"] == status["last_attempt_at"]
        assert incident["category"] == "invalid_match_event"
        assert incident["archive_sha256"] == hashlib.sha256((worker.root / incident["archive_path"]).read_bytes()).hexdigest()
        assert all(secret not in json.dumps(incident) + json.dumps(status)
                   for secret in (TOKEN, "Private Home", "Private Away", "Private League"))
        restarted = LiveWorker(tmp_path, config, client=client, clock=lambda: clock[0])
        assert restarted.ledger.rejected_row(status["poll_id"]) == incident
        assert restarted.ledger.rejected_row(first["poll_id"]) is None
        with closing(restarted.ledger.connect()) as db:
            assert db.execute("SELECT COUNT(*) FROM rejected_provider_rows").fetchone()[0] == 1
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                db.execute("UPDATE rejected_provider_rows SET row_index=99")
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                db.execute("DELETE FROM rejected_provider_rows")
        # A successful later full candidate is allowed, without deleting its earlier incident.
        failed[0], clock[0] = False, NOW + timedelta(seconds=120)
        recovered = restarted.poll_once()
        assert recovered["status"] == "ready"
        assert restarted.ledger.rejected_row(status["poll_id"]) == incident
        assert restarted.ledger.rejected_row(recovered["poll_id"]) is None


def rejection():
    return RowRejection("football-data", "competitions/PL/matches",
                        "provider_raw/football-data/2026-10-13/" + "a" * 32 + ".json", "b" * 64, 1)


def test_rejection_insert_and_failed_poll_are_one_transaction(tmp_path):
    ledger = LiveLedger(tmp_path / "live.db", LiveConfig("football-data"))
    poll = ledger.begin(NOW.isoformat())
    with closing(ledger.connect()) as db, db:
        db.execute("CREATE TRIGGER fail_incident BEFORE INSERT ON rejected_provider_rows BEGIN "
                   "SELECT RAISE(ABORT, 'fixture write failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match="fixture write failure"):
        ledger.finish(poll, NOW.isoformat(), "failed", "invalid_provider_payload", rejection=rejection())
    assert ledger.summary()["last"]["status"] == "polling"
    assert ledger.rejected_row(poll) is None


@pytest.mark.parametrize("status,error", [("succeeded", None), ("failed", "network_error"),
                                          ("quota_limited", "quota_exhausted")])
def test_rejection_cannot_be_attached_to_an_unrelated_terminal_status(tmp_path, status, error):
    ledger = LiveLedger(tmp_path / "live.db", LiveConfig("football-data"))
    poll = ledger.begin(NOW.isoformat())
    with pytest.raises(ValueError, match="context"):
        ledger.finish(poll, NOW.isoformat(), status, error, rejection=rejection())
    assert ledger.summary()["last"]["status"] == "polling" and ledger.rejected_row(poll) is None


def test_rejection_cannot_relabel_a_successful_or_absent_poll(tmp_path):
    ledger = LiveLedger(tmp_path / "live.db", LiveConfig("football-data"))
    poll = ledger.begin(NOW.isoformat())
    ledger.finish(poll, NOW.isoformat(), "succeeded")
    for candidate in (poll, poll + 1):
        with pytest.raises(ValueError, match="Missing failed"):
            ledger.finish(candidate, NOW.isoformat(), "failed", "invalid_provider_payload", rejection=rejection())
    assert ledger.summary()["last"]["status"] == "succeeded" and ledger.rejected_row(poll) is None


@pytest.mark.parametrize("field,value", [
    ("provider", "secret"), ("endpoint", "competitions/PL/matches?token=secret"),
    ("endpoint", "competitions/../matches"), ("archive_path", "C:/private/archive.json"),
    ("archive_path", "provider_raw/football-data/2026-10-13/../secret.json"),
    ("archive_sha256", "secret"), ("row_index", -1), ("row_index", True),
    ("category", "Finished match Private Home invalid: secret"),
])
def test_rejection_rejects_unbounded_or_secret_bearing_provenance(field, value):
    record = rejection().record()
    record[field] = value
    with pytest.raises(ValueError, match="provenance"):
        RowRejection(**record)


def test_pandascore_calendar_eligibility_fallback_is_not_a_rejection(tmp_path):
    row = dict(id=200, status="finished", begin_at="2026-10-12T18:00:00Z",
        end_at="2026-10-12T20:00:00Z", tournament={"name": "VCT"}, number_of_games=3,
        opponents=[{"opponent": {"id": 1, "name": "One"}}, {"opponent": {"id": 2, "name": "Two"}}],
        results=[])
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
            200, json=[row] if request.url.path.endswith("/past") else []))) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=lambda: NOW)
        status = worker.poll_once()
    assert status["last_error_code"] is None and status["last_poll"]["unsupported_rows"] == 1
    assert worker.ledger.rejected_row(status["poll_id"]) is None
    assert current_publication(worker.root, now=NOW)["calendar"] is not None


def test_canonical_failure_preserves_pandascore_page_local_index(tmp_path, monkeypatch):
    # Inject a later canonical-contract failure after valid calendar validation;
    # real normalize_valorant eligibility fallback remains separately checked.
    import arena_lakehouse.connectors as module
    original = module.normalize_valorant
    row = dict(id=200, status="finished", begin_at="2026-10-12T18:00:00Z",
        end_at="2026-10-12T20:00:00Z", tournament={"name": "VCT"}, number_of_games=3,
        opponents=[{"opponent": {"id": 1, "name": "One"}}, {"opponent": {"id": 2, "name": "Two"}}],
        results=[{"team_id": 1, "score": 2}, {"team_id": 2, "score": 1}])
    def normalize(raw, observed):
        match = original(raw, observed)
        if raw["id"] == 202:
            match["best_of"] = 0
        return match
    monkeypatch.setattr(module, "normalize_valorant", normalize)
    def respond(request):
        page = int(request.url.params["page"])
        rows = [row] if page == 1 else [dict(row, id=201), dict(row, id=202)]
        return httpx.Response(200, json=rows if request.url.path.endswith("/past") else [])
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(RejectedProviderRow) as captured:
            collect("pandascore", tmp_path / "cursor.json", pages=2, client=client, clock=lambda: NOW)
    record = captured.value.rejection.record()
    assert record["endpoint"] == "valorant/matches/past?page=2&per_page=100"
    assert record["row_index"] == 1  # Not flattened row 2.
    body = (tmp_path / record["archive_path"]).read_bytes()
    assert record["archive_sha256"] == hashlib.sha256(body).hexdigest()
    assert json.loads(body)["payload"][record["row_index"]]["id"] == 202
    assert not (tmp_path / "cursor.json").exists()
