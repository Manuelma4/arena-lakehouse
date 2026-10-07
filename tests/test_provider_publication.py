"""Synthetic provider publications; no provider, model, or Spark execution."""

import copy
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

import arena_lakehouse.live as live
import arena_lakehouse.provider_publication as publication
from arena_lakehouse.calendar import publish_calendar, validate_calendar
from arena_lakehouse.live import LiveConfig, LiveWorker
from arena_lakehouse.snapshot import export_snapshot

NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
RESOLVED = dict(id=201, name="Example fixture", status="not_started", begin_at="2026-10-07T18:00:00Z",
                tournament={"name": "Example tournament"}, number_of_games=3,
                opponents=[{"opponent": {"id": 1, "name": "Team One"}},
                           {"opponent": {"id": 2, "name": "Team Two"}}], results=[])
UNRESOLVED = dict(id=201, name="Example pending fixture", status="not_started", begin_at=None,
                  tournament=None, opponents=[], results=[])


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        return self.value

    def advance(self):
        self.value += timedelta(seconds=60)


@pytest.fixture(autouse=True)
def synthetic_credentials(monkeypatch):
    monkeypatch.setenv("PANDASCORE_TOKEN", "synthetic-unit-test")
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "synthetic-unit-test")
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _seconds: None)


def panda_client(rows, requests):
    def respond(request):
        phase = request.url.path.rsplit("/", 1)[-1]
        requests.append(phase)
        return httpx.Response(200, json=rows if phase == "upcoming" else [])
    return httpx.Client(transport=httpx.MockTransport(respond))


def selection(worker, clock):
    return publication.current_publication(worker.root, now=clock())


def test_config_rejects_scope_codes_that_cannot_be_published():
    with pytest.raises(ValueError, match="uppercase"):
        LiveConfig("football-data", competition="pl")
    with pytest.raises(ValueError, match="competition codes"):
        LiveConfig("football-data", competitions=("PL", "pd"))


@pytest.mark.parametrize("rows", [[], [UNRESOLVED]])
def test_complete_empty_or_unresolved_panda_poll_selects_valid_calendar(tmp_path, rows):
    clock, requests = Clock(), []
    with panda_client(rows, requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        status = worker.poll_once()
    selected = selection(worker, clock)
    assert status["status"] == "empty" and selected["poll_id"] == 1
    assert selected["canonical"] is None and selected["calendar"] is not None
    assert requests == ["past", "upcoming", "running"]
    package = worker.root / "calendars" / selected["calendar"]["basename"]
    calendar = validate_calendar(package)
    assert calendar["quality"]["source_records"] == len(rows)
    assert len(calendar["sources"]) == 3
    assert not (worker.snapshots / "LATEST").exists()
    assert (package.parent / "CALENDAR_LATEST").read_text().strip() == package.name
    unsigned = {key: value for key, value in selected.items() if key != "fingerprint"}
    assert selected["fingerprint"] == publication.sha256(publication.canonical(unsigned))
    if rows:
        row = calendar["announcements"][0]
        assert row["canonical_match_id"] is row["start_time"] is row["team_a"] is row["team_b"] is None


def test_same_identity_nullable_announcement_retains_valid_historic_canonical(tmp_path):
    clock, rows, requests = Clock(), [copy.deepcopy(RESOLVED)], []
    with panda_client(rows, requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        assert worker.poll_once()["status"] == "ready"
        first = selection(worker, clock)
        observations = worker.pipeline.events()
        rows[:] = [copy.deepcopy(UNRESOLVED)]
        clock.advance()
        assert worker.poll_once()["status"] == "ready"
        second = selection(worker, clock)
    assert second["poll_id"] == 2 and second["canonical"] == first["canonical"]
    assert second["calendar"] != first["calendar"] and worker.pipeline.events() == observations
    calendar = validate_calendar(worker.root / "calendars" / second["calendar"]["basename"])
    row = calendar["announcements"][0]
    assert row["canonical_match_id"] == "pandascore:201"
    assert row["start_time"] is row["team_a"] is row["team_b"] is None
    assert publication.validate_publication(worker.root, second, now=clock()) == second


def test_partial_panda_phase_never_replaces_last_good_pair_or_cursors(tmp_path):
    clock, requests, fail = Clock(), [], [False]
    def respond(request):
        phase = request.url.path.rsplit("/", 1)[-1]
        requests.append(phase)
        return httpx.Response(403) if fail[0] and phase == "upcoming" else httpx.Response(200, json=[])
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        first = worker.poll_once()
        selector_bytes = (worker.snapshots / publication.SELECTOR).read_bytes()
        cursor_bytes = worker.cursor.read_bytes()
        calendar_pointer = (worker.root / "calendars" / "CALENDAR_LATEST").read_bytes()
        fail[0] = True
        clock.advance()
        failed = worker.poll_once()
    assert failed["last_error_code"] == "authentication_denied"
    assert failed["last_success_at"] == first["last_success_at"]
    assert (worker.snapshots / publication.SELECTOR).read_bytes() == selector_bytes
    assert worker.cursor.read_bytes() == cursor_bytes
    assert (worker.root / "calendars" / "CALENDAR_LATEST").read_bytes() == calendar_pointer
    assert requests == ["past", "upcoming", "running", "past", "upcoming"]
    assert worker.ledger.prepared() is None


def crash_after(original):
    def wrapped(*args, **kwargs):
        original(*args, **kwargs)
        raise KeyboardInterrupt("synthetic crash boundary")
    return wrapped


@pytest.mark.parametrize("boundary", ["prepared", "before_cursor", "after_cursor", "selection_persisted",
                                      "before_selector", "after_selector", "ledger", "legacy", "status", "ack"])
def test_restart_finishes_same_prepared_poll_without_requests_or_reselection(tmp_path, monkeypatch, boundary):
    clock, requests = Clock(), []
    with panda_client([UNRESOLVED], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        with monkeypatch.context() as patch:
            if boundary == "prepared":
                patch.setattr(worker.ledger, "prepare", crash_after(worker.ledger.prepare))
            elif boundary in {"before_cursor", "after_cursor"}:
                original = live.commit_state
                if boundary == "after_cursor":
                    patch.setattr(live, "commit_state", crash_after(original))
                else:
                    patch.setattr(live, "commit_state", lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()))
            elif boundary == "selection_persisted":
                patch.setattr(worker.ledger, "update_selection", crash_after(worker.ledger.update_selection))
            elif boundary in {"before_selector", "after_selector"}:
                original = publication.atomic_json
                def interrupt_selector(path, value):
                    if path.name == publication.SELECTOR and boundary == "before_selector":
                        raise KeyboardInterrupt()
                    original(path, value)
                    if path.name == publication.SELECTOR:
                        raise KeyboardInterrupt()
                patch.setattr(publication, "atomic_json", interrupt_selector)
            elif boundary == "ledger":
                patch.setattr(worker.ledger, "complete_publication", crash_after(worker.ledger.complete_publication))
            elif boundary == "legacy":
                patch.setattr(worker, "_legacy_pointers", crash_after(worker._legacy_pointers))
            elif boundary == "status":
                original = worker._publish_status
                def interrupt_final_status(now):
                    result = original(now)
                    if worker.ledger.summary()["last"]["status"] == "succeeded":
                        raise KeyboardInterrupt()
                    return result
                patch.setattr(worker, "_publish_status", interrupt_final_status)
            else:
                patch.setattr(worker.ledger, "acknowledge_publication", lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()))
            with pytest.raises(KeyboardInterrupt):
                worker.poll_once()
        pending = worker.ledger.prepared()
        intended = copy.deepcopy(pending["intent"]["candidate"])
        before = selection(worker, clock)
        observations = worker.pipeline.events()
        assert pending["poll_id"] == 1 and requests == ["past", "upcoming", "running"]
        if before is None:
            assert not (worker.root / "calendars" / "CALENDAR_LATEST").exists()
        clock.advance()
        restarted = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        recovered = restarted.poll_once()
        after = selection(restarted, clock)
    assert recovered["status"] == "empty" and recovered["poll_counts"]["attempted"] == 1
    assert recovered["last_poll"] == dict(source_rows=1, changed_rows=0, unsupported_rows=1, requests=3)
    assert requests == ["past", "upcoming", "running"]
    assert after["poll_id"] == restarted.ledger.summary()["last"]["id"] == 1
    assert all(after[key] == value for key, value in intended.items())
    assert restarted.pipeline.events() == observations and restarted.ledger.prepared() is None
    if before is not None:
        assert after == before
    else:
        assert after["selected_at"] == publication.stamp(clock())
        assert after["selected_at"] > after["collected_at"]


def test_interruption_before_intent_is_a_new_collection_attempt(tmp_path, monkeypatch):
    clock, requests = Clock(), []
    with panda_client([], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        with monkeypatch.context() as patch:
            patch.setattr(worker.ledger, "prepare", lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()))
            with pytest.raises(KeyboardInterrupt):
                worker.poll_once()
        assert worker.ledger.prepared() is None and selection(worker, clock) is None
        assert not worker.cursor.exists() and not (worker.root / "calendars" / "CALENDAR_LATEST").exists()
        clock.advance()
        recovered = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        status = recovered.poll_once()
    assert selection(recovered, clock)["poll_id"] == 2
    assert status["poll_counts"]["interrupted"] == status["poll_counts"]["succeeded"] == 1
    assert requests == ["past", "upcoming", "running"] * 2


def test_selected_poll_is_not_downgraded_after_status_write_failure(tmp_path, monkeypatch):
    clock, requests = Clock(), []
    with panda_client([], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        original, failures = worker._publish_status, []
        def fail_once(now):
            if worker.ledger.summary()["last"]["status"] == "succeeded" and not failures:
                failures.append(True)
                raise OSError("synthetic status failure")
            return original(now)
        monkeypatch.setattr(worker, "_publish_status", fail_once)
        assert worker.poll_once()["status"] == "empty"
        before = selection(worker, clock)
        clock.advance()
        assert worker.poll_once()["status"] == "empty"
    assert selection(worker, clock) == before and worker.ledger.prepared() is None
    assert requests == ["past", "upcoming", "running"]


def test_selected_but_unacknowledged_tamper_is_degraded_and_never_recollected(tmp_path, monkeypatch):
    clock, requests = Clock(), []
    with panda_client([], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        with monkeypatch.context() as patch:
            patch.setattr(worker.ledger, "acknowledge_publication", lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()))
            with pytest.raises(KeyboardInterrupt):
                worker.poll_once()
        assert worker.ledger.summary()["last"]["status"] == "succeeded"
        pending = worker.ledger.prepared()["intent"]
        archive = worker.root / pending["archives"][0]["path"]
        archive.write_bytes(archive.read_bytes() + b" ")
        clock.advance()
        status = worker.poll_once()
    assert status["status"] == "degraded" and status["last_error_code"] == "publication_failed"
    assert worker.ledger.prepared() is not None and requests == ["past", "upcoming", "running"]


@pytest.mark.parametrize("tamper", ["archive", "intent", "manifest", "linked_manifest"])
def test_recovery_rejects_tampered_exact_evidence_without_new_requests(tmp_path, monkeypatch, tamper):
    clock, requests = Clock(), []
    with panda_client([], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        with monkeypatch.context() as patch:
            patch.setattr(worker.ledger, "prepare", crash_after(worker.ledger.prepare))
            with pytest.raises(KeyboardInterrupt):
                worker.poll_once()
        pending = worker.ledger.prepared()["intent"]
        package = worker.root / "calendars" / pending["candidate"]["calendar"]["basename"]
        if tamper == "archive":
            archive = worker.root / pending["archives"][0]["path"]
            archive.write_bytes(archive.read_bytes() + b" ")
        elif tamper == "intent":
            with worker.ledger.connect() as db:
                db.execute("UPDATE publication_intents SET payload=payload || ' '")
                db.commit()
        elif tamper == "manifest":
            (package / "manifest.json").write_bytes((package / "manifest.json").read_bytes() + b" ")
        else:
            target = package / "manifest.json"
            outside = tmp_path / "outside.json"
            outside.write_bytes(target.read_bytes())
            target.unlink()
            try:
                target.symlink_to(outside)
            except OSError:
                pytest.skip("Creating symlinks requires local Windows privilege")
        clock.advance()
        failed = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock).poll_once()
    assert failed["last_error_code"] == "publication_failed" and failed["status"] == "degraded"
    assert requests == ["past", "upcoming", "running"] and not worker.cursor.exists()
    assert not (worker.snapshots / publication.SELECTOR).exists()


def test_calendar_link_requires_selected_canonical_identity(tmp_path):
    clock, requests = Clock(), []
    with panda_client([UNRESOLVED], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        worker.poll_once()
    current = selection(worker, clock)
    with worker.ledger.connect() as db:
        body = db.execute("SELECT payload FROM publication_intents").fetchone()[0]
    intent = json.loads(body)
    paths = [worker.root / row["path"] for row in intent["archives"]]
    manifest = publish_calendar(paths, worker.root / "calendars", archive_root=worker.root / "provider_raw",
                                canonical_match_ids=["pandascore:201"], generated_at=clock(), select_latest=False)
    path = worker.root / "calendars" / f"calendar-{manifest['dataset_version']}"
    candidate = {key: current[key] for key in publication.Candidate.model_fields}
    candidate["calendar"] = publication.describe_calendar(path, now=clock())
    with pytest.raises(publication.PublicationError, match="canonical identity"):
        publication.validate_candidate(worker.root, candidate, now=clock())


def test_standalone_calendar_selection_default_is_preserved(tmp_path):
    clock, requests = Clock(), []
    with panda_client([], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        worker.poll_once()
    with worker.ledger.connect() as db:
        intent = json.loads(db.execute("SELECT payload FROM publication_intents").fetchone()[0])
    paths = [worker.root / row["path"] for row in intent["archives"]]
    output = tmp_path / "standalone"
    manifest = publish_calendar(paths, output, archive_root=worker.root / "provider_raw", generated_at=clock())
    assert (output / "CALENDAR_LATEST").read_text().strip() == f"calendar-{manifest['dataset_version']}"


def test_selector_rejects_fingerprint_clock_scope_and_descriptor_mutations(tmp_path):
    clock, requests = Clock(), []
    with panda_client([], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        worker.poll_once()
    selected = selection(worker, clock)
    mutations = []
    changed = copy.deepcopy(selected)
    changed["fingerprint"] = "0" * 64
    mutations.append(changed)
    changed = copy.deepcopy(selected)
    changed["poll_id"] = True
    mutations.append(changed)
    changed = copy.deepcopy(selected)
    changed["selected_at"] = "2026-10-06T11:59:59.000000Z"
    mutations.append(changed)
    changed = copy.deepcopy(selected)
    changed["source_scope"]["pages"] = 2
    mutations.append(changed)
    changed = copy.deepcopy(selected)
    changed["calendar"]["manifest_sha256"] = "0" * 64
    mutations.append(changed)
    changed = copy.deepcopy(selected)
    changed["calendar"]["basename"] = "../calendar-0123456789abcdef"
    mutations.append(changed)
    changed = copy.deepcopy(selected)
    changed["unknown"] = None
    mutations.append(changed)
    for changed in mutations:
        if changed["fingerprint"] != "0" * 64:
            changed["fingerprint"] = publication.sha256(publication.canonical(
                {key: value for key, value in changed.items() if key != "fingerprint"}))
        with pytest.raises(publication.PublicationError):
            publication.validate_publication(worker.root, changed, now=clock())
    assert selection(worker, clock) == selected


def test_standalone_snapshot_default_and_candidate_selection(tmp_path):
    clock, requests = Clock(), []
    with panda_client([RESOLVED], requests) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        assert worker.poll_once()["status"] == "ready"
    output = tmp_path / "standalone-snapshots"
    candidate = export_snapshot(worker.pipeline, output, with_predictions=False, select_latest=False, generated_at=clock())
    assert candidate.is_dir() and not (output / "LATEST").exists()
    selected = export_snapshot(worker.pipeline, output, with_predictions=False, generated_at=clock())
    assert (output / "LATEST").read_text().strip() == selected.name
