"""Polling status identifies the latest durable attempt, not its retained data."""

import copy
import json

import httpx

from arena_lakehouse.live import LiveConfig, LiveWorker, _utc
from arena_lakehouse.provider_publication import current_publication
from test_live import Clock, FOOTBALL, VALORANT, client_for, credentials as credentials, requests_made


def selected(worker, clock):
    return current_publication(worker.root, now=clock())


def test_successful_status_identifies_selected_publication_and_serialized_file(tmp_path):
    clock = Clock()
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        status = worker.poll_once()
    assert status["poll_id"] == selected(worker, clock)["poll_id"] == 1
    stored = json.loads((worker.snapshots / "LIVE_STATUS.json").read_text())
    assert stored["poll_id"] == 1


def test_unchanged_data_still_identifies_a_new_completed_collection(tmp_path):
    clock = Clock()
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        first = worker.poll_once()
        clock.advance(60)
        second = worker.poll_once()
    assert first["latest_snapshot"] == second["latest_snapshot"]
    assert first["poll_id"] == 1 and second["poll_id"] == selected(worker, clock)["poll_id"] == 2


def test_failed_new_attempt_preserves_the_older_publication_identity(tmp_path):
    clock, failure = Clock(), [False]

    def respond(_request):
        return httpx.Response(403) if failure[0] else httpx.Response(200, json={"matches": [FOOTBALL]})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        first = worker.poll_once()
        clock.advance(60)
        failure[0] = True
        second = worker.poll_once()
    assert second["status"] == "degraded" and second["poll_id"] == 2
    assert selected(worker, clock)["poll_id"] == first["poll_id"] == 1
    assert second["latest_snapshot"] == first["latest_snapshot"]


def test_calendar_only_success_retains_attempt_identity_without_snapshot(tmp_path):
    clock = Clock()
    pending = copy.deepcopy(VALORANT)
    pending.update(status="not_started", opponents=[], begin_at=None, end_at=None, results=[])
    with client_for([pending]) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        status = worker.poll_once()
    publication = selected(worker, clock)
    assert status["poll_id"] == publication["poll_id"] == 1
    assert publication["canonical"] is None and publication["calendar"] is not None


def test_prepared_recovery_keeps_original_attempt_without_more_requests(tmp_path, monkeypatch):
    import arena_lakehouse.live as module

    clock, original = Clock(), module.commit_state
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        monkeypatch.setattr(module, "commit_state", lambda *_args: (_ for _ in ()).throw(OSError("TEST ONLY interrupted cursor")))
        failed = worker.poll_once()
        requests = requests_made(worker)
        monkeypatch.setattr(module, "commit_state", original)
        clock.advance(60)
        recovered = worker.poll_once()
    assert failed["poll_id"] == recovered["poll_id"] == selected(worker, clock)["poll_id"] == 1
    assert requests_made(worker) == requests and recovered["status"] == "ready"


def test_initial_status_has_no_invented_attempt_id(tmp_path):
    clock = Clock()
    with client_for({"matches": []}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=clock)
        assert worker._status(_utc(clock()))["poll_id"] is None
        assert requests_made(worker) == 0
