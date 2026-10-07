import json
import sqlite3
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from arena_lakehouse.live import LiveConfig, LiveWorker
from arena_lakehouse.source_health import source_health

NOW = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
ROW = dict(id=1, status="FINISHED", utcDate="2026-10-06T18:00:00Z",
    competition={"name":"TEST ONLY League"}, homeTeam={"id":1,"name":"TEST ONLY A"},
    awayTeam={"id":2,"name":"TEST ONLY B"}, score={"fullTime":{"home":2,"away":1}})


def test_actual_local_poll_ledger_records_failure_freshness_and_retains_publication(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "test-only-never-real")
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _: None)
    clock = [NOW]
    fail = [False]
    def respond(_):
        return httpx.Response(503) if fail[0] else httpx.Response(200, json={"matches":[ROW]})
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data", interval=60), client=client, clock=lambda:clock[0])
        worker.poll_once()
        before = (worker.snapshots / "CURRENT_PUBLICATION.json").read_bytes()
        fresh = source_health(tmp_path, "football-data", now=NOW, max_age_seconds=120)
        assert fresh["status"] == "healthy" and fresh["success_ratio"] == 1
        fail[0] = True
        clock[0] += timedelta(seconds=61)
        worker.poll_once()
        unhealthy = source_health(tmp_path, "football-data", now=NOW+timedelta(seconds=121), max_age_seconds=120)
        assert unhealthy["completed_checks"] == 2 and unhealthy["successful_checks"] == 1
        assert unhealthy["success_ratio"] == .5
        assert set(unhealthy["alerts"]) == {"source_check_overdue", "check_success_below_target"}
        assert unhealthy["error_counts"] == {"provider_unavailable":1}
        assert unhealthy["publication_selected"]
        assert (worker.snapshots / "CURRENT_PUBLICATION.json").read_bytes() == before
        with worker.ledger.connect() as db:
            requests = db.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
        source_health(tmp_path, "football-data", now=clock[0])
        with worker.ledger.connect() as db:
            assert db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == requests


def test_missing_ledger_does_not_create_worker_state(tmp_path):
    result = source_health(tmp_path, "pandascore", now=NOW)
    assert result["alerts"] == ["no_local_ledger"]
    assert not list(tmp_path.iterdir())


def test_window_uses_finished_check_time_and_does_not_fabricate_success_without_history(tmp_path):
    path = tmp_path / "pandascore" / "live.db"
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE polls(id INTEGER,started_at TEXT,finished_at TEXT,status TEXT,error_code TEXT)")
        db.execute("INSERT INTO polls VALUES(1,?,?,?,NULL)",
                   ((NOW-timedelta(seconds=400)).isoformat(), (NOW-timedelta(seconds=61)).isoformat(), "succeeded"))
        db.execute("INSERT INTO polls VALUES(2,?,NULL,'polling',NULL)", ((NOW-timedelta(seconds=300)).isoformat(),))
    result = source_health(tmp_path, "pandascore", now=NOW, window_seconds=60, max_age_seconds=120)
    assert result["completed_checks"] == 0 and result["success_ratio"] is None
    assert result["alerts"] == ["poll_not_finished"]
    assert result["success_age_seconds"] == 61


def test_corrupt_ledger_is_sanitized_and_read_only(tmp_path):
    path = tmp_path / "football-data" / "live.db"
    path.parent.mkdir()
    path.write_bytes(b"SECRET malformed data")
    before = path.read_bytes()
    report = source_health(tmp_path, "football-data", now=NOW)
    assert report["alerts"] == ["invalid_local_ledger"]
    assert "SECRET" not in json.dumps(report)
    assert path.read_bytes() == before


@pytest.mark.parametrize("policy", [{"window_seconds":0}, {"max_age_seconds":True},
                                    {"success_target":float("nan")}, {"success_target":1.1}])
def test_invalid_health_policy_is_rejected_before_creating_state(tmp_path, policy):
    with pytest.raises(ValueError):
        source_health(tmp_path, "football-data", now=NOW, **policy)
    assert not list(tmp_path.iterdir())
