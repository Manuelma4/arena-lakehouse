import itertools
import json
from datetime import datetime, timedelta

import pytest

from arena_lakehouse.engine import Pipeline, latest, team_stats
from arena_lakehouse.operations import STAGES, StageLedger, bounded_window, validated_new_events
from arena_lakehouse.schema import MatchEvent
from arena_lakehouse.synthetic import generate


@pytest.mark.parametrize("incompatibility", ["team_identity", "observation_time"])
def test_inconsistent_revision_chain_is_order_independent(tmp_path, incompatibility):
    versions = [r for r in generate(20, faults=False) if r["match_id"] == "synthetic:football:000000"]
    one, two = sorted(versions, key=lambda r: r["revision"])
    if incompatibility == "team_identity":
        two = dict(two, team_a_id="football:changed")
    else:
        one = dict(one, observed_at=two["observed_at"])
    for index, ordering in enumerate(itertools.permutations([one, two])):
        root = tmp_path / str(index)
        pipeline = Pipeline(root)
        for event in ordering:
            pipeline.ingest([event])
        assert pipeline.events() == []
        assert pipeline.quality()["summary"]["quarantined"] == 2
        # The decision survives a restart and later retransmission.
        Pipeline(root).ingest([one, two])
        assert Pipeline(root).events() == []
        assert Pipeline(root).quality()["summary"]["quarantined"] == 2


def test_reference_uses_latest_team_name_with_stable_scoped_identity():
    events = generate(60, faults=False)
    target = next(r for r in latest(events) if r["status"] == "scheduled" and r["sport"] == "football")
    corrected = MatchEvent.model_validate(dict(target, event_id=target["event_id"] + "rename", revision=2,
        observed_at="2026-01-01T00:00:00Z", team_a="Renamed Atlas")).row()
    results = team_stats(latest(events + [corrected]))
    team = next(r for r in results if r["team_id"] == corrected["team_a_id"] and r["sport"] == "football")
    assert team["team_name"] == "Renamed Atlas"
    assert len([r for r in results if r["team_id"] == team["team_id"] and r["sport"] == team["sport"]]) == 1


def test_global_event_id_collision_blocks_both_matches_regardless_of_order(tmp_path):
    completed = [e for e in generate(30, faults=False) if e["status"] == "finished" and e["sport"] == "football"][:2]
    original = completed[0]
    conflict = dict(completed[1], event_id=original["event_id"])
    for index, ordering in enumerate(itertools.permutations([original, conflict])):
        pipeline = Pipeline(tmp_path / str(index))
        pipeline.ingest(list(ordering))
        assert pipeline.events() == []
        pipeline.ingest([dict(original, event_id="retry-original"), dict(conflict, event_id="retry-conflict")])
        assert pipeline.events() == []


def test_spark_candidate_rejects_conflicts_before_commit_and_preserves_api_retry_time():
    rows = generate(30, faults=False)
    old = next(r for r in rows if r["status"] == "finished" and r["sport"] == "football")
    assert validated_new_events([old], [old, old]) == []
    with pytest.raises(ValueError, match="Conflicting"):
        validated_new_events([old], [dict(old, score_a=old["score_a"] + 1)])
    api = dict(old, provider="football-data" if old["sport"] == "football" else "pandascore")
    retry = dict(api, observed_at="2026-10-06T12:00:00Z")
    assert validated_new_events([api], [retry]) == []


def test_bounded_backfill_half_open_window_and_budgets():
    records = generate(40, faults=False)
    start = min(datetime.fromisoformat(e["observed_at"].replace("Z", "+00:00")) for e in records)
    end = start + timedelta(days=1)
    selected = bounded_window(records, start.isoformat(), end.isoformat())
    assert selected
    assert all(start <= datetime.fromisoformat(e["observed_at"].replace("Z", "+00:00")) < end for e in selected)
    with pytest.raises(ValueError, match="timezone"):
        bounded_window(records, "2025-01-01", "2025-01-02")
    with pytest.raises(ValueError, match="31 days"):
        bounded_window(records, start.isoformat(), (start + timedelta(days=32)).isoformat())
    with pytest.raises(ValueError, match="event budget"):
        bounded_window(records, start.isoformat(), end.isoformat(), max_events=1)


def test_stage_resume_skips_successful_commits_and_records_attempts(tmp_path):
    ledger = StageLedger(tmp_path)
    calls = []
    callbacks = {stage: lambda payload, stage=stage: calls.append(stage) for stage in STAGES}
    with ledger.writer():
        run = ledger.create({"events": []})
        with pytest.raises(RuntimeError, match="Injected"):
            ledger.execute(run, callbacks, fail_after="silver")
        assert ledger.pending() == run
        with pytest.raises(ValueError, match="Resume"):
            ledger.create({"events": []})
    restored = StageLedger(tmp_path)
    with restored.writer():
        restored.execute(run, callbacks)
    assert calls == list(STAGES)
    assert restored.pending() is None
    assert all(a["status"] == "succeeded" for a in restored.attempts(run))


def test_stage_transient_retries_are_bounded_and_permanent_failures_surface(tmp_path):
    ledger, calls = StageLedger(tmp_path), []
    def flaky(_):
        calls.append("bronze")
        if len(calls) == 1:
            raise TimeoutError("source transient")
    callbacks = {stage: lambda _: None for stage in STAGES}
    callbacks["bronze"] = flaky
    run = ledger.create({})
    ledger.execute(run, callbacks)
    attempts = ledger.attempts(run)
    assert [a["status"] for a in attempts if a["stage"] == "bronze"] == ["failed", "succeeded"]
    run2 = ledger.create({})
    callbacks["bronze"] = lambda _: (_ for _ in ()).throw(ValueError("invalid schema"))
    with pytest.raises(ValueError):
        ledger.execute(run2, callbacks)
    assert len(ledger.attempts(run2)) == 1


def test_stage_writer_lock_prevents_concurrent_mutation(tmp_path):
    ledger = StageLedger(tmp_path)
    with ledger.writer():
        with pytest.raises(RuntimeError, match="writer lock"):
            with StageLedger(tmp_path).writer():
                pass


def test_interrupted_attempt_is_retried_after_process_restart(tmp_path):
    ledger = StageLedger(tmp_path)
    run = ledger.create({})
    with ledger.connect() as db:
        db.execute("INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?)",
                   (run, "bronze", 1, "2026-10-06T00:00:00Z", None, "running", None, None))
    ledger.execute(run, {stage: lambda _: None for stage in STAGES})
    assert ledger.attempts(run)[0]["status"] == "interrupted"
    assert ledger.attempts(run)[1]["attempt"] == 2


def test_demo_workspace_configuration_cannot_be_mixed(tmp_path, monkeypatch, capsys):
    from arena_lakehouse.cli import main
    root = tmp_path / "demo"
    root.mkdir()
    (root / "demo-config.json").write_text(json.dumps({"matches": 20, "seed": 42}))
    monkeypatch.setattr("sys.argv", ["arena-lakehouse", "--workspace", str(root), "demo", "--matches", "30"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
    assert "another seed/match configuration" in capsys.readouterr().err
