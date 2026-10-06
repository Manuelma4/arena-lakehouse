import json
from collections import Counter

import pytest

from arena_lakehouse import cli
from arena_lakehouse.engine import Pipeline
from arena_lakehouse.prediction import train_and_predict
from arena_lakehouse.synthetic import SHOWCASE_COMPETITIONS, generate


def test_showcase_has_distinct_competition_windows_without_changing_results():
    original = generate(600, faults=False)
    showcase = generate(600, faults=False, showcase=True)
    by_id = {r["event_id"]: r for r in original}
    scheduled = [r for r in showcase if r["revision"] == 1]
    counts = Counter((r["sport"], r["competition"]) for r in scheduled)
    assert set(counts) == {(sport, name) for sport, names in SHOWCASE_COMPETITIONS.items() for name in names}
    assert set(counts.values()) == {100}
    windows = {}
    for row in showcase:
        assert {k: v for k, v in row.items() if k != "competition"} == {
            k: v for k, v in by_id[row["event_id"]].items() if k != "competition"}
    for row in scheduled:
        windows.setdefault(row["competition"], []).append(row["start_time"])
    for names in SHOWCASE_COMPETITIONS.values():
        assert all(max(windows[a]) < min(windows[b]) for a, b in zip(names, names[1:]))


def test_showcase_metadata_preserves_point_in_time_baseline_predictions():
    original, _ = train_and_predict(generate(160, faults=False))
    showcase, _ = train_and_predict(generate(160, faults=False, showcase=True))
    assert showcase == original and len(showcase) == 8


def test_demo_refuses_to_mix_showcase_and_legacy_workspaces(tmp_path, monkeypatch):
    workspace = tmp_path / "demo"
    pipeline = Pipeline(workspace)
    pipeline.ingest(generate(20))
    (workspace / "demo-config.json").write_text(json.dumps(dict(matches=20, seed=42)))
    before = pipeline.events()
    monkeypatch.setattr("sys.argv", ["arena-lakehouse", "--workspace", str(workspace),
                                    "demo", "--matches", "20", "--showcase"])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2 and pipeline.events() == before
