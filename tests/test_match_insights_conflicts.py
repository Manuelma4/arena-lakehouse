"""Conflicting source phases cannot be resolved by whichever endpoint is last."""

import copy
import json
from datetime import datetime, timezone

from arena_lakehouse.match_insights import publish
from arena_lakehouse.provider_publication import atomic_json


def test_conflicting_phase_metadata_omitted(tmp_path):
    now = datetime(2026, 10, 7, 19, tzinfo=timezone.utc)
    root = tmp_path / "pandascore"
    raw = dict(id=100, status="finished", opponents=[], games=[], modified_at="2026-10-07T18:00:00Z")
    other = copy.deepcopy(raw)
    other["status"] = "not_started"
    paths = []
    for index, (phase, payload) in enumerate(zip(("past", "upcoming", "running"), ([raw], [other], []), strict=True)):
        path = root / "provider_raw/pandascore/2026-10-07" / f"{index:032x}.json"
        atomic_json(path, dict(provider="pandascore", endpoint=f"valorant/matches/{phase}?page=1&per_page=100",
            requested_at=now.isoformat(), observed_at=now.isoformat(), payload=payload))
        paths.append(path)
    selection = dict(provider="pandascore", fingerprint="a"*64, collected_at=now.isoformat(),
        source_scope=dict(kind="match_phases", phases=["past", "upcoming", "running"], competition_codes=[], pages=1))
    result = publish(root, selection, paths, started_at=now, generated_at=now)
    assert result["records"] == 0 and result["conflicting_matches_omitted"] == 1
    pointer = json.loads((root / "snapshots/CURRENT_MATCH_INSIGHTS.json").read_text())
    assert json.loads((root / "match-insights" / pointer["basename"]).read_text())["records"] == []
