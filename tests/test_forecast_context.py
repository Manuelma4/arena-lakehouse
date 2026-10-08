"""Original TEST ONLY seasons and source records, no provider requests."""
import copy
import json

import pytest

from arena_lakehouse.forecast_context import build_selected, normalize
from arena_lakehouse.provider_publication import PublicationError
from test_live import FOOTBALL, NOW, Clock, LiveConfig, LiveWorker, client_for


def seasonal():
    value = copy.deepcopy(FOOTBALL)
    value["competition"]["id"] = 2021
    value["season"] = dict(id=1900, startDate="2026-08-01", endDate="2027-05-31", winner={"id": 999})
    return value


def test_supplied_season_and_home_away_roles_exclude_future_season_winner():
    row = normalize(seasonal(), "a" * 64, NOW)
    assert row["season_id"] == "football-data:season:1900"
    assert row["competition_id"] == "football-data:competition:2021"
    assert row["home_team_id"] == "football-data:1" and row["away_team_id"] == "football-data:2"
    assert row["season_start"] == "2026-08-01" and row["season_end"] == "2027-05-31"
    assert "winner" not in json.dumps(row)


@pytest.mark.parametrize("change", [{"id": True}, {"id": "1900"}, {"startDate": "2027-08-01", "endDate": "2026-05-31"}])
def test_invalid_season_stays_unknown_without_inference(change):
    raw = seasonal()
    raw["season"].update(change)
    assert normalize(raw, "a" * 64, NOW)["season_id"] is None
    assert normalize(FOOTBALL, "a" * 64, NOW)["season_id"] is None


def test_optional_worker_package_and_rebuild_use_only_exact_successful_originals(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "TEST ONLY TOKEN")
    with client_for({"matches": [seasonal()]}) as client:
        requests = []
        original = client.get
        def get(*args, **kwargs):
            requests.append(1)
            return original(*args, **kwargs)
        monkeypatch.setattr(client, "get", get)
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        assert worker.poll_once()["status"] == "ready"
    pointer = worker.snapshots / "CURRENT_FORECAST_CONTEXT.json"
    package = json.loads(pointer.read_text())
    assert len(requests) == 1
    body = json.loads((worker.root / "forecast-context" / package["basename"]).read_text())
    assert body["records"][0]["season_id"] == "football-data:season:1900"
    assert "TEST ONLY TOKEN" not in json.dumps(body)
    assert build_selected(worker.root, now=NOW)["known_season_records"] == 1
    before = pointer.read_bytes()
    archive = next((worker.root / "provider_raw").rglob("*.json"))
    archive.write_text("{}", encoding="utf-8")
    with pytest.raises(PublicationError):
        build_selected(worker.root, now=NOW)
    assert pointer.read_bytes() == before
