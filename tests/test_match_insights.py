"""Original match-detail fixtures; no actual provider calls or inference."""

import copy
import json

import pytest

from arena_lakehouse.match_insights import build_selected, normalize
from arena_lakehouse.provider_publication import PublicationError

from test_live import FOOTBALL, VALORANT, NOW, Clock, LiveConfig, LiveWorker, client_for


def test_whitelist_lineup_nulls_and_real_zero():
    raw = copy.deepcopy(FOOTBALL)
    raw["homeTeam"].update(formation="4-3-3", coach={"name": "TEST ONLY coach"},
        lineup=[dict(id=11, name="TEST ONLY player", position="Goalkeeper", shirtNumber=0)],
        statistics={"shots": 0, "ball_possession": 54, "red_cards": None, "token": "must not be copied"})
    record = normalize(raw, "football-data", "a"*64, NOW)
    assert record["teams"][0]["statistics"]["shots"] == 0
    assert record["teams"][0]["statistics"]["red_cards"] is None
    assert record["teams"][0]["lineup"][0]["shirt_number"] == 0
    assert record["lineup_status"] == "supplied"
    assert record["player_statistics"] == [] and record["player_statistics_status"] == "not_supplied"
    assert "must not be copied" not in json.dumps(record)


def test_unplayed_map_cannot_establish_winner_or_duration():
    raw = copy.deepcopy(VALORANT)
    raw["games"] = [dict(id=71, match_id=200, position=1, status="not_started", finished=False,
                         length=0, winner={"id": 1, "type": "Team"})]
    record = normalize(raw, "pandascore", "a"*64, NOW)
    assert record["games"][0]["winner_team_id"] is None
    assert record["games"][0]["length_seconds"] is None
    raw["games"][0].update(status="finished", finished=True, length=2620)
    game = normalize(raw, "pandascore", "a"*64, NOW)["games"][0]
    assert game["winner_team_id"] == "pandascore:1" and game["length_seconds"] == 2620
    raw["games"][0]["winner"]["id"] = 999
    assert normalize(raw, "pandascore", "a"*64, NOW)["games"][0]["winner_team_id"] is None


def test_worker_publishes_exact_selected_capture_without_extra_requests(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "TEST ONLY")
    requests = []
    with client_for({"matches": [FOOTBALL]}) as client:
        original = client.get
        def request(*args, **kwargs):
            requests.append(kwargs["headers"])
            return original(*args, **kwargs)
        monkeypatch.setattr(client, "get", request)
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        status = worker.poll_once()
    pointer = json.loads((worker.snapshots / "CURRENT_MATCH_INSIGHTS.json").read_text())
    package = json.loads((worker.root / "match-insights" / pointer["basename"]).read_text())
    assert status["status"] == "ready" and len(requests) == 1
    assert requests[0]["X-Unfold-Lineups"] == "true"
    assert package["publication_fingerprint"] == pointer["publication_fingerprint"]
    assert package["records"][0]["match_id"] == "football-data:100"
    assert "TEST ONLY" not in json.dumps(package)
    assert build_selected(worker.root, now=NOW)["records"] == 1


def test_changed_archive_rejected_and_last_package_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "TEST ONLY")
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        worker.poll_once()
    pointer = worker.snapshots / "CURRENT_MATCH_INSIGHTS.json"
    old = pointer.read_bytes()
    archive = next((worker.root / "provider_raw").rglob("*.json"))
    archive.write_text('{}', encoding="utf-8")
    with pytest.raises(PublicationError):
        build_selected(worker.root, now=NOW)
    assert pointer.read_bytes() == old


def test_optional_details_failure_does_not_break_results(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "TEST ONLY")
    def fail(*args, **kwargs):
        raise PublicationError("TEST ONLY damaged optional archive")
    monkeypatch.setattr("arena_lakehouse.match_insights.publish", fail)
    with client_for({"matches": [FOOTBALL]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data"), client=client, clock=Clock())
        result = worker.poll_once()
    assert result["status"] == "ready" and worker.pipeline.events()[0]["score_a"] == 2
    assert not (worker.snapshots / "CURRENT_MATCH_INSIGHTS.json").exists()
