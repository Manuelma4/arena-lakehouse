"""Original TEST ONLY observations; no provider or model requests."""

import copy
import json

import pytest

from arena_lakehouse.match_insights import AmbiguousMatchDetails, normalize, publish
from arena_lakehouse.provider_publication import atomic_json
from test_live import FOOTBALL, NOW, VALORANT


def football():
    raw = copy.deepcopy(FOOTBALL)
    raw["homeTeam"]["lineup"] = [dict(id=11, name="TEST ONLY player", shirtNumber=0)]
    return raw


def valorant():
    raw = copy.deepcopy(VALORANT)
    raw["games"] = [dict(id=71, match_id=raw["id"], position=1, status="finished",
                        finished=True, length=0, winner=dict(id=1, type="Team"))]
    return raw


def test_exact_player_duplicates_collapse_without_losing_real_zero():
    raw = football()
    raw["homeTeam"]["lineup"] *= 2
    row = normalize(raw, "football-data", "a" * 64, NOW)
    assert len(row["teams"][0]["lineup"]) == 1
    assert row["teams"][0]["lineup"][0]["shirt_number"] == 0


@pytest.mark.parametrize("changed", ["name", "shirtNumber", "position"])
def test_conflicting_player_duplicates_never_select_first_seen_facts(changed):
    raw = football()
    other = {**raw["homeTeam"]["lineup"][0], changed: 99 if changed == "shirtNumber" else "TEST ONLY conflict"}
    raw["homeTeam"]["lineup"].append(other)
    with pytest.raises(AmbiguousMatchDetails):
        normalize(raw, "football-data", "a" * 64, NOW)


@pytest.mark.parametrize("location", ["other_team", "bench"])
def test_match_participant_has_one_team_and_role(location):
    raw = football()
    participant = copy.deepcopy(raw["homeTeam"]["lineup"])
    if location == "other_team":
        raw["awayTeam"]["lineup"] = participant
    else:
        raw["homeTeam"]["bench"] = participant
    with pytest.raises(AmbiguousMatchDetails):
        normalize(raw, "football-data", "a" * 64, NOW)


def test_player_opponent_does_not_become_a_team_or_shift_slots():
    raw = valorant()
    raw["opponents"][0]["type"] = "Player"
    row = normalize(raw, "pandascore", "a" * 64, NOW)
    assert row["teams"][0]["team_id"] is None
    assert row["teams"][1]["team_id"] == "pandascore:2"
    assert row["games"][0]["winner_team_id"] is None


@pytest.mark.parametrize("duplicate", ["id", "position"])
def test_duplicate_maps_do_not_publish_ambiguous_counts(duplicate):
    raw = valorant()
    other = {**raw["games"][0], "id": 71 if duplicate == "id" else 72,
             "position": 2 if duplicate == "id" else 1}
    raw["games"].append(other)
    with pytest.raises(AmbiguousMatchDetails):
        normalize(raw, "pandascore", "a" * 64, NOW)


def publish_fixture(tmp_path, rows, provider):
    root = tmp_path / provider
    football_source = provider == "football-data"
    phases = ("PL",) if football_source else ("past", "upcoming", "running")
    paths = []
    for index, phase in enumerate(phases):
        path = root / f"provider_raw/{provider}/2026-10-06/{index:032x}.json"
        endpoint = f"competitions/{phase}/matches" if football_source else f"valorant/matches/{phase}?page=1&per_page=100"
        payload = {"matches": rows} if football_source else rows if index == 0 else []
        atomic_json(path, dict(provider=provider, endpoint=endpoint, requested_at=NOW.isoformat(),
                               observed_at=NOW.isoformat(), payload=payload))
        paths.append(path)
    scope = dict(kind="competition_list" if football_source else "match_phases", competition_codes=["PL"] if football_source else [],
                 phases=[] if football_source else list(phases), pages=1)
    selection = dict(provider=provider, fingerprint="a" * 64, collected_at=NOW.isoformat(), source_scope=scope)
    result = publish(root, selection, paths, started_at=NOW, generated_at=NOW)
    pointer = json.loads((root / "snapshots/CURRENT_MATCH_INSIGHTS.json").read_text())
    package = json.loads((root / "match-insights" / pointer["basename"]).read_text())
    return result, package


def test_one_bad_optional_match_leaves_valid_details_publishable(tmp_path):
    bad, valid = football(), copy.deepcopy(FOOTBALL)
    valid["id"] = 101
    bad["homeTeam"]["lineup"].append(dict(id=11, name="TEST ONLY contradictory player"))
    result, package = publish_fixture(tmp_path, [bad, valid], "football-data")
    assert result["records"] == 1 and result["conflicting_matches_omitted"] == 1
    assert [row["match_id"] for row in package["records"]] == ["football-data:101"]
    assert result["provider_requests"] == 0


def test_reused_game_id_omits_all_conflicting_matches(tmp_path):
    first, second = valorant(), valorant()
    second["id"] = 201
    second["games"][0]["match_id"] = 201
    result, package = publish_fixture(tmp_path, [first, second], "pandascore")
    assert result["records"] == 0 and result["conflicting_matches_omitted"] == 2
    assert package["records"] == []
