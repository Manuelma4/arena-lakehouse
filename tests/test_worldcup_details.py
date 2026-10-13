"""Original TEST ONLY observations; no downloads, account access or inference."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import httpx
import pytest

from arena_lakehouse import worldcup_details as details


def match():
    return dict(round="TEST ONLY Group", date="2026-06-11", time="13:00 UTC-6",
        team1="TEST ONLY A", team2="TEST ONLY B", score=dict(ft=[2, 0], ht=[1, 0]),
        ground="TEST ONLY ground", attendance=100, referees=[dict(name="TEST ONLY referee", country="AAA")],
        lineup=[dict(starter=[dict(name=f"TEST ONLY {side} player {n}") for n in range(11)],
            bench=[dict(name=f"TEST ONLY {side} substitute")],
            subs=[dict(on=f"TEST ONLY {side} substitute", off=f"TEST ONLY {side} player 1", minute="90+2")])
            for side in ("A", "B")])


def normalize(raw=None, basic=None):
    raw = raw or match()
    base = {key: deepcopy(raw[key]) for key in ("round", "date", "time", "team1", "team2", "score")}
    base["num"] = 1
    return details.normalize(dict(name="World Cup 2026", matches=[raw]), dict(matches=[basic or base]))


def test_preserves_unknown_families_stoppage_strings_and_source_names_without_ids():
    rows, quality = normalize()
    row = rows[0]
    assert row["mapping_status"] == "verified" and row["mapping_reasons"] == []
    assert row["source_match"]["lineup"][0]["subs"][0]["minute"] == "90+2"
    assert "goals1" not in row["source_match"] and "bookings" not in row["source_match"]
    assert row["bench_complete"] is False and row["player_identity"] == "source_names_only"
    assert quality["complete_benches"] == quality["provider_player_ids"] == 0


@pytest.mark.parametrize("field, value, reason", [
    ("time", "14:00 UTC-6", "kickoff_mismatch"),
    ("score", dict(ft=[2, 0], ht=[0, 0]), "score_ht_mismatch"),
    ("score", dict(ft=[3, 0], ht=[1, 0]), "score_ft_mismatch"),
])
def test_conflicts_withhold_mapping_without_discarding_source(field, value, reason):
    raw = match()
    base = deepcopy(raw)
    base["num"] = 7
    raw[field] = value
    rows, quality = normalize(raw, base)
    assert rows[0]["mapping_status"] == "conflict" and reason in rows[0]["mapping_reasons"]
    assert rows[0]["source_match"][field] == value and rows[0]["basic_match_id"]
    assert quality["conflicts"] == 1


def test_reviewed_aliases_and_basic_round_fallback_do_not_depend_on_full_round():
    raw = match()
    raw["team1"] = "Bosnia and Herzegovina"
    base = deepcopy(raw)
    base.update(team1="Bosnia & Herzegovina", round="TEST ONLY native basic round")
    rows, _ = normalize(raw, base)
    team_ids = ["openfootball:team:" + details.digest(["openfootball-national-men", base[key]])[:24]
                for key in ("team1", "team2")]
    expected = "openfootball:match:" + details.digest(["worldcup", "2026", base["round"], *team_ids, 1])[:24]
    assert rows[0]["basic_match_id"] == expected and rows[0]["round_label_differs"]
    assert rows[0]["basic_match_number"] is None
    assert rows[0]["aliases_applied"] == [dict(source="Bosnia and Herzegovina", basic="Bosnia & Herzegovina")]


def test_own_goals_penalties_and_empty_known_families_remain_distinct_from_missing():
    raw = match()
    raw.update(goals1=[dict(name="TEST ONLY opponent", minute="90+4", owngoal=True)], goals2=[], bookings=[[], []],
               penalties=[dict(name="TEST ONLY penalty taker", note="missed")])
    row = normalize(raw)[0][0]
    assert row["source_match"]["goals1"][0]["owngoal"] is True
    assert row["source_match"]["goals2"] == [] and row["source_match"]["bookings"] == [[], []]
    assert row["source_match"]["penalties"][0]["note"] == "missed"


@pytest.mark.parametrize("kind", ["player_id", "short_xi", "bad_minute", "bool_score", "one_goal_family"])
def test_malformed_or_invented_fields_fail_closed(kind):
    raw = match()
    if kind == "player_id":
        raw["lineup"][0]["starter"][0]["id"] = "invented"
    elif kind == "short_xi":
        raw["lineup"][1]["starter"].pop()
    elif kind == "bad_minute":
        raw["lineup"][0]["subs"][0]["minute"] = "not supplied"
    elif kind == "bool_score":
        raw["score"]["ft"] = [True, 0]
    else:
        raw["goals1"] = []
    with pytest.raises(details.WorldCupDetailsError):
        normalize(raw)


def test_duplicate_detail_and_basic_identities_are_not_arbitrarily_joined():
    raw = match()
    with pytest.raises(details.WorldCupDetailsError, match="ambiguous_detail_identity"):
        details.normalize(dict(name="World Cup 2026", matches=[raw, deepcopy(raw)]), dict(matches=[]))
    base = deepcopy(raw)
    base["num"] = 1
    with pytest.raises(details.WorldCupDetailsError, match="ambiguous_basic_identity"):
        details.normalize(dict(name="World Cup 2026", matches=[]), dict(matches=[base, deepcopy(base)]))


def test_wrong_source_pin_never_creates_destination(tmp_path):
    raw, license_path = tmp_path / "raw.json", tmp_path / "license.md"
    raw.write_text("{}")
    license_path.write_text("TEST ONLY")
    output = tmp_path / "not-created"
    with pytest.raises(details.WorldCupDetailsError, match="source_checksum_mismatch"):
        details.publish(raw, license_path, output, observed_at="2026-10-13T00:00:00Z")
    assert not output.exists()


def test_two_request_collector_pins_originals_and_records_actual_completion(monkeypatch, tmp_path):
    body = json.dumps(dict(name="World Cup 2026", matches=[match()])).encode()
    license_body = b"TEST ONLY CC0 transport fixture"
    monkeypatch.setattr(details, "SOURCE_SHA", details.digest(body))
    monkeypatch.setattr(details, "LICENSE_SHA", details.digest(license_body))
    calls = []

    def response(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=license_body if str(request.url) == details.LICENSE_URL else body)

    now = datetime.now(timezone.utc)
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        entries = details.collect(tmp_path / "capture", client=client, clock=lambda: now)
    assert calls == [details.LICENSE_URL, details.SOURCE_URL]
    assert len(entries) == 2 and all(entry["requested_at"] == entry["observed_at"] == now.isoformat() for entry in entries)
    assert (tmp_path / "capture/worldcup-full.json").read_bytes() == body
    assert json.loads((tmp_path / "capture/acquisition.json").read_text()) == entries


def test_redirect_or_error_is_not_bypassed_and_never_publishes_partial_capture(tmp_path):
    calls = []

    def response(request):
        calls.append(str(request.url))
        return httpx.Response(302, headers={"Location": "https://example.invalid/"})

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        with pytest.raises(details.WorldCupDetailsError, match="source_request_failed"):
            details.collect(tmp_path / "absent", client=client)
    assert calls == [details.LICENSE_URL] and not (tmp_path / "absent").exists()
