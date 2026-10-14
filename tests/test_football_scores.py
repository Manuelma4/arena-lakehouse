"""Source-shaped score evidence, independent of provider requests or runtime data."""

import copy
from datetime import datetime, timezone

import pytest

from arena_lakehouse.football_scores import FootballScoreError, score_details
from arena_lakehouse.forecast_context import LEGACY_SCHEMA, normalize


def pair(home, away):
    return dict(home=home, away=away)


def raw(duration="REGULAR", **stages):
    return dict(status="FINISHED", score=dict(duration=duration, **stages))


def test_regular_final_and_explicit_regulation_priority():
    result = score_details(raw(fullTime=pair(2, 1)))
    assert result["regulation"] == pair(2, 1)
    assert result["regulation_basis"] == "regular_full_time"
    assert result["native"] == {"fullTime": pair(2, 1)}
    assert result["after_extra_time"] is None
    result = score_details(raw(fullTime=pair(2, 1), regularTime=pair(2, 1)))
    assert result["regulation_basis"] == "explicit_regular_time"


@pytest.mark.parametrize("duration,full,regular,extra,penalties,after", [
    ("EXTRA_TIME", pair(2, 1), pair(1, 1), pair(1, 0), None, pair(2, 1)),
    ("PENALTY_SHOOTOUT", pair(6, 4), pair(1, 1), pair(0, 0), pair(5, 3), pair(1, 1)),
    ("PENALTY_SHOOTOUT", pair(7, 6), pair(1, 1), pair(1, 1), pair(5, 4), pair(2, 2)),
])
def test_stage_increments_and_shootouts_never_enter_regulation(duration, full, regular, extra, penalties, after):
    value = raw(duration, fullTime=full, regularTime=regular, extraTime=extra)
    if penalties is not None:
        value["score"]["penalties"] = penalties
    before = copy.deepcopy(value)
    result = score_details(value)
    assert value == before
    assert result["regulation"] == regular
    assert result["after_extra_time"] == after
    assert result["regulation_basis"] == "explicit_regular_time"
    assert result["unavailable_reason"] is None
    assert result["native"] == {k: v for k, v in value["score"].items() if k != "duration"}


def test_absence_null_and_partial_stages_preserved_without_zero_or_subtraction():
    result = score_details(raw("EXTRA_TIME", fullTime=pair(2, 1), halfTime=None,
                               regularTime=pair(None, 1), extraTime=pair(1, 0)))
    assert result["native"]["halfTime"] is None
    assert result["native"]["regularTime"] == pair(None, 1)
    assert "penalties" not in result["native"]
    assert result["after_extra_time"] is None
    assert result["regulation"] is None
    assert result["unavailable_reason"] == "missing_regulation"
    absent = score_details(dict(status="FINISHED", score={"fullTime": pair(1, 0)}))
    explicit_null = score_details(raw(None, fullTime=pair(1, 0)))
    assert absent["duration"] is None and absent["duration_present"] is False
    assert explicit_null["duration"] is None and explicit_null["duration_present"] is True


@pytest.mark.parametrize("status,reason", [("AWARDED", "awarded"), ("SCHEDULED", "not_finished"),
    ("TIMED", "not_finished"), ("IN_PLAY", "not_finished"), ("PAUSED", "not_finished"),
    ("POSTPONED", "not_finished"), ("SUSPENDED", "not_finished"), ("CANCELLED", "not_finished")])
def test_non_played_results_never_regulation_targets(status, reason):
    value = raw("unknown", fullTime=pair(4, 3), regularTime=pair(1, 1))
    value["status"] = status
    result = score_details(value)
    assert result["unavailable_reason"] == reason
    assert result["regulation"] is None


@pytest.mark.parametrize("duration", [None, "OTHER", "regular"])
def test_unverified_duration_never_uses_even_explicit_regular_time(duration):
    result = score_details(raw(duration, fullTime=pair(1, 1), regularTime=pair(1, 1)))
    assert result["unavailable_reason"] == "unknown_duration"
    assert result["regulation"] is None


@pytest.mark.parametrize("value", [
    raw(fullTime=pair(2, 1), regularTime=pair(1, None)),
    raw(fullTime=pair(2, 1), extraTime=pair(1, None)),
    raw(fullTime=pair(2, 1), penalties=pair(None, 1)),
    raw("EXTRA_TIME", fullTime=pair(3, 1), regularTime=pair(1, 1), extraTime=pair(1, 0)),
    raw("EXTRA_TIME", fullTime=pair(2, 1), regularTime=pair(1, 1), extraTime=pair(1, 0), penalties=pair(1, 0)),
    raw("PENALTY_SHOOTOUT", fullTime=pair(5, 3), regularTime=pair(1, 1), extraTime=pair(0, 0), penalties=pair(5, 3)),
])
def test_bad_stage_arithmetic_is_ineligible(value):
    result = score_details(value)
    assert result["unavailable_reason"] == "inconsistent_stages"
    assert result["regulation"] is None


@pytest.mark.parametrize("score", [
    dict(fullTime=pair(True, 1)), dict(fullTime=pair(-1, 1)), dict(fullTime=pair(51, 1)),
    dict(fullTime=pair("1", 1)), dict(fullTime=pair(1.0, 1)), dict(fullTime={"home": 1}),
    dict(fullTime={"home": 1, "away": 1, "extra": 0}), dict(fullTime=[]),
    dict(duration=[]), dict(duration=""), dict(winner="UNKNOWN"), dict(unexpected=None),
])
def test_invalid_native_keys_types_and_bounds_reject_record(score):
    with pytest.raises(FootballScoreError):
        score_details(dict(status="FINISHED", score=score))


def test_v2_retains_legacy_anchors_and_v1_row_bytes():
    from test_live import FOOTBALL
    source = copy.deepcopy(FOOTBALL)
    source["score"] = dict(duration="PENALTY_SHOOTOUT", fullTime=pair(6, 4), regularTime=pair(1, 1),
                           extraTime=pair(0, 0), penalties=pair(5, 3))
    observed = datetime(2026, 10, 14, tzinfo=timezone.utc)
    legacy = normalize(source, "a" * 64, observed, schema_version=LEGACY_SCHEMA)
    current = normalize(source, "a" * 64, observed)
    assert {k: v for k, v in current.items() if k != "score_details"} == legacy
    assert (current["score_home"], current["score_away"]) == (6, 4)
    assert current["score_details"]["regulation"] == pair(1, 1)
    assert "score_details" not in legacy


def test_complete_explicit_regulation_does_not_require_guessing_missing_extra():
    result = score_details(raw("EXTRA_TIME", fullTime=pair(2, 1), regularTime=pair(1, 1)))
    assert result["regulation"] == pair(1, 1)
    assert result["after_extra_time"] is None
    assert "extraTime" not in result["native"]


@pytest.mark.parametrize("value", [
    raw(fullTime=pair(2, 1), regularTime=pair(2, None)),
    raw("EXTRA_TIME", fullTime=pair(2, 1), regularTime=pair(1, 1), extraTime=pair(1, None)),
    raw("PENALTY_SHOOTOUT", fullTime=pair(6, 4), regularTime=pair(1, 1), extraTime=pair(None, 0), penalties=pair(5, 3)),
    raw("PENALTY_SHOOTOUT", fullTime=pair(6, 4), regularTime=pair(1, 1), extraTime=pair(0, 0), penalties=pair(5, None)),
])
def test_partial_played_stages_never_filled_from_full_time(value):
    result = score_details(value)
    assert result["regulation"] is None
    assert result["unavailable_reason"] == "missing_regulation"


@pytest.mark.parametrize("duration,full,regular", [
    ("REGULAR", pair(1, 0), None),
    ("EXTRA_TIME", pair(2, 1), pair(1, 1)),
    ("PENALTY_SHOOTOUT", pair(6, 4), pair(1, 1)),
])
def test_half_time_cannot_exceed_regulation(duration, full, regular):
    value = raw(duration, fullTime=full, halfTime=pair(2, 0))
    if regular is not None:
        value["score"]["regularTime"] = regular
    result = score_details(value)
    assert result["regulation"] is None
    assert result["unavailable_reason"] == "inconsistent_stages"


def test_all_null_optional_stages_remain_unknown_and_aggregate_matches_need_not_draw():
    result = score_details(raw(fullTime=pair(2, 1), regularTime=pair(None, None)))
    assert result["regulation"] == pair(2, 1)
    result = score_details(raw("EXTRA_TIME", fullTime=pair(2, 0), regularTime=pair(1, 0), extraTime=pair(1, 0)))
    assert result["regulation"] == pair(1, 0)


@pytest.mark.parametrize("duration", ["EXTRA_TIME", "PENALTY_SHOOTOUT"])
def test_native_total_cannot_be_less_than_regulation_when_later_stages_missing(duration):
    value = raw(duration, fullTime=pair(0, 1), regularTime=pair(1, 1))
    result = score_details(value)
    assert result["regulation"] is None
    assert result["unavailable_reason"] == "inconsistent_stages"
