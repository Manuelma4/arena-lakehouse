"""Independent source examples and trust-boundary probes, without network access.

Examples were hand-reviewed in the pinned CC0 WorldCup JSON. Parser fixtures are
deliberately incomplete and must never pass as the immutable published source.
"""

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from importlib.resources import files
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from arena_lakehouse.openfootball import (
    AcquisitionError,
    OpenFootballCollector,
    STAGING_VERSION_V2,
    load_sources,
    parse_payload,
)
from arena_lakehouse.openfootball_research import ResearchValidationError, publish_research


NOW = datetime(2026, 10, 6, 13, tzinfo=timezone.utc)
REF = "516d3825c3bd23fdc298c4014e84bde78f2d4965"
SOURCE = load_sources(files("arena_lakehouse").joinpath("resources/openfootball_sources.json"))["worldcup-2026"]
ROLLOVER = {
    "round": "Matchday 1", "date": "2026-06-11", "time": "20:00 UTC-6",
    "team1": "South Korea", "team2": "Czech Republic",
    "score": {"ft": [2, 1], "ht": [0, 0]},
    "group": "Group A", "ground": "Guadalajara (Zapopan)",
}
PENALTIES = {
    "round": "Round of 32", "num": 74, "date": "2026-06-29", "time": "16:30 UTC-4",
    "team1": "Germany", "team2": "Paraguay",
    "score": {"p": [3, 4], "et": [1, 1], "ft": [1, 1], "ht": [0, 1]},
    "ground": "Boston (Foxborough)",
}
FINAL = {
    "round": "Final", "num": 104, "date": "2026-07-19", "time": "15:00 UTC-4",
    "team1": "Spain", "team2": "Argentina",
    "score": {"et": [1, 0], "ft": [0, 0], "ht": [0, 0]},
    "ground": "New York/New Jersey (East Rutherford)",
}


def fixture_bytes(*rows):
    return json.dumps({"name": "World Cup 2026", "matches": rows}, ensure_ascii=False).encode("utf-8")


def parse(*rows):
    return parse_payload(fixture_bytes(*rows), SOURCE, NOW, staging_version=STAGING_VERSION_V2)


def test_reviewed_source_is_national_and_pinned_to_the_reviewed_repository():
    assert (SOURCE.repository, SOURCE.team_type, SOURCE.region) == ("worldcup.json", "national", "International")
    assert SOURCE.team_namespace == "openfootball-national-men"
    assert SOURCE.url == f"https://raw.githubusercontent.com/openfootball/worldcup.json/{REF}/2026/worldcup.json"
    assert SOURCE.license_url == f"https://github.com/openfootball/worldcup.json/blob/{REF}/LICENSE.md"
    with pytest.raises(ValueError):
        replace(SOURCE, source_ref="master")


def test_reviewed_rollover_preserves_source_date_group_and_unavailable_completion():
    result = parse(ROLLOVER)
    assert result.quarantine == [] and result.input_rows == 1
    row = result.rows[0]
    assert (row["source_date"], row["local_time"]) == ("2026-06-11", "20:00")
    assert row["start_time"] == "2026-06-12T02:00:00.000000Z"
    assert row["utc_offset_minutes"] == -360 and row["kickoff_precision"] == "offset_minute"
    assert row["source_group"] == "Group A" and row["ground"] == "Guadalajara (Zapopan)"
    assert row["source_match_number"] is None  # Group records contain no upstream num.
    assert row["timezone"] is row["completed_at"] is None
    assert row["team_type"] == "national" and row["prediction_eligible"] is False


@pytest.mark.parametrize("raw,clock,score,details", [
    (PENALTIES, "2026-06-29T20:30:00.000000Z", (1, 1),
     {"p": [3, 4], "et": [1, 1], "ft": [1, 1], "ht": [0, 1]}),
    (FINAL, "2026-07-19T19:00:00.000000Z", (0, 0),
     {"et": [1, 0], "ft": [0, 0], "ht": [0, 0]}),
])
def test_reviewed_knockouts_do_not_replace_source_ft_with_extra_time_or_penalties(raw, clock, score, details):
    result = parse(raw)
    assert result.quarantine == []
    row = result.rows[0]
    assert row["start_time"] == clock
    assert (row["score_a"], row["score_b"]) == score
    assert json.loads(row["score_details_json"]) == details
    assert row["source_match_number"] == raw["num"] and row["source_group"] is None
    assert row["score_basis"] == "source_ft_unverified_regulation"
    assert row["prediction_eligible"] is False and row["completed_at"] is None


@pytest.mark.parametrize("clock,expected,offset", [
    ("00:10 UTC+5:45", "2026-07-18T18:25:00.000000Z", 345),
    ("23:50 UTC-3:30", "2026-07-20T03:20:00.000000Z", -210),
    ("12:00 UTC+0", "2026-07-19T12:00:00.000000Z", 0),
])
def test_explicit_offset_arithmetic_handles_both_calendar_boundaries(clock, expected, offset):
    result = parse(dict(FINAL, time=clock))
    assert result.quarantine == []
    assert result.rows[0]["source_date"] == "2026-07-19"
    assert result.rows[0]["start_time"] == expected
    assert result.rows[0]["utc_offset_minutes"] == offset


def test_numbered_identity_survives_corrected_round_opponents_and_date_but_conflicts_fail_symmetrically():
    original = parse(FINAL).rows[0]
    revised = dict(FINAL, round="Reviewed final", date="2026-07-20", team1="Spain corrected")
    corrected = parse(revised).rows[0]
    assert corrected["match_id"] == original["match_id"]
    assert corrected["team_a_id"] != original["team_a_id"]
    assert corrected["identity_basis"] == "competition-season-source-match-number"
    conflict = parse(FINAL, revised)
    assert conflict.rows == [] and len(conflict.quarantine) == 2
    assert {item["index"] for item in conflict.quarantine} == {0, 1}
    duplicate = parse(FINAL, FINAL)
    assert len(duplicate.rows) == duplicate.duplicates == 1 and duplicate.quarantine == []


@pytest.mark.parametrize("field,value", [
    ("num", True), ("num", 104.0), ("num", "104"),
    ("time", "25:00 UTC-4"), ("time", "15:00 UTC+14:01"),
    ("time", "15:00 America/New_York"), ("ground", "Boston\nFoxborough"),
])
def test_unsupported_numbers_clocks_and_ground_text_are_quarantined_without_guessing(field, value):
    result = parse(dict(FINAL, **{field: value}))
    assert result.rows == [] and len(result.quarantine) == 1
    assert result.quarantine[0]["index"] == 0


def test_persisted_pinned_acquisition_rejects_partial_parser_fixtures_before_writing(tmp_path):
    workspace = tmp_path / "never-created"
    collector = OpenFootballCollector(workspace, staging_version=STAGING_VERSION_V2)
    with pytest.raises((AcquisitionError, ValueError), match="checksum|SHA|immutable|pinned"):
        collector.ingest_bytes(SOURCE, fixture_bytes(FINAL), observed_at=NOW)
    assert not workspace.exists()


def test_reviewed_full_source_has_104_matches_48_nations_and_replays_without_new_revisions(tmp_path):
    body = (Path(__file__).parent / "fixtures/worldcup-2026-pinned.json").read_bytes()
    reviewed_hash = "0ae2c18109b5aa86bc11928b43586ca430c234804d5bbf808d2c3bf2051ecfca"
    assert sha256(body).hexdigest() == reviewed_hash
    raw = json.loads(body)
    assert raw["name"] == "World Cup 2026" and len(raw["matches"]) == 104
    assert len({row[side] for row in raw["matches"] for side in ("team1", "team2")}) == 48
    assert sorted(row["num"] for row in raw["matches"] if "num" in row) == list(range(73, 105))
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    first = collector.ingest_bytes(SOURCE, body, observed_at=NOW)
    replay = collector.ingest_bytes(SOURCE, body, observed_at=NOW + timedelta(minutes=1))
    assert (first.accepted, first.changed, first.quarantined) == (104, 104, 0)
    assert (replay.accepted, replay.changed, replay.unchanged) == (104, 0, 104)
    assert (collector.workspace / first.archive_path).read_bytes() == body
    output = tmp_path / "export"
    manifest = collector.export(output)
    assert (manifest["matches"], manifest["revisions"], manifest["sources"]) == (104, 104, 1)
    assert manifest["staging_version"] == "openfootball-staging-2.0"
    for filename, checksum in manifest["files"].items():
        assert sha256((output / filename).read_bytes()).hexdigest() == checksum
    rows = json.loads((output / "matches.json").read_text(encoding="utf-8"))
    assert len({row[side] for row in rows for side in ("team_a_id", "team_b_id")}) == 48
    assert all(row["source_sha256"] == reviewed_hash for row in rows)
    assert all(row["team_type"] == "national" and row["prediction_eligible"] is False for row in rows)
    assert all(row["kickoff_precision"] == "offset_minute" and row["completed_at"] is None for row in rows)
    assert sum(row["source_match_number"] is None for row in rows) == 72
    assert all(row["revision"] == 1 for row in rows)


@pytest.mark.parametrize("mutation", ["ground", "consistent_ft", "source_group", "opponent"])
def test_publisher_rejects_coherent_resealed_fields_that_do_not_exist_in_the_pinned_raw_source(tmp_path, mutation):
    body = (Path(__file__).parent / "fixtures/worldcup-2026-pinned.json").read_bytes()
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    collector.ingest_bytes(SOURCE, body, observed_at=NOW)
    stage = tmp_path / "stage"
    collector.export(stage)
    for name in ("matches", "history"):
        path = stage / f"{name}.json"
        rows = json.loads(path.read_text(encoding="utf-8"))
        row = next(row for row in rows if row["source_match_number"] == 104)
        if mutation == "ground":
            row["ground"] = "Mexico City"
        elif mutation == "source_group":
            row["source_group"] = "Group Z"
        elif mutation == "consistent_ft":
            row["score_a"] = 2
            details = json.loads(row["score_details_json"])
            details["ft"] = [2, 0]
            row["score_details_json"] = json.dumps(details, sort_keys=True, separators=(",", ":"))
        else:
            france = next(candidate for candidate in rows if candidate["team_b"] == "France")
            row["team_b"], row["team_b_id"] = france["team_b"], france["team_b_id"]
        excluded = {"revision", "event_id", "observed_at", "source_sha256", "source_url", "license", "license_url"}
        business = {key: value for key, value in row.items() if key not in excluded}
        event_hash = sha256(json.dumps(business, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        row["event_id"] = f"{row['match_id']}:{row['revision']}:{event_hash[:16]}"
        path.write_text(json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
        schema = pq.read_schema(stage / f"{name}.parquet")
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), stage / f"{name}.parquet")
    manifest_path = stage / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = {filename: sha256((stage / filename).read_bytes()).hexdigest() for filename in manifest["files"]}
    manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    output = tmp_path / "not-published"
    with pytest.raises(ResearchValidationError, match="pinned|trusted|reviewed|World Cup"):
        publish_research(stage, output, generated_at=NOW + timedelta(minutes=1))
    assert not output.exists()
