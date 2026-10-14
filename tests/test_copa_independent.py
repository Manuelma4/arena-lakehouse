"""Independent Copa America source examples and acquisition/publication trust probes."""

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arena_lakehouse.copa_2024 import decode_copa_2024
from arena_lakehouse.openfootball import (
    AcquisitionError, OpenFootballCollector, STAGING_VERSION_V2, load_sources, parse_payload,
)
from arena_lakehouse.openfootball_research import ResearchValidationError, publish_research

NOW = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
REF = "902e4c51c282244ab924676b3bc471b4040de39a"
SHA = "fe63e5ef5b0390cb15654c5020866d0747c751b9b98ffb477debcce0a3575cdc"
SOURCE = load_sources(files("arena_lakehouse").joinpath("resources/openfootball_sources.json"))["copa-america-2024"]
BODY = (Path(__file__).parent / "fixtures/copa-2024-pinned.txt").read_bytes()


def test_source_contract_and_original_txt_bytes_are_pinned():
    assert SOURCE.name == "Copa Am\u00e9rica 2024"
    assert hashlib.sha256(BODY).hexdigest() == SHA
    assert files("arena_lakehouse").joinpath("resources/copa_2024.txt").read_bytes() == BODY
    assert SOURCE.url == f"https://raw.githubusercontent.com/openfootball/copa-america/{REF}/2024--usa/copa.txt"
    assert SOURCE.license_url == f"https://github.com/openfootball/copa-america/blob/{REF}/LICENSE.md"
    assert (SOURCE.competition_id, SOURCE.team_namespace, SOURCE.team_type) == (
        "copa-america", "openfootball-national-men", "national")


@pytest.mark.parametrize("changes", [
    {"source_ref": "master"}, {"source_ref": "0" * 40}, {"path": "2024--usa/other.txt"},
    {"path": "2024/copa.txt"}, {"season": "2028"}, {"team_type": "club"},
])
def test_only_reviewed_txt_path_and_edition_are_allowed(changes):
    with pytest.raises(ValueError):
        replace(SOURCE, **changes)


def test_full_source_retains_sections_offsets_unknown_completion_and_calendar_typo():
    parsed = parse_payload(BODY, SOURCE, NOW, staging_version=STAGING_VERSION_V2)
    assert parsed.input_rows == 32 and len(parsed.rows) == 32 and parsed.quarantine == []
    assert len({row[side] for row in parsed.rows for side in ("team_a_id", "team_b_id")}) == 16
    assert sum(row["source_group"] is not None for row in parsed.rows) == 24
    assert all(row["round"] == row["source_group"] for row in parsed.rows if row["source_group"])
    assert all(row["kickoff_precision"] == "offset_minute" for row in parsed.rows)
    assert all(row["source_match_number"] is row["completed_at"] is row["timezone"] is None for row in parsed.rows)
    assert all(row["prediction_eligible"] is False and row["source_sha256"] == SHA for row in parsed.rows)
    assert {row["utc_offset_minutes"] for row in parsed.rows} == {-240, -300, -420}
    opening = next(row for row in parsed.rows if row["round"] == "Group A" and row["team_a"] == "Argentina"
                   and row["team_b"] == "Canada")
    assert (opening["source_date"], opening["local_time"], opening["start_time"]) == (
        "2024-06-20", "20:00", "2024-06-21T00:00:00.000000Z")
    final = next(row for row in parsed.rows if row["round"] == "Final")
    assert (final["source_date"], final["start_time"]) == ("2024-07-14", "2024-07-15T00:00:00.000000Z")
    assert (final["team_a"], final["team_b"], final["score_a"], final["score_b"]) == ("Argentina", "Colombia", 0, 0)
    assert json.loads(final["score_details_json"]) == {"ft": [0, 0], "et": [1, 0]}
    assert b"Sat Jul 14" in BODY  # Retained source typo; never repair raw evidence.


@pytest.mark.parametrize("a,b,round_name,ft,p", [
    ("Argentina", "Ecuador", "Quarter-finals", [1, 1], [4, 2]),
    ("Venezuela", "Canada", "Quarter-finals", [1, 1], [3, 4]),
    ("Uruguay", "Brazil", "Quarter-finals", [0, 0], [4, 2]),
    ("Canada", "Uruguay", "Third place play-off", [2, 2], [3, 4]),
])
def test_shootouts_never_replace_regulation_or_invent_extra_time(a, b, round_name, ft, p):
    parsed = parse_payload(BODY, SOURCE, NOW, staging_version=STAGING_VERSION_V2)
    row = next(row for row in parsed.rows if (row["team_a"], row["team_b"], row["round"]) == (a, b, round_name))
    assert [row["score_a"], row["score_b"]] == ft
    assert json.loads(row["score_details_json"]) == {"ft": ft, "p": p}
    assert row["score_basis"] == "source_ft_unverified_regulation"


@pytest.mark.parametrize("old,new", [
    (b"UTC-4", b"America/New_York"), (b"UTC-4", b"UTC-25"),
    (b"1-0 a.e.t. (0-0)", b"1-0 a.e.t."), (b"4-2 pen. (1-1)", b"4-2 pen. (2)"),
    (b"Thu Jun 20", b"Fri Jun 20"), (b"Argentina      2-0   Canada", b"Argentina      2-0   Unknown"),
    (b"Argentina      2-0   Canada", b"Argentina      99-0   Canada"),
])
def test_unsupported_text_fails_closed_without_partial_match_results(old, new):
    changed = BODY.replace(old, new, 1)
    assert changed != BODY
    with pytest.raises(ValueError):
        decode_copa_2024(changed)
    parsed = parse_payload(changed, SOURCE, NOW, staging_version=STAGING_VERSION_V2)
    assert parsed.rows == [] and len(parsed.quarantine) == 1 and parsed.input_rows == 0


def test_unrecognized_or_missing_match_lines_do_not_silently_reduce_coverage():
    for bad in (BODY + b"unrecognized trailer\n", BODY[:BODY.index(b"Sat Jul 14")]):
        with pytest.raises(ValueError):
            decode_copa_2024(bad)


@pytest.mark.parametrize("bad", [BODY + b"\n", BODY.replace(b"Argentina", b"Changed", 1), b"{}"])
def test_changed_bytes_cannot_be_acquired_or_create_state(tmp_path, bad):
    collector = OpenFootballCollector(tmp_path / "unused", staging_version=STAGING_VERSION_V2)
    with pytest.raises(AcquisitionError, match="checksum"):
        collector.ingest_bytes(SOURCE, bad, observed_at=NOW)
    assert not collector.workspace.exists()


def test_v1_rejects_txt_before_state_creation(tmp_path):
    collector = OpenFootballCollector(tmp_path / "unused")
    with pytest.raises(ValueError, match="v2"):
        collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    assert not collector.workspace.exists()


def test_exact_txt_archive_replay_and_publication_are_consistent(tmp_path):
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    first = collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    replay = collector.ingest_bytes(SOURCE, BODY, observed_at=NOW + timedelta(seconds=1))
    assert (first.accepted, first.changed, first.quarantined) == (32, 32, 0)
    assert (replay.accepted, replay.changed, replay.unchanged) == (32, 0, 32)
    assert first.archive_path == f"raw/copa-america-2024/{SHA}.txt"
    assert (collector.workspace / first.archive_path).read_bytes() == BODY
    stage, output = tmp_path / "stage", tmp_path / "research"
    collector.export(stage)
    manifest = publish_research(stage, output, generated_at=NOW + timedelta(minutes=1))
    assert manifest["matches"] == manifest["revisions"] == 32 and manifest["sources"] == 1
    for filename, digest in manifest["files"].items():
        assert hashlib.sha256((output / filename).read_bytes()).hexdigest() == digest
    with sqlite3.connect(collector.state_path) as connection:
        connection.execute("UPDATE cache SET archive_path=replace(archive_path,'.txt','.json')")
    with pytest.raises(AcquisitionError, match="archive metadata"):
        collector._read_cache(SOURCE)


def _reseal(stage):
    path = stage / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["files"] = {name: hashlib.sha256((stage / name).read_bytes()).hexdigest() for name in manifest["files"]}
    path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")


@pytest.mark.parametrize("mutation", ["extra_time_as_ft", "ground", "offset", "weekday_date"])
def test_resealed_coherent_mutations_do_not_pass_source_binding(tmp_path, mutation):
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    stage = tmp_path / "stage"
    collector.export(stage)
    for name in ("matches", "history"):
        path = stage / f"{name}.json"
        rows = json.loads(path.read_bytes())
        row = next(row for row in rows if row["round"] == "Final")
        if mutation == "extra_time_as_ft":
            row["score_a"] = 1
            row["score_details_json"] = '{"et":[1,0],"ft":[1,0]}'
        elif mutation == "ground":
            row["ground"] = "Unknown stadium"
        elif mutation == "offset":
            row["utc_offset_minutes"] = -300
            row["start_time"] = "2024-07-15T01:00:00.000000Z"
        else:
            row["source_date"] = "2024-07-13"
            row["start_time"] = "2024-07-14T00:00:00.000000Z"
        excluded = {"observed_at", "source_sha256", "source_url", "license", "license_url", "revision", "event_id"}
        business = {key: val for key, val in row.items() if key not in excluded}
        digest = hashlib.sha256(json.dumps(business, ensure_ascii=False, sort_keys=True,
                                           separators=(",", ":")).encode()).hexdigest()
        row["event_id"] = f"{row['match_id']}:{row['revision']}:{digest[:16]}"
        path.write_text(json.dumps(rows, ensure_ascii=False) + "\n", encoding="utf-8")
        pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(stage / f"{name}.parquet")),
                       stage / f"{name}.parquet")
    _reseal(stage)
    with pytest.raises(ResearchValidationError, match="Pinned national business fields"):
        publish_research(stage, tmp_path / "not-created", generated_at=NOW + timedelta(minutes=1))
    assert not (tmp_path / "not-created").exists()
