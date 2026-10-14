"""Pinned Euro 2024 acquisition and publication boundaries; entirely offline."""

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

from arena_lakehouse.openfootball import (
    AcquisitionError, OpenFootballCollector, STAGING_VERSION_V2, load_sources, parse_payload,
)
from arena_lakehouse.openfootball_research import ResearchValidationError, publish_research

NOW = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
REF = "7bbf6309bdda6896d6bb9ca218c1e15eaf418ce3"
SHA = "fdb4b858cc88603e69fe3b1ce34923fc6b69a774d4c8416f2d166ea57c556609"
ROOT = Path(__file__).parents[1]
SOURCES = load_sources(ROOT / "config/openfootball_sources.json")
SOURCE = SOURCES["euro-2024"]
BODY = (Path(__file__).parent / "fixtures/euro-2024-pinned.json").read_bytes()


def test_reviewed_pin_and_complete_bytes_are_mirrored_without_line_ending_changes():
    assert hashlib.sha256(BODY).hexdigest() == SHA
    assert files("arena_lakehouse").joinpath("resources/euro_2024.json").read_bytes() == BODY
    assert (ROOT / "config/openfootball_sources.json").read_bytes() == files("arena_lakehouse").joinpath(
        "resources/openfootball_sources.json").read_bytes()
    assert SOURCE.url == f"https://raw.githubusercontent.com/openfootball/euro.json/{REF}/2024/euro.json"
    assert SOURCE.license_url == f"https://github.com/openfootball/euro.json/blob/{REF}/LICENSE.md"
    assert (SOURCE.competition_id, SOURCE.team_namespace, SOURCE.team_type) == (
        "euro", "openfootball-national-men", "national")
    raw = json.loads(BODY)
    assert raw["name"] == "Euro 2024" and len(raw["matches"]) == 51
    assert len({row[side] for row in raw["matches"] for side in ("team1", "team2")}) == 24


@pytest.mark.parametrize("changes", [
    {"source_ref": "master"}, {"source_ref": "0" * 40}, {"path": "2024/other.json"},
    {"season": "2028", "path": "2028/euro.json"}, {"team_type": "club"},
])
def test_unreviewed_national_source_editions_are_rejected(changes):
    with pytest.raises(ValueError, match="reviewed pinned"):
        replace(SOURCE, **changes)


def test_v1_is_rejected_before_any_write(tmp_path):
    collector = OpenFootballCollector(tmp_path / "unused")
    with pytest.raises(ValueError, match="v2"):
        collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    assert not collector.workspace.exists()
    with pytest.raises(ValueError, match="v2"):
        parse_payload(BODY, SOURCE, NOW)


@pytest.mark.parametrize("bad", [BODY + b"\n", b'{"name":"Euro 2024","matches":[]}', b"invalid"])
def test_acquisition_requires_exact_reviewed_bytes_before_any_write(tmp_path, bad):
    collector = OpenFootballCollector(tmp_path / "unused", staging_version=STAGING_VERSION_V2)
    with pytest.raises(AcquisitionError, match="checksum"):
        collector.ingest_bytes(SOURCE, bad, observed_at=NOW)
    assert not collector.workspace.exists()


def test_full_source_replays_and_publishes_without_inventing_kickoff_or_predictive_availability(tmp_path):
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    first = collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    replay = collector.ingest_bytes(SOURCE, BODY, observed_at=NOW + timedelta(seconds=1))
    assert (first.accepted, first.changed, first.quarantined) == (51, 51, 0)
    assert (replay.accepted, replay.changed, replay.unchanged) == (51, 0, 51)
    assert (collector.workspace / first.archive_path).read_bytes() == BODY
    stage, output = tmp_path / "stage", tmp_path / "research"
    collector.export(stage)
    manifest = publish_research(stage, output, generated_at=NOW + timedelta(minutes=1))
    rows = json.loads((output / "matches.json").read_bytes())
    assert manifest["matches"] == 51 and manifest["sources"] == 1
    assert all(row["team_type"] == "national" and row["prediction_eligible"] is False for row in rows)
    assert all(row["kickoff_precision"] == "local_minute" for row in rows)
    assert all(row["start_time"] is row["timezone"] is row["completed_at"] is row["utc_offset_minutes"] is None
               for row in rows)
    assert all(row["observed_at"] == "2026-10-14T09:00:00.000000Z" and row["revision"] == 1 for row in rows)
    assert all(row["source_match_number"] is None and row["source_sha256"] == SHA for row in rows)
    final = next(row for row in rows if row["round"] == "Final")
    assert (final["team_a"], final["team_b"], final["score_a"], final["score_b"]) == ("Spain", "England", 2, 1)
    assert final["source_date"] == "2024-07-14" and final["local_time"] == "21:00"
    assert final["ground"] == "Berlin" and final["source_group"] is None
    opening = next(row for row in rows if row["team_a"] == "Germany" and row["team_b"] == "Scotland")
    assert (opening["source_date"], opening["score_a"], opening["score_b"], opening["source_group"]) == (
        "2024-06-14", 5, 1, "Group A")
    for filename, digest in manifest["files"].items():
        assert hashlib.sha256((output / filename).read_bytes()).hexdigest() == digest


def test_explicit_offsets_are_not_silently_enabled_for_the_local_time_euro_adapter():
    payload = json.loads(BODY)
    payload["matches"] = [dict(payload["matches"][0], time="21:00 UTC+2")]
    parsed = parse_payload(json.dumps(payload).encode(), SOURCE, NOW, staging_version=STAGING_VERSION_V2)
    assert parsed.rows == [] and len(parsed.quarantine) == 1


def test_corrupted_cache_cannot_pass_a_conditional_fetch(tmp_path):
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    fake = "0" * 64
    with sqlite3.connect(collector.state_path) as connection:
        connection.execute("UPDATE cache SET sha256=?,archive_path=?", [fake, f"raw/{SOURCE.source_id}/{fake}.json"])
    with pytest.raises(AcquisitionError, match="checksum"):
        collector._read_cache(SOURCE)


def _reseal(stage):
    path = stage / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["files"] = {name: hashlib.sha256((stage / name).read_bytes()).hexdigest() for name in manifest["files"]}
    path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")


@pytest.mark.parametrize("field,value", [("source_group", "Group Z"), ("ground", "Unknown ground"),
                                         ("local_time", "20:00")])
def test_resealed_plausible_business_fields_are_bound_to_reviewed_source(tmp_path, field, value):
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    collector.ingest_bytes(SOURCE, BODY, observed_at=NOW)
    stage = tmp_path / "stage"
    collector.export(stage)
    for name in ("matches", "history"):
        path = stage / f"{name}.json"
        rows = json.loads(path.read_bytes())
        row = rows[0]
        row[field] = value
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


def test_full_catalogue_can_publish_eleven_editions_without_changing_v1_bounds(tmp_path):
    collector = OpenFootballCollector(tmp_path / "state", staging_version=STAGING_VERSION_V2)
    for source in SOURCES.values():
        if source.repository == "worldcup.json":
            body = files("arena_lakehouse").joinpath("resources/worldcup_2026.json").read_bytes()
        elif source.repository == "euro.json":
            body = BODY
        else:
            body = json.dumps({"name": source.name, "matches": [dict(round="Fixture round", date="2026-10-01",
                                                                   team1="Fixture One", team2="Fixture Two")]}).encode()
        collector.ingest_bytes(source, body, observed_at=NOW)
    stage, output = tmp_path / "stage", tmp_path / "research"
    collector.export(stage)
    manifest = publish_research(stage, output, generated_at=NOW + timedelta(minutes=1))
    assert manifest["sources"] == 11 and manifest["matches"] == 164
