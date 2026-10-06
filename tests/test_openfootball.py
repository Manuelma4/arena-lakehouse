"""Offline protocol and staging invariants; no test makes a network request."""

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from arena_lakehouse.openfootball import (
    AcquisitionError, OpenFootballCollector, load_sources, parse_payload,
)


NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
CATALOGUE = Path(__file__).parents[1] / "config/openfootball_sources.json"
SOURCE = load_sources(CATALOGUE)["england-premier-2026-27"]
MATCH = dict(round="Matchday 1", date="2026-08-21", team1="Original Club One", team2="Original Club Two")


def body(*rows, source=SOURCE):
    return json.dumps(dict(name=source.name, matches=list(rows)), ensure_ascii=False).encode()


def read_state(collector, table="current"):
    with sqlite3.connect(collector.state_path) as connection:
        return [json.loads(row[0]) for row in connection.execute(f"SELECT row_json FROM {table} ORDER BY match_id")]


def test_catalogue_has_exactly_verified_no_token_sources():
    sources = load_sources(CATALOGUE)
    assert len(sources) == 9
    assert {s.region for s in sources.values()} == {"Europe", "South America"}
    assert sum(s.region == "Europe" for s in sources.values()) == 5
    assert all(s.url.startswith("https://raw.githubusercontent.com/openfootball/football.json/master/")
               for s in sources.values())


@pytest.mark.parametrize("bad", ["../private", "https://example.test/data", "2026-27/../data.json"])
def test_source_cannot_fetch_arbitrary_url_or_traverse_paths(bad):
    with pytest.raises(ValueError):
        replace(SOURCE, path=bad)


def test_precise_staging_keeps_dates_local_times_and_unknown_results():
    parsed = parse_payload(body(MATCH, dict(MATCH, round="Matchday 2", time="19:30", score=[0, 0])), SOURCE, NOW)
    date_only = next(row for row in parsed.rows if row["round"] == "Matchday 1")
    local = next(row for row in parsed.rows if row["round"] == "Matchday 2")
    assert parsed.quarantine == []
    assert date_only["source_date"] == "2026-08-21"
    assert date_only["kickoff_precision"] == "date_only" and date_only["local_time"] is None
    assert date_only["result_status"] == "unknown_result" and date_only["score_a"] is None
    assert local["kickoff_precision"] == "local_minute" and local["local_time"] == "19:30"
    assert local["result_status"] == "observed_result" and local["score_a"] == local["score_b"] == 0
    for row in parsed.rows:
        assert row["timezone"] is row["start_time"] is row["completed_at"] is None
        assert row["observed_at"] == "2026-10-06T12:00:00.000000Z"
        assert row["prediction_eligible"] is False


def test_knockout_score_fields_are_preserved_without_winner_or_regulation_inference():
    score = {"ft": [1, 1], "ht": [0, 0], "et": [2, 2], "p": [4, 5]}
    row = parse_payload(body(dict(MATCH, score=score)), SOURCE, NOW).rows[0]
    assert json.loads(row["score_details_json"]) == score
    assert row["score_a"] == row["score_b"] == 1
    assert row["score_basis"] == "source_ft_unverified_regulation"
    assert row["prediction_eligible"] is False


@pytest.mark.parametrize("score", [[1], [-1, 0], [True, 0], [1.5, 0], {"winner": 1}, "1-0"])
def test_invalid_score_variants_quarantine(score):
    parsed = parse_payload(body(dict(MATCH, score=score)), SOURCE, NOW)
    assert parsed.rows == [] and len(parsed.quarantine) == 1


@pytest.mark.parametrize("change", [dict(round=None), dict(team1=""), dict(team2=MATCH["team1"]),
                                  dict(date="2026-02-30"), dict(time="24:00"), dict(occurrence=0)])
def test_schema_identity_and_clock_errors_quarantine(change):
    parsed = parse_payload(body(dict(MATCH, **change)), SOURCE, NOW)
    assert parsed.rows == [] and len(parsed.quarantine) == 1


def test_idempotency_schedule_score_corrections_and_collection_availability(tmp_path):
    collector = OpenFootballCollector(tmp_path)
    raw = body(MATCH)
    first = collector.ingest_bytes(SOURCE, raw, observed_at=NOW)
    replay = collector.ingest_bytes(SOURCE, raw, observed_at=NOW + timedelta(hours=1))
    assert (first.changed, replay.changed, replay.unchanged) == (1, 0, 1)
    first_row = read_state(collector)[0]
    assert first_row["observed_at"] == first.observed_at
    corrected = body(dict(MATCH, date="2026-09-01", time="20:00", score={"ft": [2, 1]}))
    third = collector.ingest_bytes(SOURCE, corrected, observed_at=NOW + timedelta(days=1))
    row = read_state(collector)[0]
    assert row["match_id"] == first_row["match_id"] and row["team_a_id"] == first_row["team_a_id"]
    assert row["revision"] == 2 and row["source_date"] == "2026-09-01"
    assert row["observed_at"] == third.observed_at
    assert len(read_state(collector, "history")) == 2
    assert row["completed_at"] is None
    with pytest.raises(ValueError, match="backwards"):
        collector.ingest_bytes(SOURCE, raw, observed_at=NOW)
    assert len(read_state(collector, "history")) == 2


def test_equivalent_score_shapes_do_not_create_a_revision(tmp_path):
    collector = OpenFootballCollector(tmp_path)
    collector.ingest_bytes(SOURCE, body(dict(MATCH, score=[2, 1])), observed_at=NOW)
    report = collector.ingest_bytes(SOURCE, body(dict(MATCH, score={"ft": [2, 1]})),
                                   observed_at=NOW + timedelta(days=1))
    assert report.changed == 0 and report.unchanged == 1


def test_team_identity_survives_seasons_but_country_names_do_not_conflate():
    one = parse_payload(body(MATCH), SOURCE, NOW).rows[0]
    next_season = replace(SOURCE, source_id="england-premier-2027-28", season="2027-28",
                          path="2027-28/en.1.json", name="English Premier League 2027/28")
    two = parse_payload(body(MATCH, source=next_season), next_season, NOW).rows[0]
    another_country = replace(SOURCE, team_namespace="openfootball-spain")
    three = parse_payload(body(MATCH, source=another_country), another_country, NOW).rows[0]
    assert one["team_a_id"] == two["team_a_id"] and one["match_id"] != two["match_id"]
    assert one["team_a_id"] != three["team_a_id"]
    assert one["team_identity_status"] == "unresolved_name_derived_not_official"


def test_reviewed_alias_preserves_match_identity_on_rename(tmp_path):
    source = replace(SOURCE, team_aliases={"Original Club One Renamed": "Original Club One"})
    collector = OpenFootballCollector(tmp_path)
    collector.ingest_bytes(source, body(MATCH, source=source), observed_at=NOW)
    first = read_state(collector)[0]
    report = collector.ingest_bytes(source, body(dict(MATCH, team1="Original Club One Renamed"), source=source),
                                   observed_at=NOW + timedelta(days=1))
    second = read_state(collector)[0]
    assert report.changed == 1 and second["match_id"] == first["match_id"]
    assert second["team_a_id"] == first["team_a_id"] and second["team_a"].endswith("Renamed")
    with pytest.raises(AcquisitionError, match="catalogue changed"):
        collector.ingest_bytes(SOURCE, body(MATCH), observed_at=NOW + timedelta(days=2))


def test_duplicate_collapse_collision_is_symmetric_and_explicit_occurrence_separates():
    assert parse_payload(body(MATCH, MATCH), SOURCE, NOW).duplicates == 1
    conflict = dict(MATCH, date="2026-08-22")
    for records in ((MATCH, conflict), (conflict, MATCH)):
        parsed = parse_payload(body(*records), SOURCE, NOW)
        assert parsed.rows == [] and len(parsed.quarantine) == 2
        assert all("Conflicting match identity" in q["reason"] for q in parsed.quarantine)
    parsed = parse_payload(body(MATCH, dict(conflict, occurrence=2)), SOURCE, NOW)
    assert len(parsed.rows) == 2 and parsed.quarantine == []


def test_dry_offline_validation_makes_no_workspace_or_cursor(tmp_path):
    workspace = tmp_path / "uncreated"
    collector = OpenFootballCollector(workspace)
    report = collector.ingest_bytes(SOURCE, body(MATCH), observed_at=NOW, dry_run=True)
    assert report.changed == 1 and report.dry_run and report.archive_path is None
    assert not workspace.exists()


def test_conditional_fetch_archives_once_and_304_emits_no_revision(tmp_path):
    requests = []
    raw = body(MATCH)
    def response(request):
        requests.append(request)
        assert "authorization" not in request.headers and request.url == SOURCE.url
        if len(requests) == 1:
            return httpx.Response(200, content=raw, headers={"ETag": '"v1"'})
        assert request.headers["If-None-Match"] == '"v1"'
        return httpx.Response(304)
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, min_interval=0)
        first = collector.collect(SOURCE, observed_at=NOW)
        second = collector.collect(SOURCE, observed_at=NOW + timedelta(minutes=1))
    assert second.status == "not_modified" and second.changed == 0
    assert first.source_sha256 == hashlib.sha256(raw).hexdigest()
    assert (tmp_path / first.archive_path).read_bytes() == raw
    row = read_state(collector)[0]
    assert row["source_sha256"] == first.source_sha256 and row["source_url"] == SOURCE.url
    assert row["license"] == "CC0-1.0" and len(read_state(collector, "history")) == 1
    with sqlite3.connect(collector.state_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM polls").fetchone()[0] == 2


def test_last_modified_and_dry_fetch_do_not_update_cursor(tmp_path):
    headers_seen = []
    def response(request):
        headers_seen.append(dict(request.headers))
        return httpx.Response(200, content=body(MATCH), headers={"Last-Modified": "Tue, 06 Oct 2026 11:00:00 GMT"})
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, min_interval=0)
        collector.collect(SOURCE, observed_at=NOW)
        collector.collect(SOURCE, observed_at=NOW + timedelta(hours=1), dry_run=True)
    assert headers_seen[1]["if-modified-since"] == "Tue, 06 Oct 2026 11:00:00 GMT"
    assert len(read_state(collector, "history")) == 1
    with sqlite3.connect(collector.state_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM polls").fetchone()[0] == 1


def test_missing_and_corrupt_conditional_cache_are_not_successful(tmp_path):
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(304))) as client:
        collector = OpenFootballCollector(tmp_path, client, min_interval=0)
        with pytest.raises(AcquisitionError, match="no previously archived"):
            collector.collect(SOURCE, observed_at=NOW)
        assert not collector.state_path.exists()
        first = collector.ingest_bytes(SOURCE, body(MATCH), observed_at=NOW, validators={"etag": '"v1"'})
        (tmp_path / first.archive_path).write_bytes(b"corrupt")
        with pytest.raises(AcquisitionError, match="missing or corrupt"):
            collector.collect(SOURCE, observed_at=NOW + timedelta(minutes=1))


def test_retry_and_rate_budgets_are_bounded(tmp_path):
    clock, sleeps, calls = [0.0], [], []
    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds
    def response(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(429, headers={"Retry-After": "1000"})
        return httpx.Response(200, content=body(MATCH))
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, attempts=3, min_interval=1,
                                          sleep=sleep, monotonic=lambda: clock[0])
        assert collector.collect(SOURCE, observed_at=NOW).changed == 1
    assert len(calls) == 3 and all(0 <= delay <= 10 for delay in sleeps) and sum(sleeps) == 20


@pytest.mark.parametrize("status,expected_calls", [(503, 2), (403, 1), (302, 1)])
def test_http_failure_never_commits_state(tmp_path, status, expected_calls):
    calls = []
    def response(request):
        calls.append(request)
        return httpx.Response(status)
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, attempts=2, min_interval=0, sleep=lambda _: None)
        with pytest.raises(AcquisitionError):
            collector.collect(SOURCE, observed_at=NOW)
    assert len(calls) == expected_calls and not collector.state_path.exists()


def test_timeout_retries_then_stops(tmp_path):
    calls = []
    def response(request):
        calls.append(request)
        raise httpx.ReadTimeout("offline timeout", request=request)
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, attempts=2, min_interval=0, sleep=lambda _: None)
        with pytest.raises(AcquisitionError, match="2 attempts"):
            collector.collect(SOURCE, observed_at=NOW)
    assert len(calls) == 2 and not collector.state_path.exists()


def test_malformed_payload_is_archived_and_quarantine_is_idempotent(tmp_path):
    collector = OpenFootballCollector(tmp_path)
    raw = b'{"matches": malformed}'
    first = collector.ingest_bytes(SOURCE, raw, observed_at=NOW)
    collector.ingest_bytes(SOURCE, raw, observed_at=NOW + timedelta(days=1))
    assert first.quarantined == 1 and first.changed == 0
    assert (tmp_path / first.archive_path).read_bytes() == raw
    with sqlite3.connect(collector.state_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 1


def test_duplicate_json_keys_cannot_silently_change_schema():
    raw = ('{"name":"' + SOURCE.name + '","matches":[],"matches":[{}]}').encode()
    parsed = parse_payload(raw, SOURCE, NOW)
    assert parsed.rows == [] and "Duplicate JSON key" in parsed.quarantine[0]["reason"]


def test_overlapping_source_scope_cannot_steal_a_stable_match_identity(tmp_path):
    collector = OpenFootballCollector(tmp_path)
    collector.ingest_bytes(SOURCE, body(MATCH), observed_at=NOW)
    overlapping = replace(SOURCE, source_id="another-source-same-competition-season")
    with pytest.raises(AcquisitionError, match="another source"):
        collector.ingest_bytes(overlapping, body(MATCH, source=overlapping),
                               observed_at=NOW + timedelta(days=1))
    assert len(read_state(collector, "history")) == 1 and read_state(collector)[0]["source_id"] == SOURCE.source_id


def test_database_failure_rolls_back_revision_and_poll_but_preserves_raw(tmp_path):
    collector = OpenFootballCollector(tmp_path)
    collector.ingest_bytes(SOURCE, body(MATCH), observed_at=NOW)
    with sqlite3.connect(collector.state_path) as connection:
        connection.execute("CREATE TRIGGER test_fail BEFORE INSERT ON current BEGIN SELECT RAISE(ABORT,'failure'); END")
    corrected = body(dict(MATCH, score=[2, 1]))
    with pytest.raises(sqlite3.IntegrityError, match="failure"):
        collector.ingest_bytes(SOURCE, corrected, observed_at=NOW + timedelta(days=1))
    assert len(read_state(collector, "history")) == 1 and read_state(collector)[0]["score_a"] is None
    assert (tmp_path / "raw" / SOURCE.source_id / (hashlib.sha256(corrected).hexdigest() + ".json")).exists()


def test_export_json_parquet_contract_checksums_and_empty_types(tmp_path):
    collector = OpenFootballCollector(tmp_path / "staging")
    collector.ingest_bytes(SOURCE, body(MATCH, dict(MATCH, round="Matchday 2", score=[1, 0])), observed_at=NOW)
    destination = tmp_path / "export"
    manifest = collector.export(destination)
    rows = json.loads((destination / "matches.json").read_text())
    assert rows == pq.read_table(destination / "matches.parquet").to_pylist()
    assert manifest["matches"] == manifest["revisions"] == 2 and manifest["canonical_compatible"] is False
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((destination / name).read_bytes()).hexdigest() == digest
    with pytest.raises(ValueError, match="already exists"):
        collector.export(destination)
    empty = OpenFootballCollector(tmp_path / "empty")
    empty.ingest_bytes(SOURCE, body(dict(MATCH, round=None)), observed_at=NOW)
    empty.export(tmp_path / "empty-export")
    table = pq.read_table(tmp_path / "empty-export/matches.parquet")
    assert table.num_rows == 0 and str(table.schema.field("score_a").type) == "int64"


def test_collection_time_requires_aware_instant_and_converts_offset():
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_payload(body(MATCH), SOURCE, NOW.replace(tzinfo=None))
    row = parse_payload(body(MATCH), SOURCE, NOW.astimezone(timezone(timedelta(hours=-5)))).rows[0]
    assert row["observed_at"] == "2026-10-06T12:00:00.000000Z"
