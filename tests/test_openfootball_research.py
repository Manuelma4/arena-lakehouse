"""Research publication oracle: integrity, provenance, precision and deterministic identity."""

import hashlib
import json
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arena_lakehouse.openfootball import OpenFootballCollector, STAGING_VERSION_V2, WORLD_CUP_SHA256, load_sources
from arena_lakehouse.openfootball_research import (
    BOUNDS, RESEARCH_FILES, ResearchValidationError, _schema, publish_research, validate_staging,
)


NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
SOURCE = load_sources(Path(__file__).parents[1] / "config/openfootball_sources.json")["colombia-primera-a-2025"]
MATCH = dict(round="Apertura, Matchday 1", date="2025-01-24", team1="Research Club One", team2="Research Club Two")


def _body(*rows):
    return json.dumps(dict(name=SOURCE.name, matches=list(rows)), ensure_ascii=False).encode("utf-8")


@pytest.fixture
def staging(tmp_path):
    collector = OpenFootballCollector(tmp_path / "runtime")
    first = _body(MATCH, dict(MATCH, round="Apertura, Matchday 2", time="18:00",
                             score={"ft": [1, 1], "et": [2, 2], "p": [4, 5]}))
    collector.ingest_bytes(SOURCE, first, observed_at=NOW)
    collector.ingest_bytes(SOURCE, first, observed_at=NOW + timedelta(minutes=1))
    corrected = _body(dict(MATCH, date="2025-01-25"), dict(MATCH, round="Apertura, Matchday 2", time="18:00",
                                                            score={"ft": [1, 1], "et": [2, 2], "p": [4, 5]}),
                      dict(MATCH, round=None))
    collector.ingest_bytes(SOURCE, corrected, observed_at=NOW + timedelta(minutes=2))
    destination = tmp_path / "staging"
    collector.export(destination)
    return destination


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def _rehash(staging):
    manifest = _read(staging / "manifest.json")
    manifest["files"] = {name: hashlib.sha256((staging / name).read_bytes()).hexdigest()
                         for name in manifest["files"]}
    _write(staging / "manifest.json", manifest)


def _rewrite_rows(staging, name, rows):
    _write(staging / f"{name}.json", rows)
    version = _read(staging / "manifest.json")["staging_version"]
    pq.write_table(pa.Table.from_pylist(rows, schema=_schema(staging_version=version)), staging / f"{name}.parquet")
    _rehash(staging)


def test_verified_history_availability_and_precision_preservation(staging, tmp_path):
    data = validate_staging(staging)
    assert len(data.matches) == 2 and len(data.history) == 3 and len(data.quarantine) == 1
    assert data.as_of == "2026-10-06T12:02:00.000000Z"
    output = tmp_path / "research"
    manifest = publish_research(staging, output, generated_at=NOW + timedelta(hours=1))
    matches = _read(output / "matches.json")
    history = _read(output / "history.json")
    assert {p.relative_to(output).as_posix() for p in output.rglob("*") if p.is_file()} == RESEARCH_FILES | {"manifest.json"}
    assert manifest["research_version"] == "openfootball-research-1.0"
    assert manifest["canonical_compatible"] is False and manifest["data_kind"] == "open_data"
    assert manifest["as_of"] == data.as_of
    assert matches == pq.read_table(output / "gold/matches.parquet").to_pylist()
    assert history == pq.read_table(output / "gold/match_history.parquet").to_pylist()
    assert matches == [dict(row, edition_id=row["source_id"]) for row in sorted(data.matches, key=lambda r: r["match_id"])]
    assert all(row["start_time"] is row["timezone"] is row["completed_at"] is None for row in matches)
    assert all(row["prediction_eligible"] is False for row in matches)
    known = next(row for row in matches if row["result_status"] == "observed_result")
    assert json.loads(known["score_details_json"]) == {"ft": [1, 1], "et": [2, 2], "p": [4, 5]}
    assert (known["score_a"], known["score_b"]) == (1, 1)
    for name, checksum in manifest["files"].items():
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == checksum
    assert manifest["upstream_staging_fingerprint"] == hashlib.sha256((staging / "manifest.json").read_bytes()).hexdigest()


def test_competition_coverage_uses_source_dates_and_measured_counts(staging, tmp_path):
    output = tmp_path / "research"
    publish_research(staging, output, generated_at=NOW + timedelta(hours=1))
    edition = _read(output / "competitions.json")[0]
    assert edition["edition_id"] == SOURCE.source_id and edition["competition_id"] == SOURCE.competition_id
    assert edition["name"] == SOURCE.name and edition["season"] == "2025" and edition["region"] == "South America"
    assert (edition["coverage_start"], edition["coverage_end"]) == ("2025-01-24", "2025-01-25")
    assert (edition["match_count"], edition["observed_results"], edition["unknown_results"]) == (2, 1, 1)
    assert edition["date_only"] == edition["local_minute"] == 1
    assert len(edition["participants"]) == 2
    assert [p["team_id"] for p in edition["participants"]] == sorted(p["team_id"] for p in edition["participants"])
    assert not {"standings", "points", "wins", "draws", "losses"} & edition.keys()
    quality = _read(output / "quality.json")
    assert quality["unresolved_timezones"] == quality["prediction_ineligible"] == quality["unresolved_team_identities"] == 2
    assert quality["quarantine_rows"] == 1 and quality["revision_count"] == 3
    assert isinstance(_read(output / "lineage.json"), list)
    assert _read(output / "sources.json") == _read(staging / "sources.json")


def test_dataset_identity_excludes_publication_clocks_and_matches_independent_hash(staging, tmp_path):
    one, two = tmp_path / "one", tmp_path / "two"
    first = publish_research(staging, one, generated_at=NOW + timedelta(hours=1))
    stage_manifest = _read(staging / "manifest.json")
    stage_manifest["generated_at"] = "2026-10-06T12:30:00.000000Z"
    _write(staging / "manifest.json", stage_manifest)
    second = publish_research(staging, two, generated_at=NOW + timedelta(days=1))
    assert first["dataset_version"] == second["dataset_version"]
    assert first["files"] == second["files"]
    assert first["upstream_staging_fingerprint"] != second["upstream_staging_fingerprint"]
    content = {name: _read(one / f"{name}.json") for name in
               ("matches", "history", "sources", "competitions", "quality", "lineage", "quarantine", "polls")}
    content["as_of"] = first["as_of"]
    independently = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"),
                                               ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()[:16]
    assert first["dataset_version"] == independently


def test_output_is_immutable_and_failed_validation_creates_no_destination(staging, tmp_path):
    output = tmp_path / "research"
    publish_research(staging, output, generated_at=NOW + timedelta(hours=1))
    original = (output / "manifest.json").read_bytes()
    with pytest.raises(ResearchValidationError, match="already exists"):
        publish_research(staging, output, generated_at=NOW + timedelta(hours=2))
    assert (output / "manifest.json").read_bytes() == original
    (staging / "matches.json").write_bytes(b"tampered")
    with pytest.raises(ResearchValidationError, match="checksum mismatch"):
        publish_research(staging, tmp_path / "never-created", generated_at=NOW + timedelta(hours=2))
    assert not (tmp_path / "never-created").exists()


def test_aware_publication_clock_cannot_predate_observation(staging, tmp_path):
    with pytest.raises(ResearchValidationError, match="predate"):
        publish_research(staging, tmp_path / "old", generated_at=NOW)
    with pytest.raises(ResearchValidationError, match="timezone-aware"):
        publish_research(staging, tmp_path / "naive", generated_at=NOW.replace(tzinfo=None))


@pytest.mark.parametrize("mutation", ["count", "canonical", "license", "inventory"])
def test_manifest_contract_tampering_is_rejected(staging, mutation):
    manifest = _read(staging / "manifest.json")
    if mutation == "count":
        manifest["matches"] += 1
    elif mutation == "canonical":
        manifest["canonical_compatible"] = True
    elif mutation == "license":
        manifest["license"] = "unknown"
    else:
        manifest["files"]["../private.json"] = "0" * 64
    _write(staging / "manifest.json", manifest)
    with pytest.raises(ResearchValidationError):
        validate_staging(staging)


def test_json_parquet_parity_is_enforced_even_if_hashes_are_recomputed(staging):
    rows = _read(staging / "matches.json")
    rows[0]["source_date"] = "2025-03-01"
    _write(staging / "matches.json", rows)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="parity"):
        validate_staging(staging)


def test_latest_current_history_equality_is_enforced(staging):
    rows = _read(staging / "matches.json")
    old = next(row for row in _read(staging / "history.json") if row["revision"] == 1
               and row["match_id"] == next(r["match_id"] for r in rows if r["revision"] == 2))
    rows = [old if row["match_id"] == old["match_id"] else row for row in rows]
    _rewrite_rows(staging, "matches", rows)
    with pytest.raises(ResearchValidationError, match="latest revision"):
        validate_staging(staging)


@pytest.mark.parametrize("field,value,expected", [("timezone", "UTC", "infer UTC"),
    ("start_time", "2025-01-24T00:00:00Z", "infer UTC"), ("prediction_eligible", True, "prediction eligibility"),
    ("source_url", "https://private.invalid/data", "allowlist"), ("team_a_id", "fake:identity", "Team identity"),
    ("score_details_json", '{"ft":[8,7]}', "Score fields"), ("kickoff_precision", "utc_instant", "precision")])
def test_row_semantic_tampering_is_rejected_with_matching_parquet(staging, field, value, expected):
    rows = _read(staging / "matches.json")
    rows[0][field] = value
    _rewrite_rows(staging, "matches", rows)
    with pytest.raises(ResearchValidationError, match=expected):
        validate_staging(staging)


def test_fixed_source_catalogue_scope_is_checked(staging):
    sources = _read(staging / "sources.json")
    sources[0]["path"] = "2025/private.json"
    _write(staging / "sources.json", sources)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="allowlist"):
        validate_staging(staging)


def test_poll_revision_and_quarantine_availability_evidence_are_checked(staging):
    polls = _read(staging / "polls.json")
    polls[0]["changed"] += 1
    _write(staging / "polls.json", polls)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="Poll changes"):
        validate_staging(staging)


def test_quarantine_must_have_matching_measured_poll(staging):
    problems = _read(staging / "quarantine.json")
    problems[0]["observed_at"] = "2026-10-06T12:01:00.000000Z"
    _write(staging / "quarantine.json", problems)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="Quarantine lacks"):
        validate_staging(staging)


def test_revision_history_cannot_duplicate_an_event(staging):
    history = _read(staging / "history.json")
    history[0] = history[1]
    _rewrite_rows(staging, "history", history)
    with pytest.raises(ResearchValidationError, match="Duplicate event"):
        validate_staging(staging)


def test_bounds_and_unexpected_files_are_rejected_before_publication(staging, monkeypatch):
    original_bound = BOUNDS["matches"]
    monkeypatch.setitem(BOUNDS, "matches", 1)
    with pytest.raises(ResearchValidationError, match="row bounds"):
        validate_staging(staging)
    monkeypatch.setitem(BOUNDS, "matches", original_bound)
    (staging / "extra.txt").write_text("unknown")
    with pytest.raises(ResearchValidationError, match="Unexpected"):
        validate_staging(staging)


def test_zero_result_edition_remains_explicit_with_null_coverage(tmp_path):
    collector = OpenFootballCollector(tmp_path / "runtime")
    collector.ingest_bytes(SOURCE, _body(dict(MATCH, round=None)), observed_at=NOW)
    stage = tmp_path / "staging"
    collector.export(stage)
    output = tmp_path / "research"
    manifest = publish_research(stage, output, generated_at=NOW + timedelta(hours=1))
    edition = _read(output / "competitions.json")[0]
    assert manifest["matches"] == manifest["revisions"] == 0 and manifest["quarantine_rows"] == 1
    assert edition["coverage_start"] is edition["coverage_end"] is None
    assert edition["participants"] == [] and edition["match_count"] == 0
    assert pq.read_table(output / "gold/matches.parquet").num_rows == 0


def test_packaged_catalogue_is_exact_reviewed_config_mirror():
    packaged = files("arena_lakehouse").joinpath("resources/openfootball_sources.json")
    assert packaged.read_bytes() == (Path(__file__).parents[1] / "config/openfootball_sources.json").read_bytes()
    assert load_sources(packaged) == load_sources(Path(__file__).parents[1] / "config/openfootball_sources.json")


def test_reviewed_alias_values_use_collector_nfc_and_strip_normalization(tmp_path):
    source = replace(SOURCE, team_aliases={"Research Club One": "  Canonical Cafe\u0301  "})
    collector = OpenFootballCollector(tmp_path / "runtime")
    collector.ingest_bytes(source, _body(MATCH), observed_at=NOW)
    stage = tmp_path / "staging"
    collector.export(stage)
    data = validate_staging(stage)
    assert data.sources[0]["team_aliases"]["Research Club One"] == "  Canonical Cafe\u0301  "
    assert len(data.matches) == 1


def test_semantic_parser_consumes_the_same_bytes_whose_hash_was_verified(staging, monkeypatch):
    original, reads = Path.read_bytes, []
    def read(path):
        if path == staging / "matches.json":
            reads.append(path)
            if len(reads) > 1:
                return b'[{"tampered_after_hash_check":true}]'
        return original(path)
    monkeypatch.setattr(Path, "read_bytes", read)
    assert len(validate_staging(staging).matches) == 2
    assert len(reads) == 1


def test_quarantine_rows_cannot_exceed_measured_poll_counts(staging):
    problems = _read(staging / "quarantine.json")
    problems.append(dict(problems[0], reason="Fabricated second rejection"))
    _write(staging / "quarantine.json", problems)
    manifest = _read(staging / "manifest.json")
    manifest["quarantine_rows"] = 2
    _write(staging / "manifest.json", manifest)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="measured source poll count"):
        validate_staging(staging)


@pytest.mark.parametrize("field,value,expected", [
    ("source_id", [], "source"), ("source_id", None, "source"),
    ("row_index", True, "index"), ("row_index", 1.0, "index"),
    ("row_index", -2, "index"), ("row_index", 10_000, "index"),
    ("reason", [], "reason"), ("reason", "", "reason"),
    ("sha256", [], "checksum"), ("sha256", "f" * 63, "checksum"),
    ("observed_at", "2026-10-06T12:02:00", "aware UTC"),
    ("raw_json", {}, "raw JSON"), ("raw_json", '{"duplicate":1,"duplicate":2}', "JSON"),
    ("raw_json", '{"value":NaN}', "JSON"),
])
def test_quarantine_identity_and_types_are_strict_after_resealing(staging, field, value, expected):
    problems = _read(staging / "quarantine.json")
    problems[0][field] = value
    _write(staging / "quarantine.json", problems)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match=expected):
        validate_staging(staging)


def test_quarantine_duplicate_identity_is_rejected_even_with_sufficient_poll_count(staging):
    problems = _read(staging / "quarantine.json")
    problems.append(dict(problems[0]))
    _write(staging / "quarantine.json", problems)
    polls = _read(staging / "polls.json")
    polls[-1]["quarantined"] = 2
    _write(staging / "polls.json", polls)
    manifest = _read(staging / "manifest.json")
    manifest["quarantine_rows"] = 2
    _write(staging / "manifest.json", manifest)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="Duplicate quarantine evidence"):
        validate_staging(staging)


@pytest.mark.parametrize("field,value,expected", [
    ("source_id", [], "source"), ("source_id", None, "source"),
    ("status", [], "status"), ("status", None, "status"),
    ("changed", True, "changed"), ("quarantined", 1.0, "quarantined"),
    ("poll_id", True, "poll IDs"),
])
def test_poll_types_reject_malformed_resealed_values(staging, field, value, expected):
    polls = _read(staging / "polls.json")
    polls[-1][field] = value
    _write(staging / "polls.json", polls)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match=expected):
        validate_staging(staging)


def _append_poll(staging, **changes):
    polls = _read(staging / "polls.json")
    extra = dict(polls[-1], poll_id=max(p["poll_id"] for p in polls) + 1,
                 observed_at="2026-10-06T12:03:00.000000Z", changed=0, quarantined=0)
    extra.update(changes)
    polls.append(extra)
    _write(staging / "polls.json", polls)
    _rehash(staging)


def test_304_requires_previously_collected_checksum(staging):
    _append_poll(staging, status="not_modified", sha256="f" * 64)
    with pytest.raises(ResearchValidationError, match="previously collected body checksum"):
        validate_staging(staging)


def test_304_requires_same_source_archive_not_another_sources_checksum(staging):
    sources = _read(staging / "sources.json")
    catalogue = load_sources(Path(__file__).parents[1] / "config/openfootball_sources.json")
    other = next(source for source in catalogue.values() if source.source_id != SOURCE.source_id)
    sources.append(asdict(other))
    _write(staging / "sources.json", sources)
    manifest = _read(staging / "manifest.json")
    manifest["sources"] = 2
    _write(staging / "manifest.json", manifest)
    _append_poll(staging, source_id=other.source_id, status="not_modified")
    with pytest.raises(ResearchValidationError, match="previously collected body checksum"):
        validate_staging(staging)


def test_304_archive_must_precede_response_in_poll_id_order(staging):
    polls = _read(staging / "polls.json")
    response = dict(polls[-1], poll_id=1, status="not_modified", changed=0, quarantined=0,
                    observed_at="2026-10-06T11:59:00.000000Z")
    for poll in polls:
        poll["poll_id"] += 1
    # JSON array order is not the collection order: poll IDs determine availability.
    polls.append(response)
    _write(staging / "polls.json", polls)
    _rehash(staging)
    with pytest.raises(ResearchValidationError, match="previously collected body checksum"):
        validate_staging(staging)


@pytest.mark.parametrize("status", ["collected", "not_modified"])
def test_poll_clock_cannot_move_backwards_per_source(staging, status):
    _append_poll(staging, status=status, observed_at="2026-10-06T12:01:00.000000Z")
    with pytest.raises(ResearchValidationError, match="availability moves backwards"):
        validate_staging(staging)


def test_valid_304_advances_as_of_without_revising_matches(staging):
    previous = validate_staging(staging)
    _append_poll(staging, status="not_modified")
    current = validate_staging(staging)
    assert current.as_of == "2026-10-06T12:03:00.000000Z"
    assert current.matches == previous.matches and current.history == previous.history
    assert current.quarantine == previous.quarantine


@pytest.mark.parametrize("field", ["changed", "quarantined"])
def test_304_cannot_claim_new_changes_or_rejections(staging, field):
    _append_poll(staging, status="not_modified", **{field: 1})
    with pytest.raises(ResearchValidationError, match="304 audit must not fabricate"):
        validate_staging(staging)


def test_repolling_rejected_body_preserves_single_quarantine_evidence(tmp_path):
    collector = OpenFootballCollector(tmp_path / "runtime")
    body = _body(MATCH, dict(MATCH, round=None))
    first = collector.ingest_bytes(SOURCE, body, observed_at=NOW)
    replay = collector.ingest_bytes(SOURCE, body, observed_at=NOW + timedelta(minutes=1))
    stage = tmp_path / "staging"
    collector.export(stage)
    data = validate_staging(stage)
    assert first.quarantined == replay.quarantined == 1
    assert len(data.quarantine) == len(data.matches) == len(data.history) == 1
    assert sum(p["quarantined"] for p in data.polls) == 2


@pytest.fixture
def staging_v2(tmp_path):
    collector = OpenFootballCollector(tmp_path / 'v2-runtime', staging_version=STAGING_VERSION_V2)
    collector.ingest_bytes(SOURCE, _body(MATCH), observed_at=NOW)
    worldcup = load_sources(Path(__file__).parents[1] / 'config/openfootball_sources.json')['worldcup-2026']
    raw = (Path(__file__).parent / 'fixtures/worldcup-2026-pinned.json').read_bytes()
    collector.ingest_bytes(worldcup, raw, observed_at=NOW + timedelta(minutes=1))
    output = tmp_path / 'v2-staging'
    collector.export(output)
    return output


def test_v2_publication_preserves_precise_worldcup_and_unzoned_clubs(staging_v2, tmp_path):
    data = validate_staging(staging_v2)
    assert len(data.matches) == len(data.history) == 105
    output = tmp_path / 'v2-research'
    manifest = publish_research(staging_v2, output, generated_at=NOW + timedelta(hours=1))
    assert manifest['research_version'] == 'openfootball-research-2.0'
    assert manifest['as_of'] == '2026-10-06T12:01:00.000000Z'
    matches = _read(output / 'matches.json')
    assert matches == pq.read_table(output / 'gold/matches.parquet').to_pylist()
    assert _read(output / 'history.json') == pq.read_table(output / 'gold/match_history.parquet').to_pylist()
    assert set(pq.read_table(output / 'gold/matches.parquet').schema.names) == set(matches[0])
    national = [r for r in matches if r['team_type'] == 'national']
    assert len(national) == 104 and all(r['utc_offset_minutes'] is not None for r in national)
    club = next(r for r in matches if r['team_type'] == 'club')
    assert club['source_match_number'] is club['utc_offset_minutes'] is club['start_time'] is None
    assert all(r['timezone'] is r['completed_at'] is None and r['prediction_eligible'] is False for r in matches)
    editions = {e['edition_id']: e for e in _read(output / 'competitions.json')}
    wc = editions['worldcup-2026']
    assert wc['team_type'] == 'national' and wc['region'] == 'International'
    assert wc['coverage_start'] == '2026-06-11' and wc['coverage_end'] == '2026-07-19'
    assert wc['match_count'] == wc['observed_results'] == wc['offset_minute'] == 104
    assert wc['date_only'] == wc['local_minute'] == wc['unknown_results'] == 0
    assert len(wc['participants']) == 48
    quality = _read(output / 'quality.json')
    assert quality['offset_minute'] == 104 and quality['unresolved_timezones'] == 1
    assert quality['date_only'] == 1 and quality['prediction_ineligible'] == 105
    assert all(s['repository'] in ('football.json', 'worldcup.json') and s['team_type'] in ('club', 'national')
               for s in _read(output / 'sources.json'))


@pytest.mark.parametrize('field,value,expected', [
    ('start_time', '2026-07-19T15:00:00.000000Z', 'UTC kickoff differs'),
    ('utc_offset_minutes', 841, 'UTC offset'),
    ('utc_offset_minutes', -240.0, 'UTC offset'),
    ('team_type', 'club', 'Team type'),
    ('source_match_number', 104.0, 'source match number'),
    ('source_group', ' Group A ', 'normalized'),
    ('ground', '', 'source ground'),
    ('completed_at', '2026-07-19T21:00:00.000000Z', 'completion'),
    ('kickoff_precision', 'local_minute', 'precision'),
])
def test_v2_resealed_clock_identity_and_metadata_tampering_rejected(staging_v2, field, value, expected):
    rows = _read(staging_v2 / 'matches.json')
    final = next(r for r in rows if r['source_match_number'] == 104)
    final[field] = value
    _rewrite_rows(staging_v2, 'matches', rows)
    with pytest.raises(ResearchValidationError, match=expected):
        validate_staging(staging_v2)


def test_v2_pinned_source_poll_cannot_claim_other_body(staging_v2):
    polls = _read(staging_v2 / 'polls.json')
    next(p for p in polls if p['source_id'] == 'worldcup-2026')['sha256'] = 'f' * 64
    _write(staging_v2 / 'polls.json', polls)
    _rehash(staging_v2)
    with pytest.raises(ResearchValidationError, match='Pinned national poll checksum'):
        validate_staging(staging_v2)


def test_v2_pinned_source_ref_is_not_an_arbitrary_sha(staging_v2):
    sources = _read(staging_v2 / 'sources.json')
    next(s for s in sources if s['source_id'] == 'worldcup-2026')['source_ref'] = 'f' * 40
    _write(staging_v2 / 'sources.json', sources)
    _rehash(staging_v2)
    with pytest.raises(ResearchValidationError, match='Invalid source catalogue'):
        validate_staging(staging_v2)


def test_v2_sources_require_explicit_repository_and_team_type(staging_v2):
    sources = _read(staging_v2 / 'sources.json')
    del next(s for s in sources if s['source_id'] == SOURCE.source_id)['team_type']
    _write(staging_v2 / 'sources.json', sources)
    _rehash(staging_v2)
    with pytest.raises(ResearchValidationError, match='explicit repository/team type'):
        validate_staging(staging_v2)


def test_v2_cannot_masquerade_as_v1_by_resealing_manifest(staging_v2):
    manifest = _read(staging_v2 / 'manifest.json')
    manifest['staging_version'] = 'openfootball-staging-1.0'
    _write(staging_v2 / 'manifest.json', manifest)
    with pytest.raises(ResearchValidationError, match='schema/count differs'):
        validate_staging(staging_v2)


def test_v2_logical_identity_excludes_publication_clock_and_preserves_pin(staging_v2, tmp_path):
    first = publish_research(staging_v2, tmp_path / 'one', generated_at=NOW + timedelta(hours=1))
    later = publish_research(staging_v2, tmp_path / 'two', generated_at=NOW + timedelta(hours=2))
    assert first['dataset_version'] == later['dataset_version'] and first['files'] == later['files']
    assert first['generated_at'] != later['generated_at']
    rows = _read(tmp_path / 'one/matches.json')
    assert {r['source_sha256'] for r in rows if r['team_type'] == 'national'} == {WORLD_CUP_SHA256}


def test_v2_numbered_revision_preserves_identity_across_team_and_round_corrections(tmp_path):
    collector = OpenFootballCollector(tmp_path / 'runtime', staging_version=STAGING_VERSION_V2)
    before = dict(MATCH, num=5)
    after = dict(before, round='Reviewed source round', team1='Reviewed Club One', date='2025-01-25')
    collector.ingest_bytes(SOURCE, _body(before), observed_at=NOW)
    collector.ingest_bytes(SOURCE, _body(after), observed_at=NOW + timedelta(minutes=1))
    collector.export(tmp_path / 'staging')
    data = validate_staging(tmp_path / 'staging')
    assert len(data.matches) == 1 and len(data.history) == 2
    assert data.history[0]['match_id'] == data.history[1]['match_id'] == data.matches[0]['match_id']
    assert {r['revision'] for r in data.history} == {1, 2}
    assert data.matches[0]['team_a'] == 'Reviewed Club One'


@pytest.mark.parametrize('field', ['score', 'date', 'group', 'ground', 'opponent'])
def test_pinned_publication_rejects_coherent_resealed_business_edits(staging_v2, field):
    rows = _read(staging_v2 / 'matches.json')
    target = next(r for r in rows if r['source_match_number'] == 104)
    ident = target['match_id']
    if field == 'score':
        details = json.loads(target['score_details_json'])
        details['ft'] = [2, 2]
        details['et'] = [3, 2]
        target.update(score_a=2, score_b=2, score_details_json=json.dumps(details, sort_keys=True, separators=(',', ':')))
    elif field == 'date':
        target.update(source_date='2026-07-20', start_time='2026-07-20T19:00:00.000000Z')
    elif field == 'group':
        target['source_group'] = 'Reviewed-looking invented group'
    elif field == 'ground':
        target['ground'] = 'Reviewed-looking invented ground'
    else:
        target['team_a'] = 'Invented National Team'
        identity_bytes = json.dumps([target['team_namespace'], target['team_a']], ensure_ascii=False,
                                    sort_keys=True, separators=(',', ':')).encode('utf-8')
        target['team_a_id'] = 'openfootball:team:' + hashlib.sha256(identity_bytes).hexdigest()[:24]
    excluded = {'observed_at', 'source_sha256', 'source_url', 'license', 'license_url', 'revision', 'event_id'}
    content = {k: v for k, v in target.items() if k not in excluded}
    digest = hashlib.sha256(json.dumps(content, ensure_ascii=False, sort_keys=True,
                                      separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()
    target['event_id'] = target['match_id'] + ':1:' + digest[:16]
    history = [dict(target) if r['match_id'] == ident else r for r in _read(staging_v2 / 'history.json')]
    _rewrite_rows(staging_v2, 'history', history)
    _rewrite_rows(staging_v2, 'matches', rows)
    with pytest.raises(ResearchValidationError, match='business fields differ from bundled trusted source'):
        validate_staging(staging_v2)


def test_pinned_publication_requires_reviewed_aliases(staging_v2):
    sources = _read(staging_v2 / 'sources.json')
    next(s for s in sources if s['source_id'] == 'worldcup-2026')['team_aliases'] = {'Spain': 'Invented National Team'}
    _write(staging_v2 / 'sources.json', sources)
    _rehash(staging_v2)
    with pytest.raises(ResearchValidationError, match='aliases differ from reviewed catalogue'):
        validate_staging(staging_v2)


def test_worldcup_offline_fixture_and_packaged_trusted_bytes_are_identical():
    packaged = files('arena_lakehouse').joinpath('resources/worldcup_2026.json').read_bytes()
    offline = (Path(__file__).parent / 'fixtures/worldcup-2026-pinned.json').read_bytes()
    assert packaged == offline and hashlib.sha256(packaged).hexdigest() == WORLD_CUP_SHA256
