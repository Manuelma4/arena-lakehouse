"""Validate and publish precision-aware OpenFootball research, never snapshot 2.0."""

import argparse
import hashlib
import json
import os
import re
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from importlib.resources import files as resource_files
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .openfootball import (
    LICENSE_URL, STAGING_VERSION, STAGING_VERSION_V2, PINNED_NATIONAL_SOURCES, Source, _name,
    _PARQUET_FIELDS, _PARQUET_FIELDS_V2, load_sources, parquet_schema, parse_payload,
)


RESEARCH_VERSION = "openfootball-research-1.0"
RESEARCH_VERSION_V2 = "openfootball-research-2.0"
STAGING_FILES = {"matches.json", "history.json", "matches.parquet", "history.parquet",
                 "sources.json", "quarantine.json", "polls.json"}
RESEARCH_FILES = {"gold/matches.parquet", "gold/match_history.parquet", "matches.json", "history.json",
                  "sources.json", "competitions.json", "quality.json", "lineage.json", "quarantine.json", "polls.json"}
BOUNDS = {"matches": 25_000, "revisions": 50_000, "quarantine_rows": 100_000,
          "sources": 9, "polls": 100_000}
MAX_FILE_BYTES = 128_000_000
MAX_PACKAGE_BYTES = 512_000_000
PRECISION_NOTE = "Source calendar dates and optional local wall times; no inferred UTC kickoff or completion"
PRECISION_NOTE_V2 = ("Source calendar dates and local wall times; UTC kickoff derived only from an explicit "
                     "numeric offset; completion remains unknown")
COVERAGE_NOTE = "Observed open-data coverage; incomplete or stale source results remain unknown; not official standings"


class ResearchValidationError(ValueError):
    """Invalid hashes, source scope or semantic staging/research evidence."""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value):
    return hashlib.sha256(value if isinstance(value, bytes) else _json(value).encode("utf-8")).hexdigest()


def _require(condition, message):
    if not condition:
        raise ResearchValidationError(message)


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "Duplicate JSON key")
        result[key] = value
    return result


def _decode(body):
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite JSON")))
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ResearchValidationError("Invalid UTF-8/JSON payload") from error


def _timestamp(value, label):
    _require(isinstance(value, str) and len(value) <= 40, f"Invalid {label} timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ResearchValidationError(f"Invalid {label} timestamp") from error
    _require(parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0,
             f"{label} must be an aware UTC instant")
    return parsed.astimezone(timezone.utc)


def _utc(value):
    _require(isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None,
             "generated_at must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _integer(value, label, low=0, high=None):
    _require(type(value) is int and value >= low and (high is None or value <= high), f"Invalid {label}")


def _schema(edition=False, staging_version=STAGING_VERSION):
    return parquet_schema(staging_version, edition=edition)


@dataclass
class ValidatedStaging:
    manifest: dict
    upstream_staging_fingerprint: str
    matches: list[dict]
    history: list[dict]
    sources: list[dict]
    quarantine: list[dict]
    polls: list[dict]
    as_of: str


def _trusted_national_rows(sources, staging_version):
    trusted = {}
    for source in sources.values():
        pinned = PINNED_NATIONAL_SOURCES.get(source.repository)
        if pinned is None:
            continue
        body = resource_files("arena_lakehouse").joinpath("resources", pinned[3]).read_bytes()
        _require(_sha(body) == pinned[2], "Bundled trusted national source checksum differs")
        parsed = parse_payload(body, source, datetime(1970, 1, 1, tzinfo=timezone.utc), staging_version=staging_version)
        _require(not parsed.quarantine and len(parsed.rows) == pinned[4], "Trusted national parser coverage differs")
        trusted.update({row["match_id"]: row for row in parsed.rows})
    return trusted


def _validate_row(row, sources, staging_version=STAGING_VERSION, trusted_national=None):
    v2 = staging_version == STAGING_VERSION_V2
    fields = _PARQUET_FIELDS_V2 if v2 else _PARQUET_FIELDS
    _require(isinstance(row, dict) and set(row) == set(fields), "Staging row fields differ from contract")
    _require(isinstance(row["source_id"], str), "Invalid row source_id type")
    source = sources.get(row["source_id"])
    _require(source is not None, "Match references an unknown source")
    for field in fields:
        if field not in {"occurrence", "score_a", "score_b", "revision", "quality_score", "prediction_eligible",
                          "timezone", "start_time", "completed_at", "local_time", "source_match_number",
                          "utc_offset_minutes", "source_group", "ground"}:
            _require(isinstance(row[field], str) and 0 < len(row[field]) <= 2000, f"Invalid row field {field}")
    _require(row["staging_version"] == staging_version and row["provider"] == "openfootball"
             and row["sport"] == "football", "Invalid staging row provider/version")
    _require(row["license"] == "CC0-1.0" and
             row["license_url"] == (source.license_url if v2 else LICENSE_URL), "Row license differs from CC0")
    _require(row["competition_id"] == source.competition_id and row["competition"] == source.name
             and row["season"] == source.season and row["team_namespace"] == source.team_namespace,
             "Row scope differs from source catalogue")
    _require(row["source_ref"] == source.source_ref and row["source_url"] == source.url,
             "Row URL/ref is outside the fixed source allowlist")
    _require(re.fullmatch(r"[0-9a-f]{64}", row["source_sha256"]) is not None, "Invalid raw source checksum")
    pinned = PINNED_NATIONAL_SOURCES.get(source.repository)
    _require(pinned is None or row["source_sha256"] == pinned[2],
             "Pinned national row checksum differs from reviewed source")
    _require(row["timezone"] is row["completed_at"] is None,
             "Staging must not infer UTC timezone/completion")
    _require(re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["source_date"]) is not None, "Invalid source calendar date")
    try:
        date.fromisoformat(row["source_date"])
    except ValueError as error:
        raise ResearchValidationError("Invalid source calendar date") from error
    local = row["local_time"]
    _require(local is None or isinstance(local, str) and
             re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", local) is not None, "Invalid unzoned local clock")
    offset = row["utc_offset_minutes"] if v2 else None
    if offset is not None:
        _integer(offset, "UTC offset minutes", -840, 840)
        _require(local is not None and source.repository in {"worldcup.json", "copa-america"}, "Offset lacks a supported source clock")
        expected_start = _utc(datetime.fromisoformat(row["source_date"] + "T" + local).replace(
            tzinfo=timezone(timedelta(minutes=offset))))
        _require(row["start_time"] == expected_start, "UTC kickoff differs from explicit source offset")
    else:
        _require(row["start_time"] is None, "Staging must not infer UTC kickoff")
    _require(row["kickoff_precision"] == ("offset_minute" if offset is not None else
                                          "local_minute" if local is not None else "date_only"),
             "Kickoff precision does not match source clock")
    _require(row["prediction_eligible"] is False, "Staging cannot assert prediction eligibility")
    _require(type(row["quality_score"]) in (int, float) and row["quality_score"] == 1.0,
             "Invalid staging schema quality score")
    _require(row["team_identity_status"] in {"unresolved_name_derived_not_official", "derived_reviewable_not_official"},
             "Staging cannot claim official team identity")
    number = row["source_match_number"] if v2 else None
    if v2:
        _require(row["team_type"] == source.team_type, "Team type differs from reviewed source")
        for field in ("source_group", "ground"):
            if row[field] is not None:
                _require(isinstance(row[field], str) and 0 < len(row[field]) <= 200,
                         f"Invalid optional source {field}")
                try:
                    normalized = _name(row[field], field)
                except ValueError as error:
                    raise ResearchValidationError(f"Invalid source {field}") from error
                _require(normalized == row[field], f"Invalid normalized source {field}")
        if number is not None:
            _integer(number, "source match number", 1, 10_000)
    _require(row["identity_basis"] == ("competition-season-source-match-number" if number is not None
                                       else "competition-season-round-ordered-teams-occurrence")
             and row["availability_basis"] == "first_collected_revision", "Unsupported identity/availability basis")
    aliases = source.team_aliases or {}
    for side in ("a", "b"):
        name = row[f"team_{side}"]
        _require(name.strip() == name and 0 < len(name) <= 200, "Invalid team name")
        expected = "openfootball:team:" + _sha([source.team_namespace, _name(aliases.get(name, name), "alias")])[:24]
        _require(row[f"team_{side}_id"] == expected, "Team identity does not match source namespace/name mapping")
    _require(row["team_a_id"] != row["team_b_id"], "Match opponents share one identity")
    _integer(row["occurrence"], "occurrence", 1, 100)
    _require(row["round"].strip() == row["round"] and len(row["round"]) <= 200, "Invalid round")
    ident = [source.competition_id, source.season, row["round"], row["team_a_id"], row["team_b_id"], row["occurrence"]]
    if number is not None:
        ident = [source.competition_id, source.season, number]
    _require(row["match_id"] == "openfootball:match:" + _sha(ident)[:24], "Match identity differs from source scope")
    details = _decode(row["score_details_json"].encode("utf-8"))
    _require(isinstance(details, dict) and not set(details) - {"ft", "ht", "et", "p"}, "Unsupported source score fields")
    for pair in details.values():
        _require(isinstance(pair, list) and len(pair) == 2, "Source score must be a pair")
        for score in pair:
            _integer(score, "source score", 0, 50)
    ft = details.get("ft")
    _require(row["score_a"] == (ft[0] if ft else None) and row["score_b"] == (ft[1] if ft else None),
             "Score fields differ from preserved source ft")
    for field in ("score_a", "score_b"):
        if row[field] is not None:
            _integer(row[field], field, 0, 50)
    _require(row["result_status"] == ("observed_result" if ft else "unknown_result") and
             row["score_basis"] == ("source_ft_unverified_regulation" if ft else "unknown"),
             "Result status/semantics do not match source scores")
    _integer(row["revision"], "revision", 1, BOUNDS["revisions"])
    observed = _timestamp(row["observed_at"], "match observation")
    excluded = {"observed_at", "source_sha256", "source_url", "license", "license_url", "revision", "event_id"}
    content_hash = _sha({k: v for k, v in row.items() if k not in excluded})
    _require(row["event_id"] == f"{row['match_id']}:{row['revision']}:{content_hash[:16]}", "Invalid event content identity")
    if source.repository in PINNED_NATIONAL_SOURCES:
        trusted = (trusted_national or {}).get(row["match_id"])
        _require(trusted is not None and
                 {k: v for k, v in row.items() if k not in {"observed_at", "revision", "event_id"}} ==
                 {k: v for k, v in trusted.items() if k != "observed_at"},
                 "Pinned national business fields differ from bundled trusted source")
    return observed, content_hash


def validate_staging(staging: Path) -> ValidatedStaging:
    """Verify all file hashes and semantic evidence before any publication writes."""
    staging = Path(staging)
    _require(staging.is_dir() and not staging.is_symlink(), "Staging directory is missing or a symlink")
    entries = list(staging.iterdir())
    _require({p.name for p in entries} == STAGING_FILES | {"manifest.json"}, "Unexpected/missing staging files")
    _require(all(p.is_file() and not p.is_symlink() for p in entries), "Staging files must be regular, without symlinks")
    _require(all(p.stat().st_size <= MAX_FILE_BYTES for p in entries)
             and sum(p.stat().st_size for p in entries) <= MAX_PACKAGE_BYTES, "Staging package exceeds byte bounds")
    manifest_bytes = (staging / "manifest.json").read_bytes()
    manifest = _decode(manifest_bytes)
    _require(isinstance(manifest, dict), "Staging manifest must be an object")
    staging_version = manifest.get("staging_version")
    _require(isinstance(staging_version, str) and staging_version in {STAGING_VERSION, STAGING_VERSION_V2}
             and manifest.get("data_kind") == "open_data"
             and manifest.get("provider") == "openfootball" and manifest.get("canonical_compatible") is False,
             "Unsupported staging manifest/version/provider")
    _require(manifest.get("license") == "CC0-1.0" and manifest.get("license_url") == LICENSE_URL,
             "Staging license differs from CC0")
    _timestamp(manifest.get("generated_at"), "staging generation")
    files = manifest.get("files")
    _require(isinstance(files, dict) and set(files) == STAGING_FILES, "Staging hash inventory differs from contract")
    verified_bytes = {}
    for name, checksum in files.items():
        _require(isinstance(checksum, str) and re.fullmatch(r"[0-9a-f]{64}", checksum) is not None,
                 "Invalid staging checksum declaration")
        body = (staging / name).read_bytes()
        _require(len(body) <= MAX_FILE_BYTES, "Staging file changed beyond byte bounds")
        _require(_sha(body) == checksum, f"Staging checksum mismatch: {name}")
        verified_bytes[name] = body
    _require(sum(len(body) for body in verified_bytes.values()) <= MAX_PACKAGE_BYTES, "Staging byte bounds changed")
    values = {name: _decode(verified_bytes[f"{name}.json"]) for name in
              ("matches", "history", "sources", "quarantine", "polls")}
    bounds = dict(BOUNDS, sources=12) if staging_version == STAGING_VERSION_V2 else BOUNDS
    for name, rows in values.items():
        _require(isinstance(rows, list), f"{name} must be an array")
        bound = bounds[{"history": "revisions", "quarantine": "quarantine_rows"}.get(name, name)]
        _require(len(rows) <= bound, f"{name} exceeds row bounds")
    for count, name in (("matches", "matches"), ("revisions", "history"), ("sources", "sources"),
                        ("quarantine_rows", "quarantine")):
        _integer(manifest.get(count), count, 0, bounds[count])
        _require(manifest[count] == len(values[name]), f"Manifest {count} count differs from actual rows")
    for name in ("matches", "history"):
        parquet = pq.ParquetFile(pa.BufferReader(verified_bytes[f"{name}.parquet"]))
        _require(parquet.metadata.num_rows == len(values[name]) and
                 parquet.schema_arrow.equals(_schema(staging_version=staging_version)),
                 f"{name} Parquet schema/count differs from contract")
        _require(parquet.read().to_pylist() == values[name], f"{name} JSON/Parquet parity failed")
    catalogue = load_sources(resource_files("arena_lakehouse").joinpath("resources/openfootball_sources.json"))
    sources, scopes = {}, set()
    for raw in values["sources"]:
        _require(isinstance(raw, dict), "Source catalogue row must be an object")
        try:
            source = Source(**raw)
        except (TypeError, ValueError) as error:
            raise ResearchValidationError("Invalid source catalogue row") from error
        expected = catalogue.get(source.source_id)
        _require(expected is not None and all(getattr(source, k) == getattr(expected, k) for k in
                 ("competition_id", "season", "path", "name", "region", "team_namespace", "repository", "team_type")),
                 "Source is outside the fixed OpenFootball catalogue allowlist")
        _require(staging_version != STAGING_VERSION or source.repository == "football.json",
                 "V1 cannot contain national tournament repositories")
        if staging_version == STAGING_VERSION_V2:
            _require("repository" in raw and "team_type" in raw, "V2 source catalogue lacks explicit repository/team type")
        if source.repository in PINNED_NATIONAL_SOURCES:
            _require(source.source_ref == expected.source_ref and source.team_aliases == expected.team_aliases,
                     "Pinned national source ref/aliases differ from reviewed catalogue")
        scope = (source.competition_id, source.season)
        _require(source.source_id not in sources and scope not in scopes, "Duplicate source/scope")
        sources[source.source_id] = source
        scopes.add(scope)
    _require(bool(sources) and bool(values["polls"]), "Research requires a source and a successful observed poll")
    trusted_national = _trusted_national_rows(sources, staging_version)
    poll_ids, observations, poll_sources, changes, times = set(), Counter(), set(), 0, []
    quarantine_polls, quarantine_counts, known_archives, last_observed = set(), Counter(), set(), {}
    _require(all(isinstance(p, dict) and type(p.get("poll_id")) is int for p in values["polls"]), "Invalid poll IDs")
    for poll in sorted(values["polls"], key=lambda row: row["poll_id"]):
        _require(isinstance(poll, dict) and set(poll) ==
                 {"poll_id", "source_id", "observed_at", "status", "sha256", "changed", "quarantined"},
                 "Invalid poll row fields")
        _integer(poll["poll_id"], "poll_id", 1)
        _require(isinstance(poll["source_id"], str) and poll["source_id"] in sources,
                 "Invalid poll source")
        _require(poll["poll_id"] not in poll_ids, "Duplicate poll ID")
        poll_ids.add(poll["poll_id"])
        _require(isinstance(poll["status"], str) and poll["status"] in {"collected", "not_modified"},
                 "Invalid successful poll status")
        _require(isinstance(poll["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", poll["sha256"]) is not None,
                 "Invalid poll source checksum")
        pinned = PINNED_NATIONAL_SOURCES.get(sources[poll["source_id"]].repository)
        _require(pinned is None or poll["sha256"] == pinned[2],
                 "Pinned national poll checksum differs from reviewed source")
        instant = _timestamp(poll["observed_at"], "poll observation")
        _require(poll["source_id"] not in last_observed or last_observed[poll["source_id"]] <= instant,
                 "Source poll availability moves backwards")
        last_observed[poll["source_id"]] = instant
        _integer(poll["changed"], "poll changed", 0, 10_000)
        _integer(poll["quarantined"], "poll quarantined", 0, 10_000)
        _require(poll["status"] != "not_modified" or poll["changed"] == poll["quarantined"] == 0,
                 "304 audit must not fabricate changes/quarantine")
        archive_key = (poll["source_id"], poll["sha256"])
        _require(poll["status"] != "not_modified" or archive_key in known_archives,
                 "304 poll lacks a previously collected body checksum")
        if poll["status"] == "collected":
            known_archives.add(archive_key)
        if poll["quarantined"]:
            quarantine_polls.add((poll["source_id"], poll["observed_at"], poll["sha256"]))
            quarantine_counts[(poll["source_id"], poll["observed_at"], poll["sha256"])] += poll["quarantined"]
        observations[(poll["source_id"], poll["observed_at"], poll["sha256"])] += poll["changed"]
        poll_sources.add(poll["source_id"])
        changes += poll["changed"]
        times.append(instant)
    _require(poll_sources == set(sources), "Source lacks a successful poll audit")
    _require(changes == len(values["history"]), "Poll changes differ from revision history")
    as_of = max(times)
    history, event_ids, actual_observations = defaultdict(list), set(), Counter()
    for row in values["history"]:
        observed, content_hash = _validate_row(row, sources, staging_version, trusted_national)
        _require(observed <= as_of and row["event_id"] not in event_ids, "Duplicate event or future observation")
        event_ids.add(row["event_id"])
        history[row["match_id"]].append((row, observed, content_hash))
        actual_observations[(row["source_id"], row["observed_at"], row["source_sha256"])] += 1
    _require(actual_observations == +observations, "Revision availability has no matching source poll evidence")
    latest = {}
    for ident, records in history.items():
        records.sort(key=lambda item: item[0]["revision"])
        _require([r[0]["revision"] for r in records] == list(range(1, len(records) + 1)), "Noncontiguous revisions")
        for previous, current in zip(records, records[1:]):
            _require(previous[1] <= current[1] and previous[2] != current[2],
                     "Revision observations move backwards or duplicate business content")
        latest[ident] = records[-1][0]
    current = {}
    for row in values["matches"]:
        _validate_row(row, sources, staging_version, trusted_national)
        _require(row["match_id"] not in current, "Duplicate current match ID")
        current[row["match_id"]] = row
    _require(current == latest, "Current rows differ from latest revision history")
    _require({ident for ident, row in current.items() if sources[row["source_id"]].repository in PINNED_NATIONAL_SOURCES}
             == set(trusted_national), "Pinned national coverage differs from bundled trusted source")
    quarantine_ids, quarantine_observations = set(), Counter()
    for problem in values["quarantine"]:
        _require(isinstance(problem, dict) and set(problem) ==
                 {"source_id", "sha256", "row_index", "reason", "observed_at", "raw_json"},
                 "Invalid quarantine row fields")
        _require(isinstance(problem["source_id"], str) and problem["source_id"] in sources,
                 "Quarantine references invalid/unknown source")
        _integer(problem["row_index"], "quarantine index", -1, 9999)
        _require(isinstance(problem["reason"], str) and 0 < len(problem["reason"]) <= 2000,
                 "Invalid quarantine reason")
        _require(isinstance(problem["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", problem["sha256"]) is not None,
                 "Invalid quarantine source checksum")
        instant = _timestamp(problem["observed_at"], "quarantine observation")
        _require(instant <= as_of and isinstance(problem["raw_json"], str), "Invalid quarantine observation/raw JSON")
        _decode(problem["raw_json"].encode("utf-8"))
        ident = (problem["source_id"], problem["sha256"], problem["row_index"], problem["reason"])
        _require(ident not in quarantine_ids, "Duplicate quarantine evidence")
        quarantine_ids.add(ident)
        _require((problem["source_id"], problem["observed_at"], problem["sha256"]) in quarantine_polls,
                 "Quarantine lacks source poll evidence")
        quarantine_observations[(problem["source_id"], problem["observed_at"], problem["sha256"])] += 1
    _require(all(count <= quarantine_counts[key] for key, count in quarantine_observations.items()),
             "Quarantine rows exceed their measured source poll count")
    return ValidatedStaging(manifest, _sha(manifest_bytes), values["matches"], values["history"],
                            values["sources"], values["quarantine"], values["polls"], _utc(as_of))


def _competitions(matches, sources, staging_version=STAGING_VERSION):
    groups = defaultdict(list)
    for row in matches:
        groups[row["edition_id"]].append(row)
    result = []
    for source in sorted(sources, key=lambda row: row["source_id"]):
        rows = groups[source["source_id"]]
        participants = {}
        for row in sorted(rows, key=lambda row: (_timestamp(row["observed_at"], "match"), row["match_id"])):
            for side in ("a", "b"):
                participants[row[f"team_{side}_id"]] = row[f"team_{side}"]
        result.append(dict(edition_id=source["source_id"], competition_id=source["competition_id"],
            name=source["name"], season=source["season"], region=source["region"],
            coverage_start=min((r["source_date"] for r in rows), default=None),
            coverage_end=max((r["source_date"] for r in rows), default=None), match_count=len(rows),
            observed_results=sum(r["result_status"] == "observed_result" for r in rows),
            unknown_results=sum(r["result_status"] == "unknown_result" for r in rows),
            date_only=sum(r["kickoff_precision"] == "date_only" for r in rows),
            local_minute=sum(r["kickoff_precision"] == "local_minute" for r in rows),
            participants=[dict(team_id=ident, team_name=participants[ident]) for ident in sorted(participants)]))
        if staging_version == STAGING_VERSION_V2:
            result[-1].update(team_type=source["team_type"],
                             offset_minute=sum(r["kickoff_precision"] == "offset_minute" for r in rows))
    return result


def publish_research(staging: Path, destination: Path, *, generated_at=None) -> dict:
    """Publish a new checksum-covered research package after complete validation."""
    data = validate_staging(staging)
    staging_version = data.manifest["staging_version"]
    v2 = staging_version == STAGING_VERSION_V2
    precision_note = PRECISION_NOTE_V2 if v2 else PRECISION_NOTE
    generated = _utc(generated_at or datetime.now(timezone.utc))
    _require(_timestamp(generated, "research generation") >= _timestamp(data.as_of, "as_of"),
             "Research publication cannot predate observed source evidence")
    destination = Path(destination)
    _require(not destination.exists() and not destination.is_symlink(), "Research destination already exists")
    _require(destination.resolve() != Path(staging).resolve(), "Research cannot overwrite staging input")
    matches = [dict(row, edition_id=row["source_id"]) for row in sorted(data.matches, key=lambda row: row["match_id"])]
    history = [dict(row, edition_id=row["source_id"]) for row in
               sorted(data.history, key=lambda row: (row["match_id"], row["revision"]))]
    sources = sorted(data.sources, key=lambda row: row["source_id"])
    polls = sorted(data.polls, key=lambda row: row["poll_id"])
    quarantine = sorted(data.quarantine, key=lambda row: (row["source_id"], row["sha256"], row["row_index"], row["reason"]))
    competitions = _competitions(matches, sources, staging_version)
    teams = {row[f"team_{side}_id"] for row in matches for side in ("a", "b")}
    quality = dict(match_count=len(matches), revision_count=len(history), source_count=len(sources),
        observed_results=sum(row["result_status"] == "observed_result" for row in matches),
        unknown_results=sum(row["result_status"] == "unknown_result" for row in matches),
        date_only=sum(row["kickoff_precision"] == "date_only" for row in matches),
        local_minute=sum(row["kickoff_precision"] == "local_minute" for row in matches),
        unresolved_timezones=len(matches), unresolved_team_identities=len(teams), prediction_ineligible=len(matches),
        quarantine_rows=len(quarantine), scope="Observed current staging rows; not official standings",
        notes=[precision_note, COVERAGE_NOTE, "Name-derived team identities remain unresolved and are not official IDs",
               "Preserved ft/et/p scores do not establish a 90-minute prediction target",
               "Historical availability begins at actual revision collection, never at historical kickoff"])
    if v2:
        quality.update(offset_minute=sum(row["kickoff_precision"] == "offset_minute" for row in matches),
                       unresolved_timezones=sum(row["utc_offset_minutes"] is None for row in matches))
    lineage = [dict(source="OpenFootball CC0 raw JSON", target="staging raw archives and source poll audit",
                    description="Fixed repository source URLs, body checksums and actual UTC collection evidence"),
               dict(source="staging current/history JSON + Parquet", target="gold/matches.parquet",
                    description="Hash/schema/parity verified; all source fields preserved; edition_id=source_id"),
               dict(source="staging revision history", target="gold/match_history.parquet",
                    description="Contiguous source revisions and poll availability; no UTC kickoff inferred"),
               dict(source="gold/matches.parquet", target="competitions.json and quality.json",
                    description="Observed calendar coverage, participants, result availability and unresolved precision")]
    if v2:
        lineage[2]["description"] = ("Contiguous source revisions and poll availability; UTC kickoff only from "
                                     "explicit numeric offset; completion unknown")
    payloads = dict(matches=matches, history=history, sources=sources, competitions=competitions,
                    quality=quality, lineage=lineage, quarantine=quarantine, polls=polls)
    # Logical identity excludes both staging/research generation clocks and Parquet encoding metadata.
    dataset_version = _sha(dict(as_of=data.as_of, **payloads))[:16]
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp-" + uuid.uuid4().hex)
    temporary.mkdir()
    (temporary / "gold").mkdir()
    try:
        for name, payload in payloads.items():
            (temporary / f"{name}.json").write_text(_json(payload) + "\n", encoding="utf-8")
        pq.write_table(pa.Table.from_pylist(matches, schema=_schema(True, staging_version)), temporary / "gold/matches.parquet",
                       compression="zstd")
        pq.write_table(pa.Table.from_pylist(history, schema=_schema(True, staging_version)), temporary / "gold/match_history.parquet",
                       compression="zstd")
        files = {name: _sha((temporary / name).read_bytes()) for name in sorted(RESEARCH_FILES)}
        manifest = dict(research_version=RESEARCH_VERSION_V2 if v2 else RESEARCH_VERSION,
            data_kind="open_data", provider="openfootball",
            license="CC0-1.0", canonical_compatible=False, dataset_version=dataset_version,
            generated_at=generated, as_of=data.as_of, matches=len(matches), revisions=len(history),
            quarantine_rows=len(quarantine), sources=len(sources), files=files,
            upstream_staging_fingerprint=data.upstream_staging_fingerprint,
            precision_note=precision_note, coverage_note=COVERAGE_NOTE)
        (temporary / "manifest.json").write_text(_json(manifest) + "\n", encoding="utf-8")
        _require(not destination.exists(), "Research destination appeared during publication")
        os.rename(temporary, destination)
        return manifest
    finally:
        if temporary.exists():
            # Only explicitly named files we created; no recursive delete or input mutation.
            for name in RESEARCH_FILES | {"manifest.json"}:
                (temporary / name).unlink(missing_ok=True)
            (temporary / "gold").rmdir()
            temporary.rmdir()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = publish_research(args.staging, args.output)
    except (ResearchValidationError, OSError) as error:
        parser.error(str(error))
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
