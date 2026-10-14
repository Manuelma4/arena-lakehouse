"""Optional, source-pinned season and venue inputs from retained free originals."""

import argparse
import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from .connectors import normalize_football
from .match_insights import selected_archives
from .provider_publication import PublicationError, archive_references, atomic_json, canonical, read_bytes, sha256, stamp
from .schema import MatchEvent
from .football_scores import score_details

LEGACY_SCHEMA = "arena-football-forecast-context-1.0"
SCHEMA = "arena-football-forecast-context-2.0"
MAX_BYTES = 16 * 1024 * 1024
MAX_RECORDS = 10_000


def resource(kind, value):
    return f"football-data:{kind}:{value}" if type(value) is int and 1 <= value <= 2**53 - 1 else None


def day(value):
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value).isoformat() if len(value) == 10 else None
    except ValueError:
        return None


def normalize(raw, source_sha256, observed_at, *, schema_version=SCHEMA):
    if schema_version not in (LEGACY_SCHEMA, SCHEMA):
        raise ValueError("Unsupported football context version")
    details = score_details(raw) if schema_version == SCHEMA else None
    match = normalize_football(raw, observed_at)
    if match is None:
        return None
    validated = MatchEvent.model_validate(dict(match, event_id="forecast-context", revision=1))
    competition = raw.get("competition") or {}
    season = raw.get("season") or {}
    competition_id = resource("competition", competition.get("id")) if isinstance(competition, dict) else None
    season_id = resource("season", season.get("id")) if isinstance(season, dict) else None
    start = day(season.get("startDate")) if isinstance(season, dict) else None
    end = day(season.get("endDate")) if isinstance(season, dict) else None
    if start and end and start > end:
        season_id, start, end = None, None, None
    result = dict(
        match_id=match["match_id"], source_sha256=source_sha256, observed_at=stamp(observed_at),
        competition=match["competition"], competition_id=competition_id,
        season_id=season_id, season_start=start, season_end=end,
        home_team_id=match["team_a_id"], away_team_id=match["team_b_id"],
        start_time=stamp(validated.start_time), status=validated.status,
        score_home=validated.score_a, score_away=validated.score_b,
    )
    if details is not None:
        result["score_details"] = details
    return result


def publish(root, selection, archives, *, started_at, generated_at, schema_version=SCHEMA):
    if schema_version not in (LEGACY_SCHEMA, SCHEMA):
        raise ValueError("Unsupported football context version")
    root = Path(root)
    if selection["provider"] != "football-data":
        raise PublicationError("Football context requires the football provider")
    refs, sources = archive_references(root, archives, selection["source_scope"],
        collected_at=selection["collected_at"], started_at=started_at)
    records, conflicts = {}, set()
    for ref, source in zip(refs, sources, strict=True):
        for raw in source["payload"]["matches"]:
            try:
                record = normalize(raw, ref["sha256"], datetime.fromisoformat(source["observed_at"].replace("Z", "+00:00")),
                                   schema_version=schema_version)
            except (ValueError, KeyError, TypeError, AttributeError):
                continue
            if record is None:
                continue
            key = record["match_id"]
            if key in records:
                old = {k: v for k, v in records[key].items() if k not in ("source_sha256", "observed_at")}
                current = {k: v for k, v in record.items() if k not in ("source_sha256", "observed_at")}
                if old != current:
                    conflicts.add(key)
            else:
                records[key] = record
            if len(records) > MAX_RECORDS:
                raise PublicationError("Football context record bound exceeded")
    package = dict(
        schema_version=schema_version, provider="football-data", publication_fingerprint=selection["fingerprint"],
        collected_at=selection["collected_at"], generated_at=stamp(generated_at),
        sources=[dict(sha256=ref["sha256"], endpoint=source["endpoint"], observed_at=stamp(source["observed_at"]))
            for ref, source in zip(refs, sources, strict=True)],
        records=[records[key] for key in sorted(records) if key not in conflicts], conflicting_records_omitted=len(conflicts),
    )
    body = canonical(package) + b"\n"
    if len(body) > MAX_BYTES:
        raise PublicationError("Football context byte bound exceeded")
    checksum = sha256(body)
    basename = f"context-{checksum}.json"
    path = root / "forecast-context" / basename
    if path.exists():
        if sha256(read_bytes(path, MAX_BYTES)) != checksum:
            raise PublicationError("Football context checksum mismatch")
    else:
        atomic_json(path, package)
    atomic_json(root / "snapshots/CURRENT_FORECAST_CONTEXT.json", dict(
        schema_version=schema_version, provider="football-data", publication_fingerprint=selection["fingerprint"],
        basename=basename, sha256=checksum))
    return dict(schema_version=schema_version, provider="football-data", records=len(package["records"]),
        known_season_records=sum(bool(row["competition_id"] and row["season_id"]) for row in package["records"]),
        conflicting_records_omitted=len(conflicts), sha256=checksum, provider_requests=0)


def build_selected(root, *, now=None, schema_version=SCHEMA):
    now = now or datetime.now(timezone.utc)
    selection, paths, started = selected_archives(root, now=now)
    return publish(root, selection, paths, started_at=started, generated_at=now, schema_version=schema_version)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--schema-version", choices=(LEGACY_SCHEMA, SCHEMA), default=SCHEMA,
                        help="New builds use v2; v1 is available for explicit legacy reproduction")
    args = parser.parse_args()
    try:
        print(json.dumps(build_selected(args.workspace / "football-data", schema_version=args.schema_version), indent=2))
    except (PublicationError, OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        parser.exit(1, "Football context unavailable; selected originals failed verification.\n")


if __name__ == "__main__":
    main()
