"""Bounded private PandaScore calendar publications from one successful collection.

The canonical match contract deliberately excludes unresolved announcements. This
separate contract preserves them without inventing teams, dates, or series scores.
No provider calls or filesystem archive discovery happen in this module.
"""

import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Annotated, Iterable, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

SCHEMA_VERSION = "arena-provider-calendar-1.0"
PHASES = ("past", "upcoming", "running")
FILES = ("announcements.json", "occurrences.json", "sources.json", "quality.json")
MAX_ARCHIVE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_FILE_BYTES = 32 * 1024 * 1024
MAX_RECORDS = 3000
ID = Annotated[int, Field(ge=1, le=2**53 - 1)]
Text = Annotated[str, Field(min_length=1, max_length=1000)]
Stamp = Annotated[str, Field(min_length=1, max_length=100)]
MatchID = Annotated[str, Field(pattern=r"^pandascore:[1-9][0-9]{0,15}$")]
SourceID = Annotated[str, Field(pattern=r"^pandascore:valorant:(past|upcoming|running):page:([1-9]|10)$")]
OccurrenceID = Annotated[str, Field(pattern=r"^pandascore:valorant:(past|upcoming|running):page:([1-9]|10):row:[0-9]{3}$")]
Hex64 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PENDING = ("unresolved_opponents", "unresolved_team_names", "unsupported_opponent_type",
           "unresolved_start_time", "start_time_timezone_unknown", "unknown_status",
           "result_unavailable", "conflicting_phase_records")


class CalendarError(ValueError):
    """A bounded contract failure; messages never contain raw provider values."""


def _utc(value: str) -> str:
    if not isinstance(value, str) or len(value) > 100:
        raise ValueError("Expected an aware timestamp")
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("Expected an aware timestamp")
    return instant.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _source_date(value: str | None) -> str | None:
    if value is None:
        return None
    # Preserve calendar dates and unzoned clocks exactly; never infer UTC.
    if not isinstance(value, str) or len(value) > 100:
        raise ValueError("Invalid source date")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        date.fromisoformat(value)
    else:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SourceDates(Contract):
    begin_at: Stamp | None
    scheduled_at: Stamp | None
    original_scheduled_at: Stamp | None
    end_at: Stamp | None
    modified_at: Stamp | None

    _dates = field_validator("*", mode="after")(_source_date)


class Team(Contract):
    team_id: MatchID | None
    source_team_id: ID | None
    team_name: Text | None


class EventMetadata(Contract):
    id: Annotated[str, Field(pattern=r"^pandascore:(league|series|tournament):[1-9][0-9]{0,15}$")] | None
    source_id: ID | None
    name: Text | None
    full_name: Text | None
    begin_at: Stamp | None
    end_at: Stamp | None
    modified_at: Stamp | None
    season: Text | None
    year: Annotated[int, Field(ge=1900, le=2200)] | None

    _dates = field_validator("begin_at", "end_at", "modified_at", mode="after")(_source_date)


class Result(Contract):
    team_id: MatchID | None
    source_team_id: ID | None
    score: Annotated[int, Field(ge=0, le=1000)] | None


class Business(Contract):
    announcement_id: MatchID
    provider: Literal["pandascore"] = "pandascore"
    sport: Literal["valorant"] = "valorant"
    name: Text | None
    source_status: Annotated[str, Field(min_length=1, max_length=40)]
    start_time: Stamp | None
    source_dates: SourceDates
    team_a: Team | None
    team_b: Team | None
    league: EventMetadata | None
    series: EventMetadata | None
    tournament: EventMetadata | None
    source_number_of_games: Annotated[int, Field(ge=1, le=101)] | None
    source_results: Annotated[list[Result], Field(max_length=8)]
    result_known: bool
    pending_reasons: Annotated[list[str], Field(max_length=len(PENDING))]

    @field_validator("start_time")
    @classmethod
    def start_utc(cls, value):
        return None if value is None else _utc(value)

    @field_validator("pending_reasons")
    @classmethod
    def pending(cls, values):
        if values != sorted(set(values)) or any(value not in PENDING for value in values):
            raise ValueError("Invalid pending reasons")
        return values


class Occurrence(Business):
    occurrence_id: OccurrenceID
    source_id: SourceID
    observed_at: Stamp

    _observation = field_validator("observed_at")(_utc)


class Announcement(Business):
    canonical_match_id: MatchID | None
    observed_at: Stamp
    first_observed_at: Stamp
    last_observed_at: Stamp
    source_ids: Annotated[list[SourceID], Field(min_length=1, max_length=30)]
    occurrence_ids: Annotated[list[OccurrenceID], Field(min_length=1, max_length=MAX_RECORDS)]
    conflicted: bool

    _observation = field_validator("observed_at", "first_observed_at", "last_observed_at")(_utc)


class Source(Contract):
    source_id: SourceID
    provider: Literal["pandascore"] = "pandascore"
    phase: Literal["past", "upcoming", "running"]
    page: Annotated[int, Field(ge=1, le=10)]
    per_page: Literal[100] = 100
    endpoint: Annotated[str, Field(max_length=100)]
    requested_at: Stamp
    observed_at: Stamp
    archive_sha256: Hex64
    record_count: Annotated[int, Field(ge=0, le=100)]

    _times = field_validator("requested_at", "observed_at")(_utc)


class Quality(Contract):
    source_records: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    announcements: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    duplicate_occurrences: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    conflicted_announcements: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    pending_announcements: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    unresolved_opponents: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    unresolved_start_time: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    results_known: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    canonical_linked: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    source_phases: list[Literal["past", "upcoming", "running"]]
    pages_per_phase: Annotated[int, Field(ge=1, le=10)]
    coverage_note: str
    result_note: str


class SourceScope(Contract):
    kind: Literal["match_phases"] = "match_phases"
    competition_codes: Annotated[list[str], Field(max_length=0)]
    phases: Annotated[list[Literal["past", "upcoming", "running"]], Field(min_length=3, max_length=3)]
    pages: Annotated[int, Field(ge=1, le=10)]


class Counts(Contract):
    source_records: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    announcements: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    duplicate_occurrences: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    conflicted_announcements: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    pending_announcements: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    canonical_linked: Annotated[int, Field(ge=0, le=MAX_RECORDS)]
    sources: Annotated[int, Field(ge=3, le=30)]


class Manifest(Contract):
    schema_version: Literal["arena-provider-calendar-1.0"] = SCHEMA_VERSION
    provider: Literal["pandascore"] = "pandascore"
    data_kind: Literal["api"] = "api"
    dataset_version: Annotated[str, Field(pattern=r"^[0-9a-f]{16}$")]
    generated_at: Stamp
    as_of: Stamp
    source_scope: SourceScope
    counts: Counts
    files: dict[str, Hex64]
    publication_note: str

    _times = field_validator("generated_at", "as_of")(_utc)


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _hash(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _no_duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise CalendarError("Duplicate JSON keys")
        value[key] = item
    return value


def _json(body: bytes):
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_no_duplicates,
                          parse_constant=lambda _: (_ for _ in ()).throw(CalendarError("Non-finite JSON")))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise CalendarError("Malformed calendar JSON") from None


def _checked(model, value):
    try:
        return model.model_validate(value).model_dump()
    except (ValidationError, ValueError, TypeError):
        raise CalendarError("Invalid typed calendar record") from None


def _no_symlink(path: Path):
    for parent in (path, *path.parents):
        try:
            attributes = parent.lstat()
        except FileNotFoundError:
            continue
        # Reparse points include junctions, including on supported Python 3.11.
        if (parent.is_symlink() or getattr(attributes, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
            raise CalendarError("Calendar paths must not contain links")


def _bounded_file(path: Path, limit: int) -> bytes:
    _no_symlink(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise CalendarError("Missing or oversized calendar input")
    with path.open("rb") as handle:
        body = handle.read(limit + 1)
    if len(body) > limit:
        raise CalendarError("Oversized calendar input")
    return body


def _text(value):
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 1000:
        raise CalendarError("Invalid provider text")
    # Optional PandaScore series names can be the empty string even when the
    # series ID/full_name are supplied. Null records absence without inventing
    # a fallback name; the checksum-bound private archive retains literal input.
    if not value.strip():
        return None
    return value


def _id(value):
    if value is None:
        return None
    if type(value) is not int or not 1 <= value <= 2**53 - 1:
        raise CalendarError("Invalid provider identity")
    return value


def _metadata(raw: dict, field: str, kind: str):
    value = raw.get(field)
    if value is not None and not isinstance(value, dict):
        raise CalendarError("Invalid event metadata")
    value = value or {}
    source_id = _id(value.get("id", raw.get(f"{field}_id")))
    outer = _id(raw.get(f"{field}_id"))
    if source_id is not None and outer is not None and source_id != outer:
        raise CalendarError("Conflicting event metadata identity")
    if not value and source_id is None:
        return None
    season = value.get("season")
    if type(season) is int:
        season = str(season)
    return _checked(EventMetadata, dict(id=f"pandascore:{kind}:{source_id}" if source_id else None,
        source_id=source_id, name=_text(value.get("name")), full_name=_text(value.get("full_name")),
        begin_at=value.get("begin_at"), end_at=value.get("end_at"), modified_at=value.get("modified_at"),
        season=_text(season), year=value.get("year")))


def _business(raw: dict) -> dict:
    if not isinstance(raw, dict) or _id(raw.get("id")) is None:
        raise CalendarError("Announcement requires a provider identity")
    pending = set()
    opponents = raw.get("opponents")
    opponents = [] if opponents is None else opponents
    if not isinstance(opponents, list) or len(opponents) > 2:
        raise CalendarError("Invalid bounded opponent list")
    teams = []
    for opponent in opponents:
        if not isinstance(opponent, dict):
            raise CalendarError("Invalid opponent")
        kind, body = opponent.get("type"), opponent.get("opponent")
        if kind not in (None, "Team", "team"):
            pending.add("unsupported_opponent_type")
            teams.append(None)
            continue
        if body is None:
            teams.append(None)
            continue
        if not isinstance(body, dict):
            raise CalendarError("Invalid opponent")
        team_id = _id(body.get("id"))
        teams.append(_checked(Team, dict(team_id=f"pandascore:{team_id}" if team_id else None,
            source_team_id=team_id, team_name=_text(body.get("name")))))
    teams.extend([None] * (2 - len(teams)))
    ids = [team["team_id"] if team else None for team in teams]
    if None in ids or len(set(ids)) != 2:
        pending.add("unresolved_opponents")
    if any(team is not None and team["team_name"] is None for team in teams):
        pending.add("unresolved_team_names")
    dates = _checked(SourceDates, {field: raw.get(field) for field in SourceDates.model_fields})
    start = None
    if dates["begin_at"] is not None:
        try:
            start = _utc(dates["begin_at"])
        except ValueError:
            if "T" in dates["begin_at"] or " " in dates["begin_at"]:
                pending.add("start_time_timezone_unknown")
    if start is None:
        pending.add("unresolved_start_time")
    results = raw.get("results")
    results = [] if results is None else results
    if not isinstance(results, list) or len(results) > 8:
        raise CalendarError("Invalid bounded results")
    typed_results = []
    for result in results:
        if not isinstance(result, dict):
            raise CalendarError("Invalid result")
        team_id = _id(result.get("team_id"))
        typed_results.append(_checked(Result, dict(team_id=f"pandascore:{team_id}" if team_id else None,
            source_team_id=team_id, score=result.get("score"))))
    status = raw.get("status")
    result_ids = [result["team_id"] for result in typed_results]
    known = (status == "finished" and None not in ids and len(set(ids)) == 2
             and len(typed_results) == 2 and set(result_ids) == set(ids)
             and all(result["score"] is not None for result in typed_results))
    if status == "finished" and not known:
        pending.add("result_unavailable")
    if status not in ("not_started", "running", "finished", "canceled", "postponed"):
        pending.add("unknown_status")
    return _checked(Business, dict(announcement_id=f"pandascore:{raw['id']}", name=_text(raw.get("name")),
        source_status=status, start_time=start, source_dates=dates, team_a=teams[0], team_b=teams[1],
        league=_metadata(raw, "league", "league"), series=_metadata(raw, "serie", "series"),
        tournament=_metadata(raw, "tournament", "tournament"), source_number_of_games=raw.get("number_of_games"),
        source_results=typed_results, result_known=known, pending_reasons=sorted(pending)))


def _source_sort(source):
    return (source["page"], PHASES.index(source["phase"]))


def _aggregate(occurrences: list[dict], links: set[str]) -> list[dict]:
    groups = {}
    for row in occurrences:
        groups.setdefault(row["announcement_id"], []).append(row)
    announcements = []
    for identity, variants in sorted(groups.items()):
        business_variants = [{key: row[key] for key in Business.model_fields} for row in variants]
        conflicted = len({_canonical(value) for value in business_variants}) > 1
        selected = max(variants, key=lambda row: (row["observed_at"], row["occurrence_id"]))
        business = {key: selected[key] for key in Business.model_fields}
        selected_business = _canonical(business)
        known_at = min(row["observed_at"] for row, fields in zip(variants, business_variants, strict=True)
                       if _canonical(fields) == selected_business)
        if conflicted:
            business["pending_reasons"] = sorted(set(business["pending_reasons"]) | {"conflicting_phase_records"})
            business["result_known"] = False
        announcements.append(_checked(Announcement, dict(**business,
            canonical_match_id=identity if identity in links and not conflicted else None,
            observed_at=known_at,
            first_observed_at=min(row["observed_at"] for row in variants),
            last_observed_at=max(row["observed_at"] for row in variants),
            source_ids=sorted({row["source_id"] for row in variants}),
            occurrence_ids=sorted(row["occurrence_id"] for row in variants), conflicted=conflicted)))
    return announcements


def _quality(announcements, occurrences, pages):
    return _checked(Quality, dict(source_records=len(occurrences), announcements=len(announcements),
        duplicate_occurrences=len(occurrences) - len(announcements),
        conflicted_announcements=sum(row["conflicted"] for row in announcements),
        pending_announcements=sum(bool(row["pending_reasons"]) for row in announcements),
        unresolved_opponents=sum("unresolved_opponents" in row["pending_reasons"] for row in announcements),
        unresolved_start_time=sum("unresolved_start_time" in row["pending_reasons"] for row in announcements),
        results_known=sum(row["result_known"] for row in announcements),
        canonical_linked=sum(row["canonical_match_id"] is not None for row in announcements),
        source_phases=list(PHASES), pages_per_phase=pages,
        coverage_note="Bounded provider pages from one completed poll; not a complete event calendar or in-game feed.",
        result_note="Scores are source-reported series results. Upcoming zero scores remain unconfirmed; no rounds inferred."))


def _scope(pages):
    return dict(kind="match_phases", competition_codes=[], phases=list(PHASES), pages=pages)


def _counts(payload):
    quality = payload["quality"]
    return {key: quality[key] for key in ("source_records", "announcements", "duplicate_occurrences",
                                        "conflicted_announcements", "pending_announcements", "canonical_linked")} | {
        "sources": len(payload["sources"])}


def _payload(raw_archives, archive_root, pages, canonical_match_ids):
    if type(pages) is not int or not 1 <= pages <= 10:
        raise CalendarError("Pages must be 1..10")
    archive_root = Path(archive_root).absolute()
    _no_symlink(archive_root)
    approved = archive_root.resolve(strict=True)
    paths = []
    for value in raw_archives:
        if len(paths) >= pages * 3:
            raise CalendarError("Too many source archives")
        path = Path(value).absolute()
        _no_symlink(path)
        if not path.resolve(strict=True).is_relative_to(approved):
            raise CalendarError("Source archive escaped approved root")
        paths.append(path)
    if len(paths) != pages * 3 or len(set(paths)) != len(paths):
        raise CalendarError("Incomplete or duplicate poll archives")
    links = set()
    for value in canonical_match_ids:
        if len(links) >= MAX_RECORDS or not isinstance(value, str) or not re.fullmatch(r"pandascore:[1-9][0-9]{0,15}", value):
            raise CalendarError("Invalid canonical link set")
        links.add(value)
    sources, occurrences, total_bytes = [], [], 0
    for path in paths:
        body = _bounded_file(path, MAX_ARCHIVE_BYTES)
        total_bytes += len(body)
        if total_bytes > MAX_TOTAL_ARCHIVE_BYTES:
            raise CalendarError("Poll archives exceed byte budget")
        archive = _json(body)
        if not isinstance(archive, dict) or set(archive) != {"provider", "endpoint", "requested_at", "observed_at", "payload"}:
            raise CalendarError("Invalid source archive envelope")
        if archive["provider"] != "pandascore":
            raise CalendarError("Unsupported calendar provider")
        endpoint = archive["endpoint"]
        match = re.fullmatch(r"valorant/matches/(past|upcoming|running)\?page=([1-9]|10)&per_page=100", endpoint) if isinstance(endpoint, str) else None
        if match is None or int(match[2]) > pages:
            raise CalendarError("Invalid calendar endpoint")
        phase, page = match[1], int(match[2])
        records = archive["payload"]
        if not isinstance(records, list) or len(records) > 100:
            raise CalendarError("Invalid source page bound")
        source_id = f"pandascore:valorant:{phase}:page:{page}"
        source = _checked(Source, dict(source_id=source_id, phase=phase, page=page, endpoint=endpoint,
            requested_at=archive["requested_at"], observed_at=archive["observed_at"],
            archive_sha256=_hash(body), record_count=len(records)))
        if source["requested_at"] > source["observed_at"]:
            raise CalendarError("Source observation precedes request")
        sources.append(source)
        for index, raw in enumerate(records):
            occurrences.append(_checked(Occurrence, dict(**_business(raw),
                occurrence_id=f"{source_id}:row:{index:03d}", source_id=source_id,
                observed_at=source["observed_at"])))
    sources.sort(key=_source_sort)
    if [source["source_id"] for source in sources] != [f"pandascore:valorant:{phase}:page:{page}"
            for page in range(1, pages + 1) for phase in PHASES]:
        raise CalendarError("Incomplete or duplicate phase/page set")
    if len({source["requested_at"] for source in sources}) != 1:
        raise CalendarError("Archives must belong to one successful collection")
    occurrences.sort(key=lambda row: row["occurrence_id"])
    announcements = _aggregate(occurrences, links)
    return dict(announcements=announcements, occurrences=occurrences, sources=sources,
                quality=_quality(announcements, occurrences, pages))


def validate_calendar(package: Path) -> dict:
    """Validate every private publication hash, bound, type, and internal linkage.

    This proves package consistency, not authenticity of provider response bytes.
    Sources are collector archives supplied at publication, not a signed feed.
    """
    package = Path(package).absolute()
    _no_symlink(package)
    if not package.is_dir():
        raise CalendarError("Missing calendar package")
    entries = []
    for entry in package.iterdir():
        if len(entries) >= len(FILES) + 1:
            raise CalendarError("Unexpected calendar package inventory")
        entries.append(entry)
    if {entry.name for entry in entries} != {"manifest.json", *FILES}:
        raise CalendarError("Unexpected calendar package inventory")
    for entry in entries:
        _no_symlink(entry)
        if not entry.is_file():
            raise CalendarError("Calendar package must contain regular files only")
    manifest = _checked(Manifest, _json(_bounded_file(package / "manifest.json", 64 * 1024)))
    if set(manifest["files"]) != set(FILES):
        raise CalendarError("Invalid calendar file inventory")
    payload = {}
    models = {"announcements": Announcement, "occurrences": Occurrence, "sources": Source}
    for filename in FILES:
        body = _bounded_file(package / filename, MAX_PACKAGE_FILE_BYTES)
        if _hash(body) != manifest["files"][filename]:
            raise CalendarError("Calendar checksum mismatch")
        key = filename.removesuffix(".json")
        value = _json(body)
        if key == "quality":
            payload[key] = _checked(Quality, value)
        else:
            bound = 30 if key == "sources" else MAX_RECORDS
            if not isinstance(value, list) or len(value) > bound:
                raise CalendarError("Calendar row bound exceeded")
            payload[key] = [_checked(models[key], row) for row in value]
    sources = payload["sources"]
    pages = payload["quality"]["pages_per_phase"]
    if sources != sorted(sources, key=_source_sort) or [row["source_id"] for row in sources] != [
            f"pandascore:valorant:{phase}:page:{page}" for page in range(1, pages + 1) for phase in PHASES]:
        raise CalendarError("Invalid published phase/page scope")
    if manifest["source_scope"] != _scope(pages) or len({row["requested_at"] for row in sources}) != 1:
        raise CalendarError("Invalid published poll scope")
    source_map = {row["source_id"]: row for row in sources}
    for source in sources:
        expected = f"valorant/matches/{source['phase']}?page={source['page']}&per_page=100"
        if (source["endpoint"] != expected or source["source_id"] !=
                f"pandascore:valorant:{source['phase']}:page:{source['page']}"
                or source["requested_at"] > source["observed_at"]):
            raise CalendarError("Invalid published source provenance")
    occurrences = payload["occurrences"]
    if occurrences != sorted(occurrences, key=lambda row: row["occurrence_id"]):
        raise CalendarError("Unsorted calendar occurrences")
    occurrence_ids = {row["occurrence_id"] for row in occurrences}
    if len(occurrence_ids) != len(occurrences):
        raise CalendarError("Duplicate occurrence identity")
    for source in sources:
        rows = [row for row in occurrences if row["source_id"] == source["source_id"]]
        if {row["occurrence_id"] for row in rows} != {
                f"{source['source_id']}:row:{index:03d}" for index in range(source["record_count"])}:
            raise CalendarError("Calendar source row count mismatch")
    for row in occurrences:
        source = source_map.get(row["source_id"])
        if source is None or row["observed_at"] != source["observed_at"]:
            raise CalendarError("Invalid occurrence source linkage")
        _validate_business(row)
    announcements = payload["announcements"]
    for row in announcements:
        if row["canonical_match_id"] is not None and row["canonical_match_id"] != row["announcement_id"]:
            raise CalendarError("Invalid canonical match linkage")
    links = {row["canonical_match_id"] for row in announcements if row["canonical_match_id"] is not None}
    if announcements != _aggregate(occurrences, links):
        raise CalendarError("Calendar aggregation mismatch")
    if payload["quality"] != _quality(announcements, occurrences, pages) or manifest["counts"] != _counts(payload):
        raise CalendarError("Calendar measured counts mismatch")
    if manifest["as_of"] != max(row["observed_at"] for row in sources) or manifest["generated_at"] < manifest["as_of"]:
        raise CalendarError("Invalid publication clock")
    if manifest["dataset_version"] != _hash(_canonical(payload))[:16]:
        raise CalendarError("Calendar logical fingerprint mismatch")
    return dict(manifest=manifest, **payload)


def _write(path, body):
    with path.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


def _validate_business(row):
    # Rebuild only typed retained business fields to verify all derived values.
    opponents = []
    for team in (row["team_a"], row["team_b"]):
        if team is not None:
            if team["team_id"] != (f"pandascore:{team['source_team_id']}" if team["source_team_id"] else None):
                raise CalendarError("Invalid provider team linkage")
            opponents.append(dict(opponent=dict(id=team["source_team_id"], name=team["team_name"])))
        else:
            opponents.append(dict(opponent=None))
    raw = dict(id=int(row["announcement_id"].split(":")[1]), name=row["name"], status=row["source_status"],
        opponents=opponents, results=[dict(team_id=result["source_team_id"], score=result["score"])
            for result in row["source_results"]], number_of_games=row["source_number_of_games"], **row["source_dates"])
    for field, kind in (("league", "league"), ("series", "series"), ("tournament", "tournament")):
        value = row[field]
        if value is not None:
            if value["id"] != (f"pandascore:{kind}:{value['source_id']}" if value["source_id"] else None):
                raise CalendarError("Invalid event metadata linkage")
            raw["serie" if field == "series" else field] = dict(
                id=value["source_id"], **{key: value[key] for key in EventMetadata.model_fields
                                         if key not in ("id", "source_id")})
    for result in row["source_results"]:
        if result["team_id"] != (f"pandascore:{result['source_team_id']}" if result["source_team_id"] else None):
            raise CalendarError("Invalid result team linkage")
    expected = _business(raw)
    actual = {key: row[key] for key in Business.model_fields}
    # Unsupported Player objects are intentionally omitted; preserve that reason.
    if "unsupported_opponent_type" in actual["pending_reasons"]:
        expected["pending_reasons"] = sorted(set(expected["pending_reasons"]) | {"unsupported_opponent_type"})
    if expected != actual:
        raise CalendarError("Invalid derived calendar business fields")


def publish_calendar(raw_archives: Iterable[Path], output_root: Path, *, archive_root: Path,
                     pages: int = 1, canonical_match_ids: Iterable[str] = (),
                     generated_at: datetime | None = None) -> dict:
    """Publish one complete successful poll, returning its immutable manifest.

    ``raw_archives`` must be ``meta['raw_archives']`` from successful ``collect``.
    The caller supplies stored canonical IDs; this function never infers linkage
    from fixture fields. The calendar and canonical pointers are independent.
    """
    payload = _payload(raw_archives, archive_root, pages, canonical_match_ids)
    generated_at = generated_at or datetime.now(timezone.utc)
    if not isinstance(generated_at, datetime) or generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise CalendarError("Publication requires an aware clock")
    as_of = max(source["observed_at"] for source in payload["sources"])
    generated = _utc(generated_at.isoformat())
    if generated < as_of:
        raise CalendarError("Publication precedes collection")
    output_root = Path(output_root).absolute()
    _no_symlink(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    version = _hash(_canonical(payload))[:16]
    destination = output_root / f"calendar-{version}"
    if destination.exists():
        existing = validate_calendar(destination)
        if {key: existing[key] for key in payload} != payload:
            raise CalendarError("Calendar content-address collision")
        manifest = existing["manifest"]
    else:
        staging = output_root / f".calendar-{uuid.uuid4().hex}.tmp"
        staging.mkdir()
        try:
            file_hashes = {}
            for filename in FILES:
                body = _canonical(payload[filename.removesuffix(".json")]) + b"\n"
                if len(body) > MAX_PACKAGE_FILE_BYTES:
                    raise CalendarError("Calendar publication exceeds byte budget")
                _write(staging / filename, body)
                file_hashes[filename] = _hash(body)
            manifest = _checked(Manifest, dict(dataset_version=version, generated_at=generated, as_of=as_of,
                source_scope=_scope(pages), counts=_counts(payload), files=file_hashes,
                publication_note="Private API calendar; independent of canonical snapshot version. No forecast eligibility or in-game telemetry."))
            _write(staging / "manifest.json", _canonical(manifest) + b"\n")
            validate_calendar(staging)
            staging.rename(destination)
        finally:
            if staging.exists():
                if not staging.resolve().is_relative_to(output_root.resolve()):
                    raise CalendarError("Unsafe calendar staging cleanup")
                shutil.rmtree(staging)
    # One writer is expected, protected by the polling worker's provider lock.
    pointer = output_root / f".CALENDAR_LATEST-{uuid.uuid4().hex}.tmp"
    try:
        with pointer.open("w", encoding="ascii", newline="\n") as handle:
            handle.write(destination.name + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        pointer.replace(output_root / "CALENDAR_LATEST")
    finally:
        pointer.unlink(missing_ok=True)
    return manifest
