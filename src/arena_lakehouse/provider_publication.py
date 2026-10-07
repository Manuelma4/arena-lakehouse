"""Private, atomic provider publication selectors and bounded recovery evidence.

The selector commits a pair of immutable packages. It never discovers archives,
fetches providers, fills calendar nulls, or claims a multi-file filesystem commit.
"""

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .calendar import CalendarError, _bounded_file, _no_symlink, _payload, validate_calendar
from .engine import history, latest, team_stats
from .schema import MatchEvent, digest
from .snapshot import DOCUMENTS, TEAM_STATS_SCHEMA

SCHEMA_VERSION = "arena-provider-publication-1.0"
INTENT_VERSION = "arena-provider-publication-intent-1.0"
SELECTOR = "CURRENT_PUBLICATION.json"
MAX_SELECTOR_BYTES = 64 * 1024
MAX_INTENT_BYTES = 32 * 1024 * 1024
MAX_PACKAGE_BYTES = 512 * 1024 * 1024
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_EVENTS = 200_000
MAX_MATCHES = 100_000
Hex64 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Hex16 = Annotated[str, Field(pattern=r"^[0-9a-f]{16}$")]
PollID = Annotated[int, Field(ge=1, le=2**63 - 1)]
Stamp = Annotated[str, Field(max_length=100)]


class PublicationError(ValueError):
    """Sanitized contract errors contain no raw provider values."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def sha256(body):
    return hashlib.sha256(body).hexdigest()


def stamp(value):
    if isinstance(value, str):
        if len(value) > 100:
            raise ValueError("Invalid publication timestamp")
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Publication clocks must be aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise PublicationError("Duplicate publication JSON key")
        result[key] = value
    return result


def parse_json(body):
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(PublicationError("Nonfinite publication JSON")))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise PublicationError("Malformed publication JSON") from None


def read_bytes(path, limit):
    try:
        return _bounded_file(Path(path), limit)
    except (CalendarError, OSError):
        raise PublicationError("Missing, linked or oversized publication input") from None


def safe(path):
    if ".." in Path(path).parts:
        raise PublicationError("Publication path contains traversal")
    try:
        _no_symlink(Path(path))
    except CalendarError:
        raise PublicationError("Publication paths must not contain links") from None


def atomic_json(path, value):
    path = Path(path)
    safe(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}-{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as handle:
            handle.write(canonical(value) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        # Supported Linux filesystems can persist the directory entry as well.
        if os.name != "nt":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Scope(Contract):
    kind: Literal["competition_list", "match_phases"]
    competition_codes: Annotated[list[Annotated[str, Field(pattern=r"^[A-Z0-9]{1,12}$")]], Field(max_length=10)]
    phases: Annotated[list[Literal["past", "upcoming", "running"]], Field(max_length=3)]
    pages: Annotated[int, Field(ge=1, le=10)]

    @model_validator(mode="after")
    def scope(self):
        if self.kind == "competition_list":
            if not self.competition_codes or len(set(self.competition_codes)) != len(self.competition_codes):
                raise ValueError("Invalid football scope")
            if self.phases or self.pages != 1:
                raise ValueError("Invalid football scope")
        elif self.competition_codes or self.phases != ["past", "upcoming", "running"]:
            raise ValueError("Invalid PandaScore scope")
        return self


class Descriptor(Contract):
    basename: Annotated[str, Field(min_length=1, max_length=80)]
    dataset_version: Hex16
    manifest_sha256: Hex64


class Candidate(Contract):
    provider: Literal["football-data", "pandascore"]
    data_kind: Literal["api"] = "api"
    poll_id: PollID
    source_scope: Scope
    collected_at: Stamp
    canonical: Descriptor | None
    calendar: Descriptor | None

    _clock = field_validator("collected_at")(stamp)

    @model_validator(mode="after")
    def provider_scope(self):
        if self.source_scope.kind != ("competition_list" if self.provider == "football-data" else "match_phases"):
            raise ValueError("Provider scope mismatch")
        if (self.provider == "football-data" and self.calendar is not None
                or self.provider == "pandascore" and self.calendar is None):
            raise ValueError("Provider calendar mismatch")
        return self


class Publication(Candidate):
    schema_version: Literal["arena-provider-publication-1.0"] = SCHEMA_VERSION
    selected_at: Stamp
    fingerprint: Hex64

    _selection_clock = field_validator("selected_at")(stamp)


class Archive(Contract):
    path: Annotated[str, Field(min_length=1, max_length=180)]
    sha256: Hex64


class Cursor(Contract):
    basename: Annotated[str, Field(min_length=1, max_length=40)]
    state: dict[str, dict]


class Metrics(Contract):
    source_rows: Annotated[int, Field(ge=0, le=MAX_MATCHES)]
    changed_rows: Annotated[int, Field(ge=0, le=MAX_MATCHES)]
    unsupported_rows: Annotated[int, Field(ge=0, le=MAX_MATCHES)]
    snapshot_changed: bool


class Intent(Contract):
    schema_version: Literal["arena-provider-publication-intent-1.0"] = INTENT_VERSION
    candidate: Candidate
    previous_fingerprint: Hex64 | None
    archives: Annotated[list[Archive], Field(min_length=1, max_length=30)]
    cursors: Annotated[list[Cursor], Field(min_length=1, max_length=10)]
    metrics: Metrics
    selection: Publication | None = None


def checked(model, value):
    try:
        result = model.model_validate(value).model_dump()
    except (ValidationError, ValueError, TypeError):
        raise PublicationError("Invalid typed publication record") from None
    if result != value:
        raise PublicationError("Publication fields must be explicit and UTC-normalized")
    return result


def _relative(name):
    if not isinstance(name, str):
        raise PublicationError("Invalid relative publication path")
    path = PurePosixPath(name)
    if (path.is_absolute() or ".." in path.parts or "\\" in name
            or ":" in name or str(path) != name or len(path.parts) > 5):
        raise PublicationError("Invalid relative publication path")
    return path


def _descriptor(path, manifest):
    return dict(basename=path.name, dataset_version=manifest["dataset_version"],
                manifest_sha256=sha256(read_bytes(path / "manifest.json", MAX_SELECTOR_BYTES)))


def validate_canonical(path, provider, *, now):
    """Verify exact bytes and full canonical/Gold parity without model execution."""
    path = Path(path)
    safe(path)
    manifest = parse_json(read_bytes(path / "manifest.json", MAX_SELECTOR_BYTES))
    fields = {"schema_version", "dataset_version", "producer_version", "generated_at", "as_of", "data_kind", "providers", "files"}
    if (not isinstance(manifest, dict) or set(manifest) != fields or manifest["schema_version"] != "2.0"
            or manifest["data_kind"] != "api" or manifest["providers"] != [provider]
            or not isinstance(manifest["dataset_version"], str) or not re.fullmatch(r"[0-9a-f]{16}", manifest["dataset_version"])
            or not isinstance(manifest["producer_version"], str) or not 1 <= len(manifest["producer_version"]) <= 50):
        raise PublicationError("Invalid canonical manifest")
    if not stamp(manifest["as_of"]) <= stamp(manifest["generated_at"]) <= stamp(now):
        raise PublicationError("Invalid canonical publication clock")
    files = manifest["files"]
    required = {"bronze/events.jsonl", "gold/matches.parquet", "gold/match_history.parquet",
                "gold/team_stats.parquet", "catalog.json", "metrics.json", "lineage.json", "quality.json",
                "incidents.json", "predictions.json", "model_report.json", *[f"docs/{slug}.md" for slug in DOCUMENTS]}
    if not isinstance(files, dict) or set(files) != required:
        raise PublicationError("Invalid canonical inventory")
    actual, total = set(), 0
    for index, entry in enumerate(path.rglob("*")):
        if index >= 256:
            raise PublicationError("Canonical inventory bound exceeded")
        safe(entry)
        if entry.is_file():
            actual.add(entry.relative_to(path).as_posix())
        elif not entry.is_dir():
            raise PublicationError("Canonical inventory must be regular files")
    if actual != {"manifest.json", *files}:
        raise PublicationError("Canonical inventory mismatch")
    for name, expected in files.items():
        relative = _relative(name)
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise PublicationError("Invalid canonical checksum")
        limit = min(MAX_PACKAGE_BYTES - total, MAX_JSON_BYTES if name.endswith((".json", ".jsonl")) else MAX_PACKAGE_BYTES)
        body = read_bytes(path.joinpath(*relative.parts), limit)
        total += len(body)
        if sha256(body) != expected:
            raise PublicationError("Canonical checksum mismatch")
    raw_events = read_bytes(path / "bronze/events.jsonl", MAX_JSON_BYTES).splitlines()
    if not 1 <= len(raw_events) <= MAX_EVENTS:
        raise PublicationError("Canonical event bound exceeded")
    events, versions, ids = [], set(), set()
    for body in raw_events:
        raw = parse_json(body)
        try:
            row = MatchEvent.model_validate(raw).row()
        except (ValidationError, ValueError, TypeError):
            raise PublicationError("Invalid canonical match observation") from None
        if row != raw or row["provider"] != provider or row["sport"] != ("football" if provider == "football-data" else "valorant"):
            raise PublicationError("Canonical provider or type mismatch")
        if any(not re.fullmatch(re.escape(provider) + r":[1-9][0-9]{0,19}", row[key])
               for key in ("match_id", "team_a_id", "team_b_id")):
            raise PublicationError("Invalid canonical provider identity")
        if row["event_id"] in ids or (row["match_id"], row["revision"]) in versions:
            raise PublicationError("Duplicate canonical observation")
        if not re.fullmatch(re.escape(f"{row['match_id']}:{row['revision']}:") + r"[0-9a-f]{64}", row["event_id"]):
            raise PublicationError("Canonical observation identity mismatch")
        ids.add(row["event_id"])
        versions.add((row["match_id"], row["revision"]))
        # Preserve wire column order for Arrow schema parity; validation above
        # already requires the exact normalized event fields and values.
        events.append(raw)
    if (digest({"events": events})[:16] != manifest["dataset_version"]
            or max(row["observed_at"] for row in events) != stamp(manifest["as_of"])):
        raise PublicationError("Canonical logical version or cutoff mismatch")
    if events != sorted(events, key=lambda row: (row["match_id"], row["revision"])):
        raise PublicationError("Canonical observations must be ordered")
    previous = {}
    for row in events:
        other = previous.get(row["match_id"])
        if other and (row["observed_at"] <= other["observed_at"]
                      or any(row[key] != other[key] for key in ("provider", "sport", "team_a_id", "team_b_id"))):
            raise PublicationError("Canonical revision chain mismatch")
        previous[row["match_id"]] = row
    current = latest(events)
    if len(current) > MAX_MATCHES:
        raise PublicationError("Canonical match bound exceeded")
    expected_tables = {"matches": current, "team_stats": team_stats(current), "match_history": history(events)}
    schemas = {}
    for name, rows in expected_tables.items():
        table = path / f"gold/{name}.parquet"
        parquet = pq.ParquetFile(table)
        metadata = parquet.metadata
        if (metadata.num_rows != len(rows) or sum(metadata.row_group(i).total_byte_size
                for i in range(metadata.num_row_groups)) > MAX_PACKAGE_BYTES):
            raise PublicationError("Canonical Gold size mismatch")
        expected_schema = TEAM_STATS_SCHEMA if name == "team_stats" else pa.Table.from_pylist(rows).schema
        if not parquet.schema_arrow.equals(expected_schema) or pq.read_table(table).to_pylist() != rows:
            raise PublicationError("Canonical Gold parity mismatch")
        schemas[name] = expected_schema
    metadata = {name: parse_json(read_bytes(path / f"{name}.json", MAX_JSON_BYTES))
                for name in ("catalog", "metrics", "lineage", "quality", "incidents")}
    catalog = metadata["catalog"]
    if not isinstance(catalog, list) or [row.get("name") for row in catalog if isinstance(row, dict)] != list(expected_tables):
        raise PublicationError("Invalid canonical catalog")
    for row in catalog:
        name = row["name"]
        expected_columns = [dict(name=field.name, type=str(field.type), description=field.name.replace("_", " "))
                            for field in schemas[name]]
        primary_key = ["sport", "team_id"] if name == "team_stats" else ["match_id", "revision"] if name == "match_history" else ["match_id"]
        if (set(row) != {"name", "layer", "description", "columns", "primary_key"} or row["layer"] != "gold"
                or not isinstance(row["description"], str) or row["columns"] != expected_columns
                or row["primary_key"] != primary_key):
            raise PublicationError("Canonical catalog and Gold schema mismatch")
    for name, keys in (("metrics", {"name", "description", "unit", "table", "formula"}),
                       ("lineage", {"source", "target", "transformation"})):
        rows = metadata[name]
        if (not isinstance(rows, list) or not 1 <= len(rows) <= 128
                or any(not isinstance(row, dict) or set(row) != keys
                       or any(not isinstance(item, str) or not 1 <= len(item) <= 2000 for item in row.values()) for row in rows)):
            raise PublicationError("Invalid canonical metadata records")
    quality = metadata["quality"]
    if (not isinstance(quality, dict) or set(quality) != {"run_id", "checks", "summary"}
            or not isinstance(quality["run_id"], str) or not isinstance(quality["checks"], list)
            or not isinstance(quality["summary"], dict)
            or set(quality["summary"]) != {"valid_events", "quarantined", "last_run_status"}
            or type(quality["summary"]["valid_events"]) is not int
            or quality["summary"]["valid_events"] != len(events)
            or quality["summary"]["last_run_status"] != "succeeded"
            or type(quality["summary"]["quarantined"]) is not int or quality["summary"]["quarantined"] < 0
            or not isinstance(metadata["incidents"], list)):
        raise PublicationError("Invalid canonical quality records")
    checks = quality["checks"]
    if (len(checks) > 128 or any(not isinstance(row, dict)
            or set(row) != {"rule", "table", "passed", "failed", "severity"}
            or not isinstance(row["rule"], str) or not 1 <= len(row["rule"]) <= 120
            or row["table"] != "bronze_events" or row["severity"] != "warning"
            or type(row["passed"]) is not int or row["passed"] != len(events)
            or type(row["failed"]) is not int or row["failed"] < 1 for row in checks)
            or quality["summary"]["quarantined"] != sum(row["failed"] for row in checks)):
        raise PublicationError("Invalid canonical quality checks")
    incidents = [dict(incident_id=digest(row)[:16], **{key: row[key] for key in ("rule", "table", "severity")},
                      description=f"{row['failed']} quarantined observations", status="open", event_count=row["failed"],
                      remediation="Inspect quarantined source records, correct and replay") for row in checks]
    if metadata["incidents"] != incidents:
        raise PublicationError("Canonical incidents and quality checks mismatch")
    for slug in DOCUMENTS:
        try:
            read_bytes(path / f"docs/{slug}.md", MAX_JSON_BYTES).decode("utf-8")
        except UnicodeError:
            raise PublicationError("Invalid canonical documentation encoding") from None
    if (parse_json(read_bytes(path / "predictions.json", MAX_JSON_BYTES)) != []
            or parse_json(read_bytes(path / "model_report.json", MAX_JSON_BYTES)) != {"status": "not_run"}):
        raise PublicationError("Provider publication must not contain model predictions")
    return dict(manifest=manifest, matches=current, events=events)


def describe_canonical(path, provider, *, now):
    path = Path(path)
    return _descriptor(path, validate_canonical(path, provider, now=now)["manifest"])


def describe_calendar(path, *, now):
    path = Path(path)
    payload = validate_calendar(path)
    if stamp(payload["manifest"]["generated_at"]) > stamp(now):
        raise PublicationError("Calendar publication clock is in the future")
    return _descriptor(path, payload["manifest"])


def validate_candidate(root, value, *, now):
    root = Path(root)
    safe(root)
    candidate = checked(Candidate, value)
    if root.name != candidate["provider"] or candidate["collected_at"] > stamp(now):
        raise PublicationError("Candidate provider or clock mismatch")
    datasets = {}
    for role, directory in (("canonical", "snapshots"), ("calendar", "calendars")):
        descriptor = candidate[role]
        if descriptor is None:
            continue
        pattern = r"snapshot-([0-9a-f]{16})-[0-9a-f]{8}" if role == "canonical" else r"calendar-([0-9a-f]{16})"
        match = re.fullmatch(pattern, descriptor["basename"])
        if not match or match[1] != descriptor["dataset_version"]:
            raise PublicationError("Invalid immutable package basename")
        path = root / directory / descriptor["basename"]
        body = read_bytes(path / "manifest.json", MAX_SELECTOR_BYTES)
        if sha256(body) != descriptor["manifest_sha256"]:
            raise PublicationError("Referenced manifest checksum mismatch")
        if role == "canonical":
            payload = validate_canonical(path, candidate["provider"], now=now)
        else:
            payload = validate_calendar(path)
            if payload["manifest"]["source_scope"] != candidate["source_scope"]:
                raise PublicationError("Calendar and collection scopes differ")
        manifest = payload["manifest"]
        if (manifest["dataset_version"] != descriptor["dataset_version"]
                or stamp(manifest["as_of"]) > candidate["collected_at"]
                or stamp(manifest["generated_at"]) > stamp(now)):
            raise PublicationError("Referenced package version or clocks mismatch")
        datasets[role] = payload
    if "calendar" in datasets:
        canonical_ids = {row["match_id"] for row in datasets.get("canonical", {}).get("matches", [])}
        for row in datasets["calendar"]["announcements"]:
            if row["canonical_match_id"] is not None and row["canonical_match_id"] not in canonical_ids:
                raise PublicationError("Calendar link has no selected canonical identity")
    return datasets


def make_selection(candidate, *, selected_at):
    candidate = checked(Candidate, candidate)
    value = dict(candidate, schema_version=SCHEMA_VERSION, selected_at=stamp(selected_at))
    if value["selected_at"] < value["collected_at"]:
        raise PublicationError("Selection precedes completed collection")
    return dict(value, fingerprint=sha256(canonical(value)))


def validate_publication(root, value, *, now):
    value = checked(Publication, value)
    if not value["collected_at"] <= value["selected_at"] <= stamp(now):
        raise PublicationError("Invalid selector clock")
    fingerprint = sha256(canonical({key: item for key, item in value.items() if key != "fingerprint"}))
    if value["fingerprint"] != fingerprint:
        raise PublicationError("Selector fingerprint mismatch")
    candidate = {key: value[key] for key in Candidate.model_fields}
    datasets = validate_candidate(root, candidate, now=now)
    if any(stamp(payload["manifest"]["generated_at"]) > value["selected_at"] for payload in datasets.values()):
        raise PublicationError("Selection precedes package preparation")
    return value


def current_publication(root, *, now):
    path = Path(root) / "snapshots" / SELECTOR
    safe(path)
    return validate_publication(root, parse_json(read_bytes(path, MAX_SELECTOR_BYTES)), now=now) if path.exists() else None


def select_publication(root, value, *, now):
    value = validate_publication(root, value, now=now)
    if len(canonical(value)) > MAX_SELECTOR_BYTES:
        raise PublicationError("Selector bound exceeded")
    atomic_json(Path(root) / "snapshots" / SELECTOR, value)


def commit_selection(root, candidate, *, clock, persist):
    """Validate first, then persist the pre-swap clock and replace the selector.

    Recovery calls this again only when the intended poll is not selected. The
    timestamp is a pre-swap observation, not a filesystem transaction timestamp.
    """
    datasets = validate_candidate(root, candidate, now=clock())
    value = make_selection(candidate, selected_at=clock())
    if any(stamp(payload["manifest"]["generated_at"]) > value["selected_at"] for payload in datasets.values()):
        raise PublicationError("Selection precedes package preparation")
    persist(value)
    atomic_json(Path(root) / "snapshots" / SELECTOR, value)
    return value


def archive_references(root, paths, scope, *, collected_at, started_at):
    """Validate only caller-supplied successful-poll archives, never glob inputs."""
    root, references, sources, total = Path(root), [], [], 0
    for index, item in enumerate(paths):
        if index >= 30:
            raise PublicationError("Archive count bound exceeded")
        path = Path(item).absolute()
        safe(path)
        if not path.is_relative_to(root.absolute()):
            raise PublicationError("Archive path escaped provider workspace")
        relative = path.relative_to(root.absolute()).as_posix()
        if not re.fullmatch(r"provider_raw/(football-data|pandascore)/\d{4}-\d{2}-\d{2}/[0-9a-f]{32}\.json", relative):
            raise PublicationError("Invalid private archive reference")
        body = read_bytes(path, 4 * 1024 * 1024)
        total += len(body)
        if total > MAX_JSON_BYTES:
            raise PublicationError("Archive byte bound exceeded")
        archive = parse_json(body)
        if (not isinstance(archive, dict) or set(archive) != {"provider", "endpoint", "requested_at", "observed_at", "payload"}
                or archive["provider"] != root.name
                or not stamp(started_at) <= stamp(archive["requested_at"]) <= stamp(archive["observed_at"]) <= stamp(collected_at)):
            raise PublicationError("Invalid private archive provenance")
        if root.name == "pandascore":
            valid = re.fullmatch(r"valorant/matches/(past|upcoming|running)\?page=([1-9]|10)&per_page=100", archive["endpoint"])
            if not valid or int(valid[2]) > scope["pages"] or not isinstance(archive["payload"], list) or len(archive["payload"]) > 100:
                raise PublicationError("Invalid PandaScore archive scope")
        else:
            valid = re.fullmatch(r"competitions/([A-Z0-9]{1,12})/matches", archive["endpoint"])
            if (not valid or valid[1] not in scope["competition_codes"] or not isinstance(archive["payload"], dict)
                    or not isinstance(archive["payload"].get("matches"), list)
                    or len(archive["payload"]["matches"]) > MAX_MATCHES):
                raise PublicationError("Invalid football archive scope")
        sources.append(archive)
        references.append(dict(path=relative, sha256=sha256(body)))
    endpoints = [source["endpoint"] for source in sources]
    expected = ([f"valorant/matches/{phase}?page={page}&per_page=100" for page in range(1, scope["pages"] + 1)
                 for phase in ("past", "upcoming", "running")] if root.name == "pandascore" else
                [f"competitions/{code}/matches" for code in scope["competition_codes"]])
    if endpoints != expected or len({reference["path"] for reference in references}) != len(references):
        raise PublicationError("Incomplete or duplicate collection scope")
    return references, sources


def validate_intent(root, value, *, now, started_at):
    value = checked(Intent, value)
    candidate = value["candidate"]
    datasets = validate_candidate(root, candidate, now=now)
    paths = [Path(root).joinpath(*_relative(row["path"]).parts) for row in value["archives"]]
    references, sources = archive_references(root, paths, candidate["source_scope"],
        collected_at=candidate["collected_at"], started_at=started_at)
    if references != value["archives"]:
        raise PublicationError("Recovery archive checksum mismatch")
    expected_cursors = ([f"{code}-cursor.json" for code in candidate["source_scope"]["competition_codes"]]
                        if candidate["provider"] == "football-data" else ["cursor.json"])
    if [row["basename"] for row in value["cursors"]] != expected_cursors:
        raise PublicationError("Recovery cursor scope mismatch")
    matches = {row["match_id"]: row for row in datasets.get("canonical", {}).get("matches", [])}
    count = 0
    for cursor in value["cursors"]:
        for ident, state in cursor["state"].items():
            count += 1
            if count > MAX_MATCHES or not isinstance(state, dict) or set(state) != {"hash", "revision"}:
                raise PublicationError("Recovery cursor bound or fields mismatch")
            if (not re.fullmatch(re.escape(candidate["provider"]) + r":[1-9][0-9]{0,19}", ident)
                    or not isinstance(state["hash"], str) or not re.fullmatch(r"[0-9a-f]{64}", state["hash"])
                    or type(state["revision"]) is not int or state["revision"] < 1):
                raise PublicationError("Invalid recovery cursor identity")
            match = matches.get(ident)
            # Connector hashes retain source clock spelling before UTC schema
            # normalization. Its event ID binds that exact business hash.
            if (match is None or match["revision"] != state["revision"]
                    or match["event_id"] != f"{ident}:{state['revision']}:{state['hash']}"):
                raise PublicationError("Recovery cursor has no matching canonical record")
    metric = value["metrics"]
    returned = sum(len(source["payload"] if candidate["provider"] == "pandascore" else source["payload"]["matches"])
                   for source in sources)
    if metric["source_rows"] != returned or metric["changed_rows"] + metric["unsupported_rows"] > returned:
        raise PublicationError("Recovery metrics mismatch")
    if "calendar" in datasets:
        source_hashes = {row["archive_sha256"] for row in datasets["calendar"]["sources"]}
        if source_hashes != {row["sha256"] for row in references}:
            raise PublicationError("Calendar does not describe the selected poll archives")
        links = {row["canonical_match_id"] for row in datasets["calendar"]["announcements"]
                 if row["canonical_match_id"] is not None}
        expected = _payload(paths, Path(root) / "provider_raw", candidate["source_scope"]["pages"], links)
        if expected != {key: datasets["calendar"][key] for key in expected}:
            raise PublicationError("Calendar and exact collection evidence differ")
    if value["selection"] is not None:
        selected = validate_publication(root, value["selection"], now=now)
        if {key: selected[key] for key in Candidate.model_fields} != candidate:
            raise PublicationError("Recovery selection candidate mismatch")
    if len(canonical(value)) > MAX_INTENT_BYTES:
        raise PublicationError("Recovery intent byte bound exceeded")
    return value
