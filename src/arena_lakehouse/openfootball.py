"""No-token CC0 acquisition, explicitly separate from canonical match ingestion.

Dates and local wall times are source facts, not UTC instants. Availability starts
at actual collection. The dedicated SQLite store owns revisions, polls and cache.
"""

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .http_retry import retry_after_delay


STAGING_VERSION = "openfootball-staging-1.0"
STAGING_VERSION_V2 = "openfootball-staging-2.0"
RAW_BASE = "https://raw.githubusercontent.com/openfootball/football.json/master/"
LICENSE_URL = "https://github.com/openfootball/football.json/blob/master/LICENSE.md"
WORLD_CUP_REF = "516d3825c3bd23fdc298c4014e84bde78f2d4965"
WORLD_CUP_SHA256 = "0ae2c18109b5aa86bc11928b43586ca430c234804d5bbf808d2c3bf2051ecfca"
MAX_BYTES = 10_000_000
MAX_ROWS = 10_000


class AcquisitionError(RuntimeError):
    """An unsuccessful bounded fetch or an invalid persisted cache."""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(value if isinstance(value, bytes) else _json(value).encode()).hexdigest()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Collection time must be a timezone-aware datetime")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _name(value, field):
    if not isinstance(value, str) or not value.strip() or len(value) > 200:
        raise ValueError(f"Missing or invalid {field}")
    if any(unicodedata.category(c).startswith("C") for c in value):
        raise ValueError(f"Control characters in {field}")
    return unicodedata.normalize("NFC", value.strip())


@dataclass(frozen=True)
class Source:
    source_id: str
    competition_id: str
    season: str
    path: str
    name: str
    region: str
    team_namespace: str
    team_aliases: dict | None = None
    source_ref: str = "master"
    repository: str = "football.json"
    team_type: str = "club"

    def __post_init__(self):
        for field in ("source_id", "competition_id", "team_namespace"):
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", getattr(self, field)):
                raise ValueError(f"Invalid {field}")
        if not re.fullmatch(r"\d{4}(?:-\d{2})?", self.season):
            raise ValueError("Invalid season")
        if not re.fullmatch(re.escape(self.season) + r"/[a-z0-9.-]+\.json", self.path):
            raise ValueError("Source must be a season JSON file in the fixed OpenFootball repository")
        _name(self.name, "source name")
        _name(self.region, "region")
        if not re.fullmatch(r"master|[0-9a-f]{40}", self.source_ref):
            raise ValueError("source_ref must be master or a pinned commit SHA")
        if self.repository not in {"football.json", "worldcup.json"} or self.team_type not in {"club", "national"}:
            raise ValueError("Unsupported repository/team_type")
        if self.repository == "worldcup.json" and (self.source_ref != WORLD_CUP_REF or
                self.path != "2026/worldcup.json" or self.team_type != "national"):
            raise ValueError("World Cup source must use the reviewed pinned 2026 JSON edition")
        if self.repository == "football.json" and self.team_type != "club":
            raise ValueError("Club repository cannot declare national teams")
        if self.team_aliases is not None:
            if not isinstance(self.team_aliases, dict):
                raise ValueError("team_aliases must contain reviewed name mappings")
            for original, canonical in self.team_aliases.items():
                _name(original, "alias")
                _name(canonical, "canonical alias")

    @property
    def url(self):
        return f"https://raw.githubusercontent.com/openfootball/{self.repository}/{self.source_ref}/{self.path}"

    @property
    def license_url(self):
        return f"https://github.com/openfootball/{self.repository}/blob/{self.source_ref}/LICENSE.md"

    def to_wire(self, staging_version=STAGING_VERSION):
        row = asdict(self)
        if staging_version == STAGING_VERSION and self.repository == "football.json" and self.team_type == "club":
            row.pop("repository")
            row.pop("team_type")
        return row

    @property
    def fingerprint(self):
        return _hash(self.to_wire())


def load_sources(path: Path) -> dict[str, Source]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("catalogue_version") not in {"1.0", "2.0"} or payload.get("license") != "CC0-1.0":
        raise ValueError("Unsupported source catalogue or license")
    sources, scopes = {}, set()
    for row in payload["sources"]:
        source = Source(**row)
        if source.source_id in sources:
            raise ValueError("Duplicate source ID")
        scope = (source.competition_id, source.season)
        if scope in scopes:
            raise ValueError("Duplicate competition-season source scope")
        scopes.add(scope)
        sources[source.source_id] = source
    return sources


@dataclass
class ParseResult:
    rows: list[dict]
    quarantine: list[dict]
    input_rows: int
    duplicates: int = 0


def _score(value):
    if value is None:
        return None, None, "{}"
    details = {"ft": value} if isinstance(value, list) else value
    if not isinstance(details, dict) or not details or set(details) - {"ft", "ht", "et", "p"}:
        raise ValueError("Unsupported score shape")
    for scores in details.values():
        if not isinstance(scores, list) or len(scores) != 2 or any(type(v) is not int or not 0 <= v <= 50
                                                                 for v in scores):
            raise ValueError("Scores must be two integers between 0 and 50")
    ft = details.get("ft")
    return (ft[0], ft[1], _json(details)) if ft else (None, None, _json(details))


def _kickoff(value, source_date, *, allow_offset):
    if value is None:
        return None, None, None, "date_only"
    if isinstance(value, str) and re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
        return value, None, None, "local_minute"
    match = re.fullmatch(r"((?:[01]\d|2[0-3]):[0-5]\d) UTC([+-])(\d{1,2})(?::([0-5]\d))?", value) \
        if isinstance(value, str) and allow_offset else None
    if match is None:
        raise ValueError("time must use local HH:MM or an explicitly supported numeric UTC offset")
    clock, sign, hours, minutes = match.groups()
    offset = int(hours) * 60 + int(minutes or 0)
    if offset > 14 * 60:
        raise ValueError("UTC offset exceeds the supported 14-hour bound")
    offset *= -1 if sign == "-" else 1
    instant = datetime.fromisoformat(source_date + "T" + clock).replace(
        tzinfo=timezone(timedelta(minutes=offset))).astimezone(timezone.utc)
    return clock, offset, _utc(instant), "offset_minute"


def parse_payload(body: bytes, source: Source, observed_at: datetime, *, staging_version=STAGING_VERSION) -> ParseResult:
    """Pure offline parser. Conflicting identities are quarantined symmetrically."""
    observed = _utc(observed_at)
    if staging_version not in {STAGING_VERSION, STAGING_VERSION_V2}:
        raise ValueError("Unsupported staging version")
    if staging_version == STAGING_VERSION and source.repository != "football.json":
        raise ValueError("World Cup acquisition requires an explicit v2 staging workspace")
    if not isinstance(body, bytes) or len(body) > MAX_BYTES:
        raise ValueError("Payload must be bytes within the 10 MB staging bound")
    checksum = _hash(body)
    try:
        payload = json.loads(body.decode("utf-8-sig"), object_pairs_hook=_object,
                             parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
        if not isinstance(payload, dict) or payload.get("name") != source.name:
            raise ValueError("Unexpected competition name or top-level schema")
        matches = payload.get("matches")
        if not isinstance(matches, list) or len(matches) > MAX_ROWS:
            raise ValueError("matches must be an array of at most 10000 rows")
    except (UnicodeError, ValueError, RecursionError) as error:
        return ParseResult([], [dict(index=None, reason=str(error), raw=None)], 0)
    candidates, quarantine = {}, []
    for index, raw in enumerate(matches):
        try:
            if not isinstance(raw, dict):
                raise ValueError("Match must be an object")
            round_name = _name(raw.get("round"), "round")
            a, b = _name(raw.get("team1"), "team1"), _name(raw.get("team2"), "team2")
            if a == b:
                raise ValueError("A team cannot play itself")
            source_date = raw.get("date")
            if not isinstance(source_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", source_date):
                raise ValueError("date must use YYYY-MM-DD")
            date.fromisoformat(source_date)
            local_time, offset, start, precision = _kickoff(raw.get("time"), source_date,
                allow_offset=staging_version == STAGING_VERSION_V2 and source.repository == "worldcup.json")
            sf, sa, details = _score(raw.get("score"))
            aliases = source.team_aliases or {}
            a_identity = _name(aliases.get(a, a), "canonical team1")
            b_identity = _name(aliases.get(b, b), "canonical team2")
            if a_identity == b_identity:
                raise ValueError("Reviewed aliases resolve both opponents to one team")
            team_a_id = "openfootball:team:" + _hash([source.team_namespace, a_identity])[:24]
            team_b_id = "openfootball:team:" + _hash([source.team_namespace, b_identity])[:24]
            occurrence = raw.get("occurrence", 1)
            if type(occurrence) is not int or not 1 <= occurrence <= 100:
                raise ValueError("occurrence must be an explicit integer 1..100")
            ident = [source.competition_id, source.season, round_name, team_a_id, team_b_id, occurrence]
            number = raw.get("num") if staging_version == STAGING_VERSION_V2 else None
            if number is not None:
                if type(number) is not int or not 1 <= number <= MAX_ROWS:
                    raise ValueError("num must be an integer 1..10000")
                ident = [source.competition_id, source.season, number]
            match_id = "openfootball:match:" + _hash(ident)[:24]
            row = dict(staging_version=staging_version, provider="openfootball", sport="football",
                source_id=source.source_id, competition_id=source.competition_id, competition=source.name,
                season=source.season, round=round_name, occurrence=occurrence, match_id=match_id,
                team_a_id=team_a_id, team_a=a, team_b_id=team_b_id, team_b=b,
                team_namespace=source.team_namespace, team_identity_status="unresolved_name_derived_not_official",
                source_date=source_date, local_time=local_time, timezone=None,
                kickoff_precision=precision,
                start_time=start, completed_at=None, result_status="observed_result" if sf is not None
                else "unknown_result", score_a=sf, score_b=sa, score_details_json=details,
                score_basis="source_ft_unverified_regulation" if sf is not None else "unknown",
                prediction_eligible=False,
                observed_at=observed, source_url=source.url, source_sha256=checksum,
                source_ref=source.source_ref, license="CC0-1.0",
                license_url=LICENSE_URL if staging_version == STAGING_VERSION else source.license_url,
                availability_basis="first_collected_revision",
                identity_basis="competition-season-source-match-number" if number is not None
                else "competition-season-round-ordered-teams-occurrence", quality_score=1.0)
            if staging_version == STAGING_VERSION_V2:
                row.update(team_type=source.team_type, source_match_number=number,
                           source_group=_name(raw["group"], "group") if raw.get("group") is not None else None,
                           ground=_name(raw["ground"], "ground") if raw.get("ground") is not None else None,
                           utc_offset_minutes=offset)
            candidates.setdefault(match_id, []).append((index, raw, row))
        except ValueError as error:
            quarantine.append(dict(index=index, reason=str(error), raw=raw))
    rows, duplicates = [], 0
    for records in candidates.values():
        hashes = {_hash(row) for _, _, row in records}
        if len(hashes) > 1:
            quarantine.extend(dict(index=index, reason="Conflicting match identity within one source snapshot", raw=raw)
                              for index, raw, _ in records)
        else:
            rows.append(records[0][2])
            duplicates += len(records) - 1
    return ParseResult(sorted(rows, key=lambda row: row["match_id"]), quarantine, len(matches), duplicates)


@dataclass
class CollectionReport:
    source_id: str
    status: str
    observed_at: str
    source_sha256: str | None
    accepted: int
    changed: int
    unchanged: int
    quarantined: int
    duplicates: int
    archive_path: str | None
    dry_run: bool
    duration_seconds: float


class OpenFootballCollector:
    """Single-worker staging collector with transactional cursor/revision updates."""

    def __init__(self, workspace: Path, client=None, *, staging_version=STAGING_VERSION, attempts=3, min_interval=1.0,
                 sleep=time.sleep, monotonic=time.monotonic):
        if staging_version not in {STAGING_VERSION, STAGING_VERSION_V2}:
            raise ValueError("Unsupported staging version")
        if type(attempts) is not int or not 1 <= attempts <= 5:
            raise ValueError("attempts must be 1..5")
        if not isinstance(min_interval, (float, int)) or not math.isfinite(min_interval) or not 0 <= min_interval <= 60:
            raise ValueError("min_interval must be finite and 0..60 seconds")
        self.workspace = Path(workspace)
        self.state_path = self.workspace / "staging.sqlite"
        self.client, self.attempts, self.min_interval = client, attempts, min_interval
        self.staging_version = staging_version
        self.sleep, self.monotonic, self.last_request = sleep, monotonic, None

    def _check_version(self, connection):
        has_metadata = connection.execute("SELECT name FROM sqlite_master WHERE name='metadata'").fetchone()
        row = connection.execute("SELECT value FROM metadata WHERE key='staging_version'").fetchone() \
            if has_metadata else None
        stored = row[0] if row else STAGING_VERSION
        if stored != self.staging_version:
            raise AcquisitionError("Workspace staging version differs; create a separate workspace, never migrate implicitly")

    def _connect(self):
        self.workspace.mkdir(parents=True, exist_ok=True)
        existing = self.state_path.exists()
        connection = sqlite3.connect(self.state_path, timeout=5)
        if existing:
            try:
                self._check_version(connection)
            except Exception:
                connection.close()
                raise
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS sources(source_id TEXT PRIMARY KEY, fingerprint TEXT, source_json TEXT);
            CREATE TABLE IF NOT EXISTS cache(source_id TEXT PRIMARY KEY, sha256 TEXT, etag TEXT,
                last_modified TEXT, archive_path TEXT);
            CREATE TABLE IF NOT EXISTS current(match_id TEXT PRIMARY KEY, source_id TEXT,
                business_hash TEXT, revision INTEGER, row_json TEXT);
            CREATE TABLE IF NOT EXISTS history(event_id TEXT PRIMARY KEY, source_id TEXT,
                match_id TEXT, revision INTEGER, observed_at TEXT, row_json TEXT);
            CREATE TABLE IF NOT EXISTS quarantine(source_id TEXT, sha256 TEXT, row_index INTEGER,
                reason TEXT, observed_at TEXT, raw_json TEXT, UNIQUE(source_id,sha256,row_index,reason));
            CREATE TABLE IF NOT EXISTS polls(poll_id INTEGER PRIMARY KEY, source_id TEXT, observed_at TEXT,
                status TEXT, sha256 TEXT, changed INTEGER, quarantined INTEGER);
        """)
        if not existing:
            connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT)")
            connection.execute("INSERT INTO metadata VALUES ('staging_version',?)", [self.staging_version])
            connection.commit()
        return connection

    def _read_cache(self, source):
        if self.staging_version == STAGING_VERSION and source.repository != "football.json":
            raise ValueError("World Cup acquisition requires an explicit v2 staging workspace")
        if not self.state_path.exists():
            return None
        with closing(sqlite3.connect(self.state_path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
            self._check_version(connection)
            previous = connection.execute("SELECT fingerprint FROM sources WHERE source_id=?", [source.source_id]).fetchone()
            if previous and previous[0] != source.fingerprint:
                raise AcquisitionError("Source catalogue changed; use a reviewed new source ID/workspace")
            row = connection.execute("SELECT sha256,etag,last_modified,archive_path FROM cache WHERE source_id=?",
                                     [source.source_id]).fetchone()
        cache = dict(zip(("sha256", "etag", "last_modified", "archive_path"), row)) if row else None
        if cache and (not re.fullmatch(r"[0-9a-f]{64}", cache["sha256"]) or
                      cache["archive_path"] != f"raw/{source.source_id}/{cache['sha256']}.json"):
            raise AcquisitionError("Invalid content-addressed archive metadata")
        if cache and source.repository == "worldcup.json" and cache["sha256"] != WORLD_CUP_SHA256:
            raise AcquisitionError("Pinned World Cup cache checksum differs from the reviewed immutable source")
        return cache

    def _request(self, client, source, headers):
        for attempt in range(self.attempts):
            if self.last_request is not None:
                self.sleep(max(0, self.min_interval - (self.monotonic() - self.last_request)))
            self.last_request = self.monotonic()
            delay = min(2 ** attempt, 10)
            try:
                with client.stream("GET", source.url, headers=headers, timeout=20, follow_redirects=False) as response:
                    if response.status_code == 304:
                        return 304, b"", dict(response.headers)
                    if response.status_code == 429 or 500 <= response.status_code < 600:
                        delay = retry_after_delay(response.headers.get("Retry-After"), delay, limit=10)
                    else:
                        if response.status_code != 200:
                            raise AcquisitionError(f"OpenFootball returned HTTP {response.status_code}; no retry")
                        pieces, size = [], 0
                        for piece in response.iter_bytes():
                            size += len(piece)
                            if size > MAX_BYTES:
                                raise AcquisitionError("Source body exceeds the 10 MB staging bound")
                            pieces.append(piece)
                        return 200, b"".join(pieces), dict(response.headers)
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError):
                pass
            if attempt + 1 == self.attempts:
                raise AcquisitionError(f"OpenFootball fetch failed after {self.attempts} attempts")
            self.sleep(delay)
        raise AcquisitionError("OpenFootball fetch failed")

    def collect(self, source: Source, *, observed_at=None, dry_run=False) -> CollectionReport:
        """Fetch a fixed-repository source. dry_run performs validation without writes."""
        started = self.monotonic()
        cache = self._read_cache(source)
        headers = {"User-Agent": "Arena-Lakehouse-OpenFootball/0.1", "Accept": "application/json"}
        if cache and cache["etag"]:
            headers["If-None-Match"] = cache["etag"]
        elif cache and cache["last_modified"]:
            headers["If-Modified-Since"] = cache["last_modified"]
        owns_client = self.client is None
        client = self.client or httpx.Client()
        try:
            status, body, validators = self._request(client, source, headers)
        finally:
            if owns_client:
                client.close()
        observed_at = observed_at or datetime.now(timezone.utc)
        if status == 304:
            if not cache:
                raise AcquisitionError("304 has no previously archived source")
            archive = self.workspace / cache["archive_path"]
            if not archive.is_file() or _hash(archive.read_bytes()) != cache["sha256"]:
                raise AcquisitionError("Conditional response cannot use a missing or corrupt archive")
            report = self._apply(source, None, observed_at, dry_run, cache, "not_modified")
        else:
            report = self.ingest_bytes(source, body, observed_at=observed_at, dry_run=dry_run,
                                       validators=validators)
        report.duration_seconds = round(self.monotonic() - started, 6)
        return report

    def ingest_bytes(self, source: Source, body: bytes, *, observed_at: datetime,
                     dry_run=False, validators=None) -> CollectionReport:
        """Offline or fetched bytes follow the same parser, archive and revision path."""
        self._read_cache(source)
        _utc(observed_at)
        if not isinstance(body, bytes) or len(body) > MAX_BYTES:
            raise ValueError("Payload must be bytes within the 10 MB staging bound")
        checksum = _hash(body)
        if source.repository == "worldcup.json" and checksum != WORLD_CUP_SHA256:
            raise AcquisitionError("Pinned World Cup body checksum differs from the reviewed immutable source")
        archive_path = f"raw/{source.source_id}/{checksum}.json"
        if not dry_run:
            archive = self.workspace / archive_path
            archive.parent.mkdir(parents=True, exist_ok=True)
            if archive.exists():
                if _hash(archive.read_bytes()) != checksum:
                    raise AcquisitionError("Existing content-addressed archive is corrupt")
            else:
                temporary = archive.with_suffix("." + uuid.uuid4().hex + ".tmp")
                try:
                    temporary.write_bytes(body)
                    os.replace(temporary, archive)
                finally:
                    temporary.unlink(missing_ok=True)
        parsed = parse_payload(body, source, observed_at, staging_version=self.staging_version)
        validators = validators or {}
        cache = dict(sha256=checksum, archive_path=archive_path, etag=validators.get("etag"),
                     last_modified=validators.get("last-modified"))
        return self._apply(source, parsed, observed_at, dry_run, cache, "collected")

    def _apply(self, source, parsed, observed_at, dry_run, cache, status):
        observed = _utc(observed_at)
        changed = unchanged = 0
        accepted = len(parsed.rows) if parsed else 0
        # A dry run reads an existing database but never creates a workspace/cursor.
        connection = (sqlite3.connect(self.state_path.resolve().as_uri() + "?mode=ro", uri=True)
                      if dry_run and self.state_path.exists() else sqlite3.connect(":memory:")
                      if dry_run else self._connect())
        try:
            if self.state_path.exists():
                self._check_version(connection)
            if not dry_run:
                connection.execute("BEGIN IMMEDIATE")
            has_tables = connection.execute("SELECT name FROM sqlite_master WHERE name='sources'").fetchone()
            existing = connection.execute("SELECT fingerprint FROM sources WHERE source_id=?", [source.source_id]).fetchone() \
                if has_tables else None
            if existing and existing[0] != source.fingerprint:
                raise AcquisitionError("Source catalogue changed; use a reviewed new source ID/workspace")
            previous_time = connection.execute("SELECT MAX(observed_at) FROM polls WHERE source_id=?",
                                               [source.source_id]).fetchone()[0] if has_tables else None
            if previous_time and observed < previous_time:
                raise ValueError("Collection time cannot move backwards")
            if not dry_run:
                connection.execute("INSERT OR IGNORE INTO sources VALUES (?,?,?)",
                                   [source.source_id, source.fingerprint, _json(source.to_wire(self.staging_version))])
            for row in parsed.rows if parsed else []:
                business = {k: v for k, v in row.items() if k not in
                            ("observed_at", "source_sha256", "source_url", "license", "license_url")}
                content_hash = _hash(business)
                previous = connection.execute("SELECT business_hash,revision,source_id FROM current WHERE match_id=?",
                                              [row["match_id"]]).fetchone() if has_tables else None
                if previous and previous[2] != source.source_id:
                    raise AcquisitionError("Match identity already belongs to another source; review catalogue scope")
                if previous and previous[0] == content_hash:
                    unchanged += 1
                    continue
                changed += 1
                if dry_run:
                    continue
                revision = previous[1] + 1 if previous else 1
                event_id = f"{row['match_id']}:{revision}:{content_hash[:16]}"
                staged = dict(row, revision=revision, event_id=event_id)
                connection.execute("INSERT INTO history VALUES (?,?,?,?,?,?)",
                                   [event_id, source.source_id, row["match_id"], revision, observed, _json(staged)])
                connection.execute("INSERT OR REPLACE INTO current VALUES (?,?,?,?,?)",
                                   [row["match_id"], source.source_id, content_hash, revision, _json(staged)])
            if not dry_run:
                for problem in parsed.quarantine if parsed else []:
                    connection.execute("INSERT OR IGNORE INTO quarantine VALUES (?,?,?,?,?,?)",
                        [source.source_id, cache["sha256"], problem["index"] if problem["index"] is not None else -1,
                         problem["reason"], observed, _json(problem["raw"])])
                connection.execute("INSERT OR REPLACE INTO cache VALUES (?,?,?,?,?)",
                    [source.source_id, cache["sha256"], cache["etag"], cache["last_modified"], cache["archive_path"]])
                connection.execute("INSERT INTO polls(source_id,observed_at,status,sha256,changed,quarantined) "
                    "VALUES (?,?,?,?,?,?)", [source.source_id, observed, status, cache["sha256"], changed,
                                            len(parsed.quarantine) if parsed else 0])
                connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return CollectionReport(source.source_id, status, observed, cache["sha256"], accepted, changed,
            unchanged, len(parsed.quarantine) if parsed else 0, parsed.duplicates if parsed else 0,
            cache["archive_path"] if not dry_run else None, dry_run, 0.0)

    def export(self, output: Path) -> dict:
        """Publish an immutable staging package, not an Arena canonical snapshot."""
        if not self.state_path.exists():
            raise ValueError("No staging state exists")
        output = Path(output)
        if output.exists():
            raise ValueError("Staging export destination already exists")
        with closing(sqlite3.connect(self.state_path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
            self._check_version(connection)
            connection.execute("BEGIN")
            current = [json.loads(row[0]) for row in connection.execute("SELECT row_json FROM current ORDER BY match_id")]
            history = [json.loads(row[0]) for row in connection.execute(
                "SELECT row_json FROM history ORDER BY match_id,revision")]
            sources = [json.loads(row[0]) for row in connection.execute("SELECT source_json FROM sources ORDER BY source_id")]
            def records(table):
                cursor = connection.execute(f"SELECT * FROM {table} ORDER BY source_id")
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor]
            quarantine, polls = records("quarantine"), records("polls")
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(output.name + ".tmp-" + uuid.uuid4().hex)
        temporary.mkdir()
        for name, rows in (("matches", current), ("history", history), ("sources", sources),
                           ("quarantine", quarantine), ("polls", polls)):
            (temporary / f"{name}.json").write_text(_json(rows) + "\n", encoding="utf-8")
        schema = parquet_schema(self.staging_version)
        for name, rows in (("matches", current), ("history", history)):
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary / f"{name}.parquet", compression="zstd")
        files = {path.name: _hash(path.read_bytes()) for path in sorted(temporary.iterdir())}
        manifest = dict(staging_version=self.staging_version, data_kind="open_data", provider="openfootball",
            license="CC0-1.0", license_url=LICENSE_URL, canonical_compatible=False,
            generated_at=_utc(datetime.now(timezone.utc)), matches=len(current), revisions=len(history),
            quarantine_rows=len(quarantine), sources=len(sources), files=files,
            precision_note="Source dates/local wall times have no inferred UTC kickoff or completion instant"
            if self.staging_version == STAGING_VERSION else
            "UTC kickoff is derived only from an explicit source numeric offset; completion remains unknown")
        (temporary / "manifest.json").write_text(_json(manifest) + "\n", encoding="utf-8")
        os.replace(temporary, output)
        return manifest


_PARQUET_FIELDS = ("staging_version", "provider", "sport", "source_id", "competition_id", "competition",
    "season", "round", "occurrence", "match_id", "team_a_id", "team_a", "team_b_id", "team_b",
    "team_namespace", "team_identity_status", "source_date", "local_time",
    "timezone", "kickoff_precision", "start_time", "completed_at", "result_status", "score_a", "score_b",
    "score_details_json", "score_basis", "prediction_eligible", "observed_at", "source_url", "source_sha256",
    "source_ref", "license", "license_url",
    "availability_basis", "identity_basis", "quality_score", "revision", "event_id")
_PARQUET_FIELDS_V2 = _PARQUET_FIELDS + (
    "team_type", "source_match_number", "source_group", "ground", "utc_offset_minutes")


def parquet_schema(staging_version=STAGING_VERSION, *, edition=False):
    if staging_version not in {STAGING_VERSION, STAGING_VERSION_V2}:
        raise ValueError("Unsupported Parquet staging version")
    fields = _PARQUET_FIELDS if staging_version == STAGING_VERSION else _PARQUET_FIELDS_V2
    schema = [(field, pa.int64() if field in
               ("score_a", "score_b", "revision", "occurrence", "source_match_number", "utc_offset_minutes")
               else pa.float64() if field == "quality_score" else pa.bool_() if field == "prediction_eligible"
               else pa.string()) for field in fields]
    return pa.schema(schema + ([("edition_id", pa.string())] if edition else []))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path,
                        default=files("arena_lakehouse").joinpath("resources/openfootball_sources.json"))
    parser.add_argument("--workspace", type=Path, default=Path("data/openfootball"))
    parser.add_argument("--staging-version", choices=(STAGING_VERSION, STAGING_VERSION_V2), default=STAGING_VERSION,
                        help="Use a separate workspace for v2; existing v1 state is never migrated")
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect")
    collect.add_argument("--source", required=True)
    collect.add_argument("--dry-run", action="store_true")
    validate = commands.add_parser("validate")
    validate.add_argument("--source", required=True)
    validate.add_argument("--file", required=True, type=Path)
    export = commands.add_parser("export")
    export.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    collector = OpenFootballCollector(args.workspace, staging_version=args.staging_version)
    if args.command == "export":
        result = collector.export(args.output)
    else:
        source = load_sources(args.sources)[args.source]
        if args.command == "validate":
            result = asdict(collector.ingest_bytes(source, args.file.read_bytes(),
                           observed_at=datetime.now(timezone.utc), dry_run=True))
        else:
            result = asdict(collector.collect(source, dry_run=args.dry_run))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
