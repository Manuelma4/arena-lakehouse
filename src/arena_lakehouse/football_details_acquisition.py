"""Explicit, bounded acquisition of pinned CC BY 4.0 historical originals.

No scheduler, account, Current-provider join, mirror or challenge handling. The
subsequent football_details adapter selects World Cup 2018 from matches.zip.
"""

import argparse
import hashlib
import json
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from arena_lakehouse.football_details import INPUT_VERSION, LICENSE, LICENSE_URL, PROVIDER, SCOPE

MAX_METADATA_BYTES = 100_000
MAX_TOTAL_BYTES = 3_000_000
MAX_REQUESTS = 9
MAX_IO_SECONDS = 10.0
MAX_DEADLINE_SECONDS = 180.0
CHUNK_BYTES = 65_536
LEDGER_VERSION = "historical-football-acquisition/1.0.0"
DOWNLOAD_HOST = "s3-eu-west-1.amazonaws.com"


@dataclass(frozen=True)
class Source:
    role: str
    article_id: int
    article_version: int
    file_id: int
    filename: str
    bytes: int
    md5: str
    sha256: str
    published_at: str
    title: str

    @property
    def metadata_url(self):
        return f"https://api.figshare.com/v2/articles/{self.article_id}/versions/{self.article_version}"

    @property
    def download_url(self):
        return f"https://ndownloader.figshare.com/files/{self.file_id}"

    @property
    def metadata_filename(self):
        return f"article-{self.article_id}-v{self.article_version}.json"

    @property
    def storage_path(self):
        return f"/pfigshare-u-files/{self.file_id}/{self.filename}"


# Fixed original-body pins verified against the original Figshare captures.
# Metadata is verified structurally and hashed anew; no old capture clock is reused.
SOURCES = (
    Source("matches", 7770422, 1, 14464622, "matches.zip", 645097,
           "51d80beb17480919f69a53a0152c2d71",
           "c8f92bb7533e5c127e043cee764c991b5c25b4f5e70a65be931baae0b1765ce9",
           "2019-02-26T12:15:54Z", "Matches"),
    Source("players", 7765196, 3, 15073721, "players.json", 1737347,
           "f28ddf6326281efeda6488b2169f5609",
           "877a111cb1005b73df5645e9338bd74fb4b496bace2fbc545a72abb3b73efa2e",
           "2019-05-06T13:14:23Z", "Players"),
    Source("teams", 7765310, 3, 15073697, "teams.json", 27404,
           "1381ff9449f21105090729cf0e086b5b",
           "9f7a4a3b3d92c0be33f40613ad6e6eb4316c3b9771ec74c61a22c9b8ece23a4d",
           "2019-05-06T13:08:43Z", "Teams"),
)


class AcquisitionError(ValueError):
    """Sanitized error code, without remote URLs, bodies or local secrets."""

    def __init__(self, code):
        super().__init__(code if isinstance(code, str) and re.fullmatch(r"[a-z][a-z0-9_]{1,80}", code)
                         else "invalid_error_code")


def _require(condition, code):
    if not condition:
        raise AcquisitionError(code)


def _json(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def _sha(body):
    return hashlib.sha256(body).hexdigest()


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def _decode(body):
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, RecursionError):
        raise AcquisitionError("invalid_json") from None


def _stamp(value):
    _require(isinstance(value, datetime) and value.tzinfo is not None
             and value.utcoffset().total_seconds() == 0, "invalid_capture_clock")
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _clock(value):
    try:
        clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        raise AcquisitionError("invalid_capture_clock") from None
    _require(clock.tzinfo is not None and clock.utcoffset().total_seconds() == 0, "invalid_capture_clock")
    return clock


def validate_metadata(body, source):
    """Check the explicit public version, license and original file declaration."""
    _require(0 < len(body) <= MAX_METADATA_BYTES, "metadata_size_bound")
    value = _decode(body)
    _require(isinstance(value, dict), "invalid_metadata")
    for key, expected in (("id", source.article_id), ("version", source.article_version)):
        _require(type(value.get(key)) is int and value[key] == expected, "metadata_version_mismatch")
    _require(value.get("doi") == f"10.6084/m9.figshare.{source.article_id}.v{source.article_version}"
             and value.get("title") == source.title, "metadata_identity_mismatch")
    _require(value.get("is_public") is True, "metadata_not_public")
    for flag in ("is_confidential", "is_embargoed", "is_metadata_record", "download_disabled", "has_linked_file"):
        _require(value.get(flag) is False, "metadata_download_not_public")
    license_value = value.get("license")
    _require(isinstance(license_value, dict) and license_value.get("name") == LICENSE
             and license_value.get("url") == LICENSE_URL and type(license_value.get("value")) is int
             and license_value["value"] == 1, "metadata_license_mismatch")
    _require(value.get("published_date") == source.published_at, "metadata_publication_mismatch")
    authors = value.get("authors")
    _require(isinstance(authors, list) and len(authors) == 2
             and all(isinstance(author, dict) and isinstance(author.get("full_name"), str) for author in authors)
             and {author.get("full_name") for author in authors} == {"Luca Pappalardo", "Emanuele Massucco"},
             "metadata_authors_mismatch")
    files = value.get("files")
    _require(isinstance(files, list) and len(files) == 1 and isinstance(files[0], dict), "metadata_files_mismatch")
    file = files[0]
    _require(type(file.get("id")) is int and file["id"] == source.file_id
             and file.get("name") == source.filename and type(file.get("size")) is int
             and file["size"] == source.bytes and file.get("is_link_only") is False
             and file.get("download_url") == source.download_url
             and file.get("supplied_md5") == source.md5 and file.get("computed_md5") == source.md5,
             "metadata_file_mismatch")
    return value


def _verify_body(body, source):
    _require(len(body) == source.bytes, "original_size_mismatch")
    _require(_sha(body) == source.sha256 and hashlib.md5(body).hexdigest() == source.md5,
             "original_digest_mismatch")


def _storage_redirect(location, source):
    _require(isinstance(location, str) and 0 < len(location) <= 8192
             and not any(ord(char) < 33 or ord(char) == 127 for char in location), "invalid_redirect")
    try:
        target = urlsplit(location)
        port = target.port
    except ValueError:
        raise AcquisitionError("invalid_redirect") from None
    _require(target.scheme == "https" and target.hostname == DOWNLOAD_HOST
             and target.netloc == DOWNLOAD_HOST and port is None and target.username is None
             and target.password is None and not target.fragment and target.path == source.storage_path,
             "unapproved_redirect")
    # Its query is supplied by Figshare, followed unchanged, and never persisted.
    return location


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _Response:
    def __init__(self, response):
        self.response = response
        self.status = response.status
        self.headers = response.headers

    def read_chunk(self, maximum, timeout):
        # urllib HTTPResponse exposes BufferedReader.raw._sock. HTTPError wraps
        # that response once. Fail closed if a transport cannot bound the read.
        stream = self.response
        for _ in range(3):
            if callable(getattr(stream, "isclosed", None)) and stream.isclosed():
                return b""
            raw = getattr(getattr(stream, "fp", None), "raw", None)
            sock = getattr(raw, "_sock", None)
            if sock is not None:
                sock.settimeout(timeout)
                return self.response.read1(maximum)
            stream = getattr(stream, "fp", None)
        raise AcquisitionError("unsupported_bounded_transport")

    def close(self):
        self.response.close()


class PublicTransport:
    """Direct HTTPS only; no environmental proxy, credentials or redirect handler."""

    def __init__(self):
        self.opener = build_opener(ProxyHandler({}), _NoRedirect())

    def open(self, url, timeout):
        request = Request(url, headers={"User-Agent": "ArenaHistoricalResearch/1.0",
                                        "Accept-Encoding": "identity", "Accept": "application/json, */*"})
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        return _Response(response)


class _Budget:
    def __init__(self, seconds, monotonic, utc_now):
        self.monotonic = monotonic
        self.utc_now = utc_now
        self.started = monotonic()
        self.deadline = self.started + seconds
        self.bytes = 0
        self.requests = []

    def remaining(self):
        remaining = self.deadline - self.monotonic()
        _require(remaining > 0, "acquisition_deadline_exceeded")
        return min(MAX_IO_SECONDS, remaining)

    def now(self):
        return _stamp(self.utc_now())

    def request(self, transport, url, source, kind, maximum):
        timeout = self.remaining()
        _require(len(self.requests) < MAX_REQUESTS, "request_budget_exceeded")
        entry = dict(sequence=len(self.requests) + 1, role=source.role, kind=kind,
                     started_at=self.now(), origin=(f"https://{DOWNLOAD_HOST}" if kind == "storage"
                                                   else f"https://{urlsplit(url).hostname}"),
                     path=(source.storage_path if kind == "storage" else urlsplit(url).path),
                     status=None, bytes_read=0, body_complete=False)
        self.requests.append(entry)
        response = None
        try:
            response = transport.open(url, timeout)
            self.remaining()
            status = response.status
            _require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
            entry["status"] = status
            if status in (301, 302, 303, 307, 308):
                _require(kind == "download" and status == 302, "unapproved_redirect_status")
                location = _storage_redirect(response.headers.get("Location"), source)
                entry["redirect_origin"] = f"https://{DOWNLOAD_HOST}"
                entry["redirect_path"] = source.storage_path
                return None, location, entry
            _require(status == 200, "http_response_not_ok")
            _require(response.headers.get("Content-Encoding", "identity").lower() == "identity",
                     "encoded_body_not_allowed")
            length = response.headers.get("Content-Length")
            if length is not None:
                _require(isinstance(length, str) and length.isascii() and length.isdecimal()
                         and 0 < int(length) <= maximum, "response_size_bound")
                if kind != "metadata":
                    _require(int(length) == source.bytes, "original_size_mismatch")
            body = bytearray()
            while True:
                allowance = min(maximum - len(body), MAX_TOTAL_BYTES - self.bytes)
                _require(allowance >= 0, "total_response_size_bound")
                chunk = response.read_chunk(min(CHUNK_BYTES, allowance + 1), self.remaining())
                self.remaining()
                _require(isinstance(chunk, bytes), "invalid_response_chunk")
                self.bytes += len(chunk)
                entry["bytes_read"] += len(chunk)
                _require(self.bytes <= MAX_TOTAL_BYTES, "total_response_size_bound")
                _require(len(chunk) <= min(CHUNK_BYTES, allowance + 1)
                         and len(body) + len(chunk) <= maximum, "response_size_bound")
                if not chunk:
                    break
                body.extend(chunk)
            _require(body, "empty_response")
            if length is not None:
                _require(len(body) == int(length), "response_length_mismatch")
            entry.update(body_complete=True, sha256=_sha(body), observed_at=self.now())
            return bytes(body), None, entry
        except AcquisitionError as error:
            entry["error"] = str(error)
            raise
        except Exception:
            entry["error"] = "transport_failure"
            raise AcquisitionError("transport_failure") from None
        finally:
            entry["finished_at"] = self.now()
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass


def _safe_path(path):
    for part in (path, *path.parents):
        if part.exists() or part.is_symlink():
            stat = part.lstat()
            _require(not part.is_symlink() and not getattr(stat, "st_file_attributes", 0) & 0x400,
                     "output_link_not_allowed")


def _read(path, maximum):
    _safe_path(path)
    _require(path.is_file() and 0 < path.stat().st_size <= maximum, "existing_capture_invalid")
    with path.open("rb") as stream:
        body = stream.read(maximum + 1)
    _require(0 < len(body) <= maximum, "existing_capture_invalid")
    return body


def _atomic_write(path, body, *, replace_owned=False):
    temporary = path.with_name(f".part-{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as stream:
            _require(stream.write(body) == len(body), "local_write_failure")
            stream.flush()
            os.fsync(stream.fileno())
        _require(not path.is_symlink() and (not path.exists() or replace_owned), "existing_capture_conflict")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _validate_ledger(ledger, sources, descriptor_hash):
    fields = {"schema_version", "provider", "scope", "status", "started_at", "finished_at", "bytes_read",
              "request_count", "requests", "source_manifest_sha256", "limits", "redirect_policy"}
    _require(isinstance(ledger, dict) and set(ledger) == fields and ledger.get("schema_version") == LEDGER_VERSION
             and ledger.get("provider") == PROVIDER and ledger.get("scope") == SCOPE
             and ledger.get("status") == "acquired" and ledger.get("source_manifest_sha256") == descriptor_hash,
             "existing_capture_invalid")
    requests = ledger["requests"]
    _require(isinstance(requests, list) and 6 <= len(requests) <= MAX_REQUESTS
             and type(ledger["request_count"]) is int and ledger["request_count"] == len(requests),
             "existing_capture_invalid")
    limits = ledger["limits"]
    fixed = dict(requests=MAX_REQUESTS, response_bytes=MAX_TOTAL_BYTES, metadata_bytes=MAX_METADATA_BYTES,
                 socket_seconds=MAX_IO_SECONDS)
    _require(isinstance(limits, dict) and set(limits) == set(fixed) | {"deadline_seconds"}
             and all(limits.get(key) == value for key, value in fixed.items())
             and type(limits["deadline_seconds"]) in (int, float) and math.isfinite(limits["deadline_seconds"])
             and 0 < limits["deadline_seconds"] <= MAX_DEADLINE_SECONDS
             and ledger["redirect_policy"] == "One unchanged Figshare 302 to pinned publisher storage; query not retained",
             "existing_capture_invalid")
    observations, index, read_bytes = {}, 0, 0
    last_clock = _clock(ledger["started_at"])
    base = {"sequence", "role", "kind", "started_at", "origin", "path", "status", "bytes_read",
            "body_complete", "finished_at"}
    for source in sources:
        observations[source.role] = {}
        kinds = ["metadata", "download"]
        for kind in kinds:
            _require(index < len(requests) and isinstance(requests[index], dict), "existing_capture_invalid")
            entry = requests[index]
            index += 1
            redirected = kind == "download" and entry.get("status") == 302
            extras = {"redirect_origin", "redirect_path"} if redirected else {"sha256", "observed_at"}
            expected_url = source.metadata_url if kind == "metadata" else source.download_url
            origin = f"https://{DOWNLOAD_HOST}" if kind == "storage" else f"https://{urlsplit(expected_url).hostname}"
            path = source.storage_path if kind == "storage" else urlsplit(expected_url).path
            _require(set(entry) == base | extras and type(entry["sequence"]) is int and entry["sequence"] == index
                     and entry["role"] == source.role and entry["kind"] == kind
                     and entry["origin"] == origin and entry["path"] == path
                     and type(entry["bytes_read"]) is int and entry["bytes_read"] >= 0,
                     "existing_capture_invalid")
            started, finished = _clock(entry["started_at"]), _clock(entry["finished_at"])
            _require(last_clock <= started <= finished, "existing_capture_invalid")
            last_clock = finished
            if redirected:
                _require(entry["body_complete"] is False and entry["bytes_read"] == 0
                         and entry["redirect_origin"] == f"https://{DOWNLOAD_HOST}"
                         and entry["redirect_path"] == source.storage_path, "existing_capture_invalid")
                kinds.append("storage")
            else:
                _require(entry["status"] == 200 and entry["body_complete"] is True
                         and isinstance(entry["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
                         and 0 < entry["bytes_read"] <= (MAX_METADATA_BYTES if kind == "metadata" else source.bytes)
                         and started <= _clock(entry["observed_at"]) <= finished,
                         "existing_capture_invalid")
                observations[source.role]["metadata" if kind == "metadata" else "download"] = entry
            read_bytes += entry["bytes_read"]
    _require(index == len(requests) and type(ledger["bytes_read"]) is int
             and ledger["bytes_read"] == read_bytes <= MAX_TOTAL_BYTES
             and last_clock <= _clock(ledger["finished_at"]), "existing_capture_invalid")
    return observations


def _verify_existing(output, sources):
    expected = {"source-manifest.json", "acquisition.json"} | {
        name for source in sources for name in (source.filename, source.metadata_filename)}
    _require({path.name for path in output.iterdir()} == expected | {".acquisition.lock"},
             "existing_capture_incomplete_or_unrelated")
    descriptor_body = _read(output / "source-manifest.json", MAX_METADATA_BYTES)
    descriptor = _decode(descriptor_body)
    _require(isinstance(descriptor, dict) and set(descriptor) == {"schema_version", "provider", "scope", "sources"}
             and descriptor.get("schema_version") == INPUT_VERSION and descriptor.get("provider") == PROVIDER
             and descriptor.get("scope") == SCOPE and isinstance(descriptor.get("sources"), list)
             and len(descriptor["sources"]) == len(sources), "existing_capture_invalid")
    ledger = _decode(_read(output / "acquisition.json", MAX_METADATA_BYTES))
    observations = _validate_ledger(ledger, sources, _sha(descriptor_body))
    for source, row in zip(sources, descriptor["sources"], strict=True):
        body = _read(output / source.filename, source.bytes)
        _verify_body(body, source)
        metadata = _read(output / source.metadata_filename, MAX_METADATA_BYTES)
        validate_metadata(metadata, source)
        expected_row = dict(role=source.role, article_id=source.article_id, article_version=source.article_version,
                            file_id=source.file_id, filename=source.filename, url=source.download_url,
                            sha256=source.sha256, bytes=source.bytes, md5=source.md5, license=LICENSE,
                            license_url=LICENSE_URL, metadata_sha256=_sha(metadata), published_at=source.published_at)
        _require(isinstance(row, dict) and set(row) == set(expected_row) | {"observed_at", "metadata_observed_at"}
                 and all(row.get(key) == value for key, value in expected_row.items()), "existing_capture_invalid")
        _require(_clock(row["published_at"]) <= _clock(row["metadata_observed_at"]) <= _clock(row["observed_at"]),
                 "existing_capture_invalid")
        captured = observations[source.role]
        _require(captured["metadata"]["observed_at"] == row["metadata_observed_at"]
                 and captured["metadata"]["bytes_read"] == len(metadata)
                 and captured["metadata"]["sha256"] == _sha(metadata)
                 and captured["download"]["observed_at"] == row["observed_at"]
                 and captured["download"]["bytes_read"] == source.bytes
                 and captured["download"]["sha256"] == source.sha256, "existing_capture_invalid")
    return dict(status="already_verified", schema_version=INPUT_VERSION, source_manifest_sha256=_sha(descriptor_body),
                requests=0, bytes_read=0, original_bytes=sum(source.bytes for source in sources))


def acquire(output, *, deadline_seconds=120.0, transport=None, monotonic=time.monotonic,
            utc_now=lambda: datetime.now(timezone.utc)):
    """Acquire once, or verify a complete immutable capture without requests."""
    _require(type(deadline_seconds) in (float, int) and math.isfinite(deadline_seconds)
             and 0 < deadline_seconds <= MAX_DEADLINE_SECONDS, "invalid_deadline_seconds")
    output = Path(output).absolute()
    _safe_path(output)
    _require(not output.exists() or output.is_dir(), "output_not_directory")
    output.mkdir(parents=True, exist_ok=True)
    lock = output / ".acquisition.lock"
    try:
        with lock.open("xb") as stream:
            stream.write(b"Exclusive manual acquisition; do not bypass an existing lock.\n")
    except FileExistsError:
        raise AcquisitionError("acquisition_locked") from None
    budget = None
    ledger_written = False
    try:
        entries = {path.name for path in output.iterdir()} - {lock.name}
        if entries:
            _require("source-manifest.json" in entries, "existing_capture_incomplete_or_unrelated")
            return _verify_existing(output, SOURCES)
        budget = _Budget(deadline_seconds, monotonic, utc_now)
        transport = transport if transport is not None else PublicTransport()
        descriptor = dict(schema_version=INPUT_VERSION, provider=PROVIDER, scope=SCOPE, sources=[])
        files = {}
        for source in SOURCES:
            metadata, _, metadata_entry = budget.request(transport, source.metadata_url, source, "metadata",
                                                        MAX_METADATA_BYTES)
            validate_metadata(metadata, source)
            body, redirect, entry = budget.request(transport, source.download_url, source, "download", source.bytes)
            if redirect is not None:
                body, _, entry = budget.request(transport, redirect, source, "storage", source.bytes)
            _verify_body(body, source)
            _require(_clock(source.published_at) <= _clock(metadata_entry["observed_at"])
                     <= _clock(entry["observed_at"]), "capture_clock_order_invalid")
            descriptor["sources"].append(dict(role=source.role, article_id=source.article_id,
                article_version=source.article_version, file_id=source.file_id, filename=source.filename,
                url=source.download_url, sha256=source.sha256, bytes=source.bytes, md5=source.md5,
                observed_at=entry["observed_at"], metadata_observed_at=metadata_entry["observed_at"],
                metadata_sha256=_sha(metadata), published_at=source.published_at, license=LICENSE,
                license_url=LICENSE_URL))
            files[source.metadata_filename] = metadata
            files[source.filename] = body
        manifest = _json(descriptor)
        budget.remaining()
        for name, body in files.items():
            budget.remaining()
            _atomic_write(output / name, body)
            _require(_read(output / name, len(body)) == body, "local_write_failure")
        ledger = dict(schema_version=LEDGER_VERSION, provider=PROVIDER, scope=SCOPE, status="acquired",
                      started_at=budget.requests[0]["started_at"], finished_at=budget.now(),
                      bytes_read=budget.bytes, request_count=len(budget.requests), requests=budget.requests,
                      source_manifest_sha256=_sha(manifest),
                      limits=dict(requests=MAX_REQUESTS, response_bytes=MAX_TOTAL_BYTES,
                                  metadata_bytes=MAX_METADATA_BYTES, socket_seconds=MAX_IO_SECONDS,
                                  deadline_seconds=deadline_seconds),
                      redirect_policy="One unchanged Figshare 302 to pinned publisher storage; query not retained")
        _validate_ledger(ledger, SOURCES, _sha(manifest))
        _atomic_write(output / "acquisition.json", _json(ledger))
        ledger_written = True
        # No usable input descriptor exists until every body and the ledger is durable.
        budget.remaining()
        _atomic_write(output / "source-manifest.json", manifest)
        return dict(status="acquired", schema_version=INPUT_VERSION, source_manifest_sha256=_sha(manifest),
                    requests=len(budget.requests), bytes_read=budget.bytes,
                    original_bytes=sum(source.bytes for source in SOURCES))
    except Exception as error:
        # Keep a bounded sanitized failure ledger, without replacing existing captures.
        if budget is not None and not (output / "source-manifest.json").exists():
            failure = dict(schema_version=LEDGER_VERSION, provider=PROVIDER, scope=SCOPE, status="failed",
                           error=str(error) if isinstance(error, AcquisitionError) else "local_write_failure",
                           finished_at=budget.now(), bytes_read=budget.bytes, request_count=len(budget.requests),
                           requests=budget.requests)
            if ledger_written or not (output / "acquisition.json").exists():
                try:
                    _atomic_write(output / "acquisition.json", _json(failure), replace_owned=ledger_written)
                except (OSError, AcquisitionError):
                    pass
        if isinstance(error, AcquisitionError):
            raise
        raise AcquisitionError("local_write_failure") from None
    finally:
        lock.unlink()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Manually acquire pinned CC BY 4.0 historical football originals")
    parser.add_argument("--output", required=True, type=Path, help="Dedicated immutable local originals directory")
    parser.add_argument("--deadline-seconds", type=float, default=120.0, help="Checked total deadline, at most 180 seconds")
    args = parser.parse_args(argv)
    try:
        result = acquire(args.output, deadline_seconds=args.deadline_seconds)
    except (AcquisitionError, OSError) as error:
        code = str(error) if isinstance(error, AcquisitionError) else "local_output_failure"
        parser.exit(1, f"Historical acquisition failed: {code}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
