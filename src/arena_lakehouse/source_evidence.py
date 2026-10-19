"""Export immutable, bounded operational metadata without credentials or match rows.

The companion describes retained ingestion evidence. Failure aggregates are not
an exhaustive quality assessment, and retained events are not a quality rate.
"""

import argparse
import ctypes
import errno
import hashlib
import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pyarrow.parquet as pq

SCHEMA = "arena-source-evidence-1.0"
METADATA = ("catalog.json", "metrics.json", "lineage.json", "quality.json", "incidents.json")
SOURCE_FILES = {"source-manifest.json", *METADATA}
PACKAGE_FILES = {"manifest.json", *SOURCE_FILES}
HEX64 = re.compile(r"^[a-f0-9]{64}$")
HEX16 = re.compile(r"^[a-f0-9]{16}$")
MIB = 1_048_576
REQUIRED = {*METADATA, "predictions.json", "model_report.json", "gold/matches.parquet",
            "gold/team_stats.parquet"}
TABLES = {"matches", "team_stats", "match_history"}


class SourceEvidenceError(ValueError):
    """The source or output cannot support an immutable evidence package."""


def _fail(message):
    raise SourceEvidenceError(message)


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value, *, ensure_ascii=False):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=ensure_ascii, allow_nan=False).encode()


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                _fail("Duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        _fail("Nonfinite JSON value")

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
        def finite(item):
            if isinstance(item, float) and not math.isfinite(item):
                _fail("Nonfinite JSON value")
            if isinstance(item, dict):
                for child in item.values():
                    finite(child)
            elif isinstance(item, list):
                for child in item:
                    finite(child)
        finite(value)
        return value
    except (UnicodeError, ValueError, RecursionError) as error:
        raise SourceEvidenceError("Invalid JSON document") from error


def _keys(value, names):
    if not isinstance(value, dict) or set(value) != set(names):
        _fail("Unexpected document fields")


def _text(value, maximum=2_000):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or any(
            ord(char) < 32 or ord(char) == 127 for char in value):
        _fail("Invalid bounded text")


def _number(value):
    if type(value) is not int or not 0 <= value <= 2**53 - 1:
        _fail("Invalid aggregate count")


def _time(value):
    if not isinstance(value, str) or len(value) > 40 or "T" not in value:
        _fail("Invalid UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            _fail("Timestamps must be timezone aware")
        return parsed.astimezone(timezone.utc)
    except ValueError as error:
        raise SourceEvidenceError("Invalid UTC timestamp") from error


def _safe_chain(path):
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            _fail("Symlinks and reparse points are forbidden")


def _read(path, maximum):
    _safe_chain(path)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
        _fail("Unsafe or oversized source file")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        _fail("Oversized source file")
    return raw


def _name(name):
    if not isinstance(name, str) or len(name) > 240 or any(ord(char) < 32 or ord(char) == 127 for char in name):
        _fail("Unsafe manifest path")
    pure = PurePosixPath(name)
    if (pure.is_absolute() or ".." in pure.parts or "\\" in name or ":" in name
            or str(pure) != name or not pure.parts or any(part.endswith((" ", ".")) for part in pure.parts)):
        _fail("Unsafe manifest path")
    return pure


def _inventory(root):
    actual = set()
    for item in root.rglob("*"):
        _safe_chain(item)
        if item.is_file():
            actual.add(item.relative_to(root).as_posix())
        elif not item.is_dir():
            _fail("Unsupported source entry")
    return actual


def _metadata(documents, root):
    catalog = documents["catalog.json"]
    if not isinstance(catalog, list) or len(catalog) not in (2, 3):
        _fail("Invalid catalog")
    names = set()
    for table in catalog:
        _keys(table, ("name", "layer", "description", "columns", "primary_key"))
        name = table["name"]
        if not isinstance(name, str) or name not in TABLES or name in names or table["layer"] != "gold":
            _fail("Invalid catalog table")
        names.add(name)
        _text(table["description"])
        columns = table["columns"]
        if not isinstance(columns, list) or not 1 <= len(columns) <= 128:
            _fail("Invalid catalog columns")
        actual_schema = pq.read_schema(root / f"gold/{name}.parquet")
        declared = []
        for column in columns:
            _keys(column, ("name", "type", "description"))
            _text(column["name"], 120)
            _text(column["type"], 120)
            _text(column["description"])
            declared.append((column["name"], column["type"]))
        if (len({column[0] for column in declared}) != len(declared)
                or declared != [(field.name, str(field.type)) for field in actual_schema]):
            _fail("Catalog disagrees with Parquet schema")
        key = table["primary_key"]
        expected_key = (["sport", "team_id"] if name == "team_stats" else
                        ["match_id", "revision"] if name == "match_history" else ["match_id"])
        if key != expected_key:
            _fail("Invalid catalog primary key")
    actual_tables = {path.stem for path in (root / "gold").glob("*.parquet")}
    if not {"matches", "team_stats"} <= names or names != actual_tables:
        _fail("Catalog does not describe actual Gold tables")
    metrics = documents["metrics.json"]
    if not isinstance(metrics, list) or not 1 <= len(metrics) <= 128:
        _fail("Invalid metrics")
    metric_names = set()
    for metric in metrics:
        _keys(metric, ("name", "description", "unit", "table", "formula"))
        for key in ("name", "unit", "table"):
            _text(metric[key], 120)
        for key in ("description", "formula"):
            _text(metric[key])
        if metric["table"] not in names or metric["name"] in metric_names:
            _fail("Invalid metric table or duplicate name")
        metric_names.add(metric["name"])
    lineage = documents["lineage.json"]
    if not isinstance(lineage, list) or not 1 <= len(lineage) <= 128:
        _fail("Invalid lineage")
    edges = set()
    for edge in lineage:
        _keys(edge, ("source", "target", "transformation"))
        for key in ("source", "target"):
            _text(edge[key], 120)
        _text(edge["transformation"])
        pair = (edge["source"], edge["target"])
        if not set(pair) <= (names | {"provider_api_or_replay", "bronze_events", "predictions"}) or pair[0] == pair[1] or pair in edges:
            _fail("Invalid lineage reference")
        edges.add(pair)
    graph = {}
    for source, target in edges:
        graph.setdefault(source, set()).add(target)
    visiting, visited = set(), set()

    def visit(node):
        if node in visiting:
            _fail("Cyclic declared lineage")
        if node in visited:
            return
        visiting.add(node)
        for target in graph.get(node, ()):
            visit(target)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node)
    quality = documents["quality.json"]
    _keys(quality, ("run_id", "checks", "summary"))
    _text(quality["run_id"], 120)
    summary = quality["summary"]
    _keys(summary, ("valid_events", "quarantined", "last_run_status"))
    for key in ("valid_events", "quarantined"):
        _number(summary[key])
    if summary["last_run_status"] != "succeeded":
        _fail("Source ingest has not succeeded")
    checks = quality["checks"]
    if not isinstance(checks, list) or len(checks) > 128:
        _fail("Invalid quality checks")
    check_keys = set()
    for check in checks:
        _keys(check, ("rule", "table", "passed", "failed", "severity"))
        _text(check["rule"], 120)
        if check["table"] != "bronze_events" or check["severity"] not in ("warning", "error"):
            _fail("Invalid quality scope")
        for key in ("passed", "failed"):
            _number(check[key])
        identity = (check["rule"], check["table"], check["severity"])
        if identity in check_keys or check["failed"] == 0 or check["passed"] != summary["valid_events"]:
            _fail("Invalid failure aggregate")
        check_keys.add(identity)
    if sum(check["failed"] for check in checks) != summary["quarantined"]:
        _fail("Quality failure totals disagree")
    incidents = documents["incidents.json"]
    if not isinstance(incidents, list) or len(incidents) != len(checks):
        _fail("Incident and quality totals disagree")
    expected = {(check["rule"], check["table"], check["severity"]): check for check in checks}
    seen = set()
    for incident in incidents:
        _keys(incident, ("incident_id", "rule", "table", "severity", "description", "status",
                         "event_count", "remediation"))
        for key in ("rule", "table", "severity", "status"):
            _text(incident[key], 120)
        for key in ("description", "remediation"):
            _text(incident[key])
        identity = (incident["rule"], incident["table"], incident["severity"])
        check = expected.get(identity)
        _number(incident["event_count"])
        # Existing producer digest uses unescaped UTF-8; retain its native identity.
        native_digest = (_hash(json.dumps(check, sort_keys=True, separators=(",", ":"),
                                         ensure_ascii=False).encode())[:16] if check else None)
        if (check is None or identity in seen or incident["event_count"] != check["failed"]
                or incident["status"] != "open" or incident["incident_id"] != native_digest):
            _fail("Incident does not match its failure aggregate")
        seen.add(identity)


def _capture(root):
    manifest_raw = _read(root / "manifest.json", 2 * MIB)
    manifest = _json(manifest_raw)
    _keys(manifest, ("schema_version", "dataset_version", "producer_version", "generated_at",
                     "as_of", "data_kind", "providers", "files"))
    if manifest["schema_version"] != "2.0" or not isinstance(manifest["dataset_version"], str) or not HEX16.fullmatch(
            manifest["dataset_version"]):
        _fail("Unsupported source snapshot")
    _text(manifest["producer_version"], 80)
    if _time(manifest["as_of"]) > _time(manifest["generated_at"]):
        _fail("Source generated before its observation cutoff")
    providers = manifest["providers"]
    if not isinstance(providers, list) or not providers or any(type(p) is not str for p in providers):
        _fail("Invalid providers")
    if providers != sorted(set(providers)) or not (
            manifest["data_kind"] == "synthetic" and providers == ["synthetic"] or
            manifest["data_kind"] == "api" and set(providers) <= {"football-data", "pandascore"}):
        _fail("Provider provenance disagrees with data kind")
    files = manifest["files"]
    if not isinstance(files, dict) or not REQUIRED <= set(files) or len(files) > 256 or not any(
            name.startswith("docs/") and name.endswith(".md") for name in files):
        _fail("Missing required source payload")
    for name, checksum in files.items():
        _name(name)
        if not isinstance(checksum, str) or not HEX64.fullmatch(checksum):
            _fail("Invalid source checksum")
    if len({name.casefold() for name in files}) != len(files) or "manifest.json" in files:
        _fail("Ambiguous source paths")
    if _inventory(root) != {"manifest.json", *files}:
        _fail("Source inventory mismatch")
    declared_directories = {parent.as_posix() for name in files for parent in PurePosixPath(name).parents
                            if parent.as_posix() != "."}
    actual_directories = {item.relative_to(root).as_posix() for item in root.rglob("*") if item.is_dir()}
    if actual_directories != declared_directories:
        _fail("Unexpected source directory")
    raw = {"source-manifest.json": manifest_raw}
    total = 0
    for name, checksum in files.items():
        total += (root / name).stat().st_size
        if total > 512_000_000:
            _fail("Source exceeds size limit")
        limit = MIB if name in METADATA else 512_000_000
        content = _read(root / name, limit)
        if _hash(content) != checksum:
            _fail("Source checksum mismatch")
        if name.endswith(".json"):
            _json(content)
        if name in METADATA:
            raw[name] = content
    documents = {name: _json(raw[name]) for name in METADATA}
    _metadata(documents, root)
    # The companion never copies these rows, but the immutable source still has
    # to provide the canonical relations that its metadata describes.
    matches = pq.read_table(root / "gold/matches.parquet").to_pylist()
    for table, required_columns in {
        "matches": {"match_id", "sport", "team_a", "team_b", "status", "start_time"},
        "team_stats": {"sport", "team_id", "team_name", "matches", "wins", "draws", "losses", "win_rate"},
    }.items():
        schema_names = pq.read_schema(root / f"gold/{table}.parquet").names
        if len(schema_names) != len(set(schema_names)) or not required_columns <= set(schema_names):
            _fail("Invalid canonical table schema")
    by_id = {}
    for row in matches:
        if (not isinstance(row.get("match_id"), str) or row["match_id"] in by_id
                or row.get("sport") not in ("football", "valorant")
                or row.get("status") not in ("scheduled", "running", "finished", "cancelled", "postponed")):
            _fail("Invalid canonical match")
        _time(row["start_time"])
        by_id[row["match_id"]] = row
    predictions = _json(_read(root / "predictions.json", 512_000_000))
    if not isinstance(predictions, list):
        _fail("Invalid prediction payload")
    for prediction in predictions:
        match = by_id.get(prediction.get("match_id")) if isinstance(prediction, dict) else None
        if not match or match["status"] != "scheduled" or prediction.get("sport") != match["sport"]:
            _fail("Prediction does not match canonical data")
        probabilities = prediction.get("probabilities")
        classes = {"a", "b", "draw"} if match["sport"] == "football" else {"a", "b"}
        if (not isinstance(probabilities, dict) or set(probabilities) != classes
                or any(type(value) not in (int, float) or not 0 <= value <= 1 for value in probabilities.values())
                or not math.isclose(sum(probabilities.values()), 1, abs_tol=1e-6)
                or prediction.get("data_kind") != manifest["data_kind"]
                or _time(prediction["as_of"]) != _time(manifest["as_of"])
                or _time(prediction["as_of"]) >= _time(match["start_time"])):
            _fail("Invalid temporal prediction evidence")
    return manifest, raw


def _publish(stage, destination):
    """Atomic no-replace directory publication on Windows and supported Linux.

    Unsupported platforms/filesystems fail closed instead of emulating a
    no-replace operation with a racy existence check.
    """
    if sys.platform == "win32":
        stage.rename(destination)
        return
    if not sys.platform.startswith("linux"):
        _fail("Atomic no-replace publication is unavailable on this platform")
    try:
        rename = ctypes.CDLL(None, use_errno=True).renameat2
    except (AttributeError, OSError) as error:
        raise SourceEvidenceError("Atomic no-replace publication is unavailable") from error
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = rename(-100, os.fsencode(stage), -100, os.fsencode(destination), 1)
    if result != 0:
        code = ctypes.get_errno()
        if code in (errno.ENOSYS, errno.EINVAL, errno.ENOTSUP):
            _fail("Atomic no-replace publication is unavailable on this filesystem")
        raise OSError(code, os.strerror(code))


def export_source_evidence(snapshot, output, *, generated_at=None):
    """Validate a full snapshot, then atomically export seven exact package files.

    ``output`` is the final directory, not a parent containing multiple exports.
    An existing directory is reusable only when every byte matches this export.
    """
    stage = None
    try:
        root = Path(snapshot).absolute()
        destination = Path(output).absolute()
        _safe_chain(root)
        _safe_chain(destination)
        root = root.resolve(strict=True)
        destination = destination.resolve()
        if not root.is_dir() or root.is_relative_to(destination) or destination.is_relative_to(root):
            _fail("Source and destination must not overlap")
        clock = datetime.now(timezone.utc) if generated_at is None else generated_at
        if not isinstance(clock, datetime) or clock.tzinfo is None or clock.utcoffset() is None:
            _fail("Export requires an aware publication clock")
        clock = clock.astimezone(timezone.utc)
        manifest, raw = _capture(root)
        if clock < _time(manifest["generated_at"]) or clock > datetime.now(timezone.utc):
            _fail("Package generated before source publication")
        source = {key: manifest[key] for key in ("schema_version", "dataset_version", "producer_version",
                                                "data_kind", "providers", "generated_at", "as_of")}
        source["snapshot_fingerprint"] = _hash(_canonical({
            key: manifest[key] for key in ("schema_version", "producer_version", "files")}, ensure_ascii=True))
        source["manifest_sha256"] = _hash(raw["source-manifest.json"])
        files = {name: _hash(raw[name]) for name in sorted(raw)}
        identity = {"schema_version": SCHEMA, "source": source, "files": files}
        fingerprint = _hash(_canonical(identity))
        package = dict(identity, package_version=fingerprint[:16], package_fingerprint=fingerprint,
                       generated_at=clock.isoformat().replace("+00:00", "Z"))
        raw["manifest.json"] = json.dumps(package, indent=2, ensure_ascii=False, allow_nan=False).encode() + b"\n"
        if sum(map(len, raw.values())) > 8 * MIB:
            _fail("Package exceeds size limit")
        destination.parent.mkdir(parents=True, exist_ok=True)
        _safe_chain(destination.parent)
        stage = Path(tempfile.mkdtemp(prefix=".source-evidence-", dir=destination.parent))
        for name, content in raw.items():
            (stage / name).write_bytes(content)
        # Re-read all original hashes and source bytes after staging. A changed
        # source cannot be published as the earlier captured package.
        manifest_again, raw_again = _capture(root)
        if manifest_again != manifest or raw_again != {key: value for key, value in raw.items() if key != "manifest.json"}:
            _fail("Source changed during export")
        _safe_chain(destination)
        if destination.exists():
            if (not destination.is_dir() or _inventory(destination) != PACKAGE_FILES
                    or any(item.is_dir() for item in destination.iterdir()) or any(
                    _read(destination / name, 2 * MIB) != content for name, content in raw.items())):
                _fail("Existing destination differs from this package")
            return destination
        _publish(stage, destination)
        stage = None
        return destination
    except SourceEvidenceError:
        raise
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as error:
        raise SourceEvidenceError("Cannot export source evidence") from error
    finally:
        if stage is not None:
            # Only the unique staging directory created by this invocation.
            shutil.rmtree(stage)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--generated-at", type=_time)
    args = parser.parse_args(argv)
    try:
        result = export_source_evidence(args.snapshot, args.output, generated_at=args.generated_at)
    except SourceEvidenceError as error:
        parser.exit(2, f"Source evidence export failed: {error}\n")
    print(result)


if __name__ == "__main__":
    main()
