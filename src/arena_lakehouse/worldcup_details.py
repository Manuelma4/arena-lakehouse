"""Pinned CC0 community match detail; independent of current provider identities."""

import argparse
from datetime import date, datetime, timezone
import hashlib
from importlib.resources import files
import json
from pathlib import Path
import re
import tempfile
import time
import unicodedata

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = "openfootball-worldcup-details-1.0"
REF = "0c4b73b3c1fbb877e8274eb321b1ec4d9220a3a4"
SOURCE_URL = f"https://raw.githubusercontent.com/openfootball/worldcup.json/{REF}/2026/worldcup-full.json"
SOURCE_SHA = "42bf26a5dca941eb77f56572c6f06ca3dc78766f249ef0fb60b4da46e7698fde"
LICENSE_URL = f"https://raw.githubusercontent.com/openfootball/worldcup.json/{REF}/LICENSE.md"
LICENSE_SHA = "36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673"
BASIC_SHA = "0ae2c18109b5aa86bc11928b43586ca430c234804d5bbf808d2c3bf2051ecfca"
COMMIT_AT = "2026-08-11T10:30:42Z"
ALIASES = {"Bosnia and Herzegovina": "Bosnia & Herzegovina", "Côte d'Ivoire": "Ivory Coast",
           "Cabo Verde": "Cape Verde", "Congo DR": "DR Congo"}
MAX_BYTES = 2_000_000
LIMITATIONS = [
    "Historical community-reported match observations, not an official current feed.",
    "Source-listed starting XI and substitutes; the complete matchday bench is not supplied.",
    "Players have source names only: no provider player IDs, global identity or current roster is inferred.",
    "Missing goal/card families are unknown, not zero; source minute strings and score phases are preserved.",
    "Collection time establishes local availability, not historical pre-match knowledge or forecast eligibility.",
]


class WorldCupDetailsError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value).encode()).hexdigest()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise WorldCupDetailsError("duplicate_json_key")
        result[key] = value
    return result


def decode(body):
    if not isinstance(body, bytes) or len(body) > MAX_BYTES:
        raise WorldCupDetailsError("source_size_limit")
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise WorldCupDetailsError("invalid_source_json") from error


def instant(value):
    try:
        clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if clock.tzinfo is None or clock.utcoffset().total_seconds() != 0:
            raise ValueError
        return clock.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError, OverflowError) as error:
        raise WorldCupDetailsError("invalid_collection_clock") from error


def source_clock(value):
    match = re.fullmatch(r"([0-2]\d:[0-5]\d) UTC([+-])(\d{1,2})(?::([0-5]\d))?", value or "")
    if not match or int(match[1][:2]) > 23 or int(match[3]) > 14:
        raise WorldCupDetailsError("invalid_source_clock")
    offset = (int(match[3]) * 60 + int(match[4] or 0)) * (1 if match[2] == "+" else -1)
    if abs(offset) > 840:
        raise WorldCupDetailsError("invalid_source_clock")
    return match[1], offset


def _text(value, limit=200):
    if (not isinstance(value, str) or not value.strip() or len(value) > limit
            or any(unicodedata.category(char).startswith("C") for char in value)):
        raise WorldCupDetailsError("invalid_source_text")


def _pair(value):
    if not isinstance(value, list) or len(value) != 2 or any(type(n) is not int or not 0 <= n <= 50 for n in value):
        raise WorldCupDetailsError("invalid_source_score")


def _events(values, family):
    if not isinstance(values, list) or len(values) > 100:
        raise WorldCupDetailsError("invalid_source_events")
    allowed = {"goal": {"name", "minute", "penalty", "owngoal"}, "card": {"name", "minute", "type"},
               "sub": {"on", "off", "minute"}, "penalty": {"name", "note", "score"}}[family]
    required = {"sub": {"on", "off", "minute"}, "penalty": {"name"}}.get(family, {"name", "minute"})
    for event in values:
        if not isinstance(event, dict) or not required <= event.keys() <= allowed:
            raise WorldCupDetailsError("invalid_source_event")
        for key in ("name", "on", "off", "note"):
            if key in event:
                _text(event[key])
        if "minute" in event and (not isinstance(event["minute"], str)
                                  or re.fullmatch(r"\d{1,3}(?:\+\d{1,2})?", event["minute"]) is None):
            raise WorldCupDetailsError("invalid_source_minute")
        if family == "card" and event.get("type") not in ("Y", "R", "Y/R"):
            raise WorldCupDetailsError("invalid_source_card")
        for key in ("penalty", "owngoal"):
            if key in event and type(event[key]) is not bool:
                raise WorldCupDetailsError("invalid_source_goal_flag")
        if "score" in event:
            _pair(event["score"])


def validate_match(row):
    required = {"round", "date", "time", "team1", "team2", "score", "ground", "attendance", "lineup", "referees"}
    optional = {"goals1", "goals2", "bookings", "penalties"}
    if not isinstance(row, dict) or not required <= row.keys() <= required | optional:
        raise WorldCupDetailsError("invalid_source_match_fields")
    for key in ("round", "date", "time", "team1", "team2", "ground"):
        _text(row[key])
    if date.fromisoformat(row["date"]).isoformat() != row["date"] or row["team1"] == row["team2"]:
        raise WorldCupDetailsError("invalid_source_participants_or_date")
    source_clock(row["time"])
    score = row["score"]
    if not isinstance(score, dict) or "ft" not in score or not score.keys() <= {"ft", "ht", "et", "p"}:
        raise WorldCupDetailsError("invalid_source_score")
    for pair in score.values():
        _pair(pair)
    if type(row["attendance"]) is not int or not 0 <= row["attendance"] <= 200_000:
        raise WorldCupDetailsError("invalid_source_attendance")
    if not isinstance(row["lineup"], list) or len(row["lineup"]) != 2:
        raise WorldCupDetailsError("invalid_source_lineup")
    for lineup in row["lineup"]:
        if not isinstance(lineup, dict) or set(lineup) != {"starter", "bench", "subs"}:
            raise WorldCupDetailsError("invalid_source_lineup")
        for key, low, high in (("starter", 11, 11), ("bench", 0, 30)):
            players = lineup[key]
            if not isinstance(players, list) or not low <= len(players) <= high:
                raise WorldCupDetailsError("invalid_source_participant_count")
            for player in players:
                if not isinstance(player, dict) or not {"name"} <= player.keys() <= {"name", "captain"}:
                    raise WorldCupDetailsError("invalid_source_player")
                _text(player["name"])
                if "captain" in player and type(player["captain"]) is not bool:
                    raise WorldCupDetailsError("invalid_source_captain")
        _events(lineup["subs"], "sub")
    referees = row["referees"]
    if not isinstance(referees, list) or len(referees) > 10:
        raise WorldCupDetailsError("invalid_source_referees")
    for person in referees:
        if not isinstance(person, dict) or set(person) != {"name", "country"}:
            raise WorldCupDetailsError("invalid_source_referee")
        _text(person["name"])
        _text(person["country"], 5)
    if ("goals1" in row) != ("goals2" in row):
        raise WorldCupDetailsError("incomplete_source_goal_pair")
    for key in ("goals1", "goals2"):
        if key in row:
            _events(row[key], "goal")
    if "bookings" in row:
        if not isinstance(row["bookings"], list) or len(row["bookings"]) != 2:
            raise WorldCupDetailsError("invalid_source_bookings")
        for values in row["bookings"]:
            _events(values, "card")
    if "penalties" in row:
        _events(row["penalties"], "penalty")


def normalize(full, basic):
    """Generic pure parser used with original TEST ONLY fixtures in offline tests."""
    if (not isinstance(full, dict) or set(full) != {"name", "matches"} or full["name"] != "World Cup 2026"
            or not isinstance(full["matches"], list) or len(full["matches"]) > 200
            or not isinstance(basic, dict) or not isinstance(basic.get("matches"), list)):
        raise WorldCupDetailsError("invalid_source_scope")
    lookup, basic_ids = {}, set()
    for match in basic["matches"]:
        key = (match["date"], match["team1"], match["team2"])
        number = match.get("num")
        if key in lookup or number is not None and (type(number) is not int or number < 1):
            raise WorldCupDetailsError("ambiguous_basic_identity")
        team_ids = ["openfootball:team:" + digest(["openfootball-national-men", match[side]])[:24]
                    for side in ("team1", "team2")]
        identity = ["worldcup", "2026", number] if number is not None else [
            "worldcup", "2026", match["round"], *team_ids, 1]
        basic_id = "openfootball:match:" + digest(identity)[:24]
        if basic_id in basic_ids:
            raise WorldCupDetailsError("ambiguous_basic_identity")
        basic_ids.add(basic_id)
        lookup[key] = (match, basic_id)
    rows, identities = [], set()
    for index, raw in enumerate(full["matches"]):
        validate_match(raw)
        ident = "openfootball:detail:" + digest(["worldcup-full-2026", raw["date"], raw["team1"], raw["team2"]])[:24]
        if ident in identities:
            raise WorldCupDetailsError("ambiguous_detail_identity")
        identities.add(ident)
        teams = [ALIASES.get(raw[key], raw[key]) for key in ("team1", "team2")]
        found = lookup.get((raw["date"], *teams))
        anchor, basic_id = found if found else (None, None)
        reasons = []
        if anchor:
            if source_clock(raw["time"]) != source_clock(anchor["time"]):
                reasons.append("kickoff_mismatch")
            for phase in sorted(raw["score"].keys() & anchor.get("score", {}).keys()):
                if raw["score"][phase] != anchor["score"][phase]:
                    reasons.append(f"score_{phase}_mismatch")
            if "ft" not in anchor.get("score", {}):
                reasons.append("basic_result_missing")
        else:
            reasons.append("basic_match_not_found")
        rows.append(dict(detail_id=ident, source_row=index, basic_match_id=basic_id,
            basic_match_number=anchor.get("num") if anchor else None,
            mapping_status="verified" if not reasons else "conflict" if anchor else "unmatched",
            mapping_reasons=reasons, aliases_applied=[dict(source=raw[key], basic=teams[side])
                for side, key in enumerate(("team1", "team2")) if raw[key] != teams[side]],
            round_label_differs=bool(anchor and raw["round"] != anchor["round"]),
            bench_complete=False, player_identity="source_names_only", source_match=raw))
    rows.sort(key=lambda row: row["detail_id"])
    quality = dict(matches=len(rows), verified=sum(row["mapping_status"] == "verified" for row in rows),
        conflicts=sum(row["mapping_status"] == "conflict" for row in rows),
        unmatched=sum(row["mapping_status"] == "unmatched" for row in rows),
        alias_linked=sum(bool(row["aliases_applied"]) for row in rows),
        round_label_differences=sum(row["round_label_differs"] for row in rows),
        goal_family_supplied=sum("goals1" in row["source_match"] for row in rows),
        card_family_supplied=sum("bookings" in row["source_match"] for row in rows),
        complete_benches=0, provider_player_ids=0)
    return rows, quality


def publish(raw_file, license_file, output, *, observed_at, generated_at=None):
    def bounded(path):
        path = Path(path)
        if any(parent.is_symlink() for parent in (path, *path.absolute().parents)):
            raise WorldCupDetailsError("unsafe_source_path")
        with path.open("rb") as stream:
            body = stream.read(MAX_BYTES + 1)
        if len(body) > MAX_BYTES:
            raise WorldCupDetailsError("source_size_limit")
        return body
    raw, license_body = bounded(raw_file), bounded(license_file)
    basic = files("arena_lakehouse").joinpath("resources/worldcup_2026.json").read_bytes()
    if digest(raw) != SOURCE_SHA or digest(license_body) != LICENSE_SHA or digest(basic) != BASIC_SHA:
        raise WorldCupDetailsError("source_checksum_mismatch")
    observed = instant(observed_at)
    generated = instant(generated_at) if generated_at else datetime.now(timezone.utc)
    if observed < instant(COMMIT_AT) or not observed <= generated <= datetime.now(timezone.utc):
        raise WorldCupDetailsError("invalid_publication_chronology")
    rows, quality = normalize(decode(raw), decode(basic))
    if len(rows) != 104:
        raise WorldCupDetailsError("reviewed_source_scope_changed")
    source = dict(url=SOURCE_URL, ref=REF, sha256=SOURCE_SHA, bytes=len(raw), license_url=LICENSE_URL,
                  license_sha256=LICENSE_SHA, observed_at=observed.isoformat().replace("+00:00", "Z"))
    output = Path(output).absolute()
    if output.exists() or any(parent.is_symlink() for parent in (output, *output.parents)):
        raise WorldCupDetailsError("unsafe_or_existing_destination")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".worldcup-details-", dir=output.parent) as temporary:
        root = Path(temporary) / "publication"
        assert Path(temporary).resolve().parent == output.parent.resolve()
        root.mkdir()
        bodies = {"source.json": raw, "LICENSE.md": license_body, "basic.json": basic,
                  "matches.json": (canonical(rows) + "\n").encode(), "quality.json": (canonical(quality) + "\n").encode()}
        for name, body in bodies.items():
            (root / name).write_bytes(body)
        table = pa.Table.from_pylist([dict(detail_id=row["detail_id"], basic_match_id=row["basic_match_id"],
            mapping_status=row["mapping_status"], payload_json=canonical(row)) for row in rows], schema=pa.schema([
                ("detail_id", pa.string()), ("basic_match_id", pa.string()), ("mapping_status", pa.string()),
                ("payload_json", pa.string())]))
        pq.write_table(table, root / "matches.parquet", compression="zstd")
        hashes = {name: digest((root / name).read_bytes()) for name in (*bodies, "matches.parquet")}
        logical = dict(schema_version=SCHEMA, source=source, basic_sha256=BASIC_SHA, files=hashes)
        manifest = dict(**logical, dataset_version=digest(logical)[:16], data_kind="open_data", license="CC0-1.0",
            edition_id="worldcup-2026", generated_at=generated.isoformat().replace("+00:00", "Z"),
            quality=quality, limitations=LIMITATIONS)
        (root / "manifest.json").write_text(canonical(manifest) + "\n", encoding="utf-8")
        root.rename(output)
    return manifest


def collect(output, *, client=None, clock=None):
    """Two fixed public requests, pinned originals and actual response-completion clocks."""
    output = Path(output).absolute()
    if output.exists() or any(parent.is_symlink() for parent in (output, *output.parents)):
        raise WorldCupDetailsError("unsafe_or_existing_destination")
    now = clock or (lambda: datetime.now(timezone.utc))
    owned = client is None
    client = client or httpx.Client(timeout=10, follow_redirects=False)
    entries, bodies = [], {}
    deadline = time.monotonic() + 60
    try:
        for url, name, expected in ((LICENSE_URL, "LICENSE.md", LICENSE_SHA),
                                    (SOURCE_URL, "worldcup-full.json", SOURCE_SHA)):
            requested = now()
            chunks, size = [], 0
            with client.stream("GET", url, timeout=10, follow_redirects=False) as response:
                if response.status_code != 200:
                    raise WorldCupDetailsError("source_request_failed")
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_BYTES or time.monotonic() > deadline:
                        raise WorldCupDetailsError("source_acquisition_limit")
                    chunks.append(chunk)
            body = b"".join(chunks)
            observed = now()
            if digest(body) != expected:
                raise WorldCupDetailsError("source_checksum_mismatch")
            if not instant(COMMIT_AT) <= requested <= observed <= datetime.now(timezone.utc):
                raise WorldCupDetailsError("invalid_collection_clock")
            entries.append(dict(url=url, file=name, requested_at=requested.isoformat(),
                                observed_at=observed.isoformat(), bytes=len(body), sha256=expected))
            bodies[name] = body
    finally:
        if owned:
            client.close()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".worldcup-acquisition-", dir=output.parent) as temporary:
        assert Path(temporary).resolve().parent == output.parent.resolve()
        root = Path(temporary) / "capture"
        root.mkdir()
        for name, body in bodies.items():
            (root / name).write_bytes(body)
        (root / "acquisition.json").write_text(canonical(entries) + "\n", encoding="utf-8")
        root.rename(output)
    return entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect-to", type=Path)
    parser.add_argument("--raw-file", type=Path)
    parser.add_argument("--license-file", type=Path)
    parser.add_argument("--observed-at")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.collect_to:
        if any((args.raw_file, args.license_file, args.observed_at, args.output)):
            parser.error("Use --collect-to separately from publication arguments")
        print(json.dumps(collect(args.collect_to), indent=2))
        return
    if not all((args.raw_file, args.license_file, args.observed_at, args.output)):
        parser.error("Publication requires --raw-file, --license-file, --observed-at and --output")
    result = publish(args.raw_file, args.license_file, args.output, observed_at=args.observed_at)
    print(json.dumps(dict(dataset_version=result["dataset_version"], quality=result["quality"]), indent=2))


if __name__ == "__main__":
    main()
