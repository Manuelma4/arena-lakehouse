"""Build licensed, historical World Cup details from pinned local originals.

No network access, Current-provider join, roster inference or event statistics.
The independent JSON package is not an Arena Current or OpenFootball snapshot.
"""

import argparse
import hashlib
import io
import json
import re
import unicodedata
import uuid
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA_VERSION = "historical-football-details/1.0.0"
INPUT_VERSION = "historical-football-inputs/1.0.0"
PROVIDER = "wyscout-pappalardo-2019"
SCOPE = "historical_research"
LICENSE = "CC BY 4.0"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"
MAX_INPUT_BYTES = 25_000_000
MAX_MEMBER_BYTES = 5_000_000
MAX_OUTPUT_BYTES = 25_000_000
SOURCE_SPEC = {
    "matches": (7770422, 1, 14464622, "matches.zip"),
    "players": (7765196, 3, 15073721, "players.json"),
    "teams": (7765310, 3, 15073697, "teams.json"),
}
MATCH_MEMBER = "matches_World_Cup.json"
START = datetime(2018, 6, 14, tzinfo=timezone.utc)
END = datetime(2018, 7, 16, tzinfo=timezone.utc)
ATTRIBUTION = {
    "authors": ["Luca Pappalardo", "Emanuele Massucco"],
    "paper": "Pappalardo et al. (2019), A public data set of spatio-temporal match events "
             "in soccer competitions, Scientific Data 6:236",
    "paper_url": "https://doi.org/10.1038/s41597-019-0247-7",
    "license": LICENSE,
    "license_url": LICENSE_URL,
    "changes": ["Selected the World Cup 2018 archive member and match-bound participants",
                "Normalized identifiers, UTC clocks and bounded Unicode name escapes",
                "Kept separate native score stages and unknown values; omitted biographies and current rosters"],
    "endorsement": "No endorsement by the authors or Wyscout is implied",
    "warranty": "Source data is provided without a warranty of accuracy or completeness",
}


class FootballDetailsError(ValueError):
    """The source evidence, temporal scope or output contract is invalid."""


def _require(condition, message):
    if not condition:
        raise FootballDetailsError(message)


def _object(pairs):
    value = {}
    for key, item in pairs:
        _require(key not in value, "Duplicate JSON key")
        value[key] = item
    return value


def _decode(body):
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite JSON")))
    except (UnicodeError, ValueError, RecursionError) as error:
        raise FootballDetailsError("Invalid UTF-8/JSON source") from error


def _json(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def _hash(body):
    return hashlib.sha256(body).hexdigest()


def _fields(value, required, allowed, label):
    _require(isinstance(value, dict), f"Invalid {label} object")
    _require(set(required) <= set(value) <= set(allowed), f"{label} schema drift")


def _integer(value, label, maximum=2**53 - 1, minimum=0):
    _require(type(value) is int and minimum <= value <= maximum, f"Invalid {label}")
    return value


def _id(value, entity):
    return f"{PROVIDER}:{entity}:{_integer(value, entity + ' ID', minimum=1)}"


def _stamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _clock(value, label):
    _require(isinstance(value, str) and len(value) <= 40, f"Invalid {label}")
    try:
        clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise FootballDetailsError(f"Invalid {label}") from error
    _require(clock.tzinfo is not None and clock.utcoffset() == timedelta(0), f"{label} must be aware UTC")
    return clock.astimezone(timezone.utc)


def _text(value, label, *, nullable=False, maximum=240):
    if nullable and value in (None, "", "null"):
        return None
    _require(isinstance(value, str) and 0 < len(value) <= maximum, f"Invalid {label}")
    _require(not any(unicodedata.category(char) in {"Cc", "Cs"} for char in value),
             f"Invalid control/surrogate in {label}")
    return value


def decode_name(value):
    """Decode only valid literal Unicode escapes, at most twice; keep UTF-8 intact."""
    value = _text(value, "native name")
    for _ in range(2):
        value = re.sub(r"(?:\\u[0-9a-fA-F]{4})+",
                       lambda match: json.loads('"' + match.group() + '"'), value)
    _require("\\" not in value, "Malformed or excessively nested native name escape")
    value = _text(value, "decoded name").strip()
    _require(bool(value), "Empty decoded name")
    return value


def _audit(source):
    return dict(source_system=PROVIDER, created_at=source["observed_at"],
                updated_at=source["observed_at"], deleted_at=None, quality_score=1.0,
                source_refs=[source["role"]])


def _index(rows, label, maximum):
    _require(isinstance(rows, list) and 0 < len(rows) <= maximum, f"Invalid {label} rows")
    result = {}
    for row in rows:
        _require(isinstance(row, dict), f"Invalid {label} row")
        identity = _integer(row.get("wyId"), label + " ID", minimum=1)
        _require(identity not in result, f"Duplicate {label} ID")
        result[identity] = row
    return result


def validate_sources(descriptor, bodies):
    """Verify supplied capture metadata and the three actual original bodies."""
    _fields(descriptor, {"schema_version", "provider", "scope", "sources"},
            {"schema_version", "provider", "scope", "sources"}, "input manifest")
    _require(descriptor["schema_version"] == INPUT_VERSION and descriptor["provider"] == PROVIDER
             and descriptor["scope"] == SCOPE, "Input source version/provider/scope differs")
    sources = descriptor["sources"]
    _require(isinstance(sources, list) and len(sources) == 3, "Exactly three source observations required")
    result = {}
    required = {"role", "article_id", "article_version", "file_id", "filename", "url", "sha256",
                "bytes", "md5", "observed_at", "metadata_observed_at", "metadata_sha256", "published_at",
                "license", "license_url"}
    for source in sources:
        _fields(source, required, required, "source observation")
        role = source["role"]
        _require(isinstance(role, str) and role in SOURCE_SPEC and role not in result, "Invalid source role")
        article, version, file_id, filename = SOURCE_SPEC[role]
        _require((source["article_id"], source["article_version"], source["file_id"], source["filename"])
                 == (article, version, file_id, filename), "Different original deposit/file version")
        for key in ("article_id", "article_version", "file_id"):
            _integer(source[key], key, minimum=1)
        _require(source["url"] == f"https://ndownloader.figshare.com/files/{file_id}", "Different source URL")
        _require(source["license"] == LICENSE and source["license_url"] == LICENSE_URL,
                 "Different source data license")
        for key, size in (("sha256", 64), ("metadata_sha256", 64), ("md5", 32)):
            _require(isinstance(source[key], str) and re.fullmatch(f"[0-9a-f]{{{size}}}", source[key]),
                     f"Invalid {key}")
        body = bodies[role]
        _require(_integer(source["bytes"], "source bytes", MAX_INPUT_BYTES, 1) == len(body),
                 "Source byte count differs")
        _require(_hash(body) == source["sha256"] and hashlib.md5(body).hexdigest() == source["md5"],
                 "Source checksum differs")
        observed = _clock(source["observed_at"], "source observation")
        metadata_observed = _clock(source["metadata_observed_at"], "metadata observation")
        published = _clock(source["published_at"], "source publication")
        _require(END <= published <= metadata_observed <= observed, "Source observation/publication order differs")
        result[role] = {**source, "observed_at": _stamp(observed),
                        "metadata_observed_at": _stamp(metadata_observed), "published_at": _stamp(published),
                        "doi": f"https://doi.org/10.6084/m9.figshare.{article}.v{version}"}
    return result


def _kickoff(row):
    value = row["dateutc"]
    _require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value),
             "Invalid native UTC kickoff")
    try:
        kickoff = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise FootballDetailsError("Invalid native UTC kickoff") from error
    _require(START <= kickoff < END, "Match lies outside World Cup 2018 temporal scope")
    native = _text(row["date"], "native kickoff", maximum=100)
    # English source wall clock, explicit offset; parsing does not depend on host locale.
    match = re.fullmatch(r"(June|July) (\d{1,2}), (\d{4}) at (\d{1,2}):(\d{2}):(\d{2}) "
                         r"(AM|PM) GMT([+-])(\d{1,2})(?::?(\d{2}))?", native)
    _require(match is not None, "Native kickoff offset/format is unsupported")
    month, day, year, hour, minute, second, ampm, sign, offset_hour, offset_minute = match.groups()
    hour = _integer(int(hour), "native wall hour", 12, 1) % 12 + (12 if ampm == "PM" else 0)
    offset_minutes = _integer(int(offset_hour), "UTC offset hours", 14) * 60
    offset_minutes += _integer(int(offset_minute or 0), "UTC offset minutes", 59)
    _require(offset_minutes <= 14 * 60, "UTC offset exceeds bound")
    try:
        wall = datetime(int(year), 6 if month == "June" else 7, int(day), hour, int(minute), int(second),
                        tzinfo=timezone(timedelta(minutes=offset_minutes * (1 if sign == "+" else -1))))
    except ValueError as error:
        raise FootballDetailsError("Invalid native wall kickoff") from error
    _require(wall.astimezone(timezone.utc) == kickoff, "Native kickoff clocks disagree")
    return _stamp(kickoff)


def _counter(value, label):
    if value in (None, "null"):
        return None
    if isinstance(value, str) and re.fullmatch(r"\d{1,2}", value):
        value = int(value)
    return _integer(value, label, maximum=30)


def _card(value):
    if value in (None, "null"):
        return None
    _require(isinstance(value, str) and len(value) <= 80
             and re.fullmatch(r"[0-9,; .-]+", value), "Invalid native card token")
    return value


def _participant(row, kind, order, source):
    allowed = {"playerId", "assists", "goals", "ownGoals", "redCards", "yellowCards"}
    _fields(row, {"playerId"}, allowed, "participant")
    return dict(player_id=_id(row["playerId"], "player"), source_id=row["playerId"], kind=kind,
                source_order=order, statistics=dict(goals=_counter(row.get("goals"), "goals"),
                    own_goals=_counter(row.get("ownGoals"), "own goals"),
                    assists=_counter(row.get("assists"), "assists")),
                native_statistics={key: row.get(key) for key in ("goals", "ownGoals", "assists")},
                native_card_fields=dict(yellowCards=_card(row.get("yellowCards")),
                                        redCards=_card(row.get("redCards"))),
                **_audit(source))


def _formation(team, source):
    supplied = _integer(team["hasFormation"], "formation availability", 1)
    if not supplied:
        _require(team.get("formation") in (None, {}, {"lineup": [], "bench": [], "substitutions": []}),
                 "Unsupplied formation contains participant facts")
        return dict(has_formation=False, starters=None, bench=None, substitutions=None), set()
    formation = team.get("formation")
    _fields(formation, {"lineup", "bench", "substitutions"}, {"lineup", "bench", "substitutions"}, "formation")
    lineup, bench, substitutions = (formation[key] for key in ("lineup", "bench", "substitutions"))
    _require(isinstance(lineup, list) and len(lineup) == 11, "Supplied starting lineup must contain eleven players")
    _require(isinstance(bench, list) and len(bench) <= 12, "Invalid bench size")
    _require(isinstance(substitutions, list) and len(substitutions) <= 4, "Invalid World Cup substitution count")
    starters = [_participant(row, "starter", order, source) for order, row in enumerate(lineup)]
    reserve = [_participant(row, "bench", order, source) for order, row in enumerate(bench)]
    ids = [row["source_id"] for row in starters + reserve]
    _require(len(ids) == len(set(ids)), "Duplicate participant identity/role")
    starting_ids, bench_ids = {row["source_id"] for row in starters}, {row["source_id"] for row in reserve}
    normalized = []
    for order, row in enumerate(substitutions):
        _fields(row, {"playerIn", "playerOut", "minute"}, {"playerIn", "playerOut", "minute", "assists"},
                "substitution")
        incoming, outgoing = _integer(row["playerIn"], "substitution in ID", minimum=1), \
                             _integer(row["playerOut"], "substitution out ID", minimum=1)
        _require(incoming in bench_ids and outgoing in set(ids), "Substitution has foreign participants")
        minute = _integer(row["minute"], "substitution minute", 130)
        normalized.append(dict(player_in_id=_id(incoming, "player"), player_out_id=_id(outgoing, "player"),
                               minute=minute, source_order=order, **_audit(source)))
    active = set(starting_ids)
    for substitution in sorted(normalized, key=lambda row: (row["minute"], row["source_order"])):
        incoming = int(substitution["player_in_id"].rsplit(":", 1)[1])
        outgoing = int(substitution["player_out_id"].rsplit(":", 1)[1])
        _require(outgoing in active and incoming not in active, "Inconsistent substitution sequence")
        active.remove(outgoing)
        active.add(incoming)
    return dict(has_formation=True, starters=starters, bench=reserve, substitutions=normalized), set(ids)


def _outcome(row, ordered):
    duration = row["duration"]
    _require(isinstance(duration, str) and duration in {"Regular", "ExtraTime", "Penalties"},
             "Unsupported native duration")
    scores = {field: [_integer(team[field], field, 30) for team in ordered]
              for field in ("score", "scoreHT", "scoreET", "scoreP")}
    _require(all(half <= total for half, total in zip(scores["scoreHT"], scores["score"], strict=True)),
             "Half-time score exceeds regulation score")
    regulation = dict(home=scores["score"][0], away=scores["score"][1])
    if duration == "Regular":
        _require(scores["scoreET"] == [0, 0] and scores["scoreP"] == [0, 0],
                 "Regular match contains extra-time/shootout totals")
        extra, penalties, final = None, None, regulation
    else:
        _require(all(reg <= total for reg, total in zip(scores["score"], scores["scoreET"], strict=True)),
                 "Extra-time total is below regulation score")
        extra = dict(home=scores["scoreET"][0], away=scores["scoreET"][1])
        final = extra
        if duration == "ExtraTime":
            _require(scores["scoreP"] == [0, 0], "Extra-time match contains shootout values")
            penalties = None
        else:
            _require(extra["home"] == extra["away"] and scores["scoreP"][0] != scores["scoreP"][1],
                     "Shootout requires tied match and a distinct shootout winner")
            penalties = dict(home=scores["scoreP"][0], away=scores["scoreP"][1])
    winner = _integer(row["winner"], "winner ID")
    deciding = penalties or final
    expected = (0 if deciding["home"] == deciding["away"] else
                ordered[0 if deciding["home"] > deciding["away"] else 1]["teamId"])
    _require(winner == expected, "Native winner disagrees with score stage")
    return dict(regulation=regulation, half_time=dict(home=scores["scoreHT"][0], away=scores["scoreHT"][1]),
                extra_time_total=extra, shootout=penalties, final_match_score=final,
                winner_team_id=_id(winner, "team") if winner else None,
                native_winner=winner, native_duration=duration)


def _player(row, source):
    fields = {"birthArea", "birthDate", "currentNationalTeamId", "currentTeamId", "firstName", "foot",
              "height", "lastName", "middleName", "passportArea", "role", "shortName", "weight", "wyId"}
    _fields(row, {"wyId", "shortName", "role"}, fields, "player metadata")
    role = row["role"]
    _fields(role, {"name", "code2", "code3"}, {"name", "code2", "code3"}, "generic player role")
    foot = row.get("foot")
    if foot in (None, "", "null"):
        foot = None
    _require(foot is None or isinstance(foot, str) and foot in {"left", "right", "both"},
             "Unsupported preferred foot")
    return dict(player_id=_id(row["wyId"], "player"), source_id=row["wyId"],
                name=decode_name(row["shortName"]), native_name=row["shortName"],
                generic_deposit_role={key: _text(role[key], "generic role", maximum=40)
                                      for key in ("name", "code2", "code3")},
                preferred_foot=foot, **_audit(source))


def _team(row, source):
    _fields(row, {"wyId", "name", "officialName", "type", "area"},
            {"wyId", "name", "officialName", "type", "area", "city"}, "team metadata")
    _require(row["type"] == "national", "World Cup team metadata is not national")
    area = row["area"]
    _fields(area, {"name"}, {"name", "alpha2code", "alpha3code", "id"}, "team area")
    area_projection = {key: _text(area.get(key), "team area", nullable=key != "name", maximum=100)
                       for key in ("name", "alpha2code", "alpha3code")}
    for key, size in (("alpha2code", 2), ("alpha3code", 3)):
        _require(area_projection[key] is None or re.fullmatch(f"[A-Z]{{{size}}}", area_projection[key]),
                 "Invalid native area code")
    return dict(team_id=_id(row["wyId"], "team"), source_id=row["wyId"],
                name=decode_name(row["name"]), native_name=row["name"],
                official_name=decode_name(row["officialName"]), type="national",
                area=area_projection, **_audit(source))


def normalize_details(matches, players, teams, sources):
    """Strict source-native projection; unknowns remain distinct from real zero."""
    match_map = _index(matches, "match", 64)
    player_map = _index(players, "player", 10_000)
    team_map = _index(teams, "team", 1_000)
    normalized, selected_players, selected_teams = [], set(), set()
    match_fields = {"status", "roundId", "gameweek", "teamsData", "seasonId", "dateutc", "winner", "venue",
                    "wyId", "label", "date", "groupName", "referees", "duration", "competitionId"}
    team_fields = {"scoreET", "coachId", "side", "teamId", "score", "scoreP", "hasFormation", "formation", "scoreHT"}
    for match_id, row in sorted(match_map.items()):
        _fields(row, {"status", "roundId", "teamsData", "seasonId", "dateutc", "winner", "wyId", "date",
                      "duration", "competitionId"}, match_fields, "match")
        _require(type(row["competitionId"]) is int and row["competitionId"] == 28
                 and type(row["seasonId"]) is int and row["seasonId"] == 10078, "Different competition/season")
        _require(row["status"] == "Played", "Historical detail source is not a completed match")
        kickoff = _kickoff(row)
        _require(_clock(kickoff, "kickoff") <= _clock(sources["matches"]["observed_at"], "match observation"),
                 "Match observation precedes kickoff")
        team_data = row["teamsData"]
        _require(isinstance(team_data, dict) and len(team_data) == 2, "Match needs exactly two source teams")
        ordered = {}
        for key, team in team_data.items():
            _fields(team, team_fields - {"coachId", "formation"}, team_fields, "match team")
            identity = _integer(team["teamId"], "match team ID", minimum=1)
            _require(key == str(identity) and identity in team_map, "Team key/reference differs")
            side = team["side"]
            _require(isinstance(side, str) and side in {"home", "away"} and side not in ordered,
                     "Duplicate/invalid team side")
            ordered[side] = team
        sides = [ordered["home"], ordered["away"]]
        _require(sides[0]["teamId"] != sides[1]["teamId"], "Identical opponents")
        details, match_players = [], set()
        for team in sides:
            formation, participants = _formation(team, sources["matches"])
            _require(not (participants & match_players), "Player belongs to both match teams")
            _require(participants <= set(player_map), "Participant references unknown source player")
            match_players |= participants
            selected_teams.add(team["teamId"])
            details.append(dict(team_id=_id(team["teamId"], "team"), source_id=team["teamId"], side=team["side"],
                                native_scores={key: team[key] for key in ("score", "scoreHT", "scoreET", "scoreP")},
                                **formation, **_audit(sources["matches"])))
        selected_players |= match_players
        normalized.append(dict(match_id=_id(match_id, "match"), source_id=match_id,
            competition_id=28, season_id=10078, source_round_id=_integer(row["roundId"], "round ID", minimum=1),
            kickoff_at=kickoff, native_date=row["date"], native_dateutc=row["dateutc"], completed_at=None,
            status="finished", venue=_text(row.get("venue"), "venue", nullable=True),
            source_label=_text(row.get("label"), "match label", nullable=True),
            source_group=_text(row.get("groupName"), "group", nullable=True),
            outcome=_outcome(row, sides), teams=details, **_audit(sources["matches"])))
    return dict(schema_version=SCHEMA_VERSION, provider=PROVIDER, scope=SCOPE, sport="football",
        competition=dict(source_id=28, name="World Cup"), season=dict(source_id=10078, label="2018"),
        teams=[_team(team_map[identity], sources["teams"]) for identity in sorted(selected_teams)],
        players=[_player(player_map[identity], sources["players"]) for identity in sorted(selected_players)],
        matches=normalized)


def _read(path, maximum):
    path = Path(path)
    _require(path.is_file() and not path.is_symlink(), "Source path must be a regular local file")
    _require(0 < path.stat().st_size <= maximum, "Source file exceeds size bound")
    body = path.read_bytes()
    _require(0 < len(body) <= maximum, "Source file changed beyond size bound")
    return body


def _publish(output, files, inputs):
    output = Path(output)
    _require(not output.is_symlink() and not any(parent.is_symlink() for parent in output.parents),
             "Output path must not traverse symlinks")
    resolved = output.resolve()
    _require(resolved != Path(resolved.anchor) and len(resolved.parts) >= 3, "Output directory is too broad")
    _require(all(resolved != path.resolve() and resolved not in path.resolve().parents for path in inputs),
             "Output directory must not contain input originals")
    _require(not output.exists() or output.is_dir(), "Output path is not a directory")
    output.mkdir(parents=True, exist_ok=True)
    lock = output / ".package.lock"
    try:
        owner = lock.open("xb")
    except FileExistsError as error:
        raise FootballDetailsError("Output directory is already being built; existing lock retained") from error
    try:
        with owner:
            owner.write(uuid.uuid4().hex.encode())
            entries = [path for path in output.iterdir() if path != lock]
            _require(all(path.name in files and path.is_file() and not path.is_symlink() for path in entries),
                     "Output contains unrelated files")
            for path in entries:
                _require(path.stat().st_size <= MAX_OUTPUT_BYTES and path.read_bytes() == files[path.name],
                         "Output already contains a different package")
            for name, body in files.items():
                target = output / name
                if target.exists():
                    continue
                temporary = output / f".{name}.{uuid.uuid4().hex}.tmp"
                try:
                    with temporary.open("xb") as stream:
                        stream.write(body)
                    temporary.replace(target)
                finally:
                    if temporary.exists():
                        temporary.unlink()
    finally:
        lock.unlink()


def build_package(matches_path, players_path, teams_path, source_manifest_path, output):
    """Build/replay an immutable independent package from explicit local paths."""
    inputs = [Path(path) for path in (matches_path, players_path, teams_path, source_manifest_path)]
    _require(len({path.resolve() for path in inputs}) == 4, "Input paths must be distinct")
    descriptor = _decode(_read(inputs[3], 100_000))
    bodies, used_bytes = {}, 0
    for role, path in zip(SOURCE_SPEC, inputs[:3], strict=True):
        _require(used_bytes < MAX_INPUT_BYTES, "Total inputs exceed size bound")
        bodies[role] = _read(path, MAX_INPUT_BYTES - used_bytes)
        used_bytes += len(bodies[role])
    sources = validate_sources(descriptor, bodies)
    try:
        with zipfile.ZipFile(io.BytesIO(bodies["matches"])) as archive:
            _require(archive.namelist().count(MATCH_MEMBER) == 1, "Missing or duplicate World Cup archive member")
            info = archive.getinfo(MATCH_MEMBER)
            _require(0 < info.file_size <= MAX_MEMBER_BYTES, "World Cup member exceeds size bound")
            member = archive.read(MATCH_MEMBER)
    except (zipfile.BadZipFile, KeyError, RuntimeError, NotImplementedError) as error:
        raise FootballDetailsError("Invalid World Cup source archive") from error
    _require(len(member) <= MAX_MEMBER_BYTES, "Expanded member exceeds size bound")
    sources["matches"]["selected_member"] = dict(path=MATCH_MEMBER, bytes=len(member), sha256=_hash(member))
    details = normalize_details(_decode(member), _decode(bodies["players"]), _decode(bodies["teams"]), sources)
    detail_body = _json(details)
    _require(len(detail_body) <= MAX_OUTPUT_BYTES, "Details output exceeds size bound")
    matches = details["matches"]
    participants = [player for match in matches for team in match["teams"]
                    for player in (team["starters"] or []) + (team["bench"] or [])]
    counts = dict(matches=len(matches), teams=len(details["teams"]), players=len(details["players"]),
                  starters=sum(player["kind"] == "starter" for player in participants),
                  bench=sum(player["kind"] == "bench" for player in participants),
                  substitutions=sum(len(team["substitutions"] or []) for match in matches for team in match["teams"]))
    manifest = dict(schema_version=SCHEMA_VERSION, provider=PROVIDER, scope=SCOPE,
        source_as_of=max(source["observed_at"] for source in sources.values()),
        business_date_range=dict(start=min(match["kickoff_at"] for match in matches),
                                 end=max(match["kickoff_at"] for match in matches)),
        attribution=deepcopy(ATTRIBUTION), sources=[sources[role] for role in SOURCE_SPEC], record_counts=counts,
        quality=dict(row_score_definition="1.0 means this row passed its explicit identity/type/reference "
            "contract; allowed unknowns do not imply complete statistics", violations=0,
            participant_unknown_goals=sum(player["statistics"]["goals"] is None for player in participants),
            formations_not_supplied=sum(not team["has_formation"] for match in matches for team in match["teams"]),
            checks=["source hashes/size/version/license", "aware ordered source clocks", "2018 temporal scope",
                    "native kickoff clock agreement", "unique source identities and sides", "same-source references",
                    "match-bound participant ownership", "substitution sequence", "separate score-stage/winner parity"]),
        capabilities=dict(current_rosters=False, event_telemetry=False, forecasts=False,
                          retrospective_details=True, pre_match_availability_evidence=False,
                          match_tactical_positions=False, interpreted_card_counts=False),
        interpretation_notes=[
            "Retrospective post-match details collected in 2026 are not evidence of pre-match availability in 2018",
            "Player role and preferred foot are deposit metadata; match tactical positions are unknown",
            "Native card tokens are uninterpreted source strings, never card counts",
            "created_at/updated_at use the source capture observation as a deterministic initial-import audit clock; "
            "they do not assert the source record's original creation time",
        ],
        files=[dict(path="details.json", bytes=len(detail_body), sha256=_hash(detail_body))])
    # Bind attribution, scope/capabilities and quality as well as payload/source hashes.
    # Consumer recomputation removes only these two self-referential fields.
    fingerprint = _hash(_json(manifest))
    manifest.update(dataset_version=fingerprint[:16], publication_fingerprint=fingerprint)
    manifest_body = _json(manifest)
    _require(len(detail_body) + len(manifest_body) <= MAX_OUTPUT_BYTES, "Package exceeds size bound")
    _publish(output, {"details.json": detail_body, "manifest.json": manifest_body}, inputs)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matches-path", required=True, type=Path, help="Original licensed matches.zip")
    parser.add_argument("--players-path", required=True, type=Path)
    parser.add_argument("--teams-path", required=True, type=Path)
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = build_package(args.matches_path, args.players_path, args.teams_path,
                                 args.source_manifest, args.output)
    except (FootballDetailsError, OSError) as error:
        parser.exit(2, f"Historical detail validation failed: {error}\n")
    print(json.dumps({key: manifest[key] for key in ("schema_version", "scope", "dataset_version",
                                                    "publication_fingerprint", "record_counts", "quality")},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
