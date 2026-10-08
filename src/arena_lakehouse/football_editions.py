"""Explicit v2 historical edition packages from the retained licensed originals.

This producer path is independent of the v1 World Cup publication. It does not
activate a consumer, collect data or claim pre-match availability.
"""

import argparse
import io
import json
import re
import stat
import zipfile
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import football_details as v1

SCHEMA = "historical-football-details/2.0.0"
CATALOG_SCHEMA = "historical-football-editions/2.0.0"
MAX_EDITION_BYTES = 25_000_000
MAX_EXPANDED_BYTES = 12_000_000
ATTRIBUTION = {
    **deepcopy(v1.ATTRIBUTION),
    "changes": [
        "Selected seven source editions with competition/season IDs and match-bound participants",
        "Normalized identifiers and clocks; retained native tokens and explicit unknown/conflict flags",
        "Normalized one horizontal tab for display while preserving its native name; omitted biographies and current rosters",
    ],
}


@dataclass(frozen=True)
class Edition:
    edition_id: str
    member: str
    competition_id: int
    season_id: int
    label: str
    season: str
    team_type: str
    start: str
    end: str


EDITIONS = (
    Edition(
        "england-2017-18",
        "matches_England.json",
        364,
        181150,
        "England",
        "2017/18",
        "club",
        "2017-08-01",
        "2018-06-01",
    ),
    Edition(
        "france-2017-18",
        "matches_France.json",
        412,
        181189,
        "France",
        "2017/18",
        "club",
        "2017-08-01",
        "2018-06-01",
    ),
    Edition(
        "germany-2017-18",
        "matches_Germany.json",
        426,
        181137,
        "Germany",
        "2017/18",
        "club",
        "2017-08-01",
        "2018-06-01",
    ),
    Edition(
        "italy-2017-18",
        "matches_Italy.json",
        524,
        181248,
        "Italy",
        "2017/18",
        "club",
        "2017-08-01",
        "2018-06-01",
    ),
    Edition(
        "spain-2017-18",
        "matches_Spain.json",
        795,
        181144,
        "Spain",
        "2017/18",
        "club",
        "2017-08-01",
        "2018-06-01",
    ),
    Edition(
        "euro-2016",
        "matches_European_Championship.json",
        102,
        9291,
        "European Championship",
        "2016",
        "national",
        "2016-06-01",
        "2016-08-01",
    ),
    Edition(
        "world-cup-2018",
        "matches_World_Cup.json",
        28,
        10078,
        "World Cup",
        "2018",
        "national",
        "2018-06-01",
        "2018-08-01",
    ),
)
MONTHS = {
    name: index
    for index, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        1,
    )
}


def audit(source, score=1.0):
    return {**v1._audit(source), "quality_score": score}


def safe_path(path):
    for item in (path, *path.parents):
        try:
            attributes = item.lstat()
        except FileNotFoundError:
            continue
        v1._require(
            not stat.S_ISLNK(attributes.st_mode)
            and not getattr(attributes, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400),
            "Historical v2 path traverses a link/reparse point",
        )


def kickoff(row, edition):
    value = row["dateutc"]
    v1._require(
        isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value),
        "Invalid UTC kickoff",
    )
    utc = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    v1._require(edition.start <= utc.date().isoformat() < edition.end, "Kickoff outside selected edition")
    parsed = re.fullmatch(
        r"([A-Za-z]+) (\d{1,2}), (\d{4}) at (\d{1,2}):(\d{2}):(\d{2}) (AM|PM) GMT([+-])(\d{1,2})(?::?(\d{2}))?",
        row["date"],
    )
    v1._require(parsed is not None, "Invalid native kickoff")
    month, day, year, hour, minute, second, ampm, sign, oh, om = parsed.groups()
    v1._require(month in MONTHS, "Invalid source month")
    hour = v1._integer(int(hour), "wall hour", 12, 1) % 12 + (12 if ampm == "PM" else 0)
    offset = v1._integer(int(oh), "offset hours", 14) * 60 + v1._integer(int(om or 0), "offset minutes", 59)
    v1._require(offset <= 840, "Offset outside bound")
    wall = datetime(
        int(year),
        MONTHS[month],
        int(day),
        hour,
        int(minute),
        int(second),
        tzinfo=timezone(timedelta(minutes=offset * (1 if sign == "+" else -1))),
    )
    v1._require(wall.astimezone(timezone.utc) == utc, "Native kickoff clocks disagree")
    return v1._stamp(utc)


def formation(team, source):
    supplied = v1._integer(team["hasFormation"], "formation flag", 1)
    if not supplied:
        v1._require(
            team.get("formation") in (None, {}, {"lineup": [], "bench": [], "substitutions": []}),
            "Unsupplied formation contains facts",
        )
        return dict(
            has_formation=False,
            starters=None,
            bench=None,
            substitutions=None,
            substitution_availability="unknown",
            native_substitutions=None,
            sequence_verified=False,
        ), set()
    value = team["formation"]
    v1._fields(value, {"lineup", "bench", "substitutions"}, {"lineup", "bench", "substitutions"}, "formation")
    v1._require(type(value["lineup"]) is list and len(value["lineup"]) == 11, "Expected eleven starters")
    v1._require(type(value["bench"]) is list and len(value["bench"]) <= 20, "Bench outside v2 bound")
    starters = [v1._participant(row, "starter", i, source) for i, row in enumerate(value["lineup"])]
    bench = [v1._participant(row, "bench", i, source) for i, row in enumerate(value["bench"])]
    ids = [row["source_id"] for row in starters + bench]
    v1._require(len(ids) == len(set(ids)), "Duplicate participants")
    substitutions = value["substitutions"]
    if substitutions in (None, "null"):
        return dict(
            has_formation=True,
            starters=starters,
            bench=bench,
            substitutions=None,
            substitution_availability="unknown",
            native_substitutions=substitutions,
            sequence_verified=False,
        ), set(ids)
    v1._require(type(substitutions) is list and len(substitutions) <= 8, "Substitutions outside v2 bound")
    normalized = []
    reserves = {row["source_id"] for row in bench}
    unresolved = False
    for order, row in enumerate(substitutions):
        v1._fields(
            row,
            {"playerIn", "playerOut", "minute"},
            {"playerIn", "playerOut", "minute", "assists"},
            "substitution",
        )
        incoming = v1._integer(row["playerIn"], "incoming ID")
        outgoing = v1._integer(row["playerOut"], "outgoing ID", minimum=1)
        v1._require(
            outgoing in ids and (incoming == 0 or incoming in reserves), "Foreign substitution reference"
        )
        unresolved |= incoming == 0
        normalized.append(
            dict(
                player_in_id=v1._id(incoming, "player") if incoming else None,
                player_out_id=v1._id(outgoing, "player"),
                native_player_in=incoming,
                native_player_out=outgoing,
                reference_status="unresolved_incoming" if incoming == 0 else "resolved",
                minute=v1._integer(row["minute"], "minute", 130),
                source_order=order,
                **audit(source, 0.75 if incoming == 0 else 1.0),
            )
        )
    # Verify complete sequences only. A source ID 0 makes active-player inference unknown.
    if not unresolved:
        active = {row["source_id"] for row in starters}
        for row in sorted(normalized, key=lambda item: (item["minute"], item["source_order"])):
            incoming, outgoing = row["native_player_in"], row["native_player_out"]
            v1._require(outgoing in active and incoming not in active, "Invalid substitution sequence")
            active.remove(outgoing)
            active.add(incoming)
    return dict(
        has_formation=True,
        starters=starters,
        bench=bench,
        substitutions=normalized,
        substitution_availability="supplied",
        native_substitutions=deepcopy(substitutions),
        sequence_verified=not unresolved,
    ), set(ids)


def outcome(row, sides):
    native = v1._integer(row["winner"], "winner")
    v1._require(native == 0 or native in {side["teamId"] for side in sides}, "Foreign native winner")
    stage = (
        "scoreP"
        if row["duration"] == "Penalties"
        else "scoreET"
        if row["duration"] == "ExtraTime"
        else "score"
    )
    values = [v1._integer(side[stage], stage, 30) for side in sides]
    expected = 0 if values[0] == values[1] else sides[0 if values[0] > values[1] else 1]["teamId"]
    result = v1._outcome({**row, "winner": expected}, sides)
    conflict = expected != native
    result.update(
        native_winner=native,
        winner_team_id=None if conflict or native == 0 else v1._id(native, "team"),
        status="conflict" if conflict else "consistent",
        result_eligible=not conflict,
    )
    return result


def normalize(matches, players, teams, sources, edition):
    match_map = v1._index(matches, "match", 1000)
    player_map = v1._index(players, "player", 10_000)
    team_map = v1._index(teams, "team", 1000)
    result, used_players, used_teams = [], set(), set()
    fields = {
        "status",
        "roundId",
        "gameweek",
        "teamsData",
        "seasonId",
        "dateutc",
        "winner",
        "venue",
        "wyId",
        "label",
        "date",
        "groupName",
        "referees",
        "duration",
        "competitionId",
    }
    for identity, row in sorted(match_map.items()):
        v1._fields(
            row,
            {
                "status",
                "roundId",
                "teamsData",
                "seasonId",
                "dateutc",
                "winner",
                "wyId",
                "date",
                "duration",
                "competitionId",
            },
            fields,
            "match",
        )
        v1._require(
            type(row["competitionId"]) is int
            and row["competitionId"] == edition.competition_id
            and type(row["seasonId"]) is int
            and row["seasonId"] == edition.season_id
            and row["status"] == "Played",
            "Edition identity/status differs",
        )
        stamp = kickoff(row, edition)
        v1._require(
            v1._clock(stamp, "kickoff") <= v1._clock(sources["matches"]["observed_at"], "observation"),
            "Future observed match",
        )
        native_teams = row["teamsData"]
        v1._require(type(native_teams) is dict and len(native_teams) == 2, "Expected two teams")
        sides = {}
        for key, team in native_teams.items():
            allowed = {
                "scoreET",
                "coachId",
                "side",
                "teamId",
                "score",
                "scoreP",
                "hasFormation",
                "formation",
                "scoreHT",
            }
            v1._fields(team, allowed - {"coachId", "formation"}, allowed, "team")
            team_id = v1._integer(team["teamId"], "team", minimum=1)
            side = team["side"]
            v1._require(
                key == str(team_id)
                and team_id in team_map
                and side in {"home", "away"}
                and side not in sides,
                "Invalid team ownership",
            )
            sides[side] = team
        ordered = [sides["home"], sides["away"]]
        v1._require(ordered[0]["teamId"] != ordered[1]["teamId"], "Identical opponents")
        details, match_players = [], set()
        for team in ordered:
            facts, participants = formation(team, sources["matches"])
            v1._require(not participants & match_players, "Player belongs to both teams")
            match_players |= participants
            used_teams.add(team["teamId"])
            flags = []
            if facts["substitution_availability"] == "unknown":
                flags.append("substitutions_unknown")
            if any(item["reference_status"] != "resolved" for item in facts["substitutions"] or []):
                flags.append("substitution_reference_unresolved")
            if any(id not in player_map for id in participants):
                flags.append("player_metadata_unresolved")
            details.append(
                dict(
                    team_id=v1._id(team["teamId"], "team"),
                    source_id=team["teamId"],
                    side=team["side"],
                    native_scores={key: team[key] for key in ("score", "scoreHT", "scoreET", "scoreP")},
                    **facts,
                    **audit(sources["matches"], 0.75 if flags else 1.0),
                    quality_flags=flags,
                )
            )
        used_players |= match_players
        score = outcome(row, ordered)
        flags = ["outcome_conflict"] if score["status"] == "conflict" else []
        result.append(
            dict(
                match_id=v1._id(identity, "match"),
                source_id=identity,
                edition_id=edition.edition_id,
                competition_id=edition.competition_id,
                season_id=edition.season_id,
                source_round_id=v1._integer(row["roundId"], "round", minimum=1),
                kickoff_at=stamp,
                native_date=row["date"],
                native_dateutc=row["dateutc"],
                completed_at=None,
                status="finished",
                venue=v1._text(row.get("venue"), "venue", nullable=True),
                source_label=v1._text(row.get("label"), "label", nullable=True),
                source_group=v1._text(row.get("groupName"), "group", nullable=True),
                outcome=score,
                teams=details,
                **audit(sources["matches"], 0.75 if flags else 1.0),
                quality_flags=flags,
            )
        )
    normalized_players = []
    for identity in sorted(used_players):
        if identity in player_map:
            original = player_map[identity]
            native_name = original["shortName"]
            # Preserve the source's tab; normalize only the display whitespace.
            v1._require(isinstance(native_name, str), "Invalid native player name")
            projected = {**original, "shortName": native_name.replace("\t", " ")}
            name_flags = ["name_whitespace_normalized"] if "\t" in native_name else []
            normalized_players.append(
                dict(
                    **{
                        **v1._player(projected, sources["players"]),
                        "native_name": native_name,
                        "quality_score": 0.75 if name_flags else 1.0,
                    },
                    metadata_status="resolved",
                    quality_flags=name_flags,
                )
            )
        else:
            normalized_players.append(
                dict(
                    player_id=v1._id(identity, "player"),
                    source_id=identity,
                    name=None,
                    native_name=None,
                    generic_deposit_role=None,
                    preferred_foot=None,
                    metadata_status="unresolved",
                    quality_flags=["player_metadata_unresolved"],
                    **audit(sources["matches"], 0.75),
                )
            )
    normalized_teams = []
    for identity in sorted(used_teams):
        row = team_map[identity]
        v1._require(row["type"] == edition.team_type, "Edition team type differs")
        normalized_teams.append(
            {**v1._team({**row, "type": "national"}, sources["teams"]), "type": row["type"]}
        )
    return dict(
        schema_version=SCHEMA,
        provider=v1.PROVIDER,
        scope=v1.SCOPE,
        sport="football",
        edition=asdict(edition),
        teams=normalized_teams,
        players=normalized_players,
        matches=result,
    )


def counts(details):
    sides = [side for match in details["matches"] for side in match["teams"]]
    return dict(
        matches=len(details["matches"]),
        teams=len(details["teams"]),
        players=len(details["players"]),
        starters=sum(len(side["starters"] or []) for side in sides),
        bench=sum(len(side["bench"] or []) for side in sides),
        substitutions=sum(len(side["substitutions"] or []) for side in sides),
        unresolved_players=sum(player["metadata_status"] == "unresolved" for player in details["players"]),
        normalized_player_names=sum(
            "name_whitespace_normalized" in player["quality_flags"] for player in details["players"]
        ),
        unknown_substitution_lists=sum(side["substitution_availability"] == "unknown" for side in sides),
        unresolved_substitution_references=sum(
            item["reference_status"] != "resolved" for side in sides for item in side["substitutions"] or []
        ),
        outcome_conflicts=sum(match["outcome"]["status"] == "conflict" for match in details["matches"]),
    )


def build_catalog(matches_path, players_path, teams_path, source_manifest_path, output):
    inputs = [Path(path) for path in (matches_path, players_path, teams_path, source_manifest_path)]
    for path in (*inputs, Path(output)):
        safe_path(path)
    v1._require(len({path.resolve() for path in inputs}) == 4, "Input paths must be distinct")
    descriptor = v1._decode(v1._read(inputs[3], 100_000))
    bodies = {
        role: v1._read(path, v1.MAX_INPUT_BYTES)
        for role, path in zip(v1.SOURCE_SPEC, inputs[:3], strict=True)
    }
    v1._require(sum(map(len, bodies.values())) <= v1.MAX_INPUT_BYTES, "Inputs exceed combined bound")
    sources = v1.validate_sources(descriptor, bodies)
    players, teams = v1._decode(bodies["players"]), v1._decode(bodies["teams"])
    output = Path(output)
    resolved = output.resolve()
    v1._require(
        all(resolved != path.resolve() and resolved not in path.resolve().parents for path in inputs),
        "Edition output must not contain input originals",
    )
    if output.exists():
        v1._require(
            output.is_dir()
            and all(
                item.name in {edition.edition_id for edition in EDITIONS} | {"catalog"} and item.is_dir()
                for item in output.iterdir()
            ),
            "Edition output contains unrelated entries",
        )
    edition_entries, match_ids, team_ids, player_ids = [], set(), set(), set()
    with zipfile.ZipFile(io.BytesIO(bodies["matches"])) as archive:
        v1._require(
            sum(archive.getinfo(edition.member).file_size for edition in EDITIONS) <= MAX_EXPANDED_BYTES,
            "Combined edition members exceed expanded bound",
        )
        for edition in EDITIONS:
            v1._require(archive.namelist().count(edition.member) == 1, "Missing/duplicate edition member")
            info = archive.getinfo(edition.member)
            v1._require(0 < info.file_size <= v1.MAX_MEMBER_BYTES, "Member exceeds bound")
            member = archive.read(edition.member)
            scoped_sources = deepcopy(sources)
            scoped_sources["matches"]["selected_member"] = dict(
                path=edition.member, bytes=len(member), sha256=v1._hash(member)
            )
            details = normalize(v1._decode(member), players, teams, scoped_sources, edition)
            ids = {row["match_id"] for row in details["matches"]}
            v1._require(not match_ids & ids, "Cross-edition duplicate match")
            match_ids |= ids
            team_ids.update(row["team_id"] for row in details["teams"])
            player_ids.update(row["player_id"] for row in details["players"])
            body = v1._json(details)
            manifest = dict(
                schema_version=SCHEMA,
                provider=v1.PROVIDER,
                scope=v1.SCOPE,
                edition=asdict(edition),
                source_as_of=max(row["observed_at"] for row in scoped_sources.values()),
                business_date_range=dict(
                    start=min(row["kickoff_at"] for row in details["matches"]),
                    end=max(row["kickoff_at"] for row in details["matches"]),
                ),
                sources=list(scoped_sources.values()),
                attribution=deepcopy(ATTRIBUTION),
                record_counts=counts(details),
                capabilities=dict(
                    retrospective_details=True,
                    current_rosters=False,
                    forecasts=False,
                    pre_match_availability_evidence=False,
                    event_telemetry=False,
                    minutes_played=False,
                    official_league_tables=False,
                    interpreted_card_counts=False,
                ),
                quality_rubric="1.0: passed structural contract; 0.75: permitted unresolved reference, unknown list, outcome conflict or explicit display-name whitespace transformation. No semantic accuracy claim.",
                files=[dict(path="details.json", bytes=len(body), sha256=v1._hash(body))],
            )
            pin = v1._hash(v1._json(manifest))
            manifest.update(publication_fingerprint=pin, dataset_version=pin[:16])
            wire = v1._json(manifest)
            v1._require(len(body) + len(wire) <= MAX_EDITION_BYTES, "Edition exceeds measured v2 bound")
            safe_path(output / edition.edition_id)
            v1._publish(output / edition.edition_id, {"details.json": body, "manifest.json": wire}, inputs)
            edition_entries.append(
                dict(
                    edition=asdict(edition),
                    publication_fingerprint=pin,
                    manifest_sha256=v1._hash(wire),
                    manifest_bytes=len(wire),
                    details_bytes=len(body),
                    selected_member=scoped_sources["matches"]["selected_member"],
                    record_counts=manifest["record_counts"],
                )
            )
    catalog = dict(
        schema_version=CATALOG_SCHEMA,
        provider=v1.PROVIDER,
        scope=v1.SCOPE,
        source_as_of=max(row["observed_at"] for row in sources.values()),
        sources=list(sources.values()),
        attribution=deepcopy(ATTRIBUTION),
        editions=edition_entries,
        unique_counts=dict(matches=len(match_ids), teams=len(team_ids), players=len(player_ids)),
        consumer_activation="pending_independent_reader_api_ui_agent_validation",
    )
    pin = v1._hash(v1._json(catalog))
    catalog.update(publication_fingerprint=pin, dataset_version=pin[:16])
    # A separate immutable catalogue directory lets child replay remain bounded.
    safe_path(output / "catalog")
    v1._publish(output / "catalog", {"catalog.json": v1._json(catalog)}, inputs)
    return catalog


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("matches-path", "players-path", "teams-path", "source-manifest", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        catalog = build_catalog(
            args.matches_path, args.players_path, args.teams_path, args.source_manifest, args.output
        )
    except (v1.FootballDetailsError, OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        parser.exit(2, f"Historical edition validation failed: {error}\n")
    print(
        json.dumps(
            {
                "publication_fingerprint": catalog["publication_fingerprint"],
                "unique_counts": catalog["unique_counts"],
                "editions": [
                    {
                        "edition_id": row["edition"]["edition_id"],
                        "details_bytes": row["details_bytes"],
                        "record_counts": row["record_counts"],
                    }
                    for row in catalog["editions"]
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
