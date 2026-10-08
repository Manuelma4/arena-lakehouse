"""Private match facts from the exact archives of one selected successful poll.

This stage neither acquires data nor infers missing player statistics. Its
optional package is bound to the publication; it never changes canonical scores.
"""

import argparse
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .provider_publication import (
    MAX_INTENT_BYTES, PublicationError, archive_references, atomic_json,
    canonical, current_publication, parse_json, read_bytes, safe, sha256, stamp,
)

SCHEMA = "arena-match-insights-1.0"
MAX_BYTES = 32 * 1024 * 1024
MAX_MATCHES = 10_000
FOOTBALL_STATS = ("ball_possession", "shots", "shots_on_goal", "shots_off_goal",
                  "corner_kicks", "offsides", "fouls", "saves", "yellow_cards", "red_cards")


class AmbiguousMatchDetails(ValueError):
    """Optional facts cannot establish one match/player/map ownership."""


def text(value, maximum=120):
    if (isinstance(value, str) and 0 < len(value.strip()) <= maximum
            and not any(ord(char) < 32 or ord(char) == 127 for char in value)):
        return value.strip()
    return None


def number(value, maximum=10_000):
    return value if type(value) is int and 0 <= value <= maximum else None


def identity(provider, value):
    value = number(value, 2**53 - 1)
    return f"{provider}:{value}" if value else None


def display_id(provider, match_id):
    return f"{provider}-{sha256((provider + chr(0) + 'match' + chr(0) + match_id).encode())[:16]}"


def _players(rows, provider, role):
    if not isinstance(rows, list) or len(rows) > 50:
        return []
    result, seen = [], {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        player_id, name = identity(provider, raw.get("id")), text(raw.get("name"))
        if not player_id or not name:
            continue
        player = dict(player_id=player_id, name=name, role=role, position=text(raw.get("position")),
                      shirt_number=number(raw.get("shirtNumber"), 999))
        if player_id in seen:
            if seen[player_id] != player:
                raise AmbiguousMatchDetails("Conflicting player facts")
            continue
        seen[player_id] = player
        result.append(player)
    return result


def _team(raw, provider, role):
    raw = raw if isinstance(raw, dict) else {}
    coach = raw.get("coach") or {}
    stats = raw.get("statistics") or {}
    return dict(team_id=identity(provider, raw.get("id")), name=text(raw.get("name")),
                formation=text(raw.get("formation"), 40),
                coach=text(coach.get("name")) if isinstance(coach, dict) else None,
                lineup=_players(raw.get("lineup"), provider, "starter"),
                bench=_players(raw.get("bench"), provider, "bench"),
                statistics={key: number(stats.get(key), 100 if key == "ball_possession" else 10_000)
                            for key in FOOTBALL_STATS} if role == "football" and isinstance(stats, dict) else {})


def normalize(raw, provider, source_sha256, observed_at):
    """Whitelist supplied fields. Nulls and omitted counters never become zero."""
    match_id = identity(provider, raw.get("id"))
    if not match_id:
        return None
    football = provider == "football-data"
    teams = ([_team(raw.get(key), provider, "football") for key in ("homeTeam", "awayTeam")]
             if football else [_team(value.get("opponent") if isinstance(value, dict)
                                      and value.get("type") in (None, "Team", "team") else None,
                                      provider, "valorant")
                               for value in raw.get("opponents", [])[:2]])
    games = []
    if not football:
        for game in (raw.get("games") or [])[:20]:
            if not isinstance(game, dict) or game.get("match_id") != raw["id"]:
                continue
            winner = game.get("winner") or {}
            # Neither default winners nor 0:0 scores establish a played map.
            played = game.get("status") == "finished" and game.get("finished") is True
            winner_id = (identity(provider, winner.get("id"))
                         if played and isinstance(winner, dict) and winner.get("type") == "Team" else None)
            if winner_id not in {team["team_id"] for team in teams}:
                winner_id = None
            map_info = game.get("map") or {}
            games.append(dict(game_id=identity(provider, game.get("id")), position=number(game.get("position"), 20),
                name=text(map_info.get("name")) if isinstance(map_info, dict) else None,
                status=game.get("status") if game.get("status") in ("finished", "running", "not_started") else "unknown",
                length_seconds=number(game.get("length"), 86400) if played else None,
                winner_team_id=winner_id))
    players = [player["player_id"] for team in teams for player in team["lineup"] + team["bench"]]
    if len(players) != len(set(players)):
        raise AmbiguousMatchDetails("Conflicting player ownership")
    for values in ([game["game_id"] for game in games if game["game_id"] is not None],
                   [game["position"] for game in games if game["position"] is not None]):
        if len(values) != len(set(values)):
            raise AmbiguousMatchDetails("Conflicting map identity or position")
    referees = raw.get("referees") or []
    return dict(match_id=match_id, display_id=display_id(provider, match_id),
        sport="football" if football else "valorant", source_sha256=source_sha256, observed_at=stamp(observed_at),
        venue=text(raw.get("venue")), stage=text(raw.get("stage")), group=text(raw.get("group")),
        matchday=number(raw.get("matchday"), 1000), attendance=number(raw.get("attendance"), 2_000_000),
        referees=[name for row in referees[:10] if isinstance(row, dict) and (name := text(row.get("name")))],
        teams=teams, games=games, player_statistics=[],
        player_statistics_status="not_supplied",
        lineup_status="supplied" if any(team["lineup"] for team in teams) else "not_supplied",
        provider_has_detailed_stats=raw.get("detailed_stats") is True if not football else None)


def publish(root, selection, archives, *, started_at, generated_at):
    root = Path(root)
    refs, sources = archive_references(root, archives, selection["source_scope"],
                                      collected_at=selection["collected_at"], started_at=started_at)
    provider, records, signatures, conflicting = selection["provider"], {}, {}, set()
    for ref, source in zip(refs, sources, strict=True):
        rows = source["payload"]["matches"] if provider == "football-data" else source["payload"]
        for raw in rows:
            try:
                value = normalize(raw, provider, ref["sha256"], source["observed_at"])
            except AmbiguousMatchDetails:
                key = identity(provider, raw.get("id"))
                if key:
                    conflicting.add(key)
                continue
            if value is None:
                continue
            key = value["match_id"]
            signature = sha256(canonical({k: v for k, v in raw.items() if k != "modified_at"}))
            # Conflicting phase records are not resolved by endpoint order.
            if key in records:
                old = {k: v for k, v in records[key].items() if k not in ("source_sha256", "observed_at")}
                new = {k: v for k, v in value.items() if k not in ("source_sha256", "observed_at")}
                if old != new or signatures[key] != signature:
                    conflicting.add(key)
            else:
                records[key] = value
                signatures[key] = signature
            if len(records) > MAX_MATCHES:
                raise PublicationError("Match insight count bound exceeded")
    game_owners = {}
    for match_id, record in records.items():
        for game in record["games"]:
            if game["game_id"] is not None:
                prior = game_owners.setdefault(game["game_id"], match_id)
                if prior != match_id:
                    conflicting.update((prior, match_id))
    package = dict(schema_version=SCHEMA, provider=provider, publication_fingerprint=selection["fingerprint"],
        collected_at=selection["collected_at"], generated_at=stamp(generated_at),
        sources=[dict(sha256=ref["sha256"], endpoint=source["endpoint"], observed_at=stamp(source["observed_at"]))
                 for ref, source in zip(refs, sources, strict=True)],
        records=[records[key] for key in sorted(records) if key not in conflicting],
        conflicting_matches_omitted=len(conflicting))
    body = canonical(package)
    if len(body) > MAX_BYTES:
        raise PublicationError("Match insight byte bound exceeded")
    checksum = sha256(body + b"\n")
    name = f"insights-{checksum}.json"
    path = root / "match-insights" / name
    safe(path)
    if not path.exists():
        atomic_json(path, package)
    elif hashlib.sha256(read_bytes(path, MAX_BYTES)).hexdigest() != checksum:
        raise PublicationError("Match insight package damaged")
    atomic_json(root / "snapshots/CURRENT_MATCH_INSIGHTS.json", dict(schema_version=SCHEMA, provider=provider,
        publication_fingerprint=selection["fingerprint"], basename=name, sha256=checksum))
    return dict(provider=provider, records=len(package["records"]), sha256=checksum,
                lineup_matches=sum(row["lineup_status"] == "supplied" for row in package["records"]),
                map_matches=sum(bool(row["games"]) for row in package["records"]), player_stat_matches=0,
                conflicting_matches_omitted=len(conflicting), provider_requests=0)


def selected_archives(root, *, now=None):
    """Read exact hash-checked archives of one completed publication intent."""
    now = now or datetime.now(timezone.utc)
    root = Path(root)
    selection = current_publication(root, now=now)
    if selection is None:
        raise PublicationError("No selected provider publication")
    path = root / "live.db"
    safe(path)
    with sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True) as db:
        row = db.execute("SELECT payload,payload_sha256 FROM publication_intents WHERE poll_id=? AND phase='completed'",
                         (selection["poll_id"],)).fetchone()
        poll = db.execute("SELECT started_at FROM polls WHERE id=?", (selection["poll_id"],)).fetchone()
    if row is None or poll is None or len(row[0].encode()) > MAX_INTENT_BYTES or sha256(row[0].encode()) != row[1]:
        raise PublicationError("Completed publication evidence unavailable")
    intent = parse_json(row[0].encode())
    if intent["selection"] != selection:
        raise PublicationError("Publication intent differs from current selection")
    if not isinstance(intent.get("archives"), list) or not 1 <= len(intent["archives"]) <= 30:
        raise PublicationError("Invalid selected archive count")
    for ref in intent["archives"]:
        if not re.fullmatch(r"provider_raw/(football-data|pandascore)/\d{4}-\d{2}-\d{2}/[a-f0-9]{32}\.json", ref["path"]):
            raise PublicationError("Invalid match insight archive")
        if sha256(read_bytes(root / ref["path"], 4 * 1024 * 1024)) != ref["sha256"]:
            raise PublicationError("Match insight archive checksum mismatch")
    paths = [root / ref["path"] for ref in intent["archives"]]
    references, _ = archive_references(root, paths, selection["source_scope"],
        collected_at=selection["collected_at"], started_at=poll[0])
    if references != intent["archives"]:
        raise PublicationError("Selected archive references changed")
    return selection, paths, poll[0]


def build_selected(root, *, now=None):
    """Rebuild from one completed intent; no archive discovery or new requests."""
    now = now or datetime.now(timezone.utc)
    selection, paths, started = selected_archives(root, now=now)
    return publish(root, selection, paths, started_at=started, generated_at=now)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path("data/live"))
    parser.add_argument("--provider", choices=("football-data", "pandascore"), required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(build_selected(args.workspace / args.provider), indent=2))
    except (PublicationError, OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        parser.exit(1, "Match insights unavailable; selected source evidence failed verification.\n")


if __name__ == "__main__":
    main()
