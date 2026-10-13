"""Quota-aware free fixtures/results API adapters. Credentials never enter landing files."""

import json
import hashlib
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
from pydantic import ValidationError

from .calendar import _business, _payload
from .schema import MatchEvent, digest
from .provider_rejections import RejectedProviderRow, RowRejection


class ProviderError(RuntimeError):
    pass


def request_json(client, url, headers, params, attempts=3):
    if not 1 <= attempts <= 5:
        raise ValueError("Provider attempts must be 1..5")
    for attempt in range(attempts):
        try:
            response = client.get(url, headers=headers, params=params, timeout=30)
        except (httpx.TimeoutException, httpx.NetworkError):
            if attempt + 1 == attempts:
                raise ProviderError("Provider network outage; retry on the next poll") from None
            time.sleep(min(2 ** attempt, 30))
            continue
        if response.status_code in (401, 403):
            raise ProviderError("Provider authentication or plan access denied; check your free account")
        if response.status_code == 429 or response.status_code >= 500:
            if attempt + 1 == attempts:
                raise ProviderError("Provider rate limit or temporary outage; retry on the next poll")
            try:
                delay = min(max(float(response.headers.get("Retry-After", 2 ** attempt)), 0), 30)
            except ValueError:
                delay = 2 ** attempt
            time.sleep(delay)
            continue
        response.raise_for_status()
        return response.json()
    raise ProviderError("Provider request failed")


def normalize_football(raw: dict, observed_at: datetime) -> dict | None:
    statuses = {"SCHEDULED": "scheduled", "TIMED": "scheduled", "IN_PLAY": "running",
                "PAUSED": "running", "FINISHED": "finished", "POSTPONED": "postponed",
                "SUSPENDED": "postponed", "CANCELLED": "cancelled", "AWARDED": "finished"}
    if raw.get("status") not in statuses or not raw.get("homeTeam", {}).get("id") or not raw.get("awayTeam", {}).get("id"):
        return None
    score = raw.get("score", {}).get("fullTime", {})
    status = statuses[raw["status"]]
    # No historical availability is fabricated: results become known at collection time.
    return dict(match_id=f"football-data:{raw['id']}", provider="football-data", sport="football",
                competition=raw["competition"]["name"], team_a_id=f"football-data:{raw['homeTeam']['id']}",
                team_a=raw["homeTeam"]["name"], team_b_id=f"football-data:{raw['awayTeam']['id']}",
                team_b=raw["awayTeam"]["name"], start_time=raw["utcDate"],
                observed_at=observed_at, completed_at=observed_at if status == "finished" else None,
                status=status, score_a=score.get("home"), score_b=score.get("away"), best_of=1)


def normalize_valorant(raw: dict, observed_at: datetime) -> dict | None:
    # Valid announcements have a broader contract than canonical matches.
    # Parse that contract first: malformed fields still fail collection, while
    # unresolved, unsupported, or wider bounded source facts stay calendar-only.
    if (not isinstance(observed_at, datetime) or observed_at.tzinfo is None
            or observed_at.utcoffset() is None):
        raise ValueError("Canonical observation requires an aware clock")
    business = _business(raw)
    statuses = {"not_started": "scheduled", "running": "running", "finished": "finished",
                "canceled": "cancelled", "postponed": "postponed"}
    teams = (business["team_a"], business["team_b"])
    if (business["start_time"] is None or business["source_status"] not in statuses
            or "unsupported_opponent_type" in business["pending_reasons"]
            or any(team is None or team["team_id"] is None or team["team_name"] is None for team in teams)
            or teams[0]["team_id"] == teams[1]["team_id"]
            or business["source_number_of_games"] is None or business["source_number_of_games"] > 9
            or raw.get("tournament") is None):
        return None
    competition = (raw["tournament"].get("name") or raw.get("name"))
    if not competition:
        return None
    a, b = teams
    results = business["source_results"]
    result_ids = [result["source_team_id"] for result in results]
    # Empty non-final scores are explicitly unknown. Any supplied result list
    # must identify both distinct opponents exactly once; never collapse an
    # ambiguous list into an apparently confirmed score dictionary.
    if results and (len(result_ids) != 2 or len(set(result_ids)) != 2
                    or set(result_ids) != {a["source_team_id"], b["source_team_id"]}):
        return None
    if business["source_status"] == "finished" and not business["result_known"]:
        return None
    scores = {result["source_team_id"]: result["score"] for result in results}
    status = statuses[raw["status"]]
    match = dict(match_id=f"pandascore:{raw['id']}", provider="pandascore", sport="valorant",
                competition=competition, team_a_id=a["team_id"], team_a=a["team_name"],
                team_b_id=b["team_id"], team_b=b["team_name"],
                start_time=raw["begin_at"], observed_at=observed_at,
                completed_at=raw.get("end_at") or observed_at if status == "finished" else None,
                status=status, score_a=scores.get(a["source_team_id"]), score_b=scores.get(b["source_team_id"]),
                best_of=business["source_number_of_games"])
    try:
        MatchEvent.model_validate(dict(match, revision=1, event_id="canonical-eligibility"))
    except ValidationError:
        return None
    return match


def collect(provider: str, state_path: Path, competition="PL", pages=1, client=None, *, clock=None) -> tuple[list[dict], dict]:
    if provider not in ("football-data", "pandascore") or not 1 <= pages <= 10:
        raise ValueError("Unsupported provider or pages outside 1..10")
    token = os.environ.get("FOOTBALL_DATA_TOKEN" if provider == "football-data" else "PANDASCORE_TOKEN")
    if not token:
        raise ProviderError(f"Set {'FOOTBALL_DATA_TOKEN' if provider == 'football-data' else 'PANDASCORE_TOKEN'} in .env")
    owns_client = client is None
    client = client or httpx.Client(follow_redirects=False)
    now = clock or (lambda: datetime.now(timezone.utc))
    requested_at = now()
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    pending = dict(state)
    rows, unsupported, raw_archives = [], 0, []

    def archive(payload, endpoint):
        # Preserve the unnormalized provider body before schema interpretation.
        # Request headers/credentials are never serialized. Licensed response
        # bodies are private runtime artifacts, excluded from public snapshots.
        received_at = now()
        directory = state_path.parent / "provider_raw" / provider / received_at.date().isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / f"{uuid.uuid4().hex}.json"
        body = json.dumps(dict(provider=provider, endpoint=endpoint,
            requested_at=requested_at.isoformat(), observed_at=received_at.isoformat(), payload=payload),
            ensure_ascii=False).encode("utf-8")
        destination.write_bytes(body)
        raw_archives.append(str(destination))
        return dict(provider=provider, endpoint=endpoint,
                    archive_path=destination.relative_to(state_path.parent).as_posix(),
                    archive_sha256=hashlib.sha256(body).hexdigest())

    try:
        if provider == "football-data":
            if not competition.isalnum() or len(competition) > 12:
                raise ValueError("Invalid competition code")
            payload = request_json(client, f"https://api.football-data.org/v4/competitions/{competition}/matches",
                                   {"X-Auth-Token": token, "X-Unfold-Lineups": "true",
                                    "X-Unfold-Goals": "true", "X-Unfold-Bookings": "true",
                                    "X-Unfold-Subs": "true"}, {})
            provenance = archive(payload, f"competitions/{competition}/matches")
            raw_rows = [(raw, provenance, index) for index, raw in enumerate(payload.get("matches", []))]
        else:
            raw_rows = []
            for page in range(1, pages + 1):
                for phase in ("past", "upcoming", "running"):
                    data = request_json(client, f"https://api.pandascore.co/valorant/matches/{phase}",
                                        {"Authorization": f"Bearer {token}"}, {"page": page, "per_page": 100})
                    provenance = archive(data, f"valorant/matches/{phase}?page={page}&per_page=100")
                    raw_rows.extend((raw, provenance, index) for index, raw in enumerate(data))
                    time.sleep(0.4)
        # Complete calendar validation precedes any canonical candidate or
        # cursor revision. Conflicting phase variants are evidence to retain,
        # never multiple revisions at the same post-collection observation time.
        conflicted = set()
        if provider == "pandascore":
            calendar = _payload(raw_archives, state_path.parent / "provider_raw", pages, ())
            conflicted = {row["announcement_id"] for row in calendar["announcements"] if row["conflicted"]}
        # A result can finish while a request is in flight. Availability is
        # recorded after the responses were received, never at request start.
        observed = now()
        normalizer = normalize_football if provider == "football-data" else normalize_valorant
        for raw, provenance, index in raw_rows:
            if provider == "pandascore" and f"pandascore:{raw['id']}" in conflicted:
                unsupported += 1
                continue
            match = normalizer(raw, observed)
            if match is None:
                unsupported += 1
                continue
            business = {k: v for k, v in match.items() if k not in ("observed_at", "completed_at")}
            content_hash = digest(business)
            previous = pending.get(match["match_id"])
            if previous and previous["hash"] == content_hash:
                continue
            revision = previous["revision"] + 1 if previous else 1
            match.update(revision=revision, event_id=f"{match['match_id']}:{revision}:{content_hash}")
            # Validation failure aborts collection; the cursor is not advanced.
            try:
                validated = MatchEvent.model_validate(match).row()
            except ValidationError:
                raise RejectedProviderRow(RowRejection(**provenance, row_index=index)) from None
            rows.append(validated)
            pending[match["match_id"]] = {"hash": content_hash, "revision": revision}
        return rows, dict(state=pending, unsupported=unsupported, returned=len(raw_rows), raw_archives=raw_archives)
    finally:
        if owns_client:
            client.close()


def commit_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    temporary.replace(path)
