"""Quota-aware free fixtures/results API adapters. Credentials never enter landing files."""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .schema import MatchEvent, digest


class ProviderError(RuntimeError):
    pass


def request_json(client, url, headers, params, attempts=3):
    for attempt in range(attempts):
        response = client.get(url, headers=headers, params=params, timeout=30)
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
    statuses = {"not_started": "scheduled", "running": "running", "finished": "finished",
                "canceled": "cancelled", "postponed": "postponed"}
    opponents = raw.get("opponents", [])
    if len(opponents) != 2 or not raw.get("begin_at") or raw.get("status") not in statuses:
        return None
    a, b = [o.get("opponent") for o in opponents]
    if not a or not b:
        return None
    scores = {r["team_id"]: r["score"] for r in raw.get("results", [])}
    status = statuses[raw["status"]]
    return dict(match_id=f"pandascore:{raw['id']}", provider="pandascore", sport="valorant",
                competition=raw.get("tournament", {}).get("name") or raw.get("name") or "Unknown tournament",
                team_a_id=f"pandascore:{a['id']}", team_a=a["name"],
                team_b_id=f"pandascore:{b['id']}", team_b=b["name"],
                start_time=raw["begin_at"], observed_at=observed_at,
                completed_at=raw.get("end_at") or observed_at if status == "finished" else None,
                status=status, score_a=scores.get(a["id"]), score_b=scores.get(b["id"]),
                best_of=raw.get("number_of_games") or 3)


def collect(provider: str, state_path: Path, competition="PL", pages=1, client=None) -> tuple[list[dict], dict]:
    if provider not in ("football-data", "pandascore") or not 1 <= pages <= 10:
        raise ValueError("Unsupported provider or pages outside 1..10")
    token = os.environ.get("FOOTBALL_DATA_TOKEN" if provider == "football-data" else "PANDASCORE_TOKEN")
    if not token:
        raise ProviderError(f"Set {'FOOTBALL_DATA_TOKEN' if provider == 'football-data' else 'PANDASCORE_TOKEN'} in .env")
    owns_client = client is None
    client = client or httpx.Client(follow_redirects=False)
    observed = datetime.now(timezone.utc)
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    pending = dict(state)
    rows, unsupported = [], 0
    try:
        if provider == "football-data":
            if not competition.isalnum() or len(competition) > 12:
                raise ValueError("Invalid competition code")
            payload = request_json(client, f"https://api.football-data.org/v4/competitions/{competition}/matches",
                                   {"X-Auth-Token": token}, {})
            raw_rows = payload.get("matches", [])
        else:
            raw_rows = []
            for page in range(1, pages + 1):
                for phase in ("past", "upcoming"):
                    data = request_json(client, f"https://api.pandascore.co/valorant/matches/{phase}",
                                        {"Authorization": f"Bearer {token}"}, {"page": page, "per_page": 100})
                    raw_rows.extend(data)
                    time.sleep(0.4)
        normalizer = normalize_football if provider == "football-data" else normalize_valorant
        for raw in raw_rows:
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
            rows.append(MatchEvent.model_validate(match).row())
            pending[match["match_id"]] = {"hash": content_hash, "revision": revision}
        return rows, dict(state=pending, unsupported=unsupported, returned=len(raw_rows))
    finally:
        if owns_client:
            client.close()


def commit_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    temporary.replace(path)
