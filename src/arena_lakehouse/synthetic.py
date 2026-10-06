"""Original deterministic fixtures. Team names and every result are fictional."""

import random
from datetime import datetime, timedelta, timezone

from .schema import MatchEvent

TEAMS = {
    "football": ["Northbridge FC", "Harbor United", "Oakfield City", "Riverside Athletic",
                 "Summit Rovers", "Eastgate FC", "Westhaven United", "Cedar Town"],
    "valorant": ["Neon Wolves", "Vector Five", "Crimson Circuit", "Aurora Protocol",
                 "Echo Division", "Nova Agents", "Prism Collective", "Zero Latitude"],
}


def generate(matches: int = 600, seed: int = 42, faults: bool = True) -> list[dict]:
    if not 20 <= matches <= 500_000:
        raise ValueError("matches must be between 20 and 500000")
    rng = random.Random(seed)
    events = []
    origin = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for index in range(matches):
        sport = "football" if index % 2 == 0 else "valorant"
        a, b = rng.sample(range(8), 2)
        start = origin + timedelta(hours=index * 4)
        ident = f"synthetic:{sport}:{index:06d}"
        base = dict(match_id=ident, provider="synthetic", sport=sport,
                    competition="Demo Premier Division" if sport == "football" else "Demo VCT Circuit",
                    team_a_id=f"{sport}:{a}", team_a=TEAMS[sport][a],
                    team_b_id=f"{sport}:{b}", team_b=TEAMS[sport][b],
                    start_time=start, best_of=1 if sport == "football" else 3)
        scheduled = MatchEvent(**base, event_id=ident + ":1", revision=1, status="scheduled",
                               observed_at=start - timedelta(days=2)).row()
        events.append(scheduled)
        if index >= matches - 8:
            continue
        if sport == "football":
            # A reproducible skill signal; this is not an estimate for real teams.
            sa = sum(rng.random() < (0.28 + (7 - a) * 0.035) for _ in range(5))
            sb = sum(rng.random() < (0.20 + (7 - b) * 0.035) for _ in range(5))
        else:
            awins = rng.random() < 1 / (1 + 2 ** ((a - b) / 3))
            sa, sb = (2, rng.randrange(2)) if awins else (rng.randrange(2), 2)
        end = start + timedelta(hours=2 if sport == "football" else 3)
        events.append(MatchEvent(**base, event_id=ident + ":2", revision=2, status="finished",
                                 observed_at=end + timedelta(minutes=10), completed_at=end,
                                 score_a=sa, score_b=sb).row())
    if faults:
        events.extend([dict(events[1]), dict(events[1])])
        bad = dict(events[1], event_id="invalid:score", score_a=-1)
        events.append(bad)
        events.append({"event_id": "invalid:schema", "sport": "valorant", "surprise": True})
    rng.shuffle(events)  # Delivery order differs from source revision order.
    return events
