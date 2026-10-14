"""Football-data score evidence: native totals are not necessarily 90-minute goals.

Native ``extraTime`` is an increment; native ``fullTime`` includes shootout
goals. Regulation is never reconstructed by subtracting later score stages.
"""

STATUSES = {"SCHEDULED", "TIMED", "IN_PLAY", "PAUSED", "FINISHED", "POSTPONED",
            "SUSPENDED", "CANCELLED", "AWARDED"}
STAGES = ("fullTime", "halfTime", "regularTime", "extraTime", "penalties")
DURATIONS = {"REGULAR", "EXTRA_TIME", "PENALTY_SHOOTOUT"}


class FootballScoreError(ValueError):
    """Invalid evidence; no private source content is included in the error."""


def _invalid():
    raise FootballScoreError("invalid_football_score_evidence")


def _complete(pair):
    return isinstance(pair, dict) and all(type(pair.get(side)) is int for side in ("home", "away"))


def _sum(*pairs):
    return {side: sum(pair[side] for pair in pairs) for side in ("home", "away")}


def _partial(pair):
    return isinstance(pair, dict) and sum(value is not None for value in pair.values()) == 1


def _positive(pair):
    return isinstance(pair, dict) and any(value is not None and value > 0 for value in pair.values())


def score_details(raw):
    """Return preserved native stages and an explicitly justified regulation pair.

    Absent stages stay absent and explicit nulls stay null. Missing or unknown
    duration never establishes regulation, even if a regularTime pair exists.
    """
    if not isinstance(raw, dict) or not isinstance(raw.get("status"), str) or raw["status"] not in STATUSES:
        _invalid()
    score = raw.get("score", {})
    if not isinstance(score, dict) or set(score) - {*STAGES, "duration", "winner"}:
        _invalid()
    if "winner" in score and score["winner"] not in (None, "HOME_TEAM", "AWAY_TEAM", "DRAW"):
        _invalid()
    duration = score.get("duration")
    if duration is not None and (not isinstance(duration, str) or not 1 <= len(duration) <= 40
                                 or duration != duration.strip() or any(ord(c) < 32 or ord(c) == 127 for c in duration)):
        _invalid()
    native = {}
    for stage in STAGES:
        if stage not in score:
            continue
        pair = score[stage]
        if pair is not None:
            if not isinstance(pair, dict) or set(pair) != {"home", "away"}:
                _invalid()
            if any(value is not None and (type(value) is not int or not 0 <= value <= 50) for value in pair.values()):
                _invalid()
            pair = dict(pair)
        native[stage] = pair
    full, regular, extra, penalties = (native.get(key) for key in ("fullTime", "regularTime", "extraTime", "penalties"))
    after_extra = _sum(regular, extra) if _complete(regular) and _complete(extra) else None
    inconsistent = False
    if duration == "REGULAR":
        inconsistent = (_positive(extra) or _positive(penalties)
            or isinstance(regular, dict) and isinstance(full, dict)
            and any(regular[side] is not None and full[side] is not None and regular[side] != full[side]
                    for side in ("home", "away")))
    elif duration == "EXTRA_TIME":
        inconsistent = _positive(penalties) or (_complete(full) and after_extra is not None and full != after_extra)
    elif duration == "PENALTY_SHOOTOUT":
        inconsistent = (_complete(full) and after_extra is not None and _complete(penalties)
                        and full != _sum(after_extra, penalties))
    if _complete(full) and _complete(regular):
        inconsistent = inconsistent or any(full[side] < regular[side] for side in ("home", "away"))
    candidate_regulation = regular if _complete(regular) else full if duration == "REGULAR" and not _partial(regular) else None
    half = native.get("halfTime")
    if _complete(half) and _complete(candidate_regulation):
        inconsistent = inconsistent or any(half[side] > candidate_regulation[side] for side in ("home", "away"))
    partial_stages = (_partial(regular)
                      or duration in ("EXTRA_TIME", "PENALTY_SHOOTOUT") and _partial(extra)
                      or duration == "PENALTY_SHOOTOUT" and _partial(penalties))
    reason, regulation, basis = None, None, "unavailable"
    if raw["status"] == "AWARDED":
        reason = "awarded"
    elif raw["status"] != "FINISHED":
        reason = "not_finished"
    elif duration not in DURATIONS:
        reason = "unknown_duration"
    elif inconsistent:
        reason = "inconsistent_stages"
    elif not _complete(full) or partial_stages:
        reason = "missing_regulation"
    elif _complete(regular):
        regulation, basis = dict(regular), "explicit_regular_time"
    elif duration == "REGULAR":
        regulation, basis = dict(full), "regular_full_time"
    else:
        reason = "missing_regulation"
    return dict(source_status=raw["status"], duration=duration, duration_present="duration" in score,
                native=native, regulation=regulation, regulation_basis=basis, unavailable_reason=reason,
                after_extra_time=after_extra)
