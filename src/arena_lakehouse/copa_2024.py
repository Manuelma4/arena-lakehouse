"""Narrow decoder for the reviewed CC0 Copa America 2024 Football.TXT edition.

This is not a general Football.TXT parser. Source goal annotations are retained in
raw bytes, but not interpreted as an event feed. Numeric source dates take priority
over the known final-line weekday typo; kickoff delays are not reconstructed.
"""

from collections import Counter
from datetime import date
import re

COPA_REF = "902e4c51c282244ab924676b3bc471b4040de39a"
COPA_SHA256 = "fe63e5ef5b0390cb15654c5020866d0747c751b9b98ffb477debcce0a3575cdc"

_GROUPS = {
    "Group A": {"Argentina", "Peru", "Chile", "Canada"},
    "Group B": {"Mexico", "Ecuador", "Venezuela", "Jamaica"},
    "Group C": {"United States", "Uruguay", "Panama", "Bolivia"},
    "Group D": {"Brazil", "Colombia", "Paraguay", "Costa Rica"},
}
_SECTIONS = (*_GROUPS, "Quarter-finals", "Semi-finals", "Third place play-off", "Final")
_MATCHDAYS = {
    "Matchday 1 | Thu Jun 20 - Mon Jun 24",
    "Matchday 2 | Tue Jun 25 - Fri Jun 28",
    "Matchday 3 | Sat Jun 29 - Tue Jul 2",
}
_MATCH = re.compile(
    r"(?P<weekday>Mon|Tue|Wed|Thu|Fri|Sat|Sun) (?P<month>Jun|Jul) (?P<day>\d{1,2}) "
    r"(?P<time>(?:[01]\d|2[0-3]):[0-5]\d) (?P<offset>UTC-[457])\s+"
    r"(?P<a>[A-Za-z ]+?)\s+(?P<one>\d{1,2})-(?P<two>\d{1,2})"
    r"(?: (?P<stage>pen\.|a\.e\.t\.) \((?P<ft_a>\d{1,2})-(?P<ft_b>\d{1,2})\))?"
    r"\s+(?P<b>[A-Za-z ]+?)\s+@\s+(?P<ground>[^#]+?)(?:\s+#.*)?"
)


def decode_copa_2024(body: bytes) -> dict:
    """Decode all 32 source records; reject unsupported or incomplete source grammar."""
    lines = body.decode("utf-8").splitlines()
    if not lines or lines[0].strip() != "= Copa América 2024      # in USA":
        raise ValueError("Unexpected Copa America TXT header")
    matches, declarations, matchdays, sections = [], {}, set(), []
    section = None
    nations = set.union(*_GROUPS.values())
    for line_number, line in enumerate(lines[1:], 2):
        line = line.strip()
        if not line:
            continue
        if line.startswith("Group ") and "|" in line:
            group, raw_names = (part.strip() for part in line.split("|", 1))
            names = set(re.split(r"\s{2,}", raw_names))
            if group not in _GROUPS or names != _GROUPS[group] or group in declarations or section is not None:
                raise ValueError(f"Invalid Copa group declaration at line {line_number}")
            declarations[group] = names
            continue
        if line.startswith("▪ Matchday"):
            description = " ".join(line[1:].split())
            if description not in _MATCHDAYS or description in matchdays or section is not None:
                raise ValueError(f"Invalid Copa matchday declaration at line {line_number}")
            matchdays.add(description)
            continue
        if line.startswith("▪ "):
            section = line[2:].strip()
            if len(sections) >= len(_SECTIONS) or section != _SECTIONS[len(sections)]:
                raise ValueError(f"Unexpected Copa section at line {line_number}")
            sections.append(section)
            continue
        if line.startswith("(") and line.endswith(")") and matches:
            # These scorer annotations are not match records or independently timed events.
            continue
        parsed = _MATCH.fullmatch(line)
        if parsed is None or section is None:
            raise ValueError(f"Unsupported Copa match/annotation at line {line_number}")
        values = parsed.groupdict()
        a, b = values["a"].strip(), values["b"].strip()
        if a not in nations or b not in nations or a == b:
            raise ValueError(f"Unknown Copa opponents at line {line_number}")
        if section in _GROUPS and not {a, b} <= _GROUPS[section]:
            raise ValueError(f"Copa opponents outside source group at line {line_number}")
        calendar = date(2024, {"Jun": 6, "Jul": 7}[values["month"]], int(values["day"]))
        # The reviewed source writes 'Sat Jul 14' for the final. Retain its numeric date.
        known_weekday_typo = (section, calendar.isoformat(), values["weekday"], a, b) == (
            "Final", "2024-07-14", "Sat", "Argentina", "Colombia")
        if ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[calendar.weekday()] != values["weekday"] and not known_weekday_typo:
            raise ValueError(f"Unexpected Copa source weekday mismatch at line {line_number}")
        lead = [int(values["one"]), int(values["two"])]
        stage = values["stage"]
        details = {"ft": lead}
        if stage:
            if section in _GROUPS or stage == "a.e.t." and section != "Final":
                raise ValueError(f"Unsupported Copa score stage at line {line_number}")
            details = {"ft": [int(values["ft_a"]), int(values["ft_b"])],
                       "p" if stage == "pen." else "et": lead}
        if any(value > 50 for pair in details.values() for value in pair):
            raise ValueError(f"Unsupported Copa score bound at line {line_number}")
        matches.append(dict(round=section, date=calendar.isoformat(),
                            time=values["time"] + " " + values["offset"],
                            team1=a, team2=b, score=details,
                            group=section if section in _GROUPS else None, ground=values["ground"].strip()))
    expected_counts = dict.fromkeys(_GROUPS, 6) | {
        "Quarter-finals": 4, "Semi-finals": 2, "Third place play-off": 1, "Final": 1}
    if declarations != _GROUPS or matchdays != _MATCHDAYS or tuple(sections) != _SECTIONS:
        raise ValueError("Incomplete Copa source declarations")
    if Counter(row["round"] for row in matches) != expected_counts:
        raise ValueError("Incomplete Copa source coverage; expected 32 matches")
    for group in _GROUPS:
        pairs = [frozenset((row["team1"], row["team2"])) for row in matches if row["round"] == group]
        if len(set(pairs)) != 6:
            raise ValueError("Duplicate Copa group pair")
    return {"name": "Copa América 2024", "matches": matches}
