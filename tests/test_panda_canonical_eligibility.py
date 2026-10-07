"""Original synthetic regressions for calendar evidence versus strict matches."""

import copy
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from arena_lakehouse.calendar import validate_calendar
from arena_lakehouse.connectors import normalize_valorant
from arena_lakehouse.live import LiveConfig, LiveWorker
from arena_lakehouse.provider_publication import PublicationError, current_publication, validate_intent

NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
RUNNING = dict(id=501, name="Original synthetic fixture", status="running", begin_at="2026-10-05T18:00:00Z",
               end_at=None, tournament={"id": 61, "name": "Original synthetic circuit"}, number_of_games=3,
               opponents=[{"type": "Team", "opponent": {"id": 21, "name": "Synthetic One"}},
                          {"type": "Team", "opponent": {"id": 22, "name": "Synthetic Two"}}],
               results=[{"team_id": 21, "score": 1}, {"team_id": 22, "score": 0}])


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        return self.value

    def advance(self):
        self.value += timedelta(seconds=60)


@pytest.fixture(autouse=True)
def synthetic_credentials(monkeypatch):
    monkeypatch.setenv("PANDASCORE_TOKEN", "original-synthetic-only")
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "original-synthetic-only")
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _seconds: None)


def panda_client(phases):
    return httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
        200, json=phases.get(request.url.path.rsplit("/", 1)[-1], []))))


def selected_calendar(worker, clock):
    selected = current_publication(worker.root, now=clock())
    return selected, validate_calendar(worker.root / "calendars" / selected["calendar"]["basename"])


def test_cross_phase_conflict_preserves_history_cursor_and_resolves_next_poll(tmp_path):
    clock, phases = Clock(), {"running": [copy.deepcopy(RUNNING)]}
    finished = dict(copy.deepcopy(RUNNING), status="finished", end_at="2026-10-05T20:00:00Z",
                    results=[{"team_id": 21, "score": 2}, {"team_id": 22, "score": 0}])
    with panda_client(phases) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        assert worker.poll_once()["status"] == "ready"
        original, _ = selected_calendar(worker, clock)
        original_events, original_cursor = worker.pipeline.events(), worker.cursor.read_bytes()
        phases["past"] = [finished]
        clock.advance()
        conflict = worker.poll_once()
        conflicted, calendar = selected_calendar(worker, clock)
        assert conflict["status"] == "ready" and conflict["last_poll"]["changed_rows"] == 0
        assert conflict["last_poll"]["unsupported_rows"] == 2
        assert conflicted["canonical"] == original["canonical"]
        assert worker.pipeline.events() == original_events and worker.cursor.read_bytes() == original_cursor
        assert worker.pipeline.quality()["summary"]["quarantined"] == 0
        assert calendar["announcements"][0]["conflicted"] is True
        assert calendar["announcements"][0]["canonical_match_id"] is None
        assert "conflicting_phase_records" in calendar["announcements"][0]["pending_reasons"]
        phases["running"] = []
        clock.advance()
        resolved = worker.poll_once()
        latest, calendar = selected_calendar(worker, clock)
    assert resolved["last_poll"]["changed_rows"] == 1 and resolved["last_poll"]["unsupported_rows"] == 0
    assert latest["canonical"] != original["canonical"]
    assert [row["revision"] for row in worker.pipeline.events()] == [1, 2]
    assert [row["status"] for row in worker.pipeline.events()] == ["running", "finished"]
    assert worker.pipeline.quality()["summary"]["quarantined"] == 0
    assert calendar["announcements"][0]["conflicted"] is False
    assert calendar["announcements"][0]["canonical_match_id"] == "pandascore:501"


def test_duplicate_finished_result_preserves_history_cursor_then_corrected_poll_converges(tmp_path):
    clock, phases = Clock(), {"running": [copy.deepcopy(RUNNING)]}
    ambiguous = dict(copy.deepcopy(RUNNING), status="finished", end_at="2026-10-05T20:00:00Z",
                     results=[{"team_id": 21, "score": 1}, {"team_id": 21, "score": 2},
                              {"team_id": 22, "score": 0}])
    with panda_client(phases) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        assert worker.poll_once()["status"] == "ready"
        original, _ = selected_calendar(worker, clock)
        events, cursor = worker.pipeline.events(), worker.cursor.read_bytes()
        phases["running"] = []
        phases["past"] = [ambiguous]
        clock.advance()
        unsupported = worker.poll_once()
        retained, calendar = selected_calendar(worker, clock)
        announcement = calendar["announcements"][0]
        assert unsupported["last_poll"]["changed_rows"] == 0
        assert unsupported["last_poll"]["unsupported_rows"] == 1
        assert retained["canonical"] == original["canonical"] and worker.pipeline.events() == events
        assert worker.cursor.read_bytes() == cursor and worker.pipeline.quality()["summary"]["quarantined"] == 0
        assert announcement["result_known"] is False and "result_unavailable" in announcement["pending_reasons"]
        assert [row["source_team_id"] for row in announcement["source_results"]] == [21, 21, 22]
        assert [row["score"] for row in announcement["source_results"]] == [1, 2, 0]
        corrected = copy.deepcopy(ambiguous)
        corrected["results"] = corrected["results"][1:]
        phases["past"] = [corrected]
        clock.advance()
        resolved = worker.poll_once()
        latest, calendar = selected_calendar(worker, clock)
    assert resolved["last_poll"]["changed_rows"] == 1 and resolved["last_poll"]["unsupported_rows"] == 0
    assert latest["canonical"] != original["canonical"]
    assert [(row["revision"], row["status"], row["score_a"]) for row in worker.pipeline.events()] == [
        (1, "running", 1), (2, "finished", 2)]
    assert worker.pipeline.quality()["summary"]["quarantined"] == 0
    assert calendar["announcements"][0]["result_known"] is True


@pytest.mark.parametrize("status", ["not_started", "running", "finished", "canceled", "postponed"])
@pytest.mark.parametrize("case", ["missing_identity", "wrong_identity", "duplicate_identity", "partial_identity"])
def test_normalizer_excludes_ambiguous_result_identities_for_every_status(status, case):
    raw = dict(copy.deepcopy(RUNNING), status=status, end_at="2026-10-05T20:00:00Z" if status == "finished" else None)
    raw["results"][0]["score"] = 2
    if case == "missing_identity":
        raw["results"][0].pop("team_id")
    elif case == "wrong_identity":
        raw["results"][0]["team_id"] = 999
    elif case == "duplicate_identity":
        raw["results"].insert(0, {"team_id": 21, "score": 1})
    else:
        raw["results"] = raw["results"][:1]
    assert normalize_valorant(raw, NOW) is None


def test_normalizer_requires_confirmed_finished_scores_and_preserves_unknown_running_scores():
    raw = dict(copy.deepcopy(RUNNING), status="finished", end_at="2026-10-05T20:00:00Z")
    raw["results"][0]["score"] = None
    assert normalize_valorant(raw, NOW) is None
    raw["status"], raw["end_at"] = "running", None
    normalized = normalize_valorant(raw, NOW)
    assert normalized["score_a"] is None and normalized["score_b"] == 0
    raw["results"] = []
    normalized = normalize_valorant(raw, NOW)
    assert normalized["score_a"] is normalized["score_b"] is None


@pytest.mark.parametrize("case", ["date_only", "unzoned", "bo101", "score1000", "tournament_null",
                                  "player", "team_name_null", "format_null"])
def test_valid_broader_source_facts_publish_calendar_only(tmp_path, case):
    raw, clock = copy.deepcopy(RUNNING), Clock()
    if case == "date_only":
        raw["begin_at"] = "2026-10-05"
    elif case == "unzoned":
        raw["begin_at"] = "2026-10-05T18:00:00"
    elif case == "bo101":
        raw["number_of_games"] = 101
    elif case == "score1000":
        raw["results"][0]["score"] = 1000
    elif case == "tournament_null":
        raw["tournament"] = None
    elif case == "player":
        raw["opponents"][0]["type"] = "Player"
    elif case == "team_name_null":
        raw["opponents"][0]["opponent"]["name"] = None
    else:
        raw["number_of_games"] = None
    with panda_client({"running": [raw]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        status = worker.poll_once()
        selected, calendar = selected_calendar(worker, clock)
    assert status["status"] == "empty" and selected["canonical"] is None
    assert status["last_poll"] == dict(source_rows=1, changed_rows=0, unsupported_rows=1, requests=3)
    assert worker.pipeline.events() == [] and json.loads(worker.cursor.read_text()) == {}
    announcement = calendar["announcements"][0]
    assert announcement["canonical_match_id"] is None
    assert announcement["source_dates"]["begin_at"] == raw["begin_at"]
    assert announcement["source_number_of_games"] == raw["number_of_games"]
    if case in ("date_only", "unzoned"):
        assert announcement["start_time"] is None
    if case == "score1000":
        assert announcement["source_results"][0]["score"] == 1000
    if case == "player":
        assert announcement["team_a"] is None and "unsupported_opponent_type" in announcement["pending_reasons"]
    if case == "tournament_null":
        assert announcement["tournament"] is None


@pytest.mark.parametrize("case", ["negative_score", "fractional_score", "boolean_id", "bad_date", "wrong_tournament_type"])
def test_malformed_calendar_source_fails_before_any_canonical_ingest(tmp_path, case, monkeypatch):
    clock, phases = Clock(), {"running": [copy.deepcopy(RUNNING)]}
    with panda_client(phases) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        worker.poll_once()
        previous, _ = selected_calendar(worker, clock)
        events, cursor = worker.pipeline.events(), worker.cursor.read_bytes()
        bad = copy.deepcopy(RUNNING)
        if case == "negative_score":
            bad["results"][0]["score"] = -1
        elif case == "fractional_score":
            bad["results"][0]["score"] = 1.5
        elif case == "boolean_id":
            bad["id"] = True
        elif case == "bad_date":
            bad["begin_at"] = "not-a-source-date"
        else:
            bad["tournament"] = "wrong-type"
        phases["past"] = [dict(copy.deepcopy(RUNNING), id=502)]
        phases["upcoming"] = [bad]
        phases["running"] = []
        ingest_calls = []
        monkeypatch.setattr(worker.pipeline, "ingest", lambda rows: ingest_calls.append(rows))
        clock.advance()
        failed = worker.poll_once()
    assert failed["status"] == "degraded" and failed["last_error_code"] == "invalid_provider_payload"
    assert ingest_calls == [] and worker.pipeline.events() == events and worker.cursor.read_bytes() == cursor
    assert current_publication(worker.root, now=clock()) == previous


def test_truncated_panda_cursor_intent_cannot_omit_selected_canonical_identity(tmp_path):
    clock = Clock()
    with panda_client({"running": [RUNNING]}) as client:
        worker = LiveWorker(tmp_path, LiveConfig("pandascore"), client=client, clock=clock)
        worker.poll_once()
    with worker.ledger.connect() as db:
        row = db.execute("SELECT i.payload,p.started_at FROM publication_intents i JOIN polls p ON p.id=i.poll_id").fetchone()
    intent = json.loads(row["payload"])
    assert validate_intent(worker.root, intent, now=clock(), started_at=row["started_at"]) == intent
    intent["cursors"][0]["state"] = {}
    with pytest.raises(PublicationError, match="omit selected canonical identities"):
        validate_intent(worker.root, intent, now=clock(), started_at=row["started_at"])


def test_complete_scope_cursor_union_rejects_one_truncated_football_cursor(tmp_path):
    clock = Clock()
    def respond(request):
        code = request.url.path.split("/")[-2]
        raw = dict(id=601 if code == "PL" else 602, status="SCHEDULED", utcDate="2026-10-07T18:00:00Z",
                   competition={"name": code}, homeTeam={"id": 31, "name": "Synthetic Home"},
                   awayTeam={"id": 32, "name": "Synthetic Away"}, score={"fullTime": {"home": None, "away": None}})
        return httpx.Response(200, json={"matches": [raw]})
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        worker = LiveWorker(tmp_path, LiveConfig("football-data", competitions=("PL", "PD")), client=client, clock=clock)
        assert worker.poll_once()["status"] == "ready"
    with worker.ledger.connect() as db:
        row = db.execute("SELECT i.payload,p.started_at FROM publication_intents i JOIN polls p ON p.id=i.poll_id").fetchone()
    intent = json.loads(row["payload"])
    assert validate_intent(worker.root, intent, now=clock(), started_at=row["started_at"]) == intent
    intent["cursors"][0]["state"] = {}
    with pytest.raises(PublicationError, match="omit selected canonical identities"):
        validate_intent(worker.root, intent, now=clock(), started_at=row["started_at"])
