from datetime import datetime, timezone

import httpx
import pytest

from arena_lakehouse.connectors import ProviderError, collect, normalize_football, normalize_valorant, request_json
from arena_lakehouse.schema import MatchEvent

NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
FOOTBALL = dict(id=100, status="FINISHED", utcDate="2026-10-05T18:00:00Z",
                competition={"name": "Premier League"}, homeTeam={"id": 1, "name": "Club One"},
                awayTeam={"id": 2, "name": "Club Two"}, score={"fullTime": {"home": 2, "away": 1}})
VALORANT = dict(id=200, status="finished", begin_at="2026-10-05T18:00:00Z", end_at="2026-10-05T21:00:00Z",
                tournament={"name": "VCT"}, number_of_games=3,
                opponents=[{"opponent": {"id": 1, "name": "Team One"}}, {"opponent": {"id": 2, "name": "Team Two"}}],
                results=[{"team_id": 1, "score": 2}, {"team_id": 2, "score": 1}])


@pytest.mark.parametrize("normalizer,payload", [(normalize_football, FOOTBALL), (normalize_valorant, VALORANT)])
def test_normalization_has_provider_scoped_ids_and_actual_availability(normalizer, payload):
    row = normalizer(payload, NOW)
    model = MatchEvent.model_validate(dict(row, revision=1, event_id="test"))
    assert model.observed_at == NOW
    assert model.team_a_id.startswith(model.provider + ":")
    assert model.completed_at <= NOW


def test_missing_token_is_actionable(tmp_path, monkeypatch):
    monkeypatch.delenv("FOOTBALL_DATA_TOKEN", raising=False)
    with pytest.raises(ProviderError, match="FOOTBALL_DATA_TOKEN"):
        collect("football-data", tmp_path / "cursor.json")


def test_403_never_retries_or_exposes_token():
    attempts = []
    def response(request):
        attempts.append(request)
        return httpx.Response(403, json={"message": "restricted"})
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        with pytest.raises(ProviderError, match="access denied"):
            request_json(client, "https://example.test", {"Authorization": "private"}, {})
    assert len(attempts) == 1


def test_poll_emits_only_changed_observations_and_does_not_commit_cursor(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "test-only-not-a-real-token")
    cursor = tmp_path / "cursor.json"
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"matches": [FOOTBALL]}))) as client:
        rows, meta = collect("football-data", cursor, client=client)
    assert len(rows) == 1 and not cursor.exists()
    from arena_lakehouse.connectors import commit_state
    commit_state(cursor, meta["state"])
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"matches": [FOOTBALL]}))) as client:
        rows, _ = collect("football-data", cursor, client=client)
    assert rows == []


def test_crash_after_data_commit_before_cursor_does_not_quarantine_valid_result(tmp_path, monkeypatch):
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "test-only-not-a-real-token")
    from arena_lakehouse.engine import Pipeline
    pipeline = Pipeline(tmp_path / "pipeline")
    cursor = tmp_path / "cursor.json"
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"matches": [FOOTBALL]}))) as client:
        first, _ = collect("football-data", cursor, client=client)
        pipeline.ingest(first)
        # Cursor remains absent: simulate a crash before its separate atomic write.
        retry, _ = collect("football-data", cursor, client=client)
        result = pipeline.ingest(retry)
    assert result["duplicates"] == 1 and result["quarantined"] == 0
    assert len(pipeline.events()) == 1


def test_unnormalized_provider_response_is_preserved_before_schema_failure(tmp_path, monkeypatch):
    import json
    monkeypatch.setenv("FOOTBALL_DATA_TOKEN", "test-only-not-a-real-token")
    invalid = dict(FOOTBALL, score={"fullTime": {"home": -1, "away": 1}}, new_provider_field="retained")
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"matches": [invalid]}))) as client:
        with pytest.raises(ValueError):
            collect("football-data", tmp_path / "cursor.json", client=client)
    archived = list((tmp_path / "provider_raw").rglob("*.json"))
    assert len(archived) == 1
    body = json.loads(archived[0].read_text(encoding="utf-8"))
    assert body["payload"]["matches"][0] == invalid
    assert "test-only-not-a-real-token" not in archived[0].read_text(encoding="utf-8")
    assert not (tmp_path / "cursor.json").exists()


def test_network_failure_retries_within_budget_and_never_exposes_headers(monkeypatch):
    calls = []
    def unavailable(request):
        calls.append(request)
        raise httpx.ConnectError("temporary outage", request=request)
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _: None)
    with httpx.Client(transport=httpx.MockTransport(unavailable)) as client:
        with pytest.raises(ProviderError, match="network outage") as error:
            request_json(client, "https://example.test", {"Authorization": "test-secret"}, {})
    assert len(calls) == 3
    assert "test-secret" not in str(error.value)


def test_valorant_polls_running_as_well_as_past_and_upcoming(tmp_path, monkeypatch):
    monkeypatch.setenv("PANDASCORE_TOKEN", "test-only-not-a-real-token")
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _: None)
    requested = []
    def respond(request):
        phase = request.url.path.rsplit("/", 1)[-1]
        requested.append(phase)
        payload = dict(VALORANT)
        if phase != "past":
            payload.update(id=201 if phase == "upcoming" else 202,
                status="not_started" if phase == "upcoming" else "running", end_at=None, results=[])
        return httpx.Response(200, json=[payload])
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        rows, meta = collect("pandascore", tmp_path / "cursor.json", client=client)
    assert requested == ["past", "upcoming", "running"]
    assert {r["status"] for r in rows} == {"finished", "scheduled", "running"}
    assert len(meta["raw_archives"]) == 3


def test_result_finishing_during_request_is_observed_after_response(tmp_path, monkeypatch):
    import json
    from datetime import timedelta

    monkeypatch.setenv("PANDASCORE_TOKEN", "test-only-not-a-real-token")
    monkeypatch.setattr("arena_lakehouse.connectors.time.sleep", lambda _: None)
    clock = [NOW]
    payload = dict(VALORANT, begin_at=(NOW - timedelta(hours=3)).isoformat(),
                   end_at=(NOW + timedelta(seconds=1)).isoformat())

    def response(request):
        clock[0] = NOW + timedelta(seconds=2)
        return httpx.Response(200, json=[payload] if request.url.path.endswith("/past") else [])

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        rows, meta = collect("pandascore", tmp_path / "cursor.json", client=client, clock=lambda: clock[0])
    assert len(rows) == 1
    assert MatchEvent.model_validate(rows[0]).observed_at == NOW + timedelta(seconds=2)
    archive = json.loads(open(meta["raw_archives"][0], encoding="utf-8").read())
    assert datetime.fromisoformat(archive["requested_at"]) == NOW
    assert datetime.fromisoformat(archive["observed_at"]) == NOW + timedelta(seconds=2)
