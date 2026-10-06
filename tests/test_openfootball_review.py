"""Independent collector regressions identified during the release review.

The source-title fixtures were checked against the original raw OpenFootball
files on 2026-10-06. These tests never access the network.
"""

import json
import gc
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from arena_lakehouse.openfootball import OpenFootballCollector, load_sources, parse_payload
from arena_lakehouse import openfootball


CATALOGUE = Path(__file__).parents[1] / "config/openfootball_sources.json"
NOW = datetime(2026, 10, 6, 16, tzinfo=timezone.utc)


def match_body(name):
    return json.dumps({"name": name, "matches": [{
        "round": "Matchday 1", "date": "2026-08-21",
        "team1": "Independent Club A", "team2": "Independent Club B",
        "score": {"ft": [2, 1]},
    }]}, ensure_ascii=False).encode("utf-8")


@pytest.mark.parametrize("source_id,reviewed_title", [
    ("spain-primera-2026-27", "Spain Primera Divisi\u00f3n 2026/27"),
    ("brazil-serie-a-2026", "Brasileiro S\u00e9rie A 2026"),
])
def test_catalogue_unicode_titles_accept_reviewed_upstream_schema(source_id, reviewed_title):
    source = load_sources(CATALOGUE)[source_id]
    assert source.name == reviewed_title
    parsed = parse_payload(match_body(reviewed_title), source, NOW)
    assert len(parsed.rows) == parsed.input_rows == 1
    assert parsed.quarantine == []
    assert parsed.rows[0]["competition"] == reviewed_title


@pytest.mark.parametrize("folder", ["open#football", "league%20data", "open football"])
def test_workspace_filename_characters_do_not_change_sqlite_uri_identity(tmp_path, folder):
    source = load_sources(CATALOGUE)["england-premier-2026-27"]
    collector = OpenFootballCollector(tmp_path / folder)
    raw = match_body(source.name)
    first = collector.ingest_bytes(source, raw, observed_at=NOW)
    assert first.changed == 1
    replay = collector.ingest_bytes(source, raw, observed_at=NOW + timedelta(minutes=1))
    assert replay.changed == 0 and replay.unchanged == 1
    dry = collector.ingest_bytes(source, raw, observed_at=NOW + timedelta(minutes=2), dry_run=True)
    assert dry.unchanged == 1
    manifest = collector.export(tmp_path / "export")
    assert manifest["matches"] == manifest["revisions"] == 1


def test_disconnected_response_retries_a_transient_protocol_failure(tmp_path):
    source = load_sources(CATALOGUE)["england-premier-2026-27"]
    calls = []

    def response(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.RemoteProtocolError("Server disconnected without sending a response", request=request)
        return httpx.Response(200, content=match_body(source.name))

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, attempts=2, min_interval=0, sleep=lambda _: None)
        report = collector.collect(source, observed_at=NOW)
    assert len(calls) == 2 and report.changed == 1


def test_local_protocol_misconfiguration_is_not_retried(tmp_path):
    source = load_sources(CATALOGUE)["england-premier-2026-27"]
    calls = []

    def response(request):
        calls.append(request)
        raise httpx.LocalProtocolError("Invalid local request header", request=request)

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        collector = OpenFootballCollector(tmp_path, client, attempts=2, min_interval=0, sleep=lambda _: None)
        with pytest.raises(httpx.LocalProtocolError):
            collector.collect(source, observed_at=NOW)
    assert len(calls) == 1 and not collector.state_path.exists()


@pytest.mark.parametrize("operation", ["read_cache", "export"])
def test_readonly_operations_close_connections_before_returning(tmp_path, monkeypatch, operation):
    source = load_sources(CATALOGUE)["england-premier-2026-27"]
    collector = OpenFootballCollector(tmp_path / "state")
    collector.ingest_bytes(source, match_body(source.name), observed_at=NOW)
    original_connect = sqlite3.connect
    connections = []

    def retained_connection(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(openfootball.sqlite3, "connect", retained_connection)
    try:
        if operation == "read_cache":
            collector._read_cache(source)
        else:
            collector.export(tmp_path / "export")
        assert len(connections) == 1
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connections[0].execute("SELECT 1")
    finally:
        for connection in connections:
            connection.close()


def test_windows_workspace_can_move_state_immediately_after_replay_and_export(tmp_path):
    """Windows denies moving an open SQLite file; do not rely on cyclic GC."""
    source = load_sources(CATALOGUE)["england-premier-2026-27"]
    collector = OpenFootballCollector(tmp_path / "state")
    enabled = gc.isenabled()
    gc.disable()
    try:
        collector.ingest_bytes(source, match_body(source.name), observed_at=NOW)
        collector.ingest_bytes(source, match_body(source.name), observed_at=NOW + timedelta(minutes=1))
        collector.export(tmp_path / "export")
        moved = tmp_path / "completed.sqlite"
        collector.state_path.replace(moved)
        assert moved.is_file() and not collector.state_path.exists()
    finally:
        if enabled:
            gc.enable()
        gc.collect()
