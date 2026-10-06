"""Offline synthetic contract cases; licensed provider responses never enter Git."""

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from arena_lakehouse.calendar import CalendarError, publish_calendar, validate_calendar

NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
PHASES = ("past", "upcoming", "running")
COMPLETE = dict(id=501, name="Northwind vs Ember", status="finished",
    begin_at="2026-10-05T18:00:00Z", end_at="2026-10-05T21:00:00Z", modified_at="2026-10-05T21:01:00Z",
    scheduled_at="2026-10-05T18:00:00Z", original_scheduled_at="2026-10-05T17:00:00Z",
    number_of_games=3,
    opponents=[dict(type="Team", opponent=dict(id=10, name="Northwind")),
               dict(type="Team", opponent=dict(id=11, name="Ember"))],
    results=[dict(team_id=10, score=2), dict(team_id=11, score=1)],
    league_id=100, league=dict(id=100, name="Original Circuit", image_url="https://unused.example/logo.png"),
    serie_id=200, serie=dict(id=200, name="Autumn", full_name="Original Circuit Autumn 2026",
                           begin_at="2026-10-01", end_at="2026-10-31", season="Autumn", year=2026),
    tournament_id=300, tournament=dict(id=300, name="Stage One", begin_at="2026-10-02T18:00:00Z"))


def archives(tmp_path, payloads=None, *, pages=1, seconds=0):
    root = tmp_path / f"raw-{seconds}"
    root.mkdir(exist_ok=True)
    payloads = payloads or {"past": [copy.deepcopy(COMPLETE)], "upcoming": [], "running": []}
    paths = []
    for page in range(1, pages + 1):
        for index, phase in enumerate(PHASES):
            path = root / f"{phase}-{page}.json"
            requested = NOW + timedelta(seconds=seconds)
            path.write_text(json.dumps(dict(provider="pandascore",
                endpoint=f"valorant/matches/{phase}?page={page}&per_page=100",
                requested_at=requested.isoformat(), observed_at=(requested + timedelta(seconds=index + 1)).isoformat(),
                payload=payloads.get((phase, page), payloads.get(phase, []))), ensure_ascii=False), encoding="utf-8")
            paths.append(path)
    return root, paths


def publish(tmp_path, payloads=None, **kwargs):
    root, paths = archives(tmp_path, payloads, pages=kwargs.get("pages", 1))
    output = tmp_path / "calendars"
    manifest = publish_calendar(paths, output, archive_root=root, generated_at=NOW + timedelta(minutes=1), **kwargs)
    package = output / f"calendar-{manifest['dataset_version']}"
    return package, validate_calendar(package), root, paths


def reseal(package, key, update):
    path = package / f"{key}.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    update(value)
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode() + b"\n"
    path.write_bytes(body)
    manifest_path = package / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][path.name] = hashlib.sha256(body).hexdigest()
    all_data = {item: json.loads((package / f"{item}.json").read_text(encoding="utf-8"))
                for item in ("announcements", "occurrences", "sources", "quality")}
    logical = json.dumps(all_data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    manifest["dataset_version"] = hashlib.sha256(logical).hexdigest()[:16]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def test_complete_rows_metadata_and_explicit_canonical_link(tmp_path):
    package, payload, root, paths = publish(tmp_path, canonical_match_ids=["pandascore:501"])
    row = payload["announcements"][0]
    assert row["announcement_id"] == row["canonical_match_id"] == "pandascore:501"
    assert row["team_a"]["team_id"] == "pandascore:10"
    assert row["start_time"] == "2026-10-05T18:00:00.000000Z"
    assert row["source_dates"]["original_scheduled_at"] == "2026-10-05T17:00:00Z"
    assert row["series"]["id"] == "pandascore:series:200" and row["series"]["begin_at"] == "2026-10-01"
    assert row["tournament"]["name"] == "Stage One" and row["result_known"]
    assert row["pending_reasons"] == [] and not row["conflicted"]
    assert payload["manifest"]["counts"] == dict(source_records=1, announcements=1, duplicate_occurrences=0,
        conflicted_announcements=0, pending_announcements=0, canonical_linked=1, sources=3)
    assert payload["sources"][0]["archive_sha256"] == hashlib.sha256(paths[0].read_bytes()).hexdigest()
    assert (package.parent / "CALENDAR_LATEST").read_text().strip() == package.name
    assert "unused.example" not in (package / "announcements.json").read_text()
    assert str(root) not in (package / "sources.json").read_text()


def test_empty_optional_series_name_is_null_without_fabricated_fallback(tmp_path):
    raw = copy.deepcopy(COMPLETE)
    raw["serie"]["name"] = ""
    _, payload, _, _ = publish(tmp_path, {"past": [raw], "upcoming": [], "running": []})
    row = payload["announcements"][0]
    assert row["series"]["name"] is None
    assert row["series"]["full_name"] == COMPLETE["serie"]["full_name"]
    assert row["series"]["id"] == "pandascore:series:200"
    assert row["pending_reasons"] == [] and row["result_known"]
    assert payload["occurrences"][0]["series"]["name"] is None


def test_tbd_null_opponents_and_missing_clock_are_preserved(tmp_path):
    pending = dict(id=502, name="TBD vs TBD", status="not_started", opponents=[], begin_at=None,
                   scheduled_at="2026-10-20T18:00:00Z", results=[])
    _, payload, _, _ = publish(tmp_path, {"past": [], "upcoming": [pending], "running": []})
    row = payload["announcements"][0]
    assert row["team_a"] is row["team_b"] is row["start_time"] is row["canonical_match_id"] is None
    assert row["source_dates"]["scheduled_at"] == "2026-10-20T18:00:00Z"
    assert row["source_results"] == [] and not row["result_known"]
    assert row["pending_reasons"] == ["unresolved_opponents", "unresolved_start_time"]
    assert payload["quality"]["pending_announcements"] == 1


@pytest.mark.parametrize("clock,expected", [("2026-10-20", ["unresolved_start_time"]),
    ("2026-10-20T19:30:00", ["start_time_timezone_unknown", "unresolved_start_time"]),
    ("2026-10-20T23:30:00-04:00", [])])
def test_source_date_and_unzoned_clock_never_become_midnight_utc(tmp_path, clock, expected):
    raw = dict(COMPLETE, status="not_started", begin_at=clock, results=[])
    _, payload, _, _ = publish(tmp_path, {"past": [], "upcoming": [raw], "running": []})
    row = payload["announcements"][0]
    assert row["source_dates"]["begin_at"] == clock and row["pending_reasons"] == expected
    if expected:
        assert row["start_time"] is None
    else:
        assert row["start_time"] == "2026-10-21T03:30:00.000000Z"


def test_upcoming_zero_results_are_not_a_known_result_and_canceled_is_retained(tmp_path):
    upcoming = dict(COMPLETE, id=502, status="not_started", results=[dict(team_id=10, score=0), dict(team_id=11, score=0)])
    canceled = dict(COMPLETE, id=503, status="canceled", begin_at=None, results=None)
    _, payload, _, _ = publish(tmp_path, {"past": [canceled], "upcoming": [upcoming], "running": []})
    rows = {row["announcement_id"]: row for row in payload["announcements"]}
    assert [result["score"] for result in rows["pandascore:502"]["source_results"]] == [0, 0]
    assert not rows["pandascore:502"]["result_known"]
    assert rows["pandascore:503"]["source_status"] == "canceled" and rows["pandascore:503"]["source_results"] == []
    assert rows["pandascore:503"]["start_time"] is None


def test_unknown_status_null_scores_and_partial_team_do_not_create_identities(tmp_path):
    raw = dict(COMPLETE, status="provider_new_status", opponents=[dict(type="Team", opponent=dict(name="TBD"))],
               results=[dict(team_id=10, score=None)])
    _, payload, _, _ = publish(tmp_path, {"past": [raw], "upcoming": [], "running": []})
    row = payload["announcements"][0]
    assert row["source_status"] == "provider_new_status" and row["team_a"]["team_name"] == "TBD"
    assert row["team_a"]["team_id"] is row["team_b"] is None
    assert row["pending_reasons"] == ["unknown_status", "unresolved_opponents"] and not row["result_known"]


def test_player_objects_are_not_published_as_teams(tmp_path):
    raw = dict(COMPLETE, opponents=[dict(type="Player", opponent=dict(id=900, name="Unused Player"))])
    package, payload, _, _ = publish(tmp_path, {"past": [raw], "upcoming": [], "running": []})
    row = payload["announcements"][0]
    assert row["team_a"] is row["team_b"] is None
    assert "unsupported_opponent_type" in row["pending_reasons"]
    assert "Unused Player" not in (package / "announcements.json").read_text()


def test_identical_phase_duplicates_preserve_every_occurrence(tmp_path):
    _, payload, _, _ = publish(tmp_path, {"past": [COMPLETE], "upcoming": [], "running": [COMPLETE]})
    row = payload["announcements"][0]
    assert len(row["occurrence_ids"]) == 2 and len(payload["occurrences"]) == 2
    assert not row["conflicted"] and row["result_known"]
    assert row["observed_at"] == "2026-10-06T12:00:01.000000Z"
    assert row["last_observed_at"] == "2026-10-06T12:00:03.000000Z"
    assert payload["quality"]["duplicate_occurrences"] == 1


def test_conflicting_phase_records_are_auditable_and_unlinked(tmp_path):
    changed = dict(COMPLETE, status="running", results=[dict(team_id=10, score=1), dict(team_id=11, score=0)])
    _, payload, _, _ = publish(tmp_path, {"past": [COMPLETE], "upcoming": [], "running": [changed]},
                               canonical_match_ids=["pandascore:501"])
    row = payload["announcements"][0]
    assert row["conflicted"] and "conflicting_phase_records" in row["pending_reasons"]
    assert row["canonical_match_id"] is None and not row["result_known"]
    assert row["first_observed_at"] == "2026-10-06T12:00:01.000000Z"
    assert row["observed_at"] == row["last_observed_at"] == "2026-10-06T12:00:03.000000Z"
    assert [occurrence["source_status"] for occurrence in payload["occurrences"]] == ["finished", "running"]
    assert payload["quality"]["conflicted_announcements"] == 1


def test_same_inputs_reuse_immutable_package_excluding_publication_clock(tmp_path):
    package, payload, root, paths = publish(tmp_path)
    bytes_before = {file.name: file.read_bytes() for file in package.iterdir()}
    manifest = publish_calendar(reversed(paths), package.parent, archive_root=root,
                                generated_at=NOW + timedelta(hours=1))
    assert manifest == payload["manifest"]
    assert {file.name: file.read_bytes() for file in package.iterdir()} == bytes_before
    assert len(list(package.parent.glob("calendar-*"))) == 1


def test_new_poll_clock_has_independent_version_without_rewriting_old_package(tmp_path):
    package, payload, _, _ = publish(tmp_path)
    before = (package / "manifest.json").read_bytes()
    root, paths = archives(tmp_path, seconds=60)
    manifest = publish_calendar(paths, package.parent, archive_root=root, generated_at=NOW + timedelta(minutes=2))
    assert manifest["dataset_version"] != payload["manifest"]["dataset_version"]
    assert manifest["as_of"] > payload["manifest"]["as_of"]
    assert (package / "manifest.json").read_bytes() == before


def test_all_three_phases_and_pages_are_required_and_empty_pages_are_valid(tmp_path):
    package, payload, root, paths = publish(tmp_path, {"past": [], "upcoming": [], "running": []}, pages=2)
    assert len(payload["sources"]) == 6 and payload["quality"]["announcements"] == 0
    with pytest.raises(CalendarError, match="Incomplete"):
        publish_calendar(paths[:-1], package.parent, archive_root=root, pages=2)
    with pytest.raises(CalendarError, match="duplicate"):
        publish_calendar(paths[:-1] + [paths[0]], package.parent, archive_root=root, pages=2)


def test_mixed_poll_requests_and_incomplete_endpoint_set_are_rejected(tmp_path):
    root, paths = archives(tmp_path)
    value = json.loads(paths[2].read_text())
    value["requested_at"] = (NOW + timedelta(seconds=1)).isoformat()
    paths[2].write_text(json.dumps(value))
    with pytest.raises(CalendarError, match="one successful"):
        publish_calendar(paths, tmp_path / "out", archive_root=root)
    value["requested_at"] = NOW.isoformat()
    value["endpoint"] = "valorant/matches/past?page=1&per_page=100"
    paths[2].write_text(json.dumps(value))
    with pytest.raises(CalendarError, match="phase/page"):
        publish_calendar(paths, tmp_path / "out", archive_root=root)
    assert not (tmp_path / "out").exists()


def test_outside_root_traversal_and_symlink_are_rejected_before_publication(tmp_path):
    root, paths = archives(tmp_path)
    outside = tmp_path / "outside.json"
    outside.write_bytes(paths[0].read_bytes())
    with pytest.raises(CalendarError, match="escaped"):
        publish_calendar([outside, *paths[1:]], tmp_path / "out", archive_root=root)
    link = root / "link.json"
    try:
        link.symlink_to(outside)
    except OSError:
        return  # Windows symlinks require developer mode; traversal still tested.
    with pytest.raises(CalendarError, match="links"):
        publish_calendar([link, *paths[1:]], tmp_path / "out", archive_root=root)


@pytest.mark.parametrize("change", [lambda value: value.update(provider="football-data"),
    lambda value: value.update(endpoint="https://api.pandascore.co/valorant/matches/past?token=secret"),
    lambda value: value.update(observed_at="2026-10-06T11:59:00Z"),
    lambda value: value.update(observed_at="2026-10-06T12:00:01"),
    lambda value: value.update(payload=[COMPLETE] * 101),
    lambda value: value.update(payload={}),
    lambda value: value.update(headers={"Authorization": "test-credential"})])
def test_bad_archive_envelope_phase_clock_and_page_bounds(tmp_path, change):
    root, paths = archives(tmp_path)
    value = json.loads(paths[0].read_text())
    change(value)
    paths[0].write_text(json.dumps(value))
    with pytest.raises(CalendarError):
        publish_calendar(paths, tmp_path / "out", archive_root=root)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("change", [lambda raw: raw.update(id=True), lambda raw: raw.update(id=-1),
    lambda raw: raw.update(name="x" * 1001), lambda raw: raw.update(begin_at="not a date"),
    lambda raw: raw.update(results=[dict(team_id=10, score=1.5)]),
    lambda raw: raw.update(results=[dict(team_id=10, score=True)]),
    lambda raw: raw.update(results={}), lambda raw: raw.update(number_of_games=True),
    lambda raw: raw.update(league_id=999), lambda raw: raw.update(opponents=[{}, {}, {}])])
def test_invalid_typed_provider_rows_never_publish(tmp_path, change):
    raw = copy.deepcopy(COMPLETE)
    change(raw)
    root, paths = archives(tmp_path, {"past": [raw], "upcoming": [], "running": []})
    with pytest.raises(CalendarError):
        publish_calendar(paths, tmp_path / "out", archive_root=root)
    assert not (tmp_path / "out").exists()


def test_duplicate_json_keys_nonfinite_and_oversized_archive_are_rejected(tmp_path, monkeypatch):
    root, paths = archives(tmp_path)
    paths[0].write_text('{"provider":"pandascore","provider":"pandascore"}')
    with pytest.raises(CalendarError, match="Duplicate"):
        publish_calendar(paths, tmp_path / "out", archive_root=root)
    paths[0].write_text('{"payload":NaN}')
    with pytest.raises(CalendarError, match="Non-finite"):
        publish_calendar(paths, tmp_path / "out", archive_root=root)
    monkeypatch.setattr("arena_lakehouse.calendar.MAX_ARCHIVE_BYTES", 10)
    with pytest.raises(CalendarError, match="oversized"):
        publish_calendar(paths, tmp_path / "out", archive_root=root)


def test_corrupted_package_is_never_reused_or_repointed(tmp_path):
    package, _, root, paths = publish(tmp_path)
    pointer = (package.parent / "CALENDAR_LATEST").read_bytes()
    (package / "announcements.json").write_text("[]")
    with pytest.raises(CalendarError, match="checksum"):
        validate_calendar(package)
    with pytest.raises(CalendarError, match="checksum"):
        publish_calendar(paths, package.parent, archive_root=root, generated_at=NOW + timedelta(minutes=2))
    assert (package.parent / "CALENDAR_LATEST").read_bytes() == pointer


@pytest.mark.parametrize("key,change", [("occurrences", lambda rows: rows[0].update(start_time="2026-10-05T19:00:00Z")),
    ("occurrences", lambda rows: rows[0]["source_results"][0].update(team_id="pandascore:999")),
    ("occurrences", lambda rows: rows[0].update(observed_at="2026-10-06T12:00:02Z")),
    ("announcements", lambda rows: rows[0].update(canonical_match_id="pandascore:999")),
    ("sources", lambda rows: rows[0].update(record_count=2)),
    ("quality", lambda value: value.update(pending_announcements=1))])
def test_resealed_internal_semantic_mutations_are_rejected(tmp_path, key, change):
    package, _, _, _ = publish(tmp_path)
    reseal(package, key, change)
    with pytest.raises(CalendarError):
        validate_calendar(package)


def test_failed_validation_retains_last_good_pointer(tmp_path):
    package, _, _, _ = publish(tmp_path)
    pointer = (package.parent / "CALENDAR_LATEST").read_bytes()
    root, paths = archives(tmp_path, {"past": [dict(COMPLETE, id=0)], "upcoming": [], "running": []}, seconds=60)
    with pytest.raises(CalendarError):
        publish_calendar(paths, package.parent, archive_root=root, generated_at=NOW + timedelta(minutes=2))
    assert (package.parent / "CALENDAR_LATEST").read_bytes() == pointer
    validate_calendar(package)


@pytest.mark.parametrize("field,change", [("counts", lambda value: value.update(announcements=True)),
    ("source_scope", lambda value: value.update(pages=True)),
    ("source_scope", lambda value: value.update(competition_codes=["unscoped"]))])
def test_manifest_scope_and_counts_have_strict_types(tmp_path, field, change):
    package, _, _, _ = publish(tmp_path)
    path = package / "manifest.json"
    manifest = json.loads(path.read_text())
    change(manifest[field])
    path.write_text(json.dumps(manifest))
    with pytest.raises(CalendarError):
        validate_calendar(package)


def test_write_failure_before_publish_preserves_previous_pointer_and_cleans_staging(tmp_path, monkeypatch):
    package, _, _, _ = publish(tmp_path)
    root, paths = archives(tmp_path, seconds=60)
    pointer = (package.parent / "CALENDAR_LATEST").read_bytes()
    def fail(*_args):
        raise OSError("Synthetic disk failure")
    monkeypatch.setattr("arena_lakehouse.calendar.os.fsync", fail)
    with pytest.raises(OSError, match="disk failure"):
        publish_calendar(paths, package.parent, archive_root=root, generated_at=NOW + timedelta(minutes=2))
    assert (package.parent / "CALENDAR_LATEST").read_bytes() == pointer
    assert not list(package.parent.glob(".calendar-*.tmp"))
    assert len(list(package.parent.glob("calendar-*"))) == 1


def test_many_records_across_two_pages_preserve_all_source_records(tmp_path):
    pages = {("past", 1): [dict(COMPLETE, id=1000 + i) for i in range(100)],
             ("upcoming", 1): [dict(COMPLETE, id=2000 + i, status="not_started", begin_at=None,
                                   opponents=[], results=[]) for i in range(24)],
             ("past", 2): [dict(COMPLETE, id=3000)], "upcoming": [], "running": []}
    _, payload, _, _ = publish(tmp_path, pages, pages=2)
    assert payload["quality"]["source_records"] == payload["quality"]["announcements"] == 125
    assert payload["quality"]["unresolved_opponents"] == payload["quality"]["unresolved_start_time"] == 24
    assert payload["quality"]["results_known"] == 101
    assert sum(source["record_count"] for source in payload["sources"]) == 125


def test_sensitive_unrecognized_raw_fields_are_not_exported(tmp_path):
    raw = dict(COMPLETE, streams_list=[{"raw_secret": "fake-secret-synthetic"}],
               live={"player": "private-raw-field"}, authorization="fake-synthetic-token")
    package, _, _, _ = publish(tmp_path, {"past": [raw], "upcoming": [], "running": []})
    for path in package.iterdir():
        body = path.read_text(encoding="utf-8")
        assert "fake-secret-synthetic" not in body and "private-raw-field" not in body
        assert "fake-synthetic-token" not in body


@pytest.mark.parametrize("extra", ["untracked.json", "untracked-directory"])
def test_exact_package_inventory_rejects_untracked_files_and_directories(tmp_path, extra):
    package, _, root, paths = publish(tmp_path)
    destination = package / extra
    if extra.endswith(".json"):
        destination.write_text('{"synthetic-untracked":"not-publication-data"}')
    else:
        destination.mkdir()
    with pytest.raises(CalendarError, match="inventory"):
        validate_calendar(package)
    with pytest.raises(CalendarError, match="inventory"):
        publish_calendar(paths, package.parent, archive_root=root, generated_at=NOW + timedelta(minutes=2))
