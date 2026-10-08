"""Original TEST ONLY historical observations; no downloaded bodies or network."""

import copy
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from arena_lakehouse.football_details import (
    INPUT_VERSION, LICENSE, LICENSE_URL, MATCH_MEMBER, PROVIDER, SCHEMA_VERSION,
    SCOPE, SOURCE_SPEC, FootballDetailsError, build_package, decode_name, main,
)

OBSERVED = "2026-10-08T09:00:00Z"


def fixture_observations(duration="Regular"):
    players, team_rows, team_data = [], [], {}
    for team_id, side, offset in ((11, "home", 1000), (12, "away", 2000)):
        lineup, bench = [], []
        for index in range(13):
            player_id = offset + index + 1
            players.append(dict(wyId=player_id, shortName=f"TEST ONLY {player_id} Mbapp\\u00e9",
                                role=dict(name="Forward", code2="FW", code3="FWD"), foot="right",
                                currentTeamId=999, currentNationalTeamId=999,
                                birthDate="2000-01-01", height=190, weight=80))
            participant = dict(playerId=player_id, assists="0", goals="null" if index == 1 else "0",
                               ownGoals="0", yellowCards="92" if index == 0 else "0", redCards="0")
            (lineup if index < 11 else bench).append(participant)
        scores = {"Regular": (2, 1, 0, 0, 0, 0), "ExtraTime": (1, 1, 2, 1, 0, 0),
                  "Penalties": (1, 1, 2, 2, 3, 4)}[duration]
        team_data[str(team_id)] = dict(teamId=team_id, side=side, hasFormation=1,
            score=scores[0 if side == "home" else 1], scoreHT=0,
            scoreET=scores[2 if side == "home" else 3], scoreP=scores[4 if side == "home" else 5],
            formation=dict(lineup=lineup, bench=bench,
                substitutions=[dict(playerIn=offset + 12, playerOut=offset + 1, minute=0, assists="0")]))
        team_rows.append(dict(wyId=team_id, name=f"TEST ONLY {side}", officialName=f"TEST ONLY {side}",
                              type="national", area=dict(name="TEST ONLY area", alpha2code="ZZ", alpha3code="ZZZ")))
    matches = [dict(wyId=101, competitionId=28, seasonId=10078, roundId=1, status="Played", duration=duration,
                    winner=12 if duration == "Penalties" else 11, teamsData=team_data,
                    dateutc="2018-06-14 15:00:00", date="June 14, 2018 at 5:00:00 PM GMT+2",
                    label="TEST ONLY historical fixture", venue="TEST ONLY venue", groupName="")]
    return matches, players, team_rows


def inputs(tmp_path, observations=None, *, extra_member=False):
    observations = observations or fixture_observations()
    originals = tmp_path / "originals"
    originals.mkdir()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MATCH_MEMBER, json.dumps(observations[0]).encode())
        if extra_member:
            archive.writestr("../TEST ONLY never extracted.json", b"invalid unrelated JSON")
    bodies = {"matches": buffer.getvalue(), "players": json.dumps(observations[1]).encode(),
              "teams": json.dumps(observations[2]).encode()}
    descriptor = dict(schema_version=INPUT_VERSION, provider=PROVIDER, scope=SCOPE, sources=[])
    paths = []
    for role, body in bodies.items():
        article, version, file_id, filename = SOURCE_SPEC[role]
        path = originals / filename
        path.write_bytes(body)
        paths.append(path)
        descriptor["sources"].append(dict(role=role, article_id=article, article_version=version, file_id=file_id,
            filename=filename, url=f"https://ndownloader.figshare.com/files/{file_id}",
            sha256=hashlib.sha256(body).hexdigest(), md5=hashlib.md5(body).hexdigest(), bytes=len(body),
            observed_at=OBSERVED, metadata_observed_at="2026-10-08T08:00:00Z", metadata_sha256="a" * 64,
            published_at="2019-05-06T13:00:00Z", license=LICENSE, license_url=LICENSE_URL))
    manifest_path = originals / "source-manifest.json"
    manifest_path.write_text(json.dumps(descriptor), encoding="utf-8")
    return paths, manifest_path, descriptor


def publish(tmp_path, observations=None, **options):
    paths, descriptor, _ = inputs(tmp_path, observations, **options)
    output = tmp_path / "package"
    manifest = build_package(*paths, descriptor, output)
    details = json.loads((output / "details.json").read_text(encoding="utf-8"))
    return manifest, details, output, paths, descriptor


def amend_descriptor(path, mutator):
    value = json.loads(path.read_text())
    mutator(value)
    path.write_text(json.dumps(value), encoding="utf-8")


def replace_source(paths, descriptor_path, role, body):
    index = list(SOURCE_SPEC).index(role)
    paths[index].write_bytes(body)
    def update(value):
        source = value["sources"][index]
        source.update(sha256=hashlib.sha256(body).hexdigest(), md5=hashlib.md5(body).hexdigest(), bytes=len(body))
    amend_descriptor(descriptor_path, update)


def test_publish_preserves_zero_unknowns_source_identity_and_excludes_biographies(tmp_path):
    manifest, details, output, _, _ = publish(tmp_path)
    assert manifest["record_counts"] == dict(matches=1, teams=2, players=26, starters=22, bench=4, substitutions=2)
    match = details["matches"][0]
    assert match["match_id"] == f"{PROVIDER}:match:101"
    assert [team["side"] for team in match["teams"]] == ["home", "away"]
    assert match["kickoff_at"] == "2018-06-14T15:00:00.000000Z" and match["completed_at"] is None
    assert match["created_at"] == "2026-10-08T09:00:00.000000Z" and match["deleted_at"] is None
    starters = match["teams"][0]["starters"]
    assert starters[0]["statistics"]["goals"] == 0
    assert starters[1]["statistics"]["goals"] is None
    assert starters[1]["native_statistics"]["goals"] == "null"
    assert starters[0]["native_card_fields"]["yellowCards"] == "92"
    assert match["teams"][0]["substitutions"][0]["minute"] == 0
    assert details["players"][0]["name"].endswith("Mbappé")
    assert details["players"][0]["native_name"].endswith(r"Mbapp\u00e9")
    assert details["players"][0]["generic_deposit_role"]["name"] == "Forward"
    assert manifest["quality"]["participant_unknown_goals"] == 2
    assert manifest["capabilities"]["pre_match_availability_evidence"] is False
    serialized = (output / "details.json").read_text(encoding="utf-8")
    for forbidden in ("currentTeamId", "currentNationalTeamId", "birthDate", "2000-01-01", '"height"',
                      '"weight"', '"shirt_number"', '"tactical_position"', '"yellow_cards"'):
        assert forbidden not in serialized
    assert match["quality_score"] == 1.0
    body = (output / "details.json").read_bytes()
    assert manifest["files"] == [dict(path="details.json", bytes=len(body), sha256=hashlib.sha256(body).hexdigest())]


@pytest.mark.parametrize("duration,final,extra,shootout,winner", [
    ("Regular", dict(home=2, away=1), None, None, 11),
    ("ExtraTime", dict(home=2, away=1), dict(home=2, away=1), None, 11),
    ("Penalties", dict(home=2, away=2), dict(home=2, away=2), dict(home=3, away=4), 12),
])
def test_score_stages_never_add_shootout_goals_to_match_score(tmp_path, duration, final, extra, shootout, winner):
    _, details, *_ = publish(tmp_path, fixture_observations(duration))
    outcome = details["matches"][0]["outcome"]
    assert outcome["final_match_score"] == final
    assert outcome["extra_time_total"] == extra and outcome["shootout"] == shootout
    assert outcome["winner_team_id"] == f"{PROVIDER}:team:{winner}"


def test_draw_preserves_native_zero_winner_and_orders_teams_by_side(tmp_path):
    raw = fixture_observations()
    match = raw[0][0]
    match.update(winner=0)
    match["teamsData"]["11"]["score"] = 1
    match["teamsData"] = dict(reversed(list(match["teamsData"].items())))
    _, details, *_ = publish(tmp_path, raw)
    normalized = details["matches"][0]
    assert [team["source_id"] for team in normalized["teams"]] == [11, 12]
    assert normalized["outcome"]["native_winner"] == 0
    assert normalized["outcome"]["winner_team_id"] is None


@pytest.mark.parametrize("duration,mutation", [
    ("ExtraTime", lambda m: m["teamsData"]["11"].update(scoreET=0)),
    ("ExtraTime", lambda m: m["teamsData"]["11"].update(scoreP=1)),
    ("Penalties", lambda m: m["teamsData"]["11"].update(scoreET=3)),
    ("Penalties", lambda m: m["teamsData"]["11"].update(scoreP=4)),
])
def test_extra_time_and_shootout_inconsistencies_are_rejected(tmp_path, duration, mutation):
    raw = fixture_observations(duration)
    mutation(raw[0][0])
    paths, descriptor, _ = inputs(tmp_path, raw)
    with pytest.raises(FootballDetailsError):
        build_package(*paths, descriptor, tmp_path / "package")


def test_replay_is_byte_identical_and_can_finish_manifest_after_a_partial_write(tmp_path):
    manifest, _, output, paths, descriptor = publish(tmp_path)
    original = {path.name: path.read_bytes() for path in output.iterdir()}
    assert build_package(*paths, descriptor, output) == manifest
    assert {path.name: path.read_bytes() for path in output.iterdir()} == original
    (output / "manifest.json").unlink()
    assert build_package(*paths, descriptor, output) == manifest
    assert {path.name: path.read_bytes() for path in output.iterdir()} == original


def test_fingerprint_binds_manifest_metadata_and_returned_objects_do_not_change_policy(tmp_path):
    manifest, _, output, paths, descriptor = publish(tmp_path)
    persisted = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    identity = copy.deepcopy({key: value for key, value in persisted.items()
                              if key not in {"dataset_version", "publication_fingerprint"}})
    canonical = (json.dumps(identity, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    assert hashlib.sha256(canonical).hexdigest() == persisted["publication_fingerprint"]
    identity["capabilities"]["forecasts"] = True
    tampered = (json.dumps(identity, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    assert hashlib.sha256(tampered).hexdigest() != persisted["publication_fingerprint"]
    manifest["attribution"]["authors"].append("TEST ONLY unintended caller edit")
    assert build_package(*paths, descriptor, output) == persisted


@pytest.mark.parametrize("value,expected", [
    ("TEST ONLY José", "TEST ONLY José"), (r"TEST ONLY Jos\u00e9", "TEST ONLY José"),
    (r"TEST ONLY Jos\u005cu00e9", "TEST ONLY José"),
    (r"TEST ONLY \ud83d\ude00", "TEST ONLY 😀"),
])
def test_unicode_decoder_is_bounded_and_keeps_existing_unicode(value, expected):
    assert decode_name(value) == expected


@pytest.mark.parametrize("value", [r"TEST ONLY \uZZZZ", r"TEST ONLY \ud800", r"TEST ONLY \u000a",
                                   r"TEST ONLY \u005cu005cu00e9", "TEST ONLY\nname", r"TEST ONLY \\u00e9"])
def test_bad_or_deep_unicode_names_do_not_silently_corrupt(value):
    with pytest.raises(FootballDetailsError):
        decode_name(value)


@pytest.mark.parametrize("mutation", [
    lambda r: r.update(competitionId=29), lambda r: r.update(seasonId=10079),
    lambda r: r.update(dateutc="2026-10-08 15:00:00"),
    lambda r: r.update(dateutc="2018-06-13 15:00:00"),
    lambda r: r.update(dateutc="2018-06-14T15:00:00Z"),
    lambda r: r.update(date="June 14, 2018 at 6:00:00 PM GMT+2"),
    lambda r: r.update(date="June 14, 2018 at 5:00:00 PM"),
    lambda r: r.update(status="Scheduled"), lambda r: r.update(winner=12),
    lambda r: r.update(duration="Penalities"), lambda r: r.update(duration=[]),
    lambda r: r.update(unrecognized_field="TEST ONLY schema drift"),
])
def test_wrong_temporal_business_or_source_contract_never_creates_package(tmp_path, mutation):
    raw = fixture_observations()
    mutation(raw[0][0])
    paths, descriptor, _ = inputs(tmp_path, raw)
    with pytest.raises(FootballDetailsError):
        build_package(*paths, descriptor, tmp_path / "package")
    assert not (tmp_path / "package").exists()


@pytest.mark.parametrize("mutation", [
    lambda t: t.update(side="away"), lambda t: t.update(side=[]), lambda t: t.update(teamId=99),
    lambda t: t.update(score=True), lambda t: t.update(scoreHT=3), lambda t: t.update(scoreET=1),
    lambda t: t.update(scoreP=3), lambda t: t.update(hasFormation=True),
    lambda t: t["formation"]["lineup"].pop(),
    lambda t: t["formation"]["bench"].append(copy.deepcopy(t["formation"]["lineup"][0])),
    lambda t: t["formation"]["lineup"][0].update(goals=-1),
    lambda t: t["formation"]["lineup"][0].update(goals="99"),
    lambda t: t["formation"]["lineup"][0].update(goals=True),
    lambda t: t["formation"]["lineup"][0].update(yellowCards=92),
    lambda t: t["formation"]["substitutions"][0].update(playerIn=99999),
    lambda t: t["formation"]["substitutions"][0].update(playerOut=1012),
    lambda t: t["formation"]["substitutions"][0].update(minute=-1),
    lambda t: t["formation"]["substitutions"].append(copy.deepcopy(t["formation"]["substitutions"][0])),
])
def test_malformed_team_player_score_or_substitution_facts_reject(tmp_path, mutation):
    raw = fixture_observations()
    mutation(raw[0][0]["teamsData"]["11"])
    paths, descriptor, _ = inputs(tmp_path, raw)
    with pytest.raises(FootballDetailsError):
        build_package(*paths, descriptor, tmp_path / "package")


def test_missing_formation_is_explicitly_unknown_instead_of_empty_roster(tmp_path):
    raw = fixture_observations()
    raw[0][0]["teamsData"]["11"].update(hasFormation=0, formation=None)
    manifest, details, *_ = publish(tmp_path, raw)
    team = details["matches"][0]["teams"][0]
    assert team["has_formation"] is False
    assert team["starters"] is None and team["bench"] is None and team["substitutions"] is None
    assert manifest["quality"]["formations_not_supplied"] == 1
    assert len(details["players"]) == 13


def test_missing_native_area_code_remains_unknown_without_an_iso_mapping(tmp_path):
    raw = fixture_observations()
    raw[2][0]["area"].update(alpha2code="", alpha3code="XEN")
    _, details, *_ = publish(tmp_path, raw)
    assert details["teams"][0]["area"] == dict(name="TEST ONLY area", alpha2code=None, alpha3code="XEN")


def test_player_cannot_belong_to_both_match_teams(tmp_path):
    raw = fixture_observations()
    raw[0][0]["teamsData"]["12"]["formation"]["lineup"][1]["playerId"] = 1002
    paths, descriptor, _ = inputs(tmp_path, raw)
    with pytest.raises(FootballDetailsError, match="both match teams"):
        build_package(*paths, descriptor, tmp_path / "package")


@pytest.mark.parametrize("entity", ["match", "player", "team"])
def test_duplicate_ids_do_not_choose_first_even_if_facts_are_identical(tmp_path, entity):
    raw = fixture_observations()
    rows = raw[{"match": 0, "player": 1, "team": 2}[entity]]
    rows.append(copy.deepcopy(rows[0]))
    paths, descriptor, _ = inputs(tmp_path, raw)
    with pytest.raises(FootballDetailsError, match=f"Duplicate {entity} ID"):
        build_package(*paths, descriptor, tmp_path / "package")


def test_unresolved_player_reference_never_uses_name_matching(tmp_path):
    raw = fixture_observations()
    raw[1][0]["wyId"] = 9000  # Same name, different source ID.
    paths, descriptor, _ = inputs(tmp_path, raw)
    with pytest.raises(FootballDetailsError, match="unknown source player"):
        build_package(*paths, descriptor, tmp_path / "package")


@pytest.mark.parametrize("field,value", [
    ("license", "MIT"), ("article_version", 99), ("article_id", True),
    ("url", "https://TEST.invalid/not-an-original"), ("sha256", "0" * 64),
    ("md5", "0" * 32), ("bytes", 1), ("observed_at", "2026-10-08T09:00:00"),
    ("metadata_observed_at", "2026-10-09T00:00:00Z"), ("published_at", "2018-06-14T00:00:00Z"),
])
def test_capture_metadata_checksum_version_license_and_clock_gates(tmp_path, field, value):
    paths, descriptor, _ = inputs(tmp_path)
    amend_descriptor(descriptor, lambda d: d["sources"][0].update({field: value}))
    with pytest.raises(FootballDetailsError):
        build_package(*paths, descriptor, tmp_path / "package")


@pytest.mark.parametrize("body", [b'[{"wyId":11,"wyId":12}]', b"[NaN]", b"\xff"])
def test_invalid_json_never_hides_duplicate_keys_nonfinite_or_encoding(tmp_path, body):
    paths, descriptor, _ = inputs(tmp_path)
    replace_source(paths, descriptor, "teams", body)
    with pytest.raises(FootballDetailsError, match="Invalid UTF-8/JSON"):
        build_package(*paths, descriptor, tmp_path / "package")


def test_only_explicit_world_cup_member_is_read_and_nothing_is_extracted(tmp_path):
    _, _, output, *_ = publish(tmp_path, extra_member=True)
    assert {p.name for p in output.iterdir()} == {"details.json", "manifest.json"}
    assert not (tmp_path / "TEST ONLY never extracted.json").exists()


def test_oversized_zip_member_is_rejected_before_expansion(tmp_path, monkeypatch):
    paths, descriptor, _ = inputs(tmp_path)
    monkeypatch.setattr("arena_lakehouse.football_details.MAX_MEMBER_BYTES", 1)
    with pytest.raises(FootballDetailsError, match="member exceeds"):
        build_package(*paths, descriptor, tmp_path / "package")


def test_total_input_budget_rejects_next_original_before_reading_it(tmp_path, monkeypatch):
    paths, descriptor, _ = inputs(tmp_path)
    budget = paths[0].stat().st_size + paths[1].stat().st_size - 1
    monkeypatch.setattr("arena_lakehouse.football_details.MAX_INPUT_BYTES", budget)
    read = Path.read_bytes
    calls = []
    def measured_read(path):
        calls.append(path)
        return read(path)
    monkeypatch.setattr(Path, "read_bytes", measured_read)
    with pytest.raises(FootballDetailsError, match="file exceeds size bound"):
        build_package(*paths, descriptor, tmp_path / "package")
    assert descriptor in calls and paths[0] in calls
    assert paths[1] not in calls and paths[2] not in calls


def test_broken_or_missing_archive_member_has_no_publication(tmp_path):
    paths, descriptor, _ = inputs(tmp_path)
    replace_source(paths, descriptor, "matches", b"TEST ONLY not a ZIP")
    with pytest.raises(FootballDetailsError, match="Invalid World Cup source archive"):
        build_package(*paths, descriptor, tmp_path / "package")


@pytest.mark.parametrize("duplicate", [False, True])
def test_zip_member_identity_cannot_be_missing_or_shadowed(tmp_path, duplicate):
    paths, descriptor, _ = inputs(tmp_path)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("TEST ONLY other member.json", b"[]")
        if duplicate:
            archive.writestr(MATCH_MEMBER, b"[]")
            with pytest.warns(UserWarning, match="Duplicate name"):
                archive.writestr(MATCH_MEMBER, b"[]")
    replace_source(paths, descriptor, "matches", buffer.getvalue())
    with pytest.raises(FootballDetailsError, match="Missing or duplicate"):
        build_package(*paths, descriptor, tmp_path / "package")


def test_tampered_original_fails_before_parsing_and_leaves_source_untouched(tmp_path):
    paths, descriptor, _ = inputs(tmp_path)
    original = paths[1].read_bytes()
    changed = original.replace(b"TEST ONLY", b"TEST FAKE", 1)  # Same byte size, different content.
    assert len(changed) == len(original)
    paths[1].write_bytes(changed)
    with pytest.raises(FootballDetailsError, match="Source checksum differs"):
        build_package(*paths, descriptor, tmp_path / "package")
    assert paths[1].read_bytes() == changed
    assert not (tmp_path / "package").exists()


def test_output_never_overwrites_originals_unrelated_files_or_different_package(tmp_path):
    _, _, output, paths, descriptor = publish(tmp_path)
    original = paths[0].read_bytes()
    with pytest.raises(FootballDetailsError, match="must not contain input"):
        build_package(*paths, descriptor, paths[0].parent)
    assert paths[0].read_bytes() == original
    (output / "details.json").write_text("TEST ONLY different package")
    with pytest.raises(FootballDetailsError, match="different package"):
        build_package(*paths, descriptor, output)
    assert (output / "details.json").read_text() == "TEST ONLY different package"
    (output / "unrelated.txt").write_text("TEST ONLY preserve")
    with pytest.raises(FootballDetailsError, match="unrelated files"):
        build_package(*paths, descriptor, output)
    assert (output / "unrelated.txt").read_text() == "TEST ONLY preserve"


def test_existing_build_lock_is_retained_and_not_bypassed(tmp_path):
    paths, descriptor, _ = inputs(tmp_path)
    output = tmp_path / "package"
    output.mkdir()
    (output / ".package.lock").write_text("TEST ONLY other build owner")
    with pytest.raises(FootballDetailsError, match="already being built"):
        build_package(*paths, descriptor, output)
    assert (output / ".package.lock").read_text() == "TEST ONLY other build owner"


def test_module_cli_uses_explicit_paths_and_returns_contract_version(tmp_path, capsys):
    paths, descriptor, _ = inputs(tmp_path)
    assert main(["--matches-path", str(paths[0]), "--players-path", str(paths[1]),
                 "--teams-path", str(paths[2]), "--source-manifest", str(descriptor),
                 "--output", str(tmp_path / "package")]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == SCHEMA_VERSION and result["scope"] == SCOPE


def test_cli_invalid_sources_exit_two_without_a_package(tmp_path):
    with pytest.raises(SystemExit) as error:
        main(["--matches-path", str(tmp_path / "missing.zip"), "--players-path", "missing.json",
              "--teams-path", "another-missing.json", "--source-manifest", "missing-descriptor.json",
              "--output", str(tmp_path / "package")])
    assert error.value.code == 2
    assert not (tmp_path / "package").exists()
