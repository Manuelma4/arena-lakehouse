"""Original TEST ONLY edition inputs; no retained source bodies or network."""

import hashlib
import io
import json
import zipfile
from copy import deepcopy
from dataclasses import replace
from datetime import datetime

import pytest

from arena_lakehouse import football_details as v1
from arena_lakehouse.football_editions import EDITIONS, build_catalog, counts, normalize
from test_football_details import fixture_observations, inputs


def source_case(tmp_path):
    observations = fixture_observations()
    paths, descriptor_path, descriptor = inputs(tmp_path, observations)
    sources = v1.validate_sources(
        descriptor, {role: path.read_bytes() for role, path in zip(v1.SOURCE_SPEC, paths, strict=True)}
    )
    return observations, sources, paths, descriptor_path


@pytest.mark.parametrize("gap", ["player", "substitutions", "incoming", "bench", "outcome", "name"])
def test_gap_families_preserve_native_values_and_withhold_unproved_facts(tmp_path, gap):
    (matches, players, teams), sources, _, _ = source_case(tmp_path)
    home = matches[0]["teamsData"]["11"]
    if gap == "player":
        players = [row for row in players if row["wyId"] != 1013]
    if gap == "substitutions":
        home["formation"]["substitutions"] = "null"
    if gap == "incoming":
        home["formation"]["substitutions"][0]["playerIn"] = 0
    if gap == "bench":
        template = home["formation"]["bench"][0]
        for identity in range(3000, 3011):
            home["formation"]["bench"].append({**template, "playerId": identity})
    if gap == "outcome":
        matches[0]["winner"] = 0
    if gap == "name":
        players[0]["shortName"] = "TEST ONLY Aritz\t Castro"
    details = normalize(matches, players, teams, sources, EDITIONS[-1])
    match = details["matches"][0]
    side = match["teams"][0]
    if gap == "player":
        player = next(row for row in details["players"] if row["source_id"] == 1013)
        assert player["name"] is None and player["metadata_status"] == "unresolved"
        assert counts(details)["unresolved_players"] == 1
    if gap == "substitutions":
        assert side["substitutions"] is None and side["native_substitutions"] == "null"
        assert side["substitution_availability"] == "unknown" and not side["sequence_verified"]
    if gap == "incoming":
        sub = side["substitutions"][0]
        assert sub["player_in_id"] is None and sub["native_player_in"] == 0
        assert sub["reference_status"] == "unresolved_incoming" and not side["sequence_verified"]
        assert all(row["source_id"] != 0 for row in details["players"])
    if gap == "bench":
        assert len(side["bench"]) == 13
        with pytest.raises(v1.FootballDetailsError, match="bench"):
            v1.normalize_details(matches, players, teams, sources)
    if gap == "outcome":
        assert match["outcome"]["native_winner"] == 0
        assert match["outcome"]["regulation"] == {"home": 2, "away": 1}
        assert match["outcome"]["winner_team_id"] is None and not match["outcome"]["result_eligible"]
        assert match["quality_flags"] == ["outcome_conflict"]
    if gap == "name":
        player = next(row for row in details["players"] if row["source_id"] == 1001)
        assert player["native_name"] == "TEST ONLY Aritz\t Castro"
        assert player["name"] == "TEST ONLY Aritz  Castro"
        assert player["quality_flags"] == ["name_whitespace_normalized"]


def test_edition_scope_and_foreign_references_cannot_be_relabelled(tmp_path):
    observations, sources, _, _ = source_case(tmp_path)
    with pytest.raises(v1.FootballDetailsError, match="Edition identity"):
        normalize(*observations, sources, EDITIONS[0])
    with pytest.raises(v1.FootballDetailsError, match="outside selected edition"):
        normalize(*observations, sources, replace(EDITIONS[-1], start="2018-07-01"))
    observations[0][0]["teamsData"]["11"]["formation"]["substitutions"][0]["playerIn"] = 2012
    with pytest.raises(v1.FootballDetailsError, match="Foreign substitution"):
        normalize(*observations, sources, EDITIONS[-1])


def all_editions(tmp_path):
    observations, _, paths, descriptor_path = source_case(tmp_path)
    raw_matches, players, club_teams = observations
    club_teams = [{**row, "type": "club"} for row in club_teams]
    national = [{**row, "wyId": row["wyId"] + 20} for row in observations[2]]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, edition in enumerate(EDITIONS):
            match = deepcopy(raw_matches[0])
            match.update(wyId=101 + index, competitionId=edition.competition_id, seasonId=edition.season_id)
            stamp = datetime.fromisoformat(edition.start).replace(day=14, hour=15)
            match.update(
                dateutc=stamp.strftime("%Y-%m-%d %H:%M:%S"),
                date=stamp.strftime("%B %d, %Y at 5:00:00 PM GMT+2"),
            )
            if edition.team_type == "national":
                match["teamsData"] = {
                    str(int(key) + 20): {**row, "teamId": row["teamId"] + 20}
                    for key, row in match["teamsData"].items()
                }
                match["winner"] += 20
            archive.writestr(edition.member, json.dumps([match]).encode())
    paths[0].write_bytes(buffer.getvalue())
    paths[2].write_bytes(json.dumps(club_teams + national).encode())
    descriptor = json.loads(descriptor_path.read_text())
    for path, source in zip(paths, descriptor["sources"], strict=True):
        body = path.read_bytes()
        source.update(
            bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), md5=hashlib.md5(body).hexdigest()
        )
    descriptor_path.write_text(json.dumps(descriptor))
    return paths, descriptor_path


def test_catalogue_binds_all_children_and_replays_without_overwriting_v1(tmp_path):
    paths, descriptor = all_editions(tmp_path)
    v1_dir = tmp_path / "v1"
    v1.build_package(*paths, descriptor, v1_dir)
    original = {path.name: path.read_bytes() for path in v1_dir.iterdir()}
    output = tmp_path / "v2"
    catalog = build_catalog(*paths, descriptor, output)
    assert len(catalog["editions"]) == 7
    assert catalog["unique_counts"] == {"matches": 7, "teams": 4, "players": 26}
    before = {path.relative_to(output).as_posix(): path.read_bytes() for path in output.rglob("*.json")}
    assert build_catalog(*paths, descriptor, output) == catalog
    assert before == {
        path.relative_to(output).as_posix(): path.read_bytes() for path in output.rglob("*.json")
    }
    assert original == {path.name: path.read_bytes() for path in v1_dir.iterdir()}
    for entry in catalog["editions"]:
        directory = output / entry["edition"]["edition_id"]
        wire = (directory / "manifest.json").read_bytes()
        assert hashlib.sha256(wire).hexdigest() == entry["manifest_sha256"]
        manifest = json.loads(wire)
        unsigned = {
            key: value
            for key, value in manifest.items()
            if key not in {"dataset_version", "publication_fingerprint"}
        }
        assert v1._hash(v1._json(unsigned)) == manifest["publication_fingerprint"]
        assert v1._hash((directory / "details.json").read_bytes()) == manifest["files"][0]["sha256"]


def test_source_damage_and_changed_publication_refuse_replay(tmp_path):
    paths, descriptor = all_editions(tmp_path)
    output = tmp_path / "v2"
    build_catalog(*paths, descriptor, output)
    target = output / EDITIONS[0].edition_id / "details.json"
    original = target.read_bytes()
    target.write_bytes(original.replace(b'"source_id": 101', b'"source_id": 999', 1))
    with pytest.raises(v1.FootballDetailsError, match="different package"):
        build_catalog(*paths, descriptor, output)
    assert target.read_bytes() != original
    paths[1].write_bytes(b"[]")
    with pytest.raises(v1.FootballDetailsError, match="byte count"):
        build_catalog(*paths, descriptor, tmp_path / "damaged-input")
    assert not (tmp_path / "damaged-input").exists()
