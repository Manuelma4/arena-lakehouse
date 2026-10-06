# World Cup research acquisition and contract v2

This extension adds the men's World Cup 2026 to the existing nine club editions. It is a research dataset with source observations, not an official feed or a canonical Arena snapshot. It makes no historical pre-match availability claim and does not enable predictions.

## Reviewed source and provenance

The source is [OpenFootball World Cup JSON](https://github.com/openfootball/worldcup.json), distributed under [CC0](https://github.com/openfootball/worldcup.json/blob/516d3825c3bd23fdc298c4014e84bde78f2d4965/LICENSE.md). The reviewed file is:

```text
https://raw.githubusercontent.com/openfootball/worldcup.json/516d3825c3bd23fdc298c4014e84bde78f2d4965/2026/worldcup.json
SHA256 0ae2c18109b5aa86bc11928b43586ca430c234804d5bbf808d2c3bf2051ecfca
42,736 bytes; 104 matches; 48 source team names
```

The pinned commit is dated August 11, 2026. The publisher bundles those exact licensed bytes in `src/arena_lakehouse/resources/worldcup_2026.json`; Git attributes disable line-ending conversion. The matching offline test fixture is also byte-protected.

Acquisition rejects different bytes before writing an archive. Conditional responses require a checksum-verified collected archive. Publication verifies the bundled hash, parses it once, and requires all 104 current match identities and every World Cup business/provenance field to match. Only observation, revision and event metadata may vary. Reviewed source aliases cannot be replaced. A new source revision needs a separately reviewed catalogue update; an immutable commit cannot legitimately acquire score corrections under the same checksum.

The source reports groups, grounds, clock offsets and separate score stages. We preserve those fields without asserting that every result was independently verified against FIFA. No players, scorers, flags, logos or inferred stadium facts are imported; the immutable raw archive retains the original input.

## Clock and score limitations

The [TXT knockout file](https://github.com/openfootball/worldcup/blob/19155966c56fca7fe987174f2506e5a874281313/2026--canada-usa-mexico/cup_finals.txt) at its August 22 commit disagrees with this JSON source for match 95: TXT has `14:00 UTC-4`, JSON has `12:00 UTC-4`. This package uses the pinned JSON clock and discloses the discrepancy; it does not silently merge the sources.

An explicit numeric offset permits exact arithmetic: `2026-06-11`, `20:00 UTC-6` becomes `2026-06-12T02:00:00.000000Z`. The source calendar date remains June 11. Numeric offsets do not identify an IANA timezone, so `timezone` stays null. Unsupported abbreviations, malformed clocks and offsets beyond 14 hours are quarantined. Club files with date-only or unzoned local times retain null UTC kickoff.

`score_a`/`score_b` continue to preserve source `ft`, never `et` or shootout `p`. `score_details_json` preserves each supplied `ft`, `ht`, `et` and `p` pair separately. For source match 104, Spain–Argentina, `ft=[0,0]` and `et=[1,0]`; interpreting the first pair as the tournament winner would be wrong. `score_basis` remains `source_ft_unverified_regulation`, reflecting the absence of an independent verification guarantee for every result. Missing scores remain unknown, rather than a fabricated draw or scheduled status.

Completion time is always unknown. The first collection of a result today does not prove it was available before a past match. Git commit timestamps do not supply a complete historical availability audit. Every row has `prediction_eligible=false`, including rows with exact kickoff offsets.

## Explicit version handling

Existing `openfootball-staging-1.0` and `openfootball-research-1.0` packages remain supported with their exact field sets, schemas and source serialization. Default collection remains v1. Old SQLite stores without version metadata are treated as v1. A collector configured for the other version refuses that workspace before writes; stores and old fixtures are not migrated implicitly.

V2 uses `openfootball-staging-2.0` and `openfootball-research-2.0` in a new workspace. Catalogue version 2.0 adds the source below and retains the previous nine definitions. V1 club source fingerprints omit the new default repository/type keys exactly as before. Replaying clubs into v2 preserves team IDs and ordinary match IDs; versioned event content changes because the row contract changes. Original body checksums are preserved.

```json
{
  "source_id": "worldcup-2026",
  "competition_id": "worldcup",
  "season": "2026",
  "path": "2026/worldcup.json",
  "name": "World Cup 2026",
  "region": "International",
  "team_namespace": "openfootball-national-men",
  "source_ref": "516d3825c3bd23fdc298c4014e84bde78f2d4965",
  "repository": "worldcup.json",
  "team_type": "national"
}
```

V2 exported source records explicitly include `repository` and `team_type`. Each row's source URL and license URL use its repository and ref. The staging manifest's provider-level CC0 link remains the existing OpenFootball link; row links preserve the particular repository evidence.

## V2 wire schema

All v1 row fields retain their meaning. Five fields are appended after `event_id` in typed Parquet; research then appends `edition_id=source_id`.

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `team_type` | string | Reviewed `club` or `national` source scope |
| `source_match_number` | int64, nullable | Original positive `num`, without assigning absent numbers |
| `source_group` | string, nullable | Original normalized group text |
| `ground` | string, nullable | Original normalized ground text |
| `utc_offset_minutes` | int64, nullable | Explicit numeric offset east of UTC, within -840…840 |

With an explicit supported numeric offset, `local_time` remains `HH:MM`, `kickoff_precision="offset_minute"`, and `start_time` is the derived UTC instant with microseconds and `Z`. Otherwise existing `local_minute`/`date_only` rules apply. `timezone` and `completed_at` remain null in every row.

Identity uses SHA256 of canonical UTF-8 JSON, truncated to 24 hexadecimal characters:

- Team: `openfootball:team:` + hash of `[team_namespace, NFC reviewed-name]`. The national namespace does not merge nations with clubs.
- Numbered match: `openfootball:match:` + hash of `[competition_id, season, source_match_number]`, with `identity_basis="competition-season-source-match-number"`. This survives opponent, round, schedule and score corrections; conflicting copies of a number within an input are symmetrically quarantined across rounds.
- Unnumbered match: the existing `[competition_id, season, round, team_a_id, team_b_id, occurrence]` formula and basis. The 72 group rows have no fabricated match numbers; unresolved opponent/round renames cannot be silently reconciled.

Each competition row retains the v1 fields and adds `team_type` and measured `offset_minute`. Coverage boundaries remain original calendar dates. Participants are sorted by team ID. No official standings, group tiebreak rules, cup W/D/L or prediction outputs are introduced.

V2 quality retains the v1 fields, adds `offset_minute`, and counts `unresolved_timezones` as rows without a numeric source offset. This measure does not assert an IANA timezone for offset-bearing rows. Row bounds remain 25,000 current, 50,000 revisions and 100,000 polls/quarantine; v2 allows ten reviewed source editions, v1 nine. File and package byte bounds are unchanged.

The required files and logical hash algorithm remain unchanged:

```text
gold/matches.parquet       gold/match_history.parquet
matches.json              history.json              sources.json
competitions.json         quality.json              lineage.json
quarantine.json           polls.json                manifest.json

dataset_version = sha256(canonical JSON {
  as_of, matches, history, sources, competitions,
  quality, lineage, quarantine, polls
})[:16]
```

Canonical JSON uses `sort_keys=True`, `separators=(',', ':')`, `ensure_ascii=False`, `allow_nan=False`, then UTF-8. Existing deterministic array ordering applies. Publication clocks and the upstream staging manifest fingerprint are excluded; real poll observations remain part of dataset identity. Replaying inputs preserves match/history content, while a newly published poll audit may produce a new dataset version.

## Run and verify

Use a new v2 workspace after installing the project dependencies. The fixed source fetch requires no API key:

```bash
python -m arena_lakehouse.openfootball \
  --workspace data/openfootball-v2 \
  --staging-version openfootball-staging-2.0 \
  collect --source worldcup-2026

python -m arena_lakehouse.openfootball \
  --workspace data/openfootball-v2 \
  --staging-version openfootball-staging-2.0 \
  export --output data/worldcup-staging-v2

python -m arena_lakehouse.openfootball_research \
  --staging data/worldcup-staging-v2 --output data/worldcup-research-v2

python -m pytest tests/test_openfootball.py tests/test_openfootball_review.py \
  tests/test_openfootball_research.py tests/test_worldcup_independent.py -q
```

Every export destination must be new. Recollecting the same pinned source preserves revisions. For a combined package, ingest each previously verified club archive through `ingest_bytes(..., observed_at=datetime.now(timezone.utc))` into the same new v2 workspace, then collect the pinned World Cup and export. This is an archive replay with actual new workspace observation clocks, not a new HTTP freshness check for those clubs.

## Measured milestone evidence

The real ten-edition export contains **2,831 current matches and revisions**, ten sources, 20 successful polls and zero quarantine rows. Its dataset version is **`20d06e8e5af0886e`**, with cutoff **`2026-10-06T12:19:31.263499Z`**. Nine verified v0.4 archives were replayed locally; the pinned World Cup was fetched through the real bounded HTTP collector in 0.688 seconds. All ten inputs were then replayed offline: zero changed rows, unchanged current/history content.

Measured quality: 1,192 observed results, 1,639 unknown results, 1,042 date-only rows, 1,685 unzoned local-minute rows and 104 offset-minute rows; all 2,831 rows remain prediction-ineligible. There are 261 derived team identities, including 48 national teams. Acquisition and replay details are retained locally in `data/openfootball-v2-ten-20261006/verification.json`.

The actual legacy nine-source package was republished with its original publication clock: dataset `5f99ba32f796fb23`, every payload checksum and entire manifest were unchanged. A dry replay against the original v1 SQLite store reported 2,727 unchanged rows and zero changes, with the database bytes unchanged. Tests additionally cover hostile resealing, trusted source business binding, numbered cross-round collisions, UTC date rollover, source score separation and version mismatch rejection.
