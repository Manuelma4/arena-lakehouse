# Euro 2024 acquisition — 14 October 2026

Archive now includes Euro 2024 alongside the existing ten OpenFootball editions.
The original checked-in research fixture remains unchanged. This is an additional
CC0 research dataset; it does not extend the live provider feed or activate a forecast.

## Reviewed source

- Repository: [openfootball/euro.json](https://github.com/openfootball/euro.json).
- Commit: `7bbf6309bdda6896d6bb9ca218c1e15eaf418ce3`.
- File: `2024/euro.json`; SHA-256
  `fdb4b858cc88603e69fe3b1ce34923fc6b69a774d4c8416f2d166ea57c556609`.
- [CC0 license at that commit](https://github.com/openfootball/euro.json/blob/7bbf6309bdda6896d6bb9ca218c1e15eaf418ce3/LICENSE.md);
  license SHA-256 `189c8c9ea8c9939aadb38a371161e620cbc9002446d5ff048ee4076741b0be45`.
- 51 matches, 24 national-team names, 36 group-stage rows and 15 knockout rows.

The collector accepts only this reviewed national edition and exact source bytes.
The publisher separately compares its rows against the bundled source, including
the complete match set. Git preserves the resource bytes across Windows checkouts.
The existing v1 contract and ten previous catalogue entries remain compatible.

## Actual acquisition

Eleven bounded unauthenticated source GETs succeeded, with one attempt per source,
between 08:40:02 and 08:40:12 UTC. No rows were quarantined. Each observation has
its actual collection clock and archived raw response. The ten previously known
source bodies were byte-identical to the earlier fixture; their missing results
remain unknown. A fresh check does not imply fresher upstream content.

The new isolated workspace is `data/openfootball-expanded-20261014`.
Its immutable staging export is `data/openfootball-staging-20261014`; the published
research package is `data/openfootball-research-20261014`. Dataset
`6628a6b97fc6020a` contains 2,882 matches: 1,243 supplied results and 1,639 unknown
results. The consumer independently validates it with fingerprint
`fb0d11b8be7e9927f732a5d29fe183351d70f627bb611a60461e8f15bc8a09c5`.

Euro source times are local wall times without a timezone. UTC kickoff and
completion remain null. All 2,882 research rows remain prediction-ineligible;
October collection does not establish pre-match availability in 2024. Score
stages retain the source's FT/HT/ET/penalty distinctions. Goal arrays remain in
the raw archive; this change does not expose player events or lineups.

## Verification and continuation

187 producer tests passed across acquisition, research publication, the existing
World Cup boundaries and new Euro cases. These include replay without extra
revisions, full national coverage, wrong pins, altered bytes, changed facts inside
rehashed packages, and rejection before writes in a v1 workspace. Ruff and
whitespace checks passed. The consumer separately tests its own row reconstruction.

Review metadata and original bytes are in `data/euro-review-20261014`. The consumer's
`data/resume-20261014/euro/` contains actual collection, reader and browser reports.
The private recovery snapshot includes these new directories. See the consumer's
document with this filename for local preview and delivery verification.

Broader current Latin American coverage and VALORANT player telemetry remain
unresolved. Copa América's Football.TXT source needs its own reviewed adapter;
the apparent `copa-america.json` repository redirects to an unrelated repository
and was not admitted. Spark/Linux, managed Databricks and real model evaluation
remain outside this verified scope. The overall objective is active and unfinished.
