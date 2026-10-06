# Verified checkpoint and next session

Recorded: 2026-10-06. Milestone: World Cup research and explicit contract v2,
package version 0.5.0, before the full public v1.0 portfolio release.

## Current state

- The canonical football/VALORANT showcase retains 600 original synthetic matches,
  16 teams, six competitions, point-in-time model features and eight stored
  upcoming forecasts. The legacy immutable fixture remains intact.
- Linux Spark 4.1.3 and Delta Lake 4.4.0 have measured streaming/checkpoint, MERGE,
  CDF, affected-key correction, cancellation, backfill and recovery evidence.
  The 163.19-second incremental scenario and small 16-vs-2-file experiment are
  previous measured runs, not new v0.5 benchmarks.
- No-token OpenFootball acquisition supports genuine v1 and explicit v2 stores,
  typed staging exports and immutable research publication. V1 remains the default;
  attempting to mix workspace versions fails before ingestion writes.
- V2 adds the pinned men's World Cup 2026 JSON source, its exact bundled raw bytes,
  national-team namespace, optional source match number/group/ground and numeric
  UTC offsets. Every published World Cup business field must match that source.
- The ten-edition package has 2,831 current/history rows, ten sources, 20 successful
  polls and zero quarantine. Dataset version: `20d06e8e5af0886e`; observation cutoff:
  `2026-10-06T12:19:31.263499Z`.
- Nine club inputs were replayed from verified v0.4 archives with actual new
  workspace observation clocks. Only the pinned World Cup was newly fetched over
  HTTP. Offline replay of all ten created zero revisions and preserved current/history.
- Quality counts: 1,192 observed results, 1,639 unknown, 1,042 date-only, 1,685 local
  minute and 104 explicit-offset rows. All 2,831 research rows retain unknown
  completion and disabled predictions. Exact offset arithmetic does not establish
  an IANA timezone or historical pre-match availability.
- Actual v1 package `5f99ba32f796fb23` republished with its original clock has an
  identical manifest and payload checksums. The original v1 SQLite store dry replay
  reports 2,727 unchanged rows, zero changes and identical database bytes.

## Verification and local artifacts

The final measured suite, code checks and fresh 0.5.0 wheel smoke are recorded in
`docs/evidence/openfootball-software-verification-v0.5.json`. Acquisition, archive
replay and legacy compatibility are recorded separately in
`docs/evidence/openfootball-acquisition-v0.5.json`. Independent source tests include
hostile resealing with genuine pinned checksums and internally coherent mutated rows.

The development worktree's ignored artifacts are:

- `data/openfootball-v2-ten-20261006/`: raw archives, SQLite state and replay evidence.
- `data/openfootball-staging-v2-ten-001/`: validated v2 staging input.
- `data/openfootball-research-v2-ten-001/`: immutable ten-edition publication.
- `data/openfootball-research-v1-compat-001/`: original-clock legacy publication check.
- `data/verification-v0.5/`: final test/build/smoke logs.

Use the worktree's `src` through `PYTHONPATH` while developing. The shared virtual
environment remains outside the repositories; do not replace its editable checkout.
See [development](docs/development.md), [World Cup v2 acquisition](docs/worldcup-research.md)
and [incremental operations](docs/incremental-operations.md) for executable procedures.
A source-tree test is insufficient evidence for installed package resources; retain
an isolated wheel or container check when the catalogue or trusted source changes.

## Boundaries to retain

The pinned source cannot acquire genuine content changes under the same digest.
A new commit requires reviewed catalogue/version handling and explicit provenance.
The documented JSON/TXT disagreement for World Cup match 95 remains unresolved;
this package uses the pinned JSON clock. No official standings, real-data prediction
accuracy, live-feed SLA or guaranteed historical completeness is asserted.

Authenticated football-data.org and PandaScore adapters remain separate and have
not been verified with the user's provider credentials. Polling and Delta CDF do
not constitute upstream transaction-log CDC or round-level push streams.

The Databricks Auto Loader/SDP/AUTO CDC/Job profile has official offline bundle
schema evidence and no schedule. Managed workspace execution still needs its own
permissions, feature and runtime verification report.

## Subsequent sessions

- Add source freshness checks, operational SLOs and a measured incident drill.
- Review additional international editions and unresolved team aliases before
  changing source scope; do not merge clubs, nations or historical nation identities.
- Build a genuinely collected historical availability archive and define score
  targets before considering real-data model training, calibration or evaluation.
- Verify the optional Databricks deployment in a workspace.
- Run controlled larger performance experiments and document retention/deletion.
- Complete the portfolio release acceptance matrix, dependency locks and demo.

Local Gold maintenance is incremental; full reconstruction remains the correctness
oracle. Synthetic model metrics and these bounded local demonstrations do not
establish production scale, distributed guarantees or real-team predictive accuracy.
