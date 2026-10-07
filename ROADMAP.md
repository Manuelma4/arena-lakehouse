# Arena engineering roadmap

The current ordered continuation and second-computer setup are in
[RETOMAR_MANANA](docs/RETOMAR_MANANA.md). Native 10k acceptance passed; a later
host Docker idle failure is a separate unresolved operational issue.

Each milestone ends with runnable evidence and an explanation of what it proves.
The sequence spans multiple working sessions; it is not a claim that one session
completes the entire curriculum. Module mappings use the user's Academy titles;
private lesson content has not been copied.

| Milestone | Status | Acceptance evidence |
|---|---|---|
| 01. Canonical football/VALORANT observations | Implemented | Provider adapters, strict schema, fictional replay, token isolation |
| 02. Correctness reference and snapshot | Implemented | Idempotency, shuffled revisions, conflicts, quarantine, rollback, checksum contract |
| 03. Spark/Delta streaming foundation | Implemented | AvailableNow, MERGE, CDF, checkpoint replay, correction, oracle parity |
| 04. Prediction research baseline | Implemented on synthetic fixtures | Chronological split, availability cutoff, leakage tests, baseline metrics |
| 05. Real API onboarding | Verified locally with free accounts | Six football competitions, three VALORANT phases, private archives, three repeated source polls and exact freshness scope |
| 05a. No-token open-data acquisition | Verified OpenFootball staging and research publication | Ten CC0 editions; immutable archives, replay, explicit offset precision, source authenticity and independent consumer contract |
| 06. Incremental execution path | Verified locally | Affected-match/history and team maintenance; cancellation tombstones; full oracle; bounded backfill |
| 07. Operational reliability | Local stage runner and source-check monitor verified; sustained SLO measurement pending | Persistent attempts, bounded transient retries, restart after Silver commit, publication gate, read-only check ratios/overdue alerts |
| 08. Team/roster history and schema evolution | Private source-pinned match details implemented; roster history pending | Supplied football lineups/statistics and VALORANT map outcomes; free player-stat acquisition, roster SCD2 and identity review remain gates |
| 09. Databricks target | Implemented adapter; offline schema verified; runtime pending | UC Volume input, Auto Loader, SDP expectations/AUTO CDC, manual Job and bundle |
| 10. Performance and privacy | Native 10k scenario verified on one CPU with a 1.5 GiB container cap; deeper work pending | Full Python/SQLite parity, replay/recovery/tombstones; million-event execution, controlled comparisons and retention pending |
| 11. Real-data model validation | Planned | Rolling-origin backtests, calibration, drift, model card, competition holdout |
| 12. Public v1.0.0 release | Planned | Full acceptance suite, video/captures, dependency locks, documented limitations |

## Course coverage and implementation boundary

The [7 October native continuation](docs/evidence/spark-native-local-2026-10-07.json)
records the exact image and synthetic execution separately from provider facts.
[Match insight evidence](docs/evidence/match-insights-local-2026-10-07.json)
records actual free-account coverage. The user requires only free sources:
individual kills/KDA and unavailable lineups remain acquisition gates.

| Academy course | Project learning objective | Current boundary |
|---|---|---|
| 2963 — Ingestion | REST landing, schema drift, incremental files, Auto Loader | REST/reference implemented; managed ingestion later |
| 1365 — Workflows/Jobs | Dependencies, retry/recovery and scheduling | Persistent local stage recovery tested; configurable manual Job pending workspace |
| 2971 — Declarative pipelines | Bronze/Silver/Gold + expectations | Spark layers verified; managed SDP adapter pending workspace |
| 3640 — DevOps essentials | pytest, code checks, GitHub Actions | Local and public GitHub Actions software checks verified |
| 2972 — Advanced streaming | Checkpoints, event corrections, CDC and temporal state | File replay/CDF, impacted-state corrections and bounded backfill verified locally |
| 2967 — Optimization | Plans, file layout, compaction, measured throughput | Small 16 vs 2 file experiment measured; scale/controlled trials remain pending |
| 3767 — Privacy | Token boundaries, retention and access policies | Secrets excluded; full retention/access-policy evidence later |
| 3489 — Bundles | Repeatable Databricks resources and deployment targets | Configurable bundle passed official offline schema; workspace validation pending |

## Sports showcase milestone — 0.3.0

The original seeded records can be exported across six fictional competitions,
three per sport. Match identities, results, timestamps, features and forecast
probabilities remain unchanged. A fresh workspace prevents a legacy observation
identity from silently acquiring different competition metadata. The consumer
uses this export for observed competition tables and historical match research;
neither project presents those tables as official standings.

The expanded reference suite has 37 passing tests. Remote v0.2 software and
Linux Spark workflows are recorded in the public Actions history. Showcase
metadata does not replace the separately measured incremental Spark evidence.

## Verified incremental correctness

The actual 163.19-second scenario in `docs/evidence/incremental-report.json`
proves affected-key maintenance, a correction, cancellation, postponed kickoff,
stable-ID rename, bounded duplicate backfill, unchanged unaffected rows, failure
after Silver/recovery, SCD2 parity and gated snapshot publication. This is a local
single-writer demonstration with full independent verification, not a production
scale or distributed reliability certification.

Next work: published-source freshness SLO/alerts, roster availability,
controlled larger benchmarks, explicit retention/deletion policy, managed
workspace acceptance and the full portfolio release checklist.

The [open-data acquisition design](docs/open-data-acquisition.md) adds a no-token
football route and a concrete provider/competition/season coverage catalogue.
Date-only kickoff precision, reviewed cross-provider identities and collection-time
availability are acceptance requirements before activating additional datasets or real-data predictions.

Milestone 0.4 activated the precision-aware v1 research export for OpenFootball.
Its recorded nine-source import contains 2,727 rows and preserves 1,639 unknown
results. Actual collection establishes availability; no historical forecast or
official table is derived from incomplete clocks and unverified score targets.
Source revision audit, fixed catalogue packaging and installed-wheel verification
complement the existing canonical Spark/Delta path.

## World Cup and explicit contract evolution - 0.5.0

The v2 research export adds the pinned OpenFootball men's World Cup 2026 source
to the nine club editions. The measured package contains 2,831 current matches
and revisions, ten sources, 20 successful polls and zero quarantine. A replay of
all ten archives produced no new revisions and unchanged current/history content.
See [v0.5 acquisition evidence](docs/evidence/openfootball-acquisition-v0.5.json).

The 104 national-team matches preserve source group, ground, optional match number,
separate FT/HT/ET/penalty scores and explicit numeric UTC offsets. Completion times
remain unknown and all real research rows remain prediction-ineligible. The 72
group matches receive no fabricated source numbers; 32 numbered knockout matches
use identities independent of schedule, round and opponents. Club and national
team namespaces remain distinct.

Publication binds every World Cup business field to bundled, checksum-verified
pinned bytes. Independent resealed score/group/ground/opponent mutations are
rejected even when their JSON, Parquet and event identities agree. The authored
catalogue cannot substitute a new ref or aliases for that immutable source.

Version handling is explicit: v1 remains the default collector path, v2 requires
a separate workspace, and neither original fixtures nor old stores are migrated.
Actual v1 manifest/file hashes and old-store dry replay remain unchanged. Final
software and the isolated 0.5.0 wheel are recorded in
[v0.5 software evidence](docs/evidence/openfootball-software-verification-v0.5.json).
No managed Databricks runtime, real-team predictive accuracy or new Spark scale
claim is attached to this milestone.

The next research extension must first define source availability, stable identities
and score targets. Possible work includes reviewed additional international
editions, source freshness checks and a genuine historical observation archive.
The current JSON/TXT clock discrepancy stays documented until independently
resolved; a source timestamp alone is insufficient to enable retrospective forecasts.

Optional deeper extension after this foundation: PostgreSQL + genuine source CDC.
Match API polling and downstream Delta CDF are not transaction-log CDC.

## Authenticated source operations - 0.6.0

Both free provider accounts passed actual authenticated collection. The measured
football scope contains six competition codes and 2,132 canonical matches;
VALORANT past/upcoming/running pages contain 124 source announcements, of which
106 satisfy the canonical match contract. Three repeated cycles made 27 successful
HTTP requests and preserved canonical versions when business fields did not change.

The durable polling worker records request reservations, source attempts and
sanitized errors before publishing an immutable successful version. Restart,
quota, transient failure and retained-publication behavior have offline tests.
The independent nullable calendar preserves all 124 announcements and their
source occurrences, including unresolved opponents and kickoff times. It does
not fabricate match records, historical availability or forecast eligibility.

The scale harness adds deterministic larger workloads and an independent bounded
SQLite oracle for Spark comparisons. A 10,000-event workload was generated; its
Spark execution and the million-event benchmark remain unmeasured. The existing
actual Spark/Delta evidence retains its original configuration and scope.

## Paired publication and event directories - 0.7.0

The worker selects canonical matches and the nullable calendar as a single
checksum-bound publication. Prepared intents preserve exact archives and complete
revision cursors; recovery resumes the same poll before acquisition. Contradictory
phase records and ambiguous scores remain calendar evidence and preserve prior
canonical history. Valid dates without offsets and unresolved opponents never
become invented strict records.

The companion consumer displays observed football competitions and source-provided
VALORANT league/series/tournament relationships. Original intercepted-response
integration probes distinguish pending, resolved, conflicted and failed collections
without accessing a provider or model.

Next acceptance work includes actual authenticated-source Spark replay, controlled
scale trials, forecast eligibility/calibration, retention/deletion behavior and
managed Databricks verification. Offline publication checks do not replace those
runtime measurements.

The [7 October Windows continuation](docs/evidence/local-plan-continuation-2026-10-07.json)
adds read-only source-check health and restores Docker's WSL2 runtime. A bounded
Linux container verified the prepared 10k workload's mount, count and checksum.
That container check does not complete the actual Spark/Delta pipeline gate.
