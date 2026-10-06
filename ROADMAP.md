# Arena engineering roadmap

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
| 05. Real API onboarding | Requires user's free tokens | First successful provider calls, field coverage report, reproducible private archive |
| 05a. No-token open-data acquisition | Verified OpenFootball staging and research publication | Nine CC0 editions; raw archives, replay, source corrections, null UTC clocks, independent consumer contract |
| 06. Incremental execution path | Verified locally | Affected-match/history and team maintenance; cancellation tombstones; full oracle; bounded backfill |
| 07. Operational reliability | Local stage runner verified; SLOs pending | Persistent attempts, bounded transient retries, restart after Silver commit, publication gate |
| 08. Team/roster history and schema evolution | Planned | SCD2 identity history, contract migrations, late roster changes, archive reconciliation |
| 09. Databricks target | Implemented adapter; offline schema verified; runtime pending | UC Volume input, Auto Loader, SDP expectations/AUTO CDC, manual Job and bundle |
| 10. Performance and privacy | Small measured layout experiment; deeper work pending | 16 vs 2 files, plans/bytes/timings, soft-deletion audit; larger controlled runs and retention pending |
| 11. Real-data model validation | Planned | Rolling-origin backtests, calibration, drift, model card, competition holdout |
| 12. Public v1.0.0 release | Planned | Full acceptance suite, video/captures, dependency locks, documented limitations |

## Course coverage and implementation boundary

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

Next work: real provider onboarding, freshness SLO/alerts, roster availability,
controlled larger benchmarks, explicit retention/deletion policy, managed
workspace acceptance and the full portfolio release checklist.

The [open-data acquisition design](docs/open-data-acquisition.md) adds a no-token
football route and a concrete provider/competition/season coverage catalogue.
Date-only kickoff precision, reviewed cross-provider identities and collection-time
availability are acceptance requirements before activating those datasets.

Milestone 0.4 activates the precision-aware research export for OpenFootball.
Its recorded nine-source import contains 2,727 rows and preserves 1,639 unknown
results. Actual collection establishes availability; no historical forecast or
official table is derived from incomplete clocks and unverified score targets.
Source revision audit, fixed catalogue packaging and installed-wheel verification
complement the existing canonical Spark/Delta path.

Optional deeper extension after this foundation: PostgreSQL + genuine source CDC.
Match API polling and downstream Delta CDF are not transaction-log CDC.
