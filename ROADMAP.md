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
| 06. Incremental production path | Planned | Recompute affected teams only; independent full-rebuild oracle; bounded backfill |
| 07. Operational reliability | Planned | Stage DAG, bounded transient retries, restart between commits, freshness SLO, incident drill |
| 08. Team/roster history and schema evolution | Planned | SCD2 identity history, contract migrations, late roster changes, archive reconciliation |
| 09. Databricks target | Planned, not workspace-tested | UC Volumes, Auto Loader, SDP expectations/AUTO CDC, Jobs and validated bundle |
| 10. Performance and privacy | Planned | Larger seeded workloads, compaction measurements, deletion/retention demonstration |
| 11. Real-data model validation | Planned | Rolling-origin backtests, calibration, drift, model card, competition holdout |
| 12. Public v1.0.0 release | Planned | Full acceptance suite, video/captures, dependency locks, documented limitations |

## Course coverage and implementation boundary

| Academy course | Project learning objective | Current boundary |
|---|---|---|
| 2963 — Ingestion | REST landing, schema drift, incremental files, Auto Loader | REST/reference implemented; managed ingestion later |
| 1365 — Workflows/Jobs | Dependencies, retry/recovery and scheduling | Transactional local recovery tested; managed Jobs later |
| 2971 — Declarative pipelines | Bronze/Silver/Gold + expectations | Spark layers implemented; managed SDP later |
| 3640 — DevOps essentials | pytest, code checks, GitHub Actions | Local tests and CI config implemented; remote CI unverified |
| 2972 — Advanced streaming | Checkpoints, event corrections, CDC and temporal state | File replay/CDF tested; impacted-state backfill later |
| 2967 — Optimization | Plans, file layout, compaction, measured throughput | Baseline runtime measured; optimization experiments later |
| 3767 — Privacy | Token boundaries, retention and access policies | Secrets excluded; full retention/access-policy evidence later |
| 3489 — Bundles | Repeatable Databricks resources and deployment targets | Bundle implementation and workspace verification later |

## Next session: incremental correctness

1. Keep canonical events as the replay source and introduce affected-match/team keys.
2. Apply a score correction, cancellation and postponed kickoff in separate batches.
3. Update only impacted Silver/Gold rows; compare with the independent full rebuild.
4. Add a bounded `backfill --from --to` command and preserve unaffected results.
5. Inject a failure between Silver and Gold commits; replay must converge.
6. Export from the Spark result only after a consistent successful stage graph.

Optional deeper extension after this foundation: PostgreSQL + genuine source CDC.
Match API polling and downstream Delta CDF are not transaction-log CDC.
