# Verified checkpoint and next session

Recorded: 2026-10-06. Milestone: free OpenFootball research 0.4.0, before the public v1 release.

## What works

- Strict football and VALORANT match observations, original seeded fictional data,
  provider normalizers, private raw landing and bounded REST retries.
- Transactional reference ingestion, idempotent replay, revision history,
  quarantine, conflicting revision rejection and failed-run recovery.
- Actual Docker/Linux Spark 4.1.3 + Delta 4.4.0 replay: MERGE, CDF, checkpoint
  restart, one late correction and team-aggregate parity with the reference.
- Chronological prediction baselines with point-in-time feature and label cutoffs;
  frozen snapshot export and the connected independent copilot fixture.
- 144 passing offline tests, Ruff checks and executed learning notebook cells.
- No-token CC0 OpenFootball acquisition, fixed packaged source catalogue, raw
  archives, conditional polling, bounded retries, SQLite staging and immutable
  research Gold export. Nine editions contain 2,727 current/history rows;
  exact raw-archive replay preserves JSON/Parquet state and history checksums.
- Unresolved UTC kickoff/completion remain null, missing scores stay unknown,
  and all real research rows remain prediction-ineligible. Consumer validation
  and the source research view are separate from canonical synthetic forecasts.
- Six original competitions across both sports; unchanged source identities,
  results and forecasts. The previous immutable fixture remains available.
- Persistent Spark stages, affected-match/history and team maintenance, soft
  deletion, bounded availability backfills and recovery after Silver commit.
- Actual 163.19-second incremental scenario with full oracle parity; measured
  16-vs-2-file layout experiment and official offline bundle schema validation.

The 600-match demo has 1,192 accepted observations, two duplicate deliveries,
two invalid observations and eight upcoming forecasts. The recorded Spark smoke
preserved 1,192 rows after restart and accepted the 1,193rd correction observation.

## Restart locally

From this directory on the development host:

```powershell
C:\Works\.arena-venv\Scripts\arena-lakehouse.exe demo --matches 600 --seed 42
C:\Works\.arena-venv\Scripts\python.exe -m pytest -q
```

The shared development virtual environment is outside both repositories. Follow
`docs/development.md` to create an independent environment on another computer. Docker image
`arena-lakehouse:0.1.0` exists locally; rebuild it after source changes.

## Completed incremental Spark state

The scenario has been run on the Docker Linux backend. It adds a score correction,
cancellation, postponed kickoff and team rename, preserves unaffected row audit
timestamps, resumes after a failure between layer commits, then publishes a
verified snapshot. Active Gold has 16 rows plus two audit tombstones. Immutable
source observations/CDF converge to 1,197 rows. Original foundation evidence is
retained separately. See `docs/incremental-operations.md` for commands.

The optional Databricks SDP/Auto Loader/AUTO CDC + Lakeflow Job profile is now
implemented and passed official CLI 1.19.0 offline schema validation. It has no
schedule. Workspace runtime/permissions/features still require separate evidence.

Real API onboarding needs personal free tokens in a local `.env`, never in chat,
fixtures, notebooks or Git. Provider calls have only been checked with protocol
fixtures so far. API polling is not transaction-log CDC or free round-level push.

The OpenFootball route needs no account or token and has an actual recorded
download. See `docs/evidence/openfootball-acquisition-v0.4.json` for source
checksums, collection counts, replay and the research contract. Authenticated
football-data.org and PandaScore calls remain separately unverified.

## Subsequent working sessions

- Operational reliability: stage dependencies, retry policies, freshness checks,
  incident drill and resume after a partially completed stage graph.
- Historical identity/schema evolution: team history, roster availability and
  contract migration; availability restrictions must remain explicit.
- Databricks: UC Volumes/Auto Loader, SDP expectations/AUTO CDC, Jobs and bundles;
  managed behavior requires a workspace verification report.
- Performance/privacy: larger workloads, compaction plans/files/times, retention
  and deletion limitations, original-code and provider-data license boundaries.
- Release: full acceptance matrix, dependency lock, recorded demo and GitHub CI.

Local Gold maintenance is incremental; the complete rebuild is a correctness
oracle. These demonstrations and synthetic model metrics do not establish
production scale, distributed guarantees or real-team predictive accuracy.
