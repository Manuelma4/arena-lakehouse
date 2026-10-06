# Verified checkpoint and next session

Recorded: 2026-10-06. Milestone: local foundation 0.1.0, before the public v1 release.

## What works

- Strict football and VALORANT match observations, original seeded fictional data,
  provider normalizers, private raw landing and bounded REST retries.
- Transactional reference ingestion, idempotent replay, revision history,
  quarantine, conflicting revision rejection and failed-run recovery.
- Actual Docker/Linux Spark 4.1.3 + Delta 4.4.0 replay: MERGE, CDF, checkpoint
  restart, one late correction and team-aggregate parity with the reference.
- Chronological prediction baselines with point-in-time feature and label cutoffs;
  frozen snapshot export and the connected independent copilot fixture.
- 19 passing offline tests, Ruff checks and executed learning notebook cells.

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
README.md to create an independent environment on another computer. Docker image
`arena-lakehouse:0.1.0` exists locally; rebuild it after source changes.

## Next engineering session: incremental Spark state

1. Record affected match and team keys from a batch before merging Silver.
2. Exercise a score correction, cancellation and postponed kickoff.
3. Update impacted Gold rows and compare with a separate full reconstruction.
4. Add bounded backfills, then fault injection between Silver and Gold commits.
5. Publish only a consistent snapshot from the successful Spark stage graph.

Real API onboarding needs personal free tokens in a local `.env`, never in chat,
fixtures, notebooks or Git. Provider calls have only been checked with protocol
fixtures so far. API polling is not transaction-log CDC or free round-level push.

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

The current Gold rebuild, synthetic model metrics and local smoke are foundation
evidence. They do not establish production scale or real-team predictive accuracy.
