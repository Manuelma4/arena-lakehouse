# Checkpoint — 7 October 2026

Repository: `Manuelma4/arena-lakehouse`. Resume from branch
`checkpoint/2026-10-07`. The previous local directory was named
`commerce-lakehouse`; a normal clone uses `arena-lakehouse`.

This is a development checkpoint, not a completed v1.0 release. It preserves
the source, tests and measured evidence needed to continue on another computer.

## Latest work

- Incremental preflight now checks event-ID ownership across matches before
  creating a job. A reused ID cannot silently disappear during a Bronze merge.
  [Design and verification](event-identity-preflight.md): 26 offline tests passed.
  These tests exercise relational/control behavior; native Spark verification
  of this revision remains pending.
- One million synthetic observations and an independent SQLite reference were
  generated and validated. The [evidence index](evidence/README.md) distinguishes
  preparation from actual Spark ingestion, recovery and performance measurement.
- Existing source collectors, calendar publication, contracts and Databricks
  adapters are retained. Managed workspace execution remains a separate gate.

## Resume on another computer

Follow [development.md](development.md) for the environment and commands. Use
Python 3.11 and Linux containers for Spark/Delta; keep Spark and Ollama inference
in separate stages on a 16 GB computer.

Credentials in `.env`, provider archives, local databases, generated scale data,
virtual environments and build outputs are intentionally not in Git. Recreate
local configuration from the committed examples and obtain credentials securely.
The consumer can start with its bundled fixtures without these private files.
Do not create a second polling owner alongside the existing hosted collector.

## Next gates

1. Verify this preflight change with the real Spark/Delta execution path.
2. Complete the bounded 10k pipeline gate before the million-observation run;
   compare complete state/history/Gold to the independent reference.
3. Measure restart, replay, late correction, backfill and layout parity without
   turning preparation timings into throughput claims.
4. Complete real-data forecast backtesting/calibration and authenticated
   Databricks checks before claiming those capabilities.

The checkpoint branch does not update the production branch or deploy a site.
No new GitHub Actions run was requested for this backup.
