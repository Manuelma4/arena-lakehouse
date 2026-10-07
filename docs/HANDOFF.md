# Checkpoint — 7 October 2026

Repository: `Manuelma4/arena-lakehouse`. Resume from branch
`checkpoint/2026-10-07`. The previous local directory was named
`commerce-lakehouse`; a normal clone uses `arena-lakehouse`.

This is a development checkpoint, not a completed v1.0 release. It preserves
the source, tests and measured evidence needed to continue on another computer.

**Final transfer plan:** [RETOMAR_MANANA.md](RETOMAR_MANANA.md) is the current
entry point for both projects. It includes launch/configuration commands,
a verified private-data ZIP, completed work and ordered acceptance gates.
Docker worked for the native 10k acceptance but later blocked on idle; the
[recovery attempt](evidence/docker-idle-recovery-2026-10-07.json) failed its
bounded WSL timeout. Docker is currently stopped and repair remains pending.
The host application remains independent of Docker/Ollama. Earlier successful
runtime observations below do not establish the final host state.

## Latest work

- Local source-check monitoring now reads the durable polling ledger without
  creating a worker, database or provider request. It reports a bounded window,
  observed success ratio, success age, typed overdue/failure alerts and the
  presence of a selected publication. Sustained service SLO evidence remains
  pending; a single successful check does not establish a reliable SLA.
- The source health/collector suites passed 34 tests. After isolating lightweight
  provider metadata from pipeline imports, the final eight source-health cases
  passed again. The incident check uses a mocked provider with the real durable
  worker and proves retained publication plus fresh/stale/error classification.
- Docker now responds with a Linux server on WSL2 after the user's Administrator
  repair and backend selection. A disposable container, limited to 64 MiB and
  0.5 CPU with no network, verified the 10k workload's Windows read-only bind
  mount, row count and checksum. The first shell-quoting failure and corrected
  result are retained separately from Spark evidence. No allocation increase,
  Docker data reset or Ollama execution was performed by the agent.
- A new isolated 10,000-observation / 5,000-match synthetic workload was generated
  and fully validated on this computer. This is preparation only: the actual
  Spark/Delta pipeline and event-ID preflight were initially pending. After RAM
  became available, both native gates passed as recorded below; million-event
  execution remains pending. The earlier report keeps its original resource
  boundary.

The [Windows continuation evidence](evidence/local-plan-continuation-2026-10-07.json)
retains the source hashes, scoped results, initial failures and exact boundaries
of monitoring, workload preparation and Docker verification.

- Incremental preflight now checks event-ID ownership across matches before
  creating a job. A reused ID cannot silently disappear during a Bronze merge.
  [Design and verification](event-identity-preflight.md): 26 offline tests passed.
  These tests exercise relational/control behavior; native Spark verification
  of this revision now passed in the bounded Linux container.
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

1. Obtain permitted free player statistics and match participants; preserve
   unknown coverage and review cross-source identities.
2. Prepare controlled million-observation execution after the measured 10k gate,
   with an adequate resource budget and a separate actual-process restart.
3. Measure restart, replay, late correction, backfill and layout parity without
   turning preparation timings into throughput claims.
4. Complete real-data forecast backtesting/calibration and authenticated
   Databricks checks before claiming those capabilities.

The checkpoint branch does not update the production branch or deploy a site.
No new GitHub Actions run was requested for this backup.

## Native runtime and match details - current continuation

[Native evidence](evidence/spark-native-local-2026-10-07.json) records Spark 4.1.3,
Delta 4.4.0, one CPU, 512 MiB configured driver heap and a 1536 MiB container cap,
with networking disabled during acceptance. The local build succeeded after an
initial Maven-resolution timeout was preserved. Separate pip/JVM build layers
retain dependency cache. The scenario took 716.42 seconds and reached 10,005
accepted observations after corrections. Every Bronze, Silver/current/history
and Gold row matched the Python oracle and independent SQLite crosscheck.
Replay is idempotent, two cancellation tombstones remain, invalid submissions
leave state unchanged and an injected Silver failure recovers. Recovery used a
new pipeline instance in the same process/Spark session. Peak cgroup memory was
1,590,865,920 bytes under a 1,610,612,736-byte limit; this small profile does not
establish that a million-event run fits the same cap.

[Match insight evidence](evidence/match-insights-local-2026-10-07.json) records
optional private football context/lineups/team statistics and VALORANT map
outcomes from selected poll archives. Curated files have checksums, observation
clocks and a publication pin. An optional failure leaves canonical scores usable.
Cross-phase conflicts are omitted. The worker reuses its verified intent rather
than revalidating the canonical package. The latest actual six-request football
check returned 2,132 unchanged matches and no lineups or individual statistics,
even with unfolding headers. PandaScore yielded 122 map-bearing records. Kills,
full map names and round scores are not claimed. The source constraint is
free-only; no paid endpoint, subscription, continuous collector or forecast was
added.
