# Development reference

The reference CLI runs on Python 3.11. Spark execution uses a Linux container
with Java 17, PySpark 4.1.3 and Delta Lake 4.4.0's Spark 4.1-specific artifact.
Run Spark and local model inference in separate stages on a 16 GB host.

## Reference environment

From the repository root on Windows:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\arena-lakehouse.exe --workspace data/showcase demo --matches 600 --seed 42 --showcase
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src tests scripts databricks
```

On Linux, use `python3.11`, `.venv/bin/python` and
`.venv/bin/arena-lakehouse`. Activate the environment before the commands below.

`--showcase` assigns the original records to three chronological competitions
per sport. It changes competition metadata only. Without that flag, the original
single-competition fixtures are preserved. Use a fresh workspace when changing
the seed, size or showcase option; observations with the same identity must not
silently acquire different payloads.

## Replay and export

```bash
arena-lakehouse --workspace data/replay replay observations.jsonl
arena-lakehouse --workspace data/replay backfill observations.jsonl \
  --from 2025-01-01T00:00:00Z --to 2025-02-01T00:00:00Z
arena-lakehouse --workspace data/replay export
```

Backfill bounds apply to observation availability time. Snapshot directories are
immutable; `snapshots/LATEST` names the latest completed export. The complete
interface is defined in [CONTRACT.md](../CONTRACT.md).

## Spark container

Docker Desktop must use Linux containers. From PowerShell:

```powershell
docker build -t arena-lakehouse:0.3.0 .
$arenaSnapshot = Get-Content .\data\showcase\snapshots\LATEST
$arenaData = (Resolve-Path .\data).Path
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$arenaData,target=/data" arena-lakehouse:0.3.0 --snapshot "/data/showcase/snapshots/$arenaSnapshot" --output /data/spark-smoke-v0.3
```

Use a fresh output directory to retain previous reports. Checkpoint restart is
part of the smoke. Persistent incremental commands, failure injection and layout
measurements are documented in [incremental operations](incremental-operations.md).
The `arena-spark` entry point provides `scenario`, `replay`, `backfill`, `resume`,
`export` and `benchmark` in the same container.

Local Spark defaults remain two execution threads and a 2048 MB driver heap.
For a constrained review, pass only `-e ARENA_SPARK_THREADS=1` and
`-e ARENA_SPARK_DRIVER_MEMORY_MB=512` to the container. Supported budgets are
1–2 threads and 512–2048 MB; invalid budgets fail before Spark starts. Container
limits such as `--cpus 1 --memory 1536m --memory-swap 1536m` also bound overhead;
the Java heap setting alone does not cap the whole container. These settings do
not alter Docker/WSL allocation or install an inference model.

The image separates dependency installation and the Java smoke into separate
layers, so a delayed Maven preparation does not invalidate completed Python
dependency installation. The build smoke uses one thread and a 512 MB heap;
runtime acceptance must still be measured with the actual workload.

## Optional providers

Source checks can be reviewed without starting a collector:

```powershell
python -m arena_lakehouse.source_health --workspace data/live --provider football-data --window-seconds 86400 --max-age-seconds 7200 --success-target 0.99
```

`arena-health` provides the same command after reinstalling the editable project.
Exit code zero means the observed local checks meet the configured policy; other
states return one. Failed, quota-limited, interrupted and missing-credential checks
count as failures. The window includes checks by completion time, with older
successful checks retained only for age calculation. No completed checks means
no observed success ratio. The report includes the observed history length;
a small sample does not prove a service SLA, complete match coverage or score
latency. Publication presence does not replace checksum validation by consumers.

On this Windows host, `scripts/repair-local-docker.ps1` performs read-only
diagnostics by default. Its explicit `-InstallKernel` option requires an
Administrator console and the prepared, signature/hash-checked Microsoft MSI.
It installs the missing WSL kernel without an automatic restart or memory
changes. Follow [Microsoft's kernel procedure](https://learn.microsoft.com/en-us/windows/wsl/install-manual)
and [Docker's WSL2 backend instructions](https://docs.docker.com/desktop/features/wsl/)
before claiming repaired Docker or native Spark acceptance. The existing
Hyper-V image/volume data is not reset or removed.

Copy `.env.example` to an ignored `.env` and supply personal provider tokens.
Keep API workspaces separate from synthetic data.

```bash
arena-lakehouse --workspace data/real collect --provider football-data --competition PL
arena-lakehouse --workspace data/real collect --provider pandascore --pages 1
arena-lakehouse --workspace data/real poll --provider football-data --cycles 3 --interval 300
```

The collector preserves raw bodies privately, validates normalized observations
and advances its cursor after a successful canonical transaction. Historical
downloads become available at collection time. [Provider notes](providers.md)
describe rate limits and account-dependent coverage; no live account is verified
by the offline tests.

## Managed target

[The Databricks profile](databricks-profile.md) describes the configurable
Auto Loader/SDP/AUTO CDC graph, manual Lakeflow Job and bundle. Offline schema
validation and local Spark evidence are separate from workspace acceptance.
There is no enabled schedule by default.

## Private match details from free captures

Each successful provider worker poll optionally writes a curated match-insight
package without more API calls. Football lists request the documented unfolding
headers; account coverage still determines whether lineups and statistical
counters are present. Rebuild an optional package from the selected completed
poll's exact private archives:

```powershell
python -m arena_lakehouse.match_insights --workspace data/live --provider football-data
python -m arena_lakehouse.match_insights --workspace data/live --provider pandascore
```

The command verifies the completed intent and archive checksums, never discovers
arbitrary old archives, and makes zero provider requests. Immutable files live
under `data/live/<provider>/match-insights`; the optional selector lives under
`snapshots/CURRENT_MATCH_INSIGHTS.json`. The consumer must match its exact
publication fingerprint. The worker reuses its already-verified intent so this
optional step does not repeat expensive canonical validation. Failed details
cannot replace canonical scores. Only free sources are authorized; individual
VALORANT kills/KDA and currently omitted football lineups remain acquisition
gates. Current squads cannot establish historical match participants.
