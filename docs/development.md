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

## Optional providers

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
