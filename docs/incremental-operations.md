# Incremental Spark operations

The reference CLI remains a fast Python correctness lab. `arena-spark` is the
actual Linux Spark/Delta execution path. Both use the same canonical match
contract. Their validation policies differ deliberately: the reference engine
quarantines individual invalid observations; the stage runner rejects an entire
ambiguous candidate batch before publishing any Delta changes.

## Data flow and guarantees

1. Preserve each submitted candidate in `raw_landing`, including duplicates and
   invalid payloads. A rejection creates a quarantine metadata record.
2. Validate schema, version identity, stable team IDs and observation chronology
   against the affected Bronze history. Store immutable job inputs in SQLite.
3. Bronze consumes validated landing files through `AvailableNow`, persistent
   checkpoints and insert-only Delta MERGE. Accepted observations are not updated.
4. Silver MERGEs the current state and the entire observation history of affected
   match keys. A late lower revision can close a historical interval without
   replacing a higher current revision. Cancellation sets `deleted_at` on current
   match state. Postponement preserves the changed kickoff in the next revision.
5. Gold reads every current match involving the affected sport/team keys,
   recalculates their completed-match statistics and MERGEs only those team rows.
   An aggregate that becomes empty is soft-deleted. Active consumers filter
   `deleted_at IS NULL`; cancelled results never remain as stale wins.
6. Reconstruct all current matches, SCD2 intervals and active team aggregates with
   the independent Python oracle. Mark the run successful only when all agree.

`created_at`, `updated_at`, `deleted_at`, `source_system` and `quality_score` are
explicit on execution tables. A quality score of 1.0 means a row passed the defined
contract checks; it does not rate a provider's factual accuracy. Stable IDs, not
display names, identify teams. The most recently observed current match supplies
the display name, including an upcoming or cancelled match.

Delta commits are individually atomic. There is no multi-table transaction.
The writer lock serializes local jobs; pending/failed jobs block new submissions,
active Gold access through the runner, and snapshot publication. Immutable
snapshot export holds that lock and checks full parity again. Consumers should
read published snapshots rather than paths behind a running stage graph.

## Run the scenario

Create a reference snapshot first. The Docker image pins Python 3.11, Java 17,
Spark 4.1.3 and the Spark-4.1 Delta Lake 4.4.0 artifact.

```powershell
docker build -t arena-lakehouse:0.2.0 .
$snapshot = Get-Content .\data\demo\snapshots\LATEST
$data = (Resolve-Path .\data).Path
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$data,target=/data" --entrypoint arena-spark arena-lakehouse:0.2.0 --workspace /data/incremental-new scenario --snapshot "/data/demo/snapshots/$snapshot"
```

Use a fresh scenario workspace to preserve prior reports. It exercises identical
replay, late score correction, cancellation of a team's sole completed match,
postponed kickoff, stable-ID team rename, bounded backfill and failure between
Silver and Gold commits. Unaffected Gold rows must retain both their values and
audit timestamps. The report records every actual stage attempt and duration.

## Replay, recover and backfill

After building the image, all commands use the same mounted persistent workspace:

```powershell
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$data,target=/data" --entrypoint arena-spark arena-lakehouse:0.2.0 --workspace /data/persistent replay /data/input/events.jsonl
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$data,target=/data" --entrypoint arena-spark arena-lakehouse:0.2.0 --workspace /data/persistent resume
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$data,target=/data" --entrypoint arena-spark arena-lakehouse:0.2.0 --workspace /data/persistent backfill /data/input/events.jsonl --from 2025-01-01T00:00:00Z --to 2025-01-08T00:00:00Z --max-events 10000
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$data,target=/data" --entrypoint arena-spark arena-lakehouse:0.2.0 --workspace /data/persistent export --output /data/published
```

Backfill selects **observation availability**, not kickoff, in a half-open UTC
window `[from,to)`. A window must cover at most 31 days and at most 100,000
observations. Larger histories need multiple bounded submissions. There is no
fabricated historical availability for a result collected today.

Use `replay --fail-after silver` for a deliberate incident drill. `resume`
continues the earliest unfinished run. Successful stages are skipped; an attempt
left `running` after process death becomes `interrupted` and is retried. Idempotent
MERGEs protect a commit that succeeded before its attempt record was written.
Timeout/connection failures retry at most twice; validation and correctness
errors surface immediately. Attempt history is kept in `control/stages.db`.

If the ledger recorded success before `last-success.json` could be written,
resume that run explicitly with `resume --run-id <run_id>`. The runner rebuilds a
missing, malformed or stale marker from the latest verified job without
re-executing successful stages. Marker replacement is atomic, and resuming an
older successful run preserves the latest job's publication metadata.
Export checks the marker against the latest successful ledger entry before
writing a snapshot. If the marker is missing, unreadable or points to an older
run, use the run ID printed by the error to repair it before exporting.

The reference CLI also implements the same bounded selection:

```powershell
.\.venv\Scripts\arena-lakehouse.exe --workspace data/reference backfill data/input/events.jsonl --from 2025-01-01T00:00:00Z --to 2025-01-08T00:00:00Z
```

## Verify recovery with Arena Copilot

The regression test creates two successful runs and leaves the publication
marker pointing to the older one. It verifies that export refuses the stale
marker. The test then resumes the newer run and publishes a package. The
quality report names the recovered run, and `LATEST` selects that package.

Use a Lakehouse checkout containing the
[export provenance regression](https://github.com/Manuelma4/arena-lakehouse/commit/d6471f58a3fda598295c2524448933030a67204e)
and an [Arena Copilot checkout at 59ee6a3](https://github.com/Manuelma4/lakehouse-copilot/commit/59ee6a39a675e0157cc14d461c6ecf0cc3120a20).
Install each checkout in its own `.venv`, including the Lakehouse `dev` extra.
From the Lakehouse root, run the following in a POSIX shell. Set `copilot` to
the absolute path of the consumer checkout.

```sh
copilot=/absolute/path/to/lakehouse-copilot
run=$(mktemp -d)
.venv/bin/python -m pytest -q --basetemp "$run/pytest" \
  tests/test_incremental_configuration.py::test_export_requires_publication_marker_for_latest_verified_run
pointer=$(find "$run/pytest" -type f -path '*/published/LATEST' -print)
if [ -f "$pointer" ]; then
  package="$(dirname "$pointer")/$(cat "$pointer")"
  "$copilot/.venv/bin/python" -m arena_copilot.cli --snapshot "$package" validate
else
  printf 'Expected one exported LATEST pointer under %s\n' "$run/pytest" >&2
fi
```

Expect one passing producer test and a consumer manifest with
`"schema_version": "2.0"` and `"data_kind": "synthetic"`. The test substitutes
Spark stages and table reads while exercising the real ledger, exporter and
consumer validator. This check covers the exported package contract. Use the
Docker scenario above to test the Spark stages.

## Measured layout experiment

```powershell
docker run --rm --memory 6g --cpus 2 --mount "type=bind,source=$data,target=/data" --entrypoint arena-spark arena-lakehouse:0.2.0 --workspace /data/benchmark-new benchmark --snapshot "/data/demo/snapshots/$snapshot"
```

This experiment writes the same current-match data into 16-partition and
2-partition Delta layouts. It verifies identical sport aggregates, saves executed
query plans, file counts, file sizes, write times and three query times per layout.
A speedup is recorded only if observed. A small fixture with mixed cold/warm
JVM and filesystem caches is exploratory evidence, not a production benchmark.
It is a file-layout comparison rather than a claim that Databricks `OPTIMIZE`
or liquid clustering was tested locally.

## Boundaries

- The full independent oracle deliberately collects the demonstration dataset.
  It adds verification cost and currently limits scale. The 100,000-row candidate
  budget is a safety bound, not proof of a measured throughput target.
- Full Gold reconstruction is used for correctness verification, not maintenance.
  Only affected Gold keys are written in normal incremental stages.
- Two Gold tombstones after a cancellation retain audit lineage; Delta log/history
  retention still contains older states. Soft deletion is not physical erasure.
- Managed Auto Loader, Spark Declarative Pipelines, AUTO CDC, Jobs and bundles
  require separate Databricks workspace evidence before their status changes.
