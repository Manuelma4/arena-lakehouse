# Isolated scale verification

This is a reproducible synthetic workload and an explicit measurement harness,
separate from the published 600-match showcase and the OpenFootball research
packages. Implementing or generating a workload is not evidence that Spark has
processed its requested size. No new scale measurement is claimed by this document.

## Workload contract

`arena-scale-workload-1.0` stores a deterministic manifest and UTF-8/LF JSONL
shards. Each fictional match has two observations: scheduled revision 1 and
finished revision 2. Therefore 10,000 base observations represent 5,000 matches;
1,000,000 observations represent 500,000 matches. The base files contain no
duplicates, faults or scenario corrections. Their counts are recorded separately.

The default topology has 64 scoped teams per sport. Names derive from the
project's original fictional names with an explicit Scale cohort suffix. IDs
include a caller-supplied `synthetic:scale` namespace. Results are fictional
integer SHA-256 choices, including valid best-of-three VALORANT map scores.
Kickoffs span at most 30 days by default, rather than extending centuries to
increase row count. Scheduled observations are available two days before kickoff;
results are observed ten minutes after their synthetic completion. Those clocks
describe the simulator, not historical availability of real provider data.

Generation uses canonical JSON and integer arithmetic. Pair choices hash
`[version, seed, match_index, "match"]`. A bounded block of complete match pairs
is ordered by the hash of `[version, seed, namespace, "shuffle", block_number,
event_id]`. The default buffer is 1,000 observations; shard size never exceeds
100,000. No full-dataset list or global shuffle is allocated. Changing shard size
with the same shuffle buffer preserves the observation sequence. Namespace,
seed or buffer changes remain explicit in the manifest.

The manifest records config, source date bounds, exact counts, shard byte sizes
and SHA-256 checksums. Its dataset identity is SHA-256 of canonical manifest JSON
without `dataset_version`, truncated to 16 hexadecimal characters. There is no
publication clock in the package. Full validation checks inventory, canonical
wire types, hashes and each row against deterministic regeneration. A re-sealed
score or duplicate-ID mutation therefore still fails validation. Destinations
must be new; existing fixtures and evidence are retained.

## Generate and validate without Spark

From the producer checkout, use its `src` via `PYTHONPATH` without reinstalling
the shared environment. These commands are procedures, not completed measurements.

```powershell
$env:PYTHONPATH = (Resolve-Path .\src).Path
& C:\Works\.arena-venv\Scripts\python.exe -m arena_lakehouse.scale generate --observations 10000 --namespace scale-10k-001 --output data/scale-10k-workload-001
& C:\Works\.arena-venv\Scripts\python.exe -m arena_lakehouse.scale validate --workload data/scale-10k-workload-001

& C:\Works\.arena-venv\Scripts\python.exe -m arena_lakehouse.scale generate --observations 1000000 --namespace scale-1m-001 --shard-observations 100000 --output data/scale-1m-workload-001
```

Generation prints measured elapsed time, disk bytes and available process resource
values. Capture stdout as independent generation evidence. Package checksums stay
reproducible because those measured values are not inserted into the manifest.

## Explicit Linux Spark scenario

Run only after resource scheduling on the 16 GB host. Start with a six-GB,
two-CPU container and no concurrent model inference. Bind the selected source
checkout read-only and set `PYTHONPATH=/app/src`; bind a separate ignored data
directory for writes. Reusing an earlier execution image must be declared in the
external run evidence rather than claiming that its installed package is current.

The module commands inside that container are:

```text
python -m arena_lakehouse.scale scenario --workload /data/scale-10k-workload-001 --workspace /data/scale-10k-run-001 --crosscheck-sqlite
python -m arena_lakehouse.scale scenario --workload /data/scale-1m-workload-001 --workspace /data/scale-1m-run-001 --verification sqlite --bronze-timeout-seconds 900
```

The 10,000-observation path retains the existing full Python reconstruction after
each stage graph. `--crosscheck-sqlite` additionally reconstructs all accepted
input independently in SQLite and compares final Bronze, current Silver, history
and active Gold. The explicit SQLite verification path supplies that independent
oracle to every completed graph without collecting all observations into Python.
The default Python path refuses more than 10,000 base observations before creating
a scenario workspace. A larger deadline changes a bound, not a throughput claim.

The scenario processes all base shards, replays them identically, then submits one
disconnected sole result and four revisions: late score correction, cancellation,
postponement and stable-ID name change. Successful completion therefore contains
`base + 5` unique observations. Two invalid observations are submitted separately
and must leave canonical state unchanged. Total submitted observations are
`2 × base + 7`, including replay and rejected candidates. The scenario checks
unchanged values and audit timestamps on unaffected Gold rows and exactly two
cancellation tombstones. No prediction training runs. The small Python path can
publish a canonical snapshot with `model_report.status="not_run"`.

Failure after Silver is deliberately injected and recorded. Recovery recreates
`IncrementalPipeline` in the **same Python process and Spark session**, using its
persistent ledger and skipping successful stages. This is runner recovery, not
evidence of a container or JVM restart. A separately scheduled cross-container
restart drill needs its own report.

## Evidence and acceptance

`scale-report.json` is updated after submissions and retained when execution fails.
`expected_counts` contains targets; actual base/final counts appear only after
querying Delta. Submitted counts, failed attempts and rejected candidates remain
visible. A failure must not acquire a success label or fabricated throughput.

Reports include the actual workload manifest hash, generator config and SHA-256
of runtime Python source files, rather than relying solely on a pre-commit Git
revision. Validation, shard reads, oracle input, Spark startup, dispatch/stage
attempts, recovery and snapshot export have separate timing scopes. Base throughput
includes base shard reads and graph verification; it is not a JVM-only ingestion
rate. Dispatch time includes validation, orchestration and stages, so subtracting
stage durations does not isolate schema validation perfectly.

Linux cgroup memory current/peak/limit, process and child maximum RSS, and final
workload/workspace disk footprints are measured when available; unsupported
capabilities remain null. Cgroup peak includes Python and the JVM. Child RSS
applies to reaped child processes, and these counters do not establish distributed
production scale or an operational SLA.

Acceptance requires exact unique base counts, full independent table parity,
idempotent replay, correction/cancellation/recovery checks, unchanged unaffected
audit rows and actual resource/timing evidence. A one-million-event claim requires
that specific run to finish successfully. Layout/query experiments remain distinct
from event ingestion; retain slower or failed results and all earlier evidence.
