# Local verification evidence

Environment: Windows host, Docker Desktop Linux backend, Python 3.11,
Java 17, Spark 4.1.3 and Delta 4.4.0 (Spark 4.1 JVM artifact).

The stored `spark-smoke.json` is an actual run, not an expected-output template.
It includes checkpoint restart, a second-file late correction, CDF consumption
and equality with independent Python team aggregates. Gold is fully rebuilt;
this run does not prove incremental throughput, multiple-writer safety or scale.

`synthetic-model-report.json` records chronological validation/test measurements
for seed 42, 600 fictional matches. Inspect both model and frequency baseline:
football accuracy and log loss need not improve together. These are synthetic
measurements, not real-world forecasts.

`notebook-smoke.json` records successful execution of every code cell in the first
learning notebook. Source notebooks deliberately contain no prefilled outputs.

`software-verification.json` records the original 19-test local run and code checks.
Public GitHub Actions now provides remote software and separately dispatched
Linux Spark verification. Original reports retain their measured configuration.

`showcase-verification-v0.3.json` records the 37-test local suite, the new
six-competition export and unchanged forecast payloads. This milestone adds
competition coverage; the Spark reports below remain their original measured
runs rather than being relabelled as new performance experiments.

## Incremental operations milestone

`incremental-report.json` records an actual 163.19-second Linux container run,
with 6 GB memory and two CPUs. Initial 1,193 observations include a disconnected
sole-result team pair. Four later revisions produce 1,197 observations/CDF rows:
late score correction, cancellation, postponed kickoff and stable-ID team rename.
Sixteen active team summaries remain, with two soft-deleted cancelled-only teams.
Every run matches an independent complete Python reconstruction of current
matches, observation SCD2 history and Gold metrics. An injected failure after
Silver resumes from the persistent stage ledger; incomplete state cannot publish.
Unchanged team rows preserve their original audit timestamps.

`layout-benchmark-report.json` and `layout-*-plan.txt` are the actual small-layout
experiment. Six hundred current matches produce the same aggregates in layouts
with 16 files (127,652 bytes) and two files (33,794 bytes). Median sampled query
times were 1.125 seconds and 0.644 seconds. The 24.18-second first query exposes
cold-start/cache effects; the 1.75x observed ratio is not a controlled production
performance claim. There is no million-event or managed OPTIMIZE evidence yet.

`databricks-bundle-schema.json` records official CLI 1.19.0 offline JSON Schema
validation. No authentication or workspace deployment occurred. Managed Auto
Loader, expectations, AUTO CDC and Job behavior remain pending workspace checks.

`operations-software-verification.json` records the expanded local tests/code
checks. Source is mounted via `PYTHONPATH=/workspace/src` into the previously
built image for the measured incremental run; its package is not the cached
installed foundation source. See `docs/incremental-operations.md` for equivalent
commands after a normal image rebuild.
