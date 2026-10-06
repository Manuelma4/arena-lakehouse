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

`software-verification.json` records the 19-test local run and code checks.
The GitHub workflows are configuration only until they run on a remote repository.
