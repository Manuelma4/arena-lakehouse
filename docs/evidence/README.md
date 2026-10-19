# Local verification evidence

[Completed offline baseline suites](offline-baseline-suites-2026-10-19.json) record
2,176 consumer and 774 producer passes, with nine and one Windows symlink omissions.
Their initial harness, later phase-accounting checks and additive source-feature
scopes remain separate; see [the verification note](../OFFLINE_TEST_VERIFICATION_2026-10-19.md).

[Approved source-evidence delivery](source-evidence-delivery-2026-10-19.json)
records the optional checked metadata exporter and its full-application consumer
integration. Its 76 exporter cases and final harness checks are separate from
the earlier feature and baseline full-suite scopes. The report preserves the
derived-index restart distinction and the limited role of declared lineage and
quality metadata. Read [the delivery note](../SOURCE_EVIDENCE_DELIVERY_2026-10-19.md).

[Historical acquisition and handoff](historical-acquisition-local-2026-10-08.json)
records a real nine-request Figshare capture of the pinned CC BY 4.0 originals,
successful World Cup detail normalization, independent consumer validation of all
64 matches, and zero-request immutable replay. The separate 96 transport fixtures
do not establish sporting accuracy or real-time availability.

[8 October local continuation](local-continuation-2026-10-08.json) records bounded
free collection, detail-integrity checks and durable polling identity. Earlier
reports retain their original environment and verification scope.

Final transfer: [handoff verification](handoff-verification-2026-10-07.json).
The [Docker idle recovery](docker-idle-recovery-2026-10-07.json) records a later
failed host repair, separately from the successful native Spark acceptance.

Latest continuation: [native 10k Spark and event identity](spark-native-local-2026-10-07.json)
and [private match insights](match-insights-local-2026-10-07.json). Native 10k
execution passed; million-event execution and actual-process recovery remain
pending. The older reports retain their original preparation boundaries.

The [7 October Windows continuation](local-plan-continuation-2026-10-07.json)
records read-only source monitoring, scoped tests, 10k workload preparation and
a repaired Docker/WSL2 container check. It does not claim a new Spark run,
source acquisition, managed execution or inference.

Environment: Windows host, Docker Desktop Linux backend, Python 3.11,
Java 17, Spark 4.1.3 and Delta 4.4.0 (Spark 4.1 JVM artifact).

The [7 October handoff](../HANDOFF.md) identifies the portable checkpoint and
remaining execution gates. [Event identity preflight](../event-identity-preflight.md)
records 26 passing offline relational/control tests; native Spark verification
of that revision is now recorded in the native continuation above.

## Million-observation workload preparation

[scale-1m-workload-preparation.json](scale-1m-workload-preparation.json) records
actual generation and full validation of 1,000,000 synthetic observations across
500,000 matches and ten shards. Generation took 42.49 seconds; validation took
67.98 seconds. The package occupies 599,636,595 bytes, with unchanged generator
source hashes before and after execution. These are workload preparation
measurements. Spark ingestion, replay, recovery and scale throughput remain
unmeasured for this package; the existing 10k execution gate comes first.

[scale-1m-reference-preparation.json](scale-1m-reference-preparation.json) records
the independent SQLite reference prepared in 230.00 seconds: one million
observations/history rows, 500,000 current matches and 128 team aggregates.
Replaying the first 100,000 observations preserved the state and Gold values;
a conflicting transaction rolled back completely. Four bounded Parquet exports
retain checksums. This prepares expected results for the later Spark comparison;
it measures neither Spark throughput nor pipeline recovery.

## Paired source publication - v0.7.0

[remote-software-v0.7.json](remote-software-v0.7.json) records 395 passing tests
and Ruff on the standard Linux runner at the released producer source. These
software checks use fixtures and intercepted responses; native Spark, inference
and managed workspace measurements remain separate.

[paired-publication-v0.7.json](paired-publication-v0.7.json) records the final
394-test local suite, one Windows symbolic-link privilege skip, eligibility and
cursor recovery regressions, and the four-stage cross-repository mock bridge.
Conflicting or ambiguous results preserve prior canonical history; original
source announcements remain available as calendar evidence.
[installed-wheel-v0.7.json](installed-wheel-v0.7.json) records exact package bytes,
resources, LICENSE and text-only README metadata, isolated distribution/module
origins and the same mock integration. The combined verifier's Windows socket
guard failure and a subsequent API audit field error are retained; the corrected
API-only probe passed without rebuilding or repeating the bridge. Existing runtime
dependencies were reused. No new authenticated source, native model, Spark scale
or managed Databricks measurement is claimed.

## Authenticated source polling - v0.6.0

[source-operations-software-v0.6.json](source-operations-software-v0.6.json)
records 331 passing offline tests, Ruff and unchanged original source inputs.
The scale generator and independent bounded oracle are tested; their larger
Spark execution remains a separate, unmeasured gate.
[installed-wheel-v0.6.json](installed-wheel-v0.6.json) records the isolated wheel,
checksums, packaged resources and runtime probes without provider or model calls.

[authenticated-provider-verification-v0.6.json](authenticated-provider-verification-v0.6.json)
records three actual cycles for six football competitions and the three VALORANT
fixture phases. All 27 HTTP requests succeeded, quota windows were respected and
unchanged responses preserved the selected canonical snapshots. Football retained
2,132 matches; VALORANT retained 106 canonical matches with 18 unresolved
announcements outside that strict table. These runs use the SQLite/Arrow
reference backend and do not constitute new Spark or model-training measurements.

[provider-calendar-verification-v0.6.json](provider-calendar-verification-v0.6.json)
records successful publication of the exact saved 124-record source collection:
106 canonical links, 18 pending announcements and byte-identical repeated
publication. The first actual attempt rejected thirteen empty optional series
names; a tested compatibility fix represents those names as null while preserving
IDs, full names and raw archive evidence. Calendar selection by the polling
worker and an event-directory consumer remain separate steps.

## World Cup and ten-edition research - v0.5.0

[openfootball-acquisition-v0.5.json](openfootball-acquisition-v0.5.json) records
2,831 current matches/revisions, ten editions, 20 successful polls and zero
quarantine. Nine verified club archives were replayed into a new v2 workspace;
the pinned World Cup was fetched through the bounded no-token HTTP collector.
Replaying all ten inputs produced zero revisions and identical current/history.

The 104 World Cup rows preserve numeric offsets and separate FT/HT/ET/penalty
scores. Publication binds business fields and full edition coverage to bundled,
checksum-verified pinned bytes. Re-sealed mutations do not become authentic by
retaining the original declared body checksum. Completion remains unknown, all
real research predictions remain disabled, and the JSON/TXT clock disagreement
is documented rather than silently reconciled.

[openfootball-software-verification-v0.5.json](openfootball-software-verification-v0.5.json)
records the final 207-test suite, independent hostile-source cases, Ruff and a fresh
0.5.0 wheel imported in isolated Python outside the repository. The wheel validates
and republishes both v1 and v2 with identical manifests/payload hashes at fixed
publication clocks; packaged catalogue and trusted raw resources are included.
The actual legacy SQLite store dry replay leaves its bytes and 2,727 rows unchanged.
No new Spark benchmark or managed workspace run is claimed in this milestone.

## Earlier evidence retained

`openfootball-acquisition-v0.4.json` records an actual nine-source no-token CC0
download: 2,727 current/history records, zero quarantine, 1,088 source results
and 1,639 unknowns. Exact archive replay produced zero new revisions and retained
current/history JSON and Parquet checksums. The initial 113-test offline suite
and Ruff passed. Subsequent poll/provenance hardening passes 144 tests;
`openfootball-software-verification-v0.4.json` records that final check. The research publication retains null UTC kickoff/completion, actual
collection availability and disabled forecasting; it is a distinct SQLite/Gold
path, not relabelled Spark throughput or official standings evidence.

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
performance claim. There is no million-event Spark execution or managed OPTIMIZE
evidence yet.

`databricks-bundle-schema.json` records official CLI 1.19.0 offline JSON Schema
validation. No authentication or workspace deployment occurred. Managed Auto
Loader, expectations, AUTO CDC and Job behavior remain pending workspace checks.

`operations-software-verification.json` records the expanded local tests/code
checks. Source is mounted via `PYTHONPATH=/workspace/src` into the previously
built image for the measured incremental run; its package is not the cached
installed foundation source. See `docs/incremental-operations.md` for equivalent
commands after a normal image rebuild.
