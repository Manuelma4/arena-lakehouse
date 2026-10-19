# Arena Lakehouse

A data platform for football and competitive VALORANT research.

[Companion application](https://github.com/Manuelma4/lakehouse-copilot) ·
[Measured evidence](docs/evidence/README.md) · [Engineering roadmap](ROADMAP.md)

Fixtures move, scores are corrected and the same match can arrive through several
source responses. Arena Lakehouse preserves those observations, reconstructs
match history and publishes consistent datasets for Arena Copilot. A correction
can be traced from its original response to the result and team statistics shown
in the application.

The project connects a Spark/Delta pipeline for incremental data engineering
with a SQLite/Arrow reference for acquisition, independent correctness checks
and publication. Their measurements are reported separately.

## Data and coverage

| Collection | Purpose | Boundary |
| --- | --- | --- |
| Original synthetic fixtures | Replay, corrections, recovery and baseline experiments | Fictional teams and results, preserved independently of real sources |
| OpenFootball research | Club competitions in Europe and Latin America, plus selected international editions | Community observations with explicit missing results and source-date precision |
| Historical Archive | Selected club and national-team editions with reviewed identities | Retrospective research; acquisition does not establish pre-match availability |
| Current provider captures | football-data.org fixtures/results and PandaScore VALORANT series/calendars | Documented free-account scopes, delayed or incomplete information, no in-game round feed |

Each publication records its sources, observation cutoff and checksums. Coverage
changes between captures; the [evidence index](docs/evidence/README.md) retains
the counts and conditions for each measured import. These collections do not
constitute a complete live calendar or official standings.

An optional metadata companion preserves catalogue, metrics, declared lineage,
quality and incidents, bound to the exact published snapshot for independent
inspection in Arena Copilot.

The consumer also provides a read-only SOAP data adapter over these verified
publications: source discovery, pinned match details and team statistics through
a WSDL 1.1/SOAP 1.1 contract. It can run independently without invoking this
producer or collecting data. See the
[service contract and deployment boundary](https://github.com/Manuelma4/lakehouse-copilot/blob/checkpoint/2026-10-08-reprise/docs/soap-data-service.md).

## Processing and publication

**Bronze** retains observations. **Silver** maintains current matches and revision
history. **Gold** exposes results and team aggregates through a
[versioned snapshot contract](CONTRACT.md).

The Spark path uses Python, Spark 4.1.3 and Delta Lake 4.4.0, with AvailableNow file
streaming, persistent checkpoints, MERGE and Change Data Feed. Late observations
update affected matches and aggregates. A stage ledger records attempts and
recovery; incomplete runs cannot publish snapshots. An independent reconstruction
checks state, history intervals and Gold results.

Source collection uses bounded requests, quota accounting, checksum-addressed
archives and sanitized failures. Failed checks preserve the last verified
publication. A successful provider check, a source observation and a publication
change have separate clocks. API polling and downstream Delta CDF are distinct
from transaction-log CDC.

The [paired publication contract](docs/provider-publication.md) binds canonical
matches and their nullable source calendar in one recoverable capture. Unknown
opponents or unconfirmed kickoff times remain calendar evidence. Conflicting
announcements cannot silently become confirmed scores.

## Engineering decisions

- Provider-scoped identities keep clubs, national teams and unrelated same-name
  events separate. Source corrections retain their original evidence.
- Dates and local clocks stay at their supplied precision. UTC kickoff is derived
  only from a supported explicit offset; completion times are not invented.
- Football goals, extra time and shootout kicks remain distinct. VALORANT series
  scores represent maps won, rather than rounds or individual performance.
- Exports bind schema versions, JSON/Parquet contents and checksums. Pinned-source
  validation rejects altered facts even when modified package hashes agree.
- Replay preserves accepted state. Quarantine, cancellation, backfill and recovery
  are checked against the independent reference, with original fixtures retained.

## Measured verification

The [native Spark report](docs/evidence/spark-native-local-2026-10-07.json) records
a synthetic 10,000-observation correctness run under one CPU and a 1.5 GiB
container limit, including replay and reference comparisons. It establishes that
specific configuration; million-observation Spark execution remains pending.

The [incremental scenario](docs/evidence/incremental-report.json) covers late
corrections, cancellation, backfill and failure recovery. The
[layout experiment](docs/evidence/layout-benchmark-report.json) records plans,
file counts and query times while checking identical results.

Separate reports cover [actual provider access](docs/evidence/authenticated-provider-verification-v0.6.json),
[open-data replay](docs/evidence/openfootball-acquisition-v0.5.json) and
[publication recovery](docs/evidence/paired-publication-v0.7.json). Offline
fixtures, intercepted responses and authenticated measurements are identified
within those reports.

## Development status

The active checkpoint is private. Automatic deployment is disabled during
development. The [roadmap](ROADMAP.md) maps the implementation to ingestion,
streaming, workflows, optimization, privacy and deployment learning objectives.

The optional [Databricks profile](docs/databricks-profile.md) supplies Auto Loader,
Spark Declarative Pipelines, AUTO CDC and Lakeflow Job configuration. It has
offline configuration evidence; managed workspace execution is still unverified.
Further gates include million-observation Spark execution, sustained operational
measurements, roster history and fuller retention/access-policy evidence.

The companion application's calibration studies use separately reviewed sources
through the [forecast boundary](docs/historical-forecast-source-boundary-2026-10-13.md).
Retrospective experiments cannot rewrite recorded Current forecasts.

Original code and fictional data use the MIT license. OpenFootball collections
retain their source license references. Authenticated provider data and imagery
remain subject to their respective terms; credentials and private captures stay
outside source control.
