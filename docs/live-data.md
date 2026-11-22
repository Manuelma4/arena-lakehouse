# Updated fixtures and results

`arena_lakehouse.live` is a durable polling/publication worker for the existing
football-data.org and PandaScore adapters. It keeps API data separate from the
fictional showcase and OpenFootball research. It does not train predictions.
Mock HTTP tests demonstrate the worker behavior; account coverage and freshness
require separate reports from actual authenticated calls.

The [football-data.org free plan](https://www.football-data.org/pricing) includes
fixtures with delayed scores/schedules and ten calls per minute. The worker does
not relabel delayed scores as instantaneous live data. PandaScore's
[free pricing](https://www.pandascore.co/pricing) allows 1,000 requests per hour.
Its [fixtures overview](https://developers.pandascore.co/docs/introduction)
describes match starts/ends and final results; the
[VALORANT plan reference](https://developers.pandascore.co/docs/plan-reference)
lists past/upcoming/running fixtures and explicitly excludes a real-time
VALORANT data plan. This integration supplies match-level polled observations,
not rounds, kills, economy, players or a guaranteed complete tournament calendar.

## Run with local credentials

Store `FOOTBALL_DATA_TOKEN` and `PANDASCORE_TOKEN` in an ignored local `.env` or
process environment. The explicit `--env-file` option reads that file without
copying it into a workspace or printing its values. Without the option, only the
current directory's `.env` is loaded, preserving existing process values.
Do not put credentials in command arguments, public artifacts or screenshots.

After installing the package in a local virtual environment, run from the
repository root with a local `.env` as described above.

```bash
arena-live --workspace data/live-local --provider football-data \
  --competitions PL,PD,BL1,SA,FL1,BSA --cycles 3 --interval 60 --env-file .env
arena-live --workspace data/live-local --provider pandascore \
  --pages 1 --cycles 3 --interval 60 --env-file .env
```

Use 1..100 cycles, an interval of 30..86,400 seconds, and 1..10 PandaScore pages.
The interval is a minimum wait between completed polls within the worker loop.
Each PandaScore page requests past, upcoming and running lists, so one page
normally consumes three requests before retries. Football requests one selected
competition or an explicit comma-separated competition list per poll. The example
selects the five major European league codes and Brazilian Serie A. Each code
uses its own `CODE-cursor.json`; the same publication can preserve earlier rows
from all selected competitions. The source scope is immutable for that workspace;
use a new base workspace when changing the competition list or page budget.
Account access to every selected code must still be verified. Cross-provider
identity mapping and additional Latin American provider coverage remain future work.

Six selected competition endpoints require six requests per complete cycle before
retries. The configuration refuses a cadence that would necessarily exceed the
conservative football budget; six codes at 30 seconds is rejected, while the
60-second example is accepted. Transient retries may still exhaust the rolling
quota, producing explicit `quota_limited` status rather than uncontrolled requests.

After bounded verification, replace `--cycles 3` with `--forever` to run an explicit
foreground service. These options are mutually exclusive. Ctrl+C closes owned
clients, records an interrupted in-flight poll and preserves the last selected
snapshot. The command does not spawn a background service automatically.

## Durable execution

The worker automatically uses `BASE/football-data` and `BASE/pandascore`, each
with its own cursor, raw archives, reference pipeline, local OS writer lock and
SQLite poll/request ledger. The lock serializes provider writes and is released
on process death. Every HTTP GET, including connector retries, reserves a durable
request slot **before** transmission. A process that dies after reservation may
therefore consume a conservative slot even if transmission did not occur.
Redirects are disabled, and request URLs stay on the selected provider host.

Rolling limits are ten reservations per 60 seconds for football and 1,000 per
3,600 seconds for PandaScore. Exhaustion stops the poll with `quota_exhausted`;
the next scheduled check can proceed after the window expires. Retries and failed
polls do not reset the budget. Other tools, separate base workspaces and account
activity outside this worker are not included: use one centrally scheduled
workspace per provider/account and leave headroom when sharing an account.

Successful responses are archived before interpretation by the existing adapter.
The conservative common observation clock is sampled after all requested responses
arrive, never inferred from a historical kickoff. Football completion currently
uses collection as an upper bound; PandaScore uses `end_at` when present. Their
source semantics and completion-only correction limitation remain documented in
[provider coverage](providers.md).

The worker collects every selected scope, ingests canonical observations, and
prepares and validates both packages where applicable. It records a durable
intent, commits the scope cursors, and atomically selects the pair before updating
health and compatibility files. A failed scope fetch stops before ingest and
cursor commits. An ingest that reports quarantined rows stops publication.
A pending intent is recovered with the original poll and packages before making
another provider request. See [the publication protocol](provider-publication.md)
for the exact order and recovery boundaries.

The worker reuses a validated canonical package when its dataset is unchanged.
A successful source check advances `last_success_at`. The canonical
`last_change_at` advances when a different canonical snapshot is selected.
An empty response does not remove previously accepted observations. PandaScore
publishes a validated calendar descriptor even when there is no canonical
package.

## Consumer selection and health

New readers select the canonical snapshot and calendar from
`BASE/PROVIDER/snapshots/CURRENT_PUBLICATION.json`. Read the pair from one selector
and validate both descriptors before use. Null descriptors explicitly represent
an absent package. The atomically written `LIVE_STATUS.json` reports freshness,
poll results and quota usage. It may lag the pair selector after interruption,
as can `LATEST` and `CALENDAR_LATEST`. Read those files as compatibility views.
A selected snapshot name is exactly
`snapshot-<16 hexadecimal dataset version>-<8 hexadecimal suffix>`.

The required `arena-live-status-1.0` fields are:

| Field | Meaning |
|---|---|
| `provider`, `data_kind` | Selected provider and `api`, including before the first connection |
| `source_scope` | Typed selected competition list or match phases, with page budget |
| `status` | `awaiting_credentials`, `polling`, `ready`, `empty`, `degraded` or `quota_limited` |
| `last_attempt_at` | Latest attempted poll's aware UTC-Z start, or null |
| `last_success_at` | Last successful check's aware UTC-Z completion, or null |
| `last_change_at` | Completion time of the last successful poll that changed the canonical snapshot, or null |
| `poll_interval_seconds` | Configured bounded interval |
| `last_error_code` | Fixed redacted error code or null; never exception text |
| `latest_snapshot` | Last successful snapshot basename or null |
| `poll_counts` | Attempted, succeeded, failed, awaiting-credentials, quota-limited and interrupted counts |
| `last_poll` | Source, newly accepted, unsupported row counts and reserved request count |
| `capabilities` | Free tier, `polled_rest`, fixtures/results, provider delay and no in-game round feed |
| `request_budget` | Limit, rolling window seconds, used reservations and remaining slots |

Clock strings use fixed microseconds and UTC `Z`. A successful check clock and a
dataset's observation cutoff are distinct; newer checks do not rewrite old evidence.
`source_scope` is `{kind:"competition_list", competition_codes:[...], phases:[],
pages:1}` for football and `{kind:"match_phases", competition_codes:[],
phases:["past","upcoming","running"], pages:N}` for PandaScore. Success means
every declared selected scope completed in that cycle. Legacy health records
without this field must be labelled **scope unknown**, never interpreted as proof
that every retained competition was checked. `last_change_at` comes from the
completed poll in the ledger. For the pair's selection time, read `selected_at`
from the authoritative selector.
Absent credentials produce `awaiting_credentials`, zero requests and no synthetic
substitute dataset. A successful source check without any canonical rows produces
`empty`, a successful check clock and no canonical snapshot. The PandaScore calendar can
still be present.

Errors are limited to `awaiting_credentials`, `quota_exhausted`,
`authentication_denied`, `provider_rate_limited`, `provider_unavailable`,
`provider_http_error`, `network_error`, `network_timeout`,
`invalid_provider_payload`, `ingest_failed`, `publication_failed`,
`cursor_commit_failed`, `worker_interrupted`, `clock_regression`,
`invalid_endpoint` and `worker_error`. SQLite stores status codes and these codes;
credentials, headers and response error text do not enter the health/request ledger.
Provider response archives are private runtime artifacts and remain outside Git.

## Verification boundary

Offline tests use HTTPX MockTransport and fake local credentials. They cover
unchanged/changed responses, six-competition cycles, partial fetch/cursor failures,
empty/unsupported rows, interrupted committed ingest,
cursor/publication failures, durable retry budgets, denied access, network outages,
redaction, locks and checksum tampering. These tests do not claim that an account
supports every endpoint or that results are timely. Actual onboarding evidence
must record response access, coverage, collection clocks and replay outcomes
without exposing tokens or treating provider archives as MIT fixtures.
