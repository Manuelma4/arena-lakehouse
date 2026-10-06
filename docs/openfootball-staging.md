# OpenFootball acquisition and precision-aware staging

Status: implemented and tested locally on the isolated acquisition branch. This
module is **not canonical lakehouse ingestion**, does not change snapshot 2.0,
and does not feed the public synthetic showcase or forecasting model.

## Sources and scope

[`config/openfootball_sources.json`](../config/openfootball_sources.json) fixes
nine no-token files in [OpenFootball football.json](https://github.com/openfootball/football.json):
England, Spain, Germany, Italy and France 2026/27; Brazil 2026; Colombia, Argentina
and Libertadores 2025. The schema/data use [CC0-1.0](https://github.com/openfootball/football.json/blob/master/LICENSE.md).
Repository availability does not imply exhaustive competition coverage or fresh
scores. Colombia's checked file covers Apertura, and Argentina's covers the
first semester, rather than complete current-season LATAM calendars.

The [upstream update description](https://github.com/openfootball/football.json#frequently-asked-questions--answers)
distinguishes daily generated JSON from manually maintained source data. Polling
more frequently does not make those sources live. VLR scraping is outside this
module; the existing PandaScore free-fixtures adapter remains independent.

## Run

From the repository root, with the project dependencies installed:

```bash
python -m arena_lakehouse.openfootball --workspace data/openfootball collect \
  --source colombia-primera-a-2025 --dry-run

python -m arena_lakehouse.openfootball --workspace data/openfootball collect \
  --source colombia-primera-a-2025

python -m arena_lakehouse.openfootball --workspace data/openfootball export \
  --output data/openfootball-export-001

python -m arena_lakehouse.openfootball validate \
  --source colombia-primera-a-2025 --file data/saved-source.json
```

`validate` is offline and always dry; `collect --dry-run` requests/validates a
source but creates no archive, revision, poll or cursor. Export destinations are
immutable: choose a new directory for each export. These are module commands;
the existing `arena-lakehouse` CLI has not been changed.

For isolated-worktree testing on Windows without reinstalling the shared environment:

```powershell
$env:PYTHONPATH = (Resolve-Path src).Path
& C:\Works\.arena-venv\Scripts\python.exe -m pytest tests/test_openfootball.py -q
& C:\Works\.arena-venv\Scripts\python.exe -m ruff check src/arena_lakehouse/openfootball.py tests/test_openfootball.py
```

## Staging row contract

| Fields | Meaning |
| --- | --- |
| `source_id`, `competition_id`, `competition`, `season`, `round`, `occurrence` | Catalogue/source scope and the original competition/round display labels. Occurrence defaults to 1 for a unique identity; distinct repeated occurrences require an explicit reviewed discriminator. |
| `source_date`, `local_time`, `kickoff_precision` | Original calendar day and optional `HH:MM` wall clock; precision is `date_only` or `local_minute`. |
| `timezone`, `start_time`, `completed_at` | Always null. No midnight UTC, match duration or completion instant is invented. |
| `result_status` | `observed_result` when source `ft` scores exist; otherwise `unknown_result`. A past fixture without a score is not relabelled scheduled or 0–0. |
| `score_a`, `score_b`, `score_details_json` | Source `ft` pair, plus preserved `ft`, `ht`, `et`, `p` pairs. Two-integer score arrays normalize to `ft`. Unknown shapes are quarantined. |
| `score_basis`, `prediction_eligible` | `source_ft_unverified_regulation` or `unknown`; eligibility is always false. Extra-time/shootout outcomes are not silently interpreted as a 90-minute winner. |
| `team_a_id`, `team_b_id`, `team_namespace`, `team_identity_status` | Stable, namespace-scoped name-derived IDs; `unresolved_name_derived_not_official` explicitly distinguishes them from authoritative provider IDs. Reviewed catalogue aliases can preserve a known rename. |
| `observed_at`, `revision`, `event_id` | Actual collection instant of that business revision; monotonic revision per stable match. Unchanged polls preserve the original observation time. |
| `source_url`, `source_ref`, `source_sha256`, `license`, `license_url` | Raw source reference, exact downloaded-body checksum and CC0 provenance. `source_ref` is `master` or an explicitly pinned commit, never an invented publication timestamp. |
| `staging_version`, `availability_basis`, `quality_score` | Parser contract `openfootball-staging-1.0`, `first_collected_revision`, and 1.0 for rows passing this staging schema. This score does not certify completeness, official identity or model eligibility. |

The collector accepts at most 10 MB and 10,000 input rows per source snapshot.
Scores must be two nonnegative integers no greater than 50, with booleans rejected.
Missing rounds/names, invalid dates/clocks, ambiguous identities, duplicate JSON
keys and unsupported score shapes produce explicit quarantine evidence.

## Identity, corrections and state

Team identity hashes a configured namespace plus the exact normalized name or
reviewed alias. Country namespaces are independent of season, preserving a team
across seasons while separating identical names in different countries.
Libertadores has its own namespace; cross-provider/tournament reconciliation is
not implied. Unreviewed name changes can produce distinct derived identities;
alias/namespace changes require an explicit migration or a new reviewed workspace.

Match identity hashes competition, season, round, ordered team IDs and occurrence.
Date and score are excluded, so schedule/score corrections retain the match ID.
Identical duplicates collapse. Conflicting rows sharing a match identity are all
quarantined, independent of arrival order; the collector never guesses occurrence
from list position. Overlapping source scopes cannot steal another source's match.
Omitted or quarantined records do not automatically cancel/delete previous rows:
`matches` means last accepted observation, with poll/quarantine evidence beside it.

State lives in a separate `staging.sqlite`, never the canonical pipeline database.
Raw bytes are archived before parsing under `raw/<source_id>/<sha256>.json`.
SQLite commits changed current rows, append-only history, quarantine, cache and
poll audit in one transaction. A database failure retains the raw archive for
replay without advancing revisions/cursor; quarantines are idempotent by source,
body checksum, row and reason. Concurrent collection is not an advertised workflow;
use one worker and let SQLite reject/retry a busy writer separately.

Requests use a fixed repository, no token, no redirects, a 20-second timeout,
1-second minimum interval per collector, and at most three attempts by default
(configurable 1–5). Only network outages, 429 and 5xx retry; waits cap at 10 seconds.
ETag conditional requests are preferred, with Last-Modified fallback. A 304
requires a verified cached archive and emits only a poll audit; a repeated 200
body still emits no duplicate revisions. Dry runs do not update validators.

An export contains current/history JSON and typed Parquet, sources, quarantine,
poll audit and a checksum manifest. It is built in a temporary sibling directory
and published by rename. Its manifest declares `canonical_compatible: false` and
`data_kind: open_data`; it cannot masquerade as an Arena synthetic/API snapshot.

## Measured verification

On 6 October 2026, the isolated collector fetched
[Colombia 2025](https://raw.githubusercontent.com/openfootball/football.json/master/2025/co.1.json):
52,684 bytes, 200 accepted rows, zero quarantine; 180 local-minute rows, 20
date-only rows; 167 observed results, 33 unknown results. Dates span 24 January
through 25 May 2025. The SHA-256 was
`ed5aeff8ee558178bea5d5d00da8127e15da1edd451dd78ce3a0e2baf07c5b70`.
The first poll created 200 revisions. A repeated real poll returned HTTP 200,
completed in 0.359 seconds and produced zero new revisions; offline replay also
produced zero. JSON/Parquet exported 200 current and 200 history rows.
Live 304 behavior is not claimed; conditional behavior is verified with offline
HTTP fixtures. Runtime evidence stays in ignored `data/openfootball-smoke-20261006/`.

The offline suite covers idempotency, score variants, corrections, identity
collisions, cross-season/country identity, reviewed renames, precision, cache,
bounded retries, archive provenance, database rollback and JSON/Parquet parity.
No test calls the network.

Historical results downloaded today are known **today**, not at their historical
kickoffs. Point-in-time research needs genuinely observed revision history and
resolved time precision. Canonical integration must coordinate date precision,
timezone rules, score target semantics, provider allowlists, source licensing and
consumer presentation before these staged rows enter forecasting or publication.
