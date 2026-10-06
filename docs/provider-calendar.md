# Provider announcement calendar

The strict canonical match schema requires two resolved teams and an exact kickoff
instant. A provider's fixture pages also contain announcements without either of
those fields. The private `arena-provider-calendar-1.0` publication retains these
announcements separately, so an event directory can show a pending match without
inventing opponents, midnight UTC, or a result.

## Publication boundary

`arena_lakehouse.calendar.publish_calendar()` consumes the **explicit**
`meta['raw_archives']` returned by one successful PandaScore `collect()` call. It
does not discover archives with a glob, make HTTP requests, or select the newest
file. Every configured page must have exactly one `past`, `upcoming`, and `running`
response, with the same request clock. Empty response pages remain part of that
coverage evidence. An interrupted or partially successful collection cannot
publish a complete calendar.

Each archive must be a regular file under the caller's approved `archive_root`.
Symlinks and Windows reparse points are rejected. The accepted collector envelope
contains exactly `provider`, `endpoint`, `requested_at`, `observed_at`, and `payload`.
Endpoint strings are constrained to the fixed VALORANT fixture phases and page
parameters; URLs, authorization headers, and query tokens are not accepted.

The contract permits 1–10 pages per phase, 100 records per page, and 3,000 source
records in total. Each raw file is limited to 4 MiB; a poll is limited to 64 MiB.
Published JSON files are limited to 32 MiB each. Provider identifiers are positive
integers no larger than JavaScript's maximum exact integer. Text, scores, date
fields, and nested result lists have explicit bounds. Malformed inputs fail the
publication and retain the previous calendar pointer.

## API and integration

```python
from arena_lakehouse.calendar import publish_calendar, validate_calendar

manifest = publish_calendar(
    meta['raw_archives'],
    provider_workspace / 'calendars',
    archive_root=provider_workspace / 'provider_raw',
    pages=config.pages,
    canonical_match_ids=stored_pandascore_match_ids,
)
package = provider_workspace / 'calendars' / f"calendar-{manifest['dataset_version']}"
validated = validate_calendar(package)
```

`canonical_match_ids` must come from the successfully ingested canonical store.
The publisher does not infer canonical eligibility from the announcement fields.
Omitting the argument leaves every link null. A link is the exact canonical ID,
such as `pandascore:501`; conflicting phase records remain unlinked.

Recommended worker order is: collect all selected pages, ingest and publish the
canonical snapshot, publish the calendar, commit the cursor, then record poll
success. The worker should hold its existing provider writer lock throughout.
The calendar publisher expects one writer; it does not create a second provider
lock or poller. The current polling worker remains unchanged until coordinated
integration is verified.

The canonical and calendar pointers are **independent**, with different dataset
versions. Updating two files cannot provide a shared atomic transaction. A future
consumer should use the successful poll's explicit selected versions or validate
and label each publication independently. It must not present an independently
advanced calendar as a successfully checked canonical dataset after a failure.

## Wire format

The private output directory contains immutable `calendar-<16 lowercase hex>`
packages. Each package has:

| File | Meaning |
| --- | --- |
| `manifest.json` | Contract, provider, dataset version, publication clocks, scope, counts, and SHA-256 inventory |
| `announcements.json` | One typed aggregate per provider match ID |
| `occurrences.json` | Every retrieved record, including duplicate or conflicting phase records |
| `sources.json` | Exact phase/page coverage, request and response clocks, raw archive checksum, and received record count |
| `quality.json` | Measured unresolved, duplicate, conflicting, linked, and known-result counts |

`CALENDAR_LATEST` is an atomically replaced basename pointer beside these packages.
Publication writes and validates a temporary package before renaming it and
replacing the pointer. Replaying the same explicit inputs and link set reuses the
validated package and preserves its original manifest. Failed publications leave
previous packages available. This guarantees atomic file visibility for process
failures, not a cross-pointer transaction or a power-loss storage guarantee.

The manifest exposes:

```json
{
  "schema_version": "arena-provider-calendar-1.0",
  "provider": "pandascore",
  "data_kind": "api",
  "dataset_version": "<16 hex>",
  "generated_at": "<aware UTC publication time>",
  "as_of": "<latest response observation time>",
  "source_scope": {
    "kind": "match_phases",
    "competition_codes": [],
    "phases": ["past", "upcoming", "running"],
    "pages": 1
  },
  "counts": {
    "source_records": 1,
    "announcements": 1,
    "duplicate_occurrences": 0,
    "conflicted_announcements": 0,
    "pending_announcements": 0,
    "canonical_linked": 1,
    "sources": 3
  },
  "files": {"<required filename>": "<SHA-256>"},
  "publication_note": "<scope explanation>"
}
```

The deterministic dataset identity is
`sha256(canonical_utf8_json({announcements, occurrences, sources, quality}))[:16]`,
with `sort_keys=True`, separators `(',', ':')`, `ensure_ascii=False`, and
`allow_nan=False`. Arrays use deterministic producer order. Publication time is
excluded; the actual source observations and archive hashes are included. A new
successful poll can therefore have a new calendar version even when the
canonical match snapshot is unchanged. This records a fresh observation, rather
than rewriting when the previous poll knew a result.

An announcement contains these fields:

| Fields | Contract |
| --- | --- |
| `announcement_id`, `canonical_match_id` | `pandascore:<provider match ID>`; canonical link is nullable |
| `provider`, `sport`, `name`, `source_status` | PandaScore, VALORANT, nullable original name, and original provider status |
| `start_time`, `source_dates` | Nullable normalized aware kickoff; original begin/scheduled/original scheduled/end/modified dates |
| `team_a`, `team_b` | Nullable objects with nullable `team_id`, `source_team_id`, and `team_name` |
| `league`, `series`, `tournament` | Nullable metadata objects with scoped ID, source ID, names, original date fields, season, and year |
| `source_number_of_games`, `source_results`, `result_known` | Nullable supplied series format, supplied team scores, and conservative result confirmation |
| `observed_at`, `first_observed_at`, `last_observed_at` | Selected business variant's earliest observation, identity's first observation, and latest occurrence observation |
| `pending_reasons`, `conflicted`, `source_ids`, `occurrence_ids` | Explicit gaps or conflicts and full provenance linkage |

Team IDs use the same `pandascore:<team ID>` namespace as canonical matches.
Metadata IDs use `pandascore:league:<ID>`, `pandascore:series:<ID>`, and
`pandascore:tournament:<ID>`. No identity is created from a TBD display name.
All metadata fields are nullable; the absence of an event date is retained.
Empty optional display strings normalize to null, including a supplied series
whose `name` is empty while its `full_name` and ID are present. No name is copied
from another field as a fallback. The private source archive retains the literal
string and its checksum. Wrong types and oversized text still fail publication.
Source calendar dates and unzoned clocks are preserved exactly. Only an aware
`begin_at` produces `start_time`; a different `scheduled_at` does not silently
fill a missing `begin_at`. Missing UTC offsets produce an explicit gap.

`source_results` preserves only supplied values, including null scores or
upcoming zeros. `result_known` requires a `finished` source status, two distinct
resolved teams, and two supplied integer scores matching those team IDs. Upcoming
zeros, canceled matches, missing scores, and conflicting variants remain
unconfirmed. These are series scores, not per-map rounds, player statistics, or
derived betting labels. The calendar provides no prediction eligibility.

Every occurrence has the retained business fields plus `occurrence_id`,
`source_id`, and its response `observed_at`. Identical duplicates aggregate to one
announcement while keeping all occurrence references. Conflicting business
variants select the most recently observed representation for display, set
`conflicted=true`, add `conflicting_phase_records`, clear the canonical link, and
set `result_known=false`. The selected representation never inherits an earlier
variant's availability clock. Source rows still expose every conflicting value.

## Coverage and operational limits

This is a bounded REST fixture catalogue, not an exhaustive event calendar or a
real-time in-game round stream. Series or tournament names are provider metadata;
the publisher does not fabricate stages, brackets, venues, prize pools, rosters,
logos, or player statistics. Unknown statuses remain visible as `unknown_status`.
The free provider plan and a page limit can leave older or further upcoming
announcements outside the current scope.

Calendar packages and response archives are private runtime artifacts. They must
remain outside public fixture exports and Git until redistribution permissions
are separately established. Publication copies only reviewed fields; it never
copies response headers, tokens, stream URLs, or raw payload dumps.

The validator checks typed fields, file hashes, logical dataset identity, source
counts, provenance clocks, IDs, result rules, and aggregate/occurrence parity. It
does not prove provider authenticity: an unsigned private response archive is
trusted at the collector boundary. It also does not establish historical
pre-match availability before actual collection.

Old successful publications are retained for recovery. A continuously running
deployment needs a reviewed retention policy for private archives and calendar
versions; this module does not prune evidence automatically.

## Verification status

The initial tests use original synthetic fixture rows only. They cover nullable
announcements, canceled matches, unknown and unzoned dates, strict result types,
duplicate and conflicting phase IDs, complete page coverage, malformed envelopes,
path traversal, publication replay, failed writes, checksums, and resealed
semantic mutations. Actual API acquisition and coordinated worker/consumer
integration are separate verification steps. No live provider coverage claim is
made by these unit tests.
