# OpenFootball research publication

This publisher turns a verified [precision-aware staging export](openfootball-staging.md)
into an immutable research package. It uses the distinct contract
`openfootball-research-1.0`, never the canonical Arena snapshot 2.0 contract.
Rows remain open data with unresolved kickoff timezone, name-derived identities
and prediction ineligibility. No standings, points or inferred cup winner are
published.

## Run and module API

```bash
python -m arena_lakehouse.openfootball_research \
  --staging data/openfootball-staging-nine-001 \
  --output data/openfootball-research-nine-001
```

The source must be an existing staging export; this command makes no HTTP
requests. The destination must be new. Validation completes before destination
creation. Files are prepared in a temporary sibling and published by rename;
failure removes only the temporary files created by this publisher.

```python
from arena_lakehouse.openfootball_research import validate_staging, publish_research

verified = validate_staging(staging_directory)
manifest = publish_research(staging_directory, new_destination)
```

`validate_staging(Path)` returns `ValidatedStaging` with `manifest`,
`upstream_staging_fingerprint`, `matches`, `history`, `sources`, `quarantine`,
`polls` and `as_of`. `publish_research(Path, Path, *, generated_at=None)` returns
the published manifest; an explicit generation clock must be timezone-aware and
cannot precede source observations. Validation errors are `ResearchValidationError`.

The reviewed source catalogue ships inside the Python wheel at
`arena_lakehouse/resources/openfootball_sources.json`. It exactly mirrors
`config/openfootball_sources.json`; an offline regression checks their bytes.
Publisher validation and the collector's default source path work without a
repository checkout. Namespace/alias migration remains a separate reviewed task.

## Manifest and files

Manifest fields are:

| Field | Meaning |
| --- | --- |
| `research_version` | `openfootball-research-1.0`. |
| `data_kind`, `provider`, `license`, `canonical_compatible` | `open_data`, `openfootball`, `CC0-1.0`, `false`. |
| `dataset_version` | First 16 hexadecimal characters of the deterministic logical-content SHA-256 described below. |
| `generated_at`, `as_of` | Publication UTC clock and maximum actual UTC observation of successful staged polls. These do not resolve kickoff precision. |
| `matches`, `revisions`, `quarantine_rows`, `sources` | Actual row counts. |
| `files` | Exact relative filename to SHA-256 map covering every package file except the manifest. |
| `upstream_staging_fingerprint` | SHA-256 of the original staging manifest bytes; a provenance fingerprint, not proof of publisher identity. |
| `precision_note`, `coverage_note` | Explicit unresolved time precision, unknown/stale results and observed coverage limitations. |

Required files:

```text
manifest.json
gold/matches.parquet
gold/match_history.parquet
matches.json
history.json
sources.json
competitions.json
quality.json
lineage.json
quarantine.json
polls.json
```

Current/history rows preserve **every staging field** and add
`edition_id=source_id`. Typed Parquet matches the JSON rows exactly. Original
calendar dates, optional local times, names, score fields, observation timestamps
and provenance are retained. `timezone`, `start_time` and `completed_at` remain
null; `prediction_eligible` remains false.

Each `competitions.json` row contains `edition_id`, `competition_id`, `name`,
`season`, `region`, `coverage_start`, `coverage_end`, `match_count`,
`observed_results`, `unknown_results`, `date_only`, `local_minute` and
`participants: [{team_id, team_name}]`. Coverage dates use source calendar days.
Participants use the latest observed display name for each stable derived ID and
are sorted by ID. A source with no accepted matches remains an edition with null
coverage and an empty participant list.

`quality.json` is an object with measured `match_count`, `revision_count`,
`source_count`, `observed_results`, `unknown_results`, `date_only`, `local_minute`,
`unresolved_timezones`, `unresolved_team_identities`, `prediction_ineligible`,
`quarantine_rows`, plus `scope` and explanatory `notes`. The identity count refers
to distinct current derived team IDs. A result score does not establish a
90-minute model target. `lineage.json` is an array of `{source,target,description}`.
Source configuration rows, poll audit and quarantine evidence are preserved.

## Deterministic identity

The consumer can independently calculate:

```python
logical = {
    "as_of": manifest["as_of"],
    "matches": matches,
    "history": history,
    "sources": sources,
    "competitions": competitions,
    "quality": quality,
    "lineage": lineage,
    "quarantine": quarantine,
    "polls": polls,
}
encoded = json.dumps(logical, sort_keys=True, separators=(",", ":"),
                     ensure_ascii=False, allow_nan=False).encode("utf-8")
dataset_version = hashlib.sha256(encoded).hexdigest()[:16]
```

Array order is fixed: matches by match ID; history by match ID/revision; sources
by source ID; competitions by edition ID; participants by team ID; quarantine
by source ID/body checksum/row index/reason; polls by poll ID. Lineage order and
quality notes follow the publisher contract. Publication clocks, original staging
generation clock, upstream manifest fingerprint and Parquet encoding metadata
are excluded from logical identity. Publishing unchanged staged evidence twice
preserves dataset version and file checksums, while publication provenance can
change.

## Validation and measured evidence

The publisher checks the full hash inventory, rejects extra files/symlinks, and
parses exactly the bytes whose hashes were checked. It verifies JSON/typed
Parquet parity, the fixed nine-source GitHub catalogue, CC0 provenance, source
scope and derived team/match/event IDs, contiguous revisions, unchanged-content
idempotency, current=latest history, UTC availability and source poll evidence.
Poll change counts must equal revision counts; 304 polls require a previous
collected body checksum. Quarantine needs matching observed poll evidence.

The local research scope is bounded to 25,000 current rows, 50,000 revisions,
nine sources, 100,000 quarantine/poll rows, 128 MB per file and 512 MB per staged
package. This publication scope is separate from Spark benchmark goals.

The nine-source staged export collected on 6 October 2026 published successfully:
2,727 current rows, 2,727 revisions, nine editions and zero quarantine.
Dataset version: `5f99ba32f796fb23`; observation cutoff:
`2026-10-06T11:06:31.626840Z`. Measured current coverage contains 1,088 observed
results, 1,639 unknown results, 1,042 date-only rows, 1,685 unzoned local-minute
rows and 213 distinct derived team IDs. All 2,727 rows remain prediction-ineligible.
Runtime packages stay under ignored `data/`; the publisher did not fetch them.

Offline tests cover tampering despite recomputed file hashes, precision and cup
score preservation, parity, latest-history equality, measured edition coverage,
poll/quarantine availability, immutable destinations, replay identity and packaged
catalogue/alias normalization. A built wheel was also loaded using Python `-I`
from an isolated directory; validation/publication of all nine sources preserved
the same dataset version, and offline collector validation created no workspace.
Build dependencies used a temporary isolated build environment; the shared Python
installation was not modified.
