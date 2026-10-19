# Source evidence companion 1.0

This optional companion lets Arena inspect a canonical snapshot's catalogue,
metric definitions, declared lineage, recorded quarantine aggregates and incident
groups without copying match rows or importing the producer into the consumer.
It does not change the canonical snapshot 2.0 contract or activate new sources.

The Current API preserves original clocks in `source` and additionally provides
`source_clocks`, with exactly `as_of`, `generated_at` and `packet_generated_at`.
These are derived UTC display instants normalized by the backend, not rewritten
packet members or a new observation. They accommodate aware ISO source forms
that the native Python reader accepts but JavaScript does not parse consistently.
This transport addition does not change either fingerprint or original hashes.
Browser text bounds count Unicode code points, matching the backend rather than
counting UTF-16 units. The browser checks transport structure and selected pins;
original-member checksum verification belongs to the independent backend reader.

The exporter publishes from an owned sibling staging directory and never replaces
an existing destination. Windows uses its no-replacement rename semantics;
Linux requires `renameat2(RENAME_NOREPLACE)`. Unsupported platforms or filesystem
capabilities fail closed. Windows execution is measured locally. The Linux
helper has simulated error/collision tests; real Linux execution remains pending.

## Exact inventory and binding

A packet contains exactly seven regular files: `manifest.json`,
`source-manifest.json`, `catalog.json`, `metrics.json`, `lineage.json`,
`quality.json` and `incidents.json`. The source manifest and five metadata members
retain their original bytes. They are not normalized or regenerated on export.
Links, Windows reparse points, traversal, unexpected members, duplicate JSON keys,
non-finite numbers and over-limit inputs are rejected.

Both implementations apply the same field bounds: nonblank names are limited to
120 characters, descriptions/formulas/remediation to 2,000, producer version to
80, and run ID to 120. Text rejects ASCII control characters. Counts are strict
integers in `0..2^53-1`; recorded failed/event counts start at one. Catalogue
columns number `1..128`, metric definitions and lineage edges `1..128`, and
quality/incident groups `0..128`. A source manifest declares at most 256 files.
Aware ISO timestamps contain `T` and use at most 40 characters. Incident order
and descriptions are preserved; matching uses unique rule/table/severity groups,
the native check digest, event counts and open status, never hardcoded prose.

Each metadata member is limited to 1 MiB, the source manifest to 2 MiB, and the
complete packet to 8 MiB. Original source snapshots still require their full
inventory, checksums, provenance and table/prediction contracts to pass before
export. A metadata-only companion does not prove the underlying Parquet is
present or acceptable at its destination; attachment to Current additionally
requires the already validated canonical capture.

The new manifest has exactly these fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | `arena-source-evidence-1.0` |
| `package_version` | First 16 hexadecimal characters of the package fingerprint |
| `package_fingerprint` | SHA-256 of canonical `{schema_version, source, files}` |
| `generated_at` | Aware UTC publication instant; not a new source observation |
| `source` | Original source identity and clocks, described below |
| `files` | Exact six member names mapped to SHA-256 of their original bytes |

Canonical JSON uses UTF-8, sorted keys, compact separators, unescaped Unicode and
no non-finite values. The package facts identity excludes its publication clock.
Existing destinations are reused only when every packet byte matches; changed
bytes require a new destination. The writer publishes through an owned sibling
staging directory and never overwrites or nests output within source inputs.

`source` contains exactly `schema_version`, `dataset_version`, `producer_version`,
`data_kind`, `providers`, `generated_at`, `as_of`, `snapshot_fingerprint` and
`manifest_sha256`. Its schema version is `2.0`; kinds and providers retain their
canonical meanings. The snapshot fingerprint is SHA-256 of canonical
`{schema_version, producer_version, files}` from the source manifest. The manifest
hash covers the exact `source-manifest.json` bytes. For compatibility with the
canonical snapshot 2.0 reader, snapshot fingerprint serialization escapes
non-ASCII characters (`ensure_ascii=True`); the new companion package identity
uses unescaped Unicode (`ensure_ascii=False`). Every mirrored metadata hash
must also equal the corresponding original source-manifest entry.

Clocks satisfy `source.as_of <= source.generated_at <= packet.generated_at <= now`.
Clock comparisons use instants, including UTC offsets; original bytes stay
unchanged. Checksums detect modification, not publisher identity or authenticity.

## Aggregate metadata

Unknown keys and invalid exact types are rejected. Counts are nonnegative safe
integers, excluding booleans. Text is bounded and excludes control characters.

- Catalogue: canonical Gold `matches` and `team_stats`, plus optional
  `match_history`, with unique columns and primary keys. Export verifies these
  declarations against the actual source Parquet schemas. Descriptions are
  declared metadata, not operational measurements.
- Metrics: bounded definitions with name, description, unit, table and formula.
  Tables must exist in the catalogue. Formulas are descriptions and are never
  executed as SQL or code.
- Lineage: bounded declared source/target/transformation edges. Catalogue tables
  and the canonical virtual nodes `provider_api_or_replay`, `bronze_events` and
  `predictions` are allowed; duplicate edges and cycles are rejected. This is
  declared lineage, not proof of a particular engine execution.
- Quality: run ID, checks and summary. A published source requires a succeeded
  ingest. Each recorded failure group has passed count equal to retained valid
  events; failed counts sum to quarantined observations. These are cumulative
  retained workspace aggregates, not a last-poll acceptance rate.
- Incidents: one open quarantine group per recorded failed check, with the same
  rule, table, severity and count. Its ID is the first 16 characters of SHA-256 of
  the canonical check. Descriptions and suggested remediation remain source
  metadata, not executable instructions.

An empty checks/incident list means no recorded quarantine groups. It does not
establish complete rule coverage, completeness of provider data, or a successful
evaluation of every possible quality rule. Successful source polling is separate
from both data quality and match-level coverage.

## Current attachment and evidence tools

The consumer requires an expected snapshot fingerprint. Current API attachment
also pins the selected publication and exact original manifest hash, and compares
the source manifest to the validated capture. A changed selection returns a
conflict; unavailable or malformed evidence never borrows another dataset.

Fixed read-only tools expose catalogue, metric definitions, declared lineage,
quality and incident groups. Optional table selection uses exact known names.
No tool accepts SQL or writes to the lakehouse. Composition keeps observed facts,
coverage limits and reviewable recommendations separate. Source-check freshness
uses captured collection telemetry; generating this companion cannot make a
stale provider collection fresh.

This backend increment adds stateless deterministic evidence composition. It does
not claim a new generative-model evaluation, durable investigation workflow,
automated remediation, measured SLO compliance or managed Databricks deployment.
Those require separate evidence and acceptance.
