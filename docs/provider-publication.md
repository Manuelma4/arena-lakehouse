# Atomic provider publication contract

The private `arena-provider-publication-1.0` selector lives at
`<base>/<provider>/snapshots/CURRENT_PUBLICATION.json`. One atomic file replacement
selects the canonical snapshot and announcement calendar together. Immutable
packages and the SQLite ledger remain separate files; the protocol supports
process interruption and replay under the existing provider writer lock.

## Explicit wire record

Every field below is required, including null descriptors. Unknown fields are
rejected. `provider` is `football-data` or `pandascore`; `data_kind` is `api`.
`poll_id` is the durable SQLite poll integer from 1 through `2^63-1`.

```json
{
  "schema_version": "arena-provider-publication-1.0",
  "provider": "pandascore",
  "data_kind": "api",
  "poll_id": 1,
  "source_scope": {
    "kind": "match_phases",
    "competition_codes": [],
    "phases": ["past", "upcoming", "running"],
    "pages": 1
  },
  "collected_at": "2026-10-06T12:00:00.000000Z",
  "selected_at": "2026-10-06T12:00:01.000000Z",
  "canonical": null,
  "calendar": {
    "basename": "calendar-0123456789abcdef",
    "dataset_version": "0123456789abcdef",
    "manifest_sha256": "<64 lowercase hexadecimal characters>"
  },
  "fingerprint": "<64 lowercase hexadecimal characters>"
}
```

Canonical descriptors have the same three fields and a
`snapshot-<16 lowercase hex>-<8 lowercase hex>` basename under `snapshots`.
Calendar basenames are `calendar-<16 lowercase hex>` under sibling `calendars`.
The version in each basename must match its manifest and descriptor. Paths are
basenames, never arbitrary paths. `manifest_sha256` hashes exact manifest bytes.

The fingerprint is SHA-256 of every field except `fingerprint`, serialized as
sorted, compact UTF-8 JSON with `ensure_ascii=False`, separators `(',', ':')`,
and `allow_nan=False`. Duplicate keys, bool poll IDs, unknown fields, unsafe
names, links/reparse points, oversized inputs, and checksum failures are rejected.

Football scope is `competition_list`, with a unique nonempty list of 1–10 ASCII
uppercase alphanumeric competition codes of at most 12 characters, `phases=[]`,
and `pages=1`. Its calendar is always null. PandaScore scope is `match_phases`,
with `competition_codes=[]`, exactly the ordered phases above, and 1–10 pages.
Every successful PandaScore poll selects a complete validated calendar, including
empty and wholly unresolved collections. A canonical descriptor may be null when
no canonical observations have ever been accepted.

`collected_at` is the aware UTC clock after all configured responses and canonical
normalization complete. It does not claim historical availability. `selected_at`
is the clock read immediately before persisting the intended selector and its
first atomic swap, after package validation and cursor commits. Both use UTC
microseconds and `Z`. Package observation cutoffs are at or before
`collected_at`; package preparation clocks are at or before `selected_at`.

## Preparation, commit, and recovery

The worker performs these steps while holding the provider writer lock:

1. Recover any outstanding prepared intent before beginning another poll or
   making a provider request. Reject a corrupt already-selected pair before
   another collection attempt.
2. Collect the complete configured scope. For PandaScore, validate the entire
   bounded calendar payload before producing canonical observations or cursor
   revisions. Retain only the explicit successful collector archive references;
   raw directory glob discovery is prohibited.
3. Ingest canonical observations and prepare both packages with
   `select_latest=False`. Existing standalone snapshot and calendar APIs preserve
   `select_latest=True` defaults. No legacy pointer advances during preparation.
4. Validate complete package inventories, exact checksums, typed observations,
   canonical revision chains, Gold schema/parity, calendar semantics/provenance,
   and cross-package identity links. Persist a bounded, checksummed SQLite intent
   containing poll ID, exact archive references/hashes, complete cursor states,
   metrics, descriptors, and the previous selector fingerprint.
5. Commit all cursor states idempotently and flush their bytes before selection.
   Persist the proposed selector in the intent, then replace the authoritative
   selector atomically.
6. Complete the same poll in the ledger, update legacy pointers and status, then
   acknowledge the intent. Compatibility views may lag the selector after a
   crash; new readers select only through the authoritative record.

Before a durable intent exists, an interrupted attempt is recorded as interrupted
and a subsequent poll can collect again. Accepted event identities remain
idempotent. Once an intent exists, recovery uses its original poll ID, archives,
metrics, cursor states, and packages. It completes partially committed cursors
without recollection or a new business revision.

The union of all scope cursor states must cover every selected canonical match
identity. Each entry must bind the selected revision and original connector hash
through its event ID. Empty or truncated cursor state cannot acknowledge a
nonempty canonical package; duplicate coverage across football scopes does not
replace missing identities.

If the intended poll has not been selected, recovery refreshes `selected_at`
immediately before its swap. If it has already been selected, recovery requires
exact agreement with the durable intended selector and preserves its bytes,
fingerprint, and `selected_at`. A crash after selection but before ledger, status,
or compatibility-pointer completion therefore converges on the same publication.
A completed ledger success is not downgraded by a later status write failure.
Tampered recovery evidence remains failed and pending, with no new provider call.

## Canonical history and announcement nulls

Canonical snapshots retain accepted historical observations, even when a current
announcement for the same ID has become unresolved or has unknown fields. A
calendar link is validated against the existing selected canonical identity;
current team/date/result fields need not equal that historic record. The producer
never fills calendar nulls from canonical history. A link with no selected
canonical identity fails validation, even when both packages are internally
consistent and rehashed.

Conflicting same-ID records within a complete PandaScore poll remain calendar
occurrences and produce a conflicted announcement. They do not produce canonical
revisions at one shared collection clock or change that ID's retained cursor.
A later unambiguous poll can then append one ordinary next revision without
quarantining the prior chain.

Valid calendar fields can exceed the strict match contract. Date-only or unzoned
kickoffs, unresolved teams/names, missing format or tournament metadata, Player
opponents, formats through Best of 101, and source scores through 1,000 remain
calendar evidence and count as unsupported canonical rows. Existing strict
history and cursors are retained. The collector still rejects malformed calendar
fields, such as negative/fractional scores, boolean identities, invalid dates, or
wrong metadata types, before any canonical ingest; it does not silently relabel
malformed input as unsupported.

`LATEST`, `CALENDAR_LATEST`, and `LIVE_STATUS.json` are compatibility views. The
status schema remains `arena-live-status-1.0`; it does not carry the pair's poll ID.
Those independent views cannot establish collection alignment for new readers.
The protocol supplies atomic selector visibility and process-crash recovery,
not provider authenticity, a filesystem-wide transaction, or a guarantee against
storage hardware/power loss.

## Verification

Synthetic tests cover complete empty and unresolved PandaScore collections,
retained canonical records with nullable same-ID announcements, partial phase
failure, partial multi-competition cursor commits, crashes around intent,
cursor, selector, ledger, compatibility, status, and acknowledgement boundaries,
exact archive/intent/manifest tampering, cross-package identity rejection,
selection-clock refresh and preservation, and standalone compatibility defaults.
They make no live provider, model, or Spark calls.
