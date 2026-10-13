# Rejected provider rows

A malformed canonical match rejects the complete collection candidate. The worker
now keeps a private, durable pointer to the first rejected row so an operator can
inspect the retained response without repeating provider requests.

The incident contains the provider, fixed endpoint, relative archive path, exact
archive SHA-256, zero-based row index and `invalid_match_event` category. It does
not contain validation messages, team names, request headers or credentials.
The archive preserves parsed, unnormalized JSON; it is not the HTTP wire body.

## Failure and recovery behavior

The collector carries each row's archive provenance through pagination and phase
flattening. Football indices refer to `payload.matches`; PandaScore indices
refer to the array in that individual phase/page response. Final canonical
validation still raises immediately. No accumulated row or pending cursor from
that collection reaches ingestion. A failure in a later football competition
also discards the earlier competition candidates from the same poll.

The poll keeps its existing `invalid_provider_payload` error code. Updating the
failed poll and inserting its incident use one SQLite transaction. Incident
updates and deletion are refused. A private lookup by poll ID works after a new
worker instance opens the ledger. A later successful collection can publish
normally while the earlier incident remains available.

The public `arena-live-status-1.0` contract is unchanged. There is one incident
per failed poll because collection remains fail-fast. Normalizer structural
errors and calendar-validation failures retain their previous generic errors;
this change covers final `MatchEvent` rejection only. PandaScore's valid broader
calendar facts retain their existing canonical-eligibility fallback.

## Measured offline incident

The retained football poll 10 failed on BSA row 298, marked finished without
complete scores. An isolated two-poll replay first loaded all 2,132 rows from
the preceding successful capture, then replayed the six failed-capture responses.
The second candidate failed and retained the expected row pointer. The selector,
all six cursor files and pipeline events stayed identical to the seeded state;
the incident survived a new worker instance. All 216 files in the original live
workspace were checked unchanged. Actual provider requests and model calls: zero.

The replay has its own controlled observation clocks and newly serialized archive
bytes. Its new archive checksum therefore differs from the original capture.
The original poll was not changed, backdated or retrospectively assigned an
incident. This demonstrates failure diagnostics, not an upstream fix or a
successful refresh. Current still serves the last verified football publication
with degraded/stale health.

Twenty focused fixture cases cover missing/invalid scores, endpoint-local
indices, whole-candidate rejection, restart lookup, immutable history, insertion
rollback, unrelated terminal statuses and bounded provenance. The first runtime
attempt exposed an incorrect SQL placeholder count; it was corrected, with the
failed report retained separately. The wider regression and exact source/report
pins are in [portable evidence](evidence/provider-row-rejections-2026-10-13.json).

Raw responses and detailed replay reports remain in the private continuation
snapshot. No rows are silently skipped or assigned invented scores. Result
retractions and any different quarantine policy require a separate design and
tests before changing publication behavior.
