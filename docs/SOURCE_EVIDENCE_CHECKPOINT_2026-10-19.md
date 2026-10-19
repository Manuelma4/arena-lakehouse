# Earlier isolated backend checkpoint (historical)

The owner subsequently approved the integrated inspector. The current delivery
is recorded in [the approved source-evidence increment](SOURCE_EVIDENCE_DELIVERY_2026-10-19.md).
The entries below preserve the earlier isolated-feature status.

---

# Source evidence integration checkpoint

This increment is implemented in two isolated `feature/source-evidence`
worktrees. It is not committed, pushed, merged into the reviewed checkpoint or
enabled in the user's local preview. The regulation-team statistics/chat
increment was subsequently approved and pushed as consumer `a9b6899` and producer
`fae7790`. That approval excludes these source-evidence worktrees. The later
consumer `docs/SOURCE_EVIDENCE_UI_2026-10-19.md` records the new inspector and its
independent local verification; the earlier backend report retains its own scope.

## Result

The producer exports an optional seven-file companion from a fully checked
canonical snapshot. The original manifest, catalogue, metric definitions,
declared lineage, quality aggregates and incident groups retain their bytes.
The independent consumer binds those files to the exact selected Current
snapshot and publication, then exposes five fixed read tools with member hashes.
The shared [contract](SOURCE_EVIDENCE_CONTRACT.md) defines bounds and meanings.

The optional API adds `GET /live/source-evidence` and
`POST /live/source-evidence/investigate`, also under `/api`. Both require provider,
snapshot and publication pins. The POST composes a fixed evidence summary with
facts, citations, limitations and reviewer recommendations. It is stateless and
deterministic; the question is retained as context, not interpreted by a model.
It does not claim to answer arbitrary questions or create a saved investigation.

No metadata files are automatically selected, exported or fetched. A configured
consumer reads packets from an evidence root, then a provider directory and a
snapshot-fingerprint directory. The root is configured by
`ARENA_SOURCE_EVIDENCE_WORKSPACE` or application argument `source_evidence_directory`.
The producer module exposes `export_source_evidence(snapshot, output,
generated_at=...)` and an explicit local-file CLI. These are separate feature
interfaces; the existing preview is not configured to use them.

## Measured integration

The consumer's `scripts/audit_source_evidence_integration.py` verifies the
producer and consumer together, with module paths pinned to these worktrees.
It uses a generated fault fixture and both already retained Current providers.
No provider collection, national Current activation or model operation is part
of the audit. Exported packets and API test state live in a temporary directory.

The [machine-readable report](evidence/source-evidence-integration-2026-10-19.json)
records exact source/publication identities, original file inventories,
quarantine arithmetic, immutable member bytes, checksum rejection, stale-data
semantics and root/`/api` response checks. Unit-test and revision scope are
recorded separately in `evidence/source-evidence-local-2026-10-19.json`.

Quality groups describe cumulative quarantined observations; an empty list
does not prove every rule passed or that coverage is complete. Declared lineage
does not prove runtime execution. Re-exporting a packet does not freshen the
provider collection. Checksums detect modification without authenticating the
publisher. Real Linux atomic publication and Databricks verification remain
pending; native Windows execution and simulated Linux collisions are separate.

## Continuation

1. Preserve the approved original checkpoint and its live preview. The owner's
   regulation statistics/chat approval does not include this source-evidence UI.
2. Review this independent source-evidence increment, then apply its additive
   API wiring on top of that checkpoint. Recheck conflicts and changed behavior
   after integration; current evidence describes the isolated feature base.
3. Prepare retained metadata packets explicitly and configure their local
   evidence root. Do not equate this with permission to poll providers or enable
   a new dataset. Test unavailable packets and changed source/publication pins.
4. Review the subsequently implemented source-evidence inspector as a separate
   increment. Durable source investigations and language-model integration remain
   unimplemented here; the inspector performs fixed read-only GETs.
5. Commit/push only the reviewed, authorized checkpoint. Keep original fixtures,
   private captures, credentials, model settings and read-only data intact.

No README images or execution instructions, Actions dispatch, public deployment,
main merge, release tag, paid service or Docker/Spark retry was added.
