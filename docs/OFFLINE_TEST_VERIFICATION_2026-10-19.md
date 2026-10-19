# Offline regression verification

Two local Windows default suites completed against the approved regulation
checkpoints, with explicit metadata fixtures and the initial offline boundary:

| Repository | Source checkpoint | Passed | Skipped |
| --- | --- | ---: | ---: |
| Arena Copilot | `a9b6899` | 2,176 | 9 |
| Arena Lakehouse | `fae7790` | 774 | 1 |

Every listed omission requires Windows symbolic-link privileges. No host
permissions were changed. Both suites recorded ten intentional boundary
rejections and zero unexpected runtime-attributed rejections. Their elapsed
times were 1,849.57 and 1,425.19 seconds during concurrent local runs; these are
neither application latency measurements nor estimates of CI cost.

The [machine-readable record](evidence/offline-baseline-suites-2026-10-19.json)
retains versions, raw-report hashes, exact omissions and source-tree identities.
Each repository retains its initial harness/test patch beside the report. Those
patches were checked against temporary Git indexes at their stated source bases;
the normal checkout and index were untouched. Patch files use LF for portability.

The initial harness's JSON accounts for runtime-attributed operations. The later
version additionally accounts for collection and late/unattributed operations,
and rejects swallowed attempts. That final harness has separate focused evidence
in the [source delivery report](evidence/source-evidence-delivery-2026-10-19.json).
The report does not claim the final phase-accounting harness or additive source
feature was included in the earlier default runs. Affected source checks and
final-harness scopes overlap those baseline suites; no unique aggregate is used.

These are software and fixture results. They establish no new provider access,
real-model answer quality, native Spark scale, managed Databricks acceptance or
whole-site accessibility certification. See [the boundary design](offline-tests.md).
