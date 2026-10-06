# Optional Databricks profile

**Status: implemented configuration, pending workspace verification.** Local
Spark/Delta evidence does not certify Auto Loader, managed expectations, AUTO CDC
or Unity Catalog behavior. No workspace was authenticated, created or deployed
while preparing this profile. No schedule is defined.

`databricks.yml` defines a configurable serverless Spark Declarative Pipeline and
a two-task Lakeflow Job: update layers, then verify Gold. Source files are synced;
local data, environment files and virtual environments are excluded. The bundle
does not create a catalog, schema, source volume, credentials or paid API account.

## Prepare a workspace

1. Use a dedicated existing Unity Catalog catalog/schema and a Volume with
   permission to read/write demo data. Confirm your account permits serverless
   pipelines, expectations and AUTO CDC; free-edition capabilities/quotas vary.
2. Upload **validated canonical observations**, for example the original demo's
   `bronze/events.jsonl`, into a private directory in that Volume. Keep provider
   archives/tokens out of Git and do not upload unnormalized API payloads here.
   New files must contain new accepted source versions, rather than repeatedly
   uploading a complete snapshot with ingestion timestamps on duplicate versions.
3. Install the current official Databricks CLI and authenticate with an appropriate
   workspace profile. Configuration is supplied locally, not committed.

```bash
databricks bundle validate -t dev --var 'catalog=main,schema=arena_demo,source_volume_path=/Volumes/main/arena_demo/landing/observations'
databricks bundle deploy -t dev --var 'catalog=main,schema=arena_demo,source_volume_path=/Volumes/main/arena_demo/landing/observations'
databricks bundle run -t dev arena_verified_refresh --var 'catalog=main,schema=arena_demo,source_volume_path=/Volumes/main/arena_demo/landing/observations'
```

Deploy/run commands are optional manual actions. Account quotas and compute terms
apply; there is no claim of universally free managed execution. Local execution
remains complete without this profile.

## Managed graph

`Auto Loader → Bronze → row quarantine / valid view → AUTO CDC SCD2 → Gold matches
→ Gold team statistics → Job contract verification`

Bronze uses an explicit JSON schema and rescued-data capture. Row expectations
reject malformed IDs, unsupported enums, invalid scores, missing UTC timestamps
and incompatible finished-series results. Invalid rows remain inspectable in
quarantine. AUTO CDC orders observations by `(revision, observed_at)` and retains
cancellation as a state rather than dropping its explanatory history. Gold team
statistics reuse `aggregate_teams`, the same transformation checked in the actual
local incremental scenario. Consumers use Gold rather than reading Bronze/Silver.

Cross-revision identity changes, duplicate-version ambiguity and provider retry
normalization are the upstream canonical validator's responsibility. Managed
row expectations alone cannot establish these cross-row guarantees. This adapter
is not the independently audited local stage runner transplanted into a workspace.
AUTO CDC's managed history sequence fields also differ from the local snapshot's
observation-time `valid_from`/`valid_to` contract; no automatic managed snapshot
export is advertised yet. Team roster history is a separate future extension.

## Required workspace acceptance report

- Capture CLI/runtime version, catalog, schema, pipeline update ID and Job run ID.
- Reprocess identical files; confirm no extra current match rows or altered metrics.
- Add a valid late score correction, cancelled sole result, postponed kickoff and
  stable-ID team rename; compare Gold with the independent Python reconstruction.
- Add an invalid row; verify quarantine and expectation metrics in the event log.
- Fail the verification task and exercise Job repair without full refresh.
- Check permissions, serverless quotas and retention; record any unavailable
  managed feature rather than substituting a local success report.

Record the report under `docs/evidence` before changing status to **verified in
Databricks**. Syntax/schema validation is a separate weaker evidence category.

Official references: [pipeline Python development](https://docs.databricks.com/aws/en/ldp/developer/python-dev),
[AUTO CDC](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes),
[bundle resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources),
[Job task configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types).

## Offline configuration evidence

The YAML was checked against `databricks bundle schema` emitted by official CLI
1.19.0, without authentication. See
[`databricks-bundle-schema.json`](evidence/databricks-bundle-schema.json).
Reproduce this weaker shape check separately from authenticated validation:

```bash
databricks bundle schema > data/bundle-schema.json
python scripts/validate_bundle.py --schema data/bundle-schema.json --cli-version 1.19.0
```
