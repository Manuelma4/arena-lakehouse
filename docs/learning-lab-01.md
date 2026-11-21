# Lab 01 — From changing match observations to defensible predictions

## 1. Ingestion: separate observation from event time

With the CLI installed in the [reference environment](development.md), start a
fresh synthetic workspace from the repository root. Keep this shell open for
the replay:

```sh
workspace="$(mktemp -d "${TMPDIR:-/tmp}/arena-lab-01.XXXXXX")"
arena-lakehouse --workspace "$workspace" demo --matches 240 --seed 42
snapshot="$workspace/snapshots/$(cat "$workspace/snapshots/LATEST")"
grep -F 'synthetic:football:000000' "$snapshot/bronze/events.jsonl"
```

Compare `start_time`, `observed_at` and `revision` for the two revisions. Explain
why the revision selects current state and observation time limits features.
The [generator](../src/arena_lakehouse/synthetic.py) shuffles delivery order.
The exported Bronze file contains accepted observations sorted by match ID and
revision, so use it to compare revisions.

## 2. Correctness: make replay boring

Replay the accepted observations into the same workspace:

```sh
arena-lakehouse --workspace "$workspace" replay "$snapshot/bronze/events.jsonl"
```

Expect `accepted: 0` and `quarantined: 0`, with one duplicate per input row.
The replay records its own `run_id` and leaves the selected snapshot in place.
Inspect `quality.json` from the original demo export to see the warnings from
that ingest. Invalid demo records contribute to those warnings and are excluded
from the accepted Bronze replay input.

## 3. Streaming: persistence is observable

Run the Docker smoke. Inspect `smoke-report.json`, Delta `_delta_log` folders and
the checkpoint directory. The second AvailableNow run consumes no new events.
The new correction file changes the match revision and its team aggregates.
CDF contains actual Delta change records; upstream API polling is a different
mechanism and is not source-database CDC.

## 4. Prediction: draw a timeline before choosing a model

Read the model report. Identify the last training kickoff, first validation
kickoff and first test kickoff. Why can training labels also leak if their
results were unavailable at fit time? Explain the future-result feature test.

Compare model vs frequency baseline using accuracy AND log loss. Describe a case
where the most likely outcome is right but the predicted distribution is poor.
Synthetic measurements do not answer whether a model predicts real VCT teams.

## 5. Integration: publish a complete snapshot

Read the manifest. Verify one file checksum manually. Change a copied file, then
run `arena-copilot --snapshot <copy> validate`; it must reject the copy before
querying or generating an answer. No partial producer output should become active.

## Done when

You can explain one duplicate, one invalid record, one late correction, one
point-in-time cutoff and one honest model limitation using the actual artifacts.
