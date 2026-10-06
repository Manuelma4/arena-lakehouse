# Lab 01 — From changing match observations to defensible predictions

## 1. Ingestion: separate observation from event time

Run the offline demo. Open two revisions of the same match in
`bronze/events.jsonl`. The scheduled kickoff is not the time the result became
known. Find an event delivered before its older revision in the generated input.
Explain why `revision` chooses current state while `observed_at` limits features.

## 2. Correctness: make replay boring

Run `replay` on the canonical JSONL file. Accepted observations should be zero;
current matches and aggregates stay unchanged. Then read the quarantine summary.
An invalid payload cannot silently count as a finished match.

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
