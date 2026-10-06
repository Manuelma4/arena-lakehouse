# ADR 002: affected-key maintenance and gated publication

Date: 2026-10-06. Status: accepted for the local portfolio runner.

## Decision

Store source observations append-only, record an immutable stage job, and process
Bronze → affected-match Silver/history → affected-team Gold → independent oracle.
Gate Gold access and snapshot publication while any job is unfinished. Serialize
writers with an OS-held local file lock; persist attempt history in SQLite.

## Reason

A late result can change totals after the original match date. Incrementing a
counter from the new score risks double counting and mishandling cancellation.
Recomputing all current completed matches for the affected teams is easier to
audit while limiting writes to the impacted keys. It also removes a team's final
completed result via a soft-deleted aggregate, rather than leaving a stale row.

## Recovery model

Delta table commits are atomic individually, with no cross-table transaction.
A failure after Silver can temporarily leave Gold behind. Readers use immutable
published snapshots; the runner refuses new publication until all stages succeed
and agree with the independent reconstruction. Resume skips successful stages and
replays a stage whose commit may have succeeded before its attempt record.

## Tradeoffs

The local ledger is a single-host control plane, not a distributed coordinator.
An independent full reconstruction deliberately adds cost and collects the demo
dataset for verification. Gold maintenance is incremental by team key; full
reconstruction remains a verification operation. Demonstration timings do not
prove production throughput. Managed Databricks has a separate pipeline/Job
adapter and must receive its own acceptance evidence.
