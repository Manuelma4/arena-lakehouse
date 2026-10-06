# Arena snapshot contract 2.0

This versioned sports contract defines the producer/consumer interface.
Snapshots are immutable directories activated only after validation. No consumer
needs the producer package installed.

## Manifest

`manifest.json`: `schema_version` (`2.0`), `dataset_version` (16 hexadecimal
characters), `producer_version`, `generated_at`, `as_of`, `data_kind`
(`synthetic` or `api`), `providers`, and `files` (relative POSIX path to SHA-256).
Every regular file except the manifest is covered. Reject unknown versions,
missing/extra files, symlinks, path traversal and invalid payloads. Checksums
detect modification; they are not a cryptographic proof of publisher identity.

## Required files

- `catalog.json`, `metrics.json`, `lineage.json`, `quality.json`, `incidents.json`
- `predictions.json`, `model_report.json`, `docs/*.md`
- `gold/matches.parquet`, `gold/team_stats.parquet`

Optional producer evidence includes `gold/match_history.parquet` and canonical
`bronze/events.jsonl`, also covered by checksums.

## Match meaning

Provider-scoped IDs prevent identity collisions. A match has a sport, competition,
source revision, two distinct teams, scheduled start, observation time, optional
completion time, status, optional scores and best-of. All timestamps are UTC.
Source revision is independent of arrival order. A finished match requires its
outcome to be known no earlier than completion. Football scores are goals;
VALORANT scores are maps won, not rounds, kills or player performance.

Team statistics count current completed matches. Draws count in the football
win-rate denominator but not the numerator. They never occur in a completed
VALORANT series. Cancelling/correcting a match removes/replaces its contribution.

## Predictions

Predictions identify match, sport, model, data kind, observation cutoff and feature
evidence. Football classes: `a`, `b`, `draw`; VALORANT classes: `a`, `b`.
Probabilities must be finite, within [0,1] and sum to one. Synthetic and real data
cannot share a training workspace. No result may be a probability fabricated by
the chatbot. Insufficient point-in-time history means unavailable.

## Publication and privacy

Publication follows a successful ingest only, builds a staging directory, then
renames it atomically before changing `LATEST`. Old snapshots stay intact.
No API token or private player identifier enters an export. API-derived runtime
files are ignored by Git. MIT covers original code and fictional fixtures, not
provider databases, logos or downloaded model weights.
