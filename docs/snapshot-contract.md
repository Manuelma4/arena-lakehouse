# Arena snapshot contract 2.0

This unreleased sports contract replaces the initial commerce-domain sketch.
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
renames it atomically. `export_snapshot(..., select_latest=True)` preserves the
standalone default of advancing `LATEST`; `select_latest=False` prepares a
candidate package. Provider workers select candidates through the atomic
[provider publication contract](provider-publication.md) before updating legacy
pointers. Old snapshots stay intact.
No API token or private player identifier enters an export. API-derived runtime
files are ignored by Git. MIT covers original code and fictional fixtures, not
provider databases, logos or downloaded model weights.

## Compare two producer environments on one synthetic workspace

The comparison uses the Arena producer at
[`6e721ae`](https://github.com/Manuelma4/arena-lakehouse/commit/6e721aedd5d5e248f8ddad97e161f8079dce8844)
and the independent Copilot validator at
[`dfed889`](https://github.com/Manuelma4/lakehouse-copilot/commit/dfed889eabc7b7b067b81d8e96f01d67de47c8dc).
Prepare both checkouts at the exact commits. Set `ARENA_PRODUCER` and
`ARENA_CONSUMER` to their absolute paths, and `ARENA_PY311` and `ARENA_PY313`
to the absolute paths of the Python executables in two isolated environments.

The tested macOS arm64 pair used Python 3.11.15 and 3.13.18. In both environments
install the following version pins. They cover the producer's synthetic
forecast path and the validator's package checks.

| Dependency | Version | Dependency | Version |
| --- | --- | --- | --- |
| PyArrow | 23.0.1 | Pydantic | 2.14.3 |
| pydantic-core | 2.50.4 | NumPy | 2.4.6 |
| SciPy | 1.17.1 | scikit-learn | 1.9.1 |
| joblib | 1.6.0 | threadpoolctl | 3.8.0 |
| narwhals | 2.28.0 | cloudpickle | 3.1.2 |
| annotated-types | 0.8.0 | typing-extensions | 4.16.0 |
| typing-inspection | 0.4.5 | | |

From the Arena repository root, run the Bash block below. It prepares a single
synthetic ingest, waits for that process to close, and copies its workspace for
each writer. Both exports use the same fixed publication time.

```bash
(
  set -euo pipefail
  test "$(git -C "$ARENA_PRODUCER" rev-parse HEAD)" = 6e721aedd5d5e248f8ddad97e161f8079dce8844
  test "$(git -C "$ARENA_CONSUMER" rev-parse HEAD)" = dfed889eabc7b7b067b81d8e96f01d67de47c8dc
  git -C "$ARENA_PRODUCER" diff --quiet HEAD --
  git -C "$ARENA_CONSUMER" diff --quiet HEAD --
  export PYTHONPATH="$ARENA_PRODUCER/src:$ARENA_CONSUMER/src"
  export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
  export VECLIB_MAXIMUM_THREADS=1 BLIS_NUM_THREADS=1 PYTHONDONTWRITEBYTECODE=1
  mkdir -p data
  run=$(mktemp -d data/paired-export.XXXXXX)
  "$ARENA_PY311" - "$run/input" <<'PYTHON'
import sys
from pathlib import Path

from arena_lakehouse.engine import Pipeline
from arena_lakehouse.synthetic import generate

receipt = Pipeline(Path(sys.argv[1])).ingest(generate(240, seed=42, faults=False))
assert (receipt["accepted"], receipt["duplicates"], receipt["quarantined"]) == (472, 0, 0)
PYTHON

  test ! -e "$run/input/control.db-wal"
  test ! -e "$run/input/control.db-shm"
  cp -R "$run/input" "$run/py311"
  cp -R "$run/input" "$run/py313"
  cat > "$run/export.py" <<'PYTHON'
from datetime import datetime
import json
from pathlib import Path
import sys

import pyarrow.parquet as parquet

from arena_copilot.snapshot import snapshot_fingerprint, validate_snapshot
from arena_lakehouse.engine import Pipeline
from arena_lakehouse.snapshot import export_snapshot

root = Path(sys.argv[1])
generated_at = datetime.fromisoformat("2025-02-08T15:10:00+00:00")
package = export_snapshot(Pipeline(root), root / "snapshots", generated_at=generated_at)
manifest = validate_snapshot(package)
assert manifest["dataset_version"] == "cce13957f4f272a3"
for table, count in {"matches": 240, "match_history": 472, "team_stats": 16}.items():
    assert parquet.read_metadata(package / f"gold/{table}.parquet").num_rows == count
assert len(json.loads((package / "predictions.json").read_text())) == 8
print(snapshot_fingerprint(manifest))
PYTHON
  "$ARENA_PY311" "$run/export.py" "$run/py311" > "$run/py311.fingerprint"
  "$ARENA_PY313" "$run/export.py" "$run/py313" > "$run/py313.fingerprint"
  cmp "$run/py311.fingerprint" "$run/py313.fingerprint"
  printf 'Matching package fingerprint %s\n' "$(cat "$run/py311.fingerprint")"
)
```

Matching fingerprints establish that both validated manifests list the same
payload hashes. Copying one workspace keeps the ingest ID fixed. A fresh ingest
generates a new ID, so a new pair can have a different fingerprint from the
recorded experiment. Record both manifests and compare decoded Parquet schemas
and rows before attributing any difference to a runtime. This check covers one
fixture and two local environments. Native wheels and SQLite builds can differ.
