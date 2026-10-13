# Pinned World Cup match detail

This checkpoint adds `arena_lakehouse.worldcup_details`: two bounded public
requests collect the pinned OpenFootball full-detail source and CC0 license.
Atomic JSON/Parquet publication preserves the originals and checks exact parent
matches against the separately pinned basic resource. It does not overwrite an
existing output or change the original research importer.

## Reproduce the optional detail publication

Use fresh output directories for each capture and publication. These commands
use the existing Python environment; they do not require provider credentials.

```powershell
python -m arena_lakehouse.worldcup_details --collect-to data/worldcup-details-capture-new
```

Read `source.json`'s completed `observed_at` from the generated
`acquisition.json`, then supply that actual clock to publication:

```powershell
python -m arena_lakehouse.worldcup_details --raw-file data/worldcup-details-capture-new/source.json --license-file data/worldcup-details-capture-new/LICENSE.md --observed-at <completed-source-observation-UTC> --output data/worldcup-details-new
```

Point the consumer's local `ARENA_WORLD_CUP_DETAILS` at the completed publication
directory and restart its preview. The optional reader verifies it before
exposing any detail. The recovery script rebases the current approved captured
publication on another computer; credentials remain separate.

## Recorded evidence

The actual 13 October collection contains 104 records, with 102 verified joins
and two withheld score/clock conflicts. Four reviewed country aliases link 16
records. The consumer independently validates every join and displays source
starting XI, substitutions, events and provenance. Missing event families stay
unknown; source-listed substitutes do not constitute complete matchday benches.
No global player identity, current roster or pre-match availability is inferred.

Source: [pinned OpenFootball detail](https://github.com/openfootball/worldcup.json/blob/0c4b73b3c1fbb877e8274eb321b1ec4d9220a3a4/2026/worldcup-full.json),
[CC0 license](https://github.com/openfootball/worldcup.json/blob/0c4b73b3c1fbb877e8274eb321b1ec4d9220a3a4/LICENSE.md).
Raw SHA-256: `42bf26a5dca941eb77f56572c6f06ca3dc78766f249ef0fb60b4da46e7698fde`.
Collection: `2026-10-13T09:16:20.854544Z`.
Active manifest fingerprint:
`4586540c2ae256ca1d65b34de7b1107e6107e2c143db994c259792bfa5627d4d`.

Fifteen original offline tests passed. The consumer separately checked all 104
actual endpoints and the compiled desktop/mobile interaction. Its document with
this same filename records the full verification, normalized forecast storage,
team consistency correction and measured scores.

One successful bounded football poll returned 2,132 rows/seven changes; VALORANT
returned 509 rows/one change/nine unsupported. A second football candidate failed
because a source record marked finished lacked complete scores. The last good
publication and cursors were preserved. No continuous worker was left running.

Docker Linux/Spark/Delta, authenticated Databricks, real model evaluation and
release acceptance remain unverified. Both repositories stay private on the
checkpoint branch; no Actions were dispatched or public deployment performed.
