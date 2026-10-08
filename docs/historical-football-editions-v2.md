# Historical edition producer v2

`arena_lakehouse.football_editions` normalizes the seven retained licensed
Pappalardo/Massucco members into separate edition packages. This implements the
producer part of the [coverage roadmap](historical-coverage-audit-2026-10-08.md).
Originals and normalized data retain CC BY 4.0; adapter code/tests remain MIT.
No acquisition, model, Spark or provider call occurs. World Cup v1 stays intact.

The registry binds member, competition/season IDs, reviewed file label,
club/national type and date scope. UTC and native offset clocks must agree.
Source SHA-256, MD5, sizes, official deposit/file versions, license and ordered
observation clocks reuse the original input contract. Labels are reviewed file
labels, not separately acquired competition metadata.

`historical-football-editions/2.0.0` has a catalogue at `catalog/catalog.json`.
Each edition contains `manifest.json` and `details.json` under
`historical-football-details/2.0.0`. The catalogue binds complete child manifest
hash, publication pin, member hash and measured sizes/counts. It is committed
after all children pass; interrupted builds can retain children without a
catalogue. Replay requires identical bytes and never replaces another publication.
Symlinks/reparse points, bounded originals, 12 MB total expanded members and
25 MB per normalized edition are enforced. ZIP entries are read without extraction.

Missing player metadata retains source ID, null name and unresolved status.
Incoming substitution ID 0 remains a null reference with native token; no player
0 or active-player sequence is invented. Unknown substitution lists remain null,
never empty/zero. Supplied native lists remain retained. The v2 bench bound is
20; v1 remains 12. Score stages remain separate, and conflicting native winner
and score make result eligibility false. One horizontal tab is normalized only
for display; original name and a transformation flag remain.

Score 1.0 means a structural contract passed. Score 0.75 marks an explicit
unresolved reference, unknown list, winner conflict or name transformation;
neither score claims sporting accuracy/completeness. Individual unknown counters
and uninterpreted card tokens remain distinct from zero. Completion, pre-match
availability, current rosters, telemetry, minutes, forecasts and official tables
remain unsupported.

Actual normalized source: 1,941 matches, 142 teams, 3,618 player IDs;
42,702 starters, 31,396 bench entries and 11,097 supplied substitutions.
Independent comparison checked all match IDs, scopes, teams, native scores,
participants and substitution lists. It reproduced 15 unresolved player IDs,
six unknown lists, eight unresolved incoming references, seven winner conflicts
and one name transformation. Fifteen JSON files total 86,686,065 bytes; largest
normalized edition: 19,099,956 bytes. Replay is identical and v1 remains pinned.

Local ignored output: `data/historical-editions-v2-20261008-reviewed`.
Catalogue pin: `6d377cfe3700eaedabd6d14d798486ee1e7879cf758bd1bf60d6cccf5274897d`.
Independent probe/report: `data/verify-historical-editions-v2-20261008.py` and
`data/historical-editions-v2-actual-evidence-20261008.json`.

```powershell
Set-Location D:\Escritorio\Works\arena-lakehouse
& ..\.arena-venv\Scripts\python.exe -m arena_lakehouse.football_editions `
  --matches-path data/open-details-2026-10-08/wyscout/matches.zip `
  --players-path data/open-details-2026-10-08/wyscout/players.json `
  --teams-path data/open-details-2026-10-08/wyscout/teams.json `
  --source-manifest data/open-details-2026-10-08/wyscout/source-manifest-v1.json `
  --output data/historical-editions-v2-20261008-reviewed
```

Original TEST ONLY cases cover every gap, scope/reference rejection, catalogue
hashes, damaged source/output refusal, replay and v1 preservation. The combined
v2/v1 suite passed 91 cases. Source measurements and test fixtures remain distinct.
The reprise consumer independently validates these packages and now serves edition browsing, typed deterministic match/team evidence and saved replay. World Cup v1 remains accessible. See [current reprise](REPRISE_2026-10-08.md) for activation, validation and recovery.
