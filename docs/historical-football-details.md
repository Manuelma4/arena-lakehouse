# World Cup 2018 historical detail adapter

`arena_lakehouse.football_details` builds an independent, checksum-bound Research
package from the original licensed Pappalardo/Massucco files. It adds match-bound
starting elevens, bench and substitutions, same-source player names and national
team context. It does not change the Current collectors, CDC fixtures, canonical
scores, OpenFootball research contracts or any website route.

The actual source acquisition and its CC BY 4.0 grant are recorded in
[the source report](wyscout-detail-source.md). Arena's original adapter code and
tests remain MIT; the source and normalized data retain their separate CC BY 4.0
attribution. Original files stay outside Git.

## Temporal scope

Three distinct clocks remain visible:

- Match kickoff: **14 June-15 July 2018**, native `dateutc` checked against the
  source wall clock's explicit GMT offset.
- Deposit publication: **2019**, with exact original article versions and file IDs.
- Local observation: **8 October 2026**, carried from the actual capture ledger.

This is retrospective post-match evidence. It must not be used as proof of what
was available before a 2018 kickoff, as a Current 2026 roster or as a forecast.
The package explicitly sets `pre_match_availability_evidence` to `false`.
`completed_at` remains unknown because the source does not supply an exact
completion instant.

## Explicit local inputs

The module takes four input paths and one output directory:

| Input | Required content |
| --- | --- |
| `--matches-path` | Original `matches.zip`, Matches article 7770422 v1, file 14464622 |
| `--players-path` | Original `players.json`, Players article 7765196 v3, file 15073721 |
| `--teams-path` | Original `teams.json`, Teams article 7765310 v3, file 15073697 |
| `--source-manifest` | Captured input checksums, sizes, source/version/license and observation metadata |
| `--output` | Dedicated local package directory; never a directory containing input originals |

The input manifest has `schema_version: historical-football-inputs/1.0.0`,
`provider: wyscout-pappalardo-2019`, `scope: historical_research` and exactly
three `sources`. Each source requires these fields:

`role`, `article_id`, `article_version`, `file_id`, `filename`, `url`, `sha256`,
`bytes`, `md5`, `observed_at`, `metadata_observed_at`, `metadata_sha256`,
`published_at`, `license`, `license_url`.

Roles are `matches`, `players` and `teams`; exact approved deposit versions,
file IDs, official download URLs and CC BY 4.0 declarations are checked. Sizes,
SHA-256 and MD5 are verified against the three input bodies. Aware UTC source
clocks must follow publication and metadata observation. The supplied descriptor
is capture provenance, not a digitally signed license: its original metadata
evidence is recorded separately in the source report and private capture ledger.

For the local capture made in this session:

```powershell
python -m arena_lakehouse.football_details --matches-path data/open-details-2026-10-08/wyscout/matches.zip --players-path data/open-details-2026-10-08/wyscout/players.json --teams-path data/open-details-2026-10-08/wyscout/teams.json --source-manifest data/open-details-2026-10-08/wyscout/source-manifest-v1.json --output data/open-details-2026-10-08/historical-package-v1-final
```

The captured files and descriptor are ignored and absent from a fresh clone.
The original TEST ONLY fixture cases run independently of them. The adapter
performs no acquisition, account creation, API request, model call or scheduling.

## Wire contract

The output contains exactly `manifest.json` and `details.json`. Its schema version
is **`historical-football-details/1.0.0`**; it is not a Current-provider snapshot.

`details.json` contains:

| Section | Contents |
| --- | --- |
| Header | Schema/provider/scope/sport, source competition 28 and season 10078 |
| `teams` | Selected national teams: source IDs, native/normalized names, official name and native area codes |
| `players` | Selected participant metadata: source IDs, native/normalized short name, generic deposit role and nullable preferred foot |
| `matches` | Source match/round IDs, native/UTC kickoff, nullable venue/group/label, completed status, outcome and ordered teams |
| Match `teams` | Exactly home then away, native scores, formation availability, starters, bench and substitutions |
| Participants | Source player ID, starter/bench kind, source list order, nullable goals/own goals/assists, original counters and uninterpreted card tokens |
| Substitutions | Same-team player-in/out IDs, native minute and source list order |

Entity IDs keep both a native integer `source_id` and a type-qualified ID, for
example `wyscout-pappalardo-2019:match:2058017`,
`wyscout-pappalardo-2019:team:4418` and
`wyscout-pappalardo-2019:player:353833`. Names are resolved only by those native
IDs; text similarity never joins a different provider. A bench entry does not
claim that the player entered the match.

Each published row, including nested participants and substitutions, has
`source_refs`, `source_system`, `created_at`, `updated_at`, `deleted_at` and
`quality_score`. Creation/update audit clocks use the corresponding source
observation for a deterministic initial import; they do not assert when the
provider originally created its record. `deleted_at` is null for this immutable
source selection. No absent source row is inferred to have been deleted.
The quality score is 1.0 only for a row that passed this adapter's structural
contract; allowed unknowns do not imply complete statistics or independently
verified sporting accuracy.

`manifest.json` records exact sources and selected archive-member hash,
attribution/change notice, observed/business clocks, counts, quality checks,
capability limits and `details.json` bytes/SHA-256. To recompute its full
`publication_fingerprint`, remove only `dataset_version` and
`publication_fingerprint`, serialize with Python JSON `ensure_ascii=False`,
`sort_keys=True`, `indent=2`, `allow_nan=False`, append one LF, encode UTF-8 and
hash with SHA-256. `dataset_version` is the first 16 hexadecimal characters.
Payload checksums use original file bytes, not reserialized objects.

## Deliberate semantics and limits

For a regular match, `final_match_score` is regulation and `extra_time_total` /
`shootout` are null. For extra time, the final match total uses `scoreET`. For
penalties, that tied final match score remains separate from the `scoreP`
shootout result. The declared winner must match the deciding score stage.
Native score fields and duration remain available; no shootout goal is added
to match goals.

Literal `"null"` in a participant counter becomes an unknown. Numeric or string
zero stays zero. Native card values such as `"92"` are preserved only under
`native_card_fields`, never interpreted as 92 cards. These are not event-derived
player ratings, complete telemetry, xG or possession.

Unicode normalization decodes only valid literal `\uXXXX` sequences, at most
twice, and preserves existing Unicode. Invalid escapes, lone surrogates and
control characters reject the source rather than producing corrupt names.
The verified example is native `K. Mbapp\u00e9` to `K. Mbappé`. Generic deposit
roles are not match-specific tactical positions; shirt numbers are not invented.
Biographies, birth dates, height/weight, `currentTeamId` and
`currentNationalTeamId` are excluded from the package.

Source area codes are not asserted to be ISO codes. England's source alpha-2
code is blank and becomes null; native alpha-3 `XEN` remains unchanged. There is
no inferred flag, badge or Current identity mapping.

## Integrity, replay and bounded writing

The adapter reads at most 25 MB of originals and only the selected World Cup
member, bounded at 5 MB. It does not extract unrelated ZIP paths. The published
package is bounded at 25 MB. Duplicate JSON keys, nonfinite values, conflicting
IDs/ownership, invalid sides/references, unsupported schema fields, wrong season,
out-of-scope dates, incompatible score stages and impossible substitution
sequences reject the package before publication.

Only the explicit output directory is written. An exclusive local build lock
prevents overlapping writers; existing unrelated or different files are retained
and cause refusal. Files are replaced atomically inside the dedicated directory,
with the manifest last. Identical replay writes no new records and preserves
both files byte for byte. A retained identical detail file can be completed with
its missing manifest after an interrupted publication. A pre-existing lock is
never bypassed or removed by another build. There is no recursive cleanup.

## Measured local verification

Verified on **8 October 2026** against the ignored original files:

| Measurement | Result |
| --- | --- |
| Matches / selected teams / selected players | 64 / 32 / 736 |
| Starting / bench entries | 1,408 / 1,477 |
| Substitutions | 381 |
| Row quality/audit contract checks | 4,226 passed |
| Preserved unknown participant goals | 1,944 |
| Missing formations | 0 |
| `details.json` size | 3,487,190 bytes |
| Local original fixture tests | 82 passed; no skips |
| Actual final-code replay | Exit 0; 0.470 seconds observed, not a scaling benchmark |

Dataset version: `3525a60c19065323`.
Publication fingerprint:
`3525a60c19065323b2003644f374099ffa4f835d325a0e054484aacb9ebdfb5a`.
Details SHA-256:
`879b217e1148a8b6faf55eb545543fe2e41a27abf61f7755bf082851c71d9062`.
Manifest SHA-256:
`1517beba4c9f69dfc8add36de073661c087d05821d9d795fee5de3dd5fea35cb`.

The first actual-source attempt rejected England's blank area code. That report
is retained; a regression case and deliberate null handling corrected the source
assumption. Earlier private prototype packages/reports are not the final
publication fingerprint above.

Local evidence: `football-details-fixtures-bounded.xml`,
`adapter-actual-bounded-verification.json`, original failed/successful run reports
and `historical-package-v1-final/`, under `data/open-details-2026-10-08/`.
Final verification rechecked originals unchanged, byte-identical replay, whole
manifest fingerprint, file checksums, all native participant projections,
extra-time/shootout examples and the Unicode/absent-code cases.
Ruff passed for the module and original tests. No new provider request, Spark
process, model invocation, GitHub Actions run, frontend build, service restart, public API,
frontend change, commit or deployment occurred in this adapter step.

Consumer package validation, selected-match API retrieval and the historical
Research interface remain the next separate gate. They must retain attribution,
the source observation versus business-date distinction and the explicit
retrospective scope; this report makes no consumer or browser acceptance claim.
