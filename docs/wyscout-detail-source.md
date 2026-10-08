# Licensed historical football details

Acquisition reviewed on **8 October 2026**. The original Pappalardo/Massucco
Figshare deposits provide a feasible free Research enrichment route. This pilot
is **World Cup 2018 history**, separate from Current football-data.org fixtures,
the existing OpenFootball corpus and present-day team rosters.

## Permission and normal public access

The public Figshare API returned 200 for all three original deposit records and
explicitly identifies each as **CC BY 4.0**. Published article versions and
individual file IDs pin the source; SHA-256 records pin the captured bodies.

| Deposit | Version | Official file | Bytes declared by Figshare |
| --- | ---: | --- | ---: |
| [Matches](https://doi.org/10.6084/m9.figshare.7770422.v1) | 1 | `matches.zip`, file 14464622 | 645,097 |
| [Players](https://doi.org/10.6084/m9.figshare.7765196.v3) | 3 | `players.json`, file 15073721 | 1,737,347 |
| [Teams](https://doi.org/10.6084/m9.figshare.7765310.v3) | 3 | `teams.json`, file 15073697 | 27,404 |

CC BY 4.0 permits sharing and adaptation with attribution, a license link and a
notice of modifications. Keep the data license separate from Arena's MIT code,
retain the supplied attribution/disclaimer information and imply no endorsement.
The license does not grant unrelated trademark or publicity rights and does not
require images in a README.
[License deed](https://creativecommons.org/licenses/by/4.0/),
[legal code, sections 2-4](https://creativecommons.org/licenses/by/4.0/legalcode.en).

The author records request citation of Pappalardo et al. (2019),
*A public data set of spatio-temporal match events in soccer competitions*,
Scientific Data 6:236, [DOI 10.1038/s41597-019-0247-7](https://doi.org/10.1038/s41597-019-0247-7).
The API metadata and actual licensed files are the acquisition evidence; an
indexed paper or a third-party mirror is not substituted for them.

Direct browser access to some article pages remained unavailable, the PMC page
showed a browser challenge and the Nature page redirected to an inaccessible
authentication page. None was bypassed. The documented, public
`https://api.figshare.com/v2/articles/<id>` interface worked normally without a
key, cookie, login, proxy, altered URL parameters or automated challenge solving.
Its supplied download URL returned a normal 302 to the publisher's S3 storage,
which was followed unchanged. This is the ordinary public download flow.

## Actually captured

Local originals and measurement reports are ignored under
`data/open-details-2026-10-08/wyscout/`. They are not in a fresh clone and no
dataset was committed or published. `requests.json` records each wire request,
including the 302, the exact URL, actual UTC observation clock, response status,
byte count and SHA-256. A signed storage redirect is retained only in this private
capture ledger.

The bounded capture completed **six logical resources / nine wire requests**:
three article metadata responses (200), three ordinary storage redirects (302)
and three original file responses (200). The original six-wire cap was explicitly
extended to nine after observing those redirects. Total response bytes were
**2,426,535**, including **2,409,848 bytes** of original dataset files. No retry,
provider key, model call, new dependency, Spark/Docker process, recurring collector
or GitHub Actions run was involved.

| Original body | Observation time (UTC) | SHA-256 | Figshare computed and supplied MD5 |
| --- | --- | --- | --- |
| `matches.zip` | 2026-10-08T09:18:04.905275Z | `c8f92bb7533e5c127e043cee764c991b5c25b4f5e70a65be931baae0b1765ce9` | `51d80beb17480919f69a53a0152c2d71` |
| `players.json` | 2026-10-08T09:22:27.861538Z | `877a111cb1005b73df5645e9338bd74fb4b496bace2fbc545a72abb3b73efa2e` | `f28ddf6326281efeda6488b2169f5609` |
| `teams.json` | 2026-10-08T09:22:28.047459Z | `9f7a4a3b3d92c0be33f40613ad6e6eb4316c3b9771ec74c61a22c9b8ece23a4d` | `1381ff9449f21105090729cf0e086b5b` |

Each original file matched the exact size and both MD5 values published by
Figshare. Every archived response's SHA-256 was rechecked locally.
`capture-verification.json` records that result; `profile.json` records measured
fields and the native score examples. These are acquisition/contract checks,
not lakehouse pipeline, consumer API or model-evaluation acceptance tests.

The World Cup member, `matches_World_Cup.json`, was read from the original
`matches.zip` without modifying the ZIP. Its native body is **395,677 bytes**,
SHA-256 `1ddab20c8605c063a62341eb846466c8d040885a5f0f9a3e26d023123786abb6`.
The ZIP contains seven competition files; only the World Cup member was selected
and profiled. Other members were neither normalized nor imported into Arena.

| Measured field | Actual World Cup member |
| --- | --- |
| Matches / unique match IDs | 64 / 64 |
| Source competition / season IDs | 28 / 10078 |
| Native `dateutc` interval | 2018-06-14 15:00:00 through 2018-07-15 15:00:00 |
| Status | 64 `Played` |
| Duration | 59 `Regular`, 1 `ExtraTime`, 4 `Penalties` |
| Unique teams / team-match records | 32 / 128 |
| Formation available | 128 of 128 team-match records |
| Starting entries | 1,408; exactly 11 per team-match record |
| Bench entries | 1,477; 8-12 per team-match record |
| Unique historical participant IDs | 736 across starters and bench |
| Substitution entries | 381 |
| Same team lineup/bench overlap | None |
| Literal `"null"` in participant goals | 1,944 occurrences |
| Player metadata / unique player IDs | 3,603 / 3,603 |
| Participant IDs resolved in player metadata | All 736; no missing IDs |
| Team metadata / unique team IDs | 142 / 142 |
| Match team IDs resolved in team metadata | All 32, each `national` |
| Player records containing literal Unicode escapes in name fields | 1,152 |

All 64 selected records also passed local checks for two distinct source teams,
one home and one away side, matching `teamsData` keys and IDs, unique participant
ownership across both teams and substitution references belonging to that
match-team participant set.

These are source-observed historical participant records, not independent proof
of complete event telemetry. No large event corpus was acquired; shot locations,
pass coordinates, possession and xG are not accepted capabilities of this pilot.

## Important native semantics

Preserve `date`, `dateutc`, `duration`, `winner`, all four native score fields and
the archive/member reference. The 2026 observation clock is different from a
2018 match kickoff; no historical answer may use it as evidence of a current
roster or as a pre-match feature cutoff.

The match record demonstrates why a single score field is insufficient:

| Match ID | Native label | `score` | `scoreET` | `scoreP` |
| --- | --- | --- | --- | --- |
| 2058017 | France - Croatia, 4 - 2 | 4-2 | 0-0 | 0-0 |
| 2058015 | Croatia - England, 2 - 1 (E) | 1-1 | 2-1 | 0-0 |
| 2058012 | Russia - Croatia, 2 - 2 (P) | 1-1 | 2-2 | 3-4 |

The deposit description supplies score meanings; the observed sample separates
regulation, extra-time totals and shootout values. Do not add shootout values to
match goals, interpret a non-applicable zero as an actual extra-time score or
drop the native `winner`. The description spells a duration value `Penalities`,
while these files use `Penalties`; implementation must validate observed values
and explicitly handle this documentation mismatch.

Participant statistics are native strings. A literal `"null"` becomes an unknown,
never zero. Card fields include tokens such as `"92"` and `"94"`; do not publish
those as 92 or 94 card counts. Until the native encoding is verified, preserve
the tokens or omit a derived card metric. Substitution IDs and minute values are
match-bound facts. They do not authorize replacing each player's historical
membership with a later metadata snapshot's `currentTeamId`.

## Small accepted contract proposal

Start with a separately labelled **World Cup 2018 Research detail package**:

1. Manifest: contract version, provider `wyscout-pappalardo-2019`, historical scope,
   exact article versions/file IDs, body/member hashes, actual observation clocks,
   business date range, license URL, authors, paper citation and changes notice.
2. Match context: native match/competition/season/round IDs, ordered source team
   IDs, native UTC kickoff, venue, status, duration, winner and separate native
   score fields. A displayed score is derived under a named and tested rule.
3. Participants: source player ID, source team ID, `starter` or `bench`, source
   list order and nullable native counters. Substitutions remain a separate
   match-team list. A bench entry alone is not proof that a player entered play.
4. Metadata: resolve names only by the same Wyscout IDs after the actual player
   and team files pass integrity checks; omit birth dates and physical traits
   from the initial display. Treat roles and preferred foot as deposit metadata,
   not as a match-specific tactical position.
5. Quality: unique match IDs, two distinct teams per match, consistent native
   `side`, unique participant ownership, referential integrity, deliberate null
   handling and replay invariance. Preserve source observations, audit clocks and
   a per-row quality result in the published detail layer.

No existing source catalogue, pipeline, schema, API or UI was edited by this
acquisition task. No names are joined by text across providers, no historical
lineup is attached to Current 2026 matches and no forecast is inferred from these
post-match facts. Actual API/browser integration and a reproducible importer
remain separate implementation gates.

The player payload uses `shortName`, although the deposit description says
`shortName2`. It also contains literal escaped Unicode within already-decoded
JSON strings; for example, player 353833 has native `shortName` value
`K. Mbapp\u00e9`. An importer must test a deliberate Unicode normalization rule,
retain the original name and hash, and reject malformed encodings. Applying
Python's broad `unicode_escape` decoding to every string would risk corrupting
strings that already contain valid Unicode. The generic `role` object supplies
`name`, `code2` and `code3`; it does not provide a player's tactical position or
shirt number in this match.

## Attribution for a future publication

Suggested notice, alongside the package and its Research view:

> Data: Luca Pappalardo and Emanuele Massucco, original Figshare deposits, CC BY
> 4.0. See the linked deposit versions and Pappalardo et al. (2019), Scientific
> Data 6:236. Arena selected the World Cup 2018 records and normalized identifiers,
> timestamps and participant structure; original files remain separately
> checksummed. No endorsement is implied.

This is a proposed notice for the later normalized package; no normalization or
public release is claimed by this source pilot.
