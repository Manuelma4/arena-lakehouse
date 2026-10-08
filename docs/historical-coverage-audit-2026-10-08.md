# Retained historical football coverage audit

**Status: read-only audit, frozen on 8 October 2026.** The approved producer,
consumer, API, UI and historical agent remain **World Cup 2018 only**, under
`historical-football-details/1.0.0`. The additional editions below have been
profiled, not normalized, imported, approved or served. No network request,
download, provider call, model, Spark, Docker or CI run was used for this audit.

## Evidence and measured coverage

The retained capture is `data/open-details-2026-10-08/wyscout/`. Its original
Matches v1, Players v3 and Teams v3 metadata explicitly records CC BY 4.0 and
Pappalardo/Massucco attribution. This audit rechecked original sizes, SHA-256,
MD5, metadata hashes, deposit versions, file declarations and license against
`source-manifest-v1.json`; all checks passed. See the prior
[source report](wyscout-detail-source.md), [adapter](historical-football-details.md)
and [acquisition contract](historical-football-acquisition.md). The exact source
versions are [Matches v1](https://doi.org/10.6084/m9.figshare.7770422.v1),
[Players v3](https://doi.org/10.6084/m9.figshare.7765196.v3) and
[Teams v3](https://doi.org/10.6084/m9.figshare.7765310.v3).

Original SHA-256 inventory:

| Input | Bytes | SHA-256 |
| --- | ---: | --- |
| `matches.zip` | 645,097 | `c8f92bb7533e5c127e043cee764c991b5c25b4f5e70a65be931baae0b1765ce9` |
| `players.json` | 1,737,347 | `877a111cb1005b73df5645e9338bd74fb4b496bace2fbc545a72abb3b73efa2e` |
| `teams.json` | 27,404 | `9f7a4a3b3d92c0be33f40613ad6e6eb4316c3b9771ec74c61a22c9b8ece23a4d` |

The following country/edition labels follow the existing source review; native
member filenames and competition/season IDs are directly measured. The league
labels do not come from an independently acquired competition metadata file.

| Native member | Edition | Competition / season ID | Matches | Native UTC kickoff interval | Teams | Participant IDs | Missing player metadata IDs |
| --- | --- | --- | ---: | --- | ---: | ---: | ---: |
| `matches_England.json` | England 2017/18 | 364 / 181150 | 380 | 2017-08-11 18:45:00–2018-05-13 14:00:00 | 20 club | 603 | 4 |
| `matches_France.json` | France 2017/18 | 412 / 181189 | 380 | 2017-08-04 18:45:00–2018-05-19 19:00:00 | 20 club | 629 | 3 |
| `matches_Germany.json` | Germany 2017/18 | 426 / 181137 | 306 | 2017-08-18 18:30:00–2018-05-12 13:30:00 | 18 club | 537 | 2 |
| `matches_Italy.json` | Italy 2017/18 | 524 / 181248 | 380 | 2017-08-19 16:00:00–2018-05-20 18:45:00 | 20 club | 686 | 3 |
| `matches_Spain.json` | Spain 2017/18 | 795 / 181144 | 380 | 2017-08-18 18:15:00–2018-05-20 18:45:00 | 20 club | 619 | 3 |
| `matches_European_Championship.json` | Euro 2016 | 102 / 9291 | 51 | 2016-06-10 19:00:00–2016-07-10 19:00:00 | 24 national | 552 | 0 |
| `matches_World_Cup.json` | World Cup 2018 | 28 / 10078 | 64 | 2018-06-14 15:00:00–2018-07-15 15:00:00 | 32 national | 736 | 0 |

All **1,941 match IDs are unique across the seven members**; every native status
is `Played`. The 3,882 team-match records all have `hasFormation: 1`, eleven
starters, two distinct source teams with consistent home/away ownership, and no
lineup/bench or opponent participant overlap. Native `dateutc` parsed and agreed
with each explicit-offset wall clock for all 1,941 matches. These are bounded
source-profile checks, not complete adapter or consumer acceptance.

The union contains **142 teams: 98 clubs and 44 national teams**, all resolved
in the 142-row team lookup. There are **3,618 participant IDs**, of which 3,603
resolve in the 3,603-row player lookup; no lookup rows are unused. Participant
counts by edition overlap and must not be added. Totals are **42,702 starter
entries, 31,396 bench entries and 11,097 supplied substitution entries**. The
substitution total excludes six unknown lists, so it is not a completeness claim.

| Native member | Expanded bytes | Member SHA-256 |
| --- | ---: | --- |
| `matches_England.json` | 1,694,720 | `620725c2e6a58b4db3e574ed6c559136477451d81af543f8a06bd85c3da3fe29` |
| `matches_France.json` | 1,707,222 | `851fad20616a99383ec8a6ef2136c141700cd44af235a3da6c10008dbac37cea` |
| `matches_Germany.json` | 1,377,328 | `6f962a20f50b174939c7b24d51169aaee10ae896b05dca89fc33aa81b585c0a9` |
| `matches_Italy.json` | 2,019,196 | `afb21c3fa8bd4b1d30af158fa3edfae1e61127825b481e49b32bd7d1d3b99725` |
| `matches_Spain.json` | 1,705,380 | `9787475e64c496d44dc394f98def2610cc31809637fc10c13ec151b37b6118ce` |
| `matches_European_Championship.json` | 312,151 | `79076e301b79e9621684d3399f50b0b4699b009cdb4d215d641be9476ce8000d` |
| `matches_World_Cup.json` | 395,677 | `1ddab20c8605c063a62341eb846466c8d040885a5f0f9a3e26d023123786abb6` |

Expanded match bytes total **9,211,674**. The largest member is below the
existing 5 MB input-member bound. This does not prove the normalized multi-edition
output fits the existing 25 MB package bound: the actual 64-match v1
`details.json` is already 3,487,190 bytes, including repeated provenance.

## Source gaps requiring an explicit wider contract

| Observation | Exact retained evidence | Required behavior |
| --- | --- | --- |
| Unresolved player metadata | England IDs 379199, 447214, 470819, 532900; France 381235, 471582, 530062; Germany 302605, 497353; Italy 475356, 488648, 531447; Spain 3689, 298776, 503366 | Preserve all 15 source identities with explicit unresolved metadata and null names. They appear only on benches: 23 entries across 22 matches. Never fill them from another provider, names or `currentTeamId`. |
| Unknown substitution lists | Literal `"null"` in five England team-match records and one France record | Publish unknown availability, not an empty list or zero substitutions. |
| Unresolved substitution references | Eight Italy entries have `playerIn: 0`, in matches 2576016, 2575965 and 2575959 | Retain the native token and outgoing identity; expose unresolved incoming identity. Exclude these entries from complete active-player sequence/minutes claims. ID 0 is not a real player entity. |
| Wider native bench lists | Two Italy team-match records contain 13 entries | Preserve observed lists under a reviewed v2 bound; do not silently broaden v1's World Cup limit of 12. |
| Conflicting outcome evidence | Unequal regulation scores but native winner 0 in England 2500012/2499997, France 2501057/2501047, Germany 2516998/2516941 and Spain 2565862 | Preserve both facts and flag outcome conflict. Native 0 cannot be called a draw here. Withhold affected W/D/L and official-table claims; do not silently repair the source winner. |

Domestic files contain 1,826 regular matches. Euro has 46 regular, two extra-time
and three shootout matches; World Cup has 59 regular, one extra-time and four
shootout matches. Regulation, cumulative extra-time and shootout stages must
remain separate. Participant `"null"` counters remain unknown; native card tokens
remain uninterpreted. All-match telemetry, xG, possession, tactical positions,
minutes played and current rosters remain unsupported.

## Portfolio coverage and next implementation chunk

The [original source plan](open-data-acquisition.md),
[OpenFootball staging](openfootball-staging.md) and reviewed
[catalogue](../config/openfootball_sources.json) already cover England, Spain,
Germany, Italy and France 2026/27; Brazil 2026; Colombia, Argentina and
Libertadores 2025; and World Cup 2026. Existing recorded OpenFootball v2
acquisition contains 2,831 rows across ten editions. Its Latin American files
provide fixture/result evidence, not player details; Colombia is Apertura and
Argentina is first semester. This audit did not remeasure their captures.

Retained Wyscout can add historical details for all five domestic leagues and
Euro, alongside World Cup 2018, without new acquisition. It contains **no Latin
American domestic or club-tournament member**. A Latin American national team
in a World Cup is not evidence of its domestic league. Current player details
or wider Latin American details remain source/permission/coverage gaps. The
API-Football and StatsBomb routes in
[the earlier detail review](free-match-detail-sources.md) are candidates with
separate gates; this audit grants no new entitlement or publication permission.

The next coherent chunk should be a **versioned, edition-aware historical detail
publication and matching consumer path**, with these concrete changes:

1. Producer: introduce an explicit v2 edition registry binding each native member,
   member hash, competition/season IDs, reviewed display label, team type and date
   scope. Publish a fingerprint-bound catalogue plus bounded edition packages;
   measure actual normalized sizes before choosing limits. Keep v1 and its
   World Cup selection unchanged. Reuse the pinned originals and manual
   acquisition path; no collector or new provider acquisition is needed.
2. Schema: define `edition_id`, edition-level coverage/capabilities, nullable
   player metadata resolution, substitution availability/reference status and
   outcome conflict status. Preserve native values and source references alongside
   normalized fields. Give unresolved facts explicit quality flags and row-level
   quality scores under a documented rubric; retain source-system and
   created/updated/deleted audit fields. A permitted unknown is not full coverage.
3. Reader/API: add explicit v2 dispatch and validation in the consumer's
   `historical_football.py`, then edition directory/detail routes and edition
   filters in `api.py`. Validate the approved catalogue fingerprint and child
   publication hashes before serving. Match/team queries, totals, pagination,
   available dates and aggregates must carry the selected edition scope.
4. UI: evolve `historicalFootball.ts`, `historicalChat.ts`,
   `HistoricalFootballWorkspace.tsx` and `HistoricalChatPanel.tsx` from literal
   World Cup/2018/national types and copy to validated edition metadata. Add the
   competition/season directory, preserve scoped IDs in routes, display club or
   national context and explicit unknown/conflict states, and invalidate stale
   requests when edition or publication changes. Serve bounded pages, not the
   entire archive in the browser bundle.
5. Agent: evolve `historical_agent.py` and its answer contract so edition context
   constrains every tool, citation, team aggregate and durable trace. Replace
   literal 2018 date rules and prompt text with validated edition clocks. Preserve
   publication pins, tool/evidence budgets and abstention. No name join or player
   deposit-club field may attach historical evidence to Current matches.

Acceptance must reproduce the measured seven-edition counts and member hashes,
replay unchanged captures without duplicate identities, and independently reject
resealed scope/score/reference mutations. Real-source probes must exercise all
seven editions and every gap family above, plus regular/extra-time/shootout
outcomes, cross-edition team isolation, explicit zero versus unknown, and
reload/back/filter changes at desktop/mobile widths. Producer/consumer/API/UI and
evidence-only agent checks are required before claiming multi-competition
integration; model and human semantic review remain separate work.

Kickoff clocks span 2016–2018; deposits were published in 2019; retained file
observations are 8 October 2026. Keep all three clocks. Completion remains null,
pre-match availability remains unproved, and prediction eligibility remains
false. Preserve CC BY attribution and transformation notices for each edition.

## Durable local audit artifacts

Ignored artifacts are `data/open-details-2026-10-08/audit_retained_coverage.py`
and `data/open-details-2026-10-08/retained-coverage-audit.json`; these do not ship
in a fresh clone. The script uses Python stdlib, reads the seven match members
and lookup files only, with 5 MB per original/member, 12 MB total expanded
members and 1,000 rows per member. It writes only its own report.

Measured report clock: `2026-10-08T14:05:17.155052Z`.
Report SHA-256: `2f197f7d6fd3b7c39095b4ad9222874c80f59ec08cb90ff732686ab240a10886`.
Script SHA-256: `b3db3a9a2026d0a6dc3559f53ad7d8d63cc1d827c5d52732189a6dcc7a52fdfa`.
Input descriptor SHA-256: `8c7b664c4956c61a15434bb22b8e54f85aba3eecf34c3f60b6f69a22dccc71c2`.

Only this documentation and those ignored audit artifacts were added. No source
module, contract, test, environment, service, README, workflow or Git publication
was changed by this audit. Work paused and files frozen after this handoff.
