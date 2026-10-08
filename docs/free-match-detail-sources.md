# Free match-detail sources

Observed on **8 October 2026**. This is a documentation and permissions review,
not an authenticated provider acceptance test. No account was created. No
credentials were inspected, and no provider data or dataset files were collected.

Arena can improve its match pages now by reading the details already captured.
Complete current football lineups and professional VALORANT player statistics
require a separate source gate. A free endpoint, a public website and permission
to publish a database are three different things.

## Decision

| Route | Decision for Arena | Evidence strength |
| --- | --- | --- |
| Existing football-data.org account | Keep fixtures/results; do not promise free lineups or detailed counters | High for published plan boundaries; existing capture coverage is recorded separately |
| Existing PandaScore account | Keep fixtures and supplied match/map summaries; player counters remain unavailable | High for published VALORANT plan boundaries; no new account test |
| API-Football direct free account | Prepare an optional, disabled adapter; verify accessible seasons and publication scope before acquisition | High for documented endpoints/quota; unverified for this account and target seasons |
| Pappalardo/Wyscout research release | Best candidate for an explicitly licensed historical football detail demonstration | High for the author's data descriptor and license; no local files or hashes verified |
| StatsBomb Open Data | Candidate for research after its complete user agreement is reviewed | Partial: repository terms were read; the PDF agreement could not be retrieved by this browser |
| VLR.gg extraction | Do not implement without written permission | High: current terms explicitly restrict systematic and automated collection |
| Riot/GRID professional telemetry | Leave as an access limitation under the free-only requirement | High for current public documentation; no account-specific entitlement verified |

There is **no verified generally available free source in this review** for
current professional VALORANT ACS, ADR, kills, deaths and round telemetry with
permission to republish it in Arena. This is an evidence gap, not a claim that
no such agreement could ever be obtained.

## Football: what the existing subscription actually supplies

football-data.org's free plan advertises twelve competitions, delayed scores and
schedules, fixtures and tables. Lineups, substitutions and squads appear in its
paid Deep Data plan; team match counters appear in its paid Statistics add-on.
The word “Free” in the name of a paid package does not make that package free.
[Official pricing](https://www.football-data.org/pricing).

The match-resource documentation shows richer fields, but an example response is
not an entitlement. Its unfolding headers control response shape, not subscription
access. A season squad also does not prove who started an individual match.
[Match resource](https://docs.football-data.org/general/v4/match.html),
[API policies](https://docs.football-data.org/general/v4/policies.html).

The current terms require visible attribution, keep credentials confidential and
reserve graphics rights to their owners. They do not supply an explicit open
database redistribution license. Keep raw bodies private; separately review any
downloadable public dataset. These terms display a 2018 effective date, so they
are the currently published text, not newly issued terms.
[Terms on the official About page](https://www.football-data.org/about).

## API-Football: a plausible free complement, with two unresolved gates

The direct free plan costs zero, allows **100 requests per day**, includes the
documented endpoint families and restricts available seasons. No credit card is
required for that direct free plan. The target competition-season combination
still needs an account-specific check; “all competitions” does not establish
access to its current season or completeness for every match.
[Official pricing](https://www.api-football.com/pricing).

The provider's March 2026 guide documents this useful sequence:

| Endpoint | Use |
| --- | --- |
| `/leagues` | Discover league-season coverage flags |
| `/fixtures` | Obtain provider fixture/team IDs and match status |
| `/fixtures/lineups?fixture=<id>` | Starting eleven, bench and formation when available |
| `/fixtures/statistics?fixture=<id>` | Per-team match counters |
| `/fixtures/players?fixture=<id>` | Individual match performance when supplied |

The same guide says coverage flags do not guarantee every match and missing
statistics can be null. Its reference uses `x-apisports-key` authentication.
[Official endpoint guide](https://www.api-football.com/news/post/how-to-get-started-with-api-football-the-complete-beginners-guide),
[API reference](https://www.api-football.com/documentation-v3).
The reference page itself rendered no text in this review; the endpoint claims
above are supported by the readable first-party guide.

The official terms permit application development but prohibit direct data resale
and multiple accounts used to expand free quotas. Crucially, they disclaim a
license to publish the data and assign rights-clearance responsibility to the
customer; third-party logos have separate rights. API access therefore does not
clear a public Arena dataset. Use direct free registration rather than an
overage-enabled marketplace subscription if this route is later selected.
[Terms, especially Service & data and Data/Logos/images](https://www.api-football.com/terms).

**Proposed bounded pilot, not executed:** cache catalog evidence, choose one
accessible season and three finished fixtures, and retrieve only the permitted
lineup/statistics/player families. Stop on entitlement errors or missing rights.
Use a persistent quota ledger and one acquisition owner. A ceiling of 20 requests
for the pilot leaves space for validation and avoids quietly starting a live
service. At 15-second intervals, one hour of a single poll would already consume
240 requests; a 100-request day cannot support that sustained cadence.

## A genuinely open historical football route

The original 2019 data descriptor explicitly releases the deposited datasets
under **CC BY 4.0**. Its scope is the five European first divisions in 2017/18,
World Cup 2018 and Euro 2016. It is a historical research corpus, not a current
feed, and covers neither current Latin American leagues nor every international
event. The paper reports 1,941 matches; that is an author-reported corpus size,
not a locally measured import count.
[Primary data descriptor](https://pmc.ncbi.nlm.nih.gov/articles/PMC6817871/).

The original deposits provide practical contract inputs:

| Deposit | Match-bound evidence |
| --- | --- |
| [Matches](https://figshare.com/articles/dataset/Matches/7770422/1) | Match/team IDs, lineup, bench, substitutions and score fields; `hasFormation` records whether formations are present |
| [Events](https://figshare.com/articles/dataset/Events/7770599) | Event identity, match/player/team IDs, event type, period-relative time, positions and outcome tags |
| [Players](https://figshare.com/articles/dataset/Players/7765196) | Provider player IDs and names; historical player metadata |
| [Tag mapping](https://figshare.com/articles/dataset/Mapping_of_tag_identifiers_to_tag_names/11743818) | Published meanings of event tags |
| [Competition metadata](https://figshare.com/articles/dataset/Competitions/7765316) | Competition identities and categories |

CC BY permits sharing and adaptation with credit, the license link and a notice
of changes. Keep the data license and provenance separate from Arena's MIT code.
It does not confer team-logo or other third-party rights.
[Creative Commons deed](https://creativecommons.org/licenses/by/4.0/).

**Implementation proposal:** add a historical research mode with a local-file
importer first. A later explicit acquisition can pin original deposit versions,
record archive/member checksums and preserve originals outside Git. Normalize
`teamsData.lineup`, bench and substitutions into match-specific participants;
join names only by provider player ID. Derive only documented counters from
events and label them as Arena-derived, with metric version and definition.
Do not describe event counts as possession or add xG without a separately
validated model. File replay can demonstrate streaming with an explicit
“historical replay” label.

The `currentTeamId` field in a historical player file refers to that file's
snapshot context. It is neither a current 2026 roster nor proof of membership
at every historical match. Match participants and dated roster observations
must remain separate contracts. Source IDs also remain separate from
football-data.org IDs until a reviewed cross-provider mapping exists.

Direct Figshare reads of some match/event pages returned 403 or were unavailable.
Their indexed first-party records and the original data descriptor supplied the
schema/license evidence; no access restriction was bypassed. Consequently, the
download path, individual files and checksums still require verification before
declaring this importer accepted.

StatsBomb's current repository also documents JSON matches, events and lineups
and requests source attribution plus its logo for published analyses. The old
StatsBomb GitHub URL redirects to Hudl. Its PDF user agreement was not readable
through this browser, so this review does **not** assert CC BY, unrestricted
redistribution or equivalent permissions for StatsBomb. It remains a separate
candidate pending that review.
[Official repository](https://github.com/hudl/open-data),
[complete agreement](https://github.com/hudl/open-data/blob/master/LICENSE.pdf).

## VALORANT: fixtures, directories and telemetry are different products

PandaScore's free tier has a published 1,000-request hourly ceiling and includes
schedules and context. Its VALORANT plan matrix places player/team post-game
statistics in Historical and detailed events/rounds in Historical Pro. It also
states that VALORANT has no live/real-time data plan. Free player/team directory
access does not establish per-match counters or historical team membership.
[Pricing](https://www.pandascore.co/pricing),
[game-specific plan reference](https://developers.pandascore.co/docs/plan-reference).

PandaScore's current terms distinguish own-site informational reproduction from
raw-data transfer. Sections 6.3–6.4 restrict transfer of raw data, including free
transfer, and require the attribution `Source: PandaScore`. Section 2.8 prohibits
odds and odds-related products. Do not infer that a public JSON export is cleared
because the website can display processed information. Any future forecast use
requires a separate scope review of these terms; this review establishes no
permission for betting products.
[Official terms](https://www.pandascore.co/terms-and-condition).

Riot's documented public VALORANT API lists game content, match lookup/history,
ranked leaderboards and platform status. It documents no complete professional
tournament/calendar endpoint. Personal key applications are unsupported and
identifiable player-data applications generally require production approval and
opt-in through Riot Sign On. This is not a shortcut to free VCT telemetry.
[Official VALORANT documentation and policy](https://developer.riotgames.com/docs/valorant).

GRID's current `/open-access/` URL redirects to its plans page, whose FAQ says
every listed plan is commercial and scoped by agreement. Earlier GRID pages
advertise Open Access or free professional-team portals; those statements do not
establish Arena's present entitlement. Indexed hackathon terms limited access to
selected historical series for that hackathon, while their direct page returned
410 Gone. No free, transferable production license was verified.
[Current plans](https://grid.gg/plans/),
[redirect checked](https://grid.gg/open-access/),
[earlier VALORANT portal](https://grid.gg/get-valorant/),
[historical hackathon terms](https://grid.gg/hackathon-terms-and-conditions/).

VLR's current terms, sections 3 and 5, restrict automated access/data extraction
and systematic collection into a database without written permission. A public
match page, an unofficial scraper's code license, or successful HTTP access
does not override those restrictions. Arena can learn from VLR's organization
and link to an external source; it cannot treat VLR as an approved ingestion
provider from this evidence.
[VLR terms](https://www.vlr.gg/terms).

## Evidence quality and search boundaries

The review used targeted web searches and direct first-party pages in English:
provider pricing, documentation and terms; Riot/GRID access policies; the original
football data descriptor and its author deposits. Plan evidence was checked as
published on 8 October 2026; the historical corpus covers 2016–2018. Search terms
included provider names with “free”, “lineups”, “fixtures players”, “statistics”,
“terms”, “Open Access”, and the original dataset DOI. This was a targeted technical
review, not an exhaustive literature review or a global proof of absence.

| Evidence family | Type and weight | Independence / limitation |
| --- | --- | --- |
| Provider pricing, API guides and plan matrices | Primary commercial documentation; high for stated product boundaries | Same-provider pages corroborate policy, not independently measured data coverage |
| Provider/VLR terms and Riot policy | Primary published conditions; high for stated restrictions | Not a legal opinion or proof that all rights have been cleared |
| GRID current plans vs older portal/hackathon pages | Primary but inconsistent across time; current plans carry greater weight for current general access | An old free-access claim cannot substitute for a current grant |
| Pappalardo et al. data descriptor | Peer-reviewed primary dataset description; high for corpus/schema/license | One dataset origin; repetitions elsewhere are not independent confirmations |
| Author Figshare records | Primary deposit metadata; high when readable, moderate for browser-indexed records that returned 403 | No file download, integrity test or observed field completeness |
| Creative Commons deed | Primary license summary; high for stated license obligations | Does not license unrelated brands or assets |
| Hudl/StatsBomb repository | Primary repository; partial permission evidence | Complete PDF agreement remained unreadable |
| Previous Arena captures | Local empirical evidence recorded in existing reports | No fresh collection in this review; fixtures are not real coverage evidence |

Paid offerings, third-party scraping wrappers, undocumented website endpoints,
forum claims and mirrored datasets without an attributable original license were
excluded as implementation evidence. Account coverage, latency, complete rights
clearance and exhaustive current player telemetry were not verified.

## Next accepted development step

1. Finish a read-only assistant tool over the existing publication-bound detail
   package. Cite match/source identifiers and abstain for absent fields.
2. Add offline source/contract fixtures for an API-Football adapter and a historical
   Wyscout importer; leave network acquisition and scheduling disabled.
3. Before a real pilot, record `provider`, competition/season, endpoint family,
   entitlement observation, permission reference/date, quota, source hash and
   collection time. Require allowed acquisition **and** the intended publication
   scope; neither silently implies the other.
4. Accept a detail source only after match/player IDs, both team associations,
   originals/hashes, missing-value handling, replay invariance and date semantics
   pass against a permitted real capture. Keep historical participants separate
   from current roster claims.

These steps let the two repositories advance without replacing missing evidence
with fictional professional statistics or spending on a paid plan.
