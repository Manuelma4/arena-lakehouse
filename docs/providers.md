# Data-provider decision

Verified against official documentation and authenticated free-account responses
on 6 October 2026. Protocol-shaped fixtures remain synthetic test inputs;
authenticated collections are separate private runtime artifacts. Credentials
are read from the local environment and excluded from Git and exported datasets.

## Football

[football-data.org](https://www.football-data.org/client/register) offers a free
tier for basic fixtures, results and league tables across 12 competitions with
10 requests per minute. The adapter uses API v4 and an `X-Auth-Token` environment
secret. The verified account exposes twelve free competition codes: `BSA`, `ELC`,
`PL`, `CL`, `EC`, `FL1`, `BL1`, `SA`, `DED`, `PPL`, `PD` and `WC`.
[Policies](https://docs.football-data.org/general/v4/policies.html) and
[competition matches](https://docs.football-data.org/general/v4/competition.html).

## Professional VALORANT

Riot's [documented public VALORANT APIs](https://developer.riotgames.com/docs/valorant)
expose game content, match IDs/player match lists, ranked leaderboards and platform
status. They do not document a complete professional tournament calendar. The
[official VCT data partnership](https://grid.gg/riot-games-and-grid-announce-exclusive-global-esports-data-partnership/)
uses GRID. Its [current plans](https://grid.gg/plans/) describe commercial access
scoped by agreement; historical Open Access announcements do not prove that a
new account can obtain all VCT data free today.

[PandaScore plan reference](https://developers.pandascore.co/docs/plan-reference)
lists past/upcoming/running VALORANT match endpoints for all plans. Free fixtures
are distinct from paid historical team/player stats and detailed rounds.
[Pricing](https://www.pandascore.co/pricing) lists 1,000 requests per hour for
free accounts. The adapter sends a Bearer token header and bounded pagination.
The VALORANT plan reference does not promise a real-time live-data tier.
The collector requests past, upcoming and running match lists on each bounded
poll; a running series is a match-level observation, not round telemetry.

When a score is unavailable or incompatible with the common schema, collection
reports that limitation and preserves the original response without inventing
scores. The first authenticated VALORANT collection retrieved 124 records across
past, upcoming and running endpoints: 106 canonical matches and 18 announcements
pending normalization. Fifteen announcements lacked resolved opponents and
three lacked a usable start time. These announcements remain in the private raw
archive; the strict match table does not replace missing teams with placeholders.

Football's endpoint does not supply a trustworthy completion instant in the
current mapping: `completed_at` is the collection-time upper bound. PandaScore's
`end_at` is used when supplied; otherwise it also uses that conservative bound.
Repeated polling ignores availability/completion timestamps when deciding whether
business content changed. A correction consisting only of completion time is
therefore not currently versioned. This limitation remains explicit even after
the authenticated field checks.

## Expanded football coverage

The [free football-data coverage](https://www.football-data.org/coverage) includes
Premier League, La Liga, Bundesliga, Serie A and Ligue 1, plus the World Cup,
European Championship, Champions League and Brazilian Serie A. Its
[free pricing](https://www.football-data.org/pricing) allows ten calls per minute
with delayed scores/schedules. Current and historical availability must be
verified per account before a data collection or backtest is declared complete.
The first authenticated six-competition collection produced 2,132 matches across
`PL`, `PD`, `BL1`, `SA`, `FL1` and `BSA`, with zero quarantine records. It contained
529 finished matches, one postponed match and 1,602 scheduled matches. These are
observations from the provider's selected current seasons, not a claim that every
season or international competition has been collected.

[API-Football](https://www.api-football.com/coverage) is a possible complementary
provider for Colombia, Argentina, Mexico, Chile and international tournaments
such as Copa America. Its [free plan](https://www.api-football.com/pricing/)
includes 100 requests per day and all competitions/endpoints, but limits the
available seasons. The adapter is not implemented or account-verified yet.

The proposed collection policy is one explicit competition/season catalogue,
with provider, stable source IDs, account-access status, request budget, score
delay and field coverage. A scheduled poll only requests supported seasons and
changed windows. Provider-scoped identities remain separate until a reviewed
cross-provider mapping exists. Unsupported competitions remain visible as
coverage gaps instead of empty datasets labelled as complete.

PandaScore's tournament and league directories can enrich the existing match
adapter, subject to actual account coverage. Fixtures/results are enough for
event research, but do not establish round/player telemetry or exhaustive
coverage of every community tournament listed by VLR.

## Streaming and redistribution

The free route polls observations and replays changes through file streaming.
It does not manufacture play-by-play, kills, rounds, xG or player telemetry.
Successful provider JSON bodies are archived before normalization under the
workspace's `provider_raw` directory, including newly introduced fields or bodies
that later fail the canonical schema. Credentials/request headers are excluded.
API archives remain private runtime artifacts and are not included in snapshots.
Original fictional fixtures may
be committed; provider databases and logos do not become MIT data.
