# Data-provider decision

Verified against official documentation on 6 October 2026. API connectors are
implemented and tested against explicitly synthetic protocol-shaped fixtures.
No user account or token was supplied; authenticated live responses remain unverified.

## Football

[football-data.org](https://www.football-data.org/client/register) offers a free
tier for basic fixtures, results and league tables across 12 competitions with
10 requests per minute. The adapter uses API v4 and an `X-Auth-Token` environment
secret. Default `PL`; `FL1` can be selected where account coverage permits it.
[Policies](https://docs.football-data.org/general/v4/policies.html) and
[competition matches](https://docs.football-data.org/general/v4/competition.html).

## Professional VALORANT

[PandaScore plan reference](https://developers.pandascore.co/docs/plan-reference)
lists past/upcoming/running VALORANT match endpoints for all plans. Free fixtures
are distinct from paid historical team/player stats and detailed rounds.
[Pricing](https://www.pandascore.co/pricing) lists 1,000 requests per hour for
free accounts. The adapter sends a Bearer token header and bounded pagination.
The VALORANT plan reference does not promise a real-time live-data tier.
The collector requests past, upcoming and running match lists on each bounded
poll; a running series is a match-level observation, not round telemetry.

When a score is unavailable or incompatible with the common schema, collection
must report that limitation, never invent scores. API timestamps and fields need
verification with the user's actual account before claiming coverage.

Football's endpoint does not supply a trustworthy completion instant in the
current mapping: `completed_at` is the collection-time upper bound. PandaScore's
`end_at` is used when supplied; otherwise it also uses that conservative bound.
Repeated polling ignores availability/completion timestamps when deciding whether
business content changed. A correction consisting only of completion time is
therefore not currently versioned; actual field coverage needs provider verification.

## Streaming and redistribution

The free route polls observations and replays changes through file streaming.
It does not manufacture play-by-play, kills, rounds, xG or player telemetry.
Successful provider JSON bodies are archived before normalization under the
workspace's `provider_raw` directory, including newly introduced fields or bodies
that later fail the canonical schema. Credentials/request headers are excluded.
API archives remain private runtime artifacts and are not included in snapshots.
Original fictional fixtures may
be committed; provider databases and logos do not become MIT data.
