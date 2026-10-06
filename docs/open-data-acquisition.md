# Free data acquisition and our own API

Source checks: 6 October 2026. This is an acquisition design; the OpenFootball and
HTML adapters described below are not implemented.

Arena already has its own FastAPI in
[Lakehouse Copilot](https://github.com/Manuelma4/lakehouse-copilot): `/matches`,
`/competitions`, match/competition details, `/teams`, `/predictions`, and evidence
endpoints read validated lakehouse snapshots. Owning that API does not create a
new source of match facts. We can avoid paid feeds by collecting reusable open
datasets and using permitted free provider endpoints.

## Verified sources

| Source | Free access and verified coverage | Limits |
| --- | --- | --- |
| [OpenFootball football.json](https://github.com/openfootball/football.json) | Raw JSON without a token; [CC0](https://github.com/openfootball/football.json/blob/master/LICENSE.md). European leagues include England, Spain, Germany, Italy and France. Verified samples: [Premier League 2026/27](https://raw.githubusercontent.com/openfootball/football.json/master/2026-27/en.1.json), 380 rows; [Brazil Serie A 2026](https://raw.githubusercontent.com/openfootball/football.json/master/2026/br.1.json), 380 rows. | A season schedule is not a complete set of results. Some rows lack scores or times. |
| OpenFootball LATAM files | [Colombia 2025](https://raw.githubusercontent.com/openfootball/football.json/master/2025/co.1.json): 200 Apertura rows; [Argentina 2025](https://raw.githubusercontent.com/openfootball/football.json/master/2025/ar.1.json): 240 first-semester rows; [Libertadores 2025](https://raw.githubusercontent.com/openfootball/football.json/master/2025/copa.l.json): 155 rows. | These checked files do not establish complete full-year or current-season coverage across LATAM. Missing scores occur for past dates too. |
| [OpenFootball worldcup](https://github.com/openfootball/worldcup) | No-token Football.TXT downloads; [CC0](https://github.com/openfootball/worldcup/blob/master/LICENSE.md). [2026 source](https://github.com/openfootball/worldcup/blob/master/2026--canada-usa-mexico/cup.txt) includes explicit UTC offsets; historical World Cups include Latin American national teams. | This is structured text, not the same JSON schema. Coverage of World Cups does not imply domestic leagues or Copa America coverage. |
| [PandaScore](https://developers.pandascore.co/docs/plan-reference) | Existing adapter collects past, upcoming and running VALORANT match lists. These endpoints are included in the free Fixtures plan; an account token is required. | Paid tiers cover detailed historical stats/rounds. Running-match observations do not establish real-time round telemetry. Live account coverage remains unverified. |

The [football.json README](https://github.com/openfootball/football.json#frequently-asked-questions--answers)
describes daily JSON generation at 05:00 UTC, but upstream Football.TXT updates
are not automatically daily. Measure observed freshness; a generated file is not
proof that every result has been updated.

VLR remains a design reference. Its [terms](https://www.vlr.gg/terms), sections 3
and 5, restrict automated access and systematic extraction into a database without
written permission, and prohibit unauthorized scrapers. A community scraper or
wrapper does not establish permission to collect or republish VLR data. An HTML
adapter would require a source that explicitly permits that use, or permission
covering collection and redistribution. No VLR scraper is implemented here.

## Schema and time precision

The checked JSON contains `name` and `matches`; rows have `round`, `date`,
`team1`, `team2`, optional `time`, and optional `score`. Scores can be an object
with `ft`/`ht`, or a two-integer array: both occur in Colombia's current sample.
There are no native match/team IDs or per-row observation timestamps in these
samples. JSON `time` values are not accompanied by a timezone.

The current canonical contract requires a timezone-aware kickoff instant and a
completion/availability instant for finished matches. Do not convert a date-only
fixture into a fabricated midnight kickoff. Keep source dates and precision in
a staging contract; resolve documented timezone rules and daylight saving before
conversion. Date-only rows need a future contract/UI extension or stay outside
canonical match research. Extra-time and shootout outcomes need explicit score
semantics before feeding a football win/draw model.

## Concrete acquisition path

1. Maintain a small explicit source catalogue: repository, file, competition,
   season, license, precision, expected coverage and last successful check.
   Begin with one European competition and one LATAM file.
2. Fetch allowed raw URLs once daily with timeouts, bounded retries and a single
   worker. Cache bodies and SHA-256; use `ETag`/`If-None-Match` or
   `Last-Modified`/`If-Modified-Since` when supported. A `304` creates a poll audit,
   not duplicate match revisions. Pin commits for reproducible replay.
3. Archive source bytes before parsing. Record URL, commit when known, collection
   UTC time, response validators, checksum, parser version and license separately
   from normalized match rows. Never put tokens in archives or exports.
4. Allocate persisted provider-scoped team IDs using reviewed aliases. Allocate
   match IDs from competition/season/round/ordered team IDs plus an explicit
   occurrence discriminator. Exclude kickoff and score from identity so schedule
   corrections preserve the match. Quarantine ambiguous collisions; do not use
   row indexes or silently merge names across providers.
5. Validate score variants, timezone, identity and statuses explicitly. An absent
   score on a past date means unknown coverage, not a confirmed scheduled match.
   Append business changes as revisions through Bronze/Silver/Gold; preserve
   unsupported raw rows and report quarantine/coverage counts.
6. Export the versioned package and serve Gold through our existing FastAPI.
   File polling followed by `AvailableNow` demonstrates streaming ingestion;
   its freshness remains bounded by source updates and our polling interval.

Set `observed_at` to actual first collection of each revision. A repository commit
or historical match date does not prove we knew its result at kickoff. Where
completion is unknown, collection is a conservative upper bound, explicitly
labelled; do not subtract an assumed match duration. Importing old results today
cannot support an honest historical point-in-time backtest without historical
availability evidence.

Adding OpenFootball requires coordinated producer/consumer provider allowlists
and a precision/provenance contract; it cannot be relabelled `synthetic` or
`football-data`. The current public web exporter permits only original synthetic
data. A future CC0 publication mode must preserve real-source provenance and
license metadata. Until then, the public showcase remains fictional and the
existing authenticated adapters can run locally. See [provider decisions](providers.md)
and [snapshot contract](snapshot-contract.md).
