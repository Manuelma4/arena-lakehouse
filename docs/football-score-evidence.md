# Football score evidence

National knockout coverage needs an explicit score basis. The existing canonical
match contract retains football-data's `fullTime` pair. That pair is a provider
result, not a guarantee of a 90-minute football score.

According to the provider's [score documentation](https://docs.football-data.org/general/v4/overtime.html),
`fullTime` is the running/final aggregate, `regularTime` describes 90 minutes,
`extraTime` counts goals during extra time, and `penalties` counts shootout goals.
In a shootout, the aggregate can therefore be much larger than the match score.
An after-extra-time total requires adding complete regulation and extra-time
pairs; the native extra-time value is not already cumulative.

The retained national capture illustrates the distinction: England–Switzerland
has provider aggregate 6–4, regulation 1–1, extra-time increment 0–0 and shootout
5–3. A regulation forecast must be evaluated as a draw. Its display can separately
show who won the shootout. The source fields must not be renamed or combined
without recording that derivation.

## Contract boundary

Forecast context v2 adds independently verifiable score evidence beside the
existing season/venue context. The existing score anchors still match canonical
records exactly. Canonical event hashes, published snapshots and original
fixtures retain their previous bytes and meaning. V1 context remains readable
for legacy replay; it does not establish a verified regulation target.

The score evidence preserves native stage presence, null values, source status
and duration. It exposes a separately derived regulation pair and reason when
one cannot be used. Finished, explicit regular-time matches may use their valid
full-time pair; extra-time and shootout matches require a complete supplied
regulation pair. Unknown duration, awarded results, incomplete or inconsistent
evidence cannot silently become a sporting regulation result. Missing stages
are not zero, and missing regulation scores are never reconstructed by
subtracting penalties.

Each context package remains bound to a selected publication fingerprint,
source archive hashes and observation clocks. Generation uses retained originals
and needs no additional provider requests. The new contract is a prerequisite
for national Current coverage; it is not permission to activate those records
under the existing forecast methods.

## Required consumer integration

Before expanding Current to national tournaments:

1. Use one verified regulation selector for recent-form and season/venue
   histories, competition goal means, comparator frequencies and settlement.
2. Version football methods and evaluation identities. Keep existing forecast
   and outcome payloads immutable, and keep legacy reports distinguishable.
3. Include score-stage changes in evidence identity. A new interpretation at the
   same source clock must not masquerade as a later source correction.
4. Present regulation, derived match goals and shootout separately in match
   rows, drawers, team history and assistant evidence. Label team aggregates
   with their basis and exclusions.
5. Verify provider selection, raw provenance, temporal cutoffs, duplicate replay,
   stage-only corrections, API contracts and desktop/mobile navigation together.

The isolated national capture must remain unselected until those integration
gates pass. Historic calibration artifacts and VALORANT semantics stay separate.

## Offline package check, 14 October 2026

The retained national workspace was copied into the separate
`data/national-score-context-20261014` directory: 36 files, including two SQLite
databases copied through the online backup API. Only the copy's optional context
pointer was advanced; the original capture and active Current workspace were
not changed. A v2 build produced 155 records with context SHA-256
`60fb0f8149586d77730d843573a89d971cbdb7ef2c204885f31d1b687318e563`.
The independent consumer reader accepted all 155, selecting regulation draws
for the 14 non-regular matches. No provider requests or model calls were made.

The integrated producer run passed 102 score/context/ingestion/publication tests.
One existing symlink test was skipped because Windows lacked the required
privilege. Ruff and diff checks passed. Explicit v1 reproduction in the isolated
copy recreated the original 155-record package byte hash
`464d2b44fc068e6759925d58db1788171ded6119310fd82b92bfc3ab9553b48c`;
the copy's v2 pointer was restored afterward.
