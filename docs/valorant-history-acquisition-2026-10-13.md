# Bounded VALORANT history acquisition

A separate research workspace collected ten pages through the existing durable
PandaScore worker on 13 October 2026. Current's selected publication and provider
status were unchanged. The acquisition did not create or reconcile forecasts.

| Measurement | Result |
| --- | --- |
| Started | 2026-10-13 14:06:40 UTC |
| Successfully published | 2026-10-13 14:07:17 UTC |
| Request budget | 40, including retries |
| Requests used | 30 |
| Source rows | 1,009 |
| Accepted changed rows | 999 |
| Unsupported source occurrences | 10 |
| Cycles | One |
| Selected canonical snapshot | `snapshot-1a9e42fc839dd9d0-70e02400` |

The workspace is `data/valorant-history-20261013-pages10`; its source scope is
ten pages of each of `past`, `upcoming` and `running`. Upcoming/running responses
are retained because the existing publication contract validates the complete
declared scope. This is bounded match-level history, not an in-game feed or a
complete tournament archive. Source occurrences and accepted native match IDs
are different populations; neither is a count of usable evaluation targets.

## Source and clock semantics

The [official past-match endpoint](https://developers.pandascore.co/reference/get_valorant_matches_past)
is documented as available to all customers. PandaScore's
[pagination contract](https://developers.pandascore.co/docs/pagination) permits
at most 100 records per page. The existing adapter starts at page one and does
not claim a stable cross-request snapshot or complete time interval.

Private raw responses retain request/receipt clocks, endpoint identifiers and
checksums. The canonical observation clock is sampled after collection. Historical
kickoff and source end times are not substituted for acquisition timestamps.
The worker reserves every request in its durable budget, validates candidates
and calendar evidence, then selects an immutable publication. No validation was
weakened to admit this larger capture.

## Research boundary

The consumer's read-only history audit checks native identities, agreeing
calendar links, resolved series outcomes and chronological history eligibility
before any fitting. Its stricter research cohort must not be compared directly
with earlier counts that allowed canonical rows absent from the current calendar.

The new workspace is included in the private state export inventory, without
changing the restored application's Current path. Provider response bodies stay
out of source history. A later deeper acquisition needs a separate historical
contract for bounded page windows and overlap/conflict accounting; changing the
Current calendar's phase/page contract is not required for this capture.

## Subsequent consumer experiment

The consumer froze a separate protocol and fitted a retrospective calibration
pilot on this capture: 252 calibration targets and 63 held-out targets. Its
independent source and numerical audit passed 79,408 comparisons. All 66 files
in this producer workspace remained byte-identical after fitting and auditing;
no additional provider requests were made for those steps.

The fitted temperature is at the declared grid boundary, and six nonempty time
blocks cannot support the planned uncertainty interval. This experiment is
exploratory and does not activate Current forecasts or demonstrate prospective
accuracy. Full results and the frozen protocol are in the consumer repository's
`docs/valorant-calibration-2026-10-13.md`. The private state export carries both
this source workspace and the distinct fitted artifact.
