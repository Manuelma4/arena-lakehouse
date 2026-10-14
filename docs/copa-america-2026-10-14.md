# Copa América 2024 source adapter

The reviewed source is [OpenFootball's Copa América archive](https://github.com/openfootball/copa-america),
at commit `902e4c51c282244ab924676b3bc471b4040de39a`. Its
[`2024--usa/copa.txt`](https://github.com/openfootball/copa-america/blob/902e4c51c282244ab924676b3bc471b4040de39a/2024--usa/copa.txt)
contains 32 match records and sixteen national-team names. This is community
research data, separate from the live provider feed and Current forecasts.

The source is covered by its pinned [CC0 license](https://github.com/openfootball/copa-america/blob/902e4c51c282244ab924676b3bc471b4040de39a/LICENSE.md).
Source SHA-256: `fe63e5ef5b0390cb15654c5020866d0747c751b9b98ffb477debcce0a3575cdc`.
License SHA-256: `36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673`.

## Interpretation

The adapter handles this reviewed Football.TXT edition, not arbitrary text files.
It retains the original bytes, source URL, checksum and actual collection clock.
Group headings and knockout rounds become source filters. Supplied numeric UTC
offsets permit derived UTC timestamps; they do not establish actual kickoff or
completion times. No match numbers are invented from bracket comments.

Four penalty shootouts retain separate goal and penalty tallies. The final keeps
the source's pre-extra-time 0–0 score and after-extra-time 1–0 score as separate
stages. They are not added together. Goal commentary remains in the raw archive;
it is not exposed as verified player telemetry.

The final line says **Sat Jul 14**, although 14 July 2024 was a Sunday. The explicit
month/day is retained and this weekday inconsistency is disclosed rather than
silently corrected. The source's 20:00 UTC-4 remains a reported clock, not a claim
that the match actually started at that instant.

## Actual acquisition and verification

The eleven-source SQLite store was copied using the online backup API into a new
isolated workspace. Its raw archives were copied unchanged. One unauthenticated
GET, one attempt, collected the pinned Copa source at
`2026-10-14T09:17:52.500266Z`: 32 accepted records, zero quarantine. No unrelated
source was refreshed or assigned a new collection clock.

The published package is `data/openfootball-copa-research-20261014`, dataset
`b70234738d4a05e0`, with 2,914 records and twelve editions. Its independently
validated fingerprint is
`705def1e23ed25045b83430a2b38bb54b63eb3310c7c1fc0ade5a7d3ed884d2f`.
All 2,882 earlier records are identical. The original acquisition's twelve files
and the consumer's eleven fixture files retain their hashes.

216 producer tests passed, including 29 new Copa cases. The independent consumer
passed 69 reader tests. An isolated wheel build/install confirmed both packages
contain the exact source bytes and can parse/read the actual new package without
relying on the editable checkout. Ruff and whitespace checks passed.

All 32 added rows remain prediction-ineligible. Twenty-four source clocks cross
to the following date when their explicit offset is applied. This does not
reconstruct actual kickoff delays, completion clocks or pre-match availability.
No Current forecast or live-provider state was changed.

The consumer's document with this filename records the UI checks and private
delivery. The original JSON fixtures and earlier research exports remain intact;
the overall objective remains active and unfinished.
