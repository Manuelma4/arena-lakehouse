# Event identity preflight

An event ID belongs to one retained observation across the entire Bronze table.
Match revisions also form an immutable, ordered history. Replay must validate
both identities before it creates a stage job or inserts an observation.

The previous preflight loaded existing rows only for candidate match IDs. A new
match could therefore reuse another match's event ID and pass validation. The
Bronze insert MERGE, keyed by event ID, would skip that accepted observation.
Reconstructing expected Silver and Gold from Bronze could miss this input loss.

The corrected lookup combines complete histories for candidate match IDs with
all retained observations matching candidate event IDs, including historical
revisions of other matches. Both joins are semi joins; their union is deduplicated
by event ID before the existing validator runs. It does not collect the entire
Bronze table. Conflicts preserve the raw candidate and quarantine the entire
batch before stage-job creation. Exact replays and provider retries keep their
existing duplicate semantics.

The local regression uses original synthetic observations, a small relational
double and the actual replay, validator, ledger and quarantine paths. Three
cross-match cases failed against the previous lookup. The corrected suite also
checks historical collisions, both input orders, same-revision conflicts,
unrelated valid observations and provider retry clocks. The final combined
offline run passed 26 tests in 4.64 seconds.

JUnit evidence is retained locally under `data/event-preflight-before.xml` and
`data/event-preflight-final.xml`. These are preflight/control tests. They do not
certify Spark join execution, Delta MERGE behavior, AvailableNow recovery or
million-event throughput. The isolated Linux Spark scenario remains a separate
runtime gate.
