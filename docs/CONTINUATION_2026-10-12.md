# Continuation — 12 October 2026

Both private repositories remain on `checkpoint/2026-10-08-reprise`. This
entry supersedes 9 October. Both clones started clean; fetch and fast-forward
found no new remote changes. The consumer's full handoff with this same filename
is authoritative for UI, evidence contracts and delivery verification.

Follow-up verification in the consumer passed a 600-request, ten-minute warmed
local read observation across eight routes: 75 valid attempts per route, no
failures or gaps, maximum route p95 109 ms. Independent journal arithmetic and
unchanged source/ledger checks passed. Two earlier observer versions/windows
remain diagnostics; 42 focused observer regressions passed.

A current consumer wheel and dependencies also installed in a new Python 3.11.9
environment outside both checkouts. `pip check` and the final offline installed
read verifier passed: 44 GETs plus resource, semantic and cleanup checks. Initial
verifier failures exposed urllib3's import-only IPv6 bind probe; the final tool
suppresses that probe with the network audit still strict. No application code
or wheel change was needed. This fixture/absent-source subset is separate from
full private-state restoration, frontend setup and release acceptance.

See consumer `docs/local-read-observation-2026-10-12.md` and
`docs/installed-read-api-2026-10-12.md` with their portable evidence. Raw records
are included in private exports. All 215 decisions and 122 outcome/evidence rows
remain unchanged; no provider polls or model calls occurred in this follow-up.
Wider SLO, Spark/restart, human review, Databricks and full clean-install/release
gates remain open.

Latest follow-up delivery: private draft `private-state-20261012-read-verification`,
asset `arena-read-verification-20261012.zip`, 943 files, 48 SQLite online backups,
34,438,205 bytes. SHA-256:
`2cfaff415905f1ebfcb1b294919e97c1781e781eda1e1215443405e43ea17215`.
Normal consumer `-VerifyOnly` downloaded and verified it again without changing
state/configuration. The pointer pins producer
`cb99e8d1fb2de3b5668e5a4dc39c5e0a5886285c` and consumer
`0296d1c6a5884c0634a2c8a2ffb7b91ebdea5b1a`; later delivery commits retain them
as ancestors. Both repositories are private, Pages disabled and implementation
workflow-run counts zero. Pull both branch tips for the final pointer and notes.
Next feasible clean-install task: actual restoration into isolated new clones,
installed-reader/captured-ledger checks, then a fresh frontend build.

One bounded free poll refreshed `data/live-extended-20261008` for each provider:

| Provider | Scope | Requests | Source rows | Changed rows | Unsupported rows |
| --- | --- | ---: | ---: | ---: | ---: |
| football-data.org | PL, PD, BL1, SA, FL1, BSA | 6 | 2,132 | 48 | 0 |
| PandaScore | Past/upcoming/running, five pages per phase | 15 | 510 | 11 | 9 |

Both completed `ready`; no ongoing worker was left running. These are saved
REST observations rather than a guaranteed continuously live feed. Producer
poll logs are at `data/live-refresh-20261012-{football,pandascore}.json`.
Their structured status is also recorded in the consumer's portable evidence.

The consumer reconciled the original immutable forecast cohort. All 215 original
decisions over 108 distinct matches retain their payload checksums. Each football
method now has 42 eligible scored matches on the same sample. Season/home-away
Poisson log loss is 1.000894 and Brier 0.596880; recent form is 1.054916 and
0.634951. The saved frequency reference is 1.046136 and 0.626728. Lower is better;
this small non-random sample does not establish general accuracy. PandaScore has
nine excluded decisions and zero scored estimates; its losses remain null.

Archive now supports typed, pinned historical head-to-head explanations with
retained meetings, W/D/L, separate penalty scores and source-conflict exclusions.
Saved comparisons reopen from their immutable catalogue/edition captures. The
consumer independently compared real Arsenal–Leicester and Spain–Russia
responses against the original licensed source at desktop and mobile widths.
No producer transforms or catalogue contracts changed for this feature.

The consumer recorded 51 passing backend checks, 65 answer/navigation contract
checks, 18 historical comparison/navigation browser checks and 16 forecast
tracking browser checks. Actual local Current/Archive/forecast views were
checked separately from TEST ONLY fixtures. See its dated evidence report;
counts include existing coverage and must not be added to older suite totals.

The private recovery snapshot pinned by the consumer's `scripts/private-state.json`
contains provider observations, licensed originals, all seven historical editions,
saved evidence, the original forecast ledger, later outcomes and session audits.
Pull both clean checkpoint clones and use the consumer recovery script. Adjust
`-LakehousePath` to this clone's folder; do not restore while a preview is running.
Existing local credentials are preserved and excluded from the snapshot.

Verified delivery: private draft `private-state-20261012-continuation`, asset
`arena-continuation-20261012.zip`, 916 files, 48 SQLite online backups and
34,200,915 bytes. SHA-256:
`92b130b7085a0cfe4107f1b444bae5f01eced60b036650252484ca74b7391849`.
The normal consumer recovery script downloaded and verified it again with
`-VerifyOnly` without changing local state. It pins producer
`14a2e1ba77ab56586b44bef17c6ce9af2abe24fd` and consumer
`3f4c0aee18eb9436225d545db1b842839e573c11`, which remain ancestors of the final
delivery commits. GitHub reported zero workflow runs for these implementation
commits. Pull the branch tips to obtain the final pointer and delivery notes.

The Linux Docker engine was checked and was unavailable (missing Desktop engine
pipe). No Spark/container attempt was made. Remaining gates: one-million Spark
and real restart, 120 genuine human answer labels, sustained SLO, authenticated
Databricks and clean-install/release verification. Continue free outcome
reconciliation without retuning against the scored sample.

Keep Current and Archive; no Demo. Only free sources. **Do not install, download,
start, probe or test Ollama.** No main merge, workflow dispatch or public deployment.
Software push workflows remain main-only; Spark remains manual. Read the
consumer's final delivery record before transferring ignored state to another PC.
