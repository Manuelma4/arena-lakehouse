# Consumer recovery of the producer's private captures

Two new GitHub checkpoint clones started without private data or `.env` files.
The normal private recovery command restored the pinned snapshot into them.
All 943 transferred files matched their manifest checksums, and all 48 SQLite
databases passed integrity checks before consumer startup. Credentials were
excluded, and restored runtime paths pointed to the new clones.

The installed consumer then verified 2,132 football and 510 VALORANT records,
seven historical editions containing 1,941 matches, unchanged original forecast
payloads, and saved investigations. A fresh frontend installation/build and 14
actual desktop/mobile scenes passed, including provider CDN logos and a saved
historical comparison. These are captured-source recovery checks; no provider
poll, model execution or producer Spark workload occurred.

Read the consumer's `docs/clean-recovery-2026-10-12.md` and portable evidence for
the exact scope, retained failed verifier attempts and source pins. Detailed
artifacts live in its private `data/resume-20261012/recovery/` directory and are
included by the next private snapshot export.

This closes the bounded consumer clean-clone recovery subset, not whole-producer
installation, second-PC verification or release acceptance. Continue free outcome
reconciliation and genuine human review. Spark million-event/restart, wider SLO,
authenticated Databricks and full release gates remain open. Do not install,
download, start, probe or test Ollama, dispatch workflows, merge main or publicly
deploy.
