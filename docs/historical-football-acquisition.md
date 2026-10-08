# Reproducible historical football acquisition

`arena_lakehouse.football_details_acquisition` provides the missing acquisition
step for the independent World Cup 2018 Research package. A fresh clone can
retrieve the public originals and produce the adapter's input descriptor without
a private transfer ZIP, provider key or paid service. Acquisition is an explicit
manual command; it does not add a collector, schedule, GitHub Actions job or
Current-provider join.

The acquisition code is MIT. The source data retains **CC BY 4.0** and the
Pappalardo/Massucco attribution. The downstream adapter includes that attribution,
the license link, modification notice and no-endorsement statement in its package.
See [source permission and retained evidence](wyscout-detail-source.md) and
[the detail adapter contract](historical-football-details.md).

## Pinned originals

| Source | Version | File ID | Filename | Bytes |
| --- | ---: | ---: | --- | ---: |
| [Matches](https://doi.org/10.6084/m9.figshare.7770422.v1) | 1 | 14464622 | `matches.zip` | 645,097 |
| [Players](https://doi.org/10.6084/m9.figshare.7765196.v3) | 3 | 15073721 | `players.json` | 1,737,347 |
| [Teams](https://doi.org/10.6084/m9.figshare.7765310.v3) | 3 | 15073697 | `teams.json` | 27,404 |

The source list, sizes, SHA-256 and MD5 values are fixed in the module and checked
against the original capture report. There is no command-line option to replace
them. The public metadata is requested from
`https://api.figshare.com/v2/articles/<article_id>/versions/<version>`.
Article identity, version, DOI, title, publication date, public-download flags,
authors, exact file declaration, both supplied/computed MD5 values and CC BY 4.0
license name/link must agree before that file is requested.

Metadata bodies are hashed **when acquired**, rather than compared with a
fabricated copy of an earlier response. Their actual bytes and new UTC
observation clocks are retained. An altered source file, license or version is a
failure; a newly captured valid metadata body is a new observation.

Original file SHA-256 pins:

```text
matches.zip  c8f92bb7533e5c127e043cee764c991b5c25b4f5e70a65be931baae0b1765ce9
players.json 877a111cb1005b73df5645e9338bd74fb4b496bace2fbc545a72abb3b73efa2e
teams.json   9f7a4a3b3d92c0be33f40613ad6e6eb4316c3b9771ec74c61a22c9b8ece23a4d
```

## Fresh-clone handoff

Run these commands from the producer repository using its installed Python
environment. Use separate dedicated directories for originals and the normalized
package; both belong under ignored `data/`.

```powershell
python -m arena_lakehouse.football_details_acquisition --output data/historical-football-originals
python -m arena_lakehouse.football_details --matches-path data/historical-football-originals/matches.zip --players-path data/historical-football-originals/players.json --teams-path data/historical-football-originals/teams.json --source-manifest data/historical-football-originals/source-manifest.json --output data/historical-football-package
```

The first command prints a small JSON result with `status: acquired`, request and
read-byte counts, original-byte count and input-manifest SHA-256. The second
command selects only `matches_World_Cup.json` from the pinned archive and prints
the normalized package's dataset version and publication fingerprint. It does
not extract or import the other six competition members.

The consumer's approved package fingerprint must come from that newly built
package. An acquisition performed on another day has its own observation clocks
and therefore must not reuse the earlier session's publication fingerprint.
Replaying the same complete capture preserves its clocks and bytes.

## Output and clocks

A successful originals directory contains exactly eight files:

| Files | Purpose |
| --- | --- |
| `matches.zip`, `players.json`, `teams.json` | Unmodified, pinned original bodies |
| `article-7770422-v1.json`, `article-7765196-v3.json`, `article-7765310-v3.json` | Exact captured public version metadata bodies |
| `acquisition.json` | Sanitized request ledger, bounds, status and input-manifest hash |
| `source-manifest.json` | `historical-football-inputs/1.0.0` descriptor accepted by the existing adapter |

Each input source includes the exact deposit/file IDs, original download URL,
body size/SHA-256/MD5, captured metadata SHA-256, actual `metadata_observed_at`
and `observed_at`, original `published_at`, and license. Metadata observation
must follow deposit publication; file observation must follow metadata
observation. The capture clock is **not** a 2018 match kickoff, a present-day
roster update or proof of pre-match information availability. The separate
historical package preserves those distinctions.

The ledger records request attempts, status, fixed origin/path, bytes actually
read, completed-body hashes and observation clocks. A Figshare redirect has
`body_complete: false` and zero bytes read: its body is not collected. Reported
bytes exclude HTTP headers and any unread response body. They are not a network
traffic bill or a claim that the server returned no other bytes.

## Permitted transport and bounds

The transport uses ordinary public HTTPS with certificate verification. No
credentials, account, cookies, environmental proxy, mirror or automatic redirect
handler is used. It follows at most one **302** from the approved
`ndownloader.figshare.com/files/<file_id>` URL to this independently observed
publisher storage location:

```text
https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/<file_id>/<filename>
```

The original publisher-provided query is followed unchanged in memory. Signed
query strings are never placed in exceptions, printed results, manifests or the
ledger. A different host/path, user information, port, HTTP scheme, fragment,
metadata redirect, alternate redirect status or second redirect is refused.
Authentication failures, rate limits and challenges terminate acquisition
without retry or bypass.

Bounds are nine wire attempts, 100,000 bytes per metadata body, exact declared
original sizes and 3,000,000 total response-body bytes. Unknown-length streams
use a single extra byte to detect overflow, so a failed bounded-size probe may
record the limit plus one byte. The valid originals total 2,409,848 bytes.
Only identity transfer encoding is accepted; compressed response expansion is
not delegated to an unbounded decoder.

The default checked acquisition deadline is 120 seconds; `--deadline-seconds`
may set a positive finite value up to 180. Before and after each request/body
read, the code checks the same monotonic deadline. Each connection/read timeout
is at most ten seconds and is reduced to the remaining budget; the budget is
not renewed by successful reads or additional requests. Body reads use bounded
`read1` operations with the socket timeout updated before each read.

**This is not a guaranteed hard wall-clock cutoff.** Python's synchronous
request opening, including operating-system DNS resolution and response-header
processing, cannot be force-preempted by this command. Socket timeouts bound
individual blocking operations; the deadline is checked when they return.
Filesystem writes also rely on the operating system. An elapsed deadline rejects
the acquisition before input-manifest publication, but the CLI may return later
than the configured deadline when such an operation stalls.

## Immutable publication and failure

An exclusive `.acquisition.lock` prevents overlapping cooperative writers.
Symlinks/reparse points in the output path are refused. A successful acquisition
verifies all bodies before writing, writes through unique temporary files,
flushes them, checks the stored bytes and publishes `source-manifest.json`
**last**. No useful input descriptor is published after a metadata, license,
checksum, redirect, deadline, transport or partial-write failure.

A failure retains a bounded sanitized `acquisition.json` with `status: failed`
when the filesystem can still write it. Already completed local originals may
remain as failure evidence. If manifest publication fails after the success
ledger was written, this invocation replaces only its own ledger with the
failure result. No remote exception text or signed URL is copied into the report.
If local storage itself is unwritable, persisting any failure report is naturally
unavailable; the command still exits unsuccessfully.

Calling the command on a complete existing capture rechecks every pinned
original, retained metadata, descriptor hash, ledger, observation clock and
directory membership. It returns `already_verified` with zero requests and
preserves every existing file byte for byte. This checks integrity, not a
digital signature or independent source authenticity.

Different, unrelated, partial or invalid directories are refused unchanged.
Choose a new dedicated directory for a subsequent acquisition; no recursive
cleanup, replacement of an existing capture, lock bypass, automatic retry or
repair is performed. A pre-existing lock is retained for investigation.

## Verification scope

On 8 October 2026, **96 original TEST ONLY cases passed** in 1.22 seconds.
They cover version/license/download metadata rejection, malformed and duplicate
JSON, exact file pins, direct and approved redirected flow, host/path/status
rejection, no retry, size/request/deadline bounds, response closure, sanitized
errors, partial publication, immutable replay and a full handoff to the existing
detail adapter. The adapter handoff uses original test fixtures, not downloaded
sporting observations. A local socket pair separately exercises the standard
library's final body chunk/EOF behavior; it is not an internet connection.

Ignored test evidence: `data/football-acquisition-test-only.xml`.

A subsequent, separate **actual Figshare acquisition** on the same day exercised
the versioned metadata endpoints, the normal publisher redirects, fresh original
downloads and the documented handoff to the existing adapter. It used the
dedicated ignored directory
`data/open-details-2026-10-08/acquisition-cli-actual/` and completed with
`status: acquired` between **11:45:07.217048Z and 11:45:09.641002Z**. This is an
observed capture interval, not a performance benchmark.

| Actual command measurement | Result |
| --- | --- |
| Wire requests | 9: three version metadata responses, three redirects and three file responses |
| Response-body bytes read | 2,426,541 |
| Original-file bytes | 2,409,848 |
| Original size/SHA-256/MD5 checks | All three passed |
| Acquisition replay | `already_verified`; 0 requests, 0 bytes read; same descriptor SHA-256 |
| Adapter matches / teams / players | 64 / 32 / 736 |
| Adapter starters / bench / substitutions | 1,408 / 1,477 / 381 |
| Preserved unknown-goal fields | 1,944 |
| Structural contract violations | 0 |

The captured metadata bodies and clocks are new observations. They are not
copied from the earlier capture's private descriptor:

| Source | Deposit publication (UTC) | New metadata observation (UTC) | New original-file observation (UTC) |
| --- | --- | --- | --- |
| Matches | 2019-02-26T12:15:54Z | 2026-10-08T11:45:07.574283Z | 2026-10-08T11:45:08.207073Z |
| Players | 2019-05-06T13:14:23Z | 2026-10-08T11:45:08.447485Z | 2026-10-08T11:45:09.047849Z |
| Teams | 2019-05-06T13:08:43Z | 2026-10-08T11:45:09.263024Z | 2026-10-08T11:45:09.587383Z |

Input descriptor SHA-256:
`ef98dff2cb4332e3eb8edb558d7216865f8952195e3996fd6c53f5d523d7db1d`.

The normalized output in
`data/open-details-2026-10-08/acquisition-cli-package/` has publication fingerprint
`4ff3400f27785988b4a4eaf26012717d623509f35a220bd065413928d247de9b`
and `source_as_of: 2026-10-08T11:45:09.587383Z`. It differs from the earlier
`3525a60c19065323...` package because its capture provenance is different, while
the fixed original bodies still match. The running local interface retains the
earlier approved capture during visual review; this acquisition did not switch
its data source.

The actual CLI run establishes the producer acquisition/adapter path. It does
not establish fresh-machine dependency installation, consumer API/UI acceptance,
current rosters, pre-match information availability, complete event telemetry or
model quality. Consumer verification against this new publication is a separate
check. The acquisition ran without a provider key, paid service, model call,
Spark/Docker process, recurring job or GitHub Actions run. Transport rejection,
deadline and failure cases remain demonstrated by the separate TEST ONLY suite;
no deliberate failure traffic was sent to Figshare.
