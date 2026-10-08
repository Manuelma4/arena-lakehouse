# StatsBomb historical detail source: permission gate

Reviewed on **8 October 2026**. Status: **not acquired for Arena**. This review
resolved the unreadable-agreement gap in [the earlier source review](free-match-detail-sources.md).
It did not collect competitions, matches, lineups, events or player data.

## Decision

Do not enable StatsBomb as a public match-detail provider or redistribute its
JSON through Arena's API, export packages or Git repositories under the permission
currently verified. Private, noncommercial research and a public analysis are a
different scope from republishing a match/player dataset. An attributed analysis
may be possible, but Arena has not verified the registration process, an approved
logo asset or permission for the proposed detail publication.

This is a source-specific gate. The existing OpenFootball Research corpus uses
CC0 and remains separate; this review does not change its permissions or invent
a StatsBomb-to-OpenFootball identity mapping.

## Primary agreement, fully reviewed

The official repository now resolves to `hudl/open-data`. The five-page agreement
was downloaded from an exact repository commit, extracted with `pdftotext` and
visually checked across all pages. It states an update date of **8 September 2023**.

- Section 1.1 permits analysis and research.
- Sections 1.2.1 and 7 restrict reproduction, distribution and transfer of data
  to others; section 7 requires prior written consent for otherwise prohibited use.
- Section 1.2.2 excludes commercial exploitation of data or derived analysis.
- Section 1.4 requires the StatsBomb logo on published analysis. The repository
  README additionally requests source credit.
- Section 2.2 asks users to provide their name and email before access. No personal
  information was submitted and no registration was verified.
- Sections 3.2-3.4 provide no accuracy or completeness guarantee.

The grant is a **custom research agreement**, not CC0, CC BY, MIT or an unrestricted
open database license. A public GitHub file or a free download does not establish
permission for Arena's proposed public data products.
[Pinned agreement](https://github.com/hudl/open-data/blob/4b73468fc5b0f1950f9f66fada70ad3a4f9327cb/LICENSE.pdf),
[pinned repository README](https://github.com/hudl/open-data/blob/4b73468fc5b0f1950f9f66fada70ad3a4f9327cb/README.md).

The registration/resource URL in the agreement currently redirects to a Hudl
product page. The README's media-pack URL redirects to Hudl's asset portal, which
the browser could not retrieve. Neither redirect resolves the outstanding scope
or supplies a new license.
[Resource URL](https://statsbomb.com/resource-centre/),
[media-pack URL](https://statsbomb.com/media-pack/).

## Measured permission capture

Exact source ref: `4b73468fc5b0f1950f9f66fada70ad3a4f9327cb`.
The GitHub commit response reports commit time `2026-09-07T16:27:15Z`.
That is repository metadata, not a match date or the data collection time.

| Archived object | Actual observation time (UTC) | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| GitHub `commits/master` response | 2026-10-08T09:13:03.898439Z | 6,784 | `baaa5c056e3beb331e29c6eae1b4238a33e81ce525ab4c44681d6849f441b41a` |
| Pinned `README.md` | 2026-10-08T09:13:04.328199Z | 1,753 | `98a757466c4d931dba4b6c4290c55c2fe74e4550d7455bbe05fd884dc831aff3` |
| Pinned `LICENSE.pdf` | 2026-10-08T09:13:04.933444Z | 165,130 | `a5462e69aeb71a39268b760b110c5c2190a2e0cee3015ba976f66bd71f6c2bb4` |

The direct capture made **three HTTP requests**, all 200, no redirects, totaling
**173,667 bytes**. There were **zero dataset requests**. Six separate browser page
accesses were used for first-party permission review; those are not part of the
direct capture count. Two browser accesses failed on raw/API rendering, and the
asset-portal access failed. The permitted direct downloads resolved the PDF
rendering problem without bypassing an access restriction.

Local evidence is ignored under `data/open-details-2026-10-08/`:
`permission-requests.json`, `source-ref.json`, the three original bodies,
`LICENSE.txt`, five rendered agreement pages and `permission-decision.json`.
These artifacts are not present in a fresh clone and are not redistributed.
No credentials, sports data API calls, model calls, Spark process, new dependency,
scheduled collector, GitHub Actions run, commit or deployment was used.

## Observed data coverage

The README describes competition-season inventories, match inventories, lineups,
events and selected 360 files. This is a documented repository structure, **not
an observed sample**. No completed match ID, season ID, lineup size, player ID,
event count, shot field, pass location, coordinate range or score convention was
measured. Arena therefore has no accepted StatsBomb detail contract.

## Next small contract proposal

The useful first implementation is a **source publication decision**, based on
the metadata actually captured, rather than a guessed event parser:

| Field | Verified value or rule |
| --- | --- |
| `provider` | `statsbomb-open-data` |
| `scope` | `historical_research` only |
| `source_ref` | Exact 40-character Git commit above |
| `permission_document_sha256` | Exact agreement body hash above |
| `permission_observed_at` | Actual agreement observation time above |
| `permission_kind` | `custom_research_agreement` |
| `raw_publication_allowed` | `false` under this verified scope |
| `registration_verified` | `false` |
| `data_sample_verified` | `false` |
| `public_enabled` | `false` |

This proposal is documentation only; existing schemas and adapters were not
changed. Before a private research pilot, resolve registration and the intended
analysis scope. Before a public raw detail endpoint, obtain a corresponding
written grant or choose an explicitly redistributable source. Only then capture
one exact competition-season inventory and one completed match's lineups/events,
within four dataset requests and 25 MB, with immutable originals, checksums,
native source IDs and separate observation and business clocks. Build the actual
detail contract from those files, preserving unknown values and documented score
semantics. Keep all historical participants separate from current rosters,
current fixtures and prediction claims.
