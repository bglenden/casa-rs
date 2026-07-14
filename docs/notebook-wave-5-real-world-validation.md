# Notebook Wave 5 Real-World Validation

Truth class: implementation evidence
Last reality check: 2026-07-14
Wave issue: #414

Wave 5 validates the production scientific-notebook and assistant workflow
against real user-like inputs. The deterministic default gates remain offline;
tests named `OptIn` require explicit local inputs or a live ChatGPT
subscription and skip otherwise.

## Wave 5A: live subscription chat (#415)

The production GUI has exercised ChatGPT subscription login, model selection,
two consecutive turns, cancellation, durable transcript reload, and backend
resume or an explicit handoff. Wave 5A repairs found during that review include
logout, compact adaptive notebook chrome, chronological notebook insertion,
current nonce/profile attachment on every turn, a visible two-minute
no-activity failure, and immediate surfacing of `turn/start` JSON-RPC errors.

The retained opt-in entrypoints are:

- `just assistant-live-gui`
- `CASA_RS_CODEX_LIVE_SMOKE=1 swift test --package-path apps/casars-mac --filter AssistantDiscussionTests.testOptInCodexSubscriptionSmoke`

## Wave 5B: scientific documents and source retrieval (#416)

### Public-paper fixture

- Title: *Unlocking ultra-deep wide-field imaging with sidereal visibility averaging*
- Authors: J. M. G. H. J. de Jong et al.
- Publication: A&A 694, A98 (2025), 9 pages
- DOI: <https://doi.org/10.1051/0004-6361/202452492>
- Article and license statement: <https://www.aanda.org/articles/aa/abs/2025/02/aa52492-24/aa52492-24.html>
- Direct PDF: <https://www.aanda.org/articles/aa/pdf/2025/02/aa52492-24.pdf>
- License: CC BY 4.0, stated by the publisher on the article page
- Downloaded for disposable local validation: 2026-07-14T04:21:09Z
- SHA-256: `4a805a8f41c420cfc594e500bfc94a1453ea568dd5267007153cd3db40e73a64`
- Size: 4,862,071 bytes
- Repository policy: the downloaded PDF is not committed; opt-in validation
  receives its local path through `CASA_RS_WAVE5B_PUBLIC_PDF`.

### Current local evidence

The production PDFKit -> host request -> UniFFI -> SQLite/FTS5 path indexed all
9 pages. A search for the 3000-hour estimate returned page 1, whose publisher
text independently states a data-volume reduction up to a factor of 169 and a
14-fold computing-time decrease. The same index run retrieved the shipped
radio-interferometry primer and `crates/casa-notebook/src/corpus.rs` with source
path, line range, and revision identity. Dirty live checkouts are labeled
`<HEAD>+dirty` rather than falsely claiming their content is the clean commit.

The following focused evidence is green:

- deterministic PDF page citation, unreadable/unsupported diagnostics,
  replacement, removal, and stale-hit eviction
- the real 9-page public PDF plus baseline and current-source retrieval through
  the production persistence boundary
- exact-nonce `corpus.search` and `source.search` project-MCP calls, including
  scientific section and source commit/line metadata
- all assistant core tests: 27 executed, 4 opt-in skips, 0 failures
- current `just verify` equivalent: workspace SPDX/fmt/clippy/tests green (the
  two localhost HTTP tests were rerun outside the shell sandbox after its
  expected socket denial), Python package 100 passed and 2 skipped
- full `CasarsMacUITests` app/test bundle: build-for-testing succeeded; the
  foreground run remains consolidated for the review window

The production refresh UI retains the Rust index report and local extraction
diagnostics. Its ordinary context row stays compact; details are available in
AI settings and in debug JSON.

### Pending external-agent acceptance

The opt-in test
`AssistantDiscussionTests.testOptInCodexSubscriptionUsesScientificAndSourceCorpusTools`
requires a live Codex agent to call both nonce-bound retrieval tools and cite
the paper page plus source path/revision/lines. Running it sends selected paper
excerpts and current source excerpts to the user's ChatGPT subscription. It
must therefore be run only after explicit approval for that external egress:

```sh
CASA_RS_CODEX_LIVE_CORPUS=1 \
CASA_RS_WAVE5B_PUBLIC_PDF=/absolute/path/to/paper.pdf \
CASA_RS_SOURCE_ROOT=/absolute/path/to/casa-rs \
swift test --package-path apps/casars-mac \
  --filter AssistantDiscussionTests.testOptInCodexSubscriptionUsesScientificAndSourceCorpusTools
```

The consolidated GUI gate is run after the Wave 5B interaction changes have
accumulated, following the repository policy that foreground XCUITests run in
one exclusive batch.

## Bounded refactor pass

The 2026-07-14 pass was bounded to assistant corpus ingestion, refresh state,
project MCP retrieval, and their tests. No public application-developer API or
parallel implementation path was introduced: PDF/text extraction remains
host-owned, indexing/search remains Rust-owned, and the project MCP remains the
single agent boundary. The report/diagnostic projection is package-internal and
has one producer and one GUI/debug consumer path. No refactor edit was warranted,
so a separate pre-refactor checkpoint and migration guide are not applicable.

The reviewed numeric bounds were retained as real policy constants rather than
dataset or machine tuning: the 2,000-byte FTS chunk is a claim-local retrieval
unit, the 32-hit MCP maximum is a bounded tool response, and the two-minute
watchdog is an agent no-activity UX timeout. This work adds no science-data,
document-size, memory, worker, or executable-location limit. There is no corpus
benchmark in the repository; the real 843-document/13,759-chunk checkout-plus-
paper run completed successfully and serves as functional timing evidence only.
