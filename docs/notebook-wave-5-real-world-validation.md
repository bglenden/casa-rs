# Notebook Wave 5 Real-World Validation

Truth class: implementation evidence
Last reality check: 2026-07-15
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
The follow-up production review also repaired real event-backed progress age,
automatic transcript tail following, scalable assistant/notebook task fonts,
clickable citation previews, typed task-cell insertion from **Add to notebook**,
compact sparkle-only AI provenance, and provenance clearing after manual edits.

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
- all assistant core tests: 31 executed, 4 opt-in skips, 0 failures
- current `just verify` equivalent: workspace SPDX/fmt/clippy/tests green (the
  two localhost HTTP tests were rerun outside the shell sandbox after its
  expected socket denial), with the unchanged Python package evidence retained
  at 100 passed and 2 skipped
- full `CasarsMacUITests` app/test bundle: build-for-testing succeeded and one
  consolidated 25-test foreground run completed (18 passed, 1 opt-in skip,
  6 failures). One focused production-assistant run then passed citation
  preview, streaming tail-following, typed task-cell insertion, and provenance
  clearing. The other five failures all exposed tutorial approval actions that
  could remain below the sheet viewport; the sheet now keeps approval and
  cancellation in a fixed footer, and the five affected tutorial workflows
  passed together (5 passed, 0 failures). Together these results account for
  every non-skipped GUI workflow; repository policy permits reusing the 18
  unaffected green results because their surfaces did not change.

The production refresh UI retains the Rust index report and local extraction
diagnostics. Its ordinary context row stays compact; details are available in
AI settings and in debug JSON.

### Live external-agent acceptance

The opt-in test
`AssistantDiscussionTests.testOptInCodexSubscriptionUsesScientificAndSourceCorpusTools`
requires explicit approval because it sends selected paper excerpts and current
source excerpts to the user's ChatGPT subscription. Brian provided that approval
on 2026-07-14. The live run completed in 30.241 seconds: the Codex agent called
both exact nonce-derived retrieval tools and reported the independently verified
169-fold data-volume and 14-fold computing-time reductions from PDF page 1. It
also cited `crates/casa-notebook/src/corpus.rs`, lines 1-61, at revision
`3985f969bfc939ada7ad61934e0d5f3c313cb397+dirty`.

The first authorized run exposed a production adapter defect. CASA-RS supplied
`turn/start.additionalContext` to refresh the nonce-bound profile on every turn,
but initialized Codex App Server with `experimentalApi: false`; App Server
therefore rejected the turn and the UI remained in Thinking state. CASA-RS now
declares the required capability, surfaces a rejected turn immediately, and the
opt-in test distinguishes thread startup, terminal agent errors, tool use, and
turn completion. The passing command was:

```sh
CASA_RS_CODEX_LIVE_CORPUS=1 \
CASA_RS_WAVE5B_PUBLIC_PDF=/absolute/path/to/paper.pdf \
CASA_RS_SOURCE_ROOT=/absolute/path/to/casa-rs \
swift test --package-path apps/casars-mac \
  --filter AssistantDiscussionTests.testOptInCodexSubscriptionUsesScientificAndSourceCorpusTools
```

The later manual review timeout was produced by a Workbench process launched on
2026-07-13, before the `experimentalApi` repair was built. The stale process was
terminated and the current rebuilt app was launched directly for review. Rich
mode now suppresses metadata-only notebook control comments, rendered Markdown
enters editing reliably without swallowing Markdown links, and the purple
notebook-chat affordance has hover help. XCUITest explicitly checks that the
control-comment element is absent while the saved AI note and its chat-only
task action remain usable.

The next manual task-discovery review successfully listed the native
`simobserve`, `simalma`, and `simanalyze` surfaces and produced an Open task
action, but exposed a second boundary defect: `task.suggest` accepted known
parameter names without checking that they were mutually active in the resolved
request mode. The Workbench then applied the suggestion one field at a time,
leaving a partially resolved form with a page of inactive-parameter errors.
The project MCP now exposes mode predicates in `task.schema` and validates the
complete proposed draft before returning an action. The Workbench independently
applies accepted values as one Defaults-based patch and refuses any invalid
draft without opening a task tab. Focused Rust tests cover a valid ALMA family
mosaic and reject the inactive `polarization_basis` combination; a focused
Swift test covers atomic application and the GUI-side refusal boundary.

The consolidated GUI gate is run after the Wave 5B interaction changes have
accumulated, following the repository policy that foreground XCUITests run in
one exclusive batch.

## Bounded refactor pass

The 2026-07-15 follow-up pass was bounded to the assistant progress, citation,
task-pin, parameter-provenance, and font-scaling paths touched by the live GUI
review. The pre-change checkpoint is branch commit `e34eb30b7`. Public
application-developer API remains unchanged: App Server events still enter
through one `AgentSession`, typed task intent still uses `NotebookTaskIntent`,
and parameter edits still flow through the canonical schema-driven setters.
The pass found no duplicate implementation, compatibility shim, runtime
fallback, or scale-dependent planning constant to remove. The two-minute
no-activity timeout is an explicit interaction policy rather than a workload
heuristic; one-second displayed event age and the short scroll animation are UI
presentation intervals. No further refactor edit or performance benchmark was
warranted for this bounded surface.

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
