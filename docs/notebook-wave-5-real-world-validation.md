# Notebook Wave 5 Real-World Validation

Truth class: implementation evidence
Last reality check: 2026-07-16
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

## Wave 5C: automatic incremental project corpus (#421)

Project documents are reconciled when a real project opens and after debounced
recursive filesystem events. The watcher is only an event hint: Rust-owned
SQLite fingerprints over path, type, size, mtime, ctime, and file identity are
the durable correctness boundary. The host reads/extracts only paths returned by
the Rust plan, supplies the complete source snapshot for rename/delete removal,
and marks failed or concurrently changing sources so their last valid content
survives and is retried. Watcher-triggered refresh is project-only; baseline and
release/live-source layers remain independent. Manual refresh performs the same
incremental project planning while also refreshing those shared layers.

Focused evidence on 2026-07-15:

- 11 Rust corpus contract tests passed, including metadata no-op, preserved-
  mtime/atomic identity, failure retention, rename/delete, and schema-v2
  multi-page migration.
- 5 native Swift corpus tests passed, including a PDF-bearing no-change refresh
  with zero content/PDF/OCR work, real Workbench startup plus automatic edit
  retrieval, burst coalescing, `documents/` creation recovery, and transient
  disappearance/retry with symlink and unsupported-input exclusion.
- the compact diagnostic records metadata reads, content reads, PDF extractions,
  and OCR calls for each refresh without adding routine full-width UI.

## Wave 5D: versioned standard radio-astronomy corpus (#420)

The v1 selection is the 27-deck 2026 NRAO workshop epoch plus the open Springer
book; 2024 remains fully inventoried but excluded as superseded, and the paid
ASP book is explicitly excluded. Brian authorized NRAO bundling and confirmed
this smaller selection in issue #420. The clean-install app resource contains
2,314 external pages/slides plus the CASA-RS primer and requires neither the
Oracle checkout nor network access.

Focused evidence on 2026-07-15:

- the self-contained verifier accounted for all 55 candidate sources, checked
  all 28 selected content files/digests, and matched the 2,314-page/slide pack;
- the installed notice and per-source manifest record contributors, complete
  license identity/URL, origins, and the normalized-text/image-omission
  modification notice, including explicit attribution for the Springer book;
- the recorded network audit reached all 28 authoritative selected origins with
  zero failures;
- 3 native Swift pack tests passed: clean-install load and cited retrieval,
  tamper rejection, and baseline upgrade preserving project documents and an
  assistant conversation;
- the compact pack measured 3,087,373 bytes versus 359,670,733 bytes of selected
  original PDFs; clean debug ingestion/indexing took 2.79 seconds and produced
  a 14,782,464-byte SQLite index;
- rendered Perley geometry slide 15 was visually compared with its normalized
  text. Targeted corrections restore `H_0`, `delta_0`, `nu`, and `nu_F` where
  OCR had confused zero/degree and Greek/Latin glyphs.
- a production ChatGPT-subscription agent completed two separate bounded
  baseline searches after the ingestion refactor in 24.97 seconds and repeated
  the independently checked locators `Interferometry and Synthesis in Radio
  Astronomy.pdf, page 806` and `Perley-Geometry-SIW2026.pdf, slide 15`; no shell
  or web tool was used.

## Production notebook/task/Python/plot round-trip (#417)

The reusable acceptance entrypoint is:

```sh
just notebook-roundtrip-gui
```

On a configured dedicated macOS worker, use
`just notebook-roundtrip-gui-remote` instead. The remote runner checks out the
exact pushed revision, keeps Cargo/Xcode state and full result artifacts on the
worker's configured external storage, and copies only the sanitized JSON report
back into the local ignored `.gui-test/remote/` directory. This avoids taking
the development Mac's foreground focus while preserving the same production
acceptance path. Disposable projects live under the dedicated, unprotected
`~/.casa-rs-gui-tests/` directory by default; use
`CASA_RS_GUI_TEST_PROJECT_BASE` to choose another unprotected parent directory.
They intentionally do not live under `~/Library` or inside the XCTest runner's
app container because either location makes the Workbench request cross-app-data
permission. The sandboxed XCTest runner alone has a home-relative read/write
exception for this test directory; the production Workbench target does not.

It is deliberately opt-in and requires an interactive macOS session. The
harness requires a logged-in Codex CLI using the existing ChatGPT subscription,
chooses the normal user/system Python through `scripts/resolve-python.sh` unless
`CASA_RS_GUI_TEST_PYTHON=/absolute/path/to/python` is supplied, and rejects
metered API-key configuration. It builds the real `simobserve`, `msexplore`,
frontend-service, app, and XCUITest executables before announcing one exclusive
foreground window.

The launched test owns its entire disposable project and performs these steps
without user parameter entry or receipt inspection:

1. Ask the live agent to use the nonce-bound project corpus and canonical task
   tools, require a genuine citation, and obtain a typed `simobserve`
   suggestion.
2. Append the answer once at the chronological notebook tail, open the existing
   task tab directly, prove all seven proposed non-defaults—and no sampled
   default—carry the compact AI provenance decoration, approve the task's normal
   safety control, and run it.
3. Generate `products/wave5c-synthetic.ms` with the real task using an ALMA
   compact family request, Band 6, three pointings, and a deliberately tiny
   0.00001-GiB target suitable for acceptance testing. Verify receipt schema v2,
   the recorded product, and later parameter reload without re-execution.
4. Select the resolved project Python, record implementation/version/package
   identity and its environment fingerprint, retain one intentional
   `RuntimeError` receipt, replace the exact source, and successfully execute
   and then regenerate a small angular-resolution calculation and Matplotlib
   figure. Verify two immutable successful receipts, exact source hashes, and
   PNG artifacts.
5. Generate a real UV Coverage explorer plot, save it explicitly, change to
   Amplitude vs Time, and update the same notebook visualization. Verify two
   distinct 960-by-600 PNG assets, source references, latest/previous revision
   UI, enlargement, and direct reopen into the Plot explorer with the latest
   preset.
6. Terminate and relaunch production `casars-mac` twice. Verify the cited pin,
   conversation, notebook Markdown, task receipt/parameters, Python revisions,
   plot revisions, and direct task/plot reopen behavior after process loss.

On success, the test-owned project is removed. The Xcode result bundle and a
sanitized machine-readable evidence summary remain at
`apps/casars-mac/.gui-test/NotebookRoundTripGUI.xcresult` and
`apps/casars-mac/.gui-test/NotebookRoundTripGUI.report.json`. On failure, the
project is retained and the same command can run the focused restart-only
diagnostic with `CASA_RS_NOTEBOOK_ROUNDTRIP_RESUME_AFTER_TASK=1`; that mode is
for diagnosing a consolidated failure, not acceptance evidence.

The live work exposed and repaired four production-boundary defects: output
visualizations were parsed as raw Markdown in rich mode; the data-first
MeasurementSet plot backend had no encoded image for notebook storage; nested
accessibility identifiers hid revision and explorer controls; and plot reopen
restored the preset but left the explorer on its Summary surface. Focused rich
document and Workbench-store regressions now cover these boundaries, while the
opt-in XCUITest covers real rendering, persistence, and process restart.

## Complete TW Hya production tutorial journey (#418)

The reusable acceptance entrypoint is:

```sh
just tutorial-journey-gui
```

The normal dedicated-worker entrypoint is
`just tutorial-journey-gui-remote`. It uses the same production app and
XCUITest path while keeping the exclusive foreground window on the remote Mac.
The harness preflights the installed ChatGPT-subscription Codex CLI and the
selected Python, rejects metered API configuration, and verifies the exact
committed tutorial template before taking focus.

The authoritative uninterrupted run passed on the remote Mac mini at revision
`3aeefa55712939cc49d803824ca4d70c096e39c6` on 2026-07-16: one test executed in
715.887 seconds with zero failures. The complete Xcode result and sanitized
report remain on the worker at
`/Volumes/Extra Storage (not encrypted)/casa-rs-gui-worker-state/artifacts/20260716T230016Z-3aeefa557129-tutorial-journey-gui/`;
the report is also copied to the ignored local
`apps/casars-mac/.gui-test/remote/TutorialJourneyGUI.report.json`.

The run independently demonstrated:

- immutable template manifest and Markdown digests, learner-copy creation, a
  persisted learner note, and unchanged template sources;
- exact approval of the 435,742,720-byte NRAO archive with SHA-256
  `f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2`,
  real download/digest/safe extraction in 453.191 seconds, a succeeded
  `tutorial.acquire.twhya-calibrated` receipt, and discovery of the staged
  MeasurementSet;
- a real UV Coverage explorer plot explicitly stored as one notebook
  visualization with `msexplore` reopen provenance;
- tutorial overrides loaded into the normal imager tab, a real succeeded
  receipt-schema-v2 imager run, ten recorded image/PNG products, and parameter
  reopen after process loss without re-execution;
- two successful immutable executions through `/opt/homebrew/bin/python3`, two
  byte-distinct Matplotlib figures, and visible previous/latest revisions;
- live bounded baseline and current-source searches with independently matched
  citations, one native approval for the agent-requested calculation, and one
  AI snapshot appended at the chronological notebook tail;
- full application termination and relaunch with the same backend session,
  preserved conversation/notebook/tutorial/plot/Python/task state, followed by
  removal of the test-owned project only after the evidence report was written.

The acceptance work exposed only harness gaps after the production behavior
was already correct: a stale cell-tail assertion did not reflect Markdown AI
notes, SwiftUI exposes native approval through its visible button label inside
a combined accessibility container, and the post-restart notebook opens at its
chronological tail and therefore needs an upward scroll to reopen the original
tutorial task. Each gap is now asserted on the real production path. No
approved #418 check was deferred.

## Bounded refactor pass

The #418 pass used pre-journey checkpoint `dfdda7373` and was bounded to the
opt-in TW Hya GUI acceptance harness, its dedicated remote entrypoint, and the
two package-internal native approval accessibility identifiers it exercises.
The delta adds no public application-developer API, compatibility surface,
runtime fallback, or second production implementation: the test composes the
existing tutorial, notebook, explorer, task, Python, corpus, App Server, and
project-MCP owners directly. The broad alternative—extracting another generic
live-journey framework from the #417 and #418 tests—would increase indirection
without consolidating production behavior, so no refactor edit was warranted.
The exact archive size/digest and tutorial science values are provenance and
acceptance inputs; wait bounds and scroll distances are automation interaction
policy rather than dataset- or machine-planning heuristics. Public API remains
zero before and after, no migration guide applies, and no performance benchmark
covers or is warranted for this opt-in acceptance-only surface. The green
authoritative journey, `just docs-check`, and stale fallback/compatibility scan
are the verification evidence.

The #417 pass used checkpoint commit `9417f2457` and was bounded to the
package-internal MeasurementSet plot-document -> SwiftUI render -> notebook
visualization -> explorer-reopen path plus its acceptance harness. The narrower
test-only alternative was rejected because it would preserve the production
storage defect. Public application-developer API remains zero before and after;
the persisted visualization and receipt contracts are unchanged. Discovery
found two image inputs at the store boundary: the new explicit rendered image
and a retained optional fallback to legacy `MeasurementSetPlotResultSummary`
bytes. Every in-repo caller now supplies the rendered image, the fallback was
deleted, and the package-internal method requires that single input. The result
is one storage implementation instead of two, with nine fallback-construction
lines removed and no shim or migration burden outside the package. The separate
whole-window evidence renderer remains intentionally distinct because it waits
for asynchronous view settling and supports a caller-selected HiDPI scale; it
does not store notebook products. No performance benchmark covers AppKit image
encoding, and none is warranted for this user-triggered acceptance-scale path.
The 960-by-600 plot product is the existing explicit render contract, the
0.00001-GiB MeasurementSet is test-owned fixture sizing, and timeouts/scroll
distances are automation interaction policies—not dataset- or machine-planning
heuristics. Focused store/rich-document tests, Xcode test compilation, the
restart diagnostic, and the final opt-in live journey provide the verification
evidence.

The #417 architecture review found no dependency-direction, public-API,
persisted-format, or provider-contract change: Rust still owns scientific plot
data, SwiftUI owns native rendering, and `casa-notebook` owns visualization
revision persistence. The test-adversary review identified three shallow-proof
risks in the first acceptance draft. The final journey therefore requires a
successful cited `corpus.search` activity, independently recomputes both Python
source hashes recorded in receipt schema v2, and reads the stored Python and
MeasurementSet plot artifacts back as distinct PNG byte streams. No accepted
check was deferred. The reality-sync review found `ARCHITECTURE.md`, ADR-0007,
and the provider-contract documentation consistent with the implementation;
`TESTING.md` and this procedure are the only documentation updates needed, and
there are no generated contract artifacts or follow-up drift items.

The #420/#421 corpus pass used checkpoint commit `8231c5bc9`. The broad bounded
scope was the package-internal ingestion operation and its Workbench/test
callers; watcher implementation, SQLite contracts, and provider/public APIs
were left unchanged. Discovery found two ingestion entry points: a legacy
full-extraction wrapper and the planned incremental operation. The wrapper and
its project-layer replacement fallback were deleted, every in-repo caller now
supplies the source inventory/extraction plan to the single `collect` operation,
and tests commit project-source snapshots through the same canonical index
boundary as production. Package-internal ingestion operations fell from two to
one; public application-developer API remained zero before and after. No shim,
alias, or fallback was retained. The consolidated 40-test assistant/corpus run
passed (5 explicit live-test skips); the separately enabled production corpus
test also passed. No corpus benchmark exists, so the clean-pack timing above is
the relevant measured performance evidence. The 600 ms debounce is an explicit
event-coalescing UX policy, not a dataset- or machine-tuned planner constant.

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

## Architecture, test-adversary, and reality-sync reviews

The dedicated architecture review found no dependency-direction, public
provider-contract, runtime-authority, or approved-scope drift. Swift remains the
host extraction/watcher owner, Rust remains the fingerprint/index/citation
owner, and project MCP remains the bounded agent boundary. Its one blocking
finding was incomplete CC BY-NC attribution/change disclosure for the bundled
Springer-derived text; the generated manifest and installed `NOTICE.md` now name
all authors, the full license and source, and the normalized-text/image-omission
modifications. No follow-up issue or ADR update is required. Confidence: high.

The adversarial test review mapped every #421 and #420 acceptance check to
executable or generated evidence. It found that the transient-read guarantee
was split between a Rust retention test and Swift extraction diagnostics. A new
end-to-end regression now removes a changed file after inventory, proves its
last valid search hit survives, excludes symlink/unsupported inputs with bounded
diagnostics, and proves the recreated source is retried and replaces the old
hit. The review found no remaining shallow or approval-scope gaps. Testing
readiness: ready.

Reality sync updated the maintained architecture, testing, security,
scientific-notebook design, and macOS README reality dates. The committed pack,
inventory, origin audit, generated bindings, tests, and docs describe the same
2026-plus-book selection and automatic incremental project-corpus behavior.
Drift classification: no ADR change; generated artifacts and maintained docs
current; no follow-up issue.

## Final #420/#421 gates

Current final evidence on 2026-07-15, after the refactor and review fixes:

- `just verify` passed outside the shell sandbox: SPDX, formatting, workspace
  clippy with warnings denied, the complete Rust workspace test suite, and the
  Python package suite (100 passed, 2 skipped). The first sandboxed attempt was
  blocked only by localhost-listener permissions. A later transient
  `casars-imager` shared test-lock failure passed immediately in isolation and
  did not reproduce in the green full rerun.
- `python3 scripts/assistant-corpus-pack.py check` accounted for all 55
  candidate sources; `git diff --check` passed.
- `just gui-test` built the native app/test bundle and passed the single
  consolidated foreground batch: 25 tests executed, 1 skipped, 0 failures.

## Wave 5 combined closeout

Wave 5 closes with every issue in the approved #414 scope implemented and no
accepted outcome or acceptance check deferred. The combined evidence is carried
by immutable production journeys rather than by rerunning account-backed work:

- #415/#416 exercised the real ChatGPT subscription, nonce-bound project MCP,
  public-paper lifecycle, baseline retrieval, and current casa-rs source
  retrieval. Citations were independently checked and the resulting defects
  were repaired before user review.
- #417 passed the dedicated remote production notebook/task/Python/plot
  round-trip at `790f44bf44a501d4bd87f5f8d275aeac8d7af75f`: one XCUITest,
  zero failures, 210.703 seconds. Its artifact is
  `20260716T203939Z-790f44bf44a5-notebook-roundtrip-gui` on the GUI worker.
- #418 passed the dedicated remote TW Hya production journey at
  `3aeefa55712939cc49d803824ca4d70c096e39c6`: dataset acquisition and digest
  verification, notebook/task replay, Python/plot work, cited assistant use,
  and application reopen all completed with zero failures in 715.887 seconds.
  Its artifact is
  `20260716T230016Z-3aeefa557129-tutorial-journey-gui` on the GUI worker.
- #420/#421 shipped the approved 2026-workshop-plus-book baseline corpus and
  proved automatic incremental add/change/atomic-replace/rename/remove project
  document reconciliation, including retention and retry across transient
  read failures.

Brian's final production-app review found the scientific interactions generally
worked as expected. The reported objective defects—assistant activity
visibility, conversation tail following, font zoom, task-suggestion notebook
pinning, edited-parameter provenance, and clickable citations—returned to agent
ownership and were repaired with focused regression coverage. His remaining
comments were interaction refinements rather than rejected Wave 5 outcomes.

The authoritative gates remain the 2026-07-15 `just verify` run, the green
25-test consolidated `just gui-test` batch above, the two production remote
journeys, and hosted PR CI. No production behavior changed after those GUI
journeys. A later attempt to rerun one deterministic XCUITest on the newly
provisioned macOS 26.5 remote worker was blocked by the operating system's
`SystemPolicyAppBundles` (App Management) modal when XCTest snapshot the
application menu hierarchy. The final diagnostic artifact is
`20260717T012714Z-5a0877f560f0-gui-test`. Test-only menu workarounds were removed
instead of distorting the product. This host-policy limitation is recorded as a
GUI-runner exclusion; it does not replace or invalidate the current green local
batch or the green remote production journeys.

The bounded refactor, architecture, test-adversary, and reality-sync evidence
recorded above covers the complete changed surface. No public API, persisted
format, provider contract, dependency direction, or runtime model changed
during closeout, and no additional refactor or ADR work is required.
