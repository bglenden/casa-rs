# Scientific Notebooks, Tutorials, Python, and AI Assistant

Truth class: accepted design
Last reality check: 2026-07-18
Verification: just docs-check
Architecture decision: [ADR-0007](adr/0007-scientific-notebooks-and-assistant-boundary.md)

## Purpose

This document defines the accepted product and implementation contract for the
CASA-RS scientific notebook program. It covers notes, task execution records,
parameter reuse, Python calculations and plots, tutorial datasets, and
agent-neutral AI discussions in the native macOS workbench.

The primary user is a working scientist. A project still opens on its selected
dataset or explorer; notebooks are a first-class project surface rather than a
replacement landing page.

## Project layout

```text
notebooks/
  default.md
  <name>.md
  assets/<notebook-id>/<cell-id>/<execution-id>/*
documents/
  <copied user documents>

.casa-rs/
  notebook-runs/<run-id>/
    receipt.json
    events.jsonl
    stdout.log
    stderr.log
  conversations/<thread-id>.jsonl
  corpus/*
  python/*
  tutorials/<notebook-id>/lock.toml
```

`default.md` is created lazily on the first notebook edit or recorded
operation. Named notebooks use stable UUIDs independent of filenames. Markdown
and assets are visible and portable. Receipts, indexes, locks, environments,
and transcripts are managed state.

The default export contains the Markdown files and referenced assets only. An
advanced export may include receipts. Products, datasets, credentials, Python
environments, and corpus indexes are never included implicitly.

## Markdown and typed cells

The v1 document uses ordinary Markdown plus invisible version markers:

```markdown
<!-- casa-rs-notebook:v1 id=019f... -->

# TW Hya reduction

User-authored prose remains ordinary Markdown.

<!-- casa-rs-cell:v1 id=019f... kind=task -->
```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 3

[parameters]
vis = "data/twhya.ms"
imagename = "products/twhya"
niter = 1000
```
<!-- /casa-rs-cell -->
```

Recognized v1 cell kinds are `task`, `python`, `tutorial`, `assistant-pin`, and
`output`. Notes need no typed wrapper. Unknown comments, fences, Markdown, and
future cell kinds are preserved without interpretation.

Execution cells render only their latest revision at normal prominence.
Earlier immutable revisions remain available under a collapsed **Previous
revisions** disclosure; they do not repeat full-width output in the normal
reading flow.

## Explorer visualization snapshots

Explorer inputs and control panels are not notebook content. MeasurementSet
plots and image views enter a notebook only when the user selects **Save to
Notebook** beside the rendered visualization. The notebook stores a stable
image/plot asset plus the source explorer kind and the parameters required to
recreate the view; it does not embed the explorer's input form.

Selecting a saved visualization enlarges the stable snapshot. **Open in
Explorer** restores its generating parameters into the corresponding explorer,
where the user can make a new preview. Explorer edits never update the notebook
implicitly. **New plot** creates a new notebook visualization, while **Update**
appends an immutable revision to the selected visualization. The prior
revisions remain available through a collapsed disclosure. Table-browser input
forms are never recorded automatically; a separately shaped table excerpt or
other rendered artifact may use this same explicit snapshot boundary later.

Saved artifacts keep the producer-declared pixel aspect recorded in rendering
metadata when shown as a compact preview or enlarged view. Image and equal-axis
scientific renderers therefore choose square output when appropriate, while
ordinary plots may choose another stable aspect. The notebook does not inspect
axis labels or compare units at display time.

The rich editor provides bounded WYSIWYG editing for prose throughout the
complete document, including text before, between, and after typed cells, and
always offers raw Markdown mode over that same complete source. Unsupported
syntax remains intact. Raw HTML is not executed in rich rendering. External
modifications are detected; conflicting dirty edits pause saving and require
reconciliation rather than being overwritten.

Task cells contain sparse ADR-0006 TOML. Selecting or double-clicking the
parameter block opens a normal task tab populated from that intent. A new task
tab loads directly; replacing an already edited task tab first displays a typed
diff and requires confirmation. Rerunning a cell creates a new immutable
execution revision. The latest is shown by default, with previous revisions
available from history.

### Native workbench interaction

Notebooks use the existing project dock instead of introducing another
horizontal navigation pane. A **Notebooks** selector sits beside Datasets,
Files, and History at the bottom of the left dock; choosing a named notebook
opens it in the central tab area.

The central notebook is a continuous, notes-first document. Task parameter
blocks are interspersed with the prose at their Markdown positions and render
like ordinary neutral Markdown code blocks. Execution history is deliberately
quiet: the latest outcome is a small colored status dot and revision label
attached to its parameter block. That affordance expands in place for a compact
result, while double-clicking it opens the associated task tab for full
parameters, products, logs, diagnostics, and revision history. The generic
right inspector is not required for ordinary notebook reading.

## Execution receipts and automatic recording

Every receipt has a stable run ID and records at least:

- schema and provider-contract versions
- initiating surface and notebook/cell IDs
- task or operation identity
- timestamps and terminal status
- sparse user intent and fully resolved parameters
- run-safety classification and approval record
- affected paths, products, output roles, and artifact references
- CASA-log references plus stdout/stderr sidecars
- failure, cancellation, interruption, and partial-product diagnostics

Receipts support parameter replay into the current installed contract. They do
not promise an identical binary, environment, input snapshot, or output.
Contract/default drift is shown before rerun.

Project-aware GUI, TUI, `casars run`, and Python surfaces record all
app-mediated write attempts. This includes tasks, downloads/imports, Python
cells, region or mask changes, and similar persistent operations. Failed,
cancelled, and interrupted attempts remain visible. Direct provider programs
never infer project or notebook state. Recorded direct execution uses `casars
run` with an explicit workspace and, when needed, an explicit existing notebook
filename or stable ID. Workspace resolution uses an explicit directory or the
exact current directory; it never searches parents.

The initiating notebook receives the event, otherwise it is appended to
`notebooks/default.md`. A one-run bypass is available and is never stored in a
parameter profile. If the recorder fails, the scientific operation continues
and the UI presents a durable warning.

## Python cells and plots

Each open notebook may own one persistent project Python kernel. The GUI
supports Run, Interrupt, Restart, and Run All. Creating or repairing the
project environment and installing packages are explicit user actions.

The user kernel runs with the user's normal authority and exposes the installed
CASA-RS Python bindings. Consequently, every Python execution is treated as
potentially mutating and receives a receipt. Ordered stdout, stderr, errors,
interrupts, and environment identity are retained.

Matplotlib figures are captured as PNG and SVG under the execution asset
directory. The code and input references remain in the cell, so **Regenerate**
creates a new execution and artifact revision without destroying the old one.

Receipt schema v2 stores the exact Python source and SHA-256, execution
authority, selected input references, interpreter identity, Python
implementation/version, installed CASA-RS and plotting package versions, and a
validated environment fingerprint. Schema-v1 task receipts remain readable.
Ordered stream events are retained as an immutable JSON execution artifact in
addition to stdout/stderr sidecars.

The normal reading view shows the latest status and scientific figures. Routine
stdout, stderr, environment identity, source hashes, diagnostics, and artifact
paths remain available under **Details**. A failed run is therefore a compact
status row until expanded instead of displacing the surrounding notes.

Python-native explorer data does not round-trip through screenshots. Use
`casars.data.measurement_set_plot(...)` for the shared Rust `casa-ms` numeric
plot document (NumPy series and point provenance), then
`casars.data.plot_matplotlib(...)` for editable figure/axes objects. Use
`casars.imexplore.data(...)` for image planes, masks, coordinate/WCS records,
beam metadata, and overlays, then `casars.imexplore.imshow(...)` for editable
Matplotlib/WCSAxes objects. The optional dependencies are installed with
`casa-rs-python[plot]`; executable-rendered exports remain appropriate for
GUI/TUI parity and regression images.

The coding agent uses the user-selected scientific Python through the selected
Explore, Work, or Full-access authority. Explore has no generic execution;
Work and Full access use Codex's normal visible activity and native approvals.
Reproducible calculations belong in notebook Python cells, where exact source,
inputs, interpreter provenance, output, and plot revisions are recorded.

## Tutorial notebooks and datasets

A tutorial template is a portable folder:

```text
<tutorial>/
  tutorial.md
  tutorial.toml
  assets/*
```

Opening it creates an editable learner notebook while leaving the template
immutable. Tutorial-pack v0 is replaced rather than supported through a
compatibility layer. Existing regression evidence remains usable during
migration.

Dataset entries specify a URI, destination, optional expected size, optional
SHA-256, optional unpack instruction, and optional verification checks. Checks
are executable but skippable and their outcome is recorded.

Acquisition is never automatic. Before approval the GUI shows the scheme,
source and redirects, expected size, project-local destination, checksum, disk
requirement, and extraction plan. The handler registry accepts arbitrary URI
schemes, but v1 ships only `file`, `http`, and `https`; an unknown scheme stays
inert until a handler is installed. No handler delegates to a shell or system
opener.

Only verified content becomes staged. Missing digests require explicit
approval; the computed SHA-256 is then pinned in
`.casa-rs/tutorials/<notebook-id>/lock.toml`. Extraction rejects path traversal,
escaping symlinks, device files, archive bombs, and destination collisions.

## AI discussions and local corpus

The normal notebook state is the full-width notes-first document with no AI
pane. Invoking AI from a notebook, selection, task, output, plot, or source
opens an on-demand, resizable contextual drawer beside the notebook. The drawer
can expand into a first-class central AI tab, and the central tab can dock back
beside the notebook. These are two presentations of the same conversation,
not copied or synchronized transcripts. Closing either presentation preserves
the transcript, unsent draft, scroll position, selected context, running agent
work, and pending approvals.

Conversations are project-owned and stored separately from Markdown. A
conversation opened from a notebook receives that notebook as its primary
attachment and default pin destination, but may explicitly attach other
notebooks, datasets, task tabs, outputs, history entries, documents, or source
trees. A transcript contains only visible messages, citations, timestamps,
agent/model labels, authority preset, used CASA resources, and pin references.
It excludes hidden reasoning and raw App Server or provider envelopes. Agent or
model selection creates an explicit backend-session handoff while preserving
the project-owned visible conversation.

Both drawer and central-tab presentations use a conventional free-form
multiline composer fixed at the bottom. Optional suggested questions are
empty-state aids only: selecting one fills editable composer text and never
sends it or inserts a predetermined user message. Inline AI remains a
transient, locally scoped invocation for a selected cell or artifact; durable
conversation stays in the separate project transcript.
Return sends the current message; Shift-Return inserts a newline. Subscription
model and reasoning-effort selectors remain directly available below the
composer, alongside a compact summary of subscription usage remaining. The
usage summary expands to show the active rate-limit windows and reset times.
Both chat presentations follow new output to the chronological tail. While a
turn is pending, the status names the latest real App Server or tool activity
and shows how long ago that event arrived; it never substitutes a decorative
animation or exposes hidden model reasoning. Command-Plus, Command-Minus, and
Command-Zero adjust, reduce, and reset the shared Workbench font size,
including the transcript, composer, raw notebook editor, and typed task blocks.
Agent, ChatGPT subscription/account state, trust preset, and scientific Python
live behind one secondary settings control. The account row is status, not a
model or billing-action picker: Codex owns sign-in and CASA-RS stores no
credential. Signed-in accounts expose a Log out action in that surface; after
App Server confirms it, the composer remains disabled until the standard Codex
subscription sign-in flow completes again. These controls project the
CASA-owned agent profile; App Server types do not cross into durable project
formats.

When the conversation is closed, a single purple sparkle in the lower-right of
the notebook pane opens it and exposes a descriptive tooltip/accessibility
label. It disappears while the drawer is visible because the drawer owns close
and expand controls. Purple consistently denotes AI invocation and AI-suggested
state; safety warnings such as Full access remain orange. The compact drawer
header shows its primary attachment, history, expand, and close controls.
The assistant automatically knows the semantic state of every
open workbench tab and has standing read-only tools for notebooks, task schemas
and current parameters, explorers, run history, plots, persistent CASA-RS data
types, the radio-astronomy and project-document corpus, and release/live-
checkout source. Users do not manually attach each open tab or corpus before
ordinary questions.

One compact row above the composer summarizes the typed CASA context and local
retrieval sources available through the verified project MCP server. Opening
it shows open-tab resources, corpus layers, source lookup, and the selected
authority preset. CASA records the domain tools/resources and citations used,
but does not claim to know the exact hidden model prompt or provider egress of
a coding agent that may also read files or run commands.

Citations attach to the claims they support. A lightweight preview shows the
relevant document excerpt, page/section, source path/lines/commit, notebook
block, or run provenance; citation rows are clickable and open that preview,
with a direct local-file action when the cited artifact is available. Pins
may add a conclusion, code, task intent, plot, citations, or a transcript link
directly to the chronological end of the notebook from the explicit
**Add to notebook** action.

Answers and suggested actions remain ordinary chat messages until the user
chooses a destination action. **Add to notebook** immediately appends once at
the chronological tail, then the notebook opens and scrolls to the new block;
there is no redundant destination or preview dialog. Task parameters open
directly in the normal task tab with suggested
non-defaults marked by a small purple sparkle with explanatory hover text. A
manual edit clears that parameter's AI provenance immediately. Running the task
uses its ordinary Run workflow.
Plots use the notebook's explicit import/update workflow and downloads use the
acquisition surface. Routine agent steps, commands, and logs stay collapsed by
default. The same suggestion is never duplicated as proposal cards in both
chat and Markdown.

### Interaction precedents

The 2026-07-12 interaction review selected this hybrid from established
notebook and document-chat behavior rather than inventing a CASA-RS-only
conversation model:

- [Jupyter AI](https://jupyter-ai.readthedocs.io/en/v3/users/) uses a persistent
  side-panel conversation with notebook/cell context and explicit code
  insertion into the active notebook rather than a second action workspace in
  chat.
- [VS Code Chat](https://code.visualstudio.com/docs/agents/chat-view)
  permits one chat session to move between the secondary side bar, editor, and
  window, while [notebook AI](https://code.visualstudio.com/docs/agents/guides/notebooks-with-ai)
  keeps inline chat scoped to targeted cell work and reviews proposed changes
  in the notebook/editor with Keep/Undo controls.
- [Databricks Genie Code](https://docs.databricks.com/aws/en/genie-code/use-genie-code)
  uses a contextual side pane and can [maximize the same work into a full-page
  chat](https://docs.databricks.com/aws/en/genie-code/full-page).
- [Deepnote Agent](https://deepnote.com/docs/deepnote-agent) separates
  notebook-level side chat from [block-local AI editing](https://deepnote.com/docs/ai-code-editing).
- [NotebookLM](https://support.google.com/notebooklm/answer/16179559?hl=en)
  establishes explicit source selection, claim-local citation preview, and
  promotion of selected chat results into durable notes.

These references are interaction precedent only. CASA-RS retains its own
project-owned transcripts, typed scientific context, canonical task/notebook
surfaces, and approval boundaries.

CASA-RS launches the official Codex App Server over stdio behind its own
agent-session interface. The initial path uses Codex's ChatGPT subscription
login and account state; CASA-RS neither handles the credential nor requires a
metered API key. A future ACP adapter may add OpenCode or another agent without
changing transcripts, CASA MCP tools, notebook pins, or the authority model.

The local corpus has four layers:

1. an optional versioned, redistribution-cleared baseline pack
2. project papers and documents copied into `documents/`
3. release-matched CASA-RS documentation and source
4. an optional live-checkout source overlay keyed to its Git commit

The installed baseline is the compact versioned pack described in
`assistant-standard-corpus.md`: the full 2026 NRAO workshop epoch, the open
Springer synthesis-imaging book, and the CASA-RS primer. It is an app resource
installed once, not copied into project `documents/`; page/slide labels and
numbers remain exact retrieval citations.

SQLite holds manifests, chunks, metadata, citations, and FTS5 indexes behind a
replaceable retrieval interface. Project-document refresh first compares a
metadata fingerprint (path, type, size, mtime, ctime, and filesystem identity),
then reads and content-hashes only the changed sources. Debounced recursive
filesystem events request this reconciliation automatically; startup and the
manual refresh action use the same path. No-change refresh performs no content
read, PDF extraction, or OCR. Rename/delete reconciliation and changed-source
replacement are atomic, while an unreadable or concurrently changing source
keeps its last valid indexed content for a later retry. Project watcher events
do not rebuild the baseline or release/live source layers, and no periodic full
scan is used. Extraction and OCR are local by default; cloud OCR is a
per-document opt-in. A
real local embedding model is optional future work only after retrieval
evaluation shows a material benefit; its dimensions are model metadata rather
than a fixed architecture constant.

Scientific factual claims cite source pages or sections. Implementation claims
cite paths, symbols, line ranges, and release/commit identity. Retrieved text
and web content are evidence, never instructions.

The trusted CASA MCP server gives the agent typed access to bounded metadata,
statistics, samples, previews, source lookup, corpus retrieval, and canonical
CASA actions. **Explore** is read-only and disables project instructions,
shell, writes, and network. **Work** deliberately enables the trusted project,
the user's shell and selected scientific Python, with native Codex approvals.
**Full access** is an explicit expert opt-in with a persistent indicator.

The standing read-only surface includes all open tab projections: notebook
source and selection, task identity/schema/current values/default differences,
explorer configuration and selected data products, Python cells and retained
outputs, plots, and processing history. It also includes task and parameter
documentation plus persistent table, MeasurementSet, image, coordinate, and
measures semantics. Selected open-tab excerpts and corpus retrieval share the
active backend model's deterministic resource plan; unselected tabs consume no
payload and there is no fixed fallback. The assistant may retrieve from these sources as needed
without per-read approval. CASA shows what domain context is available and
which CASA resources and citations were used, not a fictional exact record of
all bytes sent by the coding agent to its model.

Task suggestions open the canonical task tab with non-defaults highlighted.
The ordinary task Run action remains the single approval and launches normal
validation, safety checks, and notebook recording. Generic command, file,
Python, and network approvals remain owned by Codex App Server. **Add to
notebook** is itself the explicit CASA action and appends once without another
confirmation; other typed CASA mutations retain their canonical confirmation.

## Prototype-first wave gate

Every wave begins with a real, launchable `casars-mac` view backed by
deterministic fixture adapters. All meaningful controls are live. The prototype
must cover the primary flow plus failure, cancellation, retry, and restart
states, publish accessibility identifiers and debug JSON, and produce visual
evidence. It does not contact a network, run tasks or Python, invoke a model, or
write durable project state.

The common review surface is planned as:

```text
swift run casars-mac --show-prototype <notebook|python|tutorial|ai>
swift run casars-mac --dump-debug-state --show-prototype <kind>
```

The prototype is a mandatory entry gate:

1. open a draft PR with the live prototype
2. review the staged or debug app
3. record screenshots, accessibility coverage, and debug-state evidence
4. obtain explicit interaction approval
5. replace fixture adapters incrementally with production adapters

The accepted view and interaction model remain in the product. Fixture
adapters remain for deterministic tests. Material UX changes return to the
prototype gate. Production integration cannot be silently substituted for or
started ahead of this review.

## Delivery waves

### Wave 1: notebooks and receipts

Prototype: rich/raw Markdown, named-notebook selection in the existing project
dock, whole-document rich/raw editing, notes-first task cells whose parameter
blocks open fixture task tabs, tiny expandable execution indicators, simulated
running/succeeded/failed/cancelled receipts, retry/restart revision history, and
external-edit conflicts.

Production: `casa-notebook`, versioned Markdown/receipt contracts, atomic
writes, project locking, cross-surface recording, exports, GUI notebook list and
editor, and task parameter replay.

Wave 1 production uses one Rust-owned store across GUI, TUI, `casars run`, and
the generated Python task wrappers. Pending attempts retain per-run advisory
leases so a live operation is not recovered as interrupted; a released lease
is recovered on the next project load. Rust-parsed Markdown cells, including
authored cells with no receipt, drive the native task blocks. Replay opens a
fresh canonical schema-driven task tab when needed, replaces a clean matching
target directly, and shows a typed diff confirmation before replacing a dirty
target. Provider/default drift remains a warning rather than an exact-replay
claim. GUI and TUI region/mask writes also record immutable operation receipts.
Direct provider binaries do not infer a project or record implicitly;
project-aware execution is routed through an app surface with explicit
workspace context. CLI and Python task callers can select an existing notebook
filename or stable ID explicitly; otherwise recording routes to `default.md`.

### Wave 2: Python and plots

Prototype: a full-width continuous notes-first notebook with inline expandable
Python cells, live editor, Run/Stop/Restart/Run All, ordered stdout/stderr/error
output behind compact execution details, latest-first execution history with
collapsed prior revisions and failures,
deterministic matplotlib-style PNG/SVG figures, regeneration, notebook
insertion, explicit MeasurementSet/Image Explorer visualization snapshots,
enlargement, parameter restoration, New plot/immutable Update actions, and
producer-declared stable visualization aspects without display-time unit
inference, plus exact-source approval for AI-proposed code. Deterministic
`happy-path`, `failure`, and `nonresponsive` fixture states exercise retry,
interrupt, forced restart, immutable revisions, and approval invalidation.

Current reality: Wave 2 Phase A was accepted on issues #227 and #370. Phase B
now uses an explicit project environment and one supervised persistent user
kernel per notebook; records exact schema-v2 Python receipts and ordered
output; captures immutable PNG/SVG execution assets; and presents environment
creation and plotting-package installation only as explicit user actions.
`casa-ms` owns the shared numeric MS plot-data contract consumed by both the
UniFFI GUI projection and PyO3. Python-native image planes include WCS, masks,
beam metadata, and overlays. Wave 4 agent Python is a separate concern and uses
the user-selected or inherited scientific environment under the active agent
authority preset.

Production explorer snapshots are Rust-owned `casa-notebook` records. MS and
image explorers expose **Save to Notebook**; **New** creates a new identity and
**Update** appends an immutable asset revision. Markdown contains only the
quiet latest image link and identity marker. Managed metadata retains source
references, canonical typed reopen parameters, contract and renderer identity,
and every prior revision. Notebook previews enlarge without launching an
explorer; **Open in Explorer** restores the saved parameters.

### Wave 3: tutorials and acquisition

Prototype: learner notebook, annotations, section progress, dataset acquisition
through download/verify/unpack/ready, cancellation/resume, checksum and disk
failures, and task-parameter loading.

Current Phase A reality: `casars-mac --show-prototype tutorial` launches an
in-memory TW Hya learner notebook in the existing Notebooks dock. Compact
inline controls expose the exact source, resolved redirects, size, project
destination, checksum, disk, extraction, and optional-check plan before a
deterministic Download/Verify/Unpack/Ready simulation. Attempt generations make
cancel/resume/restart and retry stale-safe; offline, checksum, unsafe-archive,
and insufficient-disk states never appear staged. A ready fixture enables a
parameter block that opens the fixture task tab directly; the loaded tutorial
overrides are identified in the form without an intermediate preview. This
projection performs no file, network, archive, task, provider, or durable
project operation and is not the tutorial-template v1 or acquisition contract.

Production: tutorial-template v1, clean v0 migration, URI handlers, verified
project-local acquisition, safe materialization, optional checks, and the TW Hya
end-to-end walkthrough.

### Wave 4: assistant and corpus

Prototype: purple sparkle activation, persistent model/reasoning-effort and
usage-remaining controls, consolidated agent/account/access/Python settings,
Explore/Work/Full-access controls, context-availability disclosure, streaming cited
answers, collapsed agent activity, direct task opening, one notebook-tail
append, corpus/indexing states, rate limits, and cancellation/restart states.

Approved Phase A reality: the user approved the replacement prototype and its
runtime/authority interaction on 2026-07-13. It begins from a full-width
notebook with no AI pane. A purple lower-right sparkle reveals a conventional
free-form chat in a resizable contextual drawer; the same fixture conversation
expands into a central AI tab and docks back without losing its draft or state.
The footer keeps model, reasoning effort, and subscription usage remaining in
view; a single settings popover contains Codex agent, ChatGPT subscription
status, trust preset, and scientific-Python fixtures. A compact context panel lists open tabs and
the standing CASA MCP/corpus/source capabilities without claiming an exact
provider-egress manifest. Routine agent activity is collapsed. **Add to
notebook** immediately appends once at the chronological tail, then opens and
focuses the new notebook block; there is no intermediate popup or duplicate
proposal card. Task suggestions open
the normal task tab with non-defaults marked. Full access requires explicit
confirmation and remains visibly indicated. Deterministic rate-limit,
cancellation, restart, accessibility, debug-state, and zero-production-call
fixtures remain retained for deterministic CI.

Phase B implementation: `casa-notebook` retains agent-neutral transcript, citation,
immutable pin, context-use, and scientific-receipt contracts. A CASA-owned
agent-session interface initially launches the official Codex App Server and
uses ChatGPT subscription authentication; raw metered OpenAI APIs are excluded.
`casa-rs-agent-profile/v1` carries invariant guidance, bundled CASA skill
identity, the nonce-bearing project MCP identity, authority vector, adapter
capabilities, and resume metadata. Explore starts from a neutral directory and
behaviorally denies project instructions, writes, execution, and network. Work
uses the trusted project plus the user's shell and selected scientific Python
with native Codex approval events. Full access is an explicit expert opt-in.
The project MCP supplies typed open-tab/task/data/notebook/tutorial and cited
corpus/source operations. SQLite/FTS5 indexes the cleared baseline, project
documents, release source/docs, and commit-keyed live overlay; the removed
fixed feature hash is not an embedding. A future ACP adapter may add OpenCode
without changing these CASA-owned contracts. The Xcode app stages a compact
release/commit-labelled source snapshot, while local checkout runs add a live
commit overlay. See
[`assistant-security.md`](assistant-security.md) for the executable boundary.

### Assistant resource plan and retained limits

The active backend model must report both input capacity and output reserve.
CASA-RS treats one UTF-8 byte in the encoded JSON content as one conservative
capacity unit, including escaping; it does not guess token ratios. The planner
first subtracts the backend output reserve,
the exact CASA runtime instructions, the encoded durable conversation, and
the encoded metadata for selected context projections. It then assigns one
share to each selected tab, one additional priority share to the active
selected tab, and one share to corpus retrieval. Consumers that need less than
their share are satisfied first and the unused capacity is redistributed.
Integer remainder units go to the active tab first, then stable tab order, then
corpus retrieval. Unselected tabs receive zero units.
Missing capacity, checked-arithmetic
overflow, or reserves larger than capacity produces an explicit unavailable
plan with zero context and corpus allocation; there is no fixed fallback.

The remaining numeric bounds have separate owners and meanings:

- The 2,000-byte corpus chunk target is a citation-locality correctness bound:
  FTS hits remain independently reviewable near one prose page. It is not a
  result budget; the resource plan limits both hit count and returned text.
- The MCP input-schema maximum of 32 search hits is a protocol abuse bound.
  A request is accepted only when the current resource plan allows a lower or
  equal count. The final serialized hit array, including citation locators and
  all other metadata, is measured against that allocation; the last hit text is
  shortened on a UTF-8 boundary when necessary.
- The 10-second App Server startup timeout and 120-second response-activity
  timeout are named UI liveness policies. They do not allocate prompt or
  retrieval capacity.
- The 600-millisecond project-document watcher interval is an event-coalescing
  policy. Prepared reconciliation generations, rather than the delay, provide
  correctness against stale work.
- PDF OCR renders at no more than two source points per output pixel and caps
  the largest dimension at 4,000 pixels. This is the local Vision extraction
  envelope that bounds transient bitmap work; it does not truncate extracted
  text or alter corpus result planning.
- The model-list request limit of 100 is the adapter's discovery page size;
  model selection remains backend-owned and this value is not a context
  capacity input.

Representative measurements recorded on the reference Apple Silicon
development system on 2026-07-18:

- The removed policy always exposed a 65,536-byte total / 16,384-byte-per-tab
  envelope without using model capacity, instructions, history, metadata, or
  actual item demand.
- With a 32,768-unit model, 4,096 output reserve, 1,900 runtime-instruction
  units, 4,096 conversation units, 768 projection-metadata units, a 12,000-unit
  active notebook, 512-unit task, 4,096-unit Python tab, one unselected
  8,000-unit history tab, and an 8,000-unit corpus demand, the current planner
  allocated 11,534 / 512 / 4,096 context units and 5,766 corpus units. The
  21,908-unit payload allocation plus 10,860 reserved units exactly equals the
  backend capacity; the unselected history used zero.
- Ten thousand executions of that representative plan took 79.94 ms in a
  debug Swift test (about 0.008 ms per plan). This is observational evidence,
  not a performance threshold.
- A metadata-heavy retrieval fixture with a long locator and multibyte text was
  reduced to one valid cited result whose complete serialized array was at
  most its 900-unit plan. A 32-unit plan explicitly rejected the citation
  because its metadata alone could not fit. Exact-nonce retrieval tests still
  returned both scientific and source citations.

## Program acceptance

The TW Hya walkthrough must demonstrate: fork a tutorial; acquire data; edit
notes; run and record a task; reload parameters; execute Python; regenerate a
plot; ask a scientific question with document citations; ask an implementation
question with source citations; approve an AI calculation; and pin its result.

CI uses deterministic fixture agents. Opt-in live smoke covers Codex App Server
with ChatGPT subscription authentication and never requires a metered API key.
OpenCode over a future ACP adapter is the planned second agent, not a Wave 4
implementation target. Timings for startup, indexing, retrieval, recording, and
execution are recorded without imposing a hard performance budget in these
waves.

Each code wave runs `just quick`, `just verify`, the bounded `refactor` pass,
and risk-appropriate architecture, test-adversary, and reality-sync reviews.
Accepted outcomes or checks cannot move to follow-ups without explicit user
signoff.

## Non-goals

- hermetic or bit-for-bit replay
- dataset snapshot/rollback or time-machine semantics
- arbitrary shell cells
- execution or download when opening a notebook
- multi-user collaboration
- a cross-platform GUI
- a personal cross-project document library in v1
- provider-hosted RAG or a required PostgreSQL service
- autonomous AI writes or bulk scientific-data upload
- tutorial-pack v0 compatibility
- completion of all tutorial-content backlog items as a platform prerequisite
