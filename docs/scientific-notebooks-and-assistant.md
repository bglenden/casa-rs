# Scientific Notebooks, Tutorials, Python, and AI Assistant

Truth class: accepted design
Last reality check: 2026-07-12
Verification: just docs-check
Architecture decision: [ADR-0007](adr/0007-scientific-notebooks-and-assistant-boundary.md)

## Purpose

This document defines the accepted product and implementation contract for the
CASA-RS scientific notebook program. It covers notes, task execution records,
parameter reuse, Python calculations and plots, tutorial datasets, and
provider-neutral AI discussions in the native macOS workbench.

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

Receipt schema v2 stores the exact Python source and SHA-256, `user` or
`ai_worker` authority, selected input references, interpreter identity, Python
implementation/version, installed CASA-RS and plotting package versions, and a
validated environment fingerprint. Schema-v1 task receipts remain readable.
Ordered stream events are retained as an immutable JSON execution artifact in
addition to stdout/stderr sidecars.

The normal reading view shows the latest status and scientific figures. Routine
stdout, stderr, environment identity, source hashes, diagnostics, and artifact
paths remain available under **Details**. A failed run is therefore a compact
status row until expanded instead of displacing the surrounding notes.

Python-native explorer data does not round-trip through screenshots. Use
`casars.tasks.msexplore.data(...)` for the shared Rust `casa-ms` numeric plot
document (NumPy series and point provenance), then
`msexplore.plot_matplotlib(...)` for editable figure/axes objects. Use
`casars.imexplore.data(...)` for image planes, masks, coordinate/WCS records,
beam metadata, and overlays, then `casars.imexplore.imshow(...)` for editable
Matplotlib/WCSAxes objects. The optional dependencies are installed with
`casa-rs-python[plot]`; executable-rendered exports remain appropriate for
GUI/TUI parity and regression images.

AI Python is separate. The exact proposed code and expected artifact paths are
shown before approval. Approval is bound to a content hash; any code change
invalidates it. The worker has read-only project science data, a writable
artifact staging directory, no network, and no inherited secrets.

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
the transcript, unsent draft, scroll position, selected context, running
read-only work, and pending proposals.

Conversations are project-owned and stored separately from Markdown. A
conversation opened from a notebook receives that notebook as its primary
attachment and default pin destination, but may explicitly attach other
notebooks, datasets, task tabs, outputs, history entries, documents, or source
trees. A transcript contains only visible messages, citations, timestamps,
provider/model labels, attachment/context manifests, proposal states, and pin
references. It excludes hidden reasoning and raw provider envelopes.
Provider/model selection may change within a thread.

Both drawer and central-tab presentations use a conventional free-form
multiline composer fixed at the bottom. Optional suggested questions are
empty-state aids only: selecting one fills editable composer text and never
sends it or inserts a predetermined user message. Inline AI remains a
transient, locally scoped invocation for a selected cell or artifact; durable
conversation stays in the separate project transcript.
Return sends the current message; Shift-Return inserts a newline. Subscription
and model selectors sit directly below the composer. The production options
come from the CASA-RS sidecar's provider-neutral catalog projection; its Pi
adapter may reuse Pi authentication and model discovery internally, but Pi
types do not cross into the GUI or durable project formats.

The notebook toolbar shows **Discuss** only while the conversation is closed.
It disappears while the drawer is visible because the drawer owns close and
expand controls. If the conversation is expanded in a central tab and the user
returns to the notebook, the toolbar instead offers **Dock chat**. The compact
drawer header shows its primary attachment, history, expand, and close controls.
The assistant automatically knows the semantic state of every
open workbench tab and has standing read-only tools for notebooks, task schemas
and current parameters, explorers, run history, plots, persistent CASA-RS data
types, the radio-astronomy and project-document corpus, and release/live-
checkout source. Users do not manually attach each open tab or corpus before
ordinary questions.

Automatic workspace awareness and local read-only retrieval do not imply bulk
provider disclosure. One compact row above the composer summarizes the precise
per-message provider payload and destination, for example `current section +
plot + 2 papers -> OpenAI`. Opening that row shows retrieved excerpts, typed
state, bounded summaries, estimated egress, include/exclude overrides, and tool
authority. Every sent turn retains an expandable manifest of what was
disclosed and to which provider.

Citations attach to the claims they support. A lightweight preview shows the
relevant document excerpt, page/section, source path/lines/commit, notebook
block, or run provenance; opening it uses a normal central preview tab. Pins
may snapshot a conclusion, code, task intent, plot, citations, or a transcript
link into a user-chosen notebook location after preview and confirmation.

Mutations are destination-first rather than duplicated as proposal cards in
the conversation. Chat shows only a compact status/link such as **Suggestions
are in Analysis.md**. Pending notes, Python cells, plots, and parameter blocks
appear at their intended notebook location and are reviewed there. The link
retains a stable destination block or proposal identifier and switches to the
notebook before scrolling and focusing that exact insertion. By default, a new
suggestion is appended at the notebook's chronological tail; an explicit
user-selected insertion target may override that default. Task parameters open
directly in the normal task tab with AI-suggested non-defaults
visibly marked; downloads use the acquisition surface. Detailed diffs, code,
parameters, commands, and logs stay collapsed until **Review** at that
destination. Insertion, execution, download, and file-write authority remain
separate approvals. Ordinary prose and read-only answers remain ordinary chat
messages.

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
project-owned transcripts, typed scientific context, provider-egress manifest,
canonical task/Python review surfaces, and approval boundaries.

The `casars-assistant` sidecar speaks a versioned CASA-RS JSONL/stdio protocol
and uses Pi only as an adapter layer. Initial supported authentication is an
OpenAI ChatGPT/Codex subscription; OpenCode Zen is the non-OpenAI live-smoke
provider. Credentials stay outside projects, notebooks, indexes, and Python
workers.

The local corpus has four layers:

1. an optional versioned, redistribution-cleared baseline pack
2. project papers and documents copied into `documents/`
3. release-matched CASA-RS documentation and source
4. an optional live-checkout source overlay keyed to its Git commit

SQLite holds manifests, chunks, metadata, citations, and FTS5 indexes. A
versioned local embedding model produces a flat float32 matrix searched by exact
cosine similarity behind a private vector-index interface. Content hashes make
updates incremental. Extraction and OCR are local by default; cloud OCR is a
per-document opt-in.

Scientific factual claims cite source pages or sections. Implementation claims
cite paths, symbols, line ranges, and release/commit identity. Retrieved text
and web content are evidence, never instructions.

The assistant has standing read-only access to bounded metadata, statistics,
samples, previews, source lookup, corpus retrieval, and public web research.
Web queries and citations are visible. Uploads, downloads, logins, writes,
Python, and tasks cross explicit approval boundaries. Hosted models may receive
selected excerpts and bounded summaries or plots, but never bulk arrays,
visibilities, full datasets, secrets, or unrestricted project files by default.

The standing read-only surface includes all open tab projections: notebook
source and selection, task identity/schema/current values/default differences,
explorer configuration and selected data products, Python cells and retained
outputs, plots, and processing history. It also includes task and parameter
documentation plus persistent table, MeasurementSet, image, coordinate, and
measures semantics. The assistant may retrieve from these sources as needed
without per-read approval; the resulting provider payload remains bounded and
visible per turn.

Task proposals show the typed parameter diff, run-safety class, affected paths,
and expected products. Approval launches the canonical task path so normal
validation, safety checks, and notebook recording still apply.

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
beam metadata, and overlays. The separate macOS AI worker uses Seatbelt with
network denial, scrubbed credentials, read-only science inputs, one writable
staging root, exact-source approval, symlink-escape coverage, and inherited
subprocess restrictions.

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

Prototype: provider/model selector, context chips, streaming cited answers,
corpus/indexing states, source links, approval cards, plot insertion,
pin-to-notebook, redaction preview, rate limits, and offline/error states.

Current Phase A reality: the first `casars-mac --show-prototype ai`
implementation proved the deterministic provider, context, citation, corpus,
proposal, recovery, accessibility, and zero-production-call fixtures, but its
full-width action-heavy layout and predetermined-question workaround were not
accepted. That presentation is superseded interaction evidence and must not
unlock Phase B.

The revised `casars-mac --show-prototype ai` Phase A prototype now begins from
a full-width notebook with no AI pane. **Discuss** reveals a conventional
free-form chat in a resizable contextual drawer; the same fixture conversation
expands into a central AI tab and docks back without losing its draft or state.
The compact header identifies the attached notebook and the zero-production-
call boundary. Subscription and model selectors sit below the composer, with
Return-to-send and Shift-Return-for-newline behavior. A bounded in-drawer
context panel lists every open tab plus the standing radio astronomy corpus,
project papers, CASA-RS source, and CASA-RS task/parameter/data-type semantics,
while separately
showing the selected provider-egress payload. Suggested prompts only fill the
composer. Answers use claim-local citations and source previews. Chat links to
pending suggestions in `Analysis.md` and focuses their stable notebook
destination at the chronological end of the document by default; review and
Apply/Discard controls live in the notebook, while task
suggestions open the normal Imager tab with the
non-default parameter marked. Pinning previews the notebook representation and
location before confirmation. The right-anchored drawer uses stable global
drag coordinates so its divider tracks the pointer in the expected direction.
Deterministic rate-limit, cancellation, restart, accessibility, debug-state,
and zero-production-boundary fixtures remain live. The user explicitly accepted
this destination-first Phase A interaction on 2026-07-12.

Current Phase B reality: `casa-notebook` owns provider-neutral transcript,
citation, egress, immutable pin, exact proposal, separate insertion/execution
approval, and incremental SQLite/FTS5/flat-vector corpus contracts.
`casars-assistant` uses the exactly pinned Pi adapter behind a versioned
CASA-RS JSONL/stdio protocol; Swift brokers Keychain credentials, typed context,
and bounded host tools. The sidecar is Seatbelt-constrained to provider network
access and has no project filesystem, SQLite, shell, Python, or credential-file
authority. The CASA-RS-owned corpus combines its cleared baseline primer,
project `documents/`, release source/docs, and a commit-keyed live overlay.
Task, note, Python, plot, and download requests become notebook-tail proposals;
AI Python uses a separate no-network Seatbelt worker and plot assets require an
explicit notebook import. Approved AI Python, plot, and download attempts use
the shared immutable notebook receipt path in addition to their conversation
proposal state. Deterministic fake-provider coverage is part of
`just quick`; the Keychain-backed OpenAI/OpenCode smoke remains opt-in through
`just assistant-live-smoke <provider> <model>`. See
[`assistant-security.md`](assistant-security.md) for the executable boundary.

## Program acceptance

The TW Hya walkthrough must demonstrate: fork a tutorial; acquire data; edit
notes; run and record a task; reload parameters; execute Python; regenerate a
plot; ask a scientific question with document citations; ask an implementation
question with source citations; approve an AI calculation; and pin its result.

CI uses deterministic fixture providers. Opt-in live smoke covers OpenAI
ChatGPT/Codex subscription authentication and OpenCode Zen. Timings for startup,
indexing, retrieval, recording, and execution are recorded without imposing a
hard performance budget in these waves.

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
