# ADR-0007: Scientific notebooks and assistant boundary

Status: accepted
Date: 2026-07-10
Truth class: normative
Supersedes:
Superseded by:

## Context

`casa-rs` needs a durable scientific record that working astronomers can read
and edit outside the application. The same record must support ordinary notes,
tutorials, task parameter reuse, Python calculations and reproducible plots,
and selected conclusions from AI discussions. Task, Python, tutorial, and AI
surfaces already have partial or fixture-only GUI representations, but there is
no shared notebook contract, execution receipt, project corpus, or assistant
runtime.

The design crosses public and persisted formats, GUI/runtime boundaries,
Python execution, network acquisition, model authentication, and user-data
egress. Allowing each surface to invent its own storage or execution model
would conflict with ADR-0001, ADR-0003, ADR-0005, and ADR-0006.

## Decision

### Notebook and provenance contracts

The user-facing source of truth is ordinary Markdown under `notebooks/`, with
standard asset links and unobtrusive versioned HTML comments identifying the
notebook and typed cells. Task intent is represented as sparse TOML using the
ADR-0006 CASA-facing parameter vocabulary. Users may edit the Markdown with
third-party tools.

Machine-generated execution history is separate, versioned managed state under
`.casa-rs/notebook-runs/`. A cell has a stable ID and each execution creates an
immutable revision. Receipts record the resolved parameters, provider-contract
version, status, approvals, affected paths, products, artifacts, and log
references. Sparse task cells remain reusable intent under current defaults;
receipts are historical execution evidence, not a claim of hermetic replay.

The default portable export contains Markdown and referenced notebook assets.
Managed receipts and project products are excluded unless the user explicitly
selects an advanced export.

### Ownership and dependency direction

A new reusable Rust crate, `casa-notebook`, owns Markdown/cell parsing,
versioned receipt schemas, locking, conflict detection, exports, and tutorial
manifest contracts. It sits above provider contracts and the parameter runtime
and below app/runtime projections. It does not implement scientific task
behavior.

Swift owns native interaction and rendering. `casars-frontend-services` may
project notebook and run information but must not become a second persistence
implementation. GUI/TUI/CLI/Python surfaces use the same Rust-owned contracts.

The assistant is a separate `casars-assistant` TypeScript sidecar behind a
versioned CASA-RS-owned JSONL/stdio protocol. The sidecar may use Pi for model
and authentication adapters, but no persisted notebook, transcript, proposal,
tool, or corpus contract may depend on Pi-specific envelopes.

### Recording and execution

Project-aware GUI, TUI, `casars run`, and Python operations automatically
record all app-mediated write attempts, including successful, failed,
cancelled, and interrupted operations. Direct provider binaries record only
with explicit workspace/notebook context. Workspace selection is explicit or
the exact current directory; it never searches parent directories. A visible
one-run bypass is allowed. Recording failure warns but does not block the
scientific operation.

The user Python kernel is persistent per open notebook and runs with normal
user-process authority. Every Python-cell execution is therefore recorded as
potentially mutating. AI-generated Python runs in a separate constrained
worker: project scientific data are read-only, only a dedicated artifact
staging directory is writable, networking is disabled, and credentials are
not inherited. AI code and task requests require approval bound to the exact
code or typed parameter proposal.

### Tutorials and data acquisition

Tutorials use an immutable portable template folder containing
`tutorial.md`, `tutorial.toml`, and `assets/`. Opening a tutorial forks an
editable learner notebook into the project. Tutorial-pack v0 is migrated
without a compatibility layer.

Dataset acquisition is explicit and project-local. A versioned URI-handler
registry permits arbitrary schemes without delegating to a shell; v1 ships
`file`, `http`, and `https` handlers. The approval view shows the resolved
source, redirects, expected size, destination, checksum, and extraction plan.
Downloads are verified and archives are materialized defensively. When no
digest is supplied, the user approves the risk and the computed SHA-256 is
pinned in managed tutorial state.

### Assistant, corpus, and trust

AI discussions are shown in the notebook workspace but persist as separate,
provider-neutral visible-message transcripts under `.casa-rs/conversations/`.
Selected messages, code, plots, or conclusions may be pinned into Markdown.
Every assistant turn records its provider/model label and citations, but not
hidden reasoning or raw provider envelopes.

Retrieval is local and layered: a versioned redistributable baseline pack,
project documents copied under `documents/`, release-matched `casa-rs` source,
and an optional live-checkout overlay keyed to a Git commit. SQLite stores
source/chunk metadata, citations, and FTS5 text indexes. A versioned float32
embedding matrix provides exact cosine search behind a private vector-index
interface. Scientific claims cite document pages or sections; implementation
claims cite source paths, symbols, lines, and release/commit identity.

Read-only public web research is available with visible queries and citations.
Downloads, uploads, authenticated actions, and writes require separate
approval. Hosted models may receive selected document excerpts and bounded
scientific summaries or plots, but not bulk arrays, visibilities, full
datasets, credentials, or unrestricted project files by default.

### Prototype-first delivery

Each of the four implementation waves begins with a launchable, fully
interactive `casars-mac` prototype backed only by deterministic fixture
adapters. The prototype covers primary, failure, cancellation, retry, and
restart states; exposes accessibility identifiers and debug state; and produces
review evidence. It performs no real network, task, Python, model, or durable
project operation.

Real adapter integration cannot begin until the prototype is reviewed and
explicitly accepted. The reviewed views and interaction models remain; fixture
adapters are retained for deterministic tests. A material interaction change
returns the wave to the prototype approval gate.

Delivery order is:

1. Markdown notebooks and execution receipts.
2. Persistent Python cells and reproducible plots.
3. Tutorial notebooks and verified dataset acquisition.
4. Pi-backed assistant, local corpus, and approved actions.

## Consequences

Positive:
- notes, task intent, plots, and pinned conclusions remain readable outside the app
- execution history is durable without turning Markdown into an opaque machine log
- all user surfaces share one notebook and receipt contract
- provider/model adapters can change without migrating project data
- prototype approval settles interaction design before expensive backend work

Negative:
- the workspace gains a Rust contract crate and a TypeScript sidecar
- safe Python, download, OCR, retrieval, and credential boundaries require
  packaging and platform-specific work
- Markdown round-trip fidelity and external-edit reconciliation become release
  requirements rather than editor polish

Neutral / tradeoffs:
- parameter replay is supported, but exact environment or data-state replay is not
- the user Python kernel is intentionally powerful; only AI execution is constrained
- project-local downloads favor isolation and portability over deduplicated caching
- exact vector search is preferred over an external vector service for the first release

## Alternatives considered

1. Store notebooks as opaque JSON or a private database and export Markdown.
2. Put exact run envelopes directly in Markdown cells.
3. Use a Jupyter server and browser-based notebook as the primary GUI.
4. Put AI provider SDKs and retrieval logic directly in Swift.
5. Use provider-hosted RAG or require PostgreSQL/pgvector.
6. Permit AI to share the unrestricted user Python kernel.
7. Implement backend contracts before validating each wave's GUI interaction.

## Enforcement

This decision is enforced by:
- tests: Markdown golden/round-trip tests, receipt lifecycle and cross-surface
  contract tests, GUI fixture-state tests, Python-worker isolation tests,
  tutorial download/extraction tests, and deterministic fake-provider tests
- lint/import/dependency rules: Swift and frontend services may project but may
  not redefine persisted or provider semantics; Pi-specific shapes stop at the
  assistant adapter
- CI checks: `just quick`, `just verify`, Swift tests, GUI debug-state smoke,
  and targeted Python/assistant tests for the active wave
- review trigger: persisted-format, public-API, dependency, runtime, security,
  model-egress, or prototype-gate changes require explicit architecture review
- none / guidance only:

## Drift detection

Suspect drift if:
- notebooks require CASA-RS to be readable or editable
- a GUI/TUI/Python surface writes a different receipt shape
- mutable run state replaces immutable execution revisions
- Pi or one model provider appears in durable project schemas
- assistant tools can bypass typed proposals and user approval
- AI Python inherits project write access, network access, or credentials
- opening a notebook or tutorial automatically runs code or downloads data
- production integration begins before the wave's live prototype is accepted
