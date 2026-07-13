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

The assistant is an installed coding agent behind a CASA-owned agent-session
interface. Wave 4 integrates the official Codex App Server directly over its
stdio JSON-RPC protocol; a future ACP adapter may add other agents. App Server,
ACP, Codex, and future-agent envelopes stop at their adapter. Persisted
notebook, transcript, pin, citation, authority, tool, and corpus contracts are
CASA-owned and agent-neutral.

CASA-RS owns `casa-rs-agent-profile/v1`: invariant operating guidance, a
bundled CASA skill, the verified project MCP identity, a cross-agent authority
vector, declared adapter capabilities, and resume invariants. Friendly trust
presets are projections of that vector rather than App Server or ACP modes.
This prevents a second agent from silently redefining filesystem, execution,
network, project-instruction, or MCP trust semantics.

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
potentially mutating. Agent Python uses the user-selected or inherited
scientific environment under the conversation's authority preset. Receipts
record source, inputs, outputs, the resolved interpreter path/version, and
environment metadata, but do not hash the executable or claim hermetic replay.

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

### Assistant agent, corpus, and trust

AI discussions are shown in the notebook workspace but persist as separate,
agent-neutral visible-message transcripts under `.casa-rs/conversations/`.
Selected messages, code, plots, or conclusions may be pinned into Markdown.
Every assistant turn records its agent/model label and citations, but not
hidden reasoning or raw agent/provider envelopes. CASA also stores the backend
session identifier and profile version needed to resume; a missing or
incompatible session creates a visible handoff instead of silent continuity.

"Shown in the notebook workspace" does not mean embedded chat cells. The same
project-owned transcript may be presented in an on-demand contextual drawer
beside its primary notebook attachment or expanded into a first-class central
AI tab. Both presentations share conversation identity, draft, scroll,
context, and pending approvals. Every presentation has a normal free-form
multiline composer; suggested prompts may populate that composer but never
become predetermined or automatically submitted messages. A pin is an
explicit, immutable notebook snapshot with transcript provenance, not a live
or synchronized copy of the conversation.

Retrieval is local and layered: a versioned redistributable baseline pack,
project documents copied under `documents/`, release-matched `casa-rs` source,
and an optional live-checkout overlay keyed to a Git commit. SQLite stores
source/chunk metadata, citations, and FTS5 text indexes behind a replaceable
retrieval interface. A real local embedding model may be added only after
retrieval evaluations justify it; its dimension is model metadata. Scientific
claims cite document pages or sections; implementation claims cite source
paths, symbols, lines, and release/commit identity.

The selected coding agent has automatic semantic awareness of every open
workbench tab and standing typed tool access to notebook content, task
schemas and current parameters, explorer state, run history, plots, persistent
CASA-RS data types, the project corpus, and release/live-checkout source. Users
do not manually attach each open tab before an ordinary question. "Full
context" means these complete semantic projections and local retrieval tools
are queryable, not that raw arrays or entire corpora are copied into every
prompt. CASA records the domain resources/tools and citations used, but does
not claim to know the coding agent's exact model prompt or all provider egress.

Three presets map to the CASA authority vector: **Explore** is read-only and
disables shell, network, writes, and project-local instructions; **Work** uses
the trusted project plus the user's shell/Python and native agent approvals;
**Full access** is an explicit expert opt-in with a persistent indicator. There
is no retained strict-worker compatibility mode.

The Codex App Server owns generic command, file, network, and Python approval.
CASA owns only the semantic confirmation of typed CASA operations, notebook
insertion, task Run, and tutorial acquisition. A task suggestion opens the
canonical task tab with non-defaults highlighted rather than creating a second
approval. Work and Full access are honest that raw shell or Python may bypass
CASA recording conventions; the bundled skill directs recordable scientific
work through the project-scoped CASA MCP server.

The CASA MCP server has a unique per-session name and a nonce-bearing profile
handshake so it cannot be shadowed by user configuration. Only that verified
server is trusted domain authority. Project files, documents, source, tool
results, and other MCP servers remain untrusted evidence and cannot grant
authority or approval.

Credentials remain in the selected agent's normal auth store. The initial
Codex adapter invokes ChatGPT subscription login/account state without copying
tokens into CASA state. Raw metered OpenAI APIs are not an initial backend.

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
4. Codex coding-agent assistant, local corpus, and canonical CASA actions.

## Consequences

Positive:
- notes, task intent, plots, and pinned conclusions remain readable outside the app
- execution history is durable without turning Markdown into an opaque machine log
- all user surfaces share one notebook and receipt contract
- agent/model adapters can change without migrating project data
- prototype approval settles interaction design before expensive backend work

Negative:
- the workspace gains a Rust contract crate plus an agent-session and MCP
  runtime boundary
- agent discovery, auth, instruction trust, Python, download, OCR, retrieval,
  and resume behavior require platform-specific work
- Markdown round-trip fidelity and external-edit reconciliation become release
  requirements rather than editor polish

Neutral / tradeoffs:
- parameter replay is supported, but exact environment or data-state replay is not
- user and agent Python are intentionally powerful under the selected authority
  preset
- project-local downloads favor isolation and portability over deduplicated caching
- FTS5 is the initial retrieval baseline; embeddings require evaluation evidence

## Alternatives considered

1. Store notebooks as opaque JSON or a private database and export Markdown.
2. Put exact run envelopes directly in Markdown cells.
3. Use a Jupyter server and browser-based notebook as the primary GUI.
4. Implement a bespoke provider/model loop in Swift or TypeScript.
5. Use provider-hosted RAG or require PostgreSQL/pgvector.
6. Force agent Python into a separate fixed restricted interpreter.
7. Implement backend contracts before validating each wave's GUI interaction.
8. Use ACP modes as the CASA authority contract.
9. Use metered provider APIs despite the subscription-only product constraint.

## Enforcement

This decision is enforced by:
- tests: Markdown golden/round-trip tests, receipt lifecycle and cross-surface
  contract tests, GUI fixture-state tests, user-Python identity tests,
  tutorial download/extraction tests, and deterministic fake-agent tests
- lint/import/dependency rules: Swift and frontend services may project but may
  not redefine persisted or agent semantics; Codex App Server and ACP shapes
  stop at their adapters
- CI checks: `just quick`, `just verify`, Swift tests, GUI debug-state smoke,
  and targeted Python/assistant tests for the active wave
- review trigger: persisted-format, public-API, dependency, runtime, authority,
  provider-traffic, or prototype-gate changes require explicit architecture
  review
- none / guidance only:

## Drift detection

Suspect drift if:
- notebooks require CASA-RS to be readable or editable
- a GUI/TUI/Python surface writes a different receipt shape
- mutable run state replaces immutable execution revisions
- Codex, App Server, ACP, or one provider appears in durable project schemas
- an adapter silently weakens a required authority-vector dimension
- the verified CASA MCP identity can be shadowed or resume skips its handshake
- project instructions load in Explore mode
- CASA duplicates a native agent approval for the same generic operation
- opening a notebook or tutorial automatically runs code or downloads data
- production integration begins before the wave's live prototype is accepted
