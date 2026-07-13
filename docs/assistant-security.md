# Assistant Agent Runtime, Authority, and Corpus

Truth class: current descriptive
Last reality check: 2026-07-13
Verification: just docs-check
Architecture decision: [ADR-0007](adr/0007-scientific-notebooks-and-assistant-boundary.md)

## Runtime choice

CASA-RS does not implement a model loop. It launches a user-installed coding
agent behind a CASA-owned agent-session interface and supplies CASA semantics
through a project-scoped MCP server.

Wave 4 targets the official Codex App Server directly. This is the only initial
OpenAI surface that combines ChatGPT subscription authentication, a local
coding-agent runtime, approvals, sandbox policy, MCP, and resumable sessions.
The metered Responses API and Agents SDK are not Wave 4 backends. The ChatGPT
Apps SDK targets an app hosted inside ChatGPT rather than this native local
workbench.

The direct adapter is intentionally replaceable. Durable conversations,
notebook pins, citations, CASA tool schemas, and the authority vocabulary do
not contain Codex request or event shapes. A future ACP adapter may add
OpenCode or another conforming agent. ACP is not the authority contract: it
standardizes transport and session operations but does not normalize sandbox,
permission, skill, instruction, or trust semantics.

The initial adapter discovers a user-selected or installed `codex` executable
without assuming Homebrew or any other package manager. It speaks App Server
JSON-RPC over stdio. CASA invokes the App Server account login flow and reads
account/rate-limit status, but never receives, copies, or stores the ChatGPT
credential. API-key authentication is not part of the initial subscription-
funded product path.

## CASA agent profile and capability contract

`casa-rs-agent-profile/v1` is the internal cross-agent contract. It contains:

- a small invariant developer instruction describing CASA recording,
  citations, and approval ownership
- the version and identity of the bundled CASA skill
- the expected CASA MCP identity and one-time session nonce
- a CASA authority vector and the selected friendly preset
- adapter capabilities and any unsupported or partially enforced dimensions
- the conversation-to-backend-session mapping needed for resume

The authority vector is independent of App Server and ACP modes:

```text
project filesystem: none | read | write
external filesystem: none | prompted-read | prompted-write | unrestricted
execution: none | prompted | normal | unrestricted
network: none | prompted | normal | unrestricted
project instructions: disabled | enabled
CASA MCP identity: required profile version + session nonce
```

The GUI presents three expert-oriented presets:

- **Explore**: read-only project context through the trusted CASA MCP server;
  no shell execution, project writes, network, or project-local instructions
- **Work**: the normal trusted-project mode; project writes, user-selected
  additional directories, the user's shell and Python environment, and native
  Codex approval for commands, file changes, network, and escalation
- **Full access**: explicit expert opt-in to unrestricted Codex authority, with
  a persistent visible indicator

There is no separate strict/Seatbelt assistant mode. A deployment may later add
custom presets, but Wave 4 does not preserve the removed constrained worker as
a compatibility path.

For Explore, Codex starts from a neutral working directory and receives the
project through read-only CASA resources so project `AGENTS.md`, skills, hooks,
or other local instructions cannot become authority before trust. Work and Full
access deliberately use the trusted project as the working directory and may
load its normal agent instructions.

Each adapter declares how it maps the vector to its actual primitives and must
fail visibly if it cannot honor a required dimension. Conformance is behavioral,
not a catalog check: startup invokes the nonce-bearing profile tool, exercises
a harmless resource read, verifies permission denial/escalation behavior, and
proves cancellation and resume with the MCP server reattached.

## Approval ownership

Approval is workflow control, not a claim of containment. Ownership is kept
single-source:

| Action | Approval owner |
|---|---|
| shell commands, ordinary file changes, generic Python, and network | Codex App Server and its native approval events |
| opening a task with suggested parameters | no mutation; CASA opens the canonical task tab and highlights non-defaults |
| running a task from that tab | the ordinary CASA task Run workflow |
| typed CASA data mutation requested through MCP | the canonical CASA operation and its normal scientific receipt path |
| adding an answer, code, or plot to a notebook | one explicit CASA notebook insertion confirmation |
| tutorial data acquisition | the existing CASA tutorial acquisition workflow |

The bundled skill tells the agent to prefer typed CASA MCP operations for
scientific work that should be validated and recorded. Work and Full access do
not pretend that raw shell or Python cannot bypass those conventions; such
commands remain visible agent activity but may not produce CASA scientific
receipts. CASA does not add a second confirmation merely because Codex already
approved a generic command.

## Python and downloads

The assistant uses the same user-selected or inherited scientific Python
environment that an astronomer expects to work with CASA and local packages.
The executable may change between sessions. Receipts record the resolved path,
version, environment label, and useful package/CASA metadata when available;
they do not hash the executable or invalidate a workflow solely because the
path or version changed.

Ad-hoc agent Python runs under the selected authority preset. Reproducible
notebook calculations still store exact source, named inputs, environment
metadata, outputs, and plot assets. This is provenance, not a hermetic replay
claim.

There is no assistant-specific 128 MiB download ceiling or whole-body buffer.
Generic downloads use the agent's normal network and filesystem authority.
Tutorial and other CASA-managed acquisition streams into a temporary file in
the destination project, honors ordinary proxy/TLS configuration, checks
declared size and available disk when known, optionally verifies a digest, and
atomically promotes a completed artifact. Configurable user or deployment
quotas are allowed; an arbitrary built-in scientific-data policy cap is not.

## CASA MCP and context

The project-scoped CASA MCP server is the typed boundary for:

- all open notebook, task, explorer, plot, Python, and history tabs
- task schemas, defaults, current parameters, and canonical task opening
- persistent data-type metadata and immutable run receipts
- notebook reading, explicit append/pin, plot import, and explorer reopening
- local radio-astronomy, project-document, release/live-source, and CASA
  semantics retrieval with exact citations
- tutorial acquisition and other canonical CASA workflows

The MCP server name is unique per backend session. Its required profile tool
returns the profile version and one-time nonce so a user-configured server
cannot shadow it. CASA treats only that verified server as trusted domain
authority; MCP annotations and every other server remain untrusted hints.

"Full context" means the agent can query complete typed semantic projections
and local retrieval tools as needed. It does not mean raw visibility arrays or
entire corpora are copied into every prompt. The UI shows the context sources
available to the agent and records the CASA resources/tools and citations used.
CASA cannot truthfully promise an exact provider-egress manifest for a coding
agent that can also read files and run commands, so the UI does not make that
claim. Codex owns its model traffic under the selected account and authority.

## Local corpus

The corpus belongs to the CASA-RS project; it does not depend on a separately
installed Radio Astronomy Oracle. Its layers are:

1. a versioned baseline pack of standard radio-astronomy material that CASA-RS
   has a recorded right to redistribute
2. user-supplied project documents, which do not require redistribution rights
3. release-matched CASA-RS documentation and source
4. an optional live-checkout overlay identified by Git commit

SQLite stores document metadata, chunks, citations, and FTS5 text indexes
behind a replaceable retrieval interface. Wave 4 removes the fixed 384-
dimensional word/bigram feature hash and does not call it an embedding. A real
local embedding model and index may be added only after retrieval evaluations
show a material benefit; its dimension is model metadata, not architecture.

Retrieved documents and source are evidence, never instructions. Scientific
claims cite document/page/section identities. Implementation claims cite the
CASA-RS release or commit plus path, symbol, and lines. Project documents can
contain prompt injection, so they cannot change the authority vector, load
skills, or grant approval.

## Conversations and resume

CASA owns the visible durable transcript, citations, pins, context-use records,
agent/model labels, authority preset, and backend-session mapping. Hidden
reasoning and raw App Server envelopes are not project data. The Codex thread is
resumed when compatible; every resume re-verifies profile version/nonce,
authority, MCP registration, and current agent capabilities before accepting a
new turn.

If a backend session is missing or incompatible, CASA starts a new one and
shows a visible handoff boundary with a bounded conversation summary. It does
not silently claim continuity. Switching to a future agent likewise creates a
new backend session while preserving the provider-neutral CASA transcript.

## Verification

Wave 4 requires deterministic adapter and GUI fixtures plus opt-in live Codex
smoke using a user's existing ChatGPT subscription. Acceptance covers:

- ChatGPT login/account state without CASA handling a credential
- profile/MCP identity verification and collision resistance
- Explore denial of project instructions, writes, execution, and network
- Work command/file/network approvals with no duplicate CASA prompt
- user-selected Python identity and environment changes across runs
- cancellation, process failure, and explicit retry without widened authority
- session resume with authority and MCP registration restored
- corpus FTS retrieval and exact document/source citations
- task suggestions opening the canonical task tab with non-defaults highlighted
- one confirmed notebook append at the chronological tail, with no duplicate
  proposal copy in chat and notebook
- fixture-only GUI review state proving zero production boundary calls

`just gui-test` remains the native interaction gate. Live account/model tests
are opt-in and never require a metered API key.
