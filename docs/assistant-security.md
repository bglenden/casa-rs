# Assistant Security and Corpus Runtime

Truth class: current descriptive
Last reality check: 2026-07-12
Verification: just docs-check
Architecture decision: [ADR-0007](adr/0007-scientific-notebooks-and-assistant-boundary.md)

## Authority model

`casars-assistant` is an untrusted provider adapter. It may keep conversation
state in memory, contact the selected model provider, and return text or typed
tool requests over the CASA-RS JSONL/stdio protocol. It has no project
filesystem, SQLite, shell, Python, credential-file, or direct host-tool
authority. The native host starts it under Seatbelt and fails closed if the
protocol version or deny-by-default policy attestation differs.

Permissions are the intersection of host, conversation, and request policy.
No lower layer can widen authority. There is no ordinary elevated chat mode.
Credentials are stored as device-only Keychain items and are leased to the
adapter over stdin for the selected provider. They are excluded from projects,
transcripts, notebooks, indexes, process environments, logs, and Python.

The installed application bundles the built adapter, its exactly locked
production dependencies, and a release/commit-identified projection of tracked
CASA-RS source and maintained documentation. The sidecar searches for a
compatible Node runtime in the application bundle, an explicit override,
`PATH`, and common user package-manager and version-manager locations. It
requires Node 22.19 or later.
Seatbelt read rules are derived from the resolved runtime and its linked
libraries; security does not depend on a Homebrew path. Only literal ancestor
metadata needed for path traversal is exposed; directory enumeration outside
adapter/runtime or explicitly approved science roots remains denied.

## CASA-RS-owned corpus

The scientific corpus belongs to the current CASA-RS project. It does not
query, mount, or depend on a separately installed Radio Astronomy Oracle.
CASA-RS builds one incremental index from:

1. a versioned redistribution-cleared baseline pack shipped by CASA-RS
2. user documents under the project `documents/` directory
3. release-matched CASA-RS documentation and source
4. an optional live source overlay identified by Git commit

The initial baseline is the original CASA-RS radio-interferometry primer under
`CasarsMacCore/Resources/assistant-corpus`; its manifest records the pack
version and redistribution clearance. Additional standard texts may be added
only with an explicit redistribution basis.

The host extracts UTF-8 text locally and uses PDFKit text extraction with
Vision OCR only for pages without embedded text. It enforces file, corpus, and
document-count limits. SQLite stores document metadata, chunks, citations, and
FTS5 terms. A private versioned 384-dimensional feature-hash embedding produces
a flat float32 matrix searched by exact cosine similarity. Both representations
are rebuildable managed state under `.casa-rs/corpus/`.

Pi never receives a database path or SQL interface. It calls the typed
`corpus.search` or `source.search` host tool and receives only bounded cited
excerpts. Project documents and retrieved pages are always marked untrusted
evidence, so their text cannot grant instructions or permissions.

## Context and egress

The native host owns typed projections for every open notebook, task,
explorer, plot, Python, and history tab. Separate read-only tools expose task
schemas, persistent-data metadata, corpus/source retrieval, and credential-free
public scholarly research. `web.search` uses Crossref discovery;
`web.fetch` accepts one explicit public HTTPS URL, resolves and rejects local
or private addresses and unsafe redirects, verifies the actual URLSession
remote address after connection to close DNS-rebinding gaps, rejects proxy
connections, sends no cookies or credentials,
accepts text responses only, caps the fetched body at 1 MiB, and sends at most
48 KiB of its extracted text to the provider. Initial tab context is capped at
64 KiB; each host-tool result is capped at 64 KiB and dynamic tool context at
256 KiB per turn. The stored egress manifest is the exact bounded projection
sent, including tool failures and proposal receipts.

The model receives only selected bounded tab projections and tool results.
Raw arrays, visibilities, full datasets, unrestricted files, and secrets are
excluded. Visible transcript history is limited to the newest 128 messages and
256 KiB per turn; sidecar protocol lines are limited to 1 MiB. Every visible
assistant message retains the provider/model and a manifest of initial plus
dynamically retrieved context sent on that turn.
Document, source, run, and web evidence becomes a claim-local citation.

## Proposals, execution, and pins

Mutation requests can only create pending typed proposals. The host binds the
proposal hash to operation type, canonical parameters, exact source, input and
output paths, working directory, and executable canonical path, version, and
SHA-256. Executable discovery is portable, but approval is always bound to the
resolved executable actually used. Any change invalidates approval.

Insertion approval is separate from execution approval. Downloads additionally
bind the exact HTTPS URL, project-relative destination, and optional digest.
The host uses an ephemeral credential-free session, rejects redirects, and
limits an assistant-proposed download to 128 MiB; larger scientific datasets
use the tutorial acquisition contract.
Tasks open the canonical task tab with suggested non-defaults marked; the
normal task Run action remains separate. AI Python runs in a second Seatbelt
worker with no network, read-only approved scientific inputs, and one
content-addressed staging directory. Plot files enter notebook visualization
history only after a separate explicit import action.

Project-owned write destinations reject absolute paths, traversal, and every
existing symbolic-link ancestor both when the proposal is bound and again at
execution. AI-worker input paths are canonicalized before approval. The worker
grants literal reads for approved files and subtree reads only for explicitly
approved directories. It cannot fork subprocesses, so cancellation terminates
the complete isolated execution authority rather than leaving descendants
behind. Worker stdout and
stderr are drained continuously and retained up to 16 MiB each, with explicit
truncation markers beyond that bound.

Failure and cancellation are recorded honestly and do not retry. An explicit
retry creates a new execution approval. A notebook pin is an immutable,
user-previewed Markdown snapshot containing conversation/message provenance
and a content SHA-256; later transcript changes never update it.

Approved AI Python, plot, and download attempts also enter the shared notebook
receipt path. Receipt v2 records AI-worker authority and exact source for
Python, the approved hashes, resolved operation parameters, affected paths,
products, stdout/stderr, diagnostics, and the terminal success, failure, or
cancellation state. Conversation proposals remain the review surface; they do
not replace scientific execution provenance.

## Verification

`just assistant-test` runs the deterministic protocol adapter. Native focused
tests cover Rust persistence/corpus projections, the full Seatbelt tool
round-trip, baseline/project/source ingestion, URL denial, and immutable pin
contracts. `just assistant-live-smoke <provider> <model>` is opt-in: it uses a
credential already stored by the GUI in Keychain and never accepts a secret on
the command line or in an environment variable. The consolidated native GUI
gate remains `just gui-test`.
