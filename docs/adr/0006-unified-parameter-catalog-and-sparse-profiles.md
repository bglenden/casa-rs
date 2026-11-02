# ADR-0006: Unified parameter catalog and sparse profiles

Status: accepted
Date: 2026-07-09
Truth class: normative
Supersedes:
Superseded by: ADR-0017, in part (session surfaces, session Last handling and
the TUI and GUI consumers were removed on 2026-10-20; the parameter catalog
and sparse task profiles stand)

Amended: 2026-11-02, on owner approval. This record now also holds the
current-only profile contracts (decided 2026-09-03, #551) and the surviving
rule of the provider schema bundle decision (2026-04-19); their separate
records were deleted and git history keeps them. Text that ADR-0017
superseded is removed.

## Context

Task defaults, aliases, validation, command-line parsers, Python signatures
and saved requests drift apart when each consumer defines its own
parameters. Users need parameter files that are readable in a Markdown code
block, easy to edit and review, sparse enough to adopt newly added defaults,
and usable from a task's command line and from Python without changing
meaning.

## Decision

### One catalog; the bundle is a projection

The repository maintains one checked aggregate catalog. Its authoritative
concepts are:

- `ParameterCatalog`: reusable parameter concepts with stable semantic IDs,
  canonical CASA-facing names, value types, units or dimensions,
  normalization, semantic roles, persistence classes, constraints, and
  documentation.
- `SurfaceParameterBinding`: a surface-specific use of a catalog concept,
  including its default, requirement and activation rules, narrowing
  constraints, aliases, presentation hints, and private provider projection.
- `SurfaceDefinition`: a task definition containing the ordered bindings,
  the migrations and the provider projection that surface needs.

Each provider's versioned schema bundle is the machine boundary between the
provider and its consumers. It embeds the catalog concepts and surface
definitions it uses and is generated from them. The bundle must not become a
second source of truth: CLI parsers, JSON Schema and Python signatures are
views derived from it, and no consumer hand-defines a default, alias, shape
or behaviour that the catalog does not carry.

A canonical name has one meaning. `imsize` keeps the same value semantics
wherever it is bound. Parameters that share a spelling but differ in meaning
use different semantic IDs and are never silently unified; such a binding
carries a `reviewed_homonym` record citing this ADR. Roles such as input,
output, selection, science control and presentation stay explicit, so a path
or scalar cannot be reused in an unsafe role because its storage type
matches.

Task definitions resolve parameters into one-shot invocation requests. Tool
objects (ADR-0019), runtime controls and transient state are not
parameterized surfaces.

### Sparse TOML profiles

Human parameter documents use sparse TOML. The metadata table identifies the
document format, logical surface, surface kind and contract version; the
`parameters` table holds required values and semantic differences from the
current defaults. Optional values equal to their defaults are omitted.

```toml
[casars]
format = 1
surface = "tclean"
kind = "task"
contract = 29

[parameters]
vis = "data/target.ms"
imagename = "products/target"
imsize = 1024
cell = "0.2arcsec"
niter = 10000
threshold = "2mJy"
```

Evolution rules:

- Adding an optional defaulted parameter does not require existing files to
  change.
- An omitted parameter adopts the current default. Loading warns when a
  migration lists that default as changed since the document's contract.
- Renames and type or value changes use explicit ordered migrations of the
  sparse overrides, each advancing one contract version. A chain may start
  after contract 1; a profile older than its first migration is refused
  (`tclean` migrates contract 28 to 29 and refuses older profiles). A
  renamed surface keeps its former ids, which a saved profile may still
  name (`imager`, now `tclean`).
- A newly required parameter needs a migration or causes a load error.
- Future document or contract versions are rejected.
- Saving rewrites a document at the current contract version against the
  current defaults.

### Current-only surfaces

A surface declares a **current-only** profile contract by publishing an
empty migration set. Such a surface accepts exactly its current contract
version, exposes and accepts only canonical names with no compatibility
aliases, rejects stale and future contract versions instead of interpreting
or rewriting them, and serializes only required values and non-default
overrides whose persistence class is `profile`. Omitted optional values adopt
the current defaults; they are not materialized to preserve an older
contract's behaviour. Shipped profiles, tutorials and examples advance with
the contract. `simobserve` and `msconvert` are current-only; the other
surfaces keep ordered migration chains.

### Managed Last state

The managed workspace store is:

```text
<workspace>/.casa-rs/parameters/<surface-id>/
  last.toml
  last-successful.toml
```

`CASA_RS_STATE_DIR` may redirect the managed state root for automation
without changing how relative task paths resolve. Managed writes use a
per-surface lock, a same-directory temporary file and atomic replacement.

`last.toml` records the validated resolved intent of a command-line run
immediately before execution; `last-successful.toml` updates only after
successful completion. Failure or cancellation does not replace the
successful state. `--json-run`, which Python's task calls use, reads and
writes no Last (ADR-0017 item 4).

`casa-task-runtime` is the sole owner of these rules: source parsing,
resolution, attempted-state capture, managed persistence and completion.
Task executables and the Python package delegate to it and keep no alternate
lifecycle maps, Last writers or test-only persistence paths.

Opening an explicit profile never grants permission to overwrite that file.
Automatic persistence always targets the managed store; changing an explicit
file needs an explicit save.

## Consequences

Positive:
- defaults, validation, docs, projections and persistence share one source
  of truth
- sparse files stay readable and naturally adopt additive parameters
- a profile has the same meaning on a task's command line and in Python
- task aliases keep distinct logical identities and Last files even when
  they share a provider binary
- a current-only surface's meaning is its one canonical request vocabulary

Negative:
- every parameterized surface defines complete typed bindings, and migrations
  unless it is current-only
- default changes need deliberate compatibility diagnostics
- a contract bump of a current-only surface deliberately invalidates
  profiles that were not updated

Neutral / tradeoffs:
- sparse profiles reproduce user intent under current defaults; they are
  not exact historical run-replay records
- provider bundles may use JSON as a machine transport, but JSON Schema and
  CLI flags are projections rather than the semantic model

## Alternatives considered

1. Persist complete JSON request envelopes, freezing old defaults into every
   file.
2. Keep independent command-line and Python parameter definitions, or let
   each consumer define its own bundle shape.
3. Use unversioned TOML maps without catalog identities or explicit
   migrations.
4. Keep every historical migration and alias on every surface, including
   those that declare themselves current-only.
5. Load stale profiles silently against current defaults, which changes
   meaning while looking valid.

## Enforcement

This decision is enforced by:
- tests: catalog completeness, contract invariants (including the homonym
  review and the refusal of aliases on a surface with no migrations), sparse
  TOML round trips, migrations, managed-store lifecycle and cross-surface
  resolution parity in `casa-provider-contracts` and `casa-task-runtime`;
  the Python package's parameter and binding tests
- lint/import/dependency rules: `scripts/check-parameter-surface-catalog.py`;
  consumers may project provider definitions but add no authoritative
  default or alias
- CI checks: `just arch-check`, whose `scripts/check-parameter-contract-history.py`
  keeps semantic fingerprints append-only (a changed concept revision or
  surface contract cannot replace an existing `(id, version)` entry), and
  `just docs-check`, which checks the generated parameter reference
- review trigger: changes to catalog concepts, public bindings, defaults,
  migrations, persistence behaviour or the parameter document format
- none / guidance only:

## Drift detection

Suspect drift if:
- the same canonical parameter name acquires different value semantics on
  two surfaces
- a command-line parser or Python wrapper supplies a default not present in
  the provider definition, or hand-defines schema that does not round-trip
  through the bundle
- a parameter file contains current default values after canonical save
- task aliases share Last state only because they use the same executable
- a current-only surface publishes an alias or migration, or accepts a stale
  contract
- a shipped profile does not name its surface's current contract
- run state, plans, caches or resource inventory enter a human profile
- an explicit source profile changes without an explicit save action
