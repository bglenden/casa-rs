# ADR-0006: Unified parameter catalog and sparse profiles

Status: accepted
Date: 2026-07-09
Truth class: normative
Supersedes:
Superseded by:

## Context

ADR-0003 makes the provider bundle the boundary contract, but it does not yet
define one authoritative model for user-editable parameters. Task defaults,
aliases, validation, UI fields, Python signatures, and saved requests can
therefore drift between providers and consumers. Session surfaces add another
failure mode: a useful startup configuration can be confused with viewport,
cursor, cache, or command-stream state.

Users need parameter files that are readable in a Markdown code block, easy to
edit and review, sparse enough to adopt newly added defaults, and usable from
the CLI, TUI, native GUI, and Python without changing meaning.

## Decision

The provider bundle remains the self-contained boundary contract and gains a
format-neutral parameter model. The repository also maintains one checked
aggregate catalog from which the concepts embedded in those bundles are
selected. Its authoritative concepts are:

- `ParameterCatalog`: reusable parameter concepts with stable semantic IDs,
  canonical CASA-facing names, value types, units or dimensions,
  normalization, semantic roles, persistence classes, constraints, and
  documentation.
- `ParameterBinding`: a surface-specific use of a catalog concept, including
  its default, requirement and activation rules, narrowing constraints,
  aliases, presentation hints, and private provider projection.
- `SurfaceDefinition`: a tagged task or session definition containing the
  ordered parameter bindings and the adapter or protocol projection needed by
  that surface.

A canonical name has one meaning. For example, `imsize` must retain the same
value semantics wherever it is bound. Parameters that happen to share a CLI
spelling but have different meanings use different semantic IDs and must not be
silently unified. Roles such as input, output, selection, science control, and
presentation remain explicit so a path or scalar cannot be reused in an unsafe
role merely because its storage type matches.

Task definitions resolve parameters into one-shot invocation requests. Session
definitions resolve only durable startup intent for stateful surfaces such as
`imexplore` and `tablebrowser`; their command/event protocols remain separate.
The `casars` launcher, object constructors and handles, runtime controls, and
transient session state are not parameterized surfaces.

Human parameter documents use sparse TOML. The metadata table identifies the
document format, logical surface, surface kind, and contract version; the
`parameters` table contains required values and semantic differences from the
current defaults. Optional values equal to their defaults are omitted.

```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
vis = "data/target.ms"
imagename = "products/target"
imsize = 1024
cell = "0.2arcsec"
niter = 10000
threshold = "2mJy"
```

The following evolution rules apply:

- Adding an optional defaulted parameter does not require existing files to
  change.
- An omitted parameter adopts the current default. Loading warns when that
  default changed since the document contract version.
- Renames and type or value changes use explicit ordered migrations of the
  sparse overrides.
- A newly required parameter needs a migration or causes a load error.
- Future document or contract versions are rejected.
- Saving rewrites a document at the current contract version against the
  current defaults.

The managed workspace store is:

```text
<workspace>/.casa-rs/parameters/<surface-id>/
  last.toml
  last-successful.toml
```

`CASA_RS_STATE_DIR` may redirect the managed state root for automation without
changing how relative task paths resolve. Managed writes use a per-surface lock,
a same-directory temporary file, and atomic replacement.

For tasks, `last.toml` records the validated resolved user intent immediately
before execution and `last-successful.toml` updates only after successful
completion. Failure or cancellation does not replace the successful state. For
sessions, `last.toml` updates after a successful open and after debounced,
validated durable-setting changes; sessions have no `last-successful.toml`
because they have no single successful-completion event. Failed opens and
transient navigation do not replace Last.

Opening an explicit profile never grants permission to overwrite that file.
Automatic persistence always targets the managed store; changing an explicit
file requires Save As or another explicit save operation.

## Consequences

Positive:
- defaults, validation, docs, projections, and persistence share one source of truth
- sparse files remain readable and naturally adopt additive parameters
- the same profile has the same meaning across CLI, TUI, GUI, and Python
- task aliases retain distinct logical identities and Last files even when they share a provider binary
- browser startup settings can be reloaded without persisting event logs or UI internals

Negative:
- every parameterized surface must define complete typed bindings and migrations
- default changes require deliberate compatibility diagnostics
- consumers must migrate away from app-local default, alias, and type inference

Neutral / tradeoffs:
- sparse profiles reproduce user intent under current defaults; they are not exact historical run-replay records
- provider bundles may still use JSON as a machine transport, but JSON Schema and CLI flags are projections rather than the semantic model
- exact workspace restoration may use a separate opaque artifact, but it is not a human parameter profile

## Alternatives considered

1. Persist complete JSON request envelopes, freezing old defaults into every file.
2. Keep independent CLI, TUI, GUI, and Python parameter definitions.
3. Serialize complete session snapshots or replay command histories.
4. Use unversioned TOML maps without catalog identities or explicit migrations.

## Enforcement

This decision is enforced by:
- tests: catalog completeness, contract invariants, sparse TOML round trips, migrations, managed-store lifecycle, and cross-surface resolution parity
- lint/import/dependency rules: consumer layers may project provider definitions but may not introduce authoritative app-local defaults or aliases
- CI checks: `just verify` plus frontend-specific contract and projection tests
- append-only semantic fingerprints: a changed concept revision or surface
  contract cannot replace an existing `(id, version)` history entry
- review trigger: changes to catalog concepts, public bindings, defaults, migrations, persistence behavior, or parameter document format require boundary-contract review
- none / guidance only:

## Drift detection

Suspect drift if:
- the same canonical parameter name acquires different value semantics on two surfaces
- a UI or Python wrapper supplies a default not present in the provider definition
- a parameter file contains current default values after canonical save
- task aliases share Last state only because they use the same executable
- session command arrays, cursor positions, viewports, caches, or playback state enter a human profile
- an explicit source profile changes without an explicit save action
