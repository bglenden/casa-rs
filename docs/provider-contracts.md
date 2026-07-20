# Provider Contracts and Schema Bundles

This document defines the contract model between functionality providers and
consumers such as:

- `casars` TUI
- native GUI applications
- Python bindings
- MCP servers
- standalone task binaries

The goal is to keep one source of truth for each provider surface while still
allowing different transports and different UI capabilities.

## Core Rule

The canonical boundary contract is a versioned, typed provider bundle.

- Providers own request/result, command/response, object, and private adapter
  semantics. The shared catalog owns reusable public parameter concepts.
- Rust types implement that contract inside the provider.
- Generated machine artifacts publish that contract to UIs, Python parity
  checks, MCP projections, and test fixtures.
- Presentation hints belong in the same schema bundle as annotations. They do
  not replace the semantic contract.

JSON and JSON Schema remain valid machine transports and projections. They are
not the semantic model itself, and the user-editable parameter profile format
is sparse TOML.

The source of truth is therefore not:

- raw CLI flags
- an app-local TUI form schema
- hand-maintained Python wrappers
- an app-local default or alias table

It is the checked catalog plus the self-contained provider schema bundle that
embeds the concepts referenced by its surface definition.

## Surface Kinds

Not every provider surface should look like a standalone executable task. The
repo supports three primary surface kinds.

### Task Surfaces

Task surfaces are one-shot operations with a request and a result.

Examples:

- `calibrate`
- `casars-imager`

Recommended machine-facing shape:

- `--protocol-info`
- `--json-schema`
- `--json-run <SOURCE>`

The protocol is request/result oriented. CLI flags, TUI forms, native GUI forms,
and Python wrappers are projections of the same task contract.

A task `SurfaceDefinition` also binds its public parameter names to the shared
`ParameterCatalog` and adapts the resolved set into the provider's private,
idiomatic Rust request.

### Session Surfaces

Session surfaces are stateful command streams with request/response envelopes.

Examples:

- `tablebrowser --session`
- `imexplore --session`

These are appropriate when the backend owns interactive state such as:

- current open root
- selection and scroll position
- viewport-dependent snapshots
- prefetch or cache state

Recommended machine-facing shape:

- `--protocol-info`
- `--json-schema`
- a stateful transport such as `--session` over JSON Lines

The contract is a versioned command/event/state protocol, not a one-shot task
request/result pair.

The table and image browser protocols instantiate one shared JSONL session
foundation in `casa-provider-contracts::session_protocol`. That owner provides
the common envelope/error shape, protocol metadata, deterministic schema
generation, and version access; each protocol crate owns only its typed domain
commands and payloads. The `casars` runtime package owns the browser executables,
session engines, and generic subprocess client. Reusable `casa-tables` and
`casa-images` code does not depend on these protocol crates.

A session `SurfaceDefinition` may additionally define durable startup
parameters. Those parameters open or configure the session; they do not turn
cursor movement, scrolling, viewport updates, caches, or commands into task
parameters.

### Object Surfaces

Object surfaces expose stateful handles with constructors, properties, methods,
and lifecycle operations.

Examples:

- `casars.data.Image`
- `casars.data.Table`

These are appropriate for persistent data access APIs that should be used
in-process from Rust or Python, and remotely through a stateful bridge such as
MCP. They should not be forced into the task-binary model if there is no real
CLI or TUI use case.

Recommended machine-facing shape:

- a versioned schema bundle describing constructors, properties, methods, and
  common value types
- no standalone executable required
- handle-based MCP projection for remote access

For object surfaces, the Python binding is normally direct and in-process, while
the MCP projection is stateful and handle-based.

## Bundle Shape

Each provider surface should expose one canonical schema bundle. The bundle
should contain at least:

1. `protocol`
   - protocol name
   - protocol version
   - surface kind: `task`, `session`, or `object`
2. semantic contract
   - tasks: operations with request/result schemas
   - sessions: commands, responses, events, and state snapshots
   - objects: constructors, properties, methods, lifecycle operations
3. parameter contract where the surface is parameterized
   - shared `ParameterCatalog` concepts
   - task or session `SurfaceDefinition`
   - parameter bindings, defaults, roles, validation, migrations, and aliases
4. shared component schemas
   - reusable logical value definitions referenced across operations
5. annotations
   - labels, descriptions, ordering, groupings, widget hints, examples,
     advanced/basic flags, units, and other presentation metadata
6. projection metadata where needed
   - CLI flag names
   - TUI/native GUI rendering hints
   - MCP tool naming or handle semantics

The semantic layer is authoritative. Annotations and projections may add
presentation or mapping metadata, but they must not change semantic meaning.

Task and session providers use the shared
`ProviderContractEnvelope<Semantic, Schemas>` implementation in
`casa-provider-contracts`. The envelope owns protocol identity, semantic
operations, reusable components, annotations, projections, and embedded
parameter surfaces. Provider crates supply only their typed domain schemas and
operations. The common validator rejects wrong surface kinds, missing
parameter surfaces, noncanonical actions, duplicate operations, protocol
metadata errors, and semantic/flattened schema drift.

## Application Catalog and Launching

`casa-provider-contracts` also owns the single `ApplicationCatalog`. Every
parameter surface has exactly one task-application entry that references the
surface by stable ID; the `casars` launcher is the sole typed exception without
a surface. Display name, category, and protocol family are projected from the
referenced surface rather than copied into another inventory. Each application
adds only executable/package identity, its explicit override variable,
interaction metadata, and packaging visibility.

The supported launch modes are `installed_suite` and
`development_workspace`. A process selects one mode at composition/startup.
Installed resolution uses only the installed-suite configuration; development
resolution uses only the declared workspace and Cargo package. A missing
executable is an error in that mode and never triggers a search of the current
directory, neighboring targets, or another mode. GUI, TUI, Python, MCP,
packaging scripts, and suite inventory all project from this catalog.

## Parameter Catalog and Surface Definitions

ADR-0006 defines the common parameter model.

`ParameterCatalog` entries describe reusable concepts:

- stable semantic ID and canonical CASA-facing name
- value type, normalization, base constraints, semantic equality, and, where
  applicable, unit dimension
- semantic role and persistence class
- meaning and public documentation

A canonical name has one meaning across surfaces. Reuse is by semantic concept,
not by matching strings. Two parameters with different meanings must not share
a catalog concept even when a legacy CLI happens to spell them alike.

`ParameterBinding` describes one concept's use on one surface:

- absent, literal, or pure conditional default
- required and activation predicates
- provably narrowing refinements, aliases, and projection metadata
- an optional context role consistent with the concept's semantic role

Aliases are unique within a surface and cannot shadow any canonical parameter
name on that surface.

Concept roles are part of the contract. A path that is safe as an input does not become
an output merely because both serialize as strings. Runtime-only controls such
as executable overrides, progress paths, telemetry, and safety confirmations
are explicitly non-persistable.

`SurfaceDefinition` is tagged as task or session:

- a task definition maps the resolved CASA-named parameter set into a one-shot
  provider invocation
- a session definition maps durable startup parameters into the session open
  operation and leaves later commands to the session protocol

Every optional parameter has an explicit default. Conditional defaults depend
only on other parameters and form an acyclic graph. Dataset-derived context is
an explicit suggestion or startup override, never an undeclared default.

Semantic evolution is guarded by the append-only fingerprints in
`resources/parameter-contract-history.json`. Changing a concept's type,
normalization, constraints, units, role, or persistence requires a new semantic
revision. Changing a surface's bindings, defaults, predicates, safety rules,
provider projections, or migrations requires a new surface contract version.
Run `scripts/check-parameter-contract-history.py --update` only after making and
reviewing those version bumps; the normal architecture gate is check-only.

## Sparse Parameter Documents

The human interchange format is versioned sparse TOML. The `[casars]` table
identifies format version, surface ID, surface kind, and contract version. The
`[parameters]` table contains required values and normalized semantic
differences from current defaults.

This makes additive evolution cheap: a newly added optional parameter receives
its current default without editing existing files. A changed default is also
adopted when the old file omitted that parameter, with a compatibility warning.
Renames and type/value changes require explicit ordered migrations. Future
versions and unmigratable required values are errors.

Parameter documents do not support includes, environment or shell expansion,
remote URLs, or executable expressions. Relative task paths resolve against the
selected workspace or process current directory, not the profile file's parent.
See [Task and Session Parameter Profiles](task-parameters.md) for the user
contract and managed-state lifecycle.

## Canonical schema discovery and presentation projection

`--json-schema` emits a machine representation of the
canonical provider bundle, including the parameter catalog and applicable
surface definition.

Frontends project presentation forms in-process from the embedded canonical
surface. Providers expose no second schema-discovery action or cached
presentation payload.

One-shot provider binaries use `casa-task-runtime::TaskCliHost` for
`--json-schema`, `--protocol-info`, and `--json-run <SOURCE>`. The host owns
stdin/file ingestion, JSON decoding and encoding, and stable failure wording.
Provider CLIs retain only their human argument projection and typed execution
function.

## Shared Versioned Component Schemas

Yes: the architecture should include versioned underlying shared schemas for
protocol-independent basic types.

These shared components should be reused across task, session, and object
surfaces rather than redefined per app. Candidate shared component families
include:

- scalar values
  - bool
  - integer and floating-point values
  - complex values
  - strings
- array values
  - dtype / primitive type
  - shape
  - axis-order semantics
  - fixed-shape and variable-shape array forms
- record values
  - named fields with nested values
- keyword maps
  - table keywords
  - column keywords
  - image misc/info-like nested metadata maps
- error envelopes
  - stable error code
  - message
  - optional structured details
- file/path references
  - file path
  - directory path
  - path role hints for pickers or validation
- viewport / region / selection references where relevant to session surfaces

These shared schemas should be versioned explicitly. A practical model is:

- a versioned family name for the logical type set
- stable `$defs` / component identifiers within that family
- additive extension within a compatible protocol version
- explicit protocol-version bumps for breaking shape changes

The logical schema should describe semantics, not commit every consumer to the
same transport encoding. For example:

- Python can map image planes directly to NumPy arrays in-process.
- MCP can project the same logical array type as a structured payload plus a
  transport-specific binary or JSON representation.

## Projection Rules

All consumers should project from the same canonical schema bundle.

### CLI

CLI parsing is a projection from the semantic contract plus CLI annotation
metadata such as flag names and positional rules.

CLI flags are not the source of truth.

The CLI also projects the common Defaults, Last, Last Successful, explicit
profile, reset, save, and no-save source operations. Noninteractive invocation
starts from Defaults unless a source is explicit.

### `casars` TUI

The TUI projects forms, panes, and result rendering from the same schema bundle
plus TUI-specific annotations. It should not require app-local parallel schemas
that drift from the provider contract.

Framework-owned parameter sessions expose source selection, origins, dirty
state, validation, reset, open, Save As, and revert. A browser's live navigation
state remains in its session model.

### Native GUI

The native macOS GUI reads the same semantic schema bundle and uses
the annotations as hints, not strict layout instructions. Keep hints coarse:

- section ordering
- basic vs advanced
- picker hints
- enum display labels
- units and ranges

Do not freeze app-specific pixel layouts into the provider schema.

Native forms use the same typed parameter session and managed Last store as the
TUI. Swift models may decode a UniFFI projection, but they must not infer
authoritative types, aliases, or defaults.

### Python

Python should remain ergonomic and typed, but its wrappers and docs should be
checked against the canonical schema bundle so signatures and enums cannot drift.

For task surfaces, generated Python callables invoke the canonical `casars run`
transport; Python does not host a second provider protocol engine. For object
surfaces, Python can bind directly in-process.

`casars.parameters` exposes the common typed profile session, while generated
task-specific wrappers delegate through the same runtime. Python must not keep
a second default or migration implementation.

### MCP

MCP should also project from the same schema bundle.

- task surfaces map naturally to MCP tools
- session surfaces map to stateful command/event protocols
- object surfaces map to handle-based services such as `image_open`,
  `image_get_slice`, `image_put_slice`, and `image_close`

Object surfaces should remain stateful over MCP. They should not be forced into
fake-stateless request patterns that reopen files and discard backend state on
every call.

The Wave 4 project agent server is an authenticated composition of these MCP
projections, notebook and receipt operations, open-tab semantic resources, and
local corpus/source retrieval. Its trust identity is not another provider
bundle: `casa-rs-agent-profile/v1` binds a unique server name and per-session
nonce, and only a successful profile handshake grants CASA domain-authority
status. Agent-specific App Server or ACP envelopes stop outside the provider
schemas and durable project contracts.

## Current Repo Mapping

This is the intended classification for current and near-term surfaces.

- `calibrate`: task surface
- `casars-imager`: task surface
- `msexplore`: task surface
- `tablebrowser`: session surface
- `imexplore`: session surface
- `casars.data.Image`: object surface
- `casars.data.Table`: object surface

The first three task examples and all other one-shot catalog tasks have task
definitions. `tablebrowser` and `imexplore` have session startup definitions.
The `casars` launcher and the two object surfaces are not parameter-profile
surfaces.

## Implementation Guidance

- Define typed Rust structs and enums that serialize exactly to the canonical
  schema bundle shapes.
- Define reusable catalog concepts once and bind them explicitly on every
  parameterized task or session surface.
- Generate schema output from those types rather than hand-maintaining schema
  JSON.
- Keep presentation hints in the schema bundle as annotations.
- Keep protocol names and protocol versions explicit and machine-checkable.
- Add CI parity tests so catalog coverage, wrappers, docs, sparse profiles, and
  projections cannot drift from the canonical schema bundle.
- Regenerate committed browser-session schemas only through
  `CASA_RS_REGENERATE_SESSION_SCHEMAS=1 cargo test -p
  casars-tablebrowser-protocol -p casars-imagebrowser-protocol`, then rerun the
  tests normally to prove the artifacts are current.

This preserves one source of truth while still supporting:

- direct Rust APIs
- Python bindings
- `casars` TUI
- native GUIs
- MCP servers
- standalone binaries where they make sense
