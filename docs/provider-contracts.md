# Provider Contracts and Schema Bundles

This document defines the contract model between functionality providers and
their consumers:

- the headless `casars` task host (`casars run`, `casars params`)
- the Python package `casars`
- standalone task binaries

ADR-0017 makes these the product surfaces; casa-rs has no terminal UI, native
GUI, or interactive browser sessions.

The goal is to keep one source of truth for each provider surface while still
allowing different transports.

## Core Rule

The canonical boundary contract is a versioned, typed provider bundle.

- Providers own request/result, object, and private adapter semantics. The
  shared catalog owns reusable public parameter concepts.
- Rust types implement that contract inside the provider.
- Generated machine artifacts publish that contract to the CLI, Python parity
  checks, and test fixtures.
- Presentation hints belong in the same schema bundle as annotations. They do
  not replace the semantic contract.

JSON and JSON Schema remain valid machine transports and projections. They are
not the semantic model itself, and the user-editable parameter profile format
is sparse TOML.

The source of truth is therefore not:

- raw CLI flags
- hand-maintained Python wrappers
- an app-local default or alias table

It is the checked catalog plus the self-contained provider schema bundle that
embeds the concepts referenced by its surface definition.

## Surface Kinds

A provider bundle advertises one of two surface kinds (`ProviderSurfaceKind`).
Every parameter surface is a task; the profile `SurfaceKind` has the single
variant `task`.

### Task Surfaces

Task surfaces are one-shot operations with a request and a result.

Examples:

- `calibrate`
- `casars-imager`

Machine-facing shape:

- `--protocol-info`
- `--json-schema`
- `--json-run <SOURCE>`

The protocol is request/result oriented. CLI flags and Python wrappers are
projections of the same task contract.

A task `SurfaceDefinition` also binds its public parameter names to the shared
`ParameterCatalog` and adapts the resolved set into the provider's private,
idiomatic Rust request.

### Object Surfaces

Object surfaces expose stateful handles with constructors, properties, methods,
and lifecycle operations.

The object surfaces are:

- `casars.data.Image`
- `casars.data.Table`
- `casars.tools.simulator`, CASA's `sm` tool (ADR-0019)
- `casars.tools.measures`, CASA's `me` tool
- `casars.tools.quanta`, CASA's `qa` tool

The `casars.data` classes are persistent data access APIs; the `casars.tools`
classes are CASA tools with CASA's method names, parameter order and
effective defaults. All are used in-process from Python, are not forced into
the task-binary model and have no standalone executable. One bundle,
`casars_data_objects` (`casars.data.schema_bundle()`), describes them all;
its tool entries all come from one list of the `casars.tools` classes.
`casars.simutil`, a port of CASA's `simutil` helper class rather than a CASA
tool, is not an object surface and is not in the bundle.

A CASA tool's entry carries:

- each method's parameters as a JSON Schema object whose properties give the
  value kind and the installed CASA binding's default (a parameter without a
  default is `required`); CASA's `any` parameters refer to the shared
  `CasaQuantityArgument` and `CasaMeasureArgument` components, and its
  `record` parameters are JSON objects;
- each method's result kind (a boolean, string, record, list of strings or
  array of numbers);
- in `projections.python`, the tool's module and class and the parameter
  order of its constructor (keyword-only casa-rs controls) and of each
  method, which JSON Schema's properties do not keep;
- in `annotations.python`, whether casa-rs implements each method.

The Python tests compare each tool's entry with `inspect.signature` of the
class and with the installed CASA binding.

Machine-facing shape:

- a versioned schema bundle describing constructors, properties, methods, and
  common value types
- no standalone executable required

## Bundle Shape

Each provider surface exposes one canonical schema bundle. The bundle contains
at least:

1. `protocol`
   - protocol name
   - protocol version
   - surface kind: `task` or `object`
2. semantic contract
   - tasks: operations with request/result schemas
   - objects: constructors, properties, methods, lifecycle operations
3. parameter contract where the surface is parameterized
   - shared `ParameterCatalog` concepts
   - the task `SurfaceDefinition`
   - parameter bindings, defaults, roles, validation, migrations, and aliases
4. shared component schemas
   - reusable logical value definitions referenced across operations
5. annotations
   - labels, descriptions, ordering, groupings, examples, advanced/basic and
     hidden flags, units, and other presentation metadata
6. projection metadata where needed
   - CLI flag names and positional rules
   - Python parameter names

The semantic layer is authoritative. Annotations and projections may add
presentation or mapping metadata, but they must not change semantic meaning.

Task providers use the shared
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
surface by stable ID; the `casars` task host is the one application without a
surface. Display name, category, and protocol family are projected from the
referenced surface rather than copied into another inventory. Each application
adds only executable/package identity, its explicit override variable,
interaction and presentation metadata, and packaging visibility. (ADR-0017
keeps the older presentation fields, such as `shell_kind` and `interaction`,
until the CASA-shaped Python work reshapes them.) Several surfaces may share one executable: `imhead` and
`imstat` run `imexplore`, and `gaincal`, `bandpass`, `applycal`, `fluxscale`,
and `gencal` run `calibrate` with a fixed mode.

The supported launch modes are `installed_suite` and
`development_workspace`. A process selects one mode at composition/startup.
Installed resolution uses only the installed-suite configuration; development
resolution uses only the declared workspace and Cargo package. A missing
executable is an error in that mode and never triggers a search of the current
directory, neighboring targets, or another mode. The CLI, Python, packaging
scripts, and suite inventory all project from this catalog.

## Parameter Catalog and Surface Definitions

ADR-0006 defines the common parameter model. ADR-0012 defines the optional
current-only profile contract used by imager.

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

A task `SurfaceDefinition` maps the resolved CASA-named parameter set into a
one-shot provider invocation.

Every optional parameter has an explicit default. Conditional defaults depend
only on other parameters and form an acyclic graph. Dataset-derived context is
an explicit suggestion or override, never an undeclared default.

Semantic evolution is guarded by the append-only fingerprints in
`resources/parameter-contract-history.json`. Changing a concept's type,
normalization, constraints, units, role, or persistence requires a new semantic
revision. Changing a surface's bindings, defaults, predicates, safety rules,
provider projections, or migrations requires a new surface contract version.
Run `scripts/check-parameter-contract-history.py --update` only after making and
reviewing those version bumps; the normal architecture gate is check-only.

## Sparse Parameter Documents

The human interchange format is versioned sparse TOML. The `[casars]` table
identifies format version, surface ID, surface kind (always `task`), and
contract version. The `[parameters]` table contains required values and
normalized semantic differences from current defaults.

This makes additive evolution cheap: a newly added optional parameter receives
its current default without adding an override to a saved file. A surface may
publish explicit ordered migrations for older contracts, including warnings for
changed defaults. An empty migration set declares a current-only surface: only
the exact current contract and canonical parameter names are accepted. The
imager surface is current-only. Future, stale, and otherwise unmigratable
contracts are errors.

Parameter documents do not support includes, environment or shell expansion,
remote URLs, or executable expressions. Relative task paths resolve against the
selected workspace or process current directory, not the profile file's parent.
See [Task Parameter Profiles](task-parameters.md) for the user contract and
managed-state lifecycle.

## Canonical schema discovery

`--json-schema` emits a machine representation of the
canonical provider bundle, including the parameter catalog and applicable
surface definition.

Consumers project from the embedded canonical surface in-process. Providers
expose no second schema-discovery action or cached presentation payload.

One-shot provider binaries use `casa-task-runtime::TaskCliHost` for
`--json-schema`, `--protocol-info`, and `--json-run <SOURCE>`. The host owns
stdin/file ingestion, JSON decoding and encoding, and stable failure wording.
Provider CLIs retain only their human argument projection and typed execution
function.

## Shared Versioned Component Schemas

Protocol-independent basic types have versioned shared schemas, reused across
task and object surfaces rather than redefined per provider. Component
families include:

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
  - path role hints for validation

These shared schemas are versioned explicitly:

- a versioned family name for the logical type set
- stable `$defs` / component identifiers within that family
- additive extension within a compatible protocol version
- explicit protocol-version bumps for breaking shape changes

The logical schema describes semantics, not one transport encoding. For
example, Python maps image planes directly to NumPy arrays in-process.

## Projection Rules

All consumers project from the same canonical schema bundle.

### CLI

CLI parsing is a projection from the semantic contract plus CLI annotation
metadata such as flag names and positional rules.

CLI flags are not the source of truth.

`casars run` and `casars params` also project the common Defaults, Last, Last
Successful, explicit profile, reset, save, and no-save source operations.
Invocation starts from Defaults unless a source is explicit.

### Python

Python stays ergonomic and typed, but its wrappers and docs are checked against
the canonical schema bundle so signatures and enums cannot drift.

For task surfaces, generated Python callables invoke the canonical `casars run`
transport; Python does not host a second provider protocol engine. For object
surfaces, Python binds directly in-process.

`casars.parameters` exposes the common typed task profile, while generated
task-specific wrappers delegate through the same runtime. Python must not keep
a second default or migration implementation.

## Current Repo Mapping

- `calibrate`: task surface
- `casars-imager` (surface `imager`): task surface
- `msexplore` and `plotms`: task surfaces served by `msexplore`
- `imhead` and `imstat`: task surfaces served by `imexplore`
- `casars.data.Image`: object surface
- `casars.data.Table`: object surface
- `casars.tools.simulator`: object surface
- `casars.tools.measures`: object surface
- `casars.tools.quanta`: object surface

All 27 catalog surfaces have task definitions; the
[generated parameter reference](reference/task-parameters.md) lists them. The
`casars` task host and the five object surfaces are not parameter-profile
surfaces.

## Implementation Guidance

- Define typed Rust structs and enums that serialize exactly to the canonical
  schema bundle shapes.
- Define reusable catalog concepts once and bind them explicitly on every
  parameterized task surface.
- Generate schema output from those types rather than hand-maintaining schema
  JSON.
- Keep presentation hints in the schema bundle as annotations.
- Keep protocol names and protocol versions explicit and machine-checkable.
- Add CI parity tests so catalog coverage, wrappers, docs, sparse profiles, and
  projections cannot drift from the canonical schema bundle.

This preserves one source of truth while supporting direct Rust APIs, the
Python package, the `casars` task host, and standalone binaries.
