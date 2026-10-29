# Provider Contracts and Schema Bundles

This document defines the contract model between functionality providers and
their consumers:

- the Python package `casars`
- the task executables of the application catalog, run directly from the
  command line

ADR-0017 makes these the product surfaces; casa-rs has no separate launcher,
terminal UI, native GUI, or interactive browser sessions.

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
`casars_data_objects` (`casars.data.schema_bundle()`, protocol version 1),
describes them all; its tool entries all come from one list of the
`casars.tools` classes, and each names its own module in its projection
entry ([Compatibility](python/versioning.md)).
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
surface by stable ID, and every application is a task: the catalog's 26
entries are its 26 task surfaces. Display name, category, and protocol
family are projected from the referenced surface rather than copied into
another inventory. Each application adds only executable/package identity,
its explicit override variable, interaction and presentation metadata, and
packaging visibility. (ADR-0017 keeps the older presentation fields, such as
`shell_kind` and `interaction`, until the CASA-shaped Python work reshapes
them.) Several surfaces may share one executable, a family whose first word
names the surface: `imhead` and `imstat` run `imexplore`; `plotms` runs
`msexplore`; `split` runs `mstransform`; and `gaincal`, `bandpass`,
`applycal`, `fluxscale`, and `gencal` run `calibrate`, whose own `calibrate`
surface is its default.

`casa_provider_contracts::resolve_task_executable` is the one search for the
file a task runs; Python reaches it through `casars._core`. The first step
that applies decides:

1. the task's override variable (`override_env`, `CASARS_<APP>_BIN`, such as
   `CASARS_IMAGER_BIN` or `CASARS_CALIBRATE_BIN`);
2. a suite root, `CASARS_SUITE_ROOT` or else the installed suite the Python
   package belongs to (the installer's environment is `<suite>/python`,
   beside `<suite>/bin` and `bundle-manifest.json`), whose `bin/` holds the
   executable;
3. a development checkout: the newest `debug` or `release` build of the
   executable under the workspace the package was built from, in
   `$CARGO_TARGET_DIR` or `target/`.

A step that applies but lacks the file is an error naming what it lacked, and
never falls through to a later step, so an installed suite never runs a
checkout's build. A missing development build is reported with the
`cargo build -p <package> --bin <executable>` that makes it. Nothing is run
through `cargo`, `PATH` is not searched, and the caller's own path
(`current_exe`) is not consulted. Python, packaging scripts, and the suite
inventory all project from this catalog.

## Parameter Catalog and Surface Definitions

ADR-0006 defines the common parameter model. ADR-0012 defines the optional
current-only profile contract used by simobserve.

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
as executable overrides, progress paths, telemetry, and Last controls are
explicitly non-persistable. There are no run-time confirmations: CASA's own
parameters, such as `overwrite`, are the consent to replace or change data
(#931).

A task `SurfaceDefinition` maps the resolved CASA-named parameter set into a
one-shot provider invocation.

Every optional parameter has an explicit default. Conditional defaults depend
only on other parameters and form an acyclic graph. Dataset-derived context is
an explicit suggestion or override, never an undeclared default.

Semantic evolution is guarded by the append-only fingerprints in
`resources/parameter-contract-history.json`. Changing a concept's type,
normalization, constraints, units, role, or persistence requires a new semantic
revision. Changing a surface's bindings, defaults, predicates, provider
projections, or migrations requires a new surface contract version.
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
simobserve surface is current-only. A chain may start after contract 1:
`tclean`, the imager's surface since contract 29 (#931), migrates profiles
from contract 28, the `imager` surface's last, and answers to that former
id. Future, stale, and otherwise
unmigratable contracts are errors.

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

Every one-shot task binary answers `--json-schema`, `--protocol-info` and
`--json-run <SOURCE>` through the shared task front end below, which owns
stdin and file ingestion, JSON decoding and encoding, and the typed reply.
A binary keeps only its typed execution callback.

## The shared task front end and typed replies

`casa-task-runtime::TaskFrontEnd` is the one entry every task binary's `main`
calls (#931): the image binaries and `imexplore`; the MeasurementSet binaries
(`msexplore` with `plotms`, `mstransform` with `split`, `flagdata`,
`flagmanager` and `importvla`); `calibrate`, which hosts `calibrate`,
`applycal`, `gaincal`, `bandpass`, `fluxscale` and `gencal`;
`casars-imager`; and `simobserve`. A
surface's choices take their canonical names (`--preset amplitude_vs_time`),
and every flag it declares, short ones such as msexplore's `-o` included,
reaches its parameter. The front end is generic over the
binary's execution callback and owns the surface's declared flags, the base
sources, `--workspace`, `--unset`, `--save-params`, `--no-save-last`, the
non-executing `--show`, `--template` (also `--profile-template`), `--describe`
and `--validate`, and the machine actions. A spelling the surface declares is
always its parameter, so imregrid's `--template` names its template image and
`--profile-template` gives its profile. A family binary's first word names the
surface it runs, such as `imexplore imstat`, `mstransform split` or
`calibrate gaincal`; the first hosted surface runs otherwise. A
`--json-run` request names its surface itself; the `fixed_args` that imhead,
imstat and the calibration surfaces carry repeat it as the first word, while
split and plotms carry none. The callback reads the resolved values
into the binary's typed request through `TaskInvocation`'s typed reads, and
`TaskInvocation::is_set` tells a value the run sets from a default. What a
command line prints is the callback's text (msexplore's summary in the
user's `format`), with notes such as msexplore's flag-edit line on stderr;
`--json-run` carries the value and result instead.

The binary's `main` owns the process. It reads the logging configuration from
the environment (`CasaLoggingConfig::from_env`) and passes it, with its
arguments, how the process enters a directory (`std::env::set_current_dir`)
and its standard output and error, to `TaskFrontEnd::main`. The front end
reads no environment variable and changes no process state itself
(ADR-0016): it applies the logging flags, and for a run installs logging that
writes to the stderr it was handed and to a log table only when one is named.
An action that runs nothing installs no logging, so a bad `CASA_RS_LOG_*`
setting refuses only a run.

A command-line run enters its workspace (`--workspace`, the current directory
otherwise) before the task starts, as a Python task call starts the
executable in its workspace.
Every relative path the run names then resolves in the workspace: inputs,
products, and paths inside a parameter's text, such as `imstat`'s region file
or the image in `immoments`' mask expression. The files the controls name
(`--params`, `--save-params`, a log table) are read and written from where the
command was run, before it enters. A `--json-run` runs where its caller
started it.

`--json-run SOURCE` takes a `TaskRunRequest`, `{"surface": ..., "values":
{...}}` with the CASA-named values the caller set, and resolves it against the
surface's contract inside the binary. It writes one `TaskReply` to stdout:

- `{"status": "succeeded", "surface", "value", "result"}`, where `value` is
  what CASA's task returns (`null` for `None`), kept ahead of the provider's
  full `result`, from which products are projected;
- `{"status": "failed", "surface", "kind", "message", "parameter"?}`, where
  `kind` is `validation` (refused before anything changed), `execution`, or
  `interrupted`: the task caught an interrupt, stopped at its next safe point
  and published nothing, as `casars-imager` does on SIGINT.

The exit status agrees with the reply: 0 for success, 2 for a validation
failure, 1 for an execution failure and 130 for an interrupted run.
`classify_task_exit` reads a finished process from its exit status and stdout
alone, never stderr, and keeps a launch failure, a signal and a malformed reply
distinct from the task's own failures. A provider contract carries both
schemas under `semantic.front_end`.

A surface with a `managed_output` contract names the decoder that reads its
provider result (`calibration-report-v1`, `imager-run-v1`,
`simobserve-run-v1`): the reply's `result` is that result, from which the
completion decoder projects products and the summary. Nothing is injected
into the binary's arguments to ask for it.

Last has one owner on each path: a command line run of a binary records Last
before the callback and Last Successful only after success, unless
`--no-save-last`; `--json-run` reads and writes neither, so Python task calls
save none. Every Python task call is one invocation shape: the executable the
resolver names, the surface's `fixed_args`, then `--json-run -` with the
values the call set on stdin. Python passes the task's log lines to
`sys.stderr` as they arrive and classifies the reply through `casars._core`.
A success returns a `TaskCompletion` carrying the reply's `value` and the
products the decoder projects. A `validation` reply raises
`TaskParameterError`, a `ValueError` whose completion's `status` is
`refused`; anything else raises `TaskExecutionError`, whose completion's
`status` is `failed`, `interrupted`, `signalled` or `malformed_reply`. A
value the contract refuses when Python resolves the call raises `ValueError`
before anything starts, or `TypeError` for a value of the wrong type.

A succeeded reply's `value` is CASA's return value as a tagged `CasaValue`
(`casa-provider-contracts`): `none`, `bool`, `int`, `float` (non-finite
values as `"nan"`, `"inf"` and `"-inf"`), `string`, `list`, an ordered
`dict` whose keys are strings or integers, and typed arrays (`bool_array`,
`int_array` with dtype `int32` or `int64`, `float_array` with `float32` or
`float64`, `complex_array`, `str_array`) that carry their shape and their
data in column-major (Fortran) order. `casars._core` rebuilds Python's
types and NumPy arrays from it, so a CASA-signature wrapper returns what
CASA returns. `tools/casa-compat/record_casa_returns.py` records the
structure of CASA's own return values
(`resources/casa-compat/casa-6.7.6.14/returns.json`), which the image,
MeasurementSet and calibration task tests compare against.

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

Each task executable's command line, through the shared task front end, also
projects the common Defaults, Last, Last Successful, explicit profile, reset
(`--unset`), save (`--save-params`), and no-save (`--no-save-last`) source
operations, and the non-executing `--show`, `--template`, `--describe` and
`--validate`. Invocation starts from Defaults unless a source is explicit.

### Python

Python stays ergonomic and typed, but its wrappers and docs are checked against
the canonical schema bundle so signatures and enums cannot drift.

For task surfaces, generated Python callables start the task's executable
with the canonical `--json-run -` transport; Python does not host a second
provider protocol engine. For object surfaces, Python binds directly
in-process.

`casars.parameters` exposes the common typed task profile, while generated
task-specific wrappers delegate through the same runtime. Python must not keep
a second default or migration implementation.

## Current Repo Mapping

- `calibrate`: task surface
- `casars-imager` (surface `tclean`): task surface
- `msexplore` and `plotms`: task surfaces served by `msexplore`
- `imhead` and `imstat`: task surfaces served by `imexplore`
- `casars.data.Image`: object surface
- `casars.data.Table`: object surface
- `casars.tools.simulator`: object surface
- `casars.tools.measures`: object surface
- `casars.tools.quanta`: object surface

All 26 catalog surfaces have task definitions; the
[generated parameter reference](reference/task-parameters.md) lists them. The
five object surfaces are not parameter-profile surfaces.

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
Python package, and the task executables.
