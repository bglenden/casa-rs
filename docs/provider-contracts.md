# Provider Contracts and Schema Bundles

This document defines the contract model between functionality providers and
consumers such as:

- `casars` TUI
- future native GUI applications
- Python bindings
- MCP servers
- standalone task binaries

The goal is to keep one source of truth for each provider surface while still
allowing different transports and different UI capabilities.

## Core Rule

The canonical boundary contract is a versioned JSON schema bundle.

- Providers own a semantic contract expressed as JSON request/result,
  command/response, or object/method/property schemas.
- Rust types implement that contract inside the provider.
- Generated schema artifacts publish that contract to UIs, Python parity checks,
  MCP projections, and test fixtures.
- Presentation hints belong in the same schema bundle as annotations. They do
  not replace the semantic contract.

The source of truth is therefore not:

- raw CLI flags
- an app-local TUI form schema
- hand-maintained Python wrappers

It is the provider schema bundle.

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
3. shared component schemas
   - reusable logical value definitions referenced across operations
4. annotations
   - labels, descriptions, ordering, groupings, widget hints, examples,
     advanced/basic flags, units, and other presentation metadata
5. projection metadata where needed
   - CLI flag names
   - TUI/native GUI rendering hints
   - MCP tool naming or handle semantics

The semantic layer is authoritative. Annotations and projections may add
presentation or mapping metadata, but they must not change semantic meaning.

## `--json-schema` and `--ui-schema`

For new work, `--json-schema` should emit the canonical schema bundle.

If `--ui-schema` exists, it should be treated as:

- a derived compatibility view of the canonical schema bundle, or
- a legacy alias retained for current launcher integration

`--ui-schema` must not become a separate source of truth. If a field, enum, or
operation exists for UI use, it should exist in the canonical schema bundle
first and then be projected into the UI-specific view.

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

### `casars` TUI

The TUI projects forms, panes, and result rendering from the same schema bundle
plus TUI-specific annotations. It should not require app-local parallel schemas
that drift from the provider contract.

### Native GUI

A future native macOS GUI should read the same semantic schema bundle and use
the annotations as hints, not strict layout instructions. Keep hints coarse:

- section ordering
- basic vs advanced
- picker hints
- enum display labels
- units and ranges

Do not freeze app-specific pixel layouts into the provider schema.

### Python

Python should remain ergonomic and typed, but its wrappers and docs should be
checked against the canonical schema bundle so signatures and enums cannot drift.

For task surfaces, Python can invoke a task binary or other provider transport.
For object surfaces, Python can bind directly in-process.

### MCP

MCP should also project from the same schema bundle.

- task surfaces map naturally to MCP tools
- session surfaces map to stateful command/event protocols
- object surfaces map to handle-based services such as `image_open`,
  `image_get_slice`, `image_put_slice`, and `image_close`

Object surfaces should remain stateful over MCP. They should not be forced into
fake-stateless request patterns that reopen files and discard backend state on
every call.

## Current Repo Mapping

This is the intended classification for current and near-term surfaces.

- `calibrate`: task surface
- `casars-imager`: task surface
- `msexplore`: task surface
- `tablebrowser`: session surface
- `imexplore`: session surface
- `casars.data.Image`: object surface
- `casars.data.Table`: object surface

## Implementation Guidance

- Define typed Rust structs and enums that serialize exactly to the canonical
  schema bundle shapes.
- Generate schema output from those types rather than hand-maintaining schema
  JSON.
- Keep presentation hints in the schema bundle as annotations.
- Keep protocol names and protocol versions explicit and machine-checkable.
- Add CI parity tests so wrappers, docs, and projections cannot drift from the
  canonical schema bundle.

This preserves one source of truth while still supporting:

- direct Rust APIs
- Python bindings
- `casars` TUI
- future native GUIs
- MCP servers
- standalone binaries where they make sense
