# Table Browser Protocol v1

`tablebrowser --session` speaks a versioned JSON Lines protocol over stdio.
Each request and response is one JSON object per line, terminated by `\n`.

Version `1` is read-only. The contract is still shaped for later edit support:
responses carry stable selection addresses, typed inspector payloads, and
capability flags so future mutation commands can target the same objects
without redesigning the wire format.

## Transport

- Producer: `tablebrowser --session`
- Consumer: `casars`
- Encoding: UTF-8 JSON
- Framing: one object per line
- Versioning: top-level `version` field in both directions

The committed JSON Schemas live in:

- `crates/casacore-tablebrowser-protocol/schemas/request.schema.json`
- `crates/casacore-tablebrowser-protocol/schemas/response.schema.json`

## Request Envelope

Every request has:

- `version`: protocol version expected by the client
- `command`: tagged command payload

Supported commands in v1:

- `open_root { path, viewport }`
- `resize { viewport }`
- `cycle_view { forward, viewport? }`
- `move_up { steps, viewport? }`
- `move_down { steps, viewport? }`
- `move_left { steps, viewport? }`
- `move_right { steps, viewport? }`
- `page_up { pages, viewport? }`
- `page_down { pages, viewport? }`
- `activate { viewport? }`
- `back { viewport? }`
- `escape { viewport? }`
- `get_snapshot { viewport? }`

`casars` sends semantic navigation commands. The backend owns table stack,
selection, scroll state, and inspector state.

## Response Envelope

Every response has:

- `version`: protocol version returned by the backend
- `response`: tagged payload

Response variants:

- `snapshot`: full render snapshot after applying the command
- `error`: structured machine-readable error

Errors are part of the protocol, not inferred from stderr. Stderr is reserved
for debugging context.

## Snapshot Model

A snapshot contains:

- `capabilities`: currently `editable: false` in v1
- `view`: one of `overview`, `columns`, `keywords`, `cells`, `subtables`
- `focus`: `main` or `inspector`
- `table_path`
- `breadcrumb`
- `viewport`
- `status_line`
- `content_lines`
- `selected_address`
- `inspector`

`content_lines` is already viewport-bounded and render-ready for the TUI.

## Stable Addresses

Selections are identified with stable addresses so later edit commands can
target the same object graph:

- `column`
- `cell`
- `table_keyword`
- `column_keyword`
- `subtable`

Nested values use `value_path` segments:

- `record_field { name }`
- `array_index { flat_index }`

## Typed Inspector Payloads

The inspector never degrades to string-only values on the wire. v1 supports:

- all scalar primitives
- all array primitives and ranks
- records
- `table_ref`
- `undefined`

Complex numbers remain typed:

- `complex32 { re, im }`
- `complex64 { re, im }`

Arrays are paged by flat row-major element index and carry both flat and
multidimensional indices. Records are paged by field summaries.

## Future Edit Extension

Edit mode is intentionally not implemented in v1. The protocol is designed so
later additive commands can target the existing addresses and inspector paths,
for example:

- `begin_edit`
- `update_draft`
- `commit`
- `cancel`

That future extension should remain backward-compatible with the v1 snapshot
shape and use `capabilities.editable` to advertise availability.
