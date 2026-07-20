# AGENTS

## Purpose

`ratatui-graphics` is an internal Rust library for embedding images and scientific plots into
`ratatui` applications, with Ghostty and the Kitty graphics protocol as the primary target.

The first real consumer is `../casa-rs`.
Optimize for that integration, not for crates.io or external users.

## Intended Use

Keep this repo as a sibling of `../casa-rs` and consume it with a local path dependency:

```toml
[workspace.dependencies]
ratatui-graphics = { path = "../ratatui-graphics" }
```

Then in the consuming crate:

```toml
[dependencies]
ratatui-graphics.workspace = true
```

Do not copy source files into `casa-rs`.

## Architectural Boundaries

The library owns:

- `PanelScheduler` in `src/panel.rs`
  - one worker engine for latest-wins and bounded-ordered policies
  - request-id and generation-based stale-result filtering
  - typed worker, panic, disconnect, and queue failures
  - app-owned current protocol/current image state

- `KittyLayerManager` in `src/kitty.rs`
  - typed layer handles
  - id allocation
  - upload/place/clear/delete operations

- generic helpers in:
  - `src/bitmap.rs`
  - `src/image_ops.rs`
  - `src/terminal.rs`

The library should not grow demo-only UX concerns.

Keep out of the public library API:

- named plot background presets
- palette cycling
- scientific plot copy/text
- ad hoc keybindings
- example-only layout helpers unless they become broadly reusable

Those belong in `examples/` or in the consumer app.

## Rendering Model

There are two distinct rendering paths:

1. Panel rendering via `ratatui-image`
   - image is adapted to a `ratatui` rectangle
   - image is clipped by the widget/layout system
   - good for normal subpanels inside a TUI layout

2. Direct Kitty layers
   - image placement is outside `ratatui`'s clipping model
   - caller must explicitly clear/delete layers
   - good for overlays/underlays/floating graphics

Do not blur these two models together in the API.

## Repository Expectations

- Use `apply_patch` for edits.
- Prefer `rg` / `rg --files` for search.
- Keep public APIs typed and explicit.
- Avoid reintroducing raw channel tuples or raw image/placement ids as public interfaces.
- Favor request ids over cloned payload matching.
- Keep feature flags working:
  - `panel`
  - `kitty`
  - `plotters`
  - `terminal-detect`

## Verification

Run these before concluding a refactor:

- `cargo check --all-features`
- `cargo test --all-features`
- `cargo check --example panel_only`
- `cargo check --example kitty_overlay`
- `cargo check --example demo`

If you change Ghostty/Kitty behavior, also do a manual smoke run:

- `cargo run --example demo`

But do not claim compositing/layering is verified unless it was viewed in Ghostty itself.

## Notes for Future Refactors

- If `../casa-rs` needs additional lifecycle/state owned by the library, prefer adding it here
  rather than pushing more orchestration back into the consumer.
- If multiple consumers appear later, revisit whether this should become a dedicated reusable
  crate or remain an internal sibling dependency.
