# ratatui-graphics

`ratatui-graphics` is an internal Rust library for embedding plots and images into
`ratatui` applications that target Ghostty and the Kitty graphics protocol first.

It intentionally optimizes for one near-term consumer: `../casa-rs`.
This crate is not shaped for public release yet.

## What It Provides

The library supports two rendering paths:

- Panel rendering through `ratatui-image`, where the image is adapted to a `ratatui` layout
  rectangle and clipped by the widget system.
- Direct Kitty graphics layers, where uploaded images are placed independently of `ratatui`
  and must be explicitly cleared or deleted by the application.

The current public API is centered on:

- `PanelRenderer` for request coalescing, worker lifecycle, stale-result filtering, and current
  panel protocol state.
- `KittyLayerManager` for typed image/placement handles and explicit upload/place/delete steps.
- `PlottersBitmap` for `plotters` RGB raster generation.
- Generic image operations such as aspect fitting, chroma-key background transparency, and
  opacity adjustment.
- Terminal helpers for detecting background color and checking whether direct Kitty layers are
  supported for the chosen `ratatui-image` picker.

## API Shape

This crate is intentionally more than thin glue. It owns:

- panel worker lifecycle and latest-wins request behavior
- stale-result filtering via request ids
- Kitty layer id allocation
- terminal capability policy for direct Kitty layers

Demo-specific theme presets and scientific-plot styling do not belong in the library API.
Those live in `examples/`.

## Using It from `../casa-rs`

Keep this crate as a sibling repo and consume it with a local path dependency.
Do not copy sources into `casa-rs`.

In the `casa-rs` workspace root:

```toml
[workspace.dependencies]
ratatui-graphics = { path = "../ratatui-graphics" }
```

In the consuming crate inside `casa-rs`:

```toml
[dependencies]
ratatui-graphics.workspace = true
```

That keeps the dependency editable in place while both repos evolve together.

## Layout

- `src/panel.rs`: stateful panel rendering API
- `src/kitty.rs`: direct Kitty layer management
- `src/bitmap.rs`: checked plotters bitmap helper
- `src/image_ops.rs`: generic image manipulation helpers
- `src/terminal.rs`: terminal capability and background detection helpers
- `examples/`: interactive demo plus smaller focused examples

## Features

- `panel`: `ratatui-image` panel rendering support
- `kitty`: direct Kitty graphics layer support
- `plotters`: plotters bitmap helper support
- `terminal-detect`: terminal background probing via `termbg`

Default features enable all of the above.

## Examples

- `cargo run --example panel_only`
- `cargo run --example kitty_overlay`
- `cargo run --example demo`

The `demo` example is the full interactive Ghostty experiment.

## Verification

The main local verification commands are:

- `cargo check --all-features`
- `cargo test --all-features`
- `cargo check --example panel_only`
- `cargo check --example kitty_overlay`
- `cargo check --example demo`

## Notes

- The direct Kitty layer path is intentionally explicit: callers are responsible for deciding
  when to upload, place, clear, and delete layers.
- The panel path is stateful but higher-level: `PanelRenderer` owns the worker thread and
  latest-wins request semantics so the app does not need raw channel plumbing.
- Ghostty-specific compositing behavior should be validated in a real Ghostty session; a generic
  PTY smoke test is not enough to verify layering.
