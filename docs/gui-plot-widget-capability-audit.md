# GUI Plot Widget Capability Audit

Truth class: implementation audit
Last reality check: 2026-05-05
Verification: `swift test` from `apps/casars-mac`; `just quick`

## Scope

This audit maps the standalone Swift workbench plot widget prototype against the
plot surfaces already produced by `msexplore`, `imexplore`, and the current
imaging plot preview path. It is not a signoff document; it records what #207
must still cover before the widget is considered a sensible replacement for the
existing rendered-PNG plot surfaces.

## Source Surfaces

- `msexplore` ships 26 presets through `MsPlotPreset::ALL`.
- `msexplore` render payloads are one of:
  `ListObs`, `Scatter`, `ScatterGrid`, or `ScatterPage`.
- The reused `listobs` plot payloads include UV coverage, raw visibility
  scatter, antenna layout, scan timeline bars, and spectral-window coverage
  bars.
- `imexplore` has two plot-like surfaces:
  a 2D image plane raster with cursor/probe/region overlays, and a linked 1D
  image profile/spectrum with optional overlay profiles.
- The TUI imaging workflow also renders artifact previews and channel-series
  line diagnostics.

## Current Swift Coverage

| Existing source surface | Current Swift status | Notes |
| --- | --- | --- |
| Raw visibility scatter: amplitude, phase, real, imaginary, weight, sigma, flags, geometry axes vs time/uv/channel/frequency/velocity | Partial | Basic numeric scatter, grouped series, point-cloud retention, viewport rasterization, and channel-bin footprints are present. Adapter coverage from real `MsScatterPlotPayload` is still missing. |
| Large true scatter clouds | Covered for prototype | The continuous 2M-point sample exercises single-pixel rasterization without channel combing. Density remains a separate strategy option. |
| Channelized visibility clouds | Covered for prototype | The channelized 2M-point sample uses explicit channel-bin footprints so channels are not treated as zero-width points. |
| UV coverage | Partial | Mirrored uv scatter is represented. Joined tracks vs point-only rendering and exact `draw_points` behavior need adapter coverage. |
| 1D profile / spectrum / imaging channel series | Partial | Line layers exist, but masked gaps, selected-sample markers, and overlay profile styling need first-class model support. |
| Image plane raster | Partial | Raster display, stretch, and colormap controls exist in the sample. Aspect-ratio constraints, histogram/colorbar metadata, mask/non-finite treatment, and image-browser overlay semantics are incomplete. |
| Artifact preview image | Partial | Can be represented as a raster/image layer, but file-backed preview lifecycle is not modeled in the plot widget yet. |

## Current Gaps

These are required for sensible recreation of all existing source plot types:

- Interval/bar layers for scan timelines and spectral-window coverage.
- Categorical or lane axes for timeline, spectral-window, and antenna-layout
  style plots.
- Per-point marker labels and marker sizes for antenna layouts.
- Multi-panel and faceted layout support for `ScatterGrid`,
  `ScatterPage`, and the stacked amplitude/phase page.
- Secondary y-axis support for non-iterated dual-axis scatter plots.
- Masked line gaps and selected-sample markers for `imexplore` profiles.
- Image-plane overlay primitives for cursor probes, pinned probes, polygonal
  regions, and region masks.
- Adapter tests that transform each Rust payload shape into a Swift plot
  document without falling back to PNG buffers.
- Hit-testing/provenance for rasterized point clouds so selected points can be
  traced back to row/correlation/channel spans.

## Signoff Bar

Before #207 should be treated as plot-widget-ready, the sample surface should
include at least:

- one msexplore-style large scatter sample,
- one channelized visibility sample,
- one metadata interval/bar sample,
- one multi-panel or stacked sample,
- one image-plane sample with overlays,
- one profile/spectrum sample with masked gaps and selected marker,
- model tests for each source payload family listed above.
