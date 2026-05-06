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
| Raw visibility scatter: amplitude, phase, real, imaginary, weight, sigma, flags, geometry axes vs time/uv/channel/frequency/velocity | Partial | Basic numeric scatter, grouped series, point-cloud retention, viewport rasterization, channel-bin footprints, secondary axes, and multi-panel pages are present. Adapter coverage from real `MsScatterPlotPayload` is still missing. |
| Large true scatter clouds | Covered for prototype | The continuous 2M-point sample exercises single-pixel rasterization without channel combing. Density remains a separate strategy option. |
| Channelized visibility clouds | Covered for prototype | The channelized 2M-point sample uses explicit channel-bin footprints so channels are not treated as zero-width points. |
| UV coverage | Partial | Mirrored uv scatter is represented. Joined tracks vs point-only rendering and exact `draw_points` behavior need adapter coverage. |
| Metadata interval/bar surfaces | Covered for prototype | Scan timelines and spectral-window coverage now have interval layers on categorical lanes. |
| Antenna layout | Covered for prototype | Per-point labels and marker sizes are modeled and rendered as point metadata. |
| ScatterGrid / ScatterPage / stacked plot pages | Covered for prototype | The stacked visibility sample exercises panel-local axes and a trailing secondary y-axis. Exact Rust payload adapters are still pending. |
| 1D profile / spectrum / imaging channel series | Covered for prototype | Line layers now support masked gaps, selected-sample markers, and overlay profile styling. |
| Image plane raster | Partial | Raster display, stretch, colormap controls, cursor/probe annotations, region overlays, and profile-cut overlays exist in the sample. Aspect-ratio constraints, histogram/colorbar metadata, mask/non-finite treatment, and exact image-browser overlay semantics are incomplete. |
| Artifact preview image | Partial | Can be represented as a raster/image layer, but file-backed preview lifecycle is not modeled in the plot widget yet. |

## Current Gaps

These remain after the prototype model/rendering pass:

- Exact adapter tests that transform each Rust payload shape into a Swift plot
  document without falling back to PNG buffers.
- Exact `draw_points` and joined-track behavior for UV coverage.
- Image-plane aspect-ratio constraints, histogram/colorbar metadata,
  mask/non-finite treatment, and exact image-browser overlay semantics.
- Region mask fill/selection semantics beyond stroked polygon overlays.
- Hit-testing/provenance for rasterized point clouds so selected points can be
  traced back to row/correlation/channel spans.

## Signoff Bar

Before #207 should be treated as plot-widget-ready, the sample surface should
include at least:

- one msexplore-style large scatter sample,
- one channelized visibility sample,
- one metadata interval/bar sample,
- one antenna-layout sample with labels and marker sizes,
- one multi-panel or stacked sample with a secondary axis,
- one image-plane sample with overlays,
- one profile/spectrum sample with masked gaps and selected marker,
- model tests for each source payload family listed above.
