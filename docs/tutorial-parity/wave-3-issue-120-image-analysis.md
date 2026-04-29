# Wave 3 Issue 120 - TW Hydra Image Analysis

Truth class: current descriptive
Last reality check: 2026-04-29
Verification: focused image-analysis commands and tests below

Child issue: #120

This note records the casa-rs mapping for the ALMA First Look / TW Hydra image
analysis tutorial. `imhead` and `imstat` are exposed through `imexplore`;
`immoments` and `exportfits` are separate task binaries with matching Python
task wrappers.

## Tutorial Mapping

| CASA tutorial operation | casa-rs owner | Wave 3 #120 mapping |
| --- | --- | --- |
| `imhead` | `casa-images`, `casa-coordinates` | `imexplore imhead <image> [--json]` returns image shape, units, masks, beam, and coordinate-axis summaries. |
| `imstat` | `casa-images`, `casa-lattices` | `imexplore imstat <image> [--box ...] [--chans ...] [--json]` returns CASA-style global statistics, mask-aware min/max positions, and Jy/beam flux for tutorial image selections. |
| `immoments` | `casa-images`, `casa-lattices`, `casa-coordinates` | `immoments <image> --outfile <image> --moments 0|1 --chans ... --includepix ...` writes CASA image moment maps with default masks. |
| `exportfits` | `casa-images`, `casa-coordinates` | `exportfits <image> <fits> [--velocity] [--overwrite]` writes a FITS primary HDU through `fitsio`/CFITSIO. |

## Evidence

Tutorial inputs:

- `/Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image`
- `/Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_n2hp.image`

The continuum `imstat` tutorial box matches CASA to displayed precision:

| Statistic | CASA | casa-rs |
| --- | ---: | ---: |
| `npts` | `2601` | `2601` |
| `min` | `-0.015594057738780975` | `-0.015594057738780975` |
| `max` | `0.31544822454452515` | `0.31544822454452515` |
| `sum` | `43.682481561353285` | `43.682481561353285` |
| `mean` | `0.016794495025510683` | `0.016794495025510683` |
| `rms` | `0.05042437731579099` | `0.05042437731579099` |
| `sigma` | `0.04755451831046641` | `0.04755451831046634` |
| `median` | `0.001873743487522006` | `0.001873743487522006` |
| `flux` | `1.6551433135292184` | `1.6551433135292184` |

The tutorial moment maps use `twhya_n2hp.image`, `chans=4~12`, and the CASA
tutorial `includepix` thresholds. The saved casa-rs images preserve CASA-like
shape `[250, 250, 1, 1]`, units, and default masks.

| Moment output | Statistic | CASA | casa-rs |
| --- | --- | ---: | ---: |
| `mom0` | `npts` | `41833` | `41833` |
| `mom0` | `sum` | `1818.937259840779` | `1818.922259840183` |
| `mom0` | `mean` | `0.04348091840988643` | `0.04348055984127801` |
| `mom0` | `rms` | `0.05210225894287384` | `0.05210160337265946` |
| `mom0` | `median` | `0.03653864189982414` | `0.03653864189982414` |
| `mom1` | `npts` | `8302` | `8302` |
| `mom1` | `sum` | `29977.270833969116` | `29977.270833969116` |
| `mom1` | `mean` | `3.6108492934195513` | `3.6108492934195513` |
| `mom1` | `rms` | `3.806944397936337` | `3.806944397936337` |
| `mom1` | `median` | `3.2443543672561646` | `3.2443543672561646` |

The `mom0` integrated sum differs by `0.015000000596`, about `8.2e-6`
relative, while the mask cardinality, extrema, and median match.

`exportfits` produced a valid FITS image file for `twhya_cont.image`:

```sh
target/debug/exportfits \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image \
  target/wdad-wave3-120/rust_twhya_cont.fits \
  --overwrite

file target/wdad-wave3-120/rust_twhya_cont.fits
# FITS image data, 32-bit, floating point, single precision
```

## Commands

```sh
target/debug/imexplore imhead \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image \
  --json

target/debug/imexplore imstat \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image \
  --box 100,100,150,150 \
  --json

target/debug/immoments \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_n2hp.image \
  --outfile target/wdad-wave3-120/rust_n2hp.mom0 \
  --moments 0 \
  --chans 4~12 \
  --includepix 0.03,100 \
  --overwrite

target/debug/immoments \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_n2hp.image \
  --outfile target/wdad-wave3-120/rust_n2hp.mom1 \
  --moments 1 \
  --chans 4~12 \
  --includepix 0.06,100 \
  --overwrite

target/debug/exportfits \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image \
  target/wdad-wave3-120/rust_twhya_cont.fits \
  --overwrite
```

Focused verification:

```sh
cargo test -p casa-lattices global_stats_respect_non_standard_mask_layout
cargo test -p casa-images analysis
cargo test -p casars resolve_app_defaults_and_rejects_unknown_ids
cargo test -p casars app_metadata_matches_interaction_kind
cd crates/casars-python && uv run --extra test python -m pytest python/tests/test_image_analysis.py -q
```
