# Wave 3 Issue 120 - TW Hydra Image Analysis

Truth class: current descriptive
Last reality check: 2026-04-30
Verification: focused image-analysis commands and tests below

Child issue: #120

This note records the casa-rs mapping for the ALMA First Look / TW Hydra image
analysis tutorial. `imhead` and `imstat` are exposed through `imexplore`;
`immoments`, `exportfits`, and `importfits` are separate task binaries with
matching Python task wrappers.

## Tutorial Mapping

| CASA tutorial operation | casa-rs owner | Wave 3 #120 mapping |
| --- | --- | --- |
| `imhead` | `casa-images`, `casa-coordinates` | `imexplore imhead <image> [--json]` returns image shape, units, masks, beam, and coordinate-axis summaries. |
| `imstat` | `casa-images`, `casa-lattices` | `imexplore imstat <image> [--box ...] [--chans ...] [--json]` returns CASA-style global statistics, mask-aware min/max positions, and Jy/beam flux for tutorial image selections. |
| `immoments` | `casa-images`, `casa-lattices`, `casa-coordinates` | `immoments <image> --outfile <image> --moments 0|1 --chans ... --includepix ...` writes CASA image moment maps with default masks. |
| `exportfits` | `casa-images`, `casa-coordinates` | `exportfits <image> <fits> [--velocity] [--overwrite]` writes a FITS primary HDU through `fitsio`/CFITSIO. |
| `importfits` | `casa-images`, `casa-coordinates` | `importfits <fits> <image> [--overwrite]` reads a FITS primary HDU through `fitsio`/CFITSIO and writes a CASA image. |

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

`exportfits` produces a valid FITS image file for `twhya_cont.image` and
matches the important CASA FITS metadata for this tutorial image: structural
shape, BUNIT, restoring beam (`BMAJ`, `BMIN`, `BPA`), `OBJECT`, direction WCS,
spectral WCS, Stokes axis, `RESTFRQ`, and `SPECSYS`. The structural FITS NAXIS
cards are emitted in FITS axis order; casa-rs does not write duplicate NAXIS
cards.

```sh
target/debug/exportfits \
  /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image \
  target/wdad-wave3-120/rust_twhya_cont.fits \
  --overwrite

file target/wdad-wave3-120/rust_twhya_cont.fits
# FITS image data, 32-bit, floating point, single precision
```

`importfits` round-trip evidence uses a real FITS read/write/read cycle. The
focused unit test creates an asymmetric 4D image so axis-order mistakes are
visible, exports FITS, imports it back to a CASA image, re-exports FITS, and
checks exact FITS pixel equality plus structural shape, BUNIT, restoring beam,
OBJECT, direction WCS, spectral WCS, Stokes axis, `RESTFRQ`, and `SPECSYS`.
The same round-trip was also run on `twhya_cont.image`; all listed keys matched
and FITS pixel max absolute difference was `0.0`. CASA 6.7.5-9 successfully
imported the casa-rs FITS output and reported shape `[250, 250, 1, 1]`, axes
`Right Ascension`, `Declination`, `Frequency`, `Stokes`, and the expected
restoring beam.

In-process timings against CASA 6.7.5-9 on the local TW Hydra tutorial inputs
meet the performance target. Medians below are from seven warm runs after both
processes were already running: CASA was timed as task calls inside the local
CASA Python environment, and casa-rs was timed as direct `casa-images` library
calls via `cargo run --release -p casa-images --example profile_image_analysis`.
This excludes process startup time for both implementations.

| Operation | CASA median s | casa-rs median s | casa-rs/CASA |
| --- | ---: | ---: | ---: |
| `imhead twhya_cont.image` | `0.001977` | `0.000206` | `0.10` |
| `imstat twhya_cont.image box=100,100,150,150` | `0.005176` | `0.000891` | `0.17` |
| `exportfits twhya_cont.image` | `0.003969` | `0.000551` | `0.14` |
| `importfits rust_twhya_cont.fits` | `0.006443` | `0.002547` | `0.40` |
| `immoments moment=0 chans=4~12 includepix=0.03,100` | `0.020663` | `0.006461` | `0.31` |
| `immoments moment=1 chans=4~12 includepix=0.06,100` | `0.019894` | `0.005782` | `0.29` |

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

target/debug/importfits \
  target/wdad-wave3-120/rust_twhya_cont.fits \
  target/wdad-wave3-120/rust_twhya_cont_imported.image \
  --overwrite
```

Focused verification:

```sh
cargo test -p casa-lattices global_stats_respect_non_standard_mask_layout
cargo test -p casa-images analysis
cargo test -p casars resolve_app_defaults_and_rejects_unknown_ids
cargo test -p casars app_metadata_matches_interaction_kind
cd crates/casars-python && uv run --extra test python -m pytest python/tests/test_image_analysis.py -q
cargo run --release -p casa-images --example profile_image_analysis
```
