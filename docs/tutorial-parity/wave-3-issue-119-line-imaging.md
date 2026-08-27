# Wave 3 Issue 119 - TW Hydra Line Imaging And Continuum Subtraction

Truth class: implementation note
Last reality check: 2026-04-28
Verification: focused imaging transform / `casars-imager` tests; local CASA 6.7.5-9 comparisons below

## Scope

This note records the casa-rs mapping for the ALMA First Look / TW Hydra line
imaging segment. It adds UV continuum subtraction and validates spectral cube
image products from the self-calibrated tutorial MeasurementSet.

The tutorial source is:

- key: `alma/first-look/twhya/selfcal-ms`
- source artifact: `twhya_selfcal.ms.tgz`
- staged test path: `target/issue-119/twhya_selfcal.ms`

## CASA Mapping

| CASA guide step | casa-rs path | Status |
| --- | --- | --- |
| `uvcontsub(..., field="5", fitspw="0:0~239;281~383", fitorder=0)` followed by `tclean(...)` | `casars-imager --specmode cube --fitspw '0:0~239;281~383' --fitorder 0 ...` | Implemented as one bounded visibility transform feeding the line-only cube; no intermediate casa-rs continuum-subtracted MS is required. |
| `tclean(..., specmode="cube", nchan=15, start="0.0km/s", width="0.5km/s", outframe="LSRK", restfreq="372.67249GHz")` | `casars-imager --specmode cube --channel-count 15 --start 0.0km/s --width 0.5km/s --outframe LSRK --restfreq 372.67249GHz` | Implemented; natural-weight dirty cube matches CASA at floating-point noise. |
| `restoringbeam="common"` | `--restoringbeam common` | Implemented for restored `.image` beam metadata. |
| `weighting="briggsbwtaper", perchanweightdensity=True` | partially implemented | The option is exposed and follows CASA's `BriggsCubeWeightor` robust/bandwidth-taper formula, but exact TW Hydra weighted-cube parity is still open in the per-channel density source. |

Current CASA 6.7 `uvcontsub(outputvis=...)` keeps the selected field id as `5`;
the older CASA guide text says the output may be relabeled to `0`. The retained
historical residual-visibility evidence below therefore used `field=5`. The
current casa-rs route transforms selected visibility rows inline and preserves
the original selection lineage.

## Commands

```bash
cargo build --release -q -p casars-imager --bin casars-imager
target/release/casars-imager \
  --ms target/issue-119/twhya_selfcal.ms \
  --imagename target/issue-119/casars-natural-twhya-n2hp \
  --field 5 \
  --spw 0 \
  --specmode cube \
  --channel-count 15 \
  --start 0.0km/s \
  --width 0.5km/s \
  --outframe LSRK \
  --restfreq 372.67249GHz \
  --deconvolver hogbom \
  --weighting natural \
  --perchanweightdensity \
  --restoringbeam common \
  --imsize 250 \
  --cell-arcsec 0.08 \
  --phasecenter-field 5 \
  --niter 0 \
  --threshold-jy 0 \
  --datacolumn DATA \
  --fitspw '0:0~239;281~383' \
  --fitorder 0 \
  --no-preview-pngs
```

## Evidence

Historical standalone residual-visibility timing on the staged TW Hydra dataset
(retained as parity evidence, not as a current command surface):

| Engine | Command | Wall time |
| --- | --- | --- |
| CASA 6.7.5-9 | `uvcontsub(..., outputvis=..., field="5", fitspec="0:0~239;281~383", fitorder=0)` | `1.926 s` |
| pre-T40 casa-rs release | former standalone continuum-subtraction route | `5.65 s` wall, report `4.490 s` elapsed |

The former comparison output and CASA continuum-subtracted MS both opened with
CASA tools and contained `44772` rows with field id `[5]`. That evidence pins
the fitting semantics now owned by the inline transform. For valid unflagged
`DATA` cells, CASA vs casa-rs residual visibility differences were:

- RMS absolute difference: `2.6704246849068624e-09`
- max absolute difference: `1.9073486328125e-06`
- relative RMS difference: `2.4508231445204896e-10`

Natural-weight dirty cube comparison, CASA vs casa-rs end to end:

| Product | shape | RMS diff | max abs diff | relative RMS |
| --- | --- | ---: | ---: | ---: |
| `.image` | `[250, 250, 1, 15]` | `2.4392191501798007e-08` | `3.650784492492676e-07` | `7.338334194025458e-07` |
| `.residual` | `[250, 250, 1, 15]` | `2.4392191501798007e-08` | `3.650784492492676e-07` | `7.338334194025458e-07` |
| `.psf` | `[250, 250, 1, 15]` | `7.911510293371164e-09` | `2.384185791015625e-07` | `1.574092510029854e-07` |
| `.sumwt` | `[1, 1, 1, 15]` | `0.0` | `0.0` | `0.0` |

The CASA and casa-rs cubes both report spectral reference `LSRK` and rest
frequency `372672490000 Hz` within floating-point roundoff.

## Weighted-Cube Status

The exact tutorial weighting uses `weighting="briggsbwtaper"` with
`perchanweightdensity=True`. In CASA this routes through
`BriggsCubeWeightor`, which builds a source-channel weight-density cube via
the FTMachine and then applies per-channel `f2/d2` factors; `briggsbwtaper`
also applies a fractional-bandwidth uv-distance factor. casa-rs now exposes
`--weighting briggsbwtaper`, computes CASA's
`2*(maxfreq-minfreq)/(maxfreq+minfreq)` fractional bandwidth, uses CASA's
signed cube-weightor density-cell convention, and routes dirty cubes with
per-plane density through the same cube weighting path as cleaned cubes.

Fresh CASA 6.7.5-9 reference command:

```python
tclean(
    vis="target/issue-119/twhya_selfcal.ms.contsub",
    imagename="target/issue-119/casa-briggsbwtaper-refresh-twhya-n2hp",
    field="5",
    spw="0",
    specmode="cube",
    nchan=15,
    start="0.0km/s",
    width="0.5km/s",
    outframe="LSRK",
    restfreq="372.67249GHz",
    gridder="standard",
    deconvolver="hogbom",
    weighting="briggsbwtaper",
    perchanweightdensity=True,
    restoringbeam="common",
    imsize=250,
    cell="0.08arcsec",
    phasecenter=5,
    niter=0,
    threshold="0Jy",
    datacolumn="data",
    interactive=False,
)
```

casa-rs command:

```bash
target/release/casars-imager \
  --ms target/issue-119/twhya_selfcal.ms.contsub \
  --imagename target/issue-119/casars-briggsbwtaper-centered-twhya-n2hp-rerun2 \
  --field 5 \
  --spw 0 \
  --specmode cube \
  --channel-count 15 \
  --start 0.0km/s \
  --width 0.5km/s \
  --outframe LSRK \
  --restfreq 372.67249GHz \
  --deconvolver hogbom \
  --weighting briggsbwtaper \
  --perchanweightdensity \
  --restoringbeam common \
  --imsize 250 \
  --cell-arcsec 0.08 \
  --phasecenter-field 5 \
  --niter 0 \
  --threshold-jy 0 \
  --datacolumn DATA \
  --no-preview-pngs
```

Current weighted-cube comparison:

| Product | shape | RMS diff | max abs diff | relative RMS |
| --- | --- | ---: | ---: | ---: |
| `.image` | `[250, 250, 1, 15]` | `0.00010815705738803386` | `0.0005521811544895172` | `0.003737659372503618` |
| `.residual` | `[250, 250, 1, 15]` | `0.00010815705738803386` | `0.0005521811544895172` | `0.003737659372503618` |
| `.psf` | `[250, 250, 1, 15]` | `0.000056537305188819865` | `0.00027957186102867126` | `0.0015209825039802264` |
| `.sumwt` | `[1, 1, 1, 15]` | `0.00011457723208023715` | `0.0002052783966064453` | `0.00004646506431534153` |

Fresh local timings on the no-pointing CASA build were `2.76s` for CASA
6.7.5-9 and `1.69s` warm / `2.64s` cold-output for
`target/release/casars-imager` on the same 15-channel weighted cube.

Direct CASA instrumentation showed that CASA's weighted cube path builds a
separate `GridFT` spectral path for weighting: a 15-plane image uses a
17-channel intermediate Briggs density grid and 19 selected visibility
channels before mapping back to the output planes. Matching this required
using the cube path even for `niter=0`, summing the parallel-hand density
weights the same way CASA's `VisImagingWeight::unPolChanWeight` does, and
using the centered selected source channel for each output-plane density
estimate while leaving the visibility interpolation path unchanged.
