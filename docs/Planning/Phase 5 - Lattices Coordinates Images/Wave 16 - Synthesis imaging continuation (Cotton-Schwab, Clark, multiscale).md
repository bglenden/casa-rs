# Wave 16 - Synthesis Imaging Checkpoint

## Scope

Checkpoint status for the imaging work that began as the Wave 16
Cotton-Schwab / Clark / multiscale plan. This file now records what is
implemented, what has source-backed CASA parity, and what remains open.

## Implemented

- Pure imaging engine in `crates/casa-imaging` for dirty imaging and CLEAN.
- CASA-style major/minor-cycle controller with shared diagnostics, stop reasons,
  per-plane/channel traces, and stage timings.
- MFS deconvolution modes:
  - `hogbom`
  - `clark`
  - `multiscale`
- Spectral-cube imaging with real spectral-axis products.
- Cube CLEAN for:
  - `hogbom`
  - `clark`
  - `multiscale`
- Weighting and beam machinery:
  - natural, uniform, and Briggs-style density weighting
  - shared vs per-plane density weighting for cubes
  - UV taper
  - per-plane and common restoring beams
- CASA-compatible image products through `casa-images`:
  - `.psf`
  - `.residual`
  - `.model`
  - `.image`
  - `.sumwt`
- Benchmark/profiling tooling:
  - `crates/casars-imager/examples/profile_imager.rs`
  - `scripts/bench-imager-vs-casa.sh`
  - `tools/perf/imager/casa_phase_bench.py`

## Verified Against CASA

### MFS

- Hogbom parity remains the base compatibility target.
- Clark parity is established on compact-source cases including:
  - `sim_data_VLA_jet.ms`
  - `ngc5921.ms`
- Multiscale parity is established on:
  - `sim_data_VLA_jet.ms`
  - `n2403.short.ms`
  - one ALMA extended-source case

### Cube

- Dirty-cube parity is established on:
  - `sim_data_VLA_jet.ms`
  - `refim_Cband.G37line.ms`
  - selected source-backed `refim_point.ms` `test_cube_*` cases
  - the full `refim_point_descendingfreqs.ms` channel-order suite
- Source-backed cleaned-cube oracles are green on `refim_eptwochan.ms` for:
  - Hogbom
  - Clark
  - Multiscale
- Cube iteration-control parity is green for the authoritative CASA cases:
  - `test_iterbot_cube_1`
  - `test_iterbot_cube_2`
  - `test_iterbot_cube_3`
  - `test_iterbot_cube_tol`
- Source-backed restoring-beam/common-beam cube oracles on
  `refim_point.ms` and `refim_point_withline.ms` are green.

### Core dirty-image parity

- For the current `refim_point_withline.ms` cube investigation, the Rust
  imaging core matches a casacore whole-image dirty-plane reconstruction very
  closely once given the same prepared samples.
- That means the remaining open `nsigma` issue is not in the low-level gridder
  or FFT/correction path.

## Known Open Gaps

### 1. Cube Hogbom `nsigma` stopping

- The source-backed slow parity case
  `hogbom_cube_nsigma_stopping_tracks_casa_on_refim_point_withline`
  is still open.
- Current observed result:
  - Rust `iterdone = 409`
  - CASA `iterdone = 407`
  - `nmajordone = 11` matches
- Current evidence points to a tiny upstream dirty-plane or prepared-sample
  parity gap near the stopping floor, not a major controller bug.
- This oracle is intentionally left out of the default green slow suite until
  the underlying dirty/sample-prep mismatch is understood.

### 2. Full CASA cube semantics

- Full `specmode='cube'` parity is not complete.
- Remaining work includes:
  - more source-backed `test_cube_*` coverage
  - broader non-default interpolation/frame combinations
  - finishing the move of remaining scalar spectral helper logic behind
    measures-backed library APIs in `casa-ms`

### 3. Performance follow-up

- Major clean-path performance work has been done, but this checkpoint does not
  claim final performance parity or optimization completeness.
- Benchmark tooling is present; further optimization should be guided by the
  staged Rust/CASA phase breakdowns already added.

## Explicitly Deferred

- `wproject`
- A/W-projection
- primary-beam correction / PB-aware restoration
- mosaic gridder behavior
- `mtmfs`
- GPU/distributed execution

## Suggested Next Work

1. Close the cube Hogbom `nsigma` parity gap by comparing CASA-prepared
   interpolated samples against the Rust `prepare_plane_input()` tuples for the
   `refim_point_withline.ms` case.
2. Expand authoritative cube `test_cube_*` coverage now that the beam and
   multiscale cube oracles are green.
3. Move the remaining scalar spectral helper formulas in `casa-ms` behind
   measures-backed reusable APIs.
4. Only after that, move on to wider-field work such as `wproject`.
