# Simulation programme: CASA's `sm`, `simobserve`, `simanalyze` and `simalma` in Rust

Truth class: normative plan
Date: 2026-10-20
Work issue: #841
Status: accepted by the owner on 2026-10-20, after a Fable xhigh review.

## 1. Goal and requirements

Make CASA simulation scripts run on casa-rs with only their imports changed,
and run them dramatically faster:

- the `simulator` tool (`sm`), with every user-facing method under CASA's
  name, signature and effective default;
- the `simobserve`, `simanalyze` and `simalma` tasks, with CASA's parameters
  and defaults.

Requirements:

- **ADR-0018 applies.** All computation is in Rust, including the parts CASA
  writes in Python (`task_simobserve.py`, `simutil.py`). Python only presents
  CASA's interface.
- **ADR-0017 applies.** Tasks are one-shot surfaces, and tools are in-process
  objects.
- **Agreement with CASA follows AGENTS.md.**
  - The MS has the same columns, subtables and keywords as CASA's, apart from
    decision 6. Data managers may differ; see the storage layout below.
  - Predicted visibilities agree to about 1e-3 normalized RMS.
  - Noise and corruptions are statistically equivalent, not bit-identical.
  - Every divergence is justified and recorded in `docs/CASA (C++) bugs.md`.
- **casacore C++ can read the MS.** An interop test proves it.
- **Performance is the point** (owner, 2026-10-20).
  - Prediction uses Metal where it pays.
  - Geometry, prediction, corruption and writing use multiple workers.
  - Results do not depend on the worker count or the backend.
  - Every benchmark reports wall time, single-worker (W1) user CPU and peak
    RSS, so parallelism cannot mask serial cost.
  - The targets for each mode are in §5.
- **Memory is bounded** (owner, 2026-10-20).
  - Peak resident memory stays under a declared budget, whatever the number
    of rows, channels or fields. A counting-law test proves it, as in imaging.
  - Work is streamed in time and channel chunks sized from the budget, and
    Metal device buffers count against the same budget.
  - The MS write session stays bounded (ADR-0008).
- **The MS I/O routines are open to optimization** (owner, 2026-10-20).
  - Once prediction runs on Metal, writing the MS is likely the bottleneck,
    so the `casa-ms` and `casa-tables` write path may be reworked.
  - **What is on disk does not change:** the same storage managers, tile
    formats and table metadata, guarded by the casacore C++ interop tests.
  - **What can change is how the bytes get there:** larger tile-aligned batch
    writes, fewer copies and system calls, write-behind that overlaps
    prediction with I/O, and buffers sized from the memory budget.
  - Every casa-rs MS writer gains: `split`, `mstransform`, `importvla`,
    `flagdata`, and imaging's MODEL_DATA writes.
- **The storage layout is conventional** (owner, 2026-10-20).
  - The simulator uses the layout casa-rs's other MS writers share
    (`casa-ms/src/ms.rs:1183-1234`), and writes every row:
    - IncrementalStMan for the slowly varying scalar columns;
    - StandardStMan for ANTENNA1/2, DATA_DESC_ID and FLAG_ROW;
    - TiledShapeStMan, with CASA's standard tile shapes, for the data, flag
      and weight columns;
    - TiledColumnStMan for UVW.
  - It does not copy `NewMSSimulator`, which binds every other column to
    IncrementalStMan (UVW included) and writes the scalar columns only at the
    first row of each integration.

## 2. Starting point

### CASA

- **`sm` is C++** (`Simulator.cc`), and casacore's `NewMSSimulator` writes the
  MS.
- **`predict` builds a SkyEquation.**
  - Its FTMachine is `GridFT` by default, with padding 1.3 from `setoptions`.
    The alternatives are `mosaic`, `wproject`, `sd` and `both`.
  - The primary beam is a separate `VPSkyJones`. It is attached only after
    `setvp(dovp=True)`; `doVP_p` is false at construction
    (`Simulator.cc:327`, `:2549`). `simobserve` always calls
    `setvp(dovp=True, usedefaultvp=False)` for interferometric runs
    (`task_simobserve.py:1045`).
  - Default voltage patterns: `PBMath1DEVLA` for EVLA, a polynomial for the old
    VLA, and Airy only for ALMA and ACA (`PBMath.cc`).
- **`corrupt` runs `VisEquation::collapseForSim`.** Its terms:
  - noise: `ANoise`, scaled by a `T` or `Tf` term for Tsys noise;
  - gain: `G`, in fBM or random mode;
  - troposphere: a `Tf` phase screen;
  - leakage: `Dlin`;
  - any calibration table, through `setapply`.

  `setbandpass`, `setpa` and `setpointingerror` are disabled in CASA itself and
  throw "temporarily disabled".
- **Defaults come from the installed binding, not the XML.**
  - Parameters of "any" type default to `[]`, which the C++ reads as 0 before
    applying its own fallbacks: `setnoise()` gives simplenoise 0 Jy, and
    `settimes()` gives a 0 s integration.
  - If `setlimits` is never called, the limits are 8° elevation and 1e-6
    shadowing. If `setauto` is never called, autocorrelations are written
    with weight 1.
  - The seed is 11111 until `setseed` is called. `setseed()` with no argument
    sets 185349251.
  - Task defaults are the Python signature defaults; casashell's differ.
- **TIME is TAI labelled UTC.** Verified 2026-10-20 with CASA 6.7: data start
  37 s after the requested epoch. UVW is computed with that stored number read
  as UT1, so the data are self-consistent; only the schedule is shifted.
- **`simobserve`, `simanalyze`, `simalma` and `simutil` are Python.** They
  cover:
  - sky-model rescaling;
  - antenna `.cfg` reading in XYZ, LOC and UTM coordinates;
  - pointing generation: hexagonal, square, and ALMA OT tiling;
  - scheduling;
  - `noisetemp` efficiencies;
  - imaging and the fidelity analysis.

  `simanalyze` itself calls `concat` and `tsdimaging`
  (`task_simanalyze.py:7`, `:231`).
- **ATM.** `tsys-atm` noise, which is `simobserve`'s default, and `settrop`
  need ATM: about 27,000 lines of C++ in 53 files under
  `casatools/src/code/atmosphere`.

### casa-rs

**Existing simulator.** `casa-ms/src/simulation.rs` (6,660 lines) and
`simulation_task.rs` (2,669 lines) are both on the file-length allowlist.
- It generates in one shot: one request writes one new MS.
- It handles several spectral windows, mosaics, analytic and FITS models, and
  total power.
- Its corruptions are simple noise, gain, bandpass, leakage and a global
  pointing offset.
- MS writes are streamed and bounded.
- Prediction uses its own serial spheroidal degridder
  (`casa-simulation-synthesis`), with no w-term and one beam for the whole
  array.

**Evidence on record.**
- Same-input CASA comparisons from July (#257) are 3.9× (VLA) and 4.5×
  (ALMA) for image models.
- The July analytic-model family runs wrote 1,253 MB/s end to end on the
  internal disk (1 GiB; it probably fits in the page cache), or 1,243 MB/s
  with prediction disabled. They reached 0.77 GB/s on a 100 GiB run to an
  external drive. CASA's small ACA and simalma runs wrote 4.1–4.6 MB/s, but
  that is not a same-input comparison.

**Gaps.**
- a stateful `sm`-style API, and appending rows to an MS: the write plan fixes
  the row count up front (`write_session.rs:335`, `:1559`);
- corrupting an existing MS;
- CASA images and CASA component lists as input (nothing in casa-rs reads
  component lists);
- `tsys-*` noise, `settrop` and `setapply`;
- pointing generation, `simutil`, `simanalyze` and `simalma`;
- CASA parameter names;
- a casacore C++ interop test;
- bounded prediction memory;
- two defects (#840);
- packaging: `simobserve` is missing from the installed suite.

**Prediction in the imaging operator.**
- It provides row-level degridding (`SpectralResampler::predict_row` over
  prepared model grids, with a `ModelPrescale` hook for image-plane weighting)
  and four kernel sets: standard, W, AW and mosaic.
- Metal grids only the standard and mosaic sets; W and AW wait on #653
  (`availability.rs:32-34`).
- `GridGeometry` hard-codes CASA's tclean padding of 1.2 (`geometry.rs:29`).
- ADR-0016 fixes the kernel sets at exactly four.
- No entry point predicts from an external model.
- Worker teams, Metal backends and the memory budget (`HostResources`,
  `ResourcePolicy`, `admit`/`Reservation`, `WorkerTeam`) live in
  `casa-imaging-runtime`.

**Python.** `casars._core` (pyo3) provides `Image` and `Table`. Both are
`unsendable` and hold the GIL (#839).

## 3. Owner decisions (accepted 2026-10-20)

1. **Namespace.**
   - CASA tool classes go in `casars.tools` under casatools' names, so
     `from casars.tools import simulator; sm = simulator()`.
   - Tasks stay in `casars.tasks`.
   - There are no `casatools` or `casatasks` alias packages.
2. **Replace the `simobserve` task surface.**
   - The new surface has CASA's parameters and defaults, at contract 5.
   - Profiles that use today's casa-rs-only parameters fail with a clear
     diagnostic.
   - The family generator, used for benchmark and fixture datasets, becomes a
     developer binary outside the catalog, `synthetic-ms`.
   - *Consequence found in review:* `synthetic-ms` needs predicted
     visibilities, so it must live in `casa-simulation`, not `casa-ms`.
     The imaging T1 fixtures and the perf fixtures depend on it through a
     dev-dependency, which the layer check allows.
3. **New crate `casa-simulation` at the application layer.**
   - It depends on `casa-ms`, `casa-images`, `casa-imaging-operator` and
     `casa-imaging-runtime` (worker teams, Metal, the budget), and later
     `casa-calibration` and `casa-atmosphere`.
   - `casa-ms` keeps the MS primitives.
   - `simulation.rs`, `simulation_task.rs` and `casa-simulation-synthesis`
     are deleted at the end.
   - The crate is added to ADR-0016's dependency policy. The edges it needs
     (application → observation, reconstruction and execution) are already
     allowed.
4. **Port ATM to Rust** as `casa-atmosphere`, in its own phase. Until it lands,
   `tsys-atm` and `settrop` raise a clear "not yet implemented" error.
5. **TIME honours the requested UTC.**
   - The divergence is recorded.
   - CASA computed its geometry at the requested time + (TAI − UTC). A parity
     run therefore requests a start 37 s later, and compares rows at equal
     stored TIME.
6. **DATA only, unless a later step needs MODEL_DATA.**
   - MODEL_DATA is created when something needs it, such as
     `predict(incremental=True)`.
   - CORRECTED_DATA is not written.
   - casa-rs's `imager` falls back from `datacolumn='corrected'` to DATA, as
     CASA's `tclean` does.

## 4. Design decided in planning

These choices are open to objection.

### Tool binding
- `simulator` is a pyo3 class in `casars._core`.
- Its method signatures and defaults are declared in Rust (pyo3 `signature`),
  copying the installed CASA binding's effective defaults. The object-surface
  bundle describes the methods from the same Rust types.
- Session state is `Send`, so long calls release the GIL; #839 brings `Image`
  and `Table` to the same pattern.
- The memory budget and the backend are constructor arguments, such as
  `simulator(memory_budget=..., backend='auto')`. No environment variables are
  read (ADR-0016).
- Every method in `simulator.xml` exists:
  - `setbandpass`, `setpa` and `setpointingerror` raise CASA's "temporarily
    disabled" error;
  - `oldsetnoise` raises a deprecation error;
  - `name`, `type` and `summary` are implemented.

### Deferred, single-pass execution
- CASA's `sm` writes DATA three times: zeros at `observe`, again at `predict`
  and again at `corrupt`.
- The casa-rs session records `observe`, `predict` and `corrupt` as a plan.
  At `close`/`done` it materializes the MS in one streamed pass: geometry,
  prediction, corruption and the write for each chunk.
- The MS is published at `close`, as ADR-0008 staging requires, so no reader
  can see an intermediate state.
- **A call that needs data on disk** forces the pending pass. These are a
  second `predict` or `corrupt` after one has already run, `openfromms` on
  the output, and `setdata` selections that depend on written rows.
- **Corrupting an existing MS** (`openfromms` + `corrupt`) patches it in place
  through the bounded selected-row mutation path (`write_session.rs:541`). The
  chunks are aligned to the DATA tile's channel extent, so each tile is
  written once.
- `simobserve`'s noise-free and noisy MSs are written in the same pass, rather
  than CASA's copy-then-`corrupt`.

### Prediction
- Prediction goes through the imaging operator:
  - a new application-layer entry point prepares model grids from an
    external sky model (cell, frequency axis, Stokes);
  - the padding factor becomes a parameter of the operator's geometry, set
    from `setoptions` with default 1.3;
  - the primary beam is applied through `ModelPrescale` only after
    `setvp(dovp=True)`, using CASA's voltage pattern for the telescope;
  - the EVLA model is shared from `casa-imaging-products` rather than copied.
- Execution uses `casa-imaging-runtime`'s worker teams and Metal backends.
  The budget is `HostResources`/`ResourcePolicy` with `admit`/`Reservation`.
  The simulator builds no second budget or worker model.
- **Backends.** `auto` chooses Metal where it is available and pays: standard
  and mosaic kernels. W-projection is CPU-only until Metal W lands (#653).
- **Total power (`sd`)** is image-plane sampling of the model convolved with
  the primary beam, inside `casa-simulation`. ADR-0016 keeps the operator to
  four kernel sets.
- `casa-simulation-synthesis` and the old predictor are deleted once nothing
  uses them.

### Random numbers
- A counter-based generator keys each draw by seed, row, channel, correlation
  and real/imaginary part. Results are therefore bitwise identical for any
  worker count, chunking or backend, and need no shared state.
- Its statistical quality is tested, including independence across rows,
  channels and correlations.
- It is not CASA's MLCG stream; AGENTS.md says bitwise reproducibility is not
  a goal.

### CASA quirks
- **Matched**, where they shape outputs users compare:
  - default limits;
  - SIGMA and WEIGHT left at their geometric values after `corrupt`;
  - noise scaling: independent real and imaginary draws for
    cross-correlations, one real draw for autocorrelations, and an
    independent draw for every correlation, the cross-hands of
    4-correlation data included (measured on CASA 6.7.6 in Phase 1d; this
    plan first said the cross-hands get none, #841);
  - fBM normalisation;
  - scan numbering;
  - autocorrelation defaults.
- **Diverged and recorded**, only for crashes, undefined behaviour or data
  corruption:
  - 2-D fBM out-of-bounds writes;
  - UVW throwing for non-J2000 fields;
  - the integer-division bug in `setmosaicfield`.

  These three are checked against CASA when their phase starts.
- Other quirks are decided phase by phase and recorded.

### Code shape
- The crate's module split is planned before any code, so every file stays
  under 1,500 lines.
- Plotting in `simobserve` and `simanalyze` (their `graphics` option) is
  rendered in Python (ADR-0018 item 4).
- Every new binary and surface is in the installed suite.

### The tool pattern is ADR-0019
- ADR-0019 (accepted 2026-10-20) records, for every tool:
  - decision 1's `casars.tools` namespace;
  - the binding pattern above: Rust-declared signatures and defaults;
  - budget and backend as constructor arguments;
  - the GIL rule.
- Decision 6 stays in this plan, under ADR-0008's existing rule that casa-rs
  writes nothing CASA does not.

## 5. Performance and I/O targets

### Per-mode targets
Each benchmark is timed for CASA, today's simulator, and casa-rs on CPU and on
Metal. CASA's time is measured in-process, per `sm` call, excluding Python
start-up.

| Mode | Target |
|---|---|
| Analytic and component-list prediction | No slower than July's record on the same case. The CASA multiple is measured on the same input, on a case where CASA runs ≥ 60 s. |
| Image-model prediction, CPU | ≥ 10× CASA on the same input (July: 3.9–4.5×). W1 user CPU is reported. |
| Image-model prediction, Metal | I/O-bound: meets the end-to-end I/O fraction below. The CASA multiple is reported. |
| I/O-bound passes (geometry only, noise only, write only) | Judged against the device's ceiling, not against CASA. |

**No-regression rule.** On every case, the new simulator is no slower than
today's, within 5%. Any trade-off is recorded and needs owner approval.

### I/O target
- **The measure** is a sustained MAIN-table rate: bytes on disk divided by the
  wall time from the first row to publication, including the final sync. It
  is measured on a dataset at least twice the size of RAM, with the page cache
  purged first.
- **The comparison** is a fraction of the same volume's raw sequential-write
  ceiling, measured in the same session with a file larger than RAM:
  - at least 70% for the write-only case;
  - at least 50% end to end with Metal prediction.
- **Where it runs.** Sustained runs go on GLENDENNING, the 1 TB NVMe external
  drive (about 3 GB/s). The internal disk has 72 GB free and must stay above
  40 GB.
- **Fast regression check.** The 1 GiB internal-disk case stays as a quick
  check, labelled cache-resident. A 4 GB `dd` on the internal disk measured
  about 2 GB/s on 2026-10-20, but that file is smaller than RAM and so went
  partly through the page cache.

### Benchmark cases
1. **ALMA:** about 43 antennas, 1 h of 10 s integrations, 256 channels and a
   1024² image model.
2. **VLA**, of similar size.
3. **MFS model with many channels:** one model plane degridded at 256
   frequencies, the common `simobserve` shape.
4. **Analytic and component-list:** carries July's record.
5. **Write-only.**

## 6. Phases

The phases are ordered by risk. Each PR is reviewed independently when it
touches science, persistence or interop.

### Phase 1, the first example: an `sm` script runs unchanged

This is delivered as four PRs, in order.

**1a. Prediction spike (the gating risk).**
- Prove that the imaging operator reproduces CASA's `sm.predict`:
  - with `GridFT` at padding 1.3;
  - with and without `setvp`;
  - on a CASA-made MS, compared sample by sample with the geometry held
    fixed.
- If it misses 1e-3 normalized RMS and the cause cannot be fixed in the
  operator, stop and report before going further.

**1b. MS write path.**
- Profile the write session on the benchmark shapes.
- Improve buffering, batch layout and overlap.
- Persistence review.
- Time `split` and `mstransform` before and after.
- Run the I/O targets of §5.

**1c. Session and observe.**
- The `casa-simulation` crate, with its module plan.
- The `Simulator` session with `open`, `setconfig`, `setfeed`,
  `setspwindow`, `setfield`, `settimes`, `setlimits`, `setauto`, `observe`
  and `close`/`done`.
- `simutil.readantenna`.
- Writing that appends rows within staging.
- The casacore C++ interop test.
- `casars.tools.simulator` and its object-surface bundle.

**1d. Predict and corrupt.**
- `predict` from a CASA image, `setvp`, `setoptions` (padding),
  `setseed`, `setnoise(mode='simplenoise')` and `corrupt`.
- Deferred single-pass execution.
- Metal.
- The benchmarks.
- The #840 fixes in the new code.

**Acceptance for Phase 1.**
- **The scripts.** A CASA `sm` script runs with only its imports changed:
  - for a VLA configuration and an ALMA configuration;
  - with one spectral window and one field;
  - with 1, 2 and 4 correlations.
- **Agreement with CASA.**
  - Columns, subtables, keywords, row counts, scans and flags are identical.
  - TIME follows decision 5.
  - UVW agrees within 1 µm at equal stored TIME as the goal; the tolerance is
    set from the first measurement, and the existing oracle uses 1 mm.
  - DATA agrees within 1e-3 normalized RMS.
- **Noise.** The variance of noisy minus noise-free data matches CASA's
  effective σ:
  - real and imaginary parts tested separately;
  - autocorrelations separately;
  - draws tested for independence across rows, channels and correlations.
- **Backends and workers.**
  - CPU and Metal agree within the tolerance of the imaging T0 Metal law.
  - Geometry and noise are bitwise identical for 1 and N workers.
- **casacore C++ reads the MS.**
- **Performance:** the §5 targets on cases 1 to 3 and 5, plus the
  no-regression rule.
- **Memory:** a counting-law test shows peak memory under the budget as rows
  and channels grow 10×.
- The oracle and benchmark scripts live under `tools/simulation/`.

### Phase 2: complete `observe` and `predict`

**Scope.**
- `observemany`, `setmosaicfield`, `setdata` and `openfromms`.
- **Component lists:**
  - reading CASA `ComponentList` tables, as an interop deliverable tested
    against a fixture written by C++;
  - the `cl` tool, as far as simulation scripts need it.
- **FTMachines:**
  - `mosaic`, including heterogeneous arrays (CPU and Metal);
  - `wproject` (CPU only);
  - `sd`, by image-plane sampling.
- The default voltage patterns for VLA and EVLA.

**Acceptance.**
- Parity with CASA for:
  - a heterogeneous ALMA/ACA mosaic;
  - a wide-field W case;
  - total power;
  - a component-list case.
- Benchmark case 4 meets its §5 target.

### Phase 3: corruptions

**Scope.**
- `setnoise(mode='tsys-manual')`.
- `setgain`, in fBM and random modes.
- `setleakage`.
- `setapply`, through `casa-calibration`.
- `reset`.
- Receptor pairs for 4 correlations.

**Acceptance.**
- Statistical parity with CASA for each corruption.
- Exact agreement for the deterministic parts: the Tsys formula, the
  radiometer σ and the collapse order.

### Phase 4: ATM

**Scope.**
- `casa-atmosphere`.
- `setnoise(mode='tsys-atm')`.
- `settrop`, in individual and screen modes.

**Acceptance.**
- Tebb and opacity agree with CASA's ATM, through the `at` tool, to about
  1e-6 relative across its bands.
- `tsys-atm` noise agrees with CASA statistically.

### Phase 5: `simutil` and `simobserve`

**Scope.**
- The CASA-signature `simobserve` (decision 2), built on the Rust session.
- The parts of `simutil` it uses:
  - `modifymodel` and `flatimage`;
  - `calc_pointings2`, including the ALMA OT tiling;
  - pointing files;
  - `noisetemp`, `bandname` and the direction helpers.
- The calibrator, leakage and the project-directory outputs.
- `synthetic-ms` replaces the family mode.
- Callers moved to it:
  - `tools/perf/imager/bench_simobserve.py`;
  - `bench_aca_simalma.py`;
  - `perf_harness/casa_aca_scenarios.py`;
  - `stage_wave1_datasets.py`;
  - `generate_wave1_casa_datasets.py`;
  - the justfile's t48 target;
  - `scripts/test-task-cli-hosts.py`.

**Acceptance.**
- CASA `simobserve` parity on the VLA ppdisk tutorial and on an ALMA
  configuration: the same output files, with the MS agreeing as in Phase 1.
- Pointing lists identical to CASA's.

### Phase 6: `simanalyze` and `simalma`

**Scope.**
- Both tasks, built on `imager`, `feather`, `imregrid`, `impbcor` and
  `immath`.
- The analysis step: the convolved model, the difference and the fidelity.

**Dependencies, planned separately.** Both tasks need:
- a `concat` task;
- single-dish imaging (`tsdimaging`).

**Acceptance.** Parity with CASA products for an ALMA case combining 12 m,
7 m and total power.

### Phase 7: retire the old code

**Scope.**
- Delete `simulation.rs`, `simulation_task.rs` and `casa-simulation-synthesis`,
  shrinking the allowlist.
- Move the imaging T1 fixtures and the perf benches to the new API.
- Update the capability matrix and the docs.

**Acceptance.** No caller of the old API remains, and `just quick` is green.

## 7. Risks

- **The operator's model preparation is shaped for imaging.** Phase 1a tests
  it before anything is built on it.
- **ATM's size.** About 27,000 lines may take more than one phase. Tests
  against CASA's `at` tool keep the port honest.
- **The ALMA OT tiling is intricate.** Pointing lists are compared with CASA's
  exactly.
- **Shared I/O code.** A faster write path touches every MS writer.
  - The casacore C++ interop suite must stay green.
  - The bounded write session keeps its memory model.
  - Gains are measured on quiet runs; the fanless laptop throttles, so A/B
    runs alternate with rests.
- **GLENDENNING must be mounted** for the sustained I/O runs. It is not
  mounted today.
- **Deferred execution changes when work happens.** The tests must cover
  scripts that interleave calls, such as `predict` twice, or `corrupt` before
  `predict`, and check they match CASA's sequential results.
