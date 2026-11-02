# Simulation programme: CASA's `sm`, `simobserve`, `simanalyze` and `simalma` in Rust

Truth class: normative plan
Date: 2026-10-20
Work issue: #841, which keeps the phase checklist and status
Status: accepted by the owner on 2026-10-20.

## 1. Goal and requirements

Make CASA simulation scripts run on casa-rs with only their imports changed,
and run them dramatically faster:

- the `simulator` tool (`sm`), with every user-facing method under CASA's
  name, signature and effective default;
- the `simobserve`, `simanalyze` and `simalma` tasks, with CASA's parameters
  and defaults.

Requirements:

- **ADR-0017, ADR-0018 and ADR-0019 apply.** All computation is in Rust,
  including the parts CASA writes in Python (`task_simobserve.py`,
  `task_simanalyze.py`, `task_simalma.py`, `simutil.py`); Python only
  presents CASA's interface.
- **Agreement with CASA follows AGENTS.md.** The MS has CASA's columns,
  subtables and keywords, apart from decision 6; predicted visibilities agree
  to about 1e-3 normalized RMS; noise and corruptions are statistically
  equivalent, not bit-identical. casacore C++ reads the MS, proved by an
  interop test.
- **Performance is the point.** Prediction uses Metal where it pays;
  geometry, prediction, corruption and writing use multiple workers; results
  do not depend on the worker count or the backend. Every benchmark reports
  wall time, single-worker (W1) user CPU and peak RSS, so parallelism cannot
  mask serial cost. The targets are in §5.
- **Memory is bounded.** Peak resident memory stays under a declared budget
  whatever the number of rows, channels or fields, proved by a counting-law
  test. Work streams in time and channel chunks sized from the budget, and
  Metal device buffers count against it. The MS write session stays
  bounded (ADR-0008).
- **The MS write path may be optimized; what is on disk may not change.**
  The same storage managers, tile formats and table metadata, guarded by the
  casacore C++ interop tests. The simulator writes every row in the layout
  casa-rs's other MS writers share, not `NewMSSimulator`'s.

## 2. Status

Phases 1 to 5 are merged, except 5e (`simobserve` graphics), which is now
part of #1025. By the owner's priority of 2026-10-31 (#841), the new tasks of
Phase 6 wait until the existing conversions reach behavioral equivalence
with CASA; their design and dependencies stand (§6). Phase 7 follows.

## 3. Owner decisions (accepted 2026-10-20)

1. **Namespace.** CASA tool classes are in `casars.tools` under casatools'
   names, tasks in `casars.tasks`; there are no `casatools` or `casatasks`
   alias packages (ADR-0019).
2. **The `simobserve` surface is CASA's.** The old family generator is the
   developer binary `synthetic-ms`, outside the catalog.
3. **`casa-simulation` is an application-layer crate** in ADR-0016's
   dependency policy. `simulation.rs`, `simulation_task.rs` and
   `casa-simulation-synthesis` are deleted at the end (Phase 7).
4. **ATM is ported to Rust** as `casa-atmosphere`.
5. **TIME honours the requested UTC.** CASA computes its geometry at the
   requested time + (TAI − UTC); the divergence is recorded in
   `docs/CASA (C++) bugs.md`. A parity run requests a start later by
   TAI − UTC at the reference epoch and compares rows at equal stored TIME.
6. **DATA only, unless a later step needs MODEL_DATA.** MODEL_DATA is created
   only when something needs it, and CORRECTED_DATA is not written.
   casa-rs's imager falls back from `datacolumn='corrected'` to DATA, as
   CASA's `tclean` does.

## 4. Design decided in planning

### Deferred, single-pass execution
The session records `observe`, `predict` and `corrupt` as a plan and, at
`close`/`done`, materializes the MS in one streamed pass (geometry,
prediction, corruption and the write for each chunk), published at `close`
as ADR-0008 staging requires. The plan is a fold: each `predict` and
`corrupt` applies to the scans observed before it, and a `predict` that is
not incremental replaces every step before it. A call that needs data on
disk forces the pending pass. Corrupting an existing MS (`openfromms` +
`corrupt`) patches it in place through the bounded selected-row mutation
path, in chunks aligned to the DATA tile.

### Prediction
Prediction goes through the imaging operator, so simulation and imaging
share one forward model, and through `casa-imaging-runtime`'s worker teams,
Metal backends and budget (`HostResources`, `ResourcePolicy`): the simulator
builds no second budget or worker model. Total power is image-plane
sampling of the model convolved with the primary beam, in
`casa-imaging-singledish`.

### Random numbers
A counter-based generator keys each draw by seed, row, channel, correlation
and real/imaginary part, so noise is bitwise identical for any worker count,
chunking or backend. It is not CASA's MLCG stream.

### CASA quirks
CASA quirks that shape outputs users compare are matched (default limits,
SIGMA and WEIGHT left at their geometric values after `corrupt`, noise
scaling, fBM normalization, scan numbering, autocorrelation defaults).
casa-rs diverges only for crashes, undefined behaviour or data corruption,
such as `setmosaicfield`'s integer division; every divergence is recorded
in `docs/CASA (C++) bugs.md`.

## 5. Performance and I/O targets

Each benchmark is timed for CASA, the old simulator, and casa-rs on CPU and
on Metal. CASA's time is measured in-process, per call, excluding Python
start-up.

| Mode | Target |
|---|---|
| Analytic and component-list prediction | No slower than July's record (#257) on the same case; the CASA multiple measured on a case where CASA runs ≥ 60 s. |
| Image-model prediction, CPU | ≥ 10× CASA on the same input, with W1 user CPU reported. |
| Image-model prediction, Metal | I/O-bound: meets the I/O target below; the CASA multiple reported. |
| I/O-bound passes (geometry only, noise only, write only) | Judged against the device's ceiling, not against CASA. |

**No-regression rule.** On every case, the new simulator is no slower than
the old one, within 5%; a trade-off needs owner approval.

**I/O target.** The sustained MAIN-table rate (bytes on disk over the wall
time from the first row to publication, including the final sync), on a
dataset at least twice the size of RAM with the page cache purged, is at
least 70% of the same volume's raw sequential-write ceiling for the
write-only case and at least 50% end to end with Metal prediction
(`tools/simulation/bench_sustained_io.py`). Phase 1 met both on the external
NVMe drive (owner decision, 2026-10-22, #855). The 1 GiB internal-disk case
stays as a quick, cache-resident check.

**Benchmark cases.** ALMA (about 43 antennas, 1 h of 10 s integrations,
256 channels, a 1024² image model); VLA of similar size; one MFS model plane
degridded at 256 frequencies; analytic and component-list models; write only.

## 6. Phases

Phases 1 to 5 are on #841. What remains:

### Phase 6: `simanalyze` and `simalma`

**Scope** (planned 2026-10-27; the split is on #841).
- **Both tasks,** ported to Rust (ADR-0018) from CASA 6.7.6's installed
  `task_simanalyze.py` and `task_simalma.py`, with the `simutil` and
  `sdbeamutil` parts they use: `imtclean`, `convimage`, `statim`,
  `cellsize`, `ismstp`, `primaryBeamArcsec` and `TheoreticalBeam`.
- **`simanalyze`:** finds the project's MeasurementSets and sky model; grids
  total power with `tsdimaging`; concatenates several interferometric
  MeasurementSets and images them through the imager, with `tclean`'s
  arguments as `imtclean` builds them; feathers; and analyses the convolved
  model, the difference, the fidelity and the statistics.
- **`simalma`:** runs `simobserve`, `simanalyze`, `concat` and `feather`.
  Its own logic chooses the configurations and allocates the time; it lays
  out the total-power pointings, scales the total power, and makes the
  primary-beam-weighted feather.
- **The Python surfaces** mimic CASA's exactly from the start (#931).

**Owner decisions (2026-10-27).**
- The single-dish gridder is in its own library crate,
  `casa-imaging-singledish` (#938), which holds the kernels, the image-plane
  gridder and `simobserve`'s total-power predictor (the gridder's adjoint).
  Single-dish image-plane gridding is its own operator family, outside
  ADR-0016's four interferometric kernel sets.
- **`tsdimaging` and `concat` read and write through `casa-observation`**
  (#937, #938, #939), not through `casa-ms`'s older access paths.

**Decided in planning.** Each part records these in `docs/CASA (C++) bugs.md`
when it implements them.
- **Total-power intensity scaling:** each task keeps its own formula, which
  differ by 8.7%. A part departs from one only if it measures that formula
  to be physically wrong.
- **`simalma`'s `TypeError`** for configuration names without "cycleN"
  (`task_simalma.py:523`) is fixed.
- **The user's `modelimage`** is not changed in place; CASA's master branch
  has removed those lines.
- **No `.tclean.last`** is written (#931: no Last profiles from Python).
- **Products do not depend on `graphics`.** In CASA, `simalma`'s combine
  figure deletes `simanalyze`'s flat images.
- **Sky-model discovery** takes candidates in sorted order, not in `glob`
  order.
- **Acceptance avoids `simanalyze`'s feather of total power with
  interferometer data,** which is not primary-beam weighted. CASA skips its
  own test of that path because `feather` fails. The `simanalyze` part
  decides it by the better-physics rule; `simalma`'s weighted feather is the
  candidate.

**Parts,** one PR each. 6a (image analysis in `casa-images`), 6b (imager
`startmodel`, `uvtaper`, non-square `imsize`, optimum size), 6c (the
`casa-imaging-singledish` core) and 6f (`simanalyze`'s analysis) are merged.

| Part | Content | Waits for |
|---|---|---|
| 6d | `tsdimaging` on `casa-observation` (#938 part 2) | #931 2a and 3 |
| 6e | `concat` core on `casa-observation` (#939) | #931 2a and 3 |
| 6g | `simanalyze`: discovery, imaging, total power, concat, feather; its host and Python surface | 6d, 6e, #931 3 |
| 6h | `simalma` | 6g |
| 6i | Graphics for both, on 5e's plot document | 5e (#1025) |

**Acceptance.**
- **`regression/sim_multi_arrays_and_TP`.** It runs `simobserve` for 12 m,
  7 m and total power, then `simanalyze` twice.
  - The oracle is CASA 6.7.6-14's products, regenerated by a committed
    recipe. The references in the test data are from an older CASA:
    6.7.6-14 differs from them by 4–7% NRMS in DATA.
  - Compared by NRMS: the MeasurementSets (DATA within 1e-6, as in
    Phase 5), the total-power image, and the images and difference images.
- **`regression/simalma_12m_ACA_combination`.** CASA's test checks only that
  14 PNGs and 5 MeasurementSets exist. Acceptance instead compares every
  product with CASA 6.7.6-14's: the MeasurementSets, the concatenation
  included; the total-power image, each configuration's image and the
  concatenation's; the feathered image, the differences and the fidelities.
  The PNGs come after 6i.
- **Timing rows** for both cases, against CASA. The multi-array case takes
  about 30 s in CASA 6.7.6-14 on the Mac mini.

### Phase 7: retire the old code

**Scope.**
- Delete `simulation.rs`, `simulation_task.rs` and
  `casa-simulation-synthesis`, shrinking the file-length allowlist.
- Move the imaging T1 fixtures and the perf benches to the new API.
- Update the capability matrix and the docs.

**Acceptance.** No caller of the old API remains, and `just quick` is green.
