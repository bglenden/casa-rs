# Imaging foundation: review and phased refactor plan

Truth class: normative plan
Date: 2026-10-07
Branch: `claude/imaging-foundation` (cut from `codex/t55-full-size-validation`
at `e714fd2339`)
Status: APPROVED by the owner on 2026-10-07 (decisions D1–D10 and closure of
the #486 family included). COMPLETE: IF-0 to IF-11 landed by 2026-10-16.
Section 5 states the landed signatures (IF-11, #660), and the deviations the
tickets took are reconciled into sections 3 and 5 (section 12).
Reviewed sources: six read-only deep dives over the imaging crates, the
RadioAstronomyOracle corpus (NRAO 2024/2026 synthesis workshop slides,
Synthesis Imaging II), CASA `synthesis/` C++, LibRA/HPG, and issues #486,
#541, #543, #546, #552–#554, #625, #445–#450, #341, #217.

## 1. Purpose

Review the cube, MFS, Clark, multi-worker and Metal imaging code added under
programme #486 and define one refactor that leaves a single mathematical
imaging foundation. After it, standard, W-projection, AW-projection, mosaic,
MT-MFS, cube and MFS imaging are parameterisations of one measurement
operator, one major-cycle pass, one deconvolution driver and one execution
model, on CPU or Metal. Performance and memory stay within the measured
checkpoints. Code that exists only to re-verify, re-hash, re-validate or
re-describe what the type system or an earlier layer already guarantees is
removed.

This document is written so that a different implementer (Opus) can execute
each ticket from the text without re-deriving the design. Where a signature is
given, it is the intended signature. Where a file is named for deletion, the
ticket is not done until the file is gone.

Section 2 records owner decisions already made. Section 3 lists decisions
this plan asks the owner to confirm. Section 4 is the review. Sections 5 to 8
are the design, deletion list and test strategy. Section 9 is the ticket set
with the review gates. Section 10 is the anti-slop rule set every ticket is
reviewed against. Section 11 is process.

## 2. Owner decisions recorded 2026-10-07

- Base: branch from the T55 tip, not `main`. T55 stays unmerged; the first
  merged ticket of this plan carries its content to `main`.
- Plan first, then tickets. Opus implements most tickets; Fable lands the
  foundation tickets (IF-0, IF-1) so later tickets have a concrete pattern.
- Breadth first: convert all imaging code to the shared design using tests
  that take at most about ten minutes each, then run one systematic
  performance and memory pass against the representative datasets.
- Shrink runtime bookkeeping (receipts, evidence, ledgers, resource authority)
  to what admission, cancellation, Metal fences, bounded streaming and atomic
  publication need. ADR text is superseded where required.
- Move mosaic, W and AW onto the shared operator now and thoroughly, so no
  second refactor follows.
- Prefer fewer law-based tests over fixture-heavy tests. Each guaranteed
  behaviour keeps coverage; the count of tests is not a goal.
- Programme #486 and its subsidiary tickets may be closed as superseded, with a
  new ticket set, provided no work or evidence is lost.
- Resource limits: 16 GiB native, two Cargo jobs. GLENDENNING is mounted; CASA
  is at `/opt/homebrew/bin/casa`.
- Review gates: at logical points the owner has Fable and (probably) OpenAI
  Astra review the implementation for drift from these goals and replan from
  discoveries. Section 9.3 defines them.
- D2 agreed, with the addition that CPU f32 grids are a user-selectable
  switch for large problems. Obit and LibRA are to be surveyed for processing
  techniques worth adopting; adopted ones are recorded in section 5.9.

## 3. Decisions requested from the owner

Each is stated with the recommendation. Approval of the plan approves these
unless the owner strikes one.

- **D1 Replay cache.** Delete the gridded-normal replay subsystem
  (`gridded_normal_operator*`, `complete_data_operator.rs`, `metal_normal.rs`,
  the replay half of `managed_spill.rs`; about 20k lines). Later major cycles
  re-traverse the MeasurementSet and grid `V − A·m` per sample, as CASA does.
  Reason: the cache is not scientifically required; it stores about 40 bytes
  per row-channel sample, which for the Wave3 workload is an 86 GB scratch
  reservation against a 36 GB input; it is valid only while weighting is
  frozen; and it forms the residual as a difference of two large f64 images
  rather than gridding the residual visibilities. The performance pass (IF-10)
  may add a bounded tap-plan cache as an execution strategy over the same
  operator if measurement shows it pays. **Qualified (#659 deviation 7, owner
  2026-10-14):** a run keeps its samples' imaging weights, not the samples,
  across passes (`WeightCache`, 4 bytes a selected row and channel) when it
  has one image domain, a constant or Taylor basis and every plane resident,
  and the slots fit a sixteenth of the free memory; they are admitted before
  they are allocated. A record or placement cache across majors (2.6–8.9 GB a
  major on the 32-channel cube) stays rejected.
- **D2 Grid precision (owner agreed 2026-10-07).** Metal is one
  implementation of the backend trait and accumulates in f32 with atomic adds
  (nondeterministic order). The CPU accumulates in f64 by default. This is
  consistent with the owner's 2026-09-24 numerical direction: acceptance is
  the 1e-3 normalised tolerance, not bitwise agreement. Grid precision is a
  user-selectable request field, `gridprecision = auto | f32 | f64`, exposed
  in the parameter catalog. `auto` selects f64 for constant and Taylor bases
  on CPU and f32 for channel-local cubes and for Metal. `f32` halves grid and
  accumulator memory so large problems become tractable on this workstation;
  the CPU backend supports both precisions for every convolution-function set.
  **Amended by the owner 2026-10-15 (#659):** `auto` selects f64 on the CPU
  for every basis, channel-local cubes included, and f32 on Metal. An f32
  running sum drifts with the number of samples a grid cell takes: a mosaic
  cube's weight image, which takes every sample of a channel at the uv
  origin, missed CASA (which accumulates in double) by 2.6e-3. `f32` stays a
  user choice for large problems, and choosing the GPU accepts it.
  **Device choice (#659 deviation 14, owner 2026-10-15):** `backend = auto |
  cpu | gpu`, defaulting to `auto`, which takes Metal only for problems a T1
  Metal law compares with the CPU (section 5.8); an explicit
  `gridprecision = f64` keeps `auto` on the CPU.
- **D3 AW convolution-function cache.** Replace the private content-addressed
  prepared-artifact store (manifest schema 7, 18 hash domains, eviction
  ledger; about 15k lines across `aw_cache.rs`, `prepared_aw_phase.rs`,
  `prepared_artifact*`) with: read CASA `CFS_*`/`WTCFS_*` images directly
  into an in-memory LRU of cells; native EVLA CF generation writes CASA-format
  CF images into a cache directory, so one loader serves both. Payload
  checksums on an importable, regenerable cache are dropped.
- **D4 Crate layout.** Split `casa-imaging-reconstruction` into
  `casa-imaging-operator` (measurement operator, convolution functions,
  weighting, normal images) and `casa-imaging-deconvolution` (minor-cycle
  driver, solvers, PSF summary, masks). Add `casa-imaging-metal` (macOS only)
  implementing the backend trait. Move the CASA image writer into
  `casa-imaging-products`. Delete `casa-imaging-reconstruction` when empty.
  **As landed (IF-11, #660 deviation 1):** the operator owns the
  `NormalImages` a pass finishes into and CASA's continuum fit; the normal
  state the minor cycle and products read (resident or paged), the model
  lifecycle and its storage seam, the masks and the `MajorCycle` that pairs
  model and normal state went to deconvolution, because the masks read the
  normal state and the lifecycle uses the masks' reprojection. The
  reconstruction crate was deleted in IF-11.
- **D5 Runtime core.** Replace receipts (schema 25), the execution DAG,
  execution bindings, the resource-authority topology, observation
  transactions and the cost model with: `HostResources`, one `admit()` per
  phase returning an RAII reservation, a sequential phase list with a cancel
  token, the trimmed Metal execution state, the bounded worker team, and an
  O(1)-per-phase `RunSummary`. ADR-0010 is superseded by a new ADR-0016.
  **Extended (#659 deviation 8, owner 2026-10-15):** the policy returns a
  core allocation (`ResourcePolicy::cores`): compute workers for minor cycles
  and products, and streaming workers for passes, as many as fit beside the
  source's producer thread within the policy's core budget. A run holds two
  worker teams (`WorkerTeams`), one pool when the widths agree.
- **D6 Architecture checker.** Retire `scripts/check-imaging-architecture.py`
  (3,578 lines; hashes Rust function bodies and pins four SHA-256 ratchets),
  `test-imaging-architecture-structural.py`, `migration-matrix.json`,
  `baselines/` and the representative-science receipt registry. Replace with a
  dependency-direction check (about 150 lines) over `cargo metadata` plus a
  forbidden-import grep (device APIs only in `casa-imaging-metal`; no
  `std::env` in library crates; no `sha2`/`crc` in imaging crates outside
  external-format readers).
- **D7 Programme closure.** Close #486 and T-tickets #541, #543, #546, #552,
  #553, #554 as superseded by the IF ticket set, each with a closing comment
  that links this plan, the final T55 commit, the durable evidence roots on
  GLENDENNING and the NAS, and the IF ticket that inherits its outcome. #625
  closes into IF-10 (its serial AW/MT-MFS time bar becomes an IF-10
  acceptance row). #341 closes into IF-5; #217 into IF-8. The VLASS wave
  #445–#450 stays open with a comment that IF-3/IF-10 are its route.
- **D8 Request layer.** One `ImagingRequest` serde struct, defaulted and
  validated from the provider-contracts catalog, replaces `CliConfig`,
  `ImagerRunTaskRequest`'s mirror types, `ContinuumImagingRequest` and
  `ApplicationRequest`. Dead and always-rejected task requirements are removed
  from the catalog, not merely from the Rust enum.
- **D9 Diagnostics.** Library crates read no environment variables. Diagnostic
  switches become fields of `ImagingRequest.diagnostics` (stage timing, Metal
  stage profiling, science trace); everything is emitted through `tracing`
  via `casa-logging`. The required `CASA_RS_IMAGING_SPILL_*_BYTES_PER_SECOND`
  variables and the same-filesystem requirement disappear with the cost model.
- **D10 Cube state paging.** Keep one paged cube-state implementation
  (`managed_cube_blocks` + `managed_model`/`managed_normal`); delete
  `paged_cube_state.rs`.

## 4. Review findings

### 4.1 Shape of the code today

Imaging source (non-test) is about 200k lines across six crates; tests add
about 65k. Twenty-two files exceed 3,000 lines; `spectral_operator.rs` is
15,182 lines and `receipt.rs` is 8,455.

The runtime split is not cube versus MFS. It is a fast cube path
(`BulkCubePhase`: natural weighting, no start model, no visibility transform,
one SPW/field/polarisation layout, CPU gridding loop in
`reconstruction/src/streaming_cube/band.rs`) versus a general path
(`SpectralCycleExecutor`) serving MFS, MT-MFS, Briggs/uniform cubes, mosaic
and any `MODEL_DATA` write. Both plug into one application-level
`MajorCyclePhase` loop (`application/src/lib.rs:404-864`) and converge on
`run_reconstruction_cycle` (`runtime/src/spectral_cycle.rs:3357`).

Four CPU drivers implement the same grid/degrid mathematics:

| Driver | Lines | Capabilities |
|---|---|---|
| cube band loop `streaming_cube/band.rs` | 1,712 | standard CF, natural weighting, f32 grids, one SPW layout |
| `SpectralSlabOperator` in `spectral_operator.rs:7542-10871` | ~3,300 | standard, W, AW, mosaic via enum `match`; sample-serial on one f64 grid; a Mutex per AW sample |
| gridded-normal replay `gridded_normal_operator*` + runtime `complete_data_operator.rs` + `managed_spill.rs` | ~11,300 + 6,400 + 4,200 | residual major cycles over spilled 40-byte records; fixed 4+4 logical lanes; six record layouts; a legacy sector path compiled only under `cfg(test)` |
| initial MFS planes `spectral_operator/initial_planes*` | ~1,100 | initial pass with empty model only; 64-row strips |

Metal has two more drivers (`streaming_cube/metal_wave.rs` 1,068 and
`complete_data_operator/metal_normal.rs` 647) over one runtime
(`metal_runtime.rs` 3,113) with six MSL kernels in one inline string
(`metal_cube.rs:19-268`). The kernels hard-code seven separable real taps and
oversampling 100, accumulate with relaxed f32 atomics, and re-implement the
CPU row interpolation and Stokes reduction in MSL. Three double-buffer ticket
rings and two arena layout schemes are duplicated across the drivers. FFTs are
FFTW everywhere; CLEAN, normalisation and publication are CPU.

Gridders are closed enums (`ConvolutionOperator{Standard,WProjection}`,
`OperatorTaps{Standard,Aw,Mosaic}`) matched at about twenty sites. AW
(`aw_projection.rs` 3,137) and mosaic (`mosaic.rs` 1,593) are separate code
paths that neither the fast cube path nor the Metal kernels can use. CASA's
linear channel resampling is implemented five times; the Stokes-I reducer four
times; the standard seven-tap loop six times; model FFT preparation and image
formation twice each; seven different per-sample transport structs exist.

Clark is one implementation (`minor_cycle/clark.rs`) with sparse and FFT
refresh. But one dispatch (`run_reconstruction_cycle`) feeds four separate
CLEAN loops (per-plane Hogbom/Clark/multiscale, image-domain Hogbom, Taylor,
joint) with four copies of the threshold formula, four of mask validation and
three of evidence minting. Peak search exists at about nine sites and PSF
subtraction at about eight. The Taylor and joint candidate selectors are
about 80% identical. The circular residual refreshes are O(iterations ×
samples × N²) where an FFT refresh exists already. Clark's sparse refresh
spawns raw `std::thread::scope` threads outside the worker team. Known
inconsistencies: Clark admits peaks at or below threshold where the others
require strictly below; the 1% global-convergence check runs only for
single-plane and Taylor runs; divergence tests differ per solver; and the
multi-plane cycle-threshold prepass filters the residual by mask only while
the in-solve peak uses mask plus valid support, which changes the shared
threshold in PB-limited fields.

### 4.2 Bookkeeping and process machinery

- Execution is sequential: `run_inner` starts no threads; `next_action`
  dispatches the first ready DAG node in id order. Parallelism exists only
  inside a node (`FixedWorkerTeam`) plus asynchronous Metal/IO fences. A
  Hogbom run with N major cycles builds N+2 plans, each admitted, run,
  receipted and reopened.
- Receipts are write-only. The single production read is the planned worker
  count, which the plan already holds. Each phase reopens its receipt, which
  does a payload SHA-256, about 86 integrity checks and a full DAG rebuild and
  rehash. `receipt.rs` (8,316 production lines) defines about fifty
  projection structs. External readers: one manual tool
  (`tools/perf/imager/intermediate_profile_evidence.py`). The Mac app, TUI and
  `casars-imager` read none.
- Admission happens twice (`plan()` acquires and releases a lease; the
  scheduler acquires again). Host memory is layered at least eight times over
  one number. "Preferred" is always set equal to "hard". About 120
  budget/claim-style type names exist. The time prediction that orders worker
  candidates is a constant 1 ms per stage, so the search is a sort by node
  count. `cost_model.rs` (804 lines) is only ever bootstrapped.
- Cancellation is not wired: the controller returns Continue or Adapt; there
  is no SIGINT handler. The low-memory adaptation is unreachable from
  `casars-imager`.
- ADR-0015 violation on the hot path: `bounded_stream.rs` finalises a SHA-256
  per partition per worker, then per block, then per window
  (`:863-1370`); nothing reads them but `eprintln!` and tests.
- Probable bug: attempt ids are deterministic over paths, image size, cell and
  phase centre; `begin()` refuses an existing receipt, so re-running into the
  same image name should fail until retention prunes the old receipt. It goes
  away with receipts.
- Native runs require two environment variables for storage rates and that
  input and output share a filesystem.
- About 163 distinct `CASA_RS_*` names exist; 12 are read in production
  (section 8.3). There are about 120 production `eprintln!` sites. No imaging
  crate depends on `tracing` although `casa-logging` exists.
- Request parameters are carried by eight struct types between the command
  line and `CompiledProblem`; `specmode` has seven spellings; AW controls
  have five. Defaults drifted between the CLI and JSON paths (`gridder`,
  `write_preview_pngs`). `task_contract.rs` (5,033 lines) is about 65% mirror
  types, restated defaults and their round-trip tests; its progress schema is
  emitted by no Rust code. Of 38 `TaskRequirement` variants, 21 are dead or
  always rejected, and 12 catalog parameters exist only to be rejected when
  set. `JointContinuumLine` is unreachable. About 1,950 lines compute SHA-256
  identities over contracts that are only compared with themselves in one
  process. The selected-row sequence is re-hashed per sample in production.
- Three tests on the T55 branch have stale fixture budgets after T55's
  accounting changes (issue #541, 2026-10-06 comment).
- `scripts/check-imaging-architecture.py` hashes Rust function bodies and
  struct fields and pins four SHA-256 ratchets over a 115 KB migration matrix
  (77 rows, contract revision 93). It would fight every commit of this
  refactor. ADR-0009's dependency-direction rule is the only part to keep.
- Representative-science evidence: 18 of 19 matrix rows have lost their
  external CASA receipts (`--require-external` fails), so T2 oracles must be
  regenerated during IF-10. `scripts/test-imaging-parity.sh` runs a deleted
  test binary.

### 4.3 What is worth keeping

- ADR-0009's mathematical contract (A, A*, W, H = A*WA, paired operators,
  spectral law/sampling/basis/coupling, product contract). The refactor
  implements it rather than replacing it.
- The CPU seven-tap kernel maths, the W-plane kernel construction, the AW cell
  selection rules (conjugate frequency, Mueller swap on w sign, pointing
  phasor, division by norm on degrid only), the mosaic PB-phased kernels and
  the density-weighting cell rules. These are CASA-pinned science and are
  moved, not rewritten.
- One Clark implementation with sparse and FFT refresh; the half-spectrum
  `RealFft2<f32>` refresh.
- `FixedWorkerTeam`, `execute_bounded`/`execute_overlapped` and the one or two
  slot producer/consumer stream in `bounded_stream.rs`, without measurements
  and digests.
- `MetalExecutionState` device, queue, buffer residency, submit/wait/drain.
- The managed spill v4 framed writer/reader (same-run, no CRC) for cube
  inputs; one paged cube-state backend.
- Atomic individual-image replacement in `casa_product_sink.rs`.
- `generate_synthetic_observation_ms` in `casa-ms` (analytic point and
  Gaussian skies, multiple pointings, full polarisation; extended to several
  spectral windows in IF-0).
- The CASA comparison tooling (`tools/perf/imager/perf_harness/image_compare.py`,
  `tolerances.py`, `run_workload.py`, `mfs_4096_pilot.py`), the golden datasets
  on GLENDENNING and the NAS archive.
- Measured checkpoints as the acceptance bars for IF-10: cube Metal W4 deep32
  109.95 s; MFS intermediate-90 four-SPW pilot CPU W1 29.80 s / W4 15.82 s /
  Metal W4 14.80 s versus CASA 52.82 s; MFS 512-channel 512² W1 48.46 s / W4
  20.25 s; C-array 512-channel cube CASA 6.0 h; Wave3 4096² MFS CASA 7,268 s.

## 5. Target architecture

### 5.1 Formalism (normative vocabulary for code and docs)

Per baseline, frequency ν and time, the measurement equation is

```
V_ij(ν,t) = M_ij · S · ∬ M^S_ij(l,m,ν,t) I(l,m,ν) e^{-2πi(ul+vm+w(n-1))} dl dm / n
```

with M_ij direction-independent gains (calibration, outside imaging), M^S_ij
the direction-dependent Mueller response (primary beam, parallactic angle,
leakage), and w(n−1) the w-term. Let x be the model coefficients on the pixel
grid and d the selected unweighted visibility samples. The forward operator
A : X → D and its adjoint A* are composed as

```
A   = Degrid ∘ Fft ∘ Correction ∘ PolBasis⁻¹ ∘ ModelPrescale   (R1: the same diagonal 1/grdsf on both sides, CASA GridFT)
A*  = PolBasis ∘ Correction ∘ Fft⁻¹ ∘ Grid
W   = diag(input weight · taper · density weight), flags removed from D
b   = A* W d          dirty image (unnormalised)
H   = A* W A          normal operator; PSF = A* W 1
g(x)= A* W (d − A x)  residual image, formed by gridding residual samples
sumwt(plane,pol) = Σ W  (AW: Σ W·|Σ taps|)
```

Capability-specific factors and the per-sample data they need:

| Capability | What changes | Per-sample / per-row data |
|---|---|---|
| Standard | kernel = prolate spheroidal, real, separable, support 3, oversampling 100 | u,v,w,ν, weight, flag, polarisation |
| W-projection | kernel = C ⋆ FT[e^{2πi w(√(1−l²−m²)−1)}] per w-plane; complex; support grows with |w| | w-plane index; sign(w) selects conjugation |
| A-projection | kernel = FT[A_i ⊗ A_j*]; image divided by PB response afterwards | PA cell, antenna-type pair, frequency cell, Mueller row |
| AW (wideband) | both; conjugate-beam frequency √(2ν0²−ν²) when conjbeams | w, PA, frequency, Mueller, pointing phase gradient |
| Mosaic | kernel = FT[PB] with phase ramp e^{2πi(uΔl_k+vΔm_k)}; weight image from FT[PB²] | per-row pointing offset → phase gradient |
| MT-MFS | A_t = A·diag(((ν−ν0)/ν0)^t); 2N_t−1 PSFs, N_t residuals | per-sample spectral factor s = (ν−ν0)/ν0 |
| Cube | same A per plane; chanMap routes samples to planes | plane index |
| Stokes/Mueller | polMap correlation → grid pol; Mueller row maps swap between forward and adjoint | Mueller row per direction |

Paired (same choice in forward and adjoint, ADR-0009): the kernel set,
support and oversampling; the gridding correction; the conjugation convention
(w sign, Mueller row swap); the phase gradient (ramp vs conj ramp); the
phase-centre phasor; chanMap/polMap; Taylor factors; the FT[PB]/FT[PB²] pair.
Data-only: flags, W, density weights, sumwt. Image-only, owned by the product
contract and never folded into A*: PB normalisation (`flatnoise`, `flatsky`,
`pbsquare`), pblimit blanking, restoring beam, pbcor. CASA's normalisation
conventions are tabulated in section 5.7 and are the acceptance reference.

### 5.2 Crate ownership

| Crate | Owns | Depends on |
|---|---|---|
| `casa-imaging-model` | `ImagingRequest`, `CompiledProblem` (geometry, spectral law/sampling/basis, weighting spec, operator spec, polarisation, product contract, selection, write set, visibility transform, model target), typed errors | ecosystem only |
| `casa-imaging-operator` | `Placement`, `SampleBlock`, `ConvolutionFunctionSet` + four implementations, `GridBackend` trait + `CpuBackend`, `GridAccumulator`, `MeasurementOperator`, `WeightingGeneration`, `NormalImages`, FFT and correction operators, polarisation routing, spectral resampling (one implementation), CASA's visibility-domain continuum fit | model, casa-fft, casa-numerics; casa-images, casa-coordinates, casa-types for AW cell images |
| `casa-imaging-deconvolution` | `MinorCycleView`, `Driver`, `Solver` trait + Hogbom/Clark/Multiscale/Taylor solvers, `PsfSummary`, masks and automask, `ModelDelta`; the model lifecycle and its storage seam (`ModelLifecycle`, `ModelStorageFactory`), the normal state (`FinalNormalState`, `NormalBasis`, `NormalStorageFactory`), `MajorCycle` (IF-11, #660 deviation 1) | model, casa-numerics, casa-fft |
| `casa-imaging-products` | normalisation (section 5.7), restoration, Taylor products (alpha, error), PB/pbcor, product list and metadata, CASA image writer with atomic replacement | model, deconvolution, casa-fft, casa-numerics, casa-images, casa-coordinates, casa-types |
| `casa-imaging-metal` (macOS) | `MetalBackend: GridBackend`, MSL kernels, device/queue/buffer/fence state | operator, objc/metal crates |
| `casa-imaging-runtime` | `HostResources`, `ResourcePolicy` + `CoreAllocation`, `admit()`, `Phase`/`run_phase`, cancel token, `WorkerTeam`/`WorkerTeams` + bounded stream, `Partition`, `Residency`, `MajorCyclePass`, `WeightCache`, cube state paging, the source's content budget, `RunSummary` | model, operator, deconvolution, products, metal (cfg macos), casa-ms, casa-tables, casa-lattices |
| `casa-imaging-application` | compile → plan → run composition, MeasurementSet access and selection, availability of installed capabilities, publication orchestration | all of the above |
| `casars-imager` | CLI parsing via the catalog, request projection, progress and result presentation | application, provider contracts |

Dependency direction is enforced by the replacement checker (D6). Only
`casa-ms` reads a MeasurementSet: the runtime finalises the bounded access's
content budget and the application turns its blocks into native rows. No
crate but `metal` imports a device API; no crate but `casars-imager` and
`application` reads `std::env`.

### 5.3 The operator core (`casa-imaging-operator`)

The signatures below are the landed ones (IF-11); bodies, derives and minor
accessors are omitted.

```rust
/// One selected row×channel sample after flagging, phase-centre shift and
/// correlation routing. Flagged samples are not placed; samples whose kernel
/// support would leave the padded grid are dropped here (CASA rule), so the
/// kernels have no bounds tests.
#[derive(Clone, Copy)]
pub struct Placement {
    pub u: f64, pub v: f64, pub w: f64,  // wavelengths at this sample's frequency
    pub prediction_w_positive: bool,     // sign of the w the prediction keys on (AW: the MS w)
    pub phase: f64,                      // phase-centre shift argument (rad); 0 when none
    pub plane: u32,                      // target grid plane (channel-local); 0 for constant basis
    pub spectral: f32,                   // (ν−ν0)/ν0; read only by a Taylor basis
    pub cf: CfKey,                       // opaque to the kernel; resolved by the CF set
    pub gradient: [f32; 2],              // pointing phase gradient (rad per cell); 0 otherwise
}

/// Two-level cell key (HPG `CFSimpleIndexer`): `group` linearises the
/// variable-support axes (w-plane, frequency cell, PA cell, antenna-type
/// pair); `cube` indexes fixed-size slices inside the group. Mueller planes
/// live inside the cell, not in the key. Conjugation is decided in the
/// kernel from `sign(w)` and the direction; baseline-order conjugation for
/// heterogeneous arrays is baked into the cell by the CF set.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CfKey { pub group: u16, pub cube: u16 }

/// Structure-of-arrays view of one bounded block. `values` and `weights` are
/// `npol × n`, sample-major. Weight 0 never appears (flagged samples are dropped).
/// Values are pre-multiplied by the phase-centre phasor and the weight once,
/// here, not per tap. A `Work::Predict` block may hold no values or weights;
/// a `Work::NativeResidualGrid` block also holds each sample's native
/// endpoints in `sources`. `SampleBuffer` owns the storage.
pub struct SampleBlock<'a> {
    pub placements: &'a [Placement],
    pub values: &'a [Complex32],
    pub weights: &'a [f32],
    pub sources: &'a [NativeSource],
    pub npol: usize,
}

pub enum TapLayout<'a> {
    /// Standard: one real row per oversampled offset, `rows[offset*support + i]`,
    /// oversampling-major so one fractional offset is one contiguous row.
    SeparableReal { rows: &'a [f32], support: u16, oversampling: u16 },
    /// W, AW, mosaic: dense complex kernel ordered `[oy][ox][mueller][iy][ix]`
    /// (x fastest; one fractional offset contiguous) with `oversampling + 1`
    /// fractional rows per axis and no padding cells (R1): `GridGeometry::locate`
    /// is the one rounding rule (anchor = nearest cell, offset =
    /// round((anchor − coordinate)·oversampling) + oversampling/2, even
    /// oversampling), under which the fine-offset sign never changes the
    /// major index. `mueller_planes` is 1 for scalar kernels.
    Dense { data: &'a [Complex32], support: [u16; 2], oversampling: u16,
            mueller_planes: u8 },
}

/// Which CF Mueller plane serves each (grid pol, visibility pol) pair, per
/// direction of the transform and sign of `w`, each indexed by
/// `usize::from(w > 0)` and read through `table(w_positive, forward)`. The
/// kernel flips the imaginary part with the conjugate partner (HPG
/// `mueller_indexes` / `conjugate_mueller_indexes`). `None` skips a pair.
pub struct MuellerRouting { pub adjoint: [Vec<Vec<Option<u8>>>; 2], pub forward: [Vec<Vec<Option<u8>>>; 2] }

pub trait ConvolutionFunctionSet: Send + Sync {
    /// Pure. Row context carries PA, antenna pair and types, field, SPW, time.
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey;
    /// The one cell every sample keys, when the cells do not vary.
    fn uniform_key(&self) -> Option<CfKey> { None }
    /// Imaging taps of a cell; `hold` keeps a cached cell alive while read.
    fn taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s>;
    fn max_half_support(&self) -> [u16; 2];
    /// FT[PB²] taps for the weight/sensitivity image (mosaic, AW); None otherwise.
    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>>;
    fn mueller(&self) -> &MuellerRouting;
    /// The w the prediction keys on; AW overrides with the MeasurementSet's.
    fn prediction_w(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> f64 { w_lambda }
    /// Paired image-domain gridding correction (separable 1-D vectors per axis).
    fn image_correction(&self) -> &ImageCorrection;
    fn normalisation(&self) -> KernelNormalisation;   // UnitSum, RealSum or KernelSum
    fn pointing_ramp(&self) -> bool;
    // Defaulted, overridden where CASA differs: `psf_taps` and
    // `prediction_taps` (AW: weight cells, native-frequency cell),
    // `placement_half_support`, `admits(w)` (WProjectFT drops w beyond the
    // last plane), `corrects_weight_image`, `weight_image_magnitude`.
}
```

Implementations: `Spheroidal` (standard), `WPlanes`, `AwCatalog` (CASA
`CFS_`/`WTCFS_` import or native EVLA generation, in-memory LRU of cells,
section 5.6), `MosaicPb` (per frequency and antenna-class pair, Lanczos
oversampling 10, re-phased per pointing). Each keeps the CASA-pinned
rounding and conjugation rules listed in section 4.3 in its own `key()`.

Kernel contract (both backends implement exactly this; integer grid and
fine-offset coordinates are rounded in the kernel once per sample from
(u, v, ν) with one rule shared by CPU and Metal):

```
adjoint:  table = mueller.table(w > 0, forward = false)
          for each (gpol, vpol) with table[gpol][vpol] = Some(m):
            tap' = (w > 0 ? t : conj(t))[m] · e^{i(ix·gx + iy·gy)}
            grid[plane, gpol][v0+iy, u0+ix] += V'_vpol · tap'                 (Data; V' = W·V·e^{iφ})
                                             += W_vpol · tap'                 (Psf: V = 1)
                                             += W_vpol · weight_tap' at uvw=0 (Weight)
            sumwt[plane, gpol] += W_vpol · |Σ (w > 0 ? t : conj(t))[m]|   (the norm, before the gradient;
                                  not in a residual pass, whose accumulator is `without_weight_sums`
                                  because nothing reads its sums: #659 deviation 9)
          Taylor: term t grid receives the same with W · s^t (data: t < N_t; psf: t < 2N_t−1)
forward:  table = mueller.table(prediction_w_positive, forward = true); tap' conjugated
          V_pred_vpol = e^{−iφ} · Σ_t s^t · Σ_{gpol,ix,iy} conj(tap'_t) · model[t][gpol][v0+iy, u0+ix]
                        ÷ N_vpol,   N_vpol = Σ_{gpol: table[gpol][vpol] = Some(m)} Σ_{ix,iy} (w > 0 ? t : conj(t))[m]
                        (R1: one division per visibility over every routed Mueller plane; the
                         norm omits the pointing gradient; N = 0 predicts 0 —
                         CASA `AWVisResampler::GridToData`, `faccumulateFromGrid`)
residual: V − V_pred, then the adjoint on the residual in the same dispatch
```

Adjoint law (R1): with `N` the per-visibility norm above, the pair is
exactly adjoint once each sample's data are divided by the conjugate norm,
`⟨A x, d⟩_W = ⟨x, A*(W d ⊘ conj N)⟩`; `MeasurementOperator::prediction_norm`
exposes `N` and `sumwt` accumulates `W·|N|`. The unnormalised law
`⟨A x, d⟩_W = ⟨x, A* W d⟩` is the special case `N ≡ 1` (unit-sum spheroidal
rows). The image-domain polarization conversion is CASA's `½·Sᴴ`
(`StokesImageUtil::From`), so a Stokes request from correlation planes
carries CASA's factor (2 for `[I, Q]` from `[XX, YY]`); the T0 tests pin both.

```rust
pub enum Mode { Data, Psf, Weight }
/// The modes one pass accumulates (`DATA`, `PSF`, `DATA_PSF`, `ALL`); sizes
/// the accumulator so a later major cycle never pays for PSF terms and the
/// planner knows what it charges (R1).
pub struct ModeSet { pub data: bool, pub psf: bool, pub weight: bool }
pub enum GridPrecision { F32, F64 }

/// Per-worker grid storage: `[plane][pol][term][y][x]`, x fastest, interleaved
/// complex, over a whole plane or a tile with halo. Owns `sumwt` as f64,
/// unless made `without_weight_sums` for a residual pass (#659 deviation 9).
/// The same layout is the Metal buffer, so a device copy is one memcpy.
pub struct GridAccumulator { /* private */ }
impl GridAccumulator {
    pub fn new(layout: AccumulatorLayout, precision: GridPrecision) -> Self;
    pub fn with_storage(layout: AccumulatorLayout, storage: GridStorage) -> Self;
    pub fn without_weight_sums(self) -> Self;
    pub fn merge_from(&mut self, other: &GridAccumulator) -> Result<(), OperatorError>;
    // and `sumwt`, `block`, `reset`, `layout`, `precision`, `storage`, `backend_parts`
}
/// `Device` cells live in shared device memory; device work left running on
/// them is recorded with the `unsafe` `DeviceCells::defer`, and the host's
/// next access waits for it (#659 deviations 1–2).
pub enum GridStorage { F32(Vec<Complex32>), F64(Vec<Complex64>), Device(DeviceCells) }

pub enum Work<'a> {
    Grid { mode: Mode, acc: &'a mut GridAccumulator },
    /// Reads only the block's placements; values and weights may be empty.
    Predict { model: &'a PreparedModelGrids, out: &'a mut [Complex32] },
    /// Predict, subtract, grid the residual in one dispatch. `residual_out`
    /// hands the residual samples back for the T0 laws; the pass passes
    /// `None`. The final pass's visibility writer takes the native-channel
    /// predictions instead (`VisibilitySink`).
    ResidualGrid { model: &'a PreparedModelGrids, acc: &'a mut GridAccumulator,
                   residual_out: Option<&'a mut [Complex32]> },
    /// Predict the block and interpolate it to native cells
    /// (`P_low + (P_high − P_low)·fraction` per `NativeTerm`), writing or with
    /// `add` adding, so several image domains' predictions sum before the
    /// residual is formed (CASA `SIMapperCollection::degrid`; #659 deviation 6).
    PredictNative { grids: &'a PreparedModelGrids, terms: &'a [NativeTerm],
                    model: NativeTarget<'a>, add: bool },
    /// Grid each placement's values less `scale · Σ factors · M[cells]` of its
    /// native endpoints (`SampleBlock::sources`) into the data terms.
    NativeResidualGrid { model: NativeModel<'a>, acc: &'a mut GridAccumulator },
}

pub trait GridBackend: Send {
    /// The work is complete when this returns, except that a device backend
    /// may leave grid-only work, and `PredictNative` into device cells,
    /// running; the host's next access to those cells waits for it. A Metal
    /// caller calls `MetalBackend::complete` (inherent: wait for every
    /// dispatch, report the first failure) before it trusts a grid.
    fn apply(&mut self, block: &SampleBlock<'_>, cf: &dyn ConvolutionFunctionSet,
             work: Work<'_>) -> Result<(), OperatorError>;
}
```

`CpuBackend` is support-generic over both tap layouts with a specialised
seven-tap separable path whose inner loop is one vector multiply-add per v
row over the interleaved, x-contiguous grid row (Obit `fast_grid7` shape on
NEON). `MetalBackend` (IF-4) implements the same trait with support-generic
kernels and `float2` dense kernel storage.

```rust
pub enum Basis { Constant, ChannelLocal { planes: u32 }, Taylor { terms: u32, reference_hz: f64 } }

pub struct MeasurementOperator {
    geometry: GridGeometry,           // padded grid, cell, image crop, FFT plan
    basis: Basis,
    polarization: PolarizationRouting, // correlation → Stokes/Mueller rows, both directions
    cf: Box<dyn ConvolutionFunctionSet>,
    precision: GridPrecision,
}
impl MeasurementOperator {
    pub fn new(geometry: GridGeometry, basis: Basis, polarization: PolarizationRouting,
               cf: Box<dyn ConvolutionFunctionSet>, precision: GridPrecision) -> Self;
    /// Planner input (R1): the layout of the pass a caller intends; admission
    /// charges `layout.bytes(precision)` before anything is allocated.
    pub fn accumulator_layout(&self, planes: PlaneRange, tile: Option<Tile>, modes: ModeSet) -> AccumulatorLayout;
    /// Allocates exactly `accumulator_layout(planes, tile, modes)`, zeroed.
    pub fn accumulator(&self, planes: PlaneRange, tile: Option<Tile>, modes: ModeSet) -> GridAccumulator;
    /// The kernel norm `N` of one placement for one visibility polarization (R1 law).
    pub fn prediction_norm(&self, placement: &Placement, vpol: usize) -> Complex64;
    /// model (× PB prescale when the product contract says flatnoise) ÷ correction → FFT
    pub fn prepare_model(&self, model: &ModelImages, prescale: ModelPrescale<'_>)
        -> Result<PreparedModelGrids, OperatorError>;
    /// FFT⁻¹, × correction, crop: unnormalised dirty/psf/weight planes + sumwt.
    /// The accumulator must cover the whole grid.
    pub fn finish(&self, acc: GridAccumulator) -> Result<NormalImages, OperatorError>;
}

pub struct NormalImages {
    pub first_plane: u32, pub pols: usize, pub data_terms: usize, pub psf_terms: usize,
    pub planes: Vec<NormalPlane>,  // one per output plane; Taylor terms inside;
    // indexing goes through `data(plane, term, pol)`, `psf`, `weight` and the
    // `*_sumwt` accessors (R1)
}
pub struct NormalPlane {
    pub data: Vec<Array2<f32>>,    // residual/dirty, `[term][pol]` (N_t terms)
    pub psf: Vec<Array2<f32>>,     // `[term][pol]`, 2N_t−1 terms
    pub weight: Vec<Array2<f32>>,  // mosaic/AW sensitivity, one per pol; empty otherwise
    pub sumwt: Vec<f64>,           // one per image, in the order data, psf, weight (R1);
                                   // zero from a residual pass's accumulator
}
```

`NormalImages` is what a pass finishes into. The normal state the minor
cycle and products read across a run (`FinalNormalState`: the unnormalised
residual, PSF moments, sensitivity and `sumwt` per plane, resident or paged,
with its `NormalBasis` of constant, channel-local or Taylor) is
deconvolution's, with the model lifecycle and `MajorCycle` (IF-11, #660
deviation 1).

Weighting:

```rust
pub enum WeightingGeneration {
    Natural { taper: Option<Taper> },
    /// `robust: None` is uniform; `bandwidth` is CASA `briggsbwtaper`
    /// (`BriggsCubeWeightor::uvDistanceFactor`) and needs `robust` (R1).
    Density { grid: DensityGrid, robust: Option<RobustFactors>,
              bandwidth: Option<BandwidthTaper>, taper: Option<Taper> },
}
impl WeightingGeneration {
    /// Pure per-sample function; CASA cell rules live in `DensityGrid::lookup`.
    pub fn imaging_weight(&self, p: &Placement, density_uv: DensityUv, input_weight: f32) -> f32;
}
// The density grid is built by one traversal that reads flags and weights
// only: `casa_imaging_runtime::pass::run_density_pass` (section 5.4).
```

The cube density rule bins and looks up each row at its rotated uvw, where
CASA's `BriggsCubeWeightor` bins at the rotated uvw and looks up at the
unrotated one; this is a recorded CASA bug (`docs/CASA (C++) bugs.md`, #659
deviation 12), pinned by `the_cube_density_bins_and_looks_up_the_rotated_uvw`.

Spectral resampling (CASA linear pairing with nearest weight and linear
flags) is one type, `SpectralResampler` (`direct(basis)` or
`channel_local(axis, kernel)`), producing `Placement`s and values from a
native row (`place`, `place_each`, `place_each_sourced` with native
endpoints, `place_density`) and predictions back at native channels
(`predict_row`, `native_terms`); cube density weighting consumes the same
function. CASA's visibility-domain continuum fit (`fitspw`,
`fit_and_subtract_continuum`) sits beside it (IF-11).

### 5.4 Execution (`casa-imaging-runtime`)

The landed signatures (IF-11). The pass items live under
`casa_imaging_runtime::pass`.

```rust
pub struct HostResources { pub threads: usize, pub performance_cores: usize,
                           pub available_memory: u64, pub metal: bool }   // detected once per process
pub enum ResourcePolicy { Interactive, Balanced, Exclusive, Explicit { workers: usize, memory: u64 } }
impl ResourcePolicy {
    pub fn workers(&self, host: &HostResources) -> usize;
    /// Compute width for minor cycles and products; streaming width for
    /// passes: the workers that fit beside the source's producer thread
    /// (`STREAM_PRODUCERS = 1`) within the core budget, the performance cores
    /// under `Balanced` and the workers otherwise (#659 deviation 8).
    pub fn cores(&self, host: &HostResources) -> CoreAllocation;
    pub fn memory(&self, host: &HostResources) -> u64;   // Explicit: min(memory, available)
}
pub struct CoreAllocation { pub workers: usize, pub streaming_workers: usize }
pub struct Demand { pub phase: &'static str, pub memory: u64 }
pub struct Reservation { /* RAII; releases on drop; `retain`, `split`, `join` */ }
pub fn admit(host: &HostResources, policy: &ResourcePolicy, demand: &Demand) -> Result<Reservation, Admission>;

pub struct Cancel(Arc<AtomicBool>);        // set by SIGINT handler in casars-imager
pub fn run_phase<T, E: From<Cancelled>>(name: impl Into<String>, cancel: &Cancel,
    diagnostics: Diagnostics, summary: &mut RunSummary,
    step: impl FnOnce() -> Result<T, E>) -> Result<T, E>;
pub struct Phase { pub name: String, pub completed: bool, pub seconds: f64,
                   pub process_peak_rss_bytes: u64, pub peak_reserved_bytes: u64,
                   pub reserved_bytes: u64 }

pub enum Partition {
    /// Each owner takes a disjoint range of planes; no merge.
    Planes { owners: usize },
    /// Each owner takes a strip of anchor rows plus the kernel's y halo; the
    /// tiles are added in region order.
    Regions { regions: Vec<Region>, halo: usize },
}
pub enum Residency { All, Waves { planes_per_wave: u32 } }
impl Residency {
    /// `All` when every plane's wave demand fits `budget`, else the largest
    /// wave that fits (at least one plane).
    pub fn plan(demand: &WaveDemand<'_>, budget: u64) -> Self;
}

/// What a traversal reads of each sample: values, flags and weights, or flags
/// and weights only (the density pass, as CASA's).
pub enum SampleContent { Visibilities, FlagsAndWeights }
/// A re-traversable source of native rows; a dependency boundary (rule 7).
pub trait BoundedSource: Send {
    fn begin(&mut self, planes: PlaneRange, restrict: bool, content: SampleContent) -> Result<(), SourceError>;
    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError>;
    fn maximum_block(&self) -> BlockShape;
}

pub struct PassDomain<'a> { pub operator: &'a MeasurementOperator,
                            pub resampler: &'a SpectralResampler, pub partition: Partition }
pub struct MajorCyclePass<'a> {
    pub domains: &'a [PassDomain<'a>],
    pub weighting: &'a WeightingGeneration,
    pub modes: ModeSet,
    pub model: Option<&'a ModelPreparation<'a>>,  // None: initial (dirty/psf/weight)
    pub residency: Residency,
    pub native_spacing_hz: f64,
    pub backend: BackendChoice,                   // Cpu | Metal
    pub metal_stage_timing: bool,
    pub weights: Option<&'a WeightCache>,         // kept imaging weights (#659 deviation 7)
}
/// Hands each domain's `NormalImages` to `images` per wave; a final pass
/// writes MODEL_DATA or CORRECTED_DATA through `visibilities`.
pub fn run_major_cycle(pass: &MajorCyclePass<'_>, source: &mut dyn BoundedSource,
    team: &WorkerTeam, cancel: &Cancel,
    images: &mut dyn FnMut(usize, NormalImages) -> Result<(), PassError>,
    visibilities: Option<&mut VisibilitySink<'_>>) -> Result<PassSummary, PassError>;
pub fn run_density_pass(operator: &MeasurementOperator, resampler: &SpectralResampler,
    shape: DensityGridShape, source: &mut dyn BoundedSource, team: &WorkerTeam,
    cancel: &Cancel) -> Result<DensityGrid, PassError>;
```

One pass serves initial, residual and final major cycles for every
capability: read block → select, flag, phase-shift, route correlations →
build `SampleBlock` → (predict and form V − A·m when a model is present, or,
for several domains or a linear cube, predict at native channels and grid the
native-channel residual) → `backend.apply` into the worker's accumulator →
merge → `operator.finish`. Cube waves are `Residency::Waves` with the source
restricted to the wave's native channel window. A channel-local run keeps
its two model generations, its PSF (and a mosaic's weight image) and two
residual epochs in the paged `CubeState`, a bounded cache over disk. A cube
runs
`Partition::Planes`; its precision is the run's (`gridprecision`, section
5.8), f64 on the CPU by default since D2's amendment, so the earlier "f32
cube fast path" is the user's `gridprecision = f32` or the GPU.

A run holds two worker teams (`WorkerTeams`: compute and streaming, one pool
when the widths agree). A multi-window selection is walked one
DATA_DESCRIPTION at a time, recorded by two scans of MAIN before the first
traversal and charged in the content plan, so the pass order is the walk,
not storage order (#659 deviation 10). The source fails a run on an unflagged
non-finite input (section 5.8). The kept-weight cache holds one f32 imaging
weight per selected row and native channel for a run with one image domain,
a direct (constant or Taylor) resampler and `Residency::All`, admitted when
it fits a sixteenth of the free memory; the first complete pass fills it.

### 5.5 Deconvolution (`casa-imaging-deconvolution`)

One plane's minor cycle, as landed (`solver.rs`, `driver.rs`,
`controller.rs`):

```rust
/// x-major planes; residual in Jy/beam; PSF term 0 peaks at 1. A Taylor view
/// holds N_t residual terms and 2·N_t − 1 PSF terms, every other view one.
pub struct MinorCycleView<'a> {
    pub shape: PlaneShape,
    pub residual: &'a [Vec<f64>],
    pub psf: &'a [Vec<f64>],
    pub summary: &'a PsfSummary,   // term 0: peak, beam fit, sidelobe, Clark patch
    pub support: &'a Support,      // mask ∧ valid support
}
#[non_exhaustive]
pub enum Candidate { Pixel { index: usize, scale: usize, strength: f64 } }
pub enum Next { Clean(Candidate), Stop }   // Stop: the solver's own rule ended the step

pub trait Solver {
    type State;
    fn extra_iterations(&self) -> usize { 0 }  // 1 for CASA's inclusive Högbom loop
    fn charges_stop(&self) -> bool { false }   // MatrixCleaner charges the stopping iteration
    fn initialize(&self, view: &MinorCycleView<'_>, residual: &[Vec<f64>], threshold: f64)
        -> Result<Self::State, Error>;
    fn next(&self, state: &mut Self::State, view: &MinorCycleView<'_>, residual: &mut [Vec<f64>])
        -> Result<Next, Error>;
    /// Flux per model term, term 0 first; the default is gain × strength.
    fn component_fluxes(&self, state: &Self::State, candidate: Candidate, gain: f64,
                        fluxes: &mut Vec<f64>);
    fn accept(&self, state: &mut Self::State, view: &MinorCycleView<'_>,
              residual: &mut [Vec<f64>], candidate: Candidate, gain: f64, delta: &mut Delta)
        -> Result<(), Error>;
    fn finalize(&self, state: Self::State, view: &MinorCycleView<'_>,
                residual: &mut [Vec<f64>], delta: &Delta) -> Result<StepEnd, Error>;
    /// Heap one step's state holds at most, admitted before the cycle runs.
    fn working_bytes(&self, shape: PlaneShape, terms: usize) -> u64;
    /// Pixels one component adds flux to, per term: it bounds a `Delta`.
    fn component_cells(&self, shape: PlaneShape) -> usize;
}
pub struct StepEnd { pub peak: f64, pub refreshes: usize }
pub struct Delta { /* per term: storage index → flux */ }

pub struct CycleControls { pub iterations: usize, pub threshold: f64,
                           pub threshold_reached: bool, pub gain: f64, pub nsigma: f64 }
pub fn run_plane<S: Solver>(solver: &S, view: &MinorCycleView<'_>, cycle: &CycleControls,
                            statistics: &PlaneStatistics, trace: usize)
    -> Result<PlaneOutcome, Error>;
pub fn solve_bytes<S: Solver>(solver: &S, shape: PlaneShape, terms: usize,
                              components: usize, trace: usize) -> u64;
```

The runtime (`casa-imaging-runtime`, `minor.rs`) runs every plane of a major
cycle's completion on the worker team:

```rust
pub fn prepare_minor_cycle(completion: &MajorCycleCompletion,
                           mask_plans: &ImageDomainReconstructionMaskPlans,
                           setup: &MinorCycleSetup, cache: &mut PsfCache, team: &WorkerTeam)
    -> Result<PreparedMinorCycle, MinorCycleRunError>;
pub fn run_minor_cycle(prepared: PreparedMinorCycle, completion: &MajorCycleCompletion,
                       setup: &MinorCycleSetup, controls: &CycleControls, cache: &mut PsfCache,
                       team: &WorkerTeam, (host, policy): (&HostResources, &ResourcePolicy))
    -> Result<MinorCycleOutcome, MinorCycleRunError>;
```

Solvers: `Hogbom` (with CASA's inclusive loop), `Clark` (active list, exact
refresh by `LinearRefresh`, FFT or direct sparse sums, whichever costs less),
`Multiscale` and `Taylor` (the crate-private `ScaleBank`; Taylor with N_t
terms and the per-scale Hessian). Planes are independent: `run_minor_cycle`
runs several at once, one thread each, under the same `CycleControls`, and
merges their terms in model order, so image domains (outlier fields, facets)
share one cycle threshold. A single plane spreads Clark's sparse refresh
across the team instead. Each plane's working bytes (`solve_bytes`) and
result terms are admitted before they are formed.

Control is in two places, as in CASA. The run-wide `Controller`
(`SIIterBot`) derives each cycle's `CycleControls`: the PSF-sidelobe cycle
threshold, never below the global threshold, `cycleniter` (CASA's automatic
value, #341) and `nsigma`. It stops the run with CASA's `stopcode`
(`CleanStop`: `niter`, threshold within 1%, no change, the two divergence
rules, empty mask, n-sigma, `nmajor`). The per-plane `PlaneControl`
(`SIMinorCycleController`) stops a plane with the minor-cycle codes 0–6
(`PlaneStop`), including the 10% divergence rule.

Future, not landed: an ASP solver would add a Gaussian `Candidate` (amplitude,
sub-pixel centre, width) and re-fit its active set in `accept`; `Candidate`
is `#[non_exhaustive]` for it.

### 5.6 AW and mosaic convolution functions

`AwCatalog::open_casa(path)` reads only the CF image headers at open
(support, sampling, W, frequency, Mueller and PA lists; LibRA's lazy-fill
`CFCache`) and reads cell pixels on demand into an in-memory LRU bounded by
the reservation. `AwCatalog::generate_native(...)` (the existing native EVLA
generation) writes CASA-format CF images into `<cache>/cfcache/` so the same
loader serves both. No manifest, no payload hash, no eviction ledger. The
index rules are pinned to LibRA/CASA: `w_index = clamp(round(√(wIncr·|w|)),
0, nW−1)` with `wVal[i] = i²/dW`; frequency cell = nearest listed value,
precomputed per (spw, channel) with the conjugate map at √(2f0²−f²); PA cell
by `|PA − PA_cf| ≤ dPA`; each cell normalised by its un-oversampled area
before use. For AW the bounded source iterates spectral-window-major so one
CF group is resident at a time; as landed, every multi-window selection is
walked one DATA_DESCRIPTION at a time (#659 deviation 10), which is what
stops #625's CF cache from thrashing. `MosaicPb` builds projectors per (frequency,
antenna-class pair) from the PB model and caches re-phased kernels per
pointing pair.

### 5.7 Products and normalisation (`casa-imaging-products`)

| Quantity | Rule (CASA) |
|---|---|
| sumwt | Σ W per plane/pol; AW: Σ W·|Σ taps|; MT-MFS uses term-0 sumwt for all terms |
| dirty/residual (standard) | FFT⁻¹ grid ÷ correction × N_x N_y ÷ sumwt |
| PSF | each plane ÷ max of term-0 PSF; peak = 1 |
| weight image (mosaic/AW) | gridded FT[PB²] at uvw = 0, FFT, Stokes convert, ÷ sumwt |
| PB | √\|weight\| ÷ pbmax, zero where PB ≤ pblimit |
| residual, flatnoise (default) | ÷ (pbmax·√\|weight\|) where that exceeds pblimit·pbmax², else 0 |
| residual, flatsky, one term | ÷ weight where it exceeds pblimit²·pbmax², else 0 |
| residual, flatsky, Taylor family | ÷ weight where it exceeds \|pblimit\|·pbmax², else 0 |
| model prescale before degrid (flatnoise) | × PB before prediction; ÷ PB after the solve |
| pbcor | restored ÷ PB where PB > pblimit |
| MT-MFS | each term ÷ term-0 sumwt and the same weight image; alpha and error products from terms |

These are `SIImageStore::makePBFromWeight` and `divideResidualByWeight`, as
`casa-imaging-products/src/normalization.rs` implements them. For one term
both residual cutoffs are PB > pblimit where the weight is positive. At
negative weights, which FFT ringing leaves at a mosaic's edges, flat noise
keeps CASA's absolute value and flat sky has no support.
`SIImageStoreMultiTerm::divideResidualByWeight` keeps the flat-noise cutoff
for flat sky, so a Taylor family's flat-sky residual is blanked where
PB ≤ √pblimit. For example, with pbmax = 1, pblimit = 0.2 and a weight of
0.1, one term keeps the sample (0.1 > 0.04) and a Taylor family blanks it
(0.1 ≤ 0.2).

The product list (image, residual, psf, sumwt, model, pb, mask, weight,
alpha, alpha.error, pbcor) replaces the product graph topology. The CASA
writer stages each image beside its target and promotes it with the existing
atomic rename/swap and parent fsync. Standard-gridder PB product and pixel
masks (#217) are rows in this table.

Facets (#664) stitch each facet's residual divided by its own `sumwt` and
place a reference facet's PSF at the image centre, as CASA's
`setPsfFromOneFacet` does. The reference is facet 0 unless it took no
samples; then it is the lowest populated facet, where CASA would copy and
normalise a zero PSF (#659 deviation 17).

### 5.8 Request and compile (`casa-imaging-model`, `casa-imaging-application`)

`ImagingRequest` is one serde struct whose field names, choices and defaults
are the provider-contracts catalog entries; `casars-imager` fills it from the
resolved catalog values and nothing else. New catalog entries introduced by
this plan: `gridprecision = auto | f32 | f64` (D2, semantic revision 2),
`backend = auto | cpu | gpu` (IF-4 as `cpu | metal`; `auto`, the default,
and the spelling `gpu` since #723, #659 deviation 14), and the `diagnostics`
group (section 8.1). Each is added to the catalog in the ticket that lands
the behaviour, with the Python wrappers and parameter reference regenerated
in the same PR. `validate()` runs once. `compile()` returns `CompiledProblem`
with no identity hashes; the AW cache reuses a cell because its file exists.

```rust
pub enum BackendRequest { Auto, Cpu, Gpu }   // request field `backend`, default Auto
/// `Cpu` and `Gpu` as asked; `Auto` is Metal iff a T1 Metal law covers the
/// problem (one image domain, facets allowed, any basis; Stokes I; no
/// visibility write) and `check(problem, Metal, precision, host)` admits it
/// (a Metal device, `gridprecision` not f64, the standard kernel set).
pub fn resolve_backend(problem: &CompiledProblem, requested: BackendRequest,
    precision: Option<GridPrecision>, host: &HostResources) -> BackendChoice;
pub fn check(problem: &CompiledProblem, backend: BackendChoice,
    precision: Option<GridPrecision>, host: &HostResources) -> Result<(), ImplementationUnavailable>;
```

`availability::check` is the one gate for capabilities that are not
installed: polarized or multi-domain Taylor bases, a non-constant basis or a
wide-field kernel set with facets, AW cubes (#719), and on Metal a host
without a device, f64 grids and the W, mosaic and AW kernel sets (#653).
Capabilities that are rejected are not in the catalog. `gridprecision =
auto` resolves beside the backend: f64 on the CPU and f32 on Metal (D2 as
amended). An image size the facet count does not divide fails validation,
where CASA drops the remainder strips (#659 deviation 15).

Inputs must be finite where they are used (#659 deviation 18, owner
2026-10-16): under `FiniteValuePolicy::RejectUnflaggedInput`, a non-finite
visibility or weight in an unflagged sample, or non-finite UVW in an imaged
row, fails the first pass that reads it, naming the MAIN row, channel and
correlation and asking the user to flag it; one in a flagged sample is
ignored. CASA images an unflagged NaN as NaN everywhere, without a message.

### 5.9 Techniques adopted from Obit and LibRA/HPG

Surveyed 2026-10-07 from the local Obit checkout (`ebc1c229`, GPL-2+, used as
design reference only) and LibRA `0ab99e26` with vendored HPG 3.3.0. The
section 5.3 signatures already reflect the "adopt now" rows.

| Technique | Source | Where it lands |
|---|---|---|
| Mueller routing tables (direct and conjugate) selected by sign(w) and direction in the kernel; no per-sample Mueller index | HPG `gridding.hpp:605-680`, `hpg.hpp:1158` | `MuellerRouting`, kernel contract (IF-1) |
| Two-level CF key: variable-support `group`, fixed `cube`; Mueller planes inside the cell | HPG `indexing.hpp:82-190` | `CfKey` (IF-1), `AwCatalog` (IF-3) |
| Dense CF layout `[oy][ox][mueller][iy][ix]`, two padding cells per edge so the fine-offset sign selects the major index without a branch | HPG `impl.hpp:231-262`, `gridding.hpp:331-355` | `TapLayout::Dense` (IF-1) |
| Separable kernel tabulated oversampling-major so one fractional offset is one contiguous row; seven-tap row as one vector multiply-add over an interleaved, x-contiguous grid row | Obit `ObitUVGrid.c:ConvFunc 1247-1438`, `fast_grid7 1902-1989` | `TapLayout::SeparableReal`, `CpuBackend` (IF-1) |
| Samples whose support leaves the padded grid are dropped at placement, so kernels have no bounds tests; zero-weight samples never reach the tap loop | Obit `fast_prep_grid 1296-1348`; HPG `as_weight(flag)` | `Placement` construction (IF-1) |
| Values pre-multiplied by phasor and weight once per sample; one fused predict → residual → grid dispatch per block | HPG `gridding.hpp:806-866` | `SampleBlock`, `Work::ResidualGrid` (IF-1, IF-4) |
| sumwt accumulated in the tap loop as `W·|Σ taps used|`; degrid divides by the unconjugated sum of the taps used | HPG `gridding.hpp:590-680`; CASA `AWVisResampler.cc:404,558` | kernel contract (IF-1) |
| Accumulator layout `[plane][pol][term][y][x]`, x fastest, shared by CPU and device so a device copy is one memcpy | HPG LayoutLeft grid | `GridAccumulator` (IF-1), `MetalBackend` (IF-4) |
| f64 grid with f32 CF and visibilities as the default; f32 grid as the production precision when memory demands it | HPG `core.hpp:55-70`; Obit f32 grids with f64 phase | D2, `GridPrecision` |
| Per-plane FFTs single-threaded on the worker team; FFTW threads never nested inside worker parallelism | Obit `ObitUVGridFFT2ImPar 852-1047` | `MajorCyclePass` finish rule (IF-2); FFTW threads only for the single-plane case, measured in IF-10 |
| Memory admission as one arithmetic formula over visibility buffer, grids and images | Obit `ObitUVImagerGetNumPar 863-921` | `admit()` (D5, IF-6) |
| Metadata-first CF cache: read headers at open, cell pixels on demand; pinned index rules for w, frequency (with conjugate map), PA and area normalisation | LibRA `CFCache.cc:231-560`, `CFBuffer.h:253-262`, `CFBuffer.cc:520-580`, `AWConvFunc.cc:463,546-553` | `AwCatalog` (IF-3), section 5.6 |
| Spectral-window-major source order for AW so one CF group is resident at a time | LibRA `DataBase.h:165-192` | `BoundedSource` ordering (IF-3) |
| Device stream rotation: upload of block k+1 overlaps gridding of block k; per-stream visibility buffers | HPG `runtime.hpp:333-575, 2249-2262` | `MetalBackend` buffer ring (IF-4): six slots, each `apply` cut into three sub-blocks, grid-only work left running on `DeviceCells` (IF-10, #659 deviations 1–3) |
| Weight image = visibility 1 at uvw 0 with the FT[PB²] set; PSF = visibility 1 at true uvw; same kernel, three accumulators in one pass | LibRA roadrunner modes, `HPGVisBuffer.inc:160-220` | `Mode::{Data,Psf,Weight}` (already planned) |
| `Candidate` as an enum with a Gaussian variant and `accept` allowed to revise earlier components, so an ASP solver can land without changing the trait | LibRA `AspMatrixCleaner.cc` | `Solver` (IF-5) |
| sumwt image carries `useweightimage`/`imagingmode` miscinfo for CASA-tool compatibility | LibRA `roadrunner.cc:878-918` | products (IF-8) |
| Component merging at equal positions before prediction (already the behaviour of `ModelDelta`) | Obit `ObitTableCCUtilMergeSel` | kept |

Performance-pass candidates (IF-10), each gated by a T0 law or a measurement
before adoption. Outcome (#648 results): FFTW threads for a single plane's
Clark refresh measured +7.6% cycles and were rejected; a tap-plan, record or
placement cache across majors was rejected by design (D1, R3); the others
were not taken up and stay open. IF-10 adopted instead the 27 measured
changes the #648 results table lists, among them baseline-grouped blocks,
deferred Metal completion, the device's native-channel residual, kept
imaging weights, the core allocation, the DATA_DESCRIPTION walk and the
flags-and-weights density pass.

- Hermitian half-plane gridding with guard columns and conjugate folding
  (Obit `ObitUVGridSetup`, `ThreadFlip`, `ThreadMerge`): halves grid memory
  and FFT work for Hermitian plane sets only; needs a T0 law (half-plane =
  full-plane to 1e-6) and a rule that cross-hand correlation grids are
  excluded. R1: `GridAccumulator` neither reserves nor excludes it; all
  addressing goes through `AccumulatorLayout::block_offset` and `Tile`, so
  the layout is a `Tile` variant plus a conjugate fold in `finish`, with no
  change to `GridBackend::apply`.
- Replica-merge partition (planes × sample chunks with private grids and an
  O(grid) merge per replica; Obit `ObitThreadGridSetupMF`): a third
  decomposition between `Planes` and `Regions`, measured against `Regions`
  for single-plane MFS.
- Shared-grid atomics (HPG) versus region or lane ownership on Metal for
  wide-support W/AW kernels.
- Vectorised sin/cos for the phase rotation (Obit `ObitSinCosVec`, Pommier
  polynomial) only if profiling shows the phasor in the top costs.
- CF prefetch of the next spectral window on a helper thread (LibRA
  `CFServer`).
- Bounded tap-plan cache as a `MajorCyclePass` strategy (D1 follow-up).
- Parallel active-list subtraction in Clark (Obit `ThreadCLEAN`: split the
  pixel list across workers, each subtracts and finds a local peak, reduce).
- FFTW threading for single-plane 4096² MFS.

Rejected for this plan, with reasons: Obit faceting as the W-term method
(changes the product contract; VLASS parity uses AW); beam-peak
normalisation (differs from CASA sumwt); DFT component prediction (loses to
gridded prediction at 10⁷–10⁸ samples); the common-resolution frequency
taper and joint subband CLEAN (not CASA MT-MFS semantics); Obit autoWindow
(cheaper but weaker than CASA automask, and parity needs automask); Obit's
"middle loop" of patch-only residual updates between major cycles (our Clark
exact refresh already covers it); LibRA's disk-image process split per stage
(full-image round trips per major cycle); the persistent in-place residual
visibility file (pays only with DFT prediction of few components).

## 6. Deletion list

Line counts are production lines from the deep dives, rounded. Each ticket in
section 9 names which rows it owns. "Replaced by" names the section 5 type.

| Delete | Lines | Replaced by |
|---|---|---|
| `reconstruction/src/streaming_cube/band.rs`, `spatial.rs`, `residual_device.rs`, `completion.rs`, `preparation.rs`, runtime `streaming_cube/{phase,plan,execute,input,prepare,bulk_phase,bulk_wave,metal_wave,metal_plan}.rs` | ~9,000 (+2,650 cfg(test)) | `MajorCyclePass` + `Partition::Planes` + `Residency::Waves` |
| `SpectralSlabOperator` and the slab half of `spectral_operator.rs` (`:7542-10871`), `initial_planes*`, `OperatorTaps`/`GridOperatorTaps`/`grid_operator`/`degrid_operator`, `ConvolutionOperator` enum, `StandardConvolution::{grid,grid_rows,grid_float,degrid,degrid_float}` | ~6,500 | `CpuBackend`, `Spheroidal`, `WPlanes`, `MeasurementOperator` |
| `gridded_normal_operator.rs` + `gridded_normal_operator/*`, runtime `complete_data_operator.rs` + `complete_data_operator/*`, `complete_data_parallel_mfs_tests.rs`, replay half of `managed_spill.rs` | ~20,000 | re-traversal through `MajorCyclePass` (D1) |
| `aw_projection.rs` grid/degrid loops and lease provider, `aw_generation/paired.rs` duplication, `mosaic.rs` grid/degrid loops | ~3,000 | `AwCatalog`, `MosaicPb` + `CpuBackend` |
| application `aw_cache.rs`, `prepared_aw_phase.rs`, runtime `prepared_artifact.rs` + `prepared_artifact/*`, model `prepared_artifact.rs` identity layers | ~15,000 | `AwCatalog` (D3) |
| `metal_cube.rs` MSL `normal_*` and `cube_residual_connected`, `metal_wave.rs`, `metal_normal.rs`, three ticket rings and two layout schemes | ~2,500 | `MetalBackend` with one dispatch path |
| `weighting.rs` (reconstruction 3,499 and runtime 5,133), `weighting/bulk_source.rs`, `serial_compute_probe.rs`; keep the density cell rules and robust factor (~500) | ~8,000 | `WeightingGeneration`, `build_density_grid` |
| five CASA-linear resamplers, four Stokes-I reducers, seven transport structs | ~1,500 | one resampler, one router, `Placement`/`SampleBlock` |
| `minor_cycle.rs` duplicates: `run_image_domain_hogbom_controllers`, `run_joint_block_minor_cycle`, `select_joint/taylor_candidate`, `principal_taylor_peak`, `*_kernel_fits`, `refresh_circular_residual`, `refresh_taylor_residuals`, `subtract_psf_circular`, `subtract_psf_patch`, the four threshold copies, four validation copies, three evidence blocks; `psf_beam.rs` duplicate peaks; `restore.rs:24` | ~1,800 | driver, `Solver`, primitives |
| `receipt.rs`, `cost_model.rs`, `execution.rs` DAG/claims/slots/validators, `execution_bindings.rs`, `resource_authority.rs` topology/rates/queues/locks/certificates, `observation_transaction.rs`, `publication_layout.rs`, `product_publication.rs`, `serial_product_publication.rs`, `spectral_cycle_plan.rs` candidate search, `spectral_cycle.rs`, `paged_cube_state.rs`, bounded-stream measurements and digests, managed-spill measurements and retention identity, `reload_probe.rs`; tests `execution/tests.rs`, `resource_authority/tests.rs`, `compile_plan_run.rs` receipt and admission portions | ~40,000 (+30,000 tests) | section 5.4 core (D5) |
| `task_contract.rs` mirror types, `From` impls, `default_*` fns, `from/to_cli_config`, `IMAGER_PROJECTED_PARAMETERS`, progress schema, round-trip tests; `CliConfig` and test-only `parse`; `ContinuumImagingRequest`, `ApplicationRequest`; `ManagedImagingOutput::from_run` | ~4,000 | `ImagingRequest` (D8) |
| 21 dead/always-rejected `TaskRequirement`s and 12 catalog parameters; `JointContinuumLine` end to end; four unreachable `UnsupportedRequirement` constraints; `PreviewPng` | ~1,300 | nothing |
| identity encoders (`compiled_problem.rs:3153-3825`, `geometry.rs:1721-1942`, `product_graph.rs:1176-1440`, `observation.rs:2865-3191`, `model_state.rs:1272-1600`), `validate_compiled_problem_identity`, selected-row sequence hashing and per-sample re-inspection, unused selection predicate families, duplicate row structs | ~2,500 | plain comparison; count + ascending-order check |
| product graph `dependencies`/`schema`/`IndependentProductStoreProtocol`/`ProductMemberContract`, `demand.rs` residency re-checks | ~400 | product list (5.7) |
| `scripts/check-imaging-architecture.py`, `test-imaging-architecture-structural.py`, `resources/imaging-architecture/{migration-matrix.json,baselines,representative-science-evidence}`, `scripts/test-imaging-parity.sh` | ~3,800 + data | `scripts/check-imaging-dependencies.py` |
| 151 non-production `CASA_RS_*` names, `eprintln!` sites, module-level `#[allow(dead_code)]`, diagnostic probe modules | ~1,000 | `tracing` via `casa-logging`; `diagnostics` request field |

Target after conversion: imaging non-test source under 70k lines (from about
200k); stretch 50k. IF-11 records the final count.

## 7. Test strategy and tiers

Build profile: add `[profile.dev.package."casa-imaging-*"] opt-level = 3`
(debug assertions kept) so T1 runs at release speed under `cargo test`.

**T0, laws (each under one minute; every ticket; CI).** In-process synthetic
MeasurementSets at 64–128 px with a few hundred rows via
`generate_synthetic_observation_ms`. For every `ConvolutionFunctionSet` and
both backends:

- adjoint dot-product test ⟨A x, y⟩ = ⟨x, A* y⟩ to 1e-6 relative (f64) or
  1e-4 (f32/Metal);
- PSF peak = 1 after normalisation and PSF Hermitian symmetry;
- point-source dirty image = flux × shifted PSF within 1e-3;
- sumwt = Σ W (standard, mosaic) and the AW rule;
- worker-count invariance within tolerance (bitwise for CPU f64
  `Partition::Planes`; 1e-6 for f64 `Regions`; 1e-4 for CPU f32 and Metal);
- f32 versus f64 CPU grids agree within 1e-4 normalised on every CF set;
- mosaic: weight image equals Σ_k PB_k² at the pointing centres; W: zero
  |w| reduces to the standard kernel; AW: cold and warm catalogs give identical
  keys and taps;
- deconvolution: each solver recovers a known two-component model from a
  synthetic residual/PSF to the cycle threshold; controller stop codes;
- weighting: uniform density equals CASA cell rules on a hand-built grid.

**T1, end to end per capability (under ten minutes each; CI subset).**
256–512 px, 1e5–1e6 samples, through `casars-imager`'s production route, with
analytic point plus Gaussian skies: standard MFS (Hogbom, Clark, multiscale,
box mask, automask, model-column write), 16-channel cube (natural, Briggs),
mosaic (three overlapping fields), full Stokes (four correlations),
W-projection (low declination, long baselines), MT-MFS (multi-SPW synthetic,
two terms), Metal variants on macOS. Checks: recovered flux, position and beam
within analytic tolerance, residual RMS bound, complete product inventory and
WCS, worker invariance, bounded memory. R3 amended the memory check. Owned
memory must fit its admission: every live phase is admitted, and the
process's peak memory (macOS footprint or Linux high-water RSS, in bytes) is
at most the largest total reservation plus a separately measured allowance for
fixed process and allocator overhead. A separate check that admission does not
grossly overestimate may stay. The earlier "planned ≤ observed peak RSS × 1.1"
did not bound underestimation.

**T1.5, CASA-paired small fixtures (local only; run before a ticket's
review).** Copies of casatestdata `refim_point`, `refim_point_withline`,
`refim_twochan`, `refim_alma_mosaic`, `refim_mawproject`,
`vla_wideband_2ptg_w_squint`, `polcal_LINEAR_BASIS`, `ngc5921` with CASA
references generated once into `/Volumes/GLENDENNING/casa-rs-evidence/if/`;
plus the intermediate-90 four-SPW pilot (saved CASA reference, casa-rs W4
about 16 s) as the quick timing regression. Comparison: existing
`image_compare.py` with NRMS ≤ 1e-3 on every product, beam ≤ 1e-3, exact
inventory and WCS.

**T2, representative (hours; IF-10 only).** The nineteen matrix scenarios
(oracles regenerated), the intermediate-90 16-SPW and deeper variants, the
C-array 512-channel cube (CASA 6.0 h), Wave3 4096² MFS (CASA 7,268 s), the
VLASS fragment (#625 serial bar 9,073 s), full-360 once finalised. As run,
the T2 harness (`scripts/bench-imager-vs-casa.sh`) gives CASA
`conjbeams=False` for mosaic rows, since casa-rs mosaic has no conjugate
beams (#720), and tclean's default `True` for AW (#721; #659 deviation 16).
Wave3's CASA time of record became 7,944.7 s on the regenerated MS (#648
results, owner decision 3).

Tests that assert implementation details (batch sizes, receipt counts,
internal ordering, measurement projections) are deleted with the code they
describe. The three stale T55 tests are fixed in IF-0 by tightening their
fixture budgets, and are then deleted in IF-7 with the admission machinery
they exercise, once the IF-7 admission test covers the same guarantee
(explicit ceiling rejects a run; one-frame window forces batch 1).

## 8. Diagnostics, logging and configuration

### 8.1 Retained switches (as `ImagingRequest.diagnostics` fields)

| Field | Replaces |
|---|---|
| `stage_timing: bool` | `CASA_RS_TRACE_IMAGING_STAGE_TIMING` (14 sites), `CASA_RS_PROFILE_CUBE`, `CASA_RS_PROFILE_PRODUCTS`, `CASA_RS_TRACE_CLARK_TIMING`, `CASA_RS_TRACE_AW_REPLAY_TIMING`, `CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES` |
| `metal_stage_timing: bool` | `CASA_RS_PROFILE_METAL_STAGES` |
| `science_trace: bool` | `CASA_RS_TRACE_IMAGING_SCIENCE`, `CASA_RS_IMAGING_SCIENCE_PROBE`, `CASA_RS_TRACE_AW_*` |

All emission is `tracing` events at `info`/`debug` through `casa-logging`.
Stage-timing consumers under `tools/perf/imager/` are updated to parse the
structured log lines (the T51 pair driver was deleted in IF-0).

### 8.2 Removed

`CASA_RS_IMAGING_SPILL_READ/WRITE_BYTES_PER_SECOND` (required today),
`CASA_RS_T51_RECEIPT_BOUNDARY_PROBE`, `CASA_RS_APPLICATION_ISOLATED_CASE`
(subprocess re-exec in tests), and the roughly 150 test- and probe-only
names. `eprintln!` in library crates is forbidden by the checker.

### 8.3 Run summary

`RunSummary` is a small JSON written beside the products: request echo,
phases with wall time and peak RSS, worker count, backend, minor-cycle
totals, product list. It is the only runtime record. If
`intermediate_profile_evidence.py` is still wanted, it reads this.

## 9. Ticket set and review gates

### 9.1 Conventions

- Issues are titled `[Imaging foundation] IF-n: <outcome>` under a new
  umbrella issue `[Imaging foundation] IF: one measurement operator, one
  pass, one driver`. Each issue body carries: outcome, in-scope files,
  deletion rows from section 6, acceptance tests, non-goals, and a
  `## Deviations` section the implementer fills when the plan and the code
  disagree. Pull requests target `main` and carry `Work issue: #N`.
- Each PR passes: `cargo fmt --check`, `cargo clippy --all-targets -D
  warnings` on touched crates, the checker (D6), T0 for touched operators,
  T1 for touched capabilities, and `just quick` at ticket end. T1.5 runs
  before review for science-bearing tickets.
- Review per ticket: one independent review by a separate agent or person
  (so far OpenAI Astra, posted under the owner's login) against the issue's
  outcome, deletion rows, section 10 and the CASA source. Preferences cannot
  block; a cited rule or a failing acceptance test can. Once the gates are
  green, the review's findings are resolved and no deviation is open for the
  owner, the implementer marks the PR ready and arms auto-merge without
  asking (AGENTS.md, Merging); the owner is consulted only for section 3
  decisions, external contracts and the Ask First list.
- Intermediate breakage is accepted and stated: between IF-2 and IF-4 Metal
  is unavailable; between IF-2 and IF-3 W, AW and mosaic are unavailable.
  `availability::check` rejects them with a typed reason during that window.

### 9.2 Tickets

**IF-0 Foundation and process (Fable).** Approve this plan; add ADR-0016
(imaging foundation; supersedes ADR-0010 and the attestation wording of
ADR-0009; records D1–D10). Retire the checker and matrix (D6) and land
`check-imaging-dependencies.py` with a trimmed `dependency-policy.json`.
Create the three crate skeletons (D4). Fix the three stale T55 tests. Delete
dead code: cfg(test) legacy cube engine, `JointContinuumLine`, dead
`TaskRequirement`s and catalog parameters, `cost_model.rs`, `reload_probe.rs`,
diagnostic probe modules, `test-imaging-parity.sh`. Extend
`generate_synthetic_observation_ms` to several spectral windows; add the T1
harness (`crates/casa-imaging-application/tests/t1/`) and the dev-profile
opt-level. Add `tracing` to the imaging crates and the checker rule against
`std::env` and `eprintln!` (enforced from IF-1 on new code; old sites go with
their files). Close the #486 family (D7) and open IF-1 … IF-11. Acceptance:
`just quick` green; dependency checker green; test and line counts recorded in
the umbrella issue.

**IF-1 Operator core (Fable).** `casa-imaging-operator`: `Placement`,
`SampleBlock`, `CfKey`, `TapLayout`, `ConvolutionFunctionSet`, `Spheroidal`,
`GridBackend`, `CpuBackend` (support-generic plus the specialised seven-tap
path, both precisions, Taylor terms, Data/Psf/Weight modes),
`GridAccumulator`, `MuellerRouting`, `Work`, `MeasurementOperator`
(`accumulator`, `prepare_model`, `finish`), `ImageCorrection`, FFT centring
(one implementation), `PolarizationRouting`, the single spectral resampler,
`WeightingGeneration` and `build_density_grid` (moved cell rules). The
section 5.9 "adopt now" rows for IF-1 are part of the contract. T0 laws for
`Spheroidal` on `CpuBackend`, both precisions. No production caller yet; this
is the pattern ticket.
Deletions: none (IF-2 deletes the drivers). Acceptance: T0 green; rustdoc on
every public item states the contract; file sizes under 1,500 lines.

**IF-2 Major-cycle pass and migration of standard routes (Opus).**
`casa-imaging-runtime`: `WorkerTeam` + bounded stream trimmed, `Partition`,
`Residency`, `MajorCyclePass`, `run_major_cycle`, model-column write in the
final pass, cube state paging on one backend (D10). Migrate standard MFS,
MT-MFS (Taylor basis), channel-local cube (all weightings, waves), dirty-only
and model-column runs onto the pass. Delete the four CPU drivers, the replay
subsystem (D1), `metal_normal.rs` and the runtime cube engine (section 6 rows
1–3, 7, 8 partially). Acceptance: T0; T1 standard MFS (all deconvolvers),
cube, MT-MFS, model-column; T1.5 `refim_point`, `refim_twochan`,
intermediate-90 pilot within 1e-3 of the saved CASA reference; pilot W4 time
recorded (no bar yet).

**IF-3 W, mosaic and AW on the shared operator (Opus).** `WPlanes`,
`MosaicPb` (with `weight_taps` and phase gradients), `AwCatalog` (D3, section
5.6, native EVLA generation writing CASA-format CF images). Delete
`aw_projection.rs` loops, `mosaic.rs` loops, `aw_cache.rs`,
`prepared_aw_phase.rs`, `prepared_artifact*` (section 6 rows 4, 5).
Acceptance: T0 laws per CF set including the pairing pitfalls (w-sign
conjugation, ramp conjugation, Mueller swap, conjugate-beam frequency); T1
mosaic, W, AW synthetic; T1.5 `refim_alma_mosaic`, `refim_mawproject`,
`vla_wideband_2ptg_w_squint`, `refim_point_withline` within 1e-3; cold and
warm AW catalogs give identical products.

**IF-4 Metal backend (Opus).** `casa-imaging-metal`: `MetalBackend`
implementing `GridBackend::apply` for both tap layouts and all three `Work`
variants, support-generic kernels, one dispatch path (validate, map, commit,
pending), a per-stream buffer ring with upload of block k+1 overlapping
gridding of block k, status bits for errors, grids in the shared
`GridAccumulator` layout. `BackendChoice::Metal` selectable from the request on macOS
only. Delete the two Metal drivers and dead MSL kernels (section 6 row 6).
Acceptance: T0 Metal versus CPU within 1e-4 for every CF set; T1 cube and MFS
on Metal; T1.5 pilot Metal W4 time recorded.

**IF-5 Deconvolution (Opus).** `casa-imaging-deconvolution` per section 5.5,
including the four controller inconsistencies fixed (prepass support filter,
inclusive threshold, 1% check for all solvers, one divergence rule) and CASA
auto `cycleniter` (#341). Sparse refresh on the worker team. Delete section 6
row 9. Acceptance: T0 solver recovery and controller stop codes; T1 all
deconvolvers; T1.5 `refim_point` Hogbom/Clark/multiscale and MT-MFS within
1e-3; iteration counts per major cycle within ±5% of CASA on `refim_point`.

**IF-6 Runtime core shrink (Opus).** Section 5.4 types; SIGINT → `Cancel` in
`casars-imager`; `RunSummary`; delete section 6 row 10 and the three T55
tests once the new admission test exists. Acceptance: T1 all capabilities
unchanged; admission test (explicit ceiling rejects; one-frame window forces
batch 1); cancellation test (SIGINT during a pass leaves no staged products
and returns within one block); no SHA-256 or CRC in runtime or application
production code.

**IF-7 Request and compile layer (Opus).** D8 and section 5.8; delete
section 6 rows 11–14; `prepare` deduplicated (five spectral-axis literals,
three velocity mappings, five single-SPW guards, three IO budgets become one
each); `availability::check` as the single gate. Acceptance: CLI and JSON
paths produce identical products for identical parameters (T1 subset run both
ways); catalog, Python wrappers and parameter reference regenerate with no
diff beyond removed parameters; `task_contract.rs` under 1,200 lines.

**IF-8 Products and publication (Opus).** Section 5.7; product list replaces
graph topology; writer moved into `casa-imaging-products`; standard-gridder
PB product and pixel masks (#217). Delete section 6 row 15. Acceptance: T1
product inventory, WCS and normalisation; T1.5 `refim_alma_mosaic` flatnoise
and flatsky residual/pb/pbcor within 1e-3.

**IF-9 Diagnostics and logging (Opus).** Section 8; delete all `CASA_RS_*`
reads and `eprintln!` in library crates; update the stage-timing consumers
under `tools/perf/imager/`. Acceptance: checker green; T1 with
`diagnostics.stage_timing` emits one line per phase.

**IF-10 Performance and memory pass (Opus with owner).** Regenerate T2 oracles;
run every T2 row before and after each optimisation; bars: within 10% of the
section 4.3 checkpoints for cube Metal W4, MFS pilot W1/W4/Metal, MFS 512²
W1/W4, and the #625 serial AW/MT-MFS bar; peak RSS at or below the recorded
values. Candidate optimisations only from measurement, taken from the
section 5.9 candidate list in the order the profile suggests. Acceptance: a
results table in the umbrella issue; no new abstraction without a measured
win. As run, IF-10 was two PRs, #711 for the timed bars and #712 onward for
the rest of T2 (owner, 2026-10-12; #659 deviation 5), and closed with the
Gate R4 outcome below.

**IF-11 Docs and closure (Fable).** `ARCHITECTURE.md`, `TESTING.md`,
`docs/agent-reference.md`, `.agents/skills`, ADR index; delete
`casa-imaging-reconstruction` remnants and historical imaging-architecture
docs that this plan supersedes; final line and test counts; umbrella issue
closed.

Order: IF-0 → IF-1 → IF-2 → IF-4 → IF-3 → IF-5 → IF-6 → IF-7 → IF-8 → IF-9 →
IF-10 → IF-11. IF-5 may run in parallel with IF-3/IF-4 (different crate).

### 9.3 Review gates

Each gate is the independent review of the gate ticket's PR plus the owner's
decisions on the deviations recorded on that ticket. The implementer
prepares the inputs below, posts deviations as they occur and asks the owner
only what needs the owner (section 3 decisions, external contracts); the
reviewer answers the gate questions; the implementer records the outcome as
a dated section appended to this document (`## Gate Rn outcome — date`),
including any replanning. The gate closes when that section is on `main`
and the ticket's PR is merged under the AGENTS.md merge rule. A gate that
replans edits the affected ticket bodies before the next ticket starts.

Inputs for every gate: the merged PR list with diff stats; current non-test
and test line counts per imaging crate; T0/T1/T1.5 results; the union of the
tickets' `## Deviations` sections; the intermediate-90 pilot W4 time; a
one-page implementer note on what was harder or different than the plan
expected.

Questions for every gate: Is the code converging on the section 5 types, or
has a parallel route or wrapper appeared? Did any deletion row survive, and
why? Does every new trait have a second implementation in this plan? Are
tests law-based or did fixture-specific expectations return? Is anything in
section 10 being violated repeatedly, and does the rule need sharpening? Has a
discovery invalidated a section 3 decision or a section 5 signature?

- **R1, after IF-1 (pattern gate).** Reviews the operator core before any
  caller depends on it. Specific questions: are `Placement`/`SampleBlock`
  sufficient for W, AW, mosaic and Taylor without extension (check against
  section 5.1's table and the HPG record); does `GridBackend::apply` with
  `Work` fit the Metal kernel plan; are the CASA-pinned rules placed in
  `key()` and `MuellerRouting` where IF-3 expects them; does
  `GridAccumulator` reserve or exclude the Hermitian half-plane layout
  (section 5.9 candidate); is the T0 law set strong enough to replace the
  fixture tests IF-2 will delete. Replan output: amended signatures in
  section 5.3, amended IF-2/IF-3/IF-4 bodies.
- **R2, after IF-4 (operator complete gate).** All drivers deleted, both
  backends live, replay gone. Specific questions: did D1 cost anything visible
  on the pilot (W4 time versus 15.8 s) and is a tap cache now justified or
  not; is Metal's f32 accumulation within tolerance on every T1.5 row; is
  `Partition::Regions` merge deterministic and is the fixed-lane model gone;
  has anything cube-specific survived outside `Residency::Waves`. Replan
  output: IF-3/IF-5 adjustments; whether IF-10 should start earlier for one
  capability.
- **R3, after IF-7 (core shrink gate).** Runtime, request and product layers
  shrunk. Specific questions: line count against the 70k target; does the
  run summary cover what the owner needs to see after a run; is admission
  correct on the 16 GiB machine for the pilot and the 512-channel cube
  (planned versus observed RSS); did any receipt, identity or evidence type
  come back under a new name; is the catalog now the single source of
  defaults. Replan output: IF-8/IF-9 scope; the IF-10 workload list and bars.
- **R4, after IF-10 (closure gate).** Performance and memory evidence against
  the section 4.3 checkpoints and the #625 bar. Specific questions: which bars
  are met, which regressed and why; is each retained optimisation justified by
  a measurement; what remains for IF-11 docs; what the next programme (wider
  VLASS parity, heterogeneous mosaic) should assume. Replan output: IF-11
  scope and the closing state of #445–#450.

## 10. Anti-slop rules for every ticket

Derived from Alexis King's "Parse, don't validate"
(https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/), the
Rust typestate pattern (https://cliffle.com/blog/rust-typestate/), the
fail-fast versus defensive-programming distinction, and John Ousterhout's *A
Philosophy of Software Design* (deep modules; define errors out of existence).

1. **Parse at the boundary, trust inside.** Validate user input, MeasurementSet
   contents and external files once, at the layer that reads them, and return a
   type that carries the proof. No function below that layer re-checks shape,
   finiteness, identity or ordering of data it received through a typed
   argument. If a check must exist in two places, the type is wrong.
2. **Typestate instead of runtime lifecycle checks.** "Was X called before Y",
   "is this bound to the current problem", "has this been sealed" are encoded
   by consuming `self` and returning the next state, not by flags, generation
   counters or `Result` returns that can only fail through programmer error.
3. **No silent fallbacks on required data.** `unwrap_or_default`,
   `unwrap_or(0)`, `.ok()?` and `if let Some` that skips work are only allowed
   for genuinely optional data, and the type must say it is optional.
4. **No content hashing for trust.** ADR-0014/0015 stand: no SHA-256/CRC over
   scientific arrays, plans, receipts or same-run spill payloads. Identity is
   ownership. External-format checksums where a format defines them are the
   only exception.
5. **One owner per invariant.** Each fact (worker count, memory budget, grid
   shape, channel map, weighting generation) is computed once, stored once and
   passed by reference or by value. Duplicated projections of the same fact
   into receipts, evidence and identities are deleted, not synchronised.
   R3 sharpened this for memory: the owner of an allocation charges what it
   holds, which means retained capacities, overlapping temporaries and storage
   read footprints, not lengths. One authoritative charge, no ledger of proofs.
   R4 added a requirement: a change that moves or adds scratch ownership (a
   buffer, a batch, a cache, a second owner of the same data) adds a counting
   law. The law measures the owner's peak against its charge, under the
   inputs that make the charge grow: wide cells, high native-to-output
   spectral ratios, and several live owners at once. #706, #712's CPU batch
   and IF-11's `MODEL_DATA` write were missed charges of this kind.
6. **Bookkeeping must not scale with data.** Telemetry and summaries are O(1)
   per phase. Nothing serialises, hashes or walks whole plans, models, sample
   streams or historical records for progress reporting.
7. **No one-use abstractions.** A trait with one production implementation, a
   factory that builds one thing, a wrapper that forwards every method, or a
   builder for a struct with public fields is removed. Keep a trait only where
   a second implementation exists in this plan (`GridBackend`: CPU/Metal;
   `ConvolutionFunctionSet`: four; `Solver`: four). R3 clarified the rule.
   Production polymorphism needs two production implementations. A trait
   that exists only to erase a type inside one owner, or to place an
   implementation behind a dependency boundary, is allowed when its rustdoc
   names that reason. Test doubles don't count as a second implementation.
   Don't invent a dummy implementation to satisfy this rule. The exceptions
   at R4, each with its reason in rustdoc: `BoundedSource` (a dependency
   boundary), the private `BlockIo` and `BackendIo` (type erasure), and the
   normal-state and model storage traits, which runtime's paged storage
   implements beside the resident ones (a dependency boundary with two
   implementations).
8. **Errors are types, not strings.** No `io::Error::other(format!(..))` for
   domain failures. Each crate has one `Error` enum with variants a caller
   could act on; everything else is a bug and panics with the invariant named.
   `unreachable!`/`assert!` in production code state an invariant the type
   system cannot express, with a message, and are rare.
9. **No environment-variable behaviour switches in library crates.** Diagnostic
   switches live in `ImagingRequest.diagnostics`. A library reads no
   `std::env`.
10. **No narration.** Comments say why, not what; no step numbering, no
    restated signatures, no historical notes ("formerly", "T41 added").
    Rustdoc on public items states the contract.
11. **File and function size.** No longer a review rule; the build enforces
    it workspace-wide (#705): clippy's `too_many_lines` (threshold in
    `clippy.toml`) and `scripts/check-file-length.py` in `just arch-check`.
12. **Delete in the same change.** A replacement migrates callers and deletes
    the displaced code, tests, docs and resources in one PR. Nothing is kept
    "for reference" in the tree; git history is the reference.
13. **Tests test laws and contracts.** Prefer adjoint identities, conservation,
    tolerance against a reference, and resource bounds over fixture-specific
    expected numbers. A test that asserts an implementation detail is deleted
    unless that detail is a promised contract. R3 added two requirements:
    - **Resource-bound laws measure.** They compare the charge with
      independently measured allocations (`capacity()`, RSS or footprint),
      not one formula with another.
    - **Interop laws use an independent reader.** Use a casacore reader in
      its own process, held open across the write, not only a fresh open.
14. **No new mode, flag or route to preserve old machinery.** If an existing
    mechanism cannot be expressed through section 5 types, the ticket records
    the gap in `## Deviations` for the next gate instead of keeping a second
    path.

## 11. Process

- The first merge to `main` is IF-0 and carries the T55 branch content. From
  then on every IF PR targets `main`; there is no long-lived integration
  branch.
- One ticket active at a time, except IF-5 alongside IF-3/IF-4. A ticket is
  active only with an open PR containing code.
- The implementer records deviations in the issue as they occur, not at the
  end. A deviation that changes a section 3 decision or a section 5 signature
  stops the ticket until the owner answers, unless it is clearly a
  correction of an error in this document (then it is recorded and continued).
- Evidence that must survive (T1.5 CASA references, T2 oracles, timing
  tables) lives under `/Volumes/GLENDENNING/casa-rs-evidence/if/<ticket>/`
  with a `README.md` naming the commit, command and result. Nothing
  restart-critical under `/tmp`.
- A ticket PR merges without asking once its gates are green, its
  independent review is resolved and no deviation is open for the owner
  (AGENTS.md, Merging; decided at R1). Cleanup and release remain
  owner-authorised actions. Closing the #486 family (D7) happened in IF-0.
- AGENTS.md is updated in IF-0 to point programme text at this plan and
  remove the #486-specific rules.

## Gate R1 outcome — 2026-10-07

Gate ticket IF-1 (#650), PR #662 on `claude/if-1-operator-core`, final
commit 71a27e7cd4. Reviewers: OpenAI Astra (independent review of the PR at
45386478de, posted under the owner's login, five findings), the owner
(deviation decisions, the normalisation decision), Fable (implementer).

### Inputs

| Input | Value |
|---|---|
| PR | #662: 4 commits, 24 files, +6,636 / −4 lines, base `main` |
| `casa-imaging-operator` | 4,443 non-test lines (`src/`, largest file 589), 2,201 test lines |
| Tests | 46 (9 unit, 37 integration), each under a second, both precisions; `just quick` green at 71a27e7cd4: 4,514 passed, 68 skipped |
| T1 / T1.5 | none (no production caller until IF-2) |
| Pilot W4 time | none yet; IF-2 records the first |
| Deviations | eleven on #650, each with an owner decision |
| Implementer note | the IF-1 retrospective on #648 (eight rules, applied to IF-2 … IF-11 as per-ticket comments) |

### Answers to the R1 questions

- **`Placement`/`SampleBlock` sufficient for W, AW, mosaic and Taylor
  without extension.** Yes. `w` and `CfKey { group, cube }` carry the
  W-plane and AW cell, `gradient` the mosaic pointing phasor, `phase` the
  phase-centre shift, `spectral` the Taylor power; `RowContext` carries
  time, antennas, parallactic angles, field and pointing offset for `key()`.
  The dense kernel laws run with complex taps, gradients and a two-plane
  Mueller routing. IF-3 confirms this against the CASA AW and mosaic
  machines.
- **`GridBackend::apply` with `Work` fits the Metal plan.** Yes. The
  accumulator layout `[plane][pol][term][y][x]`, interleaved complex, is the
  device buffer and `AccumulatorLayout::bytes(precision)` sizes it exactly.
  Metal accumulates in f32, so `gridprecision = f64` with the Metal backend
  is a typed rejection (IF-4); the rounding rule in `GridGeometry::locate`
  is shared by both backends.
- **CASA-pinned rules placed where IF-3 expects them.** `key()` on the CF
  set (cache index, conjugate-beam frequency map), `MuellerRouting::table`
  (the w-sign and direction swap), `Placement::gradient` and `phase` (the
  pointing phasor), `CpuBackend` with `kernel::norm` and `prediction_norm`
  (the AW normalisation). Each rule's rustdoc names the CASA function
  (`StokesImageUtil`, `VisImagingWeight`, `BriggsCubeWeightor`,
  `FTMachine::matchChannel`, `interpolateFrequencyTogrid`,
  `AWVisResampler::GridToData`, `faccumulateFromGrid`).
- **Hermitian half plane.** Neither reserved nor excluded (section 5.9): a
  `Tile` variant plus a conjugate fold in `finish`; stays an IF-10 candidate.
- **T0 law set strong enough to replace the fixture tests IF-2 deletes.**
  After the review, yes. Three of the five findings came from laws tested on
  the convenient case (real unit-sum taps, equal XX/YY weights, several
  channels); the fix added the general-case laws (complex taps with
  gradients and leakage routing, polarized PSF, one-channel image and
  one-channel row). Retrospective rule 2 makes that the standard for every
  later ticket.

### Decisions

- Deviations 1–11 on #650: `ModeSet` on `accumulator` plus
  `accumulator_layout` as the planner's memory input (kept); HPG padding
  dropped; `NormalSection` dropped for the plan's vectors with one `sumwt`
  per image; `briggsbwtaper` kept as `BandwidthTaper`; operator-local input
  types; the paired correction on both sides; polarization pinned to CASA
  `StokesImageUtil`; CASA's unpolarized weight and any-correlation-flagged
  rule; the density pass on the resampler's blocks; the Hermitian layout
  neither reserved nor excluded.
- Normalisation (the review's contract question): CASA's normalisation
  kept; the law is "exactly adjoint once each sample's data are divided by
  the conjugate kernel norm", with the unnormalised law as the `N ≡ 1` case.
- The five review findings fixed in 71a27e7cd4 and answered inline on the PR.

### Replanning

- Section 5.3 amended in place (marked R1): `TapLayout::Dense` without
  padding, the kernel contract's single division by the norm and the
  adjoint law, `ModeSet`, `accumulator_layout`/`accumulator`/
  `prediction_norm`, `NormalPlane`/`NormalImages`,
  `WeightingGeneration::Density`. Section 5.9's Hermitian row records the
  decision.
- Ticket bodies: IF-2 … IF-11 each carry a "Learnings from IF-1" comment
  with the interfaces to build on; IF-3 is reassigned to Fable (the densest
  CASA-pinned science); IF-4 notes that Metal implies f32 accumulation and
  that the rounding-rule choice is separate.
- Process: a ticket PR merges without an owner consult once its gates are
  green, its independent review is resolved and no deviation is open for the
  owner (AGENTS.md, Merging; sections 9.1, 9.3 and 11 amended). Gates R2 to
  R4 close the same way: outcome section on `main`, PR merged.

Gate closed: #662 marked ready and auto-merge armed on 2026-10-07.

## Gate R3 outcome — 2026-10-10

**Gate ticket:** IF-7 (#656).
**PRs:**
- #683 (PR-A, the request surface);
- #684 (PR-B, the MeasurementSet as CASA writes it, with casacore table locking);
- #685–#688 (C1–C4);
- #692 (per-plane minor-cycle normalisation, found during review);
- follow-ups #689, #693 and #696;
- #699, which fixes the held-open-reader notification.

**Reviewers:**
- OpenAI Astra: independent reviews of every PR, three rounds on #684, and the gate review on #656. Posted under the owner's login.
- Fable: structural reviews of #683–#688.
- The owner: decision C, table locking waits as casacore does, the waiver of a fourth Astra pass on #684, and the admission routing below.
- Claude (Opus): implementer.

**This is a conditional closure.** Admission is not correct for every live phase (question 9). By owner decision on 2026-10-10, its correction is IF-8's first change, not a further IF-7 PR.

### Inputs

All inputs are on #656: gate inputs, admission, review-round records and final evidence.

| Input | Value |
|---|---|
| PRs | #683 +8,194 / −17,689; #684 +6,137 / −5,653; #685 +1,429 / −11,299; #686 +1,565 / −11,032; #687 +985 / −465; #688 +1,204 / −3,450; #692 +96 / −67 |
| Imaging non-test source | 47,044 lines at `25d4f32320`, against 70k (target) and 50k (stretch); see question 7 for the counting convention. 60,965 at IF-7's start, excluding `kernels.metal`. |
| casa-ms non-test source | 65,412 → 58,758 (outside the target) |
| T0 / T1 | `just quick` at `25d4f32320`: 3,720 passed. Task CLI hosts (18 binaries) and GUI acceptance (7) pass. C++ locking interop: `tables_locking_interop` 5/5 and `ms_locking_vs_cpp` 3/3. |
| T1.5 | All 21 rows equal the C4 head to rounding. Forced-paging rows are equal, with identical traversal counts and row-channels read. |
| Pilot W4 | 14.7–15.0 s at `f7575d9dc2` and 15.49 s at `25d4f32320`, against the 15.82 s checkpoint. Peak RSS is 4.73 GB. |
| Deviations | 1–26 plus the review-round records on #656 |
| Implementer note | in the gate inputs on #656 |

### Answers to the R3 questions

These summarise Astra's gate review on #656, which is the record.

1. **Converging on §5 types: yes.** `CompiledProblem`, `MeasurementOperator` and `MajorCyclePass` are the live route. `MajorCycle`/`PassAppender` owns the model and its accumulator, and one Measures provider is shared per run. What remains is ownership moves: the CASA writer into products (IF-8), and deleting the reconstruction crate (IF-11).
2. **Surviving deletion rows: row 16 remains, and some old names.**
   - Rows 1–15 are removed. Row 14, the product graph, is converted to a product list (normalisation boundary, nodes, publication) but keeps the `ProductGraph` name and "DAG" rustdoc.
   - Row 16, the environment and `eprintln!` diagnostics, is pending IF-9: `products/taylor.rs:135` and `casa_product_sink.rs:199`.
   - `ObservationTransactionContract` stays as a storage-selection contract; it is not the deleted runtime transaction.
3. **A second implementation for every new trait: no.**
   - `GridBackend`, `ConvolutionFunctionSet`, `Solver`, `GridScalar` and the model and normal storage traits meet the rule.
   - `ProductOutput`/`ProductWriter` and `BoundedSource` have one production implementation each. The private `BlockIo`/`BackendIo` traits are type erasure.
   - Rule 7 is sharpened below.
4. **Law-based tests: partly.**
   - The review-round laws are sound: sparse selection, the selected-row count, borrow-enforced views and per-plane weights.
   - Two classes of gap were found and fixed before closure: a held-open-reader notification law (#699), and a law comparing actual buffer capacities with the plan's charge (#686).
   - The pruned-DDID preparation bug that Astra's probe found is fixed in #686, with laws for selections that remove whole DDIDs and an empty-selection control.
5. **Repeated §10 violations: yes.**
   - Resource accounting (rules 5 and 13) and one-implementation adapters (rule 7) are sharpened below.
   - Invariant-style `ProblemShapeMismatch` returns (rule 8) go to IF-8, and environment diagnostics (rule 9) to IF-9.
   - §7's bounded-memory inequality is corrected below.
6. **Discoveries invalidating a §3 decision or §5 signature: none.**
   - D1's re-traversal stands, and nothing argues for a replay cache.
   - The recorded deviations stand: `ImagingRequest` in the application; availability taking backend and precision; the shared Measures provider; `MajorCycle`/`PassAppender`; the removed start model and selectors.
   - The 16 GiB native ceiling stays. The evidence workstation has 32 GiB, so the ceiling is emulated by policy.
7. **Line count against 70k: yes, 47,044 non-test lines, also under the 50k stretch.**
   - Counted at `25d4f32320`: physical lines in every source language under `crates/casa-imaging-*/src` and `crates/casars-imager/src`.
   - Excluded as test code: `tests.rs`, `*_tests.rs`, `tests/` and inline `#[cfg(test)]`.
   - The gate input's 46,776 left out `kernels.metal` (268 lines). IF-11 recounts.
8. **Run summary: partly.**
   - `image.summary.json` has the request, error, per-phase status, time and cumulative peak RSS, workers, backend, minor-cycle totals and products.
   - It lacks admitted demand and capacity, the resolved precision and the stop reason.
   - It loses completed counts when a later phase fails, and is not written on cancellation.
   - Its "peak RSS" is the process high-water mark sampled at each phase end.
   - The fix goes to IF-9.
9. **Admission correct under the 16 GiB policy: no.**
   - The source plan, paged cube state and major-cycle passes are admitted.
   - Not admitted: the minor cycle (preparation and solver), and nonpaged model, normal, PSF and mask state that outlives its pass reservation.
   - The pilot planned 3.81 GB against a 4.73 GB observed peak. The 512-channel cube planned 12.88 GB, its whole budget, against a 13.61 GB footprint.
   - The fix is IF-8's first change; see the IF-8 replan.
10. **Receipt, identity or evidence types back under new names: no.**
    - `ModelGeneration`, `WeightingGeneration` and the product generations own data, `Epoch` is observing time, and `AutoMultithreshEvidence` holds algorithm statistics.
    - Cleanup goes to IF-8: products' unused `sha2` dependency, and stale "lineage", "versioned generation" and "DAG" wording.
11. **The catalog as the single source of defaults: yes,** for the supported request surface.

### Decisions

- **Owner:**
  - Decision C, nothing in an MS that CASA does not write (2026-10-09).
  - In-place writes wait for a held table lock as casacore does, and refuse deadlocks. ADR-0008 is amended.
  - #684's round-3 fixes merge without a fourth Astra pass.
  - Admission correctness is IF-8's first change, so R3 closes conditionally.
- **Deviations** 1–26 and the review-round records on #656 stand as recorded.
- **Fixed before closure:** #699 (held-open-reader notification after a resync), #686 (buffer capacities charged; selections that remove whole DDIDs).
- **Follow-ups kept visible, outside IF-8–IF-10:** #694 (release retained read locks on request), #695 (flag-version delete/rename while open elsewhere), #697 and #698 (pre-existing interop bugs). They are casa-ms and casa-tables storage correctness, not performance work.

### Replanning

- **§7 amended in place (marked R3):** the bounded-memory check no longer reads "planned ≤ observed peak RSS × 1.1", which does not bound underestimation.
- **§10 rules 5, 7 and 13 sharpened in place (marked R3).**
- **Ticket bodies:** IF-8 (#657), IF-9 (#658) and IF-10 (#659) each carry a "Gate R3 replan" section:
  - **IF-8:** admission correctness first, then the product scope. Also the `ProblemShapeMismatch` audit, removing writer indirection, products' `sha2`, and terminology. The deletion-row reference corrected to row 14, already converted.
  - **IF-9:** the run-summary and admission observability; the `taylor.rs` and `casa_product_sink.rs` switches. The deleted `t51_pair_driver.py` dropped.
  - **IF-10:** the full T2 list and bars kept. Admitted-versus-observed memory added for resident and paged runs. The three MAIN walks profiled first. Near-tie rows carried explicitly.

Gate closed (conditional on IF-8's first change): #692 (carrying #686–#688) and #699 merged and this section reached `main` on 2026-10-10.

## Gate R4 outcome — 2026-10-16

**Gate ticket:** IF-10 (#659).

**PRs:**
- #711 (part 1, the timed bars);
- #712 (part 2, CPU work and the remaining T2 rows);
- #722 (the mosaic MFS, mosaic cube and facets rows);
- #723 (`backend = auto | cpu | gpu`);
- #724 (the density pass reads no visibilities, and an unflagged non-finite input fails the run).

**Reviewers:**
- OpenAI Astra: independent reviews of every PR, and the gate review on #659. Posted under the owner's login.
- Fable: structural reviews of #712, #722 and #723.
- The owner: the decisions below.
- Claude (Opus): implementer.

### Inputs

All inputs are on #659 ([gate inputs](https://github.com/bglenden/casa-rs/issues/659#issuecomment-6218158934)) and #648 ([results table](https://github.com/bglenden/casa-rs/issues/648#issuecomment-6210600264)).

| Input | Value |
|---|---|
| PRs | #711 +1,035 / −131; #712 +5,823 / −1,500; #722 +3,098 / −428; #723 +484 / −82 (its own diff); #724 +521 / −132 at review, plus `5ca1b5d244` |
| Imaging non-test source | 54,311 lines at `5ca1b5d244` (54,282 at the reviewed `af10699a5e`), against 50,572 at IF-10's start and 47,044 at R3, by R3's convention (question 7). Under 70k (target), over 50k (stretch). |
| casa-ms non-test source | 58,871 → 59,211 |
| T0 / T1 | `just quick` green at `af10699a5e` and `5ca1b5d244`. casa-ms C++ interop (`ms_full_verify_vs_cpp`, `ms_data_interop`, `ms_locking_vs_cpp`) green against casacore. Metal laws on the GPU at `4056e91abe`. |
| T1.5 | At the final head, every case equals the previous head to rounding, except the recorded near-ties. Every `.sumwt` case is within the 5e-5 limit. |
| T2 | 18 of 19 matrix rows pass; one is excluded by the owner. Every timed bar is met. #625 is 1.90× faster than CASA, the full cube 5.22×, and Wave3 1.71× on the same MS and machine. |
| Pilot W4 | CPU 13.1 s, Metal 10.9 s, against bars of 17.4 and 16.3 s |
| Deviations | 1–18 on #659 |
| Implementer note | in the gate inputs on #659 |

### Answers to the R4 questions

These summarise Astra's gate review on #659, which is the record.

1. **Converging on §5 types: yes.** The live route is still compile → `MeasurementOperator` / `MajorCyclePass` → normal and model state → products.
   - `SampleContent` changes a traversal's content, not the driver.
   - The native-residual `Work` variants are backend operations.
   - `ChartStitch` assembles facets onto the common normal state's reference scale and must stay limited to that.
2. **Deletion rows: the displaced routes stay removed.** No replay operator, execution DAG, prepared-artifact cache, slab driver or request mirror is back. `casa-imaging-reconstruction`, 5,478 lines of live mathematics and storage, is IF-11's to inventory, move and delete.
3. **Second implementations:** IF-10 added no new trait. `BoundedSource` (a dependency boundary) and the private `BlockIo` and `BackendIo` (type erasure) need their reasons in rustdoc (rule 7); that goes to IF-11.
4. **Law-based tests: yes, predominantly.** The #724 continuum law discriminates a missing role flag: Astra reproduced the mutation, 0.5 against 1. The core-allocation and native-residual laws exist, and the evidence table should name them.
5. **Repeated rule problems:** allocation accounting (rule 5) and the size exceptions.
   - IF-11 adds a counting law whenever scratch ownership changes, splits `aw/mod.rs` (1,554 lines) and `access.rs` (2,569), and shrinks the allowlist.
   - Evidence runs must name their backend, precision, FFTW build, input, comparator and memory policy.
6. **Discoveries that changed decisions and signatures: yes, all recorded.**
   - D2: the CPU default is f64.
   - D1: a narrow, charged kept-weight cache.
   - The native prediction and residual `Work` variants.
   - `ResourcePolicy::cores`.
   - `backend = auto | cpu | gpu`.
   - Residual passes keep their response.
   - Explicit traversal content.
   - The finite-input rule.

   D1's re-traversal stands: the MAIN-walk profile does not justify a visibility or replay cache.
7. **Bars:**
   - **Met:** every timed checkpoint and every peak RSS.
     - The 512-plane row is met on its re-baselined f32 bars of 30.5 and 12.5 s, for a resimulated input.
     - The f64 CPU default costs about 13.5 s at W4 and a 9.1–9.2 GB footprint. That is the accepted cost of the D2 amendment, not a pass of the f32 bar.
   - **Deep32 cycles:** 1.045× the pre-refactor build, against the 1.10 bar.
   - **#625:** 4,764.57 s against 9,073.69 s, at `01ceae21eb`. A cold run is estimated at 4,924 s; it was not run.
   - **Full cube:** 4,144.27 s against 21,622.67 s. The 9.61 GB footprint against an 11.55 GiB sampled RSS is not a matched peak comparison.
   - **Wave3:** 4,654 s against 7,944.7 s on the same MS and machine.
   - **Exceptions, each with the owner's decision:**
     - deep-clean residuals, both denominators recorded;
     - the `.sumwt` limit, 5e-5;
     - Wave3 on the CPU and Metal;
     - the near-ties, below;
     - `.weight.tt1`, until #690.
   - The forced-paging `withline` exception is retired: 1.87e-6 at f64.
8. **Retained optimisations:** each has a reported measurement, with attribution limits.
   - Row 2 was measured jointly with row 3.
   - Row 12 was superseded by row 16.
   - Row 26 corrects the evidence build; it is not a production optimisation.
   - Do not sum the percentages.
9. **IF-11 scope:** consolidated in #660's body ("Gate R4 replan").
10. **The next programme should assume:**
    - one foundation route;
    - an f64 CPU and an f32 Metal, validated by tolerance;
    - `auto` as an eligibility choice, not a guarantee of the fastest backend;
    - W-projection, mosaic and AW on the CPU (#653);
    - #719, #720, #721, #714 and #690 carried;
    - no global tolerance from Wave3's near-tie;
    - storage I/O measured before it is assumed.

### Decisions

- **Owner, 2026-10-15:** the closure decisions ([6200701530](https://github.com/bglenden/casa-rs/issues/659#issuecomment-6200701530)):
  - the three matrix rows pass before closure;
  - the moving-source cube is excluded;
  - rows without a timed checkpoint are judged against CASA's time;
  - deep-clean residuals are judged against the image peak;
  - the T1.5 exceptions;
  - the memory table goes to IF-11 and full-360 is dropped;
  - Wave3 runs on the mini.
- **Owner, 2026-10-16:**
  - the `.sumwt` limit is 5e-5;
  - Wave3's acceptance extends to Metal;
  - an unflagged non-finite input fails the run (deviation 18, `5ca1b5d244`, no separate re-review);
  - deviations 11 and 12 are accepted;
  - deviations 4, 9, 10, 15, 16 and 17 are accepted with their stated costs and divergences ([6219345824](https://github.com/bglenden/casa-rs/issues/659#issuecomment-6219345824)), so every deviation 1–18 has an owner disposition;
  - the T1.5 near-ties are accepted on the corrected evidence: the flip predates IF-10, and no pre-refactor control was run.
- **Corrections to the record:**
  - #659's 512-plane bars are re-baselined.
  - The near-tie repeats are all on post-refactor builds.
  - The refim T1.5 harness pins the CPU for every binary of protocol 12 or later.

### Replanning

- **IF-11 (#660):** its body carries the "Gate R4 replan" scope: documents, the reconstruction crate, counts, the evidence table, the memory table, the file splits, rules 5 and 7, evidence discipline, and the per-issue mapping of #445–#450.
- **#445–#450:** #446 stays closed. #445 and #447–#450 stay open until IF-11 maps them.

Gate closed: #724, which carries this section, merged on 2026-10-16.

## 12. Deviations reconciled (IF-11, 2026-10-16)

Every deviation the tickets recorded under `## Deviations` has an owner
disposition on its ticket. IF-11 folded the IF-10 deviations (#659 1–18) and
its own (#660) into the sections above; this table says where each now
stands. Earlier tickets' deviations were decided at Gates R1–R3 and are
recorded in those outcome sections.

| Deviation | What landed | Where the plan states it |
|---|---|---|
| #659 1–2 | `GridBackend::apply` may leave grid-only work on device cells; `DeviceCells::defer` (`unsafe`), `DeferredWork`, `MetalBackend::{with_deferred_grid_completion, complete}`, `NativeBlock::{order_rows_by_baseline, ordered_row}` | §5.3 `GridStorage`, `GridBackend`; §5.9 stream-rotation row |
| #659 3 | Six-slot Metal ring (18 MiB per owner with two correlations) | §5.9 stream-rotation row |
| #659 4 | Baseline-grouped blocks cost the Metal pilot about 3% before deferral; kept for the cube's −7% | this table (an accepted cost) |
| #659 5 | IF-10 in two PRs | §9.2 IF-10 |
| #659 6 | The backend forms the native-channel residual (`Work::PredictNative`, `Work::NativeResidualGrid`) | §5.3 `Work`, `GridBackend`; §5.4 pass description |
| #659 7 | Kept imaging weights across passes (`WeightCache`) | D1; §5.4 |
| #659 8 | Core allocation and two worker teams | D5; §5.4 `ResourcePolicy::cores` |
| #659 9 | Residual passes sum no weights (`without_weight_sums`) | §5.3 kernel contract, `GridAccumulator`, `NormalPlane` |
| #659 10 | Multi-window selections walked one DATA_DESCRIPTION at a time | §5.4; §5.6 |
| #659 11 | `aw/mod.rs` and `access.rs` over 1,500 lines | Resolved in IF-11: both split below the limit and removed from the allowlist |
| #659 12 | Cube density looked up at the rotated uvw (CASA bug) | §5.3 weighting |
| #659 13 | `gridprecision = auto` is f64 on the CPU, cubes included | D2 amendment; §5.4; §5.8 |
| #659 14 | `backend = auto \| cpu \| gpu`, default `auto` | D2; §5.8 `resolve_backend` |
| #659 15 | A facet count that does not divide the image size fails validation | §5.8 |
| #659 16 | T2 harness: CASA `conjbeams=False` for mosaic, `True` for AW | §7 T2 |
| #659 17 | An empty facet 0 hands the reference to the lowest populated facet | §5.7 |
| #659 18 | An unflagged non-finite input fails the run | §5.8 |
| #660 1 | Normal state, model lifecycle, masks and `MajorCycle` in deconvolution; continuum fit in the operator; reconstruction crate deleted | D4; §5.2; §5.3 |
| #660 2 | The `MODEL_DATA` writer's fix needed a casa-tables API (`Table::array_column_tile_bytes`) and fixed a latent sparse-save bug; four allowlisted casa-tables files grew | this table; `TESTING.md` memory rules |
| #660 3 | A Metal purge-on-drop, withdrawn: the reading it answered was macOS's footprint lag after frees | this table; `TESTING.md` memory rules |
| #660 4 | The three documents ADR-0013 and ADR-0014 name, and the source study one of them linked, deleted too; the ADRs keep their text as history (owner, 2026-10-16) | this table |
| #660 5 | The catalog's `backend` details say the GPU predicts at native channels and forms the residual there; description only, so `semantic_revision` stays 2 (owner, 2026-10-16) | §5.8; this table |
| #660 6 | The visibility writer accepts only `TiledShapeStMan` and `TiledColumnStMan` destinations and refuses any other manager, whose save rewrites all of MAIN, when the writing pass is admitted (#726 review) | `ARCHITECTURE.md` (MODEL_DATA and CORRECTED_DATA); this table |
