# Imaging foundation: review and phased refactor plan

Truth class: proposed normative plan (becomes normative on owner approval)
Date: 2026-10-07
Branch: `claude/imaging-foundation` (cut from `codex/t55-full-size-validation`
at `e714fd2339`)
Status: DRAFT, awaiting owner approval
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
  operator if measurement shows it pays.
- **D2 Metal precision.** Metal is one implementation of the backend trait and
  accumulates in f32 with atomic adds (nondeterministic order). The CPU
  accumulates in f64 by default. This is consistent with the owner's
  2026-09-24 numerical direction: acceptance is the 1e-3 normalised tolerance,
  not bitwise agreement. Metal cube planes may use f32 grids on CPU as well
  (grid precision is a parameter).
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
- **D5 Runtime core.** Replace receipts (schema 25), the execution DAG,
  execution bindings, the resource-authority topology, observation
  transactions and the cost model with: `HostResources`, one `admit()` per
  phase returning an RAII reservation, a sequential phase list with a cancel
  token, the trimmed Metal execution state, the bounded worker team, and an
  O(1)-per-phase `RunSummary`. ADR-0010 is superseded by a new ADR-0016.
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
  acceptance row). #341 closes into IF-5; #217 into IF-9. The VLASS wave
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
A   = Degrid ∘ Fft ∘ Correction⁻¹ ∘ PolBasis⁻¹ ∘ ModelPrescale
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
| `casa-imaging-operator` | `Placement`, `SampleBlock`, `ConvolutionFunctionSet` + four implementations, `GridBackend` trait + `CpuBackend`, `GridAccumulator`, `MeasurementOperator`, `WeightingGeneration`, `NormalImages`, FFT and correction operators, polarisation routing, spectral resampling (one implementation) | model, casa-fft, casa-numerics |
| `casa-imaging-deconvolution` | `MinorCycleView`, `Driver`, `Solver` trait + Hogbom/Clark/Multiscale/Taylor solvers, `PsfSummary`, masks and automask, `ModelDelta` | model, casa-numerics, casa-fft |
| `casa-imaging-products` | normalisation (section 5.7), restoration, Taylor products (alpha, error), PB/pbcor, product list and metadata, CASA image writer with atomic replacement | model, operator, deconvolution, casa-images |
| `casa-imaging-metal` (macOS) | `MetalBackend: GridBackend`, MSL kernels, device/queue/buffer/fence state | operator, objc/metal crates |
| `casa-imaging-runtime` | `HostResources`, `admit()`, `Phase`/`run_phase`, cancel token, `WorkerTeam` + bounded stream, `Partition`, `Residency`, `MajorCyclePass`, cube state paging, managed spill (same-run), `RunSummary` | model, operator, deconvolution, products, metal (cfg macos), casa-ms |
| `casa-imaging-application` | compile → plan → run composition, MeasurementSet access and selection, availability of installed capabilities, publication orchestration | all of the above |
| `casars-imager` | CLI parsing via the catalog, request projection, progress and result presentation | application, provider contracts |

Dependency direction is enforced by the replacement checker (D6). No crate
below `application` reads a MeasurementSet; no crate but `metal` imports a
device API; no crate but `casars-imager` and `application` reads `std::env`.

### 5.3 The operator core (`casa-imaging-operator`)

```rust
/// One selected row×channel sample after flagging, phase-centre shift and
/// correlation routing. Flagged samples are not placed.
#[derive(Clone, Copy)]
pub struct Placement {
    pub u: f64, pub v: f64, pub w: f64,  // wavelengths at this sample's frequency
    pub phase: f64,                      // phase-centre shift argument (rad); 0 when none
    pub plane: u32,                      // target grid plane (channel-local); 0 for constant basis
    pub spectral: f32,                   // (ν−ν0)/ν0; read only by a Taylor basis
    pub cf: CfKey,                       // opaque to the kernel; resolved by the CF set
    pub gradient: [f32; 2],              // pointing phase gradient (rad per cell); 0 otherwise
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CfKey {
    pub w_plane: u16, pub freq_cell: u16, pub pa_cell: u16, pub pair: u16,
    pub mueller: u8, pub conjugate: bool,
}

/// Structure-of-arrays view of one bounded block. `values` and `weights` are
/// `npol × n`, sample-major. Weight 0 never appears (flagged samples are dropped).
pub struct SampleBlock<'a> {
    pub placements: &'a [Placement],
    pub values: &'a [Complex32],
    pub weights: &'a [f32],
    pub npol: usize,
}

pub enum TapLayout<'a> {
    /// Standard: one real row per oversampled offset, `rows[offset*support + i]`.
    SeparableReal { rows: &'a [f32], support: u16, oversampling: u16 },
    /// W, AW, mosaic: dense complex kernel, `data[(oy*support_x + ox) * ...]`
    /// laid out oversampling-major so one fractional offset is contiguous.
    Dense { data: &'a [Complex32], support: [u16; 2], oversampling: u16 },
}

pub enum Direction { Adjoint, Forward }

pub trait ConvolutionFunctionSet: Send + Sync {
    /// Pure. Row context carries PA, antenna pair, field pointing offset, time.
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64, pol: usize,
           direction: Direction) -> CfKey;
    fn taps(&self, key: CfKey) -> TapLayout<'_>;
    /// FT[PB²] taps for the weight/sensitivity image (mosaic, AW); None otherwise.
    fn weight_taps(&self, key: CfKey) -> Option<TapLayout<'_>>;
    /// Paired image-domain gridding correction (separable 1-D vectors per axis).
    fn image_correction(&self) -> &ImageCorrection;
    /// Σ taps for sumwt accounting (AW); 1.0 otherwise.
    fn normalization(&self, key: CfKey) -> f32;
}
```

Implementations: `Spheroidal` (standard), `WPlanes`, `AwCatalog` (CASA
`CFS_`/`WTCFS_` import or native EVLA generation, in-memory LRU of cells,
section 5.6), `MosaicPb` (per frequency and antenna-class pair, Lanczos
oversampling 10, re-phased per pointing). Each keeps the CASA-pinned
rounding and conjugation rules listed in section 4.3 in its own `key()`.

Kernel contract (both backends implement exactly this):

```
adjoint:  for each sample, tap' = (key.conjugate ? conj(t) : t) · e^{i(ix·gx + iy·gy)}
          grid[plane, pol][u0+ix, v0+iy] += W · V · e^{iφ} · tap'         (Data)
                                          += W · tap'                       (Psf: V = 1)
                                          += W · weight_tap' at uvw = 0     (Weight)
          sumwt[plane, pol] += W · normalization(key)
          Taylor: term t grid receives W · s^t (data: t < N_t; psf: t < 2N_t−1)
forward:  V_pred = e^{−iφ} · Σ_t s^t · Σ_{ix,iy} conj(tap'_t) · model[t][u0+ix, v0+iy]
          AW divides by conj(normalization(key)).
```

```rust
pub enum Mode { Data, Psf, Weight }
pub enum GridPrecision { F32, F64 }

/// Per-worker grid storage: planes × pols × terms over a plane or a tile with halo.
pub struct GridAccumulator { /* private */ }

pub trait GridBackend: Send {
    fn grid(&mut self, block: &SampleBlock, cf: &dyn ConvolutionFunctionSet,
            mode: Mode, acc: &mut GridAccumulator) -> Result<(), OperatorError>;
    fn degrid(&mut self, block: &SampleBlock, cf: &dyn ConvolutionFunctionSet,
              model: &PreparedModelGrids, out: &mut [Complex32]) -> Result<(), OperatorError>;
}
```

`CpuBackend` is support-generic over both tap layouts with a specialised
seven-tap separable path. `MetalBackend` (IF-4) implements the same trait with
support-generic kernels and `float2` dense kernel storage.

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
    pub fn accumulator(&self, planes: PlaneRange, tile: Option<Tile>) -> GridAccumulator;
    /// model (× PB prescale when the product contract says flatnoise) ÷ correction → FFT
    pub fn prepare_model(&self, model: &ModelImages, prescale: ModelPrescale) -> PreparedModelGrids;
    /// FFT⁻¹, × correction, crop: unnormalised dirty/psf/weight planes + sumwt
    pub fn finish(&self, acc: GridAccumulator) -> NormalImages;
}

pub struct NormalImages {
    pub planes: Vec<NormalPlane>,  // one per output plane; Taylor terms inside
}
pub struct NormalPlane {
    pub data: Vec<Array2<f32>>,    // residual/dirty per term (N_t) and pol
    pub psf: Vec<Array2<f32>>,     // 2N_t−1 terms
    pub weight: Option<Array2<f32>>, // mosaic/AW sensitivity
    pub sumwt: Vec<f64>,
}
```

Weighting:

```rust
pub enum WeightingGeneration {
    Natural { taper: Option<Taper> },
    Density { grid: DensityGrid, robust: Option<f64>, taper: Option<Taper> },
}
impl WeightingGeneration {
    /// Pure per-sample function; CASA cell rules live in `DensityGrid::lookup`.
    pub fn imaging_weight(&self, p: &Placement, input_weight: f32) -> f32;
}
pub fn build_density_grid(source: impl Iterator<Item = SampleBlock>, shape: DensityGridShape) -> DensityGrid;
```

Spectral resampling (CASA linear pairing with nearest weight and linear
flags) is one function producing `Placement`s and values from a native block;
cube density weighting consumes the same function.

### 5.4 Execution (`casa-imaging-runtime`)

```rust
pub struct HostResources { pub threads: usize, pub performance_cores: usize,
                           pub available_memory: u64, pub fd_limit: u64, pub metal: bool }
pub enum ResourcePolicy { Interactive, Balanced, Exclusive, Explicit { workers: usize, memory: u64 } }
pub struct Reservation { /* RAII; releases on drop */ }
pub fn admit(host: &HostResources, policy: &ResourcePolicy, demand: &Demand) -> Result<Reservation, Admission>;

pub struct Cancel(Arc<AtomicBool>);        // set by SIGINT handler in casars-imager
pub struct Phase { pub name: &'static str, pub steps: Vec<Step> }
pub fn run_phase(phase: Phase, team: &WorkerTeam, cancel: &Cancel, summary: &mut RunSummary) -> Result<(), RuntimeError>;

pub enum Partition {
    /// Each worker owns a disjoint set of planes; no merge.
    Planes(Vec<PlaneRange>),
    /// Each worker owns disjoint output regions (tiles with halo or row strips);
    /// a sample is routed to every region its support touches; merge in region order.
    Regions(Vec<Region>),
}
pub enum Residency { All, Waves { planes_per_wave: u32 } }

pub struct MajorCyclePass<'a> {
    pub operator: &'a MeasurementOperator,
    pub weighting: &'a WeightingGeneration,
    pub model: Option<&'a PreparedModelGrids>,   // None: initial (dirty/psf/weight)
    pub write_model_column: bool,                 // final pass only
    pub partition: Partition,
    pub backend: BackendChoice,                   // Cpu | Metal
}
pub fn run_major_cycle(pass: &MajorCyclePass, source: &mut dyn BoundedSource,
                       team: &WorkerTeam, cancel: &Cancel) -> Result<NormalImages, RuntimeError>;
```

One pass serves initial, residual and final major cycles for every
capability: read block → select, flag, phase-shift, route correlations →
build `SampleBlock` → (predict with `backend.degrid` and form V − A·m when a
model is present) → `backend.grid` into the worker's accumulator → merge →
`operator.finish`. Cube waves are `Residency::Waves` with the source restricted
to the wave's native channel window; the fast cube path becomes
`Partition::Planes` + `Residency::Waves` + `GridPrecision::F32`.

### 5.5 Deconvolution (`casa-imaging-deconvolution`)

```rust
pub struct MinorCycleView<'a> {
    pub residual: Planes<'a>,          // N_t terms × pols × planes
    pub psf: &'a PsfSummary,           // cached per PSF generation: peak, beam fit, sidelobe, Clark patch
    pub support: &'a BitMask,          // mask ∧ valid support, built once
    pub controls: &'a CleanControls,   // gain, niter, threshold, nsigma, cycleniter, cyclefactor, min/max psf fraction
    pub budget: Budget,
}
pub struct Controller { /* global, cycle and effective thresholds; inclusive flag; 1% check; divergence */ }

pub trait Solver {
    type State;
    fn initialize(&self, view: &MinorCycleView) -> Self::State;
    fn next(&self, state: &mut Self::State, view: &MinorCycleView) -> Option<Candidate>;
    fn accept(&self, state: &mut Self::State, c: Candidate, gain: f64) -> Update;
    fn finalize(self, state: Self::State, residual: &mut Planes) -> ModelDelta;
}
pub fn run_minor_cycle<S: Solver>(solver: S, view: MinorCycleView, ctl: &mut Controller) -> (ModelDelta, StopReason);
```

Solvers: `Hogbom`, `Clark` (active-list state), `Multiscale` (`ScaleBank`
with one term), `Taylor` (`ScaleBank` with N_t terms and per-scale Hessian);
image-domain multi-field Hogbom is the driver run over several views with a
shared threshold. Shared primitives: `peak_search` (masked, windowed, scored),
`patch_subtract` (clip | circular), `ScaleBank`, generalised `LinearRefresh`
(FFT or sparse direct) used by Clark, the multiscale terminal refresh and the
Taylor refresh. The controller is the one place for cycle threshold,
`nsigma`, `cycleniter`, inclusive accounting, the 1% convergence check and the
divergence rule (CASA stop codes 1–4, 6). Sparse refresh runs on the worker
team, not raw threads. CASA auto `cycleniter` semantics (#341) are part of
`Controller`.

### 5.6 AW and mosaic convolution functions

`AwCatalog::open_casa(path)` reads CASA `CFS_*`/`WTCFS_*` images into typed
cells on demand with an in-memory LRU bounded by the reservation.
`AwCatalog::generate_native(...)` (the existing native EVLA generation) writes
CASA-format CF images into `<cache>/cfcache/` so the same loader serves both.
No manifest, no payload hash, no eviction ledger. `MosaicPb` builds projectors
per (frequency, antenna-class pair) from the PB model and caches re-phased
kernels per pointing pair.

### 5.7 Products and normalisation (`casa-imaging-products`)

| Quantity | Rule (CASA) |
|---|---|
| sumwt | Σ W per plane/pol; AW: Σ W·|Σ taps|; MT-MFS uses term-0 sumwt for all terms |
| dirty/residual (standard) | FFT⁻¹ grid ÷ correction × N_x N_y ÷ sumwt |
| PSF | each plane ÷ max of term-0 PSF; peak = 1 |
| weight image (mosaic/AW) | gridded FT[PB²] at uvw = 0, FFT, Stokes convert, ÷ sumwt |
| PB | √weight ÷ pbmax, masked where > pblimit |
| residual, flatnoise (default) | ÷ (pbmax·√weight) where > pblimit·pbmax², else 0 |
| residual, flatsky | ÷ weight where > pblimit²·pbmax² |
| model prescale before degrid (flatnoise) | × √weight ÷ pbmax; undone after |
| pbcor | restored ÷ PB where PB > 0 |
| MT-MFS | each term ÷ term-0 sumwt and the same weight image; alpha and error products from terms |

The product list (image, residual, psf, sumwt, model, pb, mask, weight,
alpha, alpha.error, pbcor) replaces the product graph topology. The CASA
writer stages each image beside its target and promotes it with the existing
atomic rename/swap and parent fsync. Standard-gridder PB product and pixel
masks (#217) are rows in this table.

### 5.8 Request and compile (`casa-imaging-model`, `casa-imaging-application`)

`ImagingRequest` is one serde struct whose field names, choices and defaults
are the provider-contracts catalog entries; `casars-imager` fills it from the
resolved catalog values and nothing else. `validate()` runs once. `compile()`
returns `CompiledProblem` with no identity hashes except the AW cache key.
`availability::check(&CompiledProblem, &HostResources)` is the one gate for
capabilities that are not installed (Taylor-basis mosaic, Metal on
non-macOS, joint continuum-line). Capabilities that are rejected are not in
the catalog.

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
- worker-count invariance within tolerance (bitwise for CPU `Partition::Planes`;
  1e-6 for `Regions`; 1e-4 for Metal);
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
WCS, worker invariance, bounded memory (planned ≤ observed peak RSS × 1.1).

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
VLASS fragment (#625 serial bar 9,073 s), full-360 once finalised.

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
`tools/perf/imager/t51_pair_driver.py` and the stage-timing consumers are
updated to parse the structured log lines.

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
- Review per ticket: one contract review (owner or Fable) against the issue's
  outcome, deletion rows and section 10. Preferences cannot block; a cited
  rule or a failing acceptance test can.
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
`GridAccumulator`, `MeasurementOperator` (`accumulator`, `prepare_model`,
`finish`), `ImageCorrection`, FFT centring (one implementation),
`PolarizationRouting`, the single spectral resampler, `WeightingGeneration`
and `build_density_grid` (moved cell rules). T0 laws for `Spheroidal` on
`CpuBackend`. No production caller yet; this is the pattern ticket.
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
implementing `GridBackend` for both tap layouts, support-generic kernels,
one dispatch path (validate, map, commit, pending), one buffer ring, status
bits for errors. `BackendChoice::Metal` selectable from the request on macOS
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
reads and `eprintln!` in library crates; update `t51_pair_driver.py` and
stage-timing consumers. Acceptance: checker green; T1 with
`diagnostics.stage_timing` emits one line per phase.

**IF-10 Performance and memory pass (Opus with owner).** Regenerate T2 oracles;
run every T2 row before and after each optimisation; bars: within 10% of the
section 4.3 checkpoints for cube Metal W4, MFS pilot W1/W4/Metal, MFS 512²
W1/W4, and the #625 serial AW/MT-MFS bar; peak RSS at or below the recorded
values. Candidate optimisations only from measurement: bounded tap-plan
cache strategy, tile/lane ownership for wide-support Metal kernels, FFTW
threading in the driver. Acceptance: a results table in the umbrella issue;
no new abstraction without a measured win.

**IF-11 Docs and closure (Fable).** `ARCHITECTURE.md`, `TESTING.md`,
`docs/agent-reference.md`, `.agents/skills`, ADR index; delete
`casa-imaging-reconstruction` remnants and historical imaging-architecture
docs that this plan supersedes; final line and test counts; umbrella issue
closed.

Order: IF-0 → IF-1 → IF-2 → IF-4 → IF-3 → IF-5 → IF-6 → IF-7 → IF-8 → IF-9 →
IF-10 → IF-11. IF-5 may run in parallel with IF-3/IF-4 (different crate).

### 9.3 Review gates

Each gate is a scheduled review by the owner with Fable and (probably) OpenAI
Astra. The implementer prepares the inputs; the reviewers answer the
questions; the owner records the outcome as a dated section appended to this
document (`## Gate Rn outcome — date`), including any replanning. A gate that
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
  section 5.1's table); does `GridBackend` fit the Metal kernel plan; are the
  CASA-pinned rules placed in `key()` where IF-3 expects them; is the T0 law
  set strong enough to replace the fixture tests IF-2 will delete. Replan
  output: amended signatures in section 5.3, amended IF-2/IF-3/IF-4 bodies.
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
6. **Bookkeeping must not scale with data.** Telemetry and summaries are O(1)
   per phase. Nothing serialises, hashes or walks whole plans, models, sample
   streams or historical records for progress reporting.
7. **No one-use abstractions.** A trait with one production implementation, a
   factory that builds one thing, a wrapper that forwards every method, or a
   builder for a struct with public fields is removed. Keep a trait only where
   a second implementation exists in this plan (`GridBackend`: CPU/Metal;
   `ConvolutionFunctionSet`: four; `Solver`: four).
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
11. **File and function size.** A new or touched file over 1,500 lines or a
    function over about 100 lines is split along a domain boundary in the same
    ticket, unless the ticket names it as an accepted exception.
12. **Delete in the same change.** A replacement migrates callers and deletes
    the displaced code, tests, docs and resources in one PR. Nothing is kept
    "for reference" in the tree; git history is the reference.
13. **Tests test laws and contracts.** Prefer adjoint identities, conservation,
    tolerance against a reference, and resource bounds over fixture-specific
    expected numbers. A test that asserts an implementation detail is deleted
    unless that detail is a promised contract.
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
- Merge, cleanup and release remain owner-authorised actions. Closing the
  #486 family (D7) happens in IF-0 after this plan is approved.
- AGENTS.md is updated in IF-0 to point programme text at this plan and
  remove the #486-specific rules.
