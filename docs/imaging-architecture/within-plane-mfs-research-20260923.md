# Within-plane MFS CPU parallelism: research and proposed next slice

Truth class: non-normative research and proposal; not implementation authority
Last reality check: 2026-09-30
Work issue: [T55 / #541](https://github.com/bglenden/casa-rs/issues/541)
Source: `405d01adc58c6a4844c41924dbef62c984091492`

## Decision in brief

**Implementation checkpoint, 2026-09-30 (Linux focused checks only).** The
existing weighted-stream executor now schedules the empty-model, standard,
single-term Stokes-I MFS initial pass over disjoint 64-row strips of its shared
dirty and PSF grids. It retains uniform-density generation, multi-SPW selection,
the shared Clark controller and bounded residual replay. Routing storage is
bounded by the admitted sample batch; each standard stencil reaches at most
two strips, and workers borrow existing grid storage without private full grids.
W1 uses the same routed numerical kernel. Worker admission follows available
regions and the resource policy, including the existing replay compiler.

Focused checks compare W1/W4/W8 strip execution with the original scalar
single-term operator, including boundary crossings and flags. A small four-SPW
uniform-weighted Clark application case admits W1 and W4 in both major phases
and compares all emitted products at normalized error <= 1e-6, with exact mask,
shape and unit checks. These are correctness checks, not a timing result or a
replacement for the approved 4096-square workload and matched CASA comparison.
macOS execution remains unverified for this checkpoint. The historical
two-worker initial-consumer limitation below describes the parent revision.

**Forward-model preparation checkpoint, 2026-09-30.** A bounded follow-up
uses the existing four-SPW, uniform, 256-square Clark application test (24 rows,
8 channels/SPW), on Linux with the existing debug/test profile and unchanged
FFTW 3.3.10 static f32/f64 SIMD libraries. The existing stage trace switch now
separates gridded model preparation, execution including finish, and window
folding. These timings are per window and exclude earlier FFT preparation;
stream worker timers are nested within execution, not additive to it.

The measured residual replay processes 27 reduced prediction groups. In the
first current control, stream numerical execution was 0.86/0.73 ms for W1/W4,
versus 33.8/44.0 ms stream wall time. Instrumented model preparation was
48.7/37.8 ms, with execution including finish at 36.8/34.2 ms. The tiny fixture
therefore does not measure the target's visibility-throughput scaling.

`SpectralSlabOperator::prepare_forward_generation` formerly reconstructed and
validated a canonical `ModelCell` index for every pixel. It now validates both
ends of each row, then uses contiguous canonical indices within that row.
This retains model-window bounds, sample/support and domain-ownership checks,
correction arithmetic, FFTs and nonfinite rejection; no buffer or worker state
is added. At 256 square it reduces shape lookups from 65,536 to 512 per plane.
The old `fff9c2d` serial MFS path was inspected: it likewise prepared a model
grid before the paired prediction/residual operation. No old package is restored.

Observed model-preparation times in milliseconds (W1/W4): parent 48.7/37.8,
candidate 52.3/43.8; a subsequent parent control 69.4/35.9, candidate 40.8/34.4.
These are individual debug observations, with W4 following W1 in each process;
filesystem caches were not flushed and initial FFT planning has a cold-start
cost. They do not establish a speedup, and no release or full-workload claim
follows from the deterministic reduction in indexing work. The source change
uses the same path for W1/W4 and both operating systems; macOS is not executed.

The directly affected checks pass: the uniform multi-SPW W1/W4 application
comparison (all six products at normalized error <= 1e-6, exact masks/shapes/units),
nonempty-model continuation, and delta composition into the next major cycle.
Candidate build/application peak sampled aggregate RSS was 2,400,899,072 bytes;
the standalone candidate application control peaked at 73,056,256 bytes. These
small-fixture peaks do not establish 4096-square residency.

Evidence resides in `/workspace/casa-rs-tools/logs/mfs-refresh-*.log`; the
instrumented parent test executable is retained outside the repository as
`/workspace/casa-rs-tools/mfs-refresh-parent-test`. Builds use two jobs,
`CARGO_INCREMENTAL=0` and an 8-GiB aggregate RSS guard. The exact 4096-square
fixture is still unavailable in this executor, and a matched CASA reference
is still required for full numerical/visual and performance acceptance.

### Main-environment handoff (2026-09-30)

The development branch is `codex/mfs-cpu-optimization`, based on approved
`64ae6f1f6837bd6f690da17f7b0d1ebbe5a8579c` from
`codex/t55-full-size-validation`. Implementation checkpoints are `bbf54e6d`
(worker reporting/Linux setup), `2d17324a` (bounded shared-grid MFS regions),
and `14cd1754` (forward-model indexing and residual-stage timing). This is an
interim development checkpoint, not completed T55 acceptance. The `Source`
revision at the top identifies the historical research below.

Start in `SpectralCycleExecutor`, the initial weighted consumer in `weighting.rs`,
and `spectral_operator/initial_planes/mfs_regions.rs`. The uniform multi-SPW
application takes this spectral path; do not route it through the cube executor
or assume the legacy bulk MFS consumer is the running path. Residual refresh
uses the existing gridded-normal prediction/replay path. Requested/admitted
worker counts are not measured concurrency; use actual execution telemetry.

Focused commands, if relevant after subsequent changes:

```sh
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
export RUST_MIN_STACK=33554432 RUST_TEST_THREADS=1
cargo test --locked -p casa-imaging-reconstruction --lib initial_planes
cargo test --locked -p casa-imaging-application --test continuum_application uniform_multi_spw_mfs_clark_matches_serial_with_four_admitted_workers -- --exact --nocapture
cargo test --locked -p casa-imaging-reconstruction --test minor_cycle next_major_cycle
```

Apply the agreed 8-GiB aggregate RSS build/comparison guard around these
commands. Reuse existing green evidence for unaffected work. The Linux build,
linked CLI smoke, five initial-grid tests, uniform W1/W4 application comparison,
serial/Clark controls and two nonempty-model continuation tests passed. Clippy
and docs checks passed with existing warnings; broad `just verify` and macOS
execution were not run. Main-environment macOS validation is still needed,
with coordination before using resources occupied by the cube campaign.

The full fixture has not reached this cloud executor. Its original verified
location is
`/Volumes/GLENDENNING/casa-rs-evidence/t55/mfs-4096-workload-20260923/intermediate-90-evla-v1/pilot.ms`;
verify current availability before running. The transfer package is
`casa-rs-mfs-intermediate-90-evla-v1.tar`, 4,380,395,520 bytes, SHA-256
`ab1f07491217e5abf13ec38568a55948b6c2c82416465c4de462a0bcd6d4e7b7`.
It preserves 2,021,760 rows and 32 SPWs of 64 channels. Do not substitute a
new simulation. NAS/VPN/WebDAV setup is handled separately; credentials and
network configuration are not part of this branch.

After fixture verification, use the existing opt-in
`t55_mfs_pilot::full_field_application` harness in `continuum_application`.
Set `CASA_RS_MFS_MS` to the verified MS, `CASA_RS_MFS_OUTPUT` to a fresh durable
directory, `CASA_RS_MFS_WORKERS=4` (then 1 for the control),
`CASA_RS_MFS_TERMS=1`, `CASA_RS_MFS_GRIDDER=standard`, and
`CASA_RS_MFS_NITER=10000`. Build before timing with the 8-GiB build guard; execute
the test binary with the 16-GiB aggregate native RSS guard and check planned
admission against that same cap. Preserve all rows/channels, 4096 square,
0.05 arcsec, the intended 6-GHz reference, global uniform weighting, Stokes I,
Clark gain 0.1, 5-mJy threshold and 1000 cycle iterations. Verify the resolved
reference frequency and matched CASA request instead of assuming defaults.
The harness's shape/unit-PSF smoke is not the full scientific comparison.

Reach a complete W4 application run, obtain W1 and matched CASA serial controls,
then use dominant measured costs to choose the next intervention. Compare every
required product numerically and visually, retaining approximately 1e-3
normalized agreement and stricter existing checks. The historical CASA
415.481-second W-projection/nterms=2 run is not a reference for this milestone.
Keep FFTW precision/SIMD linkage matched, batched Clark, bounded input/grid
residency and the common W1/multiworker scientific path. ADR-0014 publication
attestation remains prohibited. No Metal, MT-MFS, W/AW, mosaics, cube tuning,
restart of the stopped full512 campaign, release or cleanup is implied.

The local handoff archive `/workspace/mfs-handoff-14cd1754.tar.gz` preserves the
three implementation commits, bootstrap scripts and essential logs through
`14cd1754`; it predates this documentation handoff and is not stored in Git.
Cloud setup scripts/tools and raw logs are workspace-local and do not accompany
a branch fetch. On macOS use the existing development setup; on Linux follow
`TESTING.md` for workspace-backed spill storage. Preserve the original workspace
until any separately requested archive transfer/restoration has been verified.

**Owner update, 2026-09-24: simple imaging baseline.** Reformulate the existing
90-time/configuration, DATA-only intermediate as single-term MFS, Clark CLEAN,
standard gridding, Stokes I and uniform weighting. Preserve all selected rows
and channels, 4096-square / 0.05-arcsec geometry, gain 0.1, 5-mJy threshold,
1000 cycle iterations and 10000 total iterations. No W-projection, Taylor
coupling, facets, additional data columns or new simulator run. The existing
sky is chromatic and contains nonzero w; this is a matched-implementation
baseline, not exact intrinsic-sky recovery. Both CASA and native harnesses now
default to this simple gridder; the old advanced case remains explicitly
selectable and its evidence is retained.

A production-plan-only probe gives an initial native reservation of
8,889,167,767 bytes (8.279 GiB) for W1, versus 18,978,355,077 bytes (17.675 GiB)
for the previous W32/two-term case. These are reservations, not measured
imaging RSS or complete-run peak predictions. A four-worker request produces
only two-worker initial consumers plus a one-worker alternative; this is not
four-way gridding. The source comparison with LibRA and Obit is recorded in
`mfs-4096-workload-20260923/simple-memory-20260924/memory-comparison.md` under
the durable evidence root. No simple-case imaging/timing or CASA acceptance
has run yet. This update supersedes earlier next-action/status text below;
historical workload and rejected-run evidence remain intact.

**Owner clarification, 2026-09-24: numerical acceptance, not identical bits.**
Do not spend memory or execution time solely to make floating-point results
bit-identical across worker counts, reduction orders or compiler optimization
settings. Ordinary rounding variation is acceptable under the existing
scientific accuracy tolerances. Remove reproducibility-only accumulation
machinery and corresponding exact-equality requirements; retain scientific,
shape, mask, metadata, ownership and I/O checks. Extra precision or compensated
summation needs a demonstrated accuracy purpose, not an assumed bitwise
reproducibility requirement. The 8.279-GiB census describes current code before
that removal; it does not establish that those costs are required.

**Owner update, 2026-09-23: dataset first.** Brian challenged the previous
approach and requested a substantial input with a source-rich image around
4096 square, then explicitly confirmed 4096 as realistic. The image size is
therefore fixed at 4096 x 4096. The next deliverable is a scientifically suitable workload and
its sky/UV/beam checks, not an FFT implementation. The FFT-first recommendation
below is an earlier conditional hypothesis, not a committed optimization order.
Choose the intervention from the complete application's measured costs on the
new workload; do not design the workload to make FFT parallelism look good.

Extend the existing reconstruction-owned MFS operator through the existing
application phase interface. Do not force MFS into the cube implementation,
restore the old imager, or create another CLEAN controller. The desired unit of
parallel ownership is **a bounded portion of one grid**, not an output image.

There are two distinct questions: parallel coverage of the complete application,
and grid residency at large image sizes. Current residual replay already has
within-image parallelism. Initial MFS gridding and within-image FFTs do not have
the corresponding coverage. The existing tiled replay also retains substantially
more than one full-grid equivalent, despite being independent of worker count.
These are source observations, not a measured current MFS bottleneck ranking.

**Earlier candidate, conditional on the new workload: within-plane FFT scheduling only**, connected
to the complete application. Keep current initial gridding, prediction,
tile/shard accumulation and reduction order. First measure their existing
complete-application phase shares on the approved MFS selection; implement the
FFT experiment only if its optimistic end-to-end saving is useful. Do not bundle
new gridding ownership, transpose algorithms and residency redesign into it.
The exclusive-region design below is the conditional next architecture, not
an instruction to implement all of it now.

The initial workload preparation added a diagnostic script, focused tests and
sky/UV previews. The subsequently approved generation pilot now exercises CASA
and the current native application; its bounded observation-label repair is
described below. No optimization campaign, installation, commit, push, merge or
cleanup is part of this pilot. The closed channel-276 near-tie investigation
stays closed; efficient batched Clark updates stay unchanged. Pre-existing
`work/` is preserved.

## Generation pilot: application preflights

**Owner clarification, 2026-09-23:** proceed with the 90-time/configuration
intermediate as an explicitly idealized matched-application fixture. The
component-center PB and time/channel-center approximations do not block this
comparison: both imagers consume identical DATA. The measured 0.07146-arcsec
PB-weighted centroid shift is a rendered-sky calculation, not an observed
reconstruction error. Physical finite-bin/intrinsic-sky truth acceptance is
not claimed. This supersedes the earlier prerequisite below to resolve those
approximations before larger comparison generation; no oversampled simulation
or new forward-model implementation is included in this intermediate.

The [CASA fixture driver](../../tools/perf/imager/mfs_4096_pilot.py) creates
separate A/C observations, concatenates them, flags the approved channel edges
and predicts the analytic sky. The
[native diagnostic](../../crates/casa-imaging-application/tests/continuum_application/t55_mfs_pilot.rs)
calls the existing `execute_continuum` entry with explicit one/four-worker and
16-GiB planning settings. It does not add a new imaging implementation.

* The 36-time-sample/configuration pilot has 808,704 rows, 72 distinct times,
  2-second exposure/interval, 32 SPWs and 54 antennas. CASA concat produces 63
  valid DDIDs referencing the 32 SPWs; the fixture now follows the explicit
  DATA_DESCRIPTION mapping rather than assuming DDID equals SPW ID.
* Its actual CASA uniform/W-projection PSF at 4096 square has a fitted beam
  0.21208 x 0.20698 arcsec (4.24 x 4.14 pixels). This PSF-only task took
  27.31 seconds and the guarded process peaked at 2.40 GB. This is not a
  deconvolution or performance-parity result.
* Measured storage is about 4.8 GiB for the concatenated pilot MS, plus 2.4 GiB
  for each retained source configuration. CASA includes MODEL_DATA and
  CORRECTED_DATA as well as DATA. Thus the full concatenated fixture would be
  roughly 48 GiB with this storage layout, not the 18.56-GiB basic-payload
  estimate. Retaining source MSes would approximately double that. No full
  fixture has been generated. **Owner correction:** retain only DATA. Removed
  MODEL_DATA and CORRECTED_DATA from the corrected EVLA smoke's three MSes after
  all imaging handles closed. Its concatenated MS shrank from 282,341,376 to
  98,013,184 allocated bytes; row counts and three sampled DATA cells per MS
  were unchanged. That projects to about 17.6 GB for the full concatenated MS,
  subject to measured tile overhead. The driver now performs this removal
  automatically after CASA prediction, which internally writes scratch columns.
* A two-time/configuration, full-band smoke contains 44,928 rows. It first
  exposed an application rejection of multiple OBSERVATION_IDs solely for
  image telescope/observer labels. The bounded repair permits matching labels
  across observations and retains rejection of conflicting labels. The
  production-path regression verifies publication and both conflict cases.
* The next native preflight rejected the legacy `VLA` beam model at 6 GHz.
  CASA's prediction log also showed legacy VLA_NVSS/Airy selection. The modern
  WIDAR fixture must use CASA's **EVLA** telescope identity and beam model.
  The driver is corrected; original VLA-labelled datasets/results are retained
  as rejected input-model evidence, not silently relabelled or accepted.
* The corrected EVLA smoke completes the real 4096-square uniform/W-projection
  dirty imaging and publication path in both applications. CASA task time is
  16.021 s; casa-rs W1 application time is 25.017 s, sampled native peak 4.83 GB.
  These are single smoke measurements, not the full benchmark. Full-field
  residual maximum difference is 5.96e-8 Jy/beam (relative L2 3.62e-7); PSF
  maximum difference is 7.75e-7; sumwt agrees exactly. Peaks coincide. The PB
  initial difference was structured and reached 0.0031014 absolute response;
  the PSF/PB correction below supersedes that discrepancy. The `comparison-dirty-v2`
  panels have been inspected. Only two timestamps per configuration were used:
  their strong sidelobes are not evidence about the full track's sky recovery.
* CASA's component predictor evaluates time/channel centers and applies the
  beam at component centers. It does **not** implement finite 2-second/2-MHz
  averaging or spatial PB variation across an extended Gaussian. Current
  prediction/image runs are explicit application smoke tests. Resolve these
  approximations, together with the Taylor-order/deep-clean contract, before
  promoting any fixture to full sky-truth acceptance.

Durable pilot source snapshots, binaries, commands, guard results and failures
are in `mfs-4096-workload-20260923/pilot-runs/` under the evidence root below.
The original frozen measures snapshot was missing IERS payload files; a fresh
snapshot from intact installed CASA data is preserved at `pilot-runs/reference/`.
It does not overwrite or repair historical evidence. All CASA stages use that
snapshot with auto-updates disabled. Resource limits remain 8 GiB for CASA/setup,
16 GiB native planning and sampled RSS, and two Cargo build jobs.
The current restart point is the DATA-only `smoke-evla-v1/pilot.ms`, not the
rejected legacy-VLA inputs. Before larger prediction/deep CLEAN: resolve the
finite-bin/extended-component forward-model
approximations, then settle Taylor order and thresholds. No process is left
running at this checkpoint. Focused metadata publication/conflict checks,
single-observation/pointing controls, eight Python fixture checks, Ruff,
`cargo fmt --all --check`, and `just docs-check` passed; this is not full T55
or new-workload science acceptance.

### PSF/PB normalization repair, 2026-09-23

At the owner's direction, product catalog v10 publishes each ordinary valid
PSF plane with an exact unit peak, independently by channel, polarization and
domain. The common publication path covers MFS, streaming cubes, W/AW projection,
mosaics and facets. Taylor/MVC moments and joint normal blocks share the peak of
their principal term; independently normalizing higher terms would corrupt their
relative amplitudes. Empty planes remain zero. Raw normal states, CLEAN and the
restoration Hessian are unchanged; Taylor publication scaling is fused into its
existing payload copy.

EVLA PB evaluation now follows CASA's upper-frequency midpoint tie, Float radial
table indexing and Float voltage squaring. The frequency fix alone left 680
radial-bin outliers; source-matched arithmetic explained every one exactly.
The final full-resolution 4096-square rerun has PSF peak exactly 1, maximum PSF
difference 1.1921e-7, and **zero PB difference** across all 16,777,216 pixels.
Residual/model/restored/sumwt pixels, masks, coordinates, beams and units are
bit-identical or exactly equal to the pre-fix native smoke. Native W1 application
time is 24.938 s with 4.831 GB sampled peak RSS (prior 25.017 s; no speed claim).

Verification: 53 product tests and 50 application tests pass, including
one/two/four-worker exact cube products. One existing channel-window fixture
fails identically on the saved pre-fix binary: its 9,534,052-byte budget cannot
admit an 11,668,298-byte plan. It is explicitly excluded, not weakened or fixed
as part of this repair. Nine external/diagnostic application tests remain ignored;
the 4096-square pilot was run explicitly. Formatting, Ruff and docs checks pass.
The DATA-only fixture's stale owner keyword was archived and reinitialized using
the standard owner initializer after the earlier authorized column removal;
visibility data were not changed.

Durable details: `mfs-4096-workload-20260923/pilot-runs/psf-pb-normalization-report.md`;
final source/binary: `source-psf-pb-v2`; final output: `native-dirty-w1-psf-pb-v3`;
panels: `comparison-psf-pb-v2`. This fixes publication normalization, not full
new-workload CLEAN, sky-model, W4 performance or T55 acceptance. No push/merge.

## Initial workload checkpoint: sky and geometry prepared

The [preview script](../../tools/perf/imager/prepare_mfs_4096.py) and
[focused checks](../../tools/perf/imager/test_prepare_mfs_4096.py) implement
the proposed recipe. At this initial checkpoint there was no generated MS or
CLEAN result; the generation pilot above supersedes that status.
Durable scripts, array snapshots, commands, logs and images live under
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/mfs-4096-workload-20260923`.
Use `preview-ac-v4/recipe-and-checks.json`, `sky-preview.png` and
`uv-psf-preview.png`; v1--v3 are retained preliminary records.

* Geometry: 4096 x 4096, 0.05 arcsec pixels, 204.8 arcsec field, J2000
  12h +30 degrees. Actual CASA `vla.a.cfg` and `vla.c.cfg`, 27 antennas each,
  physical baselines 793--36,623 m and 78--3,387 m. These specific configurations,
  rather than the generic observatory table, define the fixture. Their ranges
  overlap; no A+C coverage hole requiring a third configuration was identified
  in this preliminary diagnostic.
* Sampling, revised after owner feedback: **360 separate 2-second integrations
  per configuration, evenly spanning HA -3 to +3 h**. Centers are about
  60 seconds apart, but the exposure/averaging duration remains 2 seconds,
  never 60 seconds. This replaces the v1--v3 clusters of twelve one-minute
  scans. Total exposure remains 24 minutes across both configurations and row
  count is unchanged. Use separate epochs with correct antenna positions,
  not simultaneous arrays sharing one antenna table. The actual CASA PSF
  still needs checking; denser HA samples do not prove deconvolution fidelity.
* Sensitivity: explicitly **noise-free** for the initial algorithmic fixture,
  with finite equal weights on unflagged samples, not infinite inverse-noise
  weights. It is a time-sampled synthesis test, not a claim about the sensitivity
  of a real 24-minute observation of this sky. Preserve channel/time averaging,
  PB and w physics. No noise has been removed from an existing dataset.
* Sky: **79 point sources and 12 extended complexes**, built from 325 analytic
  point/Gaussian components: rings, jets, curved filaments and diffuse emission
  with compact knots. Intrinsic total flux is 8.7266 / 6.6948 / 5.5990 Jy at
  4/6/8 GHz, with component-dependent power laws. The display uses a common
  0.35-arcsec smoothing beam, not a fitted restoring beam. The field contains
  the rendered analytic flux to better than 0.0001%; compact sources extend
  to about 88 arcsec radius.
* Channelization: 32 x 128-MHz SPWs, each 64 x 2-MHz channels, RR/RL/LR/LL.
  Nominal baseband centers 5 and 7 GHz give 3.976--8.024 GHz coverage with
  48 MHz overlap. Flag two channels at each SPW edge and channels outside
  4--8 GHz: 1,900 unflagged stored frequency channels, 1,880 unique centers.
  This is an explicit simulator tuning on a real WIDAR resource structure,
  **not an exported RCT resource or observed dataset**. Keep LSRK as the
  approved simulation convention; do not relabel an existing TOPO dataset.
* Size: 8,087,040 cross-correlation MS rows, 2,070,282,240 stored complex samples;
  16.56 GB DATA, approximately **19.93 GB (18.56 GiB)** basic column payload.
  This excludes table/tile overhead and MODEL_DATA/CORRECTED_DATA duplicates;
  flags may be packed. Stokes I uses 960,336,000 unflagged parallel-hand samples,
  not all four stored correlations. Measure actual disk size in the generation
  pilot. Checked headroom: 115 GiB internal and 1.2 TiB external.
* Beam/field: nearest-cell, SPW-center geometric uniform-density axis-cut FWHM
  is 0.216/0.222 arcsec (4.3--4.4 pixels), versus 1.42/1.22 arcsec under natural
  density weighting. The latter has a broad pedestal, so natural weighting
  is not the intended high-resolution endpoint. These are **not CASA fitted
  beams**. At 90 arcsec, rectangular channel/time averaging worst-baseline
  peak-loss bounds are 1.86%/0.635%; ignored sampled w phase reaches 0.360 rad.
  Gaussian PB response is about 0.797 at 8 GHz. Preserve wide-field and
  chromatic-PB effects, including spectral-index bias.
* Spectral-model check: least-squares fits to the component power laws on the
  selected frequencies give worst relative errors 12.39% / 2.71% / 0.561%
  for 2/3/4 Taylor terms. These are spectral fits, not measured MT-MFS image
  errors. Do not mistake their unmodelled curvature for a code defect. Keep
  a separate flat-spectrum one-term control; select the coupled case's Taylor
  order and truth tolerances before its deep-clean acceptance.

**Next:** a small CASA generation/PSF pilot with this sky, SPW identities and
real configuration positions. Verify frames, scan/antenna association,
finite weights, averaging and the PB/w model; measure storage per sample and
inspect the actual weighted beam. Fix the deconvolution threshold and
Taylor-order acceptance there before bulk generation. The native simulator
currently exposes one SPW per request; use a fixture driver and existing tools,
not a speculative public-API expansion. No bulk MS generation or performance
campaign has started. The 16-GiB native and 8-GiB diagnostic limits remain;
the old full-32GB dataset is unrelated and still excluded.

The performance skill kept this at the representative-workload boundary; the
radio corpus review prompted explicit sparse-track, smearing, w/PB and Taylor
caveats. V1 produced figures but failed serializing a NumPy float32 beam width;
an explicit Python-float conversion and regression test repaired the diagnostic.
Failed and successful records are retained.
Final preview v4 completed in 7.64 seconds with sampled peak aggregate RSS
2.02 GB under the 8-GiB diagnostic guard; six focused tests, Ruff and
`just docs-check` passed. That time is preview generation, **not imaging**.

## Revised workload proposal: substantial, source-rich 4096-square continuum

Read-only inspection on 2026-09-23 found that simply enlarging the old control
would not satisfy the owner's intent:

* `issue607-standard-mfs-representative.json` selects 24 channels, a 512-square
  image and only 25 Hogbom iterations. Its stored simulation request starts at
  8 GHz with 2-MHz channels; `du -sk` reports 1,088,364 KiB (about 1.04 GiB) for
  the existing small MS. These are the inspected files and allocated disk size,
  not a new measurement of selected unflagged samples.
* The Wave 1 source generator has a 512-square structured sky with arms/ring/
  halo and three Gaussian compact features. Its cube generator multiplies the
  same spatial plane by a channel-dependent scalar; it does not supply a rich
  spatially varying broad-band spectral-index truth model. Keep it as a
  regression control, not the new large-image performance benchmark.
* The C-array Q-band fixture was designed for line-plus-continuum cube work at
  1024 square over 44--45.022 GHz. Its interesting line morphology is not an
  adequate substitute for a broad-band continuum/MT-MFS truth sky.

Evidence: [control manifest](../../tools/perf/imager/workloads/issue607-standard-mfs-representative.json),
[registry](../../tools/perf/imager/wave1_dataset_registry.json),
[Wave 1 source generator](../../tools/perf/imager/stage_wave1_datasets.py),
[C-array source generator](../../tools/perf/imager/stage_t55_c_array.py).
The existing on-disk request and MS are under
`/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/small/`.
No existing input has been changed.

The 4096-square image size is owner-approved. Other design criteria below are
proposed, not an already approved or generated observing manifest:

* **4096 x 4096 output**, with roughly 4--6 pixels across the narrowest measured
  synthesized beam. Choose frequency/configuration/cell together. Distribute
  emission through a substantial usable field; neither empty padding nor
  excessive oversampling counts as a representative large image.
* **A populated deterministic continuum sky:** roughly 50--100 unresolved or
  barely resolved sources, plus 10--20 structured extended complexes including
  jets/lobes, filaments, rings and diffuse emission. Include several bright and
  many fainter features with varied positions and orientations, not copies of
  one Gaussian. These counts are proposed construction ranges, not acceptance
  thresholds or claims of an observed population.
* **Recoverable scales:** extended structures should span several to tens of
  synthesized beams, with enough short baselines to constrain them. The
  primary-beam field and largest recoverable scale are different constraints.
  Use actual VLA configurations and, if necessary, a justified combination;
  inspect UV coverage and the PSF before freezing the choice. Check frequency-
  dependent PB, time/channel averaging and the w term; do not force a standard
  gridder or narrow field merely to match the earlier proposal. NRAO's
  [resolution guidance](https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/resolution)
  and [field-of-view guidance](https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/fov)
  describe these independent limitations.
* **Substantial actual input:** aim initially at several hundred million
  correlation-channel samples and a roughly 5--15-GiB MS, with sustained
  hour-angle coverage and scientifically suitable time/channel resolution.
  Record actual rows, channels, correlations, unflagged sample count and bytes;
  do not inflate file size using unused columns, repeated rows or duplicated
  channel data. As arithmetic only, 1.5 million rows x 128 channels x two
  correlations is 384 million complex samples, or 3.072 GB for Complex32 DATA
  alone; total MS size depends on the stored columns and cannot be inferred
  from that number. Exact design may change after this accounting.
* **MFS and MT-MFS controls are distinct.** Use a flat-spectrum or otherwise
  explicitly defined one-term control, and a genuinely broad-band case with
  spatially varying known spectra for coupled MT-MFS. Account for Taylor
  approximation error and the chromatic beam. Do not confuse unmodelled source
  spectra with an implementation failure or reuse the line-cube exception rule.
* **Real deconvolution:** the initial noise-free sky has enough flux/structure
  for deep cleaning and repeated visibility-domain major refreshes. This is not
  a real-observation sensitivity claim. Stop at a scientifically specified
  residual threshold with a safety iteration limit, rather than a token 25-
  component run or inflated iteration count. Record residual convergence,
  component/major counts and recovered morphology/flux.

Next sequence: specify the exact observing/sky manifest and storage estimate;
inspect truth panels, UV/beam and smearing checks; then obtain the matched
complete CASA and native W1/W4 measurements under the existing resource limits.
Choose the optimization only after those results. A small smoke fixture may
validate generation, but cannot become the performance endpoint. Generating
the new MS or starting imaging has not been done by this read-only assessment.
The no-full-32GB, no additional Obit install and no push/merge/release/cleanup
boundaries remain. Preserve historical inputs and acceptance evidence.

### Native spectral setup must correspond to a real VLA resource

Brian additionally requires explicit, realistic MS channelization. Proposed
continuum parent setup: VLA C band, nominally 4--8 GHz with the 3-bit samplers,
32 spectral windows of 128 MHz each, 64 channels per window at 2 MHz, full
RR/RL/LR/LL correlations. That is **2048 frequency channels per correlation**
across the windows, not 2048 channels in every MS row and not 8192 distinct
frequencies. Each cross-correlation MS row belongs to one DATA_DESC_ID/SPW and
contains its 64 x 4 samples. This proposal fixes the correlator structure for
the recipe discussion, not the yet-unverified exact tuning or configuration.

NRAO's [resource guidance](https://science.nrao.edu/facilities/vla/docs/manuals/obsguide/presubmission-checklists)
specifies 32 subbands for C/X 3-bit continuum and distinguishes integration
defaults by array. The
[OPT manual](https://science.nrao.edu/facilities/vla/docs/manuals/opt-manual/referencemanual-all-pages),
under NRAO Defaults / Wide Band Continuum Resources, explicitly describes
64 channels per 128-MHz full-polarization window (its worked example's total
window count is K-band-specific, not our C-band count). The sum
of nominal subband widths is 4.096 GHz; this is not a claim of 4.096 GHz unique
usable sky coverage inside a nominal 4--8-GHz receiver band. Copy verified
baseband/subband tunings and record overlap, band-edge exclusions and flags
before generating the MS. Explicitly specify the actual frequency reference
frame and its conversion; do not relabel coordinates merely to match a frame.

Any reduced-resolution imaging MS must be labelled as an averaged derivative
of that parent setup. Set the averaging from the longest baselines and furthest
science source's allowed bandwidth/time smearing, retaining SPW identity and
actual channel widths. Do not silently replace this with an arbitrary total
of 128 or 256 channels to fit a target file size. The earlier 5--15-GiB estimate
is provisional: realistic channelization and track length may require a larger
input and a separately agreed resource/storage decision. No such expanded run
is authorized or launched by this proposal.

## Authority and evidence boundaries

The live top section of [#541][t55] explicitly requires within-one-image MFS and
coupled MT-MFS, complete-application W1/W2/W4 evidence, reuse of #581/PR #585,
unchanged scientific checks, and no per-worker full grids. It requires the
workload, numerical performance acceptance, and resources to be recorded before
a campaign. The cube's revised >=2x criterion is **not** an MFS criterion.

[#625][t625] is a separate, queued AW/MT-MFS full-dataset science and serial-parity
obligation: 4096-square, 63 fields, 16 SPWs; frozen CASA 9073.69128 s; its own
32-GiB ceiling. [#449][t449] retains separate frozen VLASS four-row 10x and final
warm-run obligations. Neither is activated, satisfied, or weakened here. This
proposal uses the current task's **16-GiB native aggregate planning/RSS limit**,
8-GiB build/diagnostic guard, two Cargo jobs, and no full-32GB run. A short new
diagnostic does not replace either ticket's final selection or run protocol.

Live checkout verification: branch `codex/t55-full-size-validation`, HEAD
`405d01adc58c6a4844c41924dbef62c984091492`; tracked source clean, `work/` untracked.
The current work summary records the pushed source checkpoint and accepted
C-array v3 result. Those cube results are not evidence of MFS speed.

## Actual application path and reusable code

| Phase | Current executing owner | Consequence for the proposal |
| --- | --- | --- |
| Selection/request | `execute_continuum` prepares, then `execute` resolves/compiles/validates the selected observation. | Retain selection, weighting, flags, coordinates and capability checks. |
| Application lifecycle | `run_native` selects `CubePhase` only for its supported channel-local capability; MFS/Taylor use `SpectralCycleExecutor`. Both implement `MajorCyclePhase`. | Extend the MFS implementation behind this existing seam; do not introduce a public mode switch or error fallback. |
| Initial imaging | `run_stream` consumes bounded weighted chunks and can overlap science with replay compilation. `InitialPlaneBatch` only supports certified-zero channel-local, non-AW/non-mosaic work. | MFS/Taylor cannot inherit cube initial-plane parallelism simply by requesting four workers. Preparation overlap is not within-plane gridding. |
| CLEAN | One loop in application `lib.rs` consumes reconstruction completion, controls continued cleaning and requests the final major refresh. | Preserve the existing Clark/MT-MFS controller and component-order dependencies. |
| Residual refresh | `run_gridded_replay` binds the model/prior state, executes bounded frozen replay, then returns ordinary scientific completion. | Preserve the proven `dirty - A* W A model` computation and reusable PSF/normal state; do not require rereading the original MS each major. |
| Publication | `publish_products` admits existing product windows and the serial CASA-compatible sink. | Keep bounded preparation/restoration and ordered writing, masks/WCS/beams and individual-image atomic replacement. |

Source locations: [request][request], [application and CLEAN loop][app],
[phase interface][phase], [cube eligibility][cube-eligibility],
[runtime initial and refresh][runtime], [initial-plane eligibility][initial],
[publication][publication]. `GridThreads` exists as a historical task requirement
but is not in the installed supported-task list; use the existing explicit
resource worker policy rather than assume an old flag activates a path
([availability][availability]).

The current replay is not a blank slate. It has four worker-count-independent
prediction lanes, then four accumulation lanes. It classifies a bounded record
window, counts/prefix-routes records into tiles, subdivides hot tiles into at most
four shards with at most three additional shards globally, assigns by tap load,
and merges in canonical tile/shard order. Prediction reads shared forward grids.
Taylor records preserve their moment width. Existing tests check identical work
and commit identities and worker-independent grid residency. The production
merge is ordered but visits all active tile/shard accumulators on the committing
thread. Its storage pool is bounded by the **window and tile catalog**, not by
the number of workers. [Routing/merge][routing], [layout][layout], [tests][tests].

`PreparedFft` currently gathers every axis lane into one scratch lane, transforms
it, then scatters it back; both axes and the surrounding shifts are serial.
Forward-model construction and residual completion loop through term grids
serially. Parallelizing independent cube planes does not parallelize these
operations on one MFS image. [FFT][fft], [forward model][forward],
[residual completion][finish].

## Upstream mechanisms: what transfers, and what does not

### CASA / libRA

Inspected libRA revision: `0ab99e261878334d6588eafa360cef3b673e897f`, local fork of
ARDG-NRAO/LibRA. Its tracked source is unchanged apart from an unrelated deleted
Python cache file. CASA science was checked using `git show` at
`61020062cee290f5466cffed5ec5032e0c7a3434`, not the locally instrumented working
files.

* libRA `GridFT::put` prepares row geometry in parallel and dispatches sectors
  into **one shared grid**. The Fortran kernel clips each stencil to its owned
  rectangle. It scans the visibility buffer for each sector; this is a useful
  ownership example, not a recommendation to reproduce repeated scans.
  Sum weights are separately accumulated/reduced. `get` assigns visibility row
  ranges that read the shared forward grid. [GridFT][libra-grid],
  [clipped kernel][libra-fortran].
* `MultiTermFTNew` makes T model/residual terms and 2T-1 PSF moments. Prediction
  sums term contributions before returning model visibilities; gridding loops
  terms with spectral weights. This is coupled science, not independent CLEANs
  or evidence that its outer term loop scales. [libRA Taylor][libra-taylor],
  [pinned CASA counterpart][casa-taylor].
* `FFT2D` can request threaded FFTW. Any future Rust FFT work should use the
  already admitted team or an explicitly charged exclusive FFT team, not nest
  W FFT threads inside W workers. This research does not propose an FFTW
  dependency. [libRA FFT][libra-fft].
* libRA also contains a different `MultiThreadedVisibilityResampler` with
  per-resampler grid storage and a full-grid gather, and HPG-backed device
  gridding with bounded visibility buckets. Neither GPU/distributed results
  nor the replicated-grid path establish an acceptable laptop CPU architecture.
  [replicated gather][libra-replicas], [HPG][libra-hpg].

### Obit

The [developer site][obit-home] points to Bill Cotton's upstream repository;
the live master pin is `ebc1c229e5e3870b5ce3c342bddb7313d986a06f`.
Existing [local source notes](obit-data-structure-source-notes.md) distinguish
flat shared visibility buffers from private gridding arrays. Its previously
recorded dirty/CLEAN timings concern a cube and different products/precision;
they are **not an MFS scaling baseline**. No installation is needed to examine
the source.

## Proposed ownership and numerical contract

The target is a deep reconstruction module with simple numeric buffers behind
the existing phase interface, not more caller-visible stages:

1. **Shared bounded input.** Retain the existing weighted-source and frozen
   replay contracts. Decode/precompute geometry once per bounded block. Keep
   numeric arrays and borrowed views; do not recreate per-sample object graphs,
   copy an entire MS, or duplicate CF payloads. Freeze row/channel/tap facts at
   their scientifically valid lifetime, not across changed selection/weighting.
2. **Prediction ownership.** Workers own disjoint complete prediction groups,
   sharing immutable model grids for the current model epoch. Preserve group
   coupling across correlations/domains/Taylor terms. Only bounded predicted
   values are exchanged with accumulation.
3. **Output ownership.** The preferred large-grid design has non-overlapping
   output regions of shared value and compensation arrays. Route a small record
   reference to every region intersected by its support; each owner updates only
   its cells. Clip work, **not the mathematical kernel or its normalization**.
   Do not assign solely by stencil centre and lose boundary taps. For standard
   support narrower than a rectangular tile, at most four tile memberships are
   needed; W/AW support requires the actual intersection bound, not this shortcut.
4. **Order and skew.** Stable count/prefix/scatter retains source order in each
   cell across bounded blocks; logical regions do not change with W. Scheduling
   can vary without changing that order. No cell atomics or locks in the hot
   stencil loop. Fine regions and tap-cost scheduling mitigate central-UV skew;
   they cannot parallelize every contribution to one hot cell. A bounded hot-tile
   shard exception would require an explicit fixed reduction tree and measured
   benefit, not a hidden return to replicated full grids. Initially use a block
   barrier; do not invent a complex pipelined scheduler.
5. **Taylor coupling.** Keep `BlockNormalPlan`: T coefficients and 2T-1 moments,
   with normal block entry `(a,b)` using moment `a+b`. Reuse the current rounding,
   weights, flags and conjugate/normalization kernels. Term waves can bound
   storage, but must include **every cross term** before the existing coupled
   minor solve. Inverse transforms may be scheduled independently; choosing
   separate CLEAN components per term is not valid. [algebra][algebra].
6. **FFT and image work.** Parallelize disjoint rows/lanes and correction blocks
   with an axis barrier on the admitted team. Prefer bounded blocked gathers or
   in-place blocked transpose over an uncharged second full grid. This changes
   traversal, not the transform convention or precision. Keep component selection
   and MT-MFS solves coupled; only legally independent pixel work may parallelize.
   Reuse existing product preparation and writer; do not replace them merely
   because publication has some serial work.
7. **Major-cycle reuse and deletion.** Reuse weighting, PSF/normal state and
   geometry/replay; rebuild only model-dependent prediction. Integrate initial
   imaging and subsequent refresh through the same reconstruction kernel surface
   where their algebra agrees. Replace covered tile/merge or serial dispatch code
   after acceptance; comparison code is temporary, not a second production route.
   Do not prematurely delete owners still serving W/AW, mosaic or other bases.

Changing the accumulation tree can legitimately differ from the old code while
remaining deterministic across W; it must pass the existing science checks and
be described honestly. Bitwise equality with an old tree is not assumed. Avoid
fast-math, precision changes, revised tolerances, new attestation, or extra
verification-only array passes in this work.

## Memory and data movement: calculated, not measured

Let `P=Nx*Ny`, `G=Gx*Gy` after the existing padding, `T` be coefficient count,
and `B=16G` bytes for one `Complex64` grid. Current standard padding is 1.2,
rounded to an even 2/3/5-smooth length; shapes below apply to that standard
one-domain, Stokes-I case, **not automatically to AW/VLASS geometry**.
[allocation][allocation], [padding][padding].

| Output side | Padded side | One f64 image (GiB) | B (GiB) | One private grid each: W1 / W2 / W4 (GiB) |
| --- | --- | --- | --- | --- |
| 4096 | 5000 | 0.125 | 0.373 | 0.373 / 0.745 / 1.490 |
| 8192 | 10000 | 0.500 | 1.490 | 1.490 / 2.980 / 5.960 |
| 12150 | 14580 | 1.100 | 3.168 | 3.168 / 6.335 / 12.671 |
| 16384 | 20000 | 2.000 | 5.960 | 5.960 / 11.921 / 23.842 |

The private-grid column is **only one grid per worker**, excluding the final
shared grid, model, compensation, images and all other state. It demonstrates
why that approach is unacceptable; it is not an RSS prediction.

Current standard initial allocation is `(7T-2)B`: value+compensation for T dirty
and 2T-1 PSF grids, plus T forward grids (allocated even for an empty initial
model). At 8192 and T=2 this is **17.881 GiB of grids alone**. Current replay adds
`32*T*(G + A*min(R,K+3))` bytes of merge/pool arrays, where K is tile count, A
the maximum halo-tile capacity, R maximum simultaneous records; T forward grids,
routes and retained scientific state are additional. This is not `W*G`, but
still a substantial footprint. [allocation][allocation], [layout][layout].

Initial formation is more expensive than those grid-only figures: it crops
complex dirty and PSF arrays, builds f64 sensitivity, and clones dirty into
`invariant_dirty` while the operator still owns its grids. A source-derived
payload lower bound for this case is
`(7T-2)*16G + (4T-1)*16P + (2T-1)*8P` bytes. It gives 2.738 GiB (4096,T1),
6.595 GiB (4096,T2), 10.951 GiB (8192,T1), **26.381 GiB (8192,T2)** and
**23.537 GiB (12150,T1)**, excluding FFT, input, compiler, masks and other
runtime state. The current whole-plane schedule therefore cannot admit the last
two under 16 GiB even if refresh tile storage vanished. These are calculated
allocation lower bounds, not measured RSS or a planner-failure diagnosis.
[initial formation][formation].

For the proposed shared-owner design, an explicit conservative phase formula is:

```text
M_peak = max over phases p of [
  enclosing_live(p) + retained_science(p) + CF_cache(p)
  + 16*G*(q_p + 2*r_p) + FFT_plans(p)
  + source_and_replay_buffers(p) + route_capacity(p) + writer_windows(p)
  + W * (FFT_lane_and_scratch(p) + thread_stack + bounded_kernel_scratch(p))
]
```

`q_p` counts resident model/forward grids and `r_p` resident accumulation grids;
the factor two preserves value+compensation. Charge any distinct FFT output or
transpose buffer explicitly, never as free scratch. Retained science includes
actual model/support arrays, dirty/residual/PSF/Taylor normal planes, masks, and
coupled minor-cycle workspace (use the existing [solver workspace formula][minor]).
Use actual retained capacities and storage permits; do not price every model
sample as one f64, subtract allocations still live, or double-count shared Arcs.
Paged state still has live read windows and I/O buffers. The RSS guard remains
necessary because this is a planned allocation bound, not a promise about RSS.

For fully resident refresh, `q=T,r=T`: **3TB**, the same grid term for W1/W2/W4.
At 8192 this is 4.470 GiB for T1 or 8.941 GiB for T2 before other live state.
At 12150 it is 9.503/19.006 GiB. Thus T2 at 12150 cannot fit the 16-GiB limit by
this schedule, and even T1 at 16384 requires 17.881 GiB before other state.
T1 at 12150 is only conditionally feasible for a **redesigned refresh schedule**;
the current initial schedule already exceeds the limit as shown above.

For oversized Taylor cases, use admitted term/moment waves and existing paged
normal/model storage, preserving all cross terms. This trades memory for
replay/FFT/I/O and needs measurement; it is not implemented or free. Initial
zero-model work should not require forward grids. If the minimum resident grid
set plus CLEAN state still exceeds 16 GiB, fail admission and return for a
separate out-of-core FFT/solver design or resource decision. Tiles do not make
arbitrarily large images fit, nor does lowering W fix worker-independent arrays.

A full-grid copy costs B extra live bytes and at least 2B bytes of read/write
traffic. At 12150 that is 3.168 GiB live and 6.335 GiB traffic. Each current FFT's
two explicit grid shifts touch approximately 4B of grid read/write traffic;
two gather/scatter axis traversals add another 4B, excluding scratch and FFT
arithmetic traffic. These are source-derived traffic counts, not elapsed-time
predictions. Remove unnecessary movement before assuming more threads hide it.

## Alternatives and the smallest decision-producing experiment

Reject full-grid-per-worker and global atomic gridding: the former violates the
large-image memory requirement, the latter introduces hot-cell contention and
worker-dependent arithmetic. Independent planes/terms cannot satisfy one-image
MFS scaling. Do not copy libRA's repeated whole-buffer sector scans or Obit's
replicated accumulator architecture. Do not replace the replay algebra with
raw visibility rereads without demonstrating a complete-application benefit.

Retaining the existing bounded tile/shard merge is a legitimate comparator,
not a rejected correctness design. It already handles hot tiles and is tested.
Exclusive ownership must earn its integration by reducing actual memory/merge
cost without losing more to routing and skew. There is no current matched
large-image MFS phase measurement here establishing that grid merging is the
dominant wall-time cost.

Before a larger campaign, propose one production-connected standard-MFS slice:
same source selection, W1/W2/W4, one output image, real CLEAN, initial imaging,
at least one subsequent/final residual refresh, and existing writer. Capture one
parent observation using the existing phase/stream counters, not a broad new
instrumentation project. Let F be non-overlapping critical-path FFT wall time;
even ideal W4 FFT scheduling can save at most `0.75*F`. Stop before coding if
that ceiling cannot meet the agreed useful benefit; do not automatically start
a different optimization. No elapsed-time benefit is asserted by this research.

For the FFT prototype, retain the same RustFFT plans, axis-0 then axis-1 order,
gather/FFT/scatter loops, shifts, image layout and precision. Process one plane
at a time, splitting disjoint lane groups over W1/W2/W4 with an axis barrier.
Reconstruction owns borrowed numeric work; runtime dispatches it. In particular,
`GriddedNormalReplayKernel::complete(self, _execution)` already receives but
ignores the bounded execution capability. Thread that capability through the
necessary model-forward, initial-inverse and residual-inverse call sites,
following the existing borrowed-work convention. Preserve dependency direction;
do not import runtime into reconstruction or create a second pool. The
corresponding phase leases must cover worker dispatch, including completion and
error joins, not just the earlier gridding workers. [completion seam][completion],
[admitted dispatch][dispatch].

For maximum axis length L and actual plan scratch S, charge `16*W*(L+S)` bytes
for lane/scratch, plus shared plans/descriptors/stacks. The incremental increase
over the one-workspace parent is `16*(W-1)*(L+S)`, not W full images. A task's
borrowed arrays cannot outlive the dispatch, including on failure. Do not add
an in-place transpose, shift fusion, initial-grid parallelism, term waves, or
allocation-lifetime repair to this first candidate. Leaving strided access
unchanged deliberately distinguishes scheduling benefit from a layout change.

Reuse the `issue607-standard-mfs-representative.json` application/science harness
as a control, not a performance endpoint. Its 512-square, 24-channel Briggs
fixture has full product/WCS/beam checks. A proposed performance selection is
one source-bearing standard single-field MFS image at 4096, then 8192 only if
the full live set fits; select cell/field and enough actual input samples to
exercise realistic UV coverage, not empty pixels merely to inflate FFT time.
This is a **new workload proposal requiring a recorded manifest**, not a claim
that an existing 4096/8192 MFS reference has passed. Do not use the 32-GB-medium
turnaround manifest under the current no-full-32GB authority.

MT-MFS needs a separate broad-band, known-spectrum control with nonzero higher
terms, off-centre sources, PSF cross moments, alpha and masks. The C-array
line-rich 44–45.022-GHz cube is not a convincing broad-band MT-MFS truth case.
Existing MT-MFS normal/minor/application oracle fixtures are the reuse starting
point, not a substitute for complete large-image application evidence.

Required discriminator: initial and refresh operator results, support-boundary
and concentrated-UV cases, all coupled terms, canonical W1/W2/W4 reductions,
failure/cancellation propagation, exact bounded allocation formulas, and no
growth of full-grid residency with W. Then unchanged mode-appropriate CASA and
serial/parallel product, mask, metadata/WCS, beam, flux and panel checks. The
C-array-specific reviewed-discrepancy policy is not automatically applicable.

For the first scheduling-only FFT change, specifically test rectangular shapes,
off-centre impulses, Fourier modes, random forward/inverse data, omitted or
duplicated lanes, axis barriers and worker failure. Bitwise transform equality
across W is expected because each lane performs unchanged arithmetic. The
support-boundary/hot-tile reduction tests become directly affected tests if the
later accumulation design is pursued, not extra work invented for an FFT-only
candidate.

Time from selected input/preparation through publication; include replay writes,
reads and FFT/model setup. Report exclusive phase wall time, worker activity,
RSS, I/O and term/grid passes. Do not sum overlapping worker times as wall time.
CASA single-process does not necessarily mean one CPU thread: record the actual
OpenMP/FFT configuration and honor any frozen reference's timing convention.
One matched observation first; repeat if noise makes the decision ambiguous.
This does not waive #449's separately prescribed final repetitions.

## Approval required before implementation

Approve the bounded within-plane FFT integration experiment through the existing
owners; record its
specific data selection, geometry, CLEAN controls and resource allowance.
Record a minimum worthwhile end-to-end benefit for this experiment and a
numerical MFS scaling target before a wider campaign. A subsequent exclusive-grid
ownership or out-of-core change needs its own explicit architectural approval.
Do not silently import the cube's 2x target or promise 3x from source
inspection. Keep serial independently competitive with its matched CASA
reference; do not pad serial to improve a scaling ratio.

No approval is requested for another architecture inventory, new public format,
second controller, precision change, additional Obit install, full-size run,
push/merge/release or cleanup. None has been performed.

## Independent Oracle challenge and disposition

[Completed review](https://chatgpt.com/c/6ab45466-fb7c-83e8-8c3b-4e1c8f17c8ea),
Chrome, signed-in account, visible **6 Pro** with Latest selected and Pro effort.
Two bounded primary-source searches used GPT-6 Luna Max; source interpretation
and synthesis remained in the main task. No model was silently substituted.

Oracle independently inspected the pinned current application/runtime/operator
and libRA sector sources. It did not inspect the local tree, full historical
ticket discussion, current Obit, or every restoration lifetime. The prompt
included the issue/scope, revision URLs, decisive source excerpts, constraints,
memory arithmetic and the proposed alternatives. Its advice is not benchmark
evidence.

Adopted after local verification: one FFT-only prototype first; preserve existing
accumulator arithmetic; use the existing completion dispatch seam; account for
coexisting cropped arrays and dirty cloning, not just grids. The source-derived
initial-formation formula above was checked against `finish_bound_recycled` and
recalculated locally. The review's final table gives 26.38 GiB for 8192,T2; an
earlier thinking-progress estimate was not used.

Deferred pending measurements: exclusive-region gridding, transpose/layout
changes, initial-grid parallelism and term-wave residency. Validity of those
designs does not establish a useful application-level speedup. Additional traps
incorporated into this proposal: route fan-out must not multiply sum weights or
coverage counts; stable parallel scatter needs deterministic offsets rather
than race-ordered atomic cursors; compensation persists across blocks; free
memory is credited only after actual owner release.

## Source index

[t55]: https://github.com/bglenden/casa-rs/issues/541
[t625]: https://github.com/bglenden/casa-rs/issues/625
[t449]: https://github.com/bglenden/casa-rs/issues/449
[request]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-application/src/continuum_request.rs#L474-L512
[app]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-application/src/lib.rs#L330-L829
[phase]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-application/src/major_cycle.rs#L19-L238
[cube-eligibility]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-runtime/src/streaming_cube/phase.rs#L97-L139
[runtime]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-runtime/src/spectral_cycle.rs#L1780-L2158
[initial]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator/initial_planes.rs#L61-L113
[publication]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-application/src/lib.rs#L995-L1134
[availability]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-application/src/availability.rs#L499-L523
[routing]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/gridded_normal_operator/two_domain.rs#L1138-L1253
[layout]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/gridded_normal_operator.rs#L303-L416
[completion]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-runtime/src/complete_data_operator.rs#L2215-L2240
[dispatch]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-runtime/src/bounded_stream.rs#L330-L377
[tests]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-runtime/src/complete_data_parallel_mfs_tests.rs#L467-L489
[fft]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator.rs#L11836-L11953
[forward]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator.rs#L8821-L8909
[finish]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator.rs#L10441-L10492
[allocation]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator.rs#L7740-L7888
[formation]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator.rs#L10712-L10903
[padding]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/spectral_operator.rs#L12055-L12075
[algebra]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/block_normal.rs#L26-L180
[minor]: https://github.com/bglenden/casa-rs/blob/405d01adc58c6a4844c41924dbef62c984091492/crates/casa-imaging-reconstruction/src/minor_cycle.rs#L44-L200
[libra-grid]: https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/GridFT.cc#L790-L1185
[libra-fortran]: https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/fortran/fgridft.f#L220-L346
[libra-taylor]: https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/MultiTermFTNew.cc#L315-L468
[casa-taylor]: https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/TransformMachines2/MultiTermFTNew.cc?at=61020062cee290f5466cffed5ec5032e0c7a3434
[libra-fft]: https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/Utilities/FFT2D.cc#L60-L79
[libra-replicas]: https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc#L326-L381
[libra-hpg]: https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/AWVisResamplerHPG.cc#L100-L282
[obit-home]: https://www.cv.nrao.edu/~bcotton/Obit.html
