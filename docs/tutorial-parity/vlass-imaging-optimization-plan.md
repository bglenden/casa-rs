# VLASS Fragment Imaging Correctness And Performance Plan

Truth class: approved execution contract
Last reality check: 2026-07-26
Verification: `just docs-check`

WDAD scope:

- wave lead: [#445](https://github.com/bglenden/casa-rs/issues/445)
- CASA fiducials and evidence harness: [#446](https://github.com/bglenden/casa-rs/issues/446)
- bounded multi-SPW and POINTING foundation: [#447](https://github.com/bglenden/casa-rs/issues/447)
- bounded serial AWProject/MT-MFS parity: [#448](https://github.com/bglenden/casa-rs/issues/448), including [#52](https://github.com/bglenden/casa-rs/issues/52)
- measured 10x CPU/GPU acceleration: [#449](https://github.com/bglenden/casa-rs/issues/449)
- canonical parameter catalog and UI exposure: [#450](https://github.com/bglenden/casa-rs/issues/450), which stays open through and depends on #449 for final execution-control closeout

The child waves are delivery boundaries, not reduced scope. Current casa-rs
capability is not the algorithm boundary: missing capabilities required by the
frozen CASA recipe must be added to the shared production imaging path.

Current checkpoint: comparison-schema-v4 exact full-array cold and warm smokes
completed on 2026-07-21. Cold CASA task time was 70.564 s and the complete cold
protocol took 81.229 s; warm measured times were 1.819, 1.835, and 1.904 s
(median 1.835 s). Both complete bundles passed exact native-plane structure,
18-product inventory, frozen tolerances, request/result/hash binding, and bundle
integrity. Their paths and hashes are frozen at
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T035625Z-vlass-fragment-smoke-cold-cad8add1.json`
(`a6d81a86649ac9f64c33bb967d5205f861ba8bbf21a0cf80caa3f9507dca304c`)
and
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T035810Z-vlass-fragment-smoke-warm-a3dd3526.json`
(`c8deeef01d44c5365c91264590fee15b81c467606a9c5128ab5c011c945beace`);
all earlier smoke generations remain historical only. The exact 12,150-pixel
single-field cold dirty fiducial then completed at full geometry with no 8,192-
pixel fallback: CASA `tclean` took 1,276.157 s and the complete protocol took
1,316.767 s. Peak RSS was 13,542,998,016 bytes, process I/O was 83,842,760,704
bytes read plus 63,605,723,136 bytes written, and externally sampled swap
traffic was 52,594,638,848 bytes out plus 29,456,121,856 bytes in. The host
remained responsive and CASA continued through visible phases, so the swapping
was substantial but bounded under the explicitly approved policy. The complete
strict receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T051546Z-vlass-fragment-single-field-cold-164bd8e1.json`
(`e91ee5af3a5a28b90c2bd6a77c43fd870ab8d590534e4e67dc351f4e54e7b0b1`).
At that checkpoint, Wave #446 remained open for single-field warm repeatability
and the all-fields cold/warm fiducials. The later full-size evidence and Brian's
explicit schedule redirection below supersede that remaining repetition plan;
the implementation PR remains draft for the casa-rs acceptance work.

The full-size all-fields cold dirty fiducial subsequently completed at the same
12,150 by 12,150 geometry. CASA `tclean` took 8,183.264 s and the complete
protocol took 8,225.322 s; peak RSS was 16,742,760,448 bytes and bound host
telemetry recorded a 35% minimum free-memory level, zero throttled pages,
48,947,691,520 bytes swap-out, and 31,465,897,984 bytes swap-in. The strict
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T071009Z-vlass-fragment-all-fields-cold-3d3179ae.json`
(`f424a33d8b228a56b552cfd793b4410e9fbca3fdb8af9a3fc47ea9d6957b415e`).
It binds all 18 products, one full-array comparison, 20 panels, and the
published CF cache. The full-size warmup then completed in 9,011.462 s before
the measured schedule was operator-interrupted.

Brian explicitly redirected the wave on 2026-07-21: repeated CASA timing is
deferred solely for statistical precision so development resumes on casa-rs.
The conservative development baseline is 8,183.264 s and the corresponding
initial 10x casa-rs target is 818.326 s. The completed 12,150-pixel CASA
products remain the frozen correctness reference; the 8,192-pixel fallback is
not active. Revisit a multi-run CASA median only if CASA parameters, data
selection, geometry, or required products change, or if casa-rs approaches the
final 10x boundary closely enough that CASA variance could change pass/fail.
The interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T093618Z-vlass-fragment-all-fields-f80f9a39.json`
(`70b33ca592a71139c8f85adf99e8d4249a8852d58d18b9e3adf5550f95eb7d4f`);
it retains the completed warmup and partial measured-001 request/log, and no
measured-002/003 call was launched.

### Experiment Authorization And Incorporation Boundary

Brian explicitly authorized non-destructive performance and correctness
experiments for this wave on 2026-07-26. An experiment may test changes outside
normal production practice without another approval, including alternate FFT
libraries, reduced precision, Metal/GPU implementations, memory layouts,
thread/worker ownership, buffer sizes, instrumentation, diagnostic
dependencies, temporary benchmark manifests, and prospective algorithm or
runtime changes. Each experiment must preserve its configuration, correctness
comparison, end-to-end timing, resource evidence, and negative result.
This is a standing part of the goal for the remainder of the wave, survives
agent and machine handoffs, and supersedes a normal stop-and-ask boundary only
for creating, running, measuring, and reverting those experiments.

This standing authorization does not approve final incorporation. Before a
materially different algorithm, substantial dependency, runtime/default,
public API, persisted format, provider contract, or concurrency guarantee
graduates into the production path, present its measured evidence and request
Brian's approval. Rejected experiments remain explicitly non-fiducial and
cannot weaken the frozen correctness or final performance gates. Destructive
actions and changes outside this approved VLASS wave remain governed by the
normal repository contract.

### 2026-07-22 Mac Mini Continuation

The 24 GiB M2 mini did not have the archived full VLASS MeasurementSet. A
deterministic reduced turnaround fixture was therefore staged from CASA's
`refim_mawproject_twopointings.ms`: 108,864 MAIN rows, two fields, four
S-band SPWs, three channels per SPW, complete POINTING selection, a CASA-built
32-plane AWProject cache, and the exact 18 MT-MFS products. The generator and
manifest are `tools/perf/imager/stage_vlass_turnaround.py` and
`tools/perf/imager/workloads/vlass-awproject-turnaround.json`. Its receipt was
written to `/tmp/casa-rs-vlass-turnaround-v1/turnaround-receipt.json`; this is
development evidence only and cannot satisfy any frozen 12,150-pixel gate.

On the mode-faithful 1,024-pixel dirty row, the preserved serial implementation
measured 120.34 and 120.49 seconds. A direct nearest-key lookup rewrite measured
121.26 and 120.14 seconds and was rejected as neutral. Raising bounded CF
residency from 256 MiB to 4 GiB reduced one run to 84.75 seconds and isolated
CF paging as the limiter, but retaining that memory-only workaround was less
effective than fixing locality.

The adopted implementation groups each streamed pointing block by its exact
RR/LL imaging-and-weight CF quartet, reuses the loaded cells and projectors,
and charges the compact locality index to the shared execution plan. Serial
runs then measured 80.68, 78.92, and 78.44 seconds at the original 256 MiB
budget. CF loads fell from 5,861 to 796. A full 18-product comparison against
the pre-change output completed with matched metadata; the worst full-array
RMS ratio was `1.6805736768166808e-08` and the worst peak-normalized absolute
difference was `1.70413375132326e-07`.

The next adopted path gives deterministic CPU workers disjoint Taylor-plane
ownership. No worker receives a duplicate image-sized grid. Four plane workers
with the CF-locality-preserving automatic choice of one preparation/read-ahead
owner measured 33.91 and 34.38 seconds, a 3.53x median improvement over the
preserved 120.415-second serial baseline. The threaded and serial 18-product
arrays were bit-identical and their metadata matched. Explicit preparation and
read-ahead overrides remain honored. A four-worker experiment that also used
four preparation owners measured 49.44 to 50.27 seconds and increased CF loads
to 3,088, so that topology was rejected. These reduced-row speedups are real
Mac mini development evidence, not the final four-row 10x result.

The production `auto` policy independently resolved eight disjoint plane
workers plus one locality-preserving preparation/read-ahead owner on this host
and measured 34.58 seconds. It retained 796 CF loads and the complete product
contract; explicit grid, preparation, and read-ahead controls still override
the automatic values.

The next checkpoint adds a true AWProject Metal gridder. It packs each exact
RR/LL imaging-and-weight CF quartet once per locality group, dispatches the
eight MT-MFS PSF/residual/weight planes directly to shared Metal storage, and
uses two signed `u32` atomic limbs per component with overlap-derived
power-of-two fixed scales. The final high/low conversion uses IEEE-safe Metal
math, retains a second Float32 compensation plane, and reads the compensated
grids back into the existing f64 RustFFT finish. Explicit AWProject Metal with
an f32 dirty-product FFT is rejected because it cannot preserve that
compensation contract. The planner charges the output, compensation, fixed
limbs, packed CF batch, and routed sample batch before admitting Metal.

The reviewed non-fiducial configuration is
`tools/perf/imager/workloads/vlass-awproject-turnaround-metal.json`. One warmup
plus three measured full four-SPW runs took 22.238401, 22.255476, and
22.238511 seconds (median 22.238511 seconds), with a 2,865,397,760-byte peak
RSS in all three profile runs. This is 1.535x faster than the adopted
four-worker CPU median and 5.415x faster than the preserved serial baseline.
Each measured run accepted all 323,568 selected samples with no rejections;
the Metal grid stage used 24 locality dispatches, and compensated readback of
all eight planes took 16.6 to 19.0 ms in the measured sequence. The durable
development log is
`/private/tmp/casa-rs-vlass-metal-final-receipts/20260722T213940Z-vlass-awproject-turnaround-metal-76bebbc1.log`.

A strict full-array comparison of the preserved four-SPW Metal products to
the preserved f64 serial CPU products completed for all 18 products, passed
the frozen tolerance evaluation, and received an overall `good` structure
review. The worst RMS ratio was `2.1916123684739682e-7` and the worst
peak-normalized absolute difference was `2.0733173011344975e-6`; the complete
receipt is
`/private/tmp/casa-rs-vlass-metal-vs-cpu-final2-receipts/20260722T214929Z-vlass-awproject-turnaround-metal-e08da7a5.json`.

This does not make the reduced row CASA-correct. Comparing the same Metal
products with the preserved CASA turnaround products retained the existing
casa-rs/CASA residual gap: RMS ratios were `0.009622961759686686` for
`.residual.tt0` and `0.004643503173129556` for `.residual.tt1`, with additional
metadata and finite/mask-topology mismatches. That typed failed-comparison
receipt is
`/private/tmp/casa-rs-vlass-metal-final-comparison-receipts/20260722T214630Z-vlass-awproject-turnaround-metal-91e9c5f5.json`.
Metal-vs-CPU parity and acceleration are therefore green development evidence;
CASA correctness, the frozen 12,150-pixel four-row benchmark, and the 10x
closeout remain explicitly incomplete.

A subsequent field-0 diagnostic traced the residual gap to the AW gridding
arithmetic boundary. CASA multiplies each Complex32 visibility/weight value by
its Complex32 convolution-function tap before accumulating the contribution in
the DComplex grid; casa-rs had promoted both operands before multiplication.
Preserving CASA's Complex32 product and attaching the existing primary-beam
support mask to every MT-MFS residual and restored Taylor product brought all
18 numerical products inside the frozen RMS and peak tolerances. The strict
reduced-row comparison is
`/private/tmp/casa-rs-vlass-f32-mask-receipts/20260722T231306Z-vlass-awproject-turnaround-98ed91ee.json`.
Residual TT0 now has RMS ratio `0.000238543` and peak-normalized absolute
difference `0.000255450`; residual TT1 has `0.000229239` and `0.000253040`.
This diagnostic still reports exact coordinate-metadata differences and one
primary-beam cutoff pixel (plus eight derived-alpha mask pixels). Those remain
correctness owners before the full-size benchmark. This reduced single-field
receipt is turnaround evidence only and does not satisfy a frozen 12,150-pixel
gate.

The next source-level metadata pass preserved the raw J2000 `FIELD.PHASE_DIR`
angles for observation metadata, selected the first matching
`SOURCE.REST_FREQUENCY` through the FIELD/SOURCE/SPW/DOPPLER relationship, and
matched casacore's standard `IERSeop97`/`IERSpredict` dUT1 path while retaining
the IAU-2000 dX/dY correction columns. It also ports casacore's standard
106-term equation-of-equinoxes series and legacy AU light-time constant. On
the same reduced field-0 row, pointing center and the 2.05 GHz rest frequency
now match CASA exactly. The remaining spectral-coordinate difference is
2.86102294921875 microhertz in `crval` and 6.198883056640625 microhertz in
`cdelt`; eliminating it requires casacore's analytic cached-aberration
derivative rather than further finite-difference tuning. The strict typed
receipt is
`/private/tmp/casa-rs-vlass-coordinate3-receipts/20260723T005411Z-vlass-awproject-turnaround-4a6fc44d.json`.
All 18 numerical product comparisons remain inside the frozen RMS and peak
tolerances. Exact topology still differs at one PB-cutoff pixel for the PB,
image, and residual products and at eight pixels for each derived-alpha
product; seven products retain the small restoring-beam-fit metadata mismatch.
These are explicit correctness owners before the final laptop benchmark, and
this single-field reduced receipt remains turnaround evidence only.

The following topology pass aligned the CASA MT-MFS beam contract: only
`.psf.tt0` carries the fitted PSF beam, while `.psf.tt1` and `.psf.tt2` do not.
The comparator now retains the first 16 deterministic mask-mismatch coordinates
in its version-4 result without invalidating older version-4 receipts. The
fresh strict comparison is
`/private/tmp/casa-rs-vlass-beam-topology-receipts/20260723T021000Z-vlass-awproject-turnaround-acec364a.json`.
All 18 numerical tolerances pass. The remaining exact differences are one PB
boundary pixel at `[453,234,0,0]`, eight derived-alpha mask pixels, spectral
`crval`/`cdelt` deltas of about 2.86/6.20 microhertz, and five small fitted-beam
value differences. This remains 1,024-pixel turnaround evidence, not a frozen
12,150-pixel acceptance result.

### 2026-07-23 Mac Mini Handoff Checkpoint

The full-size dirty manifests now execute the shared Rust benchmark path while
retaining their frozen recipe, dataset-selection, CF-plan, 18-product, and
tolerance contracts. `vlass-fragment-single-field` and
`vlass-fragment-all-fields` are explicit serial-CPU correctness baselines that
reuse the already frozen CASA 6.7.5.9 products. Separate
`vlass-fragment-single-field-auto` and `vlass-fragment-all-fields-auto`
manifests exercise the public `auto` policy with the same scientific contract,
one unmeasured warmup, three measured runs, and one profile run. All four plans
fail closed when their explicit CF cache or frozen CASA product prefix is
missing; none reruns CASA. Each Rust row writes products, comparator protocol
artifacts, panels, and the benchmark log below `<run-id>.partial`, revalidates
their hashes, exact 18-product inventories, tolerance result, and panel
inventory, and only then atomically publishes `<run-id>`. Failed or interrupted
runs retain a typed partial receipt and cannot masquerade as final evidence.

The 24 GiB/32 GiB resource fixtures keep the full-size ownership decision
explicit: the serial 12,150-pixel, eight-grid-plane AWProject plan is admitted
under the 32 GiB operator budget with no per-worker full-grid copy and rejected
under the 24 GiB budget, while a 4,096-pixel turnaround remains admissible on
the mini. The public automatic policy now selects the measured compensated
Metal AW grid path for dirty MT-MFS when a Metal device is present, selects CPU
when it is absent, and logs a stable `auto_metal_reason` for both choices.
Explicit overrides remain authoritative.

The named Oracle review remains incomplete. The Oracle skill is not installed,
the in-app ChatGPT session was signed out, and no signed-in Chrome connection
was available. No evidence was sent to an unauthenticated session. The review
must be retried in a fresh signed-in conversation; this does not convert its
acceptance check into a deferral.

The preserved reduced CASA cache on the Mac mini also completed a non-final
real-cache execution ladder at `e4fc0af75`. That cache contains 256 paired
imaging/weight cells for four turnaround SPWs. Its 32-pixel one-SPW probe
rejected before gridding because the smallest paired support requires at least
43 pixels on each axis. The corresponding 64-pixel one-, two-, and four-SPW
rows completed in 3.813, 6.250, and 12.329 seconds and each wrote the exact 18
dirty MT-MFS products. The four-SPW row accepted 163,104 of 163,296 attempted
samples; the other 192 were explicit PSF-placement/outside-grid rejections,
not invalid input, kernel-index, or normalization failures. This is reduced
turnaround execution evidence only. It does not satisfy the required frozen
field-1525 one-, two-, and 16-SPW real-cache ladder.

The exact frozen ladder is now an ignored integration gate over the production
`CliConfig` and `run_from_config` path. It first requires the 32-pixel support
rejection, then runs the 64-pixel serial-CPU field-1525 rows for SPWs 2,
2 through 3, and 2 through 17. Every passing row must bind the 1,024-pair,
16-frequency, 32-W-plane CASA cache, accept real samples, retain bounded CF
residency, avoid invalid-input/kernel-index/normalization failures, and write
all 18 products:

```sh
export CASA_RS_VLASS_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-vlass/issue-446/data/b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a
export CASA_RS_VLASS_SINGLE_FIELD_CF_CACHE=/Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache/6.7.5.9/8e5679681214158629c7eb6113bc3b062d6105fbae64471905aa73de50080a69

CARGO_INCREMENTAL=0 cargo test -p casars-imager \
  tests::vlass_field_1525_real_cache_rejects_32_then_passes_64_for_1_2_16_spws \
  -- --ignored --exact --nocapture
```

On the 32 GiB laptop, run the serial dirty rows first and inspect full-array
correctness before starting `auto`:

```sh
export CASA_RS_VLASS_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-vlass/issue-446/data/b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a
export CASA_RS_CASA_PYTHON=/absolute/path/to/casa-6.7.5.9/bin/python

python3 tools/perf/imager/run_workload.py vlass-fragment-single-field \
  --output-dir /Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs \
  --artifact-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts \
  --cf-cache-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache
python3 tools/perf/imager/run_workload.py vlass-fragment-all-fields \
  --output-dir /Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs \
  --artifact-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts \
  --cf-cache-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache
```

Only after both serial receipts are correctness-green, run the corresponding
`-auto` workloads with the same three path arguments. Record the exact commit,
host, resolved allocation ledger, worker/backend plan, Metal placement/fallback
reason, total/stage times, RSS/swap/I/O, profiles, comparison receipt, and human
panel review. The deterministic clean mask, two clean manifests/fiducials, four
final correctness-green rows, four independent 10x timing gates, signed-in
Oracle review, and complete review/verification gates remain required owners;
the dirty launch manifests do not close or defer them.

### 2026-07-25 Full-Geometry Serial Checkpoint

The first casa-rs-only 12,150-pixel field-1525 diagnostic completed on
GLENDENNING without rerunning CASA. Its immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260725T010514Z-vlass-fragment-single-field-1d9922c1.json`.
The measured casa-rs runtime was 216.930656 seconds, or 5.883x faster than the
matched frozen 1,276.157-second single-field CASA task. The correct
single-field 10x boundary is 127.6157 seconds; the 818.326-second target applies
only to the all-fields row. This checkpoint is therefore neither a correctness
nor a performance pass.

All 385,862 attempted AWProject samples were accepted. Sixteen products retain
topology parity and numerical tolerance. `.alpha` and `.alpha.error` differ at
exactly two mask pixels each, `[12068,1736]` and `[6867,9898]`, where CASA's
strict principal-image threshold includes the pixel and casa-rs excludes it.
The current casa-rs threshold is `0.0024236352`. Scalar-threshold experiments
do not reproduce the CASA topology without changing other pixels.

The run also falsified applying CASA's literal `real(tmp)` projection directly
to casa-rs's shared paired-hand WTCF grid. CASA retains separate parallel-hand
planes and its `AWProjectWBFT::makeSensitivityImage` loop overwrites `tmp` with
the final plane before projecting its real part. casa-rs instead compresses the
paired hands into one shared WTCF grid. For that representation, magnitude is
the parity-preserving non-negative sensitivity: the preserved receipt before
the experiment has `.weight.tt1` RMS ratio `4.781221153330683e-09`; shared-grid
`real()` produced a coherent `2.985920079720224e-06` RMS ratio. Commit
`55b14e994` restores and documents the representation-correct projection.

Commit `54c1d4557` adds profile-only memory accounting for an exact CASA-order
AW replay. Each warmup or measured pass contains 104 POINTING/SPW metadata
groups and 385,862 planned samples. Planned-sample storage is 0.096 to 1.93 MiB
per group. The compact tap upper bound is 20.2 to 594.1 MiB per group; 32 groups
exceed 256 MiB and six exceed 512 MiB. These are mutually exclusive group
working sets, not simultaneous residency.

The remaining arithmetic-order difference cannot be fixed by replaying whole
metadata groups. `GroupedVisibilityMetadataBatch` records groups in first-seen
order and source-contiguous ranges inside each group, so group-at-a-time
execution still reorders interleaved samples. The proposed production change
must instead:

1. construct a bounded sample-to-metadata route for one streamed source block;
2. split the block only into contiguous source-order windows;
3. use cache metadata to bound each window's unique
   `(POINTING, cell, offset, conjugation, role)` tap bundles before loading
   pixels;
4. load each required full CF once, pack the phase-applied Complex32 taps, and
   release evictable full cells;
5. replay the compact sample plans in ascending row/channel order, with RR then
   LL updates and disjoint Taylor-plane workers; and
6. process consecutive windows in order so segmentation cannot change any
   plane's accumulation order.

The compact-tap budget must be an explicit allocation in the shared resource
plan. It may reduce source-window length but may not consume the five-percent
AW safety reserve or create another image-sized grid.

Brian explicitly approved this material performance-algorithm/runtime change
on 2026-07-25. The production path now constructs a strict source-sample route,
plans compact tap bundles from CF metadata before loading pixels, segments only
at source-sample boundaries, and replays consecutive windows in source order.
The planner charges the full-cell LRU and compact tap arena as independent
allocations under the same public `cf_resident_mb` per-allocation ceiling. The
five-percent AW safety reserve is unchanged and the execution topology still
owns only the original eight image-sized grids.

The first full-size serial receipt for this path is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T013019Z-vlass-fragment-single-field-1cdf84fd.json`.
All 385,862 samples were accepted, adaptive windows stayed within the
268,435,456-byte compact-tap ceiling, and peak RSS was 21,098,823,680 bytes.
The measured casa-rs time was 180.553504 seconds, a 16.8% improvement over the
216.930656-second checkpoint and 7.068x faster than frozen CASA. This still
misses the 127.6157-second single-field 10x boundary.

The exact-source-order hypothesis was not the final alpha-topology owner. The
same two `.alpha` and `.alpha.error` mask mismatches remain. At those pixels,
CASA's restored principal TT0 values are just above CASA's
`max(principal_tt0)/10` threshold while the numerically close casa-rs values
are below its corresponding threshold. All other product topology and frozen
numerical checks pass. A scalar threshold adjustment remains invalid because
correctly excluded pixels lie closer to the casa-rs threshold. The next
correctness investigation therefore owns the FFT/principal-solution numerical
boundary rather than source replay order.

An exact-shape backend probe then confirmed that the existing MPSGraph backend
can execute a 12,150 by 12,150 Complex32 dirty-product transform on this host,
but it does not support Complex64. The successful one-repeat probe reported
847 ms packing, 661 ms execution, and 1,742 ms total. A first full-workload
attempt also proved that explicit `metal-mpsgraph` correctly fails closed when
combined with the strict `--no-parallel` RustFFT comparison surface. Its
preserved receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T105536Z-vlass-fragment-single-field-metal-fft-f32-experiment-d266e985.json`
(`d72f71668151abd64a1d8de9f081bd4553e71c9dcf9959b808ff91d45200d7c5`).

The bounded full-geometry experiment therefore selected the Metal product FFT
explicitly while pinning AW gridding, preparation, and read-ahead to one CPU
owner. It reused the frozen CASA products and accepted all 385,862 visibility
samples. The measured casa-rs time was 171.372528 seconds, only 5.1% faster
than the f64 RustFFT checkpoint and still slower than the 127.6157-second
target. Its profile run took 172.107264 seconds with 21,802,172,416-byte peak
RSS; gridding remained the owner at 126.712393 seconds, while the product FFT
stage took 18.512345 seconds.

The full-array comparison rejects this f32 path. `.residual.tt0` and
`.residual.tt1` had 0.416% and 0.418% RMS ratios and 2.80% and 3.00%
peak-normalized absolute differences. `.alpha` and `.alpha.error` each had
4,138 mask mismatches, rather than the two-pixel f64 boundary. PB structure
also required investigation. The strict receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T105757Z-vlass-fragment-single-field-metal-fft-f32-experiment-c4fe43ae.json`
(`e4cde5d9581546697ae077b3fd8fd03a39d268b177caa12ffc708f563d052682`).
This result is experimental evidence only and must not be incorporated as a
default or used for final acceptance. The next FFT experiment must preserve
f64 arithmetic, such as an FFTW candidate, unless another precision strategy
first passes the same frozen comparison.

The subsequent local FFTW f64 experiment preserved the exact same two
`.alpha` and `.alpha.error` mask mismatches as RustFFT, so the FFT backend is
not their correctness owner. It reduced end-to-end time from 180.553504 to
160.893977 seconds (10.888%) and the measured FFT stage from 35.05 to 9.94
seconds, but AW gridding remained dominant at 124.64 seconds. The result is
7.932x faster than the frozen single-field CASA time and still misses the
127.6157-second boundary. Its immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T113847Z-vlass-fragment-single-field-fftw-f64-experiment-ccedb70a.json`
(`757f8397120b7a995e3d4f9eb337bc99817a80d34e0266d99ae8a7b6448ef252`).
The temporary local FFTW dependency remains experimental pending an explicit
incorporation decision.

A full 325-group diagnostic also falsified the suspected POINTING
pixel-to-direction-to-pixel round trip as the residual owner. The maximum
absolute round-trip displacement was `1.5279510989785194e-10` pixel in x and
`9.094947017729282e-12` pixel in y, many orders of magnitude below a
visibility-tap phase difference capable of explaining the observed residuals.
The diagnostic code was removed after the measurement; this negative evidence
prevents replacing the shared pointing metadata contract without a new
falsifying observation.

The MT-MFS Hessian inversion was also excluded. CASA's frozen 2 by 2
principal Hessian is `[1, 0.0399748087; 0.0399748087, 0.0350363255]`.
Reproducing casacore's hand-written Cholesky decomposition and substitution
order yields the same Float inverse coefficients as the existing nalgebra
path. The experimental replacement was removed. At the mismatch pixels, the
principal-image error instead tracks the raw residual error: approximately
`7.26e-8` versus a `9.43e-8` residual-TT0 error at `[12068,1736]`, and
`3.73e-9` versus a `4.66e-9` residual-TT0 error at `[6867,9898]`.
Correctness investigation therefore remains at the visibility-specific
gridding path rather than the principal solve.

The frozen sample census has no asymmetric polarization-flag case:
385,862 row/channel samples have both RR and LL unflagged, zero have only
one hand unflagged, and 279,738 have both flagged. `WEIGHT_SPECTRUM` is also
identical between RR and LL for every selected sample. casa-rs's paired-hand
admission and equal per-channel hand weight therefore do not explain the
residual difference.

An instrumented, weighting-only `PySynthesisImager` run then compared CASA's
`VisImagingWeight` with the production casa-rs streaming density pass at the
full 12,150-pixel geometry without running an AW grid or repeating a `tclean`
timing. Both retained 269,261 nonzero cells, the same
`3777.17456055` maximum, and exactly the same four inspected density cells and
first six per-sample output weights. CASA reported
`sumwt=31371709.7178`, `density_sum=31371709.6957`,
`density_sum_sq=9525097868.68`, and `f2=0.000823395967018`; casa-rs reported
`density_sum=31371709.69459`, `density_sum_sq=9525097880.542`, and the same
stored `f2=0.000823395967018`. The small aggregate density differences leave
density accumulation order as a possible owner, but exclude the cell
coordinate convention, robust scale stored value, and first-sample reweight
formula.

The complete 12,150 by 12,150 Float density grids then localized that
difference precisely. Group-at-a-time mosaic metadata traversal changed
CASA's source-sample addition order in 1,924 of 269,261 nonzero cells: 1,596
cells differed by one ULP, 300 by two, 20 by three, and eight by four, with a
maximum absolute difference of `0.0009765625`. A traced four-ULP cell proved
that both implementations added the same weights but casa-rs interleaved
source ranges belonging to different metadata groups.

The shared streaming density path now reuses the strict sample-to-group route
owned by compact AW replay and visits samples in ascending source order.
Its complete density dump is byte-for-byte identical to CASA across all
147,622,500 cells, including SHA-256
`92d5fab098f635d59995b2517b83785d0b615350f6d360a3bbef46a66cfe2162`.
The before/after comparison receipts are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/experiments/weight-density-20260726/comparison.json`
and `comparison-source-order.json`.

This exact density correction still does not own the final alpha topology.
The full-size production-path run completed in 201.62 seconds with
20,040,840,704-byte maximum RSS and no swaps. All sixteen non-alpha products
passed full-array topology, numerical, metadata, source-region, and structured
difference checks, but `.alpha` and `.alpha.error` retained the same two mask
mismatches at `[12068,1736]` and `[6867,9898]`. The bound comparison is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/experiments/weight-density-source-order-20260726/comparison/result.json`.
The next instrumentation boundary is therefore downstream AW visibility/grid
arithmetic, not Briggs density construction.

A full-size experiment that instead retained the natural-weight sum and
performed the robust scale and final division in `f64` was rejected. The exact
18-product comparison increased `.alpha` and `.alpha.error` mask mismatches
from two to four. CASA source confirms why: `VisImagingWeight.h` declares
`f2_p` and `d2_p` as `Vector<Float>`, and the density, denominator, and output
weight operations are Float. The experimental code was removed. Its retained
products and comparison are under
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/experiments/briggs-double-20260726`;
the comparison result is `comparison/result.json`.

The next full-geometry diagnostic compared CASA and casa-rs immediately before
the residual FFT. A one-off CASA source hook wrote its two `Array<DComplex>`
residual grids; the hook was then removed and the clean synthesis library was
rebuilt and reinstalled. The corresponding casa-rs hook stopped after writing
the two host `Complex64` grids and was also removed. Across each complete
147,622,500-cell plane, CASA and casa-rs had exactly the same 14,516,243-cell
nonzero support, with zero CASA-only or casa-rs-only cells. TT0 and TT1 relative
L2 differences were only `2.264666091902243e-8` and
`2.3100603247288452e-8`; maximum absolute differences were
`5.073921230761869e-7` and `1.4184678244399078e-7`, and no cell differed by
`1e-6`. This excludes residual-grid support, placement, and missing-sample
topology while proving that the two alpha failures originate no later than the
ordered float contribution arithmetic. The immutable comparison is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/experiments/aw-prefft-grid-20260726/grid-comparison.json`.

CASA's active `PhaseGrad` path constructs the two axis phasors through
`Complex`, holds them as `DComplex`, multiplies them in double precision, and
finally stores the result in a `Matrix<Complex>`, matching the existing
casa-rs rounding boundaries. A full-size counter-experiment that multiplied
the rounded axis phasors directly as `Complex32` was rejected: TT0 and TT1
relative L2 differences worsened to `2.3007923917612846e-8` and
`2.3489987007346052e-8`, and the number of differing cells increased from
about 2.68 million to about 14.03 million per plane. The experiment was
reverted completely. Its negative-evidence receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/experiments/aw-prefft-grid-20260726/grid-comparison-phase-f32.json`.

CASA's normalized image RA does make its `UVWMachine` report non-NOP against
the equivalent negative-RA FIELD direction. A direct casacore probe measured
rotation phase vector
`[5.9770827158878294e-16, -4.0610742006744055e-18,
-1.3450926695114884e-17]` and only `5.9704862335009717e-12` m phase path for
the representative `[10000,-5000,2000]` m baseline. The corresponding
3 GHz phase is about `4e-10` rad, far below the observed residual error, so
the nominally identical field/image phase-center shortcut is not the owner.

The final two-pixel owner was CASA's exact Float arithmetic contract rather
than another geometric effect. The CASA and casa-rs image coordinate systems
both record the same MT-MFS reference frequency,
`2987890056.5468006` Hz. Aligned CASA/casa-rs contribution traces showed no
case where Taylor term 1 differed while Taylor term 0 agreed, excluding the
Taylor coordinate itself. A representative ordinary-Briggs sample then
localized a one-ULP difference: separate Float multiply and add produced bits
`1103137364`, while CASA's contracted
`1.0f + density * f2` produced bits `1103137365`. The shared Briggs paths now
state that fused rounding explicitly with `f32::mul_add`. A second bit-exact
probe showed that CASA evaluates higher MT-MFS Taylor powers and the
weight-times-power multiplication in Double before rounding once to Float;
the shared helper now preserves that contract instead of using a Float
recurrence.

The resulting full-size casa-rs-only run reused the frozen CASA products,
accepted all 385,862 AWProject samples, and completed the full comparison for
all eighteen 12,150 by 12,150 products. Every numerical, metadata,
source-region, structured-difference, and topology check passed.
`.alpha` and `.alpha.error` now have exact mask topology, with zero mismatched
pixels rather than two. Their maximum relative peak differences are
`4.493874399805625e-7` and `4.766473515036953e-7`, and their relative RMS
differences are `1.7554905894413146e-7` and
`1.7864514337408348e-7`. The measured correctness pass took
`206.227333` seconds; its separate profiling pass reported
`151.832349` seconds frontend total, including `120.903774` seconds in
gridding and `10.195894` seconds in the experimental local FFTW transforms.
This cold/no-warmup diagnostic is correctness evidence, not the final
single-field performance row. Its immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T152614Z-vlass-fragment-single-field-fftw-f64-experiment-dc10bd78.json`.
The workload manifest was restored to its one-warmup, warm-cache experimental
configuration after the diagnostic.

A bounded full-geometry Metal follow-up tested adaptive plane segmentation
under the same 32 GiB resource contract. The planner admitted one 2.362 GiB
fixed-point plane at a time after charging 2.483 GiB of non-plane scratch,
leaving 121.67 MiB for each packed source-order window. That forced eight
plane segments and only about 1,500 to 3,000 samples per window. The first
25,031-sample row block then needed eleven windows and 23.09 seconds; the
second 17,669-sample block needed eight windows and 11.46 seconds. The run was
stopped after those two blocks because even a linear extrapolation put
gridding near 370 seconds, over three times the correctness-green CPU
gridding time of 120.904 seconds. The preserved operator-interrupt receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T162408Z-vlass-fragment-single-field-metal-fftw-f64-experiment-0b7f4b3e.json`
(`33bd4f42187cc7cc8966e2b6e8f0c4d86a4e81673386a9b24935c29ff2e55a9c`).
Adaptive segmentation is therefore a correctness-enabling memory fallback,
not the winning full-size Metal topology on this 32 GiB host. A future GPU
experiment must avoid repeated clear/finalize/readback work, for example by
keeping fixed grids and global scales resident across source windows.

The first exact source-order multi-CPU experiment assigned the eight disjoint
MT-MFS output planes to eight workers. It exposed and fixed a routing bug in
the density prepass: splitting grouped metadata before constructing the
source-sample route incorrectly required each worker's subset to cover the
whole batch. The corrected path validates the complete route once, preserves
the single combined Briggs-density accumulation order used by this VLASS
configuration, and parallelizes only genuinely disjoint weighting keys. With
that fix, all 385,862 samples reached the initial-dirty finish, but the
eight-worker replay took 156.945 seconds versus 120.904 seconds for the
correctness-green serial run. The experiment was rejected before product FFT
and comparison because its gridding stage alone was already slower. Its
partial receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T165043Z-vlass-fragment-single-field-multicpu-fftw-f64-experiment-ab732816.json`
(`dd6e23e7fea25fb2763814b62086cd03002fcf111fdf313f1ee881715454d020`).
A four-plane-worker follow-up matched this host's four performance cores but
remained negative: its complete warmup took 192.415 seconds, including
147.578 seconds in the exact AW replay. The redundant measured and profiling
runs were stopped. Its partial receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T165813Z-vlass-fragment-single-field-multicpu-fftw-f64-experiment-750c6d70.json`
(`6ec2e61cbff657bf453d7c3717cc06a08f585b492e74d3279aa2130fa873757c`).
The result identifies repeated streaming of the same large tap bundles across
plane workers as the likely bandwidth owner. The next bounded CPU experiment
fuses Taylor planes by PSF, residual, and weight family, so each convolution
tap is loaded once per family while every plane retains exact source order.
Three parallel fused-family workers reduced replay only to 145.208 seconds
and completed the warmup in 187.250 seconds, still slower than serial. Its
redundant measured and profiling runs were stopped; the partial receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T170831Z-vlass-fragment-single-field-multicpu-fftw-f64-experiment-b8e00a2c.json`
(`0c60eed8080435e32c2dd69c449cd14fd5dd01d268b38025b77364dbdd0762de`).
The family fusion is bit-identical to independent plane replay, but parallel
execution remains limited by contention among the distinct imaging, PSF, and
weight tap streams. A serial fused-family run is the next isolation test.
That isolation test also failed: serial fused-family replay took 145.114
seconds and reduced CF loads from 10,188 to 9,002, but interleaving writes
across multiple 1.18 GiB grids destroyed the output locality of independent
plane replay. Its partial receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T171325Z-vlass-fragment-single-field-fftw-f64-experiment-8890f476.json`
(`77c870c5ff250f28529c626c8fe7154f41feb4e73885241492aaffb2b317617e`).
The fusion implementation and test were reverted completely. The production
candidate remains independent-plane, exact-source-order replay; the next
experiment changes only its explicitly charged CF/tap residency.
A 4 GiB residency request was rejected before allocation because the full-cell
LRU and compact tap arena coexist and are charged independently: fixed
requirements were 39,338,681,354 bytes against 34,359,738,368 assigned bytes.
The fail-closed receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T171923Z-vlass-fragment-single-field-fftw-f64-experiment-1d9bcd96.json`
(`2da73efeab6e6b2d93fb16c1bbe52ff84612a13816d153e0cc86157e4d1e4650`).
The next admissible experiment uses 1 GiB for each pool rather than weakening
the planner's fixed-allocation or safety accounting.
That 1 GiB run was the first large memory win. It reduced full-cell CF loads
from 10,188 to 2,888, increased resident cells from 7 to 28, and reduced exact
AW replay from 120.904 to 98.329 seconds. Its complete warmup took 132.261
seconds, equivalent to about 9.65x the frozen 1,276.157-second CASA baseline,
but still missed the 127.6157-second 10x boundary. Redundant repetitions were
stopped. The partial receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T172117Z-vlass-fragment-single-field-fftw-f64-experiment-ccbb41bf.json`
(`d2c22a6783c804a55c65a6b4f6560e6b9d4eeb6c6d4175c470e160d12afefa83`).
A 1.5 GiB request was then rejected at the FFT peak rather than weakening the
planner: it needed 34,560,462,234 bytes against 34,359,738,368 assigned bytes.
Its fail-closed receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T172445Z-vlass-fragment-single-field-fftw-f64-experiment-15cdf926.json`
(`254edc41b2af0f08a5cbf5dbdddceb3f214726a461a33886f08ec3524cbe3fb695`).
An admitted intermediate 1.375 GiB experiment also lost to the 1 GiB setting:
replay took 103.593 seconds despite reducing full-cell loads to 1,988 and
raising the terminal resident-cell count to 59. The larger source-window
working set outweighed the extra hits, so the redundant measured and profiling
runs were stopped. Its operator-interrupt receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T172533Z-vlass-fragment-single-field-fftw-f64-experiment-0127271f.json`
(`a16647bc3d63e74550294772c27db4d0a6e9af9e35d454808b9afb0bd6262b15`).
The resource-adaptive candidate therefore retains 1 GiB for each independently
charged pool on this 32 GiB host.

The subsequent production-path run kept that 1 GiB residency choice, disabled
per-block diagnostic detail, skipped redundant profiling, and compared the
measured output against the frozen CASA products. It completed in
104.542929 seconds, crossing the 127.6157-second single-field dirty target by
23.072771 seconds and delivering a 12.207x speedup over the frozen
1,276.157-second CASA row. All eighteen full 12,150 by 12,150 products were
present; every full-array tolerance, topology, metadata, beam, and source-region
check passed with no failed or incomplete checks. The bundle-integrity check
passed and produced twenty review panels. This remains experimental evidence
until the final FFTW/runtime and resource-planner incorporation decision. Its
immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T173015Z-vlass-fragment-single-field-fftw-f64-experiment-87770c83.json`
(`0faf0102ce09802c234de1880f1bcc4f5da9d8bb34e3678dd5ff09e9adec6e34`).
The next performance work moves to the three remaining independent acceptance
rows; no further single-field dirty tuning is justified unless another change
regresses this margin.

The first all-fields performance evidence used the plan's full-geometry,
four-separated-SPW turnaround (`2,7,12,17`) without rerunning CASA. It selected
163,800 MAIN rows and 10,483,200 channel visits. The 32 GiB planner admitted
18,895,680,000 bytes of complex64 grids, a 554,566,720-byte POINTING index,
and independent 1 GiB full-cell and compact-tap pools, leaving only 11,507
bytes below its modeled peak. Exact AW replay accepted all 6,416,526 gridable
samples and took 1,032.990 seconds; the complete first imager invocation,
including all eighteen product writes, took 1,073.175 seconds. Its four replay
blocks cost approximately 164.1, 241.5, 350.3, and 277.1 seconds as compact
tap windows changed from 63 to 90, 128, and 121. CF residency ended with
38,046 loads, no hits, 38,018 evictions, and 28 resident cells. This
quarter-band turnaround is already slower than the 818.326-second full-band
target and projects the unchanged topology to roughly 4,300 seconds. The
redundant measured invocation was stopped; the bound operator-interrupt
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T174730Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-5beb02f7.json`
(`3568d3a4dc75412e2591c0077b58f330c1c4fb5692a929f53b73766b20af9da6`).
The next CPU experiment spatially tiles each source-order tap window while
keeping the single admitted grid set resident. It may proceed only after a
bit-identical replay test; the falsifying measurement is the first full
turnaround block, not a full 16-SPW run.
A 128-pixel tiled prototype passed its focused bit-identical replay test across
all eight planes, including kernels that crossed tile boundaries, but failed
that performance criterion. The same first block took 183.560 seconds versus
164.123 seconds for plane-major replay, 11.8% slower, so the run was stopped
and the complete tiling implementation and test were reverted. Its bound
operator-interrupt receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T180907Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-51ae78f3.json`
(`a198a045484546d8be12ecc0a6ba7277751032628275dd181f8b5646f7923de2`).
The stop also exposed that the operator-interrupt receipt overwrote an
incrementally streamed log with a generic marker. The harness now appends the
typed failure marker instead, so later bounded experiments preserve all output
already flushed to the evidence log.

Alternating the immutable CF-cell materialization direction between compact
source windows then converted the all-field cache's cyclic LRU failure into
real reuse without changing source-order accumulation. The four block
cumulative replay times were 139.248, 340.454, 637.773, and 860.353 seconds,
versus 164.123, 405.612, 755.889, and 1,032.990 seconds for the otherwise
identical baseline. Full replay therefore improved by 16.7 percent. The cache
completed 18,907 disk loads and 19,139 resident hits instead of 38,046 loads
and zero hits, with the same 28 terminal resident cells and all 6,416,526
samples accepted. The run stopped before FFT/products because this new
turnaround manifest omitted the required `CASA_RS_FFTW_LIBRARY_DIR`; its
gridding evidence remains valid but it is not a correctness-bearing receipt.
The preserved failed-execution receipt and incrementally streamed log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T182304Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-46bcc76a.json`
(`3a5716a6c83d37464ab7af343f6e3faf398b7927876cafc3bf8307ac3971b672`).
The next bounded experiment ranks already-resident cells ahead of misses
within each window while retaining the alternating order among equally
resident cells; its falsifying checkpoint remains the first full block.
That extra ranking layer was rejected: the first block took 146.876 seconds,
5.5 percent longer than alternating-only's 139.248 seconds. The process was
stopped before block two and the resident-probe implementation was reverted.
The now-correct incremental log retained 2,333,727 bytes of output plus the
typed interrupt marker. Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T184035Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-d7013fda.json`
(`c64a796297231207fafba109072a37a29ef49eb1cc2ad3c46299a59a22625c23`).

Bounded stage instrumentation then decomposed an alternating-only first block:
the 144.475-second replay spent 34.695 seconds planning compact samples,
82.428 seconds materializing phase-applied tap bundles, 0.088 seconds preparing
accepted samples, and 25.733 seconds accumulating all eight grid planes.
Planning plus materialization therefore owns 81.1 percent of the measured
block; optimizing only plane replay, FFT, or product generation cannot close
the all-field gap. The run was deliberately stopped after that block. Its
receipt and preserved 2,333,806-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T184656Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-ad42a158.json`
(`ca9a9ff9fc6142dbad4ccafa688c71bac38f8c476f41c61aeaff0488281fdae5`).
The next CPU experiment must parallelize or eliminate compact planning and tap
materialization while retaining exact source-order grid accumulation.

An adjacent-window key census rejected a persistent exact-tap arena as the
next memory optimization. Only 230,132 of 4,644,922 requested bundles in the
first block appeared in the immediately preceding window, a 4.95 percent
upper bound before byte-capacity evictions. Retaining an extra tap-cache
ownership layer under the already exact 32 GiB plan would not repay its
complexity or risk. The stopped diagnostic receipt and 2,333,933-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T185503Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-18a51ef9.json`
(`a8b53c692f487fa0f668cb99e0722f506eff10eabcf66061aaf27ae6f0b14f40`).
The next experiment instead fuses normalization planning and phase-applied tap
packing into one kernel traversal before adding CPU planning/packing threads.

The fused projector matched the prior normalization and every packed tap
bit-for-bit in focused tests. Its first all-field block took 142.510 seconds:
planning 34.588 seconds, materialization 80.221 seconds, sample preparation
0.104 seconds, and gridding 25.680 seconds. The removed traversal reduced the
materialization owner by only 2.7 percent from 82.428 seconds, showing that CF
cell loading dominates this stage. The fused path remains a small experimental
candidate while the next bounded run adds four planning workers. The stopped
receipt and 2,334,216-byte incremental log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T190140Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-4266aaad.json`
(`5b3f4f2735d05abe8f2033c64b7eef54cd5b5cf78a10c9b1fa04af11dee89108`).

Four bounded planning workers over 16,384-sample chunks then reduced the exact
source-order planning owner from 34.588 to 12.041 seconds, a 65.2 percent
improvement. Completed classifications were consumed in original sample
order before window admission, so grid arithmetic order did not change. The
first block fell to 128.858 seconds despite materialization and gridding noise;
the parallel plan is retained as a candidate. The stopped receipt and
2,334,255-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T190719Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-c5a35ade.json`
(`48563730bb200bf60fefffb45d9d3b611bac91f32a158660ba09a18c24932426`).
The next diagnostic splits the remaining 84.906-second materialization stage
into CF cache loading and phase-applied tap packing.

That split measured 19.812 seconds in deterministic CF-cache access and
63.469 seconds in projector construction plus phase-applied tap packing; the
whole first block was 127.288 seconds. The next threading experiment therefore
keeps cell loading serial in the successful alternating order and parallelizes
only independent bundles within each already-loaded immutable cell. The
stopped diagnostic receipt and 2,333,893-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T191236Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-6d0a1c1c.json`
(`1e25c43f2b884ffa89c9f60dab618111c393a7e2df4602d13126785a254837d9`).

Using a four-thread Rayon pool only within each already-loaded immutable CF
cell reduced first-block tap packing from 63.469 to 23.537 seconds and the
block from 127.288 to 96.476 seconds. The complete four-SPW replay then took
647.226 seconds versus the original 1,032.990 seconds, a 37.3 percent
improvement; all 6,416,526 samples were accepted and CF counters remained
exactly 18,907 loads, 19,139 hits, and 18,879 evictions. The full frontend,
FFT, derived-product, and eighteen-product write path completed in 695.865
seconds. Because the harness then began its redundant measured invocation, it
was interrupted; temporary warmup products were cleaned, so this is complete
turnaround timing but not a correctness-bearing product receipt. Its preserved
5,018,545-byte log and interrupt receipt are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T191900Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-996b3a7b.json`
(`75aa4017b1d2c921b53385db65bcfcf708c7d93c1a933fe6f909e01d705d93e9`).
The four-SPW row is below the full-band 818.326-second boundary but is not a
10x pass because it selects only one quarter of the required SPWs. Further
work must reduce the remaining CF-load, tap-pack, and grid owners before the
16-SPW launch.

The threaded planning and tap-packing candidate then completed a second
correctness-bearing single-field run. Its measured end-to-end time was
80.509509 seconds, a 15.851x speedup over the frozen 1,276.157-second CASA
row and 47.106191 seconds below the 10x boundary. Exact AW replay accepted all
385,862 samples. All eighteen full 12,150 by 12,150 products passed the exact
inventory, full-array numerical, topology, metadata, beam, source-region, and
structured-difference contracts with no failed or incomplete checks. Bundle
integrity passed and the twenty panels are ready for Brian's still-required
visual review. The result establishes that consuming parallel placement plans
and packed tap bundles in original source order preserves the serial
correctness contract; it does not yet approve the experimental FFTW, Rayon,
or runtime-control incorporation. Its immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T193436Z-vlass-fragment-single-field-fftw-f64-experiment-97a9ade4.json`
(`dd4e85fdb5e67138bcb2510ecdc282d67d1ede96a8042a72036633b6b0e117d3`).

One-cell CF lookahead then overlapped loading the next immutable cache cell
with four-thread tap packing for the current cell. It retained the alternating
load order and exact cache behavior: all 6,416,526 samples were accepted and
the terminal counters remained exactly 18,907 loads, 19,139 hits, and 18,879
evictions. Four-SPW replay fell from 647.226 to 615.915 seconds (4.84 percent)
and the complete warmup through all eighteen product writes fell from 695.865
to 660.858 seconds (5.03 percent). Materialization improved by about ten
seconds in each large block, but shared memory-bandwidth contention raised
some packing and grid timings, limiting the end-to-end benefit. The redundant
measured invocation was stopped. The retained operator-interrupt receipt and
5,018,114-byte incremental log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T194746Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-927f8659.json`
(`c2556cd54e3d138da98ce56ac6d7af6624bffef2156b7511488a25403bfc24ba`).
The result is a positive experiment, not approval to hide the lookahead cell
inside the existing 32 GiB plan: final incorporation must charge its maximum
paired-cell residency explicitly and revalidate full-product correctness.

Four disjoint Taylor-plane grid workers atop that lookahead pipeline reduced
the four-SPW replay again, from 615.915 to 540.970 seconds (12.17 percent).
The complete warmup through eighteen product writes fell from 660.858 to
595.516 seconds (9.89 percent), and is 14.42 percent faster than the
695.865-second four-thread tap-pack candidate without lookahead or plane
workers. Grid time fell from 31.179 to 10.744 seconds in the first block,
41.621 to 16.758 in the second, 49.266 to 20.733 in the third, and 48.907 to
22.827 in the fourth. Each worker owns distinct complete output planes and
consumes every plane's samples in exact source order; all 6,416,526 samples
were accepted and the CF counters again remained 18,907 loads, 19,139 hits,
and 18,879 evictions. The redundant measured invocation was stopped. The
retained receipt and 5,017,598-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T200152Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-67f4743d.json`
(`b9aa322fdd91a4ddfd29d4278bb3702756a476a9689d63904b0ec1b74ce63b30`).
This worker count is a positive candidate, but the combined path still needs a
fresh full-product comparison before incorporation.

Using all eight disjoint plane tasks did not beat the four-performance-core
choice. The first block took 70.239 seconds versus 64.525 seconds with four
workers, 8.85 percent slower; planning, materialization, and grid time all
increased. The run was stopped before block two and the turnaround manifest
was restored to four grid workers. Its retained negative-evidence receipt and
2,333,452-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T201306Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-feb2a938.json`
(`23dc9038b7c23b19bcb659f773f1e6a604a1852e831fdd61cb31e6da7884995f`).

## Outcome

Make two imaging workloads derived from the archived VLASS test MeasurementSet
fully comparable with CASA and at least 10x faster than matched CASA 6.7.5.9
single-process wall clock on this workstation:

1. the supplied single-field, extremely pixel-dominated workload; and
2. a joint image of all 63 MAIN-table fields, which form a connected three-row
   OTF raster fragment.

The 10x requirement applies independently to the dirty and deterministic-clean
variants of both workloads. It is a closeout gate, not an assumed result. A
measured miss remains unfinished unless Brian explicitly changes the target.

Correctness precedes performance. Optimized results must retain the complete
CASA product contract and must not hide scientific or structured differences
behind aggregate speed numbers.

## Scientific Scope

The source archive is:

```text
/Volumes/GLENDENNING/vlass_test.tgz
sha256 b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a
```

It contains a 2.521 GiB uncompressed MeasurementSet and `tclean.last`. The
MeasurementSet has 655,200 MAIN rows, 63 referenced fields, 16 used spectral
windows (2 through 17), 64 channels, four correlations, and a 3,335,002-row
POINTING table. The 63 fields form three overlapping raster rows rather than a
complete VLASS per-image MeasurementSet.

These workloads are valid for algorithm and systems work, but neither is
evidence for full-PIMS throughput, full survey sensitivity, or Quick Look / Single
Epoch image fidelity.

## Frozen Workload Contracts

Both workloads use the same 12,150 by 12,150 SIN-projection image at 0.6 arcsec
per pixel, centered on field 1525. This holds the 2.025-degree output geometry
fixed while the selected visibility volume changes by a factor of 63.

| Contract | Single field | Raster patch |
|---|---:|---:|
| Stable ID | `vlass-fragment-single-field` | `vlass-fragment-all-fields` |
| Frozen selection name | `single_field` | `all_fields` |
| Field selector | `1525` | `1107~1127,1512~1532,1542~1562` |
| Phase center | field `1525` | field `1525` |
| MAIN rows | 10,400 | 655,200 |
| Used SPWs | `2~17` | `2~17` |
| Image pixels | 147,622,500 | 147,622,500 |
| Role | Pixel/FFT/product-dominated sentinel | Connected pointing/visibility-volume patch |

Each contract has an explicit measured cold-CF manifest with the `-cold`
suffix and an unsuffixed measured warm-reuse manifest. The pair shares the
exact same science, geometry, product, comparison, and CF plan-key contract;
only run/evidence role and repeat policy differ.

The common CASA science parameters remain those in `tclean.last`:

- `specmode='mfs'`, `stokes='I'`, `projection='SIN'`;
- `gridder='awproject'`, `wprojplanes=32`, `aterm=True`, `psterm=False`,
  `wbawp=True`, `conjbeams=True`, and `usepointing=True`;
- `normtype='flatnoise'`, `pblimit=0.0001`, and `mosweight=False`;
- `deconvolver='mtmfs'`, `nterms=2`, and `scales=[0,5,12]`;
- Briggs weighting with `robust=1.0`;
- `uvrange='<12km'`, `intent='OBSERVE_TARGET#UNSPECIFIED'`, and SPWs 2 through
  17; and
- for clean, `niter=2000`, `gain=0.1`, `nsigma=5.0`, `cycleniter=2000`,
  `cyclefactor=3.0`, `minpsffraction=0.05`, and `maxpsffraction=0.8`.

The frozen geometry also names `single_field_spw9`: field 1525, SPW 9, all 64
channels and four correlations, the archived UV-range/intent, and 650 verified
MAIN rows. Every manifest names one geometry selection and must match all of its
field, SPW, channel, correlation, UV-range, intent, and row-count facts.

### Reproducibility Overrides

The archived interactive session is incomplete: it has `interactive=True`,
`usemask='user'`, and no saved mask. The historical cluster CF-cache path and
`parallel=True` setting are also not portable. The canonical local oracle
therefore makes only these explicit overrides:

- `datacolumn='data'`, because this MS has no `CORRECTED_DATA` column;
- `interactive=False` and `parallel=False`;
- a run-owned absolute `imagename`;
- an external-disk CF cache identified by the complete CF-plan key;
- `restart=False`; and
- `niter=0` for the dirty oracle.

These are reviewed manifest fields, not sweep knobs. Recipe-backed workloads
reject `--set-imaging` and nonempty `run.env`; a proposed variant requires a
separately reviewed non-fiducial manifest rather than mutating frozen evidence.

Before clean parity begins, create one deterministic explicit CASA mask from
the accepted dirty fiducial, preserve it by checksum, and use that identical
mask for CASA and casa-rs. The resulting clean is a new reproducible fiducial;
it is not described as a reconstruction of undocumented interactive choices.

## Evidence Tiers

### Smoke

Use the verified `single_field_spw9` selection (field 1525, SPW 9, 650 MAIN
rows), 1,024 pixels, and `niter=0`. Retain AW terms,
`usepointing`, MT-MFS, weighting, and product generation. Run once with an
empty CF cache and once warm. This proves that the CASA/runtime/cache setup
works; it is not performance evidence.

### Turnaround

Keep the full image geometry and all fields required by the workload. If a
full-band iteration would take more than 30 minutes, select four well-separated
SPWs (`2,7,12,17`) or proportionally reduce channels in every selected SPW.
Do not drop patch fields or reduce the image until the specific experiment is
image-size scaling. Turnaround results guide implementation but cannot close a
performance requirement.

### Final

Use all 16 SPWs, all 64 channels, the full 12,150-pixel image, and the exact
single-field or 63-field selection. Final evidence includes both dirty and
deterministic-clean variants.

## Correctness Contract

### Required Products

Discover the complete CASA product inventory on the first oracle run and then
freeze it in the workload manifests. At minimum, compare every product CASA
writes from these families:

- `.image.tt0`, `.image.tt1`;
- `.residual.tt0`, `.residual.tt1`;
- `.model.tt0`, `.model.tt1`;
- `.psf.tt0`, `.psf.tt1`, `.psf.tt2`;
- `.sumwt.tt0`, `.sumwt.tt1`, `.sumwt.tt2`;
- `.weight` and `.pb` Taylor products where CASA writes them;
- `.alpha`, `.alpha.error`, and the clean mask; and
- restoring-beam and coordinate metadata.

No missing, extra, silently renamed, or semantically substituted product is
accepted without explicit signoff.

### Numerical And Structural Acceptance

Run the serial CASA oracle twice before freezing tolerances. Set each product's
tolerance to the tighter of its hard ceiling and a documented repeatability
floor allowance. Tolerances are frozen before casa-rs optimization begins.

Hard outer ceilings are:

- exact shape, coordinate frame, reference pixel/value, increments, units,
  masks, and product topology;
- beam major/minor relative error no greater than `1e-3`, beam position-angle
  error no greater than 0.1 degree, and source-centroid error no greater than
  0.05 pixel;
- peak and integrated source-flux relative error no greater than `1e-3`;
- `diff_rms_over_casa_rms <= 1e-3` and
  `diff_abs_max_over_casa_peak <= 5e-3` on the CASA-valid comparison domain;
- finite/nonfinite and mask topology identical on valid science regions; and
- no unexplained beam-scale or larger coherent structure in difference images.

Final reductions must stream over the full arrays. Sampled comparisons are
allowed for iteration and panels, but cannot provide the final numerical gate.
The full stream writes every native central-spatial-plane pixel to bounded-
memory disk-backed Float64 operand/difference stores plus an exact coverage map.
Beam-scale and larger structure checks consume that native plane and must
record complete, non-overlapping source-pixel coverage. Use the existing
beam-aware `structured_difference` metrics and same-scale CASA/casa-rs panels.
Low-amplitude structured `.weight` or `.pb` differences remain correctness
failures until explained and accepted.

Comparator schema-v4 request hashes bind every normalized operand, label,
requested product/path, inventory/metadata policy, source box, tolerance,
chunk budget, panel destination, and absolute structure-workspace path. Results
are accepted only when that binding, exact native-plane evidence, the exact
product inventory, and protocol request/result/log digests validate. The run
receipt must pass its strict schema and every required constituent comparison,
and every successful structure workspace must be absent, before the complete
same-parent bundle can be atomically published; all other states retain a typed
partial bundle.

## Evidence Storage And CF Identity

The raw and fully resolved MeasurementSet paths, output receipts, product and
protocol bundle, scratch/temp paths, masks, panels, logs, and CF caches must all
remain beneath `/Volumes/GLENDENNING/casa-rs-vlass/issue-446`, avoid the generic
disposable tree, and share the mounted dataset device. Resolving the MS before
the second boundary check prevents a symlink from escaping that root.

The CF-affecting CASA projection is exactly: field/SPW; image size, cell, phase
center, Stokes and projection; spectral definition and interpolation; gridder,
facets, PSF phase center, W planes and `vptable`; A/P/WB/conjugate-beam terms;
pointing/parallactic-angle controls; and `pblimit`. Runtime/measures, recipe,
verified MS, named-selection and frequency identities are bound separately.
Mask, deconvolution/minor-cycle/restoration controls, output paths, and casa-rs
memory/worker/backend policy are deliberately excluded. A relocated mask is
instead content-addressed in each request and revalidated immediately before
`tclean`, so it cannot silently change while also not fragmenting the CF key.

Warm evidence requires a separately completed matching cold cache and receipt
and never bootstraps cold. Exact-request replay may recover only the publication
of a completed cold cache with its commit-intent receipt; it does not reinvoke
`tclean`, reports `recovered_publication`, and is non-benchmark evidence.

## Performance Contract

The primary metric is end-to-end wall clock, including MS open/selection,
weighting, gridding, FFTs, deconvolution and residual refresh, normalization,
restoration, PB/weight work, and writing the complete matched product set.

For each of the four final rows (two field selections by dirty/clean):

```text
speedup = median CASA wall seconds / median casa-rs wall seconds
required speedup >= 10.0
```

Measurement rules:

- same workstation, dataset path, output volume, science parameters, and
  product set;
- CASA 6.7.5.9 single-process is the fixed comparison baseline;
- warm CF-cache performance is the 10x gate; cold CF creation/loading is
  reported separately for both implementations;
- at least three counterbalanced warm runs after one unmeasured warm-up, unless
  a final run exceeds 60 minutes, in which case record the approved bounded
  schedule explicitly;
- for current development, Brian approved the bounded all-fields schedule
  recorded above: use 8,183.264 s as the conservative baseline and do not spend
  further hours on CASA repetitions solely to refine statistical precision;
- preserve CASA products and timing once parameters are frozen; do not rerun
  CASA merely because casa-rs changes;
- report total wall time first, then stage timings;
- record peak RSS, memory pressure/swap change, bytes read/written, CF-cache
  size, worker plan, grid residency, CPU/GPU utilization, and fallback reasons;
  and
- final runs must finish without OOM or genuinely untenable sustained
  thrashing. Noticeable finite swapping is allowed while the host remains
  operational and CASA makes meaningful stage/pass progress; stop for an
  effectively unusable host, prolonged swap-dominated execution with negligible
  progress, credible stability/storage risk, or opaque periods longer than
  three minutes without stage/pass progress.

The exact 12,150-pixel geometry is the active comparison geometry. It completed
successfully for both frozen selections with tolerable swapping, so the
8,192-pixel fallback is not active. Any future geometry change is a scope
change requiring new matched CASA products and timing.

An explicit serial CPU casa-rs baseline remains in every evidence bundle even
when `auto`, multi-worker CPU, or Metal is faster. The final user-facing `auto`
plan must select the winning safe backend without diagnostic environment
variables.

## Known Capability Gaps At Plan Start

The current checkout cannot run either full contract:

- `--gridder awproject` is a W-projection-only alias and reports A-term CF
  planning as unimplemented;
- production selection rejects multiple `DATA_DESC_ID` values, so SPWs 2
  through 17 cannot yet be imaged together;
- the bounded MT-MFS and mosaic MT-MFS routes reject `usepointing=True`;
- mosaic MT-MFS excludes W/AW combinations; and
- the workload schema does not express all VLASS AW, pointing, intent,
  UV-range, CF-cache, normalization, common-beam, and mask controls.

The plan closes these as shared capabilities. It must not add a VLASS-only
materialization path or mislabel W-projection as AW parity.

## Execution Plan

### 1. Freeze Data And Workload Receipts

- Verify the archive hash and gzip integrity before extraction.
- Stage the extracted MS and all large products on GLENDENNING, not the internal
  disk.
- Record row/channel/correlation/field/SPW/POINTING geometry in a small receipt.
- Add two stable workload manifests and one mechanically derived CASA recipe
  snapshot.
- Record CASA, casa-rs, OS, hardware, git, dataset, recipe, and CF-plan
  identities in every result.

Acceptance: both manifests dry-run to exact, reviewable CASA and casa-rs command
plans; no large personal dataset becomes an implicit test fallback.

### 2. Extend The Shared Evidence Harness

Extend `tools/perf/imager/run_workload.py`, its strict schema, and
`scripts/bench-imager-vs-casa.sh` rather than creating a VLASS-only runner.
Add the missing evidence-workload fields, cold/warm CF-cache roles, full Taylor
product inventory, full-array streamed comparison, peak-memory evidence, and
progress capture. These manifest fields are an internal evidence contract, not
the production `ParameterCatalog` or a substitute for #450.

Frozen recipe-backed manifests reject `--set-imaging` and nonempty `run.env`.
They bind the named dataset selection, use an explicit CF-affecting projection,
keep mask identity at request level, require an independent cold receipt before
warm execution, and validate comparator request/result hashes plus whole-bundle
publication integrity. Publication recovery is reachable through exact request
replay but is always marked non-benchmark.

Acceptance: focused harness tests, dry-run snapshots for both workloads, and a
green smoke bundle containing products, comparisons, logs, wall clock, stage
timing, memory, and cache receipts.

### 3. Generate CASA Fiducials

- Preserve the completed exact full-array cold/warm smoke receipts; the earlier
  2026-07-20 21:49/21:51 receipts remain historical only.
- Run full-size dirty single-field CASA twice for repeatability and once for the
  frozen timing.
- Run full-size dirty all-field CASA on the same image grid.
- Define and preserve the explicit clean mask.
- Run the deterministic-clean single-field and all-field CASA fiducials.
- Freeze product lists, tolerances, and CASA timings.

Acceptance: complete CASA artifact bundles exist for all four final rows. No
CASA rerun is needed unless data or CASA parameters change.

### 4. Build A Correct Bounded Serial Reference Path

Implement in shared imaging infrastructure:

1. multi-SPW / multi-DDID MFS streaming with correct frequency, weight-density,
   Taylor-term, and selection semantics;
2. selection-windowed POINTING resolution for the 3.335-million-row subtable;
3. a real EVLA wideband A+W projection plan with 32 W planes, A-term,
   wideband-AWP, conjugate-beam, parallactic-angle, and reusable CF-cache
   semantics;
4. pointing-aware joint MT-MFS for one or many fields without retained full-MS
   materialization;
5. CASA flat-noise normalization, common-beam restoration, multiscale clean,
   cycle controls, and full Taylor/PB/weight/alpha products; and
6. bounded, progress-reporting product output.

Build the serial CPU implementation first as the auditable correctness
reference. Reuse existing selection, streaming, weighting, projection-plan,
MT-MFS, product-writing, and planner boundaries; remove superseded paths rather
than maintaining duplicate implementations.

Acceptance: both dirty and clean workloads meet the complete correctness
contract in serial CPU mode before performance claims begin.

### 5. Capture Initial Performance Evidence

Run serial CPU casa-rs on the turnaround and final rows. Attribute total time to:

- MS open, row selection, and column reads;
- POINTING indexing and per-sample direction resolution;
- preparation, frequency/Taylor metadata, and weighting density;
- CF generation/load and A/W gridding/degridding;
- Taylor PSF/residual FFT, correction, and normalization;
- multiscale minor cycle and every major-cycle residual refresh;
- restoration, PB/weight/alpha generation; and
- each product-write family.

Capture live/peak bytes per full image plane, Taylor scratch set, visibility
block, CF cache, worker scratch, and output buffer. Record a flamegraph or
equivalent sample profile for the dominant CPU stages.

Acceptance: one evidence packet ranks measured bottlenecks separately for the
single-field and all-field workload; no proposed optimization depends only on
speculation.

### 6. Oracle Evidence Review And Plan Revision

Only after step 5, use the Oracle skill through the Chrome plugin in a fresh
ChatGPT conversation. The compact prompt must include:

- the two frozen manifests and correctness contract;
- total and stage timings, peak-memory/residency plan, profiles, and hardware;
- the 10x gate and 32 GiB unified-memory constraint;
- exact shared code boundaries and current backend plans; and
- a request for ranked memory/dataflow, multi-worker CPU, and GPU experiments,
  each with predicted benefit, correctness risk, and a falsifying measurement.

Evaluate Oracle advice against current source and evidence; do not relay it as
authority. Translate accepted recommendations into a revised ranked experiment
ledger. Reuse the verified conversation for concise evidence-delta follow-ups,
and leave the completed conversation open as a user-visible deliverable.

Acceptance: the ledger records adopted, rejected, and deferred Oracle proposals
with local rationale. A generic or evidence-free Oracle answer does not change
the plan.

### 7. Memory And Dataflow Experiments

Test measured hypotheses such as:

- time-windowed/indexed POINTING access instead of loading unrelated rows;
- compact per-pointing/per-SPW dictionaries and row/run preservation;
- bounded read-ahead with overlap of I/O, preparation, and gridding;
- one traversal for all required Taylor PSF/residual moments;
- scratch reuse, in-place transforms, and elimination of full-image copies;
- f32 grids or mixed precision only where the correctness gate remains green;
- tiled/streamed normalization and product writing; and
- persistent, keyed CF-cache residency without repeated decode or rebuild.

Each experiment states a predicted owner and speedup, changes one architectural
hypothesis, reruns correctness first, and then reports total wall time. Memory
work must not trade bounded residency for a hidden full materialization.

### 8. Multi-Worker CPU Experiments

Start from the best correctness-green memory plan. Test worker counts and
ownership schemes for selection/preparation, CF work, gridding/degridding,
Taylor planes, FFTs, minor-cycle scale work, and product output.

Avoid a complete 12,150-pixel grid per worker. Prefer disjoint output tiles,
bounded worker scratch, deterministic reductions, and measured producer/
consumer overlap. Counterbalance serial and worker runs and retain the serial
result in the bundle.

Acceptance: choose workers from measured total wall time and memory pressure,
not core count. Any change in numerical reduction order must remain inside the
frozen parity limits.

### 9. Metal/GPU Experiments

Start from the best CPU/dataflow plan and keep the 32 GiB unified-memory budget
explicit. Measure:

- grouped, compact visibility/CF inputs;
- A/W convolutional grid/degrid kernels;
- resident batched Taylor FFT/correction/normalization;
- multiscale convolution and minor-cycle peak work;
- resident major-cycle prediction and residual refresh;
- PB/weight/alpha finishing; and
- direct or tiled product output paths that avoid unnecessary host copies.

Prefer keeping grids resident across adjacent stages over isolated kernels with
large upload/readback costs. Log eligibility, placement, command timing, staged
bytes, cache hits, and every fallback. `auto` may fall back safely; an explicit
Metal request must fail closed rather than silently measuring CPU.

Acceptance: GPU results include end-to-end wall time and transfer/residency
evidence. A faster kernel with slower total runtime is not a win.

### 10. Integrate `auto` And Close

- Combine only independently verified wins.
- Teach explicit public parameters and `auto` to choose safe memory, worker,
  and Metal plans from image/visibility/CF shape and available resources.
- Represent every new imaging and execution capability once in the canonical
  parameter catalog and project it with identical semantics through CLI, TUI,
  native macOS, Python, sparse profiles, and assistant/task schema surfaces.
  Specialist AW/CF/resource controls belong in a clear advanced wide-field
  section; no surface may silently simplify an unsupported request.
- Account explicitly for every production request or resolved-plan field added
  or changed by this wave family, including every user-selectable field in
  `ImagerRunTaskRequest` and `ImagingExecutionPlan`. Each field must either bind
  to one canonical catalog concept and round-trip through CLI, TUI, native
  macOS, Python, sparse profiles, and assistant/task schemas. Only a resolved-
  plan implementation detail that is not user-selectable may instead be
  classified as internal and non-persistable, with a recorded rationale. No
  field may remain unclassified, schema-only, CLI-only, or environment-only.
- Run both final workloads, dirty and clean, with CASA products reused.
- Produce same-scale panels, full metrics, timing tables, stage budgets, memory
  plans, and a concise human review artifact.
- Run the relevant focused tests and `just verify` before Review.

Keep #450 open through #449 so that measured memory, worker, Metal, CF-cache,
and final `auto` controls are included in the cross-surface accounting. Close
only when all four final rows are correctness-green, each reaches at least
10.0x matched CASA wall clock, and the complete field-accounting contract is
green. Record the actual achieved speedups; do not round a miss upward.

Until the post-hardening smokes and exact fiducials exist, #446 stays open and
the implementation PR stays draft. Do not use `Closes #446` or treat the
capacity-stop partial as accepted evidence.

## Iteration Rules

- Correctness regression stops performance iteration immediately.
- If a large run is opaque for more than three minutes, stop it and add stage
  or pass progress before retrying.
- If an estimated iteration exceeds 30 minutes, use a mode-faithful turnaround
  row. Runs exceeding 60 to 90 minutes are reserved for final evidence or
  explicit approval.
- Keep single-field and raster-patch ledgers separate; their dominant owners
  may differ.
- Measure serial, multi-worker, and Metal end to end. Do not infer a win from a
  component benchmark.
- Do not rerun fixed CASA oracles after casa-rs-only changes.
- Optimization controls graduate from diagnostics to explicit parameters; the
  final path cannot depend on hidden environment variables.
- Non-destructive experiments are pre-approved under the experiment boundary
  above. Stop for approval when evidence supports final incorporation, not
  merely to create or run the experiment.

## Stop Conditions

Stop and request direction rather than changing the contract if:

- the full CASA or casa-rs geometry cannot complete on the 32 GiB host;
- a requested optimization requires weakening or deleting a parity product;
- the deterministic clean mask or CASA-valid comparison domain cannot be
  frozen objectively;
- the all-field workload reveals that a connected-fragment assumption is wrong;
- either workload remains below 10x after the measured memory, CPU, and GPU
  owners have been exhausted; or
- completing the goal requires distributed execution or a different machine;
  or
- the best measured result requires final incorporation of a materially
  different algorithm, substantial dependency, runtime/default, public API,
  persisted format, provider contract, or concurrency guarantee that Brian has
  not yet approved.

Reduced workloads, accepted correctness differences, target changes, or scope
deferrals require explicit Brian signoff. They are not implicit closeout paths.
