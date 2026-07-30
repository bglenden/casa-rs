# VLASS Fragment Imaging Correctness And Performance Plan

Truth class: approved execution contract
Last reality check: 2026-07-30
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

### Iteration And Promotion Ladder

Brian redirected the wave's iteration strategy on 2026-07-27 without changing
its approved scope or acceptance criteria. Full-size `12,150`-square clean
runs are no longer development-turnaround rows. They are reserved for
promoted final candidates after the reduced real-VLASS gates below pass.
Existing full-size dirty evidence and frozen products remain authoritative;
this redirection does not replace or weaken any final laptop gate.

The normal single-field clean correctness and performance gate is the real
VLASS field-1525 workload at `4,096` by `4,096`, using SPWs `2,7,12,17` and all
64 channels from each SPW. It retains the production recipe rather than a
simplified proxy:

- CASA AWProject with 32 W planes, A term, wideband A projection, conjugate
  beams, and the CASA CF cache;
- real POINTING resolution, Briggs weighting with robust `1.0`, and the
  existing UV and intent selection;
- MT-MFS with `nterms=2`, scales `[0,5,12]`, the checksum-bound deterministic
  mask, `niter=2000`, `gain=0.1`, `nsigma=5.0`, and the frozen major/minor-cycle
  controls; and
- the exact 19-product numerical, topology, metadata, and inventory comparison
  contract, including mask, PB, PSF, residual, model, restored image, Taylor
  weight/sum-weight, alpha, and alpha-error products.

Smaller diagnostic rows may be used only to isolate one semantic boundary.
They cannot promote an implementation. Each reduced CASA oracle is generated
once, checksum-bound, and frozen. An unchanged CASA reduced case must be
reused, not rerun for timing repetition or convenience.

Promotion proceeds in this order:

1. Make the `4,096`-square four-SPW single-field clean row correct and fast.
2. Validate a `4,096`-square full-16-SPW single-field row against its one-time
   frozen CASA oracle.
3. Run the frozen `12,150`-square 16-SPW single-field dirty and clean final
   rows on the acceptance laptop only after the implementation is a promoted
   candidate.
4. Apply the same four-SPW, then 16-SPW, then final-size progression to the
   all-fields workload without reducing its 63 FIELD IDs, POINTING rows, or
   mosaic behavior.

Every promotion requires CASA-equivalent multiscale component selection and
major-cycle residual behavior with no iteration divergence; every numerical,
mask/topology, metadata, product-inventory, and protocol-integrity gate green;
bounded planner-accounted memory; and credible end-to-end stage timings.
Performance-only wins, matched final peaks with divergent component histories,
or partial product comparisons do not promote.

The final acceptance contract remains unchanged: single-field and all-fields,
dirty and deterministic clean, at the frozen `12,150` by `12,150` 16-SPW
geometry; full CASA correctness for every required product; and an independent
minimum 10x speedup for each final row on the acceptance laptop. Reduced rows
are development evidence only.

### Required Full-Geometry Memory Campaign

Brian added memory as a measured optimization dimension on 2026-07-30 without
changing the wave scope, correctness contract, or iteration ladder. Routine
`12,150`-square clean development runs remain prohibited. First promote the
`4,096`-square full-16-SPW candidate. Then use bounded planner dry-runs and the
required full-size dirty row to reject untenable policies before launching a
full clean row. Stop a run early when swap thrashing, memory-pressure stalls,
or projected runtime is clearly destructive. Do not rerun any unchanged CASA
reference.

Every full-size receipt records per-stage and peak process physical footprint
and RSS; CPU and Metal/unified-memory allocations; compressed memory; swap used
and swap-in/swap-out deltas; page faults where available; external-disk read
and write volume; replay-program, grid, FFT, product, CF, and transient
materialization bytes; stage timings; and GPU stalls. The planner and receipt
must also produce an explicit lifetime ledger. It must account in particular
for the approximately 17.6 GiB initial compensated eight-plane AW grid, the
current approximately 7.31 GiB compact replay programs, residual-cycle grids,
model grids, FFT staging, product arrays, and temporary f64 conversion or
readback storage. No promoted run may have an unaccounted allocation above the
planner ledger.

Compare each of these policies with bounded, non-repeated experiments:

1. conservative no-swap admission with partial replay retention;
2. aggressive use of nearly all physical memory while allowing compression
   and modest swapping;
3. intentional oversubscription only far enough to locate the boundary where
   swapping changes from tolerable to destructive;
4. application-managed, stage-aware release and demotion: release PSF/weight
   and initial dirty grids immediately after final use; stream or memory-map
   the 19 products; spill or memory-map compiled replay blocks while the
   initial grids are live; and reload or prefetch them after that peak; and
5. a hybrid of high physical-memory utilization and explicit eviction based
   on known next use.

Neither macOS swapping nor application-managed eviction is presumed superior.
Ordinary LRU is specifically suspect because all 16 replay blocks are visited
cyclically and a sub-working-set cache can thrash; prefer measured
next-use-aware staging or pinned subsets. Replace the artificial quarter-memory
replay ceiling with accounting based on actual compact resident-program bytes
and their stage overlap.

Promotion from this campaign requires unchanged CASA component selection and
major-cycle trajectory; every 19-product numerical, topology, metadata, and
inventory gate; no divergence; credible end-to-end and stage timings; a
recorded peak-memory and swap receipt; no unexplained allocation; and
successful operation on the 32 GiB laptop. The production planner must adapt
to detected physical memory, current headroom, unified-memory requirements,
CPU/GPU characteristics, measured storage bandwidth, and the selected
memory-pressure policy. Public task and UI surfaces expose a memory target and
memory-pressure policy with a safe automatic default. Intentional dependence
on swap, a persisted replay-cache format, or another materially different
production default still requires Brian's approval after the experiment
evidence is presented.

That promotion is explicitly a memory-campaign promotion, not final wave
acceptance. Its receipt uses scope `memory-campaign-only`, status
`memory-candidate-promoted`, and embeds the still-unsatisfied
`vlass_final_four_row_10x_acceptance` contract. Final acceptance remains four
independently measured, content-addressed matched CASA/casa-rs rows:
single-field dirty, single-field clean, all-fields dirty, and all-fields clean,
each at least 10x. Their CASA baselines are supplied only by the real matched
receipts; this campaign does not invent missing baseline values.

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
Brian subsequently approved adding an f64 FFTW backend. That closes the
algorithm/dependency choice gate, but not the packaging gate: the current
`fftw-local-bench` loader points at a workstation CASA application bundle and
must become a portable, licensed production dependency/backend before final
`auto` integration.

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

A bounded four-cell CF-load batch was also slower than the retained one-cell
lookahead. The first block took 69.750 seconds versus 64.525 seconds, an
8.10-percent regression. Materialization was 36.225 seconds, while concurrent
loads increased tap packing to 25.507 seconds and grid work to 13.565 seconds;
the added memory-bandwidth and cache contention outweighed parallel I/O. The
run was stopped after the first block, and the load-batch code and manifest
control were removed rather than retained as a second runtime path. Its
interrupted receipt and 2,333,794-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T203235Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-433ed3d8.json`
(`53a49fb0292fc2a92b6d567b7fca575069a4ea3cc30db8c17385650a864baae2`).

Halving only the compact-tap arena to 512 MiB while retaining the 1 GiB CF
cache also failed the first-block criterion. It increased the number of exact
source-order windows from 63 to 160, raised CF-load worker time from 20.651 to
53.687 seconds, and raised first-block replay from 64.525 to 108.537 seconds.
The extra boundaries defeated the successful alternating reuse pattern. The
run was stopped after the first block, and the internal tap-budget override
was removed. Its interrupted receipt and 2,333,841-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T203849Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-6ce370b7.json`
(`5376d1fb33eb831c0bcffe9dc768177409aef9ced2535c830462afe8bcf6b6f4`).

Increasing only that arena to 1.5 GiB, temporarily borrowing 512 MiB from the
logged 1.7 GiB safety margin, did not help either. It reduced the first block
to 46 windows but raised planning to 23.319 seconds, tap packing to 34.011
seconds, grid work to 15.227 seconds, and total replay to 82.685 seconds.
The run was stopped after the first block, the override was removed, and the
already admitted 1 GiB arena remains the measured optimum among these three
sizes. The interrupted receipt and 2,333,777-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T204330Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-64476dd8.json`
(`b7d81710827433a31c850a1ab05c9555237d8a2a7700b0a43e364f5c06e8814f`).

Factoring the exact CASA phase-gradient construction into reusable x and y
axis phasors was positive. CASA first narrows each axis phasor to Complex32,
promotes both values for a Complex64 multiply, and narrows the product again;
the retained implementation preserves those rounding boundaries while
evaluating trigonometric functions once per axis coordinate instead of once
per two-dimensional tap. Exact focused replay tests remained bit-identical.
On the four-SPW all-fields turnaround, complete replay fell from 540.970 to
452.208 seconds (16.41 percent), and the complete warmup through all eighteen
product writes fell from 595.516 to 492.692 seconds (17.27 percent). First
block tap packing fell from 24.068 to 15.078 seconds, while all 6,416,526
samples were accepted and the terminal CF counters remained exactly 18,907
loads, 19,139 hits, and 18,879 evictions. The redundant measured invocation
was stopped. The interrupted receipt and 5,350,081-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T204904Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-d8b0ce4a.json`
(`0af6b891d2af31e1decf348b5a46defb1cd53b26dce698b1a5ee0b75b8e95327`).
The combined phase-factorization, one-cell lookahead, and four-plane-worker
path still requires the planned fresh eighteen-product single-field
comparison before incorporation.

That fresh comparison then passed. The measured complete single-field
invocation took 66.378748 seconds, 17.552 percent faster than the preceding
80.509509-second exact-source-order candidate, 19.225x faster than the frozen
1,276.157-second CASA row, and 61.236952 seconds below the independent 10x
boundary. All 385,862 samples were accepted. The exact eighteen-product
inventory matched; full-array numerical, topology, metadata, beam,
source-region, and structured-difference contracts passed with zero failed or
incomplete checks; the overall structured-difference label was `good`; and
twenty panels are ready for Brian's still-required visual review. Its
immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T210158Z-vlass-fragment-single-field-fftw-f64-experiment-7dc431a7.json`
(`694d3a255c5e67b589adbef77ff8570324c4a0534f6749c8783b9efd92c63ff9`).
This supplies the full-product correctness evidence for the combined
lookahead, four-plane-worker, and phase-factorized AW replay experiment. Brian
explicitly approved incorporating the compact exact-source-order replay,
adaptive segmentation, and production runtime change on 2026-07-26; commits
`86a57a2e4` and `175c122f4` contain that implementation. Human panel
acceptance and the remaining portable FFTW/resource-control integration stay
separate gates.

A more aggressive factorization that shared unphased raw taps across POINTING
groups was negative on CPU and was removed. It preserved exact source order
and CASA's phase-product rounding, charged one raw-tap array plus every
per-group axis vector, and passed all 290 active `casa-imaging` tests. On the
four-SPW all-fields first block it reduced source-order windows from 63 to 47,
materialization from 29.517 to 25.784 seconds, and tap packing from 15.078 to
11.072 seconds. Reconstructing each phase-applied tap during every grid
contribution, however, increased grid work from 11.586 to 26.221 seconds and
raised the block from 58.814 to 72.957 seconds (24.05 percent slower). The run
was stopped after the first block and the factorized runtime representation
was fully removed. Its interrupted receipt and 2,333,777-byte log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T212748Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-3eaaba72.json`
(`229503d42a59563108011b26a2f17c655989c3f9c70ab5aa5578340f0d09cf82`).
The result suggests raw-tap sharing is useful only if a GPU or another fused
consumer can absorb the extra phase multiply more cheaply than the CPU path.

A direct Float32 Metal-atomic experiment then removed fixed-grid plane
segmentation and accumulated all eight resident output planes across every
source-order window. It retained the f64 FFTW finish and zero compensation
readback, so the experiment isolated unordered Float32 grid summation. All
385,862 samples were accepted. The measured complete invocation took
137.660846 seconds, while its initial-dirty pass took 96.915 seconds. The
actual GPU accumulation across 114 windows took only 7.487 seconds, but the
path still packed 3,610,756,844 phase-applied Complex32 kernel values;
materialization and transfer, not the atomic kernel, owned the wall clock.
The redundant profile invocation was interrupted after the completed measured
products were retained. Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T214047Z-vlass-fragment-single-field-metal-fftw-f64-experiment-c17cc14c.json`
(`9e0c755ad6475bcf46c54cb0a1b75642d63822dfd6913d122939bce797137f63`).

The frozen full-array comparator rejected those products. Direct Float32
summation changed 39,703 mask pixels in the image, residual, and PB products
and 1,419,558 pixels in each alpha mask. PSF Taylor terms remained close, but
the weight-term structured-difference reviews were `investigate`,
`investigate`, and `bad`; weight TT2 had relative RMS
`1.846013502377971e-4`. Normalization amplified the weight-grid error in the
residual and image terms, whose relative RMS differences exceeded 0.82.
The bound comparator output is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/vlass-fragment-single-field-metal-fftw-f64-experiment/experimental_metal_grid_fftw_f64/20260726T214047Z-vlass-fragment-single-field-metal-fftw-f64-experiment-c17cc14c.partial/comparisons/direct-f32-measured/frozen-casa.comparison.json`
(`9cedb040e9aacfde9aa9e1fddf61e239fdcffc6b08c60f85467bc06b0c2517b8`).
Direct Float32 accumulation is therefore rejected. The useful result is
architectural: a viable GPU path must eliminate phase-applied tap packing
while retaining deterministic fixed64 accumulation.

A subsequent Metal experiment did exactly that while retaining the existing
full-grid fixed64 accumulator. Raw CF taps were shared by cell/kind/offset and
exact precomputed Complex32 phase tables were shared by
POINTING/support/offset; a focused Metal test proved the GPU factor multiply
bit-identical to the pre-phased fixed64 dispatch. All 291 active
`casa-imaging` tests passed. The full 12,150-square probe nevertheless failed
the performance stop rule. Each source-order window carried about ten million
raw taps and five million shared phase values within the 121,670,694-byte
packed ceiling, but the 2,361,960,000-byte fixed accumulator admitted only one
output plane. Every window therefore cleared, dispatched, and finalized eight
full-image segments. The first three of sixteen SPW blocks reached 26.282,
14.675, and 14.444 seconds respectively, or 55.406 seconds before one fifth
of the input, so the run was interrupted rather than spend most of another
baseline on a known loss. Its retained receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T221329Z-vlass-fragment-single-field-metal-fftw-f64-experiment-a4a1b516.json`
(`a417d9af30540aaafab35dfce1a8dbec23550d60a65e809c679c40e98bd86889`).
The next bounded Metal candidate must compact the fixed64 accumulator to
touched tiles (or otherwise eliminate full-image clear/finalize per window);
further tap-packing changes cannot own the missing speedup while every window
streams 18.9 GB of fixed-grid limbs.

A touched-tile fixed64 experiment then replaced that full-image accumulator
with deterministic 128 by 128 tiles selected from the exact support of every
source-order window. A focused Metal replay test was bit-identical to the
pre-phased full-grid fixed64 result. On the full 12,150-square workload, the
windows touched only 20 to 122 of roughly 9,000 image tiles. All eight Taylor
planes consequently fit in one segment, with 42 to 256 MB of fixed limbs per
window, and steady-state Metal dispatch fell to approximately 85 to 121 ms.
The old full-plane reservation nevertheless continued to cap packed taps at
121,670,694 bytes. CF loading and exact raw/phase materialization therefore
dominated: the initial-dirty pass had reached 153.366 seconds at 7,800 of
10,400 rows, already beyond the independent 127.616-second 10x boundary, so it
was stopped before a redundant measured invocation. Its retained interrupt
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T222223Z-vlass-fragment-single-field-metal-fftw-f64-experiment-83235e87.json`
(`b86a6ecedb4dc07ba8990202bf14356cb9152dd3c92eb10a5f44f43eedc16852`).
The next experiment may reclaim the obsolete full-plane reservation for a
larger packed window, but must charge exact tile metadata and fixed-limb
residency before final incorporation.

Reclaiming that reservation while retaining the existing 256 MB tap ceiling
was positive but insufficient. Windows fell from 16--28 per SPW block to
4--12, and the first block fell from 10.387 to 7.863 seconds despite including
the one-time Metal pipeline setup. Later CF materialization still dominated:
the initial-dirty pass reached 92.845 seconds at 7,150 of 10,400 rows, leaving
less than the 127.616-second boundary for five remaining blocks plus all FFT
and product finishing. The run was stopped before the redundant measured
invocation. Its retained receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T222925Z-vlass-fragment-single-field-metal-fftw-f64-experiment-31149613.json`
(`0dba1982587112d311b3ef62a8be032cc8edea7fe77090a2ab833dc2e4c0a171`).
The follow-up planner therefore grows the experimental tap window beyond the
CF-resident ceiling only while charging exact active-tile lookup/list bytes,
packed sample and Taylor weights, and fixed64 high/low limbs for all eight
planes against the admitted Metal scratch budget.

An adaptive 768 MiB packed-tap window then completed the full-geometry f64
Metal warmup in 142.744412 seconds. Exact replay took 102.193344 seconds,
the Metal grid summary charged 40.982 seconds, and the f64 finish took 19.094
seconds; the run-imaging and product-write stages were 134.46 and 5.777
seconds. This proved the compact tiled fixed64 topology is viable at full
geometry but still misses the independent 127.6157-second target. Its receipt
is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T223806Z-vlass-fragment-single-field-metal-fftw-f64-experiment-ee901572.json`
(`1022016a1f13513e74257eb2d7421603ee71c8be9b7cde5201a266834526b5fc`).

Reusing the CPU path's 1 GiB CF and 1 GiB tap requests was negative under the
shared 32 GiB Metal ledger. Charging 1 GiB of resident CF pixels reduced the
actually admitted tap window to 873,017,958 bytes, increased each SPW block
to six through eight exact windows, and reached 121.441 seconds at only
9,750 of 10,400 rows. The run was interrupted before completing a known loss,
and the Metal manifest was restored to 256 MiB CF residency and a 768 MiB tap
request. Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T233319Z-vlass-fragment-single-field-metal-fftw-f64-experiment-97abc283.json`
(`907930fd7de4e7da4824d015dff8bd39d7cc4152774150ce12e427b9731ce3b8`).

The approved Float32 Metal FFT experiment retained deterministic fixed64
gridding and rounded the high-plus-low compensation limbs exactly once before
MPSGraph. Direct eight-plane execution produced non-finite or zero PSF
normalization both on the complete 16-SPW workload and on a same-geometry
SPW-9 probe. Segmenting the merge dispatch did not change that result, which
isolated the failure to the downstream large-batch MPSGraph topology rather
than replay or the compensated merge. The retained receipts are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T224941Z-vlass-fragment-single-field-metal-fft-f32-experiment-09a2d0cb.json`
(`ab6bc35b1d9dfa896121d66af5f00da7ef79946fe2be29c2fcee0058b4badee2`)
and
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T225916Z-vlass-fragment-single-field-spw9-metal-fft-f32-segmented-probe-9f676c33.json`
(`07921a073fbd149ca8a9bcda6fc91942954e25a71338a1b85ac3554b41d3d1fa`).

Executing one 12,150-square plane at a time bounded MPSGraph memory and
produced all eighteen finite products. The non-fiducial SPW-9 diagnostic took
48.467369 seconds measured, including 9.244 seconds in the plane-chunk FFT;
its expected comparison failure reflects the intentional one-SPW selection.
Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T230344Z-vlass-fragment-single-field-spw9-metal-fft-f32-segmented-probe-8631bbbc.json`
(`0e48cb15941c33e688a966ed185a09c5cc9f278b751ac4b37f6d0d8e59bc995c`).

The scientific 16-SPW run then measured 132.570199 seconds, 9.626x faster
than CASA and only 4.9545 seconds outside the 10x boundary. It nevertheless
failed correctness: residual TT0 and TT1 differed by 0.4156 and 0.4182
percent RMS against a 0.1 percent ceiling, and by 2.797 and 3.000 percent at
peak against a 0.5 percent ceiling. PSF Taylor terms, sumweights, and weight
terms remained numerically close, but the residual error propagated to image,
alpha, and mask products. Float32 FFT is therefore rejected for frozen CASA
parity despite its performance; further cache or unpack tuning on that path is
not justified. The immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T231427Z-vlass-fragment-single-field-metal-fft-f32-experiment-3d594222.json`
(`37987401918c28ac8f29da8172e53727c27fc31f535956c4f05fc8e5430ea286`).

The next exact-f64 Metal family removed raw-tap extraction entirely. It packed
the underlying CASA CF pixels into a direct-cell arena, retained the exact
CASA-rounded POINTING phase tables and fixed64 touched-tile accumulation, and
looked up each source-ordered tap on the GPU from support, sampling, offset, and
conjugation metadata. Rebuilding an arena from every window's complete cells
was immediately negative: repeated 90--97-million-pixel arenas reached 87.935
seconds at only 3,250 rows. The operator stopped that noncompetitive run; its
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260726T235102Z-vlass-fragment-single-field-metal-fftw-f64-experiment-f2b65a8e.json`
(`1f775b30469e9db87dd0fe6f5da8bed07b54519534c2f37950282527a867d4b8`).

Retaining one dense direct-cell arena for each source block made the low-SPW
blocks fast but exposed the exact memory boundary at high frequency. The first
three arenas were 1.619 GB, 0.814 GB, and 2.434 GB; the later 2,519,007,236-byte
arena exceeded the 2,483,630,694-byte scratch ledger. Cropping every resident
cell to the exact support/sampling/offset bounding box reduced traffic and
reached 7,150 rows in 70.339 seconds, but a later 2,658,753,988-byte crop still
required 2,671,373,040 bytes with six active tiles. The dense and cropped
capacity-failure receipts are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T000309Z-vlass-fragment-single-field-metal-fftw-f64-experiment-19ec9bc5.json`
(`8996ba70524ff318d7f59c6c22ecc1036f2a84afc0d819b3847415bc5ef0a65d`)
and
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T001121Z-vlass-fragment-single-field-metal-fftw-f64-experiment-ad3cbbdf.json`
(`34742387b96b1fc78441b1ffc46a72de0c8eadcdbbf49d5699d8793cccb6ee00`).

Adaptive exact-source-order segmentation then binary-searched the largest
source prefix whose cropped arena fit a planner-selected target. A 60-percent
scratch target was too conservative: it split normal blocks into six through
eight arenas, repeated CF loading, and reached 3,900 and 4,550 rows in 42.014
and 60.200 seconds. That known loss was stopped and retained at
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T001824Z-vlass-fragment-single-field-metal-fftw-f64-experiment-fe288255.json`
(`131765d31c74d2752750e6234cdb80f5924a2fafcdd1ef34671a3550021b45d7`).
The final bounded experiment instead reserved 64 MiB for packed samples, phase
tables, descriptors, tile metadata, and fixed limbs, leaving a
2,416,521,830-byte direct-cell target inside the 2,483,630,694-byte scratch
budget. Normal blocks stayed whole; only oversized blocks split, including an
exact 16,523/10,310/254-source partition with 2.371 GB and 1.102 GB later
arenas.

That capacity-only adaptive run completed both warmup and measured invocations,
accepted all 385,862 samples, and passed the full eighteen-product frozen CASA
comparison. The measured complete invocation was 144.135067 seconds, or 8.854x
faster than CASA: scientifically green but slower than the 66.378748-second CPU
winner and still above the 127.6157-second gate. The worst full-array RMS ratio
was `3.7653703972780365e-7`, the worst peak-normalized absolute difference was
`6.0261272215318e-7`, every topology and source check passed, and the overall
structured-difference label was `good`. This proves that cropped adaptive
direct-cell replay is correct and memory-safe, but not that it should replace
the production CPU path on this workload. Its immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T002108Z-vlass-fragment-single-field-metal-fftw-f64-experiment-c8404d26.json`
(`d2b911610981bbb611cb4bc381daf00b78389044083032df0f8d10d3add36d87`).

The corresponding 63-field/four-SPW turnaround was rejected even earlier.
Its first source block contained 1,031,284 samples and a 1,616,737,792-byte
cropped arena, but exact active-tile scratch admitted only roughly 8,200 to
11,000 samples per dispatch. Forty-three completed dispatches consumed 0.67
to 0.88 seconds each in the steady sequence, with one 1.62-second outlier;
the 103-dispatch lower bound for GPU work alone was therefore already about
70--80 seconds. That exceeds the retained CPU path's 58.814-second complete
first block before adding arena planning, FFT, derived products, or writes.
The run was stopped at the 30-second progress marker, the temporary manifest
was restored, and the negative receipt was preserved at
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T003908Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-748d541b.json`
(`c0be9a46eb998739100a57a5c280510c9730180e774170968a64bd6606ce983e`).
Direct-cell fixed64 Metal is consequently neither the single-field nor the
all-fields winner on this M1 Max; further work should improve the exact CPU
path or remove the GPU's per-window fixed-scale/tile-finalization ownership
before another full-size launch.

A 64-pixel touched-tile follow-up confirmed that simply shrinking tiles does
not remove that ownership. It approximately doubled admitted windows to
15,000--24,000 samples and reduced fixed limbs from roughly 650 MB to
516--543 MB, but raised active-tile metadata from about 310 to 984--1,036
tiles. Steady dispatches consequently took 1.34--1.83 seconds instead of
roughly 0.7--0.8 seconds, implying an approximately 90-second first-block GPU
lower bound. The experiment-only control and temporary manifest change were
removed after the stop. Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T004439Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-5223e536.json`
(`d208c3f5ca4bc06cd191ba58395f7c9ffa28236716547863867f4e1b2dcef686`).

A compensated Float32 atomic experiment then captured each atomic addition's
TwoSum rounding error in the existing compensation grid. It reduced the
residual and image RMS ratios by about fiftyfold from the uncompensated
Float32 path: residual TT0/TT1 measured
`8.416465006765291e-5`/`8.325405172424891e-5`, image TT0/TT1 measured
`8.54432734743225e-5`/`8.353511140650134e-5`, and all numeric product ceilings
passed. The 144.919723-second measured invocation still missed 10x, and 113
alpha-domain pixels changed mask topology even though overlap-domain alpha
error was only about `1.74e-7`. The pure compensated path is therefore rejected
for residual, image, and alpha parity, but its PSF and weight accuracy supports
the next bounded hybrid: compensated Float32 PSF/weight accumulation with
deterministic touched-tile fixed64 accumulation retained for the two residual
planes. The immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T005241Z-vlass-fragment-single-field-metal-fftw-f64-experiment-69ff62b0.json`
(`902e0dbe75190c8337dbe3e4e32a2d6327513919eb2a8d608336dfbe5bfd26eb`).

A hybrid follow-up retained deterministic touched-tile fixed64 accumulation
for both residual Taylor planes while using compensated Float32 atomics for
the three PSF and three weight planes. This cut the maximum fixed scratch from
the eight-plane path to 392,167,424 bytes and reduced the measured complete
invocation to 124.648980 seconds, 10.237x faster than the frozen CASA row and
2.966720 seconds inside the independent 10x boundary. All 385,862 samples were
accepted; measured grid replay took 85.745860 seconds, including 48.788 seconds
of Metal grid work and 22.937 seconds of dispatch wait across 95 windows.

The full eighteen-product comparator nevertheless rejected the candidate.
Every enforceable numeric ceiling passed and the overlapping alpha domain had
a relative RMS difference of only `1.739239766968442e-7`, but the compensated
weight path changed the PB-derived mask at 109 pixels. Both `.alpha` and
`.alpha.error` therefore had topology mismatch status. The hybrid is the first
Metal path to cross the timing boundary, but it is not correctness-green and
cannot be incorporated as production behavior in this form. The next bounded
experiment should make the mask-owning weight TT0 accumulation deterministic
without restoring all eight fixed64 planes. The immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T012035Z-vlass-fragment-single-field-metal-fftw-f64-experiment-84fff6a2.json`
(`9d00ad781ec28b57d4796d4a09903692118a05edfbe6ff0c57719925da2d5ab0`).
The run used a temporary 960-GiB experimental storage floor because the
evidence volume was 47 GB below the normal one-TiB free-space precondition;
the checked-in one-TiB policy was restored immediately after the receipt was
published.

A second hybrid made weight TT0 deterministic along with both residual Taylor
planes, while retaining compensated Float32 accumulation for all PSF planes
and weight TT1/TT2. The focused Metal equivalence test still matched the
all-fixed64 planes exactly and all nine focused Metal tests passed. Its warmup
was 168.427768 seconds, but the measured invocation benefited from the warm CF
cache and completed in 124.475033 seconds: 10.252x faster than CASA and
3.140667 seconds inside the independent 10x boundary. All 385,862 samples were
accepted. The measured dirty replay took 85.768024 seconds; the Metal summary
reported 115 windows, a maximum 588,251,136-byte fixed grid, 49.475668 seconds
of grid work, and 22.988926 seconds of dispatch wait.

This candidate passed the complete frozen CASA contract. The eighteen-product
inventory matched; all numerical, topology, metadata, beam, source-region,
and structured-difference checks passed with no failed or incomplete checks;
and the overall structured-difference label was `good`. This confirms that
weight TT0 owned the remaining PB/alpha mask topology. The immutable receipt
is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T013841Z-vlass-fragment-single-field-metal-fftw-f64-experiment-3ec9be66.json`
(`370d3c80a44cd2439d2f04730526e75aeec185c4a52e5af3eaed6e4205bbb88b`).
The experiment used a temporary 850-GiB free-space floor after retained
negative artifacts lowered available space below 960 GiB; the checked-in
one-TiB production evidence policy was restored immediately afterward.
Although this is the first Metal candidate to pass both independent gates, the
66.378748-second exact CPU path remains the single-field performance winner.
Metal incorporation therefore still requires all-fields evidence and the
separate production-incorporation approval.

The matching 63-field/four-SPW turnaround rejects that incorporation. Its
first 25,416-row source block contained 1,031,284 samples and completed in
105.063 seconds, including 11.667 seconds of planning, 4.209 seconds of
materialization, and 86.772 seconds of Metal gridding across 32 exact-order
windows. The retained four-worker CPU path completed the same entire block in
58.814 seconds. Because Metal was already 78.64 percent slower before the
second of roughly seven blocks, the run was stopped rather than spend a full
warmup on a known loss. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T015348Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-78b798b4.json`
(`7dcd49776e7bbb120d842f5579b729c7e605cc68ce6b58fb25fb7796678737ff`);
the streamed log hash is
`7a464e723506075850c4e4b0fc01bf5cffddacdc7f04d8944ebaa57839445142`.
The temporary Metal manifest and 800-GiB evidence-storage floor were restored
immediately. The three-plane hybrid remains useful experimental evidence, but
it is not a production candidate for this VLASS wave: the exact CPU compact
replay is faster on both the single-field and raster-patch cases.

A first full-band CPU baseline then selected all sixteen used SPWs (`2~17`)
with the retained exact compact replay, four disjoint plane workers, 1-GiB CF
residency, 1-GiB compact-tap arena, and local f64 FFTW backend. The complete
warmup wrote all eighteen products in 1,632.017 seconds: the initial-dirty
replay took 1,589.374 seconds, `run_imaging` took 1,621.602 seconds, and
product writing took 7.586 seconds. All 25,030,848 visibility samples were
accepted with zero rejection. The CF cache reported 80,839 loads, 77,187
hits, 80,811 evictions, 28 resident cells, and 1,055,645,696 resident bytes.
Against the frozen 8,183.264-second CASA all-fields row, this is approximately
5.014x faster and misses the 818.326-second 10x boundary by 813.691 seconds.
The measured repetition was stopped after its first block reached 54.374
seconds because statistical repetition is not the current objective. The
retained interrupted receipt and streamed log are
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T020345Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-cc64c85a.json`
(`8f38247cdcba6717878816603666405a028918313d0004054f01d7b12a1a542f`)
and the adjacent `.log`. Peak process RSS was approximately 17.22 GB and
system-wide swap use was 4.56 GB without throttled pages; this was not
pathological swapping. The block growth and cache counters instead identify
serialized CF-cell loading and decode as the principal full-band limiter.

The first compact-cache experiment replaced the hot exact-key `BTreeMap`
lookup with the existing deterministic hash policy and wrote all 1,024
validated imaging/weight CF pairs to one fingerprint-bound source-order pack.
The 23,079,466,528-byte pack was produced in 47.154 seconds with metadata
fingerprint `f24a584cbe2dd782` and SHA-256
`002f2a8d3252f7357c3faa1a6592dee19b2a43561df3d566ca219beec123201f`.
On the four-SPW turnaround the complete warmup fell from 492.692 to 294.391
seconds and replay fell from 452.208 to 249.483 seconds, improvements of 40.25
and 44.83 percent respectively. The immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T024128Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-6cb16a5b.json`
(`940ed28483272e03c5a2c5c0771677cd4e775dbd0f0c92a4e597947b210b52ab`).

The same copy-backed pack reduced the full-band warmup from 1,632.017 to
1,163.022 seconds and replay from 1,589.374 to 1,092.236 seconds. This raised
the all-fields speedup from 5.014x to 7.036x, still 344.696 seconds outside
the 818.326-second boundary. The retained products have immutable receipt
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T024923Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-b173bdd9.json`
(`c163d4566dbcb44e811ef56aae3b556a54fbc9173cb17ca1b3cd00ff3e5d4922`).
A comparison-only invocation reused those products and the frozen CASA prefix.
All eighteen numerical and topology comparisons passed, including exact-zero
model planes and an overall structured-difference label of `good`. Strict
metadata parity remained incomplete: all products shared a
`3.6716461181640625e-5` Hz spectral-reference difference, and the four
beam-bearing image products plus `psf.tt0` were one Float32 ULP below CASA in
major axis, minor axis, and position angle. That receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T034034Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-399a92a4.json`
(`594c19ad95f168101a8ea18f0f1df5aeeaecd446d624355754193ee444c139d3`).
These are real metadata defects to close; they are not grounds to weaken the
full-product comparator.

A read-only memory map then removed per-cell heap copies while preserving the
same fingerprint, shape, length, finite-value, and exact-key validation, and a
per-source-block phase table removed repeated trigonometric evaluation. On the
four-SPW turnaround this reduced replay to 217.683 seconds, 12.7 percent below
the copy-backed pack; its interrupted diagnostic receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T033324Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-25ca332b.json`
(`4a59152ec05163d2185a1d531c113f720053421c3e4c96586845411770753245`).
The phase table itself costs only 4 to 11 milliseconds per block and is not a
remaining owner.

The next experiment used the cache's already validated complete Cartesian
axes to compute CASA's nearest frequency, quadratic W-plane, conjugate
frequency/Mueller, and PA-bin key directly. It retained the scanned selector
as an oracle and compared the two across frequency, signed W, conjugate-beam,
and Mueller cases. The four-SPW warmup completed in 242.242 seconds and replay
in 170.992 seconds. Relative to the copy-backed pack, wall time improved 17.7
percent and replay 31.5 percent; relative to the mapped/phase-table run, replay
improved 21.4 percent. All 6,416,526 samples were accepted with unchanged CF
cache counters. The immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T034843Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-4278cbb1.json`
(`aec91569a0f47754da5c62cfcaa1c1c5b38cc629808d0fe560e0932acfc6ebcb`).
This establishes direct selection as a genuine hot-path win; full-band
evidence and final incorporation review remain required.

The corresponding full-band direct-selection/mapped/phase-table warmup reached
`827.969462` seconds at the harness boundary (`819.427` seconds through the
frontend and `816.506` seconds in `run_imaging`). That is `9.884x` faster than
the frozen CASA row and only `9.643` seconds outside the `818.326`-second
boundary. Its initial-dirty replay took `884.315` seconds in the measured
invocation, which completed in `946.335376` seconds at the harness boundary;
the block-to-block variation was mapped-page locality rather than memory
pressure, with `memory_pressure -Q` reporting 89 percent free. All
`25,030,848` samples were accepted. After products and timing had been
preserved, the full comparator remained silent for more than three minutes and
was interrupted under the opaque-run stop rule. The immutable interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T035753Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-c552474b.json`
(`0fce7dcd3a395a9b018e2ea757a942fe223443ab3249aef2e593835cae491582`);
the adjacent log hash is
`b1a5a91eed1260c5d767b4b3967c40fa5a2908bd07883b1eb936f84dfa2029a1`.
The receipt is typed as interrupted because the harness had not reached final
publication, so these timings are taken from the preserved streamed log rather
than its empty result fields.

An x-contiguous packed-file counter-experiment tested whether matching the
projector's inner x loop would improve mapped-page locality. The
`23,079,466,528`-byte v2 pack had SHA-256
`320f954406dab154d9af1e682d166876e62eaed8649b9ddea4d9f396ade2491f`.
Its first four-SPW block improved to `24.152` seconds, but later blocks took
`37.557`, `76.646`, and `51.008` seconds as page locality deteriorated. The
complete replay was therefore `189.363` seconds and the frontend completed in
`257.238` seconds, both slower than the prior mapped-layout result of
`170.992` and `242.242` seconds. The measured repetition was stopped during
setup. The rejected experiment's interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T043941Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-d7c82e4f.json`
(`61a240b01dd9887e7355e2c49ab717277d3e92d2e665002ee28f4ceae9a57ea8`);
the adjacent log hash is
`448138b6e40941ff78f6f1b479bece6dd96f4d0dff7bb84210ccff1cab7f6ac1`.
The v2 layout was reverted; the fingerprint-bound v1 layout remains the
experimental candidate.

Doubling only the v1 tap-pack worker count from four to eight was also a clear
loss. The first identical four-SPW block increased from `24.393` to `31.404`
seconds; materialization increased from `11.758` to `17.019` seconds and grid
time from `10.758` to `12.216` seconds. The run was stopped at that bounded
decision point and the four-worker setting restored. The interrupted receipt
is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T044756Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-bbca868b.json`
(`83ca5d8b6465e4cd9512b15d9c0e1a3fd8ff9d101631ba0ea01640914dca87c4`);
the adjacent log hash is
`5aa8b346c2edab4ac22b3cbef3d0f15b9f9cbff79916bcbfe11a992b749ec000`.

The next bounded experiment kept the accepted v1 packed layout and four pack
workers but trusted its already fingerprint-bound payload instead of scanning
all `23,079,466,528` bytes for finite values on every open. The four-SPW
warmup completed in `201.378130` seconds at the harness boundary, `200.293`
seconds through the frontend, and `166.606` seconds in initial-dirty replay.
Compared with the otherwise equivalent finite-scan result (`242.242` seconds
frontend, `170.992` seconds replay), this isolates about 42 seconds of
redundant open-time validation while leaving replay arithmetic unchanged. All
`6,416,526` samples were accepted; POINTING diagnostics were quiet under their
new narrow trace control. The measured repetition was stopped after its first
block because the warmup had already answered the experiment. The immutable
interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T050039Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-1c2abfa1.json`
(`47d72a7f044f2514f538854323c5d4e3122fdc147d74a19615cdaec1295df57c`);
the adjacent log hash is
`35ad3f62852878ed87ac53ca9204ae1a09f94b15a61ee233c929774deee9210c`.
This saving is larger than the full-band warmup's remaining `9.643`-second
10x gap, so the next full-band run will verify the boundary before production
incorporation is proposed.

The full-band trusted-open counterexample showed why the four-SPW projection
was insufficient. With the eager scan removed, cold mapped pages moved into
the source-order replay: initial dirty took `812.783` seconds, `run_imaging`
took `849.950` seconds, the frontend took `864.281` seconds, and the harness
wall time was `865.952824` seconds. That is `9.450x` the frozen CASA baseline
and misses 10x by `47.627` seconds. All `25,030,848` samples were accepted,
system-wide memory remained 91 percent free, and no swapping or admission
failure occurred. The measured invocation was retained only through six dirty
blocks: it took `194.718` seconds versus `188.790` seconds at the same warmup
checkpoint, proving that the 20+ GiB grid working set evicts enough mapped
pages that a second completion would not establish a resident-cache win. The
run was then stopped under the non-repetition rule. Its immutable interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T050557Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-ea7c49f9.json`
(`5f84599b4d317fcfb822486cae6b7e28882bd4a9110f75cf9408ce82a2c09922`);
the adjacent log hash is
`29a255996b8281df08acfac888b6c43695da0e35fb0007d87cac93b87317b2bb`.
The next experiment therefore makes the existing one-cell lookahead perform
real page-aligned `WILLNEED` advice for the next mapped cell, rather than
merely constructing its zero-copy view.

The first advice experiment deliberately tested both whole-map and exact-cell
advice so that a large kernel read-ahead opportunity would not be missed. It
was a decisive loss. Cold whole-map `MADV_WILLNEED` blocked for `62.485`
seconds, and the four dirty blocks then took `25.535`, `41.594`, `58.690`, and
`51.515` seconds (`177.335` seconds total replay). The complete warmup took
`283.212503` seconds at the harness boundary and `282.222` seconds through the
frontend, substantially worse than the trusted-open control's `201.378130`
and `200.293` seconds. The measured invocation was stopped after its first
block. The interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T052927Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-78f4d1ac.json`
(`3a07d9c4977787686d204e87b9ae6f84850597766cacebe586c867bd87170199`);
the adjacent log hash is
`02e18759d4b0fef84d5bdab7f04959e4057e63ba64d1a214472d173e1bdc7841`.
Whole-map advice is rejected. The next bounded run isolates source-order
exact-cell advice on a cold v1 mapping.

Exact-cell advice was also rejected at its first bounded decision point. After
re-evicting the v1 mapping, the first dirty block took `28.973` seconds versus
`25.620` seconds in the cold trusted-open control. Cache-load-worker time rose
to `3.825` seconds; the kernel advice did not overlap enough useful I/O with
tap packing to offset its cost. The run was stopped after that block. Its
interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T053630Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-1f7cec09.json`
(`7346ce9932e0f6f12822739e1f8540b8b88d9aede7d9592a88737920af0cedc9`);
the adjacent log hash is
`9c0101066d7e6967a73151f86ad6e6f72a2126030e27d1e8bbd8ec6ae5a0ef46`.
Both advice controls and their dependency were removed from the candidate.

Repartitioning the same admitted 2 GiB AW working-memory sum from 1 GiB mapped
CF residency plus 1 GiB taps to 256 MiB plus 1.75 GiB was a small end-to-end
win. It reduced the four per-block adaptive-window counts from
`63,90,128,121` to `38,50,64,63`; initial-dirty replay fell from `166.606` to
`160.783` seconds. The complete warmup took `199.716744` seconds at the harness
boundary and `198.597` seconds through the frontend, versus `201.378130` and
`200.293` seconds for the trusted-open control. The measured invocation was
stopped during setup. The interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T054307Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-793c4ac5.json`
(`0b02f68d1d3e21758e376412bf40d305ea1f8fd97a1b56acc49f2a5f24b0d1a6`);
the adjacent log hash is
`8e4379111a8993134ecbb874bc876283b16150243eac28c6f373dca8bef05bcb`.
This candidate remains benchmark-only until its full-band effect and a
resource-adaptive production formula are established.

Eight requested grid workers did not expose more parallel work on this M4
host: the execution plan still admitted four effective disjoint plane owners.
The first otherwise identical block took `25.554` seconds with `11.290`
seconds in gridding, statistically indistinguishable from the four-worker
candidate's `26.036` and `11.332` seconds. The run was stopped after that
block and four requested workers restored. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T054850Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-e1fec56d.json`
(`5e4306a1a30c74c39d768576251eff248c6affd730584ebc69fdbf5e54d05b17`);
the adjacent log hash is
`b7b9547d95137e9ac2814b06af44e575eefae32cc9e697e859cbd347bd0c5f62`.

The full-band 256 MiB mapped-CF plus 1.75 GiB tap repartition cleared the dirty
10x gate while retaining the 32 GiB admitted peak. All `25,030,848` samples
were accepted. Initial-dirty replay fell from the trusted-open control's
`812.783` to `670.987` seconds; `run_imaging` completed in `701.639` seconds,
the frontend in `710.925` seconds, and the harness boundary in `712.571613`
seconds. Relative to the frozen `8,183.264`-second CASA baseline this is
`11.484x`, with `105.754` seconds of margin below the `818.326`-second gate.
The measured repetition was stopped during setup. The immutable interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T055104Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-df685944.json`
(`715ab4cf08f85f2e731e5138935f08a92acd23484a067f29d7f3e2fd73ca721f`);
the adjacent log hash is
`db88b4b333072bcbfcbb683fbe21863e621aa6727f5a0f145cb73f713e03e63e`.
This establishes the first full-size casa-rs all-fields dirty performance
pass; product parity and a resource-adaptive production expression remain
required before incorporation.

The next correctness pass removed the remaining spectral-coordinate defect.
The casacore-compatible frequency converter now preserves CASA's multi-SPW
`MSUtil` ordering and lifecycle, and the MeasurementSet engine loads scalar
metadata columns in bulk without changing row semantics. Focused ignored
casacore interoperability tests passed for both the single-field and
all-fields selections. The retained full-band run then completed in
`677.269094` seconds, or `12.083x` the frozen CASA baseline, with
`665.626` seconds in imaging and `7.724` seconds in product writing. All
eighteen full arrays, masks, finite topology, exact coordinates, numerical
ceilings, source-region checks, and structured-difference checks passed. The
only incomplete checks were the restoring-beam metadata on `.alpha`,
`.alpha.error`, `.image.tt0`, `.image.tt1`, and `.psf.tt0`.

That run also uses the shared CASA-order nonlinear beam fitter and CASA's
AWProject PSF scalar placement. Its fitted Float64 beam is
`2.955340630794701` by `2.084298237704540` arcsec at
`71.11363515754748` degrees, while the stored Float32 beam remains
`2.955340623855591` by `2.0842981338500977` arcsec at
`71.11363220214844` degrees. CASA stores `2.95534086227417` by
`2.0842983722686768` arcsec at `71.11363983154297` degrees. The immutable
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T080701Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-26e694e4.json`
(`2675f857f66bb468532d851b154ee7af71e26f84a4ce682c4d7f8ea2f71193fe`).
This is the current retained full-band dirty candidate: it clears the
performance gate and has complete scientific-array parity, but exact beam
keyword parity is still open.

A compact split-polarization PSF experiment tested whether CASA forms Stokes I
by Fourier transforming RR and LL separately before their Float32 average. It
added one full grid, independently admitted gridding and FFT lifetimes, and
reduced the tap arena to 512 MiB. The run accepted all `25,030,848` samples
and completed in `784.811048` seconds (`10.427x`), but it decisively failed
correctness. PSF TT1 and TT2 differed by approximately 100 percent RMS,
residual TT0 and TT1 by `1.487` and `2.007` percent RMS, and weight/PB
products by roughly `0.25` percent RMS. Its fitted beam moved farther from
CASA to `2.955338770096156` by `2.084299040603960` arcsec at
`71.11361789971096` degrees. This falsifies the split-hand/lifetime hypothesis
as a unit; the experimental runtime and planner code were removed, and the
combined-hand candidate above remains authoritative. The immutable rejected
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T084434Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-f6f3303d.json`
(`33a3cece5c98ebf104d01737c77119625a5c44521c9b798711d14eae130ae086`).

Three narrower probes then ruled out ordering and Briggs density as the
remaining exact-beam cause. CASA constructs its VI2 with default sort columns;
because this MeasurementSet has no `SORT_COLUMNS` keyword, those columns are
`ARRAY_ID,FIELD_ID,DATA_DESC_ID,TIME`. TaQL dumps of all 585,000 rows in the
relevant selected domain showed that physical row order is already exactly
that canonical order. The compact replay processes each source-order window
serially within each disjoint plane owner, so window boundaries cannot alter
the per-plane floating-point reduction order; the focused cross-window
bit-parity test remains green.

Finally, a weighting-only CASA probe mirrored `task_tclean.py`'s effective MFS
behavior, including its reset of public `perchanweightdensity=true` to false,
and exposed the pre-robust grid through uniform weighting because CASA's
`getweightdensity()` only writes that representation. CASA and casa-rs both
produced exactly `8,114,596` occupied cells at identical coordinates with zero
Float32-bit value mismatches. The comparison is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/experiments/weight-density-all-fields-20260726/comparison.json`
(`f177a9917a2faecdaf4d85274a6dc6f0c0a7ccb08c957801bc895f9635a78458`);
the same directory retains the exact sparse inputs and probe scripts. This
rules out MAIN-row sorting, compact-window segmentation, and Briggs density
construction. The open exactness investigation therefore remains inside
AWProject contribution arithmetic before the FFT and beam fit.

An exact-source-pixel experiment then tested one remaining AW phase
construction difference directly. CASA accumulates each POINTING group's image
pixel in Float32 before deriving the phase gradient, whereas the retained
casa-rs path converted the accumulated group direction back to a pixel. The
experiment propagated the exact CASA-order group pixel alongside the
authoritative direction and used it only for compact phase replay. The
`casa-imaging` library suite passed with 295 tests and 2 ignored; the
`casars-imager` library suite passed with 323 tests and 13 ignored.

The full-size run falsified this as the restoring-beam fix. The stored beam was
bit-for-bit unchanged at `2.955340623855591` by `2.0842981338500977` arcsec
and `71.11363220214844` degrees, versus CASA's
`2.95534086227417` by `2.0842983722686768` arcsec and
`71.11363983154297` degrees. All scientific arrays remained within the frozen
ceilings, including `.psf.tt0` maximum absolute error
`1.1920928955078125e-7` and RMS error `6.516492097443304e-10`; both model
planes remained exactly zero.

This invocation also exposed a distinct replay-locality failure and is not
evidence that carrying two pixel scalars caused the slowdown. Its measured
wall time was `941.899761` seconds, or `8.688x` the frozen CASA baseline, with
`895.071` seconds in dirty replay, `929.413` seconds in frontend imaging, and
`7.576` seconds in product writing. The 256 MiB CF residency held only seven
cells and recorded 69,629 loads, 15,601 hits, and 69,622 evictions; per-block
phase-table construction remained only about 4--11 ms. A cold warmup spent
`432.372` seconds scanning 3,335,002 POINTING rows and retaining 105,963,
while the measured invocation's warm preparation took `3.58` seconds. This
run and the retained 677.269-second candidate formed the same 891 adaptive
windows and routed the same 25,030,848 samples. Their aggregate grid times
were nearly unchanged at `245.572` versus `240.848` seconds, while tap
materialization increased from `363.152` to `603.789` seconds. This localizes
the regression to mapped-page/tap-pack residency rather than the new phase
arithmetic. It strengthens the case for tapless replay, a smaller exact CF
representation, and a persistent fingerprinted POINTING index.

The immutable receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T102552Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-fa0e5091.json`
(`685d2cc7abe9e8361cda9473726d2dc3a59947bb4211ea399e2ca320136966e5`).
The exact-pixel addition remains experimental pending the required
production-incorporation decision; regardless of that decision, it is rejected
as an explanation for the remaining beam-keyword mismatch.

The first scalar tapless CPU experiment removed POINTING group identity from
the compact tap key, retained each exact unphased CF/offset/conjugation bundle,
and applied the precomputed CASA axis phases inside the existing source-order
grid loop. A focused test proved bit identity against prephased compact taps,
and the complete `casa-imaging` library suite passed with 296 tests and 2
ignored. The reduced four-SPW run then provided a bounded negative performance
result. Through three identical source blocks, tapless replay reduced adaptive
windows from 152 to 73 and tap materialization from `61.017` to `38.033`
seconds. Exact scalar phase multiplication in every Taylor-plane tap increased
grid time from `45.314` to `105.032` seconds and planning from `2.504` to
`7.803` seconds; elapsed dirty replay at the same third-block checkpoint rose
from `111.643` to `153.231` seconds. The run was stopped before the fourth
block because the `37.3%` checkpoint loss already falsified scalar tapless
replay as a production win.

The interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T114423Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-1d853775.json`
(`1fb1b19dd9c13fb294d48fe17878ab99ebf798c50fc5d057f67301de23ee7303`);
the adjacent log hash is
`8ffbb8f44d7fcfe2f36cdf8186ceebace3645b73cbc0ddac59393d534aaa801c`.
The experiment remains useful as the required group-independent dataflow
foundation for explicit SIMD/NEON and fused Metal measurements, but scalar
tapless execution is rejected and cannot become a default.

A tap-order counter-experiment tested whether changing only the grid-update
traversal could recover the scalar tapless arithmetic cost. CASA and compact
tap packing use Y-outer/X-inner order; casacore's first array axis is
contiguous, while the current ndarray grid's second axis is contiguous. The
experiment therefore kept packed-tap indexing and every contribution value
bit-identical but traversed X-outer/Y-inner for contiguous grid writes. The
focused output test passed bit-for-bit. On the first identical four-SPW block,
however, grid time increased from `24.886` to `28.907` seconds and checkpoint
wall time increased from `34.079` to `36.045` seconds. The loss is consistent
with replacing contiguous packed-tap reads with strided reads without enough
grid-store benefit. The run was stopped after that block and this traversal is
rejected.

The interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T115307Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-89eee1a4.json`
(`f000fd838d75e15c9e4dfdcb1a51037bf0f2c5c3a59775741a5a8cc4b4358bd6`);
the adjacent log hash is
`7e8be21a59ac07114c97aaa3588a1bcfaef7032355e4074c356a73d0884a05d5`.

The Oracle follow-up adopted spatial ownership as the next bounded hypothesis:
source rows and adaptive windows remain the exact ingestion order, but every
accepted footprint is indexed into spatial tiles, each tile owns disjoint
authoritative-grid views for all Taylor planes, and each pixel still receives
contributions in exact source order. The review rejected tile scratch/copy-back
because it would either change accumulation grouping or copy large grid regions
per window. It also rejected a new CF format, SIMD, Metal, FFT-layout changes,
planner integration, and public parameters in this first slice. The initial
candidate set is 128, 192, and 256 pixels, with 192 primary, measured rather
than encoded as a machine constant. Advancement requires bit-identical pre-FFT
grids, no worker-count dependence, at most 2.0 sample--tile fragments per
accepted sample, at most 64 new metadata bytes per sample and 256 MiB peak, and
a three-block wall time no greater than `94.897` seconds for a clean win
(`100.479` seconds is the absolute refinement ceiling).

The first executable 256-pixel prototype intentionally proved that disjoint
views of the existing ndarray grids can be expressed safely by partitioning
every plane into non-overlapping X stripes and processing the contained 2D
tiles with four workers. Focused tests were bit-identical, including a
cancellation-sensitive source-order fixture. Its first runtime formulation was
nevertheless invalid for advancement because it emitted a separate fragment
for each of six AW roles and also used X-outer/Y-inner traversal inside each
fragment. It reached `61.316` seconds after one block and `134.698` seconds
after two, already beyond the three-block stop threshold; p95 fragments per
sample were 10 and then 12. The run was killed immediately. Its interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T120953Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-040a871f.json`
(`20c6f1dacb19675f6c8649f23fe324395ebee97d8d10ede6981ceb03c94c3fce`);
the adjacent log hash is
`b85abce5a8fff0af9350344ef646b8827a51af8dc51672de66d2dcc89d43fa78`.
The corrected slice coalesces all six roles into one sample--tile record,
constructs a deterministic two-pass window-local directory with a 256 MiB hard
cap, restores CASA Y-outer/X-inner tap traversal, and starts with 192-pixel
tiles. Production incorporation remains unapproved.

The corrected 192-pixel run demonstrated that spatial ownership is useful but
not sufficient by itself. At the first block it improved wall time from the
scalar tapless `34.079` seconds to `30.548` seconds, with `0.126` seconds of
tile planning and `22.360` seconds for tile planning plus replay versus
`24.886` seconds for scalar tapless gridding. At the identical three-block
checkpoint it reached `126.843` seconds: substantially better than scalar
tapless's `153.231` seconds, and about `82.463` seconds of total tile
planning/replay versus `105.032` seconds of scalar tapless gridding, but still
worse than the retained `111.643`-second materialized-tap control and above the
`100.479`-second absolute ceiling. Peak fragment storage was only about
2.0 MiB and planning stayed below 0.15 seconds per block. The run was stopped
at the checkpoint. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T122100Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-ced674a9.json`
(`0b0c074f46cae477356302a0e867e971b0b342edc12cac2acf1339bd3a9fbac4`);
the adjacent log hash is
`e0c9b24d35e7530f763aee900d1598ded1d4050aef7e22852fdb5c2c681d0cab`.
This rejects tile ownership alone, not the coupled Oracle A+B architecture:
the next bounded factor is a byte-bounded, group-independent compact-slice LRU
that reuses exact unphased bundles across adaptive windows without copying
their pixels or changing any tap arithmetic.

That first persistent compact-slice formulation was also falsified quickly.
It used a `1,792` MiB byte-bounded hash-map LRU and shared exact unphased
bundles through `Arc`. The first block recorded `1,013,126` hits, `291,982`
misses, `185,843` evictions, `106,139` resident entries, and
`1,879,034,904` resident bytes, so reuse across adaptive windows is real.
However, per-key hash-map lookup, recency-queue maintenance, allocation, and
eviction increased tap packing from the corrected tile-only run's `3.679`
seconds to `32.112` seconds. Materialization reached `32.241` seconds and the
first-block wall time reached `58.962` seconds, versus `30.548` seconds for
tile-only. Tile planning plus replay remained effectively unchanged at
`22.541` seconds. The run was killed after the first block, and the general
LRU representation is rejected rather than tuned. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T123207Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-85103d23.json`
(`371b5f053dd7f510455ad761e7a7a6fb9f7013bdf07a5432bc2374d4b05e3adc`);
the adjacent log hash is
`044b699626596c419011cbb7f2a887ae3cc12b0c57b7776d85ec9f22db3cfc4d`.

The next bounded representation keeps the same exact bundle pixels and
source-order tile replay but replaces the hash-map LRU with a fixed-slot,
direct-mapped arena. A key probes one slot, a collision replaces that slot,
and an independent byte ceiling evicts only if variable support sizes would
otherwise exceed the admitted tap budget. The initial `16,384`-slot screen has
a worst-case retained-payload footprint below the `1,792` MiB experiment
budget for the measured support range. This isolates whether reuse is valuable
when the cache lookup and replacement policy is deliberately trivial; it is
still private experiment code and does not authorize production incorporation.

The `16,384`-slot screen removed the LRU implementation cost but did not retain
enough of the working set. At block one it recorded `130,688` hits,
`1,174,420` misses, `1,158,268` replacements, `16,152` resident entries, and
`224,684,864` resident bytes. Materialization returned to `3.594` seconds and
tile planning plus replay was `24.480` seconds, but the block reached `32.574`
seconds versus `30.548` seconds for tile-only. At block two cumulative time was
`72.195` seconds versus tile-only's `68.437`; the run was then stopped. This
rejects the undersized direct arena, while distinguishing it from the general
LRU failure: cheap lookup is viable, but a collision/replacement rate near the
miss count cannot deliver useful reuse. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T124123Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-66c1888f.json`
(`3838c62fc276e85068a2968bfe9314df729e0329d5f3d3f365324569ae29c29e`);
the adjacent log hash is
`d165dcc162f547b7571161838666a347113099ec1a07abe07f09fb49cd3c0f74`.
A same-conversation Oracle evidence-delta review now owns the choice between
one capacity-matched cheap arena, dense stable tap identity, and proceeding
directly to SIMD tile replay.

The completed evidence-delta review selected exactly one final cache
experiment: a capacity-faithful dense identity directory feeding a
`131,072`-slot, byte-bounded CLOCK arena. It rejected a separate large direct
or four-way hash screen because a negative result at roughly 81% occupancy
would still confound reuse with set conflicts. The dense ID is derived without
a hot-path hash from CF-cell ordinal, imaging/weight kind, oversampling X/Y
offsets, and conjugation; the directory maps that ID directly to an arena
slot. CLOCK reference bits approximate recency without an LRU queue, and
bundles still held by the current adaptive window cannot be evicted. The
`1,792` MiB byte ceiling remains authoritative.

This is the last persistent-tap-cache experiment. It must be bit exact, finish
the first block in at most `27.5` seconds, reach at least a 70% hit rate, spend
at most `1.5` seconds in cache management, keep CLOCK p95 scan length at most
64, bypass at most 2% of admissions, and cause no swap. Any failed gate ends
persistent caching and advances directly to SIMD/NEON tile replay. If it
survives, three-block totals above `111.643` seconds reject it, `106`--`111`
seconds are not worth further cache work, at most about `106` seconds is
marginal, and at most `100.479` seconds is the strong advancement threshold.

The dense-ID CLOCK result cleanly ends persistent tap caching. At block one it
held `106,646` entries (`119,175` peak), reached the exact `1,792` MiB payload
ceiling, and used only `7,451,136` bytes for the identity directory and seen-ID
bits. It delivered `1,013,131` hits and `291,977` misses (`77.6%` hits),
`185,331` evictions, zero admission bypasses, CLOCK p95 scan length 24, and
only `93.085` ms of measured cache management. Materialization fell to
`1.512` seconds. The first block nevertheless took `28.799` seconds, above the
predeclared `27.5`-second gate; tile planning plus replay was still
`22.910` seconds. The run was stopped immediately. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T125800Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-f2699d44.json`
(`d6b4aa57e850d7407424491e714723a4e3e3dbe8ee3023fe45af3572f8ddfce8`);
the adjacent log hash is
`a63d6d5d4897bd3e44eb060c93e546239a811a5c0cd5112f5b095c1e4017c242`.
Because hit rate and cache-management gates passed, the remaining loss is not
an ambiguous hash/LRU-container failure. Per the signed-off stop rule, no more
persistent tap-cache variants will be tested; rejected cache machinery is
removed from the executable path and the next bounded experiment is
SIMD/NEON tile replay.

The first SIMD screen vectorized one exact complex-f32 contribution and its
complex-f64 grid accumulation with a two-lane AArch64 NEON helper. It avoided
FMA and reassociation, and its focused cases matched the scalar result
bit-for-bit. That granularity was nevertheless too narrow: on the first
identical four-SPW block, tile planning plus replay increased from the scalar
tile result's `22.360` seconds to `24.853` seconds. The block reached `43.990`
seconds, with `14.718` seconds of materialization because the rejected cache
experiment's shared-`Arc` bundle representation was still present even though
caching was disabled. The run was stopped immediately. Its interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T130634Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-f9435c6b.json`
(`26187368dd7fb95abf11c7aa8e613180699d8620780de66ed83e50409f024a411`);
the adjacent log hash is
`688a1979a3e311c5f2a6633f71d80b4be3bbebbbcf2789834ecf68a3dc1a7c3f`.
This rejects only the per-contribution two-lane formulation. Both that helper
and the cache representation are removed before establishing a fresh scalar
tile control; any subsequent SIMD experiment must amortize vector setup across
multiple taps, pixels, or Taylor planes.

After removing both rejected implementations, the fresh scalar 192-pixel tile
control restored the intended representation and slightly improved the earlier
tile baseline. Its first block reached `29.378` seconds, with `3.573` seconds
of materialization and `21.431` seconds for tile planning plus replay. At the
second checkpoint it reached `65.481` seconds, with cumulative materialization
`6.618` seconds and tile planning plus replay `25.758` seconds for that block.
This confirms that the prior `14.718`-second materialization regression came
from the cache experiment's shared-`Arc` representation, while the narrow NEON
replay itself remained slower than scalar. The run was stopped after that
control checkpoint. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T131951Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-1bbf0e55.json`
(`952ac20c3a04464e12f17bab10436acae82bdb3c23f699cc0bfd150b8ea39762`);
the adjacent log hash is
`a91e96af540c1f0a1efb6cb9ab531a5e822294e9201982e7b9bfe95d02a9cd75`.

The same Oracle conversation selected exactly one wider SIMD experiment from
that clean delta: a 2-by-2 register-transposed AArch64 NEON spatial kernel.
For two adjacent X taps in each of two tap rows, it keeps both packed-tap reads
contiguous, transposes the four values in registers, and updates the two
contiguous Y grid pixels for each X. There is no tile copy, cross-plane packing,
role fusion, FMA, reassociation, or persistent layout change. Source-window,
fragment, role, and per-pixel contribution order remain unchanged; only the
order in which distinct pixels receive one source contribution changes.

The implementation tightens one detail in the review pseudocode to match the
actual scalar contract: the two stored `f32` axis phasors are promoted and
multiplied as complex `f64`, the gradient is rounded back to `f32`, and both
tap-times-gradient and coefficient-times-tap then use `f32` in the same
multiply/add-sub order. Only the final complex contribution is promoted to
`f64` before adding it to the authoritative grid. The hard first-block gates are
tile-plan-plus-replay at most `17.48` seconds and total at most `25.43`
seconds, with at least 80% SIMD pixel coverage and no bit mismatch. A surviving
three-block run must not exceed the retained `111.643`-second control;
`106.061` seconds is the pass threshold and `100.479` seconds is a strong pass.

The 2-by-2 kernel passed its replay hypothesis but exposed a new memory
interaction. Focused tapless, tiled, and cancellation-sensitive source-order
tests were bit-identical. Its first block reached `22.838` seconds total with
`14.832` seconds of tile planning plus replay, beating both strong thresholds,
and vectorized `95.1%` of role-pixel updates. Across the first three blocks,
tile planning plus replay totaled `52.641` seconds, also inside the
`61.681`-second component pass budget, while SIMD coverage rose above 95%.

The three-block end-to-end result nevertheless reached `136.051` seconds
because tap materialization grew from `3.578` seconds in block one to `14.433`
and then `53.546` seconds. The fourth block showed the same failure mode before
the interrupt. The 23 GiB mapped packed-CF archive, roughly 1.8 GiB live tap
window, and 18.9 GiB authoritative grids now form the dominant 32 GiB residency
conflict; the SIMD loop itself is retained only as an experimental replay win
while memory/page residency is measured next. The first worker-spread counters
also compared extrema from different adaptive windows, so they are diagnostic
only and will be replaced by a per-window imbalance ratio. The interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T133805Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-97518f59.json`
(`bc0cbd6e637db531a793b26c7dabed661bf8cb62a18fc22abcabd867ce0ef6a4`);
the adjacent log hash is
`13056e8987c5aa44f084842d61c975adb74c40164c1b69d0be28a7bb5e645f95`.

The Oracle memory evidence-delta review ranked sparse authoritative tiles
first, genuinely owned exact CF residency second, and a persisted compact
tap-slice artifact third. The selected experiment allocates all eight
`Complex64` planes only for the cumulative union of touched 192-pixel tiles,
serially before parallel replay. Each tile retains Y-contiguous storage and
the existing exact 2-by-2 kernel; there are no atomics or new reductions. At
finish, one plane is zero-filled densely, populated by bitwise tile-row copies,
passed through the unchanged f64 FFT/product path, and released before the next
plane is densified.

One full plane is `2,361,960,000` bytes, while one 192-by-192 tile across eight
planes is `4,718,592` bytes. Accumulation therefore replaces the fixed
`18,895,680,000`-byte grid residency with
`4,718,592 * cumulative_active_tiles`, plus the existing tap and actually
resident CF pages. The first checkpoint is the same first three four-SPW
blocks. It requires no bit mismatch or swap, no more than 8 GiB sparse
residency (about 1,800 tiles), block-three tap-materialization cost per byte no
more than 1.25 times block one, tile replay no more than 55.273 seconds, and
total time no more than `100.479` seconds. This remains an experiment, not
approval to incorporate a new storage/runtime default.

The experiment now implements that storage contract behind
`CASA_RS_AWPROJECT_SPARSE_TILES_EXPERIMENT`. Active tiles are allocated
serially, replayed by disjoint X-stripe owners through the same scalar or exact
2-by-2 NEON arithmetic, and consumed one plane at a time into the existing f64
finish path. A focused dense-versus-sparse source-order test checks every grid
element after densification and is bit-identical with both scalar replay and,
where available, the NEON kernel. The experimental implementation refuses the
legacy per-sample grid methods, caps sparse residency at 8 GiB, and records
new, resident, and peak tile counts and bytes plus allocation and per-window
worker-imbalance timings.

The first cold checkpoint after remounting GLENDENNING reached `131.448`
seconds for three blocks, with `611` tiles and `2,883,059,712` bytes resident.
It retained the expected cold CF materialization cost and is useful cold-cache
evidence, but it missed the timing gate. Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T140213Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-80dc14ef.json`
(`0d55024c6d40b4b16deb49e681e0baadb0fb4c48131b564d1d18ada09a438c8e`);
the adjacent log hash is
`d3623ac814db49c61a30a80e2071abd120329e713d4f7a57f07685e23a909ec3`.

The immediate identical warm checkpoint reached `22.364`, `56.537`, and
`100.250` cumulative seconds after blocks one through three. It therefore
passed the Oracle strong `100.479`-second total gate by `0.229` seconds. Sparse
residency again ended at `2.883` GB, while tile-plan-plus-replay summed
`57.983` seconds, slightly above the `55.273`-second component target. Its
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T140611Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-81975a58.json`
(`c7a140422cb61a747b74619a565b0cff1b55cc0d22d3350b70a2193016abf8ff`);
the adjacent log hash is
`29207ddef4322de5083407c90c7b102fc273aa758026b9578d19bfdc8eeab4bb`.

A complete four-SPW sparse run then finished in `180.037461` seconds, with
`804` tiles and `3,793,747,968` bytes at peak. Its receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T140944Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-b2e31114.json`
(`d6a5e2d217b5263eff9520ceedf9594430a45b8ff3da1dfdb652c4f9b617fa49`);
the adjacent log hash is
`a550d1a5028424ac61b24e8b365efcaf5caa982c0b8e7ebe43d468d5f9d36a82`.
The harness had no matched four-SPW CASA prefix and incorrectly compared this
result with the full-band CASA reference; that comparison is rejected as
invalid scientific evidence. A direct diagnostic against the prior completed
four-SPW dense casa-rs products found exact sum weights, weight planes, and
masks. PSF and residual values differed only at `1.46e-11` through `1.40e-9`
absolute scale because the retained dense result predates the exact POINTING
change. The focused current-code dense-versus-sparse test remains the
bit-exact storage-isolation evidence.

The corrected full 16-SPW sparse run finished in `745.145443` seconds
end-to-end, below the frozen `818.326`-second 10x boundary by `73.181` seconds.
The CLI core took `743.479` seconds, including `9.240` seconds to write all 18
products. Full replay took `681.778` seconds; exact tap materialization summed
`284.935` seconds and tile planning plus replay summed `345.089` seconds. Peak
sparse residency was `871` tiles or `4,109,893,632` bytes. This is slower than
the retained `677.269094`-second dense full-band run on the 32 GiB acceptance
host, but removes `14.786` GB of authoritative-grid residency and still passes
the runtime target. That tradeoff makes dense versus sparse selection a
resource-adaptive planner decision rather than a universal default.

The exhaustive full-array comparison scanned every pixel of all 18 frozen
CASA products. It has zero failed tolerance checks. Thirteen products have
exact metadata parity; five are incomplete only because the current strict
metadata comparator requires bit-equal restoring-beam values. Their
major/minor beam values differ by `2.384e-7` arcsec and their position angle by
`7.629e-6` degrees. The scientific tolerances, topology checks, masks, shapes,
units, coordinates, product inventory, and all other metadata fields pass.
The remaining defect is therefore the already-owned exact restoring-beam
metadata parity, not sparse-grid arithmetic. The receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T142836Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-bae45401.json`
(`22344295498bba1eb8505ceb126735803b2538d04ca8508ba87b184c2098a1b3`);
the adjacent log hash is
`ff683674919c98343f405a0bd9270a3926b53ae91fff625476d4c0a27115b506`.
The comparison JSON hash is
`82f8c32db31f06362d1c9cf8ca7e2a803fae319edf4532e93fbdb6eb523e23ee`
and its log hash is
`e9ff84d3f3d477c4599d923e9d5e2a729c69a9ed6950e223aa54616fe1a5a518`.

The next same-conversation Oracle evidence-delta review selected dynamic
spatial-tile tasks rather than source-row partitioning. Each active
192-by-192 tile is one indivisible task for one adaptive source window. The
deterministic work estimate is
`8 * tap_pixel_reads + 32 * plane_pixel_updates`; tasks are sorted heaviest
first with tile ID as the tie breaker, then claimed through one atomic cursor
by the existing four scoped workers. A worker locks one authoritative sparse
tile, replays every fragment for that tile in exact source order and the
existing six-role order, and releases it before claiming another task. The
source-window barrier, tap arithmetic, and exact scalar/NEON paths are
unchanged. Scheduler build, sort, and claim counters plus a hard lock-failure
path make the experiment auditable.

The focused test now compares the fixed sparse path with dynamic one- through
ten-worker runs in both normal and reverse task order, covering every hardware
thread on the 10-core acceptance host. Densified planes are bit-identical in
every case, including the cancellation-sensitive fixture.
The first bounded four-SPW checkpoint reached `90.752` seconds after three
blocks, with `45.097` seconds of tile planning plus replay and `35.482`
seconds of materialization. Replay passed the Oracle advancement threshold,
but materialization exceeded its five-percent variance guard. An identical
repeat resolved that as page-state variance: the same checkpoint reached
`80.464` seconds, with `43.067` seconds of tile planning plus replay and
`26.789` seconds of materialization. It therefore passed all three
advancement gates. The repeat had already begun the fourth block before the
operator interrupt took effect, so it was allowed to finish in `172.399673`
seconds and preserve complete evidence.

The first interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T151219Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-ac58a2b5.json`
(`4a54cd4933455f472a45245222a557d236d161c65b1d433d1031efb7b2fff746`);
its adjacent log hash is
`64dac9f0766c2223763251b19823c94a024f43424f598a706a90b44b6a7b2891`.
The complete repeat receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T151548Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-72af7d8f.json`
(`b7ced5288fe98f1dae46ceaa9dd7f82aa024d01de38a360d265e9d1f6a931245`);
its adjacent log hash is
`40860c38b251f8e561768133b8b92647403ebcb93a2bfe9d61b1f19cf8256397`.
As with the earlier reduced run, the automatic four-SPW comparison has no
matched CASA prefix and is not scientific parity evidence.

The dynamic full-band run then finished in `635.628674` seconds end-to-end.
That is `109.517` seconds (`14.7%`) faster than the prior sparse run,
`41.640` seconds faster than the retained dense candidate, `12.87x` faster
than the frozen `8183.264`-second CASA baseline, and `182.697` seconds inside
the `818.326`-second target. Full source replay took `591.224` seconds.
Aggregate tile planning plus replay was `288.784` seconds, passing Oracle's
`293.326`-second meaningful-improvement threshold; exact tap materialization
was `238.025` seconds. Across all sixteen blocks, scheduler construction,
sorting, and atomic claims together cost only `12.937` ms. Peak sparse
residency remained `871` tiles or `4,109,893,632` bytes. The scheduler
reported minimum critical-path efficiency `1.000`. One adaptive window still
reported `4.523x` between the shortest and longest dynamic worker elapsed
times because one indivisible heavy tile dominated that window; this is not a
counterfactual fixed-stripe measurement, and the two counters should retain
those distinct meanings in production diagnostics.

The exhaustive frozen-CASA comparison again scanned every pixel of all 18
products and reported zero failed tolerance checks. All scientific arrays,
topology, masks, shapes, units, coordinates, inventory, and non-beam metadata
pass. The same five products remain incomplete solely because the exact
restoring-beam metadata values differ at the last stored digits; the values
and affected products are unchanged from the prior sparse run. The immutable
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T152033Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-66ab1c03.json`
(`1ff2820468668a783d4d84fc52d4c1b3d2b716f9ca70e6268eaf72a0b5bb84ce`);
the adjacent log hash is
`1fed3705a62b26ef623d380432f4443c67f74e1fb7c30658aee6a0fa8cd7a0a5`.
The comparison JSON hash is
`0b85496584dfc87072671b30c79530bf3b7de542e7123fc5c3743f32da444c18`
and its log hash is
`00bab38d418cabb6fb766453159b62d9f7de3040de517903e1827f690556f0c1`.
These results advance dynamic spatial-tile scheduling as the measured
production candidate, but final incorporation still requires the user's
explicit approval.

The final same-conversation Oracle synthesis accepts the scheduler candidate
without changing dense-grid replay, tile size, worker-count policy, CF/tap
representation, FFT finishing, or public controls. Its minimal incorporation
rule is: retain the planner's current dense-versus-sparse storage decision;
when sparse authoritative tiles are selected, traverse weighted tiles
serially for one worker and use dynamic weighted tile claims for more than one
worker. Fixed X stripes remain only as a test oracle and benchmark control.

Before removing the experiment gate, the candidate must add an exact
active-tile/task bijection check, first-error cancellation and worker joining,
bit-exact fixtures for admitted worker counts one through four in normal and
reversed task order, and failure-injection coverage for duplicate/missing
tasks, invalid fragment spans, replay errors, and occupied/poisoned tile
locks. Production telemetry must distinguish measured dynamic-worker elapsed
imbalance from the non-preemptive lower-bound efficiency. The observed
`4.523x` value is the former: maximum worker elapsed divided by minimum worker
elapsed for one dynamic window. Efficiency `1.000` means a single heavy tile
established the indivisible-task lower bound, not that all workers had equal
elapsed time. Add worker utilization, busy-time dispersion, and heavy-task
dominance counters and report the lower-bound efficiency with greater
precision.

That scheduler hardening is now implemented behind the experiment gate. Task
construction validates the active-tile bijection, allocated sparse storage,
fragment-directory bounds and monotonicity, exact traffic weights, unique
tiles, and the total scheduled fragment count before any worker starts.
Workers stop claiming new tasks after the first error, every scoped worker is
joined, and partial replay is rejected. Focused tests cover duplicate,
missing, out-of-range, wrong-weight, and invalid-directory tasks; occupied and
poisoned tile locks; an injected replay panic; zero work; fewer tasks than
requested workers; and bit-exact normal/reversed execution with one through
ten workers. Telemetry now reports dynamic-worker elapsed imbalance
separately from non-preemptive lower-bound efficiency in parts per million,
worker utilization, ideal lower bound and tail, task elapsed sum/maximum,
worker busy minimum/maximum, and empty claims. Debug assertions reject timing
ratios materially above their mathematical bounds.

The prior eight-worker losses belong to disjoint Taylor-plane ownership and
tap packing, not to this dynamic spatial-tile scheduler. Before the two warm
full-band confirmations, run a bounded 4/6/8/10-worker spatial-tile sweep on
the four-SPW development row. Select by total replay wall time while retaining
the per-window task distribution, lower-bound efficiency, utilization,
worker-busy dispersion, and memory-pressure evidence. Feed the selected count
and the negative candidates into the resource-adaptive planner work; do not
encode four as a universal constant.

The bounded sweep confirms that distinction. An initial six-worker run reached
`196.182` seconds after three blocks versus `160.803` seconds for its
four-worker counterbalance, but the difference was dominated by mapped
23-GB packed-CF page state: materialization was `140.158` versus `88.457`
seconds while tile planning plus replay was `43.161` versus `56.955` seconds.
The sequence therefore continued only after warming the mapped pages and
counterbalanced six and four again.

On the comparable warm first block, six workers are the winner on this 10-core
M4 MacBook Air: `22.766` seconds total and `14.917` seconds of tile planning
plus replay, versus four workers at `25.427` and `16.731` seconds, eight at
`27.189` and `17.406` seconds, and ten at `28.575` and `18.469` seconds.
The warm two-block comparison confirms six over four: `56.795` versus
`60.705` seconds total and `34.315` versus `36.862` seconds of tile planning
plus replay. Thus more than four workers does help the new spatial scheduler,
but using all available logical cores does not. The full-band experiment
manifest advances to six workers; four remains the retained fallback evidence,
and eight and ten are rejected on this host.

The warm six-worker receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T162828Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-f33a2900.json`
(`06dde24c1b55a0e8c64076070d3c65dbb9b8e406351251c18d5e8424cacbf5f9`);
its log hash is
`21efdb0764ec0718ee5d18be938b08f2bcae40d6b7f747996b12cfc408dd2435`.
The warm four-worker counterbalance is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T162954Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-d1f6d7a8.json`
(`bf663944a07cc952e497025915793d32a435f32e4e9f645f2d97556c81b192ae`);
its log hash is
`a57bc669c130e20d5ffdc0edb904dd282ecdcbf275bbaecb731ef2a259b474ed`.
The warm eight-worker receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T162145Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-fa115420.json`
(`80f2dcedea5597a12f9a5236fdd5c1d84bd7346ae441b16e2ef0e2c01b80eb57`);
its log hash is
`dc80a40f9f0944eb86f7d2db72c13285989e2dce930ed97bacb9b0353819241d`.
The warm ten-worker receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260727T162552Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-12a20a9f.json`
(`4455cb33f8da3e178fe9cc7a9ba3381f77e987a85b5f54c06de7c0dcac490457`);
its log hash is
`7776a76153d19c995317421cdd8deeb31838d4d9ae83e8ed420d0519f1214e4c`.
All are intentionally interrupted experiment receipts; their streamed logs,
not empty result fields, own the bounded checkpoint evidence.

This result belongs in the resource planner. The current AWProject automatic
path uses logical-core count alone and would select ten on this host, which the
sweep disproves. The replacement must bound candidates by assigned CPU
capacity and task count, model the weighted non-preemptive tile schedule,
distinguish performance and efficiency cores where the platform reports them,
and include mapped-CF/materialization bandwidth contention rather than
optimizing tile time alone. Keep an explicit worker override for expert
experiments and expose automatic versus numeric selection through the UI; do
not expose the internal dynamic scheduler as a separate user knob.

The aggregate telemetry labels are also corrected: ideal lower bound and tail
are sums across adaptive windows, while worker-busy minimum and maximum are
global extrema across window workers. Per-window minimum efficiency and
utilization remain separate extrema and are not ratios of those aggregate
fields.

Oracle also recommends two more identical warm full-band casa-rs runs after
that worker selection and before removing the gate, using the median of three
dynamic observations, and the complete four-row matrix before wave closeout.
No further CASA run is needed.

The subsequent same-conversation Oracle evidence-delta review used the
measured `4/6/8/10` results and the host's four-performance plus six-efficiency
CPU topology to refine that advice. It rejects `available_parallelism()` as a
worker selector for this stage: ten is the logical-core count and is materially
slower than six. The adopted planner design combines three independent facts:
hard admission from the assigned CPU/task/memory slice, a deterministic
longest-processing-time model over the existing weighted tile tasks, and a
bounded calibration of the exact sparse-tile replay kernel. Topology generates
coarse candidates rather than a machine-specific answer; on this host the
general formula produces `4,6,8,10`. The coarse winner's immediate untested
neighbors are then considered, and measured ties select the smaller count.

The calibration is a bounded startup decision, not continuous retuning.
Topology supplies at most four coarse anchors, and the planner expands their
adjacent integers within the admitted CPU range. Each candidate executes four
complete source-order windows in forward/reverse counterbalanced order. These
are authoritative production updates: no visibility, tile, or output write is
replayed solely for calibration. Window time is normalized by the schedule's
deterministic tap-read plus plane-update traffic. Empirical overlap excludes
unsupported counts; when intervals overlap, the tie-break prior is the
highest-capacity CPU class plus half of the remaining assigned CPU capacity.
Empirical overlap uses a two-sided 95% Student-t interval, so four-window
samples retain the uncertainty their small sample size warrants. The
resulting count is fixed for all later windows. Arithmetic, tile ownership,
lock, or replay-invariant failures still fail the run.

The accepted UI remains one canonical field:
`standard_mfs_grid_threads = auto | positive integer`. There is no scheduler
selector, P/E-core control, or machine-specific worker option. Explicit
positive values bypass calibration and remain the correct control for current
evidence runs.

The bounded laptop neighborhood experiment is complete. It used the same first
two `InitialDirty` source blocks, exact-f64 replay, 192-pixel tiles, unchanged
QoS and memory plan, and the counterbalanced order `5,6,7,7,6,5`. The measured
two-block wall times were:

- five workers: `50.838` and `44.569` seconds, mean `47.704`;
- six workers: `45.512` and `44.943` seconds, mean `45.228`; and
- seven workers: `44.025` and `43.559` seconds, mean `43.792`.

Seven workers won both replicates. Its mean is `3.18%` faster than six and
`8.20%` faster than five. This supersedes six as the explicit setting for the
next full-band experiments, but it does not encode seven as a production
default.

The first experiment-gated automatic calibrator did not reproduce that result.
It replayed a deterministic 32-task weight-stratified subset and resolved four
workers:

`standard_mfs_parallel_worker_plan requested=auto resolved=4
source=auto-calibrated hard_cap=10 candidates=3,4,5,6,8,10
calibration_tasks=32 calibration_elapsed_ms=5861.900`

The resulting two-block wall time was `52.443` seconds including calibration,
materially slower than explicit seven. Replaying even the complete first
sparse window still resolved four, proving that sample size within the first
window was not the problem. Its interrupted evidence receipt is:

`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260728T023605Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-4430758b.json`

The first and second source blocks contain 2,623 and 5,910 tile tasks across
12 and 19 source-order windows. Their different spatial concurrency makes the
first window non-representative of the two-block optimum.

The next online experiment executed each trial window exactly once as
production work. A coarse-then-neighborhood version still biased the search:
early `4,6,8,10` windows selected the later bracket, and only two later windows
per integer resolved five. Its two-block wall was `39.899` seconds and its
receipt is:

`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260728T025124Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-a0e1448f.json`

The full-bracket revision tested every integer `4..10` over 28
counterbalanced production windows. Its normalized means ranked
`5,9,6,8,10,4,7`; repeated runs also moved the raw min/max overlap boundary by
less than one percent. Selecting the lowest noisy mean or treating four raw
extrema as a confidence interval would therefore be false precision. The
final resolver uses the 95% small-sample interval, then chooses seven from the
measured overlap on this `4P+6E` host; the same formula predicts six on a
`4P+4E` host. The interrupted receipt containing the first 28-observation
full-bracket run is:

`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260728T025416Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-c5726fef.json`

The repeat whose raw extrema excluded seven by only `0.75%`, motivating the
Student-t correction, is:

`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260728T030425Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-ea8a4032.json`

The final laptop resolver retained `4,5,6,7,8,9,10` in the two-sided 95%
overlap, selected seven from the `4P+6E` topology prior, and completed the
first two source blocks in `40.006` seconds. This agrees with the independently
measured explicit neighborhood and is faster than the explicit-seven mean of
`43.792` seconds:

`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260728T031448Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-17c57a43.json`

The pushed checkpoint also passed the focused planner and authoritative
single-update exact-output tests on the `4P+4E`, 24-GiB Mac mini. Its topology
produces coarse candidates `4,5,7,8`, expands to `4..8`, and uses six as the
overlap prior. The mini did not contain the VLASS MS or 23-GiB packed-CF cache,
so no dataset timing was fabricated or transferred for this check.

The planner-selected full-band run then resolved seven workers in both the
warmup and measured invocations. The warmup completed in `520.892325` seconds;
the measured invocation completed in `696.643812` seconds. Against the frozen
`8,183.264`-second CASA all-fields baseline, the measured result is `11.746x`
faster and clears the `818.326`-second 10x boundary by `121.682` seconds
(`14.9%`). All `25,030,848` attempted samples were accepted, all eighteen
products were written, and every numerical, shape, unit, coordinate, mask,
inventory, source-region, and structured-difference tolerance passed. The
unchanged exact restoring-beam metadata defect remains on `.alpha`,
`.alpha.error`, `.image.tt0`, `.image.tt1`, and `.psf.tt0`: casa-rs stored
`2.955340623855591` by `2.0842981338500977` arcsec at
`71.11363220214844` degrees, while CASA stored
`2.95534086227417` by `2.0842983722686768` arcsec at
`71.11363983154297` degrees.

The harness completed that comparison but rejected the final receipt because
its telemetry schema had not yet admitted the new
`standard_mfs_parallel_worker_plan` log bucket. The products and raw evidence
were preserved rather than rerunning imaging. The log is
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260728T031849Z-vlass-fragment-all-fields-full-band-fftw-f64-experiment-d40678d6.log`
(`557374e36ab5053ec715e63d67381cc5e712b1a2e1391286ed8e2b53fe49c1b3`);
the comparison is the adjacent `.comparison.json`
(`20e2c15424fcefcdc515836b6a7ed028a07c5d22968bc5640234c968f3b0047d`).
The strict schema now recognizes that bounded telemetry collection, with a
focused regression test.

This run also exposed the remaining performance variability. Warmup compact
tap planning, materialization, packing, and sparse-tile replay totaled
`44.626`, `194.986`, `192.725`, and `226.517` seconds, respectively. The
measured invocation spent `44.996`, `347.126`, `344.903`, and `239.504`
seconds in the same owners. A bounded attempt to turn the existing mapped-CF
descriptor prefetch into actual range-specific `MADV_WILLNEED` page prefetch
was negative: the first two four-SPW blocks took `50.379` seconds versus the
retained planner path's approximately `40.006` seconds, with the second block
alone spending `11.990` seconds in materialization. The run was stopped and
the code was removed. Its interrupted receipt is
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260728T035859Z-vlass-fragment-all-fields-four-spw-fftw-f64-experiment-29a13398.json`
(`5a953169f3caac7a28ef4287e38738767114a91a6c9e94894e63036f6aef831c`).

### Deterministic-clean fiducials

Clean parity uses one checksum-bound 64 by 64 pixel user-mask box per workload,
derived from that workload's accepted dirty image. The all-fields patch mask
selects inclusive pixels `[6243,6003]` through `[6306,6066]` and has stable
tree digest
`a68722a8bcb3afe2181b5a2f5e012010cfccd9f5fcdde75e733f56eb97c1b0a9`.
The single-field image's strongest residual is instead at `[4633,6183]`, so
its mask selects `[4602,6152]` through `[4665,6215]` and has digest
`fabf361e6609a4d66c251458c2ed31bc80978d936e78a39a8f449bd1a63dc322`.
Both masks have the full `[12150,12150,1,1]` CASA-image geometry, exactly 4,096
one-valued pixels, and zeros elsewhere.

The first single-field probe intentionally tested the all-fields box before
changing the contract. CASA measured a `0.0306417` Jy/beam full-image peak but
only `0.00127664` Jy/beam inside that box, below the `0.00221293` Jy/beam
five-sigma threshold, and therefore restored an empty model after no minor
iterations. Its tclean call took `282.277905` seconds. That run is rejected as
a clean fiducial, but its evidence is preserved at
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260728T041618Z-vlass-fragment-single-field-clean-casa-7d4fb8c5.json`.
It also exposed a receipt-closeout defect: an unversioned comparator validation
failure was decorated with successful-protocol metadata and then reported as
an internal schema error. Live comparator validation failures now close to the
strict `status`/`reason`/empty-products terminal shape instead.

The clean manifests bind a verified warm CF cache but deliberately omit a
redundant clean warmup. The new `run.preverified_warm_cache=true` contract is
valid only with `cf_cache_role=warm` and `warmups=0`; cold roles or an
additional warmup fail schema validation.

Production `auto` now has the required dirty full-band
correctness/performance evidence, but remains experiment-gated pending Brian's
explicit incorporation approval. The exact restoring-beam defect remains a
wave-closeout blocker.

The first implementation slice is behind
`CASA_RS_STANDARD_MFS_WORKER_PLANNER_EXPERIMENT` in addition to the existing
dynamic-sparse scheduler gate. The application detects the assigned topology
and supplies bounded candidates; the reusable imaging layer receives only
those explicit resource inputs, weighted tasks, and exact-kernel observations.
Production `auto`, automatic calibration in ordinary jobs, removal of the
dynamic scheduler gate, and any dense-versus-sparse preference change still
require Brian's explicit incorporation approval after planner-selected
full-band correctness and performance evidence. The Oracle conversation is
`https://chatgpt.com/c/6a67424d-7f68-83e8-9d08-734a9cd4ef81`.

The restoring-beam difference is not a scheduler-incorporation blocker because
it is unchanged, but it remains a full VLASS metadata-parity closeout blocker.
Memory/thread/GPU improvements and any change that makes sparse storage the
planner-preferred path when dense also fits remain subsequent evidence-driven
work rather than part of this scheduler incorporation.

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

Before clean parity begins, create a deterministic explicit CASA mask from
each workload's accepted dirty fiducial, preserve it by checksum, and use the
same workload-specific mask for CASA and casa-rs. The resulting cleans are new
reproducible fiducials; they are not described as reconstructions of
undocumented interactive choices.

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

### 2026-07-28 AW prediction-CF correctness checkpoint

The first full-size serial clean run preserved dirty PSF, residual, PB, weight,
and sum-weight parity but diverged during model prediction. A 4,096-pixel,
four-SPW, one-component diagnostic isolated the failure to the first major
cycle: the initial masked peak was `0.026857983 Jy/beam`, a positive
`0.0025252474 Jy` TT0 component was selected, and the exact refresh incorrectly
flipped the source negative and raised the masked peak to
`0.049207516 Jy/beam`.

CASA `AWVisResampler::GridToData` does not reuse the forward-gridding CF. It
selects the normal-frequency CF and reverses the direct/conjugate Mueller
mapping before applying W-sign and POINTING-phase conjugation. The compact
casa-rs replay had reused the forward gridding bundle and conjugated its taps.
Commit `c1f7a061d` adds the dedicated prediction selection and source-order
prediction bundles. With otherwise identical diagnostic parameters, the exact
refresh reduced the masked peak to `0.021109872 Jy/beam`, accepted all `98,239`
samples, and emitted no divergence warning.

The same commit removes an MT-MFS frontend early return that bypassed the
already-shared clean-mask product writer. The `mask-image` and `mask-box`
parameters now produce the required `.mask` product for MT-MFS as they already
did for the other imaging families. Focused selector, synthetic AW clean, and
MT-MFS product-inventory tests pass. Full `12,150`-pixel serial clean parity is
no longer the development-turnaround gate. The exact `4,096`-pixel, four-SPW,
2,000-iteration, 19-product row in the promotion ladder above is the next
correctness and performance checkpoint; the one-component diagnostic is not
promotion evidence.

### 2026-07-28 Reduced four-SPW clean checkpoint

The one-time CASA oracle for the exact `4,096`-square, SPW `2,7,12,17`,
field-1525 clean row is frozen at
`/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/casa-reduced-clean/4096-four-spw/casa`.
It completed the requested 2,000 iterations in 171 clean cycles and
`3,631.809729` seconds. Its 19-product inventory and deterministic mask are the
reused reduced-row reference; this CASA case must not be rerun unless the
workload contract changes.

The current serial casa-rs experiment reused that oracle and completed the
same 171 cycles and 2,000 reported iterations in `2,296.570` seconds, or
`1.581x` faster than CASA. The cycle comparator aligned all 171 CASA and
casa-rs cycles with zero discrete component-selection mismatches. Its maximum
start-peak relative difference was `1.1614901342752731e-4`, and its maximum
model-flux relative difference was `2.2714348118055995e-5`. The exact final
refresh reported a `0.008116511 Jy/beam` peak and `0.016844304 Jy` TT0 model
flux. The planner admitted `5,236,080,499` bytes, and live RSS samples stayed
at or below approximately `4.24 GiB`, below the 16 GiB development target.

The full-array comparator found the exact 19 products with matched metadata,
no extra or missing products, and every ordinary numerical ceiling green.
This is not yet a promotion pass. `.alpha` and `.alpha.error` differ in mask
topology at 52 threshold-boundary pixels. `.image.tt0`, `.image.tt1`,
`.residual.tt0`, and `.residual.tt1` have numerically small differences but
retain `investigate` structured-difference labels rather than the required
`good`; their normalized difference RMS values are
`2.592666619709721e-5`, `2.6050960724245685e-5`,
`2.1406558266903393e-5`, and `2.344370703471681e-5`, respectively. The alpha
topology mismatch is a consequence of image-TT0 differences of only a few
times `1e-8` around CASA's approximately `0.00127781 Jy/beam` spectral-index
threshold, but the frozen exact-topology gate remains authoritative.

The dominant measured runtime owner is repeated AW residual replay. The 171
residual refreshes consumed `1,343.239` seconds, averaging `7.855` seconds.
Across the initial and residual passes, compact AW replay spent `758.165`
seconds rematerializing taps, including `585.542` seconds loading CF cells and
`170.182` seconds packing taps; sample planning consumed `215.261` seconds,
grid updates `327.645` seconds, and prepared-sample construction `38.282`
seconds. The final cache counters record 143,926 loads, only 48 hits, and
143,867 evictions. The next correctness diagnostic therefore isolates the
zero-model dirty operator, while the next performance experiment targets
planner-bounded reuse of invariant source-order tap geometry. Neither can
promote until the exact 2,000-iteration row is rerun after a material source
change and all topology and structured-difference gates pass.

The immutable casa-rs receipt is
`/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260728-vlass-4096-four-spw-clean-current/20260728T210052Z-vlass-fragment-single-field-clean-4096-four-spw-dense-rhs-control-248d16c8.json`;
the adjacent full comparison and major-cycle trace JSON files preserve the
product and iteration evidence. No CASA computation was repeated.

The corresponding zero-model diagnostic confirms that the dirty operator is
not the remaining clean-parity owner. Its full 19-product comparison has
matched topology, and the normalized difference RMS of the image and residual
Taylor terms is approximately `8.6e-8` to `9.7e-8`. Replaying the current
casa-rs model against CASA instead exposes a small, coherent prediction
difference: the TT0 and TT1 model-response terms have relative L2 differences
of `3.78283e-6` and `3.59685e-6`, with correlation greater than
`0.99999999999`. A same-input CASA-cleaner sandwich differs by only
`1.91e-7` for TT0 and `2.63e-7` for TT1. The remaining correctness
investigation therefore stays on model preparation and AW `GridToData`
arithmetic rather than component selection or dirty gridding. A new exact
2,000-iteration row is not warranted until that path changes materially.

### 2026-07-28 Adaptive compact-replay checkpoint

The compact replay cache now retains only model-independent source-order sample
plans and packed AW tap bundles. Residual visibilities are still recomputed
from the current model on every major cycle. Retention is planner-bounded and
adaptive within each stream block: a block may retain a source-order prefix
when its next materialization window exceeds the remaining arena, then compute
only the uncached tail. Replay validates block shape, source endpoints, and
cursor order before using retained state.

Focused synthetic MT-MFS coverage is bit-for-bit identical to uncached replay.
A deliberately constrained two-window case retains a `27,136`-byte block and
reduces CF loads from 36 to 28; a full-fit case reduces them from 24 to 16.
The bounded planner test also confirms that the two adaptive arenas remain
inside the admitted memory and headroom limits.

The real 4,096-square four-SPW diagnostic used six one-iteration major cycles
without invoking CASA. The planner admitted `2,190,082,048` bytes for replay
retention, and the four SPW blocks occupied `1,722,755,064` bytes after the
first fill, with no partial or rejected blocks. The first residual refresh
took `6.791` seconds; the next five took `2.102885`, `2.103436`, `2.089087`,
`2.097913`, and `2.100646` seconds. CF loads stopped at 1,126 after the fill
while replay-cache hits grew to 20 and misses remained at four. End-to-end
runtime was `66.03` seconds and all 19 products were written.

This is a performance checkpoint, not promotion evidence: it intentionally had
no CASA comparison and only six clean iterations. Relative to the prior
`7.855`-second average refresh, the steady-state residual stage is about
`3.74x` faster while retaining bounded memory and exact source order. Its
receipt is
`/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260728-vlass-4096-four-spw-replay-cache-adaptive-niter6/20260728T225337Z-vlass-fragment-single-field-clean-4096-four-spw-dense-rhs-control-25a8a57d.json`.
The exact 2,000-iteration promotion row remains blocked on the model-prediction
difference above; replay retention does not alter its numerical path.

### 2026-07-29 Reduced deconvolution-parity breakthrough

The six-cycle diagnostic has now removed the model-prediction divergence from
the production defaults. Instrumentation against the archived CASA trace
identified three distinct rounding contracts in CASA's AW prediction path:

- model images are transformed as complex f32 with FFTW using casacore's
  first-axis-contiguous physical layout;
- AW PSF normalization applies CASA's scale and narrowing order; and
- `BaselineType::findAntennaGroups` adds its double-precision POINTING input to
  a float accumulator, narrowing after each addition rather than before it.

The combined production path exactly reproduces the CASA diagnostic control
flow: initial masked peak `0.03540397`, six accepted component updates, TT0
model flux `0.015148571`, and refreshed masked peak `0.013047275`. A rebuilt
binary run with no hidden CASA-arithmetic experiment variables then passed the
frozen 19-product comparator. Inventory, topology, coordinate metadata, masks,
and every numerical gate are green. The worst normalized difference RMS among
the image and residual Taylor terms is `2.9412836224643946e-7` (about
`0.294 ppm`); `.image.tt0`, `.image.tt1`, `.residual.tt0`, and
`.residual.tt1` are all classified `good`.

The immutable comparison receipt is
`/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-default-niter6.comparison.json`;
the corresponding no-hidden-flags run log is
`/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-default-niter6-v6.log`.
It completed in `30.321` seconds. This supersedes the prior statement that the
2,000-iteration row was blocked on model prediction.

The production resource contract now carries the role-specific implementation
explicitly. The execution planner reports the casacore layout, selects f32
FFTW when the configured runtime supports it with a portable RustFFT fallback,
assigns a system- or user-bounded FFT thread ceiling, and charges one
image-sized `Complex32` transpose buffer to the major cycle. The global dirty
product FFT selection remains independent. The exact 4,096-square,
four-SPW, 2,000-iteration run is now the next promotion gate; the six-cycle
diagnostic remains diagnostic evidence only.

### 2026-07-29 Reduced 2,000-iteration promotion result

The production-default 4,096-square four-SPW row completed all 2,000 requested
iterations in the same 171 major cycles as CASA. Cycle starts, ends, and update
counts match exactly; there is no component-selection or major-cycle control-
flow divergence. The maximum cycle-start peak difference is `24.73 ppm`, and
the maximum accumulated model-flux difference is `13.19 ppm`. The run consumed
`1,386.916` seconds on the Mac mini versus the frozen CASA oracle's
`3,631.809729` seconds, a `2.619x` development-host speedup. This is turnaround
evidence, not a final-laptop timing result.

The exact 19-product inventory, coordinates, metadata, shapes, masks, and
ordinary numerical gates pass. Only `.alpha` and `.alpha.error` retain a
topology difference: 16 of 16,777,216 pixels fall on opposite sides of the
same f32 image-TT0 threshold. The underlying image and residual amplitudes pass
the frozen numerical contract. On 2026-07-29 Brian judged the approximately
`0.2 ppm` prediction-level agreement close enough and approved ending further
sub-ppm arithmetic investigation for this reduced development gate. This
signoff does not relax the final 12,150-square product or performance
requirements.

The exact run evidence is:

- run log:
  `/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-niter2000-promotion-v1.log`;
- full product comparison:
  `/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-niter2000-promotion-v1.comparison.json`;
- major-cycle trace:
  `/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-niter2000-promotion-v1.major-cycle-trace.json`.

The final bounded arithmetic audit confirms that the residual difference is
not an FFT implementation defect. A diagnostic linked directly against CASA
6.7.5.18 invoked casacore `LatticeFFT::cfft2d`, the same entry point used by
AWProject. Its TT0 and TT1 grids are bit-for-bit identical to the casa-rs f32
FFTW grids across all 16,777,216 complex pixels in each term:

- TT0:
  `/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/diagnostics/20260729-vlass-rust-vs-casa-cfft2d-term0.json`;
- TT1:
  `/Volumes/Extra Storage (not encrypted)/SoftwareProjects/casa-rs-vlass/issue-446/receipts/diagnostics/20260729-vlass-rust-vs-casa-cfft2d-term1.json`.

CASA-style scaled complex division, model-FFT thread count, global f64 FFT, and
several plausible FMA contraction orders did not improve the prediction trace.
The public CASA image-tool FFT was also rejected as an AWProject proxy: it uses
a separable transform path and worsened the matched prediction error. These
negative experiments remain diagnostic evidence; their environment gates and
hot-loop branches are not part of the production runtime.

### 2026-07-29 Deconvolution performance-owner redirect

The exact 2,000-iteration production log contains 171 independently timed
`ResidualRefresh` stream passes. They total `360.909` seconds of the
`1,386.916`-second end-to-end run, leaving `1,026.007` seconds outside the
already-cached residual replay. Brian explicitly redirected performance work
toward breakthrough deconvolution algorithms rather than incremental replay
changes. The next performance experiment therefore owns the MT-MFS minor cycle
while the already-launched 4,096-square full-16-SPW promotion chain continues.

The first bounded hypothesis revives the existing mask-sparse MT-MFS RHS
experiment against the corrected production prediction path. For the frozen
single-field mask, only `4,096`, `4,604`, and `5,136` candidate positions are
cleanable at scales `[0,5,12]`, respectively. Retaining two Taylor RHS values
at those positions needs `110,688` bytes rather than full-image scale planes.
An earlier isolated real-geometry block measured `640.123` milliseconds of
one-time basis setup and `7.931` milliseconds of initial sparse RHS
construction. Its old end-to-end attempt predates the AW prediction fixes and
is not correctness or performance evidence.

The current experiment must fail closed on any scale, component-position,
update-count, or major-cycle-topology divergence. Direct compact convolution
does not reproduce a full-image FFT bit for bit, so speed alone is
insufficient. Promotion requires the exact 171-cycle trace contract and the
same frozen 19-product gates used by the dense production control. Per-cycle
profiling now separates RHS preparation, candidate search, model update, RHS
subtraction, residual writeback, and total minor-cycle time. The experiment
remains behind a diagnostic gate pending evidence and explicit approval for
production incorporation.

### 2026-07-29 Mask-sparse MT-MFS breakthrough result

The corrected direct-seeded sparse experiment completed the entire reduced
production row: 171 major cycles, 2,000 component updates, and all 19
products. The minor-cycle state fell from six full 4,096-square scale/Taylor
planes, approximately 384 MiB, to 13,836 sorted candidate entries occupying
110,688 bytes. That is a structural state reduction of about `3,638x`, based
on mask and scale support rather than a VLASS-specific image-size rule.

The complete sparse minor-cycle ledger was only `1.949` seconds:

- direct RHS preparation: `1.052` seconds;
- flat deterministic candidate search and coefficient solves: `0.804`
  seconds;
- model updates: less than `0.001` seconds;
- bounded sparse RHS subtraction: `0.091` seconds; and
- sparse scale-zero residual writeback: `0.001` seconds.

The flat scan and update path are therefore already cache-sized. Hierarchical
peak trees, an interaction graph, deferred writeback, and a GPU-resident
sparse minor cycle are not current owners and must not displace work on the
new dominant stages without contrary evidence.

The run completed in `824.600` seconds on the laptop, with a live resident-set
sample of approximately `5.59 GiB`. The prior `1,386.916`-second dense run was
measured on the Mac mini, so their `1.682x` ratio is useful turnaround context
but is not a controlled same-host speedup claim. Within the laptop sparse run,
171 residual-refresh passes consumed `322.079` seconds and the initial-dirty
stream pass consumed `3.649` seconds. The sparse minor cycle is no longer a
material performance owner. About 497 seconds remain outside those explicitly
logged passes and minor-cycle work, so the profiler now emits the complete
frontend and `ImagingStageTimings` ledgers on every detailed run before
another optimization is selected.

The fail-closed dense-control comparison passes all recorded discrete
decisions: cycle count and ordinal, starting iteration, reported and actual
update counts, first selected scale and position, stop reason, residual-refresh
ordinal, and final 2,000-iteration boundary. The largest numerical difference
in the cycle ledger is `5.739 ppm` for a start/refreshed peak; accumulated
model flux differs by at most `2.104 ppm`. The receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-clean-4096-four-spw-sparse-rhs-v1.dense-control-trace.json`.
The run log is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-production-clean-4096-four-spw-sparse-rhs-v1.log`.
The frozen CASA 19-product comparison remains pending; no production
incorporation is approved from trace evidence alone.

Oracle's evidence-delta review confirmed that direct mask-sparse RHS is the
highest-value deconvolution experiment and recommended a second seeder as the
correctness bridge. The experimental implementation now supports
full-FFT-sampled sparse initialization: each scale uses the existing full FFT
convolution, gathers the exact candidate values, releases the dense scale
image, and then runs the same compact recurrence. Focused tests require every
sampled f32 value to be bit-for-bit identical to the dense source image.
Direct candidate convolution remains the maximum-performance seeder; the
full-FFT-sampled mode is the fail-closed diagnostic and prospective fallback.
Both remain internal experiments until the frozen product contract and broader
geometry gates pass and Brian explicitly approves final incorporation.

### 2026-07-29 Frozen-base image-response and radix-statistics candidate

The first complete same-host exact control after the sparse minor-cycle work
finished in `143.15` seconds end to end (`137.688` seconds in the imaging
core). Its 171 major cycles spent `33.188` seconds in model FFTs, `28.708`
seconds in residual degrid/grid, `42.779` seconds in residual FFTs, and
`24.793` seconds in the controller. The immutable control log is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-4spw-n2000-fftw-wisdom-f64-exact-v1.log`.

Model-delta census showed that 167 late major cycles update only pixel
`(581,2143)`. The promoted experiment treats the exact production AWProject
operator as a locally linear image-domain response:

1. after two stable cycles, perform one exact refresh at the current model;
2. measure a central `+/-0.125` response for each input Taylor term with the
   exact source-order production operator;
3. retain the two-by-two f64 output response planes and the exact frozen base;
4. synthesize later residuals from the accumulated model delta; and
5. invalidate immediately if model-delta support changes, while always doing
   the final residual refresh through the exact production operator.

This is a frozen-base calculation, not a recurrent residual update, so
roundoff does not accumulate with cycle count. The response planes and base
state consume about 640 MiB at 4,096 square. Direct scaling predicts about
5.50 GiB at 12,150 square, so admission must remain resource- and
reuse-dependent. Multiple hot positions and the 63-field workload are not yet
admitted by this one-position experiment.

The first forward-difference response run completed in `49.88` seconds but
left one alpha-mask threshold mismatch. The central response plus forced exact
final refresh removed that topology difference. Fusing the cached response
synthesis with the principal residual peak and replacing dense model-flux
scans with deterministic row-major sparse accumulation then reduced the
controller from `21.098` to `17.649` seconds without changing its discrete
trajectory.

The final reduced candidate additionally replaces masked-value
materialization and comparison sorting with an exact radix order statistic.
Two 16-bit histogram passes select the same f32 total-order median keys and
f64 non-negative deviation keys as the existing keyed implementation.
Focused odd- and even-cardinality tests require bit-for-bit identical median
and MAD results. Across the 171 real cycles, this reduced statistics time from
about `6.27` to `3.70` seconds.

The combined candidate completed in `28.65` seconds, a controlled
`4.996x` same-host speedup over the `143.15`-second exact control. Its core
time was `24.808` seconds:

- major-cycle refresh: `11.148` seconds;
- controller: `4.565` seconds;
- sparse minor cycle: `2.330` seconds;
- initial PSF grid: `3.554` seconds;
- model FFT: `2.069` seconds;
- residual degrid/grid: `5.587` seconds;
- residual FFT: `2.338` seconds; and
- restoration: `1.307` seconds.

All 171 component selections, cycle boundaries, refresh boundaries, and the
final 2,000-iteration boundary match the exact control. All 19 product
inventory, coordinate, metadata, finite-topology, and mask gates pass. The
worst product relative RMS is `5.642879625864284e-7`, about `0.564 ppm`.
There are zero mask, finite-topology, and metadata mismatches.

The immutable evidence is:

- run log:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-4spw-n2000-image-response-radix-madfm-v4.log`,
  SHA-256
  `f07d3b8721de81ef4aa152f3a3e0747ac597e4b3f25ec609fef279cbec2d0989`;
- exact-control trace comparison:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-4spw-n2000-image-response-radix-madfm-v4.trace-comparison.json`,
  SHA-256
  `01a403119013875db93c7f8b1679a9a5b8ffb671ee91e98b643cb391303db780`;
  and
- frozen CASA product comparison:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-4spw-n2000-image-response-radix-madfm-v4.comparison.json`,
  SHA-256
  `0df5ec1401e2aaa18998feb96b654b922e0a793050f28e86c27e53227fc4fb01`.

This passes the 4,096-square four-SPW development promotion gate but remains
an experimental candidate rather than an approved production default. The
next ladder step is the 4,096-square full-16-SPW row. The old full-band runner
and CASA generator were tied to the now-unmounted Mac-mini volume, so they now
resolve a configurable experiment root and binary on
`/Volumes/GLENDENNING`. That volume does not yet contain the one-time
full-16-SPW CASA oracle or its CF cache. Generate them once with CASA
6.7.5.18, freeze them, and do not repeat their timing before promoting the
casa-rs candidate.

Oracle's evidence-delta review judged the exact 171-cycle trajectory, exact
final refresh, complete topology contract, and `0.564 ppm` product result a
scientifically defensible reduced-workload promotion. It also identified the
remaining semantic risk: the exact operator is mathematically linear, but its
f32 execution is only locally affine, so an exact final refresh cannot repair
an earlier wrong component decision. A production candidate therefore needs
a dyadic calibration policy, a held-out exact shadow cycle, coefficient trust
regions, controller decision-margin certificates, and exact fallback on any
uncertified cycle. Lossy response compression, response-tile omission,
analytic response construction, or a changed deconvolution trajectory remain
separate approval decisions.

The first recommended no-code rank census falsified an exact rank-one cache.
Across 163 consecutive cached late deltas, the TT1/TT0 delta ratio spans
`51.7743049648762` through `51.78273215813869`. The direction is extremely
stable but is not bitwise one-dimensional, so production cannot silently
replace the two exact Taylor axes with one response. An approximate rank-one
experiment remains allowed under the experiment boundary, but promotion would
require explicit approval for the new numerical approximation.

The next exact memory experiment exploits a stronger property of the current
calibration: every f64 response sample is a dyadic-scaled difference of two
f32 residual samples. A tile may therefore be representable losslessly as a
shared binary exponent plus signed 16- or 32-bit integers. A diagnostic census
now measures exact tile widths and compression before implementation. The
promotion threshold is at least `1.5x` aggregate compression with bitwise f64
round-trip and decode, synthesis, and controller time no slower than `1.10x`
the raw response scan. No response or sidelobe tile may be omitted.

The laptop oracle runtime is isolated at
`/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312`.
It reports `casatasks 6.7.5.18` and `casatools 6.7.5-18`. The checked-in
`casasiteconfig_vlass.py` binds the existing measures data and disables
automatic data and network updates so the one-time oracle is reproducible.

### 2026-07-29 Full-16-SPW promotion checkpoint

The promoted four-SPW stack now completes the frozen 4,096-square full-band
workload with all 16 compact source-order replay blocks resident. Candidate
`vlass-clean4096-full16-prime-compact-accounted-materialization-plan8-pack1-v21`
completed in `39.84` seconds end to end and `35.423` seconds in the imaging
core. Its retained replay programs occupy `6,982,003,552` bytes. PSF gridding
took `22.565` seconds, PSF FFTs `3.543` seconds, normalization `3.653` seconds,
model FFTs `0.916` seconds, residual degrid/grid `2.560` seconds, residual
FFTs `2.961` seconds, major-cycle refresh `6.437` seconds, and the minor cycle
`0.295` seconds.

The candidate follows the same five compared CASA major-cycle boundaries and
all `641` component updates, with zero discrete mismatch or divergence.
Inventory, coordinate, metadata, ordinary numerical, and structured-difference
checks pass. The normalized differences are `1.097` ppm for `.image.tt0`,
`1.121` ppm for `.residual.tt0`, `0.440` ppm for `.model.tt0`, and about
`0.025` ppm for the PSF. Promotion nevertheless remains blocked under the
unchanged exact topology contract: `.alpha` and `.alpha.error` each differ at
two pixels, `[2837,3114]` and `[309,3290]`. The mismatching casa-rs image-TT0
values are `0.0007449517143` and `0.0007449507248`; the CASA values are
`0.0007449504919` and `0.0007449501427`. CASA includes another pixel as low
as `0.0007449507248`, so neither a scalar threshold change nor a guard band is
semantically valid.

Two bounded experiments narrowed both performance and correctness:

- The exact factorized phase-atlas candidate preserved the initial fixed-point
  window sequence and scales after separating logical segmentation bytes from
  compact resident bytes. On the four-SPW 2,000-iteration row it took
  `81.13` seconds versus the `28.65`-second promoted control, raised image and
  residual differences to roughly 4--5 ppm, produced 25 alpha-topology
  mismatches, and failed structured-difference checks. Reapplying POINTING
  phase for every tap on every replay is both slower and numerically worse on
  this CPU/GPU path. The implementation is rejected, but its memory result is
  retained: a compact primed materialization may exceed the old 256 MiB
  logical segmentation ceiling while remaining within the separately
  accounted packed-program budget.
- A forced final residual refresh accumulated on host f64 and disabled replay
  reuse for that pass. It took `19.408` seconds for the final pass and
  `72.63` seconds end to end, while reproducing the same two alpha pixels and
  effectively identical numerical metrics: `1.097019` ppm image TT0,
  `1.121071` ppm residual TT0, and `0.440304` ppm model TT0. Metal fixed64
  residual accumulation is therefore not the remaining topology owner. The
  experimental runtime branch was removed after measurement.

A frozen-final-state restoration cross matrix then isolated the remaining
owner without rerunning either clean:

- CASA model plus CASA residual, restored by casa-rs, has zero alpha-topology
  mismatches and image TT0 relative L2 error `1.2771e-7`;
- casa-rs model plus CASA residual also has zero mismatches and relative L2
  error `1.2936e-7`;
- CASA model plus casa-rs residual reproduces the exact two mismatches and
  relative L2 error `1.09687e-6`; and
- casa-rs model plus casa-rs residual reproduces the same two mismatches and
  relative L2 error `1.09662e-6`.

The final model and the casa-rs restoration arithmetic are therefore excluded.
The shared final residual prediction/subtraction operator owns both topology
pixels and the approximately `1.1` ppm image error. The next bounded diagnostic
uses the same frozen model on both sides and compares per-sample CASA and
casa-rs AWProject predictions; it does not rerun an unchanged CASA reference.

That prediction isolation is now complete. With both implementations using
the frozen CASA model, the per-sample relative L2 differences were
`1.049247e-6` for RR and `1.214855e-6` for LL. Separating the Taylor terms
measured `5.710593e-7` and `6.994239e-7` for TT0-only RR/LL, and
`4.520092e-7` and `5.164810e-7` for TT1-only RR/LL. The combined error is
amplified by cancellation rather than owned by Taylor-frequency weighting.

The following exact checks excluded the remaining compact-replay inputs:

- all `1,274` traced POINTING direction-to-pixel conversions match CASA at
  Float32 bits;
- for the traced source sample, all `361` CF values read through CASA's image
  tool reproduce the packed casa-rs taps bit-for-bit after the recorded
  pointing phase is applied;
- the packed complex normalization is identical at
  `0.9300320148468018 + 0.03877667710185051i`;
- reconstructing CASA's source order from the phase-aware degrid dump has zero
  packed-tap mismatches and reproduces the casa-rs prediction; and
- the image-cell geometry, phase-center shortcut, CF conjugation, support,
  oversampling offset, and FFT input/output ordering match the inspected CASA
  source and bounded traces.

Using the frozen CASA `weight.tt0` for model preparation improved the
same-model prediction trace but did not close it: TT0-only RR/LL became
`5.006053e-7` and `6.160259e-7`, while the two-term RR/LL result became
`9.340664e-7` and `1.047344e-6`. A direct flat-sky preparation cross matrix
shows that CASA-vs-casa-rs weight pixels change the prepared CASA model by only
about `0.006` ppm; the frozen final model differs by about `0.440` ppm in TT0
and `0.352` ppm in TT1. The final persisted CASA model is not the exact
temporary flat-sky lattice used during its preceding prediction, so this
diagnostic does not justify importing CASA products into the production path.

Two further bounded candidates were rejected:

- Emulating CASA's destructive Float divide/predict/multiply model lifecycle
  left the same two alpha-mask pixels, slightly worsened the image and residual
  metrics, and took `56.84` seconds because a dense round trip scanned both
  model planes after every major cycle. That branch was removed.
- Strict source-order host Complex32 grids with f32 FFTs completed in `54.88`
  seconds but diverged after four compared cycles (`637` updates versus
  CASA's `641`). Image TT0 and residual TT0 errors rose to `1.854%` and
  `1.337%`; model TT0 error rose to `8.000%`. CASA-equivalent behavior here is
  therefore not obtained by replacing the compensated Metal/f64 path with a
  naive f32 accumulator.

The full-16 row remains unpromoted. The positive threshold evidence is
particularly constraining: CASA and casa-rs both use the same strict
`image.tt0 > max(temporary principal residual.tt0)/10` Float rule, and the
lowest included CASA and casa-rs pixel has the same Float bit pattern
`0x3a4348cc`. The two casa-rs-only pixels cross the discontinuity because their
restored TT0 values are 10 and 21 ULP above the corresponding CASA values.
Changing the scalar threshold, adding a guard band, or using an ordinary
topology tolerance would exclude a different CASA-valid pixel with the
identical value and is not an allowed parity fix. The required full-geometry
memory campaign therefore remains gated; no `12,150`-square experiment has
started.

The final same-input FFT and model-preparation audit narrowed the remaining
difference without relaxing that gate. casacore `LatticeFFT::cfft2d` and the
casa-rs official f32 FFTW path produced bit-for-bit identical TT0 and TT1
grids over all `16,777,216` complex pixels, so FFT implementation and worker
count are excluded. Increasing model FFT workers from eight to ten was also a
negative performance/correctness experiment: its products were byte-identical
to the eight-worker candidate and retained the same two topology pixels.

CASA's LEL model preparation performs the two square roots as Float, promotes
their quotient and the model division to Double, and narrows only the final
result. The previous all-Float casa-rs denominator differed at `436` of
`1,985` active model pixels; the promoted expression matches all `1,985`
bit-for-bit. Candidate
`vlass-clean4096-full16-casa-lel-model-prep-official-fftw310-v55` preserves
the exact five compared CASA major cycles and all `641` component updates.
It reduces `.alpha` and `.alpha.error` from two topology mismatches to one,
at `[1542,3144]`, but therefore still fails the 19-product promotion contract.
Its continuous normalized RMS differences are `0.753` ppm for `.image.tt0`,
`0.810` ppm for `.residual.tt0`, and `0.268` ppm for `.model.tt0`. The
decisive next diagnostic captures CASA's normalized model, post-Stokes,
pre-FFT AW, and post-FFT planes from one disposable, frozen-model prediction
major cycle. It does not regenerate or retime the frozen CASA clean oracle.

The v55 receipts are:

- comparison:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-casa-lel-model-prep-official-fftw310-v55.comparison.json`,
  SHA-256
  `9ad8902f721e4558d2e739a221473bdf57c1a45e8433df8add7b386adb41cda8`;
- major-cycle trace:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-casa-lel-model-prep-official-fftw310-v55.trace-comparison.json`,
  SHA-256
  `3dabab4fe726e6f043b127f339962939f041c54b13884eda953fc84b987645df`.

The first reduced replay-memory ledger also found and fixed an admission
accounting defect without changing replay order or science arithmetic. The
resident calculation had charged the large inline replay-window object twice
and retained spare `Vec` capacity. In the bounded synthetic partial-retention
gate this inflated a useful five-sample prefix from its compact `22,984` bytes
to a rejected `30,472` bytes against a `28,672`-byte budget. Compacting the
retained vectors and charging each allocation once admits that prefix, rejects
the sixth sample at a projected `29,136` bytes, and reduces the following
residual-refresh CF loads from `36` to `28`; every product remains bitwise
equal to the uncached run. Detailed profile output now records each retention
candidate's source, tap-request, bundle-metadata, bundle-value, persistent
Metal-batch, and persistent Metal-program bytes. This is positive
planner-accounting evidence only. It is not a full-geometry memory-policy
result and does not relax the promotion gate above.

The immutable full-16 evidence is:

- promoted-stack candidate log:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-prime-compact-accounted-materialization-plan8-pack1-v21.log`,
  SHA-256
  `99b7286384bfe3b5c8760b64ec6ad672913a560cde6d271eaae38865b0a03243`;
- host-f64 negative log:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-forced-final-host-f64-prime-compact-plan8-pack1-v23.log`,
  SHA-256
  `4146228c724c39bbf797348de2df88563b79ce7636aacc46e3f4e4f9254eecdb`;
- host-f64 product comparison, SHA-256
  `37a06346c6041b544308626ea1ebd82d068092faf9d348add1ac7db468d641d4`;
  and
- host-f64 trace comparison, SHA-256
  `5491fe702dc9c4f792ba8d8263a57b6a288ff0a619053f9876c4b9d0e308d271`;
  and
- restoration cross matrix:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-restoration-cross-v1.json`,
  SHA-256
  `9297b93218c767f1e69daf737cada11a3aeb5b74a2e07a4bc5cb5df1804d6eb5`;
- flat-sky model/weight cross matrix:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-prediction-model-compare-v2.json`,
  SHA-256
  `41ce5dec0b9ec57a127184822a03ec0de00423a23ca809e2a06ced283d2e14a9`;
- frozen CASA-model/weight prediction comparison:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-full16-frozen-casa-model-weight-prediction-comparison-v1.json`,
  SHA-256
  `ae7f89b4f83cb28153c171c1073e09a8739b44f336cfc91ce9a7f3c2fd4720e1`;
- rejected model-round-trip comparison:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-casa-model-roundtrip-plan8-pack1-v39.comparison.json`,
  SHA-256
  `27fb551c1ea9e9cb1477830ef444339713a1ca81180e4d76933f276ff82ab5d3`;
  and
- rejected host-f32 comparison:
  `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260729-vlass-clean4096-full16-strict-host-f32-plan8-pack1-cf512-v42.comparison.json`,
  SHA-256
  `6d77666bf37d002507f45263e37e3df1ab7c704869b7ea037425d95919b02995`.

The required full-geometry memory campaign above remains queued behind this
promotion gate. No 12,150-square development or memory-policy run has been
started from this blocked candidate.

### 2026-07-30 Full-16 arithmetic and memory-lifetime checkpoint

The latest bounded full-16 correctness candidate used CASA's bundled FFTW
3.3.10 and the exact casacore-style positive-definite inverse. Candidate v59
still completed the same five CASA major cycles and all 641 component updates,
with `0.734` ppm image-TT0, `0.789` ppm residual-TT0, and `0.280` ppm model-TT0
normalized differences. Every non-alpha product gate passes. Promotion remains
blocked by exactly two alpha/alpha-error topology pixels, `[309,3290]` and
`[1542,3144]`; the frozen comparison receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260730-vlass-clean4096-full16-casa-lel-casacore-inverse-casa-fftw310-v59.comparison.json`,
SHA-256
`fb4a83e321028f50ab5234e07f2dcd30b0bcfc608a6d2bbcba06b4fff2565993`.

The corresponding negative arithmetic evidence is now specific. Candidates
v58 and v59 have byte-identical model, residual, restored image, alpha,
alpha-error, and PSF storage, so the casacore inverse change did not move the
result. Switching from Homebrew FFTW 3.3.11 to CASA FFTW 3.3.10 moved the two
crossing pixels by changing the scale-5 and scale-12 Hessian bits, but did not
remove the topology mismatch. At both current pixels the model is zero and the
restored image is bitwise the principal residual. A frozen same-input oracle
also reproduces CASA's scale-zero coefficient solve, score, and model update
bit-for-bit. Threshold changes, topology tolerance, restoration, the scalar
solve, and model-update arithmetic remain excluded. The next frozen-product
diagnostic is the cycle-zero residual/RHS boundary; no further full candidate
is warranted until that source path changes.

The first exact 12,150-square lifetime audit found that the previous planner
missed the dominant direct-Metal transform overlap. One plane contains
147,622,500 pixels. The eight-plane main-plus-compensation grid is therefore
18,895,680,000 bytes (`17.598 GiB`). The old implementation materialized all
eight Complex64 readback planes while those two Metal buffers remained live,
adding another 18,895,680,000 bytes and producing a raw `35.196 GiB` overlap
before output arrays or FFT scratch.

The production path now preserves exact plane and transform order while
reading, transforming, and releasing one compensated Complex64 plane at a
time. At full geometry the bounded f64 transient is 2,361,960,000 bytes
(`2.200 GiB`); with all eight f32 dirty products materialized, the modeled
overlap is 25,981,560,000 bytes (`24.197 GiB`). This removes
16,533,720,000 bytes (`15.398 GiB`) from the old readback peak. The Metal grid
and compensation buffers are explicitly released before normalization.
Normalization now consumes its PSF and residual arrays rather than cloning
five image planes, the mask-only weight clone is released after mask
construction, and product pixels are written through borrowed lattice views
instead of a full f32 clone. A focused Metal segmented/full dispatch test
confirms that single-plane readback is array-identical to the former
all-plane helper.

The application ledger now charges the one-plane f64 transient at initial and
residual transforms, all prepared Complex32 Taylor planes during model FFT,
the product/mask writer scratch actually retained, and compact replay as
HostHeap beginning at residual gridding. Replay admission uses exact
residual-stage overlap headroom; it describes a pinned, no-eviction
source-order subset when all 16 cyclic blocks do not fit. Runtime receipts
reconcile the actual f64 transient and its full Metal-plus-output overlap
against that allocation.

An adversarial audit then found that the first lifetime version reused the
initial eight-plane grid charge at every residual refresh. Production
residual storage actually owns only the two compensated Taylor residual
planes. The corrected full-geometry ledger therefore records
`18,895,680,000` bytes for the initial grid and `4,723,920,000` bytes for the
residual grid. The generic bootstrap planner also no longer pretends that the
run-state and model-FFT staging allocations overlap the initial grid; both are
restored before exact lifetime admission. A deterministic 32 GiB planner
fixture now admits this corrected shape and reserves `5,166,808,761` bytes of
residual-stage headroom for an exact-size, pinned replay subset. The same
fixture rejects 24 GiB at the semantic lifetime gate and 16 GiB at the fixed
initial-state gate. These are formula tests, not measured policy results.

The same semantic-ledger gate exposed a shared fixed-tile admission defect:
the bootstrap planner separately filled memory with source/read-ahead blocks
and with resident tiles plus their queue even though those allocations coexist
during gridding. Tile planning now derives row-block capacity from one combined
grid-stage budget and charges live source blocks in the tile peak. Deterministic
1, 2, and 4 GiB planner regressions prevent either side from independently
spending the same headroom. This corrects admission accounting only; it is not
a measured memory-policy result.

This is positive implementation and planner evidence, not a memory-policy
result. A read-only campaign audit found that the five policy names still need
distinct execution semantics for stage-aware replay/product demotion and
hybrid next-use eviction; the all-63-field campaign route and final clean
trajectory gate also require hardening. Those deficiencies are recorded as
negative evidence and must be closed before planner or dirty-policy evidence
can promote. The 4,096-square full-16 row remains the active gate and no
12,150-square run has been launched.

The bounded host-accumulation follow-up did not promote that row. Candidate
v60 stopped after `11.72` seconds, before the initial dirty transform, because
optional initial replay priming produced a `338,706,544`-byte arena against a
`268,435,456`-byte reservation. That is retained as a configuration and replay
admission failure, not a science result. Candidate v61 disabled only that
optional priming and exercised the genuine source-order HostF64 grid with the
same CASA LEL model preparation, bundled FFTW 3.3.10, CASA FFT0, and exact
minor-cycle arithmetic. It completed in `86.86` seconds with all `641`
updates and zero discrete trajectory mismatch, but still failed exact
alpha/alpha-error topology at two pixels. `[1542,3144]` persisted, while the
Metal crossing `[309,3290]` moved to the HostF64-only `[2837,3114]`.
Continuous normalized RMS differences were `0.7293` ppm for image TT0,
`0.7820` ppm for residual TT0, and `0.2983` ppm for model TT0. HostF64
therefore changes marginal rounding but is rejected as the topology fix. The
content-addressed comparison receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260730-vlass-clean4096-full16-hostf64-casa-lel-casacore-inverse-casa-fftw310-v61.comparison.json`,
SHA-256
`6fe0bdf804825b3afd3bbc5b01b6923e1e2bd4e1afd60c8e7356ff40776b52a0`;
the trajectory SHA-256 is
`ac84b31a5eba75bccf8b45c3f4459f3faf3481cf2ba4543be5ad6dd6ec678533`.
No further 4,096 candidate is warranted until a contribution-prefix oracle
identifies the first per-contribution, ordering, or accumulator divergence.

The campaign implementation now binds either the single-field selection or
the exact connected 63-field selection, the frozen product inventories, all
12 lifetime stages, named large allocations, measured 32 GiB Darwin/arm64
host evidence, and content-addressed component/major-cycle trajectory
receipts. The promotion receipt cannot promote from self-declared gate names.
For the 4,096-square clean full-16-SPW prerequisite it verifies hashes and
contents of the executable workload, all 19 product comparisons and
tolerances, and the component, major-cycle, and no-divergence trajectory. For
the full-size clean memory candidate, the trajectory additionally binds the
exact workload-result file SHA-256 and the canonical SHA-256 of that result's
embedded product comparison. Dirty-policy review likewise binds the exact
experiment-receipt SHA-256, so edits after review fail closed. A successful
full-size clean receipt promotes only the memory candidate and carries the
separate, unevaluated four-row 10x acceptance contract; it cannot claim final
wave acceptance. Resident process bytes and bytes stored on a mapped or
temporary spill backing are separate ledger dimensions; stored payload no
longer inflates resident-memory admission.
Every policy also emits one immutable runtime-action receipt. `auto` and
conservative admission remain no-swap, aggressive and hybrid use the measured
physical-process ceiling, oversubscription still requires an explicit target,
and ordinary replay LRU is absent. The receipt truthfully says that current
production has last-use releases and residual-stage pinned source-order
replay, but not yet product streaming, replay spill, storage demotion, or
hybrid next-use selection. Consequently planner probes for stage-aware and
hybrid are required to record negative evidence until those requested
mechanisms are real; their names alone cannot promote a row.

Execution receipts must contain complete, monotonic per-stage peak samples,
policy-specific resolved-target evidence, exact full-geometry lifetime
formulas and backing intervals, and an approximately 7.31 GiB measured
compiled-replay working set. A single reusable, content-addressed 64 MiB
uncached `fsync` probe measures the external artifact volume only for
execution; planner-only rows record storage bandwidth as unavailable rather
than inventing a value. The measured rate is injected into both the manifest
and outer process environment and revalidated at promotion. The hardened
campaign, host-telemetry, and workload suites pass `122` focused Python tests.
Current negative evidence is fail-closed: production must still emit the full
per-stage pressure schema and compiled replay total, dirty plans must omit
clean-only residual lifetimes, and stage-aware and hybrid runtime actions
remain inactive.

The first no-grid contribution audit also excludes a compact-replay
translation error in the first full-band block. It stopped after `2.71`
seconds, before allocating or gridding an image. Direct/raw AWProject and
compact replay produced the same 64-bit rolling hash,
`13953044337494127029`, over `25,031` sources, `50,062` RR/LL roles, and
`14,998,926` phased Y-outer/X-inner taps. The hash includes source ordinal,
CF key, location and offset, conjugation, f32 and f64 normalization, weight,
Taylor coefficient, visibility value, and every phased-tap bit; MS
row/channel identity is explicitly unavailable in `VisibilityBatch` and was
not inferred. At a selected high-cancellation residual-TT0 cell, the explicit
CASA Complex-Float multiply/Double-add simulation and HostF64 agreed at every
one of 52 accumulator prefixes. Fixed64 diverged at the first prefix, but its
final normalized error was only `8.17e-15`, confirming it as a secondary
Metal delta rather than the shared topology owner. The TSV SHA-256 is
`08666b52822574f9a16cb04451c1b996ceb293a04a28548f171d3af988094d9e`;
the sealed JSON receipt SHA-256 is
`e8ec4213039d297378a100eef5c210a456fb28e15ea107704065b52938370706`.
The completed all-block no-grid campaign stopped after the sixteenth replay
block, before grid dispatch, FFT, image formation, or any CASA call. All 20
source windows and all 16 block hashes match direct/raw AWProject exactly:
`385,862` sources, `771,724` RR/LL roles, and `470,070,348` phased taps. This
excludes compact replay compilation, source ordering, CF selection, phase/tap
bits, and visibility values as the shared full-band owner. The all-block TSV
SHA-256 is
`9654a350c810d1fc3ce57afeffc67aa11cd585bd38a68a7be519f9c2bdc2a6a9`;
the sealed JSON SHA-256 is
`0decd810c8e778afbce161b3fd9c558b725bb4c05a2ecce89926a5dda67e6b19`.
The next bounded discriminator is therefore the actual CASA-side
`DataToGrid`/AW-resampler contribution and visitation stream, not another
4,096-square image candidate.

An Oracle evidence-delta review adopted a bracket-before-descending rule for
that diagnostic. The first CASA probe will record the ordered accepted
`DataToGrid` call stream and cumulative raw DComplex grid plus `sumwt` at
block boundaries, then abort before normalization, FFT, or product creation.
Only a matching call stream with a differing cumulative grid justifies
instrumenting hundreds of millions of individual Fortran tap updates. Exact
call, cumulative-grid, and `sumwt` bytes would promote the replay/DataToGrid
subsystem, not the integrated row: the two alpha/alpha-error topology pixels
must still reach zero before the 4,096-square candidate promotes.

The probe itself was hardened through bounded negative evidence rather than
silently treating instrumentation failures as science. Version v1 generated
flat Mach-O imports and failed at dynamic load before CASA. Version v2 passed
the two-level import check but rejected CASA's canonical `6.7.5-18` version
string before `tclean`. Version v3 used `dlsym(RTLD_NEXT, ...)`; dyld returned
the replacement itself, so the first call recursively entered a second call
and the third dispatch hit the configured bound before either original call
could return. No version formed an image. The adopted interposer is instead
two-level-bound to the exact exported DComplex specialization and verifies
with `dladdr` that the target belongs to
`libcasacpp_synthesis.6.dylib`. It serializes diagnostic dispatch, identifies
TT0/TT1 by stable raw-grid storage plus an equal source stream, and fails
closed on recursion, unexpected precision, shape, term order, or finalize.

The first successful v4 bracket completed CASA's exact TT0 and TT1 calls and
then stopped with diagnostic exit 86 before finalize, normalization, FFT, or
image formation. It traversed exactly `16,777,216` DComplex values and one
Double `sumwt` per term. CASA's boundary hashes were
`9328098071914194885` / `5773668711911205477` for TT0 grid / `sumwt` and
`9296706034202754823` / `6979414366695050184` for TT1. Its receipt SHA-256 is
`dd6ca70a03a3fb27f2298c3ec5115fd67ca26dd2e4eea34e6b937f897015155d`.
That receipt also exposed a necessary alignment fact: CASA's first native
visibility buffer contained `12,359` accepted sources, while casa-rs's first
SPW replay block contains `25,031`; those cumulative grids must not be
compared as if they covered the same input.

The additive v5 receipt therefore recorded the native boundary without
changing the original calls or grid hashes. It is SPW 2, rows `[0,325)`, 325
ordered row IDs `0..324`, 48 flagged rows, 64 channels, and four
polarizations. The row-ID FNV hash is `15058004568616189240`, the row-flag
hash is `3526571572021233857`, the channel-map hash is
`2111453637644839429`, the polarization-map hash is
`13222926617229668273`, and the frequency hash is
`17711728193083539473`. Receipt SHA-256
`fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f`
freezes that changed diagnostic. The next bounded step is one casa-rs
HostF64 bracket over exactly those 325 SPW-2 rows. A matching grid and
`sumwt` promotes the gridding boundary; a differing grid triggers the
tap-prefix oracle. Neither outcome permits another unchanged CASA bracket.
Independent receipt decoding identifies CASA's channel map as 64 zeroes,
its Stokes-I polarization map as `[0,-1,-1,0]`, and the frequency endpoints
as `1,964,927,697.7790608` and `2,090,923,061.606115` Hz.

The same review usefully challenged physical aliasing and recommended
stage-specific release receipts, second-pass replay residency measurements,
tail-latency-aware early stops, and a lexicographic planner that first enforces
physical/unified-memory feasibility and then minimizes predicted p95 wall
time. Those recommendations are adopted as campaign design input, not as
unmeasured constants. Its suggested 4.8 GiB automatic OS reserve remains a
hypothesis for the bounded pressure-knee experiments; it does not replace the
measured planner policy. Its suggestion to start a full-geometry one-block
probe before the 4,096 promotion is rejected because Brian's iteration ladder
requires that promotion first. Its suggestion that intentional
oversubscription needs approval even as an experiment is also superseded:
Brian already authorized the bounded experiment and reserved approval for
making swap dependence or another materially different policy a production
default.

### 2026-07-30 stabilization checkpoint

Scientific and performance iteration stopped at the explicit checkpoint
boundary. No replacement VLASS diagnostic, CASA timing/reference call, or
scientific/performance imaging workload was started. The already-complete CASA
v5 bracket remains the final VLASS/CASA imaging call in this checkpoint: it
stopped before finalize, normalization, FFT, or image formation, and its
immutable receipt SHA-256 is
`fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f`.
The preceding v4 grid/sumwt receipt remains
`dd6ca70a03a3fb27f2298c3ec5115fd67ca26dd2e4eea34e6b937f897015155d`.
The complete direct/raw-versus-compact all-block audit remains frozen at TSV
SHA-256
`9654a350c810d1fc3ce57afeffc67aa11cd585bd38a68a7be519f9c2bdc2a6a9`
and sealed JSON SHA-256
`0decd810c8e778afbce161b3fd9c558b725bb4c05a2ecce89926a5dda67e6b19`.

The private casa-rs first-visibility-buffer bracket is now implemented but was
deliberately not executed during stabilization. It enforces the frozen
4,096-square field-1525/SPW-2-through-17 selection, brackets rows `[0,325)`,
checks the expected row and flag hashes, requires the expected `12,359`
accepted sources, grids serial HostF64 TT0 then TT1 in exact source order, and
writes a non-overwriting content-addressed receipt before FFT, normalization,
image formation, or products. Producer-native input hashes are explicitly
non-comparable; only the common CASA-order grid and `sumwt` hashes may be
compared. Therefore there is no casa-rs bracket receipt and no claim that the
CASA v5 grid/sumwt boundary matches yet.

The 4,096-square full-16-SPW row remains unpromoted. Candidate v59 still has
the exact five CASA major cycles and all `641` component updates, but
`.alpha` and `.alpha.error` differ at `[309,3290]` and `[1542,3144]`.
Candidate v61 HostF64 moved the former mismatch to `[2837,3114]` without
removing either topology failure. Its `0.7293` ppm image-TT0, `0.7820` ppm
residual-TT0, and `0.2983` ppm model-TT0 continuous differences are useful
negative evidence, not promotion. The immediate correctness blocker is still
the two exact topology pixels; the first unexecuted discriminator is the
casa-rs 325-row bracket comparison against CASA v5.

The full-geometry memory campaign is implemented as a fail-closed campaign
driver, not yet as promoted policy evidence. Its focused Python suite passes
all `31` tests, and Ruff check plus format check pass for the driver and test.
The production ledger distinguishes the `18,895,680,000`-byte initial grid
from the `4,723,920,000`-byte residual grid, models one
`2,361,960,000`-byte f64 transform transient, admits the deterministic 32 GiB
shape, rejects 24 and 16 GiB, and accounts source blocks plus resident/queued
tiles as one overlapping gridding peak. Runtime execution still fails closed
until all 12 stage samples, complete compiled-replay working-set bytes, CPU
allocator data, external-device I/O, and GPU-stall evidence are available.
Stage-aware product/replay demotion and hybrid next-use actions remain
unimplemented; their planner rows must record negative evidence rather than
claim execution. No 12,150-square memory-policy row has been launched.

Checkpoint verification is intentionally bounded to stabilization. Affected
package `cargo check` and warning-denying Clippy pass, including the earlier
draft-PR Linux lint surface. `casa-imaging` reports `375` passed and `9`
data-backed diagnostic tests ignored; `casars-imager` reports `345` passed
and `13` ignored. `cargo fmt`, `docs-check`, the memory-campaign tests and
lint, `git diff --check`, and the packaged Python task/UI gate pass; that
Python gate reports `48` passed and `1` skipped. The workspace `just quick`
gate passes SPDX, formatting, and warning-denying workspace Clippy, then stops
on two unrelated `casa-notebook` local-HTTP tutorial tests because the
checkpoint sandbox denies their socket operation with `PermissionDenied`; no
notebook code or test was changed to mask that environmental failure. No
12,150-square development clean and no unchanged CASA reference were run. The
workspace test suite did discover CASA and execute its existing bounded
`casa_can_open_generated_synthetic_ms_when_available` interoperability smoke:
that test opened a temporary generated synthetic MeasurementSet through
`casatools.table`, completed in `36.80` seconds, and did not call `tclean` or
perform imaging. No further CASA-backed test or workload was started.

The first checkpoint CI run exposed and repaired two wave-caused checkpoint
blockers. Linux warning-denying Clippy required the macOS-only compact-replay
audit arguments to be explicitly allowed as unused on non-macOS/coverage
builds. After that repair, CI formatting and lint, docs, and the packaged
Python job passed. The remaining workspace-test failure was only a stale
diagnostic allowlist: the 32-square synthetic fixture completed successfully,
but its test did not yet accept the new
`standard_mfs_planning_resources` telemetry line. The allowlist now accepts
that named line; its exact regression test passes (`1` passed, `389` filtered
out), as do `cargo fmt --all --check`, warning-denying `casars` Clippy, and
`git diff --check`. This focused fixture regression was the only image-forming
test rerun for the CI repair; it was not a VLASS, CASA, correctness, or
performance workload.

### 2026-07-30 production-boundary checkpoint

The stabilization freeze remained in force through this checkpoint. No
casa-rs bracket, VLASS imaging row, CASA `tclean` call, CASA reference or
timing call, 12,150-square development clean, or full-geometry memory
experiment was launched. The proposed casa-rs bracket output directory is
still absent, so no replacement receipt exists. The externally stored CASA
v4 and v5 bracket receipts were rehashed read-only and remain, respectively,
`dd6ca70a03a3fb27f2298c3ec5115fd67ca26dd2e4eea34e6b937f897015155d`
and
`fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f`.
The frozen v59 and v61 comparison receipts were also rehashed without
executing either workload and remain
`fb4a83e321028f50ab5234e07f2dcd30b0bcfc608a6d2bbcba06b4fff2565993`
and
`6fe0bdf804825b3afd3bbc5b01b6923e1e2bd4e1afd60c8e7356ff40776b52a0`.

The first casa-rs first-buffer diagnostic was executed once after the durable
checkpoint at revision
`0bb8b0bbe8e75cf7f74ae3d4c7381bd0536141eb`. It failed closed before
gridding or receipt creation because its v1 selection preflight incorrectly
compared casa-rs physical MAIN-table row `353600` with CASA's
selection-relative row ordinal `0`. The preserved v1 directory contains only
`casars-imager.log` and `provenance.tsv`, with SHA-256
`c4482993c9c1de436184590e9595c2e4c0bef25773362f2f641025e48e51f0b1`
and
`f9bad4ed51cfe30b3108be12c6e75df1f5c3e7db67aaf23ec187abd39e585bf1`,
respectively. The release binary SHA-256 was
`2ebabeda31e4d53c6d797f6b681a4bca39004cb0947b4b38dd63570715ec1fc0`.
No receipt, CASA call, grid, FFT, image, or product was created, and no
replacement run was launched.

CASA source and the two producer data models explain the mismatch: CASA VI2
reports row IDs in the selected reference MeasurementSet, while
`SelectedMainRow.row_index` is the absolute physical MAIN-table row. The v2
diagnostic therefore maps each production row through the complete selected
row order, records and validates selection-relative ordinals `0..324`
separately from absolute MAIN rows `353600..353924`, and retains the
absolute-row FNV hash `8652707267842020204`. It also derives the Stokes-I
channel map, polarization map, and first-row LSRK frequency vector from the
live casa-rs production selection path. Those observed hashes replace the
hard-coded Rust copy of CASA's first-buffer values in the comparator.

The completed Oracle review agreed that a complete replay block is required
and that no tap-prefix trace is warranted before an actual common-boundary
grid mismatch. The v2 diagnostic addressed its provenance objection by
comparing producer-observed row, flag, channel-map, polarization-map, and
frequency boundaries before comparing the CASA-order grids and production
`sumwt`. Its launcher additionally pinned the frozen source archive, declared
dataset tree receipt, dataset receipt, `tclean.last`, and CASA v5 receipt
identities. This binds the diagnostic to the declared frozen dataset, but it
is not claimed as a retroactive byte-for-byte manifest of the mutable inputs
at the time CASA v5 was captured.

The single v2 attempt at revision
`01bcc3bfbba31522484107838b282c627e32bd50` also failed closed before
receipt creation or bracket gridding. It reached the portable-input hash only
after the direct/raw-versus-compact input audit returned no mismatch, then
rejected source `50` because the diagnostic incorrectly required logical role
zero to select Mueller `0`; CASA and production casa-rs both select conjugate
Mueller `15` for that role when W is non-positive. The preserved v2
`casars-imager.log` and `provenance.tsv` have SHA-256
`cd6e684e8d632ac0442124591fd8cff515cb70827c92c438c8db6e3c6209fc3c`
and
`f794ab0f53469a822c7ce43ebc382dcc5761d6d43e7ed82e2a94271b565888df`,
respectively; its binary SHA-256 is
`4f25e3d64339a11936addf28133e6ce82cf08526a4b0b007cbed96c3ba972106`.
No receipt, CASA call, FFT, image, or product was created, and no unchanged
run was repeated.

The v3-only correction leaves production CF selection and replay untouched.
It validates the CASA W-sign rule for both logical lanes, requires exactly one
selected Mueller `0` and one `15`, and hashes each source canonically in
actual-Mueller order `0,15` with the matching residual. Focused tests cover
positive, zero, and negative W. The v3 launcher SHA-256 is
`3f3175c57a4d309bf72f3531e05dc2e0d87302fd146ca7b0aba3f2f1816d8b3a`.
The single v3 attempt at revision
`2874f5ece02949156fe87139bf3542ab92398db7` reached the intended
content-addressed receipt and then failed closed at the first common-boundary
mismatch. The preserved receipt, `casars-imager.log`, comparison receipt,
`provenance.tsv`, and release binary have SHA-256, respectively,
`4d9efe063d049a4cfd4dbfdee945a5c41f8e6a78f122a6670dca0203ee594883`,
`14d1f0ddfed37dc0111a68830df32868eec5c8106aee066920056bd7c3dc2233`,
`f5aa3b39abab8be21d0208dd8946c9b95872e776d5cdb31c4b36a2b6e089dd39`,
`3b873439de4aca00e4f53e9db4d414cf3ef9cc08e028d11f8d9ac2f390bab483`,
and
`d05aaedcb144184fec842d80f042c0fe0a7d210459761afdf2f3cc34d46ad830`.
The immutable output directory is
`casa-rs-aw-datagrid-bracket-4096-full16-first-vb-v3`.

All v3 row, flag, channel-map, polarization-map, source-count, and portable
input boundaries match the frozen CASA v5 receipt. TT0 and TT1 each contain
`12,359` accepted sources, and the direct/raw input audit and compact replay
share FNV-64 `18073604811373549886`. The first mismatch is the 64-channel
frequency vector: CASA hashes it as `17711728193083539473`, while the v3
production path hashes it as `16545700615609486995`. Their first and last
frequency bits are nevertheless identical,
`4746028312096267298` and `4746556774954748567`, which localizes the
difference to intermediate channels rather than selection or frame endpoints.
The receipt also records unpromoted casa-rs TT0 grid, `sumwt`, and `sumwt`
bits as `9898952817250783852`, `5891270812598592054`, and
`4693530481614913214`; the corresponding TT1 values are
`6319697587634581816`, `1755995775961608899`, and
`13909365493349213550`. Because the comparator stopped at the frequency
boundary, none of those Rust grid or `sumwt` hashes is claimed comparable to,
or correct against, the frozen CASA values. No FFT, image, deconvolution, or
product was formed, and no CASA call occurred.

CASA source inspection identifies the cause. AWProject consumes
`vb.getFrequencies(0)`, while VI2 constructs one frame converter and applies
it independently to every raw channel. The casa-rs v3 production path instead
converted the first channel and multiplied every source channel by that one
ratio. A read-only, metadata-only test against the frozen VLASS MeasurementSet
reproduces both exact hashes: the legacy ratio produces
`16545700615609486995`, while the production per-channel conversion helper
produces the frozen CASA hash `17711728193083539473`. That test did not grid,
FFT, form an image, or create a product.

The production repair now constructs one observatory frame per row key,
converts every selected channel independently, derives wavelength scales from
those exact converted values, and routes the shared frequency and wavelength
vectors through standard-MFS metadata, Briggs density, phase rotation, UVW
scaling, planned samples, CPU replay, and Metal preparation. Native LSRK
remains bit-preserving. The preparation cache retains only the most recent
row key rather than the full pass. Because one preparation block can still
retain a distinct pair for every row, the planner charges worst-case
frequency/wavelength vector bytes and allocator allowance per source row, plus
the separately retained one-key cache. Both the initial and residual-grid
lifetimes are recorded. This is implementation and unit-test evidence only:
the repaired production path has not yet earned a new DataToGrid receipt, so
neither the first-buffer grid/`sumwt` boundary nor the integrated
4,096-square row is promoted. The next allowed discriminator is one immutable
v4 325-row bracket after this repair is durably committed and pushed;
unchanged v1, v2, and v3 attempts must not be rerun.

A checkpoint-only actual-GPU regression exposed one wave-caused arithmetic
blocker: Metal contracted CASA-style complex multiplication and changed the
real result from f32 bits corresponding to `-0.6685914993286133` to
`-0.6685914397239685`, one f32 ULP. The Metal helper now forces each Float
product to round before the following add or subtract by using explicit
fused-multiply-add with a zero addend. The exact actual-Metal regression now
passes. This is positive isolated arithmetic evidence only. No 4,096-square
scientific row was rerun, so the remaining correctness blocker is unchanged:
v59 and v61 each fail the exact `.alpha`/`.alpha.error` topology contract at
two pixels, despite matching all 641 component updates and all five major
cycles. The Metal correction has therefore not earned candidate promotion or
a performance claim.

The full-geometry memory-campaign implementation is unchanged and remains
unmeasured policy infrastructure. Its 31-test Python suite passes, as do Ruff
check and Ruff format check. The deterministic planner evidence still admits
the modeled 32 GiB full-geometry shape, rejects 24 and 16 GiB, and distinguishes
the `18,895,680,000`-byte initial grid, `4,723,920,000`-byte residual grid,
and `2,361,960,000`-byte f64 transform transient. No dirty or clean
12,150-square policy row was executed. Complete 12-stage telemetry,
stage-aware product and replay demotion, hybrid next-use policy evidence, and
the required 32 GiB laptop receipts remain promotion blockers rather than
claimed capabilities.

Checkpoint verification after the production-boundary and Metal changes is:

- `cargo fmt --all` passed;
- affected `cargo check` and warning-denying Clippy passed for
  `casa-imaging` and `casars-imager`;
- workspace SPDX, formatting, and warning-denying Clippy passed through
  `just lint`, including the formerly failing draft-PR lint surface;
- focused `casa-imaging` bracket tests passed (`3`), the exact compact replay
  source-order test passed (`1`), and the actual-Metal CASA-f32 product test
  passed (`1`);
- focused `casars-imager` full-geometry (`2`), memory-pressure (`2`), and task
  contract (`18`) tests passed;
- the memory-campaign unit suite passed (`31`), with Ruff check and format
  check passing;
- the task CLI host conformance gate passed for all `18` binaries and the
  macOS GUI acceptance suite passed (`7`);
- the workspace Rust test and doctest gate passed with `casa-imaging`
  reporting `376` passed and `9` ignored, and `casars-imager` reporting `345`
  passed and `13` ignored; and
- `docs-check`, the final `git diff --check`, and the launcher syntax check
  passed immediately before commit.

That workspace gate explicitly skipped the existing CASA-generated synthetic
MeasurementSet smoke to respect the checkpoint freeze. One ordinary,
pre-existing 3-by-3 CRTF-region interoperability unit did discover CASA and
invoke `casatools.regionmanager`; it created only temporary unit-test data and
did not call `tclean`, process VLASS data, form an image, or create a reference
or timing receipt. No replacement CASA-backed run was launched.

The per-channel frequency repair was stabilized as a separate checkpoint
before any v4 diagnostic execution. `casa-imaging` reports `378` passed and
`9` data-backed diagnostics ignored; `casars-imager` reports `350` passed and
`14` ignored. The metadata-only frozen-VLASS frequency test passed separately
and established the two exact hashes above without reading visibility samples
or forming a grid. A normal multi-channel regression also proves that both
locally converted and precomputed exact wavelength vectors reach the routed
visibility-row boundary bitwise. Affected-crate `cargo check` and
warning-denying Clippy pass, including the draft-PR Linux lint surface; Clippy
found and the checkpoint fixed one test-only import and one needless borrow
rather than allowing either warning. Workspace SPDX, formatting, and
warning-denying Clippy pass. The sandboxed workspace test gate reaches two
unchanged `casa-notebook` local-HTTP tests and fails because socket creation is
denied with `PermissionDenied`; those exact two tests pass outside the
socket-denying sandbox (`2` passed, `15` filtered out). The workspace run also
completed its pre-existing tiny CASA table-reader interoperability smoke in
`36.58` seconds. That smoke opened a temporary generated synthetic
MeasurementSet through `casatools.table`; it did not call `tclean`, process
VLASS data, grid, FFT, form an image, or create a correctness or timing
reference.

The full-geometry campaign remains unexecuted. Its unchanged Python suite was
rerun and passed all `31` tests, and Ruff check and Ruff format check pass for
the campaign driver and test. The frequency repair adds the per-row handles,
worst-case retained vector pairs, allocator allowance, and one persistent
one-key frequency/wavelength cache to the existing lifetime ledger, including
both initial- and residual-grid residency. Focused planner regressions assert
the worst-case per-row byte charge, bounded cache allocation, and two clean
cycle residencies. This does not constitute a
12,150-square planner, dirty, clean, swap, compression, telemetry, or laptop
receipt. `docs-check` and `git diff --check` pass. No 4,096-square imaging row,
12,150-square development clean, full-geometry memory-policy row, or CASA
imaging/reference run was launched for this repair.

After that repair was durably committed, pushed, and green in exact-SHA CI,
the single authorized v4 first-buffer discriminator ran at revision
`11cdeec698b63b9023233f3d7855d6c07d47284f`. It stopped at its intended
post-grid comparison boundary and preserved only `receipt.json`,
`casars-imager.log`, `comparison.log`, and `provenance.tsv` under the immutable
external directory
`casa-rs-aw-datagrid-bracket-4096-full16-first-vb-v4`. Their SHA-256 digests
are, respectively,
`1c52961a3058f8f362e9d554c64b69a077f9414a7a44c738bed5351e6df59b40`,
`c9d75afca00dedc5e3083dbf18e493c2d8edf27e7a62fa7e6936ee9af65359e2`,
`479b6871ca3279e2a77b7ef2e6dfb391244a704a96166440b6e916350f8bc570`,
and
`8c510bfee7945898ff288b4aa84acbf145fe255dbdbadbfa46cf22a05c063888`.
The receipt's embedded evidence digest is
`5783293d3401f97b12742d8c89bd98e2b0d1303cabf4e19505f245db7cbe9e0a`;
the release binary and launcher SHA-256 digests are
`2968ce458ae6e258102cf901bd686be05f42bbaed11adad74a32499134febac5`
and
`8692df29392346690b62d2d3cd6a9318d379f0b4c41557f5fda0ed3682b3f52d`.
The provenance continues to bind the unchanged CASA v5 receipt
`fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f`;
CASA was not rerun.

The v4 positive result is exact producer-boundary alignment through selection
and frequency preparation. CASA and casa-rs now agree on rows 0 through 324, 48
flagged rows, the 64-channel map, the `[0,-1,-1,0]` polarization map, `12,359`
accepted sources in each Taylor term, both frequency endpoint bits, and the
full frequency-vector hash `17711728193083539473`. The direct/raw and compact
input hashes are also identical at `7924638447934945938`; the portable input
stream hash is `17219742412056116454`. This promotes the per-channel frequency
repair itself past the frozen metadata boundary, but it does not promote
AWProject gridding or the integrated row.

The v4 negative result is the first actual common-boundary grid mismatch.
CASA's frozen TT0 grid hash is `9328098071914194885`, while casa-rs records
`9898952817250783852`; the comparator stops fail-closed at that field. The
receipt also records casa-rs TT0 `sumwt` hash
`5891270812598592054`, TT1 grid hash `6319697587634581816`, and TT1 `sumwt`
hash `1755995775961608899`, versus frozen CASA values
`5773668711911205477`, `9296706034202754823`, and
`6979414366695050184`, respectively. Those later values are evidence for
localization, not independently promoted boundaries after the first failed
field. The repaired frequency path leaves the casa-rs grid and `sumwt` hashes
unchanged from v3, so the legacy scalar conversion was a real semantic defect
but is not the owner of the remaining grid mismatch.

No normalization, FFT, image, clean, deconvolution, product creation, CASA
call, or performance measurement occurred in v4. The integrated correctness
blocker therefore remains the two exact `.alpha` and `.alpha.error` topology
pixels in the unpromoted 4,096-square full-16-SPW row. Promotion now fails
earlier at a differing TT0 `DataToGrid` boundary after matching
cross-producer selection and frequency preparation and matching casa-rs
direct/raw versus compact inputs; whether that boundary difference causes the
two final topology pixels is not yet proven. The next justified changed
diagnostic is a bounded CASA/casa-rs tap-prefix oracle for the first divergent
grid accumulation. No unchanged v1-v4 casa-rs bracket and no unchanged CASA
bracket/reference may be rerun. The full-geometry memory campaign remains
implemented but unexecuted, with no 12,150-square policy receipt or production
memory-policy promotion.

The intentional checkpoint set consists only of the Rust production-boundary
and Metal-rounding changes, the per-channel MFS frequency repair, their tests,
this plan receipt, and the reusable fail-closed launcher. Rebuildable `target/`
content, CASA logs, TempLattice scratch, Python and Ruff caches, copied CASA
products, MeasurementSets, CF caches, and all external runtime receipts remain
generated or external evidence and are excluded from version control.

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
