# VLASS Fragment Imaging Correctness And Performance Plan

Truth class: approved execution contract
Last reality check: 2026-07-23
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

## Stop Conditions

Stop and request direction rather than changing the contract if:

- the full CASA or casa-rs geometry cannot complete on the 32 GiB host;
- a requested optimization requires weakening or deleting a parity product;
- the deterministic clean mask or CASA-valid comparison domain cannot be
  frozen objectively;
- the all-field workload reveals that a connected-fragment assumption is wrong;
- either workload remains below 10x after the measured memory, CPU, and GPU
  owners have been exhausted; or
- completing the goal requires distributed execution, a different machine, or
  a materially different persisted/public contract.

Reduced workloads, accepted correctness differences, target changes, or scope
deferrals require explicit Brian signoff. They are not implicit closeout paths.
