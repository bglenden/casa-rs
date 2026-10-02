# Cube Metal engineering checkpoint — 2026-10-02

Truth class: non-normative implementation and measured evidence
Last reality check: 2026-10-02
Verification: reused directly affected guarded tests and application/science
observations; six schema/inspection integration tests, `just docs-check` and
diff checks. Numerical disposition remains open.

## Retained implementation

This local checkpoint preserves the existing application path, not a new
optimization campaign or scientific closeout. Its parent is
`5cda8a734309c419c855248baebbbf10d7beea67` on `codex/t55-full-size-validation`.

- Compiled geometry demand skips unused antenna parallactic angles in ordinary
  Stokes-I/non-AW imaging. Absent angles are explicit `None`, not physical zeros.
  Polarized/AW consumers retain their requirements; phase-centre and spectral
  transformations are unchanged. Sample schema 6/generation 10 encode absence;
  the historical generation-9 fixture remains.
- An exact-TDB-epoch, single-entry thread-local solar-position cache reuses the
  unchanged ephemeris calculation. No approximate coordinates or growing cache.
- Opt-in Metal stage timestamps distinguish prediction from residual formation
  and gridding, using existing fences. `CASA_RS_PROFILE_METAL_STAGES=1` enables
  counters; ordinary execution creates no counter buffers or verification pass.
- Required caller/test migrations, actual-device error/tail coverage and the
  inherited test-only FFTW scratch-expectation correction are included.

## Measured decisions

The workload is 4,094,064 rows, 512 stored channels, output channels 240–271,
32 × 1024² images, natural Stokes I, Clark at 0.5 mJy, Metal with four host
workers. Timing includes preparation, refreshes, intermediate I/O and publication.
These are single complete observations, not statistical estimates.

Fresh geometry before/after: 122.350860 / 115.499924 seconds (5.60% reduction).
The geometry correction remains required independently of that estimate.
The final unprofiled retained parent took 109.954713 seconds; GPU tile-local
accumulation took 116.067265 seconds (5.56% slower), with higher GPU time too.
That candidate and the earlier eight-lane gather are retired. Neither has a live
alternate path or runtime flag; their exact source, binaries and evidence remain.
The broader coordinate-cache experiment is also withdrawn.

No matched CASA deep32 timing was generated here; full512 CASA timing is not a
substitute. Profiling timers are nested, not an additive stage decomposition.
The timestamp diagnostic places 70.03% of residual GPU time in formation/gridding;
the failed Instruments trace supplies no valid hardware-contention evidence.

## Acceptance and restart

The final parent's unchanged CASA seven-product/nine-check assessment passes all
32 strict restored planes with zero hard/strict failures; maximum normalized
restored difference is 0.000947586. Seven product panels were inspected.
Its 18 numerical review triggers on 16 planes remain unapproved. Raw nonlinear
model/residual diagnostics are not waived, and this is not full T55 acceptance.
Do not transfer approval from another output identity or mark the goal complete.

Native/aggregate limits remain 16 GiB, checks 8 GiB, two Cargo jobs. This checkpoint
does not authorize push, merge, release, cleanup, installation, MFS/W8 work or a
full512 restart. End this local Metal tuning round; next is numerical disposition
and separately authorized subsequent work, not another tile/gather trial.

The durable [experiment report and evidence](/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929/gpu-gridding-discriminator-20261002/CHECKPOINT.md)
contain exact commands, output identities, logs, resource receipts, rejected
snapshots and verification handles. Update only the existing
[CURRENT summary](/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md)
on resumption. This checked-in record is historical, not a second current log.
