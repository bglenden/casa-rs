# Issue 581 spatial-owner candidate: rejected

Date: 2026-08-29

This records the terminal evidence for the four-sector gridded-replay candidate
described in issue #581. The candidate commits remain in Git history, but the
implementation was reverted after the serial stage discriminator failed. No
four-worker or full all-channel run was performed.

## Candidate

- Parent: `5e28d920c`
- Spatial-owner core: `6eb7cf8f4`
- Exact planner accounting and focused acceptance: `fca542562`
- Dataset: directly mounted 32 GiB-class VLA MeasurementSet,
  `wave1-vla-single-medium.ms`
- Workload: 16 selected channels, 1024 pixels, Briggs robust 0.5, Hogbom,
  `niter=50`, one worker, RustFFT
- Frozen CASA oracle: reused; CASA was not rerun.

The candidate gave reconstruction four stable center-quadrant sectors with
support-three halos. Every block exposed the same four exclusive partitions
for one or many workers. Each sector scanned complete record groups, fused
prediction with compensated sector-local gridding, returned a zero-byte
partial, and left commit with coverage bookkeeping only. Finish merged sector
IDs deterministically into one ordinary grid before the existing FFT.

Exact admitted grid residency was `32 * C * (H + G)` bytes, where `H` is the
sum of the four clipped halo rectangles and `G` is the full grid cell count.
It was one logical allocation and physical slot, independent of worker count.

## Focused structural evidence

- Reconstruction sector tests: 7 passed.
- Production complete-data MFS test: bit-exact dirty, PSF, residual, model, and
  sum-weight products for 1, 2, and 4 workers; exact stable work and commit
  identities; unchanged source/artifact pass counts; zero dynamic record
  partial bytes.
- Planner test: exact sector-plus-merge demand, allocation, and slot capacity;
  prediction-only selected output retained its existing workload role.
- Focused compile-plan/run test, strict Clippy, formatting, and diff checks
  passed.
- Two unrelated broad reconstruction failures reproduced unchanged at the
  untouched parent `5e28d920c`: the unit-centre forward tolerance and one model
  identity golden. They are not candidate regressions.

## Serial compute-bearing discriminator

The comparison baseline is the previously recorded matched 16-channel serial
run from the rejected batched-dispatch study. Both runs use the same dataset,
selection, image geometry, clean controls, worker count, and interface.

| Measurement | Prior serial | Spatial-owner serial | Change |
| --- | ---: | ---: | ---: |
| Full Rust wall | 90.614444 s | 89.666224 s | 1.05% faster |
| Gridded replay wall | 4.086159 s | 5.008789 s | 22.58% slower |
| Source fill | 1.463156 s | 1.436830 s | 1.80% faster |
| Prepare | 0.720712 s | 0.683595 s | 5.15% faster |
| Execute | 0.872256 s | 4.125092 s | 372.93% slower |
| Structural commit | 2.204062 s | 0.003312 s | 99.85% faster |
| Logical partitions | 1,978,026 | 127,940 | 93.53% fewer |
| Dynamic partial capacity | 32,768 B | 0 B | eliminated |

The candidate successfully removed per-record predictions and serial
scientific commit. The mandatory four complete frame scans moved more cost
into execute than commit removal saved, however. The small full-wall
improvement is outside the targeted stage and can be explained by source and
run noise; it does not rescue a 22.58% serial replay regression.

The run completed 31,985 artifact blocks and 127,940 stable sector
partitions. Its exact replay identity digests matched between execution and
commit. The result artifact is:

`/private/tmp/issue581-spatial-ch16-serial/20260829T224643Z-wave3-standard-mfs-single-term-turnaround-3a086eda.json`

## Decision

The pre-recorded falsifier rejected a material serial regression and the
programme rule forbids using multiple workers to mask serial inefficiency.
This candidate therefore stops here. Running four workers could only obscure
the demonstrated one-worker regression and cannot make the architecture
acceptable. The full all-channel gate would add cost without changing that
decision.

The evidence rules out repeated full-frame sector scans as the next mechanism.
Any successor candidate must route each bounded frame once into stable
reconstruction-owned spatial work without restoring the old whole-run runner,
adding a second executor, or retaining per-record predictions across commit.
