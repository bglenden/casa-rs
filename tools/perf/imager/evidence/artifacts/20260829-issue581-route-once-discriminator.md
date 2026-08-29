# Issue 581 route-once discriminator

Date: 2026-08-29

This records the serial and four-worker discriminator for the approved
route-once sector candidate. CASA was not rerun: the pinned oracle, workload,
selection, and comparator contract are unchanged.

## Candidate

- Parent: `c04e5f9bd`
- Reconstruction route core: `1faebe3c5`
- Exact planner accounting: `70f071fc4`
- Runtime sector integration: `055614422`
- Receipt-boundary repair: `d68d49af5`
- Dataset: directly mounted `wave1-vla-single-medium.ms`
- Workload: 16 selected channels, 1024 pixels, Briggs robust 0.5, Hogbom,
  `niter=50`, RustFFT

Every frame is decoded and routed once into four stable center-owned sectors.
The reusable route is one packed classification array, one contiguous routed
record array, one prediction array, and five sector offsets. Planning admits
`28R + 20` bytes for maximum frame records `R`; actual vector capacities are
reported. Sector accumulation is compensated and merged in fixed sector-ID
order. Commit performs coverage bookkeeping and reusable-buffer release only.

## Focused structural evidence

- Reconstruction route tests: 10 passed.
- Production complete-data MFS: bit-exact products, work identities, and commit
  identities for one, two, and four workers.
- Runtime fixture: one routed frame, three encoded/routed/degridded/gridded
  records, zero rescans, zero dynamic partial bytes, and actual route peak
  `104 == 28 * 3 + 20` bytes for every worker count.
- Focused compile-plan/run, parser, formatting, and strict Clippy gates passed.
- The known untouched unit-centre assertion remains a parent failure:
  `0.7500004989529645` versus an exact `0.75` at a `1e-12` threshold.

## Matched 16-channel discriminator

The fixed-parent row is the recorded serial result from the batched-dispatch
study. All rows use the same directly mounted MS, selection, image geometry,
clean controls, and application path.

| Measurement | Fixed parent serial | Route serial | Route four workers |
| --- | ---: | ---: | ---: |
| Full Rust wall | 90.614444 s | 86.970694 s | 86.966139 s |
| Gridded replay wall | 4.086159 s | 3.580817 s | 4.066014 s |
| Source fill | 1.463156 s | 1.431155 s | 1.485879 s |
| Prepare | 0.720712 s | 1.744951 s | 1.750396 s |
| Execute | 0.872256 s | 1.619612 s | 2.105506 s |
| Commit | 2.204062 s | 0.003330 s | 0.004535 s |
| Dispatch waves | 31,985 | 127,940 inline | 31,985 parallel |
| Routed/degrid/grid records | unavailable | 49,141,788 each | 49,141,788 each |
| Sector rescans | unavailable | 0 | 0 |
| Dynamic partial bytes | 32,768 B | 0 B | 0 B |
| Actual/planned route peak | unavailable | 57,364 / 114,708 B | 57,364 / 114,708 B |

The route-once serial result passes both pre-recorded gates: replay is 12.37%
faster than 4.086159 seconds, and prepare plus execute plus commit is 3.367892
seconds, 11.30% faster than 3.797030 seconds. Full wall is 4.02% faster.

The four-worker result fails the next gate. Replay is 13.55% slower than the
new serial path while full wall is unchanged within run noise. The four worker
lanes accumulated 0.470201, 0.633097, 0.351257, and 0.889482 active seconds,
but each accumulated 1.216024 to 1.754249 wait seconds. The 31,985 per-frame
Rayon rendezvous and sector imbalance erase the scientific compute gain.

Serial log:
`/private/tmp/issue581-route-ch16-serial/20260829T233605Z-wave3-standard-mfs-single-term-turnaround-abfa33d2.log`

Four-worker result:
`/private/tmp/issue581-route-ch16-parallel/20260829T234301Z-wave3-standard-mfs-single-term-turnaround-a008ba86.json`

## Decision

The candidate establishes a material serial replay improvement and exact
bounded routing, but it does not yet satisfy issue #581's observed multicore
improvement. Medium and full all-channel gates stop here. The next candidate
must target demonstrated per-frame worker rendezvous/imbalance and must not use
more threads to mask serial performance, restore the old whole-run imager, or
discard the proven route-once serial mechanism without an explicit decision.

During the serial run, an attempted new `backend_plan_logs` bucket was rejected
by the strict receipt schema after science completed. Commit `d68d49af5` keeps
the exact route summary in the immutable benchmark log while leaving the
persisted receipt schema unchanged.
