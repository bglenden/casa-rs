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

## Rejected pool-residency discriminator

The user approved one bounded continuation: keep the existing source slots,
route, four sector partitions, per-frame barriers, deterministic commit order,
memory plan, and public runtime API, but enter the runtime-owned Rayon pool once
around the complete bounded consumer lifecycle. Candidate `85da4b156` made the
already-cross-thread kernel and completion state explicitly `Send` and added an
exact pool-entry counter. The candidate was rejected and reverted by
`42bdaf14c` after the predeclared performance falsifier fired.

The frozen route serial replay of `3.580817` seconds remained the acceptance
baseline; the four-worker ceiling was `3.401776` seconds. CASA was not rerun.

| Measurement | Pool-resident serial | Pool-resident four workers |
| --- | ---: | ---: |
| Full Rust wall | 89.929650 s | 94.990008 s |
| Gridded replay wall | 3.756633 s | 4.424651 s |
| Source fill | 1.443440 s | 1.866870 s |
| Prepare | 1.762016 s | 2.341838 s |
| Execute | 1.777211 s | 1.816739 s |
| Commit | 0.003510 s | 0.004813 s |
| Pool entries | 0 | 1 |
| Dispatch waves | 127,940 inline | 31,985 parallel |

Both observations retained 31,985 frames, 49,141,788 encoded, routed,
degridded, and gridded records, zero sector rescans, zero dynamic partial bytes,
the same work and commit identity digests, two source slots, and the same route
and source residency. Focused production tests separately proved bit-exact MFS
products for one, two, and four workers.

One pool entry reduced parallel execute wall by 13.71% from the prior
2.105506-second observation, but prepare rose 33.79% and source fill rose
25.64%. Total replay was 23.57% slower than the frozen route serial baseline,
17.78% slower than the matched candidate serial observation, and 8.82% slower
than the prior four-worker route observation. The hypothesis is therefore
falsified. Medium and full gates were not run, and the campaign stops before a
multi-frame window, extra source slots, or wider executor-lifecycle change.

Serial result:
`/private/tmp/issue581-pool-resident-ch16-serial/20260830T000633Z-wave3-standard-mfs-single-term-turnaround-4e8b6915.json`

Four-worker result:
`/private/tmp/issue581-pool-resident-ch16-parallel/20260830T000836Z-wave3-standard-mfs-single-term-turnaround-563580d5.json`

## Rejected 64-frame execution-window discriminator

The user approved one wider bounded candidate after the pool-residency result:
coalesce at most 64 original artifact frames directly into each of the same
two source slots, prepare one reconstruction-owned route window, and execute
the same four deterministic sector owners once per window. Candidate
`25d0e4944` retained the existing executor, public runtime API, persisted
artifact and receipt schemas, fixed sector commit order, worker-independent
scientific accumulation, and one common `workers = 1/2/4` path. It was
rejected and reverted by `0932a2df8` after the predeclared serial falsifier
fired. No multicore, medium, or full-data candidate run followed.

The frozen route serial ceiling was 3.580817 seconds for replay and 3.367893
seconds for prepare plus execute plus commit. The 64-frame candidate measured:

| Measurement | Frozen route serial | 64-frame serial |
| --- | ---: | ---: |
| Full Rust wall | 86.970694 s | 88.937580 s |
| Gridded replay wall | 3.580817 s | 3.711978 s |
| Source fill | 1.431155 s | 1.374822 s |
| Prepare | 1.744951 s | 1.649280 s |
| Execute | 1.619612 s | 2.024469 s |
| Commit | 0.003330 s | 0.000120 s |
| Artifact blocks / logical frames | 31,985 / 31,985 | 500 / 31,985 |
| Dispatch waves | 127,940 | 2,000 |
| Planned / actual route peak | 114,708 / 57,364 B | 7,347,712 / 3,677,696 B |
| Planned / peak live source capacity | 262,288 / 131,216 B | 16,786,432 / 16,786,432 B |

Replay regressed 3.66 percent and the combined consumer stages regressed 9.08
percent despite 98.44 percent fewer dispatch waves. Source fill and preparation
improved, while execution regressed 25.00 percent. The evidence therefore
rejects scheduling overhead as the dominant remaining serial cost at this
window size; sweeping one sector across 64 prepared frames introduces a larger
execution penalty, plausibly from the much wider route working set. That cache
interpretation is a hypothesis, not a retained conclusion or permission for a
window-size sweep.

The run preserved exactly two source slots, 31,985 ordered logical frames,
49,141,788 encoded/routed/degridded/gridded records, zero sector rescans, zero
dynamic partial bytes, 500 bounded artifact windows, a 64-frame high-water,
zero replay payload-copy bytes, 2 buffer allocations, and 498 reuses. Focused
tests separately proved exact dirty, PSF, model, residual, sum-weight, work
identity, and commit identity equality for one, two, and four workers, including
multi-frame and partial-tail windows and fail-closed later-frame corruption.

The benchmark bundle status is `failed_comparison` only because this
turnaround manifest skipped CASA without binding a frozen CASA product prefix;
the Rust science run completed and its timing and structural evidence were
recorded. That harness bookkeeping failure does not change the serial
performance rejection.

Serial bundle:
`/private/tmp/issue581-window-ch16-serial/20260830T012136Z-wave3-standard-mfs-single-term-turnaround-fdb15f77.json`

## Variable-budget replay study

The 64-frame rejection did not establish that every bounded multi-frame
window is slow. A current-format replay-only discriminator therefore swept
seven byte budgets over one immutable capture from the directly mounted medium
VLA MeasurementSet. Each observation rebuilt a matching private gridded
artifact and science state outside the timed replay, then used the production
planner, reader, route, kernel, and deterministic commit path with one worker.

`target frames` is only the dataset-derived input used to compute a byte
budget. It is not the resulting window width or a production constant. The
planner admitted each ordered window from its exact payload and route demand.

| Target frames | Requested bytes | Windows | Maximum frames | Actual working set | Median replay |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 188,680 | 8,227 | 1 | 188,680 B | 1.119142 s |
| 2 | 377,360 | 6,278 | 3 | 377,288 B | 1.123125 s |
| 4 | 754,720 | 5,402 | 5 | 754,664 B | 1.107823 s |
| 8 | 1,509,184 | 4,915 | 10 | 1,509,136 B | 1.148036 s |
| 16 | 3,015,808 | 5,191 | 21 | 3,015,744 B | 1.111195 s |
| 32 | 6,029,120 | 5,330 | 44 | 6,029,108 B | 1.108707 s |
| 64 | 12,057,408 | 5,310 | 89 | 12,057,164 B | 1.105714 s |

The one-frame row uses its earlier clean three-observation cohort because a
later repeat was disturbed by host noise. The remaining rows are from:
`/private/tmp/issue581-budget-discriminator-release-r2.log`.

Serial replay is essentially flat across the useful range. The sweep also
shows that the retained per-ordinal capacity law is intentionally not
monotonic in window count: more byte budget can retain wider ordinal maxima
and produce more windows. Every row preserved two source slots, zero replay
payload copies, exact planned versus actual dynamic residency, 14,520,731
encoded/degridded/gridded records, and final normal-state identity
`b49653403ac89ba12e2cb3b9e776742c1d6cc792a254e4e46e32a64c025d9077`.

The smallest serial-safe useful cohort, 754,720 requested bytes, then ran as a
same-binary matched serial/four-worker discriminator. One warmup per
configuration preceded three alternating serial/parallel pairs. This avoids
using the dirty probe worktree's `HEAD` string as binary provenance. The
predeclared parallel ceiling was 95 percent of the contemporaneous serial
median.

Command:
`CARGO_INCREMENTAL=0 CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance cargo test -p casa-imaging-runtime --release --lib weighting::serial_compute_probe::medium_vla_64ch_gridded_replay_four_worker_754720 -- --exact --ignored --nocapture`

| Measurement | Matched serial | Four workers |
| --- | ---: | ---: |
| Median replay | 1.081342125 s | 0.986170333 s |
| Median source fill | 0.396024095 s | 0.407265410 s |
| Median prepare | 0.491512131 s | 0.493869920 s |
| Median execute | 0.544969940 s | 0.445014312 s |
| Median commit | 0.000556786 s | 0.000702934 s |
| Dispatch waves | 21,608 | 5,402 |
| Worker-stack capacity | 0 B | 8,388,608 B |

The computed ceiling was 1.027275018 seconds; the four-worker replay was 8.80
percent faster and passed. All paired observations retained the exact 5,402
window plan, five-frame high-water, two source slots, 492,432-byte source
capacity, 262,232-byte route capacity, 754,664-byte combined dynamic replay
residency, zero payload copies, zero dynamic partial bytes, 21,608 partitions
and commits, exact work/commit identity
`3750ca3f383911281303cb5f38e8031766e9b5e3105ac57005af8d4dd9aea573`,
and the same final science identity quoted above. All four logical worker slots
executed exactly 5,402 partitions; the worker result still contains substantial
partition-dependent waiting, so this is an admission result rather than a
claim of ideal scaling.

The first mounted attempt exposed an over-constrained probe assertion: the
bounded source metric excludes four artifact-envelope reads and is therefore
24,591 operations while the full artifact reader reports 24,595. Correcting
that harness-only distinction produced the one permitted focused retry above;
no production behavior changed.

This stage-local pass admits a matched 16-channel production discriminator. It
does not by itself admit the all-channel, full CLEAN, or 32 GB gates, and CASA
was not rerun.

## Automatic topology-derived replay budget

The admitted budget is now derived by the production planner rather than
injected by the benchmark. For each dataset, the planner computes the exact
two-source-slot plus one-frame route minimum from the observed frame geometry.
It then requests the smaller of the detected CPU data working set and that
minimum multiplied by the useful sector-parallel lane count. The lane count is
bounded by both the detected performance-core count and the four scientific
sector owners, and is deliberately independent of the requested worker count.
Unknown topology fails replay planning instead of selecting a fallback.

On this host, the detected 4 MiB CPU data working set and four performance
cores combined with the mounted dataset's 188,680-byte one-frame minimum to
produce a 754,720-byte request. The exact planner admitted the same 5,402
windows and 754,664-byte actual working set for one and four workers. No test
budget was injected.

Command:
`CARGO_INCREMENTAL=0 CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance cargo test -p casa-imaging-runtime --release --lib weighting::serial_compute_probe::medium_vla_64ch_gridded_replay_automatic_budget_four_worker -- --exact --ignored --nocapture`

| Measurement | Matched serial | Four workers |
| --- | ---: | ---: |
| Median replay | 1.092461875 s | 0.997648791 s |
| Median source fill | 0.395719372 s | 0.406920622 s |
| Median prepare | 0.504115448 s | 0.502593279 s |
| Median execute | 0.542807551 s | 0.447922163 s |
| Median commit | 0.000552389 s | 0.000693210 s |
| Dispatch waves | 21,608 | 5,402 |
| Worker-stack capacity | 0 B | 8,388,608 B |

The computed ceiling was 1.037838781 seconds. Four-worker replay was 8.68
percent faster and passed. All observations retained two source slots,
492,432-byte source capacity, 262,232-byte route capacity, five-frame
high-water, zero payload copies, zero dynamic partial bytes, exact work and
commit identity, and final normal-state identity
`b49653403ac89ba12e2cb3b9e776742c1d6cc792a254e4e46e32a64c025d9077`.
This automatic-budget result admits the matched 16-channel production serial
gate before any four-worker production comparison. It does not admit the
all-channel, full CLEAN, or 32 GB gates, and CASA was not rerun.
