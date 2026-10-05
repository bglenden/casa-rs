# Shared cube/MFS bulk-input refactor

Truth class: engineering evidence, not full T55 acceptance

## Later PSF-workspace checkpoint — 2026-10-04

The constant-basis single-plane application now reuses Clark's immutable PSF
half-spectrum and component buffer with the exact PSF owner across major cycles.
An explicit `ClarkWorkspaceReservation` prices the cross-cycle lifetime; the
per-solve execution envelope excludes those vectors. Controller/active residual
state remains fresh, failed solves do not return dirty scratch, and the shared
batched Clark mathematics is unchanged. This is not a cache per cube channel.
The static minor-phase quote remains conservative; no lower-memory admission
claim is made. There is no content fingerprint or alternate cleaner.

On the larger capped Metal MFS case, a current parent/candidate pair measured
86.940 / 81.101 s with 40 majors each. Input-read drift accounts for 3.865 s;
the supported reconstruction saving is about 2.1 s, not the entire observed
6.7% application difference. Peak RSS stays 6.224 decimal GB. Focused checks
and unchanged seven-product comparisons pass their performance guards; the
known capped-CLEAN numerical alerts remain, not scientific acceptance.
Detailed source, commands, logs and panels are in
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/imaging-ownership-simplification-20261003/mfs-clark-reuse-20261004/REPORT.md`.
The earlier measurements below retain their original workload and boundary.

## Outcome and scope

The final integrated candidate is faster than matching CASA serial and materially
faster with four workers. All affected checks pass. This
does **not** establish the broader two-times worker-scaling target.

The approved change shares existing cube mechanisms with ordinary MFS rather
than introducing another imaging runner or CLEAN controller. The common source
owner, borrowed numeric rows, projected geometry, correlation-weight semantics,
bounded preparation workers and admission machinery are reused. Channel-plane
cube operators and frequency-collapsing MFS operators remain scientifically
distinct; pretending that their numerical operators are identical would be
incorrect. The existing controller, FFTW and CASA-compatible writer remain.

Pre-refactor recovery commit: `6386670c2e6773b6c9a7cd11aabc10dd84ae7500`, local
and unpushed, following integration commit
`097ae64f52752ca17d623cabb11ee074becfa15a`. A verified incremental bundle and
frozen binaries/patches are outside the removable worktree.

Durable evidence:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/mfs-shared-bulk-refactor-20261002/`.
Its `CHECKPOINT.md`, `verify-shared.sh`, stage command/log/resource records and
`run-shared-final.sh` are the reproduction entry points. The single current
programme summary remains
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.

## Measurements

Unchanged DATA-only EVLA A+C MS; SPWs 0,10,21,31; 252,720 selected rows;
four 64-channel windows; 180 timestamps; 4096-square images at 0.05 arcsec;
6-GHz LSRK, standard/uniform/Stokes-I/single-term Clark, gain 0.1,
5-mJy threshold, 10,000 iteration limit and 1,000 cycle limit. Full-plane mask,
flat-noise normalization, no PB correction. Native uses four major passes
(initial plus three refreshes); CASA uses three. All observations finish with
668 components. No full-32-GB or all-32-SPW run was started.

Application time includes input preparation, initial imaging, CLEAN, residual
refresh, intermediate I/O and publication. Guard time also includes process
startup/exit. These are single observations, not controlled-cache statistical
confidence intervals; the serial margin below CASA is narrow.

| Complete application | Before (s) | Final integrated candidate (s) | Elapsed reduction |
| --- | ---: | ---: | ---: |
| CASA serial, unchanged reference | 52.8241 | 52.8241 | — |
| casa-rs CPU W1 | 60.0771 | 51.8033 | 13.8% |
| casa-rs CPU W4 | 56.5028 | 39.6245 | 29.9% |

Final W1 is 1.93% faster than CASA; W4 is 25.0% faster. Native
W1/W4 is 1.31, not two times. Guard wall times are 52.8138/40.2431 seconds.
Sampled aggregate peaks are 4.634/4.637 decimal GB versus baseline
4.579/5.437 GB. W4 peak falls about 14.7%; W1 peak does not fall. A smaller
density-phase buffer is not a claim that the whole-run peak falls equally.

| Exclusive or enclosing stage | Before W1/W4 (s) | Final W1/W4 (s) |
| --- | ---: | ---: |
| Initial density traversal | 8.500 / 8.507 | 1.429 / 1.432 |
| Weighted initial imaging and replay compilation | 21.529 / 19.236 | 20.615 / 10.951 |
| Prepared source-run work, inside previous row | unavailable | 10.032 / 3.231 |
| Ordered numerical/compilation commit, inside weighted row | unavailable | 10.151 / 7.252 |

The earlier complete candidate measured 51.4255/39.0615 seconds and passed its
own exact-output comparisons. Those observations and its source are retained;
the final observations include subsequent prototype deletion and fixture/probe
repairs. This is not a six-pair timing campaign or a statistical significance
claim. The serial margin remains small in both observations.

Final frozen executable: `shared-continuum-application-v2`, SHA256
`eceb7646cd3d758a4ec661a1b0193b40ff3e3edc78425535b9d88fb03d73016c`.
`shared-candidate-v2.patch` reconstructs the tracked numerical source against
the recovery commit; the final report is separately preserved with the checkpoint.
Use fresh stage labels/output directories when reproducing the saved scripts,
not the already-retained labels.

The nested rows must not be added to their parent. Ordered commit includes
scientific gridding and replay compilation, not just bookkeeping. The W4
preparation team reports four actual concurrent jobs. This is the useful Obit
lesson: parallelize preparation over bounded shared UV input, not create four
independent readers or multiply full grids. Obit's published speedup is not a
matching-workload guarantee.

Each initial density/weighted pass still reads 610,824,240 logical/modelled
physical bytes in 13,680 operations and 720 source blocks; 38 storage buffers
are allocated and reused 13,642 times. Density's rich selected-sample handoff
traffic falls to zero. Weighted preparation still reports 15,749,510,400
cumulative handoff bytes; that is traffic, **not** live memory. It prepares
64,696,320 correlation-channel samples in 16,174,080 indexed runs, with about
2.3 MB planned preparation workspace, not a resident copy of the entire MS.

The retained normal replay artifact is unchanged: 12,176,544 records,
487,168,920 artifact bytes. Each later refresh reads 486,841,152 bytes in
4,461 operations with zero payload copies, rather than rereading the MS.
Source pass count and record inventory have not been reduced or hidden.

## Deleted machinery and migration

- Removed six-limb exact density cells, exponent-bin exact weight sums and
  signed mosaic exponent trees, their capacity formulas and conversion pass.
  Ordinary finite `f64` accumulation replaces optional bitwise reproducibility;
  invalid values and overflow still fail explicitly. The 4096-square density
  grid now owns 128 MiB instead of 768 MiB for six limbs, and transfers that
  buffer without an extra density-grid copy. Required mosaic compensation
  buffers were not blindly removed from other algorithms.
- Removed zero-byte auxiliary physical allocations and associated claims;
  there is no dummy one-byte replacement. Lifetimes and positive allocations
  remain subject to resource admission.
- Removed the unused MFS prototype inside the cube executor, both
  `consume_bulk_mfs` wrappers and two `bulk_mfs.rs` modules. The cube constructor
  rejects unsupported MFS before planning. MFS uses its existing application
  seam; the live ordinary-MFS support predicate is retained next to its operator.
  These APIs are deleted, not deprecated or retained as a fallback.
- Shared numeric geometry is allocated once and reused. Its bound comes from
  admitted source bytes, selected rows/channels/correlations and the minimum
  stored sample layout, not a fixture-specific row constant. Geometry work
  partitions actual block rows across admitted workers; using the maximum
  admitted row capacity had previously left ordinary blocks underpartitioned.
- The existing source-run preparation mechanism now serves ordinary MFS in
  both W1 and W4. Worker-specific alternative identities prevent duplicate
  planner alternatives. No new CLEAN controller, observation iterator or
  compatibility route was introduced.

Modes needing transforms, polarization/rotating beams, other spectral bases or
different density scopes retain their scientific treatment. Shape, inventory,
flags/masks, frequency/coordinate interpretation, ownership, I/O propagation,
bounded residency and publication contracts were not relaxed.

## Verification and remaining work

Final W1 and W4 each pass the unchanged seven-product comparison:
all pixels finite, shapes/units/masks/WCS/peak positions agree, PSF peaks are
one, and maximum normalized pixel discrepancy is 1.691e-6,
well below 1e-3. Both panels were inspected. PB/mask/sumwt are identical.
Exact image/PSF beam metadata retains the baseline rounding mismatch; existing
beam area/kernel metrics are 6.36e-8/5.91e-8. The mismatch is preserved, not
hidden by editing the comparator or declaring full programme acceptance.

All 99 affected checks pass: 37 weighting contracts, four density reductions,
ten mosaic reductions, 28 cube kernels, one MFS-region kernel, seven MFS runtime
contracts, two admission cases, one numeric MS consumer, one connected MFS
application, seven CPU cube applications and one actual-device Metal cube
application. The Metal test was run explicitly, not counted from its ignored
entry in the CPU suite. Formatting, diff and documentation checks pass.

Two independent bounded code-review passes found no actionable blocker.
Diagnostic-probe compilation failed on two stale internal call sites; those
test-only calls were migrated and the original log retained. The smaller density
allocation made the old three-plane fixture cap admit four planes. That
test-only cap was adjusted from 11 MiB + 640 KiB to 11 MiB; unchanged assertions
now exercise depths 1, 2, 3 and 4 and compare all science products. The first
assertion failure poisoned the suite lock; its four subsequent lock errors were
not independent scientific failures. Earlier compile/admission failures and the
first candidate source/results are also retained; unchanged failed commands
were not rerun.

Remaining performance cost is measurable: the W4 initial route still spends
7.252 seconds in ordered numerical/compilation work, about 6.72 seconds in
initial FFTs, and about 8.05 seconds in the one-plane Clark minor cycles.
Residual execution is already only about 2.94 seconds over three refreshes.
At the current serial time, two-times scaling would require W4 at 25.90 seconds,
another 13.72 seconds below the retained result; it is not established here.
Reducing source I/O alone cannot eliminate this gap. The compiler builds
prediction and accumulation standard stencils separately; reuse for the
single-contribution ordinary case is a concrete possible next discriminator,
not a measured win or permission to fuse different scientific contributions.
Do not start a larger workload, MFS Metal, or a new optimization campaign just
because this possibility exists.

Retain 16-GiB native planning/sampled aggregate RSS, 8-GiB build/CASA checks,
two Cargo jobs and the existing FFTW build. No strict time cap was reinstated.
Full `just verify`, all-mode migration/full-scale MFS performance and full T55
acceptance are not claimed by this bounded performance checkpoint. Outstanding
cube numerical-review alerts remain unapproved and unrelated.
