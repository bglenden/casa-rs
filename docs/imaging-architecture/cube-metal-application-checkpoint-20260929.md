# Complete Metal cube application checkpoint — 2026-09-29

Truth class: non-normative measured evidence
Last reality check: 2026-09-29
Verification: guarded application observations, unchanged CASA comparisons,
actual-device tests and reviewed panels; numerical disposition remains open.

## Working implementation is not a performance success

The explicitly selected natural, standard, channel-local Stokes-I cube path
now connects tiled input, initial imaging, the existing Clark controller,
repeated nonzero-model visibility residual refresh and the existing writer.
Only spatial grid/degrid moves to Metal. NEON FFTW, batched Clark and shared
scientific/runtime machinery remain. Unsupported modes fail explicitly;
there is no automatic CPU fallback, second controller or visibility replay store.
Rust uses objc2-metal directly; no Objective-C source or Swift bridge was added.

Representative observations: 4,094,064 rows, 512 stored channels, output
240–271 (32 planes), 1024², natural Clark, unchanged mask, 0.5-mJy threshold,
640,000 global iteration ceiling. Time includes preparation, all refreshes,
intermediate I/O and publication, plus first-use setup. No build/comparison
concurrency, controlled cold-cache claim or statistically significant claim.

| Deep32 implementation | Application seconds | Sampled aggregate peak GiB |
| --- | ---: | ---: |
| CPU W1, before preservation correction | 629.3643 | 2.1132 |
| CPU W4 | 270.1336 | 2.1427 |
| Metal, four host preparation workers | 345.0674 | 3.3139 |
| Fresh pre-Metal CPU W1 control | 570.9800 | 2.0943 |
| Rejected CPU inlining correction | 634.7065 | 2.1058 |
| Borrowed-row CPU W1 preservation correction | 566.4413 | 2.1149 |
| Borrowed-row CPU W4 check | 279.2921 | 2.1615 |
| Matched CASA deep32 time | unavailable | unavailable |

The CASA products are an exact window of the existing full512 reference;
extraction time and divided full512 wall time are not matched CASA timings.
The fresh CPU control exposes a 10.23% integration regression. Compiled code
shows newly out-of-line row access/cursor setup, but removing those calls in
one bounded correction does not help end-to-end (634.7065 versus 629.3643 s).
That correction is removed; its exact binary/source, commands and logs remain.
It converged in 17 passes/187,702 iterations; rejected output was not submitted
as scientific acceptance. The subsequent borrowed-row/window correction binds
selection once per block, narrows slices in place and lets the cursor borrow
the row view. Its compiled loop removes the 424-byte per-row cursor memcpy.
Deep W1 completes in 566.4413 s, 17 passes/187,690 iterations: it recovers the
10.0% regression and restores parity with the fresh pre-Metal control. The 0.8%
difference from that control is not a statistical gain claim. Do not inflate
scaling by using the regressed serial baseline. The exact frozen executable and
patch are `cpu-borrowed-row-application-v1` in the durable evidence directory.

Metal is 27.7% slower than CPU W4. Input/imaging accounts for the entire gap;
minor cycles take approximately 23 seconds for each. Metal issues 159,456
batches. Its 143.435-s GPU total nests inside 184.863-s submit/wait, not another
exclusive stage. Executed request counts imply 158.784 GB cumulative tap
staging and 34.063 GB prediction copyback, not peak allocations or PCIe traffic.
Both W4 observations have restricted channel reads, reusable source buffers,
zero copied visibility samples and the same 17 source traversals.

The 2-GiB-planning Metal dirty check passes three waves of 14/13/5 planes,
with source windows 39/14/6 channels, peak RSS 1.164 GiB and all six dirty
products/eight CPU checks passing. This is not deep multi-wave performance.

All 32 deep planes have zero hard scientific failures against the unchanged
CASA assessment; restored planes stay below 1e-3. Exact outputs still require
owner review: 15 CPU alerts and 16 Metal alerts, including added channel 257.
Raw model/residual RMS are diagnostic-only under the existing unchanged gate.
First/middle/last and all alerted Metal panels were inspected. No alert was
waived and no full T55/T57 acceptance or performance-goal completion is claimed.

The corrected CPU W1 also has zero hard failures; maximum restored-plane RMS
9.476e-4. It has 16 review alerts, adding channel 257's residual-maximum ratio
0.5041 against the unchanged 0.5 review threshold (previous CPU W1: 0.4962).
All alerted panels were inspected; the added alert is not automatically waived.
The raw seven-product/nine-check report remains out_of_tolerance for the existing
model/residual diagnostics, not a wholly green numerical report.
Corrected W4 also has zero hard failures and 16 review alerts. Its 279.2921-s
deep observation is 3.4% slower than the earlier integrated W4: deep W4
preservation is not established. One short matched W4 control/repair check
(35.6176/31.7732 s, two passes/958 iterations) favors the repair by 10.8%, but
does not settle the deep result. Current observed W1/W4 is 2.028×, not the ratio
against the regressed W1. These are single observations, not significance claims.

## Restart and next decision

CPU hot-loop preservation now has a complete deep serial result and a bounded
four-worker check, with the deeper W4 uncertainty recorded. Numerical-consumer
work accounts for 58.111 of the 58.384-s fresh parent/new W1 gap; preparation
and Clark are essentially unchanged. The failed inlining trial rules out those
two call boundaries alone as a sufficient repair, not every compiler/data-layout
effect of the shared backend seam. The retained correction removes repeated
view reconstruction and aggregate ownership transfers while keeping scientific
primitives shared and backend selection at the wave boundary. Focused cube tests
and the actual-device initial/CLEAN/two-refresh/publication application pass.
No further CPU candidate or Metal optimization is started by this checkpoint.
If deep W4 preservation needs a conclusive decision, the specific missing datum
is a fresh matched deep W4 control, not another broad inventory or speculative
candidate. Metal performance after the shared-view correction is unmeasured.

The retained CPU observation is faster than the earlier Metal W4 observation;
this is not a new matched Metal timing after the view repair. A later useful
Metal effort must address
spatial batching/dataflow, not blanket FFT replacement or small launch tweaks.
Eliminating all non-GPU submit/wait overhead would still leave Metal slower;
even halving GPU time alone would not clearly beat the CPU observation.
Before a new redesign, grid-versus-degrid GPU time on one representative wave
is the specific missing discriminator. This checkpoint does not start it.

Durable [detailed report and reproduction handles](/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929/METAL-CUBE-APPLICATION-CHECKPOINT.md)
include exact commands, binaries/source snapshots, resource receipts, rejected
results, comparisons, panels and remaining checks. The only CURRENT summary is
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.
The subsequent [CPU profile and preservation report](/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929/CPU-ROW-PROFILE-REPAIR.md)
preserves the rejected tuple-layout attempt, residual-wave profiles, assembly
evidence, corrected timings and exact numerical disposition.

Final corrected-source checks pass: 299 reconstruction and 341 runtime release
tests (19/10 ignored), focused cube shape/cursor checks, actual-device complete
application, formatting and docs-check. The directly affected contract tests
remain intact. These checks do not dispose of the 16 numerical review alerts or
establish deep W4 preservation.

Legacy architecture checks fail unchanged HEAD manifest/T18 assertions; do not
weaken them. Numerical disposition, wider programme acceptance and any future
full-size Metal validation remain open. Preserve 16-GiB native planning/RSS,
8-GiB other-work guards and two Cargo jobs. No push, merge, release, cleanup,
full512 restart, MFS expansion or local W8 tuning is authorized by this record.
