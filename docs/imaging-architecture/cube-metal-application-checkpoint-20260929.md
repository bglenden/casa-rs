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
| Matched CASA deep32 time | unavailable | unavailable |

The CASA products are an exact window of the existing full512 reference;
extraction time and divided full512 wall time are not matched CASA timings.
The fresh CPU control exposes a 10.23% integration regression. Compiled code
shows newly out-of-line row access/cursor setup, but removing those calls in
one bounded correction does not help end-to-end (634.7065 versus 629.3643 s).
That correction is removed; its exact binary/source, commands and logs remain.
It converged in 17 passes/187,702 iterations; rejected output was not submitted
as scientific acceptance. CPU preservation remains unresolved. Do not inflate
scaling by using a regressed serial baseline.

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

## Restart and next decision

The next priority is **CPU hot-loop preservation**, before another Metal
optimization campaign or broader use of this integration. Numerical-consumer
work accounts for 58.111 of the 58.384-s fresh parent/new W1 gap; preparation
and Clark are essentially unchanged. The failed inlining trial rules out those
two call boundaries alone as a sufficient repair, not every compiler/data-layout
effect of the shared backend seam. Restore the original CPU loop/data layout
while keeping scientific primitives shared and backend selection at the wave
boundary; qualify any such change against the frozen pre-Metal control.
This checkpoint does not implement another candidate.

CPU is still faster than Metal W4. A later useful Metal effort must address
spatial batching/dataflow, not blanket FFT replacement or small launch tweaks.
Eliminating all non-GPU submit/wait overhead would still leave Metal slower;
even halving GPU time alone would not clearly beat the CPU observation.
Before a new redesign, grid-versus-degrid GPU time on one representative wave
is the specific missing discriminator. This checkpoint does not start it.

Durable [detailed report and reproduction handles](/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929/METAL-CUBE-APPLICATION-CHECKPOINT.md)
include exact commands, binaries/source snapshots, resource receipts, rejected
results, comparisons, panels and remaining checks. The only CURRENT summary is
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.

Legacy architecture checks fail unchanged HEAD manifest/T18 assertions; do not
weaken them. Numerical disposition, wider programme acceptance and any future
full-size Metal validation remain open. Preserve 16-GiB native planning/RSS,
8-GiB other-work guards and two Cargo jobs. No push, merge, release, cleanup,
full512 restart, MFS expansion or local W8 tuning is authorized by this record.
