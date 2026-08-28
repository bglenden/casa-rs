# Issue #540 correlation-run candidate

Truth class: rejected matched discriminator  
Recorded: 2026-08-28  
Work issue: #540

## Decision

Reject the first correlation-run sharing candidate. It did not improve the
matched serial turnaround workload and did not remove ten percent of the
targeted stage. No 32 GiB or full frozen run is authorized from this result.

The candidate derived one spectral interval pair per selected row/channel run,
read correlation-local visibility/flag/weight values without reconstructing a
full row report, and reused one model prediction across correlation members
with identical row/channel geometry and spectral contributions. Correlation
flags, weights, visibility accumulation, canonical order, and bounded storage
remained independent.

## Matched observation

Both observations used `wave3-standard-mfs-single-term-turnaround` against the
directly mounted medium VLA MeasurementSet with one worker, one selected
channel, a 1024-pixel Briggs image, 50 requested Högbom iterations, and the
RustFFT backend. CASA was not rerun. The parent was the isolated source archive
of `1cd80c86f`; build time was excluded.

| Metric | Parent | Candidate | Change |
| --- | ---: | ---: | ---: |
| Wall | 29.217616 s | 30.306484 s | +3.73% |
| Density route/consume | 1.918881 s | 1.779943 s | -7.24% |
| Initial weighted route/consume | 4.635442 s | 4.258358 s | -8.14% |
| Final weighted route/consume | 2.379001 s | 2.479965 s | +4.24% |
| Sum of route/consume stages | 8.933324 s | 8.518266 s | -4.65% |

Both completed exactly three MeasurementSet passes: one density pass, one
initial weighted replay, and one final weighted replay. Every pass reported 42
blocks, 4,094,064 rows, 8,188,128 samples, one worker, two source slots, and
57,297,318 bytes peak live source capacity. The candidate result SHA-256 is
`131cd6f491256ec89ac20caa2b77666d9251cc93c78acfa2cfb26c723fef7738`.

The candidate did not reach the predeclared ten-percent stage discriminator,
even before considering its slower wall time. Source service varied by less
than 1.3 percent between observations, but no timing-noise interpretation can
turn the 4.65-percent targeted-stage result into a pass.

## Correctness contract

Focused selected-observation, reconstruction major/minor-cycle, and continuum
application tests were green. Exact intermediate floating-point agreement is
not an acceptance requirement. Any retained performance candidate must compare
final scientific arrays on declared valid support with normalized RMS no
greater than 0.001. Exact requirements apply only to structural work evidence
such as ordered traversal, pass/block/sample counts, residency, and bounded
execution.

The candidate was rejected on performance before a retained product comparison
was needed. Its code was removed rather than carried as an inactive alternate
path or speculative optimization.
