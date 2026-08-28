# Issue #540 matched first-cycle attribution

Truth class: diagnostic observation; intentionally stopped after the second
minor-cycle summary  
Recorded: 2026-08-28  
Work issue: #540

## Outcome

The first demonstrated CASA-relative delta is scientific control, not source
I/O. CASA and Rust enter the first minor cycle from the same normal state to
the shown precision, but Rust's application-supplied 100 Jy cumulative model
update limit ends the cycle long before CASA's `cycleniter=50` limit.

| First-cycle field | CASA retained logger | Rust diagnostic |
| --- | ---: | ---: |
| Initial peak residual | 91.7745 | 91.77448397734118 |
| Cycle threshold | 6.082975387573242 | 6.082970797853019 |
| Iterations executed | 50 | 12 |
| Final peak residual | 51.8045 | 70.00047714983185 |
| Model update / model end | 328.683 | 94.98192256898056 absolute flux |
| Stop | `cycleniter` | `StalenessBound` |

The second Rust cycle repeats the mechanism: it enters at peak
70.00046321176559, executes 15 iterations, reaches 27 cumulative iterations,
updates by 98.91941460745882 Jy absolute flux, and stops at peak
61.493997711993345 with `StalenessBound`. Its cycle threshold is
4.639751215146968. This is sufficient to explain why the retained full Rust
run required 16 later weighted replays while CASA required ten: Rust is
injecting extra major-cycle boundaries that are not in the matched CASA
control.

The diagnostic used the frozen 64-channel, 1024-pixel, Briggs, one-worker,
RustFFT workload with only total `niter` reduced from 500 to 50. It completed
one density pass, one initial weighted replay, and one later weighted replay,
then was intentionally terminated before starting another expensive replay.
The completed later replay visited 188 blocks, 4,094,064 rows and 524,040,192
samples. It reported 10.475698 seconds of source read service,
117.762318 seconds in the existing route/consume timer, 82.694 milliseconds of
source starvation, and 117.845698 seconds wall. I/O is therefore overlapped and
is not the immediate critical-path explanation.

## External later-replay sample

A 15-second, 1-millisecond `sample` captured 12,326 main-thread samples in the
completed later replay. The largest exclusive leaves were:

| Leaf | Samples | Main-thread share |
| --- | ---: | ---: |
| compensated gridding | 2,662 | 21.60% |
| prediction stencil | 1,065 | 8.64% |
| selected-buffer sample access | 1,051 | 8.53% |
| weighting replay sample consumption | 945 | 7.67% |
| spectral-contribution cache compilation | 915 | 7.42% |
| complete-data owner unclassified leaf | 721 | 5.85% |
| selected spectral projection | 655 | 5.31% |
| weight lookup from frozen state | 614 | 4.98% |

These are attribution clues, not an optimization authorization. The
scientifically unnecessary replay inflation must be repaired and remeasured
before multiplying any leaf share across the full run.

The raw density, initial-replay, and later-replay samples are retained beneath
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/issue540-first-cycle/`
with SHA-256 values respectively
`7b6d6b08238261204d5aa869fa1d35649ca64e74e6e5efa9a28e93f2c7197268`,
`fdcc825ea83dde115829dda1813c1990f7475d30a4e30181fa4d7d45f6c4219d`,
and `e14c9fd429c3b08df443e18978cd83cce30a2e0b9eaca7d3f9fd7aa663088e2d`.

## CASA source check

At casacore revision `028ef419181f5dcc103e22058ab2460ec88d98ec`,
`lattices/LatticeMath/LatticeCleaner.tcc` runs from the starting iteration to
`itsMaxNiter`, stopping for threshold, scale/point-mode, or divergence
conditions. It accumulates total flux for reporting but has no cumulative
model-flux limit. For the one-scale Högbom case, a user-supplied 100 Jy update
cap is therefore not a matched CASA control.

The current casa-rs cap was introduced as an explicit validity envelope for a
minor-cycle view. The correction must preserve that architectural concept for
approximated views while representing the exact Högbom view honestly; raising
the benchmark-only constant or adding a compatibility bypass is not an
acceptable fix.

The subsequent typed-view correction and mounted discriminator are recorded in
`20260828-issue540-exact-minor-cycle-view.md`. Intermediate cycle values remain
diagnostic; final CASA correctness is the 0.001 normalized-RMS product gate.
