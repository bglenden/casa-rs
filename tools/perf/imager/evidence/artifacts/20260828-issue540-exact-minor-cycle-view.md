# Issue #540 exact minor-cycle view discriminator

Truth class: diagnostic observation from a dirty implementation worktree  
Recorded: 2026-08-28  
Work issue: #540

## Outcome

The application-supplied 100 Jy cumulative component-flux cap was not a CASA
control and prematurely requested major-cycle reconciliation. The candidate
removes that scalar from the frontend, application, generic reconstruction
controls, task contract, benchmark harness, and solver crosscheck. Minor-cycle
view validity now belongs to reconstruction as `Exact` or as an explicitly
owner-proven `Bounded` envelope. Every currently implemented Högbom, Clark, and
multiscale view is exact; no numeric bound is invented.

The focused mounted discriminator used the full 64-channel, 1024-pixel,
Briggs, one-worker, RustFFT workload with only `niter` reduced to 50. CASA was
not rerun. Rust completed in 515.065332 seconds with exactly one density pass,
one initial weighted replay, and one final weighted replay. Every pass visited
188 blocks, 4,094,064 rows, and 524,040,192 samples while retaining two source
slots and approximately 62.3 MB peak live source storage.

The first-cycle diagnostics were:

| Field | Retained CASA log | Rust candidate |
| --- | ---: | ---: |
| Initial normalized peak | 91.7745 | 91.77448397734118 |
| Effective cycle threshold | 6.082975387573242 | 6.082970797853019 |
| Final normalized peak | 51.8045 | 51.804453927692975 |
| Reported model flux / absolute update | 328.683 | 328.68262094877775 |
| Stop | `cycleniter` | `IterationBound` |

Rust records 51 accepted updates for the workload's existing CASA-inclusive
accounting of requested `niter=50`; CASA labels the corresponding cycle count
as 50. The candidate did not emit `StalenessBound`.

These intermediate values are diagnostic only. CASA retained them at limited
printed precision, so the extra Rust digits do not establish or require exact
agreement. They show that the unmatched stopping control no longer changes the
first-cycle trajectory. Final scientific pass/fail remains the retained CASA
product comparison with a normalized-RMS ceiling of 0.001 on declared valid
support. Component order, intermediate residuals, and internal flux totals are
not CASA-parity contracts.

## Stage evidence

| Pass | Wall seconds | Source read seconds | Source-starved seconds |
| --- | ---: | ---: | ---: |
| Density | 76.833178 | 10.358526 | 0.076413 |
| Initial weighted replay | 201.011133 | 10.688993 | 0.077331 |
| Final weighted replay | 234.151244 | 12.871736 | 0.072025 |

The negligible source-starved time rejects read-ahead as the immediate
critical-path explanation. The final replay is more expensive because it also
forms terminal visibility products.

## Provenance

- Run id: `20260828T224434Z-wave3-standard-mfs-single-term-heavy-wave2-serial-d4d588ff`
- HEAD: `9a1d03ce0baf88216bea25cb68683fee84d077a3` with the candidate dirty diff
- Result: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/issue540-exact-view-first-cycle/results/20260828T224434Z-wave3-standard-mfs-single-term-heavy-wave2-serial-d4d588ff.json`
- Log: same basename with `.log`
- Log SHA-256: `0cebb38427f1280480cb2c15efe5db84d54dbbd1e3aa8ef6319296620ee5b4ef`
- Products: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/issue540-exact-view-first-cycle/products/20260828T224434Z-wave3-standard-mfs-single-term-heavy-wave2-serial-d4d588ff`

The wrapper status is `failed_comparison` only because the harness attempted a
sampled comparison against absent CASA outputs even though `skip_casa=1`.
Rust imaging and product publication completed; this run does not itself claim
final CASA product parity.
