# Proposal: choose tclean's default Hogbom accounting

Truth class: proposed owner decision
Work issue: #980, after the #991 precision correction.

## Measured behavior

CASA 6.7.6.14 commits one more component than it reports when a positive
Hogbom minor cycle reaches its iteration limit. This is the already
documented CASA niter bug, not a new interpretation of the parameter.
casa-rs's existing `strict` default limits physical updates; its existing
`casa-inclusive` option reproduces CASA's behavior and reported counts.

On the fresh pinned point-source case with niter=10 and gain=0.1:

| Run | Model flux, Jy |
|---|---:|
| CASA | 0.686189592 |
| Native default strict, CPU/f64 | 0.651321411 |
| Native explicit casa-inclusive, CPU/f64 | 0.686189234 |
| Corrected CPU/f32, explicit casa-inclusive | 0.686189234 |
| Corrected GPU/f32, explicit casa-inclusive | 0.686189234 |

Default strict residual NRMS is about 0.1111. Explicit inclusive mode makes
all seven product/inventory/mask/finite/beam/sumwt checks pass against CASA.
On the ALMA mosaic niter=50 case it also removes the common native-f64/f32
clean-product mismatch; all recorded products pass. These results are in
`resources/casa-compat/evidence/tclean-compensated-f32-experiment-20261031.json`.
The exact recipe/source is 75155cd47f77cbfc4c1e1d6822774b7d1516d1ee.

## Two concrete choices

1. **Recommended: match CASA by default.** Change casa-rs's extra
   `hogbom_iteration_mode` binding default from `strict` to `casa-inclusive`.
   Retain explicit `strict`. This uses the already implemented accounting
   mode; it adds no new solver. It changes no CASA parameter/default and
   does not modify CASA. Positive Hogbom cycles may perform one extra
   component update while retaining CASA's reported count.
2. **Retain strict as an agreed departure.** Keep the current default and
   record its affected inputs, scientific product differences and user
   impact as an explicit owner-approved exception. Users requesting CASA
   output parity would select the existing inclusive option.

If choice 1 is approved, the concrete provider change is bundle 30 to 31:
the binding in `crates/casa-provider-contracts/resources/parameter-surfaces.json`
gets the new literal default, its catalog documentation and generated
references/wrappers are updated, and old omitted defaults receive a
migration warning. Explicit old values are preserved. All 92 CASA
parameter defaults and all other native option defaults stay unchanged.
Focused migration/default/continuation tests and the normal contract,
Python, imaging and review gates must pass before publication.

No provider change or accepted-deviation decision has been applied.
AGENTS.md's Ask First rule covers provider-contract changes. This choice
is independent of the f32 correction's memory/performance tradeoff.
