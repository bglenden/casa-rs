# ADR-0011: Distinct sequential and joint continuum-line reconstruction

Status: accepted
Date: 2026-08-31
Truth class: normative
Supersedes:
Superseded by:

## Context

CASA-RS already supports CASA-compatible visibility-domain continuum fitting
and subtraction followed by channel-local line reconstruction. That sequential
transform is not a joint reconstruction: it fits each row and correlation,
preserves the input sample weights, and does not represent covariance induced by
the fit.

The programme also calls for a true continuum-plus-line reconstruction. An
unrestricted channel-local line basis contains the smooth continuum subspace,
so merely placing Taylor and channel-local coefficients in one vector creates a
singular, scientifically ambiguous decomposition. CASA supplies oracles for
sequential subtraction, line cubes, and MT-MFS Taylor coupling, but source
inspection found no same-field, same-pixel additive continuum-plus-line solver.

Brian approved the decision below on issue #531 after the mathematical,
product, error, CASA, and synthetic-evidence contracts were proposed.

The CASA source study used checkout
`61020062cee290f5466cffed5ec5032e0c7a3434`. Its relevant paths are
`casatasks/src/private/task_uvcontsub.py` and
`casatools/src/code/mstransform/TVI/UVContSubTVI.cc` for sequential
visibility subtraction; `casatools/src/tools/image/image_cmpt.cc` and
`casatools/src/code/imageanalysis/ImageAnalysis/ImageProfileFitter.cc` for
post-imaging subtraction; `SynthesisImagerVi2.cc`, `SIMapperCollection.cc`, and
`SIMapper.cc` for shared-residual multi-mapper execution; and
`MultiTermMatrixCleaner.cc` for Taylor-only block coupling. The 2026 NRAO
Synthesis Imaging Workshop spectral-line material independently describes
`uvcontsub`, `imcontsub`, and `uvsub` as subtraction workflows, not a joint
same-support inverse problem.

## Decision

CASA-RS supports two distinct spectral-coupling workflows:

1. **Sequential continuum transform.** The existing visibility-domain fit and
   subtraction remains a complete native workflow composed before independent
   channel-local line reconstruction.
2. **Joint continuum-line reconstruction.** A separate capability reconstructs
   smooth continuum coefficients and channel-local line coefficients together
   under an identifiable coupled contract.

Neither workflow may silently degrade into the other. Joint reconstruction is
not image-domain subtraction, two independent imaging runs, a shared restoring
beam, CASA mixed-mapper overlap ownership, or MT-MFS Taylor coupling relabelled
as continuum-line coupling.

### Coupled measurement and normal operators

Let `c` belong to the declared continuum coefficient space, `l` belong to the
channel-local line coefficient space, and `d` be the selected unweighted
visibility data. The joint measurement equation is

```text
d = A_c c + A_l l + n,             A = [ A_c  A_l ].
```

For one frozen data metric `W`, the authoritative right-hand side, residual,
and normal operator are

```text
b = [ A_c* W d ]
    [ A_l* W d ],

g(c,l) = [ A_c* W (d - A_c c - A_l l) ]
         [ A_l* W (d - A_c c - A_l l) ],

H = [ A_c* W A_c   A_c* W A_l ]
    [ A_l* W A_c   A_l* W A_l ].
```

Both off-diagonal blocks are mandatory and are adjoints under the declared
inner products. The same globally frozen `W` owns flags, input weights, taper,
and uniform or Briggs density weighting for all four blocks. No continuum or
line owner may independently reinterpret or renormalize it.

### Identifiability, masks, and regularization

Joint reconstruction requires a declared nonempty Continuum Anchor Set on which
the line coefficients are structurally zero. Continuum contributes across the
whole selected spectral domain; line coefficients exist only on declared line
support. The Continuum Anchor Set and line support are scientific commitments,
not solver hints.

Continuum and line spatial masks are independent and are bound with the
spectral support into one immutable coupled reconstruction-mask generation. A
shared mask is allowed only when explicitly compiled as the same two
commitments; it is not an implicit default.

The anchor samples must provide enough positively weighted, spectrally
independent support for the continuum basis. Every active coupled solve must
satisfy the Numerics Contract's rank and conditioning limits. Compilation or
reconstruction fails closed when the support is empty, contradictory,
underdetermined, nonfinite, or numerically dependent.

The first joint capability introduces no tunable smoothness penalty and no
covariance-blind soft prior. Its new regularization is the explicit hard anchor,
line-support, and mask constraint. It may reuse already accepted continuum and
line solver priors. A future soft spectral, spatial, positivity, or statistical
prior is a new scientific contract rather than a compatible implementation
detail.

### Cycles and authoritative generations

One joint Minor Cycle consumes one coupled view, makes one spatial/scale choice
under one validity envelope, and returns one atomic Model Delta that may update
continuum and line coefficients. Separate stopping decisions or independently
committed component updates do not satisfy this contract.

Every Major Cycle predicts the sum `A_c c + A_l l`, forms the one common
complete-data residual, and retains all four normal blocks. Final complete-data
reconciliation remains mandatory.

One coupled Model Generation owns both coefficient families. The final Normal
State Generation binds that model, the Selected Observation generation, frozen
Weighting Generation, coupled mask generation, all four normal blocks, shared
residual, Numerics Contract, and provenance. A continuum or line projection is
not an independently authoritative model generation.

### Products and error propagation

One planned and sealed Product Generation publishes projections of the same
coupled state:

- continuum coefficients and derived continuum products;
- the channel-local line model and restored line cube;
- the total model evaluated on the output spectral sampling;
- the one common residual from the final Normal State Generation; and
- masks, weights, PSFs, sensitivity/PB, beams, validity, and PB-corrected
  members required by the selected Product Contract.

A component-only residual is not authoritative. A restored total is legal only
when the Product Contract gives the component restorations a compatible beam
and normalization; it is never formed by adding products from independent
runs.

Joint parameter-error or covariance products may be published only from a
declared approximation to the full active block inverse, including the
continuum-line cross covariance. An MT-MFS-only covariance submatrix or CASA's
empirical spectral-index error arithmetic may not be relabelled as a joint
error. The first joint contract explicitly omits such products when that full
error contract is unavailable; omission never means zero uncertainty.

Sequential products continue to name their Continuum Transform Generation.
Their row-fit coefficients and chi-square evidence are transform provenance,
not an authoritative continuum image model, and the unchanged output weights
do not claim propagated fit covariance.

### Ownership seam

- `casa-imaging-model` owns the distinct coupling commitments, composite model
  space, masks, product requirements, and identities.
- `casa-imaging-reconstruction` owns the paired operators, four normal blocks,
  coupled Model Delta, stopping, and deterministic reduction.
- `casa-imaging-runtime` owns only bounded execution, resources, buffers,
  scheduling, and measurements.
- `casa-imaging-products` extends its single product-generation authority for
  coupled sources and members.
- applications compose the owners, and frontends project parameters and units.

There is one production execution path. This decision adds no mode runner,
compatibility fallback, alternate fast path, MeasurementSet materialization,
CASA-visible persisted format, or frontend scientific calculation.

## Acceptance

Semantics that CASA implements use exact small source-backed reference cases:

- sequential fitting/subtraction retains
  `continuum_transform_matches_casa_minmax_basis`,
  `continuum_transform_reduces_order_like_casa`, and
  `continuum_transform_preserves_flags_weights_and_application_roles`. T46
  adds pinned case `t46-sequential-uvcontsub-anchor-v1`: one field, one SPW,
  eight channels, a complex linear continuum, an injected line in channels
  3-4, fit channels `0~2;5~7`, fit order one, and all-channel application. CASA
  and Rust compare fit roles, coefficients, residual visibilities, unchanged
  flags/weights, and downstream line observables;
- the `l = 0` reduction uses `just imaging-t42-mtmfs-casa` with
  `tools/perf/imager/evidence/artifacts/20260831-issue528-t42-casa-mtmfs-two-spw-oracle.json`,
  followed by the T43 clean and T44 publication comparators for that same
  frozen small multi-SPW source; and
- the `c = 0` reduction uses
  `t37_casacore_cube_dirty_psf_sum_weight_and_normal_state_match` and
  `t38_casacore_minor_cycle_and_paired_final_residual_are_split_oracles`, plus
  their `just imaging-t37-cube-operator` and `just imaging-t38-cube-clean`
  gates.

CASA has no oracle for the novel same-support joint cross terms. The mixed case
therefore requires pinned synthetic recovery and law evidence covering all four
normal blocks, adjointness, partition and worker invariance, cross-talk,
rank/conditioning rejection, masks, coupled stopping, the common residual, and
authoritative product lineage. Evidence must state that this is synthetic law
and recovery validation rather than CASA parity.

## Consequences

Positive:

- sequential subtraction remains stable and honestly described while joint
  reconstruction gains a distinct scientific identity;
- the anchor constraint makes the first joint decomposition identifiable
  without hiding a soft prior; and
- cross terms, generations, residuals, products, and errors have one
  authoritative coupled interpretation.

Negative:

- users must declare scientifically defensible continuum anchors and separate
  component support;
- datasets without adequate line-free support cannot use the first joint
  capability; and
- CASA cannot directly validate the novel mixed solve, while full
  continuum-line covariance is required before publishing joint error products.

Neutral / tradeoffs:

- hard support is deliberately less flexible than a learned or soft spectral
  prior, but is inspectable and testable;
- coupled normal state and reduction require more bounded work than either
  degenerate operator alone; and
- the implementation may omit optional error members rather than publish a
  covariance-blind approximation.

## Alternatives considered

1. Keep only sequential visibility-domain subtraction. Rejected because it
   cannot represent direction-dependent continuum structure and line emission
   in one coupled inference, and would make T46 provide no new capability.
2. Permit an unrestricted line basis and rely on CLEAN choices to separate the
   components. Rejected because the continuum subspace is contained in that
   basis, making the decomposition non-identifiable.
3. Fit in the image domain after constructing a cube. Retained as an analysis
   operation, not a reconstruction contract.
4. Run MT-MFS and cube reconstruction separately and combine their final
   images. Rejected because it omits both cross-normal blocks, has no common
   residual or stopping decision, and cannot produce authoritative covariance.
5. Copy CASA's mixed-mapper mechanism. Rejected because CASA prevents
   overlapping image models from representing the same sky support; it is
   multi-field ownership, not additive continuum-line decomposition.

## Enforcement

This decision is enforced by:

- tests: T40 sequential-transform laws, T37-T38 line-only CASA/casacore gates,
  T42-T44 continuum-only frozen-CASA gates, and T46 mixed synthetic block,
  recovery, identity, and product-lineage laws;
- lint/import/dependency rules: model, reconstruction, runtime, products,
  application, and frontend ownership follows ADR-0009 with no second runner or
  frontend scientific implementation;
- CI checks: `just docs-check`, `just arch-check`, and the migration-matrix
  ratchet keep joint reconstruction unavailable until T46 transfers the exact
  accepted capability;
- review trigger: stop before admitting line coefficients on anchor channels,
  adding a soft prior, publishing a joint error without cross covariance,
  changing component authority, or adding an alternate execution path; and
- none / guidance only:

## Drift detection

Suspect drift if:

- sequential and joint requests share an ambiguous coupling value or silently
  route into one another;
- either off-diagonal normal block is absent or assembled with a different
  weighting generation;
- line coefficients are admitted on anchor channels without a newly accepted
  identifiability contract;
- continuum and line updates, stopping decisions, or model generations commit
  independently;
- a component residual is presented as the common final residual;
- an error product ignores continuum-line cross covariance; or
- joint products are assembled from separately restored runs.
