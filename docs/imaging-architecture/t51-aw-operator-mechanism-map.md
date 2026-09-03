# T51 paired AW operator mechanism map

Truth class: implementation reconnaissance and campaign record  
Work issue: #537  
Parent revision: `a206609e5fd5ce1b984d2f4fea58d6359ae0ef63`  
Historical revision: `fff9c2d553eace4b6a57b1df9ded4773f2263ceb`

## Old-to-current mechanism map

| Proven mechanism | Historical/upstream evidence | Current owner | T51 disposition |
|---|---|---|---|
| One paired imaging/weight CF key over frequency, W, Mueller/polarization, parallactic angle, antenna/PB identity, conjugate beam, support, sampling, UV coordinates, precision, and normalization | Historical `casa-imaging::AwConvolutionFunctionCache`; CASA `CFStore2`, `CFBuffer`, and `VB2CFBMap` | `casa-imaging-model::prepared_artifact` science identity plus `casa-imaging-runtime::PreparedArtifactDescriptor` | Adapt into one validated CASA-import/private-cache/operator boundary; imaging and weight roles remain distinct and asymmetric. |
| One CF selection drives prediction and adjoint gridding | Historical `AwProjector`; CASA `AWProjectFT::findConvFunction`, `put`, and `get` | `casa-imaging-reconstruction` paired measurement and convolution operators | Retain as one AW variant with conjugate-transposed degridding; never compose an A side path with the W operator or alias AW to W-only. |
| Fused sample placement, oversampled taps, discrete complex normalization, and W-sign conjugation | Historical `AwProjector::sample_plan`, packed/literal tap compilation; CASA/LibRA `AWVisResampler` | `casa-imaging-reconstruction::spectral_operator` and compact gridded-normal records | Adapt the mathematical mechanism to the current compact record owner and reject missing/nonfinite/out-of-grid/unsupported cells specifically. |
| Row-local feed/parallactic response and pointing phase gradient | CASA `PolOuterProduct`, `PointingOffsets`, `VB2CFBMap`; casacore `MSPointingColumns` and `ParAngleMachine` | selected-observation row coordinates plus reconstruction polarization and spectral operators | Consume the already evaluated row PA/pointing values. Apply the pointing phase in both directions; do not duplicate a pointing-specific A-term cache. |
| Wideband spectral and conjugate-beam lookup | CASA/LibRA `makeFreqValList`, `makeWValList`, and `CFBuffer::initMaps`; conjugate frequency is `sqrt(2 f_ref^2 - f^2)` | compiled spectral sampling plus prepared-CF catalog | Resolve the nearest validated direct/conjugate cells from exact output-frame frequency and W coordinates; unsupported coverage fails typed. |
| Weight CF supplies sensitivity/PB normalization through the same sample selection | Historical paired `CFS_`/`WTCFS_` cells; CASA `AWProjectWBFT` average-PB accumulation | reconstruction Normal State and products normalization boundary | Use the paired weight plane for normal-state weight/PB accumulation. Frontends do no normalization calculation. |
| Bounded shared CF residency and source-major reuse | Historical resident LRU and source-major/AOT tap deduplication; LibRA `MakeCFArray` and bounded visibility buckets | runtime prepared-artifact store plus reconstruction compact replay | Share immutable cells and compact taps across workers, account reads/residency/passes/copies, and retain deterministic source order. Never allocate a full grid per worker. |
| Cold import and warm reuse converge before numerical work | CASA dry-grid/fill/reload cache sequence; T50 plan-listed load/reuse operations | runtime prepared-artifact store | Cold CASA import and warm private reuse produce the same typed prepared cell/catalog consumed by the same AW operator. Cache disposition is receipt evidence, not operator dispatch. |

The displaced `casa-imaging` crate, whole-run AW pipeline, mmap/packed experimental
formats, environment-selected fast paths, and mode-specific worker architecture
remain deleted. LibRA's batching and CF packing are retained as techniques, but
its environment-controlled process/bucket policy is rejected. CASA's known
diagonal-dominance shortcuts are not generalized: unsupported Mueller layouts,
telescopes, disabled required terms, or absent CF coverage fail specifically.

## Campaign record

Single candidate: current-owner paired AW consumption of validated CASA-imported
prepared CF cells.

- Parent/candidate: `a206609e5fd5ce1b984d2f4fea58d6359ae0ef63` to
  `codex/t51-aw-projection`.
- Causal hypothesis: replacing the typed-unavailable boundary with one
  prepared-cell-backed paired convolution operator recovers the frozen EVLA
  A/W/pointing response without reviving the displaced runner or changing the
  generic scheduler.
- Discriminators: prepared-cell identity and corrupt/mismatch rejection; cold and
  warm operator identity equality; weighted adjoint law; first divergent grid,
  degrid, weight/PB, and product metric; deterministic serial replay digest.
- Scientific ceiling: every ticket-relevant frozen CASA product has normalized
  RMS at most `1e-3`, with exact topology, WCS, validity/support, mask, and
  independently required beam/flux/centroid checks.
- Resource ceiling: planned and observed peak below 32 GiB; no full-MS
  materialization, full grid per worker, sustained swap growth, or opaque stage
  longer than three minutes. Serial CPU must pass independently.
- Turnaround limit: run a mode-faithful bounded discriminator before any run
  projected beyond 30 minutes. Runs beyond 90 minutes are final evidence only.
- Automatic falsifiers: unequal cold/warm operator identity, adjoint failure,
  missing/extra prepared cell, wrong CF role, nonfinite normalization, serial
  scientific failure, or resource receipt above the ceiling.
- Fallback: none. A falsified candidate remains typed unavailable while its
  evidence is retained; W projection remains a distinct explicit capability.
- Artifact retention: frozen CASA products and accepted comparison/receipt
  bundles are durable; reduced diagnostics and failed local cache entries are
  rebuildable campaign artifacts.

