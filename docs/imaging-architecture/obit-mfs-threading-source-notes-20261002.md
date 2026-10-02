# Obit MFS threading: source findings and casa-rs implications

Truth class: non-normative primary-source implementation research
Last reality check: 2026-10-02
Verification: revision-pinned local source inspection; official Obit memos;
no new Obit timing or implementation trial

## Conclusion

Obit's transferable mechanism is a shared flat UV buffer, substantial worker
chunks that include numerical preparation and gridding/model prediction, private
accumulation grids, and a final reduction. Parallelism is not restricted to
independent output planes. MFImage also exploits facets and coarse subband
images, but its joint subband CLEAN is not a drop-in implementation of our
single-term CASA MFS algorithm.

The historical MFImage threading experiment achieved 3.00x at four threads and
5.03x at eight. It used a different workload, machine and scientific workflow;
it is evidence that this style can scale, not a performance prediction for
our workstation or an equal-output comparison.

## Primary sources and provenance

Inspected the local official Obit checkout at
`ebc1c229e5e3870b5ce3c342bddb7313d986a06f`:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/obit-source-study-20260920/Obit`.
Links below pin that exact source. Raw-source browser fetches were unavailable;
the local Git revision and owning functions were verified directly.

- [Official MFImage overview, Cotton, 2025-10-05](https://www.cv.nrao.edu/~bcotton/ObitDoc/MFImage.pdf):
  coarse subbands are imaged separately and jointly CLEANed using a weighted
  average; wide fields can use independent facets. Optional baseline-dependent
  averaging and spectral fitting are additional scientific operations, not
  optimizations to import silently.
- [Obit threading memo 21, Cotton and Perley, 2010-10-22](https://www.cv.nrao.edu/~bcotton/ObitDoc/EVLAThread.pdf),
  table I and discussion on pages 2–3: 12.88 h W1, 4.30 h W4, 2.56 h W8.
  The test included spectral-index/curvature imaging and self-calibration,
  38 subband images of 512 square pixels, and averaged data fitting in RAM.
  Model-response calculation dominated, with gridding secondary. The memo
  describes persistent thread groups and buffer sizes proportional to threads.

## What the source actually does

1. **Read one flat buffer, share it across workers.**
   [MF parallel reader, lines 750–813](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVGridMF.c#L750-L813)
   performs one sequential UV read and dispatches the buffer. It waits for
   workers before the next buffer; this is not an asynchronous multi-reader
   pipeline.

2. **Parallelize complete numerical chunks.**
   [MF setup, lines 419–620](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L419-L620)
   allocates private grids and channel-sized reusable scratch, assigning
   visibility ranges and coarse frequency bins. The
   [worker, lines 960–991](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L960-L991)
   calls preparation and then gridding. Preparation includes UVW/frequency
   scaling, phase/conjugation, validity/baseline checks, tapering and cell/kernel
   lookup. It is not a serial producer preparing every stencil before workers
   merely add its values.

3. **Use private half-complex float grids rather than per-cell locks.**
   [Allocation](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L547-L556)
   and [fold/merge](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L998-L1105)
   show float accumulators, a padded positive-u half-plane, conjugate folding
   and final summation. This trades memory and reduction work for uncontended
   updates; it does not justify four full-size double-complex replicas in
   casa-rs without a resource calculation.

4. **Parallelize model prediction/subtraction too.**
   [Gridded prediction dispatcher](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitSkyModelMF.c#L2461-L2542)
   partitions visibility ranges, with reusable per-worker interpolators;
   [the worker](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitSkyModelMF.c#L2690-L2833)
   predicts and subtracts directly in its part of the UV buffer.
   The [owning loop](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitSkyModel.c#L985-L1026)
   reads, predicts/subtracts and writes working UV data each invocation.
   This inspected route has no equivalent of our compiled normal-operator
   replay artifact; it pays repeated UV passes instead.

## Important qualifications

- The [single MF reader](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVGridMF.c#L546-L628)
  caps gridding threads at `nSpec`: one coarse bin gives one worker there.
  The parallel reader retains the processor count and can use visibility-range
  replicas. `nSpec` is not the Taylor-term count.
- [UV weighting](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVWeight.c#L604-L740)
  still has serial buffer loops for density construction and applying/writing
  weights. Obit is not evidence that every phase scales automatically.
- The current [buffer-size helper](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVDesc.c#L1387-L1420)
  multiplies a desired size by threads, but finally caps it at the caller's
  requested `nvis`. Therefore the 2010 buffer-scaling description must not be
  asserted as universal behavior of this pinned current source.
- Grids and reductions consume memory/bandwidth; final merges and single-plane
  CLEAN retain serial dependencies. Float/SIMD/half-plane techniques need our
  unchanged scientific checks, not an assumed numerical waiver.

## Connection to the measured casa-rs case

[Four-SPW checkpoint](/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/mfs-four-spw-4096-20261002/CHECKPOINT.md):
252,720 selected rows, four SPWs, 4096 square pixels, standard/uniform/Stokes I,
single-term Clark, all seven products compared. End-to-end application times
are CASA serial 52.8241 s, casa-rs W1 60.0771 s, W4 56.5028 s (1.0633x W1/W4).
Single observations, cache uncontrolled; no significance claim.

Density preparation is 8.50/8.51 s W1/W4, and weighted initial imaging plus
replay compilation is 21.53/19.24 s. Together they consume 27.74 s in W4:
about 49% of the application, with almost no worker benefit. Nested MS reads
total only 0.59 s and must not be added to those wall times. Three residual
execute/finish phases already total only 2.94 s in W4.

The justified next target is worker coverage and repeated work inside these
initial consumers, not more readers or residual-only tuning. Existing counters
do not yet separate compiler versus numerical gridding enough to select a
particular rewrite. A focused split of that combined hot stage is sufficient;
a new broad inventory or long benchmark is not needed.

Do not delete replay merely because Obit does without it: our compilation
produces a 487 MB artifact and makes repeated residual refreshes inexpensive.
Evaluate preparation plus all refreshes together. Likewise, do not adopt
Obit's subband CLEAN, averaging, or different spectral model to improve a
CASA-equivalent timing. The transferable design is batched worker-local
numerical preparation and accumulation with bounded ownership and reduction.

No implementation, installation, new timing run, acceptance change, push or
merge was performed for this research. Findings use a bounded Luna Max source
audit plus parent verification and interpretation.
