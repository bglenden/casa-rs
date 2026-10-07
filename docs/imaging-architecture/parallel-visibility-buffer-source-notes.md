# Parallel visibility buffers: upstream lessons

Truth class: non-normative primary-source research
Last reality check: 2026-09-28
Verification: revision-pinned source inspection and existing W4 log counters; no new imaging run

## Conclusion

Shared, bounded visibility buffers and simple numerical workers are well-supported
upstream patterns. Independently advancing consumers are a possible design, not
an established requirement for good scaling: the inspected Obit and LibRA CPU
paths explicitly join their workers between work batches. The most immediately
transferable mechanisms are parallel preparation, setup reuse, borrowed array
views, and clear grid ownership. Do not infer that a per-block barrier explains
casa-rs's remaining scaling loss merely because the barrier exists.

This qualifies the preceding discussion's emphasis on independently progressing
workers. Correct admission and redundant reads remain concrete targets; a new
consumer scheduling architecture needs evidence of the relevant wait/imbalance.

## Sources and provenance

- Obit: `ebc1c229e5e3870b5ce3c342bddb7313d986a06f`, local core sources in
  `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/obit-build/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/core`.
- LibRA: `0ab99e261878334d6588eafa360cef3b673e897f`, local checkout
  `/Users/brianglendenning/SoftwareProjects/libRA`; the cited files match HEAD.
- CASA: `61020062cee290f5466cffed5ec5032e0c7a3434`, local checkout
  `/Users/brianglendenning/SoftwareProjects/casa`. `GridFT.cc` has existing local
  diagnostic changes, so its cited revision line numbers were checked with
  `git show HEAD:casatools/src/code/synthesis/TransformMachines/GridFT.cc`.
  The cited VI/async files match HEAD.
- casa-rs: `02d337171120c5b5b07b15c30a927e27567585e4`; full-512 W4 evidence
  under `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/spectral-full-20260924/full512-retained-20260928-exec-v1`.

These are inspected implementations, not claims that every mode or the benchmark
binaries exercised every listed path. Remote source pages were unavailable to
the web fetcher; code claims below were verified from local sources. The Obit
memo and CASA public API documentation were also read online.

## Obit: preparation in workers, followed by tight channel loops

`ObitUVGridReadUVPar` reads one UV buffer, invokes the CPU gridder, and then reads
the next buffer. `ObitThreadGridGrid` dispatches batches through
`ObitThreadIterator`, which submits work to a retained pool and waits for all
completion messages. This is not a pipeline of channel consumers independently
advancing through several prepared UV buffers.

Sources: [ObitUVGrid.c, 596-619](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVGrid.c#L596-L619),
[ObitThreadGrid.c, 827-866](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L827-L866),
[ObitThread.c, 487-552](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThread.c#L487-L552).

Each gridding worker runs `fast_prep_grid` for its visibility and selected
channels, then the channel stencil loop. Preparation rotates UVW once per
visibility for that worker/facet and stores weighted complex samples, cell
locations and convolution-table pointers in reusable scratch. This is not one
global serial preparation step. It is also not proof that all setup is shared
once across every facet or replica.

Sources: [ThreadGrid, 960-985](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L960-L985),
[fast_prep_grid, 1263-1387](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L1263-L1387).

The base path points workers at the same input buffer but uses private grid
replicas and a later merge. Transfer the input-sharing and preparation/locality
ideas, not unbounded grid replication. See the fuller
[Obit source study](obit-data-structure-source-notes.md), particularly its
distinction between base CPU gridding and grouped spectral CLEAN.

Cotton's [Memo 57, sections II-IV](https://www.cv.nrao.edu/~bcotton/ObitDoc/DoubleBuffer.pdf)
tested explicit double buffering of a large image cube and found no significant
improvement on those systems. That was an image-plane read/compute experiment,
not a benchmark of our MS pipeline, and does not rule out useful buffering here.
It does caution against treating additional queues as a performance result.

## LibRA: borrowed block views and persistent, synchronized workers

`MultiThreadedVisibilityResampler::scatter` references the same `VBStore` arrays
and assigns contiguous row intervals. Workers persist, but each gridding,
degridding or residual request dispatches work and then waits for completion.
`ThreadCoordinator` also has an explicit worker barrier. These sources support
coarse work on simple borrowed buffers, not independently progressing channel
pipelines.

Sources: [shared views and row partition, 310-321](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc#L310-L321),
[dispatch/join, 451-559](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc#L451-L559),
[ThreadCoordinator.cc, 103-168](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/Utilities/ThreadCoordinator.cc#L103-L168).

This gridding implementation maintains worker-private accumulators and gathers
them later; degridding supplies workers a common read-only Fourier grid. Its
plain `VisibilityResampler` obtains array storage pointers, dimensions and
strides before the row/channel loop. Borrow those ownership/locality ideas,
not its full-grid-per-worker allocation strategy for the memory-limited cube.

Sources: [grid initialization/gather, 362-445](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc#L362-L445),
[shared prediction input, 511-533](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc#L511-L533),
[VisibilityResampler.cc, 114-168](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines/VisibilityResampler.cc#L114-L168).

## CASA: parallel block preparation; qualified lookahead example

The pinned `GridFT::put` forms block-local cell-location, convolution-offset and
phasor arrays. It parallelizes `locuvw` over rows with OpenMP, then uses those
arrays in gridding. The spatial-section gridder assigns disjoint grid regions;
this is a different decomposition from our cube's plane ownership, not a reason
to replace it. The immediate lesson is that expensive preparation need not be
serial just because one reader owns MS access. OpenMP execution depends on the
build/settings; this inspection does not establish the benchmark's thread count.

Source: [pinned GridFT.cc](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/TransformMachines/GridFT.cc?at=61020062cee290f5466cffed5ec5032e0c7a3434),
lines 763-816 for preparation and 875-985 for section gridding.

The older `VLAT` implementation is a useful bounded producer/consumer example:
it fills requested `VisBuffer` components and detaches the buffer from its
iterator before publishing it. `VlaData` bounds lookahead by `MaxNBuffers` and
signals on queue availability. But this is one consuming iterator, not proof of
multi-consumer channel scheduling. More importantly, in the pinned newer VI2,
the asynchronous constructor implementation is commented out and
`isAsynchronous()` returns false. Do not attribute CASA's measured speed to an
active asynchronous VI2 based on its header documentation.

Sources: local `casatools/src/code/msvis/MSVis/VLAT.cc:320-347,616-665`,
`AsynchronousInterface.cc:613-675,827-860`,
`VisibilityIterator2.cc:201-222,298-305`, at the CASA revision above.
The [public VI2 documentation](https://casa.nrao.edu/doxygen/VisibilityIterator2_8h_source.html)
describes the intended prefetch interface, but the executable implementation
qualification is essential.

## What the existing casa-rs log discriminates

Summing `source_starved_nanos` on the 71 `bulk_cube_wave` records in
`native-w4.log` gives **1.472297 seconds**; their summed stream wall time is
**5529.744 seconds**, within the **6198.327-second** application. This timer
measures the ordered consumer's wait for a raw source block, not worker idle
time inside preparation, dispatch or gridding. Source filling also consumes CPU
and bandwidth even while its wall latency is overlapped.

Reproduction (run in the full-512 evidence directory):

```sh
perl -ne 'if (/^bulk_cube_wave/) { $waves++; $wait += $1 if /source_starved_nanos: (\d+)/; $wall += $1 if /wall_nanos: (\d+)/; } END { printf "waves=%d source_starved_seconds=%.6f summed_stream_wall_seconds=%.3f\n", $waves, $wait/1e9, $wall/1e9; }' native-w4.log
```

Counter semantics: casa-rs `bounded_stream.rs:2202-2224` measures the blocking
receive before processing a block; `bulk_source.rs:125-263` performs preparation
before invoking `BulkWave::consume`; `bulk_wave.rs:81-112` joins numerical jobs.

**Inference:** a deeper raw-data queue alone is a weak first target. Parallel or
reused preparation has stronger direct source support. Neither the upstream
barriers nor this receive-wait counter resolves numerical-worker imbalance,
cache/bandwidth saturation or core placement. A matched residual-wave profile
must distinguish those before changing the scheduling architecture.

Recommended sequence remains: correct the demonstrated phase/shared-memory
admission mismatch; address repeated full-width first-wave preparation where
coverage semantics permit; use the existing numerical path with bounded,
parallel/reused preparation rather than serial object work. Introduce independent
consumer progress only if measured intra-compute waits justify that complexity.
This is a research recommendation, not an implementation authorization or a
promise of a particular speedup. No new imaging, installation, push or merge.

The source study used two bounded Luna Max checks (Obit and LibRA), with the
main agent independently verifying the call chains, inspecting CASA, extracting
the current W4 wait counter and making the synthesis. The research and imaging-
performance skills required the source-linked record and separation of measured
behavior from proposed changes. `just docs-check` passed.
