# VLASS Wave 1 Implementation Preflight

Truth class: approved implementation record
Last reality check: 2026-07-21
Verification: `just docs-check`

Wave issue: [#446](https://github.com/bglenden/casa-rs/issues/446)

Parent outcome: [#445](https://github.com/bglenden/casa-rs/issues/445)

## Decision

Implement the VLASS correctness oracle and evidence foundation by extending the
one canonical manifest-driven imaging harness. Do not create a VLASS-only
runner, comparator, schema, or imaging path. This wave changes repository
tooling and external evidence only; production algorithms belong to #447,
#448, and #52.

The archived CASA recipe remains the science source of truth. Its full
AWProject, pointing, multi-SPW, and MT-MFS controls are represented explicitly.
The harness must report the current Rust target as unavailable until those
algorithms exist. It must never obtain a runnable comparison by changing CASA
to W-projection, selecting one SPW, disabling pointing, shrinking final image
geometry, or dropping products.

## Architecture And Ownership

- `tools/perf/imager/perf_harness` owns strict workload/result contracts,
  checked-in CASA JSON protocols, provenance, product inventory, and the one
  image comparator.
- `tools/perf/imager/run_workload.py` remains the manifest orchestrator and
  records per-target capability. A CASA-only oracle may run when Rust is
  explicitly unavailable; an ordinary CASA/Rust comparison may not.
- `scripts/bench-imager-vs-casa.sh` remains a thin process boundary for the
  existing Rust/profile workflow. Its generated CASA Python program is replaced
  by a checked-in protocol program rather than extended.
- `casa_phase_bench.py` is not valid evidence for this recipe in its current
  form because it omits the AW, pointing, selection, normalization, cache, and
  restoration controls. It must be generalized to the exact typed request or
  remain explicitly unavailable.
- No new top-level package, dependency direction, app/runtime model, public
  casa-rs API, provider bundle, image/MS persisted format, or production
  concurrency behavior changes in this wave.
- The additive workload fields are an internal, versioned evidence-tool
  contract. Unknown fields and wrong types continue to fail; no legacy or
  permissive fallback reader is introduced.
- Frozen recipe-backed manifests are immutable evidence requests. The runner
  rejects `--set-imaging` for them, and rejects nonempty `run.env`, rather than
  allowing a command-line or unbound-environment mutation to masquerade as the
  reviewed recipe.
- Evidence-workload fields do not satisfy the production parameter/UI contract.
  #450 remains required and stays open through #449 so every science and final
  execution-control field is accounted for once in the canonical catalog and
  projected with identical semantics through CLI, TUI, native macOS, Python,
  sparse profiles, and assistant/task schemas. Every user-selectable science or
  execution-policy field must be catalog-bound across those surfaces. Only a
  resolved-plan implementation detail that is not user-selectable may be
  classified as internal and non-persistable, with a recorded rationale.

## CASA Recipe Contract

The exact archived member is:

```text
tclean.last
size 4153 bytes
sha256 a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5
```

Parse only one-name literal assignments with Python `ast`; never execute the
file or its commented `tclean(...)` call. The effective request records:

- every archived parameter;
- CASA-version compatibility normalizations;
- the small allowlisted reproducibility override set; and
- a canonical hash of the final effective CASA keyword map.

For CASA 6.7.5.9, the only accepted compatibility normalizations are:

- archived `chanchunks=1` is recorded and omitted because the current public
  `tclean` signature removed it while the private task fixes it to one; and
- archived scalar `pointingoffsetsigdev=0.0` becomes `[0.0]`, the current
  schema's equivalent vector value.

New 6.7.5.9 parameters use their version-pinned defaults and are recorded
separately. Reproducibility overrides are limited to absolute input/output/cache
paths, the frozen field/phase center, `datacolumn='data'`, noninteractive serial
execution, `restart=False`, dirty `niter=0`, documented smoke geometry/SPW, and
the later checksum-pinned deterministic mask/clean controls.

The frozen geometry names and binds three complete selections rather than
trusting a free-form field/SPW string: `single_field` is field 1525 over SPWs
2--17 with 10,400 selected MAIN rows, `all_fields` is the 63-field raster over
SPWs 2--17 with 655,200 rows, and `single_field_spw9` is the smoke selection,
field 1525 and SPW 9 with 650 verified rows. Each named selection also freezes
UV range, intent, SPW IDs, channel window, and all four correlations. A manifest
must name one selection and match every one of those facts.

The CASA identity includes Python, path-independent hashes of the complete
installed casatasks/casatools code and native-library trees, casaconfig and the
casadata entry points, the selected measures-data/VLA model tree, recipe digest,
dataset digest/receipt, and effective keyword hash. Absolute executable,
module, and data locations remain provenance but do not change the stable
identity, so a byte-identical installation can move to the larger fiducial host.
Every invocation revalidates that runtime identity and rehashes any deterministic
clean mask immediately before calling `tclean`.

## CF Cache Contract

Cold and warm evidence are different roles:

- cold requires the exact plan-keyed cache path to be absent, writes into a
  `.partial` path, and promotes only after successful inventory and receipt;
- warm requires the cold receipt and matching complete plan key, records the
  pre-run stable digest, uses a fresh image prefix, and verifies the stable
  cache inventory did not change. A missing cold cache or receipt fails before
  any CASA call; warm planning and execution never bootstrap a cold run.

The full single-field and all-fields contracts each have an explicit one-call
`-cold` manifest plus the unsuffixed three-call warm manifest. Each pair has an
identical CF plan key and separately reviewable cold-construction versus warm-
reuse timing; a warmup is not substituted for measured cold evidence.

The effective CASA-call projection used for the CF key is exactly `field`,
`spw`, `imsize`, `cell`, `phasecenter`, `stokes`, `projection`, `specmode`,
`reffreq`, `nchan`, `start`, `width`, `outframe`, `veltype`, `restfreq`,
`interpolation`, `gridder`, `facets`, `psfphasecenter`, `wprojplanes`, `vptable`,
`aterm`, `psterm`, `wbawp`, `conjbeams`, `usepointing`, `computepastep`,
`rotatepastep`, `pointingoffsetsigdev`, and `pblimit`. The plan separately binds
the stable CASA/runtime/measures identity, recipe, verified MS tree, and named
selection/frequency geometry. Mask location/content, deconvolution and minor-
cycle controls, restoration/output paths, and casa-rs memory/worker/backend
policy do not fragment a CF cache because they do not construct or select its
convolution functions. A nonempty `vptable` is rejected until it has its own
content-addressed identity.

A deterministic mask is instead a request-level identity: the request records
its resolved location, kind, checksum, and file or CASA-image-tree identity and
rehashes it immediately before `tclean`. Relocating byte-identical mask content
therefore leaves the CF plan unchanged while the exact invocation remains
auditable and fails closed on content change.

An exact replay of a cold request may finish an interrupted cache publication
only when a schema-v2 commit-intent receipt binds the exact request ID,
effective-keyword digest, and path-independent stable inventory of every
run-owned product, proving that `tclean` already completed. Changed stable
product bytes fail recovery; volatile `table.lock` state does not. This
protocol-reachable recovery never invokes `tclean`, is reported as
`recovered_publication` with zero task wall time, and is explicitly
non-benchmark evidence; it cannot supply a timing sample. Legacy schema-v1
receipts remain warm-read-only and cannot authorize recovery.

The CASA subprocess independently re-derives every CF-affecting parameter from
the effective call and checks the plan schema, CASA version, recipe digest, and
dataset binding. A self-consistent but incomplete or mismatched plan hash is
therefore not accepted.

## Timing And Resource Contract

CASA protocol result schema 3 preserves `wall_seconds` as the exact opaque
`tclean()` task time and additionally measures protocol preflight, task,
product-inventory hashing, cache postcondition/publication, and total execution.
These boundaries do not pretend to expose CASA-internal gridding, FFT,
deconvolution, or restoration attribution. Each completed call records
fail-closed process peak RSS, CPU, page faults, context switches, block-I/O
operations, and disk read/write bytes. Measured-call medians and peak maxima are
mechanically derived from the bound raw results; warmups are excluded. Bundle
publication recomputes the summary and rejects changed summaries, stage medians,
resource mirrors, or benchmark logs.

## Product And Comparison Contract

The first smoke discovers every immediate `imagename*` product and freezes the
inventory. Later runs fail on missing and extra products. Each receipt records
tree identity plus CASA image shape, unit, coordinates, reference values and
increments, restoring beam(s), masks, finite topology, and logical size.

The existing sampled data path remains for same-scale panels. A new bounded
full-array mode streams image and mask chunks and accumulates the final
numerical statistics over every pixel. For structure, the same stream writes
the exact native central spatial plane to three disk-backed Float64 stores and
one coverage map in a request-owned workspace. The metrics use every native
spatial sample; there is no downsampling or binned cancellation path. Persistent
operands and exact FFT/block-statistic intermediates are disk-backed. Masks and
statistics are row-streamed, least-squares fits use bounded QR summaries,
gradients use row halos, the two-dimensional FFT is split into row transforms
plus bounded column batches, and exact block medians/MADs partition disk-backed
arrays. The metric definitions, exact-suffix PB/weight masks, Taylor-product
full-overlap semantics, and tolerances are unchanged. The frozen 12,150 x
12,150 storage plan has no full-plane RAM array, a 16 MiB resident ndarray
budget, a 1,181,174,400-byte complex FFT store, and at most 2,361,960,000 bytes
of block-statistic stores; a 2,097,152-pixel execution test rejects unbounded
constructors and caps measured RSS growth at 192 MiB.
Sampled panels do not stand in for that final full-array/native-plane gate.
CASA repeatability uses neutral left/right prefixes; it does not pretend one
CASA run is Rust.

The comparator is a strict schema-v4 protocol, not a best-effort report. Its
canonical request hash binds every normalized operand, label, product,
inventory and metadata policy, source box, tolerance, chunk limit, panel
destination, and absolute structure-workspace path. The result must echo that
binding and match every requested product/path before it can be evaluated.
Protocol request, result, and log digests are retained. Successful structure
analysis deletes its workspace; incomplete coverage retains the workspace and
failure receipt. The run-result schema and bundle validator require exact
native-plane coverage and reject any leftover workspace before promotion.
Missing, malformed, mismatched, unavailable, out-of-tolerance, or leftover
evidence keeps the whole typed `.partial` bundle rather than publishing a
fiducial.

Frozen tolerance contracts are executable, not descriptive. Missing metrics,
missing or extra source boxes, unavailable required comparison processes,
finite/mask topology differences on the valid science domain, and unexplained
structured differences are non-green. Source-box integrated flux divides the
finite unmasked pixel sum by each image's own Gaussian restoring-beam area in
pixels. The field-1525 source box is a 64-by-64 box centered on full-array smoke
peak `[712, 472]`, translated to the unchanged 12,150-pixel grid as
`[6243, 6003]..[6306, 6066]`.

## External Storage And Recovery

All data, scratch, logs, products, masks, receipts, protocol files, comparison
panels, and CF caches use:

```text
/Volumes/GLENDENNING/casa-rs-vlass/issue-446/
```

The issue-owned layout separates hash-addressed data, `.partial` staging,
receipts, masks, CASA-versioned plan-keyed CF caches, durable artifacts, and
explicitly disposable scratch. The archive is currently healthy and the volume
has about 2.7 TiB free, but the volume history records disconnects and I/O
errors. Same-volume atomic promotion and one self-contained receipt per run are
required. Products, protocol requests/results, CASA logs, comparison inputs and
outputs, panels, the benchmark summary, and an embedded receipt start under one
same-parent `<run-id>.partial` tree. Only a completed required comparison
promotes the whole tree; failure, interruption, missing comparison evidence, or
out-of-tolerance output retains the typed partial. Raw execution paths and
request hashes remain unchanged, with an explicit execution-to-retained mapping
after promotion. No durable fiducial uses the generic `_tmp_safe_to_delete`
ImPerformance root.

Storage preflight validates both the supplied and fully resolved MS paths, so a
symlink cannot escape the issue boundary. It likewise validates the output,
artifact, scratch-derived, mask, receipt, and CF-cache locations under the exact
issue root and requires them to share the mounted dataset device.

Stop before execution if any planned path is on the internal disk, an archive
or recipe identity differs, the mount changes, free space falls below 1 TiB or
twice the remaining projection, a partial cache would be reused, a receipt or
inventory cannot be written, cache growth exceeds 256 GiB without review, the
run is opaque for more than three minutes, or swapping becomes genuinely
untenable: the host is effectively unusable, swap-dominated execution makes no
meaningful CASA progress for a sustained interval, or stability/storage is at
credible risk. Ordinary or noticeable finite swapping is accepted for the
12,150-pixel baseline.

### Recorded capacity stop

The first exact 12,150-pixel single-field attempt on the 32 GiB workstation
reached the approved sustained-swap stop and was interrupted; it is not a
fiducial. CASA estimated 29,411,146 KiB required against 33,554,432 KiB
available. Swap-out growth reached 3,583,967,232 bytes while free memory fell
from 87% to 37%, exceeding the 2 GiB stop threshold without throttled pages.
The process exited 130, no matching child remained, and free memory recovered
to 89%.

The retained partial artifact is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts/vlass-fragment-single-field/casa_warm_dirty_fiducial/20260720T202526Z-vlass-fragment-single-field-739c02c1`.
Its exact request is `protocol/warmup-001/request.json`
(`0036e4cf4b71706b148aae0c667b6feba8a78f46b3f2f349227d6f3425a49d12`),
and its CASA log is `protocol/warmup-001/casa-20260720-202527.log`
(`78e53eb2656789954bbc99a977300aabd639731467d50858a0b8f095c8a7e71e`).
The incomplete artifact is explicitly not a fiducial and cannot close any
dirty-oracle acceptance check. This stop was superseded on 2026-07-21 by the
explicitly approved policy above and the successful full-size run below. It
remains historical evidence of the former 2 GiB swap-growth threshold, not an
indication that 12,150 pixels is infeasible and not a new VLASS-only persisted
evidence schema.

### Current exact full-size single-field cold evidence

The exact 12,150 by 12,150 field-1525 cold run completed on 2026-07-21 without
the authorized 8,192-pixel fallback. `tclean` took 1,276.157 s (21m16.157s) and
the checked protocol took 1,316.767 s. The process recorded 13,542,998,016 bytes
peak RSS, 83,842,760,704 bytes read, 63,605,723,136 bytes written, 8,988,500,714
logical product bytes, and a 23,187,184,256-byte CF cache with 14,336 files.

External `vm_stat` samples bound to this invocation observed 16,384-byte pages
and deltas of 3,210,122 swap-outs (52,594,638,848 bytes, 48.98 GiB) and
1,797,859 swap-ins (29,456,121,856 bytes, 27.43 GiB). The heaviest finite
intervals were roughly 200--240 MB/s combined swap traffic around CF/full-grid
transitions. CPU utilization temporarily fell to roughly 22--25%, but the host
remained responsive, CASA continued through visible phases, swap growth stopped
after `tclean`, and memory pressure recovered to 88% free. This is substantial
but bounded swapping, not the approved definition of untenable thrashing.

The complete receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T051546Z-vlass-fragment-single-field-cold-164bd8e1.json`
(`e91ee5af3a5a28b90c2bd6a77c43fd870ab8d590534e4e67dc351f4e54e7b0b1`).
It passes the shared strict loader and pre-promotion integrity validation for
one call, 18 CASA products, one full-array self-contract comparison, 20 review
panels, and the external CF cache. The structured-difference label is `good`
and the human-review panel state is `ready`.

The comparator completed before an outer-schema mismatch rejected newly added
source-region and zoom-panel fields. Recovery rebound the existing request,
result, logs, products, panels, and cache to the frozen plan, recorded
`tclean_reinvoked=false comparator_reinvoked=false`, and used the normal
full-bundle integrity validator before atomic promotion. No timing was
re-measured or synthesized.

### Current exact full-size all-fields evidence

The exact 12,150 by 12,150 connected 63-field cold dirty fiducial completed on
2026-07-21 without the authorized 8,192-pixel fallback. `tclean` took
8,183.264 s and the checked protocol took 8,225.322 s. The process recorded
16,742,760,448 bytes peak RSS, 91,283,177,472 bytes read, 64,626,225,152 bytes
written, 8,988,500,714 logical product bytes, and a 23,166,034,560-byte CF
cache with 14,336 files. Bound five-second host telemetry observed a minimum
35% free memory, zero throttled pages, 48,947,691,520 bytes swap-out,
31,465,897,984 bytes swap-in, and a maximum combined swap rate of
681,609,133 bytes/s. CASA continued through both full SPW/POINTING traversals,
the host remained operational, and memory recovered after the call. Full
geometry is therefore feasible; the 8,192-pixel fallback is not active.

The strict complete receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T071009Z-vlass-fragment-all-fields-cold-3d3179ae.json`
(`f424a33d8b228a56b552cfd793b4410e9fbca3fdb8af9a3fc47ea9d6957b415e`).
It passes bundle integrity for one call, all 18 CASA products, one full-array
self-contract comparison, 20 panels, and the external CF cache. Structured
difference is `good` and panel review is `ready`.

A subsequent warm schedule completed its unmeasured warmup in 9,011.462 s,
with 12,238,503,936 bytes peak RSS, 105,544,388,608 bytes read, and
38,139,731,968 bytes written. Brian redirected the wave on 2026-07-21 before
the first measured warm call completed: repeated CASA timing was consuming
development time before casa-rs could run the workload. The typed interrupted
receipt is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T093618Z-vlass-fragment-all-fields-f80f9a39.json`
(`70b33ca592a71139c8f85adf99e8d4249a8852d58d18b9e3adf5550f95eb7d4f`).
It records `operator_interrupt`; the completed warmup result is retained, the
partial measured-001 request and log are retained without a completed result,
and measured-002/003 were never launched.

Brian explicitly approved deferring further CASA repetitions solely for
statistical precision. The conservative development baseline is the completed
all-fields cold `tclean` time, 8,183.264 s, and the corresponding initial 10x
casa-rs target is 818.326 s. The completed full-size CASA products are the
frozen correctness reference. Revisit a multi-run CASA median only if CASA
parameters, data selection, geometry, or required products change, or if
casa-rs approaches the final 10x boundary closely enough that CASA variance
could change pass/fail. This schedule change does not defer any casa-rs
capability, correctness, UI, or performance work.

### Current exact full-array smoke evidence

The schema-v4/schema-v3 cold smoke completed at 03:57 UTC on 2026-07-21 with
70.564 s inside `tclean` and 81.229 s end to end. It recorded 1,959,198,720
bytes peak RSS, 98,660,352 bytes read, and 369,967,104 bytes written. The warm
smoke completed at 03:58 UTC with a 1.778 s warmup and measured calls of 1.819,
1.835, and 1.904 s (median 1.835 s). Both retained complete bundles, all 18
products per call, native PSF and exact 1,048,576-pixel central-plane evidence,
green frozen tolerances, schema-v2 producer-bound CF evidence, stage/resource
summaries, and passed bundle-integrity receipts. Exact paths and hashes are
the canonical v3 receipts
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T035625Z-vlass-fragment-smoke-cold-cad8add1.json`
(`a6d81a86649ac9f64c33bb967d5205f861ba8bbf21a0cf80caa3f9507dca304c`)
and
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/20260721T035810Z-vlass-fragment-smoke-warm-a3dd3526.json`
(`c8deeef01d44c5365c91264590fee15b81c467606a9c5128ab5c011c945beace`).
Both pass the shared strict run-result loader.

### Historical pre-final-contract smoke evidence

The superseded 00:19/00:21 UTC receipts bind the old VLASS-named geometry
identity and are historical after the generic frozen-geometry contract was
closed. They do not satisfy the current immutable-identity acceptance contract.
The previous 23:19/23:22 exact-array receipts predate protocol stage/resource,
benchmark-log, and producer/product recovery binding; the still earlier
21:49/21:51 receipts also predate immutable-selection, CF projection, recovery,
comparator-integrity, and full-array structure hardening. Those receipts remain
under `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/runs/` as historical
engineering evidence only and do not satisfy the current Wave #446 smoke
acceptance contract. The current canonical receipts above replace them for
smoke acceptance.

The exact single-field and all-fields cold dirty fiducials are complete at full
geometry, and the all-fields warmup is retained. The approved precision-only
repetition deferral above replaces the remaining CASA timing schedule for
development. Wave #446 and its implementation PR remain open/draft while the
frozen correctness products and receipts are integrated into the required
casa-rs serial-parity evidence; neither interrupted partial closes a
correctness acceptance check by itself.

## Verification Plan

Focused tests cover strict schema fields and types, exact recipe parsing and
override allowlisting, frozen CLI/environment rejection, named dataset
selection identity, CASA-version normalizations, CF projection, request-level
mask identity, cold/warm/recovery cache preconditions, strict comparator
request/result binding, bundle publication integrity, product inventory,
producer-bound recovery, stage/resource evidence, neutral repeatability,
bounded full-array statistics, exact native-plane
structure and coverage maps, mask/finite topology, missing/extra products,
exact VLASS dry-run plans, and
honest per-target capability status.

Baseline before implementation:

```text
PYTHONPATH=tools/perf/imager python3 -m unittest \
  tools/perf/imager/perf_harness/test_schema.py \
  tools/perf/imager/perf_harness/test_infrastructure.py \
  tools/perf/imager/test_run_workload.py

Ran 69 tests: OK
```

Before Review, run Python compile and focused tests, dry-run both cold/warm
frozen workload pairs, cold/warm 1,024-pixel CASA smoke, `just docs-check`, `just quick`,
the bounded `refactor` pass, architecture and test-adversary reviews, and
`just verify`. Full-size dirty/clean CASA bundles remain named acceptance
evidence for #446 and cannot move to a later wave without explicit signoff.
