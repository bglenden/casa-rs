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
run is opaque for more than three minutes, or memory pressure/swap continues to
grow toward OOM.

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
dirty-oracle acceptance check. Repeating the exact full run on another machine
or changing execution shape remains a stop-and-ask decision. The stop is an
operator observation bound to those generic request/log identities, not a new
VLASS-only persisted evidence schema.

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

The exact single-field/all-field fiducials also remain blocked by the recorded
32 GiB capacity stop. Wave #446 therefore remains open and its implementation
PR remains draft: the current smokes satisfy only the 1,024-pixel smoke check,
and the capacity-stop partial closes no full dirty-oracle, correctness, or
performance acceptance check.

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
