# Imaging Evidence Harness Consolidation

Issue: #387
Wave: #410

## Result

The imaging performance tools now use one importable package below
`tools/perf/imager/perf_harness`. One strict workload schema, one strict result
envelope, one checked CASA protocol, one MeasurementSet comparator, one image
comparator, and shared artifact, provenance, stage, and subprocess services
replace the former embedded and tool-local implementations.

Runtime result readers accept schema version 2 only. A one-time converter
migrated all 30 checked-in evidence artifacts and refreshed their manifest
digests. It is not imported by production tools and does not provide an
indefinite compatibility path.

## Ownership

| Surface | Responsibility after consolidation |
|---|---|
| `perf_harness/schema.py` | Strict workload-v1 and result-v2 validation, finite values, and nested typed access |
| `perf_harness/artifacts.py` | JSON-object reads and atomic writes |
| `perf_harness/subprocesses.py` | Canonical process execution and output policy |
| `perf_harness/provenance.py` | Git, machine, executable, dataset, and storage provenance |
| `perf_harness/stages.py` | Rust/CASA stage and backend normalization |
| `perf_harness/casa_protocol.py` | JSON request/result invocation of checked CASA programs |
| `perf_harness/ms_compare.py` | One MS comparator facade and explicit full, sampled, and ACA-pairs row policies |
| `perf_harness/image_compare.py` | One product comparator preserving the existing beam and structured-difference metrics |
| `run_workload.py` | Resolve one workload and orchestrate Rust, CASA, comparison, and result emission |
| `run_alternating_comparison.py` | Schedule counterbalanced workload executions and classify their verdict |
| `bench_simobserve.py` | Orchestrate native/CASA simobserve runs through the shared services |
| `bench_aca_simalma.py` | Orchestrate ACA/simalma scenarios through the shared services |
| Ledger, worker-policy, and matrix tools | Validate and project canonical result artifacts without private result readers |
| Staging tools | Produce validated manifests and artifacts through the shared services |

## Deletions and measurements

The audit baseline recorded 22,532 total Python lines, 11,720 lines in six
major harnesses, and 2,579 lines of embedded Python programs. The consolidated
tree contains 23,839 Python lines including the new checked CASA programs,
strict migration tool, package tests, and schema fixtures. The same six major
entry points now contain 9,033 lines, a reduction of 2,687 lines (22.9%).

Embedded-program lines fell from 2,579 to zero. There are two scientific
comparator implementations: one MS comparator and one image comparator. The
second ACA MS program, generated product-comparison program, embedded CASA
runners, legacy strict MS path, duplicate `KEY_COLUMNS`, and all production
private command/JSON/result/timing/environment helpers were deleted.

## Schema and failure states

Every result has `schema_version: 2`, a `kind`, stable run identity and time,
environment/provenance, artifacts, and tool-owned data under `results`.
Statuses distinguish:

- `dry_run`
- `completed`
- `failed_execution`
- `failed_comparison`
- `out_of_tolerance`
- `unavailable`

Failure statuses carry a typed `results.failure` record. Unknown fields and
wrong types are errors at both the workload and result boundary.

## Verification evidence

- All 64 checked-in workload manifests produced schema-valid version-2 dry-run
  artifacts under `target/refactor-wave7-dry-run`.
- The focused Python harness suite passed 130 tests after the final comparator
  consolidation.
- All 30 migrated checked-in evidence artifacts validate through the strict
  version-2 reader, and the checked-in performance ledger validates their
  updated SHA-256 identities.
- The shared data preflight selected
  `/Users/brianglendenning/SoftwareProjects/casatestdata` for the
  `default-fixture` tier.
- Real CASA-plus-Rust executions completed through the same entry point and
  result schema:

  | Tier | Workload | Rust / CASA wall | Result |
  |---|---|---:|---|
  | smoke | `wave1-standard-mfs-dirty-smoke` | 1.107317 s / 0.115043 s | `target/consolidated-harness-smoke-2/20260718T113546Z-wave1-standard-mfs-dirty-smoke-90365f63.json` |
  | medium | `wave3-wprojection-single-plane-heavy-wave1-medium-auto` | 15.099477 s / 185.097479 s | `target/consolidated-harness-medium/20260718T113657Z-wave3-wprojection-single-plane-heavy-wave1-medium-auto-2965facb.json` |
  | large | `wave3-mosaic-mfs-alma-large-dirty-metal` | 1348.906088 s / 188.385230 s | `target/consolidated-harness-large/20260718T114106Z-wave3-mosaic-mfs-alma-large-dirty-metal-b6d15675.json` |

  All three results and their product-comparison records have `completed`
  status. Scientific review remains explicitly `pending`; the medium and large
  structured-difference review class is `investigate`, not silently promoted
  to a pass.
- The large run exposed 587,708 Metal diagnostic lines. Full-stream summaries
  retain the exact observed count while generated JSON keeps a representative
  first/last sample of at most 128 records per category and records
  `observed_count`, `retained_count`, and `truncated`. That reduced the valid
  generated result from 1.09 GB to 407 KB without rerunning the benchmark.
