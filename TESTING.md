# Testing Strategy

Truth class: normative
Last reality check: 2026-10-20
Verification: just verify

## Test categories

- Unit tests: pure value, parsing, math, and domain logic inside a single crate
- Integration tests: cross-crate behavior, on-disk persistence, contract boundaries, and CLI surfaces
- Interoperability tests: Rust/C++/CASA read/write matrices when on-disk bytes or metadata matter
- Packaging/runtime tests: editable Python package checks, wheel-install smoke checks, installer validation, demos, and suite layout checks
- Performance tests: smoke benchmarks and parity timing when performance is part of the requirement

Imaging evidence uses four non-substitutable tiers:

- diagnostic/law evidence localizes algorithms, invariants, and edge cases and
  may use tiny synthetic fixtures;
- representative scientific acceptance executes the application production
  compile/plan/run owner on a mode-faithful real or realistically shaped
  MeasurementSet;
- resource/performance evidence proves memory, I/O, scheduling, and turnaround
  contracts without standing in for scientific comparison; and
- persistence/interoperability evidence proves durable CASA-compatible bytes,
  metadata, reopen behavior, and model-column effects.

The representative scenario inventory is
`resources/imaging-architecture/representative-science-matrix.json`; its
rows are the T2 workloads of the imaging foundation performance pass (plan
section 7, ticket IF-10), which regenerates their CASA oracles. A representative
image-producing row normally has at least a 512x512 image and 1,000,000 selected
correlation-channel samples, uses the production route, and exercises the
mode's load-bearing dimensions. Full Stokes requires four correlations with
meaningful cross-hand flags and weights; a cube requires at least sixteen
meaningful channels; a mosaic requires overlapping pointings; MT-MFS requires
a meaningful multi-SPW frequency lever arm. Each row checks every relevant
product, exact topology/metadata/validity, NRMS no greater than 0.001 where
applicable, and independent beam, flux, centroid, spectral, polarization,
mask/support, and persistence facts. A recorded exception must explain why the
dimension is scientifically inapplicable and requires programme-owner approval.

Run the validator with `--require-external` for issue closure whenever the
bound evidence volume is mounted. That mode verifies every available external
receipt digest and reconciles the retained worker, multiscale, continuum-data,
and downstream-cube facts used by the compact checked-in receipts. The
argument-free CI form validates the durable matrix and receipt structure when
host-specific external evidence is unavailable.

CASA oracles are generated once and frozen with their CASA version, dataset
identity and selection, image and solver parameters, comparator, and artifact
inventory. Regenerate only when one of those contracts changes. Tiny fixtures
remain required when they provide faster failure localization, but a green toy
end-to-end test cannot make a matrix row representative.

Nonlinear deconvolution can amplify numerical-scale near-ties into different
component histories. A mode-specific gate may classify cross-run pixelwise
agreement limits as *review triggers*, rather than silently widen them or
require bitwise component identity, only when the owner approves a versioned
replacement rule. Structural and deterministic-product checks, independent
convergence, resource limits, and relevant scientific checks remain hard.
The T55 C-array v3 rule compares the saved model directly with CASA but does
not convolve either saved model to verify publication consistency; the owner
removed that corruption-only check on 2026-09-23. Other modes retain their
own approved checks. A trigger is unsuccessful until a reviewed
disposition binds the exact input/comparison, gate version, all affected
products and planes, unchanged measurements, visual evidence, rationale and
owner decision. Unexplained localized defects, known implementation errors,
missing evidence and stale review identities fail closed. Similar future
outputs may reuse the review procedure, never another output's approval. The
measurement pass runs once per unchanged product set; a later owner review
finalizes its saved, identity-bound report without reopening product arrays or
repeating scientific calculations. A changed product or rule requires a new
measurement, not a review-time verification pass. The
T55 C-array implementation and unchanged per-plane measurements are in
`tools/perf/imager/t55_c_array_clean_gate.py`; this paragraph alone changes
no other mode's established thresholds or acceptance status.

For #607's frozen MT-MFS row, the programme owner approved one narrow support
rule on 2026-09-02: `.alpha` and `.alpha.error` validity is exact within CASA's
`.pb.tt0` support, alpha values across that full supported plane remain subject
to NRMS <= 0.001, and outside-PB validity differences are counted and reported.
This does not change CASA's strict Taylor support law, add a numerical epsilon,
or relax any other product's exact validity comparison.

T52 native EVLA paired-CF acceptance uses the application production route at
512x512 with nine overlapping VLASS fields, four SPWs, nonzero W, both circular
parallel hands, and 5,990,400 selected correlation-channel samples. Its frozen
DIRTY and 30-iteration deterministic MT-MFS CLEAN references require all 18/19
products and the unchanged beam, flux, centroid, structure and NRMS limits.
On 2026-09-08 the programme owner approved retaining the frozen maximum mask
mismatch fraction of 1e-5 only for `.alpha` and `.alpha.error`, with every
differing pixel reported; all other validity is exact. The owner also approved
macOS-only execution evidence in place of a host with no CASA installation.
The native Rust generator and application must still neither link nor invoke
CASA; CASA is used only to prepare/compare the frozen independent reference.
These amendments are recorded in the [T52 work record](https://github.com/bglenden/casa-rs/issues/538#issuecomment-5577836559).

AW projection runs on the major-cycle pass with the `AwCatalog` kernel set
(IF-3, #652). Its laws are `casa-imaging-operator`'s `aw_laws` (cell keys,
cold and warm catalogs, native generation against the CASA cell rules) and
T1's `aw_projection` cases; its CASA evidence is the T1.5 AW rows (CASA cell
catalogs and native generation), the T2 matrix's native-AW row and #625's
full VLASS fragment (4096², 63 fields, 16 SPWs, serial). The frozen-cell
tests require explicit surface and catalog paths; no CASA installation or
model-data path is discovered implicitly. The historical T51/T52 pair-driver
acceptance harnesses were removed with their test-only Rust entry points
(IF-0, #649).

T53 spectral joins use the public compiled operator and normal application
boundaries. Run the `t53_` tests in the `continuum_application` integration
target, the cube laws in `casa-imaging-operator` (`resample`, `predict`,
`weighting`) and `casa-imaging-runtime` (`pass_laws`), typed availability
regressions, and directly affected polarization/product checks. The
`casa-tables` integration target `selected_incremental_arrays` checks public
typed UVW reads, sparse/repeated row order, output reuse and unmaterialized
metadata in both byte orders.
`tools/perf/imager/test_bench_cli.py` checks the
benchmark command against a current release imager before opening a dataset.
The representative workload is `issue607-standard-cube-dirty-representative`.
The W and mosaic cube workloads are `t53-w-cube-shared-phase-representative`
and `t53-mosaic-cube-alma-representative` (cube AW stays refused, #719). The
W one uses the immutable-source copy produced by
`tools/perf/imager/stage_t53_shared_phase.py` with CASA Python, which
preserves the original IncrementalStMan UVW layout and values.
Current native-AW Taylor evidence may be reused only after checking code and
input applicability. These rows retain the size, channel, complete-product,
validity and resource limits above; diagnostic laws do not replace them.
The historical T53 W-cube receipt is preserved in the T55 evidence archives
on the NAS (`casa-rs-glendenning-preserve/GLENDENNING/casa-rs-evidence/t55/`);
the imaging foundation performance pass (IF-10) regenerates it with the other
T2 rows.

T53 preserves the #478 named join gates
`wproject_dirty_cube_products_track_casa_on_refim_point_withline_shared_phase_multifield`
and `wproject_dirty_cube_products_track_casa_on_n2403_source_backed_calibrator_multifield`.
Their pinned source is `crates/casars-imager/tests/imager_casa_parity.rs` at
`376c7d87bb995f295a696995ad8292bc39582529`. A selected FIELD metadata row with no
MAIN rows is not evidence of a multifield join. The original #445/#448
single-field DIRTY, single-field deterministic CLEAN, 63-field DIRTY and
63-field deterministic CLEAN gates remain named obligations of those issues,
whose bodies record what the imaging foundation fulfilled and what remains.
The owner-approved representative T53 boundary does not claim their full-size
or performance acceptance. Current results and restart authority live in the
[T53 work record](https://github.com/bglenden/casa-rs/issues/539#issuecomment-5579521124).

## Required discipline

- Every approved work item defines falsifiable acceptance checks and ships verification evidence.
- Bug fixes need regression tests.
- Cross-crate and boundary changes need integration or contract coverage.
- On-disk metadata or byte changes need the applicable 2x2 interop matrix: RR, RC, CR, CC.
- Observation-transaction coverage must prove exact per-MS read/write sets,
  mechanically derived typed observation reads, mutation-before-read ordering,
  exact `ProductRequirements` staging coverage, reconciliation and
  complete-staging cuts, mandatory transaction binding through `plan`, a terminal
  publication step with atomic individual-image replacement, no controller
  polling after publication launch,
  and fail-closed cancellation plus admission, numerical, output, and
  staging-fence failures.
- ADR-0014 product publication tests exercise write-only bounded ownership,
  complete ordered coverage, unchanged science/metadata and ordinary I/O errors.
  Failed publication leaves an incomplete output set requiring rerun, not
  resumable member recovery or whole-set rollback. Architectural mutation tests
  reject content hashing and output rereads even without sealing terminology;
  test/benchmark fingerprints remain diagnostic, outside production execution.
- Binary serialization changes need endian coverage.
- Measures-data dependent tests must skip cleanly when runtime tables are unavailable.
- C++ dependent tests must skip cleanly when `pkg-config casacore` is unavailable.
- C++ interoperability callers use the typed facades in `casa-test-support`.
  `CasacoreOracleRuntime` is the sole owner of backend availability, CString
  conversion, owned C-result cleanup, and domain locks; disabled builds retain
  the same facade surface and return `OracleError::Unavailable`.
- Tests and scripts that use shared CASA C++ datasets must use the shared
  resolver policy: default fixtures may search `CASA_RS_TESTDATA_ROOT`,
  `../casatestdata`, and `~/SoftwareProjects/casatestdata`; long gates may also
  consider the shared `/Volumes/home/casatestdata` mirror after preflight. When
  a long gate declares required dataset paths, preflight should select the first
  available root that contains those paths instead of stopping at an incomplete
  earlier mirror.
- Tutorial parity datasets are registry-backed and staged separately under
  `CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity/...` or
  `~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...`; default gates
  must not select them implicitly.
- Heavy parity suites stay behind explicit opt-in gates such as `scripts/test-slow.sh`.
- Release-only Cargo integration suites should stay out of the default compile path via explicit `[[test]]` entries and `required-features`, not only file-local `cfg` guards.
- Workspace gates run Rust tests through `scripts/test-workspace.sh`, which uses
  `cargo-nextest` (one process per test, in parallel) plus `cargo test --doc`.
  The imager progress observer is process-global so that worker threads
  contribute to one run; in libtest's shared-process harness, parallel tests
  could attach unrelated imaging work to an active progress-test context. Under
  nextest each test has its own process. Without nextest the script falls back
  to serial `cargo test` (`RUST_TEST_THREADS=1`).

### Linux temporary storage

Managed-spill tests require a filesystem that can release file-backed page
cache. On Linux, a memory-backed `/tmp` (tmpfs) can return success from
`posix_fadvise(POSIX_FADV_DONTNEED)` while retaining pages; the runtime correctly
rejects this with `PageCacheRetention`. Place test temporary files on a
disk-backed filesystem instead, for example:

```bash
mkdir -p "$PWD/target/test-tmp"
TMPDIR="$PWD/target/test-tmp" CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 \
  RUST_MIN_STACK=33554432 cargo test -p casa-imaging-application \
  --test continuum_application application_algorithms_do_not_invent_a_flux_staleness_bound -- --exact
```

Check the selected filesystem rather than assuming that a path named `target`
is disk-backed. This setting relocates disposable test fixtures; retain real
inputs and acceptance evidence in durable storage. It does not disable the
page-cache residency check or change imaging science. macOS retains its
existing no-cache I/O path and does not require this Linux-specific setup.

## Mocking policy

- Prefer real fixtures and integration tests over internal mocks at crate boundaries.
- Do not mock workspace crates unless the alternative would make the test non-deterministic or prohibitively expensive.
- Mock third-party or process I/O only when the behavior under test is not the external tool itself.

## Default commands

- Fast local gate: `just quick`
- Imaging T1 end-to-end tier (synthetic MeasurementSet through the production
  `casars-imager` route; each case under ten minutes):
  `CARGO_INCREMENTAL=0 cargo test -p casars-imager --test t1`
- Full default pre-review gate: `just verify`
- Performance and evidence tool tests (`tools/perf/imager`, pytest):
  `just perf-tools-test`. CI's `perf_tools` job enforces them; `just quick`
  runs them when the host's `python3` has pytest, numpy, scipy and
  matplotlib, and prints a skip notice otherwise. A tool whose casa-rs inputs
  are deleted goes with them; its tests fail here otherwise
- Smoke/release gate: `just smoke`
- Blocking C++ interop release gate: `just release-cpp-interop`
- Informational release performance suite: `just release-perf`
- Release-only install gate: `scripts/test-install-suite.sh`
- Heavy parity suites: `scripts/test-slow.sh`
- Release/tag-only CI-like coverage: `scripts/run-coverage.sh --ci-like`
- GitHub Actions reproduction: `scripts/ci-local.sh pr` for pull-request jobs or `scripts/ci-local.sh tag` for version-tag jobs
- GitHub PR CI: lint/test, editable Python package (including the generated
  UniFFI binding check), performance-tool tests, and strict docs (rustdoc with
  broken and private intra-doc links denied, then MkDocs). The required checks
  are `lint_test`, `python_package` and `docs`
- GitHub tag CI: lint/test, editable Python package and performance-tool checks plus smoke,
  suite-install, and CI-like coverage
- Main-branch pushes run the rustdoc and MkDocs deployment workflow

The Metal backend's gate is T0 Metal versus CPU
(`cargo nextest run -p casa-imaging-metal`), the pass laws'
`metal` and residency laws, the application's
`continuum_application metal::` cases (including coarse output channels in
waves) and T1's `metal::` cases. Each compares Metal with the CPU on the same
input. On a host without a unified-memory Metal 3 device these laws print
`skipped: no Metal device` and pass, so a Metal gate must run outside command
sandboxes on Apple silicon; a skip is not evidence that a Metal workload ran.

Large spectral-cube storage changes additionally run the sparse logical-capacity
test and the explicit release throughput guard:

- `cargo test -p casa-images fifty_gib_spectral_cube_streams_selected_planes_with_bounded_storage -- --nocapture`
- `CASA_RS_CUBE_PERF_DIR=/path/to/volume cargo test -p casa-test-support --features performance-tests --test images_perf_vs_cpp full_spectral_cube_plane_io_tracks_raw_disk_speed --release -- --ignored --nocapture`

The accepted full-resolution VLASS all-field/full-band CLEAN result is a scoped
resource-contract exception: **12150² science pass; no-new-swap guard waived by user.**
The completed 19-product scientific contract retained its unchanged
normalized-RMS ceiling of `1e-3`; the observed global swap-out increase was
exactly 344,504 16,384-byte pages, or 5,644,353,536 bytes. This waiver applies
only to that accepted run. It does not weaken the no-new-swap guard for other
VLASS runs or the general acceptance tooling.

The accepted artifact root is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-candidates/runs/20260812T-12150-all63-full16-clean-r21-r4`.
It contains the products, `casa-rs.log`, and `comparison.json`; the comparison
receipt SHA-256 is
`ebd028a0c3c4a15a6a9733d6917e807bf4aa1522b437cb08ba9f79d963d1f49f`.
The run completed in approximately 40,412 seconds and its worst normalized RMS
was `2.2086646e-4`.

The post-cleanup 4096², 16-SPW, all-63-field production acceptance is retained
under
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-candidates/runs/20260813T-final-cleaned-4096-all63-full16-clean-r22`.
Its `casa-final4096.validation.json` receipt passed all 19 products with zero
failed or incomplete products, a worst normalized RMS of
`9.58363498478996e-5`, 25,030,848 accepted samples, and zero new swap-out. The
validation-receipt SHA-256 is
`b2cc4b3b1ce1062f292b3e1ae360da2fac648bf61dc033e2fa377ab0a104a9d0`.

The first test proves a 50 GiB logical cube can access separated planes with
bounded owned storage. The second physically writes and reads every plane of
both a 50 GiB raw baseline and a 50 GiB tiled image. Both write timings include
a stable-storage flush, the pass order displaces each 50 GiB file from systems
with less RAM, allocated-block checks reject sparse results, and every plane has
a distinct deterministic high-entropy value at every pixel. Pattern generation
and full-plane verification occur outside the timed I/O calls. The selected
volume needs at least 110 GiB free. Image writes must reach at least 60% and
reads at least 50% of the equivalent raw-file path on the same volume and run.

`just quick` includes `scripts/test-task-cli-hosts.py`, which builds and runs
every one-shot binary hosted by `casa-task-runtime::TaskCliHost`. The explicit
inventory proves common protocol/schema discovery, generated help, malformed
stdin, missing request sources, diagnostics, and exit classification at the
process boundary. Runtime unit tests inject execution and serialization
failures, while repository provider-conformance tests mutate protocol versions,
actions, surface IDs, and schemas through the shared envelope validator.

## Evidence source and turnaround

At the current project stage, prefer fast feedback over duplicate
cross-environment verification. A required gate needs one current green result
from either a local run or a hosted run of the same command or documented
equivalent coverage; it does not need both. Once one accepted environment is
green, do not wait for or repair the other solely to duplicate the result.
Use the second environment only to diagnose a failure, resolve genuine
platform-specific uncertainty, or satisfy an explicit user request. A narrower
hosted job cannot stand in for a broader required gate such as `just verify`.

Recent green evidence remains valid until a code, test, build, dependency, or
runtime-configuration change could affect that gate. Final review must inspect
the commits after the tested revision and reuse the result when they contain
only documentation, planning, or workflow-policy changes. Do not rerun a gate
solely because review started; if executable changes intervened, rerun only the
affected gate.

- Python package and its UniFFI service layer:
  `scripts/generate-frontend-bindings.sh --check`,
  `cargo test -p casars-frontend-services`,
  `scripts/test-frontend-services-python.sh`, and
  `scripts/test-python-package.sh`. The `casars` task host's end-to-end
  behaviour is covered by `cargo test -p casars --test casars_cli`.
  `scripts/test-python-package.sh` runs the `casars.tools` tests against the
  bundled measures snapshot unless `CASA_RS_MEASURESPATH` names a runtime;
  run directly, those tests skip when no runtime is installed.

## Imaging tiers and evidence

The imaging foundation (#648, plan section 7) tests in four tiers:

- **T0, laws** (each under a minute; CI): adjoint, PSF, sumwt, worker
  invariance, precision and kernel-set laws on in-process synthetic
  MeasurementSets, in each imaging crate's tests (`t0_laws`, `aw_laws`,
  `wplanes_laws`, `mosaic_laws`, `resample`, `predict`, `weighting`,
  `pass_laws`, the deconvolution solver laws), and the Metal-against-CPU
  laws (`cargo nextest run -p casa-imaging-metal`).
- **T1, end to end** (under ten minutes each; CI):
  `cargo test -p casars-imager --test t1` through the production route, one
  module per capability (standard MFS and masks, cube, mosaic, W and AW
  projection, MT-MFS, model column, Metal, routes, interrupt).
- **T1.5, CASA-paired small fixtures** (local, before an imaging review):
  the casatestdata `refim_*` and kernel fixtures against CASA 6.7.6
  references, NRMS ≤ 1e-3 on every product and `.sumwt` within 5e-5
  (owner, 2026-10-16). The harness lives outside the repository with its
  references (`docs/agent-reference.md`). T1 checks casa-rs against itself;
  a convention shared by casa-rs's simulator and its operator cancels there,
  so a gridding, kernel or normalisation change is not done until its T1.5
  rows agree with CASA.
- **T2, representative** (hours; once per programme or when a row's code
  changes): the matrix in
  `resources/imaging-architecture/representative-science-matrix.json` run by
  `tools/perf/imager/run_workload.py`, the timed checkpoints and the
  rows judged against CASA's own time. The #648 results table records the
  accepted figures and their exceptions.

**Memory is a tested contract.** Every live phase is admitted
(`casa_imaging_runtime::admit`), and an owner that changes what it keeps
adds a counting law: a test that runs the owner and asserts its measured
allocation stays within the bytes it charges (for example
`a_model_data_write_holds_at_most_its_bytes` in `casa-ms`). The allocator
counts the whole process, where under `cargo test` the harness and the
binary's other tests allocate too, so a law in a binary with other tests
first calls the binary's `alone`, which reruns the law in a process of its
own, as nextest runs every test. At T2, a run's peak footprint must stay
within its largest reservation plus the process's fixed overhead; the run
summary's per-phase `peak_reserved_bytes` and
`/usr/bin/time -l` give both. On macOS the footprint drops up to about a
second after memory is freed, so a phase boundary is judged after a short
idle, or with that lag counted in the allocator allowance. A measured live
allocation above its reservation is fixed with a regression law, never
documented as harmless.

**Every evidence run names its conditions.** Defaults and binary labels are
not enough: in IF-10 a build linked Homebrew's scalar FFTW instead of the
NEON one and every timing moved, and a T1.5 harness lost its CPU pin when
`backend = auto` arrived and silently compared Metal runs. A recorded run
states:

- the build (commit, plus any evidence-only patch and its purpose) and the
  FFTW it linked (`nm BIN | grep -c n1fv_` counts NEON codelets);
- the input (MeasurementSet, selection) and the host;
- the backend and grid precision, as the run summary records them, not as
  requested;
- the worker policy and memory policy (`parallel`, or an explicit budget);
- the comparator (CASA reference and version, tolerance and metric), or
  "none" for a resource-only run;
- the cache state where one matters (AW cells, warm or cold).

Timed comparisons follow the A B B A protocol on one quiet host, with user
CPU and cycles beside wall time, and the same work on both sides (cap
`--nmajor` when builds stop after different pass counts). Parallel speed
does not excuse a serial cost: a row whose bar is at W4 also reports W1.

## Long performance campaigns

Every performance run projected over 30 minutes must use a checked-in workload
harness or campaign controller and produce a launch receipt. The receipt binds:

- run and candidate identity, clean source revision or recorded tree state, and
  binary hash;
- dataset selection, source ordering and blocking, reference products, CASA
  runtime, convolution-function cache, and other mode-specific fixture
  identities;
- effective backend and runtime configuration plus required CPU, Metal device,
  pipeline, dispatch, and output receipts;
- a same-revision mode-faithful turnaround receipt;
- scientific tolerances, stage forecast, hard wall limit, and memory, disk,
  swap, and other resource ceilings;
- exclusive host ownership for the long run; and
- artifact-retention class and cleanup disposition.

Only one long performance run may be active on a host. A terminal receipt must
classify it as passed, scientifically failed, resource-aborted, rate-aborted,
fixture-invalid, or execution-failed before another candidate launches. Mere
log activity does not satisfy progress: the controller must report completed
and total stage units, update a conservative completion forecast, and abort
when that forecast exceeds the declared wall ceiling.

A run projected over 90 minutes is final evidence, not a diagnostic. After a
long-run failure, the same stage must be reproduced by a bounded probe before
another long run. Two failed full-resolution launches in one campaign require
an explicit continuation, fallback, or waiver decision. Executable or
runtime-configuration changes invalidate prior turnaround authorization, but
unchanged accepted reference products and CASA baselines remain reusable.

Complete generated products are retained only for accepted evidence, the first
unresolved scientific failure, or an explicitly named forensic case. Rejected
or diagnosed runs retain compact provenance, logs, telemetry, comparisons, and
hashes; their generated products, spill files, frozen binaries, and workspaces
then become cleanup candidates. Cleanup remains fail-closed and must never
remove an active run or unresolved first scientific failure.

## Coverage / confidence policy

- CI enforces 75% line coverage.
- Local goal is at least 78% to preserve a safety margin.
- Explain coverage regressions rather than hiding them.
- Do not trade meaningful behavioral tests for raw numeric coverage.
- Do not run `scripts/run-coverage.sh --ci-like` for routine branch merges unless the user explicitly asks for release/tag-level validation or direct reproduction of the heavy coverage gate.

## Work expectations

For each piece of work:

- acceptance checks have direct verification evidence
- changed behavior has matching tests or explicit justified exclusions
- medium/high-risk work gets independent architecture and test-quality review
- docs, interfaces, and boundary descriptions are reconciled with implementation
  before review
- approved outcome, included issues, and acceptance checks are not deferred or
  descoped without explicit user signoff recorded in the issue or PR
- release work also runs the smoke gate, the blocking C++ interop gate, and the suite-install gate; CI-like coverage remains a version-tag CI gate and is run locally only for `scripts/release.sh --full` or explicit coverage reproduction
- ordinary non-release merges stay on the affected gates (`just quick` or focused checks) plus CI unless the user explicitly asks to exercise release/tag-only heavy gates; `just verify` is for milestones and releases
- release performance evidence is informational by default and becomes blocking only when `CASA_RS_ENFORCE_PERF=1`
- slow CASA parity checks run when the approved work touches those concerns

## Done gate

Work is not complete until:

- the gates the change can affect pass (`just quick` or focused checks, plus CI)
- tests cover the claimed behavior
- reviewers checked for shallow or tautological tests on medium/high-risk work
- docs or ADRs were updated if reality changed
- any approved-scope deferral records explicit user signoff
- the PR was marked ready with the user's go-ahead (which enables auto-merge
  on green CI), and science, persistence, and interoperability changes also
  have an independent review

A user's "merge as-is" for a pull request waives its review and check gates.
Record the waiver on the pull request; do not describe waived evidence as
passing. The waiver changes process evidence only, never the accepted behavior
or persistent-interoperability contract.
