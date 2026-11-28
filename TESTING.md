# Testing Strategy

Truth class: normative
Last reality check: 2026-11-27
Verification: just docs-check (policy); applicable domain gates below (code)

## Test categories

1. Unit tests and doctests cover crate-local behavior and runnable API examples.
2. Default integration tests cover crate boundaries, data persistence, and CLI hosts.
3. Opt-in interoperability tests in `suites/casacore-interop` and
   `suites/casa-parity` compare storage and scientific behavior with independent references.
4. Packaging tests exercise editable Python, the installed wheel and sdist,
   and suite installation and command discovery.
5. Performance tests measure explicit workloads in `suites/perf` and
   the documented benchmark harnesses.

## Required discipline

- Every work item defines falsifiable acceptance checks and ships verification evidence.
- Bug fixes need a regression test that fails before the fix; new features
  need a demonstration test that runs them end to end against an
  independent reference (`AGENTS.md`, "Review").
- Changed behavior has matching tests or an explicit, justified exclusion.
- Cross-crate and boundary changes need integration or contract coverage.
- On-disk metadata or byte changes need the 2x2 interop matrix: RR, RC, CR, CC.
- Binary serialization changes need endian coverage.
- Observation-transaction tests prove the exact per-MS read and write sets,
  mechanically derived typed observation reads, mutation-before-read
  ordering, exact `ProductRequirements` staging coverage, mandatory
  transaction binding through `plan`, and fail-closed cancellation and
  admission, numerical and output failures.
- Product publication tests (ADR-0014) cover write-only bounded ownership,
  complete ordered coverage, unchanged science and metadata, and ordinary I/O
  errors. A failed publication leaves an incomplete output set to rerun, with
  no member recovery or whole-set rollback. Production imaging code neither
  hashes content nor rereads its outputs; test and benchmark fingerprints stay
  diagnostic.
- Measures-data dependent tests must skip cleanly when runtime tables are unavailable.
- C++ dependent tests must skip cleanly when `pkg-config casacore` is
  unavailable. They call casacore through the typed facades in
  `casa-test-support` (`CasacoreOracleRuntime`), which return
  `OracleError::Unavailable` in builds without the C++ backend.
- Tests and scripts that use shared CASA C++ datasets use the shared
  resolver policy: default fixtures may search `CASA_RS_TESTDATA_ROOT`,
  `../casatestdata`, and `~/SoftwareProjects/casatestdata`; long gates may also
  consider the shared `/Volumes/home/casatestdata` mirror after preflight. When
  a long gate declares required dataset paths, preflight selects the first
  available root that contains those paths instead of stopping at an
  incomplete earlier mirror.
- Tutorial parity datasets are registry-backed and staged separately under
  `CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity/...` or
  `~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...`; default gates
  must not select them implicitly.
- Heavy parity suites stay behind explicit opt-in gates such as `scripts/test-slow.sh`.
- Release-only Cargo integration suites stay out of the default compile path
  via explicit `[[test]]` entries and `required-features`, not only file-local
  `cfg` guards.
- Workspace gates run Rust tests through `scripts/test-workspace.sh`:
  `cargo-nextest` (one process per test, in parallel) plus `cargo test --doc`.
  The imager progress observer is process-global, so parallel tests in one
  libtest process could attach unrelated imaging work to a progress test;
  without nextest the script falls back to serial `cargo test`
  (`RUST_TEST_THREADS=1`).
- Opt-in suites live under `suites/` (`suites/casacore-interop`,
  `suites/casa-parity`, and `suites/perf` / `casa-perf-suite`). Because
  `[workspace] default-members` has no effect when `--workspace` is passed,
  `scripts/test-workspace.sh` passes explicit `--exclude` flags for the
  `suites/*` packages whenever `--workspace` or `--all` is used; invoke
  them explicitly (e.g., `cargo test -p casacore-interop --features cpp-interop-tests --tests`
  or `cargo test -p casa-perf-suite --release --features performance-tests --tests`).
- Developer-only binaries (`ms-read-probe`, `package_snapshot`, `synthetic-ms`)
  live in `dev/casa-dev-tools`, and shared C++/oracle infrastructure lives in
  `dev/casa-test-support`. Disposable test/oracle logs (including CASA Python
  probe logs) route under `target/test-logs/`.

## Mocking policy

- Prefer real fixtures and integration tests over internal mocks at crate boundaries.
- Do not mock workspace crates unless the alternative would make the test non-deterministic or prohibitively expensive.
- Mock third-party or process I/O only when the behavior under test is not the external tool itself.

## Commands and CI

- During development, use the cheapest meaningful regression/demonstration
  check. Before pushing a stable diff, run one affected validation batch as
  described in `AGENTS.md`, "Checks And Merging". Docs-only changes need
  `just docs-check`. CI supplies affected or workspace coverage; avoid
  repeating its green results locally. `just quick` reproduces the broad
  local gate. Use `just verify` for an explicitly required milestone or
  release.
- Imaging T1 end-to-end tier: `CARGO_INCREMENTAL=0 cargo test -p casars-cli --test t1`
- Performance and evidence tool tests (`suites/perf/imager`, pytest):
  `just perf-tools-test`. CI's `perf_tools` job enforces them; `just quick`
  runs them when the host's `python3` has pytest, numpy, scipy and
  matplotlib, and prints a skip notice otherwise. A tool whose casa-rs inputs
  are deleted goes with them
- Smoke/release gate: `just smoke`
- Blocking C++ interop release gate: `just release-cpp-interop`
- Informational release performance suite: `just release-perf`
- `just arch-check` includes source lengths, module layout, and dependency
  checks. Use `just docs-check` for documentation and generated references.
  Run `CARGO_INCREMENTAL=0 RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
  to build the Rust API documentation. For Linux docs.rs readiness, check
  `DOCS_RS=1 cargo doc -p casa-fft -p casa-imaging --no-deps` with FFTW installed.
- Use `cargo package --no-verify` with all libraries selected to package the
  unpublished dependency graph together. Inspect each `.crate` for required
  sources, its README, and registry size before publishing. Run
  `python3 scripts/test-rust-packages.py target/package` to build a standalone
  consumer from the archived manifests and sources. Pass crate names after
  the archive directory to split the build across hosts.
- Suite install gate: `scripts/test-install-suite.sh [--hide-checkout]`
  builds the release executables and wheel, installs the suite into a
  temporary root, and checks its commands on `PATH`, a task run from its
  command and from the suite's Python with the suite binaries hidden and
  an empty `PATH`. The script's `--hide-checkout` option also moves the source
  checkout aside during the installed checks.
- CASA task inventory: `python3 scripts/check-casa-inventory.py`, a step of
  `just arch-check`, needs no CASA. It checks the CASA 6.7.6.14 fixtures in
  `resources/casa-compat/casa-6.7.6.14/` and that
  `docs/reference/casa-compatibility.md` is current; `--write` regenerates
  the report after a surface changes. The reviewed semantics
  (`resources/casa-compat/semantics/`) pin evidence to exact commits, which
  must be on `main` or kept by the `evidence-pins` branch: a squash merge
  leaves a pull request's own commits reachable from its branch alone (its
  `EVIDENCE-PINS.md` says how to add one). CASA compatibility acceptance also
  needs `scripts/test-casa-inventory.sh`, which compares the fixtures with an
  installed CASA 6.7.6.14 (`CASA_RS_CASA_PYTHON` or `/Applications/CASA.app`)
  and fails, rather than skips, without it
- `just quick` includes `scripts/test-task-cli-hosts.py`, which builds and
  runs every one-shot task binary and checks the shared task front end's
  discovery, help, refusals, non-executing actions and exit classification
  at the process boundary
- Heavy parity suites: `scripts/test-slow.sh`
- Release/tag-only CI-like coverage: `scripts/run-coverage.sh --ci-like`
- `scripts/ci-local.sh pr` runs the broad Rust, Python package and installer
  jobs locally. `scripts/ci-local.sh tag` adds smoke and coverage. For selected
  Rust packages, use `scripts/test-workspace.sh -p PACKAGE` and repeat `-p`
  for additional packages.

### Local validation and exceptional CI

Opening, pushing or marking a PR ready does not start CI. Validate the
affected behavior locally and record the commands, results and required
review in the PR. Use the corresponding checks for affected interfaces and
consumers, including the CASA, casacore, performance and resource gates
required by the change. Merge after the evidence is green.

Dispatch hosted checks only when a required platform, CI workflow, toolchain,
packaging or integration result cannot be established locally, or when the
owner requests it. Name the missing evidence in the PR. Reuse any current
green evidence for the same coverage.

For an affected run on a PR branch in this repository, fetch its base and
head, then use the full SHA of their merge base.

```bash
gh workflow run ci.yml --ref <pr-branch> -f scope=affected -f base_sha=<merge-base-sha>
```

The affected run selects jobs from the diff against the supplied ancestor
and Cargo's direct dependency graph. Unknown paths, build configuration or
an incomplete package graph select full coverage. An invalid base fails the
run. Use `scope=static` for formatting, SPDX, architecture and docs checks
only, or `scope=full` when the named gap requires all jobs. Check the run's
event, tested head SHA, selected jobs and conclusion before merging with
`--match-head-commit`. For fork PRs that need hosted validation, use a
same-repository branch of the reviewed revision only with authorization.

Nightly CI tests `main` broadly. Version tags run that full suite plus smoke
and coverage. Release asset publication waits for all of those checks to
pass. The documentation site builds and publishes from `main` nightly
and can be refreshed with a manual run on `main`.

## Evidence source and turnaround

A required gate needs one current green result from either a local run or a
hosted run of the same command or documented equivalent coverage, not both.
Once one accepted environment is green, do not wait for or repair the other
solely to duplicate the result; use the second environment only to diagnose
a failure, resolve genuine platform-specific uncertainty, or satisfy an
explicit user request. A narrower hosted job cannot stand in for a broader
required gate such as `just verify`.

Follow AGENTS.md's "Continuation and Blockers" while checks run. Continue
actionable work in the assigned queue, verify required results and handle
reported failures. A passing local result does not waive investigating a
reported failure.

Green evidence remains valid until a code, test, build, dependency, or
runtime-configuration change could affect that gate. Review inspects the
commits after the tested revision and reuses the result when they contain
only documentation, planning, or workflow-policy changes; if executable
changes intervened, rerun only the affected gate.

## Domain gates

**Python package.** `cargo test -p casars-python` (which includes the
repository provider-conformance tests, `tests/provider_contract_conformance.rs`)
and `scripts/test-python-package.sh`, which runs the Python tests against an
editable checkout, then installs the built wheel and sdist into fresh
environments and checks them away from the source tree. The wheel smoke test
runs `flagdata` and `flagmanager` in-process with an empty command search path and an empty
Cargo target directory. The install-suite smoke test first exercises the
standalone commands, then hides the suite's binaries before invoking Python
from the installed suite. The package script runs the `casars.tools` tests
against the bundled measures snapshot unless
`CASA_RS_MEASURESPATH` names a runtime. The editable build enables
`transform-test-hooks`, `flagdata-test-hooks`, `imstat-test-hooks`,
`immoments-test-hooks`, and `solve-test-hooks` for the public SIGINT tests.
The features let the tests interrupt at observed staging or calculation
boundaries. Tests use small batches where they check that the next batch never starts.
Run the Rust staging concurrency checks with
`cargo test -p casa-mstransform --features transform-test-hooks --test api transform_cancellation::`. `test_binding_snapshots.py` pins what
the binding returns to Python; regenerate its fixture only for an intended
change.

**ATM.** A crate that ports a self-contained CASA C++ library is tested
against that library compiled locally, not only through CASA.
`casa-atmosphere` commits compact fixtures that
`tools/atmosphere/build-atm-oracle.sh` writes by building CASA's ATM sources
with a harness (`tools/atmosphere/atm_oracle.cc`) and with the port's
corrections of ATM's data errors (`tools/atmosphere/atm-corrections.patch`);
CI compares against the committed fixtures. Regenerate them only when the
harness's inputs, the corrections or CASA's ATM change, and record why on
the PR. `tools/atmosphere/at_tool_check.py` (run under CASA's Python)
compares the C++ built without the corrections with CASA's own `at` tool,
confirming that the compiled sources are the ATM that CASA ships, and the
port with the C++ built with them; rerun it when any of them changes.

**Upstream CASA image task suites.** With CASA 6.7.6.14's Python and the native Python package and task binaries
available, run the imported suite against both implementations:

```bash
CASA_PYTHON tools/casa-compat/upstream_image_suite.py imsubimage \
  --casatestdata /Volumes/home/casatestdata --out target/upstream-image-tests
```

Replace `CASA_PYTHON` with the installed CASA Python executable. The task
argument selects an imported upstream module, and `--implementation casa-rs`
or `--implementation casa` reruns one side. The runner uses the normal native
Python task entry point and keeps each implementation's fixture copy and
products isolated. The output directory retains logs and JUnit results.

**Upstream CASA image and coordsys tool suites.** The 12 Python modules from
CASA 6.7.6.14 can run through the public Python tools with the same runner
isolation and result reports:

```bash
CASA_PYTHON tools/casa-compat/upstream_image_tool_suite.py coordsys \
  --casatestdata /Volumes/home/casatestdata --out target/upstream-image-tool-tests
```

Use `--help` to list suite names and `--only` to select individual tests.
The casa-rs run replaces both `image` and `coordsys` so the upstream tests
can check the interaction between tools as well as standalone calls.

To compare the performance of the public tools, use the optimized native
Python extension and run the matched workloads under CASA 6.7.6.14's Python:

```bash
RAYON_NUM_THREADS=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
  CASA_PYTHON tools/casa-compat/benchmark_image_tools.py \
  --casa-rs-revision BUILD_COMMIT --output target/image-tools-benchmark.json
```

The benchmark uses the same masked Float cube and coordinate vectors in
both implementations. It checks the returned values, masks and coordinates
outside the timed interval, then records wall and process CPU samples in
alternating order. Use `--case` to repeat a specific workload.

**Upstream CASA MeasurementSet task suites.** The same runner supports the
imported MeasurementSet suites, with installed CASA 6.7.6.14 as the control.

```bash
CASA_PYTHON tools/casa-compat/upstream_ms_suite.py importvla \
  --casatestdata /Volumes/home/casatestdata --out target/upstream-ms-tests
```

`--only` takes a pytest case-name expression for a focused run. An omitted
filter runs the entire selected module. On a host with limited working space,
use `--fixture NAME` to stage only the inputs needed by the selected cases.
Repeat the flag for additional paths within that suite's fixture directory.
For timing, use release task binaries and `--trials 3`. Trials alternate the
implementation order. The JSONL output records public task call times separately
from setup and verification. Compare runs with passing assertions on both sides.

**Live calibration parity.** Run `cargo nextest run -p casa-parity --features
live-casa --test calibration_oracle` with `CASA_RS_CASA_PYTHON` pointing
to CASA 6.7.6.14 and `CASA_RS_TESTDATA_ROOT` containing
`measurementset/vla/ngc5921.ms` and `measurementset/alma/multiobs.ms`.
The tests stage writable copies of the source tables. Failures in CASA or
missing prerequisites fail the explicitly selected target.

**Simulator.** Its Metal law, `cargo nextest run -p casa-simulation --test api -E "test(backends::)"
--no-capture`, run outside the sandbox, predicts sessions on both
backends and prints each case's `max |metal − cpu| / max |cpu|` (the T0
limit is `1e-4`). Its casacore interop test, `cargo nextest run -p
casacore-interop --features cpp-interop-tests --test simulator_cpp_interop`,
needs `pkg-config casacore`: C++ casacore digests the MeasurementSets the
session writes, and the digests must equal casa-rs's. Its counting law
(`--test session_memory`) runs alone in a process of its own.

The C++ `tNewMSSimulator.cc` scenarios run natively in
`cargo test -p casa-simulation --test api upstream_cpp::`: independent constructor
lifetimes and antenna initialization for array sizes two through ten.
These tests need no C++ SDK at runtime.

`python/tests/test_simulator_upstream.py` in `frontends/casars-python` runs native
equivalents of CASA 6.7.6.14's simulator-tool cases. PLP prediction retains
the upstream analytic flux assertion. The upstream-disabled tropospheric
case checks 0.1 s frozen-screen sampling and existing `CORRECTED_DATA`
preservation, with a failing 10 s sampling control. Its original phase ratio
depends on CASA's random stream and the documented screen/wet-delay defects.
`tools/simulation/upstream_simulator_inputs.py` regenerates its compact
existing-MS input with CASA; the regular tests require only casa-rs.

`tools/simulation/upstream_simulation_suite.py`, run under CASA's Python,
executes an original simulation test file against CASA and the public native
Python surfaces, retaining the original assertions. Supply `--test-file`,
`--casatestdata` and `--out`; `--only` selects named cases. The report separates
passes, failures and skips. `--exercise-skipped` also runs disabled bodies
and returns failure if any still skip or fail; documented native counterparts
remain separate from these raw upstream outcomes.
`--implementation` selects `casa`, `casa-rs` or both for focused reruns;
`--align-epochs` applies the existing physical TAI/UTC observation control.

The pinned upstream inventory is 103 Python cases: simulator (2), simobserve
(93), simanalyze (6), and the multi-array/TP and simalma regressions (2).
Together with the three direct C++ source cases above, this is 106 cases.
The native `test_simobserve_upstream.py` and `test_simanalyze_upstream.py`
cover the exposed refusals, disabled-case intent, saved leakage equations
and combined IF/TP imaging. `upstream_simobserve_statistics.py` runs the
original noise bodies and independently checks their analytic radiometer
assertions, retaining seed-specific failures separately. Run it under CASA's
Python with the same `--test-file`, `--casatestdata` and `--out` arguments.
The existing `simanalyze_regression.py` and `simalma_regression.py` cover the
two full workflows; their completed comparisons can be reused.

`cargo test -p casa-simulation --lib simanalyze::memory` checks the full-size
combined case's admission geometry, axis-order handling and overflow refusal.
Feather FFT workspace is charged per direction plane and reused over channels;
flat-only completion does not reserve convolution or fidelity buffers.
The original 1458×1458×1×128 combined case is also checked on the Mac mini with
an explicit 13,130,571,776-byte reservation plus 128 MiB normal overhead, using
the evidence-only policy procedure in `docs/agent-reference.md`. Its native
products, including the feather cube, are read back with CASA C++. Preserve
the raw upstream `.image0` assertion failure separately: the actual TP path
is `.sd.image0`.

**simobserve.** `cargo nextest run -p casa-simulation --test api -E "test(simobserve::)"` runs
the task end to end on a small array without CASA. Its agreement with CASA
6.7.6's task is `tools/simulation/simobserve_parity.py`, run under CASA's
Python with the release `simobserve` binary, a casatestdata clone and
casa-atmosphere's `at_tool_quantities` example, over the VLA
protoplanetary-disk CASA Guide, CASA's ALMA regression on `alma.out20` and
one case for each mode. Each case compares the project's files, the pointing
files byte for byte, the images and calibrator list, and every
MeasurementSet through `compare_sm_ms.py`; its TAI − UTC shift comes from
CASA's measures.

**Component lists.** `casa-components` carries the 2x2 matrix for its tables.
`cargo test -p casa-components` reads the lists CASA 6.7.6's `cl` tool wrote
(`tests/data/casa-6.7.6`, made by `tools/simulation/componentlist_oracle.py
fixture`), writes them back in CASA's layout, and round-trips casa-rs's own
lists. casacore C++ reads what casa-rs writes in `cargo test -p
casacore-interop --features cpp-interop-tests --test component_list`, a
step of `just release-cpp-interop`. The CASA oracle is opt-in, because it
starts CASA's Python several times: `cargo test -p casa-parity --test
api component_oracle -- --ignored --nocapture` (CASA from `CASA_RS_CASA_PYTHON` or
`/Applications/CASA.app`).

**Image analysis.** `casa-imageanalysis`'s `regrid`, `convolve2d`, robust statistics,
`impbcor`, `immath` and `feather` are compared with CASA 6.7.6 in `cargo test
-p casa-parity --test api image_analysis_casa`, against the CASA results recorded
in `tests/data/image_analysis_casa.json` for the cases in
`tests/data/image_analysis_cases.json`. Regenerate the results with
`tools/images/image_analysis_oracle.py`, run with CASA's Python, whenever the
cases change.

**Frozen imaging oracles.** `scripts/test-slow.sh` explicitly selects the
moving-field phase-centre and MFS envelope tests that use recorded CASA
values. Product comparisons with separately frozen workstation artifacts
remain opt-in. Use `cargo test -p casa-parity --test api -- --ignored --list`
to list their exact names. The `imaging-t31-multidomain-geometry`,
`imaging-t41-moving-source-casa`, `imaging-t44-mtmfs-products` and
`imaging-t59-low-memory` recipes pass the frozen prefixes to the matching
tests. For T34 and the representative full-Stokes and outlier comparisons,
run the exact `casa-parity` test with `--ignored --exact` and the input
and product paths named by that case. The default `casa-parity` test
invocation does not execute these product comparisons.

**importvla.** The CASA comparisons need `--features cpp-interop-tests` and a
CASA Python (`casa_test_support::discover_casa_python`), and write temporary
MeasurementSets under `target/`. `cargo test -p casacore-interop
--features cpp-interop-tests --test cpp_import_ms_parity` imports the archive `CASA_RS_IMPORTVLA_ARCHIVE`
names with both; `--test cpp_import_ms_parity_matrix -- --ignored
--nocapture` runs the known real archives (the AG189 revision 11 and 12
exports that the test's `CASA_RS_IMPORTVLA_PARITY_*` variables name, and
casatestdata's revision-26 `AS758` cases); and `--test real_importvla_parity
-- --ignored --nocapture` also compares selected persisted metadata. `cargo
test -p casa-vla --test api real_archive_inventory:: -- --ignored --nocapture`
summarizes the modes and revisions of the archives in
`CASA_RS_IMPORTVLA_ARCHIVES` before new matrix cases are chosen.

**Large spectral cubes.** Storage changes additionally run the sparse
logical-capacity test and the explicit release throughput guard:

- `cargo test -p casa-images fifty_gib_spectral_cube_streams_selected_planes_with_bounded_storage -- --nocapture`
- `CASA_RS_CUBE_PERF_DIR=/path/to/volume cargo test -p casa-perf-suite --features performance-tests --test images_perf_vs_cpp full_spectral_cube_plane_io_tracks_raw_disk_speed --release -- --ignored --nocapture`

The throughput guard writes and reads every plane of a 50 GiB raw baseline
and a 50 GiB tiled image on the selected volume, which needs at least 110 GiB
free. Image writes must reach at least 60% and reads at least 50% of the
equivalent raw-file path on the same volume and run.

## Imaging tiers and evidence

Imaging tests run in four tiers, which do not substitute for one another: a
law localizes a defect but proves no agreement with CASA, a green toy
end-to-end test cannot make a T2 row representative, and resource evidence
does not stand in for a scientific comparison.

- **T0, laws** (each under a minute; CI): adjoint, PSF, sumwt, worker
  invariance, precision and kernel-set laws on in-process synthetic
  MeasurementSets, in each imaging crate's tests (`t0_laws`, `aw_laws`,
  `wplanes_laws`, `mosaic_laws`, `resample`, `predict`, `weighting`,
  `api::pass_laws`, the deconvolution solver laws), and the Metal-against-CPU
  laws (`cargo nextest run -p casa-imaging-metal`).
- **T1, end to end** (under ten minutes each; CI):
  `cargo test -p casars-cli --test t1` through the production route, one
  module per capability (standard MFS and masks, cube, mosaic, W and AW
  projection, MT-MFS, model column, Metal, routes, interrupt). The store
  law holds a run's batch geometry in its MeasurementSet equal to its batch
  geometry in the MSv4 conversion, every sample on every channel, to 1e-12,
  in either channel order (`casa_imaging::store_law` tests).
- **T1.5, CASA-paired small fixtures** (local, before an imaging review):
  the casatestdata `refim_*` and kernel fixtures against CASA 6.7.6
  references, NRMS ≤ 1e-3 on every product and `.sumwt` within 5e-5. The
  harness lives outside the repository with its references
  (`docs/agent-reference.md`). T1 checks casa-rs against itself; a
  convention shared by casa-rs's simulator and its operator cancels there,
  so a gridding, kernel or normalisation change is not done until its T1.5
  rows agree with CASA.
- **T2, representative** (hours; when a row's code changes): the matrix in
  `resources/imaging-architecture/representative-science-matrix.json` run by
  `suites/perf/imager/run_workload.py`, the timed checkpoints and the rows
  judged against CASA's own time. The #648 results table records the
  accepted figures and their exceptions.

**T2 rows.** A row normally has at least a 512x512 image and 1,000,000
selected correlation-channel samples, uses the production route, and
exercises the mode's load-bearing dimensions: full Stokes requires four
correlations with meaningful cross-hand flags and weights; a cube at least
sixteen meaningful channels; a mosaic overlapping pointings; MT-MFS a
meaningful multi-SPW frequency lever arm. Each row checks every relevant
product, exact topology, metadata and validity, NRMS no greater than 0.001
where applicable, and independent beam, flux, centroid, spectral,
polarization, mask/support, and persistence facts. A recorded exception must
explain why the dimension is scientifically inapplicable and requires the
owner's approval. Approved exceptions are recorded on their rows in the
matrix (`validity_rule`).

CASA oracles are generated once and frozen with their CASA version, dataset
identity and selection, image and solver parameters, comparator, and artifact
inventory. Regenerate one only when one of those contracts changes. Tiny
fixtures remain required where they localize failures faster.

**Review triggers.** Nonlinear deconvolution can amplify numerical-scale
near-ties into different component histories. A mode-specific gate may
classify cross-run pixelwise agreement limits as *review triggers*, rather
than silently widen them or require bitwise component identity, only when
the owner approves a versioned replacement rule. Structural and
deterministic-product checks, independent convergence, resource limits, and
relevant scientific checks remain hard. A trigger is unsuccessful until a
reviewed disposition binds the exact input/comparison, gate version, all
affected products and planes, unchanged measurements, visual evidence,
rationale and owner decision. Unexplained localized defects, known
implementation errors, missing evidence and stale review identities fail
closed. Approvals and waivers bind the output or run they name: similar
outputs may reuse the review procedure, never another output's approval.
The measurement pass runs once per unchanged product set; the owner's review
finalizes its saved, identity-bound report without reopening product arrays
or repeating scientific calculations. A changed product or rule requires a
new measurement.

**AW projection.** Its laws are `casa-imaging-operator`'s `aw_laws` (cell
keys, cold and warm catalogs, native generation against the CASA cell rules)
and T1's `aw_projection` cases; its CASA evidence is the T1.5 AW rows and
the T2 native-AW row. The frozen-cell tests require explicit surface and
catalog paths; no CASA installation or model-data path is discovered
implicitly.

**Metal.** The Metal backend's gate is T0 Metal versus CPU
(`cargo nextest run -p casa-imaging-metal`), the pass laws' `metal` and
residency laws, the application's `continuum_application metal::` cases and
T1's `metal::` cases, each comparing Metal with the CPU on the same input.
On a host without a unified-memory Metal 3 device these laws print
`skipped: no Metal device` and pass, so a Metal gate must run outside command
sandboxes on Apple silicon; a skip is not evidence that a Metal workload ran.

**Memory is a tested contract.** Every live phase is admitted
(`casa_imaging_runtime::admit`), and an owner that changes what it keeps
adds a counting law: a test that runs the owner and asserts its measured
allocation stays within the bytes it charges (for example
`the_batch_source_holds_at_most_its_charge` in `casa-imaging`).
The allocator counts the whole process, so a law in a binary with other
tests first calls the binary's `alone`, which reruns the law in a process of
its own, as nextest runs every test. At T2, a run's peak footprint must stay
within its largest reservation plus the process's fixed overhead; the run
summary's per-phase `peak_reserved_bytes` and `/usr/bin/time -l` give both.
On macOS the footprint drops up to about a second after memory is freed, so
a phase boundary is judged after a short idle, or with that lag counted in
the allocator allowance. A measured live allocation above its reservation is
fixed with a regression law, never documented as harmless.

**Every evidence run names its conditions.** Defaults and binary labels are
not enough: a build linked against the scalar FFTW, or a harness that lost
its CPU pin, silently changes what was measured. A recorded run states:

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

## Long performance runs

A performance run projected over 30 minutes uses a checked-in workload
harness or campaign controller and produces a launch receipt. Beyond the
conditions above, the receipt binds the run and candidate identity, the
binary hash, fixture and reference identities, a same-revision
mode-faithful turnaround receipt, the scientific tolerances, a stage
forecast, a hard wall limit, the memory, disk, swap and other resource
ceilings, exclusive host ownership, and the artifact-retention class.

Only one long performance run may be active on a host. A terminal receipt
classifies it as passed, scientifically failed, resource-aborted,
rate-aborted, fixture-invalid, or execution-failed before another candidate
launches. Log activity alone is not progress: the controller reports
completed and total stage units, updates a conservative completion forecast,
and aborts when that forecast exceeds the declared wall ceiling.

A run projected over 90 minutes is final evidence, not a diagnostic. After a
long-run failure, the same stage is reproduced by a bounded probe before
another long run. Two failed full-resolution launches in one campaign
require an explicit continuation, fallback, or waiver decision. Executable
or runtime-configuration changes invalidate prior turnaround authorization,
but unchanged accepted reference products and CASA baselines remain
reusable.

Complete generated products are retained only for accepted evidence, the
first unresolved scientific failure, or an explicitly named forensic case.
Rejected or diagnosed runs retain compact provenance, logs, telemetry,
comparisons, and hashes; their generated products, spill files, frozen
binaries, and workspaces then become cleanup candidates. Cleanup is
fail-closed and never removes an active run or an unresolved first
scientific failure.

## Coverage

- Version-tag CI enforces 75% line coverage (`scripts/run-coverage.sh --ci-like`).
- Local goal is at least 78% to preserve a safety margin.
- Explain coverage regressions rather than hiding them.
- Do not trade meaningful behavioral tests for raw numeric coverage.
- Do not run `scripts/run-coverage.sh --ci-like` for routine branch merges
  unless the user explicitly asks for release/tag-level validation or direct
  reproduction of the heavy coverage gate.

## Releases and waivers

- The local release command runs the smoke, blocking C++ interop, suite install
  and slow parity gates. Version-tag CI adds full hosted checks and coverage.
  Use `scripts/release.sh --full` for local coverage reproduction and the
  informational performance suite.
- Release performance evidence is informational by default and becomes
  blocking only when `CASA_RS_ENFORCE_PERF=1`.
- Slow CASA parity checks run when the work touches those concerns.

An owner's "merge as-is" waives a pull request's review and check gates
(`AGENTS.md`). Do not describe waived evidence as passing. The waiver changes
process evidence only, never the accepted behavior or the
persistent-interoperability contract.
