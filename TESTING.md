# Testing Strategy

Truth class: normative
Last reality check: 2026-07-15
Verification: just verify

## Test categories

- Unit tests: pure value, parsing, math, and domain logic inside a single crate
- Integration tests: cross-crate behavior, on-disk persistence, contract boundaries, and CLI surfaces
- Interoperability tests: Rust/C++/CASA read/write matrices when on-disk bytes or metadata matter
- Packaging/runtime tests: editable Python package checks, wheel-install smoke checks, installer validation, demos, and suite layout checks
- Performance tests: smoke benchmarks and parity timing when performance is part of the requirement
- Native GUI interaction tests: application-hosted XCTest/XCUIAutomation flows
  that launch `casars-mac`, address controls through stable accessibility
  identifiers, and verify critical user workflows against deterministic fixture
  adapters

## Required discipline

- Every wave defines falsifiable acceptance checks and ships verification evidence.
- Bug fixes need regression tests.
- Cross-crate and boundary changes need integration or contract coverage.
- On-disk metadata or byte changes need the applicable 2x2 interop matrix: RR, RC, CR, CC.
- Binary serialization changes need endian coverage.
- Measures-data dependent tests must skip cleanly when runtime tables are unavailable.
- C++ dependent tests must skip cleanly when `pkg-config casacore` is unavailable.
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

## Mocking policy

- Prefer real fixtures and integration tests over internal mocks at crate boundaries.
- Do not mock workspace crates unless the alternative would make the test non-deterministic or prohibitively expensive.
- Mock third-party or process I/O only when the behavior under test is not the external tool itself.

## Default commands

- Fast local gate: `just quick`
- Full default wave gate: `just verify`
- Smoke/release gate: `just smoke`
- Blocking C++ interop release gate: `just release-cpp-interop`
- Informational release performance suite: `just release-perf`
- Release-only install gate: `scripts/test-install-suite.sh`
- Heavy parity suites: `scripts/test-slow.sh`
- Release/tag-only CI-like coverage: `scripts/run-coverage.sh --ci-like`
- GitHub Actions reproduction: `scripts/ci-local.sh pr` for pull-request jobs or `scripts/ci-local.sh tag` for version-tag jobs
- GitHub PR CI: lint/test plus editable Python package checks
- GitHub tag CI: PR CI plus smoke, suite-install, and CI-like coverage

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
- Native macOS GUI prototype and frontend services:
  `cargo test -p casa-notebook --test wave1_contract`,
  `cargo test -p casars-frontend-services`,
  `cargo test -p casars-imager dirty_imaging_json_request_accepts_gui_selection_fields`,
  `scripts/test-frontend-services-python.sh`, `swift test` from
  `apps/casars-mac`,
  `swift run casars-mac --dump-debug-state --show-prototype python
  --prototype-state happy-path` for the fixture-only Wave 2 Python projection,
  and
  `CASARS_IMAGER_BIN=target/debug/casars-imager swift run casars-mac
  --dump-debug-state --simulate-main-flow --open-project <fixture-or-project>`
  for the headless debug-state smoke path that includes the dirty-imaging task
  run.
- Native macOS launched-app interaction gate: `just gui-test` from the
  repository root. Its disposable DerivedData and retained `.xcresult` bundle
  live under `apps/casars-mac/.gui-test/`.

Local native GUI automation owns an exclusive foreground window. `just
gui-test` performs all compilation first, announces a countdown, then runs the
complete XCUITest suite in one `test-without-building` batch. Do not use the
Mac during that window. During implementation, prefer core Swift tests,
debug-state assertions, and deterministic capture; batch user-visible
interaction changes and run the GUI suite at coherent prototype-handoff and
pre-Review checkpoints. Use isolated focused XCUITest runs only to diagnose a
failure found by a consolidated run, not as the normal edit loop.
The deterministic assistant journey includes logout, the signed-out composer,
and fixture reauthorization before continuing normal chat; real credentials are
never required by `just gui-test`.

For unattended development, `just gui-test-remote` runs the same gate on a
dedicated, logged-in macOS worker and leaves Xcode, Cargo, and result artifacts
on that worker. Set `CASA_RS_GUI_TEST_REMOTE=user@host`; optionally set an SSH
identity, checkout, storage root, Xcode developer directory, or Python with the
`CASA_RS_GUI_TEST_REMOTE_*` variables listed by
`scripts/test-gui-remote.sh --help`. The local checkout must be clean and its
HEAD must be the pushed tip of the same-named origin branch. The runner refuses
an uninitialized Xcode installation or a dirty remote checkout, switches the
dedicated checkout to the exact requested commit, stores build state outside
the checkout (with an ignored `target` link for the Xcode project's existing
linker contract), reuses incremental Xcode build state, and reports the remote
artifact path. By default the checkout and Xcode DerivedData live on the
worker's internal disk so regenerated app bundles do not repeatedly request
removable-volume access; the large Cargo target and retained artifacts remain
on configured external storage. Use the remote worker as
the normal exclusive GUI surface when available; a current green local or
remote run satisfies the single GUI gate. The worker needs full initialized
Xcode, Developer Tools mode, SSH access, and a real logged-in console session;
automatic login is a worker-provisioning choice rather than a repository
requirement. The console must be unlocked when the gate starts. The runner
holds a test-scoped `caffeinate` assertion while it owns the GUI window; it does
not permanently disable the worker's normal screen-lock policy. For setup or
failure diagnosis only,
`CASA_RS_GUI_TEST_REMOTE_ONLY=TestTarget/TestClass/testMethod` selects a focused
test without changing the normal consolidated-gate policy.

`just assistant-live-gui` is the opt-in real-account acceptance exception. It
uses the installed Codex CLI's existing ChatGPT subscription, an isolated
temporary project, and the user-selected Python; API-key environment variables
are removed before launch. It verifies a real exact-identity CASA MCP call,
in-flight cancellation, durable transcript state, full app restart, and either
same-backend-session resume or an honest visible handoff followed by a fresh
backend turn. The command is agent-pre-runnable, retains
`apps/casars-mac/.gui-test/AssistantLiveGUI.xcresult`, and is not part of CI or
the deterministic `just gui-test` contract. On failure it retains and
production-decodes the disposable transcript for diagnosis. The local
foreground harness hides and later restores Codex during the exclusive test
window.

`just notebook-roundtrip-gui` is the broader opt-in production-science
acceptance for issue #417. It uses the existing ChatGPT subscription, the
repository's real `simobserve` and MeasurementSet plot helpers, and the
user/system Python selected by `scripts/resolve-python.sh` (override with
`CASA_RS_GUI_TEST_PYTHON`). It removes metered API variables, builds every
helper before taking the exclusive foreground window, and drives a disposable
project through cited agent retrieval, a canonical typed task suggestion, real
task execution, one intentional Python failure and retry, two Python plot
revisions, two explorer-plot revisions, and two full application restarts. A
successful run removes the disposable project and retains the `.xcresult` plus
the sanitized `NotebookRoundTripGUI.report.json` under
`apps/casars-mac/.gui-test/`. A failed run retains the project for focused
diagnosis. This live acceptance is not part of CI or `just gui-test`.

`just notebook-roundtrip-gui-remote` runs that opt-in live acceptance on the
same remote worker. It additionally requires the worker's Codex CLI to be
logged into an approved ChatGPT subscription and its selected Python to provide
NumPy and Matplotlib. On success the full artifacts remain on the worker's
configured storage and the sanitized JSON report is copied into
`apps/casars-mac/.gui-test/remote/` locally.

## Coverage / confidence policy

- CI enforces 75% line coverage.
- Local goal is at least 78% to preserve a safety margin.
- Explain coverage regressions rather than hiding them.
- Do not trade meaningful behavioral tests for raw numeric coverage.
- Do not run `scripts/run-coverage.sh --ci-like` for routine branch merges unless the user explicitly asks for release/tag-level validation or direct reproduction of the heavy coverage gate.

## Native macOS executable GUI testing

XCTest/XCUIAutomation is the canonical end-to-end test layer for user-visible
`casars-mac` behavior. Issue #368 establishes a thin Xcode app host around the
existing Swift package sources, a macOS UI Testing Bundle, and the stable
`just gui-test` command. This gate must be implemented and green before Wave 1
production adapters are connected, lands with the completed Wave 1 PR, and
remains required for later user-visible GUI waves.

The executable GUI layer follows these rules:

- Launch deterministic fixture states with `XCUIApplication.launchArguments`.
  Production-boundary persistence tests may create and remove a unique
  test-owned temporary project; never open user projects, contact providers or
  networks, run scientific tasks, or leave project/notebook data behind. The
  separately invoked `just assistant-live-gui` acceptance may contact the
  subscribed provider only under its explicit opt-in environment gate and
  still uses and removes a unique temporary project.
- Select normal controls through stable accessibility identifiers. Coordinate-
  only automation is not an acceptable default path.
- Cover the smallest critical set of complete user workflows: editing and
  focus, view-mode changes, disclosure/gesture routing, tab navigation,
  conflict decisions, cancellation/retry, and isolation from production routes.
- Attach screenshots and useful accessibility diagnostics on failure, but do
  not use screenshot review as the only assertion that an interaction works.
- Keep Core/store tests as the broad, fast base of the pyramid. UI tests prove
  only behavior that requires the launched application boundary.
- Keep the same `just gui-test` command available locally and in the supported
  macOS CI job. One green consolidated run in either environment satisfies the
  interaction gate; the other is optional unless needed for diagnosis or
  platform-specific evidence. If neither environment can support UI automation,
  stop and record the blocker rather than replacing the gate with manual or
  computer-use testing.
- Pin the CI job's Xcode selection explicitly; the current gate uses the
  `macos-15` image with Xcode 26.2, matching the locally established compiler.
- Run locally from a logged-in GUI session with Xcode automation permission and
  no active system-authentication prompt. Diagnose failures from
  `apps/casars-mac/.gui-test/CasarsMacUITests.xcresult`, which retains the
  screenshot and accessibility hierarchy attached by the failing workflow.
- Accessibility-audit exclusions must identify a specific framework/OS
  artifact or a verified false positive. They may not blanket-exclude an audit
  category or newly introduced actionable controls.

## Wave expectations

For each wave:

- scientific-notebook waves first pass the ADR-0007 prototype gate: the live
  fixture-backed app covers primary, failure, cancellation, retry, and restart
  states; meaningful controls have accessibility IDs; debug JSON and visual
  evidence are recorded; and explicit interaction approval precedes real
  adapter integration
- user-visible native macOS GUI changes identify critical XCUITest workflows
  during shaping and record a green local or hosted `just gui-test` result
  before Review; for
  Wave 1, #368 must be implemented and green before Phase B begins and lands
  with the completed wave
- after prototype approval, real adapters must match the accepted interaction
  and state contract; deterministic fixture adapters remain available for
  regression tests and may not be treated as evidence that persistence,
  execution, download, Python, retrieval, or model integration works
- Wave 1 notebook contract tests use disposable absolute project roots and
  cover byte-preserving Markdown/future cells, atomic default/named saves,
  explicit external-edit reconciliation, concurrent immutable revisions,
  active-attempt leases and interrupted recovery, replay drift, export
  boundaries, GUI/TUI/CLI/Python recording and one-run bypasses, production
  debug state, authored task cells without receipts, clean/dirty task-tab
  parameter replacement, and launched-app persistence/task-tab interaction
- Wave 2 Phase A XCUITests cover plot regeneration and insertion, latest-first
  execution status with routine streams, paths, failures, and prior revisions
  collapsed by default, explicit producer-declared plot/image aspect behavior,
  explorer snapshot
  enlargement and parameter restoration, no live notebook mutation, New plot
  and immutable Update actions, observable running state, ordered failure
  output and retry, a nonresponsive cell's interrupt/restart path, exact-source
  AI approval invalidation after editing, accessibility audit, and zero
  production-boundary calls. Core tests cover the same fixture state
  transitions and exact source digests. These tests do not claim that Python,
  project persistence, matplotlib, or production explorer integration exists.
- Wave 2 Phase B adds production evidence for exact-source receipt-v2 Python
  execution, compact-by-default ordered output and immutable artifacts
  surviving a fresh notebook
  load, persistent namespace plus interrupt/restart recovery, tutorial-backed
  native MeasurementSet and image-plane data with
  editable Matplotlib/WCSAxes objects, and Rust-owned explorer snapshot New,
  Update, immutable history, and exact reopen intent. Launched-app coverage
  executes a production Python cell from a disposable project and verifies its
  on-disk receipt and reloaded output; the fixture XCUITests remain the
  deterministic interaction evidence for explorer New/Update/Open behavior.
- Wave 3 Phase A uses only package-internal deterministic tutorial fixtures.
  Core tests cover explicit approval, legal Download/Verify/Unpack/Ready
  transitions, generation-bound cancellation/resume/restart, stale completion
  rejection, offline/checksum/unsafe-archive/disk failures, retry recovery,
  section progress, learner annotations, and the rule that only Ready is
  staged. XCUITests cover the launched learner-notebook flow, complete approval
  facts, compact failures, cancellation/resume, recovery, direct task-parameter
  loading with identified tutorial overrides, accessibility, and zero
  production-boundary calls. Opening or selecting a tutorial never starts
  acquisition, and Phase A does not claim
  a v1 manifest, persistence, network, checksum, extraction, or task adapter.
- Wave 3 Phase B uses Rust contract tests for immutable template forking,
  one-shot v0 migration, URI registry policy, exact approvals, digest pinning,
  receipt recording, file and loopback-HTTP redirect/range acquisition,
  cancellation/resume/restart generations, offline reopen, disk/checksum/
  network/destination failures, optional checks, and bounded archive rejection.
  Swift/UniFFI tests reopen the same Rust-owned state. One production XCUITest
  forks a test-owned template into a disposable project, approves a local file
  acquisition, waits for Ready, and opens the normal task tab with accessible
  tutorial-override markers; it uses no user data or external network.
- Revised Wave 4 Phase A uses only package-internal Codex-agent, account,
  usage-window, reasoning-effort, authority, Python-environment, context,
  citation, activity, corpus, response, and recovery fixtures. Core tests cover
  agent/model/effort selection, Explore/Work/
  Full-access transitions, explicit Full-access confirmation, user Python
  selection, cited completion, rate-limit retry, nonresponsive cancellation/
  restart, canonical task opening, chronological notebook-tail insertion,
  debug projection, and zero production calls. XCUITests start from a
  full-width notebook, activate the contextual chat drawer from the purple
  lower-right sparkle, use a normal
  free-form composer, expand and dock the same fixture conversation without
  losing its draft or state, inspect context available through the trusted CASA
  MCP fixture, expand otherwise-collapsed agent activity, add a selected answer
  once at the notebook tail without a redundant dialog, and open task
  parameters directly in the canonical task tab with non-defaults highlighted.
  The fixture does not claim an exact
  model-prompt or provider-egress manifest. It also covers always-visible model,
  reasoning-effort, and compact usage-remaining controls; consolidated
  agent/subscription/access/Python settings; stable accessibility identifiers;
  and zero production-boundary
  calls. Suggested prompts may fill but never submit the composer. Superseded
  provider-sidecar, predetermined-question, duplicate proposal-card, and exact-
  egress fixtures are not Phase A approval evidence. Phase A does not claim
  authentication, agent startup, MCP, network, retrieval, Python execution,
  task execution, download, notebook persistence, or transcript persistence.
- Wave 4 Phase B uses Rust contract tests for agent-neutral atomic transcripts,
  immutable pin hashes/provenance, single-owner insertion/execution approvals,
  incremental SQLite/FTS5 corpus refresh, exact cited chunks, and cleared
  baseline paths. Adapter tests exercise official Codex App Server JSON-RPC,
  opt-in ChatGPT subscription login/account state, cancellation, process
  failure, rate limits, and session resume without exposing credentials to
  CASA-RS. Conformance tests verify `casa-rs-agent-profile/v1`, nonce-bearing
  nonce-derived CASA MCP identity and collision resistance, capability mapping,
  Explore denial of project instructions/write/exec/network and inherited
  MCP/plugin tools, Work native approval
  events without a duplicate CASA prompt, and visible failure when an authority
  dimension cannot be honored. Python tests use the user-selected or inherited
  executable and record identity changes without path or executable-hash
  invalidation. Corpus tests cover baseline/project/release/live-source FTS
  retrieval and exact citations. The consolidated `just gui-test` run covers
  drawer/tab preservation, Return/Shift-Return, context-availability and
  authority disclosure, citations, collapsed activity, one direct notebook-
  tail append with no second confirmation, canonical task routing,
  cancellation/retry, and resume. Live
  Codex smoke is explicit, uses the user's existing ChatGPT subscription, and
  never requires or accepts a metered API key. Per the project verification
  policy, one current local or hosted GUI result is sufficient and duplicate
  assurance is not required.
- Wave 5 project-corpus maintenance tests prove that a no-change refresh reads
  metadata but performs zero content reads, PDF extractions, and OCR calls;
  preserved-mtime atomic replacement is still detected; schema-v2 multi-page
  citations migrate without stale pages; and extraction failures retain the
  previous usable index. Native Swift tests exercise startup reconciliation,
  automatic edit refresh, recursive watcher recovery when `documents/` appears,
  and debounce/coalescing for bursty filesystem events. Rust contract tests
  cover add, edit, rename, delete, unrelated-document preservation, and retry
  behavior. Corpus watcher tests are headless core tests and do not take GUI
  focus.
- Wave 5 standard-corpus tests load the committed pack without an Oracle
  checkout or network access, verify all 2,314 external pages/slides plus the
  primer, reject a tampered content digest, retrieve a visually checked 2026
  slide with its exact citation, and prove a baseline-version replacement
  preserves project documents and assistant conversations. The docs check
  validates all 55 inventory decisions, the 28 selected bundle files and
  digests, the 2,314 page/slide count, and the recorded authoritative-origin
  audit.
- Wave 5 production round-trip acceptance runs only through the explicit
  `just notebook-roundtrip-gui` opt-in. It proves a cited live response is
  appended exactly once, a typed `simobserve` suggestion decorates only the
  proposed non-defaults, a real synthetic MeasurementSet and receipt-v2 record
  survive reload, the selected Python identity and exact source hashes survive
  one failed and two successful immutable revisions, and a changed
  MeasurementSet plot retains two assets plus reopen provenance. It terminates
  and relaunches production `casars-mac` twice, reopens the latest plot directly
  in the Plot explorer surface, and verifies that opening recorded task
  parameters does not rerun the task. Focused core tests retain the managed rich
  output cell and explorer-reopen regressions without requiring a provider or
  foreground window.
- acceptance checks have direct verification evidence
- changed behavior has matching tests or explicit justified exclusions
- medium/high-risk work gets architecture review and test-adversary review
- code waves record a bounded `refactor` pass on the involved code before
  review, or a not-applicable rationale for no-code waves
- reality-sync happens when docs, interfaces, or boundaries changed
- approved outcome, included issues, and acceptance checks are not deferred or
  descoped without explicit user signoff recorded in the issue or PR
- release work also runs the smoke gate, the blocking C++ interop gate, and the suite-install gate; CI-like coverage remains a version-tag CI gate and is run locally only for `scripts/release.sh --full` or explicit coverage reproduction
- ordinary non-release merges stay on `just verify` plus targeted tests unless the user explicitly asks to exercise release/tag-only heavy gates
- release performance evidence is informational by default and becomes blocking only when `CASA_RS_ENFORCE_PERF=1`
- slow CASA parity checks run when the wave touches those concerns

## Done gate

A wave is not done until:

- `just verify` passes or any intentional exclusion is called out explicitly
- tests cover the claimed behavior
- native macOS GUI waves pass `just gui-test` locally or in hosted CI for
  changed critical interactions
  once the #368 test target is present
- code-wave refactor evidence is recorded, or a no-code not-applicable
  rationale exists
- reviewers checked for shallow or tautological tests on medium/high-risk work
- docs or ADRs were updated if reality changed
- any approved-scope deferral records explicit user signoff
