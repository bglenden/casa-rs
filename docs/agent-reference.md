# Agent Reference

Truth class: normative
Last reality check: 2026-07-18
Verification: just docs-check

This is situational guidance for agents. Read only the section relevant to the
task; root `AGENTS.md` remains the short always-loaded contract.

## CASA And C++ Oracles

- Local CASA/C++ task runs use
  `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python`, which
  has the locally built `casatasks` and `casatools` wheels installed.
- Before implementing CASA/casacore behavior, inspect the corresponding
  upstream source path. Use targeted instrumentation on both implementations
  for parity investigations.

### plotms PNG Export

For headless CASA `plotms` oracle PNGs, set:

```bash
DISPLAY=${DISPLAY:-:99}
QT_QPA_PLATFORM=${QT_QPA_PLATFORM:-offscreen}
MPLBACKEND=${MPLBACKEND:-Agg}
```

Then call `casaplotms.plotms` with `showgui=False`, `plotfile=...`,
`expformat="png"`, and `overwrite=True`.

The local macOS CASA build does not use Xvfb, but `plotms` still requires
`DISPLAY`. Do not run this CASA/Qt path in a shell sandbox that blocks `sysctl`
CPU-feature queries: Qt may mis-detect arm64 NEON and print `Incompatible
processor`. Use the normal user environment or an explicitly unsandboxed
command runner.

## Shared Data Roots

`TESTING.md` is the canonical resolver policy. Current workstation locations
are:

- Shared CASA C++ test data: `CASA_RS_TESTDATA_ROOT`, `../casatestdata`, or
  `~/SoftwareProjects/casatestdata`; long-gate preflight may select
  `/Volumes/home/casatestdata` when it contains the required paths.
- Tutorial parity data: `CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity/...` or
  `~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...`.
- Measures runtime data: an explicit root supplied to `MeasuresRuntime::open`,
  or a complete `CASA_RS_MEASURESPATH` / `~/.casa/data` candidate selected by
  the application before opening the runtime. Discovery never installs data.
- Small bundled real-MS CI fixtures: `crates/casa-ms/tests/fixtures/`.

Slow, release, parity, and tutorial gates run shared-data preflight and report
the selected root. Do not use `/private/tmp` as a canonical dataset location or
add personal workstation archives as implicit default-gate fallbacks.

## Durable Work Records And Checkpoints

Anything needed in a later turn, session, restart, comparison, or review belongs
in durable storage from creation. This includes input copies with generated
owner metadata, immutable configuration, reference executables/products, raw
measurements, logs, manifests, rejected hypotheses, and current handoffs.
Do not put these under `/private`, `/tmp`, a `tmp`/temporary directory, or a
safe-to-delete cache. Check both the supplied path and its resolved symlink
target. Disposable scratch and automatically deleted unit-test fixtures are
the only temporary-storage cases; neither may be the sole copy of evidence.

For travel work on this workstation, use
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/<work-item>/<run-id>/`.
This survives worktree removal and does not require GLENDENNING. Check free
space and permissions before launching. Large external-data gates still use
their approved dataset/evidence volume; local evidence does not waive them.

- Keep one current Markdown record with source/run identity, commands,
  outcomes, rejected hypotheses, acceptance gaps, and the next executable step.
- Keep controller-owned configuration/history unchanged in its working
  location. After each completed trial, and before pausing or cleanup, copy it
  and all restart-critical evidence to a new numbered checkpoint in the durable
  root. Never overwrite an older checkpoint or edit its embedded absolute paths.
- Capture source revision plus local patches, exact executable hashes,
  dataset identity/owner metadata, all five paired results, guard outputs, and
  references or an exact executable-backed reproduction recipe. Do not retain
  only the headline ratio or assume a future rebuild is the same binary.
- Generate a SHA-256 manifest after the copy, read every file back to verify it,
  and record missing artifacts explicitly. Publish a checkpoint as complete only
  after verification; keep failed copies visibly partial. A reconstructed
  narrative is never a substitute for lost raw measurements or frozen state.
- Maintain a second verified copy on an approved backup destination. A second
  folder on the same disk protects against accidental worktree cleanup, not disk
  loss. Report backup status as unverified until the exact checkpoint can be
  read back; do not assume Time Machine includes it. Uploading or publishing
  evidence still requires the applicable authorization and data review.

Before restarting, verify the manifest and immutable controls and inspect the
current controller status. If exact continuity cannot be restored, obtain
approval for a fresh baseline, preserving remaining trial allowances and retired
hypotheses. Never reconstruct controller events or weaken acceptance to recover.

## Release And Installation

- Smoke/release gate: `just smoke`
- C++ interoperability gate: `just release-cpp-interop`
- Performance evidence: `just release-perf`
- Slow parity: `scripts/test-slow.sh`
- Release: `scripts/release.sh <version-or-flag>`
- Local installs: `just install-local`, `just install-local-suite`, and
  `just install-local-gui`
- Release install: `just install-release <version>`

Use `TESTING.md` to decide when these heavier commands apply. Routine branch
merges do not run release/tag-only gates.

## TUI Evidence

For tutorial or regression evidence that needs `casars` TUI screenshots, use
`tools/ghostty-surface-capture`, not visible terminal/window screenshots. It
runs the TUI in an offscreen GhosttyKit surface with `TERM=xterm-ghostty` and
captures Kitty graphics and terminal cells from the renderer layer in one PNG.
