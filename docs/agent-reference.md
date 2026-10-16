# Agent Reference

Truth class: normative
Last reality check: 2026-10-16
Verification: just docs-check

This is situational guidance for agents. Read only the section relevant to the
task; root `AGENTS.md` remains the short always-loaded contract.

## CASA And C++ Oracles

- Local CASA task runs use the CASA 6.7.6 release's Python,
  `/Applications/CASA.app/Contents/MacOS/python3`, which has `casatasks` and
  `casatools`. Scripts take `CASA_RS_CASA_PYTHON` to use another one.
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

## Storage Locations

Follow the storage rules in `AGENTS.md`. Current locations:

- Internal disk (small; keep at least about 40 GB free): source, builds, and
  small durable evidence for quick tests under
  `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/<work-item>/`.
- NAS `storage` over Tailscale, mounted at `/Volumes/home`: large or
  long-lived artifacts under `/Volumes/home/casa-rs/`. Shared CASA test data is
  at `/Volumes/home/casatestdata`.
- External disk `GLENDENNING` at `/Volumes/GLENDENNING/`: fast working space
  for large datasets and runs. It has had repeated disconnects, so it is never
  the only copy of anything that matters. Its preserved datasets are mirrored
  at `/Volumes/home/casa-rs-glendenning-preserve/GLENDENNING/` (see
  `/Volumes/GLENDENNING/README.md` for the path mapping).

Keep an MS or large output only when it will be reused and would take hours to
regenerate: a fixed timing input, or a long CASA run used as a correctness
reference, kept with the exact MS it ran on. Record its generator, parameters
and timings in a manifest beside the archive. T55's manifest is
`/Volumes/home/casa-rs-glendenning-preserve/GLENDENNING/casa-rs-evidence/t55/GOLDEN-DATASETS.md`.

Delete development artifacts once the code has moved past them. For a long
benchmark campaign that must survive restarts, keep one current Markdown record
(source revision, commands, results, next step) next to its evidence on the NAS
and replace superseded runs rather than accumulating them.

## Workstation Toolchain

- Keep Xcode and the Command Line Tools on the same major version. If `cc`
  links fail with `tapi error ... unknown architecture` (for example while
  building `fitsio-sys`), `xcrun` is resolving a newer Command Line Tools SDK
  than Xcode's linker understands: update Xcode, or set
  `SDKROOT=$(xcode-select -p)/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk`.
- After an Xcode update, run `sudo xcodebuild -license accept` and
  `sudo xcodebuild -runFirstLaunch`; until then `git` and `cc` fail.
- FFTW: `casa-fft` finds FFTW through pkg-config, so a plain build links
  Homebrew's FFTW, which has no NEON kernels. Releases build the SIMD FFTW
  with `scripts/build-fftw.sh`. Performance work and timing evidence use the
  same build: on this workstation it is
  `PKG_CONFIG_PATH=/Users/brianglendenning/SoftwareProjects/fftw-3.3.11-simd/lib/pkgconfig`
  (built 2026-10-13). Check which FFTW a binary linked (the
  `target/release/build/casa-fft-*/output` link path, or
  `nm BIN | grep -c n1fv_` for NEON codelets) before comparing timings, and
  link both sides of an A/B against the same FFTW.

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

## Imaging Evidence

`TESTING.md` ("Imaging tiers and evidence") says what an evidence run must
record. Locations on this workstation:

- **T1.5 harness** (not in the repository): working copies under
  `/Volumes/GLENDENNING/casa-rs-evidence/if/IF-8/{refim,kernels}`, each with
  `t15.py`, `casa_references.py` and its `data/`, `casa/` and `bin/` folders.
  Run `T15_TAG=-<commit> /Applications/CASA.app/Contents/MacOS/python3
  t15.py bin/casars-imager-<commit> [cases]` in each, with a binary built
  from the committed state; about 15 minutes for both. Durable copies of the
  scripts, the CASA 6.7.6 references and the results are on the NAS under
  `/Volumes/home/casa-rs/if10/`; the inputs are the NAS casatestdata
  (`measurementset/{vla,alma,evla}`, `unittest/tclean`). Check each case's
  `image.summary.json` `backend` when a result jumps: a case that names no
  backend runs on `auto`.
- **T2 matrix**: `tools/perf/imager/run_workload.py` with
  `CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance` and
  `CASA_RS_CASA_PYTHON`; receipts under
  `/Volumes/GLENDENNING/casa-rs-evidence/if/t2/`, durable copies on the NAS.
- **Timed rows and large inputs** (pilot, 512-plane cube, deep32, full
  cube, Wave3, #625): `~/SoftwareProjects/casa-rs-if10-data.noindex/` on
  this laptop, `~/ext/SoftwareProjects/casa-rs-data/` on the Mac mini, and
  the NAS manifests under `/Volumes/home/casa-rs/if10/`. The pre-refactor
  build `f38033668a` is the baseline for diagnosing a regression, not for
  accepting a row.
- **An explicit memory policy**: production reads none from the
  environment. An evidence run that needs one (forced paging, a fixed
  budget) builds an evidence-only patch of `ImagingRequest::resource_policy`
  and `HostResources::detect` from the committed head, labels the binary
  with the patch, and reverts the checkout afterwards; IF-11's patch is
  recorded on #660.

## TUI Evidence

For tutorial or regression evidence that needs `casars` TUI screenshots, use
`tools/ghostty-surface-capture`, not visible terminal/window screenshots. It
runs the TUI in an offscreen GhosttyKit surface with `TERM=xterm-ghostty` and
captures Kitty graphics and terminal cells from the renderer layer in one PNG.
