# Agent Reference

Truth class: normative
Last reality check: 2026-10-20
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
  `~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...`. No copy is
  kept: the NAS mirrors were removed on 2026-10-23, and
  `/Volumes/home/casa-rs/tutorial-data/` keeps only the download scripts and
  checksums to fetch the data from NRAO again.
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
  at `/Volumes/home/casatestdata`. The laptop mounts it at login and remounts
  it when it drops (a user LaunchAgent; the login is in the Keychain).
- External disk `GLENDENNING` at `/Volumes/GLENDENNING/`: fast working space
  for large datasets and runs, never the only copy of anything that matters.
  Since 2026-10-16 it is a 1 TB NVMe SSD (about 3 GB/s) that starts empty:
  copy a run's inputs onto it from the NAS first, and never read a large
  MeasurementSet over the network in place. Earlier GLENDENNING disks are
  archived on the NAS:
  - the SSD that failed on 2026-10-11, mirrored at
    `/Volumes/home/casa-rs-glendenning-preserve/GLENDENNING/` (its
    `README.md` maps the paths);
  - the USB stick used from 2026-10-12 to 2026-10-16, whose data not already
    on the NAS is in `/Volumes/home/casa-rs/glendenning-stick-20261016/`
    (its `README.md` maps the paths).

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
- Local installs: `just install-local` (or `just install-local-suite`)
- Release install: `just install-release <version>`

Use `TESTING.md` to decide when these heavier commands apply. Routine branch
merges do not run release/tag-only gates.

## Imaging Evidence

`TESTING.md` ("Imaging tiers and evidence") says what an evidence run must
record. Locations on this workstation:

- **T1.5 harness** (not in the repository): working copies at
  `/Volumes/GLENDENNING/casa-rs-evidence/if/IF-8/{refim,kernels}`, each with
  `t15.py`, `casa_references.py` and its `data/`, `casa/` and `bin/` folders.
  - **Staging:** restage them on GLENDENNING from the NAS before a run:
    - the scripts and the CASA 6.7.6 references from the `t15-*.tar`
      archives in `/Volumes/home/casa-rs/if10/`;
    - the inputs from the NAS casatestdata (`measurementset/{vla,alma,evla}`,
      `unittest/tclean`).
  - **Results:** results up to 2026-10-12 are in `t15-results-20261012.tar`;
    later ones are in `glendenning-stick-20261016/stick-records-20261016.tar`.
  - **Running:** run `T15_TAG=-<commit> /Applications/CASA.app/Contents/MacOS/python3
    t15.py bin/casars-imager-<commit> [cases]` in each folder, with a binary
    built from the committed state; both take about 15 minutes.
  - **A jumped result:** check the case's `image.summary.json` `backend`. A
    case that names no backend runs on `auto`.
- **T2 matrix**: `tools/perf/imager/run_workload.py` with
  `CASA_RS_IMPERF_DATA_ROOT` set to a staged copy of the inputs on
  GLENDENNING (`casa-rs-imperformance`, from the NAS mirror above) and
  `CASA_RS_CASA_PYTHON`.
  - **Receipts:** write them under `/Volumes/GLENDENNING/casa-rs-evidence/if/t2/`,
    then archive them to the NAS.
  - **CASA oracles:** `casa-rs/if10/t2-oracles-20261013.tar` and
    `casa-rs/glendenning-stick-20261016/casa-oracles-and-inputs-20261013-14.tar`.
- **Timed rows and large inputs** (pilot, 512-plane cube, deep32, full
  cube, Wave3, #625): `~/SoftwareProjects/casa-rs-if10-data.noindex/` on
  this laptop, `~/ext/SoftwareProjects/casa-rs-data/` on the Mac mini, and
  the NAS manifests under `/Volumes/home/casa-rs/if10/`.
  - **Wave3:** the MS that CASA's Wave3 reference was imaged on, regenerated
    on 2026-10-13, is
    `casa-rs/glendenning-stick-20261016/wave3-regenerated-ms-20261013.tar`.
    The original in the NAS mirror differs from it.
  - **Baseline:** the pre-refactor build `f38033668a` is the baseline for
    diagnosing a regression, not for accepting a row.
- **An explicit memory policy**: production reads none from the
  environment. An evidence run that needs one (forced paging, a fixed
  budget) builds an evidence-only patch of `ImagingRequest::resource_policy`
  and `HostResources::detect` from the committed head, labels the binary
  with the patch, and reverts the checkout afterwards; IF-11's patch is
  recorded on #660.
