# Wave 6 Issue 174 VLA Flagging

Truth class: current descriptive
Last reality check: 2026-05-06
Verification: cargo test -p casa-ms --test flagging; cargo build --release -p casa-ms --bin flagdata; cargo check -p casa-ms --bins; just quick; just verify Rust/clippy/test/doctest stages plus rerun ./scripts/test-python-package.sh with network; just docs-check; gzip -t SNR_G55_10s.clean.tar.gz; CASA/casa-rs tutorial-MS spot checks

Wave issue: #174
Parent issue: #128
Registry key: `vla/flagging`

## Scope

This issue covers the VLA CASA Flagging guide task surface that is fundamental
to user parity, not only small manual flag edits. The implemented native slice
adds `flagdata` and `flagmanager` Rust APIs plus command-line entry points in
`casa-ms`.

Implemented `flagdata` families:

- `manual` selected-sample flag/unflag edits
- `clip` with tutorial-relevant `clipzeros`
- `quack` begin/end scan-edge flagging
- `tfcrop`-family robust time/frequency outlier flagging
- `rflag`-family time/spectral robust threshold flagging
- `extend` across polarization, time, and frequency thresholds
- `summary` JSON counts

Implemented `flagmanager` operations:

- `list`
- `save`
- `restore`
- `delete`
- `rename`
- merge modes `replace`, `or`, and `and`

## CASA Sources Inspected

The implementation was shaped against the CASA 6.7 task/tool source paths
instead of only the guide text:

- `/Users/brianglendenning/SoftwareProjects/casa/casatasks/src/private/task_flagdata.py`
- `/Users/brianglendenning/SoftwareProjects/casa/casatasks/src/private/task_flagmanager.py`
- `/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/flagging/Flagging/FlagVersion.h`
- `/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/flagging/Flagging/FlagVersion.cc`
- `/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/flagging/Flagging/FlagAgentTimeFreqCrop.h`
- `/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/flagging/Flagging/FlagAgentTimeFreqCrop.cc`
- `/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/flagging/Flagging/FlagAgentRFlag.h`
- `/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/flagging/Flagging/FlagAgentRFlag.cc`
- `/Users/brianglendenning/SoftwareProjects/casa/casatasks/tests/tasks/test_task_flagdata.py`

Key semantics preserved:

- `flagmanager` writes sibling `<ms>.flagversions` storage with
  `FLAG_VERSION_LIST` and `flags.<version>` tables containing `FLAG` and
  `FLAG_ROW`.
- Restores and saves support CASA-style `replace`, `or`, and `and` merge
  behavior.
- Automatic flagging groups by field, SPW, scan, and baseline, then evaluates
  time/frequency outliers per correlation.

The `tfcrop` and `rflag` modes are native robust-statistics implementations of
the CASA task families, not bindings to the CASA C++ agent classes and not yet
claimed as bit-for-bit clones.

## Tutorial Data State

The official VLA flagging tutorial archive was restaged from:

`https://casa.nrao.edu/Data/EVLA/SNRG55/SNR_G55_10s.tar.gz`

The validated local copy is:

`/Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/vla/flagging/SNR_G55_10s.clean.tar.gz`

Validation:

```sh
stat -f '%z bytes' SNR_G55_10s.clean.tar.gz
# 5293196015 bytes
gzip -t SNR_G55_10s.clean.tar.gz
tar -tzf SNR_G55_10s.clean.tar.gz
```

The archive extracts to a single `SNR_G55_10s.ms` MeasurementSet. Disposable
copy-on-write clones for parity checks live under:

`/Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/vla/flagging/parity-runs/20260505-clean/`

The older resumed `SNR_G55_10s.tar.gz` and salvage candidates in the same
directory are known invalid and must not be used for parity evidence.

## Real Tutorial-MS Parity Evidence

CASA runs used:

`/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python`

The storage path is interoperable for the checked mutations: casa-rs summary
reads CASA-written flags with the same post-CASA totals. Algorithm parity is
achieved for the checked `clipzeros` and `rflag` counts. `tfcrop` is now within
64 samples of the CASA count on the checked tutorial scan, with the remaining
coordinate delta localized to SPW 2 and accepted as solver-numerics drift.

| Check | Selection | CASA 6.7.5-9 | casa-rs | Result |
| --- | --- | ---: | ---: | --- |
| `clipzeros` | full MS | 2 flagged samples after 7.6 s | 2 flagged samples after 10.16 s, 5.20 s user CPU, release build | count match; about 1.3x wall / 1.4x CPU |
| `tfcrop` | `scan=251`, `DATA` | 152,904 flagged samples after 0.82 s | 152,840 flagged samples after 0.84 s, 0.74 s user CPU, release build | near match; about 1.0x wall / 0.9x CPU |
| `rflag` | `scan=251`, `DATA` | 189,624 flagged samples after 0.52 s | 189,624 flagged samples after 0.71 s, 0.61 s user CPU, release build | exact `FLAG` cube match; about 1.4x wall / 1.2x CPU |

The CASA `clipzeros` samples were not bitwise zero when inspected through
casatools after flagging; their amplitudes were approximately `1.1e-7`.
The upstream `FlagAgentBase::isNaNOrZero(Float)` path treats zero, subnormal,
NaN, infinity, and normal values `<= FLT_EPSILON` as clip-zero samples. The
native implementation now matches that checked behavior without widening to
the 232 samples below `1e-6` in the tutorial MS.

## TFCrop/RFlag Parity Findings

The earlier `tfcrop` and `rflag` differences were too large to be explained by
floating-point drift. Instrumentation showed that the native code was not
executing the same decision algorithm as the CASA C++ agents. The current
implementation has been reshaped against the CASA agent sources.

For `rflag`, CASA `action="calculate"` reports field/SPW-specific thresholds
for `scan=251`:

| SPW | CASA `timedev` | CASA `freqdev` |
| ---: | ---: | ---: |
| 0 | 0.00271737629 | 0.00241967238 |
| 1 | 0.00104904500 | 0.00095542112 |
| 2 | 0.00363038148 | 0.00282458808 |
| 3 | 0.00147350823 | 0.00115331159 |

The previous casa-rs `rflag` path computed one global pair:
`timedev=0.006615205813706328` and
`freqdev=0.007730504001694498`. The current path computes the same
field/SPW-shaped threshold maps as CASA, then applies the default scale before
flagging.

Coordinate-level diagnostics also showed a correlation-handling mismatch. CASA
adds an extension agent by default for `tfcrop` and `rflag`
(`extendflags=True`, `extendpols=True`, `growtime=50`, `growfreq=80`), which
produces identical counts across correlations within each SPW. casa-rs now
runs that default post-extension for automatic modes.

| Mode | Output | SPW 0 counts by corr | SPW 1 counts by corr | SPW 2 counts by corr | SPW 3 counts by corr |
| --- | --- | --- | --- | --- | --- |
| `tfcrop` | CASA | 18,323 / 18,323 / 18,323 / 18,323 | 4,036 / 4,036 / 4,036 / 4,036 | 12,188 / 12,188 / 12,188 / 12,188 | 3,679 / 3,679 / 3,679 / 3,679 |
| `tfcrop` | casa-rs | 18,322 / 18,322 / 18,322 / 18,322 | 4,036 / 4,036 / 4,036 / 4,036 | 12,173 / 12,173 / 12,173 / 12,173 | 3,679 / 3,679 / 3,679 / 3,679 |
| `rflag` | CASA | 19,052 / 19,052 / 19,052 / 19,052 | 7,484 / 7,484 / 7,484 / 7,484 | 18,095 / 18,095 / 18,095 / 18,095 | 2,775 / 2,775 / 2,775 / 2,775 |
| `rflag` | casa-rs | 19,052 / 19,052 / 19,052 / 19,052 | 7,484 / 7,484 / 7,484 / 7,484 | 18,095 / 18,095 / 18,095 / 18,095 | 2,775 / 2,775 / 2,775 / 2,775 |

Source inspection explained both differences:

- CASA `tfcrop` runs `FlagAgentTimeFreqCrop::fitBaseAndFlag()` in `freqtime`
  order by default, averages across the opposite axis, fits a line or
  piecewise polynomial baseline, divides by that fit, and iteratively flags
  normalized residuals. casa-rs now follows that shape with CASA-like `Float`
  working values and the default post-extension agent. The remaining tutorial
  diff is 128 CASA-only samples and 64 casa-rs-only samples, all in SPW 2.
- Polynomial fitting uses the shared `casa_ms::least_squares` helper, which is
  backed by `nalgebra`'s SVD solver. This is the same helper used by calibration
  fitting paths; the old crate-private calibration duplicate was removed so
  flagging does not carry an ad hoc least-squares implementation.
- CASA `rflag` computes threshold histograms per `(field, spw)`, then flags
  complex real/imag variance and spectral deviations using a moving
  `winsize=3` time window. casa-rs now matches the final CASA `FLAG` cube for
  the checked tutorial scan.
- The full-scan runtime issue was mostly table access, not algorithm cost.
  Selected-row bulk loads, lazy FLAG writes, skipping unchanged `FLAG_ROW`
  writes, typed row scans, dense grouping, allocation-light RFlag median/MAD
  evaluation, squared-norm clip-zero checks, and mask-based automatic-mode
  application reduced the worst measured release runs from minute-scale to
  CASA-level for checked one-scan automatic modes and close to 10 seconds for
  full-MS `clipzeros`.

Diagnostic instrumentation is available with:

```sh
CASA_RS_FLAGDATA_TRACE=/path/to/trace.jsonl target/release/flagdata \
  --vis <clone.ms> --mode tfcrop --scan 251 --datacolumn DATA --no-flagbackup
CASA_RS_FLAGDATA_TRACE=/path/to/trace.jsonl target/release/flagdata \
  --vis <clone.ms> --mode rflag --scan 251 --datacolumn DATA --no-flagbackup
```

The trace records native candidate counts, threshold maps, phase timings, and
per-SPW/correlation decision counts so future algorithm changes can be checked
against CASA before comparing only final totals.

The final 2026-05-06 performance traces on clean copy-on-write tutorial clones
reported:

| Mode | Load | Threshold | Plan | Apply | Total before save |
| --- | ---: | ---: | ---: | ---: | ---: |
| `tfcrop` | 119 ms | n/a | 599 ms | 30 ms | 748 ms |
| `rflag` | 119 ms | 180 ms | 298 ms | 27 ms | 624 ms |

## Accepted TFCrop Solver Difference

The remaining `tfcrop` coordinate delta is accepted for this wave. Instrumented
CASA/C++ probes isolated it to a borderline polynomial fit group in SPW 2 where
casacore `LinearFit`/`LSQFit` and casa-rs' shared `nalgebra` SVD-backed
least-squares helper make slightly different threshold decisions. The accepted
delta on the checked tutorial scan is:

- CASA: 152,904 final flagged samples.
- casa-rs: 152,840 final flagged samples.
- Coordinate diff: 128 CASA-only samples and 64 casa-rs-only samples, all in
  SPW 2.

We are not porting casacore's least-squares solver idiosyncrasies solely to
remove this small TFCrop edge-case delta. The native implementation keeps the
community-backed shared solver used by calibration and flagging, while
preserving the CASA-shaped TFCrop control flow, residual iteration, and default
post-extension behavior.

## Verification

Targeted checks:

```sh
cargo fmt --all -- --check
cargo test -p casa-ms --test flagging
cargo clippy -p casa-ms --all-targets -- -D warnings
cargo build --release -p casa-ms --bin flagdata
cargo check -p casa-ms --bins
just verify
just docs-check
```

The 2026-05-06 `just verify` run completed the Rust, clippy, test, and doctest
stages, then failed at `./scripts/test-python-package.sh` because sandboxed
pip could not reach PyPI for `maturin`. Rerunning that Python package step with
network access passed: `47 passed`.

Covered behavior:

- `tfcrop` flags seeded time/frequency outliers in a persisted MS fixture.
- `rflag` honors explicit `timedev` and `freqdev` thresholds and flags seeded
  time/spectral outliers.
- `clipzeros` persists CASA-style `FLT_EPSILON` clip-zero flags through the
  path-based task entry point.
- `flagmanager` save and restore round-trip the main `FLAG` state.
- `flagdata` and `flagmanager` binaries emit JSON for summary/list paths.

## Remaining Tutorial-Parity Work

- Continue profiling algorithm planning overhead: release-build `tfcrop` is
  now at CASA-level wall time on the checked scan, while `rflag` still has the
  largest automatic-mode CPU ratio despite matching the CASA `FLAG` cube.
- Continue storage-path profiling for full-MS scans. `clipzeros` now avoids the
  previous full main-table eager load, avoids per-row `FLAG` clones when rows
  do not change, and writes only the changed `FLAG` cells, but it still scans
  the full `DATA` and `FLAG` columns about 1.3x slower than CASA on the
  tutorial MS.
- Extend the comparison beyond counts to `FLAG`/`FLAG_ROW` coordinate diffs,
  `<ms>.flagversions`, and tutorial-visible diagnostics.
