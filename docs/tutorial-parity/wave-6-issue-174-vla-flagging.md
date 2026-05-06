# Wave 6 Issue 174 VLA Flagging

Truth class: current descriptive
Last reality check: 2026-05-06
Verification: cargo test -p casa-ms --test flagging; cargo build --release -p casa-ms --bin flagdata; cargo check -p casa-ms --bins; just verify; just docs-check; gzip -t SNR_G55_10s.clean.tar.gz; CASA/casa-rs tutorial-MS spot checks

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
now achieved for the checked `clipzeros` count, but not yet for the checked
automatic-flagging modes.

| Check | Selection | CASA 6.7.5-9 | casa-rs | Result |
| --- | --- | ---: | ---: | --- |
| `clipzeros` | full MS | 2 flagged samples after 7.6 s | 2 flagged samples after 65.36 s, release build | count match; slower |
| `tfcrop` | `scan=251`, `DATA` | 152,904 flagged samples after 0.82 s | 178,106 flagged samples after 54.75 s, release build | mismatch; slower |
| `rflag` | `scan=251`, `DATA` | 189,624 flagged samples after 0.52 s | 108,929 flagged samples after 70.21 s, release build | mismatch; slower |

The CASA `clipzeros` samples were not bitwise zero when inspected through
casatools after flagging; their amplitudes were approximately `1.1e-7`.
The upstream `FlagAgentBase::isNaNOrZero(Float)` path treats zero, subnormal,
NaN, infinity, and normal values `<= FLT_EPSILON` as clip-zero samples. The
native implementation now matches that checked behavior without widening to
the 232 samples below `1e-6` in the tutorial MS.

## TFCrop/RFlag Diagnostic Findings

The remaining `tfcrop` and `rflag` differences are too large to be explained
by floating-point drift. The current native code is not executing the same
decision algorithm as the CASA C++ agents.

For `rflag`, CASA `action="calculate"` reports field/SPW-specific thresholds
for `scan=251`:

| SPW | CASA `timedev` | CASA `freqdev` |
| ---: | ---: | ---: |
| 0 | 0.00271737629 | 0.00241967238 |
| 1 | 0.00104904500 | 0.00095542112 |
| 2 | 0.00363038148 | 0.00282458808 |
| 3 | 0.00147350823 | 0.00115331159 |

The current casa-rs `rflag` path instead computes one global pair:
`timedev=0.006615205813706328` and
`freqdev=0.007730504001694498`. That global thresholding is much looser than
CASA for every SPW and explains much of the underflagging.

Coordinate-level diagnostics also show a correlation-handling mismatch. CASA's
default `ABS ALL` agent path produces identical counts across correlations
within each SPW, while casa-rs currently evaluates each correlation
independently:

| Mode | Output | SPW 0 counts by corr | SPW 1 counts by corr | SPW 2 counts by corr | SPW 3 counts by corr |
| --- | --- | --- | --- | --- | --- |
| `tfcrop` | CASA | 18,323 / 18,323 / 18,323 / 18,323 | 4,036 / 4,036 / 4,036 / 4,036 | 12,188 / 12,188 / 12,188 / 12,188 | 3,679 / 3,679 / 3,679 / 3,679 |
| `tfcrop` | casa-rs | 17,484 / 19,203 / 18,745 / 17,284 | 4,836 / 4,727 / 4,642 / 5,232 | 15,709 / 17,439 / 17,622 / 16,954 | 4,655 / 4,370 / 4,434 / 4,770 |
| `rflag` | CASA | 19,052 / 19,052 / 19,052 / 19,052 | 7,484 / 7,484 / 7,484 / 7,484 | 18,095 / 18,095 / 18,095 / 18,095 | 2,775 / 2,775 / 2,775 / 2,775 |
| `rflag` | casa-rs | 13,697 / 11,276 / 11,109 / 11,673 | 702 / 634 / 542 / 530 | 13,233 / 14,470 / 14,242 / 16,274 | 149 / 142 / 139 / 117 |

Source inspection explains both differences:

- CASA `tfcrop` runs `FlagAgentTimeFreqCrop::fitBaseAndFlag()` in `freqtime`
  order by default, averages across the opposite axis, fits a line or
  piecewise polynomial baseline, divides by that fit, and iteratively flags
  normalized residuals. The current casa-rs implementation applies
  median/MAD outlier tests directly to raw amplitudes by row and channel.
- CASA `rflag` computes threshold histograms per `(field, spw)`, then flags
  complex real/imag variance and spectral deviations using a moving
  `winsize=3` time window. The current casa-rs implementation computes global
  robust thresholds from amplitude values and applies them per correlation.
- CASA's default auto-flagging correlation expression is `ABS ALL`, mapped by
  `VisMapper`/`FlagMapper`; current casa-rs has no equivalent expression
  mapper and does not propagate an automatic-mode decision across the selected
  correlation set.

Diagnostic instrumentation is available with:

```sh
CASA_RS_FLAGDATA_TRACE=/path/to/trace.jsonl target/release/flagdata \
  --vis <clone.ms> --mode tfcrop --scan 251 --datacolumn DATA --no-flagbackup
CASA_RS_FLAGDATA_TRACE=/path/to/trace.jsonl target/release/flagdata \
  --vis <clone.ms> --mode rflag --scan 251 --datacolumn DATA --no-flagbackup
```

The trace records native candidate counts, global native thresholds, and
per-SPW/correlation decision counts so future algorithm changes can be checked
against CASA before comparing only final totals.

## Verification

Targeted checks:

```sh
cargo test -p casa-ms --test flagging
cargo build --release -p casa-ms --bin flagdata
cargo check -p casa-ms --bins
just verify
just docs-check
```

Covered behavior:

- `tfcrop` flags seeded time/frequency outliers in a persisted MS fixture.
- `rflag` honors explicit `timedev` and `freqdev` thresholds and flags seeded
  time/spectral outliers.
- `clipzeros` persists CASA-style `FLT_EPSILON` clip-zero flags through the
  path-based task entry point.
- `flagmanager` save and restore round-trip the main `FLAG` state.
- `flagdata` and `flagmanager` binaries emit JSON for summary/list paths.

## Remaining Tutorial-Parity Work

- Bring native `tfcrop` and `rflag` counts closer to CASA agentflagger behavior
  on tutorial selections before claiming parity.
- Replace the current native automatic-mode approximations with CASA-shaped
  decision logic: `ABS ALL` correlation mapping, TFCrop baseline fitting and
  normalized residual iteration, and RFlag per-field/SPW threshold histograms
  plus moving-window complex real/imag tests.
- Profile and optimize the full-MS scan path: release-build native
  `clipzeros` is still about 9x slower than CASA on the same tutorial data,
  and release-build `tfcrop`/`rflag` remain roughly minute-scale for one scan.
- Extend the comparison beyond counts to `FLAG`/`FLAG_ROW` coordinate diffs,
  `<ms>.flagversions`, and tutorial-visible diagnostics.
