# Wave 6 Issue 174 VLA Flagging

Truth class: current descriptive
Last reality check: 2026-05-05
Verification: cargo test -p casa-ms --test flagging; cargo check -p casa-ms --bins; just verify; just docs-check

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

## Local Data State

The expected staged input is:

`/Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/vla/flagging/SNR_G55_10s.tar.gz`

Local validation found the archive present but truncated:

`gzip -t` reported `unexpected end of file` and `uncompress failed`.

This blocks the full same-input CASA 6.7.5-9 versus casa-rs VLA flagging guide
run until the tutorial artifact is restaged. The issue therefore has native
implementation and focused regression coverage, but the final tutorial-product
comparison remains pending data repair.

## Verification

Targeted checks:

```sh
cargo test -p casa-ms --test flagging
cargo check -p casa-ms --bins
just verify
just docs-check
```

Covered behavior:

- `tfcrop` flags seeded time/frequency outliers in a persisted MS fixture.
- `rflag` honors explicit `timedev` and `freqdev` thresholds and flags seeded
  time/spectral outliers.
- `flagmanager` save and restore round-trip the main `FLAG` state.
- `flagdata` and `flagmanager` binaries emit JSON for summary/list paths.

## Remaining Tutorial-Parity Work

- Restage `SNR_G55_10s.tar.gz`.
- Run the VLA CASA Flagging guide through CASA 6.7.5-9 and casa-rs from the
  same input.
- Compare `FLAG`, `FLAG_ROW`, `<ms>.flagversions`, and tutorial-visible
  diagnostics.
- Record timing for the native flagging modes on the tutorial MS.
