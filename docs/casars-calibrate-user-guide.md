# `casars calibrate` User Guide

This guide documents the current `WorkflowShell`-based `calibrate` app in
`casars`.

## What `calibrate` is for

`calibrate` is the guided calibration workflow UI for MeasurementSets.

It is organized around:

- dataset context
- staged solve and apply steps
- calibration products
- apply-chain management
- diagnostics and inspection

It is not intended to be a raw form wrapper around CLI flags.

## Launching

Use a writable copy of the dataset when you intend to run `Apply`, because that
stage writes `CORRECTED_DATA`.

Example with the shared test dataset:

```bash
workdir=$(mktemp -d /tmp/casars-cal.XXXXXX)
cp -R "$HOME/SoftwareProjects/casatestdata/measurementset/vla/ngc5921.ms" "$workdir/ngc5921.ms"
cargo run -p casars -- calibrate "$workdir/ngc5921.ms"
```

## Shell layout

Left pane:

- `Context`
- `Products`
- `Stages`
- `Stage Parameters`

Right pane:

- `Overview`
- `Data`
- `Products`
- `Diagnostics`
- `History`
- `Stdout`
- `Stderr`

## Core workflow

The shortest useful walkthrough for `ngc5921.ms` is:

1. inspect dataset
2. solve gain on field `0`
3. promote the gain table into the chain
4. solve bandpass on field `0`
5. promote the bandpass table into the chain
6. apply to field `0`
7. inspect corrected-data diagnostics

## Recommended `ngc5921.ms` walkthrough

Expected field roles:

- field `0`: flux/bandpass calibrator
- field `1`: phase calibrator
- field `2`: target

Recommended reference antenna:

- `VA15`

### Step 1: Inspect Dataset

- start on `Inspect Dataset`
- press `r`
- check the `Data` tab for field, SPW, and antenna summaries
- in `Context`, set:
  - `Refant = VA15`
  - `Selected Fields = 0`

### Step 2: Solve Gain

- select `Solve Gain` in `Stages`
- in `Stage Parameters`, use:
  - `Gain Type = G`
  - `Solve Mode = p`
  - `Solution Interval = inf`
  - `SPW IDs = 0`
- press `r`
- inspect diagnostics
- promote the new gain table with `Shift-P`

### Step 3: Solve Bandpass

- keep `Selected Fields = 0`
- select `Solve Bandpass`
- press `r`
- inspect bandpass diagnostics
- promote the new bandpass table with `Shift-P`

### Step 4: Apply

- select `Apply`
- keep field `0` for the first pass
- press `r`
- inspect corrected-data diagnostics

## Products and chain management

The `Products` pane is the center of workflow state.

You can:

- inspect produced calibration tables
- promote solved products into the apply chain
- reorder chain entries with `Ctrl-K` and `Ctrl-J`
- remove chain entries with `Delete`
- edit chain policy rows such as:
  - `gainfield`
  - `interp`
  - `spwmap`
  - `calwt`

If you use a callibrary file, its entries appear as structured rows in
`Products`, and their settings can be edited there.

## Diagnostics

`Diagnostics` is contextual.

Typical presets include:

- gain amplitude/phase vs time
- bandpass amplitude/phase vs frequency
- corrected amplitude/phase vs time
- corrected amplitude/phase vs frequency

Choose diagnostics after each solve or apply step rather than waiting until the
end.

## Important keys

- `Tab` / `Shift-Tab`: move focus between panes
- `[` / `]`: switch right-pane tabs
- `Enter`: activate the selected row or open a picker
- `r`: run the selected stage action
- `Shift-P`: promote the selected workflow product into the apply chain
- `Delete`: remove the selected chain row
- `Ctrl-K`: move selected chain row up
- `Ctrl-J`: move selected chain row down
- `q`: quit
- `?`: help

## Conventions and expectations

The app intentionally follows the `WorkflowShell` conventions documented in:

- [`docs/casars-tui-framework.md`](/Users/brianglendenning/.codex/worktrees/f027/casa-rs/docs/casars-tui-framework.md)

That means:

- stages are ordered but revisitable
- products are first-class
- diagnostics are contextual
- the shell should guide the next step without hiding the workflow structure

## Current limitations

Current known limitations include:

- there is not yet a dedicated first-class `setjy` stage in the TUI
- some advanced workflows are still easier from the CLI than entirely in the UI
- multi-field and more sophisticated role assignment workflows still need more
  polish than the basic `ngc5921.ms` guided path

## Status of this guide

This is currently the in-repo user manual for `calibrate`.

There is not yet a separate public online manual for the new `casars` TUI shell
family, so this file is the best canonical user-facing reference in the repo.
