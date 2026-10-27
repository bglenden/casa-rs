# Calibration Tasks User Guide

This guide shows how to run the casa-rs calibration tasks from the `casars`
command line and from Python.

## The calibration surfaces

All calibration work runs through one provider executable, `calibrate`. The
application catalog exposes it as these task surfaces:

- `calibrate`: the general surface; `--mode` selects `apply`,
  `export_corrected_data`, `fluxscale`, `gencal`, `solve_bandpass`,
  `solve_gain`, `stats`, or `summary`
- `gaincal`, `bandpass`, `applycal`, `fluxscale`, `gencal`: CASA-named
  surfaces that run `calibrate` with a fixed mode

Every surface is a one-shot task. Run it with `casars run`, which resolves a
parameter profile (Defaults, Last, Last Successful, or a TOML file), applies
overrides, saves Last state, and launches the provider. See
[Task Parameter Profiles](task-parameters.md) for sources and managed Last
files, and the [generated parameter reference](reference/task-parameters.md)
for every parameter and its CLI flag.

Use a writable copy of the dataset when you intend to apply calibration,
because `applycal` (and `calibrate --mode apply`) writes `CORRECTED_DATA` in
place, as CASA's does. As in CASA, a `calflag` run first saves the flags to the
next `applycal_N` flag version (`flagbackup`, default on), so
`flagmanager` can restore them; `--no-flagbackup` skips it, and `calonly` and
`trial` change no flags and save none.

## `ngc5921.ms` walkthrough

Expected field roles:

- field `0`: flux/bandpass calibrator
- field `1`: phase calibrator
- field `2`: target

Recommended reference antenna: `VA15`.

```bash
workdir=$(mktemp -d /tmp/casars-cal.XXXXXX)
cp -R "$HOME/SoftwareProjects/casatestdata/measurementset/vla/ngc5921.ms" "$workdir/ngc5921.ms"
cd "$workdir"
```

1. Solve a phase-only gain on field `0`:

   ```bash
   casars run gaincal --ms ngc5921.ms --out ngc5921.G0 \
     --refant VA15 --gain-type g --mode-gain p --solint inf \
     --field 0 --spw 0
   ```

2. Solve the bandpass on field `0`, applying the gain table on the fly:

   ```bash
   casars run bandpass --ms ngc5921.ms --out ngc5921.B0 \
     --refant VA15 --gaintables ngc5921.G0 --field 0
   ```

3. Apply both tables to field `0`:

   ```bash
   casars run applycal --ms ngc5921.ms \
     --gaintables ngc5921.G0,ngc5921.B0 --field 0
   ```

4. Inspect the solutions and the corrected data with `calibrate --mode stats`
   or `--mode summary`, `plotcal`, and `plotms`.

The apply chain is the ordered `--gaintables` list. Per-table policy is given
by the aligned `--gainfield`, `--interp`, `--spwmap`, and `--calwt` lists, or by
a CASA callibrary file through `--callib`.

Use `casars params template gaincal` to see every parameter with its default,
and `--save-params FILE` to keep a run's resolved profile for reuse with
`--params FILE`.

## Python

The same surfaces are generated CASA-named functions under `casars.tasks`
(`casars.tasks.gaincal(...)`, `casars.tasks.applycal(...)`, and so on), and
`casars.tasks.run("gaincal", ...)` accepts the same parameters. CASA's
parameters are the consent; there are no separate confirmations.

`overwrite=True` replaces only the `-o` report today. A run that would write
a calibration table or MeasurementSet that already exists is refused whatever
`overwrite` says: gaincal, bandpass and gencal caltables, the fluxscale
table, and the corrected-data export. Choose a new name, or remove the old
product first. Bringing these in line with CASA, whose gaincal replaces an
existing caltable, is [#945](https://github.com/bglenden/casa-rs/issues/945).

## Current limitations

- there is not yet a dedicated `setjy` task
- multi-field role assignment and more sophisticated workflows need more
  polish than the basic `ngc5921.ms` path
