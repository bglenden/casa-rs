# Calibration Tasks User Guide

This guide shows how to run the casa-rs calibration tasks from the
`calibrate` command line and from Python.

## The calibration surfaces

All calibration work runs through one provider executable, `calibrate`. The
application catalog exposes it as these task surfaces:

- `calibrate`: the general surface; `--mode` selects `apply`,
  `export_corrected_data`, `fluxscale`, `gencal`, `solve_bandpass`,
  `solve_gain`, `stats`, or `summary`
- `gaincal`, `bandpass`, `applycal`, `fluxscale`, `gencal`: CASA's tasks,
  with CASA 6.7.6's parameters, defaults and values (#931)

Every surface is a one-shot task. Run it as `calibrate <surface>`
(`calibrate bandpass --ms ...`; `calibrate` alone is the general surface),
which resolves a parameter profile (Defaults, Last, Last Successful, or a
TOML file), applies overrides, saves Last state in the workspace, and runs
the task. See
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
   calibrate gaincal --vis ngc5921.ms --caltable ngc5921.G0 \
     --refant VA15 --gaintype G --calmode p --solint inf \
     --field 0 --spw 0 --smodel 1,0,0,0
   ```

2. Solve the bandpass on field `0`, applying the gain table on the fly:

   ```bash
   calibrate bandpass --vis ngc5921.ms --caltable ngc5921.B0 \
     --refant VA15 --gaintable ngc5921.G0 --field 0 --smodel 1,0,0,0
   ```

3. Apply both tables to field `0`:

   ```bash
   calibrate applycal --vis ngc5921.ms \
     --gaintable ngc5921.G0,ngc5921.B0 --field 0
   ```

4. Inspect the solutions and the corrected data with `calibrate --mode stats`
   or `--mode summary`, and `plotms`.

The CASA-named surfaces take CASA's chain: the `--gaintable` list, with the
positional `--gainfield`, `--interp` and `--spwmap` lists, so the i-th table
takes the i-th entry of each and a table past the end of a list takes CASA's
default (every field, `linear`, no window map); `--calwt` gives one flag for
every table or one per table. A flag's value is split at commas, so an entry
that holds a comma, such as CASA's `interp='nearest,linear'`, is given as a
Python list: `--interp "['linear', 'nearest,linear']"`, and so is a window
map per table: `--spwmap '[[0, 0], []]'`. `--docallib --callib FILE` applies a
CASA calibration library instead. `--applymode trial` reports the apply plan
without changing the MeasurementSet. `calibrate --mode apply` keeps casa-rs's
own `--gaintables` with `;`-separated per-table lists.

CASA's empty `smodel` solves against the `MODEL_DATA` column where the
MeasurementSet has one and a 1 Jy point source where it does not; `bandpass`
does not solve against `MODEL_DATA` yet, so give it a point source. CASA
reads a field's virtual model (`setjy` with `usescratch=False`) before
`MODEL_DATA`; casa-rs does not yet, so an empty `smodel` on a
MeasurementSet that holds one is refused. A CASA
parameter casa-rs does not implement yet is taken only at its default, and a
CASA value it does not implement yet (such as `gaintype='K'`) is refused;
both refusals name [#979](https://github.com/bglenden/casa-rs/issues/979).
`docs/reference/casa-compatibility.md` lists them.

Use `calibrate gaincal --template` to see every parameter with its default,
and `--save-params FILE` to keep a run's resolved profile for reuse with
`--params FILE`.

## Python

The same surfaces are generated functions under `casars.tasks` with CASA's
signatures (`casars.tasks.gaincal(...)`, `casars.tasks.applycal(...)`, and so
on), returning what CASA's tasks return, and `casars.tasks.run("gaincal",
...)` accepts the same parameters and returns the run's `TaskCompletion`.
Each call starts the `calibrate` executable in the current directory (or in
`tasks.run`'s `workspace`), passes its log to `sys.stderr`, and saves no
Last. CASA's parameters are the consent; there are no separate
confirmations.

```python
from casars import tasks

solved = tasks.gaincal(vis="ngc5921.ms", caltable="ngc5921.G0", refant="VA15",
                       gaintype="G", calmode="p", solint="inf", field="0", spw="0",
                       smodel=[1, 0, 0, 0])
solved["selectvis"]["antennas"]   # the antennas the solve selected
```

Profiles saved before these surfaces took CASA's names load as the runs they
meant: `out_table` becomes `caltable`, `gain_type='g'` becomes
`gaintype='G'`, the `;`-separated per-table lists become CASA's positional
lists, and so on. Where CASA's default differs (`applycal`'s `calwt`,
`gaincal`'s `calmode` and `smodel`, `bandpass`'s `combine` and `smodel`), a
profile that left the value out adopts CASA's default with a warning.

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
