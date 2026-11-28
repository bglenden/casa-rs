# MeasurementSet formats

MSv2 stores use casacore tables, usually in a directory ending in `.ms`.
XRADIO MSv4 stores use schema 4.0.1 in a local Zarr v3 processing set ending
in `.ps.zarr`.

## Python format support

| Public Python parameter | Operation | MSv2 | MSv4 | End-to-end tests |
| --- | --- | --- | --- | --- |
| `tasks.importvla(vis=…, outformat=…)` | Create a MeasurementSet from a VLA archive | Default or `outformat="msv2"` | `outformat="msv4"` with [archive limits](#import-vla-archives) | [CASA and XRADIO archive checks][importvla-tests] |
| `tasks.msconvert(vis=…)` | Read an existing MeasurementSet | Yes | Yes | [All four input/output pairings][format-tests] |
| `tasks.msconvert(outputvis=…, outformat=…)` | Create a new MeasurementSet | `outformat="msv2"` | `outformat="msv4"` | [All four input/output pairings][format-tests] |
| `tasks.rerefant(vis=…)` | Read antenna metadata for re-referencing a calibration table | Yes | Yes | [Read and verify the output calibration table][rerefant-tests] |
| `tasks.smoothcal(vis=…)` | Read field metadata for smoothing a calibration table | Yes | Yes | [Verify field-specific smoothing on both stores][smoothcal-tests] |
| `tasks.gaincal(vis=…)` | Read visibilities for a time-binned per-receptor G gain solve | Yes | [Point-source mode below](#solve-time-binned-gains) | [Verify gains for both stores][gaincal-tests] |
| `tasks.gencal(vis=…)` | Read metadata for explicit calibration prescriptions | Yes | `amp`, `ph`, `sbd`, `mbd`, `opac`, [manual antenna offsets](#prescribe-calibration-values) | [Create and accumulate priors on both stores][gencal-tests] |
| `tasks.listcal(vis=…)` | Read metadata for a CASA calibration listing | Yes | Fixed fields, terminal and file output | [Verify file output on both stores][listcal-tests] |

[importvla-tests]: https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/importvla_oracle.py
[format-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_msconvert_formats.py
[rerefant-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_rerefant_formats.py
[smoothcal-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_smoothcal_formats.py
[gaincal-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_gaincal_formats.py
[gencal-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_gencal_formats.py
[listcal-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_listcal_formats.py

## Choose the output format

```python
from casars import tasks

converted = tasks.msconvert(
    vis="target.ms", outputvis="target.ps.zarr", outformat="msv4"
)
rewritten = tasks.msconvert(
    vis="target.ms", outputvis="target-copy.ms", outformat="msv2"
)
print(converted.value["losses"])
print(rewritten.value["losses"])
```

The input format is detected from the store. Give both `outputvis` and
`outformat` to choose its destination and format explicitly. With
`outformat` omitted, the task selects the other format. An existing
destination requires `overwrite=True`.

## Import VLA archives

```python
from casars import tasks

tasks.importvla(archivefiles=["AS758_C030425.xp1"], vis="vla.ms", outformat="msv2")
tasks.importvla(archivefiles=["AS758_C030425.xp1"], vis="vla.ps.zarr", outformat="msv4")
```

The output format follows `outformat` regardless of the path suffix. MSv2 is
the default and preserves the CASA archive layout, including variable cell
shapes, the original flags and flag-category names, and the `Original`
flag-version snapshot. Both choices return `None`.

An MSv4 import creates XRADIO datasets directly from the archive. It
converts TAI timestamps to UTC and carries the visibility groups, weights,
UVW and element flags. It reports the features that the output profile
cannot carry. These include the independent row flags (folded into the
element flags), `FLAG_CATEGORY`, `SIGMA`, and the MSv2 history and flag-version
snapshot. MSv4 requires a supported direction and UVW frame and a time
representable as UNIX/UTC. For example, a B1950_VLA archive or an
integration on a leap-second day returns a typed `outformat` error before
publishing a store.

## Conversion limits

`msconvert` writes a new store from the data its conversion path carries.
This applies to same-format rewrites too. For example, an MSv2-to-MSv2
rewrite can report that it dropped the `HISTORY` subtable or fields such as
`OBSERVATION.LOG`. The completion's `value["losses"]` and task logs report
the specific losses.

- MSv4 carries element flags. During conversion from MSv2, a row flag
  contributes to those element flags. The original row flag and flag
  categories cannot be reconstructed from the element flags.
- MSv4 stores weights and has no `SIGMA` column. When the source lacks
  `SIGMA`, an MSv2 output derives it as `1 / sqrt(WEIGHT)` for positive
  weights.
- The conversion to MSv4 uses XRADIO names for fields and antennas. A
  conversion back to MSv2 keeps the names stated by the MSv4 input.
- An MSv2 output uses `padding="flagged"` by default for unfilled cells in
  the time/baseline grid. Use `padding="omitted"` to skip those cells.
- A feature outside the supported conversion profile can refuse the task.
  The error identifies the feature it could not carry. Inspect the reported
  losses before treating the converted store as a replacement for its input.


## Re-reference a calibration table

For `tasks.rerefant` with an MSv4 input, `refant` selects antennas by
name or row number in `tablein`'s `ANTENNA` subtable. The task matches them
to the measurement set by station and position and checks that the names
agree. The task refuses an ambiguous identity before publishing the
output. With MSv2, `refant` selects from the MeasurementSet's `ANTENNA`
subtable using its stored row numbers and position values.

## Smooth calibration tables

For `tasks.smoothcal` with an MSv4 input, `field` selects rows of
`tablein`'s `FIELD` subtable. The selected rows must identify unique
observation fields by name and phase centre in the same frame. The task
refuses field binding for ephemeris-tracking fields or polynomial calibration
directions. The calibration `FIELD` subtable must store its own rows.
With MSv2, `field` selects from the MeasurementSet's `FIELD`
subtable. An empty selector smooths every calibration-table field without
binding individual field identities across stores.

## Solve time-binned gains

For an MSv4 input, `tasks.gaincal` supports `gaintype="G"` with
`calmode="ap"`, `calmode="p"` or `calmode="a"`. Give a positive duration
such as `solint="60s"` and an explicit unpolarized point source such as
`smodel=[1, 0, 0, 0]`. Use the default selection,
no prior calibration tables, and the default averaging, flagging, and
combination controls. Keep normalization and robust fitting at their
defaults. Each MSv4 scan name must map to a distinct decimal scan number,
and each solution group must be contained in one observation partition.
The task writes a CASA-format calibration table. MSv4 uses the processing
set's stored weights. MSv2 uses weights derived from its stored sigmas,
following CASA.

## Prescribe calibration values

`tasks.gencal` accepts MSv4 for explicit amplitude, phase, delay and opacity
values. The task resolves antenna and spectral-window selectors in the
input's namespace and writes a CASA-format calibration table. You can
accumulate a phase prescription in an existing gain table as with MSv2.
MSv4 inputs must have metadata that the CASA calibration subtables can
represent. The task refuses moving MSv4 fields and updates whose existing
antenna or spectral-window IDs refer to different physical entities.
Textual scan names are accepted because these prescriptions do not name a scan.

For manual antenna offsets, supply `caltype="antpos"`, an explicit `antenna`
selection, and three offset coordinates per selected antenna in `parameter`.
`caltype="antposvla"` rotates offsets from VLA coordinates into ITRF. Both
modes add offsets to an existing, physically compatible calibration table.
MSv4 uses the declared `overall_telescope_name` of each observing session.
Manual offsets are available when those telescopes do not require the VLA
tropospheric correction check. For observations consisting entirely of VLA
or EVLA sessions, the task requires the stated `OBSERVATION.TIME_RANGE` that
determines CASA's `VLATrDelCorr` keyword. MSv4 lacks that field and the task
reports a typed `vis` error. Automatic and file-based `antpos` require MSv2.

## Write a calibration listing

`tasks.listcal` accepts fixed-field MSv4 observations for terminal display
and file output. Supply `listfile` to write a report, or leave it empty to
display the result. A positive `pagerows` sets the page size. The task resolves
`field`, `antenna`, and `spw` against the supplied observation. Use the names
in the MSv4 observation, which may have gained suffixes during conversion from
MSv2. The listing retains the supplied observation path and describes the
solutions in the CASA calibration table.

The selected field and antenna IDs must agree with the physical identities at
the same rows of that calibration table. Explicit spectral-window selections
must agree at the selected windows and channel grids. With an empty or `"*"`
`spw`, every window referenced by a calibration solution must agree. The task
reports identity conflicts before printing or writing a listing.
