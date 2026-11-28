# MeasurementSet formats

MSv2 stores use casacore tables, usually in a directory ending in `.ms`.
XRADIO MSv4 stores use schema 4.0.1 in a local Zarr v3 processing set ending
in `.ps.zarr`.

## Python format support

| Public Python parameter | Operation | MSv2 | MSv4 | End-to-end tests |
| --- | --- | --- | --- | --- |
| `tasks.msconvert(vis=…)` | Read an existing MeasurementSet | Yes | Yes | [All four input/output pairings][format-tests] |
| `tasks.msconvert(outputvis=…, outformat=…)` | Create a new MeasurementSet | `outformat="msv2"` | `outformat="msv4"` | [All four input/output pairings][format-tests] |
| `tasks.rerefant(vis=…)` | Read antenna metadata for re-referencing a calibration table | Yes | Yes | [Read and verify the output calibration table][rerefant-tests] |
| `tasks.smoothcal(vis=…)` | Read field metadata for smoothing a calibration table | Yes | Yes | [Verify field-specific smoothing on both stores][smoothcal-tests] |
| `tasks.gaincal(vis=…)` | Read visibilities for a time-binned per-receptor G gain solve | Yes | [Point-source mode below](#solve-time-binned-gains) | [Verify gains for both stores][gaincal-tests] |

[format-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_msconvert_formats.py
[rerefant-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_rerefant_formats.py
[smoothcal-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_smoothcal_formats.py
[gaincal-tests]: https://github.com/bglenden/casa-rs/blob/main/frontends/casars-python/python/tests/test_gaincal_formats.py

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
