# API Reference

## Common parameter API

ADR-0006 defines the common API for current task profiles:

```python
casars.parameters.defaults(surface, *, workspace=None)
casars.parameters.last(surface, *, workspace=None)
casars.parameters.last_successful(surface, *, workspace=None)
casars.parameters.load(path, *, workspace=None)

TaskParameters.update(**values)
TaskParameters.reset(name)
TaskParameters.reload()
TaskParameters.save(path)
TaskParameters.run(**values)

casars.tasks.run(task, parameters=None, /, *, workspace=None, **values)
casars.tasks.<casa_task>(casa_parameters..., *, casa_rs_controls...)
casars.tasks.<task>(**parameters)
```

A task call runs in process. `tasks.run` returns a `TaskCompletion` with the
task, workspace, logs, status, message, parameter, CASA return value, and
declared products. Wrappers for CASA tasks, including `plotms`, use CASA's
parameter order, argument kinds, and defaults. Additional casa-rs controls
are keyword-only. These wrappers return CASA's value, including NumPy arrays
where CASA returns them. Wrapper docstrings identify parameters limited to
their CASA defaults and cite the relevant issues. The casa-rs tasks
`calibrate`, `msexplore`, and `msconvert` take keyword arguments and return
the `TaskCompletion`.

On a single MeasurementSet, `split` and `mstransform` accept named fields
and spectral windows, antenna baselines, scan predicates, calendar times,
observation IDs and intents, array IDs, UV distances, and feed pairs.
Channel selectors can include disjoint ranges and strides. Use a
spectral-window qualifier such as `correlation="SPW0:XX,YY"` to select
correlation products within a window. The CASA comparison checks unaveraged
visibilities, flags, weights, and remapped metadata. Remaining behaviors
are tracked in
[#978](https://github.com/bglenden/casa-rs/issues/978).

For stored visibilities, `datacolumn="model"` and `"corrected"` copy the
selected source into `DATA`. Combined layouts retain the named columns.
`"all"` copies the available physical visibility columns, including
`FLOAT_DATA` and `LAG_DATA` when present.

A value the contract refuses raises `ValueError` before anything starts, or
`TypeError` for a value of the wrong type. A task that refuses its
parameters raises `TaskParameterError`, a `ValueError`, and one that starts
and does not succeed `TaskExecutionError`, a `RuntimeError`. The
`.completion` of either says how it ended. `TaskResultError` means a task
succeeded but its result could not be decoded or rendered.

A Python task call saves no Last or Last Successful profile; those record
command-line runs. The CASA-named wrappers take the surface's parameters. A
profile runs through one as `tasks.gaincal(**parameters.load(path))`, since
a draft, as a mapping, holds the parameters that have a value.

`TaskParameters` (a `SurfaceParameters` constrained to a task definition) is
mapping-like and retains value origins and validation diagnostics. Saving
writes only required values and differences from current defaults. Surface
definitions and defaults are read from the same Rust catalog that the task
executables' command lines use.

The catalog-driven generic runner and generated CASA-named functions are the
only public task path. Generated conveniences live directly in `casars.tasks`;
their checked `.pyi` file is regenerated from
`crates/casa-provider-contracts/resources/parameter-surfaces.json`.

## `casars.data`

::: casars.data

## `casars.tasks`

::: casars.tasks

## `casars.tools`

::: casars.tools

## `casars.simutil`

::: casars.simutil
