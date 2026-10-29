# API Reference

## Common parameter API

ADR-0006 defines this common surface for task profiles; ADR-0012 defines the
current-only option used by imager:

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

A task call starts the task's own executable. `tasks.run` returns a
`TaskCompletion` (`task`, `command`, `workspace`, `returncode`, `stdout`, the
reply; `stderr`, the log; `provider_invocation`, `status`, `message`,
`parameter`, `value`, CASA's return value; and `result`, the declared
products). A wrapper whose surface names its CASA task (the image tasks,
`flagdata`, `flagmanager`, `importvla`, `mstransform`, `split` and
`simobserve` so far) has CASA's signature, positional or by keyword with
CASA's defaults, then casa-rs's own controls, keyword-only, and returns
CASA's value, with NumPy arrays where CASA returns them. A parameter casa-rs
does not implement yet is taken only at CASA's default; its docstring names
the issue. The other wrappers take keyword arguments and return the
`TaskCompletion` until their families are converted (#931).

A value the contract refuses raises `ValueError` before anything starts, or
`TypeError` for a value of the wrong type. A task that refuses its
parameters raises `TaskParameterError`, a `ValueError`, and one that starts
and does not succeed `TaskExecutionError`, a `RuntimeError`; the
`.completion` of either says how it ended. `TaskBinaryNotFoundError` (a
`FileNotFoundError`) names what the executable search lacked;
`TaskInvocationError` means the process could not start; `TaskResultError`
means a success's result was malformed.

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
