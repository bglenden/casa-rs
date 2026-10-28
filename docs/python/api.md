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
casars.tasks.<task>(**casa_parameters)
```

A task call starts the task's own executable and returns a `TaskCompletion`
(`task`, `command`, `workspace`, `returncode`, `stdout`, the reply; `stderr`,
the log; `provider_invocation`, `status`, `message`, `parameter`, `value`,
CASA's return value; and `result`, the declared products). A task that does
not succeed raises `TaskExecutionError`, a `RuntimeError` whose `.completion`
says how it ended; a value the contract refuses raises `ValueError` before
anything starts; `TaskBinaryNotFoundError` (a `FileNotFoundError`) names
what the executable search lacked; `TaskInvocationError` means the process
could not start; `TaskResultError` means a success's result was malformed.

A Python task call saves no Last or Last Successful profile; those record
command-line runs. The CASA-named wrappers take CASA's parameters only. A
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
