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
TaskParameters.run(*, workspace=None)

casars.tasks.run(surface, *, parameters=None, workspace=None)
```

A Python task call saves no Last or Last Successful profile; those record
command-line runs. `save_last`, `confirm_overwrite` and `confirm_mutation` are
still accepted and ignored until the task binaries are started directly
(#931).

`TaskParameters` (a `SurfaceParameters` constrained to a task definition) is
mapping-like and retains value origins and validation diagnostics. Saving
writes only required values and differences from current defaults. Surface
definitions and defaults are read from the same Rust catalog used by
`casars run`.

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
