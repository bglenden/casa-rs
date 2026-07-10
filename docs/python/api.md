# API Reference

## Common parameter API

ADR-0006 defines this common surface for task and session profiles:

```python
casars.parameters.defaults(surface, *, workspace=None)
casars.parameters.last(surface, *, workspace=None)
casars.parameters.last_successful(surface, *, workspace=None)
casars.parameters.load(path, *, workspace=None)

TaskParameters.update(**values)
TaskParameters.reset(name)
TaskParameters.reload()
TaskParameters.save(path)
TaskParameters.run(*, workspace=None, save_last=True)
SessionParameters.open(**options)

casars.tasks.run(surface, *, parameters=None, workspace=None, save_last=True)
casars.sessions.open(
    surface,
    *,
    parameters=None,
    profile=None,
    start="defaults",
    save_last=True,
    **options,
)
```

`TaskParameters` and `SessionParameters` are mapping-like and retain value
origins and validation diagnostics. Saving writes only required values and
differences from current defaults. Surface definitions and defaults are read
from the same Rust catalog used by the other frontends.

Task-specific protocol functions remain public where they expose specialized
results; the catalog-driven generic runner is the uniform path for all tasks.
Generated CASA-named conveniences live in `casars.tasks.catalog`; session
conveniences are `casars.sessions.imexplore` and
`casars.sessions.tablebrowser`. Their checked `.pyi` files are regenerated from
`crates/casa-provider-contracts/resources/parameter-surfaces.json`.

## `casars.data`

::: casars.data

## `casars.tasks.calibrate`

::: casars.tasks.calibrate
