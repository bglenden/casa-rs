# Python Support

`casa-rs-python` installs as `casars` and separates three public concerns:

- `casars.data` provides in-process access to persistent CASA-style images and
  tables.
- `casars.tasks` provides higher-level task wrappers backed by the versioned
  provider binaries.
- `casars.parameters` is the accepted common API for typed task and session
  profiles described by ADR-0006.

The generic `casars.tasks.run()` entry point accepts every catalog task. Every
task is also exposed exactly once as `casars.tasks.<task>` through a generated
CASA-named function. These functions share the Rust-owned parameter and result
contracts and run through the canonical `casars` launcher.

`casars.data` is stateful and file-backed. It is intended for interoperability
with the Python ecosystem through NumPy-native reads and writes. For images,
the current object surface includes pixel-slice writes on existing persistent
images; image creation and coordinate-system authoring remain outside the v1
object contract.

## Parameter profiles

The accepted profile API is shared with the CLI, TUI, and native GUI:

```python
from casars import parameters, tasks

p = parameters.load("profiles/target-imaging.toml", workspace="project")
p.update(niter=20000, threshold="1mJy")

result = tasks.run(
    "imager",
    parameters=p,
    workspace="project",
    save_last=True,
)
```

`parameters.defaults(surface, ...)` and `parameters.last(surface, ...)` create
the other common source types. The resulting `TaskParameters` supports mapping
updates, reset, reload, sparse TOML save, and run. Rust remains the sole parser,
normalizer, validator, migration engine, and sparse renderer.

`casars.tasks` is generated from the 40 task definitions and supplies one
CASA-named keyword wrapper and typing stub per task. The corresponding
session conveniences are `casars.sessions.imexplore(...)` and
`casars.sessions.tablebrowser(...)`. Their signatures deliberately use an
unset sentinel instead of copying catalog defaults into Python.

Generated UniFFI owns task, parameter, session, and result transport. PyO3 is
retained only for the `casars.data.Image` and `casars.data.Table` NumPy object
layer; the wheel still ships one native `_core` library rather than two
competing application bindings. Numeric MeasurementSet plot documents are
available as `casars.data.measurement_set_plot(...)`.

Session profiles for `imexplore` and `tablebrowser` use the same load, validate,
and save model. `casars.sessions.open(...)` and `SessionParameters.open()` apply
their durable startup configuration; live browser commands remain stateful
session APIs rather than becoming task calls.

See [Task and Session Parameter Profiles](../task-parameters.md) for sparse TOML,
managed Last paths, source precedence, and examples.

Tagged releases build Python artifacts for Linux `x86_64` and macOS `arm64`,
plus a source distribution for environments that need to build from source.
For suite installation and shell `PATH` setup, see [Install](../install.md).
