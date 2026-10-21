# Python Support

`casa-rs-python` installs as `casars` and separates four public concerns:

- `casars.data` provides in-process access to persistent CASA-style images and
  tables.
- `casars.tasks` provides higher-level task wrappers backed by the versioned
  provider binaries.
- `casars.tools` provides CASA's tools as classes under CASA's names, with
  CASA's methods, parameter order and defaults, so a CASA script changes only
  its imports (ADR-0019): `simulator` (CASA's `sm`), `measures` (`me`) and
  `quanta` (`qa`). Methods casa-rs does not implement yet raise
  `NotImplementedError`. `casars.simutil` provides `readantenna` of CASA's
  `simutil` helper class (`casatasks.private.simutil`), which is not a tool;
  `from casars import simutil` is its public import.
- `casars.parameters` is the accepted common API for typed task profiles
  described by ADR-0006 and the current-only option in ADR-0012.

The generic `casars.tasks.run()` entry point accepts every catalog task. Every
task is also exposed exactly once as `casars.tasks.<task>` through a generated
CASA-named function. These functions share the Rust-owned parameter and result
contracts and run through the canonical `casars run` task host.

`casars.data` is stateful and file-backed. It is intended for interoperability
with the Python ecosystem through NumPy-native reads and writes. For images,
the current object surface includes pixel-slice writes on existing persistent
images; image creation and coordinate-system authoring remain outside the v1
object contract.

## Parameter profiles

The accepted profile API is shared with the `casars` command line:

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

`casars.tasks` is generated from the 27 task definitions and supplies one
CASA-named keyword wrapper and typing stub per task. Their signatures
deliberately use an unset sentinel instead of copying catalog defaults into
Python.

Generated UniFFI (`casars._frontend`, from `casars-frontend-services`) owns
task, parameter, and result transport. PyO3 carries the in-process objects:
the `casars.data.Image` and `casars.data.Table` NumPy object layer, the CASA
tool classes of `casars.tools` and the `casars.simutil` class. The wheel still
ships one native `_core` library rather than two competing application
bindings.

## Plotting and image inspection

casa-rs does not rasterise plots. Numeric MeasurementSet plot documents are
available as `casars.data.measurement_set_plot(...)` and are drawn with
Matplotlib by `casars.data.plot_matplotlib(...)`. `casars.imexplore` reads
image planes as NumPy arrays (`casars.imexplore.data(...)`) and draws them with
Matplotlib (`casars.imexplore.imshow(...)`).

See [Task Parameter Profiles](../task-parameters.md) for sparse TOML,
managed Last paths, source precedence, and examples.

Tagged releases build Python artifacts for Linux `x86_64` and macOS `arm64`,
plus a source distribution for environments that need to build from source.
For suite installation and shell `PATH` setup, see [Install](../install.md).
