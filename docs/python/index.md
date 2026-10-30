# Python Support

`casa-rs-python` installs as `casars` and separates four public concerns:

- `casars.data` provides in-process access to persistent CASA-style images and
  tables.
- `casars.tasks` provides higher-level task wrappers, each of which starts
  its task's own executable.
- `casars.tools` provides CASA's tools as classes under CASA's names, with
  CASA's methods, parameter order and defaults, so a CASA script changes only
  its imports (ADR-0019): `simulator` (CASA's `sm`), `measures` (`me`) and
  `quanta` (`qa`). Methods casa-rs does not implement yet raise
  `NotImplementedError`. A tool's constructor takes only casa-rs's controls,
  keyword-only: `simulator(*, memory_budget=None, backend='auto',
  workers=None)` caps the memory `close` holds (bytes), names the device to
  predict on (`'auto'`, `'cpu'` or `'metal'`) and the threads to keep busy;
  `None` takes the machine's balanced share, and the MeasurementSet written
  is the same whatever the controls. `casars.simutil` provides the parts of
  CASA's `simutil` helper class (`casatasks.private.simutil`, not a tool)
  that `sm` scripts and `simobserve` use: `readantenna`, the mosaic
  pointings (`calc_pointings2`, pointing files), the direction helpers,
  `noisetemp`, `bandname`, `polsettings`, the baseline helpers, and the
  sky-model helpers `modifymodel` and `flatimage`. `from casars import
  simutil` is its public import.
- `casars.parameters` is the accepted common API for typed task profiles
  described by ADR-0006 and the current-only option in ADR-0012.

The generic `casars.tasks.run()` entry point accepts every catalog task. Every
task is also exposed exactly once as `casars.tasks.<task>` through a generated
CASA-named function that takes CASA's parameters. These functions share the
Rust-owned parameter and result contracts. A call resolves its parameters in
Rust, starts the task's executable as a child process with `--json-run -`,
passes the task's log lines to `sys.stderr` as they arrive. A task whose
surface names its CASA task (the image tasks, the MeasurementSet and
flagging tasks, the calibration tasks and `simobserve` so far) has CASA's
exact signature and returns CASA's value; the others return a
`TaskCompletion` until their families are converted (#931). A task that
refuses its parameters raises `casars.tasks.TaskParameterError`, a
`ValueError`, and one that does not succeed
`casars.tasks.TaskExecutionError`. The executable is the task's
`CASARS_<APP>_BIN` override when it is set, else the one in a suite's `bin/`
(`CASARS_SUITE_ROOT`, or the suite the package was installed into), else, for
a package built from a checkout, that checkout's build
([Compatibility](versioning.md#task-runner-compatibility)). The in-process
`casars.data`, `casars.tools` and `casars.simutil` need no executables.

`casars.data` is stateful and file-backed. It is intended for interoperability
with the Python ecosystem through NumPy-native reads and writes. For images,
the current object surface includes pixel-slice writes on existing persistent
images; image creation and coordinate-system authoring remain outside the v1
object contract.

## Parameter profiles

The accepted profile API is shared with the task executables' command lines:

```python
from casars import parameters, tasks

p = parameters.load("profiles/target-imaging.toml", workspace="project")
p.update(niter=20000, threshold="1mJy")

result = tasks.run("imager", p)   # runs in the draft's workspace, project/
```

A loaded profile also runs through a CASA-named wrapper, in the current
directory: `tasks.imager(**parameters.load("profiles/target-imaging.toml"))`.

The call saves no Last profile. `parameters.last(...)` reads the Last that a
command-line run saved.

`parameters.defaults(surface, ...)` and `parameters.last(surface, ...)` create
the other common source types. The resulting `TaskParameters` supports mapping
updates, reset, reload, sparse TOML save, and run. Rust remains the sole parser,
normalizer, validator, migration engine, and sparse renderer.

`casars.tasks` is generated from the 26 task definitions and supplies one
CASA-named keyword wrapper and typing stub per task. Their signatures
deliberately use an unset sentinel instead of copying catalog defaults into
Python.

Python binds Rust through PyO3 alone, in one native module, `casars._core`.
It carries task parameters, task results and MeasurementSet plot documents as
well as the in-process objects: the `casars.data.Image` and
`casars.data.Table` NumPy object layer, the CASA tool classes of
`casars.tools` and the `casars.simutil` class.

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
