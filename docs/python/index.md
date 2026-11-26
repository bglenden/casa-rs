# Python Support

`casa-rs-python` installs as `casars` and separates four public concerns:

- `casars.data` provides in-process access to persistent CASA-style images and
  tables.
- `casars.tasks` provides higher-level task wrappers that run in process.
- `casars.tools` provides CASA's tools as classes under CASA's names, with
  CASA's methods, parameter order and defaults, so a CASA script changes only
  its imports (ADR-0017): `simulator` (CASA's `sm`), `measures` (`me`),
  `quanta` (`qa`), `componentlist` (`cl`), `image` (`ia`), `coordsys`
  (`cs`), `table` (`tb`), `msmetadata` (`msmd`) and `ms`. Each has every
  method of CASA 6.7.6.14's binding; a method casa-rs does not implement yet
  raises `NotImplementedError` naming the issue that records it. A tool's
  constructor takes only casa-rs's controls,
  keyword-only: `simulator(*, memory_budget=None, backend='auto',
  workers=None)` caps the memory `close` holds (bytes), names the device to
  predict on (`'auto'`, `'cpu'` or `'metal'`) and the threads to keep busy;
  `None` takes the machine's balanced share, and the MeasurementSet written
  is the same whatever the controls. `casars.simutil` provides the parts of
  CASA's `simutil` helper class (`casatasks.private.simutil`, not a tool)
  that `sm` scripts and `simobserve` use: `readantenna`, the mosaic
  pointings (`calc_pointings2`, pointing files), the direction helpers,
  `noisetemp`, `bandname`, `polsettings`, the baseline helpers, and the
  sky-model helpers `modifymodel` and `flatimage`, and the analysis helpers
  `ismstp`, `cellsize`, `statim` and `convimage`, the geodesy helpers, and
  `msg`, the report helpers, `dir_s2m`, `is4d`, `spectral` and `setcfg`.
  Its plotting methods `newfig`, `nextfig`, `endfig`, `plotants`,
  `ephemeris` and the default `statim(plot=True)` use the `plot` extra.
  Rust prepares all scientific arrays; Matplotlib draws onto the current
  figure and panel, so scripts can annotate and save them as in CASA.
  Its other methods have
  CASA's signatures and raise `NotImplementedError` naming the issue that
  records them. `from casars import simutil` is its public import.
- `casars.parameters` is the accepted common API for typed task profiles
  described by ADR-0006.

The generic `casars.tasks.run()` entry point accepts every catalog task. Every
task is also exposed exactly once as `casars.tasks.<task>` through a generated
CASA-named function that takes CASA's parameters. These functions share the
Rust-owned parameter and result contracts. Native execution releases the GIL
and relays log lines to `sys.stderr`. The generic runner returns those lines
in `TaskCompletion.logs` alongside status, CASA return value, and products.
A wrapper for a CASA-named surface returns CASA's value with its original
signature. Wrappers for `calibrate`, `msexplore`, `msconvert`, and `plotms`
return a completion. Tasks are included in the Python wheel.

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

result = tasks.run("tclean", p)   # runs in the draft's workspace, project/
```

A loaded profile also runs through a CASA-named wrapper, in the current
directory: `tasks.tclean(**parameters.load("profiles/target-imaging.toml"))`.

The call saves no Last profile. `parameters.last(...)` reads the Last that a
command-line run saved.

`parameters.defaults(surface, ...)` and `parameters.last(surface, ...)` create
the other common source types. The resulting `TaskParameters` supports mapping
updates, reset, reload, sparse TOML save, and run. Rust remains the sole parser,
normalizer, validator, and sparse renderer.

`casars.tasks` is generated from the catalog's task definitions and supplies
one CASA-named wrapper and typing stub per task. A task that mimics CASA's
has CASA 6.7.6.14's signature, defaults and return value; the casa-rs tasks
(`calibrate`, `msexplore`, `msconvert`) and `plotms` take keywords with an
unset sentinel instead of copying catalog defaults into Python.

Python binds Rust through PyO3 alone, in one native module, `casars._core`.
It carries task parameters, task results, MeasurementSet plot documents, the
classes in `casars.tools`, and `casars.simutil`. Use `casars.tools.image`
for NumPy pixel access and native image editing.

## Plotting and image inspection

casa-rs does not rasterise plots. Numeric MeasurementSet plot documents are
available as `casars.data.measurement_set_plot(...)` and are drawn with
Matplotlib by `casars.data.plot_matplotlib(...)`. `casars.imexplore` reads
image planes as NumPy arrays (`casars.imexplore.data(...)`) and draws them with
Matplotlib (`casars.imexplore.imshow(...)`).

Install `casa-rs-python[plot]` for simulation graphics. `tasks.simobserve`
honours `graphics='file'`, `'screen'`, `'both'` and `'none'`: it saves the
model/pointing and observation figures, displays them, does both, or skips
graphics. Figures include the complete model and UV samples. The
interferometric quick PSF is a CASA image beside the MeasurementSet.
The generic `tasks.run('simobserve', ...)` completion lists PNG files after
they have been rendered. Displaying figures uses the Python session's
Matplotlib backend.

See [Task Parameter Profiles](../task-parameters.md) for sparse TOML,
managed Last paths, source precedence, and examples.

Tagged releases build Python artifacts for Linux `x86_64` and macOS `arm64`,
plus a source distribution for environments that need to build from source.
For suite installation and shell `PATH` setup, see [Install](../install.md).
