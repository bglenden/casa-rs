# Task Parameter Profiles

Truth class: normative user contract
Status: implemented
Verification: `just docs-check`; `<executable> --help` (for example
`imexplore --help`)

`casa-rs` uses one parameter model for its one-shot tasks. A profile can be
loaded by a task executable's command line or manipulated from Python without
changing the parameter names or their meaning. Every parameter surface is a
task (ADR-0017).

This guide describes the implemented interface from ADR-0006, with current-only
surface evolution defined by ADR-0012. Provider JSON requests, results, and
progress remain machine transports; TOML is the sole human-authored
parameter-profile format.

The [generated parameter reference](reference/task-parameters.md) lists every
cataloged task surface, its parameters, and their canonical concept revisions,
types, units, defaults, groups, and summaries.

## Sparse TOML format

Parameter profiles are UTF-8 TOML with an ASCII-oriented syntax. They are
designed to paste cleanly into Markdown:

```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 27

[parameters]
vis = "data/target.ms"
imagename = "products/target"
imsize = 1024
cell = "0.2arcsec"
field = "target"
spw = "0:32~255"
specmode = "cube"
weighting = "briggs"
niter = 10000
threshold = "2mJy"
```

Only required values and values that differ semantically from current defaults
are written. Resetting a field removes its override and reveals the current
default. Quantities are compared by value and dimension, so equivalent values
such as `"0.001Jy"` and `"1mJy"` do not create a false override.

Files do not execute code. Includes, URL loading, environment expansion, shell
expressions, and executable selection are not part of the format. Unknown,
duplicate, inactive, or wrong-surface parameters are errors with source
locations where possible. A compound value is replaced as one value; profiles
are not recursive merge patches.

The imager profile contract is current-only. Its `contract` must equal the
current imager surface contract listed in the generated parameter reference,
and `[parameters]` must use canonical names. Stale contracts and historical
aliases are rejected rather than migrated or silently interpreted. Other
surfaces retain older profiles only when their canonical definition publishes
an explicit ordered migration chain.

Relative dataset and product paths resolve against the explicit workspace or
the process current directory. They do not resolve relative to the profile file
merely because the file lives elsewhere.

## Native EVLA AW convolution functions

With `gridder = "awproject"`, `aw_cf_source = "casa-import"` is the default and
requires an explicit `cfcache` input directory. CASA `CFS_`/`WTCFS_` cells are
imported read-only; `auto` does not select or create a cache.

Select `aw_cf_source = "native-evla"` to use the frozen homogeneous 25 m EVLA
dish model without CASA at runtime. Supply `evla_surface` (the explicit dish
surface input file), `native_cf_cache` (a separate private cache directory),
`native_cf_working_size`, `native_cf_oversampling`, `native_cf_cache_bytes`, and
`native_cf_maximum_cells`. Sampling and resource bounds are explicit request
parameters, not inferred from a cache's contents. The native cache is **not
CASA-readable** and cannot also be supplied as `cfcache`.

`native_cf_policy` defaults to `"reuse-only"`: an incomplete or invalid cache
fails without generation. `"generate-missing"` creates absent cells and retains
valid completed cells; corruption remains an error. `"regenerate"` explicitly
replaces all requested cells. Neither source falls back to the other. These parameters are
available through the same CLI flags, task JSON, sparse profiles, and generated
Python wrapper. Profiles store the requested paths/action/bounds, not compiled
plans, generated cells, cache inventory, or publication state. This exposes the
frozen EVLA case, not general telescope support.

## Sources and precedence

Every task run resolves its parameters from exactly one base source:

- Defaults
- Last attempted
- Last successful
- an explicit profile file

Resolution order is:

```text
current defaults
  -> one base source
  -> explicit CLI or Python overrides
```

Dataset-derived suggestions are explicit values, not hidden defaults. Choosing
another base source replaces the base rather than merging into it.

Command-line and Python calls start from Defaults unless a source is specified.
A missing managed profile is distinct from an unreadable or invalid one:
asking for Last when no Last file exists is reported as missing, and a present
corrupt Last file is reported as an error and is never silently replaced by
Defaults.

## Managed Last files

By default, managed state lives under the workspace:

```text
<workspace>/.casa-rs/parameters/<surface-id>/last.toml
<workspace>/.casa-rs/parameters/<surface-id>/last-successful.toml
```

Last records command-line runs. Last attempted is saved after resolution and
validation, immediately before execution. Last successful changes only after a
successful result. Cancellation or failure leaves Last successful unchanged.
`--no-save-last` skips both writes for one run.

A Python task call saves neither file; reading an existing Last
(`parameters.last(...)`) and writing one explicitly
(`TaskParameters.write_last()`) remain available. A task executable's
`--json-run`, the path Python's calls take, reads and writes no Last either.
Last therefore has one owner: the task front end, for a command-line run of a
task executable.

`casa-task-runtime::ParameterRuntime` owns source resolution and managed Last
persistence; the CLI and Python projections call that boundary rather than
keeping their own Last-state logic.

Set `CASA_RS_STATE_DIR` to redirect managed state in automation. This changes
the state location, not the workspace used for relative task paths.

Automatic Last updates never overwrite an explicit profile file.

## CLI contract

Each task runs from its own executable, which the application catalog names
(`calibrate`, `imexplore`, `casars-imager`, `simobserve`, ...). Every task
executable runs on the shared task front end in `casa-task-runtime`, so all
take the same command line; `--help` prints it with the surface's
parameters.

```text
<executable> [SURFACE] [SOURCE] [OVERRIDES] [CONTROLS]
<executable> [SURFACE] --show [SOURCE] [OVERRIDES] [--save-params FILE]
<executable> [SURFACE] --template | --profile-template | --describe [NAME] | --validate FILE
<executable> --json-run <SOURCE> | --json-schema | --protocol-info
```

- `SURFACE` is the first word of a family executable, which hosts several
  surfaces: `calibrate` (`calibrate`, `applycal`, `gaincal`, `bandpass`,
  `fluxscale`, `gencal`), `imexplore` (`imhead`, `imstat`), `msexplore`
  (`msexplore`, `plotms`) and `mstransform` (`mstransform`, `split`). Without
  it the first hosted surface runs. Other executables host one surface.
- `SOURCE` is exactly one of `--defaults` (the default), `--last`,
  `--last-successful` or `--params FILE`.
- `OVERRIDES` are the flags the surface declares, or `--unset NAME`, which
  removes an override.
- `CONTROLS` are `--workspace DIR` (where relative paths resolve and Last and
  Last Successful live), `--save-params FILE` (also write the run's resolved
  sparse profile) and `--no-save-last`.

For example:

```text
calibrate gaincal --ms ngc5921.ms --out ngc5921.G0 --refant VA15 --solint inf
imexplore imstat --last --show --save-params imstat.toml
```

The non-executing actions resolve the same sources and overrides and run
nothing: `--show` prints the resolved parameters, and with
`--save-params FILE` saves them as a sparse profile; `--template` writes a
documented example with defaults and units in comments, whose commented
defaults are not active overrides; `--describe [NAME]` describes the surface
or one parameter; `--validate FILE` checks a profile against the surface.
Overrides use the generated CLI flag spellings listed in the generated
parameter reference, or the parameter's own name, such as `--vis` or
`--threshold`; some surfaces declare their own spellings, such as
calibrate's `--ms` and `--out`.

A list's command-line form is comma-separated (`--archivefiles a.exp,b.exp`).
A flag repeated for a list without a fixed size adds to it, so
`--archivefile a.exp --archivefile b.exp` names both archives; any other
repeated flag takes its last value. A path list given as one string, on the
command line, in a profile or in a `--json-run` request, is split at its
commas before each path is normalized, so every route names the same paths.
A refused value names its parameter, on the command line as in a reply's
`parameter`.

There are no run-time confirmations. CASA's own parameters are the consent:
`overwrite=true` replaces a product, and a task that changes data in place
does so, as in CASA. Where CASA guards a change, casa-rs does too, for example
`applycal` and `flagdata` saving the flags to a new flag version first
(`flagbackup`, default on). No task takes a confirmation flag.

The machine actions (`--protocol-info`, `--json-schema`, `--json-run`) are
the same on every task executable; `--json-run` gives a typed reply
([provider contracts](provider-contracts.md#the-shared-task-front-end-and-typed-replies)).
A spelling the surface declares is always its parameter: imregrid's
`--template` is its template image, so there `--profile-template` writes the
profile. A run started from a task's command line enters its `--workspace`
first, so its relative paths resolve there, as they do for a Python call in
its workspace.

## Python contract

The Python API exposes the same profile model:

```python
from casars import parameters, tasks

p = parameters.defaults("imager", workspace="project")
p.update(
    vis="data/target.ms",
    imagename="products/target",
    imsize=1024,
    cell="0.2arcsec",
)
p.save("profiles/target-imaging.toml")

result = p.run()                          # in the draft's workspace
result = tasks.run("imager", p, niter=0)  # the draft, with one more value
```

Companion constructors are `parameters.last(...)`,
`parameters.last_successful(...)`, and `parameters.load(path, ...)`.
`TaskParameters` supports mapping-style updates, `reset(name)`, `reload()`,
`save(path)`, `provider_invocation()`, and `run(**values)`. As a mapping, a
draft holds the parameters that have a value, so a loaded profile runs
through a CASA-named wrapper, in the current directory, as
`tasks.gaincal(**parameters.load(path))`. The
invocation exposes the provider protocol name/version, the arguments (the
surface's fixed arguments, then `--json-run -`), and the exact canonical
request stdin.

`tasks.run(task, parameters=None, /, *, workspace=None, **values)` accepts
every catalog task. It starts from the draft, or Defaults, sets `values`,
and runs in `workspace`: the draft's, or the current directory. The
CASA-named wrappers take the surface's parameters and run in the current
directory. Each call starts the task's own executable, found by the resolver
([Install](install.md)), with `--json-run -`, and passes its log lines to
`sys.stderr` as they arrive. The log reaches stderr at the logging
configuration's threshold, WARN by default;
`CASA_RS_LOG_STDERR_PRIORITY=INFO` also shows a task's INFO lines, such as
its start and completion. Ctrl-C (`KeyboardInterrupt`) is passed to the task
as SIGINT; the task has ten seconds to stop before it is killed, and the
`KeyboardInterrupt` is then raised again.

A value the contract refuses, such as a parameter its gridder does not read,
raises `ValueError` before anything starts, or `TypeError` when the value
has the wrong type. A task that refuses its parameters itself, such as an
input that does not exist, raises `casars.tasks.TaskParameterError`, a
`ValueError`; one that starts and does not succeed raises
`casars.tasks.TaskExecutionError`, a `RuntimeError`. Both carry the
`.completion`, whose `status` is `refused`, `failed`, `interrupted`,
`signalled` or `malformed_reply`, with its `message` and the `parameter` it
names. `TaskBinaryNotFoundError`, a `FileNotFoundError`, says what the
resolver lacked; `TaskInvocationError` means the executable could not be
started, and `TaskResultError` that a success's result was malformed.
`tasks.run` returns a `TaskCompletion` with the `command` run, its
`workspace`, `returncode`, `stdout` (the reply), `stderr` (the log), the
`provider_invocation`, CASA's return `value` from the reply, and the
declared products (`result`, `products`); result projection remains
Rust-owned.

The CASA-named wrappers and their type stubs are generated from the catalog,
under `casars.tasks`; there is no second provider-specific task API. A
surface that names its CASA task (`casa_task`; the image tasks and
`simobserve` so far) has CASA's exact signature: CASA's parameters in CASA's
order, positional or by keyword, with CASA's defaults, then casa-rs's own
controls, keyword-only. Its wrapper returns CASA's value: `imstat`'s
statistics dictionary with NumPy arrays, `immath`'s `True`, `None` for most
tasks. A parameter casa-rs does not implement yet is taken only at CASA's
default and refused at any other value, naming its issue; the wrapper's
docstring lists them. The other wrappers take keyword arguments and return
the `TaskCompletion` until their families are converted (#931).
[CASA compatibility](reference/casa-compatibility.md) compares each surface
with its CASA task.

## What profiles intentionally exclude

- `casars.data.Image` and `casars.data.Table` object-handle lifecycle
- progress files, executable overrides, Last controls, and telemetry
- saved region and mask mutations, which are dataset artifacts
- an exact resolved historical replay record

If exact run replay is needed, record a separate run manifest containing the
fully resolved invocation and versions. A sparse profile records user intent
under the current contract and defaults.
