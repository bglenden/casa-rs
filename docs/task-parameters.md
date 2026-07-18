# Task and Session Parameter Profiles

Truth class: normative user contract
Status: implemented
Verification: `just docs-check`; `casars params --help`

`casa-rs` uses one parameter model for one-shot tasks and durable browser-session
startup settings. A profile can be loaded by the CLI, edited in the TUI or
native GUI, or manipulated from Python without changing the parameter names or
their meaning.

This guide describes the implemented interface from ADR-0006. Provider JSON
requests, results, progress, and session events remain machine transports; TOML
is the sole human-authored parameter-profile format.

The [generated parameter reference](reference/task-parameters.md) lists every
cataloged task and session surface, its parameters, and their canonical concept
revisions, types, units, defaults, groups, and summaries.

## Sparse TOML format

Parameter profiles are UTF-8 TOML with an ASCII-oriented syntax. They are
designed to paste cleanly into Markdown:

```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

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

Relative dataset and product paths resolve against the explicit workspace, GUI
project root, or process current directory. They do not resolve relative to the
profile file merely because the file lives elsewhere.

## Sources and precedence

Every parameter session has exactly one base source:

- Defaults
- Last attempted or last opened
- Last successful, for task surfaces
- an explicit profile file

Resolution order is:

```text
current defaults
  -> one base source
  -> accepted context or startup values
  -> explicit UI, CLI, or Python overrides
```

Dataset-derived suggestions are explicit context values, not hidden defaults.
Loading another base source replaces the current base rather than silently
merging into a dirty form.

Noninteractive CLI and Python calls start from Defaults unless a source is
specified. Interactive TUI and GUI editors may start from a valid Last source,
but must show the active source, modified state, validation, and per-field
origins.

A missing managed profile is distinct from an unreadable or invalid profile:
interactive callers may explicitly choose Defaults when no Last file exists,
but a present corrupt Last file is reported and is never silently replaced by
Defaults.

## Managed Last files

By default, managed state lives under the workspace:

```text
<workspace>/.casa-rs/parameters/<surface-id>/last.toml
<workspace>/.casa-rs/parameters/<surface-id>/last-successful.toml
```

For a task, Last attempted is saved after resolution and validation immediately
before execution. Last successful changes only after a successful result.
Cancellation or failure leaves Last successful unchanged.

For `imexplore` and `tablebrowser`, `last.toml` is saved only after the root
opens successfully and after validated durable settings change. Browser
sessions do not have `last-successful.toml`. Cursor movement, scrolling,
viewport changes, playback, caches, and command history are live state and do
not trigger profile writes.

`casa-task-runtime::ParameterRuntime` owns source resolution and
`SessionLastCoordinator` owns successful-open persistence, the 350 ms debounce,
coalescing by normalized workspace/surface destination, background writes, and
clean-close flushing. Swift, Python, CLI, and TUI projections report lifecycle
events to that boundary rather than carrying independent timers or Last-state
maps.

`tablebrowser` bookmark values use stable explicit forms:
`cell:ROW:COLUMN`, `table-keyword:PATH`,
`column-keyword:COLUMN:PATH`, or `subtable:NAME`.

Set `CASA_RS_STATE_DIR` to redirect managed state in automation. This changes
the state location, not the workspace used for relative task paths.

An explicitly opened profile is read-only until the user chooses Save As.
Automatic Last updates never overwrite it.

## CLI contract

The accepted noninteractive shape is:

```text
casars run <task> [--workspace DIR] [--notebook FILE_OR_ID]
  [--defaults | --last | --last-successful | --params FILE]
  [task overrides]
  [--unset NAME]
  [--save-params FILE]
  [--no-save-last]
  [--no-notebook-recording]

casars open <session> [--workspace DIR]
  [--defaults | --last | --params FILE]
  [session overrides]
  [--unset NAME]
  [--save-params FILE]
  [--no-save-last]
```

The parameter utilities are:

```text
casars params validate <file>
casars params show <surface> [source options]
casars params save <surface> <file> [source options]
casars params template <surface>
casars params describe <parameter-or-surface>
```

`template` writes a documented example with defaults and units in comments;
commented defaults are not active overrides. Direct provider binaries continue
to expose their idiomatic execution transports; source selection and profile
lifecycle are owned by the top-level `casars` runner and interactive shells.
`--notebook` routes one project-aware task attempt to an existing named
notebook. Without it, the explicit workspace's `default.md` is used.
`--no-notebook-recording` is a visible one-run bypass and is never persisted in
a profile.

The TUI launcher remains `casars` (or its existing short surface-launch form).
`casars open <session>` launches a browser session from a resolved profile.
Open, Save As, Revert, Reset to Default, and source selection are
framework-owned actions.

## TUI and native GUI

Both interactive surfaces use a typed parameter session rather than separate
string maps and frontend defaults. They must display:

- the selected source and dirty state
- current values, defaults, units, and validation messages
- per-field origin and Reset to Default
- conditional activation and required state
- Defaults, Last, Last Successful where applicable, Open, Save As, and Revert

Loading another profile or resetting a dirty form requires confirmation.
Runtime controls such as progress telemetry, executable overrides, and safety
confirmation remain outside the saved science parameters.

## Python contract

The accepted Python API exposes the same profile session:

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

result = tasks.run(
    "imager",
    parameters=p,
    workspace="project",
    save_last=True,
)
```

Companion constructors are `parameters.last(...)`,
`parameters.last_successful(...)`, and `parameters.load(path, ...)`.
`TaskParameters` supports mapping-style updates, `reset(name)`, `reload()`,
`save(path)`, and `run()`. `tasks.run()` accepts every catalog task and routes
through the common runner; existing protocol-specific convenience modules
remain available for their specialized result APIs. Catalog-generated
CASA-named keyword wrappers and type stubs are available under
`casars.tasks.catalog` without copying defaults into Python signatures.

Session profiles for `imexplore` and `tablebrowser` use the same constructors
and validation model. Open them with `sessions.open(surface, parameters=p)` or
`SessionParameters.open()`. Their live command/event streams remain session
APIs, not entries in `TaskParameters`.

## What profiles intentionally exclude

- `casars` launcher theme, pane sizes, and recent UI state
- `casars.data.Image` and `casars.data.Table` object-handle lifecycle
- progress files, executable overrides, confirmations, and telemetry
- browser focus, viewport, cursor, scroll, cache, current playback, or command history
- saved region and mask mutations, which are dataset artifacts
- an exact resolved historical replay record

If exact run replay is needed, record a separate run manifest containing the
fully resolved invocation and versions. A sparse profile records user intent
under the current contract and defaults.
