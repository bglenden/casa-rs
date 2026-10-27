# Task Parameter Profiles

Truth class: normative user contract
Status: implemented
Verification: `just docs-check`; `casars params --help`

`casa-rs` uses one parameter model for its one-shot tasks. A profile can be
loaded by the `casars` command line or manipulated from Python without changing
the parameter names or their meaning. Every parameter surface is a task
(ADR-0017).

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
contract = 26

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
(`TaskParameters.write_last()`) remain available. A provider binary's
`--json-run` reads and writes no Last either. Each run therefore has exactly
one owner of Last: `casars run` for the runs it launches, the task front end
for a binary's own command line.

`casa-task-runtime::ParameterRuntime` owns source resolution and managed Last
persistence; the CLI and Python projections call that boundary rather than
keeping their own Last-state logic.

Set `CASA_RS_STATE_DIR` to redirect managed state in automation. This changes
the state location, not the workspace used for relative task paths.

Automatic Last updates never overwrite an explicit profile file.

## CLI contract

The task host is `casars`. Run with no arguments or `--help` it prints usage.

```text
casars run <task>
  [--defaults | --last | --last-successful | --params FILE]
  [task overrides]
  [--unset NAME]
  [--workspace DIR]
  [--save-params FILE]
  [--no-save-last]
```

The parameter utilities are:

```text
casars params validate <file>
casars params show <surface> [source options] [overrides]
casars params save <surface> <file> [source options] [overrides]
casars params template <surface>
casars params describe <parameter-or-surface>
```

`template` writes a documented example with defaults and units in comments;
commented defaults are not active overrides. Overrides use the generated CLI
flag spellings listed in the generated parameter reference, or the parameter's
own name, such as `--vis` or `--threshold`.

There are no run-time confirmations. CASA's own parameters are the consent:
`overwrite=true` replaces a product, and a task that changes data in place
does so, as in CASA. Where CASA guards a change, casa-rs does too, for example
`applycal` and `flagdata` saving the flags to a new flag version first
(`flagbackup`, default on). `casars run` still accepts the retired
`--confirm-overwrite` and `--confirm-mutation` and ignores them until the
launcher is retired (#931).

Direct provider binaries continue to expose their idiomatic execution
transports (`--protocol-info`, `--json-schema`, `--json-run`); source selection
and profile lifecycle are owned by `casars run` and `casars params`. The
shared task front end in `casa-task-runtime`, which the binaries move onto
next, gives each binary those same sources and controls, `--show`,
`--template`, `--describe` and `--validate`, and a typed `--json-run` reply
([provider contracts](provider-contracts.md#the-shared-task-front-end-and-typed-replies)).

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

result = tasks.run(
    "imager",
    parameters=p,
    workspace="project",
)
```

Companion constructors are `parameters.last(...)`,
`parameters.last_successful(...)`, and `parameters.load(path, ...)`.
`TaskParameters` supports mapping-style updates, `reset(name)`, `reload()`,
`save(path)`, `provider_invocation()`, and `run()`. The invocation exposes the
provider protocol name/version and the exact canonical request stdin.
`tasks.run()` accepts every catalog task and routes through `casars run`;
a request the catalog refuses, such as a parameter its gridder does not read,
raises `ValueError` before launch, and the provider reports what the installed
build cannot run when it runs. Successful completions retain the same provider
invocation while result projection remains Rust-owned. Catalog-generated
CASA-named keyword wrappers and type stubs are available directly under
`casars.tasks` without copying defaults into Python signatures; there is no
second provider-specific task API.

## What profiles intentionally exclude

- `casars.data.Image` and `casars.data.Table` object-handle lifecycle
- progress files, executable overrides, Last controls, and telemetry
- saved region and mask mutations, which are dataset artifacts
- an exact resolved historical replay record

If exact run replay is needed, record a separate run manifest containing the
fully resolved invocation and versions. A sparse profile records user intent
under the current contract and defaults.
