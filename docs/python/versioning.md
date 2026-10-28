# Compatibility

## Parameter document compatibility

Sparse TOML documents carry two independent versions:

- `casars.format` versions the document envelope and TOML conventions.
- `casars.contract` versions the selected task definition.

Loading resolves sparse overrides against the current provider definition.
Newly added optional parameters receive current defaults. Omitted parameters
whose defaults changed also receive the new default and produce a compatibility
warning. Renames and type/value changes require an explicit ordered migration;
future or unmigratable versions fail before invocation.

This behavior preserves user intent rather than freezing an exact historical
request. Use a separate resolved run manifest when exact replay is required.
The Python implementation uses the shared Rust runtime and must not maintain an
independent migration or default table.

Managed state normally lives at:

```text
<workspace>/.casa-rs/parameters/<surface-id>/last.toml
<workspace>/.casa-rs/parameters/<surface-id>/last-successful.toml
```

`CASA_RS_STATE_DIR` redirects the managed root without changing workspace path
resolution. An explicit source profile
is never overwritten without an explicit save.

## Task-runner compatibility

Python has one task transport: every generated `casars.tasks.<task>` callable
and `casars.tasks.run` start the task's own executable, named by the
canonical application catalog, with its surface's fixed arguments and
`--json-run -`. One Rust resolver
(`casa_provider_contracts::resolve_task_executable`, through `casars._core`)
finds that executable. The first step that applies decides:

1. the task's catalog override variable, `CASARS_<APP>_BIN` (for example
   `CASARS_IMAGER_BIN` or `CASARS_CALIBRATE_BIN`), names the file;
2. a suite root holds it at `<root>/bin/<executable>`: `CASARS_SUITE_ROOT`,
   or else the installed suite the Python package belongs to (the
   installer's environment is `<suite>/python`, beside `<suite>/bin` and
   `bundle-manifest.json`);
3. a development checkout: the newest `debug` or `release` build of the
   executable under the workspace the package was built from, in
   `$CARGO_TARGET_DIR` or `target/`.

A step that applies but lacks the file raises `TaskBinaryNotFoundError`
naming what it lacked; it never falls through to a later step, so an
installed suite never runs a checkout's build. A missing development build is
reported with the `cargo build -p <package> --bin <executable>` that makes
it. Python does not search the current directory or `PATH`, and nothing runs
through `cargo`. A wheel installed outside a suite, such as one from PyPI,
runs tasks only with `CASARS_SUITE_ROOT` or the task's override set; its
in-process `casars.data`, `casars.tools` and `casars.simutil` need no
executables. The executable validates the provider contract and request
before execution; Python does not maintain a second protocol descriptor,
provider schema, or compatibility handshake.

Release tags also build and publish Python package artifacts separately from the
Rust binaries:

- GitHub release assets always receive the built wheels, source distribution,
  suite bundles, standalone binary bundles, and installer script.
- PyPI publication is attempted from the same workflow when `PYPI_API_TOKEN` is configured.

That means Python package compatibility has two layers:

1. package-version compatibility with the published wheel or sdist
2. runtime contract compatibility with the resolved suite and its
   catalog-owned task executables

The `casars.data` surface has its own narrower stability boundary in v1:

- stable in v1: reading existing persistent images and tables
- stable in v1: writing pixel slices into existing persistent images with `Image.put_slice`
- deferred from v1: image creation, coordinate-system authoring, and broader image metadata editing

The direct object-surface contract published by `casars.data.protocol_info()`
and `casars.data.schema_bundle()` is currently:

- protocol name: `casars_data_objects`
- protocol version: `1`
- surface kind: `object`

That boundary is intentional. The v1 compatibility promise covers file-backed data access and pixel updates, not the full CASA image-authoring surface.

The same bundle, still at protocol version 1, also describes the `casars.tools`
classes (`simulator`, `measures`, `quanta`). The bundle's Python projection
names `casars.data` as its module; a tool's projection entry names its own
module (`casars.tools`) and class, and records the parameter order of its
constructor's keyword-only controls and of each method, which the JSON Schema
properties do not keep. Its `annotations.python` entry records whether casa-rs
implements each method. A consumer reads an object's module from its own
projection entry and falls back to the bundle's. The provider contract
(`docs/provider-contracts.md`, "Object Surfaces") describes a tool's entry.

`casars.simutil` is the public import of CASA's `simutil` helper class
(`from casars import simutil`, for CASA's `from casatasks.private import
simutil`). It is not a CASA tool and is not in the bundle.

The installer-managed suite install layout behind steps 4-6 is:

```text
~/.local/opt/casa-rs/<version>/
  bin/
    calibrate
    casars-imager
    imexplore
    mstransform
    ...                 (every task executable of the catalog)
  python/
    ...
  wheels/
    ...
  bundle-manifest.json
~/.local/opt/casa-rs/stable -> ~/.local/opt/casa-rs/<stable-version>
~/.local/opt/casa-rs/rc -> ~/.local/opt/casa-rs/<rc-version>
~/.local/opt/casa-rs/current -> ~/.local/opt/casa-rs/<version>
~/.local/bin/
  calibrate -> ~/.local/opt/casa-rs/current/bin/calibrate
  calibrate-stable -> ~/.local/opt/casa-rs/stable/bin/calibrate
  calibrate-rc -> ~/.local/opt/casa-rs/rc/bin/calibrate
  ...                   (the same links for each executable in bin/,
                         except feather, immath and impv)
```

Each executable in the suite's `bin/` gets `<command>-<channel>` and, for
the active suite, `<command>` in `~/.local/bin`, except the CASA names that
are system commands or generic words: `feather`, `immath` and `impv` stay
off `PATH`, as `split` would (it is a coreutil; `mstransform split` runs
CASA's split). Python still starts them from the suite's `bin/`. A dangling
link into the install root, such as one to a command a newer suite no longer
has, is removed. A file of the user's, or a link elsewhere, at one of these
names is left in place, with a warning. That lets Python and the task executables behave as one
installed suite while keeping `~/.local/bin` as the only `PATH` entry users
generally need.
