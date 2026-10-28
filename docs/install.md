# Install

`casa-rs` supports an installer-driven suite install on macOS `arm64` and
Linux `x86_64`.

The installer published with each release:

1. downloads the matching suite bundle
2. installs it into a versioned suite root
3. creates a suite-local Python environment from the bundled wheel
4. updates the stable/rc channel links, and links the suite's task
   executables into `~/.local/bin`

That keeps the task executables and the Python package behaving as one
installed suite: the suite's Python starts the executables in its own
suite's `bin/`.

## macOS prerequisites

Install the Apple command-line tools and a supported Python:

```bash
xcode-select --install
python3 --version
```

`casa-rs-python` currently supports Python 3.10 through 3.12.

## Install a stable release

Choose the version you want to install:

```bash
version=0.15.0
curl -fsSL "https://github.com/bglenden/casa-rs/releases/download/v${version}/install-casa-rs.sh" \
  | bash -s -- --version "$version"
```

That creates:

- `~/.local/opt/casa-rs/<version>`
- `~/.local/opt/casa-rs/stable`
- `~/.local/opt/casa-rs/current`
- for each task executable in the suite's `bin/`, such as `calibrate`,
  `msexplore`, `imexplore`, `mstransform` and `casars-imager`:
  `~/.local/bin/<command>` and `~/.local/bin/<command>-stable`

`feather`, `immath` and `impv` are not linked into `~/.local/bin`, and there
is no `split` command: they are CASA names that are also system commands
(`split` is a coreutil) or generic words. Run split as `mstransform split`;
Python starts all of them from the suite's `bin/`. A link in `~/.local/bin`
that points into the install root but no longer resolves, such as one to a
command a newer suite does not have, is removed. A file of your own, or a
link elsewhere, already at one of these names is left in place, and the
installer says so.

## Install a release candidate side by side

Release candidates use the same installer, but by default they update only the
`rc` channel links and do not take over `current`:

```bash
version=0.16.0-rc1
curl -fsSL "https://github.com/bglenden/casa-rs/releases/download/v${version}/install-casa-rs.sh" \
  | bash -s -- --version "$version"
```

That creates:

- `~/.local/opt/casa-rs/<version>`
- `~/.local/opt/casa-rs/rc`
- `~/.local/bin/<command>-rc` for each command, such as
  `~/.local/bin/calibrate-rc`

If you want the RC install to become the default suite immediately, add
`--activate`.

## Put the suite on PATH

The installer links the suite's commands into `~/.local/bin`. Add that
directory to your shell startup file such as `~/.zshrc`:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Reload the shell:

```bash
source ~/.zshrc
```

## Use Python from the suite

Activate the suite-local virtual environment:

```bash
source "$HOME/.local/opt/casa-rs/current/python/bin/activate"
python -c "import casars; print(casars.__version__)"
```

The suite's Python starts each task's executable from its own suite: in an
install at `~/.local/opt/casa-rs/<version>`, `casars.tasks.gaincal(...)`
runs `~/.local/opt/casa-rs/<version>/bin/calibrate`. To target a different
installed suite explicitly, set:

```bash
export CASARS_SUITE_ROOT="$HOME/.local/opt/casa-rs/<version>"
```

To run one task from another file, set its override variable, such as
`CASARS_CALIBRATE_BIN` or `CASARS_IMAGER_BIN`. A wheel installed outside a
suite, such as one from PyPI, needs `CASARS_SUITE_ROOT` (or the task's
override) to run tasks; its in-process tools (`casars.data.Image`,
`casars.data.Table`, `casars.tools`) need no executables.

The Python executable lookup and protocol-compatibility rules are documented
in [Python Compatibility](python/versioning.md).

## Stable and RC installs

Stable releases and release candidates can live on the same machine at the same
time:

```text
~/.local/opt/casa-rs/0.15.0/
~/.local/opt/casa-rs/0.16.0-rc1/
~/.local/opt/casa-rs/stable -> ~/.local/opt/casa-rs/0.15.0
~/.local/opt/casa-rs/rc -> ~/.local/opt/casa-rs/0.16.0-rc1
```

The installer also maintains channel-specific commands, one per command and
channel, such as:

- `~/.local/bin/calibrate-stable`
- `~/.local/bin/calibrate-rc`
- `~/.local/bin/casars-imager-stable`
- `~/.local/bin/casars-imager-rc`

Switch the default suite by repointing `~/.local/opt/casa-rs/current`,
re-running the installer with `--activate`, or keep multiple installs in place
and choose one explicitly with `CASARS_SUITE_ROOT` or that suite's Python
virtual environment.

## Build from source

If you are working from a checkout instead of a published release asset, install
Rust and build locally:

```bash
curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"
cargo build --release --workspace --bins
```

A Python package installed from the checkout (`pip install -e
crates/casars-python`) runs the newest `debug` or `release` build of each
task executable in that checkout's `target/` (or `$CARGO_TARGET_DIR`), unless
`CASARS_SUITE_ROOT` or the task's override is set. A task whose executable
is not built names the `cargo build -p <package> --bin <executable>` that
builds it.

To package a local release-style bundle, run:

```bash
scripts/build-python-dist.sh dist/python
scripts/package-suite-bundle.sh \
  --version "$(sed -n 's/^version = \"\\(.*\\)\"/\\1/p' Cargo.toml | head -n 1)" \
  --platform "$(scripts/install-suite.sh --print-platform)" \
  --bin-dir target/release \
  --wheel-dir dist/python \
  --out-dir dist/python
```

To build, bundle, and install the current checkout in one step, run:

```bash
just install-local
```

`just install-local` runs `just install-local-suite`. Use `--force` for an
ad-hoc reinstall of the same checkout version:

```bash
just install-local -- --force
```

To install a published release suite without rebuilding the checkout or
rerunning release verification gates, run:

```bash
just install-release 0.17.0
```
