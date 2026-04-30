# Install

`casa-rs` currently supports an installer-driven suite install on macOS
`arm64`.

The installer published with each release:

1. downloads the matching suite bundle
2. installs it into a versioned suite root
3. creates a suite-local Python environment from the bundled wheel
4. updates stable/rc channel links plus optional generic launcher links

That keeps the `casars` TUI, the standalone executables, and the Python package
behaving as one installed suite.

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
- `~/.local/bin/casars`
- `~/.local/bin/calibrate`
- `~/.local/bin/casars-stable`
- `~/.local/bin/calibrate-stable`

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
- `~/.local/bin/casars-rc`
- `~/.local/bin/calibrate-rc`

If you want the RC install to become the default suite immediately, add
`--activate`.

## Put the suite on PATH

The installer creates launchers in `~/.local/bin`. Add that directory to your
shell startup file such as `~/.zshrc`:

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

In the standard suite layout, `casars.tasks.calibrate` will automatically find
the sibling `calibrate` binary. To target a different installed suite
explicitly, set:

```bash
export CASARS_SUITE_ROOT="$HOME/.local/opt/casa-rs/<version>"
```

The Python binary lookup and protocol-compatibility rules are documented in
[Python Compatibility](python/versioning.md).

## Stable and RC installs

Stable releases and release candidates can live on the same machine at the same
time:

```text
~/.local/opt/casa-rs/0.15.0/
~/.local/opt/casa-rs/0.16.0-rc1/
~/.local/opt/casa-rs/stable -> ~/.local/opt/casa-rs/0.15.0
~/.local/opt/casa-rs/rc -> ~/.local/opt/casa-rs/0.16.0-rc1
```

The installer also maintains channel-specific commands:

- `~/.local/bin/casars-stable`
- `~/.local/bin/calibrate-stable`
- `~/.local/bin/casars-rc`
- `~/.local/bin/calibrate-rc`

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
