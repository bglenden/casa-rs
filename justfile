set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

setup:
    cargo fetch

quick:
    ./scripts/check-spdx.sh
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

verify:
    just quick
    ./scripts/test-python-package.sh

smoke:
    bash scripts/test-smoke.sh

lint:
    ./scripts/check-spdx.sh
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings

typecheck:
    cargo check --workspace --all-targets

test:
    cargo test --workspace
    ./scripts/test-python-package.sh
    bash scripts/test-smoke.sh
    ./scripts/test-install-suite.sh

release-cpp-interop:
    bash scripts/test-release-cpp-interop.sh

release-perf:
    bash scripts/test-release-perf.sh

arch-check:
    bash scripts/arch-check.sh

docs-check:
    bash scripts/docs-check.sh

graph:
    bash scripts/generate-graphs.sh

install-local:
    bash scripts/install-local-suite.sh
