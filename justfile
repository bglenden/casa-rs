set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

setup:
    cargo fetch
    npm --prefix apps/casars-assistant ci --ignore-scripts

quick:
    ./scripts/check-spdx.sh
    cargo fmt --all -- --check
    CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings
    CARGO_INCREMENTAL=0 cargo test --workspace
    just assistant-test

verify:
    just quick
    ./scripts/test-python-package.sh

smoke:
    bash scripts/test-smoke.sh

lint:
    ./scripts/check-spdx.sh
    cargo fmt --all -- --check
    CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings

typecheck:
    CARGO_INCREMENTAL=0 cargo check --workspace --all-targets

test:
    CARGO_INCREMENTAL=0 cargo test --workspace
    ./scripts/test-python-package.sh
    bash scripts/test-smoke.sh
    ./scripts/test-install-suite.sh

release-cpp-interop:
    bash scripts/test-release-cpp-interop.sh

release-perf:
    bash scripts/test-release-perf.sh

external-data-cleanup *args:
    tools/perf/imager/cleanup_external_data.py {{args}}

arch-check:
    bash scripts/arch-check.sh

docs-check:
    bash scripts/docs-check.sh

gui-test:
    bash apps/casars-mac/script/test_gui.sh

assistant-test:
    npm --prefix apps/casars-assistant test

# Opt-in network smoke using a credential previously stored by casars-mac in Keychain.
assistant-live-smoke provider model:
    CASA_RS_ASSISTANT_LIVE_SMOKE_PROVIDER='{{provider}}' CASA_RS_ASSISTANT_LIVE_SMOKE_MODEL='{{model}}' swift test --package-path apps/casars-mac --filter AssistantDiscussionTests/testOptInLiveProviderSmokeUsesHostKeychainLease

graph:
    bash scripts/generate-graphs.sh

install-local *args:
    bash scripts/install-local.sh {{args}}

install-local-suite *args:
    bash scripts/install-local-suite.sh {{args}}

install-local-gui *args:
    bash apps/casars-mac/script/install-local-gui.sh {{args}}

install-release version *args:
    bash scripts/install-release.sh {{version}} {{args}}
