set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

setup:
    cargo fetch

quick:
    ./scripts/check-spdx.sh
    cargo fmt --all -- --check
    CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings
    CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 cargo test --workspace
    python3 scripts/test-task-cli-hosts.py
    python3 apps/casars-mac/script/test_gui_acceptance.py

verify:
    just quick
    scripts/generate-frontend-bindings.sh --check
    ./scripts/test-python-package.sh

frontend-bindings-check:
    scripts/generate-frontend-bindings.sh --check

smoke:
    bash scripts/test-smoke.sh

lint:
    ./scripts/check-spdx.sh
    cargo fmt --all -- --check
    CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings

typecheck:
    CARGO_INCREMENTAL=0 cargo check --workspace --all-targets

test:
    CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 cargo test --workspace
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
    python3 apps/casars-mac/script/gui_acceptance.py run gui-test

# Run the deterministic GUI gate on a dedicated logged-in remote Mac.
gui-test-remote:
    bash scripts/test-gui-remote.sh gui-test

assistant-test:
    CARGO_INCREMENTAL=0 cargo test -p casa-notebook --test assistant_contract --test corpus_contract
    CARGO_INCREMENTAL=0 cargo test -p casars-frontend-services --bin casars-project-mcp
    swift test --package-path apps/casars-mac --filter AssistantDiscussionTests

# Opt-in smoke using the installed Codex CLI's existing ChatGPT subscription login.
assistant-live-smoke:
    CASA_RS_CODEX_LIVE_SMOKE=1 swift test --package-path apps/casars-mac --filter AssistantDiscussionTests/testOptInCodexSubscriptionSmoke

# Opt-in launched-app acceptance using the installed Codex CLI's ChatGPT subscription.
assistant-live-gui:
    python3 apps/casars-mac/script/gui_acceptance.py run assistant-live-gui

# Opt-in real-world notebook/task/Python/plot round-trip using the installed
# Codex CLI's ChatGPT subscription and a disposable project.
notebook-roundtrip-gui:
    python3 apps/casars-mac/script/gui_acceptance.py run notebook-roundtrip-gui

# Run the live notebook production round-trip on a dedicated remote Mac.
notebook-roundtrip-gui-remote:
    bash scripts/test-gui-remote.sh notebook-roundtrip-gui

# Opt-in end-to-end TW Hya tutorial journey through production adapters.
tutorial-journey-gui:
    python3 apps/casars-mac/script/gui_acceptance.py run tutorial-journey-gui

# Run the production TW Hya tutorial journey on the dedicated remote Mac.
tutorial-journey-gui-remote:
    bash scripts/test-gui-remote.sh tutorial-journey-gui

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
