set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

setup:
    cargo fetch

quick:
    just arch-check
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

# Focused T24-T30 CASA/Rust solver, mask, product, and MODEL_DATA correctness gate.
imaging-solver-crosscheck input_ms output_dir:
    python tools/science/casa_rust_solver_crosscheck.py "{{input_ms}}" "{{output_dir}}"

# Focused #521 source-backed spectral identity/tracer foundation.
imaging-t35-spectral-tracer:
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction t35_
    CARGO_INCREMENTAL=0 cargo test -p casa-ms --features cpp-interop-tests t35_source_backed_identity_and_nonidentity_tracers_match_casacore

# Focused #522 paired sparse law, frame/interval evaluation, and edge coverage.
imaging-t36-spectral-law:
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --features cpp-interop-tests t36_
    CARGO_INCREMENTAL=0 cargo test -p casa-ms real_ms_cubedata_traversal_compiles_source_backed_casa_cubic_stencils
    CARGO_INCREMENTAL=0 cargo test -p casa-ms --features cpp-interop-tests --test spectral_frame_parity
    CARGO_INCREMENTAL=0 cargo test -p casa-test-support --features cpp-interop-tests --test spectral_frame_exact_interop

# Focused #523 bounded spectral cube operator, CASA comparator, and residency gate.
imaging-t37-cube-operator:
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --features cpp-interop-tests t37_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test compile_plan_run t37_runtime_residency

# Focused #524 CASA/Rust multi-channel clean and reconciliation gate.
imaging-t38-cube-clean:
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --features cpp-interop-tests --test major_cycle t38_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test compile_plan_run t38_runtime_runs_one_shared_cycle_with_combined_channel_evidence

# Focused #528 MT-MFS block-normal algebra, compact replay, persistence, and residency gate.
imaging-t42-mtmfs-normal:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test measurement_equation_contract problem_and_weighting_commitment_identities_are_pinned
    CARGO_INCREMENTAL=0 cargo test -p casa-ms block_traversal_reports_one_canonical_unequal_parallel_hand_weight_group
    CARGO_INCREMENTAL=0 cargo test -p casa-ms imaging_weight_groups_reject_ambiguous_or_mixed_multi_correlation_layouts
    CARGO_INCREMENTAL=0 cargo test -p casa-ms selected_projection_preserves_cell_flags_and_derives_parallel_hand_group_flags
    CARGO_INCREMENTAL=0 cargo test -p casa-ms refillable_block_stream_matches_scalar_traversal_and_returns_the_owner
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --test weighting
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction t42_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime continuum_transform::tests::
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --lib t42_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-products t42_

# Focused #528 frozen-CASA two-SPW MT-MFS scientific comparison.
imaging-t42-mtmfs-casa casa_npz:
    test -f "{{casa_npz}}"
    CASA_RS_T42_RUST_OUTPUT="{{justfile_directory()}}/target/t42-casa-oracle/rust-mtmfs-two-spw-normal.json" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test mtmfs_real_ms_normal t42_real_ms_mtmfs_normal_matches_casa_oracle_inputs -- --ignored --exact --nocapture
    python3 tools/science/t42_mtmfs_casa_compare.py --casa-npz "{{casa_npz}}" --rust-json "{{justfile_directory()}}/target/t42-casa-oracle/rust-mtmfs-two-spw-normal.json" --pretty

# Focused #529 coupled MT-MFS minor-cycle and frozen-CASA clean gate.
imaging-t43-mtmfs-clean testdata_root casa_python casa_prefix casa_result:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-numerics dynamic_casacore_ldlt_matches_fixed_solver
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_problem
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test measurement_equation_contract
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --lib mtmfs_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --test mtmfs_minor_cycle
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application mtmfs_runtime_claim_grows_with_taylor_terms_and_scales
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test compile_plan_run effective_problem_projection_carries_mtmfs_scales_and_bias -- --exact
    python3 tools/science/t43_test_mtmfs_clean_compare.py
    CASA_RS_TESTDATA_ROOT="{{testdata_root}}" CASA_RS_T43_RUST_OUTPUT="{{justfile_directory()}}/target/t43-t44-casa-oracle/rust-mtmfs-clean.json" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test mtmfs_clean_oracle t43_real_ms_mtmfs_clean_matches_frozen_casa -- --ignored --exact --nocapture
    "{{casa_python}}" tools/science/t43_mtmfs_clean_compare.py --casa-prefix "{{casa_prefix}}" --casa-result "{{casa_result}}" --rust-json "{{justfile_directory()}}/target/t43-t44-casa-oracle/rust-mtmfs-clean.json" --summary-output "{{justfile_directory()}}/target/t43-t44-casa-oracle/t43-comparison.json"

# Focused #530 sealed Taylor-product and frozen-CASA publication gate.
imaging-t44-mtmfs-products testdata_root casa_python casa_prefix:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_problem
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-products --test continuum_products
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-products --test taylor_products
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test availability
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test compile_plan_run serial_product_publication -- --nocapture
    python3 tools/science/t44_test_mtmfs_products_compare.py
    CASA_RS_TESTDATA_ROOT="{{testdata_root}}" CASA_RS_T44_RUST_OUTPUT="{{justfile_directory()}}/target/t43-t44-casa-oracle/rust-mtmfs-products.json" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test mtmfs_clean_oracle t44_real_ms_mtmfs_products_match_frozen_casa -- --ignored --exact --nocapture
    "{{casa_python}}" tools/science/t44_mtmfs_products_compare.py --casa-prefix "{{casa_prefix}}" --rust-json "{{justfile_directory()}}/target/t43-t44-casa-oracle/rust-mtmfs-products.json" --summary-output "{{justfile_directory()}}/target/t43-t44-casa-oracle/t44-comparison.json"

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
