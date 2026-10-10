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
    bash scripts/test-workspace.sh
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
    bash scripts/test-workspace.sh
    ./scripts/test-python-package.sh
    bash scripts/test-smoke.sh
    ./scripts/test-install-suite.sh

release-cpp-interop:
    bash scripts/test-release-cpp-interop.sh

# Focused #517 compiled multi-domain geometry and frozen-CASA product gate.
imaging-t31-multidomain-geometry testdata_root casa_prefix:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model image_domain_projections_require_canonical_ordinals_and_share_equal_psf_values
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_geometry multi_domain_centres_are_canonical_explicit_and_identity_bearing -- --exact
    CARGO_INCREMENTAL=0 cargo test -p casa-ms reproject_raw_uvw --lib
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --lib minor_cycle
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-products --test continuum_products two_domain_members_consume_their_matching_normal_and_model_chart -- --exact
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test continuum_application application_uses_weight_when_selected_weight_spectrum_cells_are_undefined -- --exact
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test continuum_application t31_application_executes_recentered_domains_through_one_scientific_route -- --exact
    CASA_RS_TESTDATA_ROOT="{{testdata_root}}" CASA_RS_T31_CASA_PREFIX="{{casa_prefix}}" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test t31_multidomain_casa_oracle t31_multidomain_geometry_matches_frozen_casa_dirty_and_hogbom -- --ignored --exact --nocapture

# Focused #521 source-backed spectral identity/tracer foundation.
imaging-t35-spectral-tracer:
    CARGO_INCREMENTAL=0 cargo test -p casa-ms t35_source_backed_identity_and_nonidentity_traversals_report_native_evaluations

# Focused #522 frame/interval evaluation and edge coverage.
imaging-t36-spectral-law:
    CARGO_INCREMENTAL=0 cargo test -p casa-ms real_ms_cubedata_traversal_reports_source_backed_native_evaluations
    CARGO_INCREMENTAL=0 cargo test -p casa-ms --features cpp-interop-tests --test spectral_frame_parity
    CARGO_INCREMENTAL=0 cargo test -p casa-test-support --features cpp-interop-tests --test spectral_frame_exact_interop

# Focused #524 CASA/Rust multi-channel clean gate.
imaging-t38-cube-clean:
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --features cpp-interop-tests --test major_cycle t38_

# Focused #527 moving-source, MVC, response, bounded-replay, and provider-contract gate.
imaging-t41-moving-source:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-provider-contracts
    CARGO_INCREMENTAL=0 cargo test -p casa-tables add_variable_shape_tiled_column_in_place_persists_defined_rows_only
    CARGO_INCREMENTAL=0 cargo test -p casa-ms t41_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_problem t41_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_geometry ephemeris_centre_laws_require_and_identify_one_bound_snapshot -- --exact
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction t41_
    CARGO_INCREMENTAL=0 cargo test -p casars-imager task_contract

# Representative #527 frozen-CASA cubesource gate, with the selected spectral
# range's Measures edge topology and the ALMA primary beam on the
# representative MVC observation (`mvc` itself is not requestable).
imaging-t41-moving-source-casa testdata_root cubesource_casa_prefix mvc_ms mvc_casa_prefix:
    #!/usr/bin/env bash
    set -euo pipefail
    test -d "{{testdata_root}}"
    test -d "{{mvc_ms}}"
    CASA_RS_T41_MVC_MS="{{mvc_ms}}" CARGO_INCREMENTAL=0 cargo test -p casa-ms selected_observation::tests::t41_ephemeris_oracle::t41_trackfield_phase_centre_matches_casa_at_three_row_times -- --ignored --exact --nocapture
    CASA_RS_T41_MVC_CASA_PREFIX="{{mvc_casa_prefix}}" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction primary_beam::tests::t41_alma_mvc_primary_beam_owner_matches_frozen_cube --release -- --ignored --exact --nocapture
    CASA_RS_T41_MVC_MS="{{mvc_ms}}" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test t41_moving_source_casa_oracle t41_mvc_selected_spectral_range_matches_casa_edge_topology --release -- --ignored --exact --nocapture
    CASA_RS_TESTDATA_ROOT="{{testdata_root}}" CASA_RS_T41_CASA_PREFIX="{{cubesource_casa_prefix}}" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test t41_moving_source_casa_oracle t41_tracked_cubesource_matches_casa_geometry_and_dirty_products --release -- --ignored --exact --nocapture

# Focused #534 heterogeneous ALMA/ACA response, bounded selection, and CASA product gate.
imaging-t48-heterogeneous-response:
    CARGO_INCREMENTAL=0 cargo test -p casa-ms selected_rows_pair_owner_derived_heterogeneous_apertures_with_antenna_pointings -- --nocapture
    CARGO_INCREMENTAL=0 cargo test -p casa-ms observation_pointing_interpolates_each_antenna_on_the_shortest_arc -- --nocapture
    CARGO_INCREMENTAL=0 cargo test -p casa-ms polarized_linear_prediction_rotates_with_parallactic_angle -- --nocapture
    CARGO_INCREMENTAL=0 cargo test -p casa-ms t33_non_toy_vla_traversal_reports_row_shared_parallactic_angles -- --nocapture
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction heterogeneous -- --nocapture

# Regenerate and compare the representative #534 2,016,000-sample dirty response with CASA.
# Mosaic imaging is unavailable from IF-2 until IF-3 (#652), so the imaging step fails typed until then.
imaging-t48-heterogeneous-response-casa:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/t48-testdata/imaging/t48
    CARGO_INCREMENTAL=0 cargo run --release -p casa-ms --bin simobserve -- --json-run tools/perf/imager/fixtures/t48-mixed-alma-aca-request.json
    CASA_RS_TESTDATA_ROOT="{{justfile_directory()}}/target/t48-testdata" python3 tools/perf/imager/run_workload.py t48-heterogeneous-mosaic-mfs --stream-log

# Focused #528 MT-MFS weight grouping and Taylor reconstruction gate.
imaging-t42-mtmfs-normal:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test measurement_equation_contract problem_and_weighting_commitment_identities_are_pinned
    CARGO_INCREMENTAL=0 cargo test -p casa-ms block_traversal_reports_one_canonical_unequal_parallel_hand_weight_group
    CARGO_INCREMENTAL=0 cargo test -p casa-ms imaging_weight_groups_reject_ambiguous_or_mixed_multi_correlation_layouts
    CARGO_INCREMENTAL=0 cargo test -p casa-ms selected_projection_preserves_cell_flags_and_derives_parallel_hand_group_flags
    CARGO_INCREMENTAL=0 cargo test -p casa-ms refillable_block_stream_matches_scalar_traversal_and_returns_the_owner
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction t42_

# Focused #529 coupled MT-MFS minor-cycle gate. The real-MS frozen-CASA
# comparison runs through the application in imaging-t44-mtmfs-products.
imaging-t43-mtmfs-clean:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-numerics dynamic_casacore_ldlt_matches_fixed_solver
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_problem
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test measurement_equation_contract
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --lib mtmfs_
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-reconstruction --test mtmfs_minor_cycle

# Focused #530 sealed Taylor-product and frozen-CASA publication gate.
imaging-t44-mtmfs-products testdata_root casa_python casa_prefix:
    just arch-check
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-model --test compiled_problem
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-products --test continuum_products
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-products --test taylor_products
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test availability
    python3 tools/science/t44_test_mtmfs_products_compare.py
    CASA_RS_TESTDATA_ROOT="{{testdata_root}}" CASA_RS_T44_APPLICATION_PREFIX="{{justfile_directory()}}/target/t43-t44-casa-oracle/rust-application/casa" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test mtmfs_publication_oracle t44_application_mtmfs_publishes_frozen_casa_product_contract -- --ignored --exact --nocapture
    "{{casa_python}}" tools/science/t44_mtmfs_products_compare.py --casa-prefix "{{casa_prefix}}" --rust-prefix "{{justfile_directory()}}/target/t43-t44-casa-oracle/rust-application/casa" --summary-output "{{justfile_directory()}}/target/t43-t44-casa-oracle/t44-comparison.json"

# Focused #545 memory admission and representative frozen-CASA production gate.
imaging-t59-low-memory representative_ms casa_prefix:
    test -d "{{representative_ms}}"
    test -e "{{casa_prefix}}.psf.tt0"
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test pass_laws residency::
    CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test continuum_application domains_and_waves::
    CASA_RS_ISSUE607_MTMFS_MS="{{representative_ms}}" CASA_RS_ISSUE607_MTMFS_CASA_PREFIX="{{casa_prefix}}" CARGO_INCREMENTAL=0 cargo test -p casa-imaging-application --test mtmfs_publication_oracle issue607_representative_mtmfs_matches_casa_products --release -- --ignored --exact --nocapture

release-perf:
    bash scripts/test-release-perf.sh

external-data-cleanup *args:
    tools/perf/imager/cleanup_external_data.py {{args}}

arch-check:
    bash scripts/arch-check.sh

docs-check:
    bash scripts/docs-check.sh

# List (or with --apply, remove) worktrees and local branches already merged into origin/main.
tidy *args:
    bash scripts/tidy-git.sh {{args}}

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
