from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib

import numpy as np
import pytest


MODULE_PATH = pathlib.Path(__file__).with_name(
    "vlass_localized_ordered_response_graph_contract.py"
)
SPEC = importlib.util.spec_from_file_location(
    "localized_ordered_response_graph", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def synthetic_source_contract() -> dict[str, object]:
    pairs: list[dict[str, object]] = []
    for pair_index in range(27):
        imaging_frequency_hz = 2_000_000_000 + (pair_index % 14) * 128_000_000
        prediction_frequency_hz = 2_000_000_000 + (pair_index % 16) * 128_000_000
        for imaging_mueller, prediction_mueller in [(0, 15), (15, 0)]:
            pairs.append(
                {
                    "imaging_frequency_hz": imaging_frequency_hz,
                    "imaging_mueller_element": imaging_mueller,
                    "prediction_frequency_hz": prediction_frequency_hz,
                    "prediction_mueller_element": prediction_mueller,
                }
            )
    return {
        "schema": MODULE.SOURCE_SCHEMA,
        "aw_screen_selection": {
            "imaging_prediction_state_pairs": pairs,
        },
    }


def test_exact_graph_dimensions_and_alias_safe_memory() -> None:
    dimensions = MODULE.graph_dimensions(synthetic_source_contract())

    assert dimensions == {
        "ordered_aw_pairs": 54,
        "imaging_states": 28,
        "prediction_states": 32,
        "minimum_embedding_side": 175,
        "embedding_side": 192,
        "response_kernels": 486,
        "stored_binomial_moment_channels": 972,
        "mixing_actions_per_hm": 1_296,
        "mixing_actions_for_major_trajectory": 14_256,
        "mixing_complex_macs_per_hm": 47_775_744,
        "mixing_complex_macs_for_major_trajectory": 525_533_184,
        "right_transforms_per_hm": 192,
        "left_transforms_per_hm": 168,
        "transforms_per_hm": 360,
        "transforms_for_major_trajectory": 3960,
        "rhs_grids": 168,
    }
    assert (
        dimensions["response_kernels"]
        * dimensions["embedding_side"] ** 2
        * MODULE.COMPLEX_F32_BYTES
        == 143_327_232
    )


def test_contracted_graph_matches_literal_and_controls_are_sensitive() -> None:
    pairs = MODULE.source_pairs(synthetic_source_contract())

    metrics = MODULE.synthetic_equivalence(pairs)

    assert metrics["contracted_relative_error"] <= MODULE.DIRECT_EQUIVALENCE_LIMIT
    assert metrics["wrong_pair_error_ratio"] >= MODULE.ADVERSARIAL_RATIO_MIN
    assert metrics["wrong_taylor_error_ratio"] >= MODULE.ADVERSARIAL_RATIO_MIN


def test_nearest_indices_preserve_the_frozen_left_tie_rule() -> None:
    candidates = np.asarray([1.0, 3.0, 5.0], dtype=np.float64)
    values = np.asarray([0.0, 2.0, 2.1, 4.0, 6.0], dtype=np.float64)

    assert MODULE.nearest_indices(values, candidates).tolist() == [0, 0, 1, 1, 2]


def test_pair_count_is_a_fail_closed_contract() -> None:
    contract = synthetic_source_contract()
    selection = contract["aw_screen_selection"]
    assert isinstance(selection, dict)
    pairs = selection["imaging_prediction_state_pairs"]
    assert isinstance(pairs, list)
    pairs.pop()

    with pytest.raises(MODULE.GraphContractError, match="expected 54"):
        MODULE.graph_dimensions(contract)


def test_explicit_resident_state_universe_is_hash_bound(
    tmp_path: pathlib.Path,
) -> None:
    universe = synthetic_source_contract()
    universe["sources"] = {"screen_manifest_sha256": "screen-sha"}
    universe_path = tmp_path / "universe.json"
    universe_path.write_text(json.dumps(universe), encoding="utf-8")
    universe_sha256 = hashlib.sha256(universe_path.read_bytes()).hexdigest()
    observed = {
        "schema": MODULE.SOURCE_SCHEMA,
        "aw_screen_selection": {
            "imaging_prediction_state_pairs": universe[
                "aw_screen_selection"
            ]["imaging_prediction_state_pairs"][:20],
        },
        "ordered_response_state_universe": {
            "source_contract": str(universe_path),
            "source_contract_sha256": universe_sha256,
            "source_screen_manifest_sha256": "screen-sha",
            "imaging_prediction_state_pairs": universe[
                "aw_screen_selection"
            ]["imaging_prediction_state_pairs"],
        },
    }

    assert MODULE.source_pairs(observed) == universe[
        "aw_screen_selection"
    ]["imaging_prediction_state_pairs"]

    observed["ordered_response_state_universe"][
        "source_contract_sha256"
    ] = "0" * 64
    with pytest.raises(MODULE.GraphContractError, match="SHA-256 differs"):
        MODULE.source_pairs(observed)
