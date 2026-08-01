from __future__ import annotations

import importlib.util
import json
import pathlib

import numpy as np
import pytest


MODULE_PATH = pathlib.Path(__file__).with_name(
    "vlass_localized_row_census_contract.py"
)
SPEC = importlib.util.spec_from_file_location("localized_row_contract", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_localized_row_dtype_is_exactly_144_bytes() -> None:
    assert MODULE.ROW_DTYPE.itemsize == 144


def test_nearest_indices_use_deterministic_left_tie_break() -> None:
    candidates = np.array([1.0, 3.0, 5.0])
    values = np.array([0.0, 2.0, 2.1, 6.0])

    assert MODULE.nearest_indices(values, candidates).tolist() == [0, 0, 1, 2]


def test_unique_pointing_count_ignores_pb_frequency() -> None:
    base = {
        "pointing_ra_rad_bits": "1",
        "pointing_dec_rad_bits": "2",
        "pointing_x_pixel_bits": "3",
        "pointing_y_pixel_bits": "4",
    }
    groups = [
        {**base, "beam_frequency_hz_bits": "a"},
        {**base, "beam_frequency_hz_bits": "b"},
        {
            **base,
            "beam_frequency_hz_bits": "c",
            "pointing_x_pixel_bits": "5",
        },
    ]

    assert MODULE.unique_pointing_count(groups) == 2


def ordered_pair(index: int) -> dict[str, object]:
    return {
        "imaging_frequency_hz": 2_000_000_000 + index,
        "imaging_mueller_element": 0,
        "prediction_frequency_hz": 2_100_000_000 + index,
        "prediction_mueller_element": 15,
    }


def census_contract(pairs: list[dict[str, object]], screen_sha: str) -> dict[str, object]:
    return {
        "schema": MODULE.SCHEMA,
        "sources": {"screen_manifest_sha256": screen_sha},
        "aw_screen_selection": {
            "persisted_cf_frequencies_hz": [2_000_000_000.0, 2_100_000_000.0],
            "mueller_elements": [0, 15],
            "imaging_prediction_state_pairs": pairs,
        },
    }


def test_ordered_response_universe_is_a_provenance_bound_superset(
    tmp_path: pathlib.Path,
) -> None:
    observed = census_contract([ordered_pair(0)], "screen-sha")
    universe = census_contract(
        [ordered_pair(0), ordered_pair(1)],
        "screen-sha",
    )
    universe_path = tmp_path / "universe.json"
    universe_path.write_text(json.dumps(universe), encoding="utf-8")

    MODULE.attach_ordered_response_state_universe(observed, universe_path)

    state_universe = observed["ordered_response_state_universe"]
    assert isinstance(state_universe, dict)
    assert state_universe["observed_ordered_aw_pairs"] == 1
    assert state_universe["resident_ordered_aw_pairs"] == 2
    assert state_universe["inactive_resident_ordered_aw_pairs"] == 1
    assert state_universe["observed_pairs_are_subset"] is True
    assert observed["aw_screen_selection"]["imaging_prediction_state_pairs"] == [
        ordered_pair(0)
    ]


def test_ordered_response_universe_rejects_a_different_screen(
    tmp_path: pathlib.Path,
) -> None:
    observed = census_contract([ordered_pair(0)], "observed-screen")
    universe = census_contract([ordered_pair(0)], "different-screen")
    universe_path = tmp_path / "universe.json"
    universe_path.write_text(json.dumps(universe), encoding="utf-8")

    with pytest.raises(
        MODULE.CensusContractError,
        match="different physical screen",
    ):
        MODULE.attach_ordered_response_state_universe(observed, universe_path)
