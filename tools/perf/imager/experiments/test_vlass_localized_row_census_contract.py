from __future__ import annotations

import importlib.util
import pathlib

import numpy as np


MODULE_PATH = pathlib.Path(__file__).with_name(
    "vlass_localized_row_census_contract.py"
)
SPEC = importlib.util.spec_from_file_location("localized_row_contract", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_localized_row_dtype_is_exactly_128_bytes() -> None:
    assert MODULE.ROW_DTYPE.itemsize == 128


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
