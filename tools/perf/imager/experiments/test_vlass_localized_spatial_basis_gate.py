from __future__ import annotations

import importlib.util
import pathlib

import numpy as np


MODULE_PATH = pathlib.Path(__file__).with_name(
    "vlass_localized_spatial_basis_gate.py"
)
SPEC = importlib.util.spec_from_file_location("localized_spatial_basis", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_total_degree_two_has_six_terms() -> None:
    assert MODULE.total_degree_exponents(2) == [
        (0, 0),
        (0, 1),
        (1, 0),
        (0, 2),
        (1, 1),
        (2, 0),
    ]


def test_quadratic_family_is_reconstructed_to_roundoff() -> None:
    basis, _ = MODULE.spatial_basis(16, 2)
    coefficients = np.array(
        [
            [1.0 + 2.0j, -0.5 + 0.25j],
            [0.25 - 0.5j, 2.0 - 1.0j],
            [0.75 + 0.125j, -1.0 + 0.5j],
            [0.1 + 0.2j, 0.3 - 0.4j],
            [-0.2 + 0.1j, 0.6 + 0.7j],
            [0.5 - 0.3j, -0.8 + 0.2j],
        ]
    )
    family = (basis @ coefficients).T.reshape(2, 16, 16)

    approximation, fitted, exponents = MODULE.fit_spatial_family(family, 2)

    assert len(exponents) == 6
    assert np.max(np.abs(approximation - family)) < 1.0e-12
    assert np.max(np.abs(fitted - coefficients.T)) < 1.0e-12


def test_degree_two_channel_count_fits_provisional_ceiling() -> None:
    basis_terms = len(MODULE.total_degree_exponents(2))
    channels = (
        basis_terms
        * basis_terms
        * MODULE.W_CHANNELS
        * MODULE.TAYLOR_PSF_ORDERS
    )

    assert channels == 648
    assert channels <= MODULE.NORMAL_CHANNEL_LIMIT
