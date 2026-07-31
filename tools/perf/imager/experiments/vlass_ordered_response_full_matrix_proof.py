#!/usr/bin/env python3
"""Prove the localized VLASS ordered-response contraction on a tiny matrix.

The proof is intentionally production-inert.  It materializes every two-term
MT-MFS basis response on a 7x6 image, compares direct f64 row evaluation with
both explicit Toeplitz and alias-safe FFT contractions, and keeps deliberately
wrong semantics as sensitivity controls.  Irregular-UV spreading and Metal
precision are separate gates.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-ordered-response-full-matrix-proof/v1"
SIDE_Y = 7
SIDE_X = 6
EMBED_Y = 16
EMBED_X = 16
MODEL_TERMS = 2
W_ORDERS = 3
MOMENTS = 3
ROWS_PER_PAIR = 2
ALGEBRA_L2_LIMIT = 2.0e-13
ALGEBRA_LINF_LIMIT = 1.0e-12
FFT_L2_LIMIT = 1.0e-12
CONTROL_RATIO_MIN = 10.0


class FullMatrixProofError(RuntimeError):
    """The source contract or a proof gate is invalid."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_graph_module() -> Any:
    path = pathlib.Path(__file__).with_name(
        "vlass_localized_ordered_response_graph_contract.py"
    )
    spec = importlib.util.spec_from_file_location("ordered_response_graph", path)
    if spec is None or spec.loader is None:
        raise FullMatrixProofError(f"cannot load graph contract module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def relative_metrics(
    candidate: np.ndarray, reference: np.ndarray
) -> dict[str, float]:
    delta = candidate - reference
    reference_l2 = max(float(np.linalg.norm(reference)), np.finfo(float).tiny)
    reference_linf = max(
        float(np.max(np.abs(reference))), np.finfo(float).tiny
    )
    return {
        "relative_l2": float(np.linalg.norm(delta)) / reference_l2,
        "normalized_linf": float(np.max(np.abs(delta))) / reference_linf,
    }


def fixture(source: dict[str, Any], graph: Any) -> dict[str, Any]:
    pairs = graph.source_pairs(source)
    imaging_keys = sorted(
        {
            graph.state_key(
                float(pair["imaging_frequency_hz"]),
                int(pair["imaging_mueller_element"]),
            )
            for pair in pairs
        }
    )
    prediction_keys = sorted(
        {
            graph.state_key(
                float(pair["prediction_frequency_hz"]),
                int(pair["prediction_mueller_element"]),
            )
            for pair in pairs
        }
    )
    imaging_index = {key: index for index, key in enumerate(imaging_keys)}
    prediction_index = {key: index for index, key in enumerate(prediction_keys)}
    pair_indices = np.asarray(
        [
            [
                imaging_index[
                    graph.state_key(
                        float(pair["imaging_frequency_hz"]),
                        int(pair["imaging_mueller_element"]),
                    )
                ],
                prediction_index[
                    graph.state_key(
                        float(pair["prediction_frequency_hz"]),
                        int(pair["prediction_mueller_element"]),
                    )
                ],
            ]
            for pair in pairs
        ],
        dtype=np.int64,
    )
    wrong_prediction_indices = np.asarray(
        [
            prediction_index.get(
                graph.state_key(
                    float(pair["imaging_frequency_hz"]),
                    int(pair["prediction_mueller_element"]),
                ),
                int(pair_indices[pair_index, 1]),
            )
            for pair_index, pair in enumerate(pairs)
        ],
        dtype=np.int64,
    )

    y_index, x_index = np.indices((SIDE_Y, SIDE_X))
    x_centered = x_index.astype(np.float64) - (SIDE_X - 1) / 2
    y_centered = y_index.astype(np.float64) - (SIDE_Y - 1) / 2
    l_image = 0.0017 * x_centered + 0.00023 * y_centered
    m_image = -0.00019 * x_centered + 0.0015 * y_centered
    eta_image = np.sqrt(1.0 - l_image**2 - m_image**2) - 1.0
    direction_l = l_image.ravel()
    direction_m = m_image.ravel()
    eta = eta_image.ravel()
    pixel_count = direction_l.size

    pointing_l = 0.0011
    pointing_m = -0.0007

    def screen_family(count: int, phase_sign: float) -> np.ndarray:
        state = np.arange(count, dtype=np.float64)[:, None]
        shifted_l = direction_l[None, :] - pointing_l
        shifted_m = direction_m[None, :] - pointing_m
        amplitude = (
            0.82
            + 0.11 * np.cos((state + 1.0) * shifted_l * 47.0)
            + 0.05 * np.sin((state + 2.0) * shifted_m * 31.0)
        )
        phase = phase_sign * (
            0.13 * state
            + (state + 1.0) * shifted_l * 23.0
            - (state + 0.5) * shifted_m * 17.0
        )
        return amplitude * np.exp(1j * phase)

    raw_imaging_screens = screen_family(len(imaging_keys), 1.0)
    left_screens = np.conj(raw_imaging_screens)
    right_screens = screen_family(len(prediction_keys), -0.73)

    pair_count = len(pairs)
    row_count = pair_count * ROWS_PER_PAIR
    pair_for_row = np.repeat(np.arange(pair_count), ROWS_PER_PAIR)
    row_ordinal = np.arange(row_count, dtype=np.float64)
    sign = np.where((np.arange(row_count) & 1) == 0, 1.0, -1.0)
    u = sign * (12.25 + np.mod(row_ordinal * 7.0, 19.0)) + 0.17
    v = -sign * (8.75 + np.mod(row_ordinal * 11.0, 23.0)) - 0.29
    w = sign * (31.0 + np.mod(row_ordinal * 13.0, 29.0))
    tau = (np.mod(row_ordinal, 11.0) - 5.0) * 0.061
    tau[::17] = 1.0e-9
    beam_tau = 0.31 * tau + 0.23
    weight = 0.37 + np.mod(row_ordinal * 5.0, 17.0) / 13.0
    pointing_phase = np.exp(
        1j * math.tau * (u * pointing_l + v * pointing_m)
    )
    left_scalar = np.exp(1j * (0.07 + 0.013 * row_ordinal))
    right_scalar = np.exp(-1j * (0.11 + 0.009 * row_ordinal))
    beta = weight * pointing_phase * left_scalar * right_scalar
    rhs_data = (
        np.sin(row_ordinal * 0.37) + 1j * np.cos(row_ordinal * 0.23)
    )
    rhs_beta = weight * pointing_phase * left_scalar * rhs_data

    return {
        "pairs": pairs,
        "pair_indices": pair_indices,
        "wrong_prediction_indices": wrong_prediction_indices,
        "l": direction_l,
        "m": direction_m,
        "eta": eta,
        "left_screens": left_screens,
        "raw_imaging_screens": raw_imaging_screens,
        "right_screens": right_screens,
        "pair_for_row": pair_for_row,
        "u": u,
        "v": v,
        "w": w,
        "tau": tau,
        "beam_tau": beam_tau,
        "beta": beta,
        "beta_without_pointing": weight * left_scalar * right_scalar,
        "rhs_beta": rhs_beta,
        "row_count": row_count,
        "pixel_count": pixel_count,
        "pointing_offset_rad": [pointing_l, pointing_m],
    }


def direct_response_matrix(
    case: dict[str, Any],
    *,
    exact_w: bool = False,
) -> np.ndarray:
    pixel_count = case["pixel_count"]
    direction_l = case["l"]
    direction_m = case["m"]
    eta = case["eta"]
    delta_l = direction_l[:, None] - direction_l[None, :]
    delta_m = direction_m[:, None] - direction_m[None, :]
    delta_eta = eta[:, None] - eta[None, :]
    output = np.zeros(
        (MODEL_TERMS * pixel_count, MODEL_TERMS * pixel_count),
        dtype=np.complex128,
    )
    for row in range(case["row_count"]):
        pair = case["pair_for_row"][row]
        imaging_state, prediction_state = case["pair_indices"][pair]
        uv_phase = np.exp(
            1j
            * math.tau
            * (
                case["u"][row] * delta_l
                + case["v"][row] * delta_m
            )
        )
        relative_w_argument = (
            1j * math.tau * case["w"][row] * delta_eta
        )
        if exact_w:
            w_phase = np.exp(relative_w_argument)
        else:
            w_phase = (
                1.0
                + relative_w_argument
                + relative_w_argument**2 / 2.0
            )
        screen_outer = (
            case["left_screens"][imaging_state][:, None]
            * case["right_screens"][prediction_state][None, :]
        )
        base = case["beta"][row] * screen_outer * uv_phase * w_phase
        for output_term in range(MODEL_TERMS):
            output_slice = slice(
                output_term * pixel_count, (output_term + 1) * pixel_count
            )
            for model_term in range(MODEL_TERMS):
                input_slice = slice(
                    model_term * pixel_count,
                    (model_term + 1) * pixel_count,
                )
                output[output_slice, input_slice] += (
                    case["tau"][row] ** (output_term + model_term) * base
                )
    return output


def build_kernels(
    case: dict[str, Any],
    *,
    taylor_coordinate: np.ndarray | None = None,
    beta: np.ndarray | None = None,
    w_sign: float = 1.0,
) -> np.ndarray:
    tau = case["tau"] if taylor_coordinate is None else taylor_coordinate
    coefficient = case["beta"] if beta is None else beta
    separation_y = np.arange(-(SIDE_Y - 1), SIDE_Y, dtype=np.float64)
    separation_x = np.arange(-(SIDE_X - 1), SIDE_X, dtype=np.float64)
    dy, dx = np.meshgrid(separation_y, separation_x, indexing="ij")
    delta_l = 0.0017 * dx + 0.00023 * dy
    delta_m = -0.00019 * dx + 0.0015 * dy
    kernels = np.zeros(
        (
            len(case["pairs"]),
            W_ORDERS,
            MOMENTS,
            separation_y.size,
            separation_x.size,
        ),
        dtype=np.complex128,
    )
    for row in range(case["row_count"]):
        pair = case["pair_for_row"][row]
        uv_phase = np.exp(
            1j
            * math.tau
            * (case["u"][row] * delta_l + case["v"][row] * delta_m)
        )
        iw = 1j * math.tau * w_sign * case["w"][row]
        for order in range(W_ORDERS):
            w_coefficient = iw**order / math.factorial(order)
            for moment in range(MOMENTS):
                kernels[pair, order, moment] += (
                    coefficient[row]
                    * tau[row] ** moment
                    * w_coefficient
                    * uv_phase
                )
    return kernels


def explicit_convolution_bank(kernels: np.ndarray) -> np.ndarray:
    y, x = np.indices((SIDE_Y, SIDE_X))
    flat_y = y.ravel()
    flat_x = x.ravel()
    separation_y = flat_y[:, None] - flat_y[None, :] + SIDE_Y - 1
    separation_x = flat_x[:, None] - flat_x[None, :] + SIDE_X - 1
    return kernels[..., separation_y, separation_x]


def fft_convolution_bank(
    kernels: np.ndarray,
    *,
    embed_y: int = EMBED_Y,
    embed_x: int = EMBED_X,
) -> np.ndarray:
    pixel_count = SIDE_Y * SIDE_X
    basis = np.zeros((pixel_count, embed_y, embed_x), dtype=np.complex128)
    y, x = np.indices((SIDE_Y, SIDE_X))
    flat_y = y.ravel()
    flat_x = x.ravel()
    basis[np.arange(pixel_count), flat_y, flat_x] = 1.0
    basis_fft = np.fft.fft2(basis, axes=(-2, -1))
    output = np.empty(
        kernels.shape[:3] + (pixel_count, pixel_count),
        dtype=np.complex128,
    )
    for pair in range(kernels.shape[0]):
        for order in range(W_ORDERS):
            for moment in range(MOMENTS):
                embedding = np.zeros((embed_y, embed_x), dtype=np.complex128)
                for dy in range(-(SIDE_Y - 1), SIDE_Y):
                    for dx in range(-(SIDE_X - 1), SIDE_X):
                        embedding[dy % embed_y, dx % embed_x] = kernels[
                            pair,
                            order,
                            moment,
                            dy + SIDE_Y - 1,
                            dx + SIDE_X - 1,
                        ]
                convolved = np.fft.ifft2(
                    basis_fft * np.fft.fft2(embedding)[None, :, :],
                    axes=(-2, -1),
                )
                output[pair, order, moment] = convolved[
                    :, flat_y, flat_x
                ].T
    return output


def apply_convolution_bank(
    case: dict[str, Any],
    convolution_bank: np.ndarray,
    *,
    left_screens: np.ndarray | None = None,
    prediction_indices: np.ndarray | None = None,
    wrong_mixed_coefficient: bool = False,
    remove_right_sign: bool = False,
) -> np.ndarray:
    left = case["left_screens"] if left_screens is None else left_screens
    right = case["right_screens"]
    eta = case["eta"]
    pixel_count = case["pixel_count"]
    output = np.zeros(
        (MODEL_TERMS * pixel_count, MODEL_TERMS * pixel_count),
        dtype=np.complex128,
    )
    for pair, (imaging_state, prediction_state) in enumerate(case["pair_indices"]):
        if prediction_indices is not None:
            prediction_state = prediction_indices[pair]
        for output_term in range(MODEL_TERMS):
            output_slice = slice(
                output_term * pixel_count, (output_term + 1) * pixel_count
            )
            for model_term in range(MODEL_TERMS):
                input_slice = slice(
                    model_term * pixel_count,
                    (model_term + 1) * pixel_count,
                )
                moment = output_term + model_term
                for order in range(W_ORDERS):
                    for left_power in range(order + 1):
                        right_power = order - left_power
                        binomial = math.comb(order, left_power)
                        if (
                            wrong_mixed_coefficient
                            and order == 2
                            and left_power == 1
                        ):
                            binomial = 1
                        output[output_slice, input_slice] += (
                            binomial
                            * (
                                left[imaging_state]
                                * eta**left_power
                            )[:, None]
                            * convolution_bank[pair, order, moment]
                            * (
                                right[prediction_state]
                                * (
                                    eta**right_power
                                    if remove_right_sign
                                    else (-eta) ** right_power
                                )
                            )[None, :]
                        )
    return output


def direct_rhs(case: dict[str, Any], *, exact_w: bool) -> np.ndarray:
    output = np.zeros((MODEL_TERMS, case["pixel_count"]), dtype=np.complex128)
    for row in range(case["row_count"]):
        pair = case["pair_for_row"][row]
        imaging_state = case["pair_indices"][pair, 0]
        phase_argument = 1j * math.tau * (
            case["u"][row] * case["l"]
            + case["v"][row] * case["m"]
            + case["w"][row] * case["eta"]
        )
        if exact_w:
            phase = np.exp(phase_argument)
        else:
            uv_phase = np.exp(
                1j
                * math.tau
                * (
                    case["u"][row] * case["l"]
                    + case["v"][row] * case["m"]
                )
            )
            iw_eta = 1j * math.tau * case["w"][row] * case["eta"]
            phase = uv_phase * (1.0 + iw_eta + iw_eta**2 / 2.0)
        for output_term in range(MODEL_TERMS):
            output[output_term] += (
                case["rhs_beta"][row]
                * case["tau"][row] ** output_term
                * case["left_screens"][imaging_state]
                * phase
            )
    return output


def contracted_rhs(case: dict[str, Any]) -> np.ndarray:
    output = np.zeros((MODEL_TERMS, case["pixel_count"]), dtype=np.complex128)
    for row in range(case["row_count"]):
        pair = case["pair_for_row"][row]
        imaging_state = case["pair_indices"][pair, 0]
        uv_phase = np.exp(
            1j
            * math.tau
            * (
                case["u"][row] * case["l"]
                + case["v"][row] * case["m"]
            )
        )
        iw = 1j * math.tau * case["w"][row]
        for output_term in range(MODEL_TERMS):
            for order in range(W_ORDERS):
                output[output_term] += (
                    case["rhs_beta"][row]
                    * case["tau"][row] ** output_term
                    * iw**order
                    / math.factorial(order)
                    * case["left_screens"][imaging_state]
                    * case["eta"] ** order
                    * uv_phase
                )
    return output


def prove(source_path: pathlib.Path) -> dict[str, Any]:
    graph = load_graph_module()
    source = graph.load_json(source_path)
    case = fixture(source, graph)
    direct_polynomial = direct_response_matrix(case)
    direct_exact_w = direct_response_matrix(case, exact_w=True)
    kernels = build_kernels(case)
    explicit_bank = explicit_convolution_bank(kernels)
    fft_bank = fft_convolution_bank(kernels)
    explicit = apply_convolution_bank(case, explicit_bank)
    fft = apply_convolution_bank(case, fft_bank)

    direct_metrics = relative_metrics(explicit, direct_polynomial)
    fft_metrics = relative_metrics(fft, explicit)
    if (
        direct_metrics["relative_l2"] > ALGEBRA_L2_LIMIT
        or direct_metrics["normalized_linf"] > ALGEBRA_LINF_LIMIT
    ):
        raise FullMatrixProofError("explicit contraction missed direct rows")
    if fft_metrics["relative_l2"] > FFT_L2_LIMIT:
        raise FullMatrixProofError("FFT embedding missed explicit Toeplitz")

    wrong_candidates = {
        "wrong_taylor_coordinate": apply_convolution_bank(
            case,
            explicit_convolution_bank(
                build_kernels(case, taylor_coordinate=case["beam_tau"])
            ),
        ),
        "wrong_w_sign": apply_convolution_bank(
            case,
            explicit_convolution_bank(build_kernels(case, w_sign=-1.0)),
        ),
        "wrong_mixed_binomial_coefficient": apply_convolution_bank(
            case,
            explicit_bank,
            wrong_mixed_coefficient=True,
        ),
        "missing_right_binomial_sign": apply_convolution_bank(
            case,
            explicit_bank,
            remove_right_sign=True,
        ),
        "wrong_same_frequency_prediction_screen": apply_convolution_bank(
            case,
            explicit_bank,
            prediction_indices=case["wrong_prediction_indices"],
        ),
        "missing_pointing_scalar": apply_convolution_bank(
            case,
            explicit_convolution_bank(
                build_kernels(case, beta=case["beta_without_pointing"])
            ),
        ),
        "missing_left_conjugation": apply_convolution_bank(
            case,
            explicit_bank,
            left_screens=case["raw_imaging_screens"],
        ),
        "forced_hermitian_symmetry": 0.5 * (
            explicit + np.conj(explicit.T)
        ),
        "wrong_inverse_fft_normalization": fft * EMBED_Y,
        "one_pixel_too_small_embedding": apply_convolution_bank(
            case,
            fft_convolution_bank(
                kernels,
                embed_y=2 * SIDE_Y - 2,
                embed_x=2 * SIDE_X - 2,
            ),
        ),
    }
    denominator = max(
        direct_metrics["relative_l2"], np.finfo(np.float64).eps
    )
    controls = {
        name: {
            **relative_metrics(candidate, direct_polynomial),
            "error_ratio": (
                relative_metrics(candidate, direct_polynomial)["relative_l2"]
                / denominator
            ),
        }
        for name, candidate in wrong_candidates.items()
    }
    insensitive = [
        name
        for name, metrics in controls.items()
        if metrics["error_ratio"] < CONTROL_RATIO_MIN
    ]
    if insensitive:
        raise FullMatrixProofError(
            f"insensitive adversarial controls: {', '.join(insensitive)}"
        )

    rhs_polynomial = direct_rhs(case, exact_w=False)
    rhs_contracted = contracted_rhs(case)
    rhs_metrics = relative_metrics(rhs_contracted, rhs_polynomial)
    if (
        rhs_metrics["relative_l2"] > ALGEBRA_L2_LIMIT
        or rhs_metrics["normalized_linf"] > ALGEBRA_LINF_LIMIT
    ):
        raise FullMatrixProofError("contracted RHS missed direct rows")

    return {
        "schema": SCHEMA,
        "role": "production-inert-full-matrix-algebra-and-embedding-proof",
        "source": {
            "contract": str(source_path),
            "contract_sha256": sha256_file(source_path),
        },
        "fixture": {
            "image_shape": [SIDE_Y, SIDE_X],
            "embedding_shape": [EMBED_Y, EMBED_X],
            "model_matrix_shape": list(direct_polynomial.shape),
            "ordered_pairs": len(case["pairs"]),
            "rows_per_pair": ROWS_PER_PAIR,
            "hand_rows": case["row_count"],
            "pointing_offset_rad": case["pointing_offset_rad"],
            "coverage": [
                "all ordered pair IDs",
                "unequal complex imaging and prediction screens",
                "both signs of u, v, and w",
                "positive, negative, and near-zero Taylor coordinates",
                "unequal weights and row scalar phases",
                "every TT0 and TT1 model basis vector",
                "complete polynomial RHS",
            ],
        },
        "metrics": {
            "explicit_contraction_vs_direct_polynomial": direct_metrics,
            "fft_embedding_vs_explicit_contraction": fft_metrics,
            "rhs_contraction_vs_direct_polynomial": rhs_metrics,
            "polynomial_w_vs_exact_w": relative_metrics(
                direct_polynomial, direct_exact_w
            ),
        },
        "adversarial_controls": controls,
        "classification": {
            "algebra": "proven for the stated total-order-two polynomial",
            "embedding": "proven for the tiny alias-safe complex FFT graph",
            "w_polynomial": "controlled approximation measured separately",
            "irregular_uv_construction": "not exercised",
            "metal_f32": "not exercised",
            "scientific_promotion": "not claimed",
        },
        "decision": (
            "pass-tiny-full-matrix-proof; proceed to exact-batch FFT and "
            "controlled-construction discriminators"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-contract", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise FullMatrixProofError(f"refusing to overwrite {args.output}")
    payload = prove(args.source_contract)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    metrics = payload["metrics"]
    print(
        "decision={decision} algebra_l2={algebra:.9e} fft_l2={fft:.9e} "
        "rhs_l2={rhs:.9e}".format(
            decision=payload["decision"],
            algebra=metrics[
                "explicit_contraction_vs_direct_polynomial"
            ]["relative_l2"],
            fft=metrics["fft_embedding_vs_explicit_contraction"][
                "relative_l2"
            ],
            rhs=metrics["rhs_contraction_vs_direct_polynomial"][
                "relative_l2"
            ],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
