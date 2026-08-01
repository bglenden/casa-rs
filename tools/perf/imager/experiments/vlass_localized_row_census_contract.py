#!/usr/bin/env python3
"""Correctly separate VLASS PB groups from selected AW screen states.

The production-inert row census records both the CASA SimplePB grouping
frequency and the per-sample visibility frequency.  They are not the same
state axis: AWProject selects the nearest persisted CF frequency after its
normal/conjugate-frequency mapping, while the SimplePB frequency remains
needed for PB and product semantics.  This reducer derives both state
inventories without rerunning the imager.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-localized-row-census-contract/v3"
ROW_SCHEMAS = {
    "casa-rs-vlass-localized-row-census/v3",
}
SCREEN_SCHEMA = "casa-rs-vlass-evla-pre-w-screens/v2"
ROW_DTYPE = np.dtype(
    [
        ("uvw_lambda", "<f8", (3,)),
        ("frequency_hz", "<f8"),
        ("beam_frequency_hz", "<f8"),
        ("pointing_direction_rad", "<f8", (2,)),
        ("pointing_pixel", "<f8", (2,)),
        ("weight", "<f4"),
        ("sumwt_factor", "<f4"),
        ("scalar_visibility", "<c8"),
        ("first_visibility", "<c8"),
        ("second_visibility", "<c8"),
        ("source_phase", "<c8"),
        ("first_prediction_normalization", "<c8"),
        ("second_prediction_normalization", "<c8"),
        ("replay_block_ordinal", "<u4"),
        ("window_ordinal", "<u4"),
        ("sample_index", "<u4"),
        ("group_index", "<u4"),
    ],
    align=False,
)


class CensusContractError(RuntimeError):
    """The row/screen artifacts violate the frozen discriminator contract."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CensusContractError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(payload, dict):
        raise CensusContractError(f"JSON root must be an object: {path}")
    return payload


def nearest_indices(values: np.ndarray, candidates: np.ndarray) -> np.ndarray:
    if candidates.ndim != 1 or candidates.size == 0:
        raise CensusContractError("screen frequency inventory is empty")
    positions = np.searchsorted(candidates, values)
    right = np.minimum(positions, candidates.size - 1)
    left = np.maximum(positions - 1, 0)
    choose_right = np.abs(values - candidates[right]) < np.abs(
        values - candidates[left]
    )
    return np.where(choose_right, right, left)


def unique_pointing_count(groups: list[dict[str, Any]]) -> int:
    fields = (
        "pointing_ra_rad_bits",
        "pointing_dec_rad_bits",
        "pointing_x_pixel_bits",
        "pointing_y_pixel_bits",
    )
    return len({tuple(group[field] for field in fields) for group in groups})


def pair_key(pair: dict[str, Any]) -> tuple[int, int, int, int]:
    try:
        return (
            int(round(float(pair["imaging_frequency_hz"]))),
            int(pair["imaging_mueller_element"]),
            int(round(float(pair["prediction_frequency_hz"]))),
            int(pair["prediction_mueller_element"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CensusContractError(
            "ordered-response state pair has an invalid shape"
        ) from error


def attach_ordered_response_state_universe(
    contract: dict[str, Any],
    state_universe_contract_path: pathlib.Path,
) -> None:
    """Attach an explicit resident superset without rewriting observed states."""

    source_path = state_universe_contract_path.resolve()
    universe_contract = load_json(source_path)
    if universe_contract.get("schema") != SCHEMA:
        raise CensusContractError(
            f"ordered-response state universe must use {SCHEMA}"
        )
    observed_sources = contract.get("sources")
    universe_sources = universe_contract.get("sources")
    observed_selection = contract.get("aw_screen_selection")
    universe_selection = universe_contract.get("aw_screen_selection")
    if not all(
        isinstance(value, dict)
        for value in (
            observed_sources,
            universe_sources,
            observed_selection,
            universe_selection,
        )
    ):
        raise CensusContractError(
            "ordered-response state universe lacks source or AW selection metadata"
        )
    assert isinstance(observed_sources, dict)
    assert isinstance(universe_sources, dict)
    assert isinstance(observed_selection, dict)
    assert isinstance(universe_selection, dict)
    if (
        observed_sources.get("screen_manifest_sha256")
        != universe_sources.get("screen_manifest_sha256")
    ):
        raise CensusContractError(
            "ordered-response state universe uses a different physical screen manifest"
        )
    if (
        observed_selection.get("persisted_cf_frequencies_hz")
        != universe_selection.get("persisted_cf_frequencies_hz")
        or observed_selection.get("mueller_elements")
        != universe_selection.get("mueller_elements")
    ):
        raise CensusContractError(
            "ordered-response state universe changes the persisted CF state axes"
        )

    observed_pairs = observed_selection.get("imaging_prediction_state_pairs")
    resident_pairs = universe_selection.get("imaging_prediction_state_pairs")
    if not isinstance(observed_pairs, list) or not isinstance(resident_pairs, list):
        raise CensusContractError(
            "ordered-response state universe lacks ordered AW pair lists"
        )
    observed_keys = {pair_key(pair) for pair in observed_pairs}
    resident_keys = {pair_key(pair) for pair in resident_pairs}
    if len(observed_keys) != len(observed_pairs):
        raise CensusContractError("observed ordered AW pair list contains duplicates")
    if len(resident_keys) != len(resident_pairs):
        raise CensusContractError("resident ordered AW pair list contains duplicates")
    if not observed_keys.issubset(resident_keys):
        raise CensusContractError(
            "ordered-response resident state universe omits an observed row route"
        )
    resident_imaging_states = {
        (key[0], key[1]) for key in resident_keys
    }
    resident_prediction_states = {
        (key[2], key[3]) for key in resident_keys
    }
    contract["ordered_response_state_universe"] = {
        "role": (
            "explicit provenance-bound resident superset; observed row selection "
            "remains authoritative in aw_screen_selection"
        ),
        "source_contract": str(source_path),
        "source_contract_sha256": sha256_file(source_path),
        "source_screen_manifest_sha256": universe_sources[
            "screen_manifest_sha256"
        ],
        "imaging_prediction_state_pairs": resident_pairs,
        "observed_ordered_aw_pairs": len(observed_pairs),
        "resident_ordered_aw_pairs": len(resident_pairs),
        "inactive_resident_ordered_aw_pairs": len(resident_pairs)
        - len(observed_pairs),
        "resident_imaging_frequency_mueller_states": len(
            resident_imaging_states
        ),
        "resident_prediction_frequency_mueller_states": len(
            resident_prediction_states
        ),
        "observed_pairs_are_subset": True,
    }


def derive_contract(
    row_manifest_path: pathlib.Path,
    screen_manifest_path: pathlib.Path,
    state_universe_contract_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    row_manifest = load_json(row_manifest_path)
    screen_manifest = load_json(screen_manifest_path)
    if row_manifest.get("schema") not in ROW_SCHEMAS:
        raise CensusContractError("unsupported localized-row census schema")
    if screen_manifest.get("schema") != SCREEN_SCHEMA:
        raise CensusContractError(f"screen manifest must use {SCREEN_SCHEMA}")
    if ROW_DTYPE.itemsize != 144:
        raise AssertionError(f"localized row dtype is {ROW_DTYPE.itemsize}, expected 144")

    row_info = row_manifest.get("rows")
    contract = row_manifest.get("contract")
    if not isinstance(row_info, dict) or not isinstance(contract, dict):
        raise CensusContractError("row manifest lacks rows or contract object")
    row_path = pathlib.Path(str(row_info.get("path", "")))
    if not row_path.is_absolute():
        row_path = row_manifest_path.parent / row_path
    row_count = int(row_info.get("count", -1))
    expected_bytes = row_count * ROW_DTYPE.itemsize
    if row_count <= 0 or row_path.stat().st_size != expected_bytes:
        raise CensusContractError(
            f"row file has {row_path.stat().st_size} bytes, expected {expected_bytes}"
        )
    row_sha256 = sha256_file(row_path)
    if row_sha256 != row_info.get("sha256"):
        raise CensusContractError("localized row payload SHA-256 does not match manifest")

    states = screen_manifest.get("states")
    if not isinstance(states, list) or not states:
        raise CensusContractError("screen manifest has no states")
    cf_frequencies_hz = np.array(
        sorted({float(state["frequency_hz"]) for state in states}), dtype=np.float64
    )
    mueller_elements = sorted({int(state["mueller_element"]) for state in states})
    if mueller_elements != [0, 15]:
        raise CensusContractError(
            f"expected Stokes-I Mueller elements [0, 15], got {mueller_elements}"
        )

    rows = np.memmap(row_path, dtype=ROW_DTYPE, mode="r", shape=(row_count,))
    weights = np.asarray(rows["weight"], dtype=np.float64)
    sumwt_factors = np.asarray(rows["sumwt_factor"], dtype=np.float64)
    prediction_normalizations = np.stack(
        [
            np.asarray(
                rows["first_prediction_normalization"],
                dtype=np.complex128,
            ),
            np.asarray(
                rows["second_prediction_normalization"],
                dtype=np.complex128,
            ),
        ],
        axis=1,
    )
    prediction_normalization_norm_squared = np.abs(prediction_normalizations) ** 2
    if np.any(~np.isfinite(weights)) or np.any(weights <= 0.0):
        raise CensusContractError("row weights are not positive and finite")
    if not np.all(sumwt_factors == 2.0):
        raise CensusContractError(
            "VLASS Stokes-I row sumwt_factor is not exactly two parallel hands"
        )
    if (
        np.any(~np.isfinite(prediction_normalizations))
        or np.any(~np.isfinite(prediction_normalization_norm_squared))
        or np.any(prediction_normalization_norm_squared <= 0.0)
    ):
        raise CensusContractError(
            "prediction CF normalizations are not non-zero and finite"
        )
    sample_frequencies_hz = np.asarray(rows["frequency_hz"], dtype=np.float64)
    reference_frequency_hz = float(contract["reference_frequency_hz"])
    radicand = (
        2.0 * reference_frequency_hz * reference_frequency_hz
        - sample_frequencies_hz * sample_frequencies_hz
    )
    if np.any(~np.isfinite(radicand)) or np.any(radicand <= 0.0):
        raise CensusContractError("conjugate-frequency map left the positive finite domain")
    requested_imaging_hz = np.sqrt(radicand)
    imaging_indices = nearest_indices(requested_imaging_hz, cf_frequencies_hz)
    prediction_indices = nearest_indices(sample_frequencies_hz, cf_frequencies_hz)
    selected_imaging_frequencies_hz = cf_frequencies_hz[
        np.unique(imaging_indices)
    ].tolist()
    selected_prediction_frequencies_hz = cf_frequencies_hz[
        np.unique(prediction_indices)
    ].tolist()
    required_frequencies_hz = sorted(
        set(selected_imaging_frequencies_hz)
        | set(selected_prediction_frequencies_hz)
    )
    positive_w = np.asarray(rows["uvw_lambda"][:, 2] > 0.0)
    operator_pairs: set[tuple[float, int, float, int]] = set()
    for mueller_element in mueller_elements:
        imaging_mueller = np.where(
            positive_w, mueller_element, 15 - mueller_element
        )
        prediction_mueller = np.where(
            positive_w, 15 - mueller_element, mueller_element
        )
        operator_pairs.update(
            zip(
                cf_frequencies_hz[imaging_indices].tolist(),
                imaging_mueller.tolist(),
                cf_frequencies_hz[prediction_indices].tolist(),
                prediction_mueller.tolist(),
                strict=True,
            )
        )
    operator_pair_metadata = [
        {
            "imaging_frequency_hz": imaging_frequency_hz,
            "imaging_mueller_element": imaging_mueller,
            "prediction_frequency_hz": prediction_frequency_hz,
            "prediction_mueller_element": prediction_mueller,
        }
        for (
            imaging_frequency_hz,
            imaging_mueller,
            prediction_frequency_hz,
            prediction_mueller,
        ) in sorted(operator_pairs)
    ]

    groups = row_manifest.get("pb_groups", row_manifest.get("groups"))
    if not isinstance(groups, list) or not groups:
        raise CensusContractError("row manifest has no PB group inventory")
    unique_pb_frequencies = {
        str(group["beam_frequency_hz_bits"]) for group in groups
    }
    screen_manifest_sha256 = sha256_file(screen_manifest_path)
    result = {
        "schema": SCHEMA,
        "role": "production-inert-contract-correction-and-row-state-discriminator",
        "sources": {
            "row_manifest": str(row_manifest_path),
            "row_manifest_sha256": sha256_file(row_manifest_path),
            "row_payload": str(row_path),
            "row_payload_sha256": row_sha256,
            "screen_manifest": str(screen_manifest_path),
            "screen_manifest_sha256": screen_manifest_sha256,
        },
        "rows": {
            "count": row_count,
            "record_bytes": ROW_DTYPE.itemsize,
            "bytes": expected_bytes,
            "parallel_hand_routes": 2 * row_count,
            "weight_semantics": (
                "stored average-hand weight is applied once per explicit RR/LL "
                "route; sumwt_factor=2 is diagnostic and is not multiplied again"
            ),
            "prediction_normalization_semantics": (
                "each explicit RR/LL route retains the executable compact-CF "
                "normalization used by degridding as 1/conj(normalization)"
            ),
            "prediction_normalization_abs_min": float(
                np.min(np.abs(prediction_normalizations))
            ),
            "prediction_normalization_abs_max": float(
                np.max(np.abs(prediction_normalizations))
            ),
        },
        "pb_grouping": {
            "unique_pointings": unique_pointing_count(groups),
            "unique_beam_frequencies": len(unique_pb_frequencies),
            "unique_pointing_frequency_groups": len(groups),
            "semantics": (
                "SimplePB product/group metadata retained per row; not a distinct "
                "AW A-screen texture axis"
            ),
        },
        "aw_screen_selection": {
            "persisted_cf_frequencies_hz": cf_frequencies_hz.tolist(),
            "mueller_elements": mueller_elements,
            "selected_imaging_frequencies_hz": selected_imaging_frequencies_hz,
            "selected_prediction_frequencies_hz": selected_prediction_frequencies_hz,
            "selected_imaging_frequency_mueller_states": len(
                selected_imaging_frequencies_hz
            )
            * len(mueller_elements),
            "selected_prediction_frequency_mueller_states": len(
                selected_prediction_frequencies_hz
            )
            * len(mueller_elements),
            "required_frequency_union_hz": required_frequencies_hz,
            "required_frequency_mueller_states": len(required_frequencies_hz)
            * len(mueller_elements),
            "required_dual_screen_texture_count": len(required_frequencies_hz)
            * len(mueller_elements)
            * 2,
            "observed_imaging_prediction_state_pairs": len(operator_pair_metadata),
            "imaging_prediction_state_pairs": operator_pair_metadata,
            "imaging_frequency_map": (
                "nearest persisted CF frequency to "
                "sqrt(2*reference_frequency_hz^2-sample_frequency_hz^2)"
            ),
            "prediction_frequency_map": (
                "nearest persisted CF frequency to sample_frequency_hz"
            ),
        },
        "decision": {
            "earlier_screen_gate": (
                "retained as real AW screen-family evidence because the 104 "
                "SimplePB frequencies map to the same 16 persisted AW CF frequencies"
            ),
            "row_manifest_v1_correction": (
                "physical_screen_states=208 mislabeled PB frequency x Mueller "
                "groups as AW screen states; this receipt supersedes that label "
                "without changing the row payload"
            ),
        },
    }
    if state_universe_contract_path is not None:
        attach_ordered_response_state_universe(
            result,
            state_universe_contract_path,
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--row-manifest", type=pathlib.Path, required=True)
    parser.add_argument("--screen-manifest", type=pathlib.Path, required=True)
    parser.add_argument(
        "--ordered-response-state-universe-contract",
        type=pathlib.Path,
        help=(
            "optional census contract whose ordered AW pairs form an explicit "
            "resident superset without changing the observed selection"
        ),
    )
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise CensusContractError(f"refusing to overwrite {args.output}")
    payload = derive_contract(
        args.row_manifest,
        args.screen_manifest,
        args.ordered_response_state_universe_contract,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
