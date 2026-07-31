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


SCHEMA = "casa-rs-vlass-localized-row-census-contract/v2"
ROW_SCHEMAS = {
    "casa-rs-vlass-localized-row-census/v1",
    "casa-rs-vlass-localized-row-census/v2",
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


def derive_contract(
    row_manifest_path: pathlib.Path,
    screen_manifest_path: pathlib.Path,
) -> dict[str, Any]:
    row_manifest = load_json(row_manifest_path)
    screen_manifest = load_json(screen_manifest_path)
    if row_manifest.get("schema") not in ROW_SCHEMAS:
        raise CensusContractError("unsupported localized-row census schema")
    if screen_manifest.get("schema") != SCREEN_SCHEMA:
        raise CensusContractError(f"screen manifest must use {SCREEN_SCHEMA}")
    if ROW_DTYPE.itemsize != 128:
        raise AssertionError(f"localized row dtype is {ROW_DTYPE.itemsize}, expected 128")

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
    return {
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--row-manifest", type=pathlib.Path, required=True)
    parser.add_argument("--screen-manifest", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise CensusContractError(f"refusing to overwrite {args.output}")
    payload = derive_contract(args.row_manifest, args.screen_manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
