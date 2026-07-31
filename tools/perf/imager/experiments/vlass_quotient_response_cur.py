#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reduce the VLASS mask-restricted quotient-response CUR discriminator.

The Rust diagnostic samples the real full-16-SPW AWProject response after
cropped CF selection, oversampling, conjugation, POINTING tap phase, residual
W, support clipping, and normalization are fixed. The emitted matrix omits the
position-dependent flat-sky weight and grid apodization, so this reducer is a
one-way operator-core rank test: high rank rejects the proposed quotient, while
low rank only promotes an exact prepared-atom discriminator.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt


EXPECTED_SCHEMA = "casa-rs-vlass-quotient-response-census-v1"
EXPECTED_ROLE = (
    "production-inert-operator-core-rank-lower-bound-"
    "not-performance-or-science-evidence"
)
EXPECTED_SHAPE = (6_144, 1_152)
EXPECTED_TRAIN_ROWS = 4_096
EXPECTED_HOLDOUT_ROWS = 2_048
EXPECTED_TRAIN_COLUMNS = 768
EXPECTED_HOLDOUT_COLUMNS = 384
EXPECTED_COMPONENT_CENTERS = 4_096
EXPECTED_DILATED_SUPPORT_PIXELS = 7_304
EXPECTED_PLAN_REFERENCES = 771_724
EXPECTED_SCALES = [0, 5, 12]
MAX_RANK = 8
RMS_GATE = 2.0e-5
MAX_GATE = 2.0e-4
STRATUM_RMS_GATE = 6.0e-5
PROMOTION_STATE_BYTES = 121_006_323
RETIRE_STATE_BYTES = 151_257_904

ComplexMatrix = npt.NDArray[np.complex128]
FloatVector = npt.NDArray[np.float64]


class QuotientResponseError(RuntimeError):
    """Raised when a quotient-response receipt changes its contract."""


def utc_now() -> str:
    """Return a stable UTC timestamp."""

    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    """Hash one file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def require_dict(value: Any, *, label: str) -> dict[str, Any]:
    """Require a JSON object."""

    if not isinstance(value, dict):
        raise QuotientResponseError(f"{label} must be an object")
    return value


def require_list(value: Any, *, label: str) -> list[Any]:
    """Require a JSON array."""

    if not isinstance(value, list):
        raise QuotientResponseError(f"{label} must be an array")
    return value


def require_int(value: Any, *, label: str) -> int:
    """Require a non-boolean integer."""

    if not isinstance(value, int) or isinstance(value, bool):
        raise QuotientResponseError(f"{label} must be an integer")
    return value


def require_finite(value: Any, *, label: str) -> float:
    """Require a finite number."""

    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise QuotientResponseError(f"{label} must be finite")
    return float(value)


def _relative_metrics(
    actual: ComplexMatrix,
    expected: ComplexMatrix,
) -> tuple[float, float, float]:
    error = actual - expected
    expected_norm = max(float(np.linalg.norm(expected)), np.finfo(np.float64).tiny)
    expected_peak = max(float(np.max(np.abs(expected))), np.finfo(np.float64).tiny)
    relative = np.abs(error).ravel() / expected_peak
    return (
        float(np.linalg.norm(error)) / expected_norm,
        float(np.max(relative)),
        float(np.quantile(relative, 0.999)),
    )


def _quartile_labels(values: FloatVector) -> npt.NDArray[np.int64]:
    edges = np.quantile(values, [0.25, 0.5, 0.75])
    return np.searchsorted(edges, values, side="right")


def _stratum_metrics(
    actual: ComplexMatrix,
    expected: ComplexMatrix,
    row_metadata: list[dict[str, Any]],
    weighted_rows: FloatVector,
) -> list[dict[str, Any]]:
    frequency = np.asarray(
        [require_finite(row.get("frequency_hz"), label="row.frequency_hz")
         for row in row_metadata],
        dtype=np.float64,
    )
    represented_w = np.asarray(
        [
            require_finite(
                row.get("represented_w_lambda"),
                label="row.represented_w_lambda",
            )
            for row in row_metadata
        ],
        dtype=np.float64,
    )
    mueller = np.asarray(
        [require_int(row.get("mueller_element"), label="row.mueller_element")
         for row in row_metadata],
        dtype=np.int64,
    )
    strata: list[tuple[str, npt.NDArray[np.bool_]]] = []
    for label, labels in (
        ("frequency-quartile", _quartile_labels(frequency)),
        ("abs-w-quartile", _quartile_labels(np.abs(represented_w))),
    ):
        strata.extend(
            (f"{label}-{index}", labels == index)
            for index in range(4)
        )
    strata.extend(
        (f"mueller-{int(value)}", mueller == value)
        for value in np.unique(mueller)
    )

    output = []
    for label, selected in strata:
        if not np.any(selected):
            continue
        scale = weighted_rows[selected, np.newaxis]
        rms, maximum, p999 = _relative_metrics(
            actual[selected] * scale,
            expected[selected] * scale,
        )
        output.append(
            {
                "stratum": label,
                "rows": int(np.count_nonzero(selected)),
                "weighted_relative_rms": rms,
                "normalized_max_abs": maximum,
                "normalized_p999_abs": p999,
            }
        )
    return output


def projected_runtime_state_bytes(
    *,
    rank: int,
    unique_response_rows: int,
    component_atoms: int,
    plan_references: int,
) -> dict[str, int]:
    """Project compact f32 factor runtime state without materialized tap data."""

    factor_bytes = 8 * rank * (unique_response_rows + component_atoms)
    plan_row_map_bytes = 8 * plan_references
    visibility_bytes = 8 * plan_references
    geometric_phase_state_bytes = 16 * unique_response_rows
    total = (
        factor_bytes
        + plan_row_map_bytes
        + visibility_bytes
        + geometric_phase_state_bytes
    )
    return {
        "factor_bytes": factor_bytes,
        "plan_row_map_bytes": plan_row_map_bytes,
        "visibility_bytes": visibility_bytes,
        "geometric_phase_state_bytes": geometric_phase_state_bytes,
        "total_bytes": total,
    }


def analyze_matrix(
    matrix: ComplexMatrix,
    *,
    train_rows: int,
    train_columns: int,
    row_weights: FloatVector,
    row_metadata: list[dict[str, Any]],
    unique_response_rows: int,
    component_atoms: int,
    plan_references: int,
    max_rank: int = MAX_RANK,
) -> dict[str, Any]:
    """Measure held-out cross-block rank without materializing full factors."""

    if matrix.ndim != 2:
        raise QuotientResponseError("matrix must be two-dimensional")
    row_count, column_count = matrix.shape
    if not (0 < train_rows < row_count and 0 < train_columns < column_count):
        raise QuotientResponseError("train split must leave held-out rows and columns")
    if row_weights.shape != (row_count,):
        raise QuotientResponseError("row weights do not match matrix rows")
    if len(row_metadata) != row_count:
        raise QuotientResponseError("row metadata do not match matrix rows")
    if not np.all(np.isfinite(matrix)):
        raise QuotientResponseError("matrix contains non-finite values")
    if not np.all(np.isfinite(row_weights)) or np.any(row_weights <= 0.0):
        raise QuotientResponseError("row weights must be finite and positive")

    normalized_weights = row_weights / float(np.mean(row_weights))
    root_weights = np.sqrt(normalized_weights)
    train_weight = root_weights[:train_rows, np.newaxis]
    holdout_weight = root_weights[train_rows:, np.newaxis]
    train_train = matrix[:train_rows, :train_columns] * train_weight
    holdout_train = matrix[train_rows:, :train_columns] * holdout_weight
    train_holdout = matrix[:train_rows, train_columns:] * train_weight
    holdout_holdout = matrix[train_rows:, train_columns:] * holdout_weight

    u, singular_values, vh = np.linalg.svd(train_train, full_matrices=False)
    if singular_values.size == 0 or singular_values[0] <= 0.0:
        raise QuotientResponseError("training matrix has no non-zero singular value")
    rows = []
    survivor_rank: int | None = None
    for rank in range(1, min(max_rank, singular_values.size) + 1):
        left = holdout_train @ vh[:rank].conj().T
        left /= singular_values[:rank]
        right = u[:, :rank].conj().T @ train_holdout
        predicted = left @ right
        rms, maximum, p999 = _relative_metrics(predicted, holdout_holdout)
        strata = _stratum_metrics(
            predicted,
            holdout_holdout,
            row_metadata[train_rows:],
            holdout_weight[:, 0],
        )
        worst_stratum = max(
            (item["weighted_relative_rms"] for item in strata),
            default=math.inf,
        )
        state = projected_runtime_state_bytes(
            rank=rank,
            unique_response_rows=unique_response_rows,
            component_atoms=component_atoms,
            plan_references=plan_references,
        )
        passes = (
            rms <= RMS_GATE
            and maximum <= MAX_GATE
            and worst_stratum <= STRATUM_RMS_GATE
            and state["total_bytes"] <= PROMOTION_STATE_BYTES
        )
        if passes and survivor_rank is None:
            survivor_rank = rank
        rows.append(
            {
                "rank": rank,
                "train_singular_value_ratio": (
                    float(singular_values[rank - 1] / singular_values[0])
                ),
                "heldout_weighted_relative_rms": rms,
                "heldout_normalized_max_abs": maximum,
                "heldout_normalized_p999_abs": p999,
                "worst_stratum_weighted_relative_rms": worst_stratum,
                "strata": strata,
                "projected_runtime_state": state,
                "passes_operator_core_gate": passes,
            }
        )

    minimum_state = rows[0]["projected_runtime_state"]["total_bytes"]
    if minimum_state > RETIRE_STATE_BYTES:
        decision = "retire-quotient-response-cur-state-floor"
        next_step = "advance-mask-normal-correction-factorization"
    elif survivor_rank is None:
        decision = "retire-current-quotient-response-cur-for-high-operator-core-rank"
        next_step = "advance-mask-normal-correction-factorization"
    else:
        decision = "promote-exact-prepared-atom-cur-discriminator"
        next_step = (
            "include-flat-sky-weight-and-apodization-then-run-"
            "target-hardware-construction-and-direct-641-race"
        )
    return {
        "curve": rows,
        "selection": {
            "decision": decision,
            "next_step": next_step,
            "first_survivor_rank": survivor_rank,
        },
    }


def load_and_analyze(census_dir: Path) -> dict[str, Any]:
    """Validate a Rust census and run the held-out operator-core rank test."""

    manifest_path = census_dir / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as error:
        raise QuotientResponseError(f"cannot read {manifest_path}: {error}") from error
    except json.JSONDecodeError as error:
        raise QuotientResponseError(f"invalid JSON in {manifest_path}: {error}") from error
    manifest = require_dict(manifest, label="manifest")
    if manifest.get("schema") != EXPECTED_SCHEMA:
        raise QuotientResponseError(f"schema must be {EXPECTED_SCHEMA}")
    if manifest.get("role") != EXPECTED_ROLE:
        raise QuotientResponseError("manifest role changed")
    contract = require_dict(manifest.get("contract"), label="contract")
    matrix_contract = require_dict(manifest.get("matrix"), label="matrix")
    if (
        contract.get("scales") != EXPECTED_SCALES
        or contract.get("quotient")
        != "central-component-phasor-and-tap-independent-normalization-only"
        or contract.get("omitted_from_response")
        != "position-dependent-flat-sky-weight-and-grid-apodization"
    ):
        raise QuotientResponseError("quotient/scale contract changed")
    expected_contract = {
        "imsize": 4_096,
        "nterms": 2,
        "component_center_pixels": EXPECTED_COMPONENT_CENTERS,
        "dilated_support_pixels": EXPECTED_DILATED_SUPPORT_PIXELS,
        "full_plan_references": EXPECTED_PLAN_REFERENCES,
        "train_rows": EXPECTED_TRAIN_ROWS,
        "holdout_rows": EXPECTED_HOLDOUT_ROWS,
        "train_columns": EXPECTED_TRAIN_COLUMNS,
        "holdout_columns": EXPECTED_HOLDOUT_COLUMNS,
    }
    for key, expected in expected_contract.items():
        if require_int(contract.get(key), label=f"contract.{key}") != expected:
            raise QuotientResponseError(f"contract.{key} must be {expected}")
    if matrix_contract.get("dtype") != "complex128-little-endian-interleaved":
        raise QuotientResponseError("matrix dtype changed")
    shape = tuple(require_list(matrix_contract.get("shape"), label="matrix.shape"))
    if shape != EXPECTED_SHAPE:
        raise QuotientResponseError(f"matrix shape must be {EXPECTED_SHAPE}")
    matrix_path_value = matrix_contract.get("path")
    if not isinstance(matrix_path_value, str) or Path(matrix_path_value).name != matrix_path_value:
        raise QuotientResponseError("matrix.path must be one local filename")
    matrix_path = census_dir / matrix_path_value
    matrix_bytes = require_int(matrix_contract.get("bytes"), label="matrix.bytes")
    expected_bytes = math.prod(EXPECTED_SHAPE) * np.dtype("<c16").itemsize
    if matrix_bytes != expected_bytes or matrix_path.stat().st_size != expected_bytes:
        raise QuotientResponseError("matrix byte count changed")
    matrix_sha256 = matrix_contract.get("sha256")
    if not isinstance(matrix_sha256, str) or sha256_file(matrix_path) != matrix_sha256:
        raise QuotientResponseError("matrix SHA-256 mismatch")
    row_metadata_values = require_list(manifest.get("rows"), label="rows")
    row_metadata = [
        require_dict(value, label=f"rows[{index}]")
        for index, value in enumerate(row_metadata_values)
    ]
    if len(row_metadata) != EXPECTED_SHAPE[0]:
        raise QuotientResponseError("row metadata length changed")
    row_weights = np.asarray(
        [
            require_finite(row.get("statistical_weight"), label="row.statistical_weight")
            for row in row_metadata
        ],
        dtype=np.float64,
    )
    matrix = np.memmap(
        matrix_path,
        dtype="<c16",
        mode="r",
        shape=EXPECTED_SHAPE,
    )
    unique_response_rows = require_int(
        contract.get("full_unique_response_rows"),
        label="contract.full_unique_response_rows",
    )
    analysis = analyze_matrix(
        matrix,
        train_rows=EXPECTED_TRAIN_ROWS,
        train_columns=EXPECTED_TRAIN_COLUMNS,
        row_weights=row_weights,
        row_metadata=row_metadata,
        unique_response_rows=unique_response_rows,
        component_atoms=EXPECTED_COMPONENT_CENTERS * len(EXPECTED_SCALES),
        plan_references=EXPECTED_PLAN_REFERENCES,
    )
    return {
        "schema": "casa-rs-vlass-quotient-response-cur-analysis-v1",
        "created_at": utc_now(),
        "role": EXPECTED_ROLE,
        "evidence_class": "measured-operator-core-cross-block-rank-lower-bound",
        "input": {
            "manifest": str(manifest_path),
            "manifest_sha256": sha256_file(manifest_path),
            "matrix": str(matrix_path),
            "matrix_sha256": matrix_sha256,
        },
        "contract": contract,
        "gates": {
            "heldout_weighted_relative_rms_max": RMS_GATE,
            "heldout_normalized_max_abs": MAX_GATE,
            "worst_stratum_weighted_relative_rms_max": STRATUM_RMS_GATE,
            "promotion_state_bytes_max": PROMOTION_STATE_BYTES,
            "retire_state_bytes_min": RETIRE_STATE_BYTES,
            "construction_and_direct_trajectory_time": "deferred-not-measured",
            "scientific_trajectory": "deferred-not-executed",
        },
        **analysis,
        "claim_boundary": (
            "High rank rejects only this quotient-response operator core. Low "
            "rank cannot promote factors because position-dependent flat-sky "
            "weight, grid apodization, exact 641-component construction/runtime, "
            "and the 19-product scientific contract remain unmeasured."
        ),
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    """Write one fail-closed analysis receipt."""

    args = parse_args()
    if args.output.exists():
        raise QuotientResponseError(f"refusing to overwrite {args.output}")
    result = load_and_analyze(args.census_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
