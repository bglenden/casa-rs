#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate and classify the bounded VLASS AW literal-coefficient audit.

Exit status zero means that the receipt satisfies the frozen experiment
contract. It does not mean that casa-rs coefficients match CASA: the ordered
coefficient hashes compare two casa-rs paths over the same frozen CF pixels.
The only cross-producer comparison is the conditional whole-TT0-grid hash.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import vlass_aw_datagrid_tt0_arithmetic_compat_compare as arithmetic


CASA_RS_V4_RECEIPT_SHA256 = arithmetic.CASA_RS_V4_RECEIPT_SHA256
CASA_RS_V4_EVIDENCE_SHA256 = arithmetic.CASA_RS_V4_EVIDENCE_SHA256
CASA_RS_V4_REVISION = arithmetic.CASA_RS_V4_REVISION
CASA_V5_RECEIPT_SHA256 = arithmetic.CASA_V5_RECEIPT_SHA256
CASA_SOURCE_COMMIT = arithmetic.CASA_SOURCE_COMMIT
CASACORE_SOURCE_COMMIT = "25b653f6963a78a1dcfc8e16954081e091a50fbe"
ARITHMETIC_V1_RECEIPT_SHA256 = (
    "a9c7fc453d343a48745269744ffd257a5ca8c532ccefe4ac74ba5a85b0ce9271"
)
ARITHMETIC_V1_EVIDENCE_SHA256 = (
    "c2b2bc4daafe12aa0090d9d00e8cdd02ca627c2fa671f846fb6625aad912af99"
)
ARITHMETIC_V1_COMPARISON_SHA256 = (
    "e50bf9642a442688dc2f5f37390c63e1a04cd0ad19729f4daea4a0bf43be608e"
)
ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256 = (
    "dfcd28767cb60a727f1486a49a9a9b9ad96748114ff69d47d9a8e3c8dec5f73b"
)
ARITHMETIC_V1_REVISION = "dc159dc629c5e09c83d2027d06b5d909bf4f4c0a"

CASA_TT0_GRID_HASH = arithmetic.CASA_TT0_GRID_HASH
CASA_RS_V4_TT0_GRID_HASH = arithmetic.CASA_RS_V4_TT0_GRID_HASH
GRID_VALUES = arithmetic.GRID_VALUES
GRID_BYTES = GRID_VALUES * 16
SOURCE_COUNT = arithmetic.SOURCE_COUNT
ROLE_COUNT = arithmetic.ROLE_COUNT
TAP_COUNT = 7_343_758
TRAVERSAL_HASH = 13_023_404_341_375_331_330

CANDIDATE_ENVELOPE_SCHEMA = "casa-rs-aw-datagrid-literal-coefficient-audit-envelope-v1"
CANDIDATE_EVIDENCE_SCHEMA = "casa-rs-aw-datagrid-literal-coefficient-audit-v1"
COMPARISON_ENVELOPE_SCHEMA = (
    "casa-rs-aw-datagrid-literal-coefficient-audit-comparison-envelope-v1"
)
COMPARISON_SCHEMA = "casa-rs-aw-datagrid-literal-coefficient-audit-comparison-v1"
RESULT_TAXONOMY = [
    "completed-literal-packed-exact-no-grid",
    "completed-literal-packed-mismatch-grid-matches-casa",
    "completed-literal-packed-mismatch-grid-matches-rust",
    "completed-literal-packed-mismatch-grid-matches-neither",
]
TRAVERSAL_CONTRACT = (
    "source-then-first-rr-logical-mueller-0-then-second-ll-logical-"
    "mueller-15-then-iy-then-ix"
)
ORDERED_HASH_CONTRACTS = {
    "destination": (
        "fnv1a64-little-endian-source-sample-group-role-bundle-tap-iy-ix-"
        "grid-y-grid-x-route-only"
    ),
    "selected_cell": (
        "fnv1a64-little-endian-source-sample-group-role-bundle-cell-key-"
        "conjugate-once-per-logical-role"
    ),
    "stage": (
        "fnv1a64-little-endian-source-sample-group-role-bundle-tap-iy-ix-"
        "grid-y-grid-x-then-complex32-stage-value"
    ),
}
CLASSIFICATIONS = {
    "completed-literal-packed-exact-no-grid": (
        "valid-negative-literal-coefficient-boundary-excluded"
    ),
    "completed-literal-packed-mismatch-grid-matches-casa": (
        "candidate-localization-literal-grid-matches-frozen-casa"
    ),
    "completed-literal-packed-mismatch-grid-matches-rust": (
        "valid-negative-literal-grid-reproduces-frozen-rust"
    ),
    "completed-literal-packed-mismatch-grid-matches-neither": (
        "valid-unanchored-literal-grid-hard-stop"
    ),
}
DISPOSITIONS = {
    "completed-literal-packed-exact-no-grid": (
        "boundary-excluded-continue-with-next-approved-localization"
    ),
    "completed-literal-packed-mismatch-grid-matches-casa": (
        "candidate-localization-requires-integrated-promotion-gates"
    ),
    "completed-literal-packed-mismatch-grid-matches-rust": (
        "boundary-excluded-no-production-promotion"
    ),
    "completed-literal-packed-mismatch-grid-matches-neither": (
        "unanchored-hard-stop-no-production-promotion"
    ),
}

EXPECTED_CASA_SOURCE = {
    "casa_commit": CASA_SOURCE_COMMIT,
    "casacore_commit": CASACORE_SOURCE_COMMIT,
    "datatogrid": (
        "casatools/src/code/synthesis/TransformMachines2/AWVisResampler.cc:233-400"
    ),
    "coefficient_loop": (
        "casatools/src/code/synthesis/TransformMachines2/accumulateToGrid.inc:30-52"
    ),
    "phase_generation": (
        "casatools/src/code/synthesis/TransformMachines2/PhaseGrad.cc:141-195"
    ),
}
EXPECTED_LITERAL_ARITHMETIC = {
    "raw_cf": "stored-complex32-pixel-already-cache-normalized",
    "runtime_cf_area_division": False,
    "w_sign": ("strict-data-w-positive-conjugates-zero-and-negative-do-not"),
    "normalization": (
        "post-w-sign-unphased-complex32-and-component-promoted-complex64-sums"
    ),
    "pointing_phase": (
        "double-axis-trig-to-complex32-double-axis-product-to-complex32"
    ),
    "phase_multiply": ("explicit-separately-rounded-complex32-products-add-sub"),
    "grid_nvalue": "complex32-weight-times-residual",
    "grid_contribution": "complex32-nvalue-times-coefficient",
    "grid_accumulator": "componentwise-complex64-add",
}
EXPECTED_ROLE_ORDER = {
    "first": {
        "correlation": "RR",
        "selected_corr_index": 0,
        "selected_corr_code": 5,
        "logical_mueller": 0,
    },
    "second": {
        "correlation": "LL",
        "selected_corr_index": 3,
        "selected_corr_code": 8,
        "logical_mueller": 15,
    },
}
ENVELOPE_KEYS = frozenset({"schema", "content_address", "evidence"})
CONTENT_ADDRESS_KEYS = frozenset({"algorithm", "scope", "digest"})
EVIDENCE_KEYS = frozenset(
    {
        "schema",
        "status",
        "result",
        "result_taxonomy",
        "role",
        "producer",
        "diagnostic_hook_added",
        "normal_execution_behavior_changed",
        "production_science_arithmetic_changed",
        "production_dispatch",
        "formed_image",
        "normalization",
        "fft",
        "products",
        "tt1",
        "terms_evaluated",
        "sumwt",
        "cross_producer_reference",
        "coefficient_reference",
        "casa_source",
        "literal_arithmetic",
        "phase_path",
        "traversal_contract",
        "expected_grid_nxy",
        "target_blocks",
        "diagnostic_terms",
        "request_nterms",
        "replay_block_ordinal",
        "replay_window_ordinal",
        "last_window_in_replay_block",
        "frozen_parent_receipts",
        "frozen_grid_hashes",
        "selection",
        "observed_first_buffer",
        "absolute_main_rows",
        "correlation_mueller_role_order",
        "input_hashes",
        "portable_call",
        "counts",
        "memory",
        "ordered_hashes",
        "coefficient_comparison",
        "conditional_grid",
        "traversal_hash",
    }
)
COUNT_KEYS = frozenset(
    {
        "source",
        "logical_role",
        "tap",
        "tap_request",
        "unique_bundle",
        "nonfinite_operand",
        "out_of_grid_support_attempt",
    }
)
MEMORY_KEYS = frozenset({"literal_operand_bytes", "conditional_grid_bytes"})
ORDERED_HASH_KEYS = frozenset(
    {
        "contracts",
        "destination",
        "selected_cell",
        "raw_cf",
        "post_w_sign",
        "pointing_phase",
        "literal_coefficient",
        "packed_coefficient",
    }
)
COEFFICIENT_COMPARISON_KEYS = frozenset({"mismatch_count", "first_mismatch"})
FIRST_MISMATCH_KEYS = frozenset(
    {
        "source_ordinal",
        "source_sample_index",
        "pointing_group_index",
        "logical_role",
        "tap_bundle",
        "tap_ordinal",
        "iy",
        "ix",
        "grid_y",
        "grid_x",
        "cell",
        "conjugate_for_grid",
        "raw_cf_bits",
        "post_w_sign_bits",
        "pointing_phase_bits",
        "literal_coefficient_bits",
        "packed_coefficient_bits",
    }
)
FIRST_MISMATCH_CELL_KEYS = frozenset(
    {
        "frequency_bits",
        "w_bits",
        "mueller",
        "parallactic_angle_bits",
    }
)
CONDITIONAL_GRID_KEYS = frozenset(
    {
        "allocated",
        "allocation_count",
        "replay_count",
        "grid_hash",
        "matches_frozen_rust",
        "matches_frozen_casa",
        "grid_values_hashed",
        "grid_bytes",
        "nonfinite_grid_value_count",
        "source_count",
        "logical_role_count",
        "tap_count",
    }
)


class ContractError(RuntimeError):
    """Raised when an evidence artifact violates the frozen contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _mapping(value: Any, label: str) -> dict[str, Any]:
    _require(type(value) is dict, f"{label} must be an object")
    return value


def _sequence(value: Any, label: str) -> list[Any]:
    _require(type(value) is list, f"{label} must be an array")
    return value


def _integer(value: Any, label: str, *, positive: bool = False) -> int:
    _require(type(value) is int, f"{label} must be an integer")
    if positive:
        _require(value > 0, f"{label} must be positive")
    return value


def _u64(value: Any, label: str, *, positive: bool = False) -> int:
    word = _integer(value, label, positive=positive)
    _require(0 <= word <= 0xFFFFFFFFFFFFFFFF, f"{label} is not a u64")
    return word


def _exact(observed: Any, expected: Any, label: str) -> None:
    _require(
        arithmetic._json_type_exact_equal(observed, expected),
        f"{label} changed: {observed!r} != {expected!r}",
    )


def _fields(
    observed: dict[str, Any],
    expected: dict[str, Any],
    label: str,
) -> None:
    arithmetic._require_fields(observed, expected, label)


def _require_exact_keys(
    observed: dict[str, Any],
    expected: frozenset[str],
    label: str,
) -> None:
    observed_keys = frozenset(observed)
    missing = sorted(expected - observed_keys)
    unexpected = sorted(observed_keys - expected)
    _require(
        not missing and not unexpected,
        f"{label} key set changed: missing={missing!r} unexpected={unexpected!r}",
    )


def _load_json_strict(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise ContractError(f"read {path}: {error}") from error

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, member in pairs:
            if key in value:
                raise ContractError(f"{path}: duplicate JSON object key {key!r}")
            value[key] = member
        return value

    def reject_nonstandard_constant(value: str) -> None:
        raise ContractError(f"{path}: nonstandard JSON constant {value!r}")

    try:
        parsed = json.loads(
            payload,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_nonstandard_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"parse {path}: {error}") from error
    return payload, _mapping(parsed, str(path))


def _validate_candidate_envelope(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    payload, envelope = _load_json_strict(path)
    _require_exact_keys(envelope, ENVELOPE_KEYS, f"{path}: envelope")
    _exact(envelope.get("schema"), CANDIDATE_ENVELOPE_SCHEMA, f"{path}: schema")
    content_address = _mapping(
        envelope.get("content_address"), f"{path}: content_address"
    )
    _require_exact_keys(
        content_address,
        CONTENT_ADDRESS_KEYS,
        f"{path}: content_address",
    )
    _fields(
        content_address,
        {
            "algorithm": "sha256",
            "scope": "embedded-evidence-json-utf8",
        },
        f"{path}: content_address",
    )
    evidence = _mapping(envelope.get("evidence"), f"{path}: evidence")
    _require_exact_keys(evidence, EVIDENCE_KEYS, f"{path}: evidence")
    _exact(
        evidence.get("schema"),
        CANDIDATE_EVIDENCE_SCHEMA,
        f"{path}: evidence.schema",
    )
    raw_evidence = arithmetic._raw_json_member(payload, "evidence", path)
    observed_digest = hashlib.sha256(raw_evidence).hexdigest()
    _exact(
        content_address.get("digest"),
        observed_digest,
        f"{path}: content_address.digest",
    )
    return envelope, evidence, observed_digest


def sha256_path(path: Path) -> str:
    return arithmetic.sha256_path(path)


def validate_frozen_parents(
    casa_rs_v4_path: Path,
    casa_v5_path: Path,
    arithmetic_v1_path: Path,
    arithmetic_v1_comparison_path: Path,
    *,
    arithmetic_sha256: str = ARITHMETIC_V1_RECEIPT_SHA256,
    arithmetic_evidence_sha256: str = ARITHMETIC_V1_EVIDENCE_SHA256,
) -> None:
    """Validate every frozen parent, including the negative arithmetic row."""

    arithmetic.validate_frozen_parents(casa_rs_v4_path, casa_v5_path)
    _require(
        sha256_path(arithmetic_v1_path) == arithmetic_sha256,
        f"{arithmetic_v1_path}: frozen arithmetic-v1 whole-file SHA-256 changed",
    )
    evidence, _, embedded_sha = arithmetic.validate_candidate(arithmetic_v1_path)
    _require(
        embedded_sha == arithmetic_evidence_sha256,
        f"{arithmetic_v1_path}: frozen arithmetic-v1 embedded digest changed",
    )
    _fields(
        evidence,
        {
            "result": "completed-no-tested-variant-matched-casa",
            "traversal_hash": TRAVERSAL_HASH,
        },
        f"{arithmetic_v1_path}: arithmetic-v1",
    )
    _fields(
        _mapping(evidence.get("counts"), "arithmetic-v1 counts"),
        {
            "source": SOURCE_COUNT,
            "logical_role": ROLE_COUNT,
            "tap": TAP_COUNT,
        },
        f"{arithmetic_v1_path}: arithmetic-v1 counts",
    )
    _require(
        sha256_path(arithmetic_v1_comparison_path) == ARITHMETIC_V1_COMPARISON_SHA256,
        (
            f"{arithmetic_v1_comparison_path}: frozen arithmetic-v1 comparison "
            "whole-file SHA-256 changed"
        ),
    )
    _, comparison_envelope = _load_json_strict(arithmetic_v1_comparison_path)
    _fields(
        comparison_envelope,
        {
            "schema": arithmetic.COMPARISON_ENVELOPE_SCHEMA,
            "content_address": {
                "algorithm": "sha256",
                "scope": "canonical-embedded-comparison-json-utf8",
                "digest": ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256,
            },
        },
        f"{arithmetic_v1_comparison_path}: arithmetic-v1 comparison envelope",
    )
    comparison = _mapping(
        comparison_envelope.get("comparison"),
        f"{arithmetic_v1_comparison_path}: comparison",
    )
    observed_comparison_digest = hashlib.sha256(
        arithmetic._canonical_json(comparison)
    ).hexdigest()
    _require(
        observed_comparison_digest == ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256,
        f"{arithmetic_v1_comparison_path}: embedded comparison digest changed",
    )
    _fields(
        comparison,
        {
            "schema": arithmetic.COMPARISON_SCHEMA,
            "status": "valid-classification",
            "classification": "valid-negative-no-exact-casa-tt0-grid-hash-match",
        },
        f"{arithmetic_v1_comparison_path}: arithmetic-v1 comparison",
    )
    _fields(
        _mapping(
            comparison.get("candidate"),
            f"{arithmetic_v1_comparison_path}: candidate",
        ),
        {
            "sha256": ARITHMETIC_V1_RECEIPT_SHA256,
            "embedded_evidence_sha256": ARITHMETIC_V1_EVIDENCE_SHA256,
            "result": "completed-no-tested-variant-matched-casa",
        },
        f"{arithmetic_v1_comparison_path}: comparison candidate",
    )


def _validate_first_mismatch(
    value: Any,
    mismatch_count: int,
    tap_request_count: int,
) -> None:
    if mismatch_count == 0:
        _require(value is None, "first_mismatch must be null when coefficients match")
        return
    mismatch = _mapping(value, "coefficient_comparison.first_mismatch")
    _require_exact_keys(
        mismatch,
        FIRST_MISMATCH_KEYS,
        "coefficient_comparison.first_mismatch",
    )
    required_integer_fields = [
        "source_ordinal",
        "source_sample_index",
        "pointing_group_index",
        "logical_role",
        "tap_bundle",
        "tap_ordinal",
        "iy",
        "ix",
        "grid_y",
        "grid_x",
    ]
    for field in required_integer_fields:
        _integer(mismatch.get(field), f"first_mismatch.{field}")
    _require(
        0 <= mismatch["source_ordinal"] < SOURCE_COUNT,
        "first_mismatch.source_ordinal is outside the frozen source census",
    )
    _require(
        mismatch["logical_role"] in (0, 1),
        "first_mismatch.logical_role must be 0 or 1",
    )
    for field in (
        "source_sample_index",
        "pointing_group_index",
        "tap_bundle",
        "tap_ordinal",
    ):
        _require(
            mismatch[field] >= 0,
            f"first_mismatch.{field} must be non-negative",
        )
    _require(
        mismatch["tap_bundle"] < tap_request_count,
        ("first_mismatch.tap_bundle exceeds the audited tap-request census"),
    )
    for field in ("grid_x", "grid_y"):
        _require(
            0 <= mismatch[field] < 4096,
            f"first_mismatch.{field} is outside the frozen grid",
        )
    _require(
        type(mismatch.get("conjugate_for_grid")) is bool,
        "first_mismatch.conjugate_for_grid must be boolean",
    )
    cell = _mapping(mismatch.get("cell"), "first_mismatch.cell")
    _require_exact_keys(
        cell,
        FIRST_MISMATCH_CELL_KEYS,
        "first_mismatch.cell",
    )
    for field in (
        "frequency_bits",
        "w_bits",
        "mueller",
        "parallactic_angle_bits",
    ):
        word = _integer(cell.get(field), f"first_mismatch.cell.{field}")
        _require(
            0 <= word <= 0xFFFFFFFFFFFFFFFF,
            f"first_mismatch.cell.{field} is not a u64",
        )
    _require(
        cell["mueller"] in (0, 15),
        "first_mismatch.cell.mueller is outside the frozen RR/LL roles",
    )
    for field in (
        "raw_cf_bits",
        "post_w_sign_bits",
        "pointing_phase_bits",
        "literal_coefficient_bits",
        "packed_coefficient_bits",
    ):
        bits = _sequence(mismatch.get(field), f"first_mismatch.{field}")
        _require(len(bits) == 2, f"first_mismatch.{field} must contain two words")
        for ordinal, word in enumerate(bits):
            value = _integer(word, f"first_mismatch.{field}[{ordinal}]")
            _require(
                0 <= value <= 0xFFFFFFFF,
                f"first_mismatch.{field}[{ordinal}] is not a u32",
            )
    _require(
        mismatch["literal_coefficient_bits"] != mismatch["packed_coefficient_bits"],
        "first_mismatch literal and packed coefficient bits must differ",
    )


def _validate_conditional_grid(
    value: Any,
    *,
    mismatch_count: int,
    source_count: int,
    role_count: int,
    tap_count: int,
    declared_bytes: int,
) -> tuple[str, int | None]:
    if mismatch_count == 0:
        _require(value is None, "conditional_grid must be null when coefficients match")
        _require(
            declared_bytes == 0,
            "conditional_grid_bytes must be zero when coefficients match",
        )
        return "completed-literal-packed-exact-no-grid", None

    grid = _mapping(value, "conditional_grid")
    _require_exact_keys(grid, CONDITIONAL_GRID_KEYS, "conditional_grid")
    _fields(
        grid,
        {
            "allocated": True,
            "allocation_count": 1,
            "replay_count": 1,
            "grid_values_hashed": GRID_VALUES,
            "grid_bytes": GRID_BYTES,
            "nonfinite_grid_value_count": 0,
            "source_count": source_count,
            "logical_role_count": role_count,
            "tap_count": tap_count,
        },
        "conditional_grid",
    )
    _require(
        declared_bytes == GRID_BYTES,
        "conditional_grid_bytes must equal one 4096-square complex64 grid",
    )
    grid_hash = _u64(grid.get("grid_hash"), "conditional_grid.grid_hash")
    matches_rust = grid_hash == CASA_RS_V4_TT0_GRID_HASH
    matches_casa = grid_hash == CASA_TT0_GRID_HASH
    _exact(
        grid.get("matches_frozen_rust"),
        matches_rust,
        "conditional_grid.matches_frozen_rust",
    )
    _exact(
        grid.get("matches_frozen_casa"),
        matches_casa,
        "conditional_grid.matches_frozen_casa",
    )
    if matches_casa:
        return "completed-literal-packed-mismatch-grid-matches-casa", grid_hash
    if matches_rust:
        return "completed-literal-packed-mismatch-grid-matches-rust", grid_hash
    return "completed-literal-packed-mismatch-grid-matches-neither", grid_hash


def validate_candidate(path: Path) -> tuple[dict[str, Any], str, str]:
    """Validate one content-addressed literal-coefficient receipt."""

    _, evidence, evidence_sha = _validate_candidate_envelope(path)
    _fields(
        evidence,
        {
            "status": "completed-controlled-stop",
            "result_taxonomy": RESULT_TAXONOMY,
            "role": "bounded-correctness-oracle-not-performance-evidence",
            "producer": "casa-rs",
            "diagnostic_hook_added": True,
            "normal_execution_behavior_changed": False,
            "production_science_arithmetic_changed": False,
            "production_dispatch": "not-entered",
            "formed_image": False,
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
            "tt1": False,
            "terms_evaluated": [0],
            "sumwt": "not-controlled",
            "cross_producer_reference": (
                "conditional-whole-TT0-grid-hash-only-from-frozen-CASA-v5"
            ),
            "coefficient_reference": (
                "source-exact-casa-6.7.5.18-replay-over-frozen-casa-rs-CF-pixels"
            ),
            "casa_source": EXPECTED_CASA_SOURCE,
            "literal_arithmetic": EXPECTED_LITERAL_ARITHMETIC,
            "phase_path": (
                "prephased-direct-projector-no-phase-table-no-tapless-replay"
            ),
            "traversal_contract": TRAVERSAL_CONTRACT,
            "expected_grid_nxy": 4096,
            "target_blocks": 1,
            "diagnostic_terms": 1,
            "request_nterms": 2,
            "replay_block_ordinal": 0,
            "replay_window_ordinal": 0,
            "last_window_in_replay_block": True,
            "frozen_parent_receipts": {
                "casa_rs_v4_sha256": CASA_RS_V4_RECEIPT_SHA256,
                "casa_rs_v4_embedded_evidence_sha256": CASA_RS_V4_EVIDENCE_SHA256,
                "casa_rs_v4_revision": CASA_RS_V4_REVISION,
                "casa_v5_sha256": CASA_V5_RECEIPT_SHA256,
                "arithmetic_v1_sha256": ARITHMETIC_V1_RECEIPT_SHA256,
                "arithmetic_v1_embedded_evidence_sha256": (
                    ARITHMETIC_V1_EVIDENCE_SHA256
                ),
                "arithmetic_v1_comparison_sha256": ARITHMETIC_V1_COMPARISON_SHA256,
                "arithmetic_v1_comparison_embedded_evidence_sha256": (
                    ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256
                ),
                "arithmetic_v1_revision": ARITHMETIC_V1_REVISION,
            },
            "frozen_grid_hashes": {
                "rust_tt0": CASA_RS_V4_TT0_GRID_HASH,
                "casa_tt0": CASA_TT0_GRID_HASH,
            },
            "selection": arithmetic.EXPECTED_SELECTION,
            "observed_first_buffer": arithmetic.EXPECTED_FIRST_BUFFER,
            "absolute_main_rows": arithmetic.EXPECTED_ABSOLUTE_ROWS,
            "correlation_mueller_role_order": EXPECTED_ROLE_ORDER,
            "input_hashes": {
                "direct_raw": arithmetic.DIRECT_COMPACT_INPUT_HASH,
                "compact": arithmetic.DIRECT_COMPACT_INPUT_HASH,
                "direct_compact_exact_match": True,
                "portable_geometry": arithmetic.PORTABLE_GEOMETRY_HASH,
                "portable_input": arithmetic.PORTABLE_INPUT_HASH,
            },
            "portable_call": {
                "call": 0,
                "block": 0,
                "term": 0,
                "source_count": SOURCE_COUNT,
            },
            "traversal_hash": TRAVERSAL_HASH,
        },
        f"{path}: candidate",
    )

    counts = _mapping(evidence.get("counts"), "candidate counts")
    _require_exact_keys(counts, COUNT_KEYS, "candidate counts")
    _fields(
        counts,
        {
            "source": SOURCE_COUNT,
            "logical_role": ROLE_COUNT,
            "tap": TAP_COUNT,
            "nonfinite_operand": 0,
            "out_of_grid_support_attempt": 0,
        },
        "candidate counts",
    )
    unique_bundle_count = _integer(
        counts.get("unique_bundle"), "candidate counts.unique_bundle", positive=True
    )
    tap_request_count = _integer(
        counts.get("tap_request"),
        "candidate counts.tap_request",
        positive=True,
    )
    _require(
        unique_bundle_count <= tap_request_count,
        "candidate unique_bundle count exceeds the tap-request census",
    )
    _require(
        unique_bundle_count <= ROLE_COUNT,
        "candidate unique_bundle count exceeds the logical-role census",
    )
    memory = _mapping(evidence.get("memory"), "candidate memory")
    _require_exact_keys(memory, MEMORY_KEYS, "candidate memory")
    literal_operand_bytes = _integer(
        memory.get("literal_operand_bytes"),
        "candidate memory.literal_operand_bytes",
        positive=True,
    )
    _require(
        literal_operand_bytes >= unique_bundle_count,
        "literal operand bytes are smaller than the unique-bundle census",
    )
    conditional_grid_bytes = _integer(
        memory.get("conditional_grid_bytes"),
        "candidate memory.conditional_grid_bytes",
    )

    hashes = _mapping(evidence.get("ordered_hashes"), "candidate ordered_hashes")
    _require_exact_keys(hashes, ORDERED_HASH_KEYS, "candidate ordered_hashes")
    _exact(
        hashes.get("contracts"),
        ORDERED_HASH_CONTRACTS,
        "ordered_hashes.contracts",
    )
    for field in (
        "destination",
        "selected_cell",
        "raw_cf",
        "post_w_sign",
        "pointing_phase",
        "literal_coefficient",
        "packed_coefficient",
    ):
        _u64(hashes.get(field), f"ordered_hashes.{field}", positive=True)

    coefficient = _mapping(
        evidence.get("coefficient_comparison"), "candidate coefficient_comparison"
    )
    _require_exact_keys(
        coefficient,
        COEFFICIENT_COMPARISON_KEYS,
        "candidate coefficient_comparison",
    )
    mismatch_count = _integer(
        coefficient.get("mismatch_count"),
        "candidate coefficient_comparison.mismatch_count",
    )
    _require(
        0 <= mismatch_count <= TAP_COUNT,
        "candidate coefficient mismatch count is outside the tap census",
    )
    hashes_equal = hashes["literal_coefficient"] == hashes["packed_coefficient"]
    _require(
        hashes_equal == (mismatch_count == 0),
        "literal/packed ordered hashes disagree with mismatch_count",
    )
    _validate_first_mismatch(
        coefficient.get("first_mismatch"),
        mismatch_count,
        tap_request_count,
    )
    expected_result, _ = _validate_conditional_grid(
        evidence.get("conditional_grid"),
        mismatch_count=mismatch_count,
        source_count=counts["source"],
        role_count=counts["logical_role"],
        tap_count=counts["tap"],
        declared_bytes=conditional_grid_bytes,
    )
    _exact(evidence.get("result"), expected_result, "candidate result")
    return evidence, sha256_path(path), evidence_sha


def classify_candidate(evidence: dict[str, Any]) -> str:
    result = evidence.get("result")
    _require(result in CLASSIFICATIONS, f"unsupported completed result: {result!r}")
    return CLASSIFICATIONS[result]


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def build_comparison(
    *,
    candidate_path: Path,
    casa_rs_v4_path: Path,
    casa_v5_path: Path,
    arithmetic_v1_path: Path,
    arithmetic_v1_comparison_path: Path,
) -> dict[str, Any]:
    validate_frozen_parents(
        casa_rs_v4_path,
        casa_v5_path,
        arithmetic_v1_path,
        arithmetic_v1_comparison_path,
    )
    candidate, candidate_sha, candidate_evidence_sha = validate_candidate(
        candidate_path
    )
    comparison = {
        "schema": COMPARISON_SCHEMA,
        "status": "valid-classification",
        "classification": classify_candidate(candidate),
        "disposition": DISPOSITIONS[candidate["result"]],
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "scope": {
            "dataset": "VLASS frozen fragment",
            "field_id": 1525,
            "spw_id": 2,
            "selection_relative_rows": [0, 325],
            "term": 0,
            "grid_shape": [4096, 4096, 1, 1],
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
            "sumwt": "not-controlled",
        },
        "parents": {
            "casa_rs_v4": {
                "path": str(casa_rs_v4_path),
                "sha256": CASA_RS_V4_RECEIPT_SHA256,
                "embedded_evidence_sha256": CASA_RS_V4_EVIDENCE_SHA256,
                "revision": CASA_RS_V4_REVISION,
            },
            "casa_v5": {
                "path": str(casa_v5_path),
                "sha256": CASA_V5_RECEIPT_SHA256,
                "source_commit": CASA_SOURCE_COMMIT,
            },
            "arithmetic_v1": {
                "path": str(arithmetic_v1_path),
                "sha256": ARITHMETIC_V1_RECEIPT_SHA256,
                "embedded_evidence_sha256": ARITHMETIC_V1_EVIDENCE_SHA256,
                "comparison_path": str(arithmetic_v1_comparison_path),
                "comparison_sha256": ARITHMETIC_V1_COMPARISON_SHA256,
                "comparison_embedded_evidence_sha256": (
                    ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256
                ),
                "revision": ARITHMETIC_V1_REVISION,
            },
        },
        "candidate": {
            "path": str(candidate_path),
            "sha256": candidate_sha,
            "embedded_evidence_sha256": candidate_evidence_sha,
            "result": candidate["result"],
            "coefficient_mismatch_count": candidate["coefficient_comparison"][
                "mismatch_count"
            ],
            "conditional_grid": candidate["conditional_grid"],
        },
        "claims": {
            "valid_structural_classification": True,
            "valid_negative_boundary_excluded": (
                candidate["result"] == "completed-literal-packed-exact-no-grid"
            ),
            "valid_negative_reproduces_frozen_rust": (
                candidate["result"]
                == "completed-literal-packed-mismatch-grid-matches-rust"
            ),
            "candidate_localization_matches_frozen_casa": (
                candidate["result"]
                == "completed-literal-packed-mismatch-grid-matches-casa"
            ),
            "unanchored_hard_stop": (
                candidate["result"]
                == "completed-literal-packed-mismatch-grid-matches-neither"
            ),
            "ordered_coefficient_hashes_cross_producer": False,
            "source_exact_casa_arithmetic_over_casa_rs_cf_pixels": True,
            "whole_grid_hash_is_only_cross_producer_comparison": True,
            "literal_and_packed_coefficients_identical": (
                candidate["coefficient_comparison"]["mismatch_count"] == 0
            ),
            "conditional_grid_allocated": candidate["conditional_grid"] is not None,
            "diagnostic_hook_added": True,
            "normal_execution_behavior_changed": False,
            "production_science_arithmetic_changed": False,
            "production_tt0_promoted": False,
            "tt1_promoted": False,
            "integrated_4096_row_promoted": False,
            "production_promotion_authorized": False,
            "performance_evidence": False,
        },
    }
    digest = hashlib.sha256(_canonical_json(comparison)).hexdigest()
    return {
        "schema": COMPARISON_ENVELOPE_SCHEMA,
        "content_address": {
            "algorithm": "sha256",
            "scope": "canonical-embedded-comparison-json-utf8",
            "digest": digest,
        },
        "comparison": comparison,
    }


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    _require(path.is_absolute(), f"comparison output must be absolute: {path}")
    _require(not path.exists(), f"refusing to overwrite comparison: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = json.dumps(value, indent=2, sort_keys=True).encode() + b"\n"
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise ContractError(f"refusing to overwrite comparison: {path}") from error
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-rs-v4", required=True, type=Path)
    parser.add_argument("--casa-v5", required=True, type=Path)
    parser.add_argument("--arithmetic-v1", required=True, type=Path)
    parser.add_argument(
        "--arithmetic-v1-comparison",
        required=True,
        type=Path,
    )
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--validate-parents-only", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.validate_parents_only:
            _require(
                arguments.candidate is None and arguments.output is None,
                "--validate-parents-only cannot be combined with candidate/output",
            )
            validate_frozen_parents(
                arguments.casa_rs_v4,
                arguments.casa_v5,
                arguments.arithmetic_v1,
                arguments.arithmetic_v1_comparison,
            )
            print(
                json.dumps(
                    {
                        "status": "valid-frozen-parents",
                        "casa_rs_v4_sha256": CASA_RS_V4_RECEIPT_SHA256,
                        "casa_v5_sha256": CASA_V5_RECEIPT_SHA256,
                        "arithmetic_v1_sha256": ARITHMETIC_V1_RECEIPT_SHA256,
                    },
                    sort_keys=True,
                )
            )
            return 0
        _require(arguments.candidate is not None, "--candidate is required")
        _require(arguments.output is not None, "--output is required")
        comparison = build_comparison(
            candidate_path=arguments.candidate,
            casa_rs_v4_path=arguments.casa_rs_v4,
            casa_v5_path=arguments.casa_v5,
            arithmetic_v1_path=arguments.arithmetic_v1,
            arithmetic_v1_comparison_path=arguments.arithmetic_v1_comparison,
        )
        atomic_write_json(arguments.output, comparison)
        print(json.dumps(comparison["comparison"], sort_keys=True))
        return 0
    except (ContractError, arithmetic.ContractError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
