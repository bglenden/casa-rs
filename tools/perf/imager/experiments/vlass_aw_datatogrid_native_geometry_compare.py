#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate the bounded VLASS native DataToGrid stream/geometry audit.

The candidate evaluates both values of CASA's private
``use_conjugate_frequency_cf`` dispatch flag.  This independent validator binds
the candidate to the frozen CASA-v5 call hashes and classifies the deepest
common prefix reached by one complete TT0/TT1 hypothesis.  V2 explicitly tests
negating U/V before CASA's ``girarUVW`` under a same-field identity assumption;
it does not claim to replay CASA's actual ``girarUVW``/refocus bits.

Exit status zero means that the evidence was validly classified.  It does not
promote CF selection, placement, tap processing, gridding, imaging products, or
performance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import vlass_aw_datagrid_literal_coefficient_compare as literal
import vlass_aw_datagrid_tt0_arithmetic_compat_compare as arithmetic


CASA_RS_V4_RECEIPT_SHA256 = arithmetic.CASA_RS_V4_RECEIPT_SHA256
CASA_RS_V4_EVIDENCE_SHA256 = arithmetic.CASA_RS_V4_EVIDENCE_SHA256
CASA_RS_V4_REVISION = arithmetic.CASA_RS_V4_REVISION
CASA_V5_RECEIPT_SHA256 = arithmetic.CASA_V5_RECEIPT_SHA256
CASA_SOURCE_COMMIT = arithmetic.CASA_SOURCE_COMMIT

ARITHMETIC_V1_RECEIPT_SHA256 = literal.ARITHMETIC_V1_RECEIPT_SHA256
ARITHMETIC_V1_EVIDENCE_SHA256 = literal.ARITHMETIC_V1_EVIDENCE_SHA256
ARITHMETIC_V1_COMPARISON_SHA256 = literal.ARITHMETIC_V1_COMPARISON_SHA256
ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256 = (
    literal.ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256
)
ARITHMETIC_V1_REVISION = literal.ARITHMETIC_V1_REVISION

LITERAL_V1_RECEIPT_SHA256 = (
    "1dbed734c0f9dc3966038302c673c2ddeae7c812e4507408b4c56647db3d3c2c"
)
LITERAL_V1_EVIDENCE_SHA256 = (
    "dfb6f809c9e96b6007321b293ee147b961a39bfc65968c21637f65058afc8d38"
)
LITERAL_V1_COMPARISON_SHA256 = (
    "7d5567e9f9d570536dd3406b5a7d8a59a330b29321fd5142b319dbf70d65908d"
)
LITERAL_V1_COMPARISON_EVIDENCE_SHA256 = (
    "a8b10b7e5fd73185762a4e55e2883e1422c96a320718e00d26c08bf0a05bdd8c"
)
LITERAL_V1_REVISION = "9604e540fb90482774eab20f858ec0930e556a53"

NATIVE_GEOMETRY_V1_RECEIPT_SHA256 = (
    "9336cc0c0cf96a8efd9c5be5e3225b3beb333a52cb7c8bc19fc29df7bb0866e2"
)
NATIVE_GEOMETRY_V1_EVIDENCE_SHA256 = (
    "5585ced995d75446aee8df3a50a1ea1153d5c0247ad10ab28ba573f817bee148"
)
NATIVE_GEOMETRY_V1_COMPARISON_SHA256 = (
    "86112da1456ea4ad3bb5b9b4da345f4e6dc5bcbac357c19ff720cd17da62f87d"
)
NATIVE_GEOMETRY_V1_COMPARISON_EVIDENCE_SHA256 = (
    "4b1bf691ae33362f986d863a8c386f42473ae5ffb42ec89452bef90fb0a78944"
)
NATIVE_GEOMETRY_V1_PROVENANCE_SHA256 = (
    "19fc0ee9d84caae9f630f6867d3bb9250cc24c8a12e492a9cc600c4d8780be00"
)
NATIVE_GEOMETRY_V1_REVISION = "f9079065689910f718884fb80a43b22b320b1bf6"
NATIVE_GEOMETRY_V1_ENVELOPE_SCHEMA = (
    "casa-rs-aw-datatogrid-native-geometry-audit-envelope-v1"
)
NATIVE_GEOMETRY_V1_EVIDENCE_SCHEMA = "casa-rs-aw-datatogrid-native-geometry-audit-v1"
NATIVE_GEOMETRY_V1_COMPARISON_ENVELOPE_SCHEMA = (
    "casa-rs-aw-datatogrid-native-geometry-audit-comparison-envelope-v1"
)
NATIVE_GEOMETRY_V1_COMPARISON_SCHEMA = (
    "casa-rs-aw-datatogrid-native-geometry-audit-comparison-v1"
)
NATIVE_GEOMETRY_V1_PROVENANCE_SCHEMA = (
    "casa-rs-vlass-aw-datagrid-native-geometry-provenance-v1"
)

SOURCE_COUNT = arithmetic.SOURCE_COUNT
IM_REF_FREQ_BITS = 4_748_556_467_228_999_524
EXPECTED_GRID_SHAPE = [4096, 4096, 1, 1]
EXPECTED_CASA_CALLS = [
    {
        "call": 0,
        "block": 0,
        "term": 0,
        "source_count": SOURCE_COUNT,
        "stream_hash": 4_740_440_223_154_359_747,
        "geometry_hash": 15_079_793_846_523_608_377,
    },
    {
        "call": 1,
        "block": 0,
        "term": 1,
        "source_count": SOURCE_COUNT,
        "stream_hash": 4_740_440_223_154_359_747,
        "geometry_hash": 14_381_099_959_812_707_833,
    },
]

CANDIDATE_ENVELOPE_SCHEMA = "casa-rs-aw-datatogrid-native-geometry-audit-envelope-v2"
CANDIDATE_EVIDENCE_SCHEMA = "casa-rs-aw-datatogrid-native-geometry-audit-v2"
COMPARISON_ENVELOPE_SCHEMA = (
    "casa-rs-aw-datatogrid-native-geometry-audit-comparison-envelope-v2"
)
COMPARISON_SCHEMA = "casa-rs-aw-datatogrid-native-geometry-audit-comparison-v2"

RESULT_TAXONOMY = [
    "completed-source-count-mismatch",
    "completed-native-stream-mismatch",
    "completed-native-stream-exact-geometry-mismatch",
    "completed-native-stream-and-geometry-exact",
]
CLASSIFICATIONS = {
    "completed-source-count-mismatch": "localized-uvw-hypothesis-source-count-mismatch",
    "completed-native-stream-mismatch": "localized-uvw-hypothesis-call-stream-mismatch",
    "completed-native-stream-exact-geometry-mismatch": (
        "localized-uvw-hypothesis-source-geometry-mismatch"
    ),
    "completed-native-stream-and-geometry-exact": (
        "exact-uvw-hypothesis-stream-and-geometry-boundary-match"
    ),
}
DISPOSITIONS = {
    "completed-source-count-mismatch": (
        "stop-inspect-source-admission-before-native-stream-comparison"
    ),
    "completed-native-stream-mismatch": (
        "stop-resolve-actual-casa-girar-refocus-uvw-dphase-bits-or-flags-"
        "no-production-defect-identified"
    ),
    "completed-native-stream-exact-geometry-mismatch": (
        "stop-inspect-hypothesis-positive-weight-source-order-channel-and-flag-"
        "geometry-without-identifying-production-defect"
    ),
    "completed-native-stream-and-geometry-exact": (
        "advance-to-cf-selection-placement-and-tap-localization-without-casa-"
        "girar-refocus-equivalence-or-promotion"
    ),
}

UVW_HYPOTHESIS = "casa-awproject-negate-uv-before-girar-assumed-same-field-identity"
PHASE_HYPOTHESIS = (
    "casa-rs-current-same-field-phase-shift-m-retained-not-casa-girar-refocus-"
    "bit-replay"
)
HASH_REFERENCE = (
    "casa-6.7.5.18-AWVisResampler-hash_call_inputs-stream-and-geometry-"
    "negate-uv-before-girar-same-field-identity-hypothesis"
)
HASH_CONTRACTS = {
    "algorithm": "fnv1a64",
    "serialization": (
        "bool-one-byte-u64-little-endian-f32-native-bits-little-endian-"
        "f64-native-bits-little-endian"
    ),
    "uvw_hypothesis": UVW_HYPOTHESIS,
    "phase_hypothesis": PHASE_HYPOTHESIS,
    "stream": (
        "header-then-row-index-row-flag-then-unflagged-negate-uv-before-girar-"
        "assumed-same-field-identity-uvw-m-casa-rs-current-phase-shift-m-then-"
        "valid-target-channel-index-frequency-and-all-pol-flags"
    ),
    "geometry": (
        "call-block-term-then-header-then-row-index-row-flag-then-unflagged-"
        "negate-uv-before-girar-assumed-same-field-identity-uvw-m-casa-rs-"
        "current-phase-shift-m-then-positive-weight-source-ordinal-channel-"
        "frequency-and-all-pol-flags"
    ),
}

FROZEN_PARENT_RECEIPTS = {
    "casa_rs_v4_sha256": CASA_RS_V4_RECEIPT_SHA256,
    "casa_rs_v4_embedded_evidence_sha256": CASA_RS_V4_EVIDENCE_SHA256,
    "casa_rs_v4_revision": CASA_RS_V4_REVISION,
    "casa_v5_sha256": CASA_V5_RECEIPT_SHA256,
    "casa_source_commit": CASA_SOURCE_COMMIT,
    "arithmetic_v1_sha256": ARITHMETIC_V1_RECEIPT_SHA256,
    "arithmetic_v1_embedded_evidence_sha256": ARITHMETIC_V1_EVIDENCE_SHA256,
    "arithmetic_v1_comparison_sha256": ARITHMETIC_V1_COMPARISON_SHA256,
    "arithmetic_v1_comparison_embedded_evidence_sha256": (
        ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256
    ),
    "arithmetic_v1_revision": ARITHMETIC_V1_REVISION,
    "literal_v1_sha256": LITERAL_V1_RECEIPT_SHA256,
    "literal_v1_embedded_evidence_sha256": LITERAL_V1_EVIDENCE_SHA256,
    "literal_v1_comparison_sha256": LITERAL_V1_COMPARISON_SHA256,
    "literal_v1_comparison_embedded_evidence_sha256": (
        LITERAL_V1_COMPARISON_EVIDENCE_SHA256
    ),
    "literal_v1_revision": LITERAL_V1_REVISION,
    "native_geometry_v1_sha256": NATIVE_GEOMETRY_V1_RECEIPT_SHA256,
    "native_geometry_v1_embedded_evidence_sha256": (NATIVE_GEOMETRY_V1_EVIDENCE_SHA256),
    "native_geometry_v1_comparison_sha256": (NATIVE_GEOMETRY_V1_COMPARISON_SHA256),
    "native_geometry_v1_comparison_embedded_evidence_sha256": (
        NATIVE_GEOMETRY_V1_COMPARISON_EVIDENCE_SHA256
    ),
    "native_geometry_v1_provenance_sha256": (NATIVE_GEOMETRY_V1_PROVENANCE_SHA256),
    "native_geometry_v1_revision": NATIVE_GEOMETRY_V1_REVISION,
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
        "cf_selection",
        "placement",
        "tap_processing",
        "grid_dispatch",
        "sumwt",
        "formed_image",
        "normalization",
        "fft",
        "products",
        "terms_evaluated",
        "hash_reference",
        "hash_contracts",
        "uvw_hypothesis",
        "expected_grid_shape",
        "target_blocks",
        "request_nterms",
        "selection",
        "observed_first_buffer",
        "absolute_main_rows",
        "im_ref_freq_bits",
        "frozen_parent_receipts",
        "hypotheses",
    }
)
HYPOTHESIS_KEYS = frozenset({"use_conjugate_frequency_cf", "calls"})
CALL_KEYS = frozenset(
    {
        "call",
        "block",
        "term",
        "source_count",
        "stream_hash",
        "geometry_hash",
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


def _integer(value: Any, label: str) -> int:
    _require(type(value) is int, f"{label} must be an integer")
    return value


def _u64(value: Any, label: str) -> int:
    word = _integer(value, label)
    _require(0 <= word <= 0xFFFFFFFFFFFFFFFF, f"{label} is not a u64")
    return word


def _exact(observed: Any, expected: Any, label: str) -> None:
    _require(
        arithmetic._json_type_exact_equal(observed, expected),
        f"{label} changed: {observed!r} != {expected!r}",
    )


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


def sha256_path(path: Path) -> str:
    return arithmetic.sha256_path(path)


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
    _exact(content_address.get("algorithm"), "sha256", "content_address.algorithm")
    _exact(
        content_address.get("scope"),
        "embedded-evidence-json-utf8",
        "content_address.scope",
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


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _load_tsv_strict(path: Path) -> dict[str, str]:
    try:
        payload = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ContractError(f"read {path}: {error}") from error
    fields: dict[str, str] = {}
    for ordinal, line in enumerate(payload.splitlines(), start=1):
        _require(
            line.count("\t") == 1,
            f"{path}: line {ordinal} must contain exactly one tab",
        )
        key, value = line.split("\t")
        _require(key not in fields, f"{path}: duplicate TSV key {key!r}")
        fields[key] = value
    return fields


def _validate_native_geometry_v1(
    receipt_path: Path,
    comparison_path: Path,
    provenance_path: Path,
) -> None:
    _require(
        sha256_path(receipt_path) == NATIVE_GEOMETRY_V1_RECEIPT_SHA256,
        f"{receipt_path}: frozen native-geometry-v1 whole-file SHA-256 changed",
    )
    receipt_payload, receipt = _load_json_strict(receipt_path)
    _exact(
        receipt.get("schema"),
        NATIVE_GEOMETRY_V1_ENVELOPE_SCHEMA,
        f"{receipt_path}: schema",
    )
    receipt_address = _mapping(
        receipt.get("content_address"),
        f"{receipt_path}: content_address",
    )
    _exact(
        receipt_address,
        {
            "algorithm": "sha256",
            "scope": "embedded-evidence-json-utf8",
            "digest": NATIVE_GEOMETRY_V1_EVIDENCE_SHA256,
        },
        f"{receipt_path}: content_address",
    )
    receipt_evidence = _mapping(receipt.get("evidence"), f"{receipt_path}: evidence")
    arithmetic._require_fields(
        receipt_evidence,
        {
            "schema": NATIVE_GEOMETRY_V1_EVIDENCE_SCHEMA,
            "status": "completed-controlled-stop",
            "result": "completed-native-stream-mismatch",
        },
        f"{receipt_path}: evidence",
    )
    _exact(
        hashlib.sha256(
            arithmetic._raw_json_member(receipt_payload, "evidence", receipt_path)
        ).hexdigest(),
        NATIVE_GEOMETRY_V1_EVIDENCE_SHA256,
        f"{receipt_path}: embedded evidence digest",
    )

    _require(
        sha256_path(comparison_path) == NATIVE_GEOMETRY_V1_COMPARISON_SHA256,
        f"{comparison_path}: frozen native-geometry-v1 comparison SHA-256 changed",
    )
    _, comparison_envelope = _load_json_strict(comparison_path)
    _exact(
        comparison_envelope.get("schema"),
        NATIVE_GEOMETRY_V1_COMPARISON_ENVELOPE_SCHEMA,
        f"{comparison_path}: schema",
    )
    comparison_address = _mapping(
        comparison_envelope.get("content_address"),
        f"{comparison_path}: content_address",
    )
    _exact(
        comparison_address,
        {
            "algorithm": "sha256",
            "scope": "canonical-embedded-comparison-json-utf8",
            "digest": NATIVE_GEOMETRY_V1_COMPARISON_EVIDENCE_SHA256,
        },
        f"{comparison_path}: content_address",
    )
    comparison = _mapping(
        comparison_envelope.get("comparison"),
        f"{comparison_path}: comparison",
    )
    _exact(
        hashlib.sha256(_canonical_json(comparison)).hexdigest(),
        NATIVE_GEOMETRY_V1_COMPARISON_EVIDENCE_SHA256,
        f"{comparison_path}: embedded comparison digest",
    )
    arithmetic._require_fields(
        comparison,
        {
            "schema": NATIVE_GEOMETRY_V1_COMPARISON_SCHEMA,
            "status": "valid-classification",
            "classification": "localized-native-call-stream-mismatch",
        },
        f"{comparison_path}: comparison",
    )
    arithmetic._require_fields(
        _mapping(comparison.get("candidate"), f"{comparison_path}: candidate"),
        {
            "sha256": NATIVE_GEOMETRY_V1_RECEIPT_SHA256,
            "embedded_evidence_sha256": NATIVE_GEOMETRY_V1_EVIDENCE_SHA256,
            "result": "completed-native-stream-mismatch",
        },
        f"{comparison_path}: candidate",
    )

    _require(
        sha256_path(provenance_path) == NATIVE_GEOMETRY_V1_PROVENANCE_SHA256,
        f"{provenance_path}: frozen native-geometry-v1 provenance SHA-256 changed",
    )
    provenance = _load_tsv_strict(provenance_path)
    for key, expected in {
        "schema": NATIVE_GEOMETRY_V1_PROVENANCE_SCHEMA,
        "revision": NATIVE_GEOMETRY_V1_REVISION,
        "receipt_sha256": NATIVE_GEOMETRY_V1_RECEIPT_SHA256,
        "comparison_exit": "0",
        "comparison_sha256": NATIVE_GEOMETRY_V1_COMPARISON_SHA256,
        "classification": "localized-native-call-stream-mismatch",
        "result": "validly-classified-native-geometry-evidence",
    }.items():
        _exact(provenance.get(key), expected, f"{provenance_path}: {key}")


def _validate_literal_comparison(path: Path) -> None:
    _require(
        sha256_path(path) == LITERAL_V1_COMPARISON_SHA256,
        f"{path}: frozen literal-v1 comparison whole-file SHA-256 changed",
    )
    _, envelope = _load_json_strict(path)
    _exact(
        envelope.get("schema"),
        literal.COMPARISON_ENVELOPE_SCHEMA,
        f"{path}: schema",
    )
    content_address = _mapping(
        envelope.get("content_address"), f"{path}: content_address"
    )
    _exact(
        content_address,
        {
            "algorithm": "sha256",
            "scope": "canonical-embedded-comparison-json-utf8",
            "digest": LITERAL_V1_COMPARISON_EVIDENCE_SHA256,
        },
        f"{path}: content_address",
    )
    comparison = _mapping(envelope.get("comparison"), f"{path}: comparison")
    _exact(
        hashlib.sha256(_canonical_json(comparison)).hexdigest(),
        LITERAL_V1_COMPARISON_EVIDENCE_SHA256,
        f"{path}: embedded comparison digest",
    )
    arithmetic._require_fields(
        comparison,
        {
            "schema": literal.COMPARISON_SCHEMA,
            "status": "valid-classification",
            "classification": "valid-negative-literal-coefficient-boundary-excluded",
        },
        f"{path}: comparison",
    )
    arithmetic._require_fields(
        _mapping(comparison.get("candidate"), f"{path}: candidate"),
        {
            "sha256": LITERAL_V1_RECEIPT_SHA256,
            "embedded_evidence_sha256": LITERAL_V1_EVIDENCE_SHA256,
            "result": "completed-literal-packed-exact-no-grid",
        },
        f"{path}: comparison candidate",
    )


def validate_frozen_parents(
    casa_rs_v4_path: Path,
    casa_v5_path: Path,
    arithmetic_v1_path: Path,
    arithmetic_v1_comparison_path: Path,
    literal_v1_path: Path,
    literal_v1_comparison_path: Path,
    native_geometry_v1_path: Path,
    native_geometry_v1_comparison_path: Path,
    native_geometry_v1_provenance_path: Path,
) -> dict[str, Any]:
    """Validate the frozen lineage and return the CASA-v5 receipt."""

    literal.validate_frozen_parents(
        casa_rs_v4_path,
        casa_v5_path,
        arithmetic_v1_path,
        arithmetic_v1_comparison_path,
    )
    _require(
        sha256_path(literal_v1_path) == LITERAL_V1_RECEIPT_SHA256,
        f"{literal_v1_path}: frozen literal-v1 whole-file SHA-256 changed",
    )
    literal_evidence, _, literal_evidence_sha = literal.validate_candidate(
        literal_v1_path
    )
    _exact(
        literal_evidence_sha,
        LITERAL_V1_EVIDENCE_SHA256,
        f"{literal_v1_path}: embedded evidence digest",
    )
    _exact(
        literal_evidence.get("result"),
        "completed-literal-packed-exact-no-grid",
        f"{literal_v1_path}: result",
    )
    _validate_literal_comparison(literal_v1_comparison_path)
    _validate_native_geometry_v1(
        native_geometry_v1_path,
        native_geometry_v1_comparison_path,
        native_geometry_v1_provenance_path,
    )

    _, casa_v5 = _load_json_strict(casa_v5_path)
    calls = _sequence(casa_v5.get("calls"), f"{casa_v5_path}: calls")
    _require(len(calls) == 2, f"{casa_v5_path}: expected two CASA calls")
    for ordinal, expected in enumerate(EXPECTED_CASA_CALLS):
        call = _mapping(calls[ordinal], f"{casa_v5_path}: calls[{ordinal}]")
        arithmetic._require_fields(
            call,
            expected,
            f"{casa_v5_path}: calls[{ordinal}]",
        )
    return casa_v5


def _validate_call(
    value: Any,
    *,
    label: str,
    ordinal: int,
) -> dict[str, int]:
    call = _mapping(value, label)
    _require_exact_keys(call, CALL_KEYS, label)
    expected_identity = {
        "call": ordinal,
        "block": 0,
        "term": ordinal,
    }
    for field, expected in expected_identity.items():
        _exact(call.get(field), expected, f"{label}.{field}")
    for field in CALL_KEYS:
        _u64(call.get(field), f"{label}.{field}")
    return call


def _validate_hypotheses(value: Any) -> list[dict[str, Any]]:
    hypotheses = _sequence(value, "candidate hypotheses")
    _require(len(hypotheses) == 2, "candidate must contain exactly two hypotheses")
    validated: list[dict[str, Any]] = []
    for ordinal, expected_flag in enumerate((False, True)):
        label = f"candidate hypotheses[{ordinal}]"
        hypothesis = _mapping(hypotheses[ordinal], label)
        _require_exact_keys(hypothesis, HYPOTHESIS_KEYS, label)
        _exact(
            hypothesis.get("use_conjugate_frequency_cf"),
            expected_flag,
            f"{label}.use_conjugate_frequency_cf",
        )
        calls = _sequence(hypothesis.get("calls"), f"{label}.calls")
        _require(len(calls) == 2, f"{label}.calls must contain TT0 and TT1")
        validated.append(
            {
                "use_conjugate_frequency_cf": expected_flag,
                "calls": [
                    _validate_call(
                        call,
                        label=f"{label}.calls[{call_ordinal}]",
                        ordinal=call_ordinal,
                    )
                    for call_ordinal, call in enumerate(calls)
                ],
            }
        )
        _require(
            validated[-1]["calls"][0]["source_count"]
            == validated[-1]["calls"][1]["source_count"],
            f"{label} produced different TT0/TT1 source counts",
        )
    for call_ordinal in range(2):
        _require(
            validated[0]["calls"][call_ordinal]["source_count"]
            == validated[1]["calls"][call_ordinal]["source_count"],
            "source_count changed between conjugate-frequency hypotheses",
        )
    return validated


def _hypothesis_matches(
    hypothesis: dict[str, Any],
) -> dict[str, Any]:
    calls = hypothesis["calls"]
    source_exact = all(
        call["source_count"] == expected["source_count"]
        for call, expected in zip(calls, EXPECTED_CASA_CALLS, strict=True)
    )
    stream_exact = source_exact and all(
        call["stream_hash"] == expected["stream_hash"]
        for call, expected in zip(calls, EXPECTED_CASA_CALLS, strict=True)
    )
    geometry_exact = stream_exact and all(
        call["geometry_hash"] == expected["geometry_hash"]
        for call, expected in zip(calls, EXPECTED_CASA_CALLS, strict=True)
    )
    return {
        "use_conjugate_frequency_cf": hypothesis["use_conjugate_frequency_cf"],
        "source_count_exact": source_exact,
        "stream_exact": stream_exact,
        "geometry_exact": geometry_exact,
        "calls": [
            {
                "call": call["call"],
                "term": call["term"],
                "source_count": call["source_count"],
                "source_count_exact": (
                    call["source_count"] == expected["source_count"]
                ),
                "stream_hash": call["stream_hash"],
                "stream_exact": call["stream_hash"] == expected["stream_hash"],
                "geometry_hash": call["geometry_hash"],
                "geometry_exact": (call["geometry_hash"] == expected["geometry_hash"]),
            }
            for call, expected in zip(calls, EXPECTED_CASA_CALLS, strict=True)
        ],
    }


def classify_hypotheses(
    hypotheses: list[dict[str, Any]],
) -> tuple[str, bool | None, list[dict[str, Any]]]:
    """Return the deepest valid prefix and the selected dispatch hypothesis."""

    matches = [_hypothesis_matches(hypothesis) for hypothesis in hypotheses]
    source_matches = [item for item in matches if item["source_count_exact"]]
    if not source_matches:
        return "completed-source-count-mismatch", None, matches
    stream_matches = [item for item in source_matches if item["stream_exact"]]
    if not stream_matches:
        return "completed-native-stream-mismatch", None, matches
    _require(
        len(stream_matches) == 1,
        "multiple conjugate-frequency hypotheses matched the native stream",
    )
    selected = stream_matches[0]
    if not selected["geometry_exact"]:
        return (
            "completed-native-stream-exact-geometry-mismatch",
            selected["use_conjugate_frequency_cf"],
            matches,
        )
    return (
        "completed-native-stream-and-geometry-exact",
        selected["use_conjugate_frequency_cf"],
        matches,
    )


def validate_candidate(
    path: Path,
) -> tuple[dict[str, Any], str, str, bool | None, list[dict[str, Any]]]:
    """Validate one content-addressed native geometry receipt."""

    _, evidence, evidence_sha = _validate_candidate_envelope(path)
    arithmetic._require_fields(
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
            "cf_selection": "not-entered",
            "placement": "not-entered",
            "tap_processing": "not-entered",
            "grid_dispatch": "not-entered",
            "sumwt": "not-entered",
            "formed_image": False,
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
            "terms_evaluated": [0, 1],
            "hash_reference": HASH_REFERENCE,
            "hash_contracts": HASH_CONTRACTS,
            "uvw_hypothesis": UVW_HYPOTHESIS,
            "expected_grid_shape": EXPECTED_GRID_SHAPE,
            "target_blocks": 1,
            "request_nterms": 2,
            "selection": arithmetic.EXPECTED_SELECTION,
            "observed_first_buffer": arithmetic.EXPECTED_FIRST_BUFFER,
            "absolute_main_rows": arithmetic.EXPECTED_ABSOLUTE_ROWS,
            "im_ref_freq_bits": IM_REF_FREQ_BITS,
            "frozen_parent_receipts": FROZEN_PARENT_RECEIPTS,
        },
        f"{path}: candidate",
    )
    hypotheses = _validate_hypotheses(evidence.get("hypotheses"))
    result, selected_flag, matches = classify_hypotheses(hypotheses)
    _exact(evidence.get("result"), result, f"{path}: candidate.result")
    return evidence, sha256_path(path), evidence_sha, selected_flag, matches


def build_comparison(
    *,
    candidate_path: Path,
    casa_rs_v4_path: Path,
    casa_v5_path: Path,
    arithmetic_v1_path: Path,
    arithmetic_v1_comparison_path: Path,
    literal_v1_path: Path,
    literal_v1_comparison_path: Path,
    native_geometry_v1_path: Path,
    native_geometry_v1_comparison_path: Path,
    native_geometry_v1_provenance_path: Path,
) -> dict[str, Any]:
    validate_frozen_parents(
        casa_rs_v4_path,
        casa_v5_path,
        arithmetic_v1_path,
        arithmetic_v1_comparison_path,
        literal_v1_path,
        literal_v1_comparison_path,
        native_geometry_v1_path,
        native_geometry_v1_comparison_path,
        native_geometry_v1_provenance_path,
    )
    candidate, candidate_sha, candidate_evidence_sha, selected_flag, matches = (
        validate_candidate(candidate_path)
    )
    result = candidate["result"]
    source_exact = result != "completed-source-count-mismatch"
    stream_exact = result in {
        "completed-native-stream-exact-geometry-mismatch",
        "completed-native-stream-and-geometry-exact",
    }
    geometry_exact = result == "completed-native-stream-and-geometry-exact"
    comparison = {
        "schema": COMPARISON_SCHEMA,
        "status": "valid-classification",
        "classification": CLASSIFICATIONS[result],
        "disposition": DISPOSITIONS[result],
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "scope": {
            "dataset": "VLASS frozen fragment",
            "field_id": 1525,
            "spw_id": 2,
            "selection_relative_rows": [0, 325],
            "terms": [0, 1],
            "grid_shape": EXPECTED_GRID_SHAPE,
            "uvw_hypothesis": UVW_HYPOTHESIS,
            "phase_hypothesis": PHASE_HYPOTHESIS,
            "grid_dispatch": "not-entered",
            "cf_selection": "not-entered",
            "placement": "not-entered",
            "tap_processing": "not-entered",
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
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
                "calls": EXPECTED_CASA_CALLS,
            },
            "arithmetic_v1": {
                "path": str(arithmetic_v1_path),
                "sha256": ARITHMETIC_V1_RECEIPT_SHA256,
                "comparison_path": str(arithmetic_v1_comparison_path),
                "comparison_sha256": ARITHMETIC_V1_COMPARISON_SHA256,
                "revision": ARITHMETIC_V1_REVISION,
            },
            "literal_v1": {
                "path": str(literal_v1_path),
                "sha256": LITERAL_V1_RECEIPT_SHA256,
                "comparison_path": str(literal_v1_comparison_path),
                "comparison_sha256": LITERAL_V1_COMPARISON_SHA256,
                "revision": LITERAL_V1_REVISION,
            },
            "native_geometry_v1": {
                "path": str(native_geometry_v1_path),
                "sha256": NATIVE_GEOMETRY_V1_RECEIPT_SHA256,
                "embedded_evidence_sha256": NATIVE_GEOMETRY_V1_EVIDENCE_SHA256,
                "comparison_path": str(native_geometry_v1_comparison_path),
                "comparison_sha256": NATIVE_GEOMETRY_V1_COMPARISON_SHA256,
                "comparison_embedded_evidence_sha256": (
                    NATIVE_GEOMETRY_V1_COMPARISON_EVIDENCE_SHA256
                ),
                "provenance_path": str(native_geometry_v1_provenance_path),
                "provenance_sha256": NATIVE_GEOMETRY_V1_PROVENANCE_SHA256,
                "revision": NATIVE_GEOMETRY_V1_REVISION,
            },
        },
        "candidate": {
            "path": str(candidate_path),
            "sha256": candidate_sha,
            "embedded_evidence_sha256": candidate_evidence_sha,
            "result": result,
            "selected_use_conjugate_frequency_cf": selected_flag,
            "hypotheses": matches,
        },
        "claims": {
            "valid_structural_classification": True,
            "source_count_exact": source_exact,
            "hypothesis_stream_exact": stream_exact,
            "hypothesis_geometry_exact": geometry_exact,
            "native_input_payload_exact": False,
            "cf_selection_equivalence_proven": False,
            "placement_equivalence_proven": False,
            "tap_stream_equivalence_proven": False,
            "whole_grid_equivalence_proven": False,
            "actual_casa_girar_refocus_uvw_dphase_and_flags_equivalence_proven": (
                False
            ),
            "production_defect_identified": False,
            "production_path_changed": False,
            "production_promotion_authorized": False,
            "integrated_4096_row_promoted": False,
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
    parser.add_argument("--arithmetic-v1-comparison", required=True, type=Path)
    parser.add_argument("--literal-v1", required=True, type=Path)
    parser.add_argument("--literal-v1-comparison", required=True, type=Path)
    parser.add_argument("--native-geometry-v1", required=True, type=Path)
    parser.add_argument("--native-geometry-v1-comparison", required=True, type=Path)
    parser.add_argument("--native-geometry-v1-provenance", required=True, type=Path)
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
                arguments.literal_v1,
                arguments.literal_v1_comparison,
                arguments.native_geometry_v1,
                arguments.native_geometry_v1_comparison,
                arguments.native_geometry_v1_provenance,
            )
            print(
                json.dumps(
                    {
                        "status": "valid-frozen-parents",
                        "casa_rs_v4_sha256": CASA_RS_V4_RECEIPT_SHA256,
                        "casa_v5_sha256": CASA_V5_RECEIPT_SHA256,
                        "arithmetic_v1_sha256": ARITHMETIC_V1_RECEIPT_SHA256,
                        "literal_v1_sha256": LITERAL_V1_RECEIPT_SHA256,
                        "native_geometry_v1_sha256": (
                            NATIVE_GEOMETRY_V1_RECEIPT_SHA256
                        ),
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
            literal_v1_path=arguments.literal_v1,
            literal_v1_comparison_path=arguments.literal_v1_comparison,
            native_geometry_v1_path=arguments.native_geometry_v1,
            native_geometry_v1_comparison_path=(
                arguments.native_geometry_v1_comparison
            ),
            native_geometry_v1_provenance_path=(
                arguments.native_geometry_v1_provenance
            ),
        )
        atomic_write_json(arguments.output, comparison)
        print(json.dumps(comparison["comparison"], sort_keys=True))
        return 0
    except (
        ContractError,
        arithmetic.ContractError,
        literal.ContractError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
