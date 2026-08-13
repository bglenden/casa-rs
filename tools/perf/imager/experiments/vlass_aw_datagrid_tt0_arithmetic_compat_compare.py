#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate and classify the bounded VLASS TT0 arithmetic experiment.

The comparison is deliberately independent of the imaging runtime. It first
validates the two frozen parent receipts, then validates a new content-addressed
casa-rs receipt. Exit status zero means that the experiment was validly
classified; it does not mean that the compatibility arithmetic matched CASA.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any


CASA_RS_V4_RECEIPT_SHA256 = (
    "1c52961a3058f8f362e9d554c64b69a077f9414a7a44c738bed5351e6df59b40"
)
CASA_RS_V4_EVIDENCE_SHA256 = (
    "5783293d3401f97b12742d8c89bd98e2b0d1303cabf4e19505f245db7cbe9e0a"
)
CASA_RS_V4_REVISION = "11cdeec698b63b9023233f3d7855d6c07d47284f"
CASA_V5_RECEIPT_SHA256 = (
    "fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f"
)
CASA_SOURCE_COMMIT = "418bb1a26df7c4aba663ff123b038b75a6fa0295"

CASA_TT0_GRID_HASH = 9_328_098_071_914_194_885
CASA_TT0_SUMWT_HASH = 5_773_668_711_911_205_477
CASA_RS_V4_TT0_GRID_HASH = 9_898_952_817_250_783_852
CASA_RS_V4_TT0_SUMWT_HASH = 5_891_270_812_598_592_054
CASA_RS_V4_TT0_SUMWT_BITS = 4_693_530_481_614_913_214
GRID_VALUES = 16_777_216
SOURCE_COUNT = 12_359
ROLE_COUNT = 24_718
DIRECT_COMPACT_INPUT_HASH = 7_924_638_447_934_945_938
PORTABLE_GEOMETRY_HASH = 5_254_298_952_548_327_824
PORTABLE_INPUT_HASH = 12_109_501_015_391_774_880

RESULT_TAXONOMY = [
    "rejected-parent-boundary-drift",
    "completed-no-tested-variant-matched-casa",
    "completed-single-tested-variant-matched-casa",
    "completed-multiple-tested-variants-matched-casa",
]
TRAVERSAL_CONTRACT = (
    "source-then-first-rr-logical-mueller-0-then-second-ll-logical-"
    "mueller-15-then-iy-then-ix"
)
GRID_HASH_CONTRACT = "fnv1a64-shape-axis0-fast-complex64-bits"
TOP_LEVEL_GRID_HASH_CONTRACT = "fnv1a64-shape-4096-4096-1-1-axis0-fast-complex64-bits"
PORTABLE_HASH_CONTRACT = (
    "fnv1a64-little-endian-call-block-term-accepted-source-ordinal-"
    "selected-mueller-0-then-15-frequency-uvw-current-weight-basis-"
    "residual-value"
)
CORRELATION_MUELLER_CONTRACT = (
    "first-is-RR-logical-mueller-0-second-is-LL-logical-mueller-15-"
    "selected-CF-mueller-may-w-sign-conjugate"
)
CORRELATION_MUELLER_HASH_CONTRACT = (
    "fnv1a64-little-endian-role-ordinal-selected-correlation-index-"
    "selected-correlation-code-logical-mueller"
)
NONFINITE_GRID_VALUE_CONTRACT = (
    "complex64-cell-with-nonfinite-real-or-imaginary-component-across-all-variant-grids"
)
OUT_OF_GRID_SUPPORT_ATTEMPT_CONTRACT = (
    "planned-support-cell-outside-grid-fails-closed-before-receipt"
)

EXPECTED_CORRELATION_MUELLER_ROLES = [
    {
        "ordinal": 0,
        "role": "first",
        "correlation": "RR",
        "selected_corr_index": 0,
        "selected_corr_code": 5,
        "logical_mueller": 0,
    },
    {
        "ordinal": 1,
        "role": "second",
        "correlation": "LL",
        "selected_corr_index": 3,
        "selected_corr_code": 8,
        "logical_mueller": 15,
    },
]

EXPECTED_VARIANTS = [
    {
        "name": "production_host_f64_baseline",
        "nvalue_contract": "native-complex32-residual-times-real-weight",
        "contribution_contract": "native-complex32-value-times-tap",
        "accumulator_contract": "native-complex64-add-assign",
    },
    {
        "name": "casa_nvalue_native_contribution",
        "nvalue_contract": ("casa-prefix-complex32-weight-plus-zero-i-times-residual"),
        "contribution_contract": "native-complex32-value-times-tap",
        "accumulator_contract": "native-complex64-add-assign",
    },
    {
        "name": "native_nvalue_casa_contribution",
        "nvalue_contract": "native-complex32-residual-times-real-weight",
        "contribution_contract": "casa-prefix-complex32-value-times-tap",
        "accumulator_contract": "native-complex64-add-assign",
    },
    {
        "name": "casa_nvalue_casa_contribution",
        "nvalue_contract": ("casa-prefix-complex32-weight-plus-zero-i-times-residual"),
        "contribution_contract": "casa-prefix-complex32-value-times-tap",
        "accumulator_contract": "native-complex64-add-assign",
    },
    {
        "name": ("casa_nvalue_casa_contribution_componentwise_f64_accumulator"),
        "nvalue_contract": ("casa-prefix-complex32-weight-plus-zero-i-times-residual"),
        "contribution_contract": "casa-prefix-complex32-value-times-tap",
        "accumulator_contract": (
            "explicit-componentwise-f64-grid-re-plus-equals-contribution-re-"
            "and-im-plus-equals-contribution-im"
        ),
    },
]

EXPECTED_SELECTION = {
    "field_id": 1525,
    "requested_spws": "2-17",
    "first_batch_spw": 2,
    "planned_source_blocks": 32,
}
EXPECTED_FIRST_BUFFER = {
    "begin_row": 0,
    "end_row": 325,
    "n_row": 325,
    "spw_id": 2,
    "row_ids_count": 325,
    "row_ids_hash": 15_058_004_568_616_189_240,
    "row_id_first": 0,
    "row_id_last": 324,
    "row_flags_count": 325,
    "row_flags_hash": 3_526_571_572_021_233_857,
    "flagged_rows": 48,
    "n_data_chan": 64,
    "n_data_pol": 4,
    "chan_map_count": 64,
    "chan_map_hash": 2_111_453_637_644_839_429,
    "pol_map_count": 4,
    "pol_map_hash": 13_222_926_617_229_668_273,
    "freq_count": 64,
    "freq_hash": 17_711_728_193_083_539_473,
    "freq_first_bits": 4_746_028_312_096_267_298,
    "freq_last_bits": 4_746_556_774_954_748_567,
}
EXPECTED_ABSOLUTE_ROWS = {
    "semantics": "physical-MAIN-table-row-index",
    "count": 325,
    "hash": 8_652_707_267_842_020_204,
    "first": 353_600,
    "last": 353_924,
}

CANDIDATE_ENVELOPE_SCHEMA = "casa-rs-aw-datagrid-tt0-arithmetic-compat-envelope-v1"
CANDIDATE_EVIDENCE_SCHEMA = "casa-rs-aw-datagrid-tt0-arithmetic-compat-v1"
COMPARISON_ENVELOPE_SCHEMA = (
    "casa-rs-aw-datagrid-tt0-arithmetic-compat-comparison-envelope-v1"
)
COMPARISON_SCHEMA = "casa-rs-aw-datagrid-tt0-arithmetic-compat-comparison-v1"


class ContractError(RuntimeError):
    """Raised when an evidence artifact violates the frozen contract."""


@dataclasses.dataclass(frozen=True)
class FrozenReceipts:
    """Content identities that bind this diagnostic to its two parents."""

    casa_rs_v4_sha256: str = CASA_RS_V4_RECEIPT_SHA256
    casa_rs_v4_evidence_sha256: str = CASA_RS_V4_EVIDENCE_SHA256
    casa_rs_v4_revision: str = CASA_RS_V4_REVISION
    casa_v5_sha256: str = CASA_V5_RECEIPT_SHA256
    casa_source_commit: str = CASA_SOURCE_COMMIT


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _mapping(value: Any, label: str) -> dict[str, Any]:
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _sequence(value: Any, label: str) -> list[Any]:
    _require(isinstance(value, list), f"{label} must be an array")
    return value


def _integer(value: Any, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool),
        f"{label} must be an integer",
    )
    return value


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise ContractError(f"read {path}: {error}") from error
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"parse {path}: {error}") from error
    return payload, _mapping(value, str(path))


def _skip_json_whitespace(text: str, index: int) -> int:
    while index < len(text) and text[index] in " \t\r\n":
        index += 1
    return index


def _raw_json_member(payload: bytes, member: str, source: Path) -> bytes:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ContractError(f"{source}: receipt is not UTF-8") from error
    decoder = json.JSONDecoder()
    index = _skip_json_whitespace(text, 0)
    _require(
        index < len(text) and text[index] == "{",
        f"{source}: content-address envelope must be a top-level object",
    )
    index += 1
    matches: list[tuple[int, int]] = []
    while True:
        index = _skip_json_whitespace(text, index)
        _require(index < len(text), f"{source}: unterminated top-level object")
        if text[index] == "}":
            index += 1
            break
        try:
            key, key_end = decoder.raw_decode(text, index)
        except json.JSONDecodeError as error:
            raise ContractError(
                f"{source}: cannot decode a top-level member name: {error}"
            ) from error
        _require(
            isinstance(key, str),
            f"{source}: top-level member name must be a string",
        )
        index = _skip_json_whitespace(text, key_end)
        _require(
            index < len(text) and text[index] == ":",
            f"{source}: malformed top-level member {key!r}",
        )
        value_start = _skip_json_whitespace(text, index + 1)
        try:
            _, value_end = decoder.raw_decode(text, value_start)
        except json.JSONDecodeError as error:
            raise ContractError(
                f"{source}: cannot decode top-level member {key!r}: {error}"
            ) from error
        if key == member:
            matches.append((value_start, value_end))
        index = _skip_json_whitespace(text, value_end)
        _require(index < len(text), f"{source}: unterminated top-level object")
        if text[index] == ",":
            index += 1
            continue
        _require(
            text[index] == "}",
            f"{source}: malformed separator after top-level member {key!r}",
        )
        index += 1
        break
    _require(
        _skip_json_whitespace(text, index) == len(text),
        f"{source}: trailing data after top-level object",
    )
    _require(matches, f"{source}: missing top-level {member!r}")
    _require(
        len(matches) == 1,
        f"{source}: ambiguous duplicate top-level {member!r}",
    )
    value_start, value_end = matches[0]
    return text[value_start:value_end].encode("utf-8")


def _validate_content_addressed_envelope(
    path: Path,
    *,
    envelope_schema: str,
    member: str,
    embedded_schema: str,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    payload, envelope = _load_json(path)
    _require(
        envelope.get("schema") == envelope_schema,
        f"{path}: expected schema {envelope_schema!r}",
    )
    content_address = _mapping(
        envelope.get("content_address"), f"{path}: content_address"
    )
    _require(
        content_address.get("algorithm") == "sha256",
        f"{path}: content-address algorithm must be sha256",
    )
    _require(
        content_address.get("scope") == f"embedded-{member}-json-utf8",
        f"{path}: unexpected content-address scope",
    )
    embedded = _mapping(envelope.get(member), f"{path}: {member}")
    _require(
        embedded.get("schema") == embedded_schema,
        f"{path}: expected embedded schema {embedded_schema!r}",
    )
    observed_digest = _sha256_bytes(_raw_json_member(payload, member, path))
    _require(
        content_address.get("digest") == observed_digest,
        f"{path}: embedded {member} digest mismatch",
    )
    return envelope, embedded, observed_digest


def _json_type_exact_equal(observed: Any, expected: Any) -> bool:
    if type(observed) is not type(expected):
        return False
    if isinstance(expected, dict):
        return observed.keys() == expected.keys() and all(
            _json_type_exact_equal(observed[key], value)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return len(observed) == len(expected) and all(
            _json_type_exact_equal(observed_value, expected_value)
            for observed_value, expected_value in zip(observed, expected, strict=True)
        )
    return observed == expected


def _require_fields(
    observed: dict[str, Any],
    expected: dict[str, Any],
    label: str,
) -> None:
    for field, value in expected.items():
        _require(field in observed, f"{label}.{field} is missing")
        observed_value = observed[field]
        _require(
            type(observed_value) is type(value),
            f"{label}.{field} JSON type changed: "
            f"{type(observed_value).__name__} != {type(value).__name__}",
        )
        _require(
            _json_type_exact_equal(observed_value, value),
            f"{label}.{field} changed: {observed_value!r} != {value!r}",
        )


def validate_casa_rs_v4(
    path: Path,
    frozen: FrozenReceipts,
) -> dict[str, Any]:
    _require(
        sha256_path(path) == frozen.casa_rs_v4_sha256,
        f"{path}: frozen casa-rs v4 whole-file SHA-256 changed",
    )
    _, evidence, observed_digest = _validate_content_addressed_envelope(
        path,
        envelope_schema="casa-rs-aw-datagrid-bracket-envelope-v4",
        member="evidence",
        embedded_schema="casa-rs-aw-datagrid-bracket-v4",
    )
    _require(
        observed_digest == frozen.casa_rs_v4_evidence_sha256,
        f"{path}: frozen casa-rs v4 embedded digest changed",
    )
    _require(
        evidence.get("status") == "completed-before-finalize",
        f"{path}: casa-rs v4 was not completed before finalize",
    )
    _require_fields(
        evidence,
        {
            "formed_image": False,
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
            "expected_grid_nxy": 4096,
            "completed_calls": 2,
            "completed_blocks": 1,
        },
        f"{path}: v4",
    )
    _require_fields(
        _mapping(evidence.get("selection"), "casa-rs v4 selection"),
        EXPECTED_SELECTION,
        "casa-rs v4 selection",
    )
    _require_fields(
        _mapping(evidence.get("observed_first_buffer"), "casa-rs v4 first buffer"),
        EXPECTED_FIRST_BUFFER,
        "casa-rs v4 first buffer",
    )
    _require_fields(
        _mapping(evidence.get("absolute_main_rows"), "casa-rs v4 absolute rows"),
        EXPECTED_ABSOLUTE_ROWS,
        "casa-rs v4 absolute rows",
    )
    _require_fields(
        evidence,
        {
            "direct_raw_input_hash": DIRECT_COMPACT_INPUT_HASH,
            "compact_input_hash": DIRECT_COMPACT_INPUT_HASH,
            "direct_compact_exact_match": True,
            "portable_hash_contract": PORTABLE_HASH_CONTRACT,
        },
        f"{path}: casa-rs v4",
    )
    calls = _sequence(evidence.get("calls"), "casa-rs v4 calls")
    _require(len(calls) == 2, f"{path}: casa-rs v4 call count changed")
    tt0_call = _mapping(calls[0], "casa-rs v4 TT0 call")
    _require_fields(
        tt0_call,
        {
            "call": 0,
            "block": 0,
            "term": 0,
            "source_count": SOURCE_COUNT,
            "portable_geometry_hash": PORTABLE_GEOMETRY_HASH,
            "portable_input_hash": PORTABLE_INPUT_HASH,
        },
        "casa-rs v4 TT0 call",
    )
    blocks = _sequence(evidence.get("block_boundaries"), "casa-rs v4 block boundaries")
    _require(len(blocks) == 1, f"{path}: casa-rs v4 block count changed")
    terms = _sequence(
        _mapping(blocks[0], "casa-rs v4 block").get("terms"),
        "casa-rs v4 terms",
    )
    _require(len(terms) == 2, f"{path}: casa-rs v4 term count changed")
    tt0 = _mapping(terms[0], "casa-rs v4 TT0")
    _require_fields(
        tt0,
        {
            "term": 0,
            "grid_hash": CASA_RS_V4_TT0_GRID_HASH,
            "sumwt_hash": CASA_RS_V4_TT0_SUMWT_HASH,
            "sumwt_value_bits": CASA_RS_V4_TT0_SUMWT_BITS,
            "grid_values_hashed": GRID_VALUES,
        },
        f"{path}: casa-rs v4 TT0",
    )
    return evidence


def validate_casa_v5(path: Path, frozen: FrozenReceipts) -> dict[str, Any]:
    _require(
        sha256_path(path) == frozen.casa_v5_sha256,
        f"{path}: frozen CASA v5 whole-file SHA-256 changed",
    )
    _, receipt = _load_json(path)
    _require(
        receipt.get("schema") == "casa-aw-datagrid-bracket-v1",
        f"{path}: CASA receipt schema changed",
    )
    _require(
        receipt.get("status") == "completed-before-finalize",
        f"{path}: CASA v5 was not completed before finalize",
    )
    _require(
        receipt.get("casa_source_commit") == frozen.casa_source_commit,
        f"{path}: CASA source commit changed",
    )
    _require_fields(
        receipt,
        {
            "formed_image": False,
            "normalization": "not-entered",
            "fft": "not-entered",
            "expected_grid_nxy": 4096,
            "completed_calls": 2,
            "completed_blocks": 1,
        },
        f"{path}: CASA v5",
    )
    first_buffer = _mapping(receipt.get("native_first_vb"), "CASA v5 first buffer")
    _require_fields(first_buffer, EXPECTED_FIRST_BUFFER, "CASA v5 first buffer")
    row_ids = _sequence(first_buffer.get("row_ids"), "CASA v5 row IDs")
    _require(row_ids == list(range(325)), f"{path}: CASA v5 row IDs changed")
    calls = _sequence(receipt.get("calls"), "CASA v5 calls")
    _require(len(calls) == 2, f"{path}: CASA v5 call count changed")
    _require_fields(
        _mapping(calls[0], "CASA v5 TT0 call"),
        {"source_count": SOURCE_COUNT},
        f"{path}: CASA v5 TT0 call",
    )
    blocks = _sequence(receipt.get("block_boundaries"), "CASA v5 blocks")
    _require(len(blocks) == 1, f"{path}: CASA v5 block count changed")
    terms = _sequence(
        _mapping(blocks[0], "CASA v5 block").get("terms"), "CASA v5 terms"
    )
    _require(len(terms) == 2, f"{path}: CASA v5 term count changed")
    tt0 = _mapping(terms[0], "CASA v5 TT0")
    _require_fields(
        tt0,
        {
            "term": 0,
            "grid_hash": CASA_TT0_GRID_HASH,
            "sumwt_hash": CASA_TT0_SUMWT_HASH,
            "grid_values_hashed": GRID_VALUES,
        },
        f"{path}: CASA v5 TT0",
    )
    return receipt


def validate_frozen_parents(
    casa_rs_v4_path: Path,
    casa_v5_path: Path,
    frozen: FrozenReceipts = FrozenReceipts(),
) -> tuple[dict[str, Any], dict[str, Any]]:
    casa_rs_v4 = validate_casa_rs_v4(casa_rs_v4_path, frozen)
    casa_v5 = validate_casa_v5(casa_v5_path, frozen)
    _require_fields(
        _mapping(casa_rs_v4.get("observed_first_buffer"), "casa-rs v4 first buffer"),
        {
            field: _mapping(casa_v5.get("native_first_vb"), "CASA v5 first buffer")[
                field
            ]
            for field in EXPECTED_FIRST_BUFFER
        },
        "cross-producer first buffer",
    )
    return casa_rs_v4, casa_v5


def _fnv1a64_usize_words(words: list[int]) -> int:
    value = 0xCBF29CE484222325
    for word in words:
        _require(word >= 0, "FNV input must be non-negative")
        for byte in word.to_bytes(8, byteorder="little", signed=False):
            value ^= byte
            value = (value * 0x00000100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


def _validate_correlation_mueller_order(
    value: Any,
    *,
    label: str,
) -> None:
    order = _mapping(value, label)
    _require_fields(
        order,
        {
            "contract": CORRELATION_MUELLER_CONTRACT,
            "count": 2,
            "hash_contract": CORRELATION_MUELLER_HASH_CONTRACT,
        },
        label,
    )
    roles = _sequence(order.get("roles"), f"{label}.roles")
    _require(
        len(roles) == len(EXPECTED_CORRELATION_MUELLER_ROLES),
        f"{label}.roles count changed",
    )
    for ordinal, (observed_role, expected_role) in enumerate(
        zip(roles, EXPECTED_CORRELATION_MUELLER_ROLES, strict=True)
    ):
        _require_fields(
            _mapping(observed_role, f"{label}.roles[{ordinal}]"),
            expected_role,
            f"{label}.roles[{ordinal}]",
        )
    words = [
        item
        for role in EXPECTED_CORRELATION_MUELLER_ROLES
        for item in (
            role["ordinal"],
            role["selected_corr_index"],
            role["selected_corr_code"],
            role["logical_mueller"],
        )
    ]
    observed_hash = _integer(order.get("hash"), f"{label}.hash")
    _require(
        observed_hash == _fnv1a64_usize_words(words),
        f"{label}.hash does not bind the declared role order",
    )


def validate_candidate(path: Path) -> tuple[dict[str, Any], str, str]:
    _, evidence, evidence_digest = _validate_content_addressed_envelope(
        path,
        envelope_schema=CANDIDATE_ENVELOPE_SCHEMA,
        member="evidence",
        embedded_schema=CANDIDATE_EVIDENCE_SCHEMA,
    )
    _require_fields(
        evidence,
        {
            "status": "completed-controlled-stop",
            "result_taxonomy": RESULT_TAXONOMY,
            "role": "bounded-correctness-oracle-not-performance-evidence",
            "producer": "casa-rs",
            "production_path_changed": False,
            "production_dispatch": "not-entered",
            "formed_image": False,
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
            "tt1": False,
            "terms_evaluated": [0],
            "sumwt": "not-controlled",
            "phase_application": (
                "inherited-prephased-production-bundles-not-controlled"
            ),
            "grid_dispatch": (
                "serial-host-f64-exact-source-order-tt0-arithmetic-variants"
            ),
            "traversal_contract": TRAVERSAL_CONTRACT,
            "grid_hash_contract": TOP_LEVEL_GRID_HASH_CONTRACT,
            "portable_hash_contract": PORTABLE_HASH_CONTRACT,
            "expected_grid_nxy": 4096,
            "target_blocks": 1,
            "diagnostic_terms": 1,
            "request_nterms": 2,
            "replay_block_ordinal": 0,
            "replay_window_ordinal": 0,
            "last_window_in_replay_block": True,
            "frozen_production_baseline_hash": CASA_RS_V4_TT0_GRID_HASH,
            "casa_target_hash": CASA_TT0_GRID_HASH,
            "baseline_gate": "matched-frozen-production-baseline",
            "nonfinite_grid_value_contract": NONFINITE_GRID_VALUE_CONTRACT,
            "out_of_grid_support_attempt_contract": (
                OUT_OF_GRID_SUPPORT_ATTEMPT_CONTRACT
            ),
        },
        f"{path}: candidate",
    )
    _validate_correlation_mueller_order(
        evidence.get("correlation_mueller_role_order"),
        label=f"{path}: candidate correlation/Mueller order",
    )
    _require_fields(
        _mapping(evidence.get("selection"), "candidate selection"),
        EXPECTED_SELECTION,
        "candidate selection",
    )
    _require_fields(
        _mapping(evidence.get("observed_first_buffer"), "candidate first buffer"),
        EXPECTED_FIRST_BUFFER,
        "candidate first buffer",
    )
    _require_fields(
        _mapping(evidence.get("absolute_main_rows"), "candidate absolute rows"),
        EXPECTED_ABSOLUTE_ROWS,
        "candidate absolute rows",
    )

    input_hashes = _mapping(evidence.get("input_hashes"), "candidate input hashes")
    _require(
        input_hashes.get("direct_raw") == DIRECT_COMPACT_INPUT_HASH,
        f"{path}: candidate direct/raw input hash changed",
    )
    _require(
        input_hashes.get("compact") == DIRECT_COMPACT_INPUT_HASH,
        f"{path}: candidate compact input hash changed",
    )
    _require(
        input_hashes.get("direct_compact_exact_match") is True,
        f"{path}: candidate direct/compact input streams did not match",
    )
    _require(
        input_hashes.get("portable_geometry") == PORTABLE_GEOMETRY_HASH,
        f"{path}: candidate portable geometry hash changed",
    )
    _require(
        input_hashes.get("portable_input") == PORTABLE_INPUT_HASH,
        f"{path}: candidate portable input hash changed",
    )

    portable_call = _mapping(evidence.get("portable_call"), "candidate portable call")
    _require_fields(
        portable_call,
        {
            "call": 0,
            "block": 0,
            "term": 0,
            "source_count": SOURCE_COUNT,
        },
        "candidate portable call",
    )

    counts = _mapping(evidence.get("counts"), "candidate counts")
    _require_fields(
        counts,
        {
            "source": SOURCE_COUNT,
            "logical_role": ROLE_COUNT,
            "grid_values_per_variant": GRID_VALUES,
            "variant_count": len(EXPECTED_VARIANTS),
            "nonfinite_grid_value": 0,
            "out_of_grid_support_attempt": 0,
        },
        "candidate counts",
    )
    tap_count = _integer(counts.get("tap"), "candidate counts.tap")
    _require(
        tap_count > 0,
        f"{path}: candidate tap count must be positive",
    )
    traversal_hash = _integer(
        evidence.get("traversal_hash"), "candidate traversal_hash"
    )
    _require(
        traversal_hash > 0,
        f"{path}: candidate traversal hash must be positive",
    )

    variants = _sequence(evidence.get("variants"), "candidate variants")
    _require(
        len(variants) == len(EXPECTED_VARIANTS),
        f"{path}: candidate variant count changed",
    )
    matching_variants: list[str] = []
    for ordinal, (value, expected_contract) in enumerate(
        zip(variants, EXPECTED_VARIANTS, strict=True)
    ):
        variant = _mapping(value, f"candidate variant {ordinal}")
        _require_fields(
            variant,
            {
                "ordinal": ordinal,
                **expected_contract,
                "traversal_contract": TRAVERSAL_CONTRACT,
                "grid_hash_contract": GRID_HASH_CONTRACT,
                "grid_values_hashed": GRID_VALUES,
                "nonfinite_grid_value_count": 0,
                "source_count": SOURCE_COUNT,
                "logical_role_count": ROLE_COUNT,
                "tap_count": tap_count,
            },
            f"candidate variant {ordinal}",
        )
        grid_hash = _integer(
            variant.get("grid_hash"), f"candidate variant {ordinal}.grid_hash"
        )
        expected_baseline_match = grid_hash == CASA_RS_V4_TT0_GRID_HASH
        expected_casa_match = grid_hash == CASA_TT0_GRID_HASH
        _require(
            variant.get("matches_frozen_production_baseline")
            is expected_baseline_match,
            f"{path}: variant {ordinal} baseline-match flag is inconsistent",
        )
        _require(
            variant.get("matches_casa_target") is expected_casa_match,
            f"{path}: variant {ordinal} CASA-match flag is inconsistent",
        )
        if expected_casa_match:
            matching_variants.append(expected_contract["name"])

    baseline = _mapping(variants[0], "candidate baseline variant")
    _require(
        baseline.get("grid_hash") == CASA_RS_V4_TT0_GRID_HASH,
        f"{path}: production baseline did not reproduce frozen casa-rs v4",
    )
    _require(
        evidence.get("casa_target_matching_variants") == matching_variants,
        f"{path}: CASA target matching-variant list is inconsistent",
    )
    expected_result = {
        0: "completed-no-tested-variant-matched-casa",
        1: "completed-single-tested-variant-matched-casa",
    }.get(
        len(matching_variants),
        "completed-multiple-tested-variants-matched-casa",
    )
    _require(
        evidence.get("result") == expected_result,
        f"{path}: candidate result does not match the validated variants",
    )
    return evidence, sha256_path(path), evidence_digest


def classify_candidate(evidence: dict[str, Any]) -> str:
    matches = _sequence(
        evidence["casa_target_matching_variants"],
        "candidate CASA target matches",
    )
    if not matches:
        return "valid-negative-no-exact-casa-tt0-grid-hash-match"
    if len(matches) == 1:
        return "exact-casa-tt0-grid-hash-match-single-variant"
    return "exact-casa-tt0-grid-hash-match-multiple-variants"


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
    frozen: FrozenReceipts = FrozenReceipts(),
) -> dict[str, Any]:
    validate_frozen_parents(casa_rs_v4_path, casa_v5_path, frozen)
    candidate, candidate_sha, candidate_evidence_sha = validate_candidate(
        candidate_path
    )
    classification = classify_candidate(candidate)
    variants = [
        {
            "ordinal": variant["ordinal"],
            "name": variant["name"],
            "grid_hash": variant["grid_hash"],
            "matches_casa_target": variant["matches_casa_target"],
        }
        for variant in _sequence(candidate["variants"], "candidate variants")
    ]
    matching_variants = _sequence(
        candidate["casa_target_matching_variants"],
        "candidate CASA target matches",
    )
    comparison = {
        "schema": COMPARISON_SCHEMA,
        "status": "valid-classification",
        "classification": classification,
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
            "phase_application": "not-controlled",
        },
        "parents": {
            "casa_rs_v4": {
                "path": str(casa_rs_v4_path),
                "sha256": frozen.casa_rs_v4_sha256,
                "embedded_evidence_sha256": frozen.casa_rs_v4_evidence_sha256,
                "revision": frozen.casa_rs_v4_revision,
                "tt0_grid_hash": CASA_RS_V4_TT0_GRID_HASH,
                "tt0_sumwt_hash": CASA_RS_V4_TT0_SUMWT_HASH,
            },
            "casa_v5": {
                "path": str(casa_v5_path),
                "sha256": frozen.casa_v5_sha256,
                "source_commit": frozen.casa_source_commit,
                "tt0_grid_hash": CASA_TT0_GRID_HASH,
                "tt0_sumwt_hash": CASA_TT0_SUMWT_HASH,
            },
        },
        "candidate": {
            "path": str(candidate_path),
            "sha256": candidate_sha,
            "embedded_evidence_sha256": candidate_evidence_sha,
            "result": candidate["result"],
            "baseline_tt0_grid_hash": candidate["variants"][0]["grid_hash"],
            "variants": variants,
            "exact_casa_tt0_grid_matching_variants": matching_variants,
            "sumwt_controlled": False,
            "phase_controlled": False,
            "correlation_mueller_role_order": candidate[
                "correlation_mueller_role_order"
            ],
        },
        "claims": {
            "valid_negative_or_positive_classification": True,
            "valid_negative_classification": not matching_variants,
            "exact_casa_tt0_grid_hash_match": bool(matching_variants),
            "exact_casa_tt0_sumwt_match": False,
            "phase_equivalence_proven": False,
            "production_path_changed": False,
            "production_tt0_promoted": False,
            "tt1_promoted": False,
            "integrated_4096_row_promoted": False,
            "alpha_topology_cause_proven": False,
            "performance_evidence": False,
        },
    }
    digest = _sha256_bytes(_canonical_json(comparison))
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
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(temporary, flags, 0o600)
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
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--validate-parents-only",
        action="store_true",
        help="validate the frozen parents without reading or writing a candidate",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.validate_parents_only:
            _require(
                arguments.candidate is None and arguments.output is None,
                "--validate-parents-only cannot be combined with candidate/output",
            )
            validate_frozen_parents(arguments.casa_rs_v4, arguments.casa_v5)
            print(
                json.dumps(
                    {
                        "status": "valid-frozen-parents",
                        "casa_rs_v4_sha256": CASA_RS_V4_RECEIPT_SHA256,
                        "casa_v5_sha256": CASA_V5_RECEIPT_SHA256,
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
        )
        atomic_write_json(arguments.output, comparison)
        print(json.dumps(comparison["comparison"], sort_keys=True))
        return 0
    except ContractError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
