#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate and classify the casa-rs VLASS native-component V3 receipt.

This validator is deliberately independent of the Rust diagnostic.  It
reconstructs every FNV-1a component, admission decision, STREAM hash, TT0
GEOMETRY hash, derived-TT1 GEOMETRY hash, and all 325 cumulative row
checkpoints from the raw published bits.  Scientifically meaningful
differences from the immutable CASA receipt are valid evidence and produce a
zero-exit mismatch classification; malformed or internally inconsistent
evidence is rejected.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from typing import Any, Iterable


CASA_RECEIPT_SHA256 = "cc30d5492f6654336f46617a696f9a7fc8da9006df4e5ae9a3c64a6a9f401644"
CASA_ENVELOPE_SCHEMA = "casa-aw-datagrid-native-components-envelope-v1"
CASA_EVIDENCE_SCHEMA = "casa-aw-datagrid-native-components-v1"
CANDIDATE_ENVELOPE_SCHEMA = "casa-rs-aw-datatogrid-native-components-audit-envelope-v3"
CANDIDATE_EVIDENCE_SCHEMA = "casa-rs-aw-datatogrid-native-components-audit-v3"
COMPARISON_ENVELOPE_SCHEMA = (
    "casa-rs-aw-datatogrid-native-components-comparison-envelope-v3"
)
COMPARISON_SCHEMA = "casa-rs-aw-datatogrid-native-components-comparison-v3"

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x00000100000001B3
U64_MASK = 0xFFFFFFFFFFFFFFFF
U32_MASK = 0xFFFFFFFF

ROW_COUNT = 325
CHANNEL_COUNT = 64
POLARIZATION_COUNT = 4
GRID_SHAPE = [4096, 4096, 1, 1]
IM_REF_FREQ_BITS = 4_748_556_467_228_999_524

FROZEN_COMPONENT_HASHES = {
    "header": 6_709_505_723_840_238_374,
    "row_ids": 15_058_004_568_616_189_240,
    "channel_map": 2_111_453_637_644_839_429,
    "polarization_map": 13_222_926_617_229_668_273,
    "frequencies": 17_711_728_193_083_539_473,
    "row_flags": 3_526_571_572_021_233_857,
    "uvw_dphase": 6_884_923_150_254_773_287,
    "flag_masks": 13_953_846_914_309_385_891,
    "imaging_weights": 2_430_234_571_011_807_313,
    "admission": 14_184_653_015_859_831_397,
}
FROZEN_COUNTS = {
    "flagged_rows": 48,
    "zero_imaging_weights": 8_441,
    "nonzero_imaging_weights": 12_359,
    "admitted_channels": 12_359,
}
FROZEN_CALLS = [
    {
        "call": 0,
        "block": 0,
        "term": 0,
        "source_count": 12_359,
        "stream_hash": 4_740_440_223_154_359_747,
        "geometry_hash": 15_079_793_846_523_608_377,
    },
    {
        "call": 1,
        "block": 0,
        "term": 1,
        "source_count": 12_359,
        "stream_hash": 4_740_440_223_154_359_747,
        "geometry_hash": 14_381_099_959_812_707_833,
    },
]
EXPECTED_ORIGINS = [
    "observed-first-tt0",
    "derived-from-observed-tt0-under-frozen-v5-contract",
]

HASH_CONTRACT = {
    "algorithm": "fnv1a64",
    "offset_basis": FNV_OFFSET,
    "prime": FNV_PRIME,
    "integer_encoding": "little-endian",
    "float_encoding": "ieee754-bits-little-endian",
    "boolean_encoding": "one-byte-0-or-1",
    "recomposition": "casa-6.7.5.18-bracket-hash-call-inputs",
}
CANDIDATE_HASH_CONTRACT = {
    **HASH_CONTRACT,
    "actual_uvw": "casa-rs-prepared-geometry-transform-uvw-m",
    "actual_phase": "casa-rs-prepared-geometry-phase-shift-m",
    "flag_masks": "unmodified-four-correlation-source-FLAG",
    "imaging_weights": (
        "production-global-Briggs-plan-with-raw-UVW-f32-rounded-density-lookup"
    ),
}

COMPONENT_OWNERS = {
    "header": "datatogrid-call-header",
    "row_ids": "selection-and-row-order",
    "channel_map": "channel-mapping",
    "polarization_map": "polarization-mapping",
    "frequencies": "spectral-frame-and-frequency-conversion",
    "row_flags": "row-flag-selection",
    "uvw_dphase": "uvw-reprojection-and-phase-rotation",
    "flag_masks": "visibility-flag-projection",
    "imaging_weights": "global-briggs-weighting",
    "admission": "target-channel-admission",
}
COMPONENT_NAMES = tuple(FROZEN_COMPONENT_HASHES)

ENVELOPE_KEYS = frozenset({"schema", "content_address", "evidence"})
ADDRESS_KEYS = frozenset({"algorithm", "scope", "digest"})
HEADER_KEYS = frozenset(
    {
        "use_conjugate_frequency_cf",
        "begin_row",
        "end_row",
        "n_row",
        "spw_id",
        "im_ref_freq_bits",
        "grid_shape",
        "channel_map",
        "polarization_map",
        "row_ids",
        "frequency_bits",
    }
)
CASA_ROW_KEYS = frozenset(
    {
        "row",
        "row_flag",
        "uvw_bits",
        "dphase_bits",
        "flag_masks",
        "imaging_weight_bits",
    }
)
CANDIDATE_ROW_KEYS = CASA_ROW_KEYS | {"admitted", "auxiliary"}
AUXILIARY_KEYS = frozenset(
    {
        "absolute_main_row",
        "raw_uvw_bits",
        "gridft_density_uvw_bits",
        "negated_uv_transform_uvw_bits",
        "first_parallel_hand_natural_weight_bits",
        "second_parallel_hand_natural_weight_bits",
        "collapsed_natural_weight_bits",
    }
)
COUNT_KEYS = frozenset(FROZEN_COUNTS)
CALL_KEYS = frozenset(
    {
        "origin",
        "call",
        "block",
        "term",
        "source_count",
        "stream_hash",
        "geometry_hash",
    }
)
CHECKPOINT_KEYS = frozenset(
    {
        "row",
        "source_count",
        "row_flags_hash",
        "uvw_dphase_hash",
        "flag_masks_hash",
        "imaging_weights_hash",
        "admission_hash",
        "stream_hash",
        "tt0_geometry_hash",
        "tt1_geometry_hash",
    }
)
CASA_EVIDENCE_KEYS = frozenset(
    {
        "schema",
        "status",
        "result",
        "role",
        "producer",
        "casa_version",
        "casa_version_string",
        "casa_source_commit",
        "casacore_source_commit",
        "datatogrid_symbol",
        "symbol_owner",
        "observed_dispatch",
        "diagnostic_hook_added",
        "normal_execution_behavior_changed",
        "production_science_arithmetic_changed",
        "original_datatogrid",
        "grid_storage",
        "grid_dispatch",
        "sumwt",
        "formed_image",
        "normalization",
        "fft",
        "products",
        "completed_calls",
        "terms_observed",
        "hash_contracts",
        "frozen_parent_receipts",
        "header",
        "component_hashes",
        "counts",
        "recomputed_frozen_hashes",
        "rows",
    }
)
CANDIDATE_EVIDENCE_KEYS = frozenset(
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
        "density_pass",
        "density_source_blocks",
        "production_dispatch",
        "cf_cache",
        "cf_selection",
        "grid_storage",
        "grid_dispatch",
        "sumwt",
        "formed_image",
        "normalization",
        "fft",
        "products",
        "completed_calls",
        "terms_observed",
        "hash_contracts",
        "frozen_parent_receipts",
        "header",
        "component_hashes",
        "component_comparison",
        "mismatched_components",
        "counts",
        "recomputed_frozen_hashes",
        "row_checkpoints",
        "rows",
    }
)


class ContractError(RuntimeError):
    """Raised when an input violates its evidence contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _exact(observed: Any, expected: Any, label: str) -> None:
    _require(
        type(observed) is type(expected) and observed == expected,
        f"{label} changed: {observed!r} != {expected!r}",
    )


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
    _require(0 <= word <= U64_MASK, f"{label} is not a u64")
    return word


def _u32(value: Any, label: str) -> int:
    word = _integer(value, label)
    _require(0 <= word <= U32_MASK, f"{label} is not a u32")
    return word


def _exact_keys(value: dict[str, Any], expected: frozenset[str], label: str) -> None:
    observed = frozenset(value)
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    _require(
        not missing and not unexpected,
        f"{label} key set changed: missing={missing!r} unexpected={unexpected!r}",
    )


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ContractError(f"read {path}: {error}") from error
    return digest.hexdigest()


def _load_json_strict(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise ContractError(f"read {path}: {error}") from error

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ContractError(f"{path}: duplicate JSON key {key!r}")
            result[key] = value
        return result

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
    _require(index < len(text) and text[index] == "{", f"{source}: expected object")
    index += 1
    matches: list[tuple[int, int]] = []
    while True:
        index = _skip_json_whitespace(text, index)
        _require(index < len(text), f"{source}: unterminated object")
        if text[index] == "}":
            index += 1
            break
        try:
            key, key_end = decoder.raw_decode(text, index)
        except json.JSONDecodeError as error:
            raise ContractError(f"{source}: malformed member name: {error}") from error
        _require(type(key) is str, f"{source}: member name must be a string")
        index = _skip_json_whitespace(text, key_end)
        _require(index < len(text) and text[index] == ":", f"{source}: missing colon")
        value_start = _skip_json_whitespace(text, index + 1)
        try:
            _, value_end = decoder.raw_decode(text, value_start)
        except json.JSONDecodeError as error:
            raise ContractError(
                f"{source}: malformed member {key!r}: {error}"
            ) from error
        if key == member:
            matches.append((value_start, value_end))
        index = _skip_json_whitespace(text, value_end)
        _require(index < len(text), f"{source}: unterminated object")
        if text[index] == ",":
            index += 1
            continue
        _require(text[index] == "}", f"{source}: malformed member separator")
        index += 1
        break
    _require(
        _skip_json_whitespace(text, index) == len(text),
        f"{source}: trailing data",
    )
    _require(len(matches) == 1, f"{source}: expected one {member!r} member")
    start, end = matches[0]
    return text[start:end].encode()


def _load_envelope(
    path: Path,
    *,
    envelope_schema: str,
    evidence_schema: str,
    expected_sha256: str | None = None,
) -> tuple[dict[str, Any], str, str]:
    whole_sha = sha256_path(path)
    if expected_sha256 is not None:
        _exact(whole_sha, expected_sha256, f"{path}: whole-file SHA-256")
    payload, envelope = _load_json_strict(path)
    _exact_keys(envelope, ENVELOPE_KEYS, f"{path}: envelope")
    _exact(envelope["schema"], envelope_schema, f"{path}: envelope.schema")
    address = _mapping(envelope["content_address"], f"{path}: content_address")
    _exact_keys(address, ADDRESS_KEYS, f"{path}: content_address")
    _exact(address["algorithm"], "sha256", f"{path}: content_address.algorithm")
    _exact(
        address["scope"],
        "embedded-evidence-json-utf8",
        f"{path}: content_address.scope",
    )
    raw_evidence = _raw_json_member(payload, "evidence", path)
    embedded_sha = hashlib.sha256(raw_evidence).hexdigest()
    _exact(address["digest"], embedded_sha, f"{path}: content_address.digest")
    evidence = _mapping(envelope["evidence"], f"{path}: evidence")
    _exact(evidence.get("schema"), evidence_schema, f"{path}: evidence.schema")
    return evidence, whole_sha, embedded_sha


class Fnv1a64:
    """FNV-1a over the exact primitive encodings used by the CASA oracle."""

    def __init__(self) -> None:
        self.value = FNV_OFFSET

    def _bytes(self, values: Iterable[int]) -> None:
        for byte in values:
            self.value ^= byte
            self.value = (self.value * FNV_PRIME) & U64_MASK

    def u64(self, value: int) -> None:
        self._bytes((value & U64_MASK).to_bytes(8, "little"))

    def u32(self, value: int) -> None:
        self._bytes((value & U32_MASK).to_bytes(4, "little"))

    def boolean(self, value: bool) -> None:
        self._bytes((1 if value else 0,))


def _hash_counted_u64(values: list[int]) -> int:
    digest = Fnv1a64()
    digest.u64(len(values))
    for value in values:
        digest.u64(value)
    return digest.value


def _hash_counted_bools(values: list[bool]) -> int:
    digest = Fnv1a64()
    digest.u64(len(values))
    for value in values:
        digest.boolean(value)
    return digest.value


def _weight_nonzero(bits: int) -> bool:
    return struct.unpack("<f", bits.to_bytes(4, "little"))[0] != 0.0


def _f32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _collapsed_natural_weight_bits(first_bits: int, second_bits: int) -> int:
    first = struct.unpack("<f", first_bits.to_bytes(4, "little"))[0]
    second = struct.unpack("<f", second_bits.to_bytes(4, "little"))[0]
    collapsed = _f32(_f32(first + second) * _f32(0.5))
    return int.from_bytes(struct.pack("<f", collapsed), "little")


def _negated_raw_uvw(bits: list[int]) -> list[int]:
    values = [struct.unpack("<d", value.to_bytes(8, "little"))[0] for value in bits]
    transformed = [-values[0], -values[1], values[2]]
    return [int.from_bytes(struct.pack("<d", value), "little") for value in transformed]


def _hash_common_header(digest: Fnv1a64, header: dict[str, Any]) -> None:
    digest.boolean(header["use_conjugate_frequency_cf"])
    digest.u64(header["begin_row"])
    digest.u64(header["end_row"])
    digest.u64(header["n_row"])
    digest.u64(header["spw_id"])
    digest.u64(header["im_ref_freq_bits"])
    for extent in header["grid_shape"]:
        digest.u64(extent)
    digest.u64(len(header["channel_map"]))
    for value in header["channel_map"]:
        digest.u64(value)
    digest.u64(len(header["polarization_map"]))
    for value in header["polarization_map"]:
        digest.u64(value)
    digest.u64(len(header["row_ids"]))
    for value in header["row_ids"]:
        digest.u64(value)


def _validate_header(value: Any, label: str) -> dict[str, Any]:
    header = _mapping(value, label)
    _exact_keys(header, HEADER_KEYS, label)
    _require(
        type(header["use_conjugate_frequency_cf"]) is bool,
        f"{label}.use_conjugate_frequency_cf must be boolean",
    )
    for key in ("begin_row", "end_row", "n_row", "spw_id", "im_ref_freq_bits"):
        _u64(header[key], f"{label}.{key}")
    _exact(header["begin_row"], 0, f"{label}.begin_row")
    _exact(header["end_row"], ROW_COUNT, f"{label}.end_row")
    _exact(header["n_row"], ROW_COUNT, f"{label}.n_row")
    _exact(header["spw_id"], 2, f"{label}.spw_id")
    shape = _sequence(header["grid_shape"], f"{label}.grid_shape")
    _exact(shape, GRID_SHAPE, f"{label}.grid_shape")
    arrays = {
        "channel_map": CHANNEL_COUNT,
        "polarization_map": POLARIZATION_COUNT,
        "row_ids": ROW_COUNT,
        "frequency_bits": CHANNEL_COUNT,
    }
    for name, count in arrays.items():
        values = _sequence(header[name], f"{label}.{name}")
        _require(len(values) == count, f"{label}.{name} must contain {count} values")
        for ordinal, value in enumerate(values):
            if name == "frequency_bits":
                _u64(value, f"{label}.{name}[{ordinal}]")
            else:
                _integer(value, f"{label}.{name}[{ordinal}]")
    return header


def _validate_rows(value: Any, *, candidate: bool, label: str) -> list[dict[str, Any]]:
    rows = _sequence(value, label)
    _require(len(rows) == ROW_COUNT, f"{label} must contain {ROW_COUNT} rows")
    expected_keys = CANDIDATE_ROW_KEYS if candidate else CASA_ROW_KEYS
    validated: list[dict[str, Any]] = []
    for ordinal, raw_row in enumerate(rows):
        row_label = f"{label}[{ordinal}]"
        row = _mapping(raw_row, row_label)
        _exact_keys(row, expected_keys, row_label)
        _exact(row["row"], ordinal, f"{row_label}.row")
        _require(type(row["row_flag"]) is bool, f"{row_label}.row_flag must be boolean")
        uvw = _sequence(row["uvw_bits"], f"{row_label}.uvw_bits")
        _require(len(uvw) == 3, f"{row_label}.uvw_bits must contain three values")
        for axis, bits in enumerate(uvw):
            _u64(bits, f"{row_label}.uvw_bits[{axis}]")
        _u64(row["dphase_bits"], f"{row_label}.dphase_bits")
        masks = _sequence(row["flag_masks"], f"{row_label}.flag_masks")
        weights = _sequence(
            row["imaging_weight_bits"], f"{row_label}.imaging_weight_bits"
        )
        _require(
            len(masks) == CHANNEL_COUNT,
            f"{row_label}.flag_masks must contain {CHANNEL_COUNT} values",
        )
        _require(
            len(weights) == CHANNEL_COUNT,
            f"{row_label}.imaging_weight_bits must contain {CHANNEL_COUNT} values",
        )
        for channel, mask in enumerate(masks):
            _require(
                0 <= _integer(mask, f"{row_label}.flag_masks[{channel}]") < 16,
                f"{row_label}.flag_masks[{channel}] uses bits outside 0..3",
            )
            _u32(weights[channel], f"{row_label}.imaging_weight_bits[{channel}]")
        if candidate:
            admitted = _sequence(row["admitted"], f"{row_label}.admitted")
            _require(
                len(admitted) == CHANNEL_COUNT,
                f"{row_label}.admitted must contain {CHANNEL_COUNT} values",
            )
            for channel, value in enumerate(admitted):
                _require(
                    type(value) is bool,
                    f"{row_label}.admitted[{channel}] must be boolean",
                )
            auxiliary = _mapping(row["auxiliary"], f"{row_label}.auxiliary")
            _exact_keys(auxiliary, AUXILIARY_KEYS, f"{row_label}.auxiliary")
            _integer(
                auxiliary["absolute_main_row"],
                f"{row_label}.auxiliary.absolute_main_row",
            )
            _exact(
                auxiliary["absolute_main_row"],
                353_600 + ordinal,
                f"{row_label}.auxiliary.absolute_main_row",
            )
            for name in (
                "raw_uvw_bits",
                "gridft_density_uvw_bits",
                "negated_uv_transform_uvw_bits",
            ):
                bits = _sequence(auxiliary[name], f"{row_label}.auxiliary.{name}")
                _require(
                    len(bits) == 3,
                    f"{row_label}.auxiliary.{name} must contain three values",
                )
                for axis, value in enumerate(bits):
                    _u64(value, f"{row_label}.auxiliary.{name}[{axis}]")
            for name in (
                "first_parallel_hand_natural_weight_bits",
                "second_parallel_hand_natural_weight_bits",
                "collapsed_natural_weight_bits",
            ):
                bits = _sequence(auxiliary[name], f"{row_label}.auxiliary.{name}")
                _require(
                    len(bits) == CHANNEL_COUNT,
                    f"{row_label}.auxiliary.{name} must contain {CHANNEL_COUNT} values",
                )
                for channel, value in enumerate(bits):
                    _u32(value, f"{row_label}.auxiliary.{name}[{channel}]")
            first_weights = auxiliary["first_parallel_hand_natural_weight_bits"]
            second_weights = auxiliary["second_parallel_hand_natural_weight_bits"]
            collapsed_weights = auxiliary["collapsed_natural_weight_bits"]
            for channel in range(CHANNEL_COUNT):
                _exact(
                    collapsed_weights[channel],
                    _collapsed_natural_weight_bits(
                        first_weights[channel], second_weights[channel]
                    ),
                    (f"{row_label}.auxiliary.collapsed_natural_weight_bits[{channel}]"),
                )
        validated.append(row)
    return validated


def recompute(
    header: dict[str, Any],
    rows: list[dict[str, Any]],
) -> tuple[
    dict[str, int],
    dict[str, int],
    list[dict[str, int]],
    list[dict[str, int]],
    list[list[bool]],
]:
    """Rebuild every claimed component and boundary from raw row bits."""

    header_hash = Fnv1a64()
    _hash_common_header(header_hash, header)
    row_flags_hash = Fnv1a64()
    row_flags_hash.u64(ROW_COUNT)
    uvw_dphase_hash = Fnv1a64()
    uvw_dphase_hash.u64(ROW_COUNT)
    flag_masks_hash = Fnv1a64()
    flag_masks_hash.u64(ROW_COUNT)
    flag_masks_hash.u64(CHANNEL_COUNT)
    flag_masks_hash.u64(POLARIZATION_COUNT)
    imaging_weights_hash = Fnv1a64()
    imaging_weights_hash.u64(ROW_COUNT)
    imaging_weights_hash.u64(CHANNEL_COUNT)
    admission_hash = Fnv1a64()
    admission_hash.u64(ROW_COUNT)
    admission_hash.u64(CHANNEL_COUNT)
    stream_hash = Fnv1a64()
    _hash_common_header(stream_hash, header)
    geometry_hashes = [Fnv1a64(), Fnv1a64()]
    for term, digest in enumerate(geometry_hashes):
        digest.u64(term)
        digest.u64(0)
        digest.u64(term)
        _hash_common_header(digest, header)

    flagged_rows = 0
    zero_weights = 0
    nonzero_weights = 0
    source_count = 0
    admission_bitmap: list[list[bool]] = []
    checkpoints: list[dict[str, int]] = []
    for row_index, row in enumerate(rows):
        row_flag = row["row_flag"]
        flagged_rows += int(row_flag)
        row_flags_hash.boolean(row_flag)
        uvw_dphase_hash.u64(row_index)
        for bits in row["uvw_bits"]:
            uvw_dphase_hash.u64(bits)
        uvw_dphase_hash.u64(row["dphase_bits"])

        stream_hash.u64(row_index)
        stream_hash.boolean(row_flag)
        for digest in geometry_hashes:
            digest.u64(row_index)
            digest.boolean(row_flag)
        if not row_flag:
            for bits in row["uvw_bits"]:
                stream_hash.u64(bits)
                for digest in geometry_hashes:
                    digest.u64(bits)
            stream_hash.u64(row["dphase_bits"])
            for digest in geometry_hashes:
                digest.u64(row["dphase_bits"])

        row_admission: list[bool] = []
        for channel in range(CHANNEL_COUNT):
            mask = row["flag_masks"][channel]
            weight_bits = row["imaging_weight_bits"][channel]
            flag_masks_hash.u64(row_index)
            flag_masks_hash.u64(channel)
            for polarization in range(POLARIZATION_COUNT):
                flag_masks_hash.boolean(bool(mask & (1 << polarization)))
            imaging_weights_hash.u64(row_index)
            imaging_weights_hash.u64(channel)
            imaging_weights_hash.u32(weight_bits)
            target = header["channel_map"][channel]
            target_valid = 0 <= target < header["grid_shape"][3]
            weight_nonzero = _weight_nonzero(weight_bits)
            admitted = not row_flag and target_valid and weight_nonzero
            row_admission.append(admitted)
            admission_hash.u64(row_index)
            admission_hash.u64(channel)
            admission_hash.boolean(not row_flag)
            admission_hash.boolean(target_valid)
            admission_hash.boolean(weight_nonzero)
            admission_hash.boolean(admitted)
            nonzero_weights += int(weight_nonzero)
            zero_weights += int(not weight_nonzero)

            if row_flag or not target_valid:
                continue
            stream_hash.u64(channel)
            stream_hash.u64(header["frequency_bits"][channel])
            for polarization in range(POLARIZATION_COUNT):
                stream_hash.boolean(bool(mask & (1 << polarization)))
            if not weight_nonzero:
                continue
            for digest in geometry_hashes:
                digest.u64(source_count)
                digest.u64(channel)
                digest.u64(header["frequency_bits"][channel])
                for polarization in range(POLARIZATION_COUNT):
                    digest.boolean(bool(mask & (1 << polarization)))
            source_count += 1
        admission_bitmap.append(row_admission)
        checkpoints.append(
            {
                "row": row_index,
                "source_count": source_count,
                "row_flags_hash": row_flags_hash.value,
                "uvw_dphase_hash": uvw_dphase_hash.value,
                "flag_masks_hash": flag_masks_hash.value,
                "imaging_weights_hash": imaging_weights_hash.value,
                "admission_hash": admission_hash.value,
                "stream_hash": stream_hash.value,
                "tt0_geometry_hash": geometry_hashes[0].value,
                "tt1_geometry_hash": geometry_hashes[1].value,
            }
        )

    components = {
        "header": header_hash.value,
        "row_ids": _hash_counted_u64(header["row_ids"]),
        "channel_map": _hash_counted_u64(header["channel_map"]),
        "polarization_map": _hash_counted_u64(header["polarization_map"]),
        "frequencies": _hash_counted_u64(header["frequency_bits"]),
        "row_flags": _hash_counted_bools([row["row_flag"] for row in rows]),
        "uvw_dphase": uvw_dphase_hash.value,
        "flag_masks": flag_masks_hash.value,
        "imaging_weights": imaging_weights_hash.value,
        "admission": admission_hash.value,
    }
    counts = {
        "flagged_rows": flagged_rows,
        "zero_imaging_weights": zero_weights,
        "nonzero_imaging_weights": nonzero_weights,
        "admitted_channels": source_count,
    }
    calls = [
        {
            "call": term,
            "block": 0,
            "term": term,
            "source_count": source_count,
            "stream_hash": stream_hash.value,
            "geometry_hash": geometry_hashes[term].value,
        }
        for term in range(2)
    ]
    return components, counts, calls, checkpoints, admission_bitmap


def _claimed_calls(
    value: Any,
    *,
    label: str,
) -> list[dict[str, Any]]:
    claims = _sequence(value, label)
    _require(len(claims) == 2, f"{label} must contain TT0 and derived TT1")
    for term, raw_claim in enumerate(claims):
        claim = _mapping(raw_claim, f"{label}[{term}]")
        _exact_keys(claim, CALL_KEYS, f"{label}[{term}]")
        _exact(claim["origin"], EXPECTED_ORIGINS[term], f"{label}[{term}].origin")
        for key in CALL_KEYS - {"origin"}:
            _u64(claim[key], f"{label}[{term}].{key}")
    return claims


def _expected_claimed_calls(calls: list[dict[str, int]]) -> list[dict[str, Any]]:
    return [
        {"origin": EXPECTED_ORIGINS[term], **call} for term, call in enumerate(calls)
    ]


def _validate_recomputed_claims(
    evidence: dict[str, Any],
    *,
    label: str,
    components: dict[str, int],
    counts: dict[str, int],
    calls: list[dict[str, int]],
) -> None:
    claimed_components = _mapping(
        evidence["component_hashes"], f"{label}.component_hashes"
    )
    _exact_keys(
        claimed_components, frozenset(COMPONENT_NAMES), f"{label}.component_hashes"
    )
    for name in COMPONENT_NAMES:
        _u64(claimed_components[name], f"{label}.component_hashes.{name}")
    _exact(
        claimed_components,
        components,
        f"{label}: independently recomputed component hashes",
    )

    claimed_counts = _mapping(evidence["counts"], f"{label}.counts")
    _exact_keys(claimed_counts, COUNT_KEYS, f"{label}.counts")
    for name in COUNT_KEYS:
        _u64(claimed_counts[name], f"{label}.counts.{name}")
    _exact(claimed_counts, counts, f"{label}: independently recomputed counts")

    claimed_calls = _claimed_calls(
        evidence["recomputed_frozen_hashes"],
        label=f"{label}.recomputed_frozen_hashes",
    )
    _exact(
        claimed_calls,
        _expected_claimed_calls(calls),
        f"{label}: independently recomputed STREAM/GEOMETRY hashes",
    )


def validate_casa(
    path: Path,
) -> tuple[
    dict[str, Any],
    str,
    str,
    dict[str, int],
    dict[str, int],
    list[dict[str, int]],
    list[dict[str, int]],
    list[list[bool]],
]:
    """Validate the immutable CASA parent and independently recompose it."""

    evidence, whole_sha, embedded_sha = _load_envelope(
        path,
        envelope_schema=CASA_ENVELOPE_SCHEMA,
        evidence_schema=CASA_EVIDENCE_SCHEMA,
        expected_sha256=CASA_RECEIPT_SHA256,
    )
    _exact_keys(evidence, CASA_EVIDENCE_KEYS, f"{path}: evidence")
    for key, expected in {
        "schema": CASA_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": "completed-native-components-exact-frozen-v5",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "CASA",
        "casa_version": "6.7.5.18",
        "casa_version_string": "6.7.5-18",
        "casa_source_commit": "418bb1a26df7c4aba663ff123b038b75a6fa0295",
        "casacore_source_commit": "25b653f6963a78a1dcfc8e16954081e091a50fbe",
        "symbol_owner": "libcasacpp_synthesis.6.dylib",
        "observed_dispatch": "first-dcomplex-non-psf",
        "diagnostic_hook_added": True,
        "normal_execution_behavior_changed": False,
        "production_science_arithmetic_changed": False,
        "original_datatogrid": "not-invoked",
        "grid_storage": "received-not-read-or-written",
        "grid_dispatch": "not-entered",
        "sumwt": "not-read-or-written",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "completed_calls": 1,
        "terms_observed": [0],
        "hash_contracts": HASH_CONTRACT,
    }.items():
        _exact(evidence[key], expected, f"{path}: evidence.{key}")
    header = _validate_header(evidence["header"], f"{path}: evidence.header")
    rows = _validate_rows(
        evidence["rows"], candidate=False, label=f"{path}: evidence.rows"
    )
    components, counts, calls, checkpoints, admission = recompute(header, rows)
    _validate_recomputed_claims(
        evidence,
        label=f"{path}: evidence",
        components=components,
        counts=counts,
        calls=calls,
    )
    _exact(
        components,
        FROZEN_COMPONENT_HASHES,
        f"{path}: immutable CASA component hashes",
    )
    _exact(counts, FROZEN_COUNTS, f"{path}: immutable CASA counts")
    _exact(calls, FROZEN_CALLS, f"{path}: immutable CASA boundary hashes")
    return (
        evidence,
        whole_sha,
        embedded_sha,
        components,
        counts,
        calls,
        checkpoints,
        admission,
    )


def _validate_candidate_comparison_claims(
    evidence: dict[str, Any],
    components: dict[str, int],
    calls: list[dict[str, int]],
    *,
    label: str,
) -> None:
    claimed_comparison = _mapping(
        evidence["component_comparison"], f"{label}.component_comparison"
    )
    _exact_keys(
        claimed_comparison,
        frozenset(COMPONENT_NAMES),
        f"{label}.component_comparison",
    )
    expected_comparison = {
        name: {
            "actual": components[name],
            "expected_casa": FROZEN_COMPONENT_HASHES[name],
            "exact": components[name] == FROZEN_COMPONENT_HASHES[name],
        }
        for name in COMPONENT_NAMES
    }
    _exact(
        claimed_comparison,
        expected_comparison,
        f"{label}: component comparison claims",
    )
    mismatches = [
        name
        for name in COMPONENT_NAMES
        if components[name] != FROZEN_COMPONENT_HASHES[name]
    ]
    _exact(
        evidence["mismatched_components"],
        mismatches,
        f"{label}: mismatched component names",
    )
    calls_exact = calls == FROZEN_CALLS
    expected_result = (
        "completed-native-components-exact-frozen-casa"
        if not mismatches and calls_exact
        else "completed-native-components-mismatch"
    )
    _exact(evidence["result"], expected_result, f"{label}: result")


def validate_candidate(
    path: Path,
) -> tuple[
    dict[str, Any],
    str,
    str,
    dict[str, int],
    dict[str, int],
    list[dict[str, int]],
    list[dict[str, int]],
    list[list[bool]],
]:
    """Validate candidate structure and all self-consistency claims.

    Equality with CASA is intentionally not a validity requirement.
    """

    evidence, whole_sha, embedded_sha = _load_envelope(
        path,
        envelope_schema=CANDIDATE_ENVELOPE_SCHEMA,
        evidence_schema=CANDIDATE_EVIDENCE_SCHEMA,
    )
    label = f"{path}: evidence"
    _exact_keys(evidence, CANDIDATE_EVIDENCE_KEYS, label)
    for key, expected in {
        "schema": CANDIDATE_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result_taxonomy": [
            "completed-native-components-mismatch",
            "completed-native-components-exact-frozen-casa",
        ],
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "casa-rs",
        "diagnostic_hook_added": True,
        "normal_execution_behavior_changed": False,
        "production_science_arithmetic_changed": False,
        "density_pass": "diagnostic-only-completed-with-production-weighting-plan",
        "density_source_blocks": 32,
        "production_dispatch": "not-entered",
        "cf_cache": "not-opened",
        "cf_selection": "not-entered",
        "grid_storage": "not-allocated",
        "grid_dispatch": "not-entered",
        "sumwt": "not-entered",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "completed_calls": 0,
        "terms_observed": [],
        "hash_contracts": CANDIDATE_HASH_CONTRACT,
        "frozen_parent_receipts": {
            "casa_native_components_v1": {
                "schema": CASA_EVIDENCE_SCHEMA,
                "receipt_sha256": CASA_RECEIPT_SHA256,
            }
        },
    }.items():
        _exact(evidence[key], expected, f"{label}.{key}")

    header = _validate_header(evidence["header"], f"{label}.header")
    rows = _validate_rows(evidence["rows"], candidate=True, label=f"{label}.rows")
    components, counts, calls, checkpoints, admission = recompute(header, rows)
    _validate_recomputed_claims(
        evidence,
        label=label,
        components=components,
        counts=counts,
        calls=calls,
    )
    claimed_checkpoints = _sequence(
        evidence["row_checkpoints"], f"{label}.row_checkpoints"
    )
    _require(
        len(claimed_checkpoints) == ROW_COUNT,
        f"{label}.row_checkpoints must contain {ROW_COUNT} rows",
    )
    for row, checkpoint in enumerate(claimed_checkpoints):
        checkpoint = _mapping(checkpoint, f"{label}.row_checkpoints[{row}]")
        _exact_keys(checkpoint, CHECKPOINT_KEYS, f"{label}.row_checkpoints[{row}]")
        for name in CHECKPOINT_KEYS:
            _u64(checkpoint[name], f"{label}.row_checkpoints[{row}].{name}")
    _exact(
        claimed_checkpoints,
        checkpoints,
        f"{label}: independently recomputed row checkpoints",
    )
    for row, (claimed, recomputed) in enumerate(zip(rows, admission, strict=True)):
        _exact(
            claimed["admitted"],
            recomputed,
            f"{label}.rows[{row}]: exact admission membership",
        )
        _exact(
            claimed["auxiliary"]["negated_uv_transform_uvw_bits"],
            _negated_raw_uvw(claimed["uvw_bits"]),
            f"{label}.rows[{row}]: independently negated internal UVW",
        )
    _validate_candidate_comparison_claims(evidence, components, calls, label=label)
    return (
        evidence,
        whole_sha,
        embedded_sha,
        components,
        counts,
        calls,
        checkpoints,
        admission,
    )


def _first_sequence_difference(
    casa: list[Any],
    candidate: list[Any],
    *,
    coordinate: str,
) -> dict[str, Any] | None:
    for ordinal, (left, right) in enumerate(zip(casa, candidate, strict=True)):
        if type(left) is not type(right) or left != right:
            return {coordinate: ordinal, "casa": left, "candidate": right}
    return None


def _first_matrix_difference(
    casa: list[list[Any]],
    candidate: list[list[Any]],
) -> dict[str, Any] | None:
    for row, (left_row, right_row) in enumerate(zip(casa, candidate, strict=True)):
        for channel, (left, right) in enumerate(zip(left_row, right_row, strict=True)):
            if type(left) is not type(right) or left != right:
                return {
                    "row": row,
                    "channel": channel,
                    "casa": left,
                    "candidate": right,
                }
    return None


def _header_first_difference(
    casa: dict[str, Any], candidate: dict[str, Any]
) -> dict[str, Any] | None:
    for name in (
        "use_conjugate_frequency_cf",
        "begin_row",
        "end_row",
        "n_row",
        "spw_id",
        "im_ref_freq_bits",
        "grid_shape",
    ):
        if (
            type(casa[name]) is not type(candidate[name])
            or casa[name] != candidate[name]
        ):
            return {"field": name, "casa": casa[name], "candidate": candidate[name]}
    for name, coordinate in (
        ("channel_map", "channel"),
        ("polarization_map", "polarization"),
        ("row_ids", "row"),
    ):
        difference = _first_sequence_difference(
            casa[name], candidate[name], coordinate=coordinate
        )
        if difference is not None:
            return {"field": name, **difference}
    return None


def _uvw_first_difference(
    casa_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    axis_names = ["u", "v", "w"]
    for row, (left, right) in enumerate(zip(casa_rows, candidate_rows, strict=True)):
        for axis, (casa_bits, candidate_bits) in enumerate(
            zip(left["uvw_bits"], right["uvw_bits"], strict=True)
        ):
            if casa_bits != candidate_bits:
                return {
                    "row": row,
                    "axis": axis_names[axis],
                    "casa": casa_bits,
                    "candidate": candidate_bits,
                }
        if left["dphase_bits"] != right["dphase_bits"]:
            return {
                "row": row,
                "axis": "dphase",
                "casa": left["dphase_bits"],
                "candidate": right["dphase_bits"],
            }
    return None


def _component_first_differences(
    casa_header: dict[str, Any],
    casa_rows: list[dict[str, Any]],
    casa_admission: list[list[bool]],
    candidate_header: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    candidate_admission: list[list[bool]],
) -> dict[str, dict[str, Any] | None]:
    row_flags_casa = [row["row_flag"] for row in casa_rows]
    row_flags_candidate = [row["row_flag"] for row in candidate_rows]
    masks_casa = [row["flag_masks"] for row in casa_rows]
    masks_candidate = [row["flag_masks"] for row in candidate_rows]
    weights_casa = [row["imaging_weight_bits"] for row in casa_rows]
    weights_candidate = [row["imaging_weight_bits"] for row in candidate_rows]
    return {
        "header": _header_first_difference(casa_header, candidate_header),
        "row_ids": _first_sequence_difference(
            casa_header["row_ids"],
            candidate_header["row_ids"],
            coordinate="row",
        ),
        "channel_map": _first_sequence_difference(
            casa_header["channel_map"],
            candidate_header["channel_map"],
            coordinate="channel",
        ),
        "polarization_map": _first_sequence_difference(
            casa_header["polarization_map"],
            candidate_header["polarization_map"],
            coordinate="polarization",
        ),
        "frequencies": _first_sequence_difference(
            casa_header["frequency_bits"],
            candidate_header["frequency_bits"],
            coordinate="channel",
        ),
        "row_flags": _first_sequence_difference(
            row_flags_casa, row_flags_candidate, coordinate="row"
        ),
        "uvw_dphase": _uvw_first_difference(casa_rows, candidate_rows),
        "flag_masks": _first_matrix_difference(masks_casa, masks_candidate),
        "imaging_weights": _first_matrix_difference(weights_casa, weights_candidate),
        "admission": _first_matrix_difference(casa_admission, candidate_admission),
    }


def _science_rows(
    rows: list[dict[str, Any]],
    *,
    uvw_selector: str | None = None,
    broadcast_any_pol_flag: bool = False,
) -> list[dict[str, Any]]:
    transformed: list[dict[str, Any]] = []
    for row in rows:
        uvw_bits = row["uvw_bits"]
        if uvw_selector == "raw":
            uvw_bits = row["auxiliary"]["raw_uvw_bits"]
        elif uvw_selector == "negated_raw":
            uvw_bits = _negated_raw_uvw(row["auxiliary"]["raw_uvw_bits"])
        elif uvw_selector == "gridft_density":
            uvw_bits = row["auxiliary"]["gridft_density_uvw_bits"]
        elif uvw_selector == "negated_internal":
            uvw_bits = row["auxiliary"]["negated_uv_transform_uvw_bits"]
        masks = row["flag_masks"]
        if broadcast_any_pol_flag:
            masks = [15 if mask else 0 for mask in masks]
        transformed.append(
            {
                "row": row["row"],
                "row_flag": row["row_flag"],
                "uvw_bits": list(uvw_bits),
                "dphase_bits": row["dphase_bits"],
                "flag_masks": list(masks),
                "imaging_weight_bits": list(row["imaging_weight_bits"]),
            }
        )
    return transformed


def _hypothesis_summary(
    *,
    casa_header: dict[str, Any],
    casa_rows: list[dict[str, Any]],
    candidate_header: dict[str, Any],
    transformed_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    components, counts, calls, _, admission = recompute(
        candidate_header, transformed_rows
    )
    casa_admission = recompute(casa_header, casa_rows)[4]
    differences = _component_first_differences(
        casa_header,
        casa_rows,
        casa_admission,
        candidate_header,
        transformed_rows,
        admission,
    )
    return {
        "component_hashes": components,
        "counts": counts,
        "recomputed_frozen_hashes": _expected_claimed_calls(calls),
        "component_exact": {
            name: (
                components[name] == FROZEN_COMPONENT_HASHES[name]
                and differences[name] is None
            )
            for name in COMPONENT_NAMES
        },
        "stream_exact": all(
            call["stream_hash"] == FROZEN_CALLS[term]["stream_hash"]
            for term, call in enumerate(calls)
        ),
        "tt0_geometry_exact": calls[0]["geometry_hash"]
        == FROZEN_CALLS[0]["geometry_hash"],
        "derived_tt1_geometry_exact": calls[1]["geometry_hash"]
        == FROZEN_CALLS[1]["geometry_hash"],
        "first_differences": differences,
    }


def _row_checkpoint_comparison(
    casa: list[dict[str, int]],
    candidate: list[dict[str, int]],
) -> list[dict[str, Any]]:
    _require(
        len(casa) == len(candidate) == ROW_COUNT,
        "row checkpoint comparison requires exactly 325 rows",
    )
    fields = [name for name in CHECKPOINT_KEYS if name != "row"]
    result: list[dict[str, Any]] = []
    for row, (left, right) in enumerate(zip(casa, candidate, strict=True)):
        result.append(
            {
                "row": row,
                "exact": all(left[name] == right[name] for name in fields),
                "fields": {
                    name: {
                        "exact": left[name] == right[name],
                        "casa": left[name],
                        "candidate": right[name],
                    }
                    for name in fields
                },
            }
        )
    return result


def _first_row_channel_difference(
    differences: dict[str, dict[str, Any] | None],
) -> dict[str, Any] | None:
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    for component, difference in differences.items():
        if difference is None or "row" not in difference:
            continue
        channel = difference.get("channel", -1)
        candidates.append((difference["row"], channel, component, difference))
    if not candidates:
        return None
    _, _, component, difference = min(candidates)
    return {"component": component, **difference}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode()


def build_comparison(
    *,
    candidate_path: Path,
    casa_path: Path,
) -> dict[str, Any]:
    """Validate both receipts and classify exact component ownership."""

    (
        casa,
        casa_sha,
        casa_embedded_sha,
        casa_components,
        casa_counts,
        casa_calls,
        casa_checkpoints,
        casa_admission,
    ) = validate_casa(casa_path)
    (
        candidate,
        candidate_sha,
        candidate_embedded_sha,
        candidate_components,
        candidate_counts,
        candidate_calls,
        candidate_checkpoints,
        candidate_admission,
    ) = validate_candidate(candidate_path)
    casa_header = casa["header"]
    candidate_header = candidate["header"]
    casa_rows = casa["rows"]
    candidate_rows = candidate["rows"]
    differences = _component_first_differences(
        casa_header,
        casa_rows,
        casa_admission,
        candidate_header,
        candidate_rows,
        candidate_admission,
    )
    component_comparison = {
        name: {
            "owner": COMPONENT_OWNERS[name],
            "casa_hash": casa_components[name],
            "candidate_hash": candidate_components[name],
            "hash_exact": casa_components[name] == candidate_components[name],
            "values_exact": differences[name] is None,
            "exact": (
                casa_components[name] == candidate_components[name]
                and differences[name] is None
            ),
            "first_difference": differences[name],
        }
        for name in COMPONENT_NAMES
    }
    mismatched_components = [
        name for name in COMPONENT_NAMES if not component_comparison[name]["exact"]
    ]
    mismatch_owners = sorted({COMPONENT_OWNERS[name] for name in mismatched_components})
    calls_exact = candidate_calls == casa_calls
    classification = (
        "exact-frozen-casa-native-components"
        if not mismatched_components and calls_exact
        else "valid-native-component-mismatch"
    )

    flag_hypotheses = {
        "internal_four_polarization_masks": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(candidate_rows),
        ),
        "casa_any_polarization_broadcast": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(candidate_rows, broadcast_any_pol_flag=True),
        ),
    }
    uvw_hypotheses = {
        "internal": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(candidate_rows),
        ),
        "raw": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(candidate_rows, uvw_selector="raw"),
        ),
        "negated_raw": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(candidate_rows, uvw_selector="negated_raw"),
        ),
        "gridft_density": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(
                candidate_rows, uvw_selector="gridft_density"
            ),
        ),
        "negated_internal": _hypothesis_summary(
            casa_header=casa_header,
            casa_rows=casa_rows,
            candidate_header=candidate_header,
            transformed_rows=_science_rows(
                candidate_rows, uvw_selector="negated_internal"
            ),
        ),
    }
    checkpoints = _row_checkpoint_comparison(casa_checkpoints, candidate_checkpoints)
    comparison = {
        "schema": COMPARISON_SCHEMA,
        "status": "valid-classification",
        "classification": classification,
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "scope": {
            "field_id": 1525,
            "spw_id": 2,
            "selection_relative_rows": [0, ROW_COUNT],
            "channels": CHANNEL_COUNT,
            "polarizations": POLARIZATION_COUNT,
            "raw_slots_compared": ROW_COUNT * CHANNEL_COUNT,
            "actual_terms_observed": [],
            "recomputed_terms": [0, 1],
            "production_dispatch": "not-entered",
            "cf_cache": "not-opened",
            "grid_storage": "not-allocated",
            "grid_dispatch": "not-entered",
            "sumwt": "not-entered",
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
        },
        "parents": {
            "casa_native_components_v1": {
                "path": str(casa_path),
                "sha256": casa_sha,
                "embedded_evidence_sha256": casa_embedded_sha,
                "schema": CASA_EVIDENCE_SCHEMA,
            }
        },
        "candidate": {
            "path": str(candidate_path),
            "sha256": candidate_sha,
            "embedded_evidence_sha256": candidate_embedded_sha,
            "schema": CANDIDATE_EVIDENCE_SCHEMA,
            "result": candidate["result"],
        },
        "component_comparison": component_comparison,
        "mismatched_components": mismatched_components,
        "mismatch_owners": mismatch_owners,
        "first_row_channel_difference": _first_row_channel_difference(differences),
        "counts": {
            "casa": casa_counts,
            "candidate": candidate_counts,
            "exact": casa_counts == candidate_counts,
        },
        "admission": {
            "membership_exact": casa_admission == candidate_admission,
            "count_exact": (
                casa_counts["admitted_channels"]
                == candidate_counts["admitted_channels"]
            ),
            "first_difference": differences["admission"],
        },
        "boundary_comparison": [
            {
                "term": term,
                "source_count_exact": (
                    casa_calls[term]["source_count"]
                    == candidate_calls[term]["source_count"]
                ),
                "stream_exact": (
                    casa_calls[term]["stream_hash"]
                    == candidate_calls[term]["stream_hash"]
                ),
                "geometry_exact": (
                    casa_calls[term]["geometry_hash"]
                    == candidate_calls[term]["geometry_hash"]
                ),
                "casa": {"origin": EXPECTED_ORIGINS[term], **casa_calls[term]},
                "candidate": {
                    "origin": EXPECTED_ORIGINS[term],
                    **candidate_calls[term],
                },
            }
            for term in range(2)
        ],
        "row_checkpoints": {
            "count": ROW_COUNT,
            "casa": casa_checkpoints,
            "candidate": candidate_checkpoints,
            "comparison": checkpoints,
        },
        "flag_hypotheses": flag_hypotheses,
        "uvw_hypotheses": uvw_hypotheses,
        "claims": {
            "frozen_casa_whole_file_sha256_exact": True,
            "raw_embedded_evidence_digests_exact": True,
            "candidate_components_independently_recomputed": True,
            "candidate_stream_and_geometries_independently_recomputed": True,
            "candidate_row_checkpoints_independently_recomputed": True,
            "admission_membership_compared_slot_by_slot": True,
            "matching_counts_used_as_membership_proof": False,
            "mismatch_is_valid_evidence": classification
            == "valid-native-component-mismatch",
            "formed_image": False,
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
    """Publish without replacing an existing result."""

    _require(path.is_absolute(), f"comparison output must be absolute: {path}")
    _require(not path.exists(), f"refusing to overwrite comparison: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
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
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--casa-native-components", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        comparison = build_comparison(
            candidate_path=arguments.candidate,
            casa_path=arguments.casa_native_components,
        )
        atomic_write_json(arguments.output, comparison)
    except (ContractError, OSError) as error:
        print(f"casa-rs native-components validation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(comparison["comparison"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
