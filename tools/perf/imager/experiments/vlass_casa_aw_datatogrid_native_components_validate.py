#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Strictly validate the bounded CASA AW native-component oracle.

The producer is a diagnostic interposer.  This file deliberately shares no
hashing implementation with it: it rebuilds CASA 6.7.5.18's frozen v5 STREAM
and TT0/TT1 GEOMETRY byte streams from the published raw bit arrays.  A zero
exit status means only that the first native visibility buffer exactly
recomposes the frozen v5 boundary without entering the original DataToGrid
implementation.
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


CASA_V5_RECEIPT_SHA256 = (
    "fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f"
)
CASA_SOURCE_COMMIT = "418bb1a26df7c4aba663ff123b038b75a6fa0295"
CASACORE_SOURCE_COMMIT = "25b653f6963a78a1dcfc8e16954081e091a50fbe"
DATATOGRID_DCOMPLEX_SYMBOL = (
    "__ZN4casa5refim14AWVisResampler16DataToGridImpl_p"
    "INSt3__17complexIdEEEEvRN8casacore5ArrayIT_EERNS0_7VBStoreERNS6_6"
    "MatrixIdEERKbb"
)

CANDIDATE_ENVELOPE_SCHEMA = "casa-aw-datagrid-native-components-envelope-v1"
CANDIDATE_EVIDENCE_SCHEMA = "casa-aw-datagrid-native-components-v1"
COMPARISON_ENVELOPE_SCHEMA = "casa-aw-datagrid-native-components-comparison-envelope-v1"
COMPARISON_SCHEMA = "casa-aw-datagrid-native-components-comparison-v1"

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x00000100000001B3
U64_MASK = 0xFFFFFFFFFFFFFFFF
U32_MASK = 0xFFFFFFFF

SOURCE_COUNT = 12_359
STREAM_HASH = 4_740_440_223_154_359_747
GEOMETRY_HASHES = [
    15_079_793_846_523_608_377,
    14_381_099_959_812_707_833,
]
IM_REF_FREQ_BITS = 4_748_556_467_228_999_524
GRID_SHAPE = [4096, 4096, 1, 1]
ROW_COUNT = 325
CHANNEL_COUNT = 64
POLARIZATION_COUNT = 4

EXPECTED_FIRST_VB = {
    "begin_row": 0,
    "end_row": 325,
    "n_row": 325,
    "spw_id": 2,
    "row_ids_count": 325,
    "row_ids_hash": 15_058_004_568_616_189_240,
    "row_id_first": 0,
    "row_id_last": 324,
    "row_ids": list(range(325)),
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
    "row_flags_count": 325,
    "row_flags_hash": 3_526_571_572_021_233_857,
    "flagged_rows": 48,
}
EXPECTED_V5_CALLS = [
    {
        "call": 0,
        "block": 0,
        "term": 0,
        "source_count": SOURCE_COUNT,
        "stream_hash": STREAM_HASH,
        "geometry_hash": GEOMETRY_HASHES[0],
        "input_hash": 149_759_612_434_800_605,
    },
    {
        "call": 1,
        "block": 0,
        "term": 1,
        "source_count": SOURCE_COUNT,
        "stream_hash": STREAM_HASH,
        "geometry_hash": GEOMETRY_HASHES[1],
        "input_hash": 6_114_644_945_970_596_433,
    },
]
EXPECTED_V5_BOUNDARIES = [
    {
        "block": 0,
        "stream_hash": STREAM_HASH,
        "input_stream_hash": 9_670_878_879_986_980_654,
        "terms": [
            {
                "term": 0,
                "grid_hash": 9_328_098_071_914_194_885,
                "sumwt_hash": 5_773_668_711_911_205_477,
                "grid_values_hashed": 16_777_216,
                "sumwt_values_hashed": 1,
            },
            {
                "term": 1,
                "grid_hash": 9_296_706_034_202_754_823,
                "sumwt_hash": 6_979_414_366_695_050_184,
                "grid_values_hashed": 16_777_216,
                "sumwt_values_hashed": 1,
            },
        ],
    }
]

HASH_CONTRACTS = {
    "algorithm": "fnv1a64",
    "offset_basis": FNV_OFFSET,
    "prime": FNV_PRIME,
    "integer_encoding": "little-endian",
    "float_encoding": "ieee754-bits-little-endian",
    "boolean_encoding": "one-byte-0-or-1",
    "recomposition": "casa-6.7.5.18-bracket-hash-call-inputs",
}
FROZEN_PARENT_RECEIPTS = {
    "casa_v5": {
        "schema": "casa-aw-datagrid-bracket-v1",
        "receipt_sha256": CASA_V5_RECEIPT_SHA256,
    }
}

ENVELOPE_KEYS = frozenset({"schema", "content_address", "evidence"})
CONTENT_ADDRESS_KEYS = frozenset({"algorithm", "scope", "digest"})
EVIDENCE_KEYS = frozenset(
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
ROW_KEYS = frozenset(
    {
        "row",
        "row_flag",
        "uvw_bits",
        "dphase_bits",
        "flag_masks",
        "imaging_weight_bits",
    }
)
COMPONENT_HASH_KEYS = frozenset(
    {
        "header",
        "row_ids",
        "channel_map",
        "polarization_map",
        "frequencies",
        "row_flags",
        "uvw_dphase",
        "flag_masks",
        "imaging_weights",
        "admission",
    }
)
COUNT_KEYS = frozenset(
    {
        "flagged_rows",
        "zero_imaging_weights",
        "nonzero_imaging_weights",
        "admitted_channels",
    }
)
RECOMPUTED_HASH_KEYS = frozenset(
    {
        "call",
        "block",
        "term",
        "origin",
        "source_count",
        "stream_hash",
        "geometry_hash",
    }
)
EXPECTED_ORIGINS = [
    "observed-first-tt0",
    "derived-from-observed-tt0-under-frozen-v5-contract",
]


class ContractError(RuntimeError):
    """Raised when an oracle artifact violates the frozen contract."""


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
    _require(0 <= word <= U64_MASK, f"{label} is not a u64")
    return word


def _u32(value: Any, label: str) -> int:
    word = _integer(value, label)
    _require(0 <= word <= U32_MASK, f"{label} is not a u32")
    return word


def _require_exact_keys(
    value: dict[str, Any], expected: frozenset[str], label: str
) -> None:
    observed = frozenset(value)
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    _require(
        not missing and not unexpected,
        f"{label} key set changed: missing={missing!r} unexpected={unexpected!r}",
    )


def _json_exact(observed: Any, expected: Any) -> bool:
    if type(observed) is not type(expected):
        return False
    if type(expected) is dict:
        return observed.keys() == expected.keys() and all(
            _json_exact(observed[key], expected[key]) for key in expected
        )
    if type(expected) is list:
        return len(observed) == len(expected) and all(
            _json_exact(left, right)
            for left, right in zip(observed, expected, strict=True)
        )
    return observed == expected


def _exact(observed: Any, expected: Any, label: str) -> None:
    _require(
        _json_exact(observed, expected),
        f"{label} changed: {observed!r} != {expected!r}",
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
                raise ContractError(f"{path}: duplicate JSON object key {key!r}")
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
        _require(type(key) is str, f"{source}: top-level name must be a string")
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
    _require(len(matches) == 1, f"{source}: expected one top-level {member!r}")
    start, end = matches[0]
    return text[start:end].encode()


class Fnv1a64:
    """Byte-exact FNV-1a with CASA's little-endian primitive encodings."""

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


def _hash_counted_f64_bits(values: list[int]) -> int:
    return _hash_counted_u64(values)


def _hash_counted_bools(values: list[bool]) -> int:
    digest = Fnv1a64()
    digest.u64(len(values))
    for value in values:
        digest.boolean(value)
    return digest.value


def _weight_nonzero(bits: int) -> bool:
    value = struct.unpack("<f", bits.to_bytes(4, "little"))[0]
    return value != 0.0


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
    _require_exact_keys(header, HEADER_KEYS, label)
    _exact(header["use_conjugate_frequency_cf"], False, f"{label}.use_conjugate")
    for key, expected in {
        "begin_row": 0,
        "end_row": ROW_COUNT,
        "n_row": ROW_COUNT,
        "spw_id": 2,
        "im_ref_freq_bits": IM_REF_FREQ_BITS,
        "grid_shape": GRID_SHAPE,
        "channel_map": [0] * CHANNEL_COUNT,
        "polarization_map": [0, -1, -1, 0],
        "row_ids": list(range(ROW_COUNT)),
    }.items():
        _exact(header[key], expected, f"{label}.{key}")
    frequencies = _sequence(header["frequency_bits"], f"{label}.frequency_bits")
    _require(
        len(frequencies) == CHANNEL_COUNT,
        f"{label}.frequency_bits must contain exactly {CHANNEL_COUNT} values",
    )
    for ordinal, bits in enumerate(frequencies):
        _u64(bits, f"{label}.frequency_bits[{ordinal}]")
    return header


def _validate_rows(value: Any) -> list[dict[str, Any]]:
    rows = _sequence(value, "candidate rows")
    _require(len(rows) == ROW_COUNT, f"candidate rows must contain {ROW_COUNT} rows")
    validated: list[dict[str, Any]] = []
    for ordinal, raw_row in enumerate(rows):
        label = f"candidate rows[{ordinal}]"
        row = _mapping(raw_row, label)
        _require_exact_keys(row, ROW_KEYS, label)
        _exact(row["row"], ordinal, f"{label}.row")
        _require(type(row["row_flag"]) is bool, f"{label}.row_flag must be boolean")
        uvw = _sequence(row["uvw_bits"], f"{label}.uvw_bits")
        _require(len(uvw) == 3, f"{label}.uvw_bits must contain three values")
        for axis, bits in enumerate(uvw):
            _u64(bits, f"{label}.uvw_bits[{axis}]")
        _u64(row["dphase_bits"], f"{label}.dphase_bits")
        masks = _sequence(row["flag_masks"], f"{label}.flag_masks")
        _require(
            len(masks) == CHANNEL_COUNT,
            f"{label}.flag_masks must contain {CHANNEL_COUNT} values",
        )
        for channel, mask in enumerate(masks):
            _require(
                0 <= _integer(mask, f"{label}.flag_masks[{channel}]") < 16,
                f"{label}.flag_masks[{channel}] uses bits outside 0..3",
            )
        weights = _sequence(row["imaging_weight_bits"], f"{label}.imaging_weight_bits")
        _require(
            len(weights) == CHANNEL_COUNT,
            f"{label}.imaging_weight_bits must contain {CHANNEL_COUNT} values",
        )
        for channel, bits in enumerate(weights):
            _u32(bits, f"{label}.imaging_weight_bits[{channel}]")
        validated.append(row)
    return validated


def recompute_components(
    header: dict[str, Any], rows: list[dict[str, Any]]
) -> tuple[
    dict[str, int],
    dict[str, int],
    list[dict[str, int]],
    list[dict[str, Any]],
]:
    """Recompute all component and frozen v5 hashes from raw published bits."""

    common_header = Fnv1a64()
    _hash_common_header(common_header, header)
    row_flags = [row["row_flag"] for row in rows]

    uvw_dphase = Fnv1a64()
    uvw_dphase.u64(ROW_COUNT)
    flag_masks = Fnv1a64()
    flag_masks.u64(ROW_COUNT)
    flag_masks.u64(CHANNEL_COUNT)
    flag_masks.u64(POLARIZATION_COUNT)
    imaging_weights = Fnv1a64()
    imaging_weights.u64(ROW_COUNT)
    imaging_weights.u64(CHANNEL_COUNT)
    admission = Fnv1a64()
    admission.u64(ROW_COUNT)
    admission.u64(CHANNEL_COUNT)
    row_flags_cumulative = Fnv1a64()
    row_flags_cumulative.u64(ROW_COUNT)

    stream = Fnv1a64()
    _hash_common_header(stream, header)
    geometries = [Fnv1a64(), Fnv1a64()]
    for ordinal, geometry in enumerate(geometries):
        geometry.u64(ordinal)
        geometry.u64(0)
        geometry.u64(ordinal)
        _hash_common_header(geometry, header)

    flagged_rows = 0
    zero_weights = 0
    nonzero_weights = 0
    admitted_channels = 0
    checkpoints: list[dict[str, Any]] = []

    for ordinal, row in enumerate(rows):
        row_flag = row["row_flag"]
        row_flags_cumulative.boolean(row_flag)
        flagged_rows += int(row_flag)

        uvw_dphase.u64(ordinal)
        for bits in row["uvw_bits"]:
            uvw_dphase.u64(bits)
        uvw_dphase.u64(row["dphase_bits"])

        stream.u64(ordinal)
        stream.boolean(row_flag)
        for geometry in geometries:
            geometry.u64(ordinal)
            geometry.boolean(row_flag)

        for channel in range(CHANNEL_COUNT):
            mask = row["flag_masks"][channel]
            weight_bits = row["imaging_weight_bits"][channel]
            flag_masks.u64(ordinal)
            flag_masks.u64(channel)
            for polarization in range(POLARIZATION_COUNT):
                flag_masks.boolean(bool(mask & (1 << polarization)))
            imaging_weights.u64(ordinal)
            imaging_weights.u64(channel)
            imaging_weights.u32(weight_bits)

            target = header["channel_map"][channel]
            target_valid = 0 <= target < header["grid_shape"][3]
            weight_nonzero = _weight_nonzero(weight_bits)
            admitted = not row_flag and target_valid and weight_nonzero
            admission.u64(ordinal)
            admission.u64(channel)
            admission.boolean(not row_flag)
            admission.boolean(target_valid)
            admission.boolean(weight_nonzero)
            admission.boolean(admitted)
            if weight_nonzero:
                nonzero_weights += 1
            else:
                zero_weights += 1

        if not row_flag:
            for bits in row["uvw_bits"]:
                stream.u64(bits)
                for geometry in geometries:
                    geometry.u64(bits)
            stream.u64(row["dphase_bits"])
            for geometry in geometries:
                geometry.u64(row["dphase_bits"])
            for channel in range(CHANNEL_COUNT):
                target = header["channel_map"][channel]
                if target < 0 or target >= header["grid_shape"][3]:
                    continue
                stream.u64(channel)
                stream.u64(header["frequency_bits"][channel])
                mask = row["flag_masks"][channel]
                for polarization in range(POLARIZATION_COUNT):
                    stream.boolean(bool(mask & (1 << polarization)))
                if not _weight_nonzero(row["imaging_weight_bits"][channel]):
                    continue
                for geometry in geometries:
                    geometry.u64(admitted_channels)
                    geometry.u64(channel)
                    geometry.u64(header["frequency_bits"][channel])
                    for polarization in range(POLARIZATION_COUNT):
                        geometry.boolean(bool(mask & (1 << polarization)))
                admitted_channels += 1

        checkpoints.append(
            {
                "row": ordinal,
                "source_count": admitted_channels,
                "stream_hash": stream.value,
                "tt0_geometry_hash": geometries[0].value,
                "tt1_geometry_hash": geometries[1].value,
                "row_flags_hash": row_flags_cumulative.value,
                "uvw_dphase_hash": uvw_dphase.value,
                "flag_masks_hash": flag_masks.value,
                "imaging_weights_hash": imaging_weights.value,
                "admission_hash": admission.value,
            }
        )

    components = {
        "header": common_header.value,
        "row_ids": _hash_counted_u64(header["row_ids"]),
        "channel_map": _hash_counted_u64(header["channel_map"]),
        "polarization_map": _hash_counted_u64(header["polarization_map"]),
        "frequencies": _hash_counted_f64_bits(header["frequency_bits"]),
        "row_flags": _hash_counted_bools(row_flags),
        "uvw_dphase": uvw_dphase.value,
        "flag_masks": flag_masks.value,
        "imaging_weights": imaging_weights.value,
        "admission": admission.value,
    }
    counts = {
        "flagged_rows": flagged_rows,
        "zero_imaging_weights": zero_weights,
        "nonzero_imaging_weights": nonzero_weights,
        "admitted_channels": admitted_channels,
    }
    calls = [
        {
            "call": ordinal,
            "block": 0,
            "term": ordinal,
            "source_count": admitted_channels,
            "stream_hash": stream.value,
            "geometry_hash": geometries[ordinal].value,
        }
        for ordinal in range(2)
    ]
    return components, counts, calls, checkpoints


def validate_frozen_casa_v5(path: Path) -> dict[str, Any]:
    """Validate the exact frozen CASA-v5 whole file and semantic boundary."""

    _exact(
        sha256_path(path),
        CASA_V5_RECEIPT_SHA256,
        f"{path}: frozen CASA v5 whole-file SHA-256",
    )
    _, receipt = _load_json_strict(path)
    expected_top_keys = frozenset(
        {
            "schema",
            "status",
            "reason",
            "role",
            "casa_version",
            "casa_version_string",
            "casa_source_commit",
            "exit_code",
            "original_invocation",
            "dispatch_identity",
            "probe_serialization",
            "formed_image",
            "normalization",
            "fft",
            "expected_grid_nxy",
            "target_blocks",
            "terms_per_block",
            "completed_calls",
            "completed_blocks",
            "input_stream_hash",
            "native_first_vb",
            "calls",
            "block_boundaries",
        }
    )
    _require_exact_keys(receipt, expected_top_keys, f"{path}: receipt")
    for key, expected in {
        "schema": "casa-aw-datagrid-bracket-v1",
        "status": "completed-before-finalize",
        "reason": "configured-block-boundary",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "casa_version": "6.7.5.18",
        "casa_version_string": "6.7.5-18",
        "casa_source_commit": CASA_SOURCE_COMMIT,
        "exit_code": 86,
        "original_invocation": "two-level-bound-exact-DComplex-specialization",
        "dispatch_identity": "stable-grid-storage-and-source-stream",
        "probe_serialization": "global-mutex",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "expected_grid_nxy": 4096,
        "target_blocks": 1,
        "terms_per_block": 2,
        "completed_calls": 2,
        "completed_blocks": 1,
        "input_stream_hash": 9_670_878_879_986_980_654,
    }.items():
        _exact(receipt[key], expected, f"{path}: {key}")
    native = _mapping(receipt["native_first_vb"], f"{path}: native_first_vb")
    _require_exact_keys(
        native, frozenset(EXPECTED_FIRST_VB), f"{path}: native_first_vb"
    )
    _exact(native, EXPECTED_FIRST_VB, f"{path}: native_first_vb")
    _exact(receipt["calls"], EXPECTED_V5_CALLS, f"{path}: calls")
    _exact(
        receipt["block_boundaries"],
        EXPECTED_V5_BOUNDARIES,
        f"{path}: block_boundaries",
    )
    return receipt


def _validate_candidate_envelope(
    path: Path,
) -> tuple[dict[str, Any], str, str]:
    payload, envelope = _load_json_strict(path)
    _require_exact_keys(envelope, ENVELOPE_KEYS, f"{path}: envelope")
    _exact(envelope["schema"], CANDIDATE_ENVELOPE_SCHEMA, f"{path}: schema")
    address = _mapping(envelope["content_address"], f"{path}: content_address")
    _require_exact_keys(address, CONTENT_ADDRESS_KEYS, f"{path}: content_address")
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
    _require_exact_keys(evidence, EVIDENCE_KEYS, f"{path}: evidence")
    return evidence, sha256_path(path), embedded_sha


def validate_candidate(
    path: Path,
) -> tuple[
    dict[str, Any],
    str,
    str,
    dict[str, int],
    dict[str, int],
    list[dict[str, int]],
    list[dict[str, Any]],
]:
    """Strictly validate and independently recompose one candidate receipt."""

    evidence, receipt_sha, embedded_sha = _validate_candidate_envelope(path)
    for key, expected in {
        "schema": CANDIDATE_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": "completed-native-components-exact-frozen-v5",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "CASA",
        "casa_version": "6.7.5.18",
        "casa_version_string": "6.7.5-18",
        "casa_source_commit": CASA_SOURCE_COMMIT,
        "casacore_source_commit": CASACORE_SOURCE_COMMIT,
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
        "hash_contracts": HASH_CONTRACTS,
        "frozen_parent_receipts": FROZEN_PARENT_RECEIPTS,
    }.items():
        _exact(evidence[key], expected, f"{path}: evidence.{key}")
    _exact(
        evidence["datatogrid_symbol"],
        DATATOGRID_DCOMPLEX_SYMBOL,
        f"{path}: evidence.datatogrid_symbol",
    )

    header = _validate_header(evidence["header"], f"{path}: evidence.header")
    rows = _validate_rows(evidence["rows"])
    components, counts, calls, checkpoints = recompute_components(header, rows)

    claimed_components = _mapping(
        evidence["component_hashes"], f"{path}: evidence.component_hashes"
    )
    _require_exact_keys(
        claimed_components,
        COMPONENT_HASH_KEYS,
        f"{path}: evidence.component_hashes",
    )
    for key in COMPONENT_HASH_KEYS:
        _u64(claimed_components[key], f"{path}: component_hashes.{key}")
    _exact(
        claimed_components,
        components,
        f"{path}: independently recomputed component_hashes",
    )

    claimed_counts = _mapping(evidence["counts"], f"{path}: evidence.counts")
    _require_exact_keys(claimed_counts, COUNT_KEYS, f"{path}: evidence.counts")
    _exact(claimed_counts, counts, f"{path}: independently recomputed counts")

    claimed_calls = _sequence(
        evidence["recomputed_frozen_hashes"],
        f"{path}: evidence.recomputed_frozen_hashes",
    )
    _require(
        len(claimed_calls) == 2,
        f"{path}: recomputed_frozen_hashes must contain TT0 and TT1",
    )
    expected_claims: list[dict[str, Any]] = []
    for ordinal, call in enumerate(calls):
        raw_claim = _mapping(
            claimed_calls[ordinal],
            f"{path}: evidence.recomputed_frozen_hashes[{ordinal}]",
        )
        _require_exact_keys(
            raw_claim,
            RECOMPUTED_HASH_KEYS,
            f"{path}: evidence.recomputed_frozen_hashes[{ordinal}]",
        )
        expected_claims.append({"origin": EXPECTED_ORIGINS[ordinal], **call})
    _exact(
        claimed_calls,
        expected_claims,
        f"{path}: independently recomputed frozen hashes",
    )

    _exact(counts["flagged_rows"], 48, f"{path}: flagged row count")
    _exact(counts["admitted_channels"], SOURCE_COUNT, f"{path}: source count")
    for ordinal, call in enumerate(calls):
        _exact(call["source_count"], SOURCE_COUNT, f"{path}: TT{ordinal} source count")
        _exact(call["stream_hash"], STREAM_HASH, f"{path}: TT{ordinal} STREAM")
        _exact(
            call["geometry_hash"],
            GEOMETRY_HASHES[ordinal],
            f"{path}: TT{ordinal} GEOMETRY",
        )

    _exact(
        components["row_ids"],
        EXPECTED_FIRST_VB["row_ids_hash"],
        f"{path}: row-ID hash",
    )
    _exact(
        components["channel_map"],
        EXPECTED_FIRST_VB["chan_map_hash"],
        f"{path}: channel-map hash",
    )
    _exact(
        components["polarization_map"],
        EXPECTED_FIRST_VB["pol_map_hash"],
        f"{path}: polarization-map hash",
    )
    _exact(
        components["frequencies"],
        EXPECTED_FIRST_VB["freq_hash"],
        f"{path}: frequency hash",
    )
    _exact(
        components["row_flags"],
        EXPECTED_FIRST_VB["row_flags_hash"],
        f"{path}: row-flag hash",
    )
    _exact(
        header["frequency_bits"][0],
        EXPECTED_FIRST_VB["freq_first_bits"],
        f"{path}: first frequency bits",
    )
    _exact(
        header["frequency_bits"][-1],
        EXPECTED_FIRST_VB["freq_last_bits"],
        f"{path}: last frequency bits",
    )
    return (
        evidence,
        receipt_sha,
        embedded_sha,
        components,
        counts,
        calls,
        checkpoints,
    )


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode()


def build_comparison(*, candidate_path: Path, casa_v5_path: Path) -> dict[str, Any]:
    """Validate both inputs and build the content-addressed exact comparison."""

    validate_frozen_casa_v5(casa_v5_path)
    (
        candidate,
        candidate_sha,
        embedded_sha,
        components,
        counts,
        calls,
        checkpoints,
    ) = validate_candidate(candidate_path)
    comparison = {
        "schema": COMPARISON_SCHEMA,
        "status": "valid-classification",
        "classification": "exact-frozen-v5-native-components",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "scope": {
            "field_id": 1525,
            "spw_id": 2,
            "selection_relative_rows": [0, ROW_COUNT],
            "channels": CHANNEL_COUNT,
            "polarizations": POLARIZATION_COUNT,
            "actual_terms_observed": [0],
            "recomputed_terms": [0, 1],
            "original_datatogrid": "not-invoked",
            "grid_dispatch": "not-entered",
            "sumwt": "not-read-or-written",
            "normalization": "not-entered",
            "fft": "not-entered",
            "products": "not-entered",
        },
        "parents": {
            "casa_v5": {
                "path": str(casa_v5_path),
                "sha256": CASA_V5_RECEIPT_SHA256,
                "schema": "casa-aw-datagrid-bracket-v1",
                "source_commit": CASA_SOURCE_COMMIT,
            }
        },
        "candidate": {
            "path": str(candidate_path),
            "sha256": candidate_sha,
            "embedded_evidence_sha256": embedded_sha,
            "schema": CANDIDATE_EVIDENCE_SCHEMA,
            "result": candidate["result"],
        },
        "component_hashes": components,
        "counts": counts,
        "recomputed_frozen_hashes": [
            {"origin": EXPECTED_ORIGINS[ordinal], **call}
            for ordinal, call in enumerate(calls)
        ],
        "row_checkpoints": checkpoints,
        "claims": {
            "frozen_casa_v5_whole_file_exact": True,
            "raw_embedded_evidence_digest_exact": True,
            "source_lineage_exact": True,
            "component_hashes_independently_exact": True,
            "source_count_exact": True,
            "stream_hash_exact": True,
            "recomputed_tt0_geometry_hash_exact": True,
            "recomputed_tt1_geometry_hash_exact": True,
            "tt1_observed": False,
            "original_datatogrid_invoked": False,
            "grid_storage_read_or_written": False,
            "formed_image": False,
            "performance_evidence": False,
            "integrated_4096_row_promoted": False,
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
    """Publish JSON atomically without replacing any existing destination."""

    _require(path.is_absolute(), f"comparison output must be absolute: {path}")
    _require(not path.exists(), f"refusing to overwrite comparison: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
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
    parser.add_argument("--casa-v5", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        comparison = build_comparison(
            candidate_path=arguments.candidate,
            casa_v5_path=arguments.casa_v5,
        )
        atomic_write_json(arguments.output, comparison)
    except (ContractError, OSError) as error:
        print(f"native-components validation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(comparison["comparison"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
