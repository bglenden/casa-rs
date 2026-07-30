#!/usr/bin/env python3
"""Compare one CASA AW degrid prefix trace with a casa-rs AWDGRD03 dump.

The CASA trace is produced by ``casa_aw_degrid_prefix_trace.patch``.  The
binary dump contains the phase-applied compact taps and model-grid cells used
by casa-rs for the same footprint.  This tool is a diagnostic: it identifies
the first bit-level divergence without treating the result as promotion
evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import struct
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from vlass_degrid_phase_arithmetic import (
    complex_add,
    complex_divide,
    complex_multiply,
    encoded,
    f32,
    read_dump,
)


TAP_COMPLEX_FIELDS = (
    "raw_cf",
    "post_w_cf",
    "phase",
    "post_phase_cf",
    "grid",
    "product",
    "norm",
    "accumulator",
)
RESULT_COMPLEX_FIELDS = (
    "pre_phasor",
    "phasor",
    "post_phasor",
    "normalization",
    "prediction",
)


def f32_from_bits(bits: int) -> np.float32:
    if not 0 <= bits <= 0xFFFF_FFFF:
        raise ValueError(f"Float32 bits are outside uint32: {bits}")
    return np.uint32(bits).view(np.float32)


def f32_bits(value: float) -> int:
    return int(f32(value).view(np.uint32))


def complex_from_bits(record: dict[str, str], label: str) -> complex:
    try:
        real = int(record[f"{label}_re"], 10)
        imag = int(record[f"{label}_im"], 10)
    except KeyError as error:
        raise ValueError(f"trace record is missing {error.args[0]}") from error
    return complex(f32_from_bits(real), f32_from_bits(imag))


def complex_conjugate(value: complex) -> complex:
    return complex(f32(value.real), f32(-f32(value.imag)))


def ordered_f32_bits(bits: int) -> int:
    """Map IEEE Float32 bits to an integer ordered by numerical value."""

    if bits & 0x8000_0000:
        return (~bits) & 0xFFFF_FFFF
    return bits | 0x8000_0000


def scalar_difference(expected: float, actual: float) -> dict[str, Any]:
    expected_bits = f32_bits(expected)
    actual_bits = f32_bits(actual)
    expected_value = float(f32(expected))
    actual_value = float(f32(actual))
    finite = math.isfinite(expected_value) and math.isfinite(actual_value)
    return {
        "exact": expected_bits == actual_bits,
        "expected_value": expected_value,
        "actual_value": actual_value,
        "expected_bits": expected_bits,
        "actual_bits": actual_bits,
        "bit_xor": expected_bits ^ actual_bits,
        "signed_ulp_delta": (
            ordered_f32_bits(actual_bits) - ordered_f32_bits(expected_bits)
            if finite
            else None
        ),
        "absolute_ulp_distance": (
            abs(ordered_f32_bits(actual_bits) - ordered_f32_bits(expected_bits))
            if finite
            else None
        ),
    }


def complex_difference(expected: complex, actual: complex) -> dict[str, Any]:
    real = scalar_difference(expected.real, actual.real)
    imag = scalar_difference(expected.imag, actual.imag)
    return {
        "exact": real["exact"] and imag["exact"],
        "expected": encoded(expected),
        "actual": encoded(actual),
        "components": {"real": real, "imag": imag},
    }


def parse_trace_record(line: str, line_number: int) -> tuple[str, dict[str, str]]:
    tokens = line.split()
    if not tokens:
        raise ValueError(f"empty trace record at line {line_number}")
    values: dict[str, str] = {}
    for token in tokens[1:]:
        if "=" not in token:
            raise ValueError(
                f"trace token lacks '=' at line {line_number}: {token!r}"
            )
        key, value = token.split("=", 1)
        if key in values:
            raise ValueError(f"duplicate trace key {key!r} at line {line_number}")
        values[key] = value
    return tokens[0], values


def parse_casa_trace(path: Path) -> dict[str, Any]:
    meta: dict[str, str] | None = None
    result: dict[str, str] | None = None
    taps: list[dict[str, str]] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line:
            continue
        kind, values = parse_trace_record(line, line_number)
        if kind == "meta":
            if meta is not None:
                raise ValueError("CASA trace contains more than one meta record")
            meta = values
        elif kind == "tap":
            taps.append(values)
        elif kind == "result":
            if result is not None:
                raise ValueError("CASA trace contains more than one result record")
            result = values
        else:
            raise ValueError(f"unknown CASA trace record {kind!r} at line {line_number}")
    if meta is None:
        raise ValueError("CASA trace has no meta record")
    if result is None:
        raise ValueError("CASA trace has no result record")
    for field in (
        "loc_x",
        "loc_y",
        "support_x",
        "support_y",
        "sampling_x",
        "sampling_y",
        "data_w_m",
    ):
        if field not in meta:
            raise ValueError(f"CASA meta record is missing {field}")
    for index, tap in enumerate(taps):
        for field in ("index", "iy", "ix", "grid_x", "grid_y"):
            if field not in tap:
                raise ValueError(f"CASA tap {index} is missing {field}")
        for field in TAP_COMPLEX_FIELDS:
            complex_from_bits(tap, field)
    for field in RESULT_COMPLEX_FIELDS:
        complex_from_bits(result, field)
    return {"meta": meta, "taps": taps, "result": result}


@dataclass
class DifferenceLedger:
    counts: Counter[str]
    component_counts: Counter[str]
    first: dict[str, Any] | None = None

    @classmethod
    def create(cls) -> "DifferenceLedger":
        return cls(Counter(), Counter())

    def integer(
        self,
        *,
        category: str,
        field: str,
        expected: int,
        actual: int,
        tap_index: int | None = None,
    ) -> None:
        if expected == actual:
            return
        self.counts[field] += 1
        if self.first is None:
            self.first = {
                "category": category,
                "field": field,
                "tap_index": tap_index,
                "expected": expected,
                "actual": actual,
            }

    def complex(
        self,
        *,
        category: str,
        field: str,
        expected: complex,
        actual: complex,
        tap_index: int | None = None,
    ) -> dict[str, Any]:
        difference = complex_difference(expected, actual)
        if difference["exact"]:
            return difference
        self.counts[field] += 1
        for component in ("real", "imag"):
            if not difference["components"][component]["exact"]:
                self.component_counts[f"{field}.{component}"] += 1
        if self.first is None:
            first_component = next(
                component
                for component in ("real", "imag")
                if not difference["components"][component]["exact"]
            )
            self.first = {
                "category": category,
                "field": field,
                "component": first_component,
                "tap_index": tap_index,
                "difference": difference["components"][first_component],
                "expected_complex": difference["expected"],
                "actual_complex": difference["actual"],
            }
        return difference

    def summary(self) -> dict[str, Any]:
        return {
            "exact": not self.counts,
            "first_divergence": self.first,
            "mismatch_counts": dict(sorted(self.counts.items())),
            "component_mismatch_counts": dict(sorted(self.component_counts.items())),
        }


def integer_value(record: dict[str, str], field: str) -> int:
    try:
        return int(record[field], 10)
    except KeyError as error:
        raise ValueError(f"trace record is missing {field}") from error


def compare_prefix(trace_path: Path, dump_path: Path) -> dict[str, Any]:
    trace = parse_casa_trace(trace_path)
    dump = read_dump(dump_path)
    if dump["sampling"] is None:
        raise ValueError("prefix comparison requires an AWDGRD03 phase-aware dump")

    meta = trace["meta"]
    taps = trace["taps"]
    result_record = trace["result"]
    x_support = int(dump["support"][0])
    y_support = int(dump["support"][1])
    expected_tap_count = (2 * x_support + 1) * (2 * y_support + 1)
    data_w_m = float(meta["data_w_m"])

    cross = DifferenceLedger.create()
    internal = DifferenceLedger.create()

    cross.integer(
        category="geometry",
        field="loc_x",
        expected=int(dump["loc"][0]),
        actual=integer_value(meta, "loc_x"),
    )
    cross.integer(
        category="geometry",
        field="loc_y",
        expected=int(dump["loc"][1]),
        actual=integer_value(meta, "loc_y"),
    )
    cross.integer(
        category="geometry",
        field="support_x",
        expected=x_support,
        actual=integer_value(meta, "support_x"),
    )
    cross.integer(
        category="geometry",
        field="support_y",
        expected=y_support,
        actual=integer_value(meta, "support_y"),
    )
    cross.integer(
        category="geometry",
        field="sampling_x",
        expected=int(dump["sampling"]),
        actual=int(float(meta["sampling_x"])),
    )
    cross.integer(
        category="geometry",
        field="sampling_y",
        expected=int(dump["sampling"]),
        actual=int(float(meta["sampling_y"])),
    )
    cross.integer(
        category="geometry",
        field="tap_count",
        expected=expected_tap_count,
        actual=len(taps),
    )

    rust_accumulator = 0j
    casa_recomputed_accumulator = 0j
    casa_recomputed_norm = 0j
    comparable_taps = min(expected_tap_count, len(taps))
    for tap_index in range(comparable_taps):
        tap = taps[tap_index]
        iy = tap_index // (2 * x_support + 1) - y_support
        ix = tap_index % (2 * x_support + 1) - x_support
        cross.integer(
            category="tap_order",
            field="tap_index",
            expected=tap_index,
            actual=integer_value(tap, "index"),
            tap_index=tap_index,
        )
        cross.integer(
            category="tap_order",
            field="iy",
            expected=iy,
            actual=integer_value(tap, "iy"),
            tap_index=tap_index,
        )
        cross.integer(
            category="tap_order",
            field="ix",
            expected=ix,
            actual=integer_value(tap, "ix"),
            tap_index=tap_index,
        )
        cross.integer(
            category="tap_geometry",
            field="grid_x",
            expected=int(dump["loc"][0]) + ix,
            actual=integer_value(tap, "grid_x"),
            tap_index=tap_index,
        )
        cross.integer(
            category="tap_geometry",
            field="grid_y",
            expected=int(dump["loc"][1]) + iy,
            actual=integer_value(tap, "grid_y"),
            tap_index=tap_index,
        )

        casa_raw = complex_from_bits(tap, "raw_cf")
        casa_post_w = complex_from_bits(tap, "post_w_cf")
        casa_phase = complex_from_bits(tap, "phase")
        casa_post_phase = complex_from_bits(tap, "post_phase_cf")
        casa_grid = complex_from_bits(tap, "grid")
        casa_product = complex_from_bits(tap, "product")
        casa_norm = complex_from_bits(tap, "norm")
        casa_accumulator = complex_from_bits(tap, "accumulator")
        rust_packed = dump["packed"][tap_index]
        rust_phase = dump["phases"][tap_index]
        rust_grid = dump["grids"][tap_index]

        expected_post_w = (
            complex_conjugate(casa_raw) if data_w_m <= 0.0 else casa_raw
        )
        internal.complex(
            category="casa_internal_cf_selection",
            field="raw_to_post_w_cf",
            expected=expected_post_w,
            actual=casa_post_w,
            tap_index=tap_index,
        )
        expected_post_phase = complex_multiply(
            casa_post_w, complex_conjugate(casa_phase)
        )
        internal.complex(
            category="casa_internal_phase_multiply",
            field="post_w_to_post_phase_cf",
            expected=expected_post_phase,
            actual=casa_post_phase,
            tap_index=tap_index,
        )
        expected_casa_product = complex_multiply(casa_post_phase, casa_grid)
        internal.complex(
            category="casa_internal_product",
            field="post_phase_cf_times_grid",
            expected=expected_casa_product,
            actual=casa_product,
            tap_index=tap_index,
        )
        casa_recomputed_norm = complex_add(casa_recomputed_norm, casa_post_w)
        internal.complex(
            category="casa_internal_norm_prefix",
            field="norm_prefix",
            expected=casa_recomputed_norm,
            actual=casa_norm,
            tap_index=tap_index,
        )
        casa_recomputed_accumulator = complex_add(
            casa_recomputed_accumulator, casa_product
        )
        internal.complex(
            category="casa_internal_accumulator_prefix",
            field="accumulator_prefix",
            expected=casa_recomputed_accumulator,
            actual=casa_accumulator,
            tap_index=tap_index,
        )

        cross.complex(
            category="input",
            field="phase",
            expected=rust_phase,
            actual=casa_phase,
            tap_index=tap_index,
        )
        cross.complex(
            category="input",
            field="grid",
            expected=rust_grid,
            actual=casa_grid,
            tap_index=tap_index,
        )
        casa_packed_from_inputs = complex_conjugate(casa_post_phase)
        cross.complex(
            category="input_or_cf_packing",
            field="packed_cf_from_casa_inputs",
            expected=rust_packed,
            actual=casa_packed_from_inputs,
            tap_index=tap_index,
        )
        rust_operational_cf = complex_conjugate(rust_packed)
        cross.complex(
            category="input",
            field="operational_cf",
            expected=rust_operational_cf,
            actual=casa_post_phase,
            tap_index=tap_index,
        )
        rust_product = complex_multiply(rust_operational_cf, rust_grid)
        cross.complex(
            category="operation",
            field="product",
            expected=rust_product,
            actual=casa_product,
            tap_index=tap_index,
        )
        rust_accumulator = complex_add(rust_accumulator, rust_product)
        cross.complex(
            category="operation",
            field="accumulator_prefix",
            expected=rust_accumulator,
            actual=casa_accumulator,
            tap_index=tap_index,
        )

    casa_result = {
        field: complex_from_bits(result_record, field)
        for field in RESULT_COMPLEX_FIELDS
    }
    internal.complex(
        category="casa_internal_result",
        field="result_pre_phasor",
        expected=casa_recomputed_accumulator,
        actual=casa_result["pre_phasor"],
    )
    expected_casa_post_phasor = complex_multiply(
        casa_result["pre_phasor"], complex_conjugate(casa_result["phasor"])
    )
    internal.complex(
        category="casa_internal_result",
        field="result_post_phasor",
        expected=expected_casa_post_phasor,
        actual=casa_result["post_phasor"],
    )
    expected_casa_prediction = complex_divide(
        casa_result["post_phasor"], casa_result["normalization"]
    )
    internal.complex(
        category="casa_internal_result",
        field="result_prediction",
        expected=expected_casa_prediction,
        actual=casa_result["prediction"],
    )

    rust_normalization = complex_conjugate(dump["normalization"])
    rust_post_phasor = complex_multiply(
        rust_accumulator, complex_conjugate(casa_result["phasor"])
    )
    rust_prediction = complex_divide(rust_post_phasor, rust_normalization)
    final_comparisons = {
        "pre_phasor": cross.complex(
            category="final_operation",
            field="final_pre_phasor",
            expected=rust_accumulator,
            actual=casa_result["pre_phasor"],
        ),
        "normalization": cross.complex(
            category="final_input",
            field="final_normalization",
            expected=rust_normalization,
            actual=casa_result["normalization"],
        ),
        "post_phasor": cross.complex(
            category="final_operation",
            field="final_post_phasor",
            expected=rust_post_phasor,
            actual=casa_result["post_phasor"],
        ),
        "prediction": cross.complex(
            category="final_operation",
            field="final_prediction",
            expected=rust_prediction,
            actual=casa_result["prediction"],
        ),
    }

    return {
        "kind": "vlass_aw_degrid_prefix_comparison",
        "role": "isolated_arithmetic_diagnostic_not_promotion_evidence",
        "casa_trace": str(trace_path),
        "casa_trace_target": {
            field: (
                float(meta[field])
                if field in {"frequency_hz", "data_w_m"}
                else int(meta[field])
            )
            for field in ("row", "channel", "pol", "mcol", "frequency_hz", "data_w_m")
            if field in meta
        },
        "rust_dump": str(dump_path),
        "shape": dump["shape"],
        "loc": dump["loc"],
        "support": dump["support"],
        "sampling": dump["sampling"],
        "offset": dump["offset"],
        "phase_gradient_rad_per_sample": dump[
            "phase_gradient_rad_per_sample"
        ],
        "expected_tap_count": expected_tap_count,
        "casa_tap_count": len(taps),
        "compared_tap_count": comparable_taps,
        "comparison": cross.summary(),
        "casa_trace_self_consistency": internal.summary(),
        "final": {
            "rust": {
                "pre_phasor": encoded(rust_accumulator),
                "normalization": encoded(rust_normalization),
                "post_phasor": encoded(rust_post_phasor),
                "prediction": encoded(rust_prediction),
            },
            "casa": {
                field: encoded(value) for field, value in casa_result.items()
            },
            "comparisons": final_comparisons,
        },
    }


def pack_complex(value: complex) -> bytes:
    return struct.pack("<ff", f32(value.real), f32(value.imag))


def write_self_test_dump(path: Path) -> dict[str, Any]:
    packed = [
        complex(f32(0.25), f32(-0.125)),
        complex(f32(-0.5), f32(0.375)),
        complex(f32(0.0625), f32(0.75)),
    ]
    phases = [complex(f32(1.0), f32(0.0)) for _ in packed]
    grids = [
        complex(f32(0.75), f32(-0.5)),
        complex(f32(-0.25), f32(0.125)),
        complex(f32(0.5), f32(0.25)),
    ]
    normalization = 0j
    for value in packed:
        normalization = complex_add(normalization, value)
    payload = bytearray(b"AWDGRD03")
    payload.extend(struct.pack("<QQqqQQQqqdd", 8, 10, 3, 4, 1, 0, 1, 0, 0, 0.0, 0.0))
    payload.extend(pack_complex(normalization))
    for tap, phase, grid in zip(packed, phases, grids, strict=True):
        payload.extend(pack_complex(tap))
        payload.extend(pack_complex(phase))
        payload.extend(pack_complex(grid))
    path.write_bytes(payload)
    return {
        "packed": packed,
        "phases": phases,
        "grids": grids,
        "normalization": normalization,
    }


def trace_complex_fields(fields: dict[str, complex]) -> str:
    tokens = []
    for label, value in fields.items():
        bits = encoded(value)["bits"]
        tokens.extend((f"{label}_re={bits[0]}", f"{label}_im={bits[1]}"))
    return " ".join(tokens)


def write_self_test_trace(
    path: Path, fixture: dict[str, Any], *, perturb_grid_tap: int | None = None
) -> None:
    lines = [
        "meta row=0 channel=11 pol=0 mcol=0 frequency_hz=2000000000 "
        "data_w_m=-1 loc_x=3 loc_y=4 support_x=1 support_y=0 "
        "sampling_x=1 sampling_y=1"
    ]
    accumulator = 0j
    norm = 0j
    for index, (packed, phase, rust_grid) in enumerate(
        zip(
            fixture["packed"],
            fixture["phases"],
            fixture["grids"],
            strict=True,
        )
    ):
        raw = packed
        post_w = complex_conjugate(raw)
        post_phase = complex_multiply(post_w, complex_conjugate(phase))
        product = complex_multiply(post_phase, rust_grid)
        norm = complex_add(norm, post_w)
        accumulator = complex_add(accumulator, product)
        traced_grid = rust_grid
        if perturb_grid_tap == index:
            traced_grid = complex(
                np.nextafter(f32(rust_grid.real), np.float32(np.inf), dtype=np.float32),
                f32(rust_grid.imag),
            )
        fields = {
            "raw_cf": raw,
            "post_w_cf": post_w,
            "phase": phase,
            "post_phase_cf": post_phase,
            "grid": traced_grid,
            "product": product,
            "norm": norm,
            "accumulator": accumulator,
        }
        ix = index - 1
        lines.append(
            f"tap index={index} iy=0 ix={ix} cf_x={ix + 20} cf_y=20 "
            f"grid_x={ix + 3} grid_y=4 {trace_complex_fields(fields)}"
        )
    phasor = complex(f32(1.0), f32(0.0))
    post_phasor = complex_multiply(accumulator, complex_conjugate(phasor))
    normalization = complex_conjugate(fixture["normalization"])
    prediction = complex_divide(post_phasor, normalization)
    lines.append(
        "result "
        + trace_complex_fields(
            {
                "pre_phasor": accumulator,
                "phasor": phasor,
                "post_phasor": post_phasor,
                "normalization": normalization,
                "prediction": prediction,
            }
        )
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_self_test() -> None:
    with tempfile.TemporaryDirectory(prefix="vlass-degrid-prefix-") as directory:
        root = Path(directory)
        dump_path = root / "sample.bin"
        trace_path = root / "sample.txt"
        fixture = write_self_test_dump(dump_path)
        write_self_test_trace(trace_path, fixture)
        exact = compare_prefix(trace_path, dump_path)
        assert exact["comparison"]["exact"], exact["comparison"]
        assert exact["casa_trace_self_consistency"]["exact"], exact[
            "casa_trace_self_consistency"
        ]
        assert exact["final"]["comparisons"]["prediction"]["exact"]

        write_self_test_trace(trace_path, fixture, perturb_grid_tap=1)
        divergent = compare_prefix(trace_path, dump_path)
        first = divergent["comparison"]["first_divergence"]
        assert first is not None
        assert first["category"] == "input", first
        assert first["field"] == "grid", first
        assert first["tap_index"] == 1, first
        assert first["difference"]["absolute_ulp_distance"] == 1, first
    print("vlass_degrid_prefix_compare self-test: PASS", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("casa_trace", type=Path, nargs="?")
    parser.add_argument("rust_dump", type=Path, nargs="?")
    parser.add_argument(
        "--json-output",
        type=Path,
        help="write the JSON receipt to this path in addition to stdout",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run a bounded synthetic exact/divergent comparison",
    )
    args = parser.parse_args()
    if args.self_test:
        if args.casa_trace is not None or args.rust_dump is not None:
            parser.error("--self-test does not accept trace or dump paths")
        run_self_test()
        return
    if args.casa_trace is None or args.rust_dump is None:
        parser.error("casa_trace and rust_dump are required")
    comparison = compare_prefix(args.casa_trace, args.rust_dump)
    rendered = json.dumps(comparison, indent=2, sort_keys=True) + "\n"
    if args.json_output is not None:
        args.json_output.write_text(rendered, encoding="utf-8")
    print(rendered, end="", flush=True)


if __name__ == "__main__":
    main()
