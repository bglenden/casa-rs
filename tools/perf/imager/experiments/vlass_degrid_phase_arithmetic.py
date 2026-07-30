#!/usr/bin/env python3
"""Replay a phase-aware casa-rs AW degrid dump with CASA operation ordering."""

from __future__ import annotations

import argparse
import json
import struct
from pathlib import Path

import numpy as np


def f32(value: float) -> np.float32:
    return np.float32(value)


def complex_multiply(left: complex, right: complex) -> complex:
    left_re = f32(left.real)
    left_im = f32(left.imag)
    right_re = f32(right.real)
    right_im = f32(right.imag)
    return complex(
        f32(f32(left_re * right_re) - f32(left_im * right_im)),
        f32(f32(left_re * right_im) + f32(left_im * right_re)),
    )


def complex_add(left: complex, right: complex) -> complex:
    return complex(
        f32(f32(left.real) + f32(right.real)),
        f32(f32(left.imag) + f32(right.imag)),
    )


def complex_divide(left: complex, right: complex) -> complex:
    denominator = f32(
        f32(f32(right.real) * f32(right.real))
        + f32(f32(right.imag) * f32(right.imag))
    )
    return complex(
        f32(
            f32(
                f32(f32(left.real) * f32(right.real))
                + f32(f32(left.imag) * f32(right.imag))
            )
            / denominator
        ),
        f32(
            f32(
                f32(f32(left.imag) * f32(right.real))
                - f32(f32(left.real) * f32(right.imag))
            )
            / denominator
        ),
    )


def neighbors(value: np.float32, radius: int = 32) -> list[np.float32]:
    values = [value]
    lower = value
    upper = value
    for _ in range(radius):
        lower = np.nextafter(lower, np.float32(-np.inf), dtype=np.float32)
        upper = np.nextafter(upper, np.float32(np.inf), dtype=np.float32)
        values.extend((lower, upper))
    return values


def recover_raw_tap(packed: complex, phase: complex) -> tuple[complex, int]:
    inverse = packed * phase.conjugate() / (abs(phase) ** 2)
    candidates = []
    for real in neighbors(f32(inverse.real)):
        for imag in neighbors(f32(inverse.imag)):
            candidate = complex(real, imag)
            if complex_multiply(candidate, phase) == packed:
                candidates.append(candidate)
    if not candidates:
        raise RuntimeError(
            f"no nearby raw tap reproduces packed={packed} phase={phase}"
        )
    candidate = min(candidates, key=lambda value: abs(value - inverse))
    return candidate, len(candidates)


def read_dump(path: Path) -> dict:
    payload = memoryview(path.read_bytes())
    offset = 0

    def take(fmt: str):
        nonlocal offset
        size = struct.calcsize(fmt)
        value = struct.unpack_from(fmt, payload, offset)
        offset += size
        return value[0] if len(value) == 1 else value

    magic = bytes(payload[:8])
    offset += 8
    if magic not in {b"AWDGRD02", b"AWDGRD03"}:
        raise RuntimeError(f"unsupported dump magic {magic!r}")
    rows = take("<Q")
    columns = take("<Q")
    loc_x = take("<q")
    loc_y = take("<q")
    x_support = take("<Q")
    y_support = take("<Q")
    if magic == b"AWDGRD03":
        sampling = take("<Q")
        off_x = take("<q")
        off_y = take("<q")
        phase_gradient = [take("<d"), take("<d")]
    else:
        sampling = None
        off_x = None
        off_y = None
        phase_gradient = None
    normalization = complex(take("<f"), take("<f"))
    count = (2 * x_support + 1) * (2 * y_support + 1)
    packed = []
    phases = []
    grids = []
    for _ in range(count):
        packed.append(complex(take("<f"), take("<f")))
        phases.append(complex(take("<f"), take("<f")))
        grids.append(complex(take("<f"), take("<f")))
    if offset != len(payload):
        raise RuntimeError(f"unparsed dump payload: {len(payload) - offset} bytes")
    return {
        "shape": [rows, columns],
        "loc": [loc_x, loc_y],
        "support": [x_support, y_support],
        "sampling": sampling,
        "offset": [off_x, off_y],
        "phase_gradient_rad_per_sample": phase_gradient,
        "normalization": normalization,
        "packed": packed,
        "phases": phases,
        "grids": grids,
    }


def accumulate(taps: list[complex], grids: list[complex]) -> complex:
    value = 0j
    for tap, grid in zip(taps, grids, strict=True):
        value = complex_add(value, complex_multiply(tap.conjugate(), grid))
    return value


def encoded(value: complex) -> dict:
    return {
        "value": [float(f32(value.real)), float(f32(value.imag))],
        "bits": [
            int(f32(value.real).view(np.uint32)),
            int(f32(value.imag).view(np.uint32)),
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dump", type=Path)
    parser.add_argument(
        "--casa",
        type=complex,
        help="optional CASA prediction as Python complex syntax, for example 0.1+0.2j",
    )
    args = parser.parse_args()

    dump = read_dump(args.dump)
    raw = []
    candidate_counts = []
    casa_order_taps = []
    for packed, phase in zip(dump["packed"], dump["phases"], strict=True):
        raw_tap, candidate_count = recover_raw_tap(packed, phase)
        raw.append(raw_tap)
        candidate_counts.append(candidate_count)
        # AWVisResampler::GridToData conjugates the selected CF first, then
        # multiplies it by the conjugated POINTING phase.
        casa_order_taps.append(
            complex_multiply(raw_tap.conjugate(), phase.conjugate()).conjugate()
        )

    packed_sum = accumulate(dump["packed"], dump["grids"])
    casa_order_sum = accumulate(casa_order_taps, dump["grids"])
    denominator = dump["normalization"].conjugate()
    packed_prediction = complex_divide(packed_sum, denominator)
    casa_order_prediction = complex_divide(casa_order_sum, denominator)
    result = {
        "kind": "vlass_aw_degrid_phase_arithmetic",
        "role": "isolated_arithmetic_diagnostic_not_promotion_evidence",
        "dump": str(args.dump),
        "shape": dump["shape"],
        "loc": dump["loc"],
        "support": dump["support"],
        "sampling": dump["sampling"],
        "offset": dump["offset"],
        "phase_gradient_rad_per_sample": dump[
            "phase_gradient_rad_per_sample"
        ],
        "tap_count": len(raw),
        "raw_recovery_candidate_count_min": min(candidate_counts),
        "raw_recovery_candidate_count_max": max(candidate_counts),
        "casa_order_tap_bit_mismatches": sum(
            encoded(left)["bits"] != encoded(right)["bits"]
            for left, right in zip(dump["packed"], casa_order_taps, strict=True)
        ),
        "packed_sum": encoded(packed_sum),
        "casa_order_sum": encoded(casa_order_sum),
        "packed_prediction": encoded(packed_prediction),
        "casa_order_prediction": encoded(casa_order_prediction),
        "casa_reference": encoded(args.casa) if args.casa is not None else None,
    }
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
