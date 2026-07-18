#!/usr/bin/env python3
"""Compare FITS headers and primary-array pixels without an external FITS dependency."""

from __future__ import annotations

import json
import math
import struct
import sys
from pathlib import Path
from typing import Any


def card_value(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith("'"):
        end = raw.find("'", 1)
        return raw[1:end].strip() if end >= 0 else raw.strip("' ")
    token = raw.split()[0]
    if token in {"T", "F"}:
        return token == "T"
    try:
        return float(token.replace("D", "E").replace("d", "e")) if any(char in token for char in ".eEdD") else int(token)
    except ValueError:
        return token


def read_primary(path: Path) -> tuple[dict[str, Any], list[float], list[int]]:
    data = path.read_bytes()
    cards: list[str] = []
    offset = 0
    while True:
        block = data[offset : offset + 2880]
        if len(block) != 2880:
            raise ValueError(f"{path} ended before FITS END")
        offset += 2880
        for index in range(0, 2880, 80):
            card = block[index : index + 80].decode("ascii", "replace")
            cards.append(card)
            if card.startswith("END"):
                header = {}
                for item in cards:
                    key = item[:8].strip()
                    if key and key not in {"END", "COMMENT", "HISTORY"} and item[8:10] == "= ":
                        header[key] = card_value(item[10:].split("/", 1)[0])
                shape = [int(header[f"NAXIS{i}"]) for i in range(1, int(header.get("NAXIS", 0)) + 1)]
                count = math.prod(shape)
                bitpix = int(header["BITPIX"])
                formats = {-32: "f", -64: "d", 16: "h", 32: "i", 64: "q"}
                if bitpix not in formats:
                    raise ValueError(f"unsupported BITPIX={bitpix}")
                width = abs(bitpix) // 8
                values = [float(value) for value in struct.unpack(f">{count}{formats[bitpix]}", data[offset : offset + count * width])]
                return header, values, shape


def compare_pair(native: Path, oracle: Path, config: dict[str, Any]) -> dict[str, Any]:
    left_header, left_values, left_shape = read_primary(native)
    right_header, right_values, right_shape = read_primary(oracle)
    atol = float(config.get("absolute_tolerance", 0.0))
    rtol = float(config.get("relative_tolerance", 0.0))
    shared = [(left, right) for left, right in zip(left_values, right_values) if math.isfinite(left) and math.isfinite(right)]
    differences = [abs(left - right) for left, right in shared]
    max_abs = max(differences) if differences else None
    scale = max((abs(right) for _, right in shared), default=0.0)
    headers = {}
    for key in config.get("header_fields", []):
        left = left_header.get(key)
        right = right_header.get(key)
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            matched = abs(float(left) - float(right)) <= atol + rtol * abs(float(right))
        else:
            matched = left == right
        headers[key] = {"native": left, "oracle": right, "matched": matched}
    passed = left_shape == right_shape and len(left_values) == len(right_values) and max_abs is not None and max_abs <= atol + rtol * scale and all(item["matched"] for item in headers.values())
    return {
        "status": "passed" if passed else "failed",
        "native": str(native),
        "oracle": str(oracle),
        "native_shape": left_shape,
        "oracle_shape": right_shape,
        "shared_finite_pixels": len(shared),
        "max_abs_diff": max_abs,
        "headers": headers,
    }


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: fits_compare.py REQUEST RESULT", file=sys.stderr)
        return 2
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("schema_version") != 1:
        raise ValueError("unsupported FITS comparator request")
    native = request["inputs"]["native"]
    oracle = request["inputs"]["oracle"]
    if isinstance(native, str):
        native, oracle = [native], [oracle]
    if len(native) != len(oracle):
        raise ValueError("native/oracle FITS lists differ in length")
    products = [compare_pair(Path(left), Path(right), request["config"]) for left, right in zip(native, oracle, strict=True)]
    result = {"schema_version": 1, "status": "passed" if products and all(item["status"] == "passed" for item in products) else "failed", "products": products}
    Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)
    Path(sys.argv[2]).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
