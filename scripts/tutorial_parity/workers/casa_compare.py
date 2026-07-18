#!/usr/bin/env python3
"""Compare tutorial CASA images or MeasurementSets through one checked worker."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
from casatools import image, table


def image_record(path: str) -> dict[str, Any]:
    tool = image()
    try:
        if not tool.open(path):
            raise RuntimeError(f"cannot open image: {path}")
        values = np.asarray(tool.getchunk(), dtype=np.float64)
        mask = np.asarray(tool.getchunk(getmask=True), dtype=np.bool_)
        summary = tool.summary(list=False)
        return {"values": values, "mask": mask, "summary": summary}
    finally:
        tool.close()


def compare_image_pair(native: str, oracle: str, config: dict[str, Any]) -> dict[str, Any]:
    left = image_record(native)
    right = image_record(oracle)
    if left["values"].shape != right["values"].shape:
        return {"status": "failed", "reason": "shape mismatch", "native_shape": list(left["values"].shape), "oracle_shape": list(right["values"].shape)}
    shared = left["mask"] & right["mask"] & np.isfinite(left["values"]) & np.isfinite(right["values"])
    difference = left["values"][shared] - right["values"][shared]
    max_abs = float(np.max(np.abs(difference))) if difference.size else None
    rms = float(np.sqrt(np.mean(np.square(difference)))) if difference.size else None
    oracle_rms = float(np.sqrt(np.mean(np.square(right["values"][shared])))) if difference.size else None
    atol = float(config.get("absolute_tolerance", 0.0))
    rtol = float(config.get("relative_tolerance", 0.0))
    passed = bool(difference.size and max_abs is not None and max_abs <= atol + rtol * float(np.max(np.abs(right["values"][shared]))))
    metadata = {}
    for field in config.get("metadata_fields", []):
        native_value = left["summary"].get(field)
        oracle_value = right["summary"].get(field)
        metadata[field] = {"native": json_safe(native_value), "oracle": json_safe(oracle_value), "matched": json_safe(native_value) == json_safe(oracle_value)}
    return {
        "status": "passed" if passed and all(item["matched"] for item in metadata.values()) else "failed",
        "native_path": native,
        "oracle_path": oracle,
        "shape": list(left["values"].shape),
        "shared_finite_pixels": int(difference.size),
        "mask_mismatch_pixels": int(np.count_nonzero(left["mask"] != right["mask"])),
        "max_abs_diff": max_abs,
        "rms_diff": rms,
        "rms_diff_over_oracle_rms": rms / oracle_rms if rms is not None and oracle_rms else None,
        "metadata": metadata,
    }


def ms_summary(path: str) -> dict[str, Any]:
    tool = table()
    try:
        tool.open(path)
        row_count = int(tool.nrows())
        field_ids = sorted({int(value) for value in np.asarray(tool.getcol("FIELD_ID")).ravel()})
        data_desc_ids = sorted({int(value) for value in np.asarray(tool.getcol("DATA_DESC_ID")).ravel()})
        scan_numbers = sorted({int(value) for value in np.asarray(tool.getcol("SCAN_NUMBER")).ravel()})
    finally:
        tool.close()
    field_tool = table()
    try:
        field_tool.open(str(Path(path) / "FIELD"))
        field_names = [str(value) for value in np.asarray(field_tool.getcol("NAME")).ravel()]
    finally:
        field_tool.close()
    return {
        "row_count": row_count,
        "field_ids": field_ids,
        "field_names": field_names,
        "data_description_ids": data_desc_ids,
        "scan_numbers": scan_numbers,
    }


def compare_ms(native: str, oracle: str, config: dict[str, Any]) -> dict[str, Any]:
    left = ms_summary(native)
    right = ms_summary(oracle)
    fields = config.get("fields", sorted(set(left) | set(right)))
    results = {field: {"native": left.get(field), "oracle": right.get(field), "matched": left.get(field) == right.get(field)} for field in fields}
    return {"status": "passed" if all(item["matched"] for item in results.values()) else "failed", "fields": results}


def json_safe(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: casa_compare.py REQUEST RESULT", file=sys.stderr)
        return 2
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("schema_version") != 1:
        raise ValueError("unsupported comparator request")
    mode = request["mode"]
    inputs = request["inputs"]
    config = request["config"]
    if mode == "image_products":
        native = inputs["native"]
        oracle = inputs["oracle"]
        if isinstance(native, str):
            native, oracle = [native], [oracle]
        if len(native) != len(oracle):
            raise ValueError("native/oracle image lists differ in length")
        products = [compare_image_pair(left, right, config) for left, right in zip(native, oracle, strict=True)]
        status = "passed" if products and all(product["status"] == "passed" for product in products) else "failed"
        result = {"schema_version": 1, "status": status, "products": products}
    elif mode == "measurement_set":
        result = {"schema_version": 1, **compare_ms(inputs["native"], inputs["oracle"], config)}
    else:
        raise ValueError(f"unsupported comparator mode: {mode}")
    Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)
    Path(sys.argv[2]).write_text(json.dumps(json_safe(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
