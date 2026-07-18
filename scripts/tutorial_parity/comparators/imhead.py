"""Normalize and compare imhead summary contracts."""

from __future__ import annotations

import json
from typing import Any

from ..model import RuntimeResources, SectionManifest
from .task_results import native_payload, numeric_vector, string_vector


def normalize_casa(raw: dict[str, Any]) -> dict[str, Any]:
    names = string_vector(raw.get("axisnames", []))
    units = string_vector(raw.get("axisunits", []))
    shape = [int(value) for value in numeric_vector(raw.get("shape", []))]
    refpix = numeric_vector(raw.get("refpix", []))
    refval = numeric_vector(raw.get("refval", []))
    increments = numeric_vector(raw.get("incr", []))
    axes = [
        {
            "name": name,
            "shape": shape[index] if index < len(shape) else None,
            "reference_value": refval[index] if index < len(refval) else None,
            "reference_pixel": refpix[index] if index < len(refpix) else None,
            "increment": increments[index] if index < len(increments) else None,
            "unit": units[index] if index < len(units) else "",
        }
        for index, name in enumerate(names)
    ]
    beam = raw.get("restoringbeam", {})
    return {
        "shape": shape,
        "units": raw.get("unit"),
        "default_mask": raw.get("defaultmask"),
        "masks": string_vector(raw.get("masks", [])),
        "restoring_beam": {
            "major_arcsec": beam.get("major", {}).get("value"),
            "minor_arcsec": beam.get("minor", {}).get("value"),
            "position_angle_deg": beam.get("positionangle", {}).get("value"),
        },
        "axes": axes,
    }


def compare(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
    native_path = resources.pack_root / manifest.comparison.inputs["native"]
    oracle_path = resources.pack_root / manifest.comparison.inputs["oracle"]
    if not native_path.is_file() or not oracle_path.is_file():
        return {"status": "unavailable", "plugin": "imhead", "reason": "imhead result inputs are missing"}
    native = native_payload(json.loads(native_path.read_text(encoding="utf-8")))
    oracle = normalize_casa(json.loads(oracle_path.read_text(encoding="utf-8")))
    config = manifest.comparison.config
    checks = []
    for field in ("shape", "units", "default_mask", "masks"):
        checks.append({"field": field, "native": native.get(field), "oracle": oracle.get(field), "passed": native.get(field) == oracle.get(field)})
    for field in ("major_arcsec", "minor_arcsec", "position_angle_deg"):
        left, right = native["restoring_beam"][field], oracle["restoring_beam"][field]
        delta = abs(float(left) - float(right))
        checks.append({"field": f"restoring_beam.{field}", "native": left, "oracle": right, "delta": delta, "tolerance": config["beam_tolerance"], "passed": delta <= config["beam_tolerance"]})
    for index, (left_axis, right_axis) in enumerate(zip(native.get("axes", []), oracle.get("axes", []), strict=False)):
        for field in ("name", "shape", "unit"):
            checks.append({"field": f"axes[{index}].{field}", "native": left_axis.get(field), "oracle": right_axis.get(field), "passed": left_axis.get(field) == right_axis.get(field)})
        for field in ("reference_value", "reference_pixel", "increment"):
            tolerance = config["spectral_tolerance_hz"] if left_axis.get("unit") == "Hz" and field in {"reference_value", "increment"} else config["axis_tolerance"]
            delta = abs(float(left_axis[field]) - float(right_axis[field]))
            checks.append({"field": f"axes[{index}].{field}", "native": left_axis[field], "oracle": right_axis[field], "delta": delta, "tolerance": tolerance, "passed": delta <= tolerance})
    passed = bool(checks) and all(check["passed"] for check in checks)
    return {"status": "passed" if passed else "failed", "plugin": "imhead", "checks": checks}
