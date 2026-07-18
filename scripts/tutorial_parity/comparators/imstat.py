"""Normalize and compare the characterized imstat cases."""

from __future__ import annotations

import json
from typing import Any

from ..model import RuntimeResources, SectionManifest
from .task_results import native_payload, numeric_vector


def normalize_casa(raw: dict[str, Any], fields: list[str], positions: list[str]) -> dict[str, Any]:
    normalized = {}
    for field in fields:
        values = numeric_vector(raw.get(field, []))
        if values:
            normalized[field] = values[0]
    for field in positions:
        values = numeric_vector(raw.get(field, []))
        if values:
            normalized[field] = [int(value) for value in values]
    return normalized


def compare(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
    native_paths = manifest.comparison.inputs["native"]
    oracle_paths = manifest.comparison.inputs["oracle"]
    if isinstance(native_paths, str):
        native_paths, oracle_paths = [native_paths], [oracle_paths]
    missing = [path for path in [*native_paths, *oracle_paths] if not (resources.pack_root / path).is_file()]
    if missing:
        return {"status": "unavailable", "plugin": "imstat", "reason": f"missing imstat inputs: {', '.join(missing)}"}
    config = manifest.comparison.config
    cases = []
    for native_name, oracle_name in zip(native_paths, oracle_paths, strict=True):
        native = native_payload(json.loads((resources.pack_root / native_name).read_text(encoding="utf-8")))
        oracle = normalize_casa(json.loads((resources.pack_root / oracle_name).read_text(encoding="utf-8")), config["fields"], config["position_fields"])
        checks = []
        for field in config["fields"]:
            if field not in native or field not in oracle:
                continue
            tolerance = config["flux_tolerance"] if field == "flux" else config["absolute_tolerance"]
            delta = abs(float(native[field]) - float(oracle[field]))
            checks.append({"field": field, "native": native[field], "oracle": oracle[field], "delta": delta, "tolerance": tolerance, "passed": delta <= tolerance})
        for field in config["position_fields"]:
            if field in native and field in oracle:
                checks.append({"field": field, "native": native[field], "oracle": oracle[field], "passed": list(native[field]) == list(oracle[field])})
        cases.append({"native": native_name, "oracle": oracle_name, "checks": checks, "passed": bool(checks) and all(check["passed"] for check in checks)})
    passed = bool(cases) and all(case["passed"] for case in cases)
    return {"status": "passed" if passed else "failed", "plugin": "imstat", "cases": cases}
