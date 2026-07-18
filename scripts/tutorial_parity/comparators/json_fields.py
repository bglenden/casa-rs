"""Compare declared fields in native and oracle JSON products."""

from __future__ import annotations

import json
from typing import Any

from ..model import RuntimeResources, SectionManifest
from .common import compare_values, nested


def compare(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
    inputs = manifest.comparison.inputs
    native_path = resources.pack_root / inputs["native"]
    oracle_path = resources.pack_root / inputs["oracle"]
    missing = [str(path) for path in (native_path, oracle_path) if not path.is_file()]
    if missing:
        return {"status": "unavailable", "plugin": "json_fields", "reason": f"missing inputs: {', '.join(missing)}", "fields": {}}
    native = json.loads(native_path.read_text(encoding="utf-8"))
    oracle = json.loads(oracle_path.read_text(encoding="utf-8"))
    config = manifest.comparison.config
    atol = float(config.get("absolute_tolerance", 0.0))
    rtol = float(config.get("relative_tolerance", 0.0))
    results = {}
    for field in config.get("fields", []):
        try:
            results[field] = compare_values(nested(native, field), nested(oracle, field), atol=atol, rtol=rtol)
        except (KeyError, IndexError, TypeError) as error:
            results[field] = {"passed": False, "reason": f"missing field: {error}"}
    passed = bool(results) and all(item["passed"] for item in results.values())
    return {"status": "passed" if passed else "failed", "plugin": "json_fields", "fields": results}
