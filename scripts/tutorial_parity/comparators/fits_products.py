"""FITS product comparison through the checked native worker."""

from __future__ import annotations

import json
from typing import Any

from ..commands import run_command
from ..evidence import write_json_atomic
from ..model import RuntimeResources, SectionManifest


def compare(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
    if resources.native_python is None:
        return {"status": "unavailable", "plugin": "fits_products", "reason": "native Python is unavailable"}
    request = resources.evidence_root / "requests" / f"{manifest.section_id}.fits_products.json"
    output = resources.evidence_root / "comparisons" / f"{manifest.section_id}.fits_products.json"
    inputs = {
        key: ([str(resources.pack_root / item) for item in value] if isinstance(value, list) else str(resources.pack_root / value))
        for key, value in manifest.comparison.inputs.items()
    }
    write_json_atomic(request, {"schema_version": 1, "inputs": inputs, "config": manifest.comparison.config})
    worker = resources.repo_root / "scripts" / "tutorial_parity" / "workers" / "fits_compare.py"
    completed = run_command(
        [str(resources.native_python), str(worker), str(request), str(output)],
        cwd=resources.pack_root,
        timeout_seconds=600,
    )
    if completed.return_code != 0 or not output.is_file():
        return {"status": "failed", "plugin": "fits_products", "reason": f"FITS comparator exited {completed.return_code}", "stderr": completed.stderr}
    result = json.loads(output.read_text(encoding="utf-8"))
    result["plugin"] = "fits_products"
    result["artifact"] = str(output)
    return result
