"""Run the checked CASA comparator worker through a JSON-file protocol."""

from __future__ import annotations

import json
from typing import Any

from ..commands import run_command
from ..evidence import write_json_atomic
from ..model import RuntimeResources, SectionManifest


def run_casa_comparison(manifest: SectionManifest, resources: RuntimeResources, mode: str) -> dict[str, Any]:
    if resources.casa_python is None or not resources.casa_python.is_file():
        return {"status": "unavailable", "plugin": mode, "reason": "CASA Python is unavailable"}
    request = resources.evidence_root / "requests" / f"{manifest.section_id}.{mode}.json"
    output = resources.evidence_root / "comparisons" / f"{manifest.section_id}.{mode}.json"
    log = resources.evidence_root / "comparisons" / f"{manifest.section_id}.{mode}.log"
    inputs = {
        key: ([str(resources.pack_root / item) for item in value] if isinstance(value, list) else str(resources.pack_root / value))
        for key, value in manifest.comparison.inputs.items()
    }
    write_json_atomic(request, {
        "schema_version": 1,
        "mode": mode,
        "inputs": inputs,
        "config": manifest.comparison.config,
    })
    worker = resources.repo_root / "scripts" / "tutorial_parity" / "workers" / "casa_compare.py"
    completed = run_command(
        [str(resources.casa_python), str(worker), str(request), str(output)],
        cwd=resources.pack_root,
        timeout_seconds=1200,
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    if completed.return_code != 0 or not output.is_file():
        return {"status": "failed", "plugin": mode, "reason": f"CASA comparator exited {completed.return_code}", "log": str(log)}
    result = json.loads(output.read_text(encoding="utf-8"))
    result["plugin"] = mode
    result["artifact"] = str(output)
    result["log"] = str(log)
    return result
