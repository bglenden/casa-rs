#!/usr/bin/env python3
"""Execute allowlisted casa-rs Python task bindings from a JSON request."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any


TASKS = {"imhead", "imstat", "immoments", "exportfits", "msexplore", "imager", "split", "impbcor"}
BINARY_BY_TASK = {
    "imhead": "casars",
    "imstat": "casars",
    "immoments": "casars",
    "exportfits": "casars",
    "msexplore": "casars",
    "imager": "casars",
    "split": "casars",
    "impbcor": "casars",
}


def python_parameters(task: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Project provider CLI values into the generated CASA-named Python API."""
    parameters = dict(raw)
    parameters.pop("json", None)
    if task == "immoments":
        if isinstance(parameters.get("moments"), int):
            parameters["moments"] = str(parameters["moments"])
        if isinstance(parameters.get("includepix"), list):
            parameters["includepix"] = ",".join(str(value) for value in parameters["includepix"])
    if task == "imager":
        if "cell_arcsec" in parameters:
            parameters["cell"] = f"{parameters.pop('cell_arcsec')}arcsec"
        if "threshold_jy" in parameters:
            parameters["threshold"] = f"{parameters.pop('threshold_jy')}Jy"
        for name in ("field", "phasecenter_field"):
            if name in parameters:
                parameters[name] = str(parameters[name])
        parameters.pop("overwrite", None)
    return parameters


def json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        if hasattr(value, "stdout"):
            return {
                "stdout": str(value.stdout),
                "stderr": str(getattr(value, "stderr", "")),
                "returncode": int(getattr(value, "returncode", 0)),
            }
        if isinstance(value, dict):
            return {str(key): json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_safe(item) for item in value]
        if hasattr(value, "as_dict"):
            return json_safe(value.as_dict())
        if hasattr(value, "__dict__"):
            return json_safe(vars(value))
        return repr(value)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: native_tasks.py REQUEST RESULT", file=sys.stderr)
        return 2
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    operations = request.get("operations")
    binary_dir = Path(request.get("binary_dir", ""))
    if request.get("schema_version") != 1 or not isinstance(operations, list):
        raise ValueError("unsupported native worker request")
    from casars import tasks

    results = []
    for operation in operations:
        task = operation["task"]
        if task not in TASKS:
            raise ValueError(f"unsupported native Python task: {task}")
        parameters = python_parameters(task, operation["parameters"])
        parameters["binary"] = str(binary_dir / BINARY_BY_TASK[task])
        started = time.perf_counter()
        result = getattr(tasks, task)(**parameters)
        capture = operation.get("capture")
        if capture:
            capture_path = Path(capture)
            capture_path.parent.mkdir(parents=True, exist_ok=True)
            capture_path.write_text(json.dumps(json_safe(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        results.append({
            "task": task,
            "elapsed_seconds": time.perf_counter() - started,
            "result": json_safe(result),
        })
    result_path = Path(sys.argv[2])
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        json.dumps({"schema_version": 1, "status": "completed", "operations": results}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
