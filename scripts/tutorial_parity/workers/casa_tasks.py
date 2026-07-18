#!/usr/bin/env python3
"""Execute allowlisted CASA tasks from a JSON request file."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable


TASKS = {"imhead", "imstat", "immoments", "exportfits", "listobs", "plotms", "tclean", "split", "impbcor"}


def task_callable(name: str) -> Callable[..., Any]:
    if name not in TASKS:
        raise ValueError(f"unsupported CASA task: {name}")
    if name == "plotms":
        from casaplotms import plotms

        return plotms
    from casatasks import exportfits, imhead, immoments, impbcor, imstat, listobs, split, tclean

    return {
        "imhead": imhead,
        "imstat": imstat,
        "immoments": immoments,
        "exportfits": exportfits,
        "listobs": listobs,
        "split": split,
        "tclean": tclean,
        "impbcor": impbcor,
    }[name]


def json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        if isinstance(value, dict):
            return {str(key): json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_safe(item) for item in value]
        if hasattr(value, "tolist"):
            return json_safe(value.tolist())
        return repr(value)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: casa_tasks.py REQUEST RESULT", file=sys.stderr)
        return 2
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    operations = request.get("operations")
    if request.get("schema_version") != 1 or not isinstance(operations, list):
        raise ValueError("unsupported CASA worker request")
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    results = []
    for operation in operations:
        task = operation["task"]
        parameters = operation["parameters"]
        started = time.perf_counter()
        result = task_callable(task)(**parameters)
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
