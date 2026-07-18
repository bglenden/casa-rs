"""Atomic typed tutorial-parity evidence writing."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .schema import RESULT_SCHEMA_VERSION, validate_result


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def result_document(
    *,
    section_id: str,
    status: str,
    resources: dict[str, Any],
    surfaces: dict[str, Any],
    comparison: dict[str, Any],
    artifacts: list[dict[str, Any]],
    failure: dict[str, str] | None = None,
) -> dict[str, Any]:
    value = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": "tutorial_parity_result",
        "section_id": section_id,
        "status": status,
        "created_at": utc_now(),
        "resources": resources,
        "surfaces": surfaces,
        "comparison": comparison,
        "artifacts": artifacts,
        "failure": failure,
    }
    return validate_result(value)


def review_document(
    *, manifest_id: str, result_ref: str, result: dict[str, Any]
) -> dict[str, Any]:
    comparison_status = result.get("comparison", {}).get("status")
    return {
        "schema_version": 1,
        "kind": "tutorial_parity_review",
        "section_id": manifest_id,
        "status": (
            "pending_human_review"
            if result["status"] == "completed" and comparison_status == "passed"
            else result["status"]
        ),
        "result_ref": result_ref,
        "surface_statuses": {
            name: surface["status"] for name, surface in result["surfaces"].items()
        },
        "comparison_status": comparison_status,
        "human_review": {"reviewed_by": None, "reviewed_at": None, "outcome": None},
    }
