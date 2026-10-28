"""Shared adapter planning and execution contracts."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ..commands import run_command
from ..model import RuntimeResources, SectionManifest, Surface


@dataclass(frozen=True)
class AdapterPlan:
    surface: str
    argv: tuple[str, ...]
    cwd: Path
    env: dict[str, str] | None
    timeout_seconds: float
    outputs: tuple[Path, ...]
    metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "surface": self.surface,
            "argv": list(self.argv),
            "cwd": str(self.cwd),
            "timeout_seconds": self.timeout_seconds,
            "outputs": [str(path) for path in self.outputs],
            "metadata": self.metadata,
        }


class SurfaceAdapter(Protocol):
    name: str

    def plan(self, manifest: SectionManifest, surface: Surface, resources: RuntimeResources) -> AdapterPlan:
        ...

    def execute(self, plan: AdapterPlan) -> dict[str, Any]:
        ...


def reset_outputs(plan: AdapterPlan) -> None:
    """Remove the plan's declared outputs inside its directory (the pack), so
    a rerun starts as a fresh pack does: split, like CASA's, refuses an
    existing output (#931). They are the section's own products; anything
    outside the pack, such as a worker's result file, is left to its writer."""

    root = plan.cwd.resolve()
    for output in plan.outputs:
        if not output.resolve().is_relative_to(root) or output.resolve() == root:
            continue
        if output.is_dir() and not output.is_symlink():
            shutil.rmtree(output)
        elif output.exists() or output.is_symlink():
            output.unlink()


def execute_plan(plan: AdapterPlan) -> dict[str, Any]:
    reset_outputs(plan)
    for output in plan.outputs:
        output.parent.mkdir(parents=True, exist_ok=True)
    result = run_command(
        list(plan.argv),
        cwd=plan.cwd,
        env=plan.env,
        timeout_seconds=plan.timeout_seconds,
    )
    capture_stdout = plan.metadata.get("capture_stdout")
    captured = result.stdout
    reason = None if result.return_code == 0 else f"command exited {result.return_code}"
    if plan.metadata.get("capture_reply_value"):
        captured, reply_reason = reply_value(result.stdout)
        reason = reason or reply_reason
    if reason is None and isinstance(capture_stdout, str):
        capture_path = Path(capture_stdout)
        capture_path.parent.mkdir(parents=True, exist_ok=True)
        capture_path.write_text(captured, encoding="utf-8")
    return {
        "status": "completed" if reason is None else "failed",
        "operations": [result.as_dict()],
        "artifacts": [str(path) for path in plan.outputs if path.exists()],
        "reason": reason,
    }


def reply_value(stdout: str) -> tuple[str, str | None]:
    """The return value in a task binary's `--json-run` reply, as JSON text,
    or why the reply holds none (#931)."""

    try:
        reply = json.loads(stdout)
    except json.JSONDecodeError as error:
        return "", f"the task's reply is not JSON: {error}"
    if not isinstance(reply, dict) or reply.get("status") != "succeeded":
        message = reply.get("message") if isinstance(reply, dict) else None
        return "", f"the task failed: {message or reply!r}"
    try:
        value = plain_value(reply.get("value"))
    except (KeyError, TypeError, ValueError) as error:
        return "", f"the task's return value is not a typed value: {error}"
    return json.dumps(value, indent=2) + "\n", None


def plain_value(value: Any) -> Any:
    """A reply's typed return value as plain JSON, as the task binary's
    command line prints it: arrays nested in their shape from column-major
    data, dictionary keys as strings (#931)."""

    kind = value["kind"]
    if kind == "none":
        return None
    if kind in ("bool", "int", "float", "string"):
        return value["value"]
    if kind == "list":
        return [plain_value(item) for item in value["items"]]
    if kind == "dict":
        return {str(entry["key"]): plain_value(entry["value"]) for entry in value["entries"]}
    if kind.endswith("_array"):
        return _nest(value["shape"], value["data"], 0, 1)
    raise ValueError(f"unknown value kind {kind!r}")


def _nest(shape: list[int], data: list[Any], offset: int, stride: int) -> Any:
    if not shape:
        return data[offset] if offset < len(data) else None
    count, rest = shape[0], shape[1:]
    return [_nest(rest, data, offset + index * stride, stride * count) for index in range(count)]
