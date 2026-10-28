"""Shared adapter planning and execution contracts."""

from __future__ import annotations

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
    if result.return_code == 0 and isinstance(capture_stdout, str):
        capture_path = Path(capture_stdout)
        capture_path.parent.mkdir(parents=True, exist_ok=True)
        capture_path.write_text(result.stdout, encoding="utf-8")
    return {
        "status": "completed" if result.return_code == 0 else "failed",
        "operations": [result.as_dict()],
        "artifacts": [str(path) for path in plan.outputs if path.exists()],
        "reason": None if result.return_code == 0 else f"command exited {result.return_code}",
    }
