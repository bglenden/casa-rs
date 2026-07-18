"""Native Python adapter backed by a checked allowlisted worker."""

from __future__ import annotations

from pathlib import Path

from ..commands import environment_with_pythonpath, run_command
from ..evidence import write_json_atomic
from ..model import RuntimeResources, SectionManifest, Surface
from ..resources import ResourceError
from .base import AdapterPlan, execute_plan


class PythonAdapter:
    name = "python"

    def plan(self, manifest: SectionManifest, surface: Surface, resources: RuntimeResources) -> AdapterPlan:
        if not resources.dry_run and (
            resources.native_python is None or not resources.native_python.is_file()
        ):
            raise ResourceError("native_python_missing", "Python surface requires resolved native Python")
        native_python = resources.native_python or Path("$CASA_RS_NATIVE_PYTHON")
        if not resources.dry_run:
            completed = run_command(
                [str(native_python), "-c", "import casars._core"],
                cwd=resources.repo_root,
                env=environment_with_pythonpath(resources.repo_root),
                timeout_seconds=30,
            )
            if completed.return_code != 0:
                raise ResourceError(
                    "native_python_binding_missing",
                    "native Python surface requires an installed casars._core extension",
                )
        request = resources.evidence_root / "requests" / f"{manifest.section_id}.python.json"
        result = resources.evidence_root / "workers" / f"{manifest.section_id}.python.json"
        if not resources.dry_run:
            write_json_atomic(request, {
                "schema_version": 1,
                "binary_dir": str(resources.binary_dir),
                "operations": [
                    {
                        "task": operation.task,
                        "parameters": operation.parameters,
                        "capture": (
                            str(resources.pack_root / operation.capture_stdout)
                            if operation.capture_stdout else None
                        ),
                    }
                    for operation in surface.operations
                ],
            })
        worker = resources.repo_root / "scripts" / "tutorial_parity" / "workers" / "native_tasks.py"
        return AdapterPlan(
            surface=self.name,
            argv=(str(native_python), str(worker), str(request), str(result)),
            cwd=resources.pack_root,
            env=environment_with_pythonpath(resources.repo_root),
            timeout_seconds=1800,
            outputs=(result, *(resources.pack_root / path for operation in surface.operations for path in operation.outputs)),
            metadata={"request": str(request)},
        )

    def execute(self, plan: AdapterPlan) -> dict[str, object]:
        return execute_plan(plan)
