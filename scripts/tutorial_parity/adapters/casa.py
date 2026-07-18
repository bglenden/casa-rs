"""CASA adapter backed by the checked allowlisted worker."""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..evidence import write_json_atomic
from ..model import RuntimeResources, SectionManifest, Surface
from ..resources import ResourceError
from .base import AdapterPlan, execute_plan


class CasaAdapter:
    name = "casa"

    def plan(self, manifest: SectionManifest, surface: Surface, resources: RuntimeResources) -> AdapterPlan:
        if not resources.dry_run and (
            resources.casa_python is None or not resources.casa_python.is_file()
        ):
            raise ResourceError("casa_python_missing", "CASA surface requires --casa-python or CASA_RS_CASA_PYTHON")
        casa_python = resources.casa_python or Path("$CASA_RS_CASA_PYTHON")
        request = resources.evidence_root / "requests" / f"{manifest.section_id}.casa.json"
        result = resources.evidence_root / "workers" / f"{manifest.section_id}.casa.json"
        if not resources.dry_run:
            write_json_atomic(request, {
                "schema_version": 1,
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
        worker = resources.repo_root / "scripts" / "tutorial_parity" / "workers" / "casa_tasks.py"
        env = dict(os.environ)
        env.setdefault("QT_QPA_PLATFORM", "offscreen")
        env.setdefault("MPLBACKEND", "Agg")
        env.setdefault("MPLCONFIGDIR", "/private/tmp/casa-matplotlib")
        env.setdefault("XDG_CACHE_HOME", "/private/tmp/casa-xdg-cache")
        return AdapterPlan(
            surface=self.name,
            argv=(str(casa_python), str(worker), str(request), str(result)),
            cwd=resources.pack_root,
            env=env,
            timeout_seconds=1800,
            outputs=(result, *(resources.pack_root / path for operation in surface.operations for path in operation.outputs)),
            metadata={"request": str(request)},
        )

    def execute(self, plan: AdapterPlan) -> dict[str, object]:
        return execute_plan(plan)
