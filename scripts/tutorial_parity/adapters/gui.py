"""GUI adapter delegating interaction claims to the consolidated XCUITest journey."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ..model import RuntimeResources, SectionManifest, Surface
from .base import AdapterPlan, execute_plan


class GuiAdapter:
    name = "gui"

    def plan(self, manifest: SectionManifest, surface: Surface, resources: RuntimeResources) -> AdapterPlan:
        if surface.journey != "tutorial-journey-gui":
            raise ValueError("GUI tutorial evidence must use tutorial-journey-gui")
        harness = resources.repo_root / "apps" / "casars-mac" / "script" / "gui_acceptance.py"
        if resources.native_python is None and not resources.dry_run:
            raise ValueError("GUI adapter requires resolved native Python")
        native_python = resources.native_python or Path("$CASA_RS_NATIVE_PYTHON")
        artifact_root = resources.evidence_root / "gui"
        env = dict(os.environ)
        env["CASA_RS_GUI_TEST_ARTIFACT_ROOT"] = str(artifact_root)
        return AdapterPlan(
            surface=self.name,
            argv=(str(native_python), str(harness), "run", surface.journey),
            cwd=resources.repo_root,
            env=env,
            timeout_seconds=4200,
            outputs=tuple(artifact_root / path for path in surface.required_artifacts),
            metadata={"journey": surface.journey, "exclusive_foreground": True},
        )

    def execute(self, plan: AdapterPlan) -> dict[str, Any]:
        return execute_plan(plan)
