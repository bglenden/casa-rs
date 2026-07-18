"""GhosttyKit-backed terminal surface adapter."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ..evidence import write_json_atomic
from ..model import RuntimeResources, SectionManifest, Surface
from ..resources import ResourceError
from .base import AdapterPlan, execute_plan
from .cli import build_operation_argv


class TuiAdapter:
    name = "tui"

    def plan(self, manifest: SectionManifest, surface: Surface, resources: RuntimeResources) -> AdapterPlan:
        capture = resources.ghostty_capture
        if not resources.dry_run and (capture is None or not capture.is_file()):
            raise ResourceError(
                "ghostty_capture_missing",
                "TUI evidence requires CASA_RS_GHOSTTY_CAPTURE or --ghostty-capture",
            )
        capture = capture or Path("$CASA_RS_GHOSTTY_CAPTURE")
        if not surface.operations or surface.screenshot is None:
            raise ValueError("TUI adapter requires operations and one final Ghostty screenshot")
        screenshot = resources.pack_root / surface.screenshot
        commands = [build_operation_argv(operation, resources, tui=True) for operation in surface.operations]
        if len(commands) == 1:
            argv = [
                str(capture),
                "--cwd", str(resources.pack_root),
                "--output", str(screenshot),
                "--width", "2200",
                "--height", "1400",
                "--font-size", "12",
                "--settle-seconds", "60",
            ]
            for after_ms, text in surface.input_events:
                argv.extend(["--input-event", f"{after_ms}:{text}"])
            argv.extend(["--", *commands[0]])
        else:
            if resources.native_python is None and not resources.dry_run:
                raise ResourceError("native_python_missing", "multi-operation TUI capture requires native Python")
            request = resources.evidence_root / "requests" / f"{manifest.section_id}.tui.json"
            if not resources.dry_run:
                write_json_atomic(request, {
                    "schema_version": 1,
                    "capture": str(capture),
                    "cwd": str(resources.pack_root),
                    "output": str(screenshot),
                    "commands": commands,
                    "input_events": [{"after_ms": ms, "text": text} for ms, text in surface.input_events],
                })
            worker = resources.repo_root / "scripts" / "tutorial_parity" / "workers" / "tui_batch.py"
            argv = [str(resources.native_python or Path("$CASA_RS_NATIVE_PYTHON")), str(worker), str(request)]
        env = dict(os.environ)
        env.setdefault("XDG_CACHE_HOME", "/private/tmp/ghostty-cache")
        return AdapterPlan(
            surface=self.name,
            argv=tuple(argv),
            cwd=resources.pack_root,
            env=env,
            timeout_seconds=600,
            outputs=(screenshot, *(resources.pack_root / path for operation in surface.operations for path in operation.outputs)),
            metadata={"renderer": "GhosttyKit"},
        )

    def execute(self, plan: AdapterPlan) -> dict[str, Any]:
        return execute_plan(plan)
