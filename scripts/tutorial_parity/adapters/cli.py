"""Allowlisted native command-line adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..model import Operation, RuntimeResources, SectionManifest, Surface
from ..resources import binary
from .base import AdapterPlan, execute_plan


BINARY_BY_TASK = {
    "imhead": "imexplore",
    "imstat": "imexplore",
    "immoments": "immoments",
    "exportfits": "exportfits",
    "msexplore": "msexplore",
    "imager": "casars-imager",
    "split": "mstransform",
    "impbcor": "impbcor",
}

POSITIONAL: dict[str, tuple[str, ...]] = {
    "imhead": ("imagename",),
    "imstat": ("imagename",),
    "immoments": ("imagename",),
    "exportfits": ("imagename", "fitsimage"),
    "msexplore": ("vis",),
    "imager": (),
    "split": (),
    "impbcor": (),
}

PREFIX: dict[str, tuple[str, ...]] = {
    "imhead": ("imhead",),
    "imstat": ("imstat",),
}

FLAG_NAMES = {
    "imagename": "imagename",
    "fitsimage": "fitsimage",
    "outfile": "outfile",
    "outputvis": "outputvis",
    "vis": "ms",
    "cell_arcsec": "cell-arcsec",
    "phasecenter_field": "phasecenter-field",
    "threshold_jy": "threshold-jy",
    "mask_box": "mask-box",
    "write_pb": "write-pb",
    "dirty_only": "dirty-only",
    "keepflags": "keepflags",
    "color_by": "color-by",
    "plot_output": "plot-output",
    "plot_format": "plot-format",
    "plot_width": "plot-width",
    "plot_height": "plot-height",
}

TASK_FLAG_NAMES = {
    "split": {"vis": "ms", "outputvis": "out"},
}

OMIT_FOR_CLI = {"json"}


def build_operation_argv(operation: Operation, resources: RuntimeResources, *, tui: bool = False) -> list[str]:
    task = operation.task
    if task not in BINARY_BY_TASK:
        raise ValueError(f"{task} has no native CLI adapter")
    if tui:
        argv = [str(binary(resources, "casars", require_existing=not resources.dry_run)), task]
    else:
        argv = [
            str(binary(resources, BINARY_BY_TASK[task], require_existing=not resources.dry_run)),
            *PREFIX.get(task, ()),
        ]
    parameters = operation.parameters
    positional = POSITIONAL[task]
    for key in positional:
        if key not in parameters:
            raise ValueError(f"{task}: missing positional parameter {key}")
        argv.append(str(parameters[key]))
    for key, value in parameters.items():
        if key in positional or key in OMIT_FOR_CLI:
            continue
        flag_name = TASK_FLAG_NAMES.get(task, {}).get(
            key, FLAG_NAMES.get(key, key.replace("_", "-"))
        )
        flag = "--" + flag_name
        if isinstance(value, bool):
            if value:
                argv.append(flag)
            elif key in {"write_pb", "dirty_only", "keepflags", "overwrite", "pbcor"}:
                argv.append("--no-" + flag[2:])
            continue
        if isinstance(value, list):
            value = ",".join(str(item) for item in value)
        argv.extend([flag, str(value)])
    if parameters.get("json") is True:
        argv.append("--json")
    return argv


class CliAdapter:
    name = "cli"

    def plan(self, manifest: SectionManifest, surface: Surface, resources: RuntimeResources) -> AdapterPlan:
        if len(surface.operations) != 1:
            runner = resources.native_python
            if runner is None and not resources.dry_run:
                raise ValueError("multi-operation CLI section requires native Python")
            runner = runner or Path("$CASA_RS_NATIVE_PYTHON")
            request = resources.evidence_root / "requests" / f"{manifest.section_id}.cli.json"
            from ..evidence import write_json_atomic

            commands = [build_operation_argv(operation, resources) for operation in surface.operations]
            if not resources.dry_run:
                write_json_atomic(request, {"schema_version": 1, "commands": commands})
            worker = resources.repo_root / "scripts" / "tutorial_parity" / "workers" / "command_batch.py"
            argv = [str(runner), str(worker), str(request)]
        else:
            argv = build_operation_argv(surface.operations[0], resources)
        return AdapterPlan(
            surface=self.name,
            argv=tuple(argv),
            cwd=resources.pack_root,
            env=None,
            timeout_seconds=1800,
            outputs=tuple(resources.pack_root / path for operation in surface.operations for path in operation.outputs),
            metadata={
                "tasks": [operation.task for operation in surface.operations],
                "capture_stdout": (
                    str(resources.pack_root / surface.operations[0].capture_stdout)
                    if len(surface.operations) == 1 and surface.operations[0].capture_stdout
                    else None
                ),
            },
        )

    def execute(self, plan: AdapterPlan) -> dict[str, Any]:
        return execute_plan(plan)
