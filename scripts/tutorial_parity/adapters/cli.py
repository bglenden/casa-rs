"""Allowlisted native command-line adapter."""

from __future__ import annotations

import functools
import json
from pathlib import Path
from typing import Any

from ..model import Operation, RuntimeResources, SectionManifest, Surface
from ..resources import binary
from .base import AdapterPlan, execute_plan

SURFACES = Path(__file__).resolve().parents[3] / "crates/casa-provider-contracts/resources/parameter-surfaces.json"


BINARY_BY_TASK = {
    "imhead": "imexplore",
    "imstat": "imexplore",
    "immoments": "immoments",
    "exportfits": "exportfits",
    "msexplore": "msexplore",
    "tclean": "casars-imager",
    "split": "mstransform",
    "impbcor": "impbcor",
}

POSITIONAL: dict[str, tuple[str, ...]] = {
    "imhead": ("imagename",),
    "imstat": ("imagename",),
    "immoments": ("imagename",),
    "exportfits": ("imagename", "fitsimage"),
    "msexplore": ("vis",),
    "tclean": (),
    "split": (),
    "impbcor": (),
}

PREFIX: dict[str, tuple[str, ...]] = {
    "imhead": ("imhead",),
    "imstat": ("imstat",),
    "split": ("split",),
}

FLAG_NAMES = {
    "imagename": "imagename",
    "fitsimage": "fitsimage",
    "outfile": "outfile",
    "outputvis": "outputvis",
    "vis": "ms",
    "write_pb": "write-pb",
    "keepflags": "keepflags",
    "color_by": "color-by",
    "plot_output": "plot-output",
    "plot_format": "plot-format",
}

TASK_FLAG_NAMES = {
    "split": {"vis": "vis", "outputvis": "outputvis"},
}

OMIT_FOR_CLI = {"json"}


def returns_value(operation: Operation) -> bool:
    """Whether the section compares the task's return value, as JSON: the
    operation then runs through the binary's `--json-run` (#931)."""

    return operation.parameters.get("json") is True


def task_request(operation: Operation) -> dict[str, Any]:
    """The front end's `--json-run` request for `operation`: its surface and
    the CASA-named values it sets."""

    values = {key: value for key, value in operation.parameters.items() if key not in OMIT_FOR_CLI}
    return {"surface": operation.task, "values": values}


@functools.cache
def surface_bindings(task: str) -> dict[str, dict[str, Any]]:
    """`task`'s surface bindings by name, from the provider-contract
    catalog; none when the catalog has no surface of that name."""

    for surface in json.loads(SURFACES.read_text(encoding="utf-8"))["surfaces"]:
        if surface["id"] == task:
            return {binding["name"]: binding for binding in surface["bindings"]}
    return {}


def boolean_argv(task: str, key: str, flag: str, value: bool) -> list[str]:
    """`key=value` on the command line, as `task`'s surface spells it: its
    flag for True, its false flag for False, and nothing for a False the
    surface defaults to. A False the surface cannot spell is refused rather
    than dropped, since the run would take the default instead."""

    binding = surface_bindings(task).get(key)
    if binding is None:
        return [flag] if value else []
    cli = binding["projections"].get("cli") or {}
    if value:
        return [(cli.get("flags") or [flag])[0]]
    if cli.get("false_flags"):
        return [cli["false_flags"][0]]
    default = binding.get("default", {})
    if default.get("kind") == "literal" and default["value"] == {"kind": "bool", "value": False}:
        return []
    raise ValueError(f"{task}: {key}=False has no command-line spelling")


def build_operation_argv(
    operation: Operation, resources: RuntimeResources, request: Path | None = None
) -> list[str]:
    task = operation.task
    if task not in BINARY_BY_TASK:
        raise ValueError(f"{task} has no native CLI adapter")
    argv = [
        str(binary(resources, BINARY_BY_TASK[task], require_existing=not resources.dry_run)),
        *PREFIX.get(task, ()),
    ]
    if returns_value(operation):
        if request is None:
            raise ValueError(f"{task}: a return value is captured from a one-operation section")
        return [*argv, "--json-run", str(request)]
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
            argv.extend(boolean_argv(task, key, flag, value))
            continue
        if isinstance(value, list):
            value = ",".join(str(item) for item in value)
        argv.extend([flag, str(value)])
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
            operation = surface.operations[0]
            request = None
            if returns_value(operation):
                from ..evidence import write_json_atomic

                request = resources.evidence_root / "requests" / f"{manifest.section_id}.cli-run.json"
                if not resources.dry_run:
                    write_json_atomic(request, task_request(operation))
            argv = build_operation_argv(operation, resources, request)
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
                # The binary's stdout is then its typed reply; the capture
                # keeps the task's return value.
                "capture_reply_value": len(surface.operations) == 1
                and returns_value(surface.operations[0]),
            },
        )

    def execute(self, plan: AdapterPlan) -> dict[str, Any]:
        return execute_plan(plan)
