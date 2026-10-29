#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the CASA task inventory and render casa-rs's compatibility report.

The inventory (`resources/casa-compat/casa-6.7.6.14/`) records each CASA task
casa-rs mimics, as `tools/casa-compat/dump_casa_task_inventory.py` read it
from CASA 6.7.6.14 (#931 4a). This script needs no CASA. It checks that the
fixtures are complete and well formed, that every CASA-named casa-rs surface
has its task among them, and that `docs/reference/casa-compatibility.md`,
which compares each surface with its task, is current.

    python3 scripts/check-casa-inventory.py           # check (CI)
    python3 scripts/check-casa-inventory.py --write   # regenerate the report

Comparing the fixtures with an installed CASA, which compatibility
acceptance requires, is `scripts/test-casa-inventory.sh`.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CASA_VERSION = "6.7.6.14"
INVENTORY = REPO_ROOT / "resources" / "casa-compat" / f"casa-{CASA_VERSION}"
SURFACES = REPO_ROOT / "crates" / "casa-provider-contracts" / "resources" / "parameter-surfaces.json"
REPORT = REPO_ROOT / "docs" / "reference" / "casa-compatibility.md"

# The CASA task each casa-rs surface mimics, when its contract does not name
# it yet (`casa_task`): imager is CASA's tclean (#931 PR 4e). calibrate and
# msexplore are casa-rs tasks with no CASA counterpart.
CASA_TASK = {"imager": "tclean"}
CASA_RS_ONLY = frozenset({"calibrate", "msexplore"})
# plotms waits for its own decision (#931): casaplotms is a GUI program.
PENDING = {"plotms": "plotms waits for its own decision (#931): CASA's is the casaplotms GUI"}


class InventoryError(RuntimeError):
    """The inventory is incomplete or malformed."""


def load_inventory() -> dict[str, dict[str, Any]]:
    index_path = INVENTORY / "inventory.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    if index.get("casa_version") != CASA_VERSION:
        raise InventoryError(f"{index_path} records CASA {index.get('casa_version')}, not {CASA_VERSION}")
    tasks = {}
    for name in index["tasks"]:
        path = INVENTORY / "tasks" / f"{name}.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        problems = [
            f"{path}: {key} is missing"
            for key in ("casa_version", "task", "source", "parameters", "defaults_call")
            if key not in record
        ]
        if record.get("task") != name or record.get("casa_version") != CASA_VERSION:
            problems.append(f"{path} records {record.get('task')} from CASA {record.get('casa_version')}")
        names = [parameter.get("name") for parameter in record.get("parameters", [])]
        if len(set(names)) != len(names) or not all(names):
            problems.append(f"{path}: parameter names are missing or repeated")
        for parameter in record.get("parameters", []):
            if "default" not in parameter or "schema" not in parameter:
                problems.append(f"{path}: {parameter.get('name')} lacks its default or schema")
        if problems:
            raise InventoryError("\n".join(problems))
        tasks[name] = record
    return tasks


def load_surfaces() -> dict[str, dict[str, Any]]:
    document = json.loads(SURFACES.read_text(encoding="utf-8"))
    return {surface["id"]: surface for surface in document["surfaces"]}


def casa_task(surface: dict[str, Any]) -> str | None:
    if surface["id"] in CASA_RS_ONLY:
        return None
    return surface.get("casa_task") or CASA_TASK.get(surface["id"], surface["id"])


def casa_default(text: str | None) -> Any:
    """CASA's default as a value, where its `repr` is a literal."""
    if text is None:
        return "required"
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return text


def plain(value: dict[str, Any]) -> Any:
    """A casa-rs parameter value as a plain value."""
    if value["kind"] == "array":
        return [plain(item) for item in value["value"]]
    return value["value"]


def rs_default(binding: dict[str, Any]) -> Any:
    default = binding["default"]
    if default["kind"] == "required":
        return "required"
    if default["kind"] == "literal":
        return plain(default["value"])
    return f"{default['kind']} (fallback {plain(default['fallback'])!r})"


def same(casa: Any, ours: Any) -> bool:
    if isinstance(casa, (int, float)) and isinstance(ours, (int, float)) and not isinstance(casa, bool):
        return float(casa) == float(ours) and not isinstance(ours, bool)
    if isinstance(casa, (list, tuple)) and isinstance(ours, list):
        return len(casa) == len(ours) and all(same(a, b) for a, b in zip(casa, ours))
    return casa == ours


def compare(surface: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    """How a casa-rs surface stands against its CASA task."""
    bindings = sorted(surface["bindings"], key=lambda binding: binding["order"])
    ours = {binding["name"]: binding for binding in bindings}
    theirs = [parameter["name"] for parameter in record["parameters"]]
    shared = [name for name in theirs if name in ours]
    in_our_order = [binding["name"] for binding in bindings if binding["name"] in set(theirs)]
    defaults = []
    for parameter in record["parameters"]:
        binding = ours.get(parameter["name"])
        if binding is None:
            continue
        casa, mine = casa_default(parameter["default"]), rs_default(binding)
        if not same(casa, mine):
            defaults.append((parameter["name"], parameter["default"], mine))
    return {
        "missing": [name for name in theirs if name not in ours],
        "extra": [binding["name"] for binding in bindings if binding["name"] not in set(theirs)],
        "shared": len(shared),
        "order_holds": shared == in_our_order,
        "defaults": defaults,
        "default_only": [
            (binding["name"], binding["unimplemented"]["issue"])
            for binding in bindings
            if binding.get("unimplemented")
        ],
        "exact": bool(surface.get("casa_task")),
    }


def code(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value)
    return "`" + text.replace("`", "'").replace("|", "\\|") + "`"


def names(items: list[str]) -> str:
    return ", ".join(code(item) for item in items) if items else "none"


def outcome(call: dict[str, Any]) -> str:
    if "raises" in call:
        return f"raises `{call['raises']}`: {call['message']}"
    if "returns" in call:
        return f"returns {code(call['returns'])}"
    return code(call)


def task_section(surface_id: str, record: dict[str, Any], result: dict[str, Any] | None) -> list[str]:
    title = surface_id if record["task"] == surface_id else f"{surface_id} (CASA's {record['task']})"
    lines = [f"### {title}", ""]
    lines.append(f"- CASA source: `{record['source']}`, {len(record['parameters'])} parameters")
    implementation = record.get("implementation")
    if implementation:
        returns = implementation["returns"]
        lines.append(
            f"- Implementation: `{implementation['source']}`, `{implementation['function']}` "
            f"(line {implementation['line']}): {len(returns)} return statements, "
            f"{len(implementation['raises'])} raise statements"
        )
        for entry in returns:
            lines.append(f"  - returns at line {entry['line']}: {code(entry['value'] or 'None')}")
    lines.append(f"- Called with its defaults in an empty directory, CASA {outcome(record['defaults_call'])}")
    if result is None:
        lines.append(f"- casa-rs: {PENDING.get(surface_id, 'no surface')}")
        return [*lines, ""]
    lines.append(f"- CASA's signature: {'yes' if result['exact'] else 'not yet'}")
    lines.append(f"- CASA parameters casa-rs lacks: {names(result['missing'])}")
    by_issue: dict[int, list[str]] = {}
    for name, issue in result["default_only"]:
        by_issue.setdefault(issue, []).append(name)
    for issue, pending in sorted(by_issue.items()):
        lines.append(f"- Accepted only at CASA's default until #{issue}: {names(pending)}")
    lines.append(f"- casa-rs parameters CASA lacks: {names(result['extra'])}")
    lines.append(
        f"- Shared parameters: {result['shared']}, in CASA's order: {'yes' if result['order_holds'] else 'no'}"
    )
    if result["defaults"]:
        lines.append("- Defaults that differ (CASA, casa-rs):")
        for name, casa, ours in result["defaults"]:
            lines.append(f"  - {code(name)}: {code(casa or 'required')}, {code(ours)}")
    else:
        lines.append("- Defaults that differ: none")
    return [*lines, ""]


def render(tasks: dict[str, dict[str, Any]], surfaces: dict[str, dict[str, Any]]) -> str:
    rows, sections = [], []
    covered = set()
    for surface_id in sorted(surfaces):
        task = casa_task(surfaces[surface_id])
        if task is None:
            continue
        if task not in tasks:
            raise InventoryError(f"surface {surface_id} mimics CASA's {task}, which the inventory lacks")
        covered.add(task)
        result = None if surface_id in PENDING else compare(surfaces[surface_id], tasks[task])
        sections.extend(task_section(surface_id, tasks[task], result))
        if result is None:
            rows.append(f"| {surface_id} | {task} | pending | {len(tasks[task]['parameters'])} | pending | | | | |")
            continue
        rows.append(
            f"| {surface_id} | {task} | {'yes' if result['exact'] else 'no'} | "
            f"{len(tasks[task]['parameters'])} | {result['shared']} | {len(result['missing'])} | "
            f"{len(result['default_only'])} | {len(result['extra'])} | {len(result['defaults'])} |"
        )
    unused = sorted(set(tasks) - covered)
    if unused:
        raise InventoryError(f"the inventory records tasks no surface mimics: {unused}")
    header = [
        "# casa-rs and CASA 6.7.6.14 tasks",
        "",
        "Truth class: generated reference",
        "Generated by: `scripts/check-casa-inventory.py --write`",
        f"Sources: `resources/casa-compat/casa-{CASA_VERSION}/` (read from CASA by "
        "`tools/casa-compat/dump_casa_task_inventory.py`), "
        "`crates/casa-provider-contracts/resources/parameter-surfaces.json`",
        "",
        "> Do not edit this file by hand. Regenerate it after changing the inventory or a surface.",
        "",
        "Each casa-rs task that mimics a CASA task (#931) against that task as CASA "
        f"{CASA_VERSION} defines it: whether it presents CASA's signature, the CASA parameters "
        "casa-rs does not take yet, those it takes but accepts only at CASA's default, the "
        "parameters casa-rs takes that CASA does not (casa-rs names for CASA concepts, or "
        "casa-rs-only controls), whether the shared parameters keep CASA's order, and the "
        "defaults that differ. `calibrate` and `msexplore` are casa-rs tasks with no CASA "
        "counterpart.",
        "",
        "The CASA parameters casa-rs lacks or accepts only at their defaults are tracked by "
        "family: images in #977, MeasurementSet and flagging in #978, calibration in #979 and "
        "`tclean` in #980.",
        "",
        "## Summary",
        "",
        "| Surface | CASA task | CASA's signature | CASA parameters | Shared | CASA's casa-rs lacks "
        "| Default only | casa-rs's CASA lacks | Defaults differ |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
        *rows,
        "",
        "## Tasks",
        "",
    ]
    return "\n".join([*header, *sections]).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--write", action="store_true", help="regenerate the report")
    args = parser.parse_args()
    try:
        tasks = load_inventory()
        text = render(tasks, load_surfaces())
    except InventoryError as error:
        print(f"casa-inventory: {error}", file=sys.stderr)
        return 1
    if args.write:
        REPORT.write_text(text, encoding="utf-8")
        print(f"casa-inventory: wrote {REPORT.relative_to(REPO_ROOT)}")
        return 0
    if not REPORT.is_file() or REPORT.read_text(encoding="utf-8") != text:
        print(
            f"casa-inventory: {REPORT.relative_to(REPO_ROOT)} is stale; run "
            "scripts/check-casa-inventory.py --write",
            file=sys.stderr,
        )
        return 1
    print(f"casa-inventory: {len(tasks)} CASA {CASA_VERSION} tasks checked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
