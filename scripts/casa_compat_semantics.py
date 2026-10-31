#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reviewed behavioral annotations for the generated CASA reference (#980).

Signature, defaults, domains and runtime refusal conditions remain generated
from the existing inventory and contracts. An implementation label never
promotes an evidence item into a successful scientific comparison.
"""

from __future__ import annotations

import json
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

STATUSES = {"Implemented", "Partial", "Default only", "Not implemented", "Unassessed"}


def cell(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def literal(value: Any) -> Any:
    if isinstance(value, dict) and "kind" in value and "value" in value:
        if value["kind"] == "array":
            return [literal(item) for item in value["value"]]
        return value["value"]
    return value


def condition(value: dict[str, Any]) -> str:
    kind = value["kind"]
    if kind in {"always", "never"}:
        return kind
    if kind == "equals":
        return f"{value['parameter']}={literal(value['value'])!r}"
    if kind == "not":
        return f"not ({condition(value['predicate'])})"
    if kind in {"any", "all"}:
        separator = " or " if kind == "any" else " and "
        return "(" + separator.join(condition(item) for item in value["predicates"]) + ")"
    return json.dumps(value, sort_keys=True)


def domain(value: dict[str, Any]) -> str:
    kind = value["kind"]
    if kind == "choice":
        return ", ".join(f"`{item}`" for item in value["values"])
    if kind == "array":
        maximum = value.get("max_items")
        extent = f"{value.get('min_items', 0)}..{maximum if maximum is not None else 'many'}"
        scalar = "; scalar allowed" if value.get("allow_scalar") else ""
        return f"list[{domain(value['element'])}], length {extent}{scalar}"
    return f"`{kind}`" + (f" ({value['resource_kind']})" if "resource_kind" in value else "")


def implementation(binding: dict[str, Any] | None, entry: dict[str, Any]) -> str:
    if binding is None:
        return "Not implemented"
    if binding.get("unimplemented"):
        return "Default only"
    status = entry.get("status", "Unassessed")
    if status not in STATUSES or status == "Default only":
        raise ValueError(f"invalid semantic implementation status: {status}")
    return status


def validate(data: dict[str, Any], record: dict[str, Any], surface: dict[str, Any]) -> None:
    if data.get("schema_version") != 1 or data.get("task") != record["task"]:
        raise ValueError("semantic metadata schema/task mismatch")
    if data.get("casa_version") != record["casa_version"]:
        raise ValueError("semantic metadata oracle version mismatch")
    parameters = data["parameters"]
    expected = {item["name"] for item in record["parameters"]}
    if set(parameters) != expected:
        raise ValueError(f"semantic parameter coverage differs: {sorted(set(parameters) ^ expected)}")
    bindings = {item["name"]: item for item in surface["bindings"]}
    evidence = data["evidence"]
    behaviors = {item["id"] for item in data["behaviors"]}
    if len(behaviors) != len(data["behaviors"]):
        raise ValueError("duplicate semantic behavior id")
    for name, entry in parameters.items():
        implementation(bindings.get(name), entry)
        if bindings.get(name, {}).get("unimplemented") and "status" in entry:
            raise ValueError(f"{name}: default-only status must come from the runtime contract")
        if not entry.get("scope") or not entry.get("evidence"):
            raise ValueError(f"{name}: scope and evidence classification required")
    for entry in [*parameters.values(), *data["behaviors"], *data["decisions"]]:
        if set(entry.get("evidence", [])) - evidence.keys():
            raise ValueError("unknown semantic evidence reference")
        if set(entry.get("behaviors", [])) - behaviors:
            raise ValueError("unknown semantic behavior reference")
    for entry in data["evidence"].values():
        if entry["state"] == "passed" and not all(entry.get(k) for k in ("casa_version", "revision", "command", "receipt")):
            raise ValueError("passed evidence needs exact oracle/revision, command and receipt")
    for entry in data["decisions"]:
        if entry["state"] not in {"Agreed for now", "Previously agreed", "Proposed", "Unfinished"}:
            raise ValueError("invalid decision state")
        if "agreed" in entry["state"].lower() and not entry.get("owner_decision"):
            raise ValueError("an agreed deviation needs the owner decision")
        for key in ("inputs", "casa", "ours", "rationale", "impact"):
            if not entry.get(key):
                raise ValueError(f"decision lacks {key}")


def evidence_links(ids: list[str]) -> str:
    return ", ".join(f"[{key}](#tclean-evidence-{key})" for key in ids) or "Unassessed"


def runtime(binding: dict[str, Any], concept: dict[str, Any]) -> str:
    parts = ["Catalog domain: " + domain(concept["value_domain"])]
    default_only = binding.get("unimplemented")
    if default_only:
        when = condition(default_only.get("applies_when", {"kind": "always"}))
        parts.append(f"Non-default refused when {when} (#{default_only['issue']})")
    forms = binding.get("unimplemented_forms")
    if forms:
        parts.append(f"Unsupported form: {forms['forms']} (#{forms['issue']})")
    constraints = [*concept.get("base_constraints", []), *binding.get("refinements", [])]
    if constraints:
        parts.append("Constraints: `" + json.dumps(constraints, sort_keys=True) + "`")
    return "; ".join(parts)


def render(data: dict[str, Any], record: dict[str, Any], surface: dict[str, Any], concepts: dict[tuple[str, int], dict[str, Any]], defaults: Any) -> list[str]:
    validate(data, record, surface)
    bindings = {item["name"]: item for item in surface["bindings"]}
    counts = Counter(implementation(bindings.get(name), entry) for name, entry in data["parameters"].items())
    lines = [
        "#### tclean parameter implementation", "",
        "Implementation status is **not behavioral verification**. Implemented means the stated scope has a native code path; Partial has known missing forms or combinations; Default only is derived from the runtime refusal contract; Not implemented has no implementation; Unassessed has not been reviewed. Evidence states are listed separately below. No signature or return-structure result establishes scientific equivalence.", "",
        f"Audit baseline: `{data['audit_revision']}`. Status counts: " + ", ".join(f"{key}: {counts[key]}" for key in sorted(STATUSES)) + ".", "",
        "Mappings, defaults, catalog domains, constraints and conditional default-only refusals are generated from the existing task contract. The catalog may admit values that the native reader then refuses: the reviewed scope and linked combinations describe those restrictions.", "",
        "| CASA parameter | CASA default | casa-rs default; mapping | Implementation | Scope and runtime restrictions | Evidence |",
        "|---|---|---|---|---|---|",
    ]
    for parameter in record["parameters"]:
        name = parameter["name"]
        entry, binding = data["parameters"][name], bindings.get(name)
        status = implementation(binding, entry)
        mapping = "no binding"
        restrictions = ""
        if binding:
            ref = binding["concept"]
            concept = concepts[(ref["id"], ref["semantic_revision"])]
            mapping = f"`{defaults(binding)!r}`; `{name}` → `{entry.get('mapping', binding['projections']['provider']['field'])}`"
            restrictions = runtime(binding, concept)
        behaviors = " ".join(f"[{key}](#tclean-behavior-{key})" for key in entry.get("behaviors", []))
        scope = f"{entry['scope']} {behaviors}<br>{restrictions}"
        lines.append("| " + " | ".join(map(cell, [f"`{name}`", f"`{parameter['default']}`", mapping, status, scope, evidence_links(entry["evidence"])])) + " |")
    lines.extend(["", "#### tclean interacting modes and task behavior", ""])
    for entry in data["behaviors"]:
        lines.extend([f"<a id=\"tclean-behavior-{entry['id']}\"></a>", f"##### {entry['title']}", "", f"**{entry['status']}** — {entry['scope']}", "", entry["gap"], "", "Evidence: " + evidence_links(entry["evidence"]) + ".", ""])
    lines.extend(["#### tclean deviations and unfinished work", ""])
    for entry in data["decisions"]:
        lines.extend([f"##### {entry['id']}: {entry['state']}", "", f"- Inputs: {entry['inputs']}", f"- CASA: {entry['casa']}", f"- casa-rs: {entry['ours']}", f"- Rationale: {entry['rationale']}", f"- User impact: {entry['impact']}", f"- Owner decision: {entry.get('owner_decision', 'Pending; not an accepted deviation.')}", f"- Evidence: {evidence_links(entry['evidence'])}.", ""])
    lines.extend(["#### tclean evidence", "", "Evidence is scoped to its named revision, inputs and comparator. Historical reported results need applicability review; planned or unexecuted tests are not passes.", ""])
    for key, entry in data["evidence"].items():
        lines.extend([f"<a id=\"tclean-evidence-{key}\"></a>", f"##### {key}", "", f"**{entry['kind']} — {entry['state']}.** {entry['scope']}", ""])
        for link in entry["links"]:
            target = link if link.startswith("https://") else f"https://github.com/bglenden/casa-rs/blob/{data['audit_revision']}/{link}"
            lines.append(f"- [{link}]({target})")
        if entry.get("receipt"):
            lines.extend([f"- Revision: `{entry['revision']}`; CASA: `{entry['casa_version']}`.", f"- Command: `{entry['command']}`.", f"- Receipt: `{entry['receipt']}`."])
        lines.append("")
    return lines


def load(root: Path, task: str) -> dict[str, Any]:
    data = json.loads((root / "resources/casa-compat/semantics" / f"{task}.json").read_text())
    validate_source_links(root, data)
    return data


def validate_source_links(root: Path, data: dict[str, Any]) -> None:
    revision = data["audit_revision"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("semantic source links need an exact commit")
    paths = sorted({link for item in data["evidence"].values() for link in item["links"] if not link.startswith("https://")})
    result = subprocess.run(
        ["git", "-C", str(root), "cat-file", "--batch-check=%(objectname) %(objecttype)"],
        input="".join(f"{revision}:{path}\n" for path in paths),
        text=True, capture_output=True, check=True,
    )
    if any(line.endswith(" missing") for line in result.stdout.splitlines()):
        raise ValueError("semantic evidence links a file absent at its pinned revision")
