#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reviewed behavioral annotations for the generated CASA reference (#980, #1053).

A task's file (`resources/casa-compat/semantics/<task>.json`) reviews each
CASA parameter; a tool's (`semantics/tools/<tool>.json`) reviews each method
of the CASA class, against the signatures recorded from CASA in
`crates/casars-python/python/tests/data/casa_<tool>_signatures.json`. Both
carry the same behaviors, decisions and evidence.

Signature, defaults, domains and runtime refusal conditions remain generated
from the existing inventory and contracts. An implementation label never
promotes an evidence item into a successful scientific comparison.
"""

from __future__ import annotations

import json
import re
import subprocess
import urllib.parse
from collections import Counter
from pathlib import Path
from typing import Any

STATUSES = {"Implemented", "Partial", "Default only", "Not implemented", "Unassessed"}
# A tool method's status. CASA itself disables some methods or deprecates
# them; casa-rs then does what CASA does.
TOOL_STATUSES = (
    "Implemented", "Partial", "Not implemented", "Disabled in CASA", "Deprecated in CASA", "Unassessed",
)
# What an evidence item is. `passed` and `failed` are executed comparisons
# against a stated gate; `measured` is executed without one, or with mixed
# results; `superseded` names its replacement in `superseded_by`. `result`
# says what was found, in words.
EVIDENCE_STATES = {
    "planned", "recorded", "inspected", "measured", "passed", "failed", "superseded", "owner decision",
}
DECISION_STATES = {"Agreed for now", "Previously agreed", "Proposed", "Unfinished"}
# Where deviations are recorded; a decision's `record` names its heading there.
DEVIATIONS = "docs/CASA (C++) bugs.md"
# Files whose evidence states predate EVIDENCE_STATES and are free text.
# This list only shrinks.
FREE_TEXT_STATES = frozenset({"tclean"})


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
    for name, entry in parameters.items():
        implementation(bindings.get(name), entry)
        if bindings.get(name, {}).get("unimplemented") and "status" in entry:
            raise ValueError(f"{name}: default-only status must come from the runtime contract")
        if not entry.get("scope") or not entry.get("evidence"):
            raise ValueError(f"{name}: scope and evidence classification required")
    validate_records(data, data["task"], parameters.values())


def validate_records(data: dict[str, Any], subject: str, rows: Any) -> None:
    """The behaviors, decisions and evidence a task's or tool's rows cite."""
    evidence = data["evidence"]
    behaviors = {item["id"] for item in data["behaviors"]}
    if len(behaviors) != len(data["behaviors"]):
        raise ValueError("duplicate semantic behavior id")
    for entry in [*rows, *data["behaviors"], *data["decisions"]]:
        if set(entry.get("evidence", [])) - evidence.keys():
            raise ValueError("unknown semantic evidence reference")
        if set(entry.get("behaviors", [])) - behaviors:
            raise ValueError("unknown semantic behavior reference")
    for key, entry in evidence.items():
        state = entry["state"]
        if state not in EVIDENCE_STATES and subject not in FREE_TEXT_STATES:
            raise ValueError(f"evidence {key}: state {state!r} is not one of {sorted(EVIDENCE_STATES)}")
        if state == "passed" and not all(entry.get(k) for k in ("casa_version", "revision", "command", "receipt")):
            raise ValueError("passed evidence needs exact oracle/revision, command and receipt")
        if state == "superseded" and entry.get("superseded_by") not in evidence:
            raise ValueError(f"evidence {key}: superseded evidence names its replacement in superseded_by")
    for entry in data["decisions"]:
        if entry["state"] not in DECISION_STATES:
            raise ValueError("invalid decision state")
        if "agreed" in entry["state"].lower() and not entry.get("owner_decision"):
            raise ValueError("an agreed deviation needs the owner decision")
        for key in ("inputs", "casa", "ours", "rationale", "impact"):
            if not entry.get(key):
                raise ValueError(f"decision lacks {key}")
        if "record" in entry and not entry["record"]:
            raise ValueError(f"decision {entry['id']}: an empty record")


def validate_tool(data: dict[str, Any], signatures: dict[str, Any]) -> None:
    """A tool's file reviews every method CASA's class has, and no other."""
    if data.get("schema_version") != 1 or data.get("tool") != signatures["tool"]:
        raise ValueError("tool semantic metadata schema/tool mismatch")
    if data.get("casa_version") != signatures["casa_version"].replace("-", "."):
        raise ValueError("tool semantic metadata oracle version mismatch")
    methods = data["methods"]
    if set(methods) != set(signatures["methods"]):
        raise ValueError(f"semantic method coverage differs: {sorted(set(methods) ^ set(signatures['methods']))}")
    for name, entry in methods.items():
        status = entry.get("status")
        if status not in TOOL_STATUSES:
            raise ValueError(f"{name}: invalid semantic implementation status: {status}")
        if not entry.get("scope") or not entry.get("evidence"):
            raise ValueError(f"{name}: scope and evidence classification required")
        if status in {"Partial", "Not implemented"} and not isinstance(entry.get("issue"), int):
            raise ValueError(f"{name}: a {status} method names the issue its refusals cite")
    validate_records(data, data["tool"], methods.values())


def blob(revision: str, path: str) -> str:
    """A repository file at an exact revision on GitHub."""
    return f"https://github.com/bglenden/casa-rs/blob/{revision}/{urllib.parse.quote(path)}"


def heading_anchor(heading: str) -> str:
    """GitHub's anchor for a Markdown heading."""
    return re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")


def evidence_links(ids: list[str], subject: str = "tclean") -> str:
    return ", ".join(f"[{key}](#{subject}-evidence-{key})" for key in ids) or "Unassessed"


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
    task = data["task"]
    bindings = {item["name"]: item for item in surface["bindings"]}
    counts = Counter(implementation(bindings.get(name), entry) for name, entry in data["parameters"].items())
    lines = [
        f"#### {task} parameter implementation", "",
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
        scope = f"{entry['scope']} {behavior_links(entry, task)}<br>{restrictions}"
        lines.append("| " + " | ".join(map(cell, [f"`{name}`", f"`{parameter['default']}`", mapping, status, scope, evidence_links(entry["evidence"], task)])) + " |")
    return [*lines, *render_records(data, task, "task")]


def behavior_links(entry: dict[str, Any], subject: str) -> str:
    return " ".join(f"[{key}](#{subject}-behavior-{key})" for key in entry.get("behaviors", []))


def render_records(data: dict[str, Any], subject: str, kind: str) -> list[str]:
    """The behaviors, decisions and evidence of a task's or tool's file."""
    lines = ["", f"#### {subject} interacting modes and {kind} behavior", ""]
    for entry in data["behaviors"]:
        lines.extend([f"<a id=\"{subject}-behavior-{entry['id']}\"></a>", f"##### {entry['title']}", "", f"**{entry['status']}** — {entry['scope']}", "", entry["gap"], "", "Evidence: " + evidence_links(entry["evidence"], subject) + ".", ""])
    lines.extend([f"#### {subject} deviations and unfinished work", ""])
    for entry in data["decisions"]:
        record = [f"- Record: [{entry['record']}]({blob(data['audit_revision'], DEVIATIONS)}#{heading_anchor(entry['record'])})"] if entry.get("record") else []
        lines.extend([f"##### {entry['id']}: {entry['state']}", "", *record, f"- Inputs: {entry['inputs']}", f"- CASA: {entry['casa']}", f"- casa-rs: {entry['ours']}", f"- Rationale: {entry['rationale']}", f"- User impact: {entry['impact']}", f"- Owner decision: {entry.get('owner_decision', 'Pending; not an accepted deviation.')}", f"- Evidence: {evidence_links(entry['evidence'], subject)}.", ""])
    lines.extend([f"#### {subject} evidence", "", "Evidence is scoped to its named revision, inputs and comparator. Historical reported results need applicability review; planned or unexecuted tests are not passes.", ""])
    for key, entry in data["evidence"].items():
        result = f" {entry['result']}" if entry.get("result") else ""
        lines.extend([f"<a id=\"{subject}-evidence-{key}\"></a>", f"##### {key}", "", f"**{entry['kind']} — {entry['state']}.**{result} {entry['scope']}", ""])
        if entry.get("superseded_by"):
            lines.append(f"- Superseded by {evidence_links([entry['superseded_by']], subject)}.")
        for link in entry["links"]:
            revision = entry.get("source_revision", data["audit_revision"])
            target = link if link.startswith("https://") else blob(revision, link)
            lines.append(f"- [{link}]({target})")
        if entry.get("receipt"):
            lines.extend([f"- Revision: `{entry['revision']}`; CASA: `{entry['casa_version']}`.", f"- Command: `{entry['command']}`.", f"- Receipt: `{entry['receipt']}`."])
        lines.append("")
    return lines


def signature(parameters: list[dict[str, Any]]) -> str:
    """CASA's parameters as `name=default`, from a recorded signature."""
    return ", ".join(p["name"] + ("" if p["default"] is None else f"={p['default']}") for p in parameters)


def tool_counts(data: dict[str, Any]) -> Counter:
    return Counter(entry["status"] for entry in data["methods"].values())


def render_tool(data: dict[str, Any], signatures: dict[str, Any]) -> list[str]:
    validate_tool(data, signatures)
    tool = data["tool"]
    counts = tool_counts(data)
    lines = [
        f"#### {tool} method implementation", "",
        "Implementation status is **not behavioral verification**. Implemented means the stated scope has a native code path; Partial has known missing modes or combinations, which raise `NotImplementedError` naming the issue shown; Not implemented raises it for every call; Disabled in CASA and Deprecated in CASA do what CASA does; Unassessed has not been reviewed. Evidence states are listed separately below.", "",
        f"Audit baseline: `{data['audit_revision']}`. Status counts: " + ", ".join(f"{key}: {counts[key]}" for key in TOOL_STATUSES) + ".", "",
        "| CASA method | CASA parameters | Implementation | Scope | Evidence |",
        "|---|---|---|---|---|",
    ]
    for name in sorted(signatures["methods"], key=str.lower):
        entry = data["methods"][name]
        status = entry["status"] + (f" (#{entry['issue']})" if entry.get("issue") else "")
        scope = f"{entry['scope']} {behavior_links(entry, tool)}".rstrip()
        lines.append("| " + " | ".join(map(cell, [f"`{name}`", f"`{signature(signatures['methods'][name])}`" if signatures["methods"][name] else "", status, scope, evidence_links(entry["evidence"], tool)])) + " |")
    return [*lines, *render_records(data, tool, "tool")]


def load(root: Path, task: str) -> dict[str, Any]:
    data = json.loads((root / "resources/casa-compat/semantics" / f"{task}.json").read_text())
    validate_source_links(root, data)
    return data


def load_tool(root: Path, tool: str) -> dict[str, Any] | None:
    """A tool's reviewed file, or None when it has not been reviewed yet."""
    path = root / "resources/casa-compat/semantics/tools" / f"{tool}.json"
    if not path.is_file():
        return None
    data = json.loads(path.read_text())
    validate_source_links(root, data)
    return data


def validate_source_links(root: Path, data: dict[str, Any]) -> None:
    versions = {entry.get("source_revision", data["audit_revision"]) for entry in data["evidence"].values()}
    if not all(re.fullmatch(r"[0-9a-f]{40}", revision) for revision in versions):
        raise ValueError("semantic source links need an exact commit")
    objects = sorted({f"{item.get('source_revision', data['audit_revision'])}:{link}"
                      for item in data["evidence"].values() for link in item["links"] if not link.startswith("https://")})
    result = subprocess.run(
        ["git", "-C", str(root), "cat-file", "--batch-check=%(objectname) %(objecttype)"],
        input="".join(f"{obj}\n" for obj in objects),
        text=True, capture_output=True, check=True,
    )
    if any(line.endswith(" missing") for line in result.stdout.splitlines()):
        raise ValueError("semantic evidence links a file absent at its pinned revision")
    records = {entry["record"] for entry in data["decisions"] if entry.get("record")}
    if records:
        text = subprocess.run(["git", "-C", str(root), "show", f"{data['audit_revision']}:{DEVIATIONS}"],
                              capture_output=True, text=True, check=True).stdout
        headings = {line[3:].strip() for line in text.splitlines() if line.startswith("## ")}
        if records - headings:
            raise ValueError(f"decision records name no heading of {DEVIATIONS}: {sorted(records - headings)}")
