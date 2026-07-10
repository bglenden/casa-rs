#!/usr/bin/env python3
"""Generate the canonical Markdown parameter reference from JSON resources."""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "resources" / "parameter-catalog.json"
SURFACES_PATH = REPO_ROOT / "resources" / "parameter-surfaces.json"
OUTPUT_PATH = REPO_ROOT / "docs" / "reference" / "task-parameters.md"


class GenerationError(RuntimeError):
    """Raised when a canonical resource cannot produce a complete reference."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate docs/reference/task-parameters.md from canonical resources."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the committed reference differs from generated output",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GenerationError(f"read {path.relative_to(REPO_ROOT)}: {error}") from error
    if not isinstance(payload, dict):
        raise GenerationError(f"{path.relative_to(REPO_ROOT)} must contain a JSON object")
    return payload


def required(mapping: dict[str, Any], key: str, context: str) -> Any:
    if key not in mapping:
        raise GenerationError(f"{context} is missing {key!r}")
    return mapping[key]


def require_list(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise GenerationError(f"{context} must be an array")
    return value


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GenerationError(f"{context} must be an object")
    return value


def plain_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return html.escape(text, quote=False).replace("|", "&#124;").replace("\n", "<br>")


def code_cell(value: Any) -> str:
    return f"<code>{plain_cell(value)}</code>"


def unwrap_typed_value(value: dict[str, Any], context: str) -> Any:
    kind = required(value, "kind", context)
    if kind == "array":
        items = require_list(required(value, "value", context), f"{context}.value")
        return [
            unwrap_typed_value(require_object(item, f"{context}.value[{index}]"), f"{context}.value[{index}]")
            for index, item in enumerate(items)
        ]
    if kind in {"bool", "float", "integer", "string"}:
        return required(value, "value", context)
    raise GenerationError(f"{context} has unsupported typed-value kind {kind!r}")


def format_typed_value(value: dict[str, Any], context: str) -> str:
    unwrapped = unwrap_typed_value(value, context)
    return json.dumps(unwrapped, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def format_predicate(predicate: dict[str, Any], context: str) -> str:
    kind = required(predicate, "kind", context)
    if kind == "equals":
        parameter = required(predicate, "parameter", context)
        value = require_object(required(predicate, "value", context), f"{context}.value")
        return f"{parameter}={format_typed_value(value, f'{context}.value')}"
    if kind in {"always", "never"}:
        return kind
    if kind == "is_set":
        return f"is_set({required(predicate, 'parameter', context)})"
    if kind == "not":
        nested = require_object(required(predicate, "predicate", context), f"{context}.predicate")
        return f"not ({format_predicate(nested, f'{context}.predicate')})"
    if kind in {"all", "any"}:
        raw_predicates = require_list(
            required(predicate, "predicates", context), f"{context}.predicates"
        )
        rendered = [
            format_predicate(
                require_object(item, f"{context}.predicates[{index}]"),
                f"{context}.predicates[{index}]",
            )
            for index, item in enumerate(raw_predicates)
        ]
        operator = " and " if kind == "all" else " or "
        return f"({operator.join(rendered)})"
    raise GenerationError(f"{context} has unsupported predicate kind {kind!r}")


def format_domain(domain: dict[str, Any], context: str) -> str:
    kind = required(domain, "kind", context)
    if kind in {"bool", "float", "integer", "string"}:
        return str(kind)
    if kind == "choice":
        values = require_list(required(domain, "values", context), f"{context}.values")
        return f"choice ({len(values)} values)"
    if kind == "path":
        resource_kind = required(domain, "resource_kind", context)
        return f"path ({resource_kind})"
    if kind == "quantity":
        dimension = required(domain, "dimension", context)
        canonical_unit = required(domain, "canonical_unit", context)
        return f"quantity<{dimension}> (canonical {canonical_unit})"
    if kind == "array":
        element = require_object(required(domain, "element", context), f"{context}.element")
        return f"array<{format_domain(element, f'{context}.element')}>"
    if kind == "optional":
        value = require_object(required(domain, "value", context), f"{context}.value")
        states = require_list(required(domain, "states", context), f"{context}.states")
        rendered_states = ", ".join(str(state) for state in states)
        return f"optional<{format_domain(value, f'{context}.value')}> (states: {rendered_states})"
    raise GenerationError(f"{context} has unsupported value-domain kind {kind!r}")


def format_type_and_unit(
    concept: dict[str, Any], binding: dict[str, Any], context: str
) -> str:
    domain = require_object(required(concept, "value_domain", context), f"{context}.value_domain")
    rendered = format_domain(domain, f"{context}.value_domain")
    for position, raw_refinement in enumerate(binding.get("refinements", [])):
        refinement = require_object(raw_refinement, f"{context}.refinements[{position}]")
        if refinement.get("kind") == "allowed_values":
            values = require_list(
                required(refinement, "values", f"{context}.refinements[{position}]"),
                f"{context}.refinements[{position}].values",
            )
            rendered = f"choice ({len(values)} values; surface-narrowed)"
    unit_dimension = concept.get("unit_dimension")
    if unit_dimension is not None:
        rendered += f"; unit dimension: {unit_dimension}"
    return code_cell(rendered)


def format_condition(condition: dict[str, Any], context: str) -> str:
    kind = required(condition, "kind", context)
    if kind == "always":
        return "required"
    if kind == "never":
        return "optional"
    return f"required when {format_predicate(condition, context)}"


def format_default_and_requirement(binding: dict[str, Any], context: str) -> str:
    default = require_object(required(binding, "default", context), f"{context}.default")
    requirement = require_object(
        required(binding, "required_when", context), f"{context}.required_when"
    )
    required_text = format_condition(requirement, f"{context}.required_when")
    default_kind = required(default, "kind", f"{context}.default")
    if default_kind == "required":
        default_text = "no default"
    elif default_kind == "literal":
        value = require_object(required(default, "value", f"{context}.default"), f"{context}.default.value")
        default_text = code_cell(format_typed_value(value, f"{context}.default.value"))
    elif default_kind == "conditional":
        cases = require_list(required(default, "cases", f"{context}.default"), f"{context}.default.cases")
        rendered_cases = []
        for index, case_value in enumerate(cases):
            case = require_object(case_value, f"{context}.default.cases[{index}]")
            when = require_object(required(case, "when", f"{context}.default.cases[{index}]"), f"{context}.default.cases[{index}].when")
            value = require_object(required(case, "value", f"{context}.default.cases[{index}]"), f"{context}.default.cases[{index}].value")
            rendered_cases.append(
                f"{format_predicate(when, f'{context}.default.cases[{index}].when')}→{format_typed_value(value, f'{context}.default.cases[{index}].value')}"
            )
        fallback = require_object(required(default, "fallback", f"{context}.default"), f"{context}.default.fallback")
        rendered_cases.append(
            f"otherwise→{format_typed_value(fallback, f'{context}.default.fallback')}"
        )
        default_text = code_cell("; ".join(rendered_cases))
    else:
        raise GenerationError(f"{context}.default has unsupported kind {default_kind!r}")
    return f"{default_text}; {plain_cell(required_text)}"


def format_summary(concept: dict[str, Any], binding: dict[str, Any], context: str) -> str:
    documentation = require_object(
        required(concept, "documentation", context), f"{context}.documentation"
    )
    summary = plain_cell(required(documentation, "summary", f"{context}.documentation"))
    surface_note = binding.get("surface_note")
    if surface_note:
        summary += f"<br><em>Surface:</em> {plain_cell(surface_note)}"
    return summary


def concept_index(catalog: dict[str, Any]) -> dict[tuple[str, int], dict[str, Any]]:
    concepts = require_list(required(catalog, "concepts", "parameter catalog"), "parameter catalog.concepts")
    index: dict[tuple[str, int], dict[str, Any]] = {}
    for position, raw_concept in enumerate(concepts):
        context = f"parameter catalog.concepts[{position}]"
        concept = require_object(raw_concept, context)
        concept_id = required(concept, "id", context)
        revision = required(concept, "semantic_revision", context)
        key = (str(concept_id), int(revision))
        if key in index:
            raise GenerationError(f"duplicate concept {key[0]!r} revision {key[1]}")
        index[key] = concept
    return index


def ordered_bindings(surface: dict[str, Any], context: str) -> list[dict[str, Any]]:
    bindings = require_list(required(surface, "bindings", context), f"{context}.bindings")
    normalized = [
        require_object(binding, f"{context}.bindings[{position}]")
        for position, binding in enumerate(bindings)
    ]
    return sorted(normalized, key=lambda binding: (int(required(binding, "order", context)), str(required(binding, "name", context))))


def surface_anchor(surface_id: str) -> str:
    if not surface_id or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in surface_id):
        raise GenerationError(f"surface id {surface_id!r} cannot be used as a stable Markdown anchor")
    return f"surface-{surface_id}"


def render_reference(catalog: dict[str, Any], surfaces_payload: dict[str, Any]) -> str:
    catalog_schema = required(catalog, "schema_version", "parameter catalog")
    surfaces_schema = required(surfaces_payload, "schema_version", "parameter surfaces")
    concepts = concept_index(catalog)
    raw_surfaces = require_list(
        required(surfaces_payload, "surfaces", "parameter surfaces"),
        "parameter surfaces.surfaces",
    )
    surfaces = [
        require_object(surface, f"parameter surfaces.surfaces[{position}]")
        for position, surface in enumerate(raw_surfaces)
    ]

    seen_surface_ids: set[str] = set()
    task_count = 0
    session_count = 0
    total_bindings = 0
    for position, surface in enumerate(surfaces):
        context = f"parameter surfaces.surfaces[{position}]"
        surface_id = str(required(surface, "id", context))
        if surface_id in seen_surface_ids:
            raise GenerationError(f"duplicate surface id {surface_id!r}")
        seen_surface_ids.add(surface_id)
        surface_anchor(surface_id)
        kind = required(surface, "kind", context)
        if kind == "task":
            task_count += 1
        elif kind == "session":
            session_count += 1
        else:
            raise GenerationError(f"{context} has unsupported surface kind {kind!r}")
        total_bindings += len(ordered_bindings(surface, context))

    lines = [
        "# Task and Session Parameter Reference",
        "",
        "Truth class: generated reference",
        "Generated by: `scripts/generate-parameter-reference.py`",
        "Sources: `resources/parameter-catalog.json`, `resources/parameter-surfaces.json`",
        "",
        "> Do not edit this file by hand. Regenerate it after changing either canonical resource.",
        "",
        "## Overview",
        "",
        f"- Parameter catalog schema version: `{catalog_schema}`",
        f"- Parameter surface schema version: `{surfaces_schema}`",
        f"- Concepts: {len(concepts)}",
        f"- Surfaces: {len(surfaces)} ({task_count} task, {session_count} session)",
        f"- Surface bindings: {total_bindings}",
        "",
        "| Surface | Kind | Contract | Provider family | Parameters | Summary |",
        "|---|---|---:|---|---:|---|",
    ]

    for position, surface in enumerate(surfaces):
        context = f"parameter surfaces.surfaces[{position}]"
        surface_id = str(required(surface, "id", context))
        display_name = required(surface, "display_name", context)
        bindings = ordered_bindings(surface, context)
        lines.append(
            "| "
            + " | ".join(
                [
                    f"[{plain_cell(display_name)}](#{surface_anchor(surface_id)})<br>{code_cell(surface_id)}",
                    plain_cell(required(surface, "kind", context)),
                    plain_cell(required(surface, "contract_version", context)),
                    code_cell(required(surface, "provider_family", context)),
                    str(len(bindings)),
                    plain_cell(required(surface, "summary", context)),
                ]
            )
            + " |"
        )

    for position, surface in enumerate(surfaces):
        context = f"parameter surfaces.surfaces[{position}]"
        surface_id = str(required(surface, "id", context))
        display_name = required(surface, "display_name", context)
        bindings = ordered_bindings(surface, context)
        lines.extend(
            [
                "",
                f'<a id="{surface_anchor(surface_id)}"></a>',
                "",
                f"## {plain_cell(display_name)} ({code_cell(surface_id)})",
                "",
                f"- Kind: `{required(surface, 'kind', context)}`",
                f"- Contract version: `{required(surface, 'contract_version', context)}`",
                f"- Category: {plain_cell(required(surface, 'category', context))}",
                f"- Provider family: `{required(surface, 'provider_family', context)}`",
                f"- Summary: {plain_cell(required(surface, 'summary', context))}",
                "",
                "| Parameter | Concept ID / revision | Type / unit | Default / required | Group | Summary |",
                "|---|---|---|---|---|---|",
            ]
        )

        for binding_position, binding in enumerate(bindings):
            binding_context = f"{context}.bindings[{binding_position}]"
            concept_ref = require_object(
                required(binding, "concept", binding_context), f"{binding_context}.concept"
            )
            concept_id = str(required(concept_ref, "id", f"{binding_context}.concept"))
            revision = int(
                required(concept_ref, "semantic_revision", f"{binding_context}.concept")
            )
            concept = concepts.get((concept_id, revision))
            if concept is None:
                raise GenerationError(
                    f"{binding_context} references missing concept {concept_id!r} revision {revision}"
                )
            presentation = require_object(
                required(
                    require_object(required(binding, "projections", binding_context), f"{binding_context}.projections"),
                    "presentation",
                    f"{binding_context}.projections",
                ),
                f"{binding_context}.projections.presentation",
            )
            lines.append(
                "| "
                + " | ".join(
                    [
                        code_cell(required(binding, "name", binding_context)),
                        code_cell(f"{concept_id}@r{revision}"),
                        format_type_and_unit(
                            concept, binding, f"concept {concept_id}@r{revision}"
                        ),
                        format_default_and_requirement(binding, binding_context),
                        plain_cell(required(presentation, "group", f"{binding_context}.projections.presentation")),
                        format_summary(concept, binding, f"concept {concept_id}@r{revision}"),
                    ]
                )
                + " |"
            )

    lines.extend(["", "_End of generated reference._", ""])
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    try:
        generated = render_reference(load_json(CATALOG_PATH), load_json(SURFACES_PATH))
    except GenerationError as error:
        print(f"generate-parameter-reference: {error}", file=sys.stderr)
        return 2

    if args.check:
        try:
            current = OUTPUT_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            print(
                f"generate-parameter-reference: missing {OUTPUT_PATH.relative_to(REPO_ROOT)}; "
                "run scripts/generate-parameter-reference.py",
                file=sys.stderr,
            )
            return 1
        except OSError as error:
            print(
                f"generate-parameter-reference: read {OUTPUT_PATH.relative_to(REPO_ROOT)}: {error}",
                file=sys.stderr,
            )
            return 2
        if current != generated:
            print(
                f"generate-parameter-reference: {OUTPUT_PATH.relative_to(REPO_ROOT)} is stale; "
                "run scripts/generate-parameter-reference.py",
                file=sys.stderr,
            )
            return 1
        return 0

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists() and OUTPUT_PATH.read_text(encoding="utf-8") == generated:
        print(f"parameter reference is current: {OUTPUT_PATH.relative_to(REPO_ROOT)}")
        return 0
    OUTPUT_PATH.write_text(generated, encoding="utf-8", newline="\n")
    print(f"generated {OUTPUT_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
