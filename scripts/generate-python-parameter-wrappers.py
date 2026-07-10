#!/usr/bin/env python3
"""Generate CASA-named Python wrappers and stubs from surface definitions."""

from __future__ import annotations

import argparse
import ast
import json
import keyword
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SURFACES_PATH = REPO_ROOT / "resources" / "parameter-surfaces.json"
CATALOG_PATH = REPO_ROOT / "resources" / "parameter-catalog.json"
TASK_OUTPUT = REPO_ROOT / "crates/casars-python/python/casars/tasks/catalog.py"
TASK_STUB = REPO_ROOT / "crates/casars-python/python/casars/tasks/catalog.pyi"
SESSION_OUTPUT = REPO_ROOT / "crates/casars-python/python/casars/_session_catalog.py"
SESSION_STUB = REPO_ROOT / "crates/casars-python/python/casars/_session_catalog.pyi"


class GenerationError(RuntimeError):
    """Raised when a surface cannot be projected to a Python signature."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail when committed generated files differ from canonical resources",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GenerationError(f"read {path.relative_to(REPO_ROOT)}: {error}") from error
    if not isinstance(payload, dict):
        raise GenerationError(f"{path.relative_to(REPO_ROOT)} must contain an object")
    return payload


def load_surfaces() -> list[dict[str, Any]]:
    payload = load_json(SURFACES_PATH)
    surfaces = payload.get("surfaces")
    if not isinstance(surfaces, list):
        raise GenerationError("parameter-surfaces.json must contain a surfaces array")
    for surface in surfaces:
        if not isinstance(surface, dict):
            raise GenerationError("every surface must be an object")
        validate_identifier(surface.get("id"), "surface")
        bindings = surface.get("bindings")
        if not isinstance(bindings, list):
            raise GenerationError(f"surface {surface['id']!r} has no bindings array")
        seen: set[str] = set()
        for binding in bindings:
            if not isinstance(binding, dict):
                raise GenerationError(f"surface {surface['id']!r} has a non-object binding")
            name = validate_identifier(binding.get("name"), f"surface {surface['id']!r} parameter")
            if name in seen:
                raise GenerationError(f"surface {surface['id']!r} repeats parameter {name!r}")
            seen.add(name)
            projection = binding.get("projections", {}).get("python", {})
            python_name = validate_identifier(
                projection.get("name"),
                f"surface {surface['id']!r} Python parameter",
            )
            if python_name != name:
                raise GenerationError(
                    f"surface {surface['id']!r} Python name {python_name!r} must retain "
                    f"canonical CASA spelling {name!r}"
                )
    return surfaces


def load_concepts() -> dict[tuple[str, int], dict[str, Any]]:
    payload = load_json(CATALOG_PATH)
    concepts = payload.get("concepts")
    if not isinstance(concepts, list):
        raise GenerationError("parameter-catalog.json must contain a concepts array")
    indexed: dict[tuple[str, int], dict[str, Any]] = {}
    for concept in concepts:
        if not isinstance(concept, dict):
            raise GenerationError("every parameter concept must be an object")
        concept_id = concept.get("id")
        revision = concept.get("semantic_revision")
        if not isinstance(concept_id, str) or not isinstance(revision, int):
            raise GenerationError("every parameter concept needs an id and semantic_revision")
        key = (concept_id, revision)
        if key in indexed:
            raise GenerationError(f"parameter catalog repeats concept {concept_id!r}@{revision}")
        indexed[key] = concept
    return indexed


def validate_identifier(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.isidentifier() or keyword.iskeyword(value):
        raise GenerationError(f"{context} name {value!r} is not a Python identifier")
    return value


def ordered_bindings(surface: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(
        surface["bindings"],
        key=lambda binding: (int(binding.get("order", 0)), str(binding["name"])),
    )


def concept_for_binding(
    binding: dict[str, Any], concepts: dict[tuple[str, int], dict[str, Any]]
) -> dict[str, Any]:
    reference = binding.get("concept", {})
    key = (reference.get("id"), reference.get("semantic_revision"))
    try:
        return concepts[key]
    except KeyError as error:
        raise GenerationError(
            f"binding {binding.get('name')!r} references missing concept "
            f"{key[0]!r}@{key[1]!r}"
        ) from error


def literal_annotation(values: list[str]) -> str:
    if not values:
        raise GenerationError("choice and optional-state domains cannot be empty")
    return f"Literal[{', '.join(repr(value) for value in values)}]"


def domain_annotation(domain: dict[str, Any]) -> str:
    kind = domain.get("kind")
    if kind == "bool":
        return "bool"
    if kind == "integer":
        return "int"
    if kind == "float":
        return "float"
    if kind == "string":
        return "str"
    if kind == "path":
        return "StrPath"
    if kind == "choice":
        values = domain.get("values")
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise GenerationError("choice domain values must be strings")
        return literal_annotation(values)
    if kind == "quantity":
        # Quantities deliberately remain strings so units and explicit states
        # cannot be confused with dimensionless Python numbers.
        return "str"
    if kind == "array":
        element = domain.get("element")
        if not isinstance(element, dict):
            raise GenerationError("array domain must contain an element domain")
        element_hint = domain_annotation(element)
        containers = f"list[{element_hint}] | tuple[{element_hint}, ...]"
        return (
            f"{element_hint} | {containers}"
            if domain.get("allow_scalar") is True
            else containers
        )
    if kind == "table":
        return "Mapping[str, ParameterData]"
    if kind == "optional":
        inner = domain.get("value")
        states = domain.get("states")
        if not isinstance(inner, dict):
            raise GenerationError("optional domain must contain a value domain")
        if not isinstance(states, list) or not all(isinstance(value, str) for value in states):
            raise GenerationError("optional domain states must be strings")
        inner_hint = domain_annotation(inner)
        if inner_hint == "str":
            return "str"
        return f"{inner_hint} | {literal_annotation(states)}"
    raise GenerationError(f"unsupported parameter domain kind {kind!r}")


def binding_annotation(
    binding: dict[str, Any], concepts: dict[tuple[str, int], dict[str, Any]]
) -> str:
    projection = binding["projections"]["python"]
    explicit = projection.get("type_hint")
    if explicit is not None:
        if not isinstance(explicit, str) or not explicit.strip():
            raise GenerationError(
                f"Python type hint for parameter {binding['name']!r} must be non-empty text"
            )
        annotation = explicit.strip()
    else:
        concept = concept_for_binding(binding, concepts)
        domain = concept.get("value_domain")
        if not isinstance(domain, dict):
            raise GenerationError(f"concept {concept['id']!r} has no value_domain")
        allowed = [
            refinement.get("values")
            for refinement in binding.get("refinements", [])
            if refinement.get("kind") == "allowed_values"
        ]
        if allowed:
            values = allowed[-1]
            if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values
            ):
                raise GenerationError(
                    f"allowed-values refinement for {binding['name']!r} must contain strings"
                )
            if domain.get("kind") == "choice":
                domain = {**domain, "values": values}
            elif (
                domain.get("kind") == "optional"
                and isinstance(domain.get("value"), dict)
                and domain["value"].get("kind") == "choice"
            ):
                domain = {
                    **domain,
                    "value": {**domain["value"], "values": values},
                }
        annotation = domain_annotation(domain)
    try:
        ast.parse(annotation, mode="eval")
    except SyntaxError as error:
        raise GenerationError(
            f"invalid Python type hint {annotation!r} for parameter {binding['name']!r}"
        ) from error
    return annotation


def parameters_block(
    bindings: list[dict[str, Any]],
    concepts: dict[tuple[str, int], dict[str, Any]],
    *,
    stub: bool,
) -> list[str]:
    default = "..." if stub else "_UNSET"
    return [
        f"    {binding['name']}: {binding_annotation(binding, concepts)}"
        f"{' | object' if not stub else ''} = {default},"
        for binding in bindings
    ]


def render_task_module(
    surfaces: list[dict[str, Any]],
    concepts: dict[tuple[str, int], dict[str, Any]],
    *,
    stub: bool,
) -> str:
    tasks = [surface for surface in surfaces if surface.get("kind") == "task"]
    lines = [
        '"""Generated CASA-named wrappers for every catalog task.',
        "",
        "Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from collections.abc import Mapping",
        "from os import PathLike",
        "from typing import Literal, TypeAlias",
        "",
        "from ..parameters import ParameterData, TaskParameters",
        "from ._runner import TaskCompletion, run as _run",
        "",
        "StrPath: TypeAlias = str | PathLike[str]",
    ]
    if not stub:
        lines.extend(
            [
                "_UNSET = object()",
                "",
                "",
                "def _explicit(values: Mapping[str, object], names: tuple[str, ...]) -> dict[str, ParameterData]:",
                "    return {name: values[name] for name in names if values[name] is not _UNSET}  # type: ignore[misc]",
            ]
        )
    lines.extend(["", ""])
    for surface in tasks:
        surface_id = str(surface["id"])
        bindings = ordered_bindings(surface)
        names = [str(binding["name"]) for binding in bindings]
        lines.append(f"def {surface_id}(")
        lines.append("    *,")
        lines.extend(parameters_block(bindings, concepts, stub=stub))
        lines.extend(
            [
                "    parameters: TaskParameters | None = None," if not stub else "    parameters: TaskParameters | None = ...," ,
                "    profile: StrPath | None = None," if not stub else "    profile: StrPath | None = ... ,",
                '    base_source: Literal["defaults", "last", "last_successful"] = "defaults",' if not stub else '    base_source: Literal["defaults", "last", "last_successful"] = ... ,',
                "    workspace: StrPath | None = None," if not stub else "    workspace: StrPath | None = ... ,",
                "    save_last: bool = True," if not stub else "    save_last: bool = ... ,",
                "    binary: StrPath | None = None," if not stub else "    binary: StrPath | None = ... ,",
                "    check: bool = True," if not stub else "    check: bool = ... ,",
                "    timeout: float | None = None," if not stub else "    timeout: float | None = ... ,",
                "    env: Mapping[str, str] | None = None," if not stub else "    env: Mapping[str, str] | None = ... ,",
                "    confirm_overwrite: bool = False," if not stub else "    confirm_overwrite: bool = ... ,",
                "    confirm_mutation: bool = False," if not stub else "    confirm_mutation: bool = ... ,",
                ") -> TaskCompletion:",
            ]
        )
        if stub:
            lines.extend(["    ...", ""])
            continue
        summary = str(surface.get("summary", "Run the catalog task.")).replace('"""', "")
        tuple_text = repr(tuple(names))
        lines.extend(
            [
                f'    """{summary}"""',
                f"    overrides = _explicit(locals(), {tuple_text})",
                "    return _run(",
                f'        "{surface_id}",',
                "        parameters=parameters,",
                "        profile=profile,",
                "        base_source=base_source,",
                "        overrides=overrides,",
                "        workspace=workspace,",
                "        save_last=save_last,",
                "        binary=binary,",
                "        check=check,",
                "        timeout=timeout,",
                "        env=env,",
                "        confirm_overwrite=confirm_overwrite,",
                "        confirm_mutation=confirm_mutation,",
                "    )",
                "",
            ]
        )
    names = [str(surface["id"]) for surface in tasks]
    lines.extend(
        [
            f"TASK_SURFACES = {tuple(names)!r}",
            "",
            f"__all__ = {['TASK_SURFACES', *names]!r}",
            "",
        ]
    )
    return "\n".join(lines)


def render_session_module(
    surfaces: list[dict[str, Any]],
    concepts: dict[tuple[str, int], dict[str, Any]],
    *,
    stub: bool,
) -> str:
    sessions = [surface for surface in surfaces if surface.get("kind") == "session"]
    lines = [
        '"""Generated CASA-named wrappers for every catalog session.',
        "",
        "Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from collections.abc import Mapping",
        "from os import PathLike",
        "from typing import Any, Literal, TypeAlias",
        "",
        "from .parameters import ParameterData, SessionParameters",
        *( ["from .sessions import JsonlSession"] if stub else [] ),
        "",
        "StrPath: TypeAlias = str | PathLike[str]",
    ]
    if not stub:
        lines.extend(
            [
                "_UNSET = object()",
                "",
                "",
                "def _explicit(values: Mapping[str, object], names: tuple[str, ...]) -> dict[str, ParameterData]:",
                "    return {name: values[name] for name in names if values[name] is not _UNSET}  # type: ignore[misc]",
            ]
        )
    lines.extend(["", ""])
    for surface in sessions:
        surface_id = str(surface["id"])
        bindings = ordered_bindings(surface)
        names = [str(binding["name"]) for binding in bindings]
        lines.append(f"def {surface_id}(")
        lines.append("    *,")
        lines.extend(parameters_block(bindings, concepts, stub=stub))
        lines.extend(
            [
                "    parameters: SessionParameters | None = None," if not stub else "    parameters: SessionParameters | None = ... ,",
                "    profile: StrPath | None = None," if not stub else "    profile: StrPath | None = ... ,",
                '    source: Literal["defaults", "last"] = "defaults",' if not stub else '    source: Literal["defaults", "last"] = ... ,',
                "    workspace: StrPath | None = None," if not stub else "    workspace: StrPath | None = ... ,",
                "    save_last: bool = True," if not stub else "    save_last: bool = ... ,",
                "    **options: Any,",
                ") -> JsonlSession:" if stub else ") -> Any:",
            ]
        )
        if stub:
            lines.extend(["    ...", ""])
            continue
        summary = str(surface.get("summary", "Open the catalog session.")).replace('"""', "")
        tuple_text = repr(tuple(names))
        lines.extend(
            [
                f'    """{summary}"""',
                "    from .sessions import open as open_session",
                "",
                f"    overrides = _explicit(locals(), {tuple_text})",
                "    return open_session(",
                f'        "{surface_id}",',
                "        parameters=parameters,",
                "        profile=profile,",
                "        start=source,",
                "        workspace=workspace,",
                "        save_last=save_last,",
                "        overrides=overrides,",
                "        **options,",
                "    )",
                "",
            ]
        )
    names = [str(surface["id"]) for surface in sessions]
    lines.extend(
        [
            f"SESSION_SURFACES = {tuple(names)!r}",
            "",
            f"__all__ = {['SESSION_SURFACES', *names]!r}",
            "",
        ]
    )
    return "\n".join(lines)


def update(path: Path, content: str, *, check: bool) -> None:
    if check:
        try:
            current = path.read_text(encoding="utf-8")
        except OSError as error:
            raise GenerationError(f"read generated file {path.relative_to(REPO_ROOT)}: {error}") from error
        if current != content:
            raise GenerationError(
                f"{path.relative_to(REPO_ROOT)} is stale; run scripts/generate-python-parameter-wrappers.py"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"generated {path.relative_to(REPO_ROOT)}")


def main() -> int:
    args = parse_args()
    try:
        surfaces = load_surfaces()
        concepts = load_concepts()
        outputs = {
            TASK_OUTPUT: render_task_module(surfaces, concepts, stub=False),
            TASK_STUB: render_task_module(surfaces, concepts, stub=True),
            SESSION_OUTPUT: render_session_module(surfaces, concepts, stub=False),
            SESSION_STUB: render_session_module(surfaces, concepts, stub=True),
        }
        for path, content in outputs.items():
            update(path, content, check=args.check)
    except GenerationError as error:
        print(f"python-parameter-wrappers: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
