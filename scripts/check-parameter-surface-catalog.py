#!/usr/bin/env python3
"""Validate parameter-surface inventory and provider-family routing."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "resources" / "task-catalog.json"
CONTRACT_ROOT = REPO_ROOT / "crates" / "casa-provider-contracts" / "resources"
PARAMETER_SURFACES_PATH = CONTRACT_ROOT / "parameter-surfaces.json"
PARAMETER_CATALOG_PATH = CONTRACT_ROOT / "parameter-catalog.json"
EXPECTED_SCHEMA_VERSION = 1

CATALOG_FIELDS = {
    "id",
    "provider_family",
    "category",
    "display_name",
    "binary_name",
    "cargo_package",
    "override_env",
    "shell_kind",
    "interaction",
    "browser_kind",
    "dataset_kinds",
    "schema_source",
    "show_in_tui",
    "show_in_swift",
    "include_in_suite",
}

EXPECTED_FAMILY_MEMBERS = {
    "calibration": {
        "applycal",
        "bandpass",
        "calibrate",
        "fluxscale",
        "gaincal",
        "gencal",
        "uvcontsub",
    },
    "casa_task_adapter": {
        "clearcal",
        "concat",
        "delmod",
        "ft",
        "hanningsmooth",
        "imcollapse",
        "imcontsub",
        "imfit",
        "plotcal",
        "simalma",
        "simanalyze",
        "statwt",
        "widebandpbcor",
    },
    "flagdata": {"flagdata"},
    "flagmanager": {"flagmanager"},
    "image_analysis": {
        "exportfits",
        "feather",
        "imhead",
        "immath",
        "immoments",
        "impbcor",
        "importfits",
        "impv",
        "imregrid",
        "imstat",
        "imsubimage",
    },
    "image_browser": {"imexplore"},
    "imager": {"imager"},
    "importvla": {"importvla"},
    "launcher": {"casars"},
    "msexplore": {"msexplore", "plotms"},
    "mstransform": {"mstransform", "split"},
    "simobserve": {"simobserve"},
    "table_browser": {"tablebrowser"},
}

BROWSER_SESSION_IDS = {"imexplore", "tablebrowser"}
LAUNCHER_IDS = {"casars"}
OBJECT_API_IDS = {"image", "table"}
EXPECTED_IDS = set().union(*EXPECTED_FAMILY_MEMBERS.values())
ONE_SHOT_IDS = EXPECTED_IDS - BROWSER_SESSION_IDS - LAUNCHER_IDS
EXPECTED_COUNTS = {"one_shot": 40, "browser_session": 2, "launcher": 1}
ALLOWED_SCHEMA_SOURCES = {"binary", "embedded_or_binary", "none"}
ALLOWED_SHELL_KINDS = {"browser", "inspect", "launcher", "workflow"}


class DuplicateKeyError(ValueError):
    """Raised when a JSON object repeats a key."""


def fail(message: str) -> None:
    print(f"parameter-surface-catalog: {message}", file=sys.stderr)
    raise SystemExit(1)


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKeyError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def load_json(path: Path, *, required: bool) -> Any | None:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle, object_pairs_hook=reject_duplicate_keys)
    except FileNotFoundError:
        if required:
            fail(f"missing {path.relative_to(REPO_ROOT)}")
        return None
    except (json.JSONDecodeError, DuplicateKeyError) as error:
        fail(f"{path.relative_to(REPO_ROOT)} is invalid JSON: {error}")
    except OSError as error:
        fail(f"cannot read {path.relative_to(REPO_ROOT)}: {error}")


def require_exact_fields(value: dict[str, Any], expected: set[str], path: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing fields {missing}")
        if extra:
            details.append(f"unknown fields {extra}")
        fail(f"{path} has invalid shape: {', '.join(details)}")


def require_nonempty_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{path} must be a non-empty string")
    return value


def expected_family_by_id() -> dict[str, str]:
    result: dict[str, str] = {}
    for family, ids in EXPECTED_FAMILY_MEMBERS.items():
        for surface_id in ids:
            if surface_id in result:
                fail(
                    f"internal checker error: surface {surface_id!r} appears in multiple "
                    "expected provider families"
                )
            result[surface_id] = family
    return result


def expected_interaction(surface_id: str) -> str:
    if surface_id in BROWSER_SESSION_IDS:
        return "browser_session"
    if surface_id in LAUNCHER_IDS:
        return "launcher"
    return "one_shot"


def validate_catalog(catalog: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(catalog, dict):
        fail("resources/task-catalog.json must contain a JSON object")
    require_exact_fields(catalog, {"schema_version", "tasks"}, "task catalog")
    if type(catalog["schema_version"]) is not int:
        fail("task catalog schema_version must be an integer")
    if catalog["schema_version"] != EXPECTED_SCHEMA_VERSION:
        fail(
            "task catalog schema_version must be "
            f"{EXPECTED_SCHEMA_VERSION}, got {catalog['schema_version']!r}"
        )
    if not isinstance(catalog["tasks"], list):
        fail("task catalog tasks must be an array")

    expected_families = expected_family_by_id()
    by_id: dict[str, dict[str, Any]] = {}
    interactions: Counter[str] = Counter()

    for index, row in enumerate(catalog["tasks"]):
        path = f"task catalog tasks[{index}]"
        if not isinstance(row, dict):
            fail(f"{path} must be an object")
        require_exact_fields(row, CATALOG_FIELDS, path)

        surface_id = require_nonempty_string(row["id"], f"{path}.id")
        if surface_id in by_id:
            fail(f"task catalog contains duplicate id {surface_id!r}")
        by_id[surface_id] = row

        for field in {
            "provider_family",
            "category",
            "display_name",
            "binary_name",
            "cargo_package",
            "override_env",
            "shell_kind",
            "interaction",
            "schema_source",
        }:
            require_nonempty_string(row[field], f"{path}.{field}")
        for field in {"show_in_tui", "show_in_swift", "include_in_suite"}:
            if type(row[field]) is not bool:
                fail(f"{path}.{field} must be a boolean")

        if row["shell_kind"] not in ALLOWED_SHELL_KINDS:
            fail(
                f"{path}.shell_kind has unsupported value {row['shell_kind']!r}; "
                f"expected one of {sorted(ALLOWED_SHELL_KINDS)}"
            )
        if row["schema_source"] not in ALLOWED_SCHEMA_SOURCES:
            fail(
                f"{path}.schema_source has unsupported value {row['schema_source']!r}; "
                f"expected one of {sorted(ALLOWED_SCHEMA_SOURCES)}"
            )

        browser_kind = row["browser_kind"]
        if browser_kind is not None and (
            not isinstance(browser_kind, str) or browser_kind not in {"image", "table"}
        ):
            fail(f"{path}.browser_kind must be null, 'image', or 'table'")
        dataset_kinds = row["dataset_kinds"]
        if not isinstance(dataset_kinds, list):
            fail(f"{path}.dataset_kinds must be an array")
        if any(not isinstance(kind, str) or not kind.strip() for kind in dataset_kinds):
            fail(f"{path}.dataset_kinds entries must be non-empty strings")
        if len(dataset_kinds) != len(set(dataset_kinds)):
            fail(f"{path}.dataset_kinds must not contain duplicates")

        interaction = row["interaction"]
        if interaction not in EXPECTED_COUNTS:
            fail(
                f"{path}.interaction has unsupported value {interaction!r}; "
                f"expected one of {sorted(EXPECTED_COUNTS)}"
            )
        interactions[interaction] += 1

        expected = expected_interaction(surface_id)
        if interaction != expected:
            fail(
                f"surface {surface_id!r} must use interaction {expected!r}, "
                f"got {interaction!r}"
            )

        expected_family = expected_families.get(surface_id)
        if expected_family is not None and row["provider_family"] != expected_family:
            fail(
                f"surface {surface_id!r} must use provider_family {expected_family!r}, "
                f"got {row['provider_family']!r}"
            )

        if interaction == "browser_session":
            if row["browser_kind"] is None:
                fail(f"configurable browser surface {surface_id!r} requires browser_kind")
            if row["shell_kind"] != "browser":
                fail(f"browser surface {surface_id!r} must use shell_kind 'browser'")
        elif row["browser_kind"] is not None:
            fail(f"non-browser surface {surface_id!r} must use browser_kind null")

        if interaction == "launcher":
            if surface_id != "casars":
                fail(f"only 'casars' may use interaction 'launcher', got {surface_id!r}")
            if row["provider_family"] != "launcher":
                fail("launcher surface 'casars' must use provider_family 'launcher'")
            if row["schema_source"] != "none":
                fail("launcher surface 'casars' must use schema_source 'none'")
            if row["show_in_tui"] or row["show_in_swift"]:
                fail("launcher surface 'casars' must be excluded from configurable UI surfaces")
        else:
            if row["provider_family"] == "launcher":
                fail(f"configurable surface {surface_id!r} cannot use provider_family 'launcher'")
            if row["schema_source"] == "none":
                fail(f"configurable surface {surface_id!r} requires a schema source")

    actual_ids = set(by_id)
    missing_ids = sorted(EXPECTED_IDS - actual_ids)
    unexpected_ids = sorted(actual_ids - EXPECTED_IDS)
    if missing_ids or unexpected_ids:
        details = []
        if missing_ids:
            details.append(f"missing ids {missing_ids}")
        if unexpected_ids:
            details.append(f"unexpected ids {unexpected_ids}")
        fail(f"task catalog does not match the current surface inventory: {', '.join(details)}")

    if interactions != Counter(EXPECTED_COUNTS):
        fail(
            "task catalog interaction counts must be "
            f"{EXPECTED_COUNTS}, got {dict(sorted(interactions.items()))}"
        )
    if len(ONE_SHOT_IDS) != EXPECTED_COUNTS["one_shot"]:
        fail("internal checker error: expected one-shot id set does not contain 40 entries")

    configurable_families = {
        row["provider_family"]
        for row in by_id.values()
        if row["interaction"] != "launcher"
    }
    if len(configurable_families) != 12:
        fail(
            "task catalog must contain exactly 12 configurable provider families, "
            f"got {sorted(configurable_families)}"
        )
    return by_id


def validate_parameter_surfaces(
    index: Any,
    catalog_by_id: dict[str, dict[str, Any]],
    concepts_by_id: dict[str, dict[str, Any]],
) -> None:
    if not isinstance(index, dict):
        fail(f"{PARAMETER_SURFACES_PATH} must contain a JSON object")
    require_exact_fields(index, {"schema_version", "surfaces"}, "parameter surface index")
    if type(index["schema_version"]) is not int:
        fail("parameter surface index schema_version must be an integer")
    if index["schema_version"] != EXPECTED_SCHEMA_VERSION:
        fail(
            "parameter surface index schema_version must be "
            f"{EXPECTED_SCHEMA_VERSION}, got {index['schema_version']!r}"
        )
    if not isinstance(index["surfaces"], list):
        fail("parameter surface index surfaces must be an array")

    indexed_ids: set[str] = set()
    for position, surface in enumerate(index["surfaces"]):
        path = f"parameter surface index surfaces[{position}]"
        if not isinstance(surface, dict):
            fail(f"{path} must be an object")
        if "id" not in surface or "provider_family" not in surface:
            fail(f"{path} requires id and provider_family")
        surface_id = require_nonempty_string(surface["id"], f"{path}.id")
        provider_family = require_nonempty_string(
            surface["provider_family"], f"{path}.provider_family"
        )
        if surface_id in indexed_ids:
            fail(f"parameter surface index contains duplicate id {surface_id!r}")
        indexed_ids.add(surface_id)
        catalog_row = catalog_by_id.get(surface_id)
        if catalog_row is None:
            fail(f"parameter surface index contains unknown catalog id {surface_id!r}")
        if catalog_row["interaction"] == "launcher":
            fail(f"launcher {surface_id!r} must not appear in parameter surface index")
        if provider_family != catalog_row["provider_family"]:
            fail(
                f"parameter surface {surface_id!r} provider_family must match catalog value "
                f"{catalog_row['provider_family']!r}, got {provider_family!r}"
            )
        bindings = surface.get("bindings")
        if not isinstance(bindings, list):
            fail(f"{path}.bindings must be an array")
        for binding_index, binding in enumerate(bindings):
            binding_path = f"{path}.bindings[{binding_index}]"
            if not isinstance(binding, dict):
                fail(f"{binding_path} must be an object")
            concept_ref = binding.get("concept")
            if not isinstance(concept_ref, dict):
                fail(f"{binding_path}.concept must be an object")
            concept_id = require_nonempty_string(
                concept_ref.get("id"), f"{binding_path}.concept.id"
            )
            concept = concepts_by_id.get(concept_id)
            if concept is None:
                fail(f"{binding_path} references unknown concept {concept_id!r}")
            context_role = binding.get("context_role")
            semantic_role = concept.get("semantic_role")
            if context_role == "input_dataset" and semantic_role != "input_data":
                fail(
                    f"{binding_path} uses input_dataset for {concept_id!r}, whose "
                    f"semantic_role is {semantic_role!r}"
                )
            if context_role == "output_product" and semantic_role != "output_data":
                fail(
                    f"{binding_path} uses output_product for {concept_id!r}, whose "
                    f"semantic_role is {semantic_role!r}"
                )

    configurable_ids = {
        surface_id
        for surface_id, row in catalog_by_id.items()
        if row["interaction"] != "launcher"
    }
    missing = sorted(configurable_ids - indexed_ids)
    unexpected = sorted(indexed_ids - configurable_ids)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing configurable surfaces {missing}")
        if unexpected:
            details.append(f"unexpected surfaces {unexpected}")
        fail(f"parameter surface index does not exactly cover the catalog: {', '.join(details)}")
    object_collisions = sorted(indexed_ids & OBJECT_API_IDS)
    if object_collisions:
        fail(
            "object APIs are outside parameter profiles and must not be catalog surfaces: "
            f"{object_collisions}"
        )


def validate_parameter_catalog(catalog: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(catalog, dict):
        fail(f"{PARAMETER_CATALOG_PATH} must contain a JSON object")
    require_exact_fields(catalog, {"schema_version", "concepts"}, "parameter catalog")
    if catalog["schema_version"] != EXPECTED_SCHEMA_VERSION:
        fail(
            "parameter catalog schema_version must be "
            f"{EXPECTED_SCHEMA_VERSION}, got {catalog['schema_version']!r}"
        )
    if not isinstance(catalog["concepts"], list):
        fail("parameter catalog concepts must be an array")
    by_id: dict[str, dict[str, Any]] = {}
    for index, concept in enumerate(catalog["concepts"]):
        path = f"parameter catalog concepts[{index}]"
        if not isinstance(concept, dict):
            fail(f"{path} must be an object")
        concept_id = require_nonempty_string(concept.get("id"), f"{path}.id")
        if concept_id in by_id:
            fail(f"parameter catalog contains duplicate concept id {concept_id!r}")
        by_id[concept_id] = concept
    return by_id


def main() -> None:
    catalog = load_json(CATALOG_PATH, required=True)
    catalog_by_id = validate_catalog(catalog)
    parameter_catalog = load_json(PARAMETER_CATALOG_PATH, required=True)
    concepts_by_id = validate_parameter_catalog(parameter_catalog)
    parameter_surfaces = load_json(PARAMETER_SURFACES_PATH, required=False)
    if parameter_surfaces is not None:
        validate_parameter_surfaces(parameter_surfaces, catalog_by_id, concepts_by_id)


if __name__ == "__main__":
    main()
