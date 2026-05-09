#!/usr/bin/env python3
"""Validate tutorial-pack contract resources without external dependencies."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "resources" / "tutorial-pack.schema.json"
REVIEW_SCHEMA_PATH = REPO_ROOT / "resources" / "tutorial-pack-review.schema.json"
TEMPLATE_PATH = (
    REPO_ROOT
    / "resources"
    / "tutorial-packs"
    / "alma-first-look-image-analysis.template.json"
)
EXPECTED_SCHEMA_VERSION = "tutorial-pack.v0"
EXPECTED_SURFACES = {"cli", "python", "tui", "gui"}
EXPECTED_TASKS = {"imhead", "imstat", "immoments", "exportfits"}
FORBIDDEN_PROVIDER_TEXT = ("casars-casa-task", "casa-python")
LOCAL_PATH_LEAK_RE = re.compile(r"(^|[\\s\"'])/(Users|private|tmp|var/folders)/|file://|~")


def load_json(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        fail(f"missing {path.relative_to(REPO_ROOT)}")
    except json.JSONDecodeError as error:
        fail(f"{path.relative_to(REPO_ROOT)} is invalid JSON: {error}")


def fail(message: str) -> None:
    print(f"tutorial-pack-contract: {message}", file=sys.stderr)
    raise SystemExit(1)


def iter_values(value: Any, path: str = "$") -> list[tuple[str, Any]]:
    values = [(path, value)]
    if isinstance(value, dict):
        for key, child in value.items():
            values.extend(iter_values(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            values.extend(iter_values(child, f"{path}[{idx}]"))
    return values


def ensure_relative(path_value: str, field_path: str) -> None:
    if path_value.startswith(("http://", "https://")):
        return
    if path_value.startswith("${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/"):
        return
    if os.path.isabs(path_value) or path_value.startswith("~"):
        fail(f"{field_path} must not be absolute: {path_value}")
    if ".." in Path(path_value).parts:
        fail(f"{field_path} must not contain '..': {path_value}")


def validate_schema(schema: dict[str, Any]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        fail("schema must declare JSON Schema draft 2020-12")
    if schema.get("properties", {}).get("schema_version", {}).get("const") != EXPECTED_SCHEMA_VERSION:
        fail("schema_version const does not match tutorial-pack.v0")

    required = set(schema.get("required", []))
    for field in {
        "customers",
        "learner",
        "regression",
        "native_provider_policy",
        "sections",
    }:
        if field not in required:
            fail(f"schema is missing required top-level field {field}")


def validate_review_schema(schema: dict[str, Any]) -> None:
    if schema.get("properties", {}).get("schema_version", {}).get("const") != "tutorial-pack-review.v0":
        fail("review schema_version const does not match tutorial-pack-review.v0")
    required = set(schema.get("required", []))
    for field in {
        "casa_source",
        "casars_equivalents",
        "observable_products",
        "regression_evidence",
        "human_evaluation",
    }:
        if field not in required:
            fail(f"review schema is missing required field {field}")


def validate_template(template: dict[str, Any]) -> None:
    if template.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        fail("template schema_version does not match tutorial-pack.v0")

    surfaces = set(template.get("surfaces", []))
    if surfaces != EXPECTED_SURFACES:
        fail(f"template surfaces must be {sorted(EXPECTED_SURFACES)}, got {sorted(surfaces)}")

    storage_policy = template.get("storage", {}).get("generated_pack_root_policy", "")
    if "${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/" not in storage_policy:
        fail("generated pack root must use CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity")

    if template.get("learner", {}).get("include_internal_evidence") is not False:
        fail("learner view must not include internal evidence")
    if "review_record_schema" not in template.get("regression", {}):
        fail("regression view must point at the review record schema")

    all_text = json.dumps(template, sort_keys=True)
    if LOCAL_PATH_LEAK_RE.search(all_text):
        fail("template contains local absolute path or user-home leak")

    input_ids = {entry.get("id") for entry in template.get("inputs", [])}
    for required_input in {"twhya-cont-image", "twhya-n2hp-image"}:
        if required_input not in input_ids:
            fail(f"template missing input {required_input}")

    for entry in template.get("inputs", []):
        if entry.get("materialization") == "committed":
            fail(f"input {entry.get('id')} attempts to commit tutorial data")
        ensure_relative(entry.get("pack_path", ""), f"inputs[{entry.get('id')}].pack_path")

    section_tasks: set[str] = set()
    for section in template.get("sections", []):
        review = section.get("review_checkpoint", {})
        if review.get("required") is not True:
            fail(f"section {section.get('id')} must require human review")
        if review.get("status") != "pending-human-review":
            fail(f"section {section.get('id')} must start pending human review")
        ensure_relative(review.get("record_path", ""), f"{section.get('id')}.review_checkpoint.record_path")
        section_tasks.update(section.get("tasks", []))

        native_surfaces = set()
        for step in section.get("steps", []):
            step_id = step.get("id")
            surface = step.get("surface")
            provider_kind = step.get("provider_kind")
            if surface == "oracle":
                if provider_kind != "casa-oracle":
                    fail(f"oracle step {step_id} must use casa-oracle provider_kind")
            else:
                native_surfaces.add(surface)
                if provider_kind != "native-rust":
                    fail(f"native step {step_id} must use native-rust provider_kind")

            step_text = json.dumps(step, sort_keys=True)
            if surface != "oracle" and any(text in step_text for text in FORBIDDEN_PROVIDER_TEXT):
                fail(f"native step {step_id} mentions forbidden provider text")

            for output in step.get("outputs", []):
                ensure_relative(output.get("path", ""), f"{step_id}.outputs.path")

        missing = EXPECTED_SURFACES - native_surfaces
        if missing:
            fail(f"section {section.get('id')} missing native surfaces {sorted(missing)}")

    missing_tasks = EXPECTED_TASKS - section_tasks
    if missing_tasks:
        fail(f"template missing planned tasks {sorted(missing_tasks)}")

    for path, value in iter_values(template):
        if not isinstance(value, str):
            continue
        if path.endswith(("_path", ".path", ".docs_index", ".section_docs_path", ".screenshot_path")):
            ensure_relative(value, path)


def main() -> None:
    schema = load_json(SCHEMA_PATH)
    review_schema = load_json(REVIEW_SCHEMA_PATH)
    template = load_json(TEMPLATE_PATH)
    validate_schema(schema)
    validate_review_schema(review_schema)
    validate_template(template)


if __name__ == "__main__":
    main()
