#!/usr/bin/env python3
"""Validate portable tutorial-template v1 resources without external dependencies."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TUTORIAL_ROOT = REPO_ROOT / "resources" / "tutorials"
IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")
TASK_CELL = re.compile(r"<!-- casa-rs-cell:v1 id=[^ ]+ kind=task -->")


def fail(message: str) -> None:
    print(f"tutorial-template-contract: {message}", file=sys.stderr)
    raise SystemExit(1)


def relative_path(value: str, field: str) -> None:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        fail(f"{field} must be a safe non-empty relative path: {value!r}")
    if path.parts[0] in {".casa-rs", "notebooks"}:
        fail(f"{field} must not target managed notebook state: {value!r}")


def validate_template(root: Path) -> None:
    manifest_path = root / "tutorial.toml"
    markdown_path = root / "tutorial.md"
    if not manifest_path.is_file() or not markdown_path.is_file():
        fail(f"{root.relative_to(REPO_ROOT)} must contain tutorial.toml and tutorial.md")
    try:
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"{manifest_path.relative_to(REPO_ROOT)} is invalid: {error}")
    if manifest.get("schema_version") != 1:
        fail(f"{manifest_path.relative_to(REPO_ROOT)} must use schema_version=1")
    tutorial_id = manifest.get("tutorial_id", "")
    if not IDENTIFIER.fullmatch(tutorial_id):
        fail(f"invalid tutorial_id {tutorial_id!r}")
    datasets = manifest.get("datasets", [])
    if not datasets:
        fail(f"{tutorial_id} must declare at least one dataset")
    dataset_ids: set[str] = set()
    for dataset in datasets:
        dataset_id = dataset.get("id", "")
        if not IDENTIFIER.fullmatch(dataset_id) or dataset_id in dataset_ids:
            fail(f"invalid or duplicate dataset id {dataset_id!r}")
        dataset_ids.add(dataset_id)
        uri = dataset.get("uri", "")
        if "://" not in uri:
            fail(f"dataset {dataset_id} must declare an explicit URI scheme")
        relative_path(dataset.get("destination", ""), f"datasets.{dataset_id}.destination")
        digest = dataset.get("sha256")
        if digest is not None and not re.fullmatch(r"[0-9A-Fa-f]{64}", digest):
            fail(f"dataset {dataset_id} has an invalid SHA-256")
        unpack = dataset.get("unpack")
        if unpack and (unpack.get("max_entries", 0) <= 0 or unpack.get("max_expanded_bytes", 0) <= 0):
            fail(f"dataset {dataset_id} must declare positive archive bounds")
    for section in manifest.get("sections", []):
        section_id = section.get("id", "")
        if not IDENTIFIER.fullmatch(section_id):
            fail(f"invalid section id {section_id!r}")
        unknown = set(section.get("dataset_ids", [])) - dataset_ids
        if unknown:
            fail(f"section {section_id} references unknown datasets {sorted(unknown)}")
    markdown = markdown_path.read_text(encoding="utf-8")
    declared_cells = sum((section.get("cell_ids", []) for section in manifest.get("sections", [])), [])
    if declared_cells and len(TASK_CELL.findall(markdown)) < len(set(declared_cells)):
        fail(f"{tutorial_id} does not contain every declared task cell")


def main() -> None:
    templates = sorted(path.parent for path in TUTORIAL_ROOT.glob("*/tutorial.toml"))
    if not templates:
        fail("no portable tutorial templates found")
    for template in templates:
        validate_template(template)


if __name__ == "__main__":
    main()
