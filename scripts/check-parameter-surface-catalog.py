#!/usr/bin/env python3
"""Validate the canonical application catalog against parameter surfaces."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = REPO_ROOT / "crates" / "casa-provider-contracts" / "resources"
APPLICATIONS_PATH = CONTRACT_ROOT / "application-catalog.json"
SURFACES_PATH = CONTRACT_ROOT / "parameter-surfaces.json"


def fail(message: str) -> None:
    print(f"parameter-surface-catalog: {message}", file=sys.stderr)
    raise SystemExit(1)


def load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot load {path.relative_to(REPO_ROOT)}: {error}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(REPO_ROOT)} must contain an object")
    return value


def main() -> int:
    applications = load(APPLICATIONS_PATH)
    surfaces = load(SURFACES_PATH)
    if applications.get("schema_version") != 1:
        fail("application catalog schema_version must be 1")
    if surfaces.get("schema_version") != 1:
        fail("parameter surface schema_version must be 1")

    entries = applications.get("applications")
    definitions = surfaces.get("surfaces")
    if not isinstance(entries, list) or not isinstance(definitions, list):
        fail("catalog arrays are missing")

    ids = [entry.get("id") for entry in entries if isinstance(entry, dict)]
    duplicates = sorted(key for key, count in Counter(ids).items() if count > 1)
    if duplicates:
        fail(f"duplicate application IDs: {duplicates}")

    task_ids = {
        entry["id"]
        for entry in entries
        if isinstance(entry, dict) and entry.get("kind") == "task"
    }
    launcher_ids = {
        entry["id"]
        for entry in entries
        if isinstance(entry, dict) and entry.get("kind") == "launcher"
    }
    surface_ids = {
        surface["id"] for surface in definitions if isinstance(surface, dict)
    }
    if task_ids != surface_ids:
        fail(
            f"task/surface mismatch: missing={sorted(surface_ids - task_ids)}, "
            f"extra={sorted(task_ids - surface_ids)}"
        )
    if launcher_ids != {"casars"}:
        fail(f"expected only the casars launcher, found {sorted(launcher_ids)}")

    for entry in entries:
        if not isinstance(entry, dict):
            fail("application entries must be objects")
        launch = entry.get("launch")
        if not isinstance(launch, dict) or not all(
            isinstance(launch.get(field), str) and launch[field]
            for field in ("executable", "cargo_package", "override_env")
        ):
            fail(f"application {entry.get('id')!r} has an incomplete launch descriptor")

    print(
        f"parameter-surface-catalog: {len(task_ids)} task applications and "
        f"{len(launcher_ids)} launcher validated"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
