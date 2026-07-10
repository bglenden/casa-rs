#!/usr/bin/env python3
"""Enforce append-only semantic revisions for parameter contracts."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "resources/parameter-catalog.json"
SURFACES_PATH = REPO_ROOT / "resources/parameter-surfaces.json"
HISTORY_PATH = REPO_ROOT / "resources/parameter-contract-history.json"


class HistoryError(RuntimeError):
    """Raised when a semantic contract changes without a version bump."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update",
        action="store_true",
        help="append new concept revisions and surface contract versions",
    )
    return parser.parse_args()


def load_object(path: Path, *, required: bool = True) -> dict[str, Any]:
    if not path.exists() and not required:
        return {"schema_version": 1, "concepts": [], "surfaces": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise HistoryError(f"read {path.relative_to(REPO_ROOT)}: {error}") from error
    if not isinstance(value, dict):
        raise HistoryError(f"{path.relative_to(REPO_ROOT)} must contain an object")
    return value


def fingerprint(value: Any) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def concept_semantics(concept: dict[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(concept)
    value.pop("documentation", None)
    return value


def surface_semantics(surface: dict[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(surface)
    for key in ("display_name", "category", "summary"):
        value.pop(key, None)
    for binding in value.get("bindings", []):
        binding.pop("surface_note", None)
        presentation = binding.get("projections", {}).get("presentation")
        if isinstance(presentation, dict):
            binding["projections"].pop("presentation", None)
        python = binding.get("projections", {}).get("python")
        if isinstance(python, dict):
            python.pop("type_hint", None)
    return value


def history_index(
    entries: Any, key_names: tuple[str, str], context: str
) -> dict[tuple[str, int], str]:
    if not isinstance(entries, list):
        raise HistoryError(f"history {context} must be an array")
    result: dict[tuple[str, int], str] = {}
    for position, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise HistoryError(f"history {context}[{position}] must be an object")
        identifier = entry.get(key_names[0])
        version = entry.get(key_names[1])
        digest = entry.get("fingerprint")
        if not isinstance(identifier, str) or not isinstance(version, int) or not isinstance(digest, str):
            raise HistoryError(f"history {context}[{position}] is malformed")
        key = (identifier, version)
        if key in result:
            raise HistoryError(
                f"history {context} repeats {identifier!r} {key_names[1]} {version}; "
                "versions are append-only"
            )
        result[key] = digest
    return result


def check_or_append(
    *,
    entries: list[dict[str, Any]],
    index: dict[tuple[str, int], str],
    identifier: str,
    version: int,
    digest: str,
    id_key: str,
    version_key: str,
    update: bool,
) -> None:
    key = (identifier, version)
    if key in index:
        if index[key] != digest:
            raise HistoryError(
                f"{identifier!r} changed without bumping {version_key} {version}; "
                f"restore its semantics or add a newer {version_key}"
            )
        return
    previous = [known_version for known_id, known_version in index if known_id == identifier]
    if previous and version <= max(previous):
        raise HistoryError(
            f"{identifier!r} uses {version_key} {version}, not newer than historical {max(previous)}"
        )
    if not update:
        raise HistoryError(
            f"{identifier!r} {version_key} {version} is not recorded; run "
            "scripts/check-parameter-contract-history.py --update after reviewing the bump"
        )
    entry = {id_key: identifier, version_key: version, "fingerprint": digest}
    entries.append(entry)
    index[key] = digest


def run(update: bool) -> None:
    catalog = load_object(CATALOG_PATH)
    surfaces = load_object(SURFACES_PATH)
    history = load_object(HISTORY_PATH, required=not update)
    if history.get("schema_version") != 1:
        raise HistoryError("parameter contract history schema_version must be 1")
    concept_entries = history.setdefault("concepts", [])
    surface_entries = history.setdefault("surfaces", [])
    concept_history = history_index(
        concept_entries, ("id", "semantic_revision"), "concepts"
    )
    surface_history = history_index(
        surface_entries, ("id", "contract_version"), "surfaces"
    )

    current_concepts = catalog.get("concepts")
    current_surfaces = surfaces.get("surfaces")
    if not isinstance(current_concepts, list) or not isinstance(current_surfaces, list):
        raise HistoryError("canonical catalog resources are malformed")

    for concept in current_concepts:
        check_or_append(
            entries=concept_entries,
            index=concept_history,
            identifier=str(concept["id"]),
            version=int(concept["semantic_revision"]),
            digest=fingerprint(concept_semantics(concept)),
            id_key="id",
            version_key="semantic_revision",
            update=update,
        )
    for surface in current_surfaces:
        check_or_append(
            entries=surface_entries,
            index=surface_history,
            identifier=str(surface["id"]),
            version=int(surface["contract_version"]),
            digest=fingerprint(surface_semantics(surface)),
            id_key="id",
            version_key="contract_version",
            update=update,
        )

    if update:
        concept_entries.sort(key=lambda entry: (entry["id"], entry["semantic_revision"]))
        surface_entries.sort(key=lambda entry: (entry["id"], entry["contract_version"]))
        HISTORY_PATH.write_text(
            json.dumps(history, indent=2, sort_keys=False) + "\n", encoding="utf-8"
        )
        print(f"updated {HISTORY_PATH.relative_to(REPO_ROOT)}")


def main() -> int:
    try:
        run(parse_args().update)
    except HistoryError as error:
        print(f"parameter-contract-history: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
