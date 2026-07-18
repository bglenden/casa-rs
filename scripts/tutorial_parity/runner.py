#!/usr/bin/env python3
"""Validate or execute declarative first-look tutorial parity sections."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tutorial_parity.adapters import adapters
from tutorial_parity.commands import path_record
from tutorial_parity.comparators import run as run_comparator
from tutorial_parity.documentation import write_documentation
from tutorial_parity.evidence import result_document, review_document, write_json_atomic
from tutorial_parity.model import RuntimeResources, SectionManifest
from tutorial_parity.resources import ResourceError, resolve_resources
from tutorial_parity.schema import ContractError, load_section, validate_result


SECTIONS_ROOT = Path(__file__).resolve().parent / "sections"


def manifests() -> list[SectionManifest]:
    loaded = [load_section(path) for path in sorted(SECTIONS_ROOT.glob("*.json"))]
    identities = [(manifest.pack_id, manifest.section_id) for manifest in loaded]
    if len(identities) != len(set(identities)):
        raise ContractError("section IDs must be unique within each pack")
    return loaded


def select_manifests(all_manifests: list[SectionManifest], *, all_sections: bool, section: str | None) -> list[SectionManifest]:
    if all_sections:
        return all_manifests
    matches = [manifest for manifest in all_manifests if section in {manifest.section_id, f"{manifest.pack_id}:{manifest.section_id}"}]
    if len(matches) != 1:
        choices = ", ".join(f"{item.pack_id}:{item.section_id}" for item in all_manifests)
        raise ContractError(f"section selector {section!r} matched {len(matches)} sections; choose from {choices}")
    return matches


def resource_summary(resources: RuntimeResources) -> dict[str, Any]:
    return {
        "pack_root": str(resources.pack_root),
        "native_python": str(resources.native_python) if resources.native_python else None,
        "casa_python": str(resources.casa_python) if resources.casa_python else None,
        "binary_dir": str(resources.binary_dir),
        "ghostty_capture": str(resources.ghostty_capture) if resources.ghostty_capture else None,
        "evidence_root": str(resources.evidence_root),
    }


def run_section(
    manifest: SectionManifest,
    resources: RuntimeResources,
    *,
    selected_surfaces: set[str],
    dry_run: bool,
    gui_cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    registry = adapters()
    surface_results: dict[str, Any] = {}
    artifacts: list[dict[str, Any]] = []
    failed = False
    unavailable = False
    for name in ("casa", "cli", "python", "tui", "gui"):
        if name not in selected_surfaces:
            continue
        surface = manifest.surfaces[name]
        try:
            plan = registry[name].plan(manifest, surface, resources)
            if dry_run:
                surface_results[name] = {"status": "dry_run", "operations": [plan.as_dict()], "artifacts": [], "reason": None}
                continue
            if name == "gui" and surface.journey in gui_cache:
                result = gui_cache[surface.journey]
            else:
                result = registry[name].execute(plan)
                if name == "gui" and surface.journey:
                    gui_cache[surface.journey] = result
            surface_results[name] = result
            for path in plan.outputs:
                if path.exists():
                    artifacts.append(path_record(path, resources.pack_root))
            failed = failed or result["status"] == "failed"
        except ResourceError as error:
            unavailable = True
            surface_results[name] = {"status": "unavailable", "operations": [], "artifacts": [], "reason": f"{error.category}: {error}"}
        except Exception as error:  # surface failures must become typed evidence
            failed = True
            surface_results[name] = {"status": "failed", "operations": [], "artifacts": [], "reason": f"{type(error).__name__}: {error}"}

    if dry_run:
        comparison = {"status": "dry_run", "plugin": manifest.comparison.plugin}
        status = "dry_run"
    elif failed:
        comparison = {"status": "not_run", "plugin": manifest.comparison.plugin, "reason": "surface execution failed"}
        status = "failed"
    else:
        comparison = run_comparator(manifest, resources)
        failed = comparison.get("status") == "failed"
        unavailable = unavailable or comparison.get("status") == "unavailable"
        status = "failed" if failed else "unavailable" if unavailable else "completed"
    result = result_document(
        section_id=manifest.section_id,
        status=status,
        resources=resource_summary(resources),
        surfaces=surface_results,
        comparison=comparison,
        artifacts=artifacts,
        failure={"kind": "execution_or_comparison", "reason": "one or more required checks failed"} if status == "failed" else None,
    )
    if not dry_run:
        result_path = resources.pack_root / manifest.evidence["result"]
        write_json_atomic(result_path, validate_result(result, str(result_path)))
        write_json_atomic(
            resources.pack_root / manifest.evidence["review"],
            review_document(manifest_id=manifest.section_id, result_ref=manifest.evidence["result"], result=result),
        )
        screenshot_paths = [surface.screenshot for surface in manifest.surfaces.values() if surface.screenshot]
        screenshot_paths.extend(manifest.surfaces["gui"].required_artifacts)
        write_json_atomic(
            resources.pack_root / manifest.evidence["screenshot_spec"],
            {"schema_version": 1, "kind": "tutorial_parity_screenshot_spec", "section_id": manifest.section_id, "paths": screenshot_paths, "gui_journey": manifest.surfaces["gui"].journey},
        )
        documentation = write_documentation(resources.pack_root, manifest, result)
        result["artifacts"].extend(path_record(path, resources.pack_root) for path in documentation)
        write_json_atomic(result_path, result)
    return result


def add_selector(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", dest="all_sections")
    group.add_argument("--section")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    add_selector(validate_parser)
    run_parser = subparsers.add_parser("run")
    add_selector(run_parser)
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--surface", action="append", choices=["casa", "cli", "python", "tui", "gui"])
    run_parser.add_argument("--pack-root", type=Path)
    run_parser.add_argument("--native-python", type=Path)
    run_parser.add_argument("--casa-python", type=Path)
    run_parser.add_argument("--binary-dir", type=Path)
    run_parser.add_argument("--ghostty-capture", type=Path)
    run_parser.add_argument("--evidence-root", type=Path)
    args = parser.parse_args()
    try:
        all_manifests = manifests()
        selected = select_manifests(all_manifests, all_sections=args.all_sections, section=args.section)
        if args.command == "validate":
            print(json.dumps({"validated": len(selected), "sections": [f"{item.pack_id}:{item.section_id}" for item in selected]}, indent=2))
            return 0
        if args.all_sections and args.pack_root is not None:
            raise ContractError("--pack-root cannot represent both first-look packs; use CASA_RS_TUTORIAL_DATA_ROOT with --all")
        selected_surfaces = set(args.surface or ["casa", "cli", "python", "tui", "gui"])
        results = []
        gui_cache: dict[str, dict[str, Any]] = {}
        for manifest in selected:
            resources = resolve_resources(
                manifest,
                pack_root=args.pack_root,
                native_python=args.native_python,
                casa_python=args.casa_python,
                binary_dir=args.binary_dir,
                ghostty_capture=args.ghostty_capture,
                evidence_root=args.evidence_root,
                require_existing=not args.dry_run,
            )
            results.append(run_section(manifest, resources, selected_surfaces=selected_surfaces, dry_run=args.dry_run, gui_cache=gui_cache))
        print(json.dumps(results, indent=2, sort_keys=True))
        return 1 if any(result["status"] == "failed" for result in results) else 0
    except (ContractError, ResourceError, OSError, json.JSONDecodeError) as error:
        print(f"tutorial parity runner: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
