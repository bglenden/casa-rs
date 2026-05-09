#!/usr/bin/env python3
"""Generate a local tutorial pack directory from a checked-in template."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATHS = {
    "alma-first-look-image-analysis": REPO_ROOT
    / "resources"
    / "tutorial-packs"
    / "alma-first-look-image-analysis.template.json"
}
REVIEW_SCHEMA_PATH = REPO_ROOT / "resources" / "tutorial-pack-review.schema.json"


def default_tutorial_root() -> Path:
    override = os.environ.get("CASA_RS_TUTORIAL_DATA_ROOT")
    if override:
        return Path(override).expanduser()
    return Path.home() / "SoftwareProjects" / "casa-tutorial-data"


def default_output(pack_id: str) -> Path:
    if pack_id != "alma-first-look-image-analysis":
        raise ValueError(f"unknown pack {pack_id!r}")
    return (
        default_tutorial_root()
        / "tutorial-parity"
        / "alma"
        / "first-look"
        / "twhya"
        / "image-analysis"
        / "alma-first-look-image-analysis.pack"
    )


def copy_directory(source: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        if destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    shutil.copytree(source, destination)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def input_source_path(tutorial_root: Path, input_entry: dict[str, Any]) -> Path:
    registry_key = input_entry["registry_key"]
    if registry_key == "alma/first-look/twhya/continuum-image":
        return tutorial_root / "tutorial-parity" / "alma" / "first-look" / "twhya" / "twhya_cont.image"
    if registry_key == "alma/first-look/twhya/n2hp-image":
        return tutorial_root / "tutorial-parity" / "alma" / "first-look" / "twhya" / "twhya_n2hp.image"
    return tutorial_root / "tutorial-parity" / registry_key


def generate_pack(pack_id: str, output: Path, tutorial_root: Path, materialize_inputs: bool) -> dict[str, Any]:
    template_path = TEMPLATE_PATHS[pack_id]
    manifest = json.loads(template_path.read_text(encoding="utf-8"))

    output.mkdir(parents=True, exist_ok=True)
    for relative in [
        "inputs",
        "workspace/native",
        "workspace/oracle",
        "workspace/scratch",
        "docs/sections",
        "evidence/review",
        "screenshots/source",
        "screenshots/annotated",
        "screenshots/specs",
    ]:
        (output / relative).mkdir(parents=True, exist_ok=True)

    input_records: list[dict[str, Any]] = []
    for input_entry in manifest["inputs"]:
        source = input_source_path(tutorial_root, input_entry)
        destination = output / input_entry["pack_path"]
        status = "missing"
        if source.exists() and materialize_inputs:
            copy_directory(source, destination)
            status = "staged"
        elif destination.exists():
            status = "staged"
        input_records.append(
            {
                "id": input_entry["id"],
                "registry_key": input_entry["registry_key"],
                "source": str(source),
                "pack_path": input_entry["pack_path"],
                "status": status,
                "checksum_policy": input_entry["checksum_policy"],
                "size_bytes": input_entry["size_bytes"],
            }
        )

    shutil.copy2(REVIEW_SCHEMA_PATH, output / "evidence" / "review" / "tutorial-pack-review.schema.json")
    write_json(output / "pack.json", manifest)
    write_json(
        output / "evidence" / "data-manifest.json",
        {
            "schema_version": "tutorial-pack-data-manifest.v0",
            "pack_id": manifest["pack_id"],
            "tutorial_id": manifest["tutorial_id"],
            "tutorial_root": str(tutorial_root),
            "inputs": input_records,
        },
    )
    (output / "docs" / "index.md").write_text(
        f"# {manifest['title']}\n\nGenerated tutorial pack skeleton. Section docs are generated as tutorial chunks are reviewed.\n",
        encoding="utf-8",
    )
    return {
        "pack_id": manifest["pack_id"],
        "path": str(output),
        "manifest": str(output / "pack.json"),
        "inputs": input_records,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pack",
        choices=sorted(TEMPLATE_PATHS),
        default="alma-first-look-image-analysis",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--tutorial-root", type=Path, default=default_tutorial_root())
    parser.add_argument(
        "--no-materialize-inputs",
        action="store_true",
        help="create the pack skeleton but leave inputs missing even if local tutorial data exists",
    )
    args = parser.parse_args()

    output = args.output.expanduser() if args.output else default_output(args.pack)
    summary = generate_pack(
        args.pack,
        output=output,
        tutorial_root=args.tutorial_root.expanduser(),
        materialize_inputs=not args.no_materialize_inputs,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
