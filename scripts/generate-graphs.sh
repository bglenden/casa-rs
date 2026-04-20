#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

out_dir="docs/generated"
metadata_json="$out_dir/cargo-metadata.json"

mkdir -p "$out_dir"

cargo metadata --format-version 1 --no-deps > "$metadata_json"

python3 - "$metadata_json" "$out_dir" <<'PY'
import json
import pathlib
import sys

metadata_path = pathlib.Path(sys.argv[1])
out_dir = pathlib.Path(sys.argv[2])

with metadata_path.open() as fh:
    data = json.load(fh)

workspace_root = pathlib.Path(data["workspace_root"]).resolve()
packages = sorted(data["packages"], key=lambda pkg: pkg["name"])

with (out_dir / "workspace-members.txt").open("w") as fh:
    for pkg in packages:
        manifest = pathlib.Path(pkg["manifest_path"]).resolve()
        try:
            rel_manifest = manifest.relative_to(workspace_root)
        except ValueError:
            rel_manifest = manifest
        fh.write(f"{pkg['name']}\t{rel_manifest}\n")

path_edges = set()
for pkg in packages:
    for dep in pkg.get("dependencies", []):
        dep_path = dep.get("path")
        if not dep_path:
            continue
        dep_root = pathlib.Path(dep_path).resolve()
        try:
            rel_dep_root = dep_root.relative_to(workspace_root)
        except ValueError:
            continue
        dep_kind = dep.get("kind") or "normal"
        path_edges.add((pkg["name"], dep["name"], dep_kind, str(rel_dep_root)))

with (out_dir / "workspace-path-deps.tsv").open("w") as fh:
    fh.write("source\ttarget\tkind\tpath\n")
    for source, target, kind, rel_path in sorted(path_edges):
        fh.write(f"{source}\t{target}\t{kind}\t{rel_path}\n")
PY
