#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reject shipped tasks or production code that delegate to CASA at runtime.

casa-rs tasks are native implementations. CASA (casatasks, casatools, or a
local CASA build) is a test and evidence oracle only. Twelve "tasks" were once
shipped through a `casars-casa-task` bridge that shelled out to CASA's Python
`casatasks`; they were removed, and this check keeps that from recurring.

It fails when:
- an application in the canonical catalog launches through a CASA bridge
  executable, or a parameter surface is routed through one or declares the
  retired `casa_task_adapter` provider family;
- an application's executable is not a Rust bin target of its declared cargo
  package (so every launched program is covered by the source scan below);
- shipped code mentions `casatasks`, `casatools`, `mpicasa`, or a hard-coded
  CASA build environment on a code line: non-test Rust and scripts under
  `crates/*/src`, the `casars` Python package, and `apps/*/Sources` Swift.

Test code is exempt: `tests/` directories, `tests.rs` / `*_tests.rs` files,
inline `#[cfg(test)] mod ...` blocks, and the dev-only `casa-test-support`
crate. Comment lines are exempt so rustdoc may cite upstream CASA sources.
"""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = REPO_ROOT / "crates" / "casa-provider-contracts" / "resources"
APPLICATIONS_PATH = CONTRACT_ROOT / "application-catalog.json"
SURFACES_PATH = CONTRACT_ROOT / "parameter-surfaces.json"

# Crates that exist only to support tests (dev-dependencies everywhere).
TEST_ONLY_CRATES = {"casa-test-support"}

BRIDGE_NAME = re.compile(r"casa[-_]task(?![-_]runtime)", re.IGNORECASE)
RETIRED_PROVIDER_FAMILIES = {"casa_task_adapter"}
CASA_RUNTIME_CODE = re.compile(r"\b(casatasks|casatools|mpicasa)\b")
CASA_INSTALL_PATH = re.compile(r"casa-build/venv")
TEST_MODULE_START = re.compile(r"^\s*mod\s+\w+\s*\{")


def load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def workspace_bin_targets() -> dict[str, set[str]]:
    """Map each workspace cargo package to its bin target names."""
    targets: dict[str, set[str]] = {}
    for manifest in sorted((REPO_ROOT / "crates").glob("*/Cargo.toml")):
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        package = data.get("package", {}).get("name")
        if not package:
            continue
        crate = manifest.parent
        bins = {str(entry["name"]) for entry in data.get("bin", []) if "name" in entry}
        if (crate / "src" / "main.rs").exists():
            bins.add(package)
        bin_dir = crate / "src" / "bin"
        if bin_dir.is_dir():
            bins.update(path.stem for path in bin_dir.glob("*.rs"))
            bins.update(path.parent.name for path in bin_dir.glob("*/main.rs"))
        targets[package] = bins
    return targets


def catalog_violations() -> list[str]:
    problems: list[str] = []
    bin_targets = workspace_bin_targets()
    for entry in load(APPLICATIONS_PATH).get("applications", []):
        launch = entry.get("launch", {})
        executable = str(launch.get("executable", ""))
        package = str(launch.get("cargo_package", ""))
        if executable not in bin_targets.get(package, set()):
            problems.append(
                f"application {entry.get('id')!r} executable {executable!r} is not a bin "
                f"target of workspace package {package!r}"
            )
        for field in ("executable", "override_env"):
            value = str(launch.get(field, ""))
            if BRIDGE_NAME.search(value):
                problems.append(
                    f"application {entry.get('id')!r} launches through CASA bridge "
                    f"{field}={value!r}"
                )
    for surface in load(SURFACES_PATH).get("surfaces", []):
        invocation = str(surface.get("execution", {}).get("invocation_name", ""))
        if BRIDGE_NAME.search(invocation):
            problems.append(
                f"surface {surface.get('id')!r} is routed through CASA bridge {invocation!r}"
            )
        family = surface.get("provider_family")
        if family in RETIRED_PROVIDER_FAMILIES:
            problems.append(
                f"surface {surface.get('id')!r} declares retired provider family {family!r}"
            )
    return problems


def is_test_path(path: Path) -> bool:
    relative = path.relative_to(REPO_ROOT / "crates")
    parts = relative.parts
    if parts[0] in TEST_ONLY_CRATES:
        return True
    if any(part in {"tests", "benches", "examples"} for part in parts[1:-1]):
        return True
    return (
        path.name == "tests.rs"
        or path.name.endswith("_tests.rs")
        or path.name.startswith("test_")
    )


def production_lines(path: Path) -> list[tuple[int, str]]:
    """Return numbered lines outside inline `#[cfg(test)] mod name { ... }` blocks."""
    lines = path.read_text(encoding="utf-8").splitlines()
    kept: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.strip() == "#[cfg(test)]":
            next_index = index + 1
            while next_index < len(lines) and lines[next_index].strip().startswith("#["):
                next_index += 1
            if next_index < len(lines) and TEST_MODULE_START.match(lines[next_index]):
                depth = 0
                cursor = next_index
                while cursor < len(lines):
                    depth += lines[cursor].count("{") - lines[cursor].count("}")
                    cursor += 1
                    if depth <= 0:
                        break
                index = cursor
                continue
        kept.append((index + 1, line))
        index += 1
    return kept


# Shipped non-Rust sources: scripts embedded in crates (e.g. via include_str!),
# the casars Python package, and the macOS app. Value is the comment prefix.
SCRIPT_COMMENT_PREFIX = {".py": "#", ".sh": "#", ".js": "//", ".mjs": "//", ".ts": "//"}


def shipped_sources() -> list[tuple[Path, list[tuple[int, str]], str]]:
    sources: list[tuple[Path, list[tuple[int, str]], str]] = []
    for path in sorted((REPO_ROOT / "crates").glob("*/src/**/*.rs")):
        if not is_test_path(path):
            sources.append((path, production_lines(path), "//"))
    script_roots = [
        *sorted((REPO_ROOT / "crates").glob("*/src")),
        REPO_ROOT / "crates" / "casars-python" / "python" / "casars",
    ]
    for root in script_roots:
        for path in sorted(root.rglob("*")):
            prefix = SCRIPT_COMMENT_PREFIX.get(path.suffix)
            if prefix is None or not path.is_file() or is_test_path(path):
                continue
            lines = path.read_text(encoding="utf-8").splitlines()
            sources.append((path, list(enumerate(lines, start=1)), prefix))
    for path in sorted((REPO_ROOT / "apps").glob("*/Sources/**/*.swift")):
        lines = path.read_text(encoding="utf-8").splitlines()
        sources.append((path, list(enumerate(lines, start=1)), "//"))
    return sources


def source_violations() -> list[str]:
    problems: list[str] = []
    for path, lines, comment_prefix in shipped_sources():
        for number, line in lines:
            if CASA_INSTALL_PATH.search(line):
                problems.append(
                    f"{path.relative_to(REPO_ROOT)}:{number}: hard-coded CASA install path"
                )
                continue
            if line.lstrip().startswith(comment_prefix):
                continue
            if CASA_RUNTIME_CODE.search(line):
                problems.append(
                    f"{path.relative_to(REPO_ROOT)}:{number}: shipped code references "
                    "CASA casatasks/casatools/mpicasa"
                )
    return problems


def main() -> int:
    problems = catalog_violations() + source_violations()
    if problems:
        print(
            "no-casa-runtime: casa-rs tasks must be native; CASA is a test/evidence "
            "oracle only (see AGENTS.md, Engineering Rules)",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
