#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Enforce the ADR-0016 imaging crate layering and source rules.

Checks, from `resources/imaging-architecture/dependency-policy.json`:

1. every workspace package with a declared layer depends (normal and build
   dependencies, any target) only on packages in layers its layer may use;
2. every native imaging package depends on exactly the declared workspace set;
3. packages in device-free layers pull no device crate;
4. source rules: regexes that must not match outside grandfathered files.
   The grandfather list only shrinks: a listed file that no longer exists or
   no longer matches fails the check so the entry is removed.

Run with `--grandfather` to print the files that currently violate each
source rule, in policy form.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
POLICY = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"


def cargo_metadata() -> dict:
    output = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    ).stdout
    return json.loads(output)


def package_dependencies(package: dict, include_dev: bool) -> list[str]:
    names = []
    for dependency in package["dependencies"]:
        if dependency["kind"] == "dev" and not include_dev:
            continue
        names.append(dependency["name"])
    return names


def check_layering(policy: dict, metadata: dict) -> list[str]:
    failures: list[str] = []
    layers = policy["package_layers"]
    edges = policy["allowed_logical_edges"]
    exact = policy["native_package_workspace_dependencies"]
    device_prefixes = tuple(policy["device_dependency_prefixes"])
    device_free = set(policy["device_free_layers"])
    workspace = {package["name"] for package in metadata["packages"]}

    for package in metadata["packages"]:
        name = package["name"]
        layer = layers.get(name)
        dependencies = package_dependencies(package, include_dev=False)
        if layer is not None:
            for dependency in dependencies:
                target_layer = layers.get(dependency)
                if target_layer is None:
                    continue
                if target_layer != layer and target_layer not in edges[layer]:
                    failures.append(
                        f"{name} ({layer}) depends on {dependency} ({target_layer}); "
                        f"{layer} may use only {edges[layer]}"
                    )
            if layer in device_free:
                for dependency in dependencies:
                    if dependency.startswith(device_prefixes):
                        failures.append(f"{name} ({layer}) is device-free but depends on {dependency}")
        if name in exact:
            actual = sorted({d for d in dependencies if d in workspace})
            expected = sorted(exact[name])
            if actual != expected:
                failures.append(
                    f"{name} workspace dependencies are {actual}; policy declares {expected}"
                )
    for name in exact:
        if name not in workspace:
            failures.append(f"policy declares dependencies for missing package {name}")
    return failures


def strip_inline_tests(source: str) -> str:
    marker = re.search(r"(?m)^#\[cfg\(test\)\]\s*$", source)
    return source if marker is None else source[: marker.start()]


def rule_files(rule: dict) -> list[Path]:
    roots = [REPO_ROOT / root for root in rule["roots"]]
    excluded = [REPO_ROOT / root for root in rule.get("exclude_roots", [])]
    extensions = tuple(rule["extensions"])
    files: list[Path] = []
    for root in roots:
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix not in extensions:
                continue
            if any(path.is_relative_to(ex) for ex in excluded):
                continue
            files.append(path)
    return files


def rule_violations(rule: dict) -> set[str]:
    pattern = re.compile(rule["regex"], re.MULTILINE)
    violating: set[str] = set()
    for path in rule_files(rule):
        source = path.read_text(encoding="utf-8")
        if rule.get("ignore_inline_tests", True):
            source = strip_inline_tests(source)
        if pattern.search(source):
            violating.add(str(path.relative_to(REPO_ROOT)))
    return violating


def check_source_rules(policy: dict) -> list[str]:
    failures: list[str] = []
    for rule in policy["source_rules"]:
        grandfathered = set(rule.get("grandfathered", []))
        violating = rule_violations(rule)
        for path in sorted(violating - grandfathered):
            failures.append(f"{rule['id']}: {path}: {rule['message']}")
        for path in sorted(grandfathered - violating):
            failures.append(
                f"{rule['id']}: {path} no longer violates the rule (or was deleted); "
                "remove it from the grandfathered list"
            )
    return failures


def print_grandfather(policy: dict) -> None:
    for rule in policy["source_rules"]:
        print(json.dumps({"id": rule["id"], "grandfathered": sorted(rule_violations(rule))}, indent=1))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grandfather", action="store_true", help="print current violations in policy form")
    args = parser.parse_args()
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    if args.grandfather:
        print_grandfather(policy)
        return 0
    failures = check_layering(policy, cargo_metadata()) + check_source_rules(policy)
    for failure in failures:
        print(f"imaging-dependencies: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("imaging-dependencies: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
