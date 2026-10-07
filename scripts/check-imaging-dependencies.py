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


TEST_GATE = re.compile(r"(?m)^[ \t]*#\[cfg\(test\)\][ \t]*\n")
ATTRIBUTE = re.compile(r"[ \t]*#\[[^\n]*\][ \t]*\n")
CHAR_LITERAL = re.compile(r"'(?:\\.|[^\\'])'")


def skip_string(source: str, start: int) -> int:
    """Return the index just past the string literal starting at `start`."""
    raw = re.match(r"b?r(#*)\"", source[start:])
    if raw:
        terminator = '"' + raw.group(1)
        end = source.find(terminator, start + raw.end())
        return len(source) if end == -1 else end + len(terminator)
    i = start + 1
    while i < len(source):
        if source[i] == "\\":
            i += 2
            continue
        if source[i] == '"':
            return i + 1
        i += 1
    return len(source)


def item_end(source: str, start: int) -> int:
    """Return the index just past the Rust item that begins at `start`.

    The item ends at the first `;` at brace depth zero, or at the brace that
    closes its first block. Comments, string and char literals are skipped.
    """
    depth = 0
    i = start
    n = len(source)
    while i < n:
        if source.startswith("//", i):
            newline = source.find("\n", i)
            i = n if newline == -1 else newline + 1
            continue
        if source.startswith("/*", i):
            close = source.find("*/", i + 2)
            i = n if close == -1 else close + 2
            continue
        c = source[i]
        if c == '"' or source.startswith('r"', i) or source.startswith('r#', i) or source.startswith('b"', i):
            i = skip_string(source, i if c == '"' else i + (1 if c in "rb" else 0))
            continue
        if c == "'":
            literal = CHAR_LITERAL.match(source, i)
            i = literal.end() if literal else i + 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        elif c == ";" and depth == 0:
            return i + 1
        i += 1
    return n


def strip_inline_tests(source: str) -> str:
    """Remove every item gated by `#[cfg(test)]`, keeping the production code
    around it (a test module, a test-only import, constant or function may be
    followed by production items)."""
    out: list[str] = []
    i = 0
    while True:
        gate = TEST_GATE.search(source, i)
        if gate is None:
            out.append(source[i:])
            return "".join(out)
        out.append(source[i : gate.start()])
        j = gate.end()
        while True:
            attribute = ATTRIBUTE.match(source, j)
            if attribute is None:
                break
            j = attribute.end()
        i = item_end(source, j)


SELF_TEST_CASES = [
    (
        "production after a test module is checked",
        "fn production() { let _ = std::env::var(\"X\"); }\n#[cfg(test)]\nmod tests { fn t() { let _ = std::env::var(\"Y\"); } }\n",
        True,
    ),
    (
        "production after a test-gated constant is checked",
        "#[cfg(test)]\nconst TEST_HELPER: u32 = 1;\npub fn production() { let _ = std::env::var(\"EXAMPLE\"); }\n",
        True,
    ),
    (
        "production after a test-gated import is checked",
        "#[cfg(test)]\nuse std::collections::BTreeMap;\nfn production() { let _ = std::env::var(\"Z\"); }\n",
        True,
    ),
    (
        "an access only inside the test module is ignored",
        "fn production() {}\n#[cfg(test)]\nmod tests { fn t() { let _ = std::env::var(\"Y\"); } }\n",
        False,
    ),
    (
        "braces in strings do not end the test module early",
        "#[cfg(test)]\nmod tests { const S: &str = \"}\"; fn t() { let _ = std::env::var(\"Y\"); } }\n",
        False,
    ),
]


def self_test() -> int:
    pattern = re.compile(r"\benv::var(?:_os)?\s*\(|\bstd::env\b")
    failures = 0
    for name, source, expected in SELF_TEST_CASES:
        found = pattern.search(strip_inline_tests(source)) is not None
        if found != expected:
            failures += 1
            print(f"imaging-dependencies self-test failed: {name}", file=sys.stderr)
    if failures == 0:
        print("imaging-dependencies: self-test ok")
    return 1 if failures else 0


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
    parser.add_argument("--self-test", action="store_true", help="check the test-stripping logic")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
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
