#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Enforce workspace tiers, publishability and the ADR-0016 imaging rules.

Checks, from `resources/imaging-architecture/dependency-policy.json`:

1. every workspace package with a declared layer depends (normal and build
   dependencies, any target) only on packages in layers its layer may use;
2. every native imaging package depends on exactly the declared workspace
   set, besides the definitional packages, which any package may use and no
   set lists;
3. definitional packages have no layer and depend on no workspace package
   but one another and on no device crate, and packages in device-free layers
   pull no device crate;
4. source rules: regexes that must not match outside grandfathered files.
   A rule whose `roots` is `"definitional"` covers the definitional packages'
   sources.
   The grandfather list only shrinks: a listed file that no longer exists or
   no longer matches fails the check so the entry is removed. Test code is
   skipped unless a rule says otherwise: items gated by `#[cfg(test)]`, and
   the files that only `#[cfg(test)]` module declarations bring into their
   crate, found by walking each crate root's `mod name;` declarations.

Run with `--grandfather` to print the files that currently violate each
source rule, in policy form.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
from graphlib import CycleError, TopologicalSorter
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
POLICY = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"


def cargo_metadata() -> dict:
    output = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
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
    definitional = set(policy["definitional_packages"])
    device_prefixes = tuple(policy["device_dependency_prefixes"])
    device_free = set(policy["device_free_layers"])
    workspace = {package["name"] for package in metadata["packages"]}

    for package in metadata["packages"]:
        name = package["name"]
        layer = layers.get(name)
        dependencies = package_dependencies(package, include_dev=False)
        if name in definitional:
            for dependency in dependencies:
                if dependency in workspace and dependency not in definitional:
                    failures.append(
                        f"{name} is definitional but depends on {dependency}; "
                        "definitional packages depend only on one another"
                    )
                if dependency.startswith(device_prefixes):
                    failures.append(f"{name} is definitional but depends on {dependency}")
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
            actual = sorted({d for d in dependencies if d in workspace and d not in definitional})
            expected = sorted(set(exact[name]) - definitional)
            if actual != expected:
                failures.append(
                    f"{name} workspace dependencies besides the definitional packages are "
                    f"{actual}; policy declares {expected}"
                )
    for name in exact:
        if name not in workspace:
            failures.append(f"policy declares dependencies for missing package {name}")
        listed = sorted(definitional.intersection(exact[name]))
        if listed:
            failures.append(
                f"policy lists definitional {listed} for {name}; any package may use "
                "the definitional packages, so no set lists them"
            )
    for name in sorted(definitional):
        if name not in workspace:
            failures.append(f"policy declares missing definitional package {name}")
        if name in layers:
            failures.append(f"definitional package {name} has a layer; it sits beneath every layer")
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
            print(f"workspace-arch self-test failed: {name}", file=sys.stderr)
    for name, files, expected in MODULE_CASES:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative, text in files.items():
                (root / relative).parent.mkdir(parents=True, exist_ok=True)
                (root / relative).write_text(text, encoding="utf-8")
            found = {str(path.relative_to(root)) for path in test_only_files(root)}
            if found != expected:
                failures += 1
                print(
                    f"workspace-arch self-test failed: {name}: {sorted(found)}",
                    file=sys.stderr,
                )
    for name, changes, packages, expected in LAYERING_CASES:
        found = check_layering(
            {**LAYERING_POLICY, **changes},
            {"packages": [layering_package(*package) for package in packages]},
        )
        if len(found) != expected:
            failures += 1
            print(f"workspace-arch self-test failed: {name}: {found}", file=sys.stderr)
    if failures == 0:
        print("workspace-arch: self-test ok")
    return 1 if failures else 0


# Crates for the module walk: files by path under `src`, and the files only
# test declarations reach.
MODULE_CASES = [
    (
        "a test module and its own submodules",
        {
            "lib.rs": "#[cfg(test)]\nmod tests;\nmod writer;\n",
            "tests.rs": "mod helpers;\n",
            "tests/helpers.rs": "",
            "writer.rs": "#[cfg(test)]\npub(crate) mod probe;\n",
            "writer/probe.rs": "",
        },
        {"tests.rs", "tests/helpers.rs", "writer/probe.rs"},
    ),
    (
        "a production file named like a nested test module stays production",
        {
            "lib.rs": "mod helper;\n#[cfg(test)]\nmod tests {\n    #[cfg(test)]\n    mod helper;\n}\n",
            "helper.rs": "",
            "tests/helper.rs": "",
        },
        {"tests/helper.rs"},
    ),
    (
        "a module inside an inline test module is test code",
        {
            "lib.rs": "#[cfg(test)]\nmod outer {\n    mod inner;\n}\n",
            "outer/inner.rs": "",
        },
        {"outer/inner.rs"},
    ),
    (
        "path attributes, attributes and doc comments between the gate and the module",
        {
            "lib.rs": (
                "#[cfg(test)]\n#[path = \"custom_tests.rs\"]\nmod tests;\n"
                "#[cfg(test)]\n/// A probe.\n#[allow(dead_code)]\nmod probe;\n"
                "#[cfg(test)] mod one_line;\n"
            ),
            "custom_tests.rs": "",
            "probe.rs": "",
            "one_line.rs": "",
        },
        {"custom_tests.rs", "probe.rs", "one_line.rs"},
    ),
    (
        "mod.rs, main.rs and binaries resolve beside themselves",
        {
            "main.rs": "mod a;\n",
            "a/mod.rs": "#[cfg(test)]\nmod t;\n",
            "a/t.rs": "",
            "bin/tool/main.rs": "#[cfg(test)]\nmod check;\n",
            "bin/tool/check.rs": "",
            "bin/single.rs": "#[cfg(test)]\nmod single_tests;\n",
            "bin/single_tests.rs": "",
        },
        {"a/t.rs", "bin/tool/check.rs"},
    ),
    (
        "a file no declaration reaches, or that production also reaches, is production",
        {
            "lib.rs": "mod shared;\n#[cfg(test)]\n#[path = \"shared.rs\"]\nmod again;\n",
            "shared.rs": "",
            "orphan.rs": "",
        },
        set(),
    ),
]

# A two-layer policy over two definitional packages, for the layering cases:
# each case changes the policy, names packages with their dependencies, and
# expects that many failures.
LAYERING_POLICY = {
    "package_layers": {"model": "science", "store": "observation"},
    "allowed_logical_edges": {"science": [], "observation": ["science"]},
    "native_package_workspace_dependencies": {"model": [], "store": ["model", "tables"]},
    "definitional_packages": ["values", "types"],
    "device_dependency_prefixes": ["metal"],
    "device_free_layers": ["science", "observation"],
}
LAYERING_WORKSPACE = [("values",), ("types", "values"), ("tables", "types"), ("store", "model", "tables", "types")]
LAYERING_CASES = [
    (
        "any package may use the definitional packages without listing them",
        {},
        [("model", "types", "values"), *LAYERING_WORKSPACE],
        0,
    ),
    (
        "a dependency outside the definitional set still breaks an exact set",
        {},
        [("model", "types", "tables"), *LAYERING_WORKSPACE],
        1,
    ),
    (
        "an exact set that lists a definitional package is refused",
        {"native_package_workspace_dependencies": {"model": ["types"], "store": ["model", "tables"]}},
        [("model", "types"), *LAYERING_WORKSPACE],
        1,
    ),
    (
        "a definitional package that depends on another workspace package is refused",
        {},
        [("model",), ("values", "tables"), *LAYERING_WORKSPACE[1:]],
        1,
    ),
    (
        "a definitional package that pulls a device crate is refused",
        {},
        [("model",), ("values", "metal"), *LAYERING_WORKSPACE[1:]],
        1,
    ),
    (
        "a definitional package with a layer is refused",
        {"package_layers": {"model": "science", "store": "observation", "types": "science"}},
        [("model",), *LAYERING_WORKSPACE],
        1,
    ),
]


def layering_package(name: str, *dependencies: str) -> dict:
    return {"name": name, "dependencies": [{"name": d, "kind": None} for d in dependencies]}


TEST_GATE_ATTRIBUTE = re.compile(r"#\[\s*cfg\s*\(\s*test\s*\)\s*\]")
PATH_ATTRIBUTE = re.compile(r'#\[\s*path\s*=\s*"([^"]*)"\s*\]')
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
SPACE = re.compile(r"\s*")
RAW_STRING = re.compile(r'b?r(#*)"')


def literal_end(source: str, i: int) -> int | None:
    """The index just past the string or char literal at `i`, or `None`
    when none starts there (a lifetime, or an identifier)."""
    raw = RAW_STRING.match(source, i)
    if raw:
        terminator = '"' + raw.group(1)
        end = source.find(terminator, raw.end())
        return len(source) if end == -1 else end + len(terminator)
    j = i + 1 if source.startswith(('b"', "b'"), i) else i
    if source.startswith('"', j):
        k = j + 1
        while k < len(source):
            if source[k] == "\\":
                k += 2
            elif source[k] == '"':
                return k + 1
            else:
                k += 1
        return len(source)
    if source.startswith("'", j):
        char = CHAR_LITERAL.match(source, j)
        return char.end() if char else None
    return None


def comment_end(source: str, i: int) -> int | None:
    """The index just past the comment at `i`, or `None`."""
    if source.startswith("//", i):
        newline = source.find("\n", i)
        return len(source) if newline == -1 else newline + 1
    if not source.startswith("/*", i):
        return None
    depth = 0
    while i < len(source):
        if source.startswith("/*", i):
            depth += 1
            i += 2
        elif source.startswith("*/", i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
        else:
            i += 1
    return len(source)


def attribute_end(source: str, i: int) -> int:
    """The index just past the attribute whose `#` is at `i`."""
    depth = 0
    while i < len(source):
        end = literal_end(source, i) if source[i] in "\"'br" else None
        if end is not None:
            i = end
            continue
        if source[i] == "[":
            depth += 1
        elif source[i] == "]":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(source)


def module_declarations(source: str) -> list[tuple[tuple[str, ...], str, bool, str | None]]:
    """Every out-of-line `mod name;` in `source`: the inline modules that
    enclose it, its name, whether `#[cfg(test)]` gates it or one of those
    modules, and its `#[path]`. Comments and literals are skipped."""
    declarations = []
    inline: list[tuple[str, int, bool]] = []  # name, brace depth inside it, gated
    attributes: list[str] = []
    depth = 0
    i = 0
    while i < len(source):
        end = comment_end(source, i)
        if end is None and source[i] in "\"'br":
            end = literal_end(source, i)
        if end is not None:
            i = end
            continue
        if source.startswith("#[", i):
            end = attribute_end(source, i)
            attributes.append(source[i:end])
            i = end
            continue
        word = IDENTIFIER.match(source, i)
        if word is None:
            if source[i] == "{":
                depth += 1
                attributes = []
            elif source[i] == "}":
                if inline and inline[-1][1] == depth:
                    inline.pop()
                depth -= 1
                attributes = []
            elif source[i] == ";":
                attributes = []
            i += 1
            continue
        i = word.end()
        if word.group() != "mod":
            continue
        name = IDENTIFIER.match(source, SPACE.match(source, i).end())
        if name is None:
            continue
        after = SPACE.match(source, name.end()).end()
        gated = any(TEST_GATE_ATTRIBUTE.fullmatch(a) for a in attributes) or any(
            enclosing for _, _, enclosing in inline
        )
        if source.startswith(";", after):
            path = next(
                (match.group(1) for a in attributes if (match := PATH_ATTRIBUTE.fullmatch(a))),
                None,
            )
            declarations.append((tuple(module for module, _, _ in inline), name.group(), gated, path))
        elif source.startswith("{", after):
            depth += 1
            inline.append((name.group(), depth, gated))
        else:
            continue
        attributes = []
        i = after + 1
    return declarations


def declared_file(declaring: Path, inline: tuple[str, ...], name: str, path: str | None, *, target_root: bool = False) -> Path | None:
    """The file a `mod name;` in `declaring`, inside the `inline` modules,
    loads (the Rust reference's module source filenames and `path`
    attribute rules), or `None` when it names no file."""
    mod_rs = target_root or declaring.name in ("mod.rs", "lib.rs", "main.rs") or declaring.parent.name == "bin"
    directory = declaring.parent if mod_rs else declaring.with_suffix("")
    if path is not None:
        base = directory.joinpath(*inline) if inline else declaring.parent
        candidate = Path(os.path.normpath(base / path))
        return candidate if candidate.is_file() else None
    base = directory.joinpath(*inline)
    for candidate in (base / f"{name}.rs", base / name / "mod.rs"):
        if candidate.is_file():
            return candidate
    return None


def test_only_files(root: Path) -> set[Path]:
    """Files under `root` that only `#[cfg(test)]` module declarations bring
    into their crate: test code kept out of line, like an inline test
    module. Each crate root under `root` (`lib.rs`, `main.rs`, `bin/`) is
    walked through its declarations; a file that production declarations
    reach, or that none reaches, is production."""
    crate_roots = [path for path in (root / "lib.rs", root / "main.rs") if path.is_file()]
    crate_roots += sorted((root / "bin").glob("*.rs")) + sorted((root / "bin").glob("*/main.rs"))
    reached = {True: set(), False: set()}
    pending = [(path, True) for path in crate_roots]
    while pending:
        path, production = pending.pop()
        if path in reached[production]:
            continue
        reached[production].add(path)
        for inline, name, gated, attribute in module_declarations(path.read_text(encoding="utf-8")):
            child = declared_file(path, inline, name, attribute)
            if child is not None:
                pending.append((child, production and not gated))
    return reached[False] - reached[True]


def rule_files(rule: dict) -> list[Path]:
    roots = [REPO_ROOT / root for root in rule["roots"]]
    excluded = [REPO_ROOT / root for root in rule.get("exclude_roots", [])]
    test_files: set[Path] = set()
    if rule.get("ignore_inline_tests", True):
        for root in roots:
            test_files |= test_only_files(root)
    extensions = tuple(rule["extensions"])
    files: list[Path] = []
    for root in roots:
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix not in extensions:
                continue
            if path in test_files or any(path.is_relative_to(ex) for ex in excluded):
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


def source_rules(policy: dict, metadata: dict) -> list[dict]:
    """The policy's source rules, a `"definitional"` root naming the
    definitional packages' sources."""
    names = set(policy["definitional_packages"])
    definitional = [
        str((Path(package["manifest_path"]).parent / "src").relative_to(REPO_ROOT))
        for package in metadata["packages"]
        if package["name"] in names
    ]
    return [
        {**rule, "roots": definitional} if rule["roots"] == "definitional" else rule
        for rule in policy["source_rules"]
    ]


def check_source_rules(policy: dict, metadata: dict) -> list[str]:
    failures: list[str] = []
    for rule in source_rules(policy, metadata):
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


def print_grandfather(policy: dict, metadata: dict) -> None:
    for rule in source_rules(policy, metadata):
        print(json.dumps({"id": rule["id"], "grandfathered": sorted(rule_violations(rule))}, indent=1))


# Workspace policy covers the physical layout. ADR-0016's logical imaging
# layers are checked separately by check_layering.
LIBRARY_TIERS = {"casacore", "imaging", "casa"}
TIER_DEPENDENCIES = {
    "casacore": {"casacore"},
    "imaging": LIBRARY_TIERS,
    "casa": LIBRARY_TIERS,
    "surfaces": LIBRARY_TIERS | {"surfaces"},
    "frontends": LIBRARY_TIERS | {"surfaces"},
    "dev": LIBRARY_TIERS | {"surfaces", "dev"},
    "suites": LIBRARY_TIERS | {"surfaces", "frontends", "dev", "suites"},
}
TASK_DOMAINS = {"calibration", "imaging", "images", "ms", "simulation", "vla"}
DOMAIN_FEATURES = {
    "casa-tasks": TASK_DOMAINS,
    "casa-plot-data": {"calibration", "ms", "simulation"},
    "casars-cli": TASK_DOMAINS,
}
RUST_TOKENS = re.compile(r"""//|/\*|b?r#*"|b?["']|(?:r#)?[A-Za-z_][A-Za-z_0-9]*|::|[{}();,=]""")


def package_tier(manifest: Path) -> str:
    parts = manifest.relative_to(REPO_ROOT).parts
    if len(parts) == 4 and parts[0] == "libs" and parts[1] in LIBRARY_TIERS:
        return parts[1]
    if len(parts) == 3 and parts[0] in {"surfaces", "frontends", "dev", "suites"}:
        return parts[0]
    return ""


def ui_dependency(name: str) -> bool:
    return name.split("-", 1)[0].split("_", 1)[0] in {"clap", "pyo3"}


def feature_closure(features: dict, roots: set[str]) -> set[str]:
    enabled = set()
    pending = list(roots & features.keys())
    while pending:
        feature = pending.pop()
        if feature in enabled:
            continue
        enabled.add(feature)
        pending.extend(features.get(feature, []))
    return enabled


def dependency_feature_is_wired(package: dict, dependency: dict) -> bool:
    name = dependency.get("rename") or dependency["name"]
    enabled = feature_closure(package["features"], DOMAIN_FEATURES[package["name"]])
    return dependency["optional"] and any(
        entry == f"dep:{name}" or entry.startswith(f"{name}/") for entry in enabled
    )


def cfg_arguments(expression: str) -> list[str]:
    depth = 0
    quoted = False
    start = 0
    arguments = []
    for index, char in enumerate(expression):
        if char == '"':
            quoted = not quoted
        elif not quoted:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                arguments.append(expression[start:index].strip())
                start = index + 1
    arguments.append(expression[start:].strip())
    return [argument for argument in arguments if argument]


def requires_macos(expression: str) -> bool:
    if re.fullmatch(r'target_os\s*=\s*"macos"', expression):
        return True
    group = re.fullmatch(r"(cfg|all|any)\s*\((.*)\)", expression)
    if group is None:
        return False
    operation, contents = group.groups()
    arguments = cfg_arguments(contents)
    if operation == "cfg":
        return len(arguments) == 1 and requires_macos(arguments[0])
    if operation == "any":
        return bool(arguments) and all(map(requires_macos, arguments))
    return any(map(requires_macos, arguments))


def dependency_rules(package: dict, by_name: dict, definitional: set[str]) -> list[str]:
    name = package["name"]
    tier = package_tier(Path(package["manifest_path"]))
    if not tier:
        return [f"{name} has no workspace tier at {package['manifest_path']}"]
    failures = []
    for dependency in package["dependencies"]:
        target_name = dependency["name"]
        directory = dependency.get("path")
        target = by_name.get(target_name)
        target_tier = package_tier(Path(target["manifest_path"])) if target is not None else ""
        dev = dependency["kind"] == "dev"
        if tier in LIBRARY_TIERS | {"surfaces"} and ui_dependency(target_name):
            failures.append(f"{name} depends on UI framework {target_name}")
        if tier in LIBRARY_TIERS and target_tier in {"surfaces", "frontends"}:
            failures.append(f"{name} has an upward dependency on {target_name}")
        elif not dev and target is not None and target_tier not in TIER_DEPENDENCIES[tier]:
            failures.append(f"{name} ({tier}) may not depend on {target_name} ({target_tier})")
        if not dev and package["publish"] != [] and (directory or target is not None):
            if target is None or target["publish"] == []:
                failures.append(f"publishable {name} depends on private workspace or path crate {target_name}")
            if dependency["req"] == "*":
                failures.append(f"publishable {name} needs a registry version for {target_name}")
        if (
            not dev and name in DOMAIN_FEATURES and target_tier in LIBRARY_TIERS
            and target_name not in definitional and not dependency_feature_is_wired(package, dependency)
        ):
            failures.append(f"{name}'s {target_name} dependency must be optional and enabled by a domain feature")
        if name == "casars-python" and target_name == "casa-aipsio":
            failures.append("casars-python must use the typed libraries and may not depend on casa-aipsio")
        if target_name == "objc2-metal":
            if name != "casa-imaging-metal":
                failures.append(f"{name} depends on objc2-metal outside casa-imaging-metal")
            elif not requires_macos(dependency.get("target") or ""):
                failures.append("objc2-metal must be behind a macOS target dependency")
    if name == "casa-imaging-runtime" and not any(
        d["name"] == "casa-imaging-metal" and d["kind"] is None
        and not d["optional"] and d.get("target") is None for d in package["dependencies"]
    ):
        failures.append("casa-imaging-runtime needs an unconditional casa-imaging-metal dependency")
    return failures


def rust_tokens(source: str):
    """Tokens relevant to the guard, outside comments and literals."""
    position = 0
    while (token := RUST_TOKENS.search(source, position)) is not None:
        start = token.start()
        position = token.end()
        if token[0].startswith("/"):
            position = comment_end(source, start)
        elif '"' in token[0] or "'" in token[0]:
            position = literal_end(source, start) or position
        else:
            yield token[0].removeprefix("r#")


def rust_crate_references(source: str) -> set[str]:
    """Crate paths, including bare use items and explicit extern crates."""
    tokens = list(rust_tokens(source))
    names = set()
    importing = False
    groups = []
    for index, token in enumerate(tokens):
        previous = tokens[index - 1] if index > 0 else ""
        before_previous = tokens[index - 2] if index > 1 else ""
        following = tokens[index + 1] if index + 1 < len(tokens) else ""
        if token == "use":
            importing = True
        elif token == ";":
            importing = False
            groups.clear()
        elif importing and token == "{":
            groups.append(previous not in {"::"} and all(groups))
        elif importing and token == "}":
            if groups:
                groups.pop()
        elif IDENTIFIER.fullmatch(token) and (
            (not importing and following == "::" and previous != "::")
            or (importing and all(groups) and (
                previous in {"use", "{", ","}
                or previous == "::" and before_previous in {"use", "{", ","}
            ))
            or (previous == "crate" and before_previous == "extern")
        ):
            names.add(token)
    return names


def module_sources(entry: Path) -> set[Path]:
    """Rust files reached from an actual Cargo target root."""
    pending = [entry]
    reached = set()
    while pending:
        path = pending.pop()
        if path in reached or not path.is_relative_to(REPO_ROOT):
            continue
        reached.add(path)
        for inline, name, _, attribute in module_declarations(path.read_text(encoding="utf-8")):
            child = declared_file(path, inline, name, attribute, target_root=path == entry)
            if child is not None:
                pending.append(child)
    return reached


def allocator_or_binary_test(entry: Path) -> bool:
    for path in module_sources(entry):
        source = path.read_text(encoding="utf-8")
        uses_allocator = re.search(r"(?m)^\s*#\[\s*global_allocator\s*\]", source)
        if (uses_allocator and "global_allocator" in rust_tokens(source)) or re.search(
            r'\b(?:env|option_env)!\s*\(\s*"CARGO_BIN_EXE_[^"]+"', source
        ):
            return True
    return False


def workspace_rust_files(root: Path, targets: list[dict]) -> set[Path]:
    files = {
        path
        for directory in ("src", "tests", "examples", "benches")
        for path in (root / directory).rglob("*.rs")
        if path.is_file()
    }
    for target in targets:
        files.update(module_sources(Path(target["src_path"])))
    return files


def unused_dependencies(package: dict, files: set[Path]) -> set[str]:
    declared = {d.get("rename") or d["name"] for d in package["dependencies"]}
    if not declared:
        return set()
    sources = {path: path.read_text(encoding="utf-8") for path in files}
    names = set().union(*(rust_crate_references(source) for source in sources.values()))
    missing = {name for name in declared if name.replace("-", "_") not in names}
    if not missing:
        return set()
    # Test targets can share fixture code through path attributes, including
    # fixtures in another workspace crate.
    root = Path(package["manifest_path"]).parent
    helpers = {
        (path.parent / attr[1]).resolve()
        for path, source in sources.items()
        for attr in PATH_ATTRIBUTE.finditer(source)
    }
    external = {
        path for path in helpers if path.is_file() and path.is_relative_to(REPO_ROOT)
        and not path.is_relative_to(root)
    }
    names.update(
        identifier
        for entry in external
        for path in module_sources(entry)
        for identifier in rust_crate_references(path.read_text(encoding="utf-8"))
    )
    return {name for name in missing if name.replace("-", "_") not in names}


def default_api_test(package: dict, target: dict, entry: Path) -> bool:
    return (
        target["name"] == "api"
        and Path(target["src_path"]) == entry
        and target["test"]
        and set(target.get("required-features", [])) <= feature_closure(package["features"], {"default"})
    )


def python_dispatch_errors(source: str) -> list[str]:
    tree = ast.parse(source)
    dispatcher = next(
        (node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_execute_task"),
        None,
    )
    errors = []
    nodes = list(ast.walk(dispatcher)) if dispatcher is not None else []

    def native_handle(node):
        return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_native"

    native_names = {
        target.id for node in nodes if isinstance(node, ast.Assign) and native_handle(node.value)
        for target in node.targets if isinstance(target, ast.Name)
    }
    if not any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run_native_task"
        and (native_handle(node.func.value) or (
            isinstance(node.func.value, ast.Name) and node.func.value.id in native_names
        ))
        for node in nodes
    ):
        errors.append("must call the PyO3 run_native_task entry point")
    process_api = re.compile(r"(?:system|popen|(?:posix_)?spawn\w*|exec[lv]\w*|create_subprocess_\w*)")
    if any(
        isinstance(node, ast.Import) and any(alias.name.split(".")[0] == "subprocess" for alias in node.names)
        or isinstance(node, ast.ImportFrom) and (
            (node.module or "").split(".")[0] == "subprocess"
            or node.module in {"os", "asyncio"} and any(process_api.fullmatch(alias.name) for alias in node.names)
        )
        or isinstance(node, ast.Attribute) and process_api.fullmatch(node.attr)
        or isinstance(node, ast.Constant) and isinstance(node.value, str)
        and re.fullmatch(r"CASARS_\w+_BIN", node.value)
        for node in ast.walk(tree)
    ):
        errors.append("must dispatch tasks in-process without a CLI fallback")
    return errors


def dual_modules(source_paths: list[Path]) -> list[Path]:
    return sorted(path for path in source_paths if path.with_suffix("").is_dir())


def has_missing_docs_lint(source: str) -> bool:
    """Require the crate lint and reject crate-wide suppressions, including cfg_attr."""
    position = 0
    enabled = False
    while match := re.match(r"\s*(//|/\*|#!\[)", source[position:]):
        start = position + match.start(1)
        if match[1] != "#![":
            position = comment_end(source, start)
            continue
        position = attribute_end(source, start + 2)
        tokens = list(rust_tokens(source[start:position]))
        if tokens == ["warn", "(", "missing_docs", ")"]:
            enabled = True
        for lints in re.findall(r"\b(?:allow|expect)\s*\(([^()]*)\)", " ".join(tokens)):
            if {"missing_docs", "warnings"} & {lint.strip() for lint in lints.split(",")}:
                return False
    return enabled


def workspace_dependency_graph(packages: list[dict]) -> dict[str, set[str]]:
    names = {package["name"] for package in packages}
    return {
        package["name"]: {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] != "dev" and dependency["name"] in names
        }
        for package in packages
    }


def check_workspace(policy: dict, metadata: dict) -> list[str]:
    packages = metadata["packages"]
    by_name = {package["name"]: package for package in packages}
    exceptions = set(policy.get("test_target_exceptions", []))
    seen_exceptions = set()
    failures = []
    try:
        tuple(TopologicalSorter(workspace_dependency_graph(packages)).static_order())
    except CycleError as error:
        failures.append("workspace dependency cycle: " + " -> ".join(error.args[1]))
    for package in packages:
        name = package["name"]
        manifest = Path(package["manifest_path"])
        root = manifest.parent
        tier = package_tier(manifest)
        failures += dependency_rules(package, by_name, set(policy["definitional_packages"]))
        source_paths = workspace_rust_files(root, package["targets"])
        failures.extend(
            f"dual Rust module: move {path.relative_to(REPO_ROOT)} into its matching directory"
            for path in dual_modules(source_paths)
        )
        if tier in LIBRARY_TIERS | {"surfaces"}:
            libraries = [Path(target["src_path"]) for target in package["targets"] if "lib" in target["kind"]]
            if not libraries or not all(has_missing_docs_lint(path.read_text(encoding="utf-8")) for path in libraries):
                failures.append(f"{name} must enable #![warn(missing_docs)] at its library root without a crate-wide suppression")
            if any("bin" in target["kind"] for target in package["targets"]) or any(
                (root / path).exists() for path in ("src/main.rs", "src/bin")
            ):
                failures.append(f"{name} must not ship binary targets below frontends")
        if tier in LIBRARY_TIERS:
            if tomllib.loads(manifest.read_text(encoding="utf-8"))["package"].get("publish") is not True:
                failures.append(f"{name} must explicitly declare publish = true")
            if "cpp-interop-tests" in package["features"]:
                failures.append(f"{name} must move C++ interop to suites/casacore-interop")
            tests = [target for target in package["targets"] if "test" in target["kind"]]
            if not any(default_api_test(package, target, root / "tests/api.rs") for target in tests):
                failures.append(f"{name} needs its default tests/api.rs target")
            for target in tests:
                if target["name"] == "api":
                    continue
                entry = Path(target["src_path"])
                relative = entry.relative_to(REPO_ROOT).as_posix()
                if relative in exceptions and allocator_or_binary_test(entry):
                    seen_exceptions.add(relative)
                else:
                    failures.append(f"{relative} must use tests/api.rs or an approved isolated target")
        for target in package["targets"]:
            if "custom-build" in target["kind"]:
                path = Path(target["src_path"])
                if tier in LIBRARY_TIERS and re.search(r'\bserde_json\b|\.json["\x27]', path.read_text(encoding="utf-8")):
                    failures.append(f"{name} library build script must not contain JSON probes")
        if name == "casars-python":
            native_tasks = root / "src/native_tasks.rs"
            runner = root / "python/casars/tasks/_runner.py"
            if not any(d["name"] == "casa-tasks" and d["kind"] is None for d in package["dependencies"]) or not (
                native_tasks.is_file()
                and re.search(r"\brun_task_in_process\s*\(", native_tasks.read_text(encoding="utf-8"))
            ):
                failures.append("casars-python must dispatch native tasks through casa-tasks")
            for file in (native_tasks, root / "src/tasks.rs"):
                if file.is_file() and re.search(r"\b(?:subprocess|Command::new|CASARS_\w+_BIN)\b", file.read_text(encoding="utf-8")):
                    failures.append(f"{file.relative_to(REPO_ROOT)} must dispatch tasks in-process")
            if runner.is_file():
                failures.extend(f"{runner.relative_to(REPO_ROOT)} {e}" for e in python_dispatch_errors(runner.read_text(encoding="utf-8")))
            else:
                failures.append("casars-python needs its native Python task runner")
        failures.extend(f"{name} declares unused dependency {d}" for d in sorted(unused_dependencies(package, source_paths)))
    failures.extend(f"stale test target exception: {p}" for p in sorted(exceptions - seen_exceptions))
    return failures


def check_workspace_self_test() -> None:
    assert has_missing_docs_lint('//! Library docs\n#![doc = "API"]\n#![warn(missing_docs)]')
    assert not has_missing_docs_lint('/*\n#![warn(missing_docs)]\n*/\npub struct A;')
    assert not has_missing_docs_lint('pub mod nested {\n#![warn(missing_docs)]\n}')
    assert not has_missing_docs_lint('// #![warn(missing_docs)]\n#![allow(missing_docs)]')
    assert not has_missing_docs_lint('#![warn(missing_docs)]\n#![allow(missing_docs)]')
    assert not has_missing_docs_lint('#![warn(missing_docs)]\n#![cfg_attr(unix, allow(dead_code, warnings))]')
    assert not has_missing_docs_lint('#![warn(missing_docs)]\n#![expect(missing_docs, reason = "audit")]')
    assert not has_missing_docs_lint('#![warn(missing_docs)]\n#![allow(/* audit */ missing_docs, reason = "text (here)")]')
    assert has_missing_docs_lint('#![warn(missing_docs)]\n#![doc = "allow(missing_docs)"]')

    def package(name, manifest, **changes):
        return {"name": name, "manifest_path": str(REPO_ROOT / manifest),
                "publish": None, "dependencies": [], "features": {}, **changes}

    top = package("app", "surfaces/app/Cargo.toml", **{"publish": []})
    low = package("lib", "libs/casacore/lib/Cargo.toml")
    low["dependencies"] = [{
        "name": "app", "path": str(REPO_ROOT / "surfaces/app"),
        "kind": "build", "req": "*", "optional": False,
    }]
    errors = dependency_rules(low, {"app": top}, set())
    assert any("upward dependency" in e for e in errors), errors
    assert any("registry version" in e for e in errors), errors
    assert any("private workspace or path crate" in e for e in errors), errors
    high = package("high", "libs/casa/high/Cargo.toml")
    low["dependencies"] = [{"name": "high", "kind": None, "req": "1", "optional": False}]
    high["dependencies"] = [{"name": "lib", "kind": "build", "req": "1", "optional": False}]
    assert any("may not depend" in e for e in dependency_rules(low, {"high": high}, set()))
    try:
        tuple(TopologicalSorter(workspace_dependency_graph([low, high])).static_order())
    except CycleError:
        pass
    else:
        raise AssertionError("registry/git dependencies must participate in the workspace DAG")
    assert rust_crate_references(
        '/* other::Call */ "absent::Call"; let bytes = 5; use used::Call; '
        'use {bare as alias, ::direct}; extern crate named as renamed; use std::{nested::Path, internal};'
    ) == {"used", "bare", "direct", "named", "std"}

    metal = package("casa-imaging-runtime", "libs/imaging/casa-imaging-runtime/Cargo.toml")
    metal["dependencies"] = [{
        "name": "casa-imaging-metal", "kind": None, "optional": False,
        "target": 'cfg(target_os = "macos")',
    }]
    assert "unconditional" in dependency_rules(metal, {}, set())[0]
    metal["dependencies"][0]["target"] = None
    assert not dependency_rules(metal, {}, set())
    assert requires_macos('cfg(all(target_os = "macos", not(coverage)))')
    assert not requires_macos('cfg(not(target_os = "macos"))')
    assert not requires_macos('cfg(any(target_os = "macos", target_os = "windows"))')

    lib_directory = str(REPO_ROOT / "libs/casacore/lib")
    adapter = package("casa-tasks", "surfaces/casa-tasks/Cargo.toml")
    dep = {"name": "lib", "path": lib_directory, "kind": None, "req": "1", "optional": True}
    adapter["dependencies"] = [dep]
    assert any("domain feature" in e for e in dependency_rules(adapter, {"lib": low}, set()))
    adapter["features"] = {"slow-tests": ["dep:lib"], "calibration": ["lib?/serde"]}
    assert any("domain feature" in e for e in dependency_rules(adapter, {"lib": low}, set()))
    adapter["features"] = {"calibration": ["logging"], "logging": ["dep:lib"]}
    assert not dependency_rules(adapter, {"lib": low}, set())

    entry = REPO_ROOT / "libs/casacore/lib/tests/api.rs"
    target = {"name": "api", "src_path": str(entry), "test": True}
    assert default_api_test(low, target, entry)
    assert not default_api_test(low, {**target, "test": False}, entry)
    assert not default_api_test(low, {**target, "required-features": ["slow-tests"]}, entry)

    native = "def _execute_task():\n    return _native().run_native_task()\n"
    assert not python_dispatch_errors(native)
    assert python_dispatch_errors("def _execute_task():\n    return 0\n")
    assert python_dispatch_errors(native + "import subprocess as sp\n")
    assert python_dispatch_errors(native + "from os import system as run\nrun('cli')\n")
    with tempfile.TemporaryDirectory() as directory:
        module = Path(directory) / "module.rs"
        module.touch()
        assert dual_modules([module]) == []
        module.with_suffix("").mkdir()
        assert dual_modules([module]) == [module]
        entry = module.with_suffix("") / "mod.rs"
        module.rename(entry)
        assert dual_modules([entry]) == []
    with tempfile.TemporaryDirectory(dir=REPO_ROOT / "target" if (REPO_ROOT / "target").is_dir() else REPO_ROOT) as directory:
        root = Path(directory)
        build = root / "build.rs"
        module = root / "codegen" / "steps.rs"
        module.with_suffix("").mkdir(parents=True)
        module.touch()
        build.write_text('#[path = "codegen/steps.rs"] mod steps;\nfn main() {}\n', encoding="utf-8")
        assert dual_modules(workspace_rust_files(root, [{"src_path": str(build)}])) == [module]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grandfather", action="store_true", help="print current violations in policy form")
    parser.add_argument("--self-test", action="store_true", help="check the test-stripping and layering logic")
    args = parser.parse_args()
    if args.self_test:
        check_workspace_self_test()
        return self_test()
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    metadata = cargo_metadata()
    if args.grandfather:
        print_grandfather(policy, metadata)
        return 0
    failures = check_workspace(policy, metadata) + check_layering(policy, metadata) + check_source_rules(policy, metadata)
    for failure in failures:
        print(f"workspace-arch: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("workspace-arch: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
