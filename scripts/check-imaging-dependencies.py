#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Enforce the ADR-0016 imaging crate layering and source rules.

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
import json
import os
import re
import subprocess
import sys
import tempfile
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
            print(f"imaging-dependencies self-test failed: {name}", file=sys.stderr)
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
                    f"imaging-dependencies self-test failed: {name}: {sorted(found)}",
                    file=sys.stderr,
                )
    for name, changes, packages, expected in LAYERING_CASES:
        found = check_layering(
            {**LAYERING_POLICY, **changes},
            {"packages": [layering_package(*package) for package in packages]},
        )
        if len(found) != expected:
            failures += 1
            print(f"imaging-dependencies self-test failed: {name}: {found}", file=sys.stderr)
    if failures == 0:
        print("imaging-dependencies: self-test ok")
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


def declared_file(declaring: Path, inline: tuple[str, ...], name: str, path: str | None) -> Path | None:
    """The file a `mod name;` in `declaring`, inside the `inline` modules,
    loads (the Rust reference's module source filenames and `path`
    attribute rules), or `None` when it names no file."""
    mod_rs = declaring.name in ("mod.rs", "lib.rs", "main.rs") or declaring.parent.name == "bin"
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


def source_rules(policy: dict) -> list[dict]:
    """The policy's source rules, a `"definitional"` root naming the
    definitional packages' sources."""
    definitional = [f"crates/{name}/src" for name in policy["definitional_packages"]]
    return [
        {**rule, "roots": definitional} if rule["roots"] == "definitional" else rule
        for rule in policy["source_rules"]
    ]


def check_source_rules(policy: dict) -> list[str]:
    failures: list[str] = []
    for rule in source_rules(policy):
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
    for rule in source_rules(policy):
        print(json.dumps({"id": rule["id"], "grandfathered": sorted(rule_violations(rule))}, indent=1))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grandfather", action="store_true", help="print current violations in policy form")
    parser.add_argument("--self-test", action="store_true", help="check the test-stripping and layering logic")
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
