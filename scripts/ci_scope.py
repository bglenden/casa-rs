#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Select hosted checks for a revision and its reviewed merge base."""

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import subprocess


PYTHON_INTERFACE = {
    "casa-contracts",
    "casa-tasks",
    "casars-cli",
    "casars-python",
}
ROOT_DOCS = {
    "AGENTS.md", "ARCHITECTURE.md", "CLAUDE.md", "GLOSSARY.md", "README.md",
    "TESTING.md",
}
PERF_PATHS = (
    "scripts/tutorial_parity/", "tools/atmosphere/", "suites/perf/imager/",
    "tools/simulation/",
)
CRATE_AREAS = {"src", "resources", "tests", "examples", "benches"}
TWO_SEGMENT_ROOTS = {"crates", "surfaces", "frontends", "dev", "suites"}


@dataclass(frozen=True)
class Scope:
    rust: str = "static"
    packages: str = ""
    python: str = "none"
    docs: bool = False
    perf: bool = False


def full_scope():
    return Scope(rust="full", python="full", docs=True, perf=True)


def command(*args):
    return subprocess.check_output(args)


def changed_paths(base):
    if not re.fullmatch(r"[a-f0-9]{40}", base):
        raise ValueError("affected checks require a full base commit SHA")
    if base == command("git", "rev-parse", "HEAD").decode().strip():
        raise ValueError("the base must precede the revision under test")
    command("git", "merge-base", "--is-ancestor", base, "HEAD")
    names = command("git", "diff", "--name-only", "--no-renames", "-z", base, "HEAD", "--")
    return [os.fsdecode(name) for name in names.split(b"\0") if name]


def workspace_packages():
    metadata = json.loads(command("cargo", "metadata", "--locked", "--no-deps", "--format-version=1"))
    root = Path(metadata["workspace_root"])
    packages = {
        Path(package["manifest_path"]).parent: package
        for package in metadata["packages"]
        if package["id"] in metadata["workspace_members"]
    }
    owners = {}
    dependencies = {}
    for directory, package in packages.items():
        name = package["name"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError(f"unexpected Cargo package name {name!r}")
        owners[directory.relative_to(root).as_posix()] = name
        dependencies[name] = set()
        for dependency in package["dependencies"]:
            if path := dependency.get("path"):
                target = packages[Path(path)]
                dependencies[name].add(target["name"])
    return owners, dependencies


def affected_scope(paths):
    if not paths:
        return full_scope()

    cargo_paths = []
    prose = python = perf = False
    for path in paths:
        parts = path.split("/")
        if path in ROOT_DOCS or (path.startswith("docs/") and path.endswith(".md")):
            prose = True
        elif path.startswith(PERF_PATHS):
            perf = True
        elif path.startswith(("resources/casa-compat/", "tools/casa-compat/")):
            # The architecture check verifies compatibility references and pins.
            continue
        elif (
            parts[:3] == ["crates", "casars-python", "python"]
            or parts[:3] == ["frontends", "casars-python", "python"]
        ):
            python = True
        elif len(parts) >= 4 and parts[0] in TWO_SEGMENT_ROOTS and parts[2] in CRATE_AREAS:
            cargo_paths.append(("/".join(parts[:2]), parts[2]))
        elif len(parts) >= 5 and parts[0] == "libs" and parts[3] in CRATE_AREAS:
            cargo_paths.append(("/".join(parts[:3]), parts[3]))
        else:
            return full_scope()

    sources, tests = set(), {"casars-python"} if python else set()
    if cargo_paths:
        owners, dependencies = workspace_packages()
        for directory, area in cargo_paths:
            name = owners[directory]
            if area in {"src", "resources"}:
                sources.add(name)
            else:
                tests.add(name)
        tests.update(name for name, inputs in dependencies.items() if inputs & sources)
        python |= bool(sources & PYTHON_INTERFACE)
        tests.update(sources)

    return Scope(
        rust="affected" if tests else "static",
        packages=" ".join(sorted(tests)),
        python="editable" if python else "none",
        docs=prose or python,
        perf=perf,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--base", help="full SHA of the reviewed merge base")
    selection.add_argument("--static", action="store_true", help="format, architecture and docs checks")
    args = parser.parse_args()
    if args.static:
        scope = Scope(docs=True)
    elif args.base is None:
        scope = full_scope()
    else:
        try:
            paths = changed_paths(args.base)
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            parser.error(f"could not compare the selected base to HEAD: {error}")
        try:
            scope = affected_scope(paths)
        except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
            print(f"Unable to scope the change, using full coverage: {error}")
            scope = full_scope()

    print(json.dumps(asdict(scope), indent=2))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as stream:
            for name, value in asdict(scope).items():
                rendered = str(value).lower() if isinstance(value, bool) else value
                print(f"{name}={rendered}", file=stream)


if __name__ == "__main__":
    main()
