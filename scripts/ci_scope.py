#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Select CI checks for the checked-out PR merge commit."""

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import subprocess


PYTHON_INTERFACE = {"casa-provider-contracts", "casa-task-runtime", "casars-python"}
ROOT_DOCS = {
    "AGENTS.md", "ARCHITECTURE.md", "CLAUDE.md", "GLOSSARY.md", "README.md",
    "TESTING.md",
}
PERF_PATHS = (
    "scripts/tutorial_parity/", "tools/atmosphere/", "tools/perf/imager/",
    "tools/simulation/",
)


@dataclass(frozen=True)
class Scope:
    rust: str = "static"
    packages: str = ""
    python: str = "none"
    docs: bool = False
    perf: bool = False


def full_scope(*, draft=False):
    if draft:
        return Scope(docs=True)
    return Scope(rust="full", python="full", docs=True, perf=True)


def command(*args):
    return subprocess.check_output(args)


def changed_paths(base):
    # GitHub checks out the test merge commit on pull_request. Compare its
    # first parent so the selection describes the exact tree the jobs test.
    parents = command("git", "show", "-s", "--format=%P", "HEAD").decode().split()
    if len(parents) != 2 or parents[0] != base:
        raise ValueError("checkout is not the expected PR merge commit")
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


def pr_scope(paths, *, draft=False):
    if not paths:
        return full_scope(draft=draft)

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
        elif parts[:3] == ["crates", "casars-python", "python"]:
            python = True
        elif (
            len(parts) >= 4 and parts[0] == "crates"
            and parts[2] in {"src", "resources", "tests", "examples", "benches"}
        ):
            cargo_paths.append(("/".join(parts[:2]), parts[2]))
        else:
            return full_scope(draft=draft)

    sources, tests = set(), {"casars-python"} if python and not draft else set()
    if cargo_paths:
        owners, dependencies = workspace_packages()
        for directory, area in cargo_paths:
            name = owners[directory]
            if area in {"src", "resources"}:
                sources.add(name)
            else:
                tests.add(name)
        if not draft:
            tests.update(name for name, inputs in dependencies.items() if inputs & sources)
        python |= bool(sources & PYTHON_INTERFACE)
        tests.update(sources)

    return Scope(
        rust="affected" if tests else "static",
        packages=" ".join(sorted(tests)),
        python="editable" if python and not draft else "none",
        docs=prose or python,
        perf=perf,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="first parent of the PR test merge commit")
    parser.add_argument("--draft", action="store_true")
    args = parser.parse_args()
    if args.base is None:
        scope = full_scope()
    else:
        try:
            scope = pr_scope(changed_paths(args.base), draft=args.draft)
        except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
            print(f"Unable to scope the PR change, using conservative coverage: {error}")
            scope = full_scope(draft=args.draft)

    print(json.dumps(asdict(scope), indent=2))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as stream:
            for name, value in asdict(scope).items():
                rendered = str(value).lower() if isinstance(value, bool) else value
                print(f"{name}={rendered}", file=stream)


if __name__ == "__main__":
    main()
