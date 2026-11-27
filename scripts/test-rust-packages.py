#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Build a standalone Cargo consumer of the packaged libraries.

Supply the directory of .crate files from a single cargo package run.
Optional crate names select direct dependencies for a test shard.
"""

import argparse
import json
import os
import re
import subprocess
import tarfile
import tempfile
import tomllib
from pathlib import Path


def cargo(directory, *args):
    return subprocess.run(
        ["cargo", *args], cwd=directory, check=True, text=True,
        capture_output=args[0] == "metadata", env=dict(os.environ, CARGO_INCREMENTAL="0"),
    ).stdout


def unpack(directory, destination):
    packages = {}
    for path in sorted(directory.glob("*.crate")):
        with tarfile.open(path, mode="r:gz") as archive:
            archive.extractall(destination, filter="data")
        manifest = destination / path.stem / "Cargo.toml"
        package = tomllib.loads(manifest.read_text(encoding="utf-8"))["package"]
        name = package["name"]
        if name in packages:
            raise ValueError(f"multiple archives for {name}")
        packages[name] = (package["version"], manifest)
    if not packages:
        raise ValueError(f"no .crate files in {directory}")
    return packages


def build_consumer(root, directory, requested):
    packages = unpack(directory, root / "archives")
    selected = requested or sorted(packages)
    unknown = set(selected) - packages.keys()
    if unknown:
        raise ValueError(f"missing archives: {', '.join(sorted(unknown))}")

    # Put the normalized registry manifests outside the source checkout.
    # Local replacements supply the not-yet-published versions without
    # restoring workspace-only dependency paths or feature unification.
    manifest = ['[package]', 'name = "casa-packaged-consumer"', 'version = "0.1.0"',
                'edition = "2024"', 'publish = false', '', '[workspace]', '', '[dependencies]']
    manifest.extend(f'{name} = "={packages[name][0]}"' for name in selected)
    manifest.extend(['', '[patch.crates-io]'])
    manifest.extend(
        f'{name} = {{ path = {json.dumps(str(path.parent))} }}'
        for name, (_, path) in packages.items()
    )
    (root / "Cargo.toml").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    (root / "src").mkdir()
    imports = [f"use {name.replace('-', '_')} as _;" for name in selected]
    (root / "src/main.rs").write_text("\n".join([*imports, "", "fn main() {}"]) + "\n", encoding="utf-8")

    host = subprocess.check_output(["rustc", "-vV"], text=True)
    host = next(line.removeprefix("host: ") for line in host.splitlines() if line.startswith("host: "))
    metadata = json.loads(cargo(root, "metadata", "--format-version", "1", "--filter-platform", host))
    resolved = set()
    for package in metadata["packages"]:
        name = package["name"]
        if name in packages or name != "casa-packaged-consumer" and re.match(r"^casa(?:rs)?-", name):
            registered = packages.get(name)
            if registered is None or package["version"] != registered[0] or Path(
                package["manifest_path"]
            ).resolve() != registered[1].resolve():
                raise ValueError(f"{name} resolved outside the packaged archive set")
            resolved.add(name)
    if not set(selected) <= resolved:
        raise ValueError("consumer resolution omitted selected archives")
    cargo(root, "build", "--locked")
    print(f"Built {len(selected)} direct crate dependencies from archives for {host}: {', '.join(selected)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", type=Path)
    parser.add_argument("crates", nargs="*")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="casa-packaged-consumer-") as directory:
        try:
            build_consumer(Path(directory), args.archives.resolve(), args.crates)
        except subprocess.CalledProcessError as error:
            if error.stderr:
                print(error.stderr, end="")
            parser.exit(1, f"test-rust-packages: Cargo failed ({error.returncode})\n")
        except (OSError, ValueError, tarfile.TarError) as error:
            parser.exit(1, f"test-rust-packages: {error}\n")


if __name__ == "__main__":
    main()
