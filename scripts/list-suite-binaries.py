#!/usr/bin/env python3
"""List package/binary entries from the canonical application catalog."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys


def load_catalog() -> list[dict[str, object]]:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    catalog_path = (
        repo_root
        / "crates"
        / "casa-provider-contracts"
        / "resources"
        / "application-catalog.json"
    )
    with catalog_path.open("r", encoding="utf-8") as handle:
        catalog = json.load(handle)
    return list(catalog["applications"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package-binary",
        action="store_true",
        help="print '<cargo-package> <binary-name>' pairs instead of binary names",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="include entries that are not part of the suite bundle",
    )
    args = parser.parse_args()

    applications = load_catalog()
    seen: set[str] = set()
    for application in applications:
        if not args.all and not application.get("include_in_suite", False):
            continue
        launch = application["launch"]
        binary = str(launch["executable"])
        if binary in seen:
            continue
        seen.add(binary)
        if args.package_binary:
            print(f"{launch['cargo_package']} {binary}")
        else:
            print(binary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
