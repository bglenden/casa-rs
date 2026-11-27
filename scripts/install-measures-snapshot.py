#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Verify and install the source release measures snapshot for development and tests."""

import argparse
import hashlib
import io
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "libs/casacore/casa-measures/data"
ARCHIVE = DATA / "casa-measures-runtime.tar.gz"
MANIFEST = DATA / "casa-measures-runtime.provenance.json"
PROVENANCE_NAME = ".casa-rs-measures-provenance.json"


def install(archive_path, manifest_path, destination, *, reuse=False):
    """Verify the exact bytes that will be extracted and return scientific provenance."""
    provenance = json.loads(manifest_path.read_text(encoding="utf-8"))
    size = provenance.pop("archive_size_bytes")
    digest = provenance.pop("archive_sha256")
    with archive_path.open("rb") as archive:
        contents = archive.read(size + 1)
    if len(contents) != size or hashlib.sha256(contents).hexdigest() != digest:
        raise ValueError(f"{archive_path} does not match the release snapshot size and SHA-256")

    provenance_path = destination / PROVENANCE_NAME
    if reuse and provenance_path.is_file():
        try:
            installed = json.loads(provenance_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            installed = None
        if installed == provenance and all(
            (destination / name).exists() for name in provenance["included_paths"]
        ):
            return provenance

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".measures-install-") as directory:
        stage = Path(directory) / "snapshot"
        stage.mkdir()
        with tarfile.open(fileobj=io.BytesIO(contents), mode="r:gz") as archive:
            archive.extractall(stage, filter="data")
        if not all((stage / name).exists() for name in provenance["included_paths"]):
            raise ValueError(f"{archive_path} is missing files listed in the release manifest")
        (stage / PROVENANCE_NAME).write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")

        if reuse and destination.exists():
            # Stage the complete replacement before updating the separate cloud cache.
            # Leave the previous tree available outside the staging directory if a
            # filesystem failure prevents restoring it.
            previous = Path(tempfile.mkdtemp(dir=destination.parent, prefix=".measures-previous-"))
            backup = previous / "snapshot"
            try:
                destination.rename(backup)
                try:
                    stage.rename(destination)
                except OSError:
                    backup.rename(destination)
                    raise
                shutil.rmtree(backup)
            finally:
                if not backup.exists():
                    previous.rmdir()
        else:
            stage.rename(destination)
    return provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--reuse", action="store_true", help="Reuse an installed development cache")
    args = parser.parse_args()
    try:
        install(ARCHIVE, MANIFEST, args.destination, reuse=args.reuse)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, f"install-measures-snapshot: {error}\n")


if __name__ == "__main__":
    main()
