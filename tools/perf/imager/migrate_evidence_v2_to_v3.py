#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""One-time strict migration of checked-in imaging evidence from v2 to v3."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import tempfile
from typing import Any

from perf_harness import (
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    load_json_object,
    validate_run_result,
)
from perf_harness.schema import (
    LEGACY_RUN_RESULT_SCHEMA_VERSION,
    validate_legacy_run_result_v2,
)


DEFAULT_MANIFEST = (
    pathlib.Path(__file__).resolve().parent
    / "evidence"
    / "imaging_performance_evidence_manifest.json"
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=pathlib.Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    migrate_manifest(args.manifest.resolve())


def migrate_manifest(manifest_path: pathlib.Path) -> None:
    manifest = load_json_object(manifest_path, description="evidence manifest")
    if manifest.get("schema_version") != 1:
        raise ValueError("evidence manifest schema_version must be 1")
    entries = manifest.get("artifacts")
    if not isinstance(entries, list) or not entries:
        raise ValueError("evidence manifest artifacts must be a non-empty list")

    imager_root = manifest_path.parent.parent.resolve()
    migrations: list[tuple[pathlib.Path, bytes, dict[str, Any], str]] = []
    observed_paths: set[pathlib.Path] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("evidence manifest artifact must be an object")
        path = artifact_path(imager_root, required_string(entry, "checked_in_path"))
        if path in observed_paths:
            raise ValueError(f"duplicate checked-in evidence artifact path: {path}")
        observed_paths.add(path)
        expected_source_sha256 = required_sha256(entry, "sha256")
        source_bytes = path.read_bytes()
        observed_source_sha256 = hashlib.sha256(source_bytes).hexdigest()
        if observed_source_sha256 != expected_source_sha256:
            raise ValueError(
                f"{path}: manifest sha256 does not match the source artifact; "
                "refusing to bless modified evidence"
            )
        value = load_json_object(path, description="run-result evidence artifact")
        version = value.get("schema_version")
        if version == RUN_RESULT_SCHEMA_VERSION:
            validate_run_result(value, source=str(path))
            target_sha256 = observed_source_sha256
        elif version == LEGACY_RUN_RESULT_SCHEMA_VERSION:
            validate_legacy_run_result_v2(value, source=str(path))
            migrated = dict(value)
            migrated["schema_version"] = RUN_RESULT_SCHEMA_VERSION
            validate_run_result(migrated, source=str(path))
            target_sha256 = hashlib.sha256(canonical_json_bytes(migrated)).hexdigest()
            migrations.append((path, source_bytes, migrated, target_sha256))
        else:
            raise ValueError(
                f"{path}: expected run-result schema "
                f"{LEGACY_RUN_RESULT_SCHEMA_VERSION} or {RUN_RESULT_SCHEMA_VERSION}"
            )
        entry["sha256"] = target_sha256

    written: list[tuple[pathlib.Path, bytes]] = []
    try:
        for path, source_bytes, migrated, target_sha256 in migrations:
            atomic_write_json(path, migrated)
            written.append((path, source_bytes))
            if sha256(path) != target_sha256:
                raise OSError(
                    f"post-write sha256 mismatch for migrated artifact {path}"
                )
        atomic_write_json(manifest_path, manifest)
    except BaseException as error:
        rollback_errors: list[str] = []
        for path, source_bytes in reversed(written):
            try:
                atomic_replace_bytes(path, source_bytes)
            except BaseException as rollback_error:
                rollback_errors.append(f"{path}: {rollback_error}")
        if rollback_errors:
            raise RuntimeError(
                "evidence migration failed and rollback was incomplete: "
                + "; ".join(rollback_errors)
            ) from error
        raise


def artifact_path(imager_root: pathlib.Path, relative: str) -> pathlib.Path:
    candidate = pathlib.Path(relative)
    if candidate.is_absolute():
        raise ValueError("checked_in_path must be relative to tools/perf/imager")
    resolved = (imager_root / candidate).resolve()
    if not resolved.is_relative_to(imager_root):
        raise ValueError("checked_in_path must remain beneath tools/perf/imager")
    if not resolved.is_file():
        raise ValueError(f"checked-in evidence artifact is missing: {resolved}")
    return resolved


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode(
        "utf-8"
    )


def atomic_replace_bytes(path: pathlib.Path, value: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".rollback", dir=path.parent
    )
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def required_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def required_sha256(value: dict[str, Any], key: str) -> str:
    item = required_string(value, key)
    if len(item) != 64 or any(
        character not in "0123456789abcdef" for character in item
    ):
        raise ValueError(f"{key} must be a lowercase SHA-256 digest")
    return item


if __name__ == "__main__":
    main()
