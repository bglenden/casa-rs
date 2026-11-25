#!/usr/bin/env python3
"""Verify and atomically stage the frozen VLASS fragment archive.

This is intentionally a thin dataset-staging utility.  Imaging remains owned by
the canonical manifest runner and CASA protocol under ``perf_harness``.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import pathlib
import shutil
import sys
import tarfile
import time
import uuid
from typing import Any, Iterable

from perf_harness import atomic_write_json
from perf_harness.tree_identity import (
    TreeIdentityError,
    sha256_file as shared_sha256_file,
    tree_identity as shared_tree_identity,
)


ARCHIVE_SHA256 = "b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a"
RECIPE_SHA256 = "a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5"
MS_MEMBER = (
    "VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
)
RECIPE_MEMBER = "tclean.last"
DEFAULT_ARCHIVE = pathlib.Path("/Volumes/GLENDENNING/vlass_test.tgz")
DEFAULT_ROOT = pathlib.Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446")
MINIMUM_FREE_BYTES = 1 << 40
READ_BLOCK_BYTES = 8 * 1024 * 1024
DATASET_RECEIPT_KIND = "frozen_archive_dataset_receipt"


class StagingError(ValueError):
    """The frozen archive or requested staging destination is unsafe."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=pathlib.Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--root", type=pathlib.Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--minimum-free-bytes",
        type=int,
        default=MINIMUM_FREE_BYTES,
        help="stop before extraction when the destination has less free space",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        receipt_path = stage_archive(
            archive=args.archive,
            root=args.root,
            minimum_free_bytes=args.minimum_free_bytes,
        )
    except (OSError, StagingError, tarfile.TarError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    print(receipt_path)


def stage_archive(
    *, archive: pathlib.Path, root: pathlib.Path, minimum_free_bytes: int
) -> pathlib.Path:
    minimum_free_bytes = validated_minimum_free_bytes(minimum_free_bytes)
    archive = archive.expanduser().resolve()
    root = root.expanduser().resolve()
    if not archive.is_file():
        raise StagingError(f"archive does not exist: {archive}")
    _require_external_issue_root(root)
    root.mkdir(parents=True, exist_ok=True)
    _require_same_device(archive, root)
    free_bytes = shutil.disk_usage(root).free
    if free_bytes < minimum_free_bytes:
        raise StagingError(
            f"destination has {free_bytes} free bytes; requires at least "
            f"{minimum_free_bytes}"
        )

    receipt_path = root / "receipts" / f"dataset-{ARCHIVE_SHA256}.json"
    final_root = root / "data" / ARCHIVE_SHA256
    final_ms = final_root / MS_MEMBER
    final_recipe = final_root / RECIPE_MEMBER
    if final_root.exists():
        _validate_existing_stage(
            receipt_path,
            final_ms,
            final_recipe,
            archive_path=archive,
            root=root,
        )
        return receipt_path

    started = time.monotonic()
    archive_digest = sha256_file(archive)
    if archive_digest != ARCHIVE_SHA256:
        raise StagingError(
            f"archive SHA-256 mismatch: expected {ARCHIVE_SHA256}, got {archive_digest}"
        )
    verify_gzip_integrity(archive)

    staging_root = root / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    partial = staging_root / f"extract-{uuid.uuid4().hex}.partial"
    partial.mkdir()
    try:
        extract_frozen_members(archive, partial)
        partial_ms = partial / MS_MEMBER
        partial_recipe = partial / RECIPE_MEMBER
        if not partial_ms.is_dir() or not partial_recipe.is_file():
            raise StagingError("archive did not produce the frozen MS and tclean.last")
        recipe_digest = sha256_file(partial_recipe)
        if recipe_digest != RECIPE_SHA256:
            raise StagingError(
                f"tclean.last SHA-256 mismatch: expected {RECIPE_SHA256}, "
                f"got {recipe_digest}"
            )
        ms_identity = tree_identity(partial_ms)
        final_root.parent.mkdir(parents=True, exist_ok=True)
        os.replace(partial, final_root)
    except BaseException:
        if partial.exists():
            shutil.rmtree(partial)
        raise

    receipt: dict[str, Any] = {
        "schema_version": 1,
        "kind": DATASET_RECEIPT_KIND,
        "archive": {
            "path": str(archive),
            "size_bytes": archive.stat().st_size,
            "sha256": archive_digest,
            "gzip_integrity": "verified",
        },
        "dataset": {
            "path": str(final_ms),
            **ms_identity,
        },
        "recipe": {
            "path": str(final_recipe),
            "size_bytes": final_recipe.stat().st_size,
            "sha256": recipe_digest,
        },
        "storage": {
            "root": str(root),
            "device": os.stat(root).st_dev,
            "free_bytes_before": free_bytes,
            "same_device_atomic_promotion": True,
        },
        "elapsed_seconds": time.monotonic() - started,
    }
    atomic_write_json(receipt_path, receipt)
    return receipt_path


def validated_minimum_free_bytes(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StagingError("minimum free bytes must be an integer")
    if value < MINIMUM_FREE_BYTES:
        raise StagingError(
            f"minimum free bytes cannot be lower than {MINIMUM_FREE_BYTES}"
        )
    return value


def sha256_file(path: pathlib.Path) -> str:
    return shared_sha256_file(path, block_bytes=READ_BLOCK_BYTES)


def verify_gzip_integrity(path: pathlib.Path) -> None:
    with gzip.open(path, "rb") as handle:
        for _block in iter(lambda: handle.read(READ_BLOCK_BYTES), b""):
            pass


def extract_frozen_members(archive: pathlib.Path, destination: pathlib.Path) -> None:
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        _validate_members(members)
        bundle.extractall(destination, members=members, filter="data")


def _validate_members(members: Iterable[tarfile.TarInfo]) -> None:
    found_ms = False
    found_recipe = False
    for member in members:
        normalized = member.name.removeprefix("./")
        parts = pathlib.PurePosixPath(normalized).parts
        if not parts or normalized.startswith("/") or ".." in parts:
            raise StagingError(f"unsafe archive member: {member.name!r}")
        if not (member.isdir() or member.isfile()):
            raise StagingError(
                f"archive member must be a regular file or directory: {member.name!r}"
            )
        if normalized == RECIPE_MEMBER:
            found_recipe = True
        elif normalized == MS_MEMBER or normalized.startswith(f"{MS_MEMBER}/"):
            found_ms = True
        else:
            raise StagingError(f"unexpected archive member: {member.name!r}")
    if not found_ms or not found_recipe:
        raise StagingError("archive member inventory is incomplete")


def tree_identity(root: pathlib.Path) -> dict[str, Any]:
    try:
        identity = shared_tree_identity(root)
    except TreeIdentityError as error:
        raise StagingError(str(error)) from error
    return {
        "tree_sha256": identity["tree_sha256"],
        "file_count": identity["file_count"],
        "size_bytes": identity["size_bytes"],
    }


def _require_external_issue_root(root: pathlib.Path) -> None:
    allowed = pathlib.Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446")
    if root != allowed and allowed not in root.parents:
        raise StagingError(
            f"destination must be {allowed} or one of its children: {root}"
        )


def _require_same_device(archive: pathlib.Path, root: pathlib.Path) -> None:
    if os.stat(archive).st_dev != os.stat(root).st_dev:
        raise StagingError(
            "archive and staging root must be on the same mounted device"
        )


def validate_frozen_archive_dataset_receipt(receipt: dict[str, Any]) -> None:
    """Validate the reusable, workload-neutral staged-dataset receipt schema."""

    _require_exact_fields(
        receipt,
        {
            "schema_version",
            "kind",
            "archive",
            "dataset",
            "recipe",
            "storage",
            "elapsed_seconds",
        },
        "receipt",
    )
    if (
        isinstance(receipt["schema_version"], bool)
        or not isinstance(receipt["schema_version"], int)
        or receipt["schema_version"] != 1
    ):
        raise StagingError("receipt schema_version must be integer 1")
    if receipt["kind"] != DATASET_RECEIPT_KIND:
        raise StagingError(f"receipt kind must be {DATASET_RECEIPT_KIND}")
    _require_nonnegative_number(receipt["elapsed_seconds"], "receipt.elapsed_seconds")

    archive = _require_object(receipt["archive"], "receipt.archive")
    _require_exact_fields(
        archive,
        {"path", "size_bytes", "sha256", "gzip_integrity"},
        "receipt.archive",
    )
    _require_nonempty_string(archive["path"], "receipt.archive.path")
    _require_nonnegative_integer(archive["size_bytes"], "receipt.archive.size_bytes")
    _require_sha256(archive["sha256"], "receipt.archive.sha256")
    if archive["gzip_integrity"] != "verified":
        raise StagingError("receipt.archive.gzip_integrity must be verified")

    dataset = _require_object(receipt["dataset"], "receipt.dataset")
    _require_exact_fields(
        dataset,
        {"path", "tree_sha256", "file_count", "size_bytes"},
        "receipt.dataset",
    )
    _require_nonempty_string(dataset["path"], "receipt.dataset.path")
    _require_sha256(dataset["tree_sha256"], "receipt.dataset.tree_sha256")
    _require_nonnegative_integer(dataset["file_count"], "receipt.dataset.file_count")
    _require_nonnegative_integer(dataset["size_bytes"], "receipt.dataset.size_bytes")

    recipe = _require_object(receipt["recipe"], "receipt.recipe")
    _require_exact_fields(
        recipe,
        {"path", "size_bytes", "sha256"},
        "receipt.recipe",
    )
    _require_nonempty_string(recipe["path"], "receipt.recipe.path")
    _require_nonnegative_integer(recipe["size_bytes"], "receipt.recipe.size_bytes")
    _require_sha256(recipe["sha256"], "receipt.recipe.sha256")

    storage = _require_object(receipt["storage"], "receipt.storage")
    _require_exact_fields(
        storage,
        {
            "root",
            "device",
            "free_bytes_before",
            "same_device_atomic_promotion",
        },
        "receipt.storage",
    )
    _require_nonempty_string(storage["root"], "receipt.storage.root")
    _require_nonnegative_integer(storage["device"], "receipt.storage.device")
    _require_nonnegative_integer(
        storage["free_bytes_before"], "receipt.storage.free_bytes_before"
    )
    if storage["same_device_atomic_promotion"] is not True:
        raise StagingError("receipt.storage.same_device_atomic_promotion must be true")


def _validate_existing_stage(
    receipt_path: pathlib.Path,
    ms_path: pathlib.Path,
    recipe_path: pathlib.Path,
    *,
    archive_path: pathlib.Path,
    root: pathlib.Path,
) -> None:
    if not receipt_path.is_file():
        raise StagingError(
            f"staged data exists without its immutable receipt: {receipt_path}"
        )
    if not ms_path.is_dir() or not recipe_path.is_file():
        raise StagingError("staged receipt exists but the frozen dataset is incomplete")

    receipt = _load_receipt(receipt_path)
    validate_frozen_archive_dataset_receipt(receipt)
    archive = receipt["archive"]
    _require_path(archive["path"], archive_path, "receipt.archive.path")
    if archive["size_bytes"] != archive_path.stat().st_size:
        raise StagingError("receipt archive size no longer matches the frozen archive")
    if archive["sha256"] != ARCHIVE_SHA256:
        raise StagingError("receipt archive SHA-256 does not match the frozen digest")

    dataset = receipt["dataset"]
    _require_path(dataset["path"], ms_path, "receipt.dataset.path")

    recipe = receipt["recipe"]
    _require_path(recipe["path"], recipe_path, "receipt.recipe.path")
    if recipe["size_bytes"] != recipe_path.stat().st_size:
        raise StagingError("receipt recipe size no longer matches staged tclean.last")
    if recipe["sha256"] != RECIPE_SHA256:
        raise StagingError("receipt recipe SHA-256 does not match the frozen digest")
    if sha256_file(recipe_path) != RECIPE_SHA256:
        raise StagingError("staged tclean.last no longer matches its frozen digest")

    storage = receipt["storage"]
    _require_path(storage["root"], root, "receipt.storage.root")
    if storage["device"] != os.stat(root).st_dev:
        raise StagingError("receipt storage device no longer matches the staging root")

    recorded_identity = {
        "tree_sha256": dataset["tree_sha256"],
        "file_count": dataset["file_count"],
        "size_bytes": dataset["size_bytes"],
    }
    current_identity = tree_identity(ms_path)
    if recorded_identity != current_identity:
        raise StagingError(
            "staged MS tree identity no longer matches its immutable receipt: "
            f"recorded={recorded_identity}, current={current_identity}"
        )


def _load_receipt(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise StagingError(f"cannot read staged receipt {path}: {error}") from error
    return _require_object(value, "receipt")


def _require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StagingError(f"{label} must be a JSON object")
    return value


def _require_exact_fields(
    value: dict[str, Any], expected: set[str], label: str
) -> None:
    actual = set(value)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    raise StagingError(
        f"{label} fields do not match the receipt schema: "
        f"missing={missing}, unexpected={unexpected}"
    )


def _require_path(value: Any, expected: pathlib.Path, label: str) -> None:
    if not isinstance(value, str) or value != str(expected):
        raise StagingError(f"{label} must be {expected}")


def _require_nonempty_string(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value:
        raise StagingError(f"{label} must be a nonempty string")


def _require_sha256(value: Any, label: str) -> None:
    lowercase_hex = set("0123456789abcdef")
    if (
        not isinstance(value, str)
        or len(value) != 64
        or not set(value) <= lowercase_hex
    ):
        raise StagingError(f"{label} must be a lowercase SHA-256 digest")


def _require_nonnegative_integer(value: Any, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise StagingError(f"{label} must be a non-negative integer")


def _require_nonnegative_number(value: Any, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value < 0
        or (isinstance(value, float) and not math.isfinite(value))
    ):
        raise StagingError(f"{label} must be a finite non-negative number")


if __name__ == "__main__":
    main()
