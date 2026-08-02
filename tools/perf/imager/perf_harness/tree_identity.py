# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compact, deterministic identities for evidence files and directory trees."""

from __future__ import annotations

import hashlib
import pathlib
from collections.abc import Collection
from typing import Any


READ_BLOCK_BYTES = 8 * 1024 * 1024


class TreeIdentityError(ValueError):
    """A requested evidence tree cannot be represented safely."""


def sha256_file(path: pathlib.Path, *, block_bytes: int = READ_BLOCK_BYTES) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(block_bytes), b""):
            digest.update(block)
    return digest.hexdigest()


def tree_identity(
    root: pathlib.Path, *, excluded_names: Collection[str] = ()
) -> dict[str, Any]:
    """Hash regular files by relative path, size, and content digest.

    The compact result deliberately omits the per-file inventory. Callers may
    exclude named volatile lock files, but every exclusion is counted and
    recorded in the identity so two policies cannot be confused.
    """

    root = root.resolve()
    if not root.is_dir() or root.is_symlink():
        raise TreeIdentityError(f"tree root must be a real directory: {root}")
    digest = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    excluded_count = 0
    for path in sorted(root.rglob("*"), key=lambda value: value.as_posix()):
        if path.is_dir() and not path.is_symlink():
            continue
        if path.name in excluded_names and not path.is_dir():
            excluded_count += 1
            continue
        if not path.is_file() or path.is_symlink():
            raise TreeIdentityError(f"tree contains a non-regular file: {path}")
        relative = path.relative_to(root).as_posix()
        size = path.stat().st_size
        file_digest = sha256_file(path)
        digest.update(f"{relative}\0{size}\0{file_digest}\n".encode())
        file_count += 1
        total_bytes += size
    return {
        "tree_sha256": digest.hexdigest(),
        "file_count": file_count,
        "size_bytes": total_bytes,
        "excluded_names": sorted(excluded_names),
        "excluded_count": excluded_count,
    }
