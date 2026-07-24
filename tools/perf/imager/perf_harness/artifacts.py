# SPDX-License-Identifier: LGPL-3.0-or-later
"""Atomic artifact IO shared by imaging performance tools."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import pathlib
import tempfile
from typing import Any


class ArtifactError(ValueError):
    """A JSON artifact is missing, malformed, or cannot be committed."""


@dataclass(frozen=True)
class AtomicDirectoryBundle:
    """A same-parent partial directory awaiting one atomic publication."""

    final_path: pathlib.Path
    partial_path: pathlib.Path


def prepare_atomic_directory_bundle(final_path: pathlib.Path) -> AtomicDirectoryBundle:
    final_path = final_path.resolve()
    partial_path = final_path.with_name(f"{final_path.name}.partial")
    if final_path.exists() or final_path.is_symlink():
        raise ArtifactError(f"final artifact bundle already exists: {final_path}")
    if partial_path.exists() or partial_path.is_symlink():
        raise ArtifactError(f"partial artifact bundle already exists: {partial_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        partial_path.mkdir()
    except OSError as error:
        raise ArtifactError(
            f"create partial artifact bundle {partial_path}: {error}"
        ) from error
    _fsync_directory(final_path.parent)
    return AtomicDirectoryBundle(final_path=final_path, partial_path=partial_path)


def promote_atomic_directory_bundle(bundle: AtomicDirectoryBundle) -> None:
    if bundle.final_path.parent != bundle.partial_path.parent:
        raise ArtifactError(
            "artifact bundle promotion must stay in one parent directory"
        )
    if bundle.final_path.exists() or bundle.final_path.is_symlink():
        raise ArtifactError(
            f"final artifact bundle already exists: {bundle.final_path}"
        )
    if not bundle.partial_path.is_dir() or bundle.partial_path.is_symlink():
        raise ArtifactError(
            f"partial artifact bundle is unavailable: {bundle.partial_path}"
        )
    try:
        os.replace(bundle.partial_path, bundle.final_path)
        _fsync_directory(bundle.final_path.parent)
    except OSError as error:
        raise ArtifactError(
            f"promote artifact bundle {bundle.partial_path} to {bundle.final_path}: {error}"
        ) from error


def _fsync_directory(path: pathlib.Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_json_object(
    path: pathlib.Path, *, description: str = "artifact"
) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactError(f"read {description} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ArtifactError(f"{description} {path} must contain a JSON object")
    return value


def atomic_write_json(path: pathlib.Path, value: Any) -> None:
    """Durably replace a JSON artifact without exposing a partial document."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
