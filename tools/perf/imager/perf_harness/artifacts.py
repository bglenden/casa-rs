# SPDX-License-Identifier: LGPL-3.0-or-later
"""Atomic artifact IO shared by imaging performance tools."""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
from typing import Any


class ArtifactError(ValueError):
    """A JSON artifact is missing, malformed, or cannot be committed."""


def load_json_object(path: pathlib.Path, *, description: str = "artifact") -> dict[str, Any]:
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
