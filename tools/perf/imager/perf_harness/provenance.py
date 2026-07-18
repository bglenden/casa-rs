# SPDX-License-Identifier: LGPL-3.0-or-later
"""Machine, repository, executable, and dataset provenance capture."""

from __future__ import annotations

import hashlib
import os
import pathlib
import platform
import subprocess
import sys
from typing import Any


def capture_provenance(
    *,
    repo_root: pathlib.Path,
    executables: dict[str, str | pathlib.Path | None],
    datasets: dict[str, str | pathlib.Path | None],
    storage_label: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "repository": {
            "root": str(repo_root),
            "revision": _git(repo_root, "rev-parse", "HEAD"),
            "branch": _git(repo_root, "branch", "--show-current"),
            "dirty": bool(_git(repo_root, "status", "--porcelain")),
        },
        "runtime": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cores": os.cpu_count(),
            "physical_cores": _physical_core_count(),
            "physical_memory_bytes": _physical_memory_bytes(),
        },
        "executables": {
            name: _path_provenance(path) for name, path in sorted(executables.items())
        },
        "datasets": {
            name: _path_provenance(path, hash_file=False)
            for name, path in sorted(datasets.items())
        },
        "storage_label": storage_label,
    }


def executable_path(provenance: dict[str, Any], name: str) -> str | None:
    """Return an executable path from canonical provenance, if configured."""

    executables = provenance.get("executables")
    if not isinstance(executables, dict):
        return None
    record = executables.get(name)
    if not isinstance(record, dict):
        return None
    value = record.get("path")
    return value if isinstance(value, str) and value else None


def _git(repo_root: pathlib.Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def _path_provenance(
    value: str | pathlib.Path | None, *, hash_file: bool = True
) -> dict[str, Any] | None:
    if value is None:
        return None
    # Preserve the invocation path. Virtual environments commonly expose their
    # interpreter through a symlink whose target is the system Python; resolving
    # that symlink here changes which environment later protocol stages execute.
    path = pathlib.Path(os.path.abspath(pathlib.Path(value).expanduser()))
    resolved_path = path.resolve()
    record: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "kind": "directory" if path.is_dir() else "file" if path.is_file() else "missing",
    }
    if resolved_path != path:
        record["resolved_path"] = str(resolved_path)
    if hash_file and path.is_file():
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        record["sha256"] = digest.hexdigest()
    return record


def _physical_core_count() -> int | None:
    return _sysctl_int("hw.physicalcpu") or os.cpu_count()


def _physical_memory_bytes() -> int | None:
    return _sysctl_int("hw.memsize") or _sysconf_int(
        "SC_PAGE_SIZE", "SC_PHYS_PAGES"
    )


def _sysctl_int(name: str) -> int | None:
    try:
        completed = subprocess.run(
            ["sysctl", "-n", name],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return int(value) if value.isdigit() else None


def _sysconf_int(page_size_name: str, page_count_name: str) -> int | None:
    try:
        page_size = os.sysconf(page_size_name)
        page_count = os.sysconf(page_count_name)
    except (AttributeError, OSError, ValueError):
        return None
    if not isinstance(page_size, int) or not isinstance(page_count, int):
        return None
    return page_size * page_count
