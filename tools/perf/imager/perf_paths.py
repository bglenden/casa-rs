"""Shared paths for ImPerformance tooling.

Keep generated benchmark data away from the repository checkout by default.
Build outputs still belong under Cargo's target directory, but MeasurementSets,
CASA/casa-rs image products, comparison panels, and benchmark scratch files are
large enough that they should live on the external ImPerformance volume when it
is mounted.
"""

from __future__ import annotations

import os
import pathlib


ARTIFACT_ROOT_ENV = "CASA_RS_IMPERF_ARTIFACT_ROOT"
EXTERNAL_VOLUME = pathlib.Path("/Volumes/GLENDENNING")
DEFAULT_EXTERNAL_ARTIFACT_ROOT = (
    EXTERNAL_VOLUME
    / "casa-rs-imperformance"
    / "_tmp_safe_to_delete"
    / "imperformance-artifacts"
)
FALLBACK_ARTIFACT_ROOT = pathlib.Path("target/imperformance-artifacts")
SAFE_TO_DELETE_MARKER = "README_SAFE_TO_DELETE.txt"


def default_artifact_root() -> pathlib.Path:
    """Return the default root for large generated ImPerformance artifacts."""

    configured = os.environ.get(ARTIFACT_ROOT_ENV)
    if configured:
        return pathlib.Path(configured).expanduser()
    if EXTERNAL_VOLUME.exists():
        return DEFAULT_EXTERNAL_ARTIFACT_ROOT
    return FALLBACK_ARTIFACT_ROOT


def artifact_path(*parts: str) -> pathlib.Path:
    """Return a path under the default artifact root."""

    path = default_artifact_root()
    for part in parts:
        path /= part
    return path


def mark_safe_to_delete(root: pathlib.Path) -> None:
    """Create a marker documenting that generated artifacts may be removed."""

    root.mkdir(parents=True, exist_ok=True)
    marker = root / SAFE_TO_DELETE_MARKER
    if marker.exists():
        return
    marker.write_text(
        "This directory contains generated casa-rs ImPerformance artifacts.\n"
        "It is safe to delete when no benchmark run is actively using it.\n"
        "Recreate the contents by rerunning the relevant tools/perf/imager command.\n",
        encoding="utf-8",
    )
