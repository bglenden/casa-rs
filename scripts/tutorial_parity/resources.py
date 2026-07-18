"""Canonical interpreter, binary, pack, and prerequisite resolution."""

from __future__ import annotations

import os
from pathlib import Path

from .commands import run_command
from .model import RuntimeResources, SectionManifest


REPO_ROOT = Path(__file__).resolve().parents[2]
TUTORIAL_ROOT_ENV = "CASA_RS_TUTORIAL_DATA_ROOT"
CASA_PYTHON_ENV = "CASA_RS_CASA_PYTHON"
GHOSTTY_CAPTURE_ENV = "CASA_RS_GHOSTTY_CAPTURE"


class ResourceError(RuntimeError):
    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category


def resolve_resources(
    manifest: SectionManifest,
    *,
    pack_root: Path | None,
    native_python: Path | None,
    casa_python: Path | None,
    binary_dir: Path | None,
    ghostty_capture: Path | None,
    evidence_root: Path | None,
    require_existing: bool,
) -> RuntimeResources:
    resolved_pack = _resolve_pack(manifest, pack_root, require_existing=require_existing)
    resolved_native = native_python or _resolve_native_python(require_existing)
    resolved_casa = casa_python or _environment_path(CASA_PYTHON_ENV)
    resolved_binary_dir = (binary_dir or REPO_ROOT / "target" / "debug").expanduser().absolute()
    resolved_ghostty = ghostty_capture or _environment_path(GHOSTTY_CAPTURE_ENV)
    resolved_evidence = (
        evidence_root.expanduser().absolute()
        if evidence_root is not None
        else resolved_pack / ".casa-rs" / "evidence" / "tutorial-parity"
    )
    if require_existing:
        if not resolved_pack.is_dir():
            raise ResourceError("dataset_missing", f"tutorial pack is unavailable: {resolved_pack}")
        _require_file(resolved_native, "native_python_missing", "native Python")
        if resolved_casa is not None:
            _require_file(resolved_casa, "casa_python_missing", "CASA Python")
        for prerequisite in manifest.prerequisites:
            path = resolved_pack / prerequisite["path"]
            if prerequisite["kind"] in {"directory", "measurement_set", "casa_image"}:
                if not path.is_dir():
                    raise ResourceError("dataset_missing", f"missing {prerequisite['kind']}: {path}")
            elif not path.is_file():
                raise ResourceError("dataset_missing", f"missing file: {path}")
    return RuntimeResources(
        repo_root=REPO_ROOT,
        pack_root=resolved_pack,
        native_python=resolved_native,
        casa_python=resolved_casa,
        binary_dir=resolved_binary_dir,
        ghostty_capture=resolved_ghostty,
        evidence_root=resolved_evidence,
        dry_run=not require_existing,
    )


def binary(resources: RuntimeResources, name: str, *, require_existing: bool = True) -> Path:
    path = resources.binary_dir / name
    if require_existing and not path.is_file():
        raise ResourceError("binary_missing", f"required binary is unavailable: {path}")
    return path


def _resolve_pack(
    manifest: SectionManifest, explicit: Path | None, *, require_existing: bool
) -> Path:
    if explicit is not None:
        return explicit.expanduser().absolute()
    root = os.environ.get(TUTORIAL_ROOT_ENV)
    if not root:
        if not require_existing:
            return Path(f"${TUTORIAL_ROOT_ENV}") / manifest.pack_relative_path
        raise ResourceError(
            "tutorial_root_missing",
            f"set {TUTORIAL_ROOT_ENV} or pass --pack-root for {manifest.section_id}",
        )
    return Path(root).expanduser().absolute() / manifest.pack_relative_path


def _resolve_native_python(require_existing: bool) -> Path | None:
    resolver = REPO_ROOT / "scripts" / "resolve-python.sh"
    if not require_existing:
        return None
    completed = run_command(
        [str(resolver), "3.10"],
        cwd=REPO_ROOT,
        timeout_seconds=30,
    )
    if completed.return_code != 0 or not completed.stdout.strip():
        raise ResourceError("native_python_missing", completed.stderr.strip() or "native Python resolution failed")
    return Path(completed.stdout.strip()).expanduser().absolute()


def _environment_path(name: str) -> Path | None:
    value = os.environ.get(name)
    return Path(value).expanduser().absolute() if value else None


def _require_file(path: Path | None, category: str, label: str) -> None:
    if path is None or not path.is_file():
        raise ResourceError(category, f"{label} is unavailable: {path}")
