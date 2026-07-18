"""Typed tutorial-parity manifest and execution models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Operation:
    task: str
    parameters: dict[str, Any]
    outputs: tuple[str, ...]
    capture_stdout: str | None = None


@dataclass(frozen=True)
class Surface:
    name: str
    operations: tuple[Operation, ...]
    screenshot: str | None = None
    input_events: tuple[tuple[int, str], ...] = ()
    journey: str | None = None
    required_artifacts: tuple[str, ...] = ()


@dataclass(frozen=True)
class Comparison:
    plugin: str
    config: dict[str, Any]
    inputs: dict[str, Any]


@dataclass(frozen=True)
class SectionManifest:
    path: Path
    section_id: str
    pack_id: str
    pack_relative_path: str
    title: str
    source: dict[str, str]
    prerequisites: tuple[dict[str, str], ...]
    surfaces: dict[str, Surface]
    comparison: Comparison
    evidence: dict[str, Any]


@dataclass(frozen=True)
class RuntimeResources:
    repo_root: Path
    pack_root: Path
    native_python: Path | None
    casa_python: Path | None
    binary_dir: Path
    ghostty_capture: Path | None
    evidence_root: Path
    dry_run: bool
