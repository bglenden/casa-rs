"""MeasurementSet structural/scientific summary comparison."""

from __future__ import annotations

from typing import Any

from ..model import RuntimeResources, SectionManifest
from .casa_protocol import run_casa_comparison


def compare(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
    return run_casa_comparison(manifest, resources, "measurement_set")
