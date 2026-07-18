"""Typed comparator registry for declarative tutorial sections."""

from __future__ import annotations

if __package__ == "comparators":
    __all__: list[str] = []
else:
    from typing import Any, Callable

    from ..model import RuntimeResources, SectionManifest
    from . import (
        fits_products,
        image_products,
        imhead,
        imstat,
        json_fields,
        measurement_set,
        plot_products,
    )

    Comparator = Callable[[SectionManifest, RuntimeResources], dict[str, Any]]
    REGISTRY: dict[str, Comparator] = {
        "imhead": imhead.compare,
        "imstat": imstat.compare,
        "json_fields": json_fields.compare,
        "image_products": image_products.compare,
        "measurement_set": measurement_set.compare,
        "plot_products": plot_products.compare,
        "fits_products": fits_products.compare,
    }

    def run(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
        return REGISTRY[manifest.comparison.plugin](manifest, resources)

    __all__ = ["REGISTRY", "run"]
