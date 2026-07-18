"""Deterministic human-review documentation over typed result evidence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .model import SectionManifest


def render_markdown(manifest: SectionManifest, result: dict[str, Any]) -> str:
    rows = []
    for name in ("casa", "cli", "python", "tui", "gui"):
        surface = result["surfaces"].get(name)
        if surface is None:
            continue
        reason = surface.get("reason") or ""
        rows.append(f"| {name} | {surface['status']} | {reason} |")
    comparison = result.get("comparison", {})
    return f"""# {manifest.title}

Section: `{manifest.section_id}`

Source: [{manifest.source['anchor']}]({manifest.source['url']})

Result status: `{result['status']}`

## Surface evidence

| Surface | Status | Reason |
| --- | --- | --- |
{chr(10).join(rows)}

## Scientific comparison

- Plugin: `{manifest.comparison.plugin}`
- Status: `{comparison.get('status', 'not-run')}`
- Result artifact: `{manifest.evidence['result']}`
- Review record: `{manifest.evidence['review']}`

This document is generated regression evidence. It does not define a tutorial,
notebook, provider, or persisted task contract.
"""


def write_documentation(pack_root: Path, manifest: SectionManifest, result: dict[str, Any]) -> list[Path]:
    content = render_markdown(manifest, result)
    paths = []
    for relative in manifest.evidence["documentation"]:
        path = pack_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".md":
            path.write_text(content, encoding="utf-8")
        elif path.suffix == ".html":
            escaped = content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            path.write_text(f"<!doctype html><meta charset=\"utf-8\"><pre>{escaped}</pre>\n", encoding="utf-8")
        else:
            raise ValueError(f"unsupported documentation output: {path}")
        paths.append(path)
    return paths
