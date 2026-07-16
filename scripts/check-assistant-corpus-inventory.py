#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate the self-contained standard radio-astronomy source inventory."""

from __future__ import annotations

import json
import hashlib
import re
from pathlib import Path

PATH = Path("resources/assistant-corpus/radio-astronomy-source-inventory-v1.json")
PACK_ROOT = Path("apps/casars-mac/Sources/CasarsMacCore/Resources/assistant-corpus")
ORIGIN_AUDIT = Path("resources/assistant-corpus/radio-astronomy-origin-audit-2026-07-15.json")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
EXPECTED = {
    "sources": 55,
    "pages_or_slides": 4_892,
    "books": 2,
    "nrao_synthesis_2024": 26,
    "nrao_synthesis_2026": 27,
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"assistant-corpus inventory: {message}")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    payload = json.loads(PATH.read_text(encoding="utf-8"))
    require(payload["schema_version"] == 1, "unsupported schema")
    require(payload["source_inventory"]["counts"] == EXPECTED, "source counts drifted")
    require(payload["retrieval_precedence"]["current_practice"][:2] == [
        "nrao-synthesis-2026", "nrao-synthesis-2024"
    ], "current-practice precedence must prefer 2026")
    require(payload["retrieval_precedence"]["theory_and_derivations"][0] == "books",
            "theory precedence must prefer books")
    sources = payload["sources"]
    require(len(sources) == 55, "expected 55 expanded source records")
    require(len({source["id"] for source in sources}) == 55, "source identities must be unique")
    included = [source for source in sources if source["acquisition"]["included_in_baseline"]]
    require(len(included) == 28, "compact baseline must include 27 2026 decks and one book")
    require(sum(source["year"] == 2026 for source in included) == 27,
            "compact baseline must include every 2026 workshop deck")
    require(not any(source["year"] == 2024 for source in included),
            "2024 workshop must remain inventoried but excluded as superseded")
    for source in sources:
        label = source["id"]
        require(bool(source["origin_url"]), f"{label} has no origin")
        require(SHA256.fullmatch(source["oracle_working_copy_sha256"]) is not None,
                f"{label} has no full Oracle working-copy digest")
        require(SHA256.fullmatch(source["normalized_text_sha256"]) is not None,
                f"{label} has no normalized-text audit digest")
        require(bool(source["citation"]["label"]), f"{label} has no citation label")
        require(source["citation"]["locator_kind"] in {"book_page", "slide"},
                f"{label} has no page/slide citation kind")
        require(bool(source["license"]["id"] and source["license"]["evidence"]),
                f"{label} has no license evidence")
        acquisition = source["acquisition"]
        if acquisition["included_in_baseline"]:
            require(bool(source["download_url"]), f"{label} has no acquisition URL")
            require(source["redistribution"]["bundled"] is True,
                    f"{label} is selected but not marked bundled")
            require(source["bundle"] is not None, f"{label} has no bundle record")
            require(bool(source["contributors"]), f"{label} has no attribution contributors")
            bundle_path = PACK_ROOT / source["bundle"]["path"]
            require(bundle_path.is_file(), f"{label} bundle content is missing")
            require(digest(bundle_path) == source["bundle"]["sha256"],
                    f"{label} bundle digest does not match")
        else:
            require(bool(acquisition["exclusion_reason"]), f"{label} has no exclusion reason")
            require(source["redistribution"]["bundled"] is False,
                    f"{label} is excluded but marked bundled")
            require(source["bundle"] is None, f"{label} is excluded but has bundle content")

    manifest = json.loads((PACK_ROOT / "corpus-pack.json").read_text(encoding="utf-8"))
    require(manifest["schema_version"] == 2, "compact resource pack must use schema v2")
    require(manifest["id"] == payload["id"] and manifest["version"] == payload["version"],
            "resource pack identity/version differs from the inventory")
    require(len(manifest["documents"]) == 29, "resource pack must contain primer plus 28 sources")
    page_total = 0
    for document in manifest["documents"]:
        path = PACK_ROOT / document["path"]
        require(path.is_file(), f"manifest content is missing: {document['path']}")
        require(digest(path) == document["content_sha256"],
                f"manifest content digest differs: {document['path']}")
        require(bool(document["origin_url"] and document["source_sha256"]),
                f"manifest origin/source digest missing: {document['path']}")
        require(bool(document["license"]["id"] and document["redistribution_basis"]),
                f"manifest license/basis missing: {document['path']}")
        require(bool(document["license"]["name"] and document["license"]["url"]),
                f"manifest license name/URL missing: {document['path']}")
        require(bool(document["contributors"] and document["modifications"]),
                f"manifest attribution/modification notice missing: {document['path']}")
        if document["format"] == "normalized_pages_json":
            pages = json.loads(path.read_text(encoding="utf-8"))
            require(all(page["page"] > 0 and page["content"].strip() for page in pages),
                    f"empty or invalid page content: {document['path']}")
            page_total += len(pages)
    require(page_total == 2_314, "resource pack page/slide count must be 2314")
    notice = (PACK_ROOT / "NOTICE.md").read_text(encoding="utf-8")
    require("A. Richard Thompson" in notice,
            "installed notice must name the book authors")
    require("Creative Commons Attribution-NonCommercial 4.0" in notice,
            "installed notice must identify the book license")
    require("images are omitted" in notice,
            "installed notice must disclose normalized-text modifications")

    audit = json.loads(ORIGIN_AUDIT.read_text(encoding="utf-8"))
    require(audit["inventory_id"] == payload["id"], "origin audit targets another inventory")
    require(audit["inventory_version"] == payload["version"], "origin audit targets another version")
    require(audit["summary"]["declared_included"] == 28, "origin audit omitted selected sources")
    require(audit["summary"]["reachable"] == 28 and audit["summary"]["failed"] == 0,
            "selected authoritative origins did not all pass the recorded audit")
    print("assistant-corpus inventory: 55 sources accounted for")


if __name__ == "__main__":
    main()
