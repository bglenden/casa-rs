#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Export the audited CASA-RS baseline inventory from RadioAstronomyOracle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

EXPECTED_COUNTS = {
    "sources": 55,
    "pages_or_slides": 4_892,
    "books": 2,
    "nrao_synthesis_2024": 26,
    "nrao_synthesis_2026": 27,
}

# Corrections are intentionally source/page-specific and are backed by a visual
# comparison with the rendered source page. Never use broad formula rewriting
# for scientific notation.
VERIFIED_PAGE_CORRECTIONS: dict[tuple[str, int], tuple[tuple[str, str], ...]] = {
    ("nrao-2026:perley-geometry-siw2026", 15): (
        (r"H _ {\circ}", r"H _ {0}"),
        (r"\delta_ {\circ}", r"\delta_ {0}"),
        (r"\upsilon", r"\nu"),
        (r"v_{F}", r"\nu_{F}"),
        (r"v _ {F}", r"\nu _ {F}"),
    ),
}

BOOK_POLICY: dict[str, dict[str, Any]] = {
    "books:interferometry-and-synthesis-in-radio-astronomy": {
        "origin_url": "https://link.springer.com/book/10.1007/978-3-319-44431-4",
        "download_url": "https://link.springer.com/content/pdf/10.1007/978-3-319-44431-4.pdf",
        "contributors": [
            "A. Richard Thompson",
            "James M. Moran",
            "George W. Swenson Jr.",
        ],
        "license": {
            "id": "CC-BY-NC-4.0",
            "name": "Creative Commons Attribution-NonCommercial 4.0 International",
            "url": "https://creativecommons.org/licenses/by-nc/4.0/",
            "evidence": "Publisher PDF copyright page and Springer book record",
        },
        "acquisition": "authoritative_download_with_license_acknowledgement",
        "included": True,
        "exclusion_reason": None,
        "redistribution": {
            "bundled": True,
            "reason": "Bundled under the publisher-recorded CC BY-NC 4.0 license.",
        },
    },
    "books:synthesis-imaging-ii-1998": {
        "origin_url": "https://www.aspbooks.org/a/volumes/table_of_contents/?book_id=292",
        "download_url": None,
        "license": {
            "id": "LicenseRef-ASP-Paid-eAccess",
            "name": "Astronomical Society of the Pacific paid electronic access",
            "url": "https://www.aspbooks.org/a/volumes/table_of_contents/?book_id=292",
            "evidence": "Publisher volume page offers paid volume/article electronic access and no open redistribution license.",
        },
        "acquisition": "user_supplied_licensed_copy",
        "included": False,
        "exclusion_reason": "No lawful unattended public acquisition or redistribution basis was found; users may add a licensed copy as a project document.",
        "redistribution": {
            "bundled": False,
            "reason": "No redistribution permission recorded.",
        },
    },
}

NRAO_SLIDE_LICENSE = {
    "id": "LicenseRef-NRAO-Public-Workshop-Authorized",
    "name": "NRAO publicly hosted workshop material; CASA-RS bundling authorized by the project owner",
    "url": "https://library.nrao.edu/public/misc/policy/Copyright_Policy_v1.1.pdf",
    "evidence": "The project owner, formerly responsible for NRAO data management and software, explicitly authorized bundling on 2026-07-15 (casa-rs issue #420).",
}

NRAO_2026_POLICY = {
    "license": {
        **NRAO_SLIDE_LICENSE,
    },
    "acquisition": "bundled_normalized_text_from_authorized_public_workshop_source",
    "included": True,
    "exclusion_reason": None,
    "redistribution": {
        "bundled": True,
        "reason": "Compact page-level text is bundled under the project-owner authorization recorded in issue #420.",
    },
}

NRAO_2024_POLICY = {
    "license": {
        **NRAO_SLIDE_LICENSE,
    },
    "acquisition": "excluded_superseded_epoch",
    "included": False,
    "exclusion_reason": "Superseded by the 2026 Synthesis Imaging Workshop epoch for the compact v1 baseline; retain in the inventory for selective future inclusion.",
    "redistribution": {
        "bundled": False,
        "reason": "Not selected for the compact v1 baseline.",
    },
}


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--oracle-root",
        type=Path,
        default=Path(os.environ.get("RADIO_ASTRONOMY_ORACLE_ROOT", "")),
        help="RadioAstronomyOracle checkout (or set RADIO_ASTRONOMY_ORACLE_ROOT)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("resources/assistant-corpus/radio-astronomy-source-inventory-v1.json"),
    )
    parser.add_argument(
        "--pack-root",
        type=Path,
        default=Path("apps/casars-mac/Sources/CasarsMacCore/Resources/assistant-corpus"),
        help="Destination for the clean-install compact baseline resource pack",
    )
    parser.add_argument("--check", action="store_true", help="Fail if output differs from the reproducible export")
    return parser.parse_args()


def collection_documents(root: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name in ("nrao-synthesis-2024.json", "nrao-synthesis-2026.json"):
        payload = json.loads((root / "collections" / name).read_text(encoding="utf-8"))
        for document in payload["documents"]:
            result[document["canonical_key"]] = document
    return result


def compact_pages(canonical_key: str, path: Path, expected_count: int) -> bytes:
    source_pages = json.loads(path.read_text(encoding="utf-8"))
    if len(source_pages) != expected_count:
        raise SystemExit(
            f"normalized page-count drift for {path}: expected {expected_count}, found {len(source_pages)}"
        )
    pages = []
    for page in source_pages:
        content = page.get("markdown") or page.get("text") or ""
        content = re.sub(r"!\[[^\]]*\]\([^)]+\)\s*", "", content).strip()
        if not content:
            content = (page.get("text") or "").strip()
        for old, new in VERIFIED_PAGE_CORRECTIONS.get(
            (canonical_key, int(page["page_number"])), ()
        ):
            content = content.replace(old, new)
        pages.append({"page": int(page["page_number"]), "content": content})
    return (json.dumps(pages, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def export(root: Path) -> tuple[dict[str, Any], dict[str, bytes]]:
    registry = root / "corpus" / "registry.sqlite"
    if not registry.is_file():
        raise SystemExit(f"missing Oracle registry: {registry}")
    collections = collection_documents(root)
    connection = sqlite3.connect(registry)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """SELECT source_id, canonical_key, source_name, source_path, source_type,
                  source_class, collection_id, title, contributors_json, year,
                  topic_tags_json, authority_level, specificity_scope, page_count,
                  normalized_pages_path
             FROM documents ORDER BY collection_id, canonical_key"""
    ).fetchall()
    connection.close()

    sources: list[dict[str, Any]] = []
    bundle_files: dict[str, bytes] = {}
    for row in rows:
        key = row["canonical_key"]
        source_path = Path(row["source_path"])
        normalized_path = Path(row["normalized_pages_path"])
        if not source_path.is_file() or not normalized_path.is_file():
            raise SystemExit(f"missing Oracle source artifacts for {key}")
        if row["source_type"] == "books":
            policy = BOOK_POLICY[key]
            citation_kind = "book_page"
        else:
            policy = NRAO_2026_POLICY if row["year"] == 2026 else NRAO_2024_POLICY
            citation_kind = "slide"
        collection = collections.get(key)
        origin_url = collection["source_url"] if collection else policy["origin_url"]
        download_url = collection["source_url"] if collection else policy["download_url"]
        source = {
                "id": key,
                "oracle_source_id": row["source_id"],
                "title": row["title"],
                "source_class": row["source_class"],
                "collection_id": row["collection_id"],
                "year": row["year"],
                "contributors": policy.get("contributors") or json.loads(row["contributors_json"]),
                "topic_tags": json.loads(row["topic_tags_json"]),
                "authority_level": row["authority_level"],
                "specificity_scope": row["specificity_scope"],
                "filename": row["source_name"],
                "page_or_slide_count": row["page_count"],
                "source_size_bytes": source_path.stat().st_size,
                "oracle_working_copy_sha256": digest(source_path),
                "authoritative_download_sha256": None,
                "normalized_text_sha256": digest(normalized_path),
                "origin_url": origin_url,
                "download_url": download_url,
                "citation": {"label": row["source_name"], "locator_kind": citation_kind},
                "license": policy["license"],
                "acquisition": {
                    "method": policy["acquisition"],
                    "included_in_baseline": policy["included"],
                    "enabled_by_default": policy["included"],
                    "exclusion_reason": policy["exclusion_reason"],
                },
                "redistribution": policy["redistribution"],
                "text_generation": {
                    "method": "local_page_or_slide_extraction",
                    "oracle_normalized_text_bundled": policy["included"],
                    "oracle_normalized_text_digest_is_audit_only": not policy["included"],
                },
            }
        if policy["included"]:
            bundle_path = f"standard-v1/{row['source_id']}.pages.json"
            encoded_pages = compact_pages(key, normalized_path, row["page_count"])
            bundle_files[bundle_path] = encoded_pages
            source["bundle"] = {
                "path": bundle_path,
                "format": "normalized_pages_json",
                "sha256": hashlib.sha256(encoded_pages).hexdigest(),
            }
        else:
            source["bundle"] = None
        sources.append(source)

    counts = {
        "sources": len(sources),
        "pages_or_slides": sum(item["page_or_slide_count"] for item in sources),
        "books": sum(item["source_class"] == "book" for item in sources),
        "nrao_synthesis_2024": sum(item["collection_id"] == "nrao-synthesis-2024" for item in sources),
        "nrao_synthesis_2026": sum(item["collection_id"] == "nrao-synthesis-2026" for item in sources),
    }
    if counts != EXPECTED_COUNTS:
        raise SystemExit(f"Oracle inventory drift: expected {EXPECTED_COUNTS}, found {counts}")
    payload = {
        "schema_version": 1,
        "id": "casa-rs-standard-radio-astronomy",
        "version": "2026.07.1",
        "source_inventory": {
            "origin": "RadioAstronomyOracle registry export",
            "registry_sha256": digest(registry),
            "counts": counts,
        },
        "retrieval_precedence": {
            "current_practice": ["nrao-synthesis-2026", "nrao-synthesis-2024", "books"],
            "theory_and_derivations": ["books", "nrao-synthesis-2026", "nrao-synthesis-2024"],
        },
        "policy": {
            "public_availability_is_not_redistribution_permission": True,
            "shared_install_only": True,
            "project_document_copying": False,
            "full_corpus_enabled_by_default": True,
            "selection": "2026 workshop plus open Springer book; 2024 retained as superseded inventory",
        },
        "sources": sources,
    }
    return payload, bundle_files


def pack_manifest(payload: dict[str, Any], bundle_files: dict[str, bytes], pack_root: Path) -> bytes:
    primer_path = pack_root / "radio-interferometry-primer.md"
    if not primer_path.is_file():
        raise SystemExit(f"missing CASA-RS primer: {primer_path}")
    documents: list[dict[str, Any]] = [{
        "path": "radio-interferometry-primer.md",
        "format": "utf8_text",
        "title": "CASA-RS Radio Interferometry Primer",
        "citation_label": "CASA-RS Radio Interferometry Primer v1.0",
        "citation_kind": "document",
        "source_path": "radio-interferometry-primer.md",
        "content_sha256": digest(primer_path),
        "source_sha256": digest(primer_path),
        "origin_url": "https://github.com/bglenden/casa-rs",
        "license": {
            "id": "LGPL-3.0-or-later",
            "name": "GNU Lesser General Public License v3.0 or later",
            "url": "https://www.gnu.org/licenses/lgpl-3.0.html",
        },
        "contributors": ["CASA-RS contributors"],
        "modifications": "None; original CASA-RS project documentation.",
        "redistribution_basis": "Original CASA-RS project documentation",
        "collection_id": "casa-rs",
        "year": 2026,
    }]
    for source in payload["sources"]:
        bundle = source["bundle"]
        if bundle is None:
            continue
        documents.append({
            "path": bundle["path"],
            "format": bundle["format"],
            "title": source["title"],
            "citation_label": source["citation"]["label"],
            "citation_kind": source["citation"]["locator_kind"],
            "source_path": source["filename"],
            "content_sha256": bundle["sha256"],
            "source_sha256": source["oracle_working_copy_sha256"],
            "origin_url": source["origin_url"],
            "license": {
                "id": source["license"]["id"],
                "name": source["license"]["name"],
                "url": source["license"]["url"],
            },
            "contributors": source["contributors"],
            "modifications": "Normalized page-level text extracted from the source; images are omitted and visually verified OCR corrections may be applied.",
            "redistribution_basis": source["redistribution"]["reason"],
            "collection_id": source["collection_id"],
            "year": source["year"],
        })
    manifest = {
        "schema_version": 2,
        "id": payload["id"],
        "version": payload["version"],
        "selection": payload["policy"]["selection"],
        "documents": documents,
    }
    return (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()


def write_pack(payload: dict[str, Any], bundle_files: dict[str, bytes], pack_root: Path) -> None:
    version_root = pack_root / "standard-v1"
    version_root.mkdir(parents=True, exist_ok=True)
    expected = {pack_root / relative for relative in bundle_files}
    for stale in version_root.glob("*.pages.json"):
        if stale not in expected:
            stale.unlink()
    for relative, content in bundle_files.items():
        destination = pack_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    (pack_root / "corpus-pack.json").write_bytes(pack_manifest(payload, bundle_files, pack_root))
    (pack_root / "NOTICE.md").write_bytes(pack_notice(payload))


def pack_notice(payload: dict[str, Any]) -> bytes:
    book = next(source for source in payload["sources"] if source["id"] == "books:interferometry-and-synthesis-in-radio-astronomy")
    lines = [
        "# CASA-RS standard radio-astronomy corpus notice",
        "",
        f"Corpus pack: `{payload['id']}@{payload['version']}`.",
        "",
        "## Interferometry and Synthesis in Radio Astronomy, Third Edition",
        "",
        "Authors: " + ", ".join(book["contributors"]),
        f"Source: {book['origin_url']}",
        f"License: {book['license']['name']} ({book['license']['url']})",
        "Changes: normalized page-level text was extracted from the source; images are omitted and source/page-specific visually verified OCR corrections may be applied.",
        "",
        "## NRAO Synthesis Imaging Workshop 2026",
        "",
        "The compact pack includes normalized page-level text from the 27 publicly hosted 2026 workshop decks. The title, contributor(s), authoritative NRAO URL, source digest, and individual license/authorization record for every deck are in `corpus-pack.json`.",
        "Changes: images are omitted and source/slide-specific visually verified OCR corrections may be applied.",
        "Bundling authorization was explicitly recorded by the CASA-RS project owner in issue #420 on 2026-07-15.",
        "",
    ]
    return ("\n".join(lines)).encode()


def main() -> None:
    args = parse_args()
    if not str(args.oracle_root):
        raise SystemExit("--oracle-root or RADIO_ASTRONOMY_ORACLE_ROOT is required")
    payload, bundle_files = export(args.oracle_root.resolve())
    encoded = (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode()
    expected_manifest = pack_manifest(payload, bundle_files, args.pack_root)
    expected_notice = pack_notice(payload)
    if args.check:
        if not args.output.is_file() or args.output.read_bytes() != encoded:
            raise SystemExit(f"inventory is stale: regenerate {args.output}")
        if not (args.pack_root / "corpus-pack.json").is_file() or (args.pack_root / "corpus-pack.json").read_bytes() != expected_manifest:
            raise SystemExit(f"corpus pack manifest is stale: regenerate {args.pack_root}")
        if not (args.pack_root / "NOTICE.md").is_file() or (args.pack_root / "NOTICE.md").read_bytes() != expected_notice:
            raise SystemExit(f"corpus pack notice is stale: regenerate {args.pack_root}")
        for relative, content in bundle_files.items():
            path = args.pack_root / relative
            if not path.is_file() or path.read_bytes() != content:
                raise SystemExit(f"corpus pack content is stale: regenerate {path}")
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(encoded)
    write_pack(payload, bundle_files, args.pack_root)
    print(f"wrote {args.output}", file=sys.stderr)
    print(f"wrote compact corpus pack at {args.pack_root}", file=sys.stderr)


if __name__ == "__main__":
    main()
