#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Generate, check, and audit the CASA-RS standard assistant corpus pack."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

EXPECTED_COUNTS = {
    "sources": 55,
    "pages_or_slides": 4_892,
    "books": 2,
    "nrao_synthesis_2024": 26,
    "nrao_synthesis_2026": 27,
}

INVENTORY_PATH = Path("resources/assistant-corpus/radio-astronomy-source-inventory-v1.json")
PACK_ROOT = Path("apps/casars-mac/Sources/CasarsMacCore/Resources/assistant-corpus")
ORIGIN_AUDIT_PATH = Path(
    "resources/assistant-corpus/radio-astronomy-origin-audit-2026-07-15.json"
)
PACK_SCHEMA_VERSION = 3
SHA256 = re.compile(r"^[0-9a-f]{64}$")

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
    subcommands = parser.add_subparsers(dest="command", required=True)

    generate = subcommands.add_parser("generate", help="reproducibly generate inventory and pack")
    generate.add_argument(
        "--oracle-root",
        type=Path,
        default=Path(os.environ.get("RADIO_ASTRONOMY_ORACLE_ROOT", "")),
        help="RadioAstronomyOracle checkout (or set RADIO_ASTRONOMY_ORACLE_ROOT)",
    )
    generate.add_argument(
        "--output",
        type=Path,
        default=INVENTORY_PATH,
    )
    generate.add_argument(
        "--pack-root",
        type=Path,
        default=PACK_ROOT,
        help="Destination for the clean-install compact baseline resource pack",
    )
    generate.add_argument(
        "--check",
        action="store_true",
        help="compare reproducible output without mutating the checkout",
    )

    check = subcommands.add_parser("check", help="validate the committed inventory and pack")
    check.add_argument("--inventory", type=Path, default=INVENTORY_PATH)
    check.add_argument("--pack-root", type=Path, default=PACK_ROOT)
    check.add_argument("--origin-audit", type=Path, default=ORIGIN_AUDIT_PATH)

    audit = subcommands.add_parser("audit", help="probe selected authoritative origins")
    audit.add_argument("--inventory", type=Path, default=INVENTORY_PATH)
    audit.add_argument("--output", type=Path, required=True)
    audit.add_argument("--timeout", type=float, default=30)
    audit.add_argument("--workers", type=int, default=8)
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
    primer_pages = (
        json.dumps(
            [{"page": 1, "content": primer_path.read_text(encoding="utf-8")}],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    bundle_files["standard-v1/radio-interferometry-primer.pages.json"] = primer_pages
    documents: list[dict[str, Any]] = [{
        "path": "standard-v1/radio-interferometry-primer.pages.json",
        "format": "normalized_pages_json",
        "title": "CASA-RS Radio Interferometry Primer",
        "citation_label": "CASA-RS Radio Interferometry Primer v1.0",
        "citation_kind": "document",
        "source_path": "radio-interferometry-primer.md",
        "content_sha256": hashlib.sha256(primer_pages).hexdigest(),
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
        "schema_version": PACK_SCHEMA_VERSION,
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


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"assistant-corpus pack: {message}")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"assistant-corpus pack: read {path}: {error}") from error
    require(isinstance(value, dict), f"{path} must contain one JSON object")
    return value


def safe_pack_path(pack_root: Path, relative: str) -> Path:
    candidate = Path(relative)
    require(not candidate.is_absolute(), f"absolute pack path is forbidden: {relative}")
    require(".." not in candidate.parts, f"parent traversal is forbidden: {relative}")
    require(candidate.parts[:1] == ("standard-v1",), f"content must live under standard-v1: {relative}")
    resolved_root = pack_root.resolve()
    resolved = (pack_root / candidate).resolve()
    require(resolved.is_relative_to(resolved_root), f"pack path escapes resource root: {relative}")
    return resolved


def check_committed(inventory_path: Path, pack_root: Path, origin_audit_path: Path) -> None:
    payload = load_json(inventory_path)
    require(payload.get("schema_version") == 1, "unsupported inventory schema")
    sources = payload.get("sources")
    require(isinstance(sources, list), "inventory sources must be an array")
    counts = {
        "sources": len(sources),
        "pages_or_slides": sum(source["page_or_slide_count"] for source in sources),
        "books": sum(source["source_class"] == "book" for source in sources),
        "nrao_synthesis_2024": sum(
            source["collection_id"] == "nrao-synthesis-2024" for source in sources
        ),
        "nrao_synthesis_2026": sum(
            source["collection_id"] == "nrao-synthesis-2026" for source in sources
        ),
    }
    require(counts == EXPECTED_COUNTS, f"inventory release invariant drifted: {counts}")
    require(payload["source_inventory"]["counts"] == counts, "stored source counts are not derived counts")
    require(len({source["id"] for source in sources}) == len(sources), "source identities must be unique")
    included = [source for source in sources if source["acquisition"]["included_in_baseline"]]
    for source in sources:
        label = source["id"]
        require(bool(source.get("origin_url")), f"{label} has no origin")
        require(SHA256.fullmatch(source.get("oracle_working_copy_sha256", "")) is not None,
                f"{label} has no source digest")
        require(SHA256.fullmatch(source.get("normalized_text_sha256", "")) is not None,
                f"{label} has no normalized-text digest")
        require(bool(source["citation"].get("label")), f"{label} has no citation label")
        require(source["citation"].get("locator_kind") in {"book_page", "slide"},
                f"{label} has no page or slide citation kind")
        require(bool(source["license"].get("id") and source["license"].get("evidence")),
                f"{label} has no license evidence")
        if source["acquisition"]["included_in_baseline"]:
            require(bool(source.get("download_url")), f"{label} has no acquisition URL")
            require(source["redistribution"].get("bundled") is True,
                    f"{label} is selected but not marked bundled")
            require(source.get("bundle") is not None, f"{label} has no bundle record")
            require(bool(source.get("contributors")), f"{label} has no contributors")
            bundle_path = safe_pack_path(pack_root, source["bundle"]["path"])
            require(bundle_path.is_file(), f"{label} bundle content is missing")
            require(digest(bundle_path) == source["bundle"]["sha256"],
                    f"{label} bundle digest does not match")
        else:
            require(bool(source["acquisition"].get("exclusion_reason")),
                    f"{label} has no exclusion reason")
            require(source["redistribution"].get("bundled") is False,
                    f"{label} is excluded but marked bundled")
            require(source.get("bundle") is None, f"{label} is excluded but has bundle content")

    manifest_path = pack_root / "corpus-pack.json"
    manifest = load_json(manifest_path)
    require(manifest.get("schema_version") == PACK_SCHEMA_VERSION,
            f"installed pack must use schema {PACK_SCHEMA_VERSION}")
    require(manifest.get("id") == payload.get("id") and manifest.get("version") == payload.get("version"),
            "installed pack identity differs from inventory")
    documents = manifest.get("documents")
    require(isinstance(documents, list), "installed pack documents must be an array")
    require(len(documents) == len(included) + 1, "installed pack source count differs from selection")
    page_total = 0
    for document in documents:
        relative = document.get("path", "")
        require(document.get("format") == "normalized_pages_json",
                f"unsupported installed content format: {relative}")
        path = safe_pack_path(pack_root, relative)
        require(path.is_file() and not path.is_symlink(), f"installed content is missing or linked: {relative}")
        require(SHA256.fullmatch(document.get("content_sha256", "")) is not None,
                f"installed content digest is missing: {relative}")
        require(digest(path) == document["content_sha256"],
                f"installed content digest differs: {relative}")
        require(SHA256.fullmatch(document.get("source_sha256", "")) is not None,
                f"source digest is missing: {relative}")
        require(bool(document.get("origin_url") and document.get("redistribution_basis")),
                f"origin or redistribution basis is missing: {relative}")
        license_record = document.get("license", {})
        require(bool(license_record.get("id") and license_record.get("name") and license_record.get("url")),
                f"license record is incomplete: {relative}")
        require(bool(document.get("contributors") and document.get("modifications")),
                f"attribution or modifications are missing: {relative}")
        pages = json.loads(path.read_text(encoding="utf-8"))
        require(isinstance(pages, list) and pages,
                f"normalized pages must be a non-empty array: {relative}")
        require(all(
            isinstance(page, dict)
            and isinstance(page.get("page"), int)
            and page["page"] > 0
            and isinstance(page.get("content"), str)
            and page["content"].strip()
            for page in pages
        ), f"normalized page content is invalid: {relative}")
        require([page["page"] for page in pages] == list(range(1, len(pages) + 1)),
                f"normalized page numbering is not contiguous: {relative}")
        page_total += len(pages)
    expected_pages = 1 + sum(source["page_or_slide_count"] for source in included)
    require(page_total == expected_pages, f"installed page count is {page_total}, expected {expected_pages}")

    notice = (pack_root / "NOTICE.md").read_text(encoding="utf-8")
    require("A. Richard Thompson" in notice, "installed notice omits book authors")
    require("Creative Commons Attribution-NonCommercial 4.0" in notice,
            "installed notice omits book license")
    require("images are omitted" in notice, "installed notice omits text-normalization disclosure")

    audit = load_json(origin_audit_path)
    require(audit.get("inventory_id") == payload.get("id"), "origin audit targets another inventory")
    require(audit.get("inventory_version") == payload.get("version"), "origin audit targets another version")
    require(audit["summary"].get("declared_included") == len(included),
            "origin audit omitted selected sources")
    require(audit["summary"].get("reachable") == len(included) and audit["summary"].get("failed") == 0,
            "selected authoritative origins did not all pass the recorded audit")
    print(f"assistant-corpus pack: {len(sources)} sources, {len(documents)} installed documents, {page_total} pages")


def probe_origin(source: dict[str, Any], timeout: float) -> dict[str, Any]:
    url = source["download_url"]
    headers = {"User-Agent": "casa-rs-corpus-audit/1"}
    request = urllib.request.Request(url, method="HEAD", headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if error.code not in {400, 403, 405}:
            raise
        response = urllib.request.urlopen(
            urllib.request.Request(url, headers={**headers, "Range": "bytes=0-0"}),
            timeout=timeout,
        )
    with response:
        response_headers = response.headers
        final_url = response.geturl()
        status = response.status
        length = response_headers.get("Content-Length")
    return {
        "id": source["id"],
        "requested_url": url,
        "final_url": final_url,
        "status": status,
        "content_length": int(length) if length and length.isdigit() else None,
        "oracle_working_copy_size": source["source_size_bytes"],
        "size_matches_oracle_working_copy": (
            int(length) == source["source_size_bytes"] if length and length.isdigit() else False
        ),
        "etag": response_headers.get("ETag"),
        "last_modified": response_headers.get("Last-Modified"),
    }


def audit_origins(inventory_path: Path, output: Path, timeout: float, workers: int) -> None:
    inventory = load_json(inventory_path)
    included = [
        source for source in inventory["sources"]
        if source["acquisition"]["included_in_baseline"]
    ]
    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(probe_origin, source, timeout): source for source in included}
        for future in concurrent.futures.as_completed(futures):
            source = futures[future]
            try:
                results.append(future.result())
            except Exception as error:  # the audit artifact records every failure
                failures.append({"id": source["id"], "url": source["download_url"], "error": str(error)})
    results.sort(key=lambda item: item["id"])
    failures.sort(key=lambda item: item["id"])
    payload = {
        "schema_version": 1,
        "inventory_id": inventory["id"],
        "inventory_version": inventory["version"],
        "measured_at": dt.datetime.now(dt.UTC).isoformat(),
        "summary": {
            "declared_included": len(included),
            "reachable": len(results),
            "failed": len(failures),
            "known_download_bytes": sum(item["content_length"] or 0 for item in results),
            "unknown_content_lengths": sum(item["content_length"] is None for item in results),
            "sizes_matching_oracle_working_copy": sum(
                item["size_matches_oracle_working_copy"] for item in results
            ),
        },
        "results": results,
        "failures": failures,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))
    if failures:
        raise SystemExit("one or more selected authoritative origins failed")


def generate(args: argparse.Namespace) -> None:
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


def main() -> None:
    args = parse_args()
    if args.command == "generate":
        generate(args)
    elif args.command == "check":
        check_committed(args.inventory, args.pack_root, args.origin_audit)
    else:
        audit_origins(args.inventory, args.output, args.timeout, args.workers)


if __name__ == "__main__":
    main()
