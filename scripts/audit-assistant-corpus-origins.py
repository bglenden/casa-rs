#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe declared baseline origins without downloading the full corpus."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("resources/assistant-corpus/radio-astronomy-source-inventory-v1.json"),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def probe(source: dict[str, Any], timeout: float) -> dict[str, Any]:
    url = source["download_url"]
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "casa-rs-corpus-audit/1"})
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if error.code not in {400, 403, 405}:
            raise
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "casa-rs-corpus-audit/1", "Range": "bytes=0-0"},
        )
        response = urllib.request.urlopen(request, timeout=timeout)
    with response:
        headers = response.headers
        final_url = response.geturl()
        status = response.status
        length = headers.get("Content-Length")
    if length is None:
        range_request = urllib.request.Request(
            url,
            headers={"User-Agent": "casa-rs-corpus-audit/1", "Range": "bytes=0-0"},
        )
        with urllib.request.urlopen(range_request, timeout=timeout) as ranged:
            headers = ranged.headers
            final_url = ranged.geturl()
            status = ranged.status
            length = headers.get("Content-Length")
            content_range = headers.get("Content-Range")
            if content_range and "/" in content_range:
                length = content_range.rsplit("/", 1)[1]
    content_length = int(length) if length and length.isdigit() else None
    return {
            "id": source["id"],
            "requested_url": url,
            "final_url": final_url,
            "status": status,
            "content_length": content_length,
            "oracle_working_copy_size": source["source_size_bytes"],
            "size_matches_oracle_working_copy": content_length == source["source_size_bytes"],
            "etag": headers.get("ETag"),
            "last_modified": headers.get("Last-Modified"),
        }


def main() -> None:
    args = parse_args()
    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    included = [
        source for source in inventory["sources"]
        if source["acquisition"]["included_in_baseline"]
    ]
    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {executor.submit(probe, source, args.timeout): source for source in included}
        for future in concurrent.futures.as_completed(futures):
            source = futures[future]
            try:
                results.append(future.result())
            except Exception as error:  # audit records every failed origin
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
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))
    if failures:
        raise SystemExit("one or more declared authoritative origins failed")


if __name__ == "__main__":
    main()
