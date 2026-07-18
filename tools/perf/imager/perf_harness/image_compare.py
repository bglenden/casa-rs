# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical imaging-product comparison boundary."""

from __future__ import annotations

import pathlib
from typing import Any

from .casa_protocol import run_json_file_protocol


CASA_IMAGE_COMPARATOR = pathlib.Path(__file__).with_name("casa_image_compare.py")


def compare_products(
    *,
    casa_python: str | None,
    request: dict[str, Any],
    artifact_prefix: pathlib.Path,
    cwd: pathlib.Path,
) -> dict[str, Any]:
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_IMAGE_COMPARATOR,
        request=request,
        request_path=artifact_prefix.with_suffix(".comparison-input.json"),
        output_path=artifact_prefix.with_suffix(".comparison.json"),
        log_path=artifact_prefix.with_suffix(".comparison.log"),
        cwd=cwd,
    )
    if protocol.status == "unavailable":
        return {
            "status": "unavailable",
            "reason": protocol.reason,
            "products": {},
            "input": str(protocol.request_path),
            "log": str(protocol.log_path),
        }
    if protocol.status != "completed" or protocol.output is None:
        return {
            "status": "failed_execution",
            "reason": protocol.reason,
            "return_code": protocol.return_code,
            "products": {},
            "input": str(protocol.request_path),
            "log": str(protocol.log_path),
        }
    comparison = protocol.output
    comparison["input"] = str(protocol.request_path)
    comparison["log"] = str(protocol.log_path)
    return comparison
