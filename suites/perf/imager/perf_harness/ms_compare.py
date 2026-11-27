# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical MeasurementSet comparison boundary and explicit row policy."""

from __future__ import annotations

import pathlib
from typing import Any

from .casa_protocol import run_json_file_protocol


CASA_MS_COMPARATOR = pathlib.Path(__file__).with_name("casa_ms_compare.py")
MS_SAMPLE_ROWS = 513


def compare_measurement_sets(
    *,
    casa_python: str | None,
    native_path: str | pathlib.Path,
    casa_path: str | pathlib.Path,
    mode: str,
    uvw_atol: float,
    data_atol: float,
    data_rtol: float,
    artifact_prefix: pathlib.Path,
    cwd: pathlib.Path,
) -> dict[str, Any]:
    if mode not in {"full", "sampled"}:
        raise ValueError(f"unsupported MeasurementSet comparison mode: {mode}")
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_MS_COMPARATOR,
        request={
            "schema_version": 1,
            "mode": mode,
            "native_path": str(native_path),
            "casa_path": str(casa_path),
            "uvw_atol": uvw_atol,
            "data_atol": data_atol,
            "data_rtol": data_rtol,
            "sample_rows": MS_SAMPLE_ROWS,
        },
        request_path=artifact_prefix.with_suffix(".request.json"),
        output_path=artifact_prefix.with_suffix(".result.json"),
        log_path=artifact_prefix.with_suffix(".log"),
        cwd=cwd,
    )
    if protocol.status != "completed" or protocol.output is None:
        return {
            "status": protocol.status,
            "reason": protocol.reason,
            "return_code": protocol.return_code,
            "comparison_mode": mode,
            "input": str(protocol.request_path),
            "log": str(protocol.log_path),
        }
    result = protocol.output
    result["comparison_mode"] = mode
    result["input"] = str(protocol.request_path)
    result["log"] = str(protocol.log_path)
    return result


def compare_measurement_set_pairs(
    *,
    casa_python: str | None,
    pairs: list[dict[str, str]],
    artifact_prefix: pathlib.Path,
    cwd: pathlib.Path,
) -> dict[str, Any]:
    """Run the ACA/simalma mode through the same canonical comparator facade."""

    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_MS_COMPARATOR,
        request={
            "schema_version": 1,
            "mode": "aca_pairs",
            "pairs": pairs,
            "sample_rows": MS_SAMPLE_ROWS,
        },
        request_path=artifact_prefix.with_suffix(".request.json"),
        output_path=artifact_prefix.with_suffix(".result.json"),
        log_path=artifact_prefix.with_suffix(".log"),
        cwd=cwd,
    )
    if protocol.status != "completed" or protocol.output is None:
        return {
            "status": protocol.status,
            "reason": protocol.reason,
            "return_code": protocol.return_code,
            "comparison_mode": "aca_pairs",
            "input": str(protocol.request_path),
            "log": str(protocol.log_path),
        }
    result = protocol.output
    result["comparison_mode"] = "aca_pairs"
    result["input"] = str(protocol.request_path)
    result["log"] = str(protocol.log_path)
    return result
