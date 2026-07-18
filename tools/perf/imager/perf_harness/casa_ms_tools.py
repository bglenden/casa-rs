#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Checked-in CASA-side MeasurementSet utility protocol."""

from __future__ import annotations

import json
import pathlib
import sys


def main() -> None:
    request_path = pathlib.Path(sys.argv[1])
    output_path = pathlib.Path(sys.argv[2])
    request = json.loads(request_path.read_text(encoding="utf-8"))
    operation = request.get("operation")
    if operation == "generate_simobserve_dataset":
        output = generate_simobserve_dataset(request["dataset"])
    elif operation == "first_ms_time":
        output = first_ms_time(request["path"])
    elif operation == "inspect_measurement_sets":
        output = inspect_measurement_sets(request["paths"])
    elif operation == "field_centers":
        output = field_centers(request["specs"])
    else:
        raise ValueError(f"unsupported CASA MS utility operation: {operation!r}")
    output_path.write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def generate_simobserve_dataset(dataset: dict) -> dict:
    tool_dir = pathlib.Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(tool_dir))
    import generate_wave1_casa_datasets as generator

    return generator.generate_dataset(
        dataset,
        skip_existing=False,
        overwrite=True,
        preview=False,
        preview_max_pixels=128,
    )


def first_ms_time(path: str) -> dict:
    from casatools import table

    handle = table()
    handle.open(path)
    try:
        value = float(handle.getcell("TIME", 0))
    finally:
        handle.close()
    return {"first_time": value}


def inspect_measurement_sets(paths: dict[str, str]) -> dict:
    import numpy as np
    from casatools import table

    def subtable_rows(path: str, name: str) -> int:
        handle = table()
        try:
            handle.open(path + "/" + name)
            return int(handle.nrows())
        finally:
            try:
                handle.close()
            except Exception:
                pass

    def stats(values) -> dict:
        array = np.asarray(values)
        return {
            "min": float(array.min()),
            "max": float(array.max()),
            "mean": float(array.mean()),
        }

    def data_stats(cell) -> dict:
        array = np.asarray(cell)
        amplitude = np.abs(array)
        return {
            "shape": list(array.shape),
            "abs_sum": float(amplitude.sum()),
            "abs_max": float(amplitude.max()),
            "real_mean": float(array.real.mean()),
            "imag_mean": float(array.imag.mean()),
        }

    def inspect(path: str) -> dict:
        handle = table()
        handle.open(path)
        try:
            rows = int(handle.nrows())
            columns = handle.colnames()
            data = handle.getcell("DATA", 0)
            uvw = handle.getcell("UVW", 0)
            selected = sorted({0, max(0, rows // 2), max(0, rows - 1)})
            selected_data = {
                str(row): data_stats(handle.getcell("DATA", row)) for row in selected
            }
            uvw_column = handle.getcol("UVW")
        finally:
            handle.close()
        return {
            "rows": rows,
            "columns": columns,
            "complex_visibility_columns": [
                column
                for column in ["DATA", "MODEL_DATA", "CORRECTED_DATA", "FLOAT_DATA"]
                if column in columns
            ],
            "data_shape": list(data.shape),
            "first_data_abs_sum": float(abs(data).sum()),
            "first_uvw": [float(value) for value in uvw],
            "uvw_stats": {
                "u_m": stats(uvw_column[0, :]),
                "v_m": stats(uvw_column[1, :]),
                "w_m": stats(uvw_column[2, :]),
            },
            "selected_data_stats": selected_data,
            "subtable_rows": {
                name: subtable_rows(path, name)
                for name in [
                    "FIELD",
                    "SPECTRAL_WINDOW",
                    "DATA_DESCRIPTION",
                    "OBSERVATION",
                    "POINTING",
                ]
            },
        }

    return {name: inspect(path) for name, path in paths.items()}


def field_centers(specs: list[dict]) -> dict:
    import numpy as np
    from casatools import table

    result = {}
    for spec in specs:
        handle = table()
        handle.open(spec["casa_ms"] + "/FIELD")
        try:
            phase_dir = np.asarray(handle.getcol("PHASE_DIR"), dtype=np.float64)
        finally:
            handle.close()
        if phase_dir.ndim == 3:
            centers = phase_dir[:, 0, :].T
        elif phase_dir.ndim == 2:
            centers = phase_dir.T
        else:
            raise RuntimeError(
                f"unexpected PHASE_DIR shape {phase_dir.shape} in {spec['casa_ms']}"
            )
        result[spec["native_run"]] = [
            [float(row[0]), float(row[1])] for row in centers
        ]
    return result


if __name__ == "__main__":
    main()
