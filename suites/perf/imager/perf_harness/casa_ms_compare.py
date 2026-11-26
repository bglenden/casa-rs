#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

import json
import math
import pathlib
import sys
import numpy as np
from casatools import table

request_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
request = json.loads(request_path.read_text(encoding="utf-8"))
MODE = request.get("mode")
if MODE not in {"full", "sampled", "aca_pairs"}:
    raise ValueError("casa_ms_compare.py requires mode=full, sampled, or aca_pairs")
SAMPLE_ROWS = int(request["sample_rows"])

def write_result(value):
    output_path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

KEY_COLUMNS = ["TIME", "FIELD_ID", "DATA_DESC_ID", "ANTENNA1", "ANTENNA2"]
ACA_SUBTABLES = [
    "ANTENNA",
    "FIELD",
    "POINTING",
    "PROCESSOR",
    "SPECTRAL_WINDOW",
    "POLARIZATION",
    "DATA_DESCRIPTION",
    "OBSERVATION",
]

def open_table(path):
    tb = table()
    tb.open(path)
    return tb

def aca_subtable_rows(path):
    rows = {}
    for name in ACA_SUBTABLES:
        tb = open_table(path + "/" + name)
        try:
            rows[name] = int(tb.nrows())
        finally:
            tb.close()
    return rows

def aca_field_centers(path):
    tb = open_table(path + "/FIELD")
    try:
        phase_dir = np.asarray(tb.getcol("PHASE_DIR"), dtype=np.float64)
    finally:
        tb.close()
    if phase_dir.ndim == 3:
        return phase_dir[:, 0, :].T
    if phase_dir.ndim == 2:
        return phase_dir.T
    return np.zeros((0, 2), dtype=np.float64)

def summarize_aca_ms(path):
    tb = open_table(path)
    try:
        rows = int(tb.nrows())
        colnames = tb.colnames()
        data_shape = list(np.asarray(tb.getcell("DATA", 0)).shape)
        times = np.asarray(tb.getcol("TIME"), dtype=np.float64)
        field_ids = np.asarray(tb.getcol("FIELD_ID"), dtype=np.int64)
        antenna1 = np.asarray(tb.getcol("ANTENNA1"), dtype=np.int64)
        antenna2 = np.asarray(tb.getcol("ANTENNA2"), dtype=np.int64)
        flags = np.asarray(tb.getcol("FLAG"), dtype=bool)
        flag_rows = np.asarray(tb.getcol("FLAG_ROW"), dtype=bool)
        weight0 = np.asarray(tb.getcell("WEIGHT", 0), dtype=np.float64)
        sigma0 = np.asarray(tb.getcell("SIGMA", 0), dtype=np.float64)
    finally:
        tb.close()
    spw = open_table(path + "/SPECTRAL_WINDOW")
    try:
        chan_freq = np.asarray(spw.getcell("CHAN_FREQ", 0), dtype=np.float64)
        chan_width = np.asarray(spw.getcell("CHAN_WIDTH", 0), dtype=np.float64)
    finally:
        spw.close()
    centers = aca_field_centers(path)
    return {
        "rows": rows,
        "columns": colnames,
        "data_shape": data_shape,
        "time": {
            "first": float(times[0]),
            "last": float(times[-1]),
            "unique": int(np.unique(times).size),
        },
        "field_unique": int(np.unique(field_ids).size),
        "field_centers_rad": [[float(row[0]), float(row[1])] for row in centers],
        "antenna_unique": int(np.unique(np.concatenate([antenna1, antenna2])).size),
        "flag_counts": {
            "flag_true_cells": int(np.count_nonzero(flags)),
            "flag_row_true_rows": int(np.count_nonzero(flag_rows)),
            "effective_flag_true_cells": int(
                np.count_nonzero(flags | flag_rows.reshape(1, 1, -1))
            ),
        },
        "first_weight": [float(value) for value in weight0],
        "first_sigma": [float(value) for value in sigma0],
        "spw": {
            "chan_freq_hz": [float(value) for value in chan_freq],
            "chan_width_hz": [float(value) for value in chan_width],
        },
        "subtable_rows": aca_subtable_rows(path),
    }

def aca_field_center_delta(native, casa):
    native_centers = np.asarray(native["field_centers_rad"], dtype=np.float64)
    casa_centers = np.asarray(casa["field_centers_rad"], dtype=np.float64)
    if native_centers.shape != casa_centers.shape:
        return {
            "shape": [list(native_centers.shape), list(casa_centers.shape)],
            "max_abs": math.inf,
        }
    if native_centers.size == 0:
        return {
            "shape": [list(native_centers.shape), list(casa_centers.shape)],
            "max_abs": 0.0,
        }
    return {
        "shape": [list(native_centers.shape), list(casa_centers.shape)],
        "max_abs": float(np.max(np.abs(native_centers - casa_centers))),
    }

def sampled_aca_value_deltas(native_path, casa_path):
    native_tb = open_table(native_path)
    casa_tb = open_table(casa_path)
    try:
        rows = int(native_tb.nrows())
        if rows <= SAMPLE_ROWS:
            sample_rows = list(range(rows))
        else:
            sample_rows = sorted(
                set(np.linspace(0, rows - 1, SAMPLE_ROWS, dtype=np.int64).tolist())
            )
        uvw_max_abs = 0.0
        data_max_abs = 0.0
        data_sum_abs = 0.0
        data_casa_sum_abs = 0.0
        data_count = 0
        data_max_relative = 0.0
        data_amplitude_ratios = []
        flag_mismatches = 0
        flag_row_mismatches = 0
        weight_max_abs = 0.0
        sigma_max_abs = 0.0
        worst = []
        for row in sample_rows:
            row = int(row)
            native_uvw = np.asarray(native_tb.getcell("UVW", row), dtype=np.float64)
            casa_uvw = np.asarray(casa_tb.getcell("UVW", row), dtype=np.float64)
            uvw_max_abs = max(uvw_max_abs, float(np.max(np.abs(native_uvw - casa_uvw))))
            native_data = np.asarray(native_tb.getcell("DATA", row))
            casa_data = np.asarray(casa_tb.getcell("DATA", row))
            native_flag = np.asarray(native_tb.getcell("FLAG", row), dtype=bool)
            casa_flag = np.asarray(casa_tb.getcell("FLAG", row), dtype=bool)
            native_flag_row = bool(native_tb.getcell("FLAG_ROW", row))
            casa_flag_row = bool(casa_tb.getcell("FLAG_ROW", row))
            flag_mismatches += int(np.count_nonzero(native_flag != casa_flag))
            flag_row_mismatches += int(native_flag_row != casa_flag_row)
            mask = ~(native_flag | casa_flag | native_flag_row | casa_flag_row)
            delta = np.abs(native_data - casa_data)
            if np.any(mask):
                selected = delta[mask]
                casa_amp = np.abs(casa_data)[mask]
                data_max_abs = max(data_max_abs, float(np.max(selected)))
                data_sum_abs += float(np.sum(selected))
                data_casa_sum_abs += float(np.sum(casa_amp))
                data_count += int(selected.size)
                relative = selected / np.maximum(casa_amp, 1.0e-12)
                data_max_relative = max(data_max_relative, float(np.max(relative)))
                ratio_mask = casa_amp > 1.0e-9
                if np.any(ratio_mask):
                    data_amplitude_ratios.extend(
                        (
                            np.abs(native_data)[mask][ratio_mask]
                            / casa_amp[ratio_mask]
                        ).ravel().tolist()
                    )
                ordinal = int(np.argmax(selected))
                corr, chan = np.argwhere(mask)[ordinal]
                worst.append(
                    {
                        "row": row,
                        "correlation": int(corr),
                        "channel": int(chan),
                        "abs": float(selected[ordinal]),
                        "relative": float(relative[ordinal]),
                        "native_abs": float(abs(native_data[corr, chan])),
                        "casa_abs": float(abs(casa_data[corr, chan])),
                    }
                )
            native_weight = np.asarray(native_tb.getcell("WEIGHT", row), dtype=np.float64)
            casa_weight = np.asarray(casa_tb.getcell("WEIGHT", row), dtype=np.float64)
            native_sigma = np.asarray(native_tb.getcell("SIGMA", row), dtype=np.float64)
            casa_sigma = np.asarray(casa_tb.getcell("SIGMA", row), dtype=np.float64)
            weight_max_abs = max(
                weight_max_abs, float(np.max(np.abs(native_weight - casa_weight)))
            )
            sigma_max_abs = max(
                sigma_max_abs, float(np.max(np.abs(native_sigma - casa_sigma)))
            )
    finally:
        native_tb.close()
        casa_tb.close()
    ratio_values = np.asarray(data_amplitude_ratios, dtype=np.float64)
    ratio_summary = {
        "count": int(ratio_values.size),
        "mean": float(np.mean(ratio_values)) if ratio_values.size else 0.0,
        "median": float(np.median(ratio_values)) if ratio_values.size else 0.0,
        "p05": float(np.quantile(ratio_values, 0.05)) if ratio_values.size else 0.0,
        "p95": float(np.quantile(ratio_values, 0.95)) if ratio_values.size else 0.0,
        "min": float(np.min(ratio_values)) if ratio_values.size else 0.0,
        "max": float(np.max(ratio_values)) if ratio_values.size else 0.0,
    }
    casa_mean_abs = data_casa_sum_abs / data_count if data_count else 0.0
    mean_abs = data_sum_abs / data_count if data_count else 0.0
    return {
        "rows_sampled": len(sample_rows),
        "uvw": {"max_abs": uvw_max_abs},
        "data": {
            "compared_unflagged_cells": data_count,
            "max_abs": data_max_abs,
            "mean_abs": mean_abs,
            "casa_mean_abs": casa_mean_abs,
            "mean_abs_over_casa_mean": mean_abs / casa_mean_abs if casa_mean_abs else 0.0,
            "max_relative": data_max_relative,
            "amplitude_ratio": ratio_summary,
            "worst_cells": worst[-10:],
        },
        "flag_mismatches": flag_mismatches,
        "flag_row_mismatches": flag_row_mismatches,
        "weight": {"max_abs": weight_max_abs},
        "sigma": {"max_abs": sigma_max_abs},
    }

def compare_aca_pair(pair):
    native = summarize_aca_ms(pair["native_ms"])
    casa = summarize_aca_ms(pair["casa_ms"])
    reasons = []
    for key in ["rows", "data_shape", "field_unique", "antenna_unique", "subtable_rows", "spw"]:
        if native[key] != casa[key]:
            reasons.append(f"{key} differs")
    for key in ["first", "last", "unique"]:
        if abs(native["time"][key] - casa["time"][key]) > 1.0e-6:
            reasons.append(f"time.{key} differs")
    fields = aca_field_center_delta(native, casa)
    if fields["max_abs"] > 1.0e-10:
        reasons.append("FIELD phase centers differ")
    deltas = sampled_aca_value_deltas(pair["native_ms"], pair["casa_ms"])
    if deltas["uvw"]["max_abs"] > 1.0e-3:
        reasons.append("sampled UVW differs")
    if deltas["flag_mismatches"] or deltas["flag_row_mismatches"]:
        reasons.append("sampled FLAG/FLAG_ROW differs")
    if deltas["weight"]["max_abs"] > 1.0e-6:
        reasons.append("sampled WEIGHT differs")
    if deltas["sigma"]["max_abs"] > 1.0e-6:
        reasons.append("sampled SIGMA differs")
    data = deltas["data"]
    ratio = data["amplitude_ratio"]
    if (
        data["mean_abs_over_casa_mean"] > 0.06
        or abs(ratio["median"] - 1.0) > 0.08
        or ratio["p05"] < 0.85
        or ratio["p95"] > 1.15
    ):
        reasons.append("sampled DATA differs")
    return {
        "id": pair["id"],
        "status": "passed" if not reasons else "failed",
        "reasons": reasons,
        "native_ms": pair["native_ms"],
        "casa_ms": pair["casa_ms"],
        "native": native,
        "casa": casa,
        "field_center_deltas": fields,
        "sampled_deltas": deltas,
    }

if MODE == "aca_pairs":
    pair_results = [compare_aca_pair(pair) for pair in request["pairs"]]
    failed_pairs = [entry for entry in pair_results if entry["status"] != "passed"]
    write_result(
        {
            "status": "passed" if not failed_pairs else "failed",
            "reason": (
                "CASA/native MS comparison failed"
                if failed_pairs
                else "CASA/native MS comparison passed"
            ),
            "pairs": pair_results,
        }
    )
    raise SystemExit(0)

native_path = request["native_path"]
casa_path = request["casa_path"]
uvw_atol = float(request["uvw_atol"])
data_atol = float(request["data_atol"])
data_rtol = float(request["data_rtol"])

def key_tuple(keys, row):
    return tuple(key[row].item() if hasattr(key[row], "item") else key[row] for key in keys)

def phase_residual_diagnostics(samples):
    if not samples:
        return {"samples": 0, "fields": {}}
    values = np.asarray(samples, dtype=np.float64)
    fields = {}
    for field_id in sorted(set(values[:, 0].astype(int))):
        field_values = values[values[:, 0] == field_id]
        if field_values.shape[0] < 4:
            continue
        high_amplitude_cut = np.percentile(field_values[:, 7], 50)
        fit_values = field_values[field_values[:, 7] >= high_amplitude_cut]
        if fit_values.shape[0] < 4:
            fit_values = field_values
        design = np.column_stack([
            fit_values[:, 2],
            fit_values[:, 3],
            fit_values[:, 4],
            np.ones(fit_values.shape[0]),
        ])
        phase = fit_values[:, 5]
        beta, *_ = np.linalg.lstsq(design, phase, rcond=None)
        residual = phase - design @ beta
        amp_ratio = fit_values[:, 6]
        abs_delta = fit_values[:, 8]
        fields[str(field_id)] = {
            "samples": int(field_values.shape[0]),
            "fit_samples": int(fit_values.shape[0]),
            "phase_fit_rad_per_lambda": {
                "u": float(beta[0]),
                "v": float(beta[1]),
                "w": float(beta[2]),
                "constant": float(beta[3]),
            },
            "phase_residual_rms_rad": float(np.sqrt(np.mean(residual * residual))),
            "phase_min_rad": float(phase.min()),
            "phase_max_rad": float(phase.max()),
            "amplitude_ratio": {
                "mean": float(amp_ratio.mean()),
                "std": float(amp_ratio.std()),
                "min": float(amp_ratio.min()),
                "max": float(amp_ratio.max()),
            },
            "abs_delta": {
                "mean": float(abs_delta.mean()),
                "max": float(abs_delta.max()),
            },
        }
    return {"samples": int(values.shape[0]), "fields": fields}

def read_keys(path):
    tb = open_table(path)
    try:
        rows = int(tb.nrows())
        keys = [np.asarray(tb.getcol(column)) for column in KEY_COLUMNS]
    finally:
        tb.close()
    return rows, keys

def flag_counts(path, chunk_rows=8192):
    tb = open_table(path)
    try:
        rows = int(tb.nrows())
        flag_true = 0
        flag_row_true = 0
        effective_true = 0
        for start in range(0, rows, chunk_rows):
            count = min(chunk_rows, rows - start)
            flag = np.asarray(tb.getcol("FLAG", startrow=start, nrow=count), dtype=bool)
            flag_row = np.asarray(tb.getcol("FLAG_ROW", startrow=start, nrow=count), dtype=bool)
            flag_true += int(np.count_nonzero(flag))
            flag_row_true += int(np.count_nonzero(flag_row))
            effective_true += int(np.count_nonzero(flag | flag_row.reshape(1, 1, -1)))
    finally:
        tb.close()
    return {
        "flag_true_cells": flag_true,
        "flag_row_true_rows": flag_row_true,
        "effective_flag_true_cells": effective_true,
    }

native_rows, native_keys = read_keys(native_path)
casa_rows, casa_keys = read_keys(casa_path)
result = {
    "status": "passed",
    "reasons": [],
    "sampled": MODE == "sampled",
    "row_policy": MODE,
    "row_count": {"native": native_rows, "casa": casa_rows},
    "flag_counts": {
        "native": flag_counts(native_path),
        "casa": flag_counts(casa_path),
    },
    "thresholds": {
        "uvw_atol": uvw_atol,
        "data_atol": data_atol,
        "data_rtol": data_rtol,
    },
}
if result["flag_counts"]["native"] != result["flag_counts"]["casa"]:
    result["status"] = "failed"
    result["reasons"].append("strict total FLAG/FLAG_ROW counts differ")
if native_rows != casa_rows:
    result["status"] = "failed"
    result["reasons"].append("strict row count mismatch")
    write_result(result)
    raise SystemExit(0)

casa_by_key = {key_tuple(casa_keys, row): row for row in range(casa_rows)}
if MODE == "full" or native_rows <= SAMPLE_ROWS:
    native_sample_rows = list(range(native_rows))
else:
    native_sample_rows = sorted(set(np.linspace(0, native_rows - 1, SAMPLE_ROWS, dtype=np.int64).tolist()))

native_tb = open_table(native_path)
casa_tb = open_table(casa_path)
spw_tb = open_table(casa_path + "/SPECTRAL_WINDOW")
try:
    channel_frequencies_hz = np.asarray(spw_tb.getcell("CHAN_FREQ", 0), dtype=np.float64)
    uvw_max_abs = 0.0
    uvw_mean_sum = 0.0
    uvw_count = 0
    data_max_abs = 0.0
    data_sum_abs = 0.0
    data_max_relative = 0.0
    data_violation_count = 0
    data_violation_max_abs = 0.0
    data_violation_max_relative = 0.0
    data_count = 0
    raw_flag_mismatches = 0
    effective_flag_mismatches = 0
    weight_max_abs = 0.0
    sigma_max_abs = 0.0
    missing_keys = []
    worst_cells = []
    phase_samples = []

    for native_row in native_sample_rows:
        key = key_tuple(native_keys, native_row)
        casa_row = casa_by_key.get(key)
        if casa_row is None:
            missing_keys.append({"native_row": int(native_row), "key": list(key)})
            continue

        native_uvw = np.asarray(native_tb.getcell("UVW", int(native_row)), dtype=np.float64)
        casa_uvw = np.asarray(casa_tb.getcell("UVW", int(casa_row)), dtype=np.float64)
        uvw_delta = np.abs(native_uvw - casa_uvw)
        uvw_max_abs = max(uvw_max_abs, float(uvw_delta.max()) if uvw_delta.size else 0.0)
        uvw_mean_sum += float(uvw_delta.sum())
        uvw_count += int(uvw_delta.size)

        native_data = np.asarray(native_tb.getcell("DATA", int(native_row)))
        casa_data = np.asarray(casa_tb.getcell("DATA", int(casa_row)))
        native_flag = np.asarray(native_tb.getcell("FLAG", int(native_row)), dtype=bool)
        casa_flag = np.asarray(casa_tb.getcell("FLAG", int(casa_row)), dtype=bool)
        native_flag_row = bool(native_tb.getcell("FLAG_ROW", int(native_row)))
        casa_flag_row = bool(casa_tb.getcell("FLAG_ROW", int(casa_row)))
        native_effective_flag = native_flag | native_flag_row
        casa_effective_flag = casa_flag | casa_flag_row

        raw_flag_mismatches += int(np.count_nonzero(native_flag != casa_flag))
        effective_flag_mismatches += int(np.count_nonzero(native_effective_flag != casa_effective_flag))
        mask = ~(native_effective_flag | casa_effective_flag)
        if np.any(mask):
            delta = np.abs(native_data - casa_data)
            amp = np.abs(casa_data)
            selected_delta = delta[mask]
            selected_amp = amp[mask]
            relative = selected_delta / np.maximum(selected_amp, data_atol)
            row_max_abs = float(selected_delta.max())
            row_max_relative = float(relative.max())
            data_max_abs = max(data_max_abs, row_max_abs)
            data_max_relative = max(data_max_relative, row_max_relative)
            data_sum_abs += float(selected_delta.sum())
            data_count += int(selected_delta.size)
            violation_mask = (selected_delta > data_atol) & (relative > data_rtol)
            if np.any(violation_mask):
                data_violation_count += int(np.count_nonzero(violation_mask))
                violation_delta = selected_delta[violation_mask]
                violation_relative = relative[violation_mask]
                data_violation_max_abs = max(
                    data_violation_max_abs,
                    float(violation_delta.max()),
                )
                data_violation_max_relative = max(
                    data_violation_max_relative,
                    float(violation_relative.max()),
                )
            if row_max_abs == data_max_abs or row_max_relative == data_max_relative:
                corr, chan = np.argwhere(mask)[int(np.argmax(selected_delta))]
                worst_cells.append({
                    "native_row": int(native_row),
                    "casa_row": int(casa_row),
                    "correlation": int(corr),
                    "channel": int(chan),
                    "abs": row_max_abs,
                    "relative": row_max_relative,
                    "native": {
                        "real": float(native_data[corr, chan].real),
                        "imag": float(native_data[corr, chan].imag),
                    },
                    "casa": {
                        "real": float(casa_data[corr, chan].real),
                        "imag": float(casa_data[corr, chan].imag),
                    },
                })
            unflagged_cells = np.argwhere(mask)
            for corr, chan in unflagged_cells:
                # Correlations are identical for these scalar simulation
                # products; keep one polarization so fits describe rows and
                # channels rather than double-counting the same residual.
                if int(corr) != 0:
                    continue
                casa_value = casa_data[corr, chan]
                native_value = native_data[corr, chan]
                casa_amplitude = abs(casa_value)
                if casa_amplitude <= data_atol:
                    continue
                ratio = native_value / casa_value
                frequency_hz = float(channel_frequencies_hz[int(chan)])
                phase_samples.append((
                    int(key[1]),
                    int(chan),
                    float(native_uvw[0] * frequency_hz / 299792458.0),
                    float(native_uvw[1] * frequency_hz / 299792458.0),
                    float(native_uvw[2] * frequency_hz / 299792458.0),
                    float(np.angle(ratio)),
                    float(abs(ratio)),
                    float(casa_amplitude),
                    float(delta[corr, chan]),
                ))

        native_weight = np.asarray(native_tb.getcell("WEIGHT", int(native_row)), dtype=np.float64)
        casa_weight = np.asarray(casa_tb.getcell("WEIGHT", int(casa_row)), dtype=np.float64)
        native_sigma = np.asarray(native_tb.getcell("SIGMA", int(native_row)), dtype=np.float64)
        casa_sigma = np.asarray(casa_tb.getcell("SIGMA", int(casa_row)), dtype=np.float64)
        weight_max_abs = max(weight_max_abs, float(np.abs(native_weight - casa_weight).max()))
        sigma_max_abs = max(sigma_max_abs, float(np.abs(native_sigma - casa_sigma).max()))
finally:
    spw_tb.close()
    native_tb.close()
    casa_tb.close()

result["rows_sampled"] = len(native_sample_rows)
result["rows_compared"] = len(native_sample_rows)
result["missing_key_count"] = len(missing_keys)
if missing_keys:
    result["missing_keys"] = missing_keys[:10]
    result["status"] = "failed"
    result["reasons"].append(f"strict {MODE} row key missing in CASA")
result["uvw"] = {
    "max_abs": uvw_max_abs,
    "mean_abs": uvw_mean_sum / uvw_count if uvw_count else 0.0,
}
result["data"] = {
    "compared_unflagged_cells": data_count,
    "max_abs": data_max_abs,
    "mean_abs": data_sum_abs / data_count if data_count else 0.0,
    "max_relative": data_max_relative,
    "violating_cells": data_violation_count,
    "violation_max_abs": data_violation_max_abs,
    "violation_max_relative": data_violation_max_relative,
    "worst_cells": worst_cells[-10:],
}
result["phase_residual_diagnostics"] = phase_residual_diagnostics(phase_samples)
result["raw_flag_mismatches"] = raw_flag_mismatches
result["effective_flag_mismatches"] = effective_flag_mismatches
result["weight"] = {"max_abs": weight_max_abs}
result["sigma"] = {"max_abs": sigma_max_abs}

if uvw_max_abs > uvw_atol:
    result["status"] = "failed"
    result["reasons"].append(f"strict {MODE} UVW max abs {uvw_max_abs:.6g} exceeds {uvw_atol:.6g}")
if data_violation_count:
    result["status"] = "failed"
    result["reasons"].append(
        f"strict {MODE} DATA has {data_violation_count} cells exceeding both tolerances "
        f"(max abs {data_violation_max_abs:.6g}, max rel {data_violation_max_relative:.6g})"
    )
if effective_flag_mismatches:
    result["status"] = "failed"
    result["reasons"].append(f"strict {MODE} effective FLAG differs in {effective_flag_mismatches} cells")
if weight_max_abs > 1.0e-6:
    result["status"] = "failed"
    result["reasons"].append(f"strict {MODE} WEIGHT max abs {weight_max_abs:.6g} exceeds 1e-6")
if sigma_max_abs > 1.0e-6:
    result["status"] = "failed"
    result["reasons"].append(f"strict {MODE} SIGMA max abs {sigma_max_abs:.6g} exceeds 1e-6")

write_result(result)
