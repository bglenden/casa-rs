#!/usr/bin/env python3
"""Freeze one exact CASA AWProject DataToGrid footprint.

This is an isolated semantic diagnostic, not promotion or performance
evidence.  It clones the real VLASS MeasurementSet, leaves exactly one
field-1525 row/channel active, copies the checksum-pinned CASA MODEL_DATA value
into DATA, and makes a one-term dirty AWProject image.  casa-rs can then image
the same scratch MeasurementSet without involving its model-prediction path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

import numpy as np
from casatasks import casalog, tclean
from casatools import table

from vlass_reduced_casa_clean_4096_four_spw import TCLEAN_PARAMETERS


PARALLEL_HANDS = (0, 3)


def flag_all_channel_samples(main_table: table, chunk_rows: int = 4096) -> None:
    """Materialize channel flags so CASA's density pass sees the sparse case.

    CASA's Briggs density construction reads the channel FLAG matrix directly;
    FLAG_ROW alone is therefore insufficient for an isolated sparse oracle.
    """

    row_count = main_table.nrows()
    for start_row in range(0, row_count, chunk_rows):
        rows = min(chunk_rows, row_count - start_row)
        flags = np.asarray(
            main_table.getcol("FLAG", startrow=start_row, nrow=rows),
            dtype=np.bool_,
        )
        flags[...] = True
        main_table.putcol("FLAG", flags, startrow=start_row, nrow=rows)


def json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def choose_sample(
    trace: dict[str, np.ndarray],
    spectral_window: int,
    w_sign: str,
) -> tuple[int, int]:
    w_m = np.asarray(trace["uvw_m"])[:, 2]
    candidate_rows = np.flatnonzero(
        (np.asarray(trace["spectral_window_id"]) == spectral_window)
        & np.asarray(trace["uv_range_selected"], dtype=np.bool_)
        & ((w_m > 0.0) if w_sign == "positive" else (w_m < 0.0))
    )
    flags = np.asarray(trace["flag"], dtype=np.bool_)
    for trace_row in candidate_rows:
        for channel in range(flags.shape[2]):
            if not flags[trace_row, PARALLEL_HANDS, channel].any():
                return int(trace_row), channel
    raise RuntimeError(
        f"no unflagged RR/LL sample for SPW {spectral_window} and {w_sign} W"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-ms", required=True, type=Path)
    parser.add_argument("--prediction-npz", required=True, type=Path)
    parser.add_argument("--scratch-root", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument("--spw", required=True, type=int)
    parser.add_argument("--w-sign", required=True, choices=("positive", "negative"))
    parser.add_argument(
        "--weighting",
        default="briggs",
        choices=("briggs", "natural"),
    )
    parser.add_argument(
        "--reuse-scratch",
        action="store_true",
        help="reuse and verify an already prepared scratch MeasurementSet",
    )
    parser.add_argument(
        "--unflag-cross-hands",
        action="store_true",
        help="unflag zero-valued RL/LR so CASA polarization conversion retains the channel",
    )
    args = parser.parse_args()

    scratch_ms = args.scratch_root / args.source_ms.name
    if args.scratch_root.exists() and not args.reuse_scratch:
        raise RuntimeError(f"refusing to overwrite scratch root: {args.scratch_root}")
    if args.reuse_scratch and not scratch_ms.is_dir():
        raise RuntimeError(f"scratch MeasurementSet is missing: {scratch_ms}")
    if any(args.output_prefix.parent.glob(f"{args.output_prefix.name}.*")):
        raise RuntimeError(f"refusing to overwrite products: {args.output_prefix}.*")

    with np.load(args.prediction_npz) as loaded:
        trace = {name: loaded[name] for name in loaded.files}
    trace_row, channel = choose_sample(trace, args.spw, args.w_sign)
    ms_row = int(trace["row_id"][trace_row])
    model_data = np.asarray(trace["model_data"][trace_row], dtype=np.complex64)

    args.scratch_root.parent.mkdir(parents=True, exist_ok=True)
    if not args.reuse_scratch:
        shutil.copytree(args.source_ms, scratch_ms)
    main_table = table()
    try:
        main_table.open(str(scratch_ms), nomodify=False)
        flag_all_channel_samples(main_table)
        flag_rows = np.ones(main_table.nrows(), dtype=np.bool_)
        flag_rows[ms_row] = False
        main_table.putcol("FLAG_ROW", flag_rows)

        flags = np.ones_like(
            np.asarray(main_table.getcell("FLAG", ms_row), dtype=np.bool_)
        )
        active_correlations = (
            tuple(range(flags.shape[0]))
            if args.unflag_cross_hands
            else PARALLEL_HANDS
        )
        flags[list(active_correlations), channel] = False
        main_table.putcell("FLAG", ms_row, flags)

        data = np.asarray(main_table.getcell("DATA", ms_row), dtype=np.complex64)
        data[:, :] = 0.0
        data[list(PARALLEL_HANDS), channel] = model_data[
            list(PARALLEL_HANDS), channel
        ]
        main_table.putcell("DATA", ms_row, data)
        main_table.flush()
    finally:
        main_table.close()

    parameters = dict(TCLEAN_PARAMETERS)
    parameters.update(
        {
            "vis": str(scratch_ms),
            "imagename": str(args.output_prefix),
            "spw": str(args.spw),
            "deconvolver": "mtmfs",
            "nterms": 1,
            "scales": [0],
            "niter": 0,
            "cycleniter": 1,
            "nsigma": 0.0,
            "calcres": True,
            "calcpsf": True,
            "restoration": False,
            "savemodel": "none",
            "weighting": args.weighting,
        }
    )
    encoded_parameters = json.dumps(
        json_value(parameters), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    casalog.filter("INFO")
    started = time.monotonic()
    summary = tclean(**parameters)
    elapsed_s = time.monotonic() - started

    result = {
        "kind": "vlass_single_sample_awproject_gridding_oracle",
        "role": "isolated_semantic_diagnostic_not_promotion_or_performance_evidence",
        "casa_version": "6.7.5.18",
        "source_ms": str(args.source_ms),
        "scratch_ms": str(scratch_ms),
        "prediction_npz": str(args.prediction_npz),
        "output_prefix": str(args.output_prefix),
        "spectral_window": args.spw,
        "w_sign": args.w_sign,
        "weighting": args.weighting,
        "trace_row": trace_row,
        "ms_row": ms_row,
        "channel": channel,
        "channel_frequency_hz": float(
            trace["channel_frequency_hz"][trace_row, channel]
        ),
        "uvw_m": np.asarray(trace["uvw_m"][trace_row], dtype=np.float64).tolist(),
        "parallel_hands": list(PARALLEL_HANDS),
        "active_correlations": list(active_correlations),
        "inactive_channel_flags_materialized": True,
        "model_data": {
            str(correlation): {
                "real": float(model_data[correlation, channel].real),
                "imag": float(model_data[correlation, channel].imag),
            }
            for correlation in PARALLEL_HANDS
        },
        "parameters_sha256": hashlib.sha256(encoded_parameters).hexdigest(),
        "parameters": json_value(parameters),
        "elapsed_s": elapsed_s,
        "summary": {
            "iterdone": int(summary.get("iterdone", -1)),
            "nmajordone": int(summary.get("nmajordone", -1)),
            "stopDescription": str(summary.get("stopDescription", "")),
        },
        "products": sorted(
            str(path)
            for path in args.output_prefix.parent.glob(
                f"{args.output_prefix.name}.*"
            )
            if path.is_dir()
        ),
    }
    receipt = args.output_prefix.parent / f"{args.output_prefix.name}.oracle.json"
    receipt.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
