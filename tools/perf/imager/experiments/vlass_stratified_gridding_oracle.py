#!/usr/bin/env python3
"""Freeze a small stratified CASA AWProject DataToGrid response.

The scratch MeasurementSet keeps a bounded number of exact MODEL_DATA samples
across every selected SPW and W sign.  This isolates cross-sample gridding and
MT-MFS aggregation without invoking either implementation's model-prediction
path.  It is semantic diagnostic evidence, never a promotion row.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
from casatasks import casalog, tclean
from casatools import table

from vlass_reduced_casa_clean_4096_four_spw import TCLEAN_PARAMETERS
from vlass_single_sample_gridding_oracle import (
    PARALLEL_HANDS,
    flag_all_channel_samples,
    json_value,
)


SELECTED_SPWS = (2, 7, 12, 17)


def choose_samples(
    trace: dict[str, np.ndarray],
    samples_per_bucket: int,
    sample_layout: str,
) -> list[tuple[int, int]]:
    flags = np.asarray(trace["flag"], dtype=np.bool_)
    spws = np.asarray(trace["spectral_window_id"])
    w_m = np.asarray(trace["uvw_m"])[:, 2]
    uv_selected = np.asarray(trace["uv_range_selected"], dtype=np.bool_)
    selected: list[tuple[int, int]] = []
    for spw in SELECTED_SPWS:
        for positive_w in (False, True):
            candidates = np.flatnonzero(
                (spws == spw)
                & uv_selected
                & ((w_m > 0.0) if positive_w else (w_m < 0.0))
            )
            usable: list[tuple[int, int]] = []
            for trace_row in candidates:
                for channel in range(flags.shape[2]):
                    if not flags[trace_row, PARALLEL_HANDS, channel].any():
                        usable.append((int(trace_row), channel))
                        break
            if len(usable) < samples_per_bucket:
                sign = "positive" if positive_w else "negative"
                raise RuntimeError(
                    f"SPW {spw} {sign}-W bucket has {len(usable)} usable rows; "
                    f"need {samples_per_bucket}"
                )
            if sample_layout == "head":
                bucket = usable[:samples_per_bucket]
            else:
                positions = np.linspace(
                    0, len(usable) - 1, samples_per_bucket, dtype=np.int64
                )
                bucket = [usable[int(position)] for position in positions]
            selected.extend(bucket)
    return selected


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-ms", required=True, type=Path)
    parser.add_argument("--prediction-npz", required=True, type=Path)
    parser.add_argument("--scratch-root", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument("--samples-per-bucket", type=int, default=4)
    parser.add_argument(
        "--sample-layout",
        choices=("head", "uniform"),
        default="head",
        help="select the first samples or distribute them across source order",
    )
    parser.add_argument(
        "--weighting",
        default="natural",
        choices=("natural", "briggs"),
    )
    parser.add_argument("--reuse-scratch", action="store_true")
    args = parser.parse_args()
    if args.samples_per_bucket <= 0:
        raise RuntimeError("--samples-per-bucket must be positive")

    scratch_ms = args.scratch_root / args.source_ms.name
    if args.scratch_root.exists() and not args.reuse_scratch:
        raise RuntimeError(f"refusing to overwrite scratch root: {args.scratch_root}")
    if args.reuse_scratch and not scratch_ms.is_dir():
        raise RuntimeError(f"scratch MeasurementSet is missing: {scratch_ms}")
    if any(args.output_prefix.parent.glob(f"{args.output_prefix.name}.*")):
        raise RuntimeError(f"refusing to overwrite products: {args.output_prefix}.*")

    with np.load(args.prediction_npz) as loaded:
        trace = {name: loaded[name] for name in loaded.files}
    selected = choose_samples(trace, args.samples_per_bucket, args.sample_layout)
    by_ms_row: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for trace_row, channel in selected:
        by_ms_row[int(trace["row_id"][trace_row])].append((trace_row, channel))

    args.scratch_root.parent.mkdir(parents=True, exist_ok=True)
    if not args.reuse_scratch:
        shutil.copytree(args.source_ms, scratch_ms)
    main_table = table()
    try:
        main_table.open(str(scratch_ms), nomodify=False)
        flag_all_channel_samples(main_table)
        flag_rows = np.ones(main_table.nrows(), dtype=np.bool_)
        for ms_row in by_ms_row:
            flag_rows[ms_row] = False
        main_table.putcol("FLAG_ROW", flag_rows)

        for ms_row, row_samples in by_ms_row.items():
            flags = np.ones_like(
                np.asarray(main_table.getcell("FLAG", ms_row), dtype=np.bool_)
            )
            data = np.asarray(
                main_table.getcell("DATA", ms_row), dtype=np.complex64
            )
            data[:, :] = 0.0
            for trace_row, channel in row_samples:
                flags[:, channel] = False
                model_data = np.asarray(
                    trace["model_data"][trace_row], dtype=np.complex64
                )
                data[list(PARALLEL_HANDS), channel] = model_data[
                    list(PARALLEL_HANDS), channel
                ]
            main_table.putcell("FLAG", ms_row, flags)
            main_table.putcell("DATA", ms_row, data)
        main_table.flush()
    finally:
        main_table.close()

    parameters = dict(TCLEAN_PARAMETERS)
    parameters.update(
        {
            "vis": str(scratch_ms),
            "imagename": str(args.output_prefix),
            "spw": ",".join(str(spw) for spw in SELECTED_SPWS),
            "nterms": 2,
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
    encoded = json.dumps(
        json_value(parameters), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    casalog.filter("INFO")
    started = time.monotonic()
    summary = tclean(**parameters)
    elapsed_s = time.monotonic() - started

    samples = []
    for trace_row, channel in selected:
        model_data = np.asarray(trace["model_data"][trace_row], dtype=np.complex64)
        samples.append(
            {
                "trace_row": trace_row,
                "ms_row": int(trace["row_id"][trace_row]),
                "spectral_window": int(trace["spectral_window_id"][trace_row]),
                "channel": channel,
                "channel_frequency_hz": float(
                    trace["channel_frequency_hz"][trace_row, channel]
                ),
                "uvw_m": np.asarray(
                    trace["uvw_m"][trace_row], dtype=np.float64
                ).tolist(),
                "model_data": {
                    str(correlation): {
                        "real": float(model_data[correlation, channel].real),
                        "imag": float(model_data[correlation, channel].imag),
                    }
                    for correlation in PARALLEL_HANDS
                },
            }
        )
    result = {
        "kind": "vlass_stratified_awproject_gridding_oracle",
        "role": "isolated_semantic_diagnostic_not_promotion_or_performance_evidence",
        "casa_version": "6.7.5.18",
        "source_ms": str(args.source_ms),
        "scratch_ms": str(scratch_ms),
        "prediction_npz": str(args.prediction_npz),
        "output_prefix": str(args.output_prefix),
        "samples_per_spw_and_w_sign": args.samples_per_bucket,
        "sample_layout": args.sample_layout,
        "sample_count": len(samples),
        "weighting": args.weighting,
        "inactive_channel_flags_materialized": True,
        "samples": samples,
        "parameters_sha256": hashlib.sha256(encoded).hexdigest(),
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
