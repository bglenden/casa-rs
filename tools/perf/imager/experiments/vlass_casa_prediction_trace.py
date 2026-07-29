#!/usr/bin/env python3
"""Persist CASA AWProject predictions for a checksum-pinned MT-MFS model."""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import numpy as np
from casatasks import casalog, tclean
from casatools import table

from vlass_common_model_major_cycle import (
    IMMUTABLE_SUFFIXES,
    MODEL_SUFFIXES,
    image_content_sha256,
    prefixed_directories,
)
from vlass_reduced_casa_clean_4096_four_spw import TCLEAN_PARAMETERS


SELECTED_SPWS = (2, 7, 12, 17)
SELECTED_FIELD = 1525
SELECTED_INTENT = "OBSERVE_TARGET#UNSPECIFIED"
MAX_UV_METERS = 12_000.0


def clone_image_bundle(zero_prefix: Path, model_prefix: Path, output_prefix: Path) -> None:
    products = prefixed_directories(zero_prefix)
    if not products:
        raise RuntimeError(f"zero-model CASA bundle is missing: {zero_prefix}.*")
    if prefixed_directories(output_prefix):
        raise RuntimeError(f"refusing to overwrite products: {output_prefix}.*")
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    for source in products:
        suffix = source.name[len(zero_prefix.name) :]
        if suffix in MODEL_SUFFIXES:
            continue
        shutil.copytree(source, Path(f"{output_prefix}{suffix}"))
    for suffix in MODEL_SUFFIXES:
        shutil.copytree(
            Path(f"{model_prefix}{suffix}"),
            Path(f"{output_prefix}{suffix}"),
        )


def selected_prediction_rows(ms_path: Path) -> dict[str, np.ndarray]:
    data_description = table()
    spectral_window = table()
    state = table()
    main = table()
    try:
        data_description.open(str(ms_path / "DATA_DESCRIPTION"))
        ddid_spw = np.asarray(
            data_description.getcol("SPECTRAL_WINDOW_ID"),
            dtype=np.int32,
        )
        data_description.close()

        state.open(str(ms_path / "STATE"))
        state_modes = [str(value) for value in state.getcol("OBS_MODE")]
        selected_states = np.asarray(
            [
                index
                for index, mode in enumerate(state_modes)
                if SELECTED_INTENT in mode
            ],
            dtype=np.int32,
        )
        state.close()
        if selected_states.size == 0:
            raise RuntimeError(f"no STATE rows match intent {SELECTED_INTENT}")

        spectral_window.open(str(ms_path / "SPECTRAL_WINDOW"))
        channel_frequencies = {
            spw: np.asarray(spectral_window.getcell("CHAN_FREQ", spw), dtype=np.float64)
            for spw in SELECTED_SPWS
        }
        spectral_window.close()

        main.open(str(ms_path))
        field_ids = np.asarray(main.getcol("FIELD_ID"), dtype=np.int32)
        data_description_ids = np.asarray(
            main.getcol("DATA_DESC_ID"),
            dtype=np.int32,
        )
        state_ids = np.asarray(main.getcol("STATE_ID"), dtype=np.int32)
        flag_rows = np.asarray(main.getcol("FLAG_ROW"), dtype=np.bool_)
        uvw = np.asarray(main.getcol("UVW"), dtype=np.float64).T
        spw_ids = ddid_spw[data_description_ids]
        selected = (
            (field_ids == SELECTED_FIELD)
            & np.isin(spw_ids, np.asarray(SELECTED_SPWS, dtype=np.int32))
            & np.isin(state_ids, selected_states)
            & (~flag_rows)
        )
        row_ids = np.flatnonzero(selected).astype(np.int64)
        # CASA reports 2600 selected rows before applying pre-existing
        # FLAG_ROW state.  The frozen fragment has 367 flagged rows, leaving
        # 2233 rows that can contribute predictions.
        if row_ids.size != 2233:
            raise RuntimeError(
                f"expected 2233 unflagged rows from CASA's 2600-row selection, selected {row_ids.size}"
            )
        first_model = np.asarray(main.getcell("MODEL_DATA", int(row_ids[0])))
        correlations, channels = first_model.shape
        model_data = np.empty(
            (row_ids.size, correlations, channels),
            dtype=np.complex64,
        )
        flags = np.empty((row_ids.size, correlations, channels), dtype=np.bool_)
        frequencies = np.empty((row_ids.size, channels), dtype=np.float64)
        for output_index, row_id in enumerate(row_ids):
            model_data[output_index] = np.asarray(
                main.getcell("MODEL_DATA", int(row_id)),
                dtype=np.complex64,
            )
            flags[output_index] = np.asarray(
                main.getcell("FLAG", int(row_id)),
                dtype=np.bool_,
            )
            frequencies[output_index] = channel_frequencies[int(spw_ids[row_id])]
        return {
            "row_id": row_ids,
            "uvw_m": uvw[row_ids],
            "data_description_id": data_description_ids[row_ids],
            "spectral_window_id": spw_ids[row_ids],
            "time_s": np.asarray(main.getcol("TIME"), dtype=np.float64)[row_ids],
            "model_data": model_data,
            "flag": flags,
            "channel_frequency_hz": frequencies,
            "uv_range_selected": (
                np.hypot(uvw[row_ids, 0], uvw[row_ids, 1]) < MAX_UV_METERS
            ),
        }
    finally:
        for tool in (main, state, spectral_window, data_description):
            try:
                tool.close()
            except Exception:
                pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-ms", required=True, type=Path)
    parser.add_argument("--scratch-ms", required=True, type=Path)
    parser.add_argument("--zero-prefix", required=True, type=Path)
    parser.add_argument("--model-prefix", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument("--output-npz", required=True, type=Path)
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="extract MODEL_DATA from an already completed scratch run",
    )
    args = parser.parse_args()

    if args.output_npz.exists():
        raise RuntimeError(f"refusing to overwrite trace: {args.output_npz}")
    protected_suffixes = MODEL_SUFFIXES + IMMUTABLE_SUFFIXES
    if args.extract_only:
        if not args.scratch_ms.is_dir():
            raise RuntimeError(f"completed scratch MS is missing: {args.scratch_ms}")
        if not prefixed_directories(args.output_prefix):
            raise RuntimeError(
                f"completed CASA output bundle is missing: {args.output_prefix}.*"
            )
        elapsed_s = None
        summary_subset = {
            "iterdone": 0,
            "nmajordone": 1,
            "stopDescription": "recovered from completed MODEL_DATA trace run",
        }
    else:
        if args.scratch_ms.exists():
            raise RuntimeError(f"refusing to overwrite scratch MS: {args.scratch_ms}")
        args.scratch_ms.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(args.source_ms, args.scratch_ms)
        clone_image_bundle(args.zero_prefix, args.model_prefix, args.output_prefix)
        protected_before = {
            suffix: image_content_sha256(Path(f"{args.output_prefix}{suffix}"))
            for suffix in protected_suffixes
        }
        parameters = dict(TCLEAN_PARAMETERS)
        parameters.update(
            {
                "vis": str(args.scratch_ms),
                "imagename": str(args.output_prefix),
                "niter": 0,
                "cycleniter": 1,
                "nmajor": 0,
                "calcres": True,
                "calcpsf": False,
                "restoration": False,
                "restart": True,
                "savemodel": "modelcolumn",
                "fullsummary": True,
            }
        )
        casalog.filter("INFO")
        started = time.monotonic()
        summary = tclean(**parameters)
        elapsed_s = time.monotonic() - started
        protected_after = {
            suffix: image_content_sha256(Path(f"{args.output_prefix}{suffix}"))
            for suffix in protected_suffixes
        }
        changed_content = sorted(
            suffix
            for suffix in protected_suffixes
            if protected_before[suffix] != protected_after[suffix]
        )
        if changed_content:
            raise RuntimeError(
                "CASA changed protected common-model/PSF/sumwt image content: "
                + ", ".join(changed_content)
            )
        summary_subset = {
            "iterdone": int(summary.get("iterdone", -1)),
            "nmajordone": int(summary.get("nmajordone", -1)),
            "stopDescription": str(summary.get("stopDescription", "")),
        }
    protected_after = {
        suffix: image_content_sha256(Path(f"{args.output_prefix}{suffix}"))
        for suffix in protected_suffixes
    }

    arrays = selected_prediction_rows(args.scratch_ms)
    args.output_npz.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(args.output_npz, **arrays)
    result = {
        "kind": "vlass_casa_awproject_prediction_trace",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "casa_version": "6.7.5.18",
        "elapsed_s": elapsed_s,
        "source_ms": str(args.source_ms),
        "scratch_ms": str(args.scratch_ms),
        "zero_prefix": str(args.zero_prefix),
        "model_prefix": str(args.model_prefix),
        "output_prefix": str(args.output_prefix),
        "output_npz": str(args.output_npz),
        "extract_only": args.extract_only,
        "selected_rows": int(arrays["row_id"].size),
        "model_shape": list(arrays["model_data"].shape),
        "protected_content_hashes": protected_after,
        "summary": summary_subset,
    }
    receipt = args.output_npz.with_suffix(".json")
    receipt.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
