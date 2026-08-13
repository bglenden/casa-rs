#!/usr/bin/env python3
"""Reconstruct the reduced VLASS CASA MFS Briggs density grid.

This is a semantic diagnostic for the 4,096-square, four-SPW development row.
It follows CASA 6.7.5.18 ``VisImagingWeight`` source order and Float32
intermediates, including:

* VI2's correlation-OR flag matrix,
* first/last-correlation statistical-weight averaging,
* one LSRK channel vector per time/SPW visibility buffer,
* Float32 ``frequency / c`` and UV coordinates, and
* positive and conjugate grid-cell accumulation.

It never writes or modifies the MeasurementSet or frozen CASA image.
"""

from __future__ import annotations

import argparse
import json
import math
import struct
from pathlib import Path

import numpy as np
from casatools import image, measures, table

SPEED_OF_LIGHT_M_S = 299_792_458.0


def float32_bits(value: np.float32) -> str:
    return f"0x{struct.unpack('<I', struct.pack('<f', float(value)))[0]:08x}"


def scalar_hz(measure_record: dict) -> float:
    return float(measure_record["m0"]["value"])


def read_column(path: Path, column: str) -> np.ndarray:
    tb = table()
    try:
        tb.open(str(path))
        return np.asarray(tb.getcol(column))
    finally:
        tb.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ms", type=Path)
    parser.add_argument("casa_density", type=Path)
    parser.add_argument("--field", type=int, default=1525)
    parser.add_argument("--spws", default="2,7,12,17")
    parser.add_argument("--intent", default="OBSERVE_TARGET#UNSPECIFIED")
    parser.add_argument("--uv-max-m", type=float, default=12_000.0)
    parser.add_argument("--imsize", type=int, default=4096)
    parser.add_argument("--cell-arcsec", type=float, default=0.6)
    parser.add_argument(
        "--frequency-time-mode",
        choices=("per-row", "first-selected"),
        default="per-row",
        help=(
            "Use each row time, or the first selected time for all rows in an "
            "SPW, when constructing the VI frequency vector."
        ),
    )
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    spws = {int(value) for value in args.spws.split(",")}
    field_ids = read_column(args.ms, "FIELD_ID")
    times = read_column(args.ms, "TIME")
    ddids = read_column(args.ms, "DATA_DESC_ID")
    state_ids = read_column(args.ms, "STATE_ID")
    array_ids = read_column(args.ms, "ARRAY_ID")
    uvw = read_column(args.ms, "UVW")
    flags = read_column(args.ms, "FLAG")
    flag_rows = read_column(args.ms, "FLAG_ROW")
    weights = read_column(args.ms, "WEIGHT")

    ddid_to_spw = read_column(args.ms / "DATA_DESCRIPTION", "SPECTRAL_WINDOW_ID")
    obs_modes = read_column(args.ms / "STATE", "OBS_MODE")
    tb = table()
    try:
        tb.open(str(args.ms / "OBSERVATION"))
        telescope_name = str(tb.getcell("TELESCOPE_NAME", 0))
    finally:
        tb.close()
    accepted_states = {
        index
        for index, value in enumerate(obs_modes)
        if str(value) == args.intent
    }

    selected_rows = [
        row
        for row in range(len(field_ids))
        if int(field_ids[row]) == args.field
        and int(ddid_to_spw[int(ddids[row])]) in spws
        and int(state_ids[row]) in accepted_states
        and math.hypot(float(uvw[0, row]), float(uvw[1, row])) < args.uv_max_m
    ]
    # SynthesisImagerVi2 constructs SortColumns(ARRAY_ID, DATA_DESC_ID,
    # FIELD_ID, TIME), with the original row as the stable final key.
    selected_rows.sort(
        key=lambda row: (
            int(array_ids[row]),
            int(ddids[row]),
            int(field_ids[row]),
            float(times[row]),
            row,
        )
    )

    tb = table()
    try:
        tb.open(str(args.ms / "FIELD"))
        phase_dir = tb.getcell("PHASE_DIR", args.field)
        direction_keywords = tb.getcolkeywords("PHASE_DIR")
    finally:
        tb.close()
    try:
        tb.open(str(args.ms / "SPECTRAL_WINDOW"))
        chan_freqs = {
            spw: np.asarray(tb.getcell("CHAN_FREQ", spw), dtype=np.float64)
            for spw in spws
        }
    finally:
        tb.close()

    direction_ref = (
        direction_keywords.get("MEASINFO", {}).get("Ref")
        or direction_keywords.get("MEASINFO", {}).get("ref")
        or "J2000"
    )
    me = measures()
    position = me.observatory(telescope_name)
    direction = me.direction(
        direction_ref,
        f"{float(phase_dir[0][0])}rad",
        f"{float(phase_dir[1][0])}rad",
    )
    converted_by_buffer: dict[tuple[float, int], np.ndarray] = {}
    first_selected_time = min(float(times[row]) for row in selected_rows)
    for row in selected_rows:
        time_s = float(times[row])
        spw = int(ddid_to_spw[int(ddids[row])])
        conversion_time_s = (
            first_selected_time
            if args.frequency_time_mode == "first-selected"
            else time_s
        )
        key = (conversion_time_s, spw)
        if key in converted_by_buffer:
            continue
        me.doframe(me.epoch("UTC", f"{conversion_time_s}s"))
        me.doframe(position)
        me.doframe(direction)
        converted_by_buffer[key] = np.asarray(
            [
                scalar_hz(
                    me.measure(me.frequency("TOPO", f"{frequency_hz}Hz"), "LSRK")
                )
                for frequency_hz in chan_freqs[spw]
            ],
            dtype=np.float64,
        )

    grid = np.zeros((args.imsize, args.imsize), dtype=np.float32)
    cell_rad = math.radians(args.cell_arcsec / 3600.0)
    scale = float(args.imsize) * cell_rad
    origin = args.imsize // 2
    unflagged_samples = 0
    sample_contributions: list[dict] = []
    flag_or_all = np.any(flags, axis=0)
    for row in selected_rows:
        if bool(flag_rows[row]):
            continue
        spw = int(ddid_to_spw[int(ddids[row])])
        conversion_time_s = (
            first_selected_time
            if args.frequency_time_mode == "first-selected"
            else float(times[row])
        )
        frequencies = converted_by_buffer[(conversion_time_s, spw)]
        currwt = np.float32(
            np.float32(weights[0, row]) + np.float32(weights[-1, row])
        )
        currwt = np.float32(currwt / np.float32(2.0))
        for channel, frequency_hz in enumerate(frequencies):
            if bool(flag_or_all[channel, row]):
                continue
            unflagged_samples += 1
            f = np.float32(frequency_hz / SPEED_OF_LIGHT_M_S)
            u = np.float32(float(uvw[0, row]) * float(f))
            v = np.float32(float(uvw[1, row]) * float(f))
            ucell = int(scale * float(u) + float(origin))
            vcell = int(scale * float(v) + float(origin))
            if 0 < ucell < args.imsize and 0 < vcell < args.imsize:
                grid[ucell, vcell] = np.float32(grid[ucell, vcell] + currwt)
                sample_contributions.append(
                    {
                        "row": row,
                        "time_mjd_seconds": float(times[row]),
                        "spw": spw,
                        "channel": channel,
                        "frequency_hz": float(frequency_hz),
                        "weight": float(currwt),
                        "sign": 1,
                        "x": ucell,
                        "y": args.imsize - 1 - vcell,
                        "continuous_u": scale * float(u) + float(origin),
                        "continuous_v": scale * float(v) + float(origin),
                        "uvw_m": [
                            float(uvw[0, row]),
                            float(uvw[1, row]),
                            float(uvw[2, row]),
                        ],
                    }
                )
            ucell = int(-scale * float(u) + float(origin))
            vcell = int(-scale * float(v) + float(origin))
            if 0 < ucell < args.imsize and 0 < vcell < args.imsize:
                grid[ucell, vcell] = np.float32(grid[ucell, vcell] + currwt)
                sample_contributions.append(
                    {
                        "row": row,
                        "time_mjd_seconds": float(times[row]),
                        "spw": spw,
                        "channel": channel,
                        "frequency_hz": float(frequency_hz),
                        "weight": float(currwt),
                        "sign": -1,
                        "x": ucell,
                        "y": args.imsize - 1 - vcell,
                        "continuous_u": -scale * float(u) + float(origin),
                        "continuous_v": -scale * float(v) + float(origin),
                        "uvw_m": [
                            float(uvw[0, row]),
                            float(uvw[1, row]),
                            float(uvw[2, row]),
                        ],
                    }
                )

    # SItool.getweightdensity writes the internal Matrix<Float>(u, v) through
    # the image/lattice bridge with the second axis reversed relative to the
    # main-table UVW convention.  Match the frozen image's storage topology
    # before making the cellwise comparison.
    casa_storage_grid = np.flip(grid, axis=1)

    ia = image()
    try:
        ia.open(str(args.casa_density))
        casa = np.asarray(ia.getchunk(), dtype=np.float32).squeeze()
    finally:
        ia.close()
    if casa.shape != casa_storage_grid.shape:
        raise RuntimeError(
            f"CASA density shape {casa.shape} != {casa_storage_grid.shape}"
        )

    union = np.logical_or(casa != 0.0, casa_storage_grid != 0.0)
    mismatch = np.logical_and(
        union, casa.view(np.uint32) != casa_storage_grid.view(np.uint32)
    )
    difference = casa_storage_grid.astype(np.float64) - casa.astype(np.float64)
    mismatch_indices = np.argwhere(mismatch)
    largest = sorted(
        (
            {
                "x": int(x),
                "y": int(y),
                "casa": float(casa[x, y]),
                "casa_bits": float32_bits(casa[x, y]),
                "reconstructed": float(casa_storage_grid[x, y]),
                "reconstructed_bits": float32_bits(casa_storage_grid[x, y]),
                "difference": float(difference[x, y]),
            }
            for x, y in mismatch_indices
        ),
        key=lambda entry: abs(entry["difference"]),
        reverse=True,
    )[:20]
    positive_largest = [
        entry for entry in largest if entry["difference"] > 0.0
    ]
    candidate_shift_samples = []
    for entry in positive_largest:
        matching = [
            contribution
            for contribution in sample_contributions
            if contribution["x"] == entry["x"]
            and contribution["y"] == entry["y"]
            and math.isclose(
                contribution["weight"],
                entry["difference"],
                rel_tol=0.0,
                abs_tol=2.0e-4,
            )
        ]
        candidate_shift_samples.extend(matching)
    evidence = {
        "ms": str(args.ms),
        "telescope_name": telescope_name,
        "casa_density": str(args.casa_density),
        "selection": {
            "field": args.field,
            "spws": sorted(spws),
            "intent": args.intent,
            "uv_max_m": args.uv_max_m,
        },
        "selected_rows": len(selected_rows),
        "selected_non_flag_row": sum(not bool(flag_rows[row]) for row in selected_rows),
        "unflagged_channel_rows": unflagged_samples,
        "frequency_buffers": len(converted_by_buffer),
        "frequency_time_mode": args.frequency_time_mode,
        "grid": {
            "imsize": args.imsize,
            "cell_arcsec": args.cell_arcsec,
            "scale": scale,
            "origin": origin,
        },
        "comparison": {
            "casa_nonzero": int(np.count_nonzero(casa)),
            "reconstructed_nonzero": int(np.count_nonzero(casa_storage_grid)),
            "union_cells": int(np.count_nonzero(union)),
            "bit_mismatch_cells": int(len(mismatch_indices)),
            "l1": float(np.abs(difference).sum()),
            "l2": float(np.sqrt(np.square(difference).sum())),
            "linf": float(np.abs(difference).max()),
            "casa_sum": float(casa.astype(np.float64).sum()),
            "reconstructed_sum": float(casa_storage_grid.astype(np.float64).sum()),
            "largest_mismatches": largest,
            "candidate_shift_samples": candidate_shift_samples,
        },
    }
    rendered = json.dumps(evidence, indent=2, sort_keys=True)
    print(rendered)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
