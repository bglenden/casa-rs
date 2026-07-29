#!/usr/bin/env python3
"""Probe CASA VI2-style per-row spectral-frame conversion for VLASS.

This diagnostic intentionally constructs the same casacore measurement frame
used by ``VisibilityIteratorImpl2::makeFrequencyConverter``: row TIME, the VLA
observatory position, and the current field PHASE_DIR.  It is a reduced-row
semantic probe, not benchmark or final acceptance evidence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from casatools import measures, table


def scalar_hz(measure_record: dict) -> float:
    """Return the scalar Hz value from a casatools frequency measure."""

    return float(measure_record["m0"]["value"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ms", type=Path)
    parser.add_argument("--field", type=int, default=1525)
    parser.add_argument("--spws", default="2,7,12,17")
    args = parser.parse_args()

    spws = [int(value) for value in args.spws.split(",")]
    tb = table()
    try:
        tb.open(str(args.ms))
        field_ids = tb.getcol("FIELD_ID")
        times = tb.getcol("TIME")
        ddids = tb.getcol("DATA_DESC_ID")
        selected_times = sorted(
            {
                float(times[row])
                for row in range(tb.nrows())
                if int(field_ids[row]) == args.field
            }
        )
    finally:
        tb.close()

    try:
        tb.open(str(args.ms / "DATA_DESCRIPTION"))
        ddid_to_spw = [int(value) for value in tb.getcol("SPECTRAL_WINDOW_ID")]
    finally:
        tb.close()
    try:
        tb.open(str(args.ms / "OBSERVATION"))
        telescope_name = str(tb.getcell("TELESCOPE_NAME", 0))
    finally:
        tb.close()

    selected_time_spws = sorted(
        {
            (float(times[row]), ddid_to_spw[int(ddids[row])])
            for row in range(len(times))
            if int(field_ids[row]) == args.field
            and ddid_to_spw[int(ddids[row])] in spws
        }
    )

    try:
        tb.open(str(args.ms / "FIELD"))
        phase_dir = tb.getcell("PHASE_DIR", args.field)
        phase_dir_keywords = tb.getcolkeywords("PHASE_DIR")
    finally:
        tb.close()

    try:
        tb.open(str(args.ms / "SPECTRAL_WINDOW"))
        chan_freqs = {
            spw: [float(value) for value in tb.getcell("CHAN_FREQ", spw)]
            for spw in spws
        }
        freq_refs = {
            spw: int(tb.getcell("MEAS_FREQ_REF", spw))
            for spw in spws
        }
    finally:
        tb.close()

    me = measures()
    position = me.observatory(telescope_name)
    direction_ref = (
        phase_dir_keywords.get("MEASINFO", {}).get("Ref")
        or phase_dir_keywords.get("MEASINFO", {}).get("ref")
        or "J2000"
    )
    ra_rad = float(phase_dir[0][0])
    dec_rad = float(phase_dir[1][0])
    direction = me.direction(direction_ref, f"{ra_rad}rad", f"{dec_rad}rad")

    results = []
    for time_s, spw in selected_time_spws:
        me.doframe(me.epoch("UTC", f"{time_s}s"))
        me.doframe(position)
        me.doframe(direction)
        converted = [
            scalar_hz(me.measure(me.frequency("TOPO", f"{frequency_hz}Hz"), "LSRK"))
            for frequency_hz in chan_freqs[spw]
        ]
        results.append(
            {
                "time_mjd_seconds": time_s,
                "spw": spw,
                "source_freq_ref_code": freq_refs[spw],
                "first_hz": converted[0],
                "last_hz": converted[-1],
                "frequencies_hz": converted,
            }
        )

    print(
        json.dumps(
            {
                "field": args.field,
                "telescope_name": telescope_name,
                "field_direction_ref": direction_ref,
                "field_direction_rad": [ra_rad, dec_rad],
                "selected_field_times_mjd_seconds": selected_times,
                "time_spw_frequencies": results,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
