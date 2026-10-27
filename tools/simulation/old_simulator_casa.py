#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA-side helpers for `bench_old_simulator.py` (simulation Phase 1d, #841).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
Each subcommand prints one JSON object on standard output:

- `tofits --image I --fits F`: export a CASA image to a float FITS file with
  `ia.tofits` (`bitpix=-32`, frequency axis, Stokes last), the input today's
  simulator reads.
- `hour-angle --ra R --dec D --mjd-days T --itrf X Y Z`: minus the hour angle
  of the J2000 direction `(R, D)` (radians) at the UTC epoch `T` (MJD days)
  seen from ITRF position `(X, Y, Z)` (metres), as seconds at 3600 s per
  hour: the offset the new simulator's `settimes(usehourangle=True)` adds to
  `observe`'s start and stop (`casa-simulation` `observe::hour_angle_offset_s`).
- `tai-minus-utc --mjd-days T`: TAI - UTC at the UTC epoch `T`, seconds,
  from CASA's measures data: CASA's simulator stores TAI seconds under the
  UTC label, so the comparison shifts casa-rs's requested times by it.
- `inspect --ms M`: rows, columns, the first row's DATA shape and RMS, the
  TIME range, autocorrelation and flagged-row counts, and the subtables'
  telescope, antenna count, correlation types, channels and field direction.
"""

from __future__ import annotations

import argparse
import json
import math
import os


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    tofits = commands.add_parser("tofits", help="export a CASA image to FITS")
    tofits.add_argument("--image", required=True)
    tofits.add_argument("--fits", required=True)
    hour_angle = commands.add_parser("hour-angle", help="the usehourangle offset")
    hour_angle.add_argument("--ra", type=float, required=True, help="J2000 RA, radians")
    hour_angle.add_argument("--dec", type=float, required=True, help="J2000 Dec, radians")
    hour_angle.add_argument("--mjd-days", type=float, required=True, help="UTC reference epoch")
    hour_angle.add_argument("--itrf", type=float, nargs=3, required=True, help="metres")
    tai = commands.add_parser("tai-minus-utc", help="TAI - UTC at an epoch")
    tai.add_argument("--mjd-days", type=float, required=True, help="UTC epoch")
    inspect = commands.add_parser("inspect", help="summarize a MeasurementSet")
    inspect.add_argument("--ms", required=True)
    return parser.parse_args()


def tofits(args: argparse.Namespace) -> dict:
    from casatools import image

    partial = args.fits + ".partial"
    if os.path.exists(partial):
        os.remove(partial)
    ia = image()
    ia.open(args.image)
    shape = [int(n) for n in ia.shape()]
    ok = ia.tofits(
        outfile=partial,
        velocity=False,
        optical=False,
        bitpix=-32,
        stokeslast=True,
        overwrite=True,
    )
    ia.done()
    if not ok:
        raise SystemExit(f"ia.tofits failed for {args.image}")
    os.replace(partial, args.fits)
    return {"image": args.image, "fits": args.fits, "image_shape": shape, "bytes": os.path.getsize(args.fits)}


def hour_angle(args: argparse.Namespace) -> dict:
    from casatools import measures

    me = measures()
    x, y, z = args.itrf
    me.doframe(me.epoch("UTC", f"{args.mjd_days!r}d"))
    me.doframe(me.position("ITRF", f"{x!r}m", f"{y!r}m", f"{z!r}m"))
    hadec = me.measure(me.direction("J2000", f"{args.ra!r}rad", f"{args.dec!r}rad"), "HADEC")
    value = hadec["m0"]["value"]
    # casacore's MVDirection longitude is in (-pi, pi].
    if value > math.pi:
        value -= 2.0 * math.pi
    elif value <= -math.pi:
        value += 2.0 * math.pi
    return {
        "hour_angle_rad": value,
        "offset_s": -value * (3600.0 * 180.0 / math.pi / 15.0),
    }


def tai_minus_utc(args: argparse.Namespace) -> dict:
    from casatools import measures

    me = measures()
    utc = me.epoch("UTC", f"{args.mjd_days!r}d")
    tai = me.measure(utc, "TAI")
    return {"tai_minus_utc_s": (tai["m0"]["value"] - utc["m0"]["value"]) * 86400.0}


def inspect(args: argparse.Namespace) -> dict:
    import numpy as np
    from casatools import table

    tb = table()
    tb.open(args.ms)
    try:
        columns = list(tb.colnames())
        rows = int(tb.nrows())
        first = tb.getcell("DATA", 0) if rows else None
        times = tb.getcol("TIME") if rows else np.zeros(0)
        antenna1 = tb.getcol("ANTENNA1") if rows else np.zeros(0)
        antenna2 = tb.getcol("ANTENNA2") if rows else np.zeros(0)
        flag_row = tb.getcol("FLAG_ROW") if rows else np.zeros(0, dtype=bool)
    finally:
        tb.close()
    summary = {
        "ms": args.ms,
        "rows": rows,
        "columns": columns,
        "data_shape": list(first.shape) if first is not None else None,
        "data_row0_rms": float(np.sqrt(np.mean(np.abs(first) ** 2))) if first is not None else None,
        "time_first": float(times.min()) if rows else None,
        "time_last": float(times.max()) if rows else None,
        "time_samples": int(np.unique(times).size),
        "autocorrelation_rows": int(np.sum(antenna1 == antenna2)),
        "flagged_rows": int(np.sum(flag_row)),
    }

    def column(subtable: str, name: str):
        tb.open(os.path.join(args.ms, subtable))
        try:
            return tb.getcol(name), int(tb.nrows())
        finally:
            tb.close()

    telescope, _ = column("OBSERVATION", "TELESCOPE_NAME")
    _, antennas = column("ANTENNA", "NAME")
    corr_type, _ = column("POLARIZATION", "CORR_TYPE")
    num_chan, _ = column("SPECTRAL_WINDOW", "NUM_CHAN")
    chan_freq, _ = column("SPECTRAL_WINDOW", "CHAN_FREQ")
    phase_dir, _ = column("FIELD", "PHASE_DIR")
    summary.update(
        telescope=[str(name) for name in telescope],
        antennas=antennas,
        corr_type=[int(value) for value in corr_type[:, 0]],
        num_chan=[int(value) for value in num_chan],
        chan_freq_first_hz=float(chan_freq[0, 0]),
        chan_freq_last_hz=float(chan_freq[-1, 0]),
        phase_dir_rad=[float(phase_dir[0, 0, 0]), float(phase_dir[1, 0, 0])],
    )
    return summary


def main() -> int:
    args = parse_args()
    handler = {
        "tofits": tofits,
        "hour-angle": hour_angle,
        "tai-minus-utc": tai_minus_utc,
        "inspect": inspect,
    }[args.command]
    print(json.dumps(handler(args)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
