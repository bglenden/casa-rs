# SPDX-License-Identifier: LGPL-3.0-or-later
"""Convert the trial's moving-field MSv2 with XRADIO in both of its ephemeris
layouts (tools/ms-trial/make_ephemeris_fixtures.sh). Python here is only the
oracle.

    make_ephemeris_fixtures.py MS OUT

writes OUT/ms_trial_ephemeris.ps.zarr (`ephemeris_interpolate=False`,
XRADIO's default: the samples on `time_ephemeris`) and
OUT/ms_trial_ephemeris_interpolated.ps.zarr (`ephemeris_interpolate=True`:
the samples interpolated onto the visibility times), one dataset each,
without pointing.
"""

import sys
from pathlib import Path

from xradio.measurement_set import convert_msv2_to_processing_set

LAYOUTS = [
    ("ms_trial_ephemeris.ps.zarr", False),
    ("ms_trial_ephemeris_interpolated.ps.zarr", True),
]


def main():
    ms, out = Path(sys.argv[1]), Path(sys.argv[2])
    for name, interpolate in LAYOUTS:
        convert_msv2_to_processing_set(
            in_file=str(ms),
            out_file=str(out / name),
            partition_scheme=[],
            with_pointing=False,
            persistence_mode="w",
            ephemeris_interpolate=interpolate,
        )


if __name__ == "__main__":
    main()
