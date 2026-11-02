# SPDX-License-Identifier: LGPL-3.0-or-later
"""Convert the sources-and-lines fixture's MSv2 with XRADIO
(tools/ms-trial/make_lines_fixtures.sh, #976). Python here is only the
oracle.

    make_lines_fixtures.py MS OUT

writes OUT/ms_trial_lines.ps.zarr with XRADIO's default partitioning, one
dataset per spectral window, each whose field_and_source dataset states
the field's source and the lines of its window, without pointing.
"""

import sys
from pathlib import Path

from xradio.measurement_set import convert_msv2_to_processing_set


def main():
    ms, out = Path(sys.argv[1]), Path(sys.argv[2])
    convert_msv2_to_processing_set(
        in_file=str(ms),
        out_file=str(out / "ms_trial_lines.ps.zarr"),
        with_pointing=False,
        persistence_mode="w",
    )


if __name__ == "__main__":
    main()
