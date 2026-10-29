#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's optimum image sizes, for `casa_imaging_model::optimum_image_size`
(#841 Phase 6b).

Run with CASA 6.7.6's Python (for example
`/Applications/CASA.app/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12 -I`):

    python3 -I tools/imaging/optimum_size_oracle.py \\
        --out crates/casa-imaging-model/tests/data/optimum_sizes_casa.json

It records `synthesisutils.getOptimumSize(n)` (`SynthesisUtilMethods::getOptimumSize`,
which `tclean` warns with and `simutil.imtclean` applies) for every `n` from 0
to 4096.
"""

from __future__ import annotations

import argparse
import json
import os

from casatools import ctsys, synthesisutils

LARGEST = 4096


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    utils = synthesisutils()
    sizes = [int(utils.getOptimumSize(n)) for n in range(LARGEST + 1)]
    utils.done()
    record = {"casa_version": ctsys.version_string(), "optimum": sizes}
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as handle:
        json.dump(record, handle, separators=(",", ":"))
        handle.write("\n")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
