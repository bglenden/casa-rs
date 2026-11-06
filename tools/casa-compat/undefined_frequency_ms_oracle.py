#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA MS frequency references with a deliberately remapped code.

Pass the extracted repository ms_trial.ms fixture as the sole argument.
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import msmetadata, table, version_string  # noqa: E402

results = {}
with tempfile.TemporaryDirectory(prefix="casa-ms-frequency-") as directory:
    root = Path(directory)
    for name, code in [("lsrk", 64), ("undefined", 9), ("type_only", 3),
                       ("unmapped_bary", 3), ("unmapped_undefined", 64)]:
        path = root / f"{name}.ms"
        shutil.copytree(sys.argv[1], path)
        tb = table()
        tb.open(str(path / "SPECTRAL_WINDOW"), nomodify=False)
        for row in range(tb.nrows()):
            tb.putcell("MEAS_FREQ_REF", row, code)
        for column in ("CHAN_FREQ", "REF_FREQUENCY"):
            metadata = {"type": "frequency"}
            if name in ("lsrk", "undefined"):
                metadata.update({
                    "VarRefCol": "MEAS_FREQ_REF",
                    "TabRefTypes": np.array(["LSRK", "Undefined"]),
                    "TabRefCodes": np.array([64, 9], dtype=np.int32),
                })
            elif name.startswith("unmapped_"):
                metadata["VarRefCol"] = "MEAS_FREQ_REF"
            tb.putcolkeyword(column, "MEASINFO", metadata)
        tb.close()
        md = msmetadata()
        md.open(str(path))
        results[name] = md.reffreq(0)
        md.done()

print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "results": results}, indent=2))
