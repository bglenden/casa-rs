#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check CASA's empty Path behavior without deleting a pre-existing table."""
import argparse
import json
from pathlib import Path
import tempfile

from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
assert version_string() == "6.7.6-14"
result = {"casatools": version_string(), "cwd": str(Path.cwd())}
with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "rename.cl"
    cl = componentlist()
    try:
        for attached in [False, True]:
            if attached:
                cl.addcomponent(label="kept")
                cl.rename(str(path))
                cl.putkeyword("example", 42)
            try:
                cl.rename("")
            except RuntimeError as error:
                result["attached_error" if attached else "virtual_error"] = str(error)
        result["exists_after"] = path.exists()
        result["label_after"] = cl.getlabel(0)
        result["keyword_after"] = cl.getkeyword("example")
    finally:
        cl.done()
args.out.write_text(json.dumps(result, indent=2) + "\n")
