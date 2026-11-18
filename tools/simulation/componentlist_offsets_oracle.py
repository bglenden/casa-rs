#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA component offset retention, setters and table materialization."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import resource
import shutil
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string
import numpy as np


def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [clean(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def capture():
    assert version_string() == "6.7.6-14"
    cl = componentlist()
    cl.addcomponent(flux=1, dir="J2000 10deg 20deg", freq="LSRK 1.4GHz")
    base = clean(cl.getcomponent(0))
    cl.done()
    direction = {"type": "direction", "refer": "J2000", "m0": {"value": 1.0, "unit": "deg"}, "m1": {"value": 2.0, "unit": "deg"}}
    frequency = {"type": "frequency", "refer": "LSRK", "m0": {"value": 1.0, "unit": "GHz"}}
    cases = []
    actions = [("identity", lambda c: True), ("convert_same", lambda c: c.convertrefdir(0, "J2000")), ("convert_galactic", lambda c: c.convertrefdir(0, "GALACTIC")), ("set_direction", lambda c: c.setrefdir(0, "30deg", "40deg")), ("set_direction_frame", lambda c: c.setrefdirframe(0, "GALACTIC")), ("set_shape", lambda c: c.setshape(0, "Gaussian", "4arcsec", "2arcsec", "30deg")), ("set_frequency", lambda c: c.setfreq(0, 2.0, "GHz")), ("set_frequency_frame", lambda c: c.setfreqframe(0, "LSRK"))]
    for field, offset in [("direction", direction), ("frequency", frequency)]:
        for name, action in actions:
            record = deepcopy(base)
            parent = record["shape"][field] if field == "direction" else record["spectrum"][field]
            parent["offset"] = offset
            cl = componentlist()
            case = {"name": field + "_" + name, "field": field, "action": name, "record": record}
            directory = tempfile.mkdtemp(prefix="component-offset-", dir="target-1252/evidence")
            try:
                cl.add(record)
                case["result"] = action(cl)
                case["before"] = clean(cl.getcomponent(0))
                table = str(Path(directory) / "offset.cl")
                cl.rename(table)
                case["renamed"] = clean(cl.getcomponent(0))
                cl.done()
                cl.open(table)
                case["after"] = clean(cl.getcomponent(0))
            except Exception as error:
                case["error"] = str(error).split("\n... thrown by")[0]
            finally:
                cl.done()
                shutil.rmtree(directory)
            cases.append(case)
    return {"casatools": version_string(), "cases": cases}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.write_text(json.dumps(capture(), indent=2) + "\n")
