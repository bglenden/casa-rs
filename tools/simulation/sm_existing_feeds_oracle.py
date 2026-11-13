#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA resumes multi-feed observations for selected calculations and append."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import gencal, version_string
from casatools import simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_feeds_oracle import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    source = Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_irregular_channels/spectrum.cl"
    shutil.copytree(source, inputs / "spectrum.cl", ignore=shutil.ignore_patterns("table.lock"))
    threshold = 60462. * 86400. + 20.
    predicate = f"TIME >= {threshold}"
    cases = []
    for count, mount in ((2, "alt-az"), (3, "alt-az"), (2, "equatorial")):
        layout = f"{count}-{mount}"
        stored = inputs / f"{layout}.ms"
        run(stored, count, mount)
        initialize(stored)
        gain = inputs / f"{layout}.cal"
        gencal(vis=str(stored), caltable=str(gain), caltype="amp", parameter=[1.])
        tb = table()
        tb.open(str(gain), nomodify=False)
        for row in range(tb.nrows()):
            tb.putcell("CPARAM", row, np.asarray([[2 + .1j], [3 - .2j]], np.complex64))
        tb.close()
        for operation in ("components", "gain", "append"):
            name = f"{layout}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(stored, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path)), name
            if operation == "append":
                assert sm.settimes(integrationtime="10s", usehourangle=False,
                                   referencetime="UTC 2024/06/01/00:00:00")
                # One integration avoids CASA's documented accumulating-offset
                # bug; feed creation's split-integration controls cover it.
                assert sm.observe(sourcename="target", spwname="band6", starttime="3s",
                                  stoptime="13s", add_observation=True, project="continued")
            else:
                assert sm.setdata(fieldid=[0], msselect=predicate)
                if operation == "components":
                    assert sm.predict(complist=str(inputs / "spectrum.cl"))
                else:
                    assert sm.setapply(table=str(gain), interp="nearest", calwt=False)
                    assert sm.corrupt()
            sm.close()
            cases.append(dict(name=name, layout=layout, operation=operation,
                              feeds=count, mount=mount, predicate=predicate, threshold=threshold,
                              before=before, after=snapshot(path)))
            write(args.fixture, dict(casa_version=version_string(), cases=cases))
            print(name, "rows", len(cases[-1]["after"]["rows"]), flush=True)


if __name__ == "__main__":
    main()
