#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA predictions and append on linear/quadratic FIELD direction polynomials."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import version_string
from casatools import simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    source = Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_irregular_channels"
    for name in ("spectrum.cl", "spectrum.image"):
        shutil.copytree(source / name, inputs / name, ignore=shutil.ignore_patterns("table.lock"))
    cases = []
    for order in (1, 2):
        stored = inputs / f"order{order}.ms"
        sm = make_session(stored, stokes="XX XY YX YY")
        sm.close()
        initialize(stored)
        tb = table()
        tb.open(str(stored / "FIELD"), nomodify=False)
        coefficients = np.asarray([CENTRE, [1e-7, -1.5e-7], [1e-9, .5e-9]][:order + 1]).T
        tb.putcell("NUM_POLY", 0, order)
        tb.putcell("TIME", 0, 60462. * 86400. + 10.)
        for column in ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"):
            tb.putcell(column, 0, coefficients)
        tb.close()
        if order == 2:
            tb.open(str(stored), nomodify=False)
            arrays = ((tb.getcol("FIELD_ID") == 0) & (tb.getcol("TIME") >= 60462. * 86400. + 10.)).astype(np.int32)
            tb.putcol("ARRAY_ID", arrays)
            tb.close()
        for operation in ("components", "image", "components_late", "image_late", "image_then_early_image", "append"):
            name = f"order{order}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(stored, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path)), name
            if operation == "append":
                assert sm.settimes(integrationtime="10s", usehourangle=False,
                                   referencetime="UTC 2024/06/01/00:00:00")
                observe_return = bool(sm.observe(sourcename="target", spwname="band6", starttime="3s",
                                  stoptime="23s", add_observation=True, project="continued"))
                assert observe_return is False, "CASA currently refuses polynomial-field append"
            else:
                assert sm.setdata(fieldid=[0], msselect=f"TIME >= {60462. * 86400. + 10.}" if (operation.endswith("_late") or operation == "image_then_early_image") else "")
                if operation.startswith("components"):
                    assert sm.predict(complist=str(inputs / "spectrum.cl"))
                else:
                    assert sm.predict(imagename=str(inputs / "spectrum.image"))
                if operation == "image_then_early_image":
                    assert sm.setdata(fieldid=[0], msselect=f"TIME < {60462. * 86400. + 10.}")
                    assert sm.predict(imagename=str(inputs / "spectrum.image"), incremental=False)
            sm.close()
            after = snapshot(path)
            cases.append(dict(name=name, order=order, operation=operation,
                              coefficients=coefficients.tolist(), reference_time=60462. * 86400. + 10.,
                              predicate=f"TIME >= {60462. * 86400. + 10.}" if (operation.endswith("_late") or operation == "image_then_early_image") else "",
                              casa_observe_return=False if operation == "append" else None,
                              before=before, after=after))
            write(args.fixture, dict(casa_version=version_string(), cases=cases))
            print(name, "rows", len(after["rows"]), "metadata unchanged", before["metadata"] == after["metadata"], "baseline01 first channel",
                  [r["data"][:4] for r in after["rows"] if r["field"] == 0 and r["antennas"] == [0, 1]][:2], flush=True)


if __name__ == "__main__":
    main()
