#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 appends observations to a reopened simulation MS (SIM-05/06)."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import gencal, version_string
from casatools import componentlist, measures, simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import CENTRE, make_session


def snapshot(path):
    rows = read_rows(path)
    tb = table()
    tb.open(str(path))
    for row, values in enumerate(rows):
        for column in ("TIME", "INTERVAL", "EXPOSURE", "SCAN_NUMBER", "OBSERVATION_ID",
                       "STATE_ID", "ARRAY_ID", "DATA_DESC_ID", "FEED1", "FEED2", "UVW"):
            value = tb.getcell(column, row)
            values[column.lower()] = value.tolist() if isinstance(value, np.ndarray) else value
    tb.close()
    metadata = {}
    for name in ("OBSERVATION", "STATE", "POINTING"):
        tb.open(str(path / name))
        metadata[name] = dict(rows=tb.nrows())
        metadata[name]["columns"] = {column: tb.getcol(column).tolist() for column in tb.colnames()
                                     if tb.nrows() and tb.iscelldefined(column, 0)}
        tb.close()
    return dict(rows=rows, metadata=metadata)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    root.mkdir(parents=True)
    inputs = root / "inputs"
    inputs.mkdir()
    component = inputs / "point.cl"
    cl = componentlist()
    assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad", flux=1., freq="230GHz")
    cl.rename(str(component))
    cl.done()
    cases = []
    for name in ("one", "observation", "many", "array_id", "predict_then_append", "selected_corrupt_then_append",
                 "append_then_selected_predict_corrupt", "append_then_selected_increment", "missing_source"):
        path = root / (name + ".ms")
        sm = make_session(path, stokes="XX XY YX YY")
        sm.close()
        initialize(path)
        if name == "array_id":
            tb = table()
            tb.open(str(path), nomodify=False)
            tb.putcol("ARRAY_ID", np.full(tb.nrows(), 7, dtype=np.int32))
            tb.close()
        before = snapshot(path)
        if name == "one":
            shutil.copytree(path, inputs / "base.ms", ignore=shutil.ignore_patterns("table.lock"))
        gain = root / "gain.cal"
        if "corrupt" in name and not gain.exists():
            gencal(vis=str(path), caltable=str(gain), caltype="amp", parameter=[2.])
            shutil.copytree(gain, inputs / "gain.cal", ignore=shutil.ignore_patterns("table.lock"))
        sm, me = simulator(), measures()
        assert sm.openfromms(str(path))
        assert sm.settimes(integrationtime="10s", usehourangle=False,
                           referencetime=me.epoch("UTC", "2024/06/01/00:00:00"))
        if name == "selected_corrupt_then_append":
            assert sm.setdata(fieldid=[1])
            assert sm.setapply(table=str(gain), interp="nearest", calwt=False)
            assert sm.corrupt()
        if name == "predict_then_append":
            assert sm.predict(complist=str(component), incremental=False)
        if name == "many":
            outcome = sm.observemany(sourcenames=["target", "offset"], spwname="band6",
                                    starttimes=["3s", "23s"], stoptimes=["23s", "43s"],
                                    add_observation=True, project="continued")
        else:
            outcome = sm.observe(sourcename="missing" if name == "missing_source" else "target",
                                 spwname="band6", starttime="3s", stoptime="23s",
                                 add_observation=name in ("observation", "array_id"), project="continued")
        if name.startswith("append_then_selected"):
            assert sm.setdata(fieldid=[0])
            assert sm.predict(complist=str(component), incremental=name.endswith("increment"))
            if name.endswith("corrupt"):
                assert sm.setapply(table=str(gain), interp="nearest", calwt=False)
                assert sm.corrupt()
        sm.close()
        me.done()
        after = snapshot(path)
        if "selected" not in name and name != "predict_then_append":
            assert after["rows"][:len(before["rows"])] == before["rows"]
        assert len(after["rows"]) == len(before["rows"]) + (144 if name == "many" else 0 if name == "missing_source" else 72)
        cases.append(dict(name=name, returned=bool(outcome), before=before, after=after))
        write(args.fixture, dict(casa_version=version_string(), cases=cases))
        print(name, bool(outcome), len(after["rows"]), flush=True)


if __name__ == "__main__":
    main()
