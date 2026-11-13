#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulator operations with swapped, sparse and aliased data descriptions."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import gencal, version_string
from casatools import simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import make_session


def base(path):
    sm = make_session(path, stokes="XX XY YX YY")
    assert sm.setspwindow(spwname="band6b", freq="200GHz", deltafreq="1GHz",
                          freqresolution="1GHz", refcode="TOPO", nchannels=4,
                          stokes="XX XY YX YY")
    assert sm.observe(sourcename="target", spwname="band6b", starttime="-37s", stoptime="-17s")
    assert sm.observe(sourcename="offset", spwname="band6b", starttime="-17s", stoptime="3s")
    sm.close()
    initialize(path)
    tb = table()
    tb.open(str(path / "SPECTRAL_WINDOW"), nomodify=False)
    for row, first in ((0, 100e9), (1, 200e9)):
        tb.putcell("CHAN_FREQ", row, first + np.arange(4) * 1e9)
        tb.putcell("CHAN_WIDTH", row, np.full(4, 1e9))
        tb.putcell("RESOLUTION", row, np.full(4, 1e9))
        tb.putcell("EFFECTIVE_BW", row, np.full(4, 1e9))
        tb.putcell("REF_FREQUENCY", row, first)
        tb.putcell("TOTAL_BANDWIDTH", row, 4e9)
    tb.close()


def description_metadata(path):
    tb = table()
    result = {}
    for name in ("DATA_DESCRIPTION", "POLARIZATION"):
        tb.open(str(path / name))
        result[name] = [{column: np.asarray(tb.getcell(column, row)).tolist()
                         for column in tb.colnames()} for row in range(tb.nrows())]
        tb.close()
    return result


def layout(path, kind):
    tb = table()
    tb.open(str(path), nomodify=False)
    ids = tb.getcol("DATA_DESC_ID")
    if kind == "swapped":
        ids = 1 - ids
    elif kind == "sparse":
        selected = tb.query("DATA_DESC_ID == 1")
        temporary = path.with_name(path.name + ".selected")
        selected.copy(str(temporary), deep=True, valuecopy=True)
        selected.close()
        tb.close()
        shutil.rmtree(path)
        temporary.rename(path)
        tb.open(str(path), nomodify=False)
        ids = np.zeros(tb.nrows(), np.int32)
    else:
        ids[(ids == 0) & (np.arange(ids.size) % 2 == 0)] = 2
    tb.putcol("DATA_DESC_ID", ids)
    tb.close()
    tb.open(str(path / "DATA_DESCRIPTION"), nomodify=False)
    if kind == "swapped":
        tb.putcol("SPECTRAL_WINDOW_ID", np.asarray([1, 0], np.int32))
    elif kind == "sparse":
        tb.removerows([0])
    else:
        tb.addrows(1)
        for name, value in (("SPECTRAL_WINDOW_ID", 0), ("POLARIZATION_ID", 0), ("FLAG_ROW", False)):
            tb.putcell(name, 2, value)
    if kind == "alias_polarization":
        tb.putcell("POLARIZATION_ID", 2, 2)
    ids = tb.getcol("SPECTRAL_WINDOW_ID").tolist()
    tb.close()
    if kind == "alias_polarization":
        tb.open(str(path / "POLARIZATION"), nomodify=False)
        tb.addrows(1)
        tb.putcell("NUM_CORR", 2, 4)
        tb.putcell("CORR_TYPE", 2, np.asarray([12, 11, 10, 9], np.int32))
        tb.putcell("CORR_PRODUCT", 2, np.asarray([[1, 1, 0, 0], [1, 0, 1, 0]], np.int32))
        tb.putcell("FLAG_ROW", 2, False)
        tb.close()
    return ids


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
    original = root / "original.ms"
    base(original)
    gencal(vis=str(original), caltable=str(inputs / "gain.cal"), caltype="amp", parameter=[1.])
    tb = table()
    tb.open(str(inputs / "gain.cal"), nomodify=False)
    for row in range(tb.nrows()):
        window = int(tb.getcell("SPECTRAL_WINDOW_ID", row))
        tb.putcell("CPARAM", row, np.asarray([[2 + .1j], [3 - .2j]] if window == 0 else [[4 + .3j], [5 - .4j]], np.complex64))
    tb.close()
    cases = []
    for kind in ("swapped", "sparse", "alias", "alias_polarization"):
        stored = inputs / f"{kind}.ms"
        shutil.copytree(original, stored, ignore=shutil.ignore_patterns("table.lock"))
        mapping = layout(stored, kind)
        operations = ("gain",) if kind == "alias_polarization" else ("components0", "components1", "gain", "append")
        if kind == "alias":
            operations += ("query",)
        for operation in operations:
            name = f"{kind}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(stored, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path)), name
            requested = 1 if kind == "sparse" else 0
            if operation == "append":
                assert sm.settimes(integrationtime="10s", usehourangle=False,
                                   referencetime="UTC 2024/06/01/00:00:00")
                assert sm.observe(sourcename="target", spwname="band6b" if requested else "band6",
                                  starttime="3s", stoptime="23s", add_observation=True,
                                  project="continued")
            else:
                windows = [int(operation[-1])] if operation.startswith("components") else []
                assert sm.setdata(spwid=windows, fieldid=[0], msselect="DATA_DESC_ID == 2" if operation == "query" else "")
                if operation.startswith("components"):
                    assert sm.predict(complist=str(inputs / "spectrum.cl"))
                else:
                    assert sm.setapply(table=str(inputs / "gain.cal"), interp="nearest", calwt=False)
                    assert sm.corrupt()
            sm.close()
            after = snapshot(path)
            cases.append(dict(name=name, layout=kind, operation=operation, mapping=mapping,
                              before=before, after=after, requested_spw=requested,
                              description_metadata=description_metadata(stored),
                              native_reversed_gain=(kind == "alias_polarization"),
                              native_append_description=mapping.index(requested)))
            write(args.fixture, dict(casa_version=version_string(), cases=cases))
            labels = sorted({r["data_desc_id"] for r in after["rows"][len(before["rows"]):]})
            print(name, len(after["rows"]), "append DD labels", labels, "mapping", mapping, flush=True)


if __name__ == "__main__":
    main()
