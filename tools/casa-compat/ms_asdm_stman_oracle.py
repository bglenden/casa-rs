#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native transforms of a real CASA 6.7.6.14 lazy ASDM input."""

import argparse
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from casaconfig import config
import numpy as np

config.data_auto_update = False
config.measures_auto_update = False

import casatasks
from casatools import table
from casars import tasks

ASDM = "test_uid___A002_X997a62_X8c-short"
SORT = ("TIME", "DATA_DESC_ID", "ANTENNA1", "ANTENNA2")
COLUMNS = ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA", *SORT)


def source_binding(source):
    reader = table()
    try:
        assert reader.open(str(source))
        assert reader.nrows() == 414450
        bindings = reader.getdminfo()
        managers = [spec for spec in bindings.values() if spec["TYPE"] == "AsdmStMan"]
        assert len(managers) == 1 and "DATA" in managers[0]["COLUMNS"], managers
        assert len(managers[0]["SPEC"]["BDFs"]) == 3
    finally:
        reader.done()


def product(path):
    reader = table()
    try:
        assert reader.open(str(path))
        assert reader.nrows() > 0, path
        assert all(spec["TYPE"] != "AsdmStMan" for spec in reader.getdminfo().values()), path
        columns = {name: reader.getcol(name) for name in COLUMNS}
        order = np.lexsort([columns[name] for name in reversed(SORT)])
        return {name: value[..., order] for name, value in columns.items()}
    finally:
        reader.done()


def main(casatestdata):
    assert casatasks.version() == [6, 7, 6, 14]
    fixture = casatestdata / "unittest" / "split" / ASDM
    with TemporaryDirectory(prefix="mstransform-asdm-") as folder:
        root = Path(folder)
        asdm = root / ASDM
        shutil.copytree(fixture, asdm)
        source = root / "lazy.ms"
        casatasks.importasdm(str(asdm), vis=str(source), convert_ephem2geo=False,
                           flagbackup=False, process_syspower=False, lazy=True, scans="1",
                           savecmds=True, outfile=str(root / "flag-commands.txt"))
        source_binding(source)
        products = []
        for task in ("split", "mstransform"):
            for selection, antenna in (("cross", "0&1"), ("auto", "0&&&")):
                outputs = tuple(root / f"{side}-{task}-{selection}.ms" for side in ("casa", "native"))
                for module, output in zip((casatasks, tasks), outputs):
                    getattr(module, task)(vis=str(source), outputvis=str(output), datacolumn="data",
                                          spw="1:4~11", antenna=antenna)
                products.append((task, selection, outputs))
        # The outputs must remain readable after moving the copied BDF payload out
        # of the paths persisted in the lazy input's index.
        (asdm / "ASDMBinary").rename(asdm / "offline")
        for task, selection, (reference, native) in products:
            expected, actual = product(reference), product(native)
            assert np.all(actual["ANTENNA1"] == 0)
            assert np.all(actual["ANTENNA2"] == (0 if selection == "auto" else 1))
            assert actual["DATA"].shape[1] == 8
            for column in COLUMNS:
                np.testing.assert_array_equal(actual[column], expected[column],
                                              err_msg=f"{task} {selection} {column}")
            print(f"passed lazy ASDM {task} {selection} ({len(actual['TIME'])} rows)", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casatestdata", required=True, type=Path)
    main(parser.parse_args().casatestdata)
