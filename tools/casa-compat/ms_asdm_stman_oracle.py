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
COLUMNS = ("FLAG", "FLAG_ROW", "WEIGHT", "SIGMA", *SORT)


def source_binding(source, column, rows):
    reader = table()
    try:
        assert reader.open(str(source))
        assert reader.nrows() == rows
        bindings = reader.getdminfo()
        managers = [spec for spec in bindings.values() if spec["TYPE"] == "AsdmStMan"]
        assert len(managers) == 1 and column in managers[0]["COLUMNS"], managers
        assert len(managers[0]["SPEC"]["BDFs"]) == 3
    finally:
        reader.done()


def product(path, visibility):
    reader = table()
    try:
        assert reader.open(str(path))
        assert reader.nrows() > 0, path
        assert all(spec["TYPE"] != "AsdmStMan" for spec in reader.getdminfo().values()), path
        columns = {name: reader.getcol(name) for name in (visibility, *COLUMNS)}
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
        source_binding(source, "DATA", 414450)
        float_source = root / "lazy-auto-only.ms"
        casatasks.importasdm(str(asdm), vis=str(float_source), ocorr_mode="ao",
                           convert_ephem2geo=False, flagbackup=False, process_syspower=False,
                           lazy=True, scans="1")
        source_binding(float_source, "FLOAT_DATA", 17550)
        products = []
        for task in ("split", "mstransform"):
            for selection, antenna, pair in (("cross", "1&2", (1, 2)), ("auto", "0&&&", (0, 0))):
                outputs = tuple(root / f"{side}-{task}-{selection}.ms" for side in ("casa", "native"))
                for module, output in zip((casatasks, tasks), outputs):
                    getattr(module, task)(vis=str(source), outputvis=str(output), datacolumn="data",
                                          spw="1:4~11", antenna=antenna)
                products.append((f"{task} {selection}", "DATA", pair, outputs))
        outputs = tuple(root / f"{side}-float.ms" for side in ("casa", "native"))
        for module, output in zip((casatasks, tasks), outputs):
            module.mstransform(vis=str(float_source), outputvis=str(output), datacolumn="float_data",
                               spw="1:4~11", antenna="0&&&")
        products.append(("mstransform float auto", "FLOAT_DATA", (0, 0), outputs))
        # The outputs must remain readable after moving the copied BDF payload out
        # of the paths persisted in the lazy input's index.
        (asdm / "ASDMBinary").rename(asdm / "offline")
        for label, visibility, pair, (reference, native) in products:
            expected, actual = product(reference, visibility), product(native, visibility)
            assert np.all(actual["ANTENNA1"] == pair[0])
            assert np.all(actual["ANTENNA2"] == pair[1])
            assert actual[visibility].shape[1] == 8
            for column in (visibility, *COLUMNS):
                np.testing.assert_array_equal(actual[column], expected[column],
                                              err_msg=f"{label} {column}")
            print(f"passed lazy ASDM {label} ({len(actual['TIME'])} rows)", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casatestdata", required=True, type=Path)
    main(parser.parse_args().casatestdata)
