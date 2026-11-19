#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public split on a partitioned CASA MeasurementSet."""

from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
from casatools import ms
import numpy as np

from casars import tasks
from ms_selection_tasks_oracle import MAIN, SUBTABLES, add_second_window, casa_table, make_input


def compare_rows(reference, native, columns, case):
    with (table(str(reference), ack=False) as want, table(str(native), ack=False) as got,
          casa_table(native) as reopen):
        assert got.nrows() == want.nrows(), (case, got.nrows(), want.nrows())
        for name in columns:
            if name not in want.colnames():
                assert name not in got.colnames(), (case, name)
                continue
            for row in range(want.nrows()):
                if not want.iscelldefined(name, row):
                    assert not got.iscelldefined(name, row), (case, name, row)
                    continue
                expected, actual = want.getcell(name, row), got.getcell(name, row)
                label = f"{case} {name} row {row}"
                if name in ("TIME", "TIME_CENTROID"):
                    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-5, err_msg=label)
                elif np.asarray(actual).dtype.kind in "fc":
                    np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-7, err_msg=label)
                else:
                    np.testing.assert_array_equal(actual, expected, err_msg=label)
                np.testing.assert_array_equal(np.asarray(reopen.getcell(name, row)).T, actual,
                                              err_msg=f"CASA reopen {label}")


def partitions(path, keep):
    tool = ms()
    assert tool.open(str(path))
    assert tool.ismultims() == keep
    tool.close()
    members = path / "SUBMSS"
    if not keep:
        assert not members.exists()
        return []
    return sorted(members.iterdir())


def compare_product(reference, native, axis, keep, case):
    members = [partitions(path, keep) for path in (reference, native)]
    assert len(members[0]) == len(members[1]), case
    for want, got in zip(*members):
        assert got.name.rsplit(".", 2)[-2:] == want.name.rsplit(".", 2)[-2:], case
        assert sorted(p.name for p in got.iterdir() if p.is_symlink()) == sorted(
            p.name for p in want.iterdir() if p.is_symlink()), case
        compare_rows(want, got, MAIN, case + " member")
    compare_rows(reference, native, MAIN + ("TIME_CENTROID", "INTERVAL", "EXPOSURE"), case)
    for name, columns in SUBTABLES.items():
        compare_rows(reference / name, native / name, columns, f"{case} {name}")
    with (table(str(reference / "HISTORY"), ack=False) as want,
          table(str(native / "HISTORY"), ack=False) as got):
        np.testing.assert_array_equal(got.getcol("ORIGIN"), want.getcol("ORIGIN"), err_msg=case)
        np.testing.assert_array_equal(got.getcol("APPLICATION"), want.getcol("APPLICATION"), err_msg=case)
        np.testing.assert_array_equal(got.getcol("PRIORITY"), want.getcol("PRIORITY"), err_msg=case)
    with table(str(native), ack=False) as got:
        assert got.info()["subType"] == ("CONCATENATED" if keep else "UVFITS"), case
        if keep:
            assert f"AxisType = {axis}\n" in got.info()["readme"]
            assert all((native / name).is_symlink() for name in SUBTABLES), case
            assert all((native / name).resolve().is_relative_to(native) for name in SUBTABLES), case


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="split-mms-") as work:
        root = Path(work)
        source = root / "source.ms"
        make_input(source, channels=4)
        add_second_window(source)
        count = 0
        for axis in ("scan", "spw"):
            mms = root / f"{axis}.mms"
            assert casatasks.partition(vis=str(source), outputvis=str(mms), separationaxis=axis,
                                       numsubms=2, datacolumn="all", flagbackup=False) is None
            original = [part.name for part in partitions(mms, True)]
            with table(str(mms), ack=False) as initial:
                source_data = initial.getcol("DATA")
                source_flags = initial.getcol("FLAG")
            for case, selection in (
                ("full", {}),
                ("drop-partition", {"spw": "1:1~3"}),
                ("time-channel", {"width": 2, "timebin": "25s"}),
            ):
                for keep in (True, False):
                    label = f"{axis}-{case}-{keep}"
                    destinations = [root / f"{label}-{side}.ms" for side in ("casa", "native")]
                    for module, target in zip((casatasks, tasks), destinations):
                        keep_arg = np.bool_(keep) if axis == "spw" and case == "full" else keep
                        result = module.split(vis=str(mms), outputvis=str(target),
                                              datacolumn="data", keepmms=keep_arg, **selection)
                        assert result is None, (label, module.__name__, result)
                    moved = root / f"{label}-moved.ms"
                    destinations[1].rename(moved)
                    compare_product(destinations[0], moved, axis, keep, label)
                    count += 1
            for invalid in ({"field": "0", "spw": "1"}, {"field": "99"}, {"spw": "99"}):
                for keep in (True, False):
                    for module in (casatasks, tasks):
                        target = root / f"{axis}-{keep}-{module.__name__}-rejected.ms"
                        try:
                            result = module.split(vis=str(mms), outputvis=str(target),
                                                  datacolumn="data", keepmms=keep, **invalid)
                        except RuntimeError:
                            assert not keep, (axis, keep, module.__name__, invalid)
                        else:
                            assert keep and result is None, (axis, keep, module.__name__, invalid)
                        assert not target.exists(), target
            for value, diagnostic in ((1, "must be of cBool type"), (None, "null value not allowed")):
                for module in (casatasks, tasks):
                    target = root / f"{axis}-{module.__name__}-invalid-keepmms.ms"
                    try:
                        module.split(vis=str(mms), outputvis=str(target), datacolumn="data", keepmms=value)
                    except AssertionError as error:
                        assert diagnostic in str(error), (module.__name__, str(error))
                    else:
                        raise AssertionError((module.__name__, value))
                    assert not target.exists(), target
            assert original == [part.name for part in partitions(mms, True)]
            with table(str(mms), ack=False) as after:
                np.testing.assert_array_equal(after.getcol("DATA"), source_data)
                np.testing.assert_array_equal(after.getcol("FLAG"), source_flags)
        print(f"{count} split Multi-MS products compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
