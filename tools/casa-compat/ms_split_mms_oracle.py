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


def compare_history(reference, native, retained, case):
    with (table(str(reference / "HISTORY"), ack=False) as want,
          table(str(native / "HISTORY"), ack=False) as got):
        for name in ("ORIGIN", "APPLICATION", "PRIORITY"):
            np.testing.assert_array_equal(got.getcol(name), want.getcol(name), err_msg=case)
        for origin, left, right in zip(want.getcol("ORIGIN"), want.getcol("MESSAGE"), got.getcol("MESSAGE")):
            if origin != "split":
                assert left == right, (case, left, right)
                continue
            if left.startswith("version:") and right.startswith("version: casa-rs "):
                continue
            if left.startswith("outputvis"):
                assert right.startswith("outputvis"), (case, left, right)
                if retained:
                    assert Path(left.split('"')[1]).name.rsplit(".", 2)[-2:] == (
                        Path(right.split('"')[1]).name.rsplit(".", 2)[-2:]), (case, left, right)
                    assert "SUBMSS" in Path(right.split('"')[1]).parts, (case, right)
                continue
            assert left == right, (case, left, right)


def seed_partition_metadata(members):
    for number, member in enumerate(members):
        with table(str(member / "SOURCE"), readonly=False, ack=False) as metadata:
            metadata.putcell("NAME", 0, f"source-member-{number}")
        with table(str(member), readonly=False, ack=False) as main:
            if "SORTED_TABLE" in main.keywordnames():
                continue
            view_path = member / "SORTED_VIEW"
            view = (table(str(view_path), ack=False) if view_path.exists()
                    else main.query(sortlist="ARRAY_ID,FIELD_ID,DATA_DESC_ID,TIME", name=str(view_path)))
            main.putkeyword("SORTED_TABLE", view)
            view.close()


def compare_product(reference, native, axis, keep, case):
    members = [partitions(path, keep) for path in (reference, native)]
    assert len(members[0]) == len(members[1]), case
    for want, got in zip(*members):
        assert got.name.rsplit(".", 2)[-2:] == want.name.rsplit(".", 2)[-2:], case
        assert sorted(p.name for p in got.iterdir() if p.is_symlink()) == sorted(
            p.name for p in want.iterdir() if p.is_symlink()), case
        compare_rows(want, got, MAIN, case + " member")
        compare_history(want, got, True, case + " member")
        compare_rows(want / "SOURCE", got / "SOURCE", SUBTABLES["SOURCE"], case + " member SOURCE")
        with table(str(got), ack=False) as part:
            assert "SORTED_TABLE" not in part.keywordnames(), case
    compare_rows(reference, native, MAIN + ("TIME_CENTROID", "INTERVAL", "EXPOSURE"), case)
    for name, columns in SUBTABLES.items():
        compare_rows(reference / name, native / name, columns, f"{case} {name}")
    compare_history(reference, native, keep, case)
    with table(str(native), ack=False) as got:
        assert "SORTED_TABLE" not in got.keywordnames(), case
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
        count = rejections = 0
        for axis in ("scan", "spw", "baseline", "auto"):
            mms = root / f"{axis}.mms"
            assert casatasks.partition(vis=str(source), outputvis=str(mms), separationaxis=axis,
                                       numsubms=2, datacolumn="all", flagbackup=False) is None
            input_members = partitions(mms, True)
            original = [part.name for part in input_members]
            if axis == "scan":
                seed_partition_metadata(input_members)
            with table(str(mms), ack=False) as initial:
                partition_axis = next(line.removeprefix("AxisType = ") for line in initial.info()["readme"].splitlines()
                                      if line.startswith("AxisType = "))
                source_data = initial.getcol("DATA")
                source_flags = initial.getcol("FLAG")
            selections = [
                ("full", {}),
                ("drop-partition", {"spw": "1:1~3"}),
                ("time-channel", {"width": 2, "timebin": "25s"}),
            ]
            if axis == "scan":
                selections.append(("span-selected-scan", {"scan": "1", "combine": "scan", "timebin": "25s"}))
            if axis == "spw":
                selections.append(("span-spw-partition", {"combine": "scan", "timebin": "1h"}))
            for case, selection in selections:
                for keep in (True, False):
                    label = f"{axis}-{case}-{keep}"
                    destinations = [root / f"{label}-{side}.ms" for side in ("casa", "native")]
                    for module, target in zip((casatasks, tasks), destinations):
                        if axis == "scan":
                            seed_partition_metadata(input_members)
                        keep_arg = np.bool_(keep) if axis == "spw" and case == "full" else keep
                        result = module.split(vis=str(mms), outputvis=str(target),
                                              datacolumn="data", keepmms=keep_arg, **selection)
                        assert result is None, (label, module.__name__, result)
                        if axis == "scan" and module is tasks:
                            for member in input_members:
                                with table(str(member), ack=False) as original_part:
                                    assert "SORTED_TABLE" in original_part.keywordnames()
                    moved = root / f"{label}-moved.ms"
                    destinations[1].rename(moved)
                    compare_product(destinations[0], moved, partition_axis, keep, label)
                    count += 1
            if axis == "scan":
                for module in (casatasks, tasks):
                    out = root / f"{module.__name__}-cannot-span.ms"
                    try:
                        module.split(vis=str(mms), outputvis=str(out), datacolumn="data",
                                     keepmms=True, combine="scan", timebin="1h")
                    except Exception as error:
                        assert type(error) is Exception and str(error) == "Unable to continue with MMS processing"
                    else:
                        raise AssertionError(module.__name__)
                    assert not out.exists()
                rejections += 1
            for module in (casatasks, tasks):
                out = root / f"{axis}-{module.__name__}-missing-column.ms"
                assert module.split(vis=str(mms), outputvis=str(out), datacolumn="float_data",
                                    keepmms=True) is None
                assert not out.exists()
            rejections += 1
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
                    rejections += 1
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
                rejections += 1
            assert original == [part.name for part in partitions(mms, True)]
            if axis == "scan":
                for number, member in enumerate(input_members):
                    with table(str(member / "SOURCE"), ack=False) as metadata:
                        assert metadata.getcell("NAME", 0) == f"source-member-{number}"
            with table(str(mms), ack=False) as after:
                np.testing.assert_array_equal(after.getcol("DATA"), source_data)
                np.testing.assert_array_equal(after.getcol("FLAG"), source_flags)
        print(f"{count} split Multi-MS products and {rejections} rejection or no-product cases compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
