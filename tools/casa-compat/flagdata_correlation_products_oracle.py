#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare stored flagdata correlation planes with CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_autocorr_oracle import assert_same_visibilities, make_mixed_fixture
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples, make_fixture
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import add_second_window


def add_circular_polarization(vis):
    with table(f"{vis}/POLARIZATION", readonly=False, ack=False) as table_pol:
        index = table_pol.nrows()
        table_pol.addrows(1)
        for name in table_pol.colnames():
            if table_pol.iscelldefined(name, 0):
                table_pol.putcell(name, index, table_pol.getcell(name, 0))
        # Keep deliberate reversed order in the stored circular product.
        table_pol.putcell("CORR_TYPE", index, np.array([8, 5], dtype=np.int32))
    return index


def mixed_products_fixture(vis):
    make_mixed_fixture(vis)
    add_second_window(Path(vis))
    circular = add_circular_polarization(vis)
    with table(f"{vis}/DATA_DESCRIPTION", readonly=False, ack=False) as descriptions:
        descriptions.putcell("POLARIZATION_ID", 1, circular)
    with table(vis, ack=False) as ms:
        return {name: ms.getcol(name) for name in (
            "DATA_DESC_ID", "ANTENNA1", "ANTENNA2", "SCAN_NUMBER", "TIME"
        )}


def compare_products(vis, metadata):
    ddid = metadata["DATA_DESC_ID"]
    scan = metadata["SCAN_NUMBER"]
    times = metadata["TIME"]
    first_at_scan = np.array([times[scan == group].min() for group in scan])
    auto = metadata["ANTENNA1"] == metadata["ANTENNA2"]
    any_row = np.ones(ddid.shape, dtype=bool)
    first_five_seconds = times <= first_at_scan + 5.0
    after_five_seconds = times > first_at_scan + 5.0
    full = slice(None)
    linear_first, linear_both = {0: (0,)}, {0: (0, 1)}
    circular_first, circular_second = {1: (0,)}, {1: (1,)}
    cases = (
        ({"correlation": "XX"}, linear_first, any_row, full),
        ({"correlation": np.str_("xx")}, linear_first, any_row, full),
        ({"correlation": "fooXXbar"}, linear_first, any_row, full),
        ({"correlation": "XX,QQ"}, linear_first, any_row, full),
        ({"correlation": "XX;YY"}, linear_both, any_row, full),
        ({"correlation": "XXYY"}, linear_both, any_row, full),
        ({"correlation": "YY,XX,YY"}, linear_both, any_row, full),
        ({"correlation": "RR"}, circular_second, any_row, full),
        ({"correlation": "ALL"}, circular_first, any_row, full),
        ({"correlation": "LL,RR"}, {1: (0, 1)}, any_row, full),
        ({"correlation": " "}, {}, any_row, full),
        ({"correlation": "XX,RR"}, {}, any_row, full),
        ({"correlation": "XX,RR", "field": "FLUX"}, {}, any_row, full),
        ({"correlation": "XX\0,YY"}, linear_first, any_row, full),
        ({"correlation": "XX\u00a0,YY"}, linear_both, any_row, full),
        ({"correlation": "XX", "spw": "0:1~2"}, linear_first, ddid == 0, slice(1, 3)),
        ({"correlation": "XX", "autocorr": True}, linear_first, auto, full),
        ({"correlation": "YY", "mode": "manualflag"}, {0: (1,)}, any_row, full),
        ({"correlation": "XX", "mode": "unflag"}, linear_first, any_row, full),
        ({"correlation": "LL,RR", "mode": "unflag"}, {1: (0, 1)}, any_row, full),
        ({"correlation": "XX", "mode": "quack", "quackinterval": 5.0},
         linear_first, first_five_seconds, full),
        ({"correlation": "RR", "mode": "quack", "quackinterval": 5.0, "quackmode": "tail"},
         circular_second, after_five_seconds, full),
        ({"correlation": "XX,RR", "mode": "quack", "quackinterval": 5.0},
         {}, any_row, full),
    )
    source = snapshot(vis)
    for index, (options, planes, rows, channels) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, flagbackup=False, **options) == {}, options
        assert_equal(reference, native)
        set_flag = options.get("mode") != "unflag"
        for row, (before, after) in enumerate(zip(source, snapshot(native), strict=True)):
            expected = before[1].copy()
            if rows[row]:
                expected[channels, list(planes.get(int(ddid[row]), ()))] = set_flag
            assert after[0] == before[0], (options, row)
            np.testing.assert_array_equal(after[1], expected, err_msg=str(options))
            np.testing.assert_array_equal(after[2], before[2])
    # The first NUL ends the selection text before the explicit filter,
    # which leaves the ordinary CASA row-flag updates active.
    for path, module in (("nul-reference.ms", casatasks), ("nul-native.ms", tasks)):
        copytree(vis, path)
        assert module.flagdata(vis=path, correlation="\0XX", flagbackup=False) == {}
        assert all(row_flag and flags.all() for row_flag, flags, _ in snapshot(path))
    assert_equal("nul-reference.ms", "nul-native.ms")
    return len(cases) + 1


def compare_missing_metadata_matches():
    vis = "linear-only.ms"
    make_fixture(vis)
    source = snapshot(vis)
    for add_unused_polarization in (False, True):
        if add_unused_polarization:
            add_circular_polarization(vis)
        for name, module in (("reference", casatasks), ("native", tasks)):
            path = f"{name}-unused-pol-{add_unused_polarization}.ms"
            copytree(vis, path)
            assert module.flagdata(vis=path, correlation="XX,RR", flagbackup=False) == {}
            after = snapshot(path)
            for before, current in zip(source, after, strict=True):
                expected = before[1].copy()
                if not add_unused_polarization:
                    expected[:, 0] = True
                np.testing.assert_array_equal(current[1], expected)
                assert current[0] == before[0]
    return 2


def compare_flagged_metadata(vis, metadata):
    source = snapshot(vis)
    for subtable in ("POLARIZATION", "DATA_DESCRIPTION"):
        reference, native = f"{subtable}-reference.ms", f"{subtable}-native.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            with table(f"{path}/{subtable}", readonly=False, ack=False) as stored:
                stored.putcell("FLAG_ROW", 1, True)
            assert module.flagdata(vis=path, correlation="RR", flagbackup=False) == {}
        assert_equal(reference, native)
        for ddid, before, after in zip(metadata["DATA_DESC_ID"], source, snapshot(native), strict=True):
            expected = before[1].copy()
            if subtable == "POLARIZATION" and ddid == 1:
                expected[:, 1] = True
            np.testing.assert_array_equal(after[1], expected)
            assert before[0] == after[0]
    return 2


def compare_preview(vis):
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for index, options in enumerate((
        {"correlation": "LL,RR", "spw": "1:1~2"},
        {"correlation": "XX", "mode": "quack", "quackinterval": 5.0},
        {"correlation": "XX,RR"},
    )):
        reference = f"preview-reference-{index}.ms"
        copytree(vis, reference)
        assert casatasks.flagdata(vis=reference, flagbackup=False, **options) == {}
        expected = changed_samples(before, snapshot(reference))
        assert casatasks.flagdata(vis=vis, action="calculate", **options) == {}
        actual = tasks.run("flagdata", vis=vis, action="calculate", **options)
        assert actual.value == {}
        report = loads(actual.stdout)["result"]
        assert report["changed_samples"] == sum(expected)
        assert report["changed_rows"] == sum(change > 0 for change in expected)
        assert report["backup_version"] is None
        assert_preserved(vis, before, auxiliary)


def compare_no_match_backup(vis):
    before = snapshot(vis)
    for path, module in (("backup-reference.ms", casatasks), ("backup-native.ms", tasks)):
        copytree(vis, path)
        assert module.flagdata(vis=path, correlation="XX,RR", flagbackup=True) == {}
        assert_same_visibilities(path, before)
        entries = casatasks.flagmanager(vis=path, mode="list")
        names = [value["name"] for value in entries.values() if isinstance(value, dict)]
        assert names == ["flagdata_1"], entries


def compare_refusals(vis):
    cases = (
        (None, AssertionError),
        (1, AssertionError),
        (True, AssertionError),
        (["XX", 1], AssertionError),
        (["XX"], AttributeError),
        ([], AttributeError),
        (("XX",), AttributeError),
        (np.array(["XX", "YY"]), AttributeError),
        (np.array("XX"), AttributeError),
    )
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for value, kind in cases:
        for action in ("calculate", "none"):
            errors = []
            for module in (casatasks, tasks):
                try:
                    module.flagdata(vis=vis, correlation=value, action=action)
                except kind as error:
                    errors.append(str(error))
                else:
                    raise AssertionError(f"{module.__name__} accepted {value!r}")
                assert_preserved(vis, before, auxiliary)
            assert errors[0] == errors[1], (action, value, errors)
    for module in (casatasks, tasks):
        assert module.flagdata(vis=vis, correlation="XX", action="none") == {}
    assert_preserved(vis, before, auxiliary)

    for mode, correlation in (("manual", "ABS_XX"), ("manual", "WVR"),
                              ("summary", "XX"), ("clip", "XX"), ("rflag", "RR")):
        try:
            tasks.flagdata(vis=vis, mode=mode, correlation=correlation)
        except RuntimeError as error:
            assert "#978" in str(error)
        else:
            raise AssertionError(f"flagdata accepted pending {mode} selector {correlation}")
        assert_preserved(vis, before, auxiliary)
    return 2 * len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-correlation-products-") as work, chdir(work):
        vis = "input.ms"
        metadata = mixed_products_fixture(vis)
        products = compare_products(vis, metadata)
        products += compare_missing_metadata_matches()
        products += compare_flagged_metadata(vis, metadata)
        compare_preview(vis)
        compare_no_match_backup(vis)
        refusals = compare_refusals(vis)
    print(f"{products} flag products, three previews, one inactive call and "
          f"{refusals} paired argument errors compared with CASA, plus one no-match backup "
          "comparison and five guarded pending cases")


if __name__ == "__main__":
    main()
