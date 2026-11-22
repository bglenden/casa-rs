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
        ({"correlation": "XX,Q"}, linear_first, any_row, full),
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
    for index, expr in enumerate(("\0XX", np.array(""))):
        reference, native = f"default-reference-{index}.ms", f"default-native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, correlation=expr, flagbackup=False) == {}
            assert all(row_flag and flags.all() for row_flag, flags, _ in snapshot(path))
        assert_equal(reference, native)
    return len(cases) + 2


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
    cases = (
        ("POLARIZATION", "RR", False, {1: 1}),
        ("DATA_DESCRIPTION", "RR", False, {}),
        ("DATA_DESCRIPTION", "XX", True, {0: 0, 1: 0}),
        ("SPECTRAL_WINDOW", "RR", False, {}),
        ("SPECTRAL_WINDOW", "XX", True, {0: 0, 1: 0}),
    )
    for index, (subtable, correlation, shared, planes) in enumerate(cases):
        reference, native = f"metadata-reference-{index}.ms", f"metadata-native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            with table(f"{path}/{subtable}", readonly=False, ack=False) as stored:
                stored.putcell("FLAG_ROW", 1, True)
            if shared:
                with table(f"{path}/DATA_DESCRIPTION", readonly=False, ack=False) as stored:
                    stored.putcell("POLARIZATION_ID", 1, 0)
            assert module.flagdata(vis=path, correlation=correlation, flagbackup=False) == {}
        assert_equal(reference, native)
        for ddid, before, after in zip(metadata["DATA_DESC_ID"], source, snapshot(native), strict=True):
            expected = before[1].copy()
            if ddid in planes:
                expected[:, planes[ddid]] = True
            np.testing.assert_array_equal(after[1], expected)
            assert before[0] == after[0]
    return len(cases)


def compare_stored_product_names(vis, metadata):
    source = snapshot(vis)
    cases = (
        ([1, 2], 0, "Q", ()),
        ([1, 2], 0, "I,Q", (0,)),
        ([9, 9], 0, "XX", (0,)),
        ([2, 3], 1, "XX,Q", (0,)),
    )
    for index, (types, pol, correlation, planes) in enumerate(cases):
        reference, native = f"names-reference-{index}.ms", f"names-native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            with table(f"{path}/POLARIZATION", readonly=False, ack=False) as stored:
                stored.putcell("CORR_TYPE", pol, np.array(types, dtype=np.int32))
            assert module.flagdata(vis=path, correlation=correlation, flagbackup=False) == {}
        assert_equal(reference, native)
        for ddid, before, after in zip(metadata["DATA_DESC_ID"], source, snapshot(native), strict=True):
            expected = before[1].copy()
            if ddid == 0:
                expected[:, list(planes)] = True
            np.testing.assert_array_equal(after[1], expected)
            assert before[0] == after[0]
    return len(cases)


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


def compare_backups(vis):
    before = snapshot(vis)
    for index, expression in enumerate(("XX,RR", ["XX"], np.array("XX"))):
        for name, module in (("reference", casatasks), ("native", tasks)):
            path = f"backup-{name}-{index}.ms"
            copytree(vis, path)
            if isinstance(expression, str):
                assert module.flagdata(vis=path, correlation=expression) == {}
            else:
                try:
                    module.flagdata(vis=path, correlation=expression)
                except AttributeError as error:
                    kind = "list" if isinstance(expression, list) else "numpy.ndarray"
                    assert str(error) == f"'{kind}' object has no attribute 'upper'", error
                else:
                    raise AssertionError(f"{name} accepted {expression!r}")
            assert_same_visibilities(path, before)
            entries = casatasks.flagmanager(vis=path, mode="list")
            names = [value["name"] for value in entries.values() if isinstance(value, dict)]
            assert names == ["flagdata_1"], entries
            with table(f"{path}.flagversions/flags.flagdata_1", ack=False) as version:
                for row, (row_flag, flags, _) in enumerate(before):
                    assert version.getcell("FLAG_ROW", row) == row_flag
                    np.testing.assert_array_equal(version.getcell("FLAG", row), flags)


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
        for expr in ("XX", np.array("")):
            assert module.flagdata(vis=vis, correlation=expr, action="none") == {}
    assert_preserved(vis, before, auxiliary)

    for mode, correlation in (("rflag", "RR"),):
        reference, native = "rflag-rr-casa.ms", "rflag-rr-native.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, mode=mode, correlation=correlation, flagbackup=False) == {}
        assert_equal(reference, native)
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
        products += compare_stored_product_names(vis, metadata)
        compare_preview(vis)
        compare_backups(vis)
        refusals = compare_refusals(vis)
    print(f"{products} flag products, three previews, two inactive calls and "
          f"{refusals} paired argument errors compared with CASA, plus three backup "
          "comparisons and paired rflag correlation parity")


if __name__ == "__main__":
    main()
