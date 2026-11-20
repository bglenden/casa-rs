#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare clip mode's bare receptor expressions with CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_autocorr_oracle import assert_same_visibilities
from flagdata_clip_range_oracle import FLOAT_SAMPLES, make_input
from flagdata_correlation_products_oracle import make_fixture, mixed_products_fixture
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot


def make_inputs():
    make_fixture("linear.ms")
    mixed_products_fixture("mixed.ms")
    make_input("float.ms", float_data=True)
    setups = (
        ("circular", [8, 5]),
        ("duplicate", [9, 9]),
        ("cross-linear", [10, 11]),
        ("cross-circular", [7, 6]),
    )
    for name, codes in setups:
        copytree("linear.ms", f"{name}.ms")
        with table(f"{name}.ms/POLARIZATION", readonly=False, ack=False) as pol:
            pol.putcell("CORR_TYPE", 0, np.array(codes, dtype=np.int32))
            if name.startswith("cross-"):
                pol.putcell("CORR_PRODUCT", 0, np.array([[0, 1], [1, 0]], dtype=np.int32))
    copytree("circular.ms", "unused-product.ms")
    with table("unused-product.ms/POLARIZATION", readonly=False, ack=False) as pol:
        row = pol.nrows()
        pol.addrows(1)
        for name in pol.colnames():
            if pol.iscelldefined(name, 0):
                pol.putcell(name, row, pol.getcell(name, 0))
        pol.putcell("CORR_TYPE", row, np.array([24, 24], dtype=np.int32))
    data = np.array([
        [0, 0.25j],
        [1, 1.5j],
        [1.5 + 2j, 2],
        [np.nan, 3j],
    ], dtype=np.complex64)
    ddids = {}
    for name in ("linear", "float", "mixed", "unused-product", *(name for name, _ in setups)):
        with table(f"{name}.ms", readonly=False, ack=False) as ms:
            for row in range(ms.nrows()):
                ms.putcell("DATA", row, data if row % 2 == 0 else data[:, ::-1])
            ddids[name] = ms.getcol("DATA_DESC_ID")
    return ddids


def compare_products(ddids):
    whole = slice(None)
    first, second, both = {0: (0,)}, {0: (1,)}, {0: (0, 1)}
    cases = (
        ("mixed", "XX", first, whole, {}),
        ("mixed", "yy", second, whole, {}),
        ("mixed", "XXYY", both, whole, {}),
        ("mixed", "YY;XX", both, whole, {}),
        ("mixed", "fooXXbar", first, whole, {}),
        ("mixed", "XX\0,YY", first, whole, {}),
        ("mixed", "RR", {1: (1,)}, whole, {}),
        ("mixed", "LL", {1: (0,)}, whole, {}),
        ("mixed", "RR", {1: (1,)}, slice(1, 3), {"spw": "1:1~2"}),
        ("mixed", "RR", {}, whole, {"spw": "0"}),
        ("mixed", "XX", first, whole, {"clipoutside": False}),
        ("mixed", "XX", first, whole, {"clipoutside": False, "clipzeros": True}),
        ("mixed", "XX,RR", {}, whole, {}),
        ("mixed", "ALL", {}, whole, {}),
        ("mixed", "I", {}, whole, {}),
        ("mixed", "ZZ", {}, whole, {}),
        ("linear", "ALL", {}, whole, {}),
        ("circular", "ALL", both, whole, {}),
        ("unused-product", "ALL", both, whole, {}),
        ("circular", "XX,QQ", {}, whole, {}),
        ("circular", "LLRR", both, whole, {}),
        ("duplicate", "XX", second, whole, {}),
        ("cross-linear", "XY", first, whole, {}),
        ("cross-linear", "YX", second, whole, {}),
        ("cross-circular", "LR", first, whole, {}),
        ("cross-circular", "RL", second, whole, {}),
        ("float", "YY", second, whole, {"datacolumn": "FLOAT_DATA"}),
    )
    for index, (source, expression, planes, channels, overrides) in enumerate(cases):
        vis = f"{source}.ms"
        before = snapshot(vis)
        options = dict(mode="clip", correlation=expression, flagbackup=False,
                       clipminmax=[1, 2], **overrides)
        reference, native = f"clip-reference-{index}.ms", f"clip-native-{index}.ms"
        for path, task in ((reference, casatasks.flagdata), (native, tasks.flagdata)):
            copytree(vis, path)
            assert task(vis=path, **options) == {}, options
        assert_equal(reference, native)
        for ddid, old, new in zip(ddids[source], before, snapshot(native), strict=True):
            selected = list(planes.get(ddid, ()))
            values = (FLOAT_SAMPLES[channels, selected] if source == "float"
                      else np.abs(old[2][channels, selected]))
            inside = (values >= 1) & (values <= 2)
            clipped = ~inside if options.get("clipoutside", True) else inside
            clipped |= ~np.isfinite(values)
            if options.get("clipzeros"):
                clipped |= values <= np.finfo(np.float32).eps
            expected = old[1].copy()
            expected[channels, selected] |= clipped
            np.testing.assert_array_equal(new[1], expected, err_msg=str(options))
            assert new[0] == old[0], options
            np.testing.assert_array_equal(new[2], old[2])
    return len(cases)


def compare_preview_and_backup(vis):
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    options = dict(mode="clip", correlation="RR", clipminmax=[1, 2], spw="1:1~2")
    copytree(vis, "preview-reference.ms")
    assert casatasks.flagdata(vis="preview-reference.ms", flagbackup=False, **options) == {}
    expected = changed_samples(before, snapshot("preview-reference.ms"))
    assert casatasks.flagdata(vis=vis, action="calculate", **options) == {}
    preview = tasks.run("flagdata", vis=vis, action="calculate", **options)
    assert preview.value == {}
    report = loads(preview.stdout)["result"]
    assert report["changed_samples"] == sum(expected)
    assert report["changed_rows"] == sum(change > 0 for change in expected)
    assert report["backup_version"] is None
    assert_preserved(vis, before, auxiliary)

    no_matches = ((vis, "XX,RR"), ("circular.ms", "XX,QQ"))
    for index, (source, expression) in enumerate(no_matches):
        flags_before = snapshot(source)
        for name, task in (("reference", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"clip-backup-{index}-{name}.ms"
            copytree(source, path)
            assert task(vis=path, mode="clip", correlation=expression, clipminmax=[1, 2]) == {}
            assert_same_visibilities(path, flags_before)
            assert Path(f"{path}.flagversions/flags.flagdata_1").is_dir()

    for expression in ("ABS_I", "NORM_Q", "XX,QQ", "WVR"):
        try:
            tasks.flagdata(vis=vis, mode="clip", correlation=expression, clipminmax=[1, 2])
        except RuntimeError as error:
            assert "#978" in str(error), expression
        else:
            raise AssertionError(f"unsupported visibility expression {expression!r}")
        assert_preserved(vis, before, auxiliary)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-correlation-") as directory, chdir(directory):
        products = compare_products(make_inputs())
        compare_preview_and_backup("mixed.ms")
    print(f"{products} clip products, one preview and two backups matched CASA 6.7.6.14, "
          "plus four guarded visibility expressions")


if __name__ == "__main__":
    main()
