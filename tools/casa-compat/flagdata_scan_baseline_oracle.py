#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check public flagdata scan and baseline selection against installed CASA."""

from contextlib import chdir
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content, make_fixture
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot


def with_autocorrelations(path):
    make_fixture(path)
    with table(path, readonly=False, ack=False) as ms:
        scans, antennas = ms.getcol("SCAN_NUMBER"), ms.getcol("ANTENNA1")
        for scan in (1, 2):
            original = int(np.flatnonzero((scans == scan) & (antennas == 0))[0])
            added = ms.nrows()
            ms.addrows(1)
            for name in ms.colnames():
                if ms.iscelldefined(name, original):
                    value = 0 if name == "ANTENNA2" else ms.getcell(name, original)
                    ms.putcell(name, added, value)


def compare_apply():
    def touches(*ids):
        return lambda a, b: a != b and (a in ids or b in ids)

    def cross(first, second):
        return first != second

    def auto(first, second):
        return first == second == 0

    def pair(first, second):
        return {first, second} == {0, 1}

    def unaffected(first, second):
        return first == second or 0 not in (first, second)

    def all_rows(_first, _second):
        return True

    cases = (
        ({"scan": "<2"}, {1}, all_rows),
        ({"scan": ">1"}, {2}, all_rows),
        ({"scan": ">0&<2"}, {1}, all_rows),
        ({"scan": ">=1&<=2"}, {1, 2}, all_rows),
        ({"scan": "1~2"}, {1, 2}, all_rows),
        ({"scan": "1,99"}, {1}, all_rows),
        ({"scan": np.str_("2")}, {2}, all_rows),
        ({"scan": "1\u00a0"}, {1}, all_rows),
        ({"scan": "1\u00a0,2"}, {1}, all_rows),
        ({"scan": "2é1"}, {2}, all_rows),
        ({"scan": "1\x00,2"}, {1}, all_rows),
        ({"scan": "\x00,2"}, {1, 2}, all_rows),
        ({"antenna": "A0"}, {1, 2}, touches(0)),
        ({"antenna": "0"}, {1, 2}, touches(0)),
        ({"antenna": "A0,A1"}, {1, 2}, touches(0, 1)),
        ({"antenna": "A0,bad"}, {1, 2}, touches(0)),
        ({"antenna": "bad;A0"}, {1, 2}, touches(0)),
        ({"antenna": "A0,99"}, {1, 2}, touches(0)),
        ({"antenna": "!bad"}, {1, 2}, all_rows),
        ({"antenna": "A0;A1"}, {1, 2}, touches(0, 1)),
        ({"antenna": "0~1"}, {1, 2}, touches(0, 1)),
        ({"antenna": "A0&A1"}, {1, 2}, pair),
        ({"antenna": "A0&&"}, {1, 2}, auto),
        ({"antenna": "A0&&&"}, {1, 2}, auto),
        ({"antenna": "!A0"}, {1, 2}, unaffected),
        ({"antenna": "*"}, {1, 2}, cross),
        ({"antenna": "A*"}, {1, 2}, cross),
        ({"antenna": " A0 "}, {1, 2}, touches(0)),
        ({"antenna": np.str_("A0")}, {1, 2}, touches(0)),
        ({"scan": "<2", "antenna": "A0&A1", "field": "FLUX", "spw": "0:1~2"}, {1}, pair),
    )
    source = snapshot("data.ms")
    with table("data.ms", ack=False) as ms:
        metadata = zip(ms.getcol("SCAN_NUMBER"), ms.getcol("ANTENNA1"), ms.getcol("ANTENNA2"), strict=True)
        metadata = list(metadata)
    for index, (options, scans, baseline) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        for vis, module in ((reference, casatasks), (native, tasks)):
            copytree("data.ms", vis)
            assert module.flagdata(vis=vis, flagbackup=False, **options) == {}, options
        assert_equal(reference, native)

        affected = 0
        for (scan, first, second), before, after in zip(metadata, source, snapshot(native), strict=True):
            expected = before[1].copy()
            if scan in scans and baseline(first, second):
                channels = slice(1, 3) if "spw" in options else slice(None)
                expected[channels] = True
                affected += int(np.count_nonzero(expected != before[1]))
            else:
                assert after[0] == before[0]
            np.testing.assert_array_equal(after[1], expected, err_msg=str(options))
            np.testing.assert_array_equal(after[2], before[2])
        assert affected > 0, options
    return len(cases)


def compare_summary():
    before, auxiliary = snapshot("data.ms"), auxiliary_content("data.ms")
    for options in (
        {"scan": "<2", "antenna": "A0&&"},
        {"scan": "1,2", "antenna": "!A0"},
        {"scan": "2", "antenna": "A0&A1", "field": "PHASE"},
        {"antenna": "*"},
        {"spw": "0,99"},
        {"scan": "<2", "antenna": "A0&A1", "spw": "0~99"},
    ):
        args = {"vis": "data.ms", "mode": "summary", "basecnt": True, **options}
        assert tasks.flagdata(**args) == casatasks.flagdata(**args), options
        assert_preserved("data.ms", before, auxiliary)
    for key, name in (("scan", "1"), ("antenna", "A0")):
        for module in (casatasks, tasks):
            assert module.flagdata(vis="data.ms", action="none", **{key: [name]}) == {}
        assert_preserved("data.ms", before, auxiliary)


def compare_literal_antenna_names():
    vis = "literal-antenna-names.ms"
    copytree("data.ms", vis)
    with table(f"{vis}/ANTENNA", readonly=False, ack=False) as antennas:
        antennas.putcell("NAME", 1, "none")
        antennas.putcell("NAME", 2, "auto")
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    full = casatasks.flagdata(vis=vis, mode="summary")["total"]
    for name in ("none", "auto"):
        args = {"vis": vis, "mode": "summary", "antenna": name, "basecnt": True}
        expected = casatasks.flagdata(**args)
        assert 0 < expected["total"] < full
        assert all(name in baseline.split("&&") for baseline in expected["baseline"])
        assert tasks.flagdata(**args) == expected
        assert_preserved(vis, before, auxiliary)


def compare_refusals():
    before, auxiliary = snapshot("data.ms"), auxiliary_content("data.ms")
    cases = (
        ("scan", "0", RuntimeError, True),
        ("scan", "99", RuntimeError, True),
        ("scan", "none", RuntimeError, False),
        ("scan", "auto", RuntimeError, False),
        ("scan", "*", RuntimeError, False),
        ("scan", "  ", RuntimeError, False),
        ("scan", "1~", RuntimeError, False),
        ("antenna", "A0&", RuntimeError, True),
        ("antenna", "none", RuntimeError, True),
        ("antenna", "bad", RuntimeError, True),
        ("antenna", "99", RuntimeError, True),
        ("antenna", "A0&bad", RuntimeError, True),
        ("antenna", "A0 !bad", RuntimeError, False),
        ("antenna", "  ", RuntimeError, False),
        ("scan", ["1"], TypeError, False),
        ("scan", np.array("1"), TypeError, False),
        ("scan", 1, AssertionError, False),
        ("scan", None, AssertionError, False),
        ("antenna", ["A0"], TypeError, False),
        ("antenna", [], TypeError, False),
        ("antenna", np.array("A0"), TypeError, False),
        ("antenna", 0, AssertionError, False),
        ("antenna", None, AssertionError, False),
    )
    for key, value, kind, empty in cases:
        errors = []
        for module in (casatasks, tasks):
            try:
                module.flagdata(vis="data.ms", action="calculate", **{key: value})
            except kind as error:
                errors.append(str(error))
            else:
                raise AssertionError(f"{module.__name__} accepted {key}={value!r}")
            assert_preserved("data.ms", before, auxiliary)
        if kind is not RuntimeError:
            assert errors[0] == errors[1], (key, value, errors)
        elif empty:
            assert all("The selected table has zero rows" in message for message in errors), errors
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-scan-baseline-") as work, chdir(work):
        with_autocorrelations("data.ms")
        products = compare_apply()
        compare_summary()
        compare_literal_antenna_names()
        refusals = compare_refusals()
    print(f"{products} scan/baseline products, eight summaries, two inactive calls and {refusals} refusals compared with CASA")


if __name__ == "__main__":
    main()
