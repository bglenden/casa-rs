#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flagdata array selection and the feed-write protection against CASA."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from signal import SIGABRT
from subprocess import run
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot
from flagdata_time_observation_oracle import make_observation_fixture
from ms_selection_tasks_oracle import add_arrays_and_feeds


def fixture(path):
    slots = make_observation_fixture(path)
    add_arrays_and_feeds(Path(path))
    with table(path, readonly=False, ack=False) as ms:
        # Cross each subarray with more than one feed pair.
        arrays = 2 + 2 * ((np.arange(ms.nrows()) // 4) % 4)
        ms.putcol("ARRAY_ID", arrays)
        pairs = list(zip(ms.getcol("FEED1"), ms.getcol("FEED2"), strict=True))
    return arrays, pairs, slots


def row_keys(path):
    with table(path, ack=False) as ms:
        return list(zip(ms.getcol("TIME"), ms.getcol("ANTENNA1"),
                        ms.getcol("ANTENNA2"), strict=True))


def assert_flags(path, source, rows):
    changed = 0
    for row, (before, after) in enumerate(zip(source, snapshot(path), strict=True)):
        expected = before[1].copy()
        if row in rows:
            changed += expected.size - np.count_nonzero(expected)
            expected[:] = True
            assert after[0], row
        else:
            assert after[0] == before[0], row
        np.testing.assert_array_equal(after[1], expected, err_msg=f"{path}, row {row}")
        np.testing.assert_array_equal(after[2], before[2])
    assert changed > 0, path
    return changed


def compare_array(vis, arrays, slots):
    cases = (
        ({"array": "2"}, {2}, {0, 1, 2, 3}),
        ({"array": "2,6"}, {2, 6}, {0, 1, 2, 3}),
        ({"array": "6,2,6"}, {2, 6}, {0, 1, 2, 3}),
        ({"array": "2,99"}, {2}, {0, 1, 2, 3}),
        ({"array": "2~6"}, {2, 4, 6}, {0, 1, 2, 3}),
        ({"array": "<4"}, {2}, {0, 1, 2, 3}),
        ({"array": ">4"}, {6, 8}, {0, 1, 2, 3}),
        ({"array": ">=4&<=6"}, {4, 6}, {0, 1, 2, 3}),
        ({"array": ">2&<8"}, {4, 6}, {0, 1, 2, 3}),
        ({"array": np.str_("8")}, {8}, {0, 1, 2, 3}),
        ({"array": "2\u00a0,6"}, {2}, {0, 1, 2, 3}),
        ({"array": "2\x00$ignored"}, {2}, {0, 1, 2, 3}),
        ({"array": "\x00,2"}, {2, 4, 6, 8}, {0, 1, 2, 3}),
        ({"array": "2,8", "timerange": ">12:00:10", "observation": 0}, {2, 8}, {1}),
    )
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for index, (options, selected, times) in enumerate(cases):
        reference, native = f"array-reference-{index}.ms", f"array-native-{index}.ms"
        rows = {row for row, (array, slot) in enumerate(zip(arrays, slots, strict=True))
                if array in selected and slot in times}
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, flagbackup=False, **options) == {}, options
        assert_equal(reference, native)
        assert_flags(native, before, rows)
    for selector in ("2", "4,8", "\x00,8"):
        options = dict(vis=vis, mode="summary", array=selector, basecnt=True)
        assert tasks.flagdata(**options) == casatasks.flagdata(**options)
        assert_preserved(vis, before, auxiliary)
    return len(cases)


def compare_feed(vis, arrays, pairs):
    equal_zero, equal_two = (0, 0), (2, 2)
    cross = {(0, 2), (2, 0)}
    cases = (
        ({"feed": "0"}, cross),
        ({"feed": "0&2"}, cross),
        ({"feed": "2&0"}, cross),
        ({"feed": np.str_("0,2&&")}, cross | {equal_zero, equal_two}),
        ({"feed": "0&&&"}, {equal_zero}),
        ({"feed": "2&&&"}, {equal_two}),
        ({"feed": "0,1"}, cross),
        ({"feed": "0~2&&"}, cross | {equal_zero, equal_two}),
        ({"feed": "!0"}, {equal_zero, equal_two}),
        ({"feed": "0&&&;2&&&"}, {equal_zero, equal_two}),
        ({"feed": "0&2", "array": "2~4"}, cross),
    )
    source_keys = row_keys(vis)
    assert len(set(source_keys)) == len(source_keys)
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for index, (options, eligible_pairs) in enumerate(cases):
        rows = {row for row, (array, pair) in enumerate(zip(arrays, pairs, strict=True))
                if pair in eligible_pairs and ("array" not in options or array in {2, 4})}
        reference, native = f"feed-selection-{index}.ms", f"feed-native-{index}.ms"
        casatasks.split(vis=vis, outputvis=reference, datacolumn="data", **options)
        assert set(row_keys(reference)) == {source_keys[row] for row in rows}, options
        copytree(vis, native)
        assert tasks.flagdata(vis=native, flagbackup=False, **options) == {}, options
        assert_flags(native, before, rows)

        summary = tasks.flagdata(vis=vis, mode="summary", **options)
        oracle_summary = casatasks.flagdata(vis=reference, mode="summary")
        for value in ("total", "flagged"):
            assert summary[value] == oracle_summary[value], (options, value)
        assert_preserved(vis, before, auxiliary)

    reference = "ignored-feed.ms"
    copytree(vis, reference)
    assert casatasks.flagdata(vis=reference, feed="0&2", flagbackup=False) == {}
    assert_flags(reference, before, set(range(len(before))))
    changes = changed_samples(before, snapshot(reference))
    assert any(delta for delta, pair in zip(changes, pairs, strict=True) if pair not in cross)

    result = tasks.run("flagdata", vis=vis, action="calculate", feed="0&2", flagbackup=True)
    report = loads(result.stdout)["result"]
    expected = [delta for delta, pair in zip(changes, pairs, strict=True) if pair in cross]
    assert report["changed_samples"] == sum(expected)
    assert report["changed_rows"] == sum(delta > 0 for delta in expected)
    assert report["backup_version"] is None
    assert_preserved(vis, before, auxiliary)
    return len(cases)


def compare_refusals(vis):
    cases = (
        ({"array": "99"}, RuntimeError, True),
        ({"array": "2~2"}, RuntimeError, False),
        ({"array": "none"}, RuntimeError, False),
        ({"array": "auto"}, RuntimeError, False),
        ({"array": " "}, RuntimeError, False),
        ({"array": "*"}, RuntimeError, False),
        ({"array": ["2"]}, TypeError, False),
        ({"array": np.array("2")}, TypeError, False),
        ({"array": 2}, AssertionError, False),
        ({"array": None}, AssertionError, False),
        ({"feed": ["0"]}, TypeError, False),
        ({"feed": []}, TypeError, False),
        ({"feed": np.array("0")}, TypeError, False),
        ({"feed": np.int64(0)}, AssertionError, False),
        ({"feed": None}, AssertionError, False),
        ({"array": ["2"], "feed": ["0"]}, TypeError, False),
    )
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for options, kind, empty in cases:
        errors = []
        for module in (casatasks, tasks):
            try:
                module.flagdata(vis=vis, action="calculate", **options)
            except kind as error:
                errors.append(str(error))
            else:
                raise AssertionError(f"{module.__name__} accepted {options}")
            assert_preserved(vis, before, auxiliary)
        if kind is not RuntimeError:
            assert errors[0] == errors[1], (options, errors)
        elif empty:
            assert all("The selected table has zero rows" in message for message in errors), errors
    for module in (casatasks, tasks):
        assert module.flagdata(vis=vis, action="none", array=["2"], feed=["0"]) == {}
    assert_preserved(vis, before, auxiliary)

    for module, kind in ((casatasks, UnicodeDecodeError), (tasks, RuntimeError)):
        try:
            module.flagdata(vis=vis, array="\u00a02", action="calculate")
        except kind as error:
            if module is tasks:
                assert "array" in str(error).lower()
        else:
            raise AssertionError(f"{module.__name__} accepted invalid array text")
        assert_preserved(vis, before, auxiliary)

    for feed in ("99", "2~0", "none", "\x00$invalid"):
        try:
            tasks.flagdata(vis=vis, feed=feed, flagbackup=False)
        except RuntimeError as error:
            assert "feed" in str(error).lower()
        else:
            raise AssertionError(f"flagdata accepted invalid feed {feed!r}")
        assert_preserved(vis, before, auxiliary)
    return len(cases)


def compare_array_bounds(vis, arrays):
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    wrapped = str(2**32 + 2)
    reference = "array-wrap.ms"
    copytree(vis, reference)
    assert casatasks.flagdata(vis=reference, array=wrapped, flagbackup=False) == {}
    assert_flags(reference, before, {row for row, array in enumerate(arrays) if array == 2})
    try:
        tasks.flagdata(vis=vis, array=wrapped, flagbackup=False)
    except RuntimeError as error:
        assert wrapped in str(error), error
    else:
        raise AssertionError(f"flagdata accepted overflowing array ID {wrapped}")
    assert_preserved(vis, before, auxiliary)

    high = "high-array.ms"
    copytree(vis, high)
    high_rows = {0, 5}
    with table(high, readonly=False, ack=False) as ms:
        for row in high_rows:
            ms.putcell("ARRAY_ID", row, 1003)
    before, auxiliary = snapshot(high), auxiliary_content(high)
    script = (
        "import resource\n"
        "import sys\n"
        "import casatasks\n"
        "resource.setrlimit(resource.RLIMIT_CORE, (0, 0))\n"
        "casatasks.flagdata(vis=sys.argv[1], array=sys.argv[2], action='calculate')\n"
    )
    for index, array in enumerate((">1002", ">=1002")):
        crashed = run([sys.executable, "-c", script, high, array], capture_output=True, text=True)
        assert crashed.returncode == -SIGABRT and "Negative shape" in crashed.stderr, (
            array, crashed.returncode, crashed.stderr
        )
        assert_preserved(high, before, auxiliary)
        native = f"high-array-native-{index}.ms"
        copytree(high, native)
        assert tasks.flagdata(vis=native, array=array, flagbackup=False) == {}
        assert_flags(native, before, high_rows)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-array-feed-") as work, chdir(work):
        vis = "data.ms"
        arrays, pairs, slots = fixture(vis)
        array_cases = compare_array(vis, arrays, slots)
        feed_cases = compare_feed(vis, arrays, pairs)
        refusals = compare_refusals(vis)
        compare_array_bounds(vis, arrays)
    print(f"{array_cases} array products, three array summaries, {feed_cases} feed products "
          f"and summaries, a feed preview, one inactive call, {refusals} paired refusals, "
          "one feed-write bug and four feed refusal protections, "
          "an array diagnostic, one array ID overflow protection and two high-array crash fixes")


if __name__ == "__main__":
    main()
