#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public flagdata time and observation selection with CASA 6.7.6.14."""

from contextlib import chdir
from datetime import datetime, timezone
from json import loads
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import (
    assert_preserved,
    auxiliary_content,
    changed_samples,
    make_fixture,
)
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot


def make_observation_fixture(path, second_day=False):
    make_fixture(path)
    start_mjd_seconds = datetime(2024, 3, 2, 12, tzinfo=timezone.utc).timestamp() + 40587 * 86400
    times = start_mjd_seconds + np.array([0, 10, 20, 30], dtype=float)
    if second_day:
        times[2:] += 86400
    with table(path, readonly=False, ack=False) as ms:
        unique, slot = np.unique(ms.getcol("TIME"), return_inverse=True)
        assert len(unique) == len(times)
        for name in ("TIME", "TIME_CENTROID"):
            ms.putcol(name, times[slot])
        ms.putcol("OBSERVATION_ID", (slot >= 2).astype(np.int32))
    with table(f"{path}/OBSERVATION", readonly=False, ack=False) as observations:
        if observations.nrows() == 1:
            observations.addrows(1)
            for name in observations.colnames():
                if observations.iscelldefined(name, 0):
                    observations.putcell(name, 1, observations.getcell(name, 0))
        for observation in range(2):
            first_slot = 2 * observation
            observations.putcell("TIME_RANGE", observation, times[first_slot:first_slot + 2])
    return slot


def compare_apply(slot):
    cases = (
        ({"timerange": "2024/03/02/12:00:00"}, {0}),
        ({"timerange": "12:00:00"}, {0}),
        ({"timerange": ">12:00:10"}, {1, 2, 3}),
        ({"timerange": "<12:00:20"}, {0, 1, 2}),
        ({"timerange": "12:00:10~12:00:20"}, {1, 2}),
        ({"timerange": "12:00:10+00:00:10"}, {1, 2}),
        ({"timerange": "12:00:00,12:00:30"}, {0, 3}),
        ({"timerange": "12:00:05"}, {0, 1}),
        ({"timerange": ">12:00:30"}, {3}),
        ({"timerange": "12:00:00~12:00:00"}, {0}),
        ({"timerange": "10"}, {1}),
        ({"timerange": np.str_("12:00:30")}, {3}),
        ({"timerange": "\x00"}, {0, 1, 2, 3}),
        ({"timerange": "12:00:00\x00,12:00:30"}, {0}),
        ({"timerange": "12:00:00\x00$unused"}, {0}),
        ({"observation": 0}, {0, 1}),
        ({"observation": 1}, {2, 3}),
        ({"observation": np.int32(1)}, {2, 3}),
        ({"observation": np.int64(1)}, {2, 3}),
        ({"observation": np.str_("1")}, {2, 3}),
        ({"observation": "0~1"}, {0, 1, 2, 3}),
        ({"observation": "<1"}, {0, 1}),
        ({"observation": ">0"}, {2, 3}),
        ({"observation": "0,99"}, {0, 1}),
        ({"observation": "0\u00a0,1"}, {0, 1}),
        ({"observation": "0\x00,1"}, {0, 1}),
        ({"observation": "\x00,1"}, {0, 1, 2, 3}),
        ({"timerange": "12:00:10~12:00:20", "observation": "1"}, {2}),
        ({"timerange": "12:00:10", "observation": 0, "field": "FLUX", "scan": "<2",
          "antenna": "A0&A1", "spw": "0:1~2"}, {1}),
    )
    source = snapshot("data.ms")
    with table("data.ms", ack=False) as ms:
        baselines = list(zip(ms.getcol("ANTENNA1"), ms.getcol("ANTENNA2"), strict=True))
    for index, (options, selected) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        for vis, module in ((reference, casatasks), (native, tasks)):
            copytree("data.ms", vis)
            assert module.flagdata(vis=vis, flagbackup=False, **options) == {}, options
        assert_equal(reference, native)

        affected = 0
        for row, (before, after) in enumerate(zip(source, snapshot(native), strict=True)):
            flags = before[1].copy()
            partial = "spw" in options
            if slot[row] in selected and (not partial or set(baselines[row]) == {0, 1}):
                flags[slice(1, 3) if partial else slice(None)] = True
                affected += int(np.count_nonzero(flags != before[1]))
            else:
                assert after[0] == before[0], options
            np.testing.assert_array_equal(after[1], flags, err_msg=str(options))
            np.testing.assert_array_equal(after[2], before[2])
        assert affected > 0, options
    return len(cases)


def compare_summary_and_preview(slot):
    vis = "data.ms"
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    summaries = (
        ({"observation": 1}, {2, 3}),
        ({"timerange": ">12:00:10", "observation": 0}, {1}),
        ({"timerange": "12:00:10~12:00:20", "observation": 1}, {2}),
    )
    for options, selected in summaries:
        args = {"vis": vis, "mode": "summary", "action": "calculate", "basecnt": True, **options}
        expected = casatasks.flagdata(**args)
        assert expected["total"] == sum(
            flags.size for index, (_, flags, _) in enumerate(before) if slot[index] in selected
        ), options
        assert tasks.flagdata(**args) == expected, options
        assert_preserved(vis, before, auxiliary)

    reference = "preview-reference.ms"
    copytree(vis, reference)
    options = {"timerange": "12:00:10~12:00:30", "observation": 1,
               "mode": "quack", "quackinterval": 5.0}
    assert casatasks.flagdata(vis=reference, flagbackup=False, **options) == {}
    deltas = changed_samples(before, snapshot(reference))
    assert {slot[index] for index, delta in enumerate(deltas) if delta} == {2}
    result = tasks.run("flagdata", vis=vis, action="calculate", flagbackup=True, **options)
    assert result.value == {}
    report = loads(result.stdout)["result"]
    assert report["changed_samples"] == sum(deltas)
    assert report["changed_rows"] == sum(delta > 0 for delta in deltas)
    assert report["backup_version"] is None
    assert_preserved(vis, before, auxiliary)

    for module in (casatasks, tasks):
        assert module.flagdata(vis=vis, action="none", timerange=["12:00:00"], observation=1) == {}
        assert module.flagdata(vis=vis, action="none", timerange="$unused") == {}
        assert module.flagdata(vis=vis, action="none", observation=2**63) == {}
    assert_preserved(vis, before, auxiliary)
    return len(summaries)


def compare_refusals():
    vis = "data.ms"
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    cases = (
        ({"timerange": ">13:00:00"}, RuntimeError, True),
        ({"timerange": "12:00:20~12:00:10"}, RuntimeError, False),
        ({"timerange": "12:00:00", "observation": 1}, RuntimeError, True),
        ({"timerange": "none"}, RuntimeError, False),
        ({"timerange": "auto"}, RuntimeError, False),
        ({"timerange": "   "}, RuntimeError, False),
        ({"timerange": "abc"}, RuntimeError, False),
        ({"timerange": "$invalid"}, RuntimeError, False),
        ({"timerange": ["12:00:00"]}, TypeError, False),
        ({"timerange": []}, TypeError, False),
        ({"timerange": np.array("12:00:00")}, TypeError, False),
        ({"timerange": 10}, AssertionError, False),
        ({"timerange": None}, AssertionError, False),
        ({"observation": 99}, RuntimeError, True),
        ({"observation": "<0"}, RuntimeError, True),
        ({"observation": "0~0"}, RuntimeError, False),
        ({"observation": "1~0"}, RuntimeError, False),
        ({"observation": "-1"}, RuntimeError, False),
        ({"observation": "none"}, RuntimeError, False),
        ({"observation": "auto"}, RuntimeError, False),
        ({"observation": " "}, RuntimeError, False),
        ({"observation": True}, RuntimeError, False),
        ({"observation": np.bool_(True)}, AssertionError, False),
        ({"observation": 1.0}, AssertionError, False),
        ({"observation": [1]}, AssertionError, False),
        ({"observation": ["1"]}, AssertionError, False),
        ({"observation": np.array(1)}, AssertionError, False),
        ({"observation": None}, AssertionError, False),
        ({"observation": np.int64(2**31)}, RuntimeError, False),
        ({"observation": 2**63}, RuntimeError, False),
    )
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
    return len(cases)


def compare_overflow(slot):
    vis = "data.ms"
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for index, observation in enumerate((2**32, str(2**32))):
        reference = f"overflow-{index}.ms"
        copytree(vis, reference)
        assert casatasks.flagdata(vis=reference, observation=observation, flagbackup=False) == {}
        differences = changed_samples(before, snapshot(reference))
        assert {slot[row] for row, count in enumerate(differences) if count} == {0, 1}
        try:
            tasks.flagdata(vis=vis, observation=observation, flagbackup=False)
        except RuntimeError as error:
            assert str(observation) in str(error), error
        else:
            raise AssertionError(f"flagdata accepted overflowing observation {observation!r}")
        assert_preserved(vis, before, auxiliary)


def compare_logical_date():
    vis = "two-days.ms"
    slot = make_observation_fixture(vis, second_day=True)
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    total_at_second_day_start = sum(
        flags.size for row, (_, flags, _) in enumerate(before) if slot[row] == 2
    )
    row_filters = ({"observation": 1}, {"scan": "2"}, {"field": "PHASE"})
    for row_filter in row_filters:
        # The physical first row is March 2. The first row that meets each
        # filter is March 3, which supplies the date for a time-of-day alone.
        options = {"vis": vis, "mode": "summary", "timerange": "12:00:20", **row_filter}
        expected = casatasks.flagdata(**options)
        assert expected["total"] == total_at_second_day_start, row_filter
        assert tasks.flagdata(**options) == expected, row_filter
        assert_preserved(vis, before, auxiliary)
    return len(row_filters)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-time-observation-") as work, chdir(work):
        slot = make_observation_fixture("data.ms")
        applied = compare_apply(slot)
        summaries = compare_summary_and_preview(slot)
        summaries += compare_logical_date()
        refusals = compare_refusals()
        compare_overflow(slot)
    print(f"{applied} flag products, {summaries} summaries, one preview, three inactive calls, "
          f"and {refusals} refusals compared with CASA, plus two verified overflow protections")


if __name__ == "__main__":
    main()
