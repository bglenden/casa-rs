#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare stored flagdata correlation summaries with CASA 6.7.6.14."""

from contextlib import chdir
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_correlation_products_oracle import (
    add_circular_polarization, make_fixture, mixed_products_fixture,
)
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot


def compare(path, **options):
    arguments = {"vis": path, "mode": "summary", **options}
    before, auxiliary = snapshot(path), auxiliary_content(path)
    expected = casatasks.flagdata(**arguments)
    actual = tasks.flagdata(**arguments)
    assert actual == expected, (options, expected, actual)
    assert list(actual) == list(expected), (options, list(expected), list(actual))
    assert_preserved(path, before, auxiliary)
    return actual


def compare_no_report(path, **options):
    before, auxiliary = snapshot(path), auxiliary_content(path)
    for task in (casatasks.flagdata, tasks.flagdata):
        try:
            task(vis=path, mode="summary", **options)
        except KeyError as error:
            assert error.args == ("nreport",), (options, error)
        else:
            raise AssertionError(f"{task!r} returned a summary for {options!r}")
        assert_preserved(path, before, auxiliary)


def check_plane_counts(path, mask, plane, channels, result, label):
    flags = [row_flags[channels, plane]
             for (_, row_flags, _), selected in zip(snapshot(path), mask, strict=True)
             if selected]
    expected = {"flagged": float(sum(np.count_nonzero(row) for row in flags)),
                "total": float(sum(row.size for row in flags))}
    assert result["correlation"] == {label: expected}
    assert all(result[name] == count for name, count in expected.items())


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-correlation-summary-") as work, chdir(work):
        path = "mixed.ms"
        ddids = mixed_products_fixture(path)["DATA_DESC_ID"]
        axes = {"spwchan": True, "spwcorr": True, "basecnt": True}
        linear = compare(path, correlation="XX", **axes)
        check_plane_counts(path, ddids == 0, 0, slice(None), linear, "XX")
        assert set(linear["spw:correlation"]) == {"0:XX"}
        assert set(linear["spw:channel"]) == {"0:0", "0:1", "0:2", "0:3"}
        circular = compare(path, correlation="RR", spw="1:1~2", **axes)
        check_plane_counts(path, ddids == 1, 1, slice(1, 3), circular, "RR")
        assert set(circular["spw:correlation"]) == {"1:RR"}
        assert set(circular["spw:channel"]) == {"1:0", "1:1"}
        all_spelling = compare(path, correlation="ALL", **axes)
        check_plane_counts(path, ddids == 1, 0, slice(None), all_spelling, "LL")
        grouped = compare(path, correlation="XX,YY", fieldcnt=True, name="counts", **axes)
        assert set(grouped) == {"FLUX", "name", "type"}
        assert set(grouped["FLUX"]["correlation"]) == {"XX", "YY"}
        for options in (
            {"correlation": "LL", "spw": "0", "name": "empty", **axes},
            {"correlation": "LL", "field": "FLUX", "fieldcnt": True, **axes},
            {"correlation": "XX", "minabs": 999, "fieldcnt": True},
            {"correlation": "XX", "minabs": 999},
            {"correlation": "XX,QQ", "action": "calculate", **axes},
            {"correlation": "\0XX", **axes},
            {"correlation": np.array(""), **axes},
            {"correlation": "XX", "action": "none"},
            {"correlation": "RR,WVR", **axes},
            {"correlation": "WVRXX", **axes},
        ):
            compare(path, **options)
        unsliced = compare(path, **axes)
        for expression in ("ABSXX", "ABS_XX", "REAL_YY", "IMAG_RR", "ARG",
                           "NORM_RR", "ABS_I", "ABS_BAD", "ABS_WVR"):
            assert compare(path, correlation=expression, **axes) == unsliced
        for options in (
            {"correlation": "XX,RR", "fieldcnt": True},
            {"correlation": "XX,RR", "field": "FLUX"},
            {"correlation": "QQ", "field": "FLUX"},
            {"correlation": "WVR"},
        ):
            compare_no_report(path, **options)
        make_fixture("linear.ms")
        compare_no_report("linear.ms", correlation="ALL")
        add_circular_polarization("linear.ms")
        compare_no_report("linear.ms", correlation="RR")
        copytree(path, "flagged.ms")
        with table("flagged.ms/DATA_DESCRIPTION", readonly=False, ack=False) as dd:
            dd.putcell("FLAG_ROW", 1, True)
        compare_no_report("flagged.ms", correlation="RR")
        with table("flagged.ms/DATA_DESCRIPTION", readonly=False, ack=False) as dd:
            dd.putcell("POLARIZATION_ID", 1, 0)
        shared = compare("flagged.ms", correlation="XX", **axes)
        check_plane_counts("flagged.ms", np.ones_like(ddids, dtype=bool),
                           0, slice(None), shared, "XX")
    print("25 stored correlation summaries and seven missing reports matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
