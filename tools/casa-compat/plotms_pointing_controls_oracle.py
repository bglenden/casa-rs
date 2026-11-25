# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired public PlotMS cases for antenna direction controls."""

from plotms_ms_columns_oracle import compare_text, reference


def compare_pointing_lists(work, native, vis):
    base = dict(vis=str(vis), xaxis="time", yaxis="ant-dec", antenna="A0&&&",
                showgui=False, overwrite=True)
    expected, actual = (work / f"one-layer.{engine}.txt" for engine in ("casa", "native"))
    options = base | dict(yframe=["j2000"], yinterp=["nearest"], expformat="txt")
    assert reference(**options, plotfile=str(expected)) is None
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
