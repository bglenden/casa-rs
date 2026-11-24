"""Visibility listings from the public MeasurementSet tool."""

import re

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def report_rows(report):
    return [line.strip().split() for line in report.splitlines()
            if re.match(r"\s*\d{2}:\d{2}:\d{2}\.\d", line)]


@pytest.mark.parametrize("selection", [{}, {"spw": "0:1~2", "correlation": "RR", "pagerows": 0}])
def test_ms_lister_matches_pinned_visibility_records(native_dataset, tmp_path, selection):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    original = table.getcol("DATA")
    data = np.arange(original.size, dtype=np.float32).reshape(original.shape, order="F")
    assert table.putcol("DATA", (data - 0.5j * data).astype(np.complex64))
    flags = table.getcol("FLAG")
    flags[0, 1, 0] = True
    assert table.putcol("FLAG", flags)
    assert table.done()

    reports = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.lister() is False
        assert ms.open(str(native_dataset))
        assert ms.select({"rows": list(range(20))})
        output = tmp_path / f"{api.__name__}.listing"
        assert ms.lister(listfile=bytes(output), **selection)
        report = output.read_text()
        assert "UVDist(wavelength)" in report
        assert "F" in next(row for row in report_rows(report) if "F" in row)
        assert ms.nrow(True) == 20
        reports.append(report)
        with pytest.raises(RuntimeError, match="Error in MSLister::list"):
            ms.lister(listfile=str(output), **selection)
        assert output.read_text() == report
        assert ms.done()
    assert len(reports[1].splitlines()) == len(reports[0].splitlines())
    expected = report_rows(reports[0])
    actual = report_rows(reports[1])
    assert len(actual) == len(expected)
    for observed, oracle in zip(actual, expected):
        assert len(observed) == len(oracle)
        for native_value, casa_value in zip(observed, oracle):
            try:
                number = float(casa_value)
            except ValueError:
                assert native_value == casa_value
            else:
                assert float(native_value) == pytest.approx(number, abs=2e-3)


def test_ms_lister_preserves_existing_files_and_does_not_publish_a_failed_report(native_dataset, tmp_path):
    casa = pinned_casa()
    outputs = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        upper = tmp_path / f"{api.__name__}-uppercase.txt"
        with pytest.raises(RuntimeError, match="Error in MSLister::list"):
            ms.lister(datacolumn="DATA", listfile=str(upper))
        assert not upper.exists()
        missing = tmp_path / f"{api.__name__}-missing.txt"
        with pytest.raises(RuntimeError, match="Error in MSLister::list"):
            ms.lister(datacolumn="model", listfile=str(missing))
        outputs.append(missing.exists())
        assert ms.nrow() > 0
        assert ms.done()
    assert outputs == [True, False]
