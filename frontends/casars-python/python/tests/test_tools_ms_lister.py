"""Visibility listings from the public MeasurementSet tool."""

import re
import subprocess
import sys

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def report_rows(report):
    return [line.strip().split() for line in report.splitlines()
            if re.match(r"\s*\d{2}:\d{2}:\d{2}\.\d", line)]


@pytest.mark.parametrize("selection", [
    {},
    {"spw": "0:1~2", "correlation": "RR", "pagerows": 0},
    {"spw": "0:3;1;3", "correlation": "ll/RR", "pagerows": 2**32 + 1},
    {"spw": "0:1~2^2", "correlation": "RRLL", "pagerows": np.int64(2)},
    {"correlation": "*", "pagerows": True, "field": b"0"},
])
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
    for report in reports:
        if selection.get("spw") == "0:3;1;3":
            assert "CHANNEL: 3" in report
        if selection.get("spw") == "0:1~2^2":
            assert "Chn" in report
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


def test_ms_lister_does_not_publish_failed_reports(native_dataset, tmp_path):
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

    actual = tools.ms()
    assert actual.open(str(native_dataset))
    for kwargs, error in [
        ({"datacolumn": "float_data"}, RuntimeError),
        ({"pagerows": np.bool_(True)}, TypeError),
    ]:
        path = tmp_path / f"{next(iter(kwargs))}.txt"
        with pytest.raises(error):
            actual.lister(listfile=str(path), **kwargs)
        assert not path.exists()
        assert actual.nrow() > 0
    assert actual.done()


def test_ms_lister_reports_alternate_visibility_products(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    data = table.getcol("DATA")
    descriptor = table.getcoldesc("DATA")
    storage = {key: descriptor[key] for key in ("ndim", "shape", "option", "maxlen")
               if key in descriptor}
    for name, value, primitive in (
        ("CORRECTED_DATA", 3 + 4j, "complex"),
        ("MODEL_DATA", 3 + 0j, "complex"),
        ("FLOAT_DATA", 2.5, "float"),
    ):
        table.addcols({name: dict(storage, valueType=primitive, dataManagerType="StandardStMan",
                                 dataManagerGroup=f"lister_{name}", comment=name)})
        dtype = np.float32 if primitive == "float" else np.complex64
        assert table.putcol(name, np.full(data.shape, value, dtype=dtype))
    assert table.done()

    reference, actual = casa.ms(), tools.ms()
    for tool in (reference, actual):
        assert tool.open(str(native_dataset))
        assert tool.select({"rows": [0]})
    for column, amplitude in (("corrected", 5), ("model", 3), ("residual", 4), ("float_data", 2.5)):
        rows = []
        for index, tool in enumerate((reference, actual)):
            path = tmp_path / f"{column}-{index}.txt"
            assert tool.lister(datacolumn=column, correlation="RR", listfile=str(path))
            observed = report_rows(path.read_text())
            assert len(observed) == data.shape[1]
            assert all(float(row[4]) == amplitude for row in observed)
            rows.append(observed)
        assert rows[0] == rows[1]
    for tool in (reference, actual):
        assert tool.done()

    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcol("FLOAT_DATA", np.full(data.shape, -7.0, dtype=np.float32))
    assert table.done()
    for tool in (reference, actual):
        assert tool.open(str(native_dataset))
        assert tool.select({"rows": [0]})
    with pytest.raises(RuntimeError, match="Error in MSLister::list"):
        reference.lister(datacolumn="float_data", correlation="RR", listfile=str(tmp_path / "negative-casa.txt"))
    path = tmp_path / "negative-native.txt"
    assert actual.lister(datacolumn="float_data", correlation="RR", listfile=str(path))
    rows = report_rows(path.read_text())
    assert len(rows) == data.shape[1]
    assert all(row[4] == "-7.000" for row in rows)
    assert reference.done()
    assert actual.done()


def test_ms_lister_interactive_quit_leaves_selection_usable(native_dataset):
    pinned_casa()
    script = """
import importlib
import sys

api = importlib.import_module(sys.argv[1])
tool = api.ms()
assert tool.open(sys.argv[2])
assert tool.select({"rows": [0]})
assert tool.lister(correlation="RR", pagerows=1)
assert tool.nrow(True) == 1
assert tool.done()
"""
    reports = []
    for api in ("casatools", "casars.tools"):
        result = subprocess.run(
            [sys.executable, "-c", script, api, str(native_dataset)],
            input="Q\n", text=True, capture_output=True, check=True, timeout=30,
        )
        assert result.stdout.count("Type Q to quit") == 1
        rows = report_rows(result.stdout)
        assert len(rows) == 1
        reports.append(rows)
    assert reports[0] == reports[1]
