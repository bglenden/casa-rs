"""The public summary record, interval edges and report output."""

import logging

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def compare_summary(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            compare_summary(actual[key], expected[key])
    elif isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        np.testing.assert_array_equal(actual, expected)
    elif isinstance(expected, float):
        assert type(actual) is float
        assert actual == pytest.approx(expected, abs=1e-10, rel=1e-13)
    else:
        assert type(actual) is type(expected)
        assert actual == expected


@pytest.mark.parametrize("selected", [False, True])
def test_ms_summary_matches_pinned_record_and_reports(native_dataset, tmp_path, caplog, selected):
    casa = pinned_casa()
    outputs = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.summary() == {}
        assert ms.open(str(native_dataset))
        if selected:
            assert ms.select({"rows": [3, 1, 0, 3]})
        filename = tmp_path / f"{api.__name__}.txt"
        with caplog.at_level(logging.INFO, logger="casars.tools.ms"):
            record = ms.summary(verbose=True, listunfl=True, cachesize=np.float32(12),
                                listfile=bytes(filename))
        assert record["numrecords"] == (3 if selected else ms.nrow())
        assert record["timeref"] == "UTC"
        assert record["nfields"] == 1
        assert record["IntegrationTime"] > 0
        report = filename.read_text()
        assert "MeasurementSet Name:" in report
        assert str(native_dataset) in report
        assert "nUnflRows" in report
        assert "ScanIntent" in report
        with pytest.raises(RuntimeError, match="already exists"):
            ms.summary(listfile=str(filename))
        assert filename.read_text() == report
        assert ms.summary(listfile=str(filename), overwrite=True, wantreturn=False) == {}
        assert "ScanIntent" not in filename.read_text()
        assert ms.summary(verbose=False, wantreturn=False) == {}
        assert ms.done()
        outputs.append(record)
    compare_summary(outputs[1], outputs[0])


def test_ms_summary_rejects_invalid_argument_forms():
    ms = tools.ms()
    for keyword in ("verbose", "listunfl", "overwrite", "wantreturn"):
        with pytest.raises(TypeError):
            ms.summary(**{keyword: 1})
    with pytest.raises(TypeError, match="argument _listfile"):
        ms.summary(listfile=None)
    with pytest.raises(TypeError):
        ms.summary(cachesize="50")


def test_ms_summary_reports_a_failed_file_write(native_dataset, tmp_path):
    rejected = tmp_path / "absent" / "summary.txt"
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    count = ms.nrow()
    with pytest.raises(RuntimeError, match="cannot write"):
        ms.summary(listfile=str(rejected))
    assert not rejected.exists()
    assert ms.summary()["numrecords"] == count
    assert ms.done()
