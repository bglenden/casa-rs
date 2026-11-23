"""Selection side records independent of the rows which happen to match."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset
from test_tools_simulator import configure


def same_record(actual, expected):
    assert set(actual) == set(expected)
    for name, value in expected.items():
        np.testing.assert_array_equal(actual[name], value, strict=True, err_msg=name)


def test_ms_selection_side_records_and_reset(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    row_count = table.nrows()
    scans = np.where(np.arange(row_count) % 2, 3, 5).astype(np.int32)
    assert table.putcol("SCAN_NUMBER", scans)
    left, right = table.getcol("ANTENNA1"), table.getcol("ANTENNA2")
    assert table.done()

    def expected_rows():
        baseline = ((left == 0) & (right == 2)) | ((left == 2) & (right == 0))
        return np.count_nonzero(baseline)

    collected = []
    for api in (casa, tools):
        ms = api.ms()
        with pytest.raises(RuntimeError, match="TIME"):
            ms.msselectedindices()
        assert ms.open(str(native_dataset))
        empty = ms.msselectedindices()
        assert all(value.dtype == np.int64 for value in empty.values())
        for name, value in empty.items():
            assert value.shape == ((0, 0) if name in ("channel", "baselines") else (0,))
        selection = {
            "field": "F", "scan": "5,3,5", "baseline": "A3&A1;A1&A3",
            "spw": "0:0~3^2", "polarization": "RR", "observation": "0",
        }
        assert ms.msselect(selection, onlyparse=True)
        assert ms.nrow(True) == row_count
        parsed = ms.msselectedindices()
        np.testing.assert_array_equal(parsed["baselines"], np.array([[2, 0]], dtype=np.int64), strict=True)
        np.testing.assert_array_equal(parsed["scan"], np.array([5, 3, 5], dtype=np.int64), strict=True)
        np.testing.assert_array_equal(parsed["channel"], np.array([[0, 0, 3, 2]], dtype=np.int64), strict=True)
        assert ms.msselect(selection)
        assert ms.nrow(True) == expected_rows()
        same_record(parsed, ms.msselectedindices())
        selected = ms.nrow(True)
        assert not ms.msselect({"field": "none_of_these"})
        with pytest.raises(RuntimeError):
            ms.msselectedindices()
        assert ms.nrow(True) == selected
        assert ms.reset()
        assert ms.nrow(True) == row_count
        same_record(ms.msselectedindices(), empty)
        assert ms.msselect({"scan": "20"}, onlyparse=True)
        np.testing.assert_array_equal(ms.msselectedindices()["scan"], np.array([20], dtype=np.int64), strict=True)
        assert ms.nrow(True) == row_count
        collected.append(parsed)
        assert ms.done()
    same_record(collected[0], collected[1])


def test_ms_selection_to_index_preserves_input_forms_without_attachment(native_dataset):
    casa = pinned_casa()
    options = {
        "field": np.array([0], dtype=np.int64), "spw": "0",
        "baseline": "!A1&A3", "scan": (7, 3, 7),
        "polarization": ["RR", "LL"], "observation": np.int32(0),
    }
    results = []
    for api in (casa, tools):
        ms = api.ms()
        indices = ms.msseltoindex(str(native_dataset), **options)
        assert ms.name() == "none"
        assert set(indices) == {
            "antenna1", "antenna2", "baselines", "channel", "dd",
            "field", "obsids", "poldd", "scan", "spw", "spwdd",
        }
        np.testing.assert_array_equal(indices["baselines"], np.array([[0, -2]], dtype=np.int64), strict=True)
        np.testing.assert_array_equal(indices["scan"], np.array([7, 3, 7], dtype=np.int64), strict=True)
        np.testing.assert_array_equal(indices["channel"], np.array([[0, 0, 3, 1]], dtype=np.int64), strict=True)
        assert ms.done() is False
        results.append(indices)
    same_record(results[0], results[1])


def test_ms_selection_indices_report_independent_polarization_and_spw_lists(tmp_path, measures):
    casa = pinned_casa()
    dataset = tmp_path / "two-spws.ms"
    simulator = tools.simulator()
    assert simulator.open(str(dataset))
    configure(simulator)
    assert simulator.setspwindow(
        spwname="H", freq="2GHz", deltafreq="2MHz", freqresolution="2MHz",
        nchannels=3, stokes="RR LL",
    )
    assert simulator.observe("F", "L", starttime="0s", stoptime="30s")
    assert simulator.observe("F", "H", starttime="30s", stoptime="60s")
    assert simulator.done()

    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(dataset))
        assert ms.msselect({"spw": "0:0~2", "polarization": "1:RR"}, onlyparse=True)
        parsed = ms.msselectedindices()
        np.testing.assert_array_equal(parsed["poldd"], np.array([1], dtype=np.int64), strict=True)
        np.testing.assert_array_equal(parsed["spwdd"], np.array([0], dtype=np.int64), strict=True)
        np.testing.assert_array_equal(parsed["dd"], np.empty(0, dtype=np.int64), strict=True)
        assert ms.nrow(True) > 0
        assert not ms.msselect({"spw": "0:0~2", "polarization": "1:RR"})
        assert ms.nrow(True) == 0
        same_record(ms.msselectedindices(), parsed)
        assert ms.done()
