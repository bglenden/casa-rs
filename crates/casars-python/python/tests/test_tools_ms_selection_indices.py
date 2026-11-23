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


@pytest.fixture
def two_spws_dataset(tmp_path, measures):
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
    return str(dataset)


def test_ms_selection_indices_report_independent_polarization_and_spw_lists(two_spws_dataset):
    casa = pinned_casa()
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(two_spws_dataset)
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


def test_ms_selection_chain_keeps_omitted_clause_indices(two_spws_dataset):
    casa = pinned_casa()
    results = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(two_spws_dataset)
        assert ms.msselect({"polarization": "RR", "field": "0", "baseline": "A1&A3"})
        initial = ms.msselectedindices()
        assert ms.msselect({"spw": "0:0~2", "scan": "1", "observation": "0"}, onlyparse=True)
        assert not ms.msselect({})
        assert ms.msselect({"taql": "FIELD_ID == 0"})
        indices = ms.msselectedindices()
        for key in ("antenna1", "antenna2", "baselines", "field", "poldd"):
            np.testing.assert_array_equal(indices[key], initial[key], strict=True)
        np.testing.assert_array_equal(indices["poldd"], np.array([0, 1], dtype=np.int64))
        np.testing.assert_array_equal(indices["spwdd"], np.array([0], dtype=np.int64))
        np.testing.assert_array_equal(indices["dd"], np.array([0], dtype=np.int64))
        assert all(indices[key].size == 0 for key in ("scan", "stateid", "observationid"))

        count = ms.nrow(True)
        assert not ms.msselect({"field": "no_such_field"})
        with pytest.raises(RuntimeError):
            ms.msselectedindices()
        assert ms.msselect({"taql": "FIELD_ID == 0"}, onlyparse=True)
        recovered = ms.msselectedindices()
        assert recovered["field"].size == 0
        np.testing.assert_array_equal(recovered["poldd"], indices["poldd"], strict=True)
        assert ms.nrow(True) == count
        assert ms.reset()
        assert ms.msselectedindices()["dd"].size == 0
        assert ms.done()
        results.append((indices, recovered))
    for native, reference in zip(results[1], results[0], strict=True):
        same_record(native, reference)


def test_ms_selection_time_defaults_and_selector_grammars(two_spws_dataset):
    casa = pinned_casa()
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(two_spws_dataset)
        assert ms.msselect({"polarization": "1:RR", "time": "*"})
        assert ms.nrow(True) == 10
        assert ms.reset()
        for selector in ({"array": "<1"}, {"array": ">=0&<=1"}, {"uvdist": "1000m:20%"}):
            assert ms.msselect(selector, onlyparse=True)
        assert not ms.msselect({"array": ">0"})
        assert ms.nrow(True) == 0
        assert ms.reset()

        for token, expected in ((":RR", [0, 1]), ("RR:", [0, 1]), ("0::RR", [0])):
            indices = ms.msseltoindex(two_spws_dataset, polarization=token)
            np.testing.assert_array_equal(indices["poldd"], np.array(expected, dtype=np.int64))
        with pytest.raises(RuntimeError):
            ms.msseltoindex(two_spws_dataset, polarization='"RR"')
        assert ms.msselect({"polarization": "0::RR"})
        assert ms.nrow(True) == 30
        assert ms.reset()

        # The first logical row satisfies the left branch. Evaluating the
        # rest of MAIN in order to choose time defaults instead raises.
        time = "1995/04/13/09:18:58.5+0:0:1.7"
        taql = ("rownumber() <= 1 || "
                "datetime(iif(abs(DATA[1,1]) >= 0, 'not-a-date', '2020/01/01')) "
                "== datetime('2020/01/01')")
        same_record(ms.msseltoindex(two_spws_dataset, time=time, taql=taql),
                    ms.msseltoindex(two_spws_dataset, time=time))
        assert ms.msselect({"time": time, "taql": taql}, onlyparse=True)
        assert ms.nrow(True) == 60
        assert ms.done()


def test_ms_selection_string_and_record_argument_forms(native_dataset, monkeypatch):
    casa = pinned_casa()
    dataset = str(native_dataset)
    for api in (casa, tools):
        ms = api.ms()
        expected = ms.msseltoindex(dataset, field="0", taql="FIELD_ID==0")
        same_record(
            ms.msseltoindex(dataset.encode(), field=b"0", taql=b"FIELD_ID==0"),
            expected,
        )
        same_record(ms.msseltoindex(np.str_(dataset), field=np.int64(0), taql="FIELD_ID==0"), expected)
        for invalid in (None, native_dataset, 1):
            with pytest.raises(TypeError, match="argument _vis must be a string"):
                ms.msseltoindex(invalid)
        with pytest.raises(TypeError, match="argument _taql must be a string"):
            ms.msseltoindex(dataset, taql=None)
        with monkeypatch.context() as patch:
            patch.chdir(native_dataset)
            same_record(ms.msseltoindex(field="0", taql="FIELD_ID==0"), expected)

        assert ms.open(dataset)
        assert ms.msselect({b"field": b"0"}, onlyparse=True)
        np.testing.assert_array_equal(ms.msselectedindices()["field"], np.array([0], dtype=np.int64))
        with pytest.raises(TypeError, match="_items is not a dictionary"):
            ms.msselect(None)
        assert not ms.msselect({"field": 0})
        np.testing.assert_array_equal(ms.msselectedindices()["field"], np.array([0], dtype=np.int64))
        for selection in (
            {"antenna": "A1&A3", "baseline": "A2&A4"},
            {"baseline": "A2&A4", "antenna": "A1&A3"},
        ):
            assert ms.msselect(selection, onlyparse=True)
            np.testing.assert_array_equal(
                ms.msselectedindices()["baselines"], np.array([[1, 3]], dtype=np.int64),
                strict=True,
            )
        assert ms.msselect({"baseline": "A1&"}, onlyparse=True)
        np.testing.assert_array_equal(
            ms.msselectedindices()["baselines"], np.empty((0, 2), dtype=np.int64),
            strict=True,
        )
        assert ms.done()


def test_ms_index_queries_parse_taql_without_reading_visibility_cells(native_dataset):
    casa = pinned_casa()
    dataset = str(native_dataset)
    expression = "abs(DATA[999,1]) > 0"
    for api in (casa, tools):
        ms = api.ms()
        empty = ms.msseltoindex(dataset)
        same_record(ms.msseltoindex(dataset, taql=expression), empty)
        for bare in ("TIME", "ANTENNA1", "FLAG", "DATA[abs(ANTENNA1),1]", "DATA[square(1),1]"):
            same_record(ms.msseltoindex(dataset, taql=bare), empty)
        invalid_expressions = (
            "noSuchColumn > 0", "noSuchFunction(TIME) > 0", "TIME + 'abc' > 0",
            "TIME[1]>0", "abs('abc') > 0", "!TIME", "FIELD_ID in ['a']",
            "FIELD_ID==0 && 1", "DATA[TIME,1] > 0", "TIME IN []",
        )
        for invalid in invalid_expressions:
            with pytest.raises(RuntimeError):
                ms.msseltoindex(dataset, taql=invalid)

        assert ms.open(dataset)
        count = ms.nrow(True)
        for invalid in invalid_expressions:
            assert not ms.msselect({"taql": invalid}, onlyparse=True)
            assert not ms.msselect({"taql": invalid})
            assert ms.nrow(True) == count
            with pytest.raises(RuntimeError):
                ms.msselectedindices()
        assert ms.msselect({"taql": expression}, onlyparse=True)
        assert ms.nrow(True) == count
        for bare in ("TIME", "ANTENNA1", "FLAG"):
            assert ms.msselect({"taql": bare}, onlyparse=True)
            assert not ms.msselect({"taql": bare})
            assert ms.nrow(True) == count
            assert ms.msselectedindices()["field"].size == 0
        with pytest.raises(RuntimeError):
            ms.msseltoindex(dataset, field="0", taql="TIME")
        assert not ms.msselect({"field": "0", "taql": "TIME"}, onlyparse=True)
        assert ms.msselect({"field": "0", "taql": "FLAG"}, onlyparse=True)
        assert not ms.msselect({"field": "0", "taql": "FLAG"})
        assert ms.nrow(True) == count
        if api is tools:
            assert not ms.msselect({"taql": expression})
            assert ms.nrow(True) == count
            assert ms.msselectedindices()["field"].size == 0
        assert ms.done()
