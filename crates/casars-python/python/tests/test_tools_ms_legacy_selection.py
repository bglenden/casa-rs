"""Validate legacy row selection against the pinned CASA MSSelector."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import same_record, two_spws_dataset
from test_tools_msmetadata import measures, native_dataset


def test_legacy_selection_rebases_without_changing_modern_selected_rows(two_spws_dataset):
    for api in (pinned_casa(), tools):
        tool = api.ms()
        assert tool.nrowold() == tool.nrowold(True) == 0
        assert tool.selectinitold() is False
        assert tool.selectold() is False
        assert tool.selecttaqlold() is False
        assert tool.open(two_spws_dataset)
        assert tool.nrowold() == tool.nrowold(True) == 60
        assert tool.selectold({}) is True
        assert tool.nrowold(True) == 30
        assert tool.nrow(True) == 60

        assert tool.selectold({"rows": np.array([41, 2, 41, 8], dtype=np.int64)})
        assert tool.nrowold(True) == 4
        assert tool.selecttaqlold(b"ROWID() == 2 && DATA_DESC_ID == 1")
        assert tool.nrowold(True) == 1
        assert tool.selecttaqlold("   ") is False
        assert tool.nrowold(True) == 1
        assert tool.selecttaqlold("DATA_DESC_ID == 9") is False
        assert tool.nrowold(True) == 0
        assert tool.selectold({"rows": [2, 8]}) is False
        assert tool.selectinitold(9) is True
        assert tool.nrowold(True) == 0
        with pytest.raises(RuntimeError, match="positive integers"):
            tool.selectinitold(-1)
        assert tool.selectinitold(-1, True)
        assert tool.nrowold(True) == 60
        assert tool.selectinitold(1)
        assert tool.nrowold(True) == 30
        assert tool.selectold({"rows": [0, 0]})
        assert tool.nrowold(True) == 2
        assert tool.selectinitold(1)
        assert tool.nrowold(True) == 30

        assert tool.selecttaql("DATA_DESC_ID == 0")
        assert tool.nrow() == 60
        assert tool.nrowold() == tool.nrowold(True) == tool.nrow(True) == 30
        assert tool.selectold({"rows": [2, 8]})
        assert tool.nrowold(True) == 2
        assert tool.nrow(True) == 30
        assert tool.reset()
        assert tool.nrowold(True) == 60
        assert tool.msselect({"spw": "1"})
        assert tool.nrowold() == tool.nrowold(True) == 30
        assert tool.selectinitold(0)
        assert tool.nrowold(True) == 0
        assert tool.selectinitold(reset=True)
        assert tool.nrowold(True) == 30
        assert tool.done()


def test_legacy_record_selects_numeric_columns_and_uvw(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset))
    time = table.getcol("TIME")
    u, v, w = table.getcol("UVW")
    antenna1, antenna2 = table.getcol("ANTENNA1"), table.getcol("ANTENNA2")
    assert table.done()
    distance = np.hypot(u, v)
    pair = antenna1[0] * 1000 + antenna2[0]
    examples = (
        ({"ANTENNA1": int(antenna1[0])}, np.ones(len(antenna1), dtype=bool)),
        ({"antenna1": int(antenna1[0])}, antenna1 == antenna1[0]),
        ({"ifr_number": [int(pair)]}, antenna1 * 1000 + antenna2 == pair),
        ({"time": [float(time[2]), float(time[-2])]}, (time >= time[2]) & (time <= time[-2])),
        ({"times": np.unique(time)[::2]}, np.isin(time, np.unique(time)[::2])),
        ({"u": [float(u.min()), float(np.median(u))]}, u <= np.median(u)),
        ({"uvdist": [0.0, float(np.median(distance))]}, distance <= np.median(distance)),
    )
    for api in (casa, tools):
        tool = api.ms()
        assert tool.open(str(native_dataset))
        for record, expected in examples:
            assert tool.selectinitold(reset=True)
            assert tool.selectold(record) is bool(expected.any()), record
            assert tool.nrowold(True) == np.count_nonzero(expected), record
        assert tool.selectinitold(reset=True)
        assert tool.selecttaqlold(f"UVW[0] <= {float(np.median(w))}")
        assert tool.nrowold(True) == np.count_nonzero(w <= np.median(w))
        assert tool.nrow(True) == len(time)
        assert tool.done()


def test_legacy_argument_forms_preserve_selection_after_errors(native_dataset):
    for api in (pinned_casa(), tools):
        tool = api.ms()
        assert tool.open(str(native_dataset))
        assert tool.selectinitold(np.int64(0))
        assert tool.selectinitold(True)
        assert tool.selectold({"rows": [3, 6, 3]})
        assert tool.nrowold(True) == 3
        for value in (1, None, np.bool_(True)):
            with pytest.raises(TypeError):
                tool.nrowold(value)
        with pytest.raises(TypeError):
            tool.selectinitold(1.5)
        with pytest.raises(TypeError):
            tool.selectold(None)
        with pytest.raises(RuntimeError, match="positive integers"):
            tool.selectinitold(None)
        with pytest.raises(RuntimeError):
            tool.selectold({"rows": [-1]})
        for predicate in ("SCAN_NUMBER ==", "\t"):
            with pytest.raises(RuntimeError):
                tool.selecttaqlold(predicate)
            assert tool.nrowold(True) == 3
        assert tool.selecttaqlold("ROWID() == 2")
        assert tool.nrowold(True) == 1
        assert tool.nrow(True) == 30
        assert tool.done()


def test_legacy_metadata_range_reads_the_saved_legacy_view(native_dataset):
    results = []
    request = ["antenna1", "scan_number", "time"]
    for api in (pinned_casa(), tools):
        tool = api.ms()
        assert tool.rangeold() == {}
        assert tool.open(str(native_dataset))
        before = tool.rangeold(request, useflags=False, blocksize=1)
        assert tool.selectold({"rows": [14, 7, 14]})
        assert tool.nrowold(True) == 3
        after = tool.rangeold(request)
        same_record(tool.range(request), before)
        assert tool.selecttaqlold("1 == 0") is False
        assert tool.rangeold(request) == {}
        assert tool.done()
        results.append((before, after))
    for expected, actual in zip(*results):
        same_record(actual, expected)

    tool = tools.ms()
    assert tool.open(str(native_dataset))
    with pytest.raises(NotImplementedError, match=r"rangeold.*#1030"):
        tool.rangeold(["chan_freq"])
    assert tool.done()


def test_legacy_taql_clauses_preserve_projection_order(native_dataset):
    order = [3, 7, 8, 3]
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        for query, expected in (
            ("True LIMIT 2 OFFSET 1", [7, 8]),
            ("True ORDERBY ROWID() DESC", order[::-1]),
            ("True # trailing comment", order),
        ):
            assert reader.selectold({"rows": order})
            assert reader.selecttaqlold(query)
            np.testing.assert_array_equal(reader.rangeold("rows")["rows"], expected)
        for span in ("0:1", "0:2", "2:1", "1:4", "3:1:-1"):
            assert reader.selectold({"rows": order})
            with pytest.raises(RuntimeError):
                reader.selecttaqlold(f"any(UVW[{span}] == 0)")
            np.testing.assert_array_equal(reader.rangeold("rows")["rows"], order)
        assert reader.done()


def test_legacy_identity_rebase_and_empty_usable_record_arguments(native_dataset):
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        count = reader.nrow()
        for method, args in (
            (reader.selecttaql, ("",)),
            (reader.selecttaql, ("True",)),
            (reader.msselect, ({},)),
            (reader.msselect, ({"taql": "True"},)),
        ):
            assert reader.selectold({"rows": [3, 7, 8, 3]})
            assert reader.select({})
            assert reader.nrowold(True) == 4
            method(*args)
            assert reader.nrowold(True) == count
        for record in ({"ifr_number": [-4000]}, {"times": [float("nan")]}):
            assert reader.selectinitold(reset=True)
            assert reader.selectold(record) is False
            assert reader.nrowold(True) == 0
        assert reader.done()


def test_legacy_taql_giving_persists_an_ordered_reference(native_dataset, tmp_path):
    casa = pinned_casa()
    original = casa.table()
    assert original.open(str(native_dataset))
    uvw = original.getcol("UVW")
    assert original.done()
    order = [8, 3, 7, 8, 3, 6]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectold({"rows": order})
        grouping = "True GROUPBY ANTENNA1 ORDERBY ANTENNA1 DESC LIMIT 2"
        assert reader.selecttaqlold(grouping)
        grouped = reader.rangeold("rows")["rows"].copy()
        if api is casa:
            expected = grouped
            assert len(expected) == 2
        else:
            np.testing.assert_array_equal(grouped, expected, strict=True)
        assert reader.selectold({"rows": order})
        output = tmp_path / f"{api.__name__}-given.ms"
        assert reader.selecttaqlold(f'True LIMIT 3 OFFSET 1 GIVING "{output}"')
        np.testing.assert_array_equal(reader.rangeold("rows")["rows"], order[1:4])
        assert reader.done()
        for disk_api in (casa, tools):
            saved = disk_api.table()
            assert saved.open(str(output))
            np.testing.assert_array_equal(saved.getcol("UVW"), uvw[:, order[1:4]], strict=True)
            assert saved.done()

    reader = tools.ms()
    assert reader.open(str(native_dataset))
    assert reader.selectold({"rows": order})
    protected = tmp_path / "valuable"
    protected.mkdir()
    (protected / "sentinel").write_text("preserve")
    with pytest.raises(RuntimeError, match="not a true table directory"):
        reader.selecttaqlold(f'True GIVING "{protected}"')
    assert (protected / "sentinel").read_text() == "preserve"
    for kind in ("MEMORY", "PLAIN"):
        with pytest.raises(NotImplementedError, match=r"GIVING.*#1030"):
            reader.selecttaqlold(f'True GIVING "{tmp_path / "typed.ms"}" AS {kind}')
    np.testing.assert_array_equal(reader.rangeold("rows")["rows"], order)
    assert reader.done()
