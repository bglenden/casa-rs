"""CASA controls for persistent writes through the public visibility tool."""

import shutil

import numpy as np
import pytest

from casars import tools
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


@pytest.fixture(params=["casa", "native"])
def writer_api(request, baseline_fixture):
    return baseline_fixture[0] if request.param == "casa" else tools


def assert_main_columns(casa, path, expected):
    table = casa.table()
    assert table.open(str(path))
    for name, values in expected.items():
        np.testing.assert_array_equal(table.getcol(name), values, strict=True, err_msg=name)
    assert table.done()


def test_ms_visibility_scalar_taql_preserves_physical_order(
    baseline_fixture, native_dataset, writer_api,
):
    _, _, raw = baseline_fixture
    antenna = raw["antenna1"]
    selections = {
        "ANTENNA1 == 0": antenna == 0,
        "ANTENNA1 != 1": antenna != 1,
        "ANTENNA1 < 1": antenna < 1,
        "1 >= ANTENNA1": antenna <= 1,
        "0 < ANTENNA1": antenna > 0,
        "ANTENNA1 >= 1": antenna >= 1,
        "4000000000 > ANTENNA1": antenna < 4_000_000_000,
    }
    reader = writer_api.ms()
    assert reader.open(str(native_dataset))
    for predicate, selected in selections.items():
        assert reader.msselect({"taql": predicate}), predicate
        actual = reader.getdata("data")["data"]
        np.testing.assert_array_equal(actual, raw["data"][..., selected], strict=True, err_msg=predicate)
        assert reader.reset()
    assert reader.done()


@pytest.mark.parametrize("correlations", [False, True], ids=["all", "stored"])
def test_ms_putdata_changes_selected_rows_and_stored_correlations(
    baseline_fixture, native_dataset, writer_api, correlations,
):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    writer = writer_api.ms()
    assert writer.putdata({"data": raw["data"]}) is False
    assert writer.open(path)
    assert writer.putdata({"data": raw["data"]}) is False
    assert writer.done()
    assert writer.open(path, nomodify=False)
    assert writer.putdata({"unknown": np.ones(1)}) is False
    assert writer.msselect({"taql": "ANTENNA1 == 0"})
    if correlations:
        assert writer.selectpolarization("LL")
    selected = writer.getdata("data")["data"]
    rows = np.flatnonzero(raw["antenna1"] == 0)
    axis = slice(1, None) if correlations else slice(None)
    np.testing.assert_array_equal(selected, raw["data"][axis, :, rows], strict=True)
    assert writer.putdata({"DATA": selected * (2 - 0.5j)})
    assert writer.done()

    expected = raw["data"].copy()
    expected[axis, :, rows] = selected * (2 - 0.5j)
    assert_main_columns(casa, path, {"DATA": expected, "FLAG": raw["flag"]})


def test_ms_putdata_round_trips_baseline_axis_records(baseline_fixture, native_dataset, writer_api):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    writer = writer_api.ms()
    assert writer.open(path, nomodify=False)
    arranged = writer.getdata(["data", "flag", "flag_row", "sigma", "weight"], ifraxis=True)
    assert writer.putdata({
        "data": arranged["data"] * 2,
        "flag": ~arranged["flag"],
        "flag_row": ~arranged["flag_row"],
        "sigma": arranged["sigma"] * 2,
        "weight": arranged["weight"] * 0.5,
    })
    assert writer.done()
    assert_main_columns(casa, path, {
        "DATA": raw["data"] * 2,
        "FLAG": ~raw["flag"],
        "FLAG_ROW": ~raw["flag_row"],
        "SIGMA": raw["sigma"] * 2,
        "WEIGHT": raw["weight"] * 0.5,
    })


@pytest.mark.parametrize("mode", ["channels", "stokes", "average", "empty"])
def test_ms_putdata_refuses_mapped_or_empty_inputs(
    baseline_fixture, native_dataset, writer_api, mode,
):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    writer = writer_api.ms()
    assert writer.open(path, nomodify=False)
    if mode == "channels":
        assert writer.selectchannel(2, 0, 1, 2)
    elif mode == "stokes":
        assert writer.selectpolarization("I")
    elif mode == "average":
        assert writer.getdata("data", average=True)
    else:
        assert not writer.msselect({"taql": "ANTENNA1 == 50"})
    assert writer.putdata({"data": raw["data"] * 2}) is False
    assert writer.done()
    assert_main_columns(casa, path, {"DATA": raw["data"]})


def test_ms_putdata_writes_positioned_time_chunk(baseline_fixture, native_dataset, writer_api):
    casa, _, raw = baseline_fixture
    original = raw["data"]
    path = str(native_dataset)
    writer = writer_api.ms()
    assert writer.open(path, nomodify=False)
    assert writer.iterinit(["TIME"], 1.0, 0, False)
    assert writer.iterorigin()
    assert writer.iternext()
    current = writer.getdata(["data", "time"])
    rows = np.flatnonzero(np.isin(raw["time"], current["time"]))
    assert 0 < len(rows) < original.shape[-1]
    np.testing.assert_array_equal(current["data"], original[..., rows], strict=True)
    assert writer.putdata({"data": current["data"] * (-2 + 0.5j)})
    assert writer.done()
    expected = original.copy()
    expected[..., rows] = original[..., rows] * (-2 + 0.5j)
    assert_main_columns(casa, path, {"DATA": expected})


def test_ms_putdata_validates_all_shapes_before_writing(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    writer = tools.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    with pytest.raises(RuntimeError, match="FLAG shape"):
        writer.putdata({"data": raw["data"] * 2, "flag": raw["flag"][..., :-1]})
    assert writer.nrow() == raw["data"].shape[-1]
    assert writer.done()
    assert_main_columns(casa, native_dataset, {"DATA": raw["data"], "FLAG": raw["flag"]})


def test_ms_putdata_converts_numeric_records_without_treating_reals_as_flags(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, raw = baseline_fixture
    writer = writer_api.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.putdata() is False
    with pytest.raises(TypeError, match="_items is not a dictionary"):
        writer.putdata(None)
    with pytest.raises(RuntimeError):
        writer.putdata({"flag_row": np.ones(raw["flag_row"].shape, dtype=np.float32)})
    assert writer.done()
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.msselect({"taql": "ROWID() == 0"})
    assert writer.putdata({"flag_row": 0, "weight": np.full((2, 1), 3, dtype=np.int32)})
    assert writer.done()
    flags = raw["flag_row"].copy()
    flags[0] = False
    weights = raw["weight"].copy()
    weights[:, 0] = 3
    assert_main_columns(casa, native_dataset, {"FLAG_ROW": flags, "WEIGHT": weights})


def test_ms_putdata_updates_only_the_positioned_maxrows_block(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    original = raw["data"]
    writer = tools.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.iterinit(["TIME"], 0.0, 10, False)
    assert writer.iterorigin()
    assert writer.iternext()
    block = writer.getdata("data")["data"]
    assert block.shape[-1] == 10
    updates = block * (-2 + 0.5j)
    assert writer.putdata({"DATA": updates})
    np.testing.assert_array_equal(writer.getdata("data")["data"], updates, strict=True)
    assert writer.done()

    expected = original.copy()
    for index in range(block.shape[-1]):
        row = np.flatnonzero(original[0, 0, :] == block[0, 0, index])
        assert len(row) == 1
        expected[..., row[0]] = updates[..., index]
    assert_main_columns(casa, native_dataset, {"DATA": expected})


def test_ms_putdata_remains_usable_after_a_type_error(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    writer = tools.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    with pytest.raises(RuntimeError, match="incompatible array type"):
        writer.putdata({"flag_row": np.ones(raw["flag_row"].shape, dtype=np.float32)})
    assert writer.msselect({"taql": "ROWID() == 0"})
    assert writer.putdata({"flag_row": 0})
    assert writer.done()
    flags = raw["flag_row"].copy()
    flags[0] = False
    assert_main_columns(casa, native_dataset, {"FLAG_ROW": flags})


def test_ms_putdata_retains_reordered_reference_rows(baseline_fixture, native_dataset):
    casa, make_view, raw = baseline_fixture
    rows = [29, 2, 4, 13, 0, 2]
    writer = tools.ms()
    assert writer.open(make_view("scrambled-write.ms", rows), nomodify=False)
    block = writer.getdata("data")["data"]
    np.testing.assert_array_equal(block, raw["data"][..., rows], strict=True)
    updates = block * (-2 + 0.5j) + np.arange(len(rows))
    assert writer.putdata({"DATA": updates})
    assert writer.done()

    expected = raw["data"].copy()
    for index, physical in enumerate(rows):
        expected[..., physical] = updates[..., index]
    assert_main_columns(casa, native_dataset, {"DATA": expected})


def test_ms_putdata_keeps_values_in_the_correct_multi_ms_member(
    baseline_fixture, native_dataset, tmp_path,
):
    casa, _, raw = baseline_fixture
    paths = []
    for index, name in enumerate(("part-z.ms", "part-a.ms")):
        path = tmp_path / name
        shutil.copytree(native_dataset, path)
        paths.append(str(path))
        table = casa.table()
        assert table.open(str(path), nomodify=False)
        assert table.putcol("DATA", raw["data"] + 1000 * index)
        assert table.done()
    path = str(tmp_path / "ordered-write.mms")
    assembly = casa.ms()
    assert assembly.createmultims(path, paths, ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
    assert assembly.done()

    writer = tools.ms()
    assert writer.open(path, nomodify=False)
    initial = writer.getdata("data")["data"]
    updates = initial * (-2 + 0.5j)
    assert writer.putdata({"DATA": updates})
    assert writer.done()

    reader = casa.ms()
    assert reader.open(path)
    np.testing.assert_array_equal(reader.getdata("data")["data"], updates, strict=True)
    assert reader.done()
