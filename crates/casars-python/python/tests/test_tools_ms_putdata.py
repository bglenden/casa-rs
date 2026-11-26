"""CASA controls for persistent writes through the public visibility tool."""

import shutil
from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_tools_ms_visibility import same_visibility
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


def make_multi_ms(casa, path, members):
    assembly = casa.ms()
    assert assembly.createmultims(str(path), list(map(str, members)), ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
    assert assembly.done()
    return str(path)


def test_ms_visibility_and_writes_in_a_saved_multi_ms_view(
    baseline_fixture, native_dataset, tmp_path,
):
    casa, _, raw = baseline_fixture
    first = native_dataset
    second = tmp_path / "second-member.ms"
    shutil.copytree(first, second)
    rows_per_member = raw["data"].shape[-1]
    other = raw["data"].astype(np.complex64) + (17 - 9j)
    table = casa.table()
    assert table.open(str(second), nomodify=False)
    assert table.putcol("DATA", other)
    assert table.done()

    multi = make_multi_ms(casa, tmp_path / "parent.mms", (first, second))
    parent = casa.ms()
    assert parent.open(multi)
    members = parent.getreferencedtables()
    assert len(members) == 2 and parent.done()
    view = str(tmp_path / "saved.ms")
    rows = np.array([rows_per_member + 13, 2, rows_per_member, 1, rows_per_member + 13])
    assert table.open(multi)
    reference = table.selectrows(rows, view)
    assert reference.done() and table.done()

    wanted = np.concatenate((raw["data"], other), axis=-1)[..., rows]
    fields = ["data", "time", "antenna1", "axis_info"]
    results = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(view)
        assert reader.ismultims()
        assert reader.getreferencedtables() == [multi]
        assert reader.nrow() == reader.nrow(True) == len(rows)
        assert reader.selectchannel(nchan=2, start=1, width=1, inc=2)
        assert reader.selectpolarization(["LL"])
        result = reader.getdata(fields)
        np.testing.assert_array_equal(np.sort(result["data"], axis=-1),
                                      np.sort(wanted[1:2, 1::2, :], axis=-1), strict=True)
        results.append(result)
        assert reader.reset()
        np.testing.assert_array_equal(reader.getdata("data")["data"], wanted, strict=True)
        assert reader.done()
    same_visibility(*results[::-1])

    writer = tools.ms()
    assert writer.open(view, nomodify=False)
    updates = wanted + np.arange(len(rows)) * (3.5 - 2j)
    assert writer.putdata({"DATA": updates})
    assert writer.done()
    expected = [raw["data"].copy(), other.astype(np.complex128)]
    for position, row in enumerate(rows):
        member, physical = divmod(row, rows_per_member)
        expected[member][..., physical] = updates[..., position]
    for index, path in enumerate(members):
        assert_main_columns(casa, path, {"DATA": expected[index], "FLAG": raw["flag"]})
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(view)
        data = reader.getdata("data")["data"]
        persisted = np.concatenate(expected, axis=-1)[..., rows].astype(np.complex128)
        np.testing.assert_array_equal(data, persisted, strict=True)
        assert reader.done()


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


def test_ms_putdata_optional_visibility_columns(baseline_fixture, native_dataset, writer_api):
    casa, _, raw = baseline_fixture
    writer = writer_api.ms()
    path = str(native_dataset)
    assert writer.open(path, nomodify=False)
    assert writer.putdata({"corrected_data": raw["data"], "model_data": raw["data"]}) is False
    assert writer.done()

    table = casa.table()
    assert table.open(path, nomodify=False)
    description = table.getcoldesc("DATA")
    assert table.addcols({"CORRECTED_DATA": description, "MODEL_DATA": description})
    expected = {"CORRECTED_DATA": raw["data"] + 55j, "MODEL_DATA": raw["data"] - 31}
    for name, values in expected.items():
        assert table.putcol(name, values)
    assert table.done()

    assert writer.open(path, nomodify=False)
    assert writer.msselect({"taql": "ANTENNA1 == 0"})
    assert writer.selectpolarization("LL")
    selected = writer.getdata(["corrected_data", "model_data"])
    changed = {name: values * (-0.5 + 2j) for name, values in selected.items()}
    assert writer.putdata(changed)
    assert writer.done()
    rows = np.flatnonzero(raw["antenna1"] == 0)
    for name, values in changed.items():
        expected[name.upper()][1:, :, rows] = values
    assert_main_columns(casa, path, expected | {"DATA": raw["data"]})


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


@pytest.mark.parametrize("baseline,extra", [(False, (4,)), (True, (2, 0)), (True, (0, 2))])
def test_ms_putdata_ignores_surplus_record_axes(
    baseline_fixture, native_dataset, writer_api, baseline, extra,
):
    casa, _, raw = baseline_fixture
    writer = writer_api.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    selected = writer.getdata("data", ifraxis=baseline)["data"]
    shape = [(0, 0), (0, 0), *((0, size) for size in extra)]
    assert writer.putdata({"data": np.pad(selected * 2, shape, constant_values=500)})
    assert writer.done()
    assert_main_columns(casa, native_dataset, {"DATA": raw["data"] * 2})


def test_ms_putdata_writes_all_rows_at_the_same_baseline_time(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, raw = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    for column in ("ANTENNA1", "ANTENNA2", "TIME", "TIME_CENTROID", "UVW"):
        assert table.putcell(column, 1, table.getcell(column, 0))
    assert table.done()

    writer = writer_api.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    arranged = writer.getdata("data", ifraxis=True)["data"]
    assert writer.putdata({"data": arranged * 2})
    assert writer.done()
    expected = raw["data"] * 2
    expected[..., 0] = expected[..., 1]
    assert_main_columns(casa, native_dataset, {"DATA": expected})


def test_ms_putdata_rejects_a_downsampled_grid_without_writing(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    writer = tools.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    arranged = writer.getdata(["data", "ifr_number"], ifraxis=True, increment=2)
    full_ifr = raw["antenna1"] * 1000 + raw["antenna2"]
    assert np.count_nonzero(~np.isin(full_ifr, arranged["ifr_number"])) == 15
    with pytest.raises(RuntimeError, match="downsampled baseline-axis record"):
        writer.putdata({"data": arranged["data"] * 2})
    assert_main_columns(casa, native_dataset, {"DATA": raw["data"]})
    complete = writer.getdata("data", ifraxis=True)["data"]
    assert writer.putdata({"data": complete * 2})
    assert writer.done()
    assert_main_columns(casa, native_dataset, {"DATA": raw["data"] * 2})


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


def test_ms_putdata_retains_single_integration_grid_order(baseline_fixture, native_dataset):
    casa, make_view, raw = baseline_fixture
    rows = [2, 0, 3]
    writer = tools.ms()
    assert writer.open(make_view("singleton-write.ms", rows), nomodify=False)
    grid = writer.getdata(["data", "ifr_number"], ifraxis=True)
    assert grid["data"].shape == raw["data"].shape[:-1] + (3,)
    assert writer.getdata("time")
    assert writer.putdata({"data": grid["data"] * 2})
    expected = raw["data"].copy()
    expected[..., rows] *= 2
    assert_main_columns(casa, native_dataset, {"DATA": expected})

    row_order = writer.getdata("data")["data"]
    np.testing.assert_array_equal(row_order, expected[..., rows], strict=True)
    updates = row_order + np.arange(len(rows))
    assert writer.putdata({"data": updates})
    assert writer.done()
    expected[..., rows] = updates
    assert_main_columns(casa, native_dataset, {"DATA": expected})


def test_ms_putdata_refuses_a_rebound_iterator_view(baseline_fixture, native_dataset):
    casa, make_view, raw = baseline_fixture
    path = Path(make_view("positioned-write.ms", [0, 1, 2, 3, 4, 5]))
    replacement = Path(make_view("replacement-write.ms", [10, 11, 12, 13, 14, 15]))
    writer = tools.ms()
    assert writer.open(str(path), nomodify=False)
    assert writer.iterinit(maxrows=3)
    assert writer.iterorigin()
    original = writer.getdata("data")["data"]
    pending = path / "replacement.dat"
    shutil.copyfile(replacement / "table.dat", pending)
    pending.replace(path / "table.dat")
    np.testing.assert_array_equal(writer.getdata("data")["data"], original, strict=True)
    with pytest.raises(RuntimeError, match="row layout changed"):
        writer.putdata({"data": original * 2})
    np.testing.assert_array_equal(writer.getdata("data")["data"], original, strict=True)
    assert writer.done()
    assert_main_columns(casa, native_dataset, {"DATA": raw["data"]})


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
    path = make_multi_ms(casa, tmp_path / "ordered-write.mms", paths)

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


@pytest.mark.parametrize("layout", ["reference", "multi-ms"])
def test_ms_putdata_keeps_a_positioned_saved_cursor(
    baseline_fixture, native_dataset, tmp_path, layout,
):
    casa, make_view, raw = baseline_fixture
    if layout == "reference":
        path = make_view("cursor-write.ms", [0, 1, 2, 3, 4, 5])
    else:
        other = tmp_path / "second.ms"
        shutil.copytree(native_dataset, other)
        path = make_multi_ms(casa, tmp_path / "cursor-write.mms", (native_dataset, other))
    writer = tools.ms()
    assert writer.open(path, nomodify=False)
    assert writer.iterinit(maxrows=3)
    assert writer.iterorigin()
    first = writer.getdata("data")["data"]
    assert first.shape[-1] == 3
    for factor in (2, -0.5j):
        expected = first * factor
        assert writer.putdata({"data": expected})
        np.testing.assert_array_equal(writer.getdata("data")["data"], expected, strict=True)
    assert writer.iternext()
    np.testing.assert_array_equal(writer.getdata("data")["data"], raw["data"][..., 3:6], strict=True)
    assert writer.done()


def test_ms_putdata_preflights_projected_cells_in_all_members(
    baseline_fixture, native_dataset, tmp_path,
):
    casa, _, raw = baseline_fixture
    members = (tmp_path / "part-a.ms", tmp_path / "part-z.ms")
    for index, member in enumerate(members):
        shutil.copytree(native_dataset, member)
        table = casa.table()
        assert table.open(str(member), nomodify=False)
        assert table.addcols({"MODEL_DATA": table.getcoldesc("DATA")})
        if index == 0:
            assert table.putcol("MODEL_DATA", raw["data"])
        else:
            assert not table.iscelldefined("MODEL_DATA", 0)
        assert table.done()
    path = make_multi_ms(casa, tmp_path / "preflight.mms", members)
    writer = tools.ms()
    assert writer.open(path, nomodify=False)
    assert writer.selectpolarization("LL")
    selected = writer.getdata("data")["data"]
    with pytest.raises(RuntimeError, match="undefined MODEL_DATA"):
        writer.putdata({"data": selected * 2, "model_data": selected * 3})
    np.testing.assert_array_equal(writer.getdata("data")["data"], selected, strict=True)
    assert writer.done()
    assert_main_columns(casa, path, {"DATA": np.concatenate([raw["data"]] * 2, axis=-1)})
