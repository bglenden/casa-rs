"""Check the physical rows and cells changed by the legacy MS writer."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import assert_main_columns, writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_legacy_writer_persists_duplicate_projected_rows(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    writer = writer_api.ms()
    order = [13, 4, 13, 5]

    assert writer.putdataold() is False
    assert writer.open(str(native_dataset))
    assert writer.putdataold({"DATA": original["data"]}) is False
    assert writer.close()
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.putdataold() is True
    assert writer.putdataold({"axis_info": 0}) is True
    assert writer.selectold({"rows": order})
    record = writer.getdataold(["data", "flag", "flag_row", "weight", "sigma"])
    replacement = {
        "DaTa": record["data"] * (2 - 0.75j) + np.arange(len(order), dtype=np.float32),
        "flag": ~record["flag"],
        "flag_row": ~record["flag_row"],
        "weight": record["weight"] + np.arange(len(order), dtype=np.float32),
        "sigma": record["sigma"] + np.arange(len(order), dtype=np.float32) / 4,
    }
    assert writer.putdataold(replacement) is True
    assert writer.done()

    expected = {}
    for name, values in replacement.items():
        full = original[name.lower()].copy()
        for index, row in enumerate(order):
            full[..., row] = values[..., index]
        expected[name.upper()] = full
    assert_main_columns(casa, native_dataset, expected)


@pytest.mark.parametrize("increment", [1, 2])
def test_legacy_grid_write_preserves_physical_rows_outside_its_record(
    baseline_fixture, native_dataset, increment,
):
    casa, _, original = baseline_fixture
    order = np.arange(0, original["data"].shape[-1], increment)
    writer = tools.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    # A gap belongs to the public axis but must never become a physical row.
    record = writer.getdataold(
        ["data", "flag", "flag_row", "weight", "sigma"], ifraxis=True, ifraxisgap=2,
        increment=increment,
    )
    replacements = {
        "data": np.full_like(record["data"], 9 + 2j),
        "flag": np.full_like(record["flag"], True),
        "flag_row": np.full_like(record["flag_row"], True),
        "weight": np.full_like(record["weight"], 17),
        "sigma": np.full_like(record["sigma"], 7),
    }
    assert writer.putdataold(replacements) is True
    assert writer.done()

    expected = {name.upper(): original[name].copy() for name in replacements}
    for name, new in replacements.items():
        expected[name.upper()][..., order] = new.flat[0]
    assert_main_columns(casa, native_dataset, expected)


def test_legacy_strided_channels_and_one_stored_hand_write_only_selected_cells(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    writer = writer_api.ms()
    rows = [0, 4, 13, 19]
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.selectpolarizationold("LL")
    assert writer.selectchannelold(2, 1, 1, 2)
    assert writer.selectold({"rows": rows})
    data = original["data"][1:2, 1::2, :][..., rows]
    np.testing.assert_array_equal(writer.getdataold("data")["data"], data, strict=True)
    assert writer.putdataold({"data": data * (0.5 - 2j)}) is True
    assert writer.done()

    expected = original["data"].copy()
    expected[1:2, 1::2, :][..., rows] = data * (0.5 - 2j)
    assert_main_columns(casa, native_dataset, {"DATA": expected, "FLAG": original["flag"]})


def test_legacy_writer_rejects_a_baseline_record_from_an_old_row_selection(
    baseline_fixture, native_dataset,
):
    casa, _, original = baseline_fixture
    writer = tools.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.selectold({"rows": [0, 1, 2]})
    record = writer.getdataold("data", ifraxis=True)
    assert record["data"].ndim == 4
    assert writer.selectold({"rows": [3, 4, 5]})
    with pytest.raises(RuntimeError, match="getdataold with ifraxis=True"):
        writer.putdataold({"data": 2 * record["data"]})
    assert writer.done()
    assert_main_columns(casa, native_dataset, {"DATA": original["data"]})


@pytest.mark.parametrize(("backend", "start"), [("casa", 0), ("native", 0), ("native", 1)])
def test_legacy_writer_expands_flag_bins_without_touching_gaps_or_averaging_data(
    baseline_fixture, native_dataset, backend, start,
):
    casa, _, original = baseline_fixture
    writer = (casa if backend == "casa" else tools).ms()
    rows = [0, 3, 6]
    assert writer.open(str(native_dataset), nomodify=False)
    # Overlap is processed in output-bin order. The final bin owns a shared sample.
    assert writer.selectchannelold(2, start, 2, 1)
    assert writer.selectold({"rows": rows})
    flagged = np.ones((2, 2, len(rows)), dtype=bool)
    flagged[:, 1, :] = False
    samples = np.full(flagged.shape, 31 + 2j, dtype=np.complex64)
    assert writer.putdataold({"data": samples, "flag": flagged}) is True
    assert writer.done()

    expected = original["flag"].copy()
    expected[:, start, rows] = True
    expected[:, start + 1:start + 3, rows] = False
    assert_main_columns(casa, native_dataset, {"DATA": original["data"], "FLAG": expected})


def test_legacy_writer_inverts_flag_dependencies_and_refuses_converted_visibilities(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    writer = writer_api.ms()
    rows = [0, 3, 6]
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.selectpolarizationold(["I", "V"])
    assert writer.selectold({"rows": rows})
    selected = np.zeros((2, 4, len(rows)), dtype=bool)
    selected[0, 0] = True
    selected[1, 2] = True
    assert writer.putdataold({"flag": selected}) is True
    assert writer.putdataold({"data": np.full(selected.shape, 71, dtype=np.complex64)}) is False
    assert writer.done()

    expected = original["flag"].copy()
    expected[:, :, rows] = np.any(selected, axis=0, keepdims=True)
    assert_main_columns(casa, native_dataset, {"DATA": original["data"], "FLAG": expected})


def test_legacy_writer_persists_float_data_with_selected_channels(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    descriptor = table.getcoldesc("DATA")
    descriptor.update(valueType="float", dataManagerType="StandardStMan", dataManagerGroup="FLOAT_DATA")
    assert table.addcols({"FLOAT_DATA": descriptor})
    shape = original["flag"].shape
    expected = np.arange(np.prod(shape), dtype=np.float64).reshape(shape, order="F")
    assert table.putcol("FLOAT_DATA", expected)
    assert table.done()

    writer = writer_api.ms()
    rows = [0, 2, 4]
    assert writer.open(str(native_dataset), nomodify=False)
    assert writer.selectchannelold(2, 1, 1, 2)
    assert writer.selectold({"rows": rows})
    replacement = np.full((2, 2, len(rows)), 377, dtype=np.float32)
    assert writer.putdataold({"FLOAT_DATA": replacement}) is True
    assert writer.done()
    for bin, channel in enumerate((1, 3)):
        expected[:, channel, rows] = replacement[:, bin]
    assert_main_columns(casa, native_dataset, {"FLOAT_DATA": expected, "FLAG": original["flag"]})
