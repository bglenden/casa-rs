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
