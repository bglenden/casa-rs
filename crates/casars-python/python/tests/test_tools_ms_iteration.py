"""Pinned public MS iterator chunk, cursor and attachment-state controls."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("options,layout", [
    ({}, "all"),
    ({"interval": 15.0}, "interval"),
    ({"interval": -15}, "interval"),
    ({"maxrows": 7}, "row_blocks"),
    ({"maxrows": 1}, "integrations"),
    ({"maxrows": 7, "interval": 15.0}, "interval_blocks"),
    ({"columns": ["ANTENNA1"], "adddefaultsortcolumns": False}, "antenna"),
    ({"columns": ["ANTENNA1"], "adddefaultsortcolumns": True}, "time_antenna"),
    ({"columns": "ANTENNA1"}, "time_antenna"),
    ({"columns": ("ANTENNA1",)}, "all"),
])
def test_ms_iteration_keeps_the_active_view_and_returns_analytic_chunks(
    baseline_fixture, native_dataset, options, layout,
):
    casa, _, raw = baseline_fixture
    indices = np.arange(raw["time"].size)
    integrations = [
        indices[raw["time"] == time] for time in np.unique(raw["time"])
    ]
    antennas = np.unique(raw["antenna1"])
    interval = [np.concatenate(integrations[:2]), integrations[2]]
    layouts = {
        "all": [indices],
        "interval": interval,
        "row_blocks": [indices[start:start + 7] for start in range(0, len(indices), 7)],
        "integrations": integrations,
        "interval_blocks": [part[start:start + 7] for part in interval for start in range(0, len(part), 7)],
        "antenna": [indices[raw["antenna1"] == antenna] for antenna in antennas],
        "time_antenna": [part[raw["antenna1"][part] == antenna] for part in integrations for antenna in antennas],
    }

    for api in (casa, tools):
        reader = api.ms()
        assert reader.iterinit(**options) is False
        assert reader.iterorigin() is False
        assert reader.iternext() is False
        assert reader.iterend() is False
        assert reader.open(str(native_dataset))
        assert reader.iterinit(**options)
        assert reader.iterorigin()
        expected = layouts[layout]
        for index, rows in enumerate(expected):
            result = reader.getdata(["data", "time", "antenna1"])
            np.testing.assert_array_equal(result["data"], raw["data"][..., rows], strict=True)
            np.testing.assert_array_equal(result["time"], raw["time"][rows], strict=True)
            np.testing.assert_array_equal(result["antenna1"], raw["antenna1"][rows], strict=True)
            assert reader.nrow(True) == len(indices)
            assert reader.iternext() is (index + 1 < len(expected))
        np.testing.assert_array_equal(reader.getdata("time")["time"],
                                      raw["time"][expected[-1]], strict=True)
        with pytest.raises(RuntimeError, match="advance .*past end"):
            reader.iternext()
        assert reader.iterorigin()
        np.testing.assert_array_equal(reader.getdata("time")["time"],
                                      raw["time"][expected[0]], strict=True)
        assert reader.iterend()
        assert reader.iternext() is False
        assert reader.nrow(True) == len(indices)
        np.testing.assert_array_equal(reader.getdata("data")["data"], raw["data"], strict=True)
        assert reader.done()


def test_ms_iteration_composes_with_row_channels_and_stored_correlations(
    baseline_fixture, native_dataset,
):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    selected = np.flatnonzero(raw["antenna1"] == 0)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        assert reader.msselect({"taql": "ANTENNA1 == 0"})
        assert reader.selectchannel(2, 0, 1, 2)
        assert reader.selectpolarization("LL")
        assert reader.iterinit(maxrows=5)
        assert reader.iterorigin()
        for first in range(0, len(selected), 5):
            rows = selected[first:first + 5]
            result = reader.getdata(["data", "time", "flag", "axis_info"])
            expected = raw["data"][1:2, ::2, rows]
            np.testing.assert_array_equal(result["data"], expected, strict=True)
            np.testing.assert_array_equal(result["flag"], raw["flag"][1:2, ::2, rows], strict=True)
            np.testing.assert_array_equal(result["axis_info"]["corr_axis"], ["LL"])
            np.testing.assert_array_equal(result["time"], raw["time"][rows], strict=True)
            assert reader.nrow(True) == len(selected)
            assert reader.iternext() is (first + 5 < len(selected))
        assert reader.iterend()
        if api is casa:
            with pytest.raises(RuntimeError, match="advance subchunk past end of chunk"):
                reader.getdata("data")
        else:
            assert reader.getdata("data")["data"].shape == (1, 2, len(selected))
        assert reader.done()


def test_ms_iterator_cursor_errors_and_increment_match_active_chunks(
    baseline_fixture, native_dataset,
):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.iterinit(maxrows=7)
        with pytest.raises(RuntimeError, match="not positioned to a subchunk"):
            reader.getdata("time")
        assert reader.iterorigin()
        for increment in (1, 2, 3):
            record = reader.getdata("time", increment=increment)
            np.testing.assert_array_equal(record["time"], raw["time"][:7], strict=True)
        while reader.iternext():
            pass
        np.testing.assert_array_equal(reader.getdata("time")["time"], raw["time"][-2:], strict=True)
        with pytest.raises(RuntimeError, match="advance subchunk past end of chunk"):
            reader.iternext()
        assert reader.iterend()
        np.testing.assert_array_equal(reader.getdata("time")["time"], raw["time"], strict=True)
        assert reader.done()


def test_ms_iterator_keeps_its_captured_rows_and_native_axes(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.iterinit(maxrows=7)
        assert reader.iterorigin()
        assert reader.msselect({"taql": "ANTENNA1 == 0"})
        assert reader.selectchannel(2, 0, 1, 2)
        assert reader.selectpolarization("LL")
        assert reader.nrow(True) == 12
        np.testing.assert_array_equal(reader.getdata("data")["data"], raw["data"][..., :7],
                                      strict=True)
        assert reader.reset()
        np.testing.assert_array_equal(reader.getdata("data")["data"], raw["data"], strict=True)
        assert reader.done()


@pytest.mark.parametrize("when", ["before", "after"])
def test_ms_iterator_reads_derived_polarization(baseline_fixture, native_dataset, when):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        if when == "before":
            assert reader.selectpolarization("I")
        assert reader.iterinit(maxrows=7)
        assert reader.iterorigin()
        if when == "after":
            assert reader.selectpolarization("I")
        record = reader.getdata(["data", "flag", "axis_info"])
        np.testing.assert_allclose(record["data"], raw["data"][..., :7].mean(axis=0, keepdims=True),
                                   rtol=1e-6, strict=True)
        np.testing.assert_array_equal(record["flag"], raw["flag"][..., :7], strict=True)
        np.testing.assert_array_equal(record["axis_info"]["corr_axis"], ["I"])
        assert reader.done()


def test_ms_iterator_reinitialization_and_invalid_parameters(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.iterinit()
        with pytest.raises(RuntimeError, match="Call to origin without first initializing chunk"):
            reader.getdata("time")
        assert reader.iterorigin()
        for arguments, error in [
            ({"columns": [1]}, TypeError),
            ({"interval": None}, SystemError),
            ({"interval": "15"}, SystemError),
            ({"maxrows": None}, SystemError),
            ({"maxrows": 3.0}, TypeError),
            ({"adddefaultsortcolumns": 1}, TypeError),
        ]:
            assert reader.iterinit(maxrows=7)
            assert reader.iterorigin()
            with pytest.raises(error):
                reader.iterinit(**arguments)
            if api is casa and error is SystemError:
                with pytest.raises(RuntimeError, match="not positioned to a subchunk"):
                    reader.getdata("time")
                assert reader.iterorigin()
                expected = raw["time"][:10]
            else:
                expected = raw["time"][:7]
            np.testing.assert_array_equal(reader.getdata("time")["time"], expected, strict=True)

        assert reader.iterinit("antenna1") is False
        assert reader.iterorigin() is False
        with pytest.raises(RuntimeError, match="not a scalar"):
            reader.iterinit(["DATA"])
        with pytest.raises(RuntimeError, match="PULSAR_BIN is unknown"):
            reader.iterinit(["PULSAR_BIN"])
        assert reader.iterinit(maxrows=7)
        assert reader.iterorigin()
        assert reader.iterinit()
        assert reader.iterorigin()
        expected = raw["time"][:10] if api is casa else raw["time"]
        np.testing.assert_array_equal(reader.getdata("time")["time"], expected, strict=True)
        assert reader.close()
        assert reader.open(str(native_dataset))
        assert not reader.iterorigin()
        assert not reader.msselect({"taql": "ANTENNA1 == 50"})
        with pytest.raises(RuntimeError, match="zero selected rows"):
            reader.iterinit()
        assert reader.done()


def test_ms_iterator_retains_selected_baseline_axis(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    baselines = np.array([0, 2, -1, -1, 1001, 1003, -1, -1, 2003])
    expected = np.zeros((2, 4, len(baselines)), dtype=raw["data"].dtype)
    expected[..., :2] = raw["data"][..., [0, 2]]
    flags = np.zeros(expected.shape, dtype=bool)
    flags[..., :2] = raw["flag"][..., [0, 2]]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.iterinit(maxrows=3)
        assert reader.iterorigin()
        items = ["data", "time", "ifr_number", "flag"]
        if api is tools:
            # Pinned CASA indexes the -1 gap as an antenna in axis_info.
            items.append("axis_info")
        record = reader.getdata(items, ifraxis=True, ifraxisgap=2, increment=2)
        np.testing.assert_array_equal(record["ifr_number"], baselines, strict=True)
        if api is tools:
            np.testing.assert_array_equal(record["axis_info"]["ifr_axis"]["ifr_number"],
                                          baselines, strict=True)
        np.testing.assert_array_equal(record["data"], expected, strict=True)
        np.testing.assert_array_equal(record["flag"], flags, strict=True)
        np.testing.assert_array_equal(record["time"], raw["time"][:1], strict=True)
        assert reader.done()


def test_ms_iterator_baseline_blocks_preserve_each_integration(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.iterinit(maxrows=12)
        assert reader.iterorigin()
        record = reader.getdata(["data", "time", "flag"], ifraxis=True)
        if api is casa:
            overwritten = np.r_[10:12, 2:10]
            expected = raw["data"][..., overwritten]
            flags = raw["flag"][..., overwritten]
            times = raw["time"][:1]
        else:
            expected = np.zeros((2, 4, 10, 2), dtype=raw["data"].dtype)
            flags = np.zeros(expected.shape, dtype=bool)
            expected[..., 0] = raw["data"][..., :10]
            expected[:, :, :2, 1] = raw["data"][..., 10:12]
            flags[..., 0] = raw["flag"][..., :10]
            flags[:, :, :2, 1] = raw["flag"][..., 10:12]
            times = raw["time"][[0, 10]]
        np.testing.assert_array_equal(record["data"], expected, strict=True)
        np.testing.assert_array_equal(record["flag"], flags, strict=True)
        np.testing.assert_array_equal(record["time"], times, strict=True)
        assert reader.done()


def test_ms_iterator_baseline_average_preserves_antenna_pairs(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.iterinit(maxrows=3)
        assert reader.iterorigin()
        record = reader.getdata(["data", "flag"], ifraxis=True, average=True)
        if api is casa:
            assert record["data"].shape == (2, 4)
            assert record["flag"].shape == (2, 4, 1)
        else:
            expected = np.zeros((2, 4, 10), dtype=raw["data"].dtype)
            flags = np.ones(expected.shape, dtype=bool)
            expected[..., :3] = np.where(raw["flag"][..., :3], 0, raw["data"][..., :3])
            flags[..., :3] = raw["flag"][..., :3]
            np.testing.assert_array_equal(record["data"], expected, strict=True)
            np.testing.assert_array_equal(record["flag"], flags, strict=True)
        assert reader.done()


def test_ms_iterator_uses_the_frequency_axis_of_each_chunk(two_spws_dataset):
    casa = pinned_casa()
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(two_spws_dataset)
        assert reader.iterinit()
        assert reader.iterorigin()
        assert reader.nrow(True) == 60
        for description, channels, frequencies in [
            (0, 4, 1.4e9 + np.arange(4) * 1e6),
            (1, 3, 2.0e9 + np.arange(3) * 2e6),
        ]:
            record = reader.getdata(["data", "data_desc_id", "axis_info"])
            assert record["data"].shape == (2, channels, 30)
            np.testing.assert_array_equal(record["data_desc_id"], np.full(30, description),
                                          strict=True)
            if api is casa and description == 1:
                frequencies = 1.4e9 + np.arange(4) * 1e6
            np.testing.assert_allclose(record["axis_info"]["freq_axis"]["chan_freq"],
                                       frequencies[:, None], rtol=1e-15, atol=0.0, strict=True)
            assert reader.nrow(True) == 30
            assert reader.iternext() is (description == 0)
        assert reader.iterend()
        assert reader.getdata("data")["data"].shape == (2, 4, 30)
        assert reader.done()
