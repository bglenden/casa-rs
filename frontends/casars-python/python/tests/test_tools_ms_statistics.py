"""Visibility statistics across CASA's legacy and current MS tool reducers."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def assert_statistics(actual, expected):
    assert actual.keys() == expected.keys()
    for key, expected_value in expected.items():
        actual_value = actual[key]
        assert type(actual_value) is type(expected_value), key
        if isinstance(expected_value, dict):
            assert_statistics(actual_value, expected_value)
        elif isinstance(expected_value, float):
            assert actual_value == pytest.approx(expected_value, rel=5e-7, abs=1e-8, nan_ok=True), key
        else:
            assert actual_value == expected_value, key


@pytest.fixture
def statistics_ms(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    old = table.getcol("DATA")
    values = np.arange(old.size, dtype=np.float32).reshape(old.shape, order="F")
    assert table.putcol("DATA", (values + 1j * (-values / 2)).astype(np.complex64))
    flags = np.zeros(old.shape, dtype=bool)
    flags[0, 0, 0] = True
    flags[:, :, 1] = True
    row_flags = np.zeros(old.shape[2], dtype=bool)
    row_flags[2] = True
    assert table.putcol("FLAG", flags)
    assert table.putcol("FLAG_ROW", row_flags)
    weights = table.getcol("WEIGHT")
    sigmas = table.getcol("SIGMA")
    weights[:, 0] = [100, 200]
    sigmas[:, 0] = [2, 3]
    assert table.putcol("WEIGHT", weights)
    assert table.putcol("SIGMA", sigmas)
    assert table.done()
    return native_dataset


@pytest.mark.parametrize("legacy", [False, True])
def test_ms_statistics_matches_pinned_masks_weights_and_axes(statistics_ms, legacy):
    casa = pinned_casa()
    method = "statisticsold" if legacy else "statistics"
    cases = [
        {"column": b"DATA", "complex_value": b"real", "useflags": False},
        {"column": "DATA", "complex_value": "imaginary", "useflags": True},
        {"column": "DATA", "complex_value": "amp", "correlation": "RR", "spw": "0:1~2"},
        {"column": "FLAG", "useflags": False},
        {"column": "FLAG_ROW", "useflags": False},
        {"column": "ANTENNA1", "useflags": True},
    ]
    if legacy:
        cases.extend([{"column": "WEIGHT"}, {"column": "SIGMA"}, {"column": "UVW"}])
    else:
        cases.extend([
            {"column": "DATA", "complex_value": "phase", "useflags": False, "doquantiles": False},
            {"column": "TIME", "reportingaxes": "integration,scan"},
            {"column": "FEED1", "reportingaxes": "ddid,field"},
        ])
    expected_tool, actual_tool = casa.ms(), tools.ms()
    for tool in (expected_tool, actual_tool):
        assert getattr(tool, method)(column="DATA", complex_value="amp") == {}
        assert tool.open(str(statistics_ms))
    expected, actual = (getattr(tool, method) for tool in (expected_tool, actual_tool))
    for kwargs in cases:
        assert_statistics(actual(**kwargs), expected(**kwargs))
    for tool in (expected_tool, actual_tool):
        assert tool.select({"rows": [0, 2, 7, 9]})
    assert_statistics(actual("DATA", "amp", field="0"), expected("DATA", "amp", field="0"))
    for tool in (expected_tool, actual_tool):
        assert tool.nrow(True) == 4
        assert tool.done()


def test_ms_statistics_matches_pinned_time_weighting_and_group_keys(statistics_ms):
    casa = pinned_casa()
    expected, actual = casa.ms(), tools.ms()
    for tool in (expected, actual):
        assert tool.open(str(statistics_ms))
    for options in [
        {"timebin": "0s", "useflags": False},
        {"timebin": "-1s", "useflags": False},
        {"timebin": "20", "useflags": True, "useweights": True},
        {"timebin": "00:00:20", "useflags": True, "useweights": True},
        {"timebin": "30s", "useflags": True, "useweights": True},
        {"timebin": "30s", "useweights": True, "useflags": True, "timespan": "scan,state",
         "reportingaxes": "integration", "maxuvwdistance": 1.0},
    ]:
        args = dict(column="DATA", complex_value="real", timeaverage=True, **options)
        assert_statistics(actual.statistics(**args), expected.statistics(**args))
    for tool in (expected, actual):
        assert tool.done()


def test_ms_statistics_preserves_attachment_on_errors(statistics_ms):
    casa = pinned_casa()
    for api in (casa, tools):
        tool = api.ms()
        assert tool.open(str(statistics_ms))
        count = tool.nrow()
        for key, value in [
            ("column", None), ("complex_value", 1), ("field", 0), ("useflags", np.bool_(True))
        ]:
            options = {"column": "DATA", "complex_value": "real", key: value}
            with pytest.raises(TypeError):
                tool.statistics(**options)
        with pytest.raises(RuntimeError, match="Unsupported column name"):
            tool.statistics("WEIGHT")
        assert tool.statistics("DATA", "unknown transform") == {}
        with pytest.raises(RuntimeError, match="complex_value must be"):
            tool.statisticsold("DATA", "unknown transform")
        assert tool.nrow() == count
        assert tool.done()


def test_ms_statistics_weights_observed_data_by_inverse_variance(statistics_ms):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(statistics_ms))
    data = table.getcol("DATA").real
    flags = table.getcol("FLAG")
    sigma = table.getcol("SIGMA")
    assert table.done()
    weights = np.broadcast_to((1.0 / np.square(sigma))[:, None, :], data.shape)
    total_weight = weights[~flags].sum(dtype=np.float64)
    weighted_sum = (weights[~flags].astype(np.float64) * data[~flags]).sum()
    actual, reference = tools.ms(), casa.ms()
    for tool in (actual, reference):
        assert tool.open(str(statistics_ms))
    options = dict(column="DATA", complex_value="real", useflags=True, useweights=True)
    stats = actual.statistics(**options)[""]
    pinned = reference.statistics(**options)[""]
    assert pinned["sumOfWeights"] == pytest.approx(224.0004)
    assert stats["npts"] == np.count_nonzero(~flags)
    assert stats["sumOfWeights"] == pytest.approx(total_weight)
    assert stats["mean"] == pytest.approx(weighted_sum / total_weight)
    assert stats["sum"] == pytest.approx(weighted_sum)
    for tool in (actual, reference):
        assert tool.done()


def test_ms_statistics_applies_requested_uvw_displacement(statistics_ms):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(statistics_ms), nomodify=False)
    _, integrations = np.unique(table.getcol("TIME"), return_inverse=True)
    uvw = table.getcol("UVW")
    uvw[0] += 10_000.0 * integrations
    assert table.putcol("UVW", uvw)
    assert table.done()

    actual, reference = tools.ms(), casa.ms()
    for tool in (actual, reference):
        assert tool.open(str(statistics_ms))
    options = dict(timeaverage=True, timebin="30s", useflags=True, useweights=True)
    expected = reference.statistics("DATA", "real", **options)
    assert_statistics(actual.statistics("DATA", "real", **options), expected)
    pinned = reference.statistics("DATA", "real", maxuvwdistance=1.0, **options)
    assert_statistics(pinned, expected)
    actual_limited = actual.statistics("DATA", "real", maxuvwdistance=1.0, **options)
    assert sum(stats["npts"] for stats in pinned.values()) == 80
    assert sum(stats["npts"] for stats in actual_limited.values()) == 240
    for tool in (actual, reference):
        assert tool.done()


def test_ms_statistics_applies_flags_and_weights_requested_for_time_averaging(statistics_ms):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(statistics_ms))
    data = table.getcol("DATA").real
    n_corr, n_chan, _ = data.shape
    times = np.unique(table.getcol("TIME"))
    data = data.reshape(n_corr, n_chan, len(times), -1)
    flags = table.getcol("FLAG").reshape(data.shape)
    sigma = table.getcol("SIGMA").reshape(n_corr, 1, len(times), -1)
    assert table.done()

    actual, reference = tools.ms(), casa.ms()
    for tool in (actual, reference):
        assert tool.open(str(statistics_ms))
    options = dict(column="DATA", complex_value="real", timeaverage=True, timebin="30s")
    pinned = reference.statistics(**options, useflags=True, useweights=True)
    for use_flags in (False, True):
        for use_weights in (False, True):
            # A flagged sample is usable only when its entire averaging bin is flagged.
            mask = (~flags | flags.all(axis=2, keepdims=True)) if use_flags else True
            weights = (1.0 / np.square(sigma, dtype=np.float64)) if use_weights else 1.0
            weights = np.broadcast_to(mask * weights, data.shape)
            averages = np.sum(data * weights, axis=2) / np.sum(weights, axis=2)
            args = dict(options, useflags=use_flags, useweights=use_weights)
            assert_statistics(reference.statistics(**args), pinned)
            samples, = actual.statistics(**args).values()
            assert samples["npts"] == averages.size
            assert samples["mean"] == pytest.approx(averages.mean(), rel=2e-7)
            assert samples["min"] == pytest.approx(averages.min(), rel=2e-7)
    for tool in (actual, reference):
        assert tool.done()


def test_ms_statistics_projects_float_and_alternate_visibility_columns(statistics_ms):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(statistics_ms), nomodify=False)
    data = table.getcol("DATA")
    flags = table.getcol("FLAG")
    descriptor = table.getcoldesc("DATA")
    storage = {key: descriptor[key] for key in ("ndim", "shape", "option", "maxlen")
               if key in descriptor}
    floating = data.real * 2.0 - 7.0
    sigma = np.where(flags, 1.0, 1.5).astype(np.float32)
    for name, values, primitive in (
        ("CORRECTED_DATA", data * 1.5 + 5j, "complex"),
        ("MODEL_DATA", data * 2.0 - 3j, "complex"),
        ("FLOAT_DATA", floating, "float"),
        ("WEIGHT_SPECTRUM", np.where(flags, 1.0, 2.0).astype(np.float32), "float"),
        ("SIGMA_SPECTRUM", sigma, "float"),
    ):
        table.addcols({name: dict(storage, valueType=primitive, dataManagerType="StandardStMan",
                                 dataManagerGroup=f"stats_{name}", comment=name)})
        assert table.putcol(name, values)
    assert table.done()

    actual, reference = tools.ms(), casa.ms()
    for tool in (actual, reference):
        assert tool.open(str(statistics_ms))
    for args in [
        dict(column="DATA", complex_value="real"),
        dict(column="CORRECTED_DATA", complex_value="amp", useweights=True),
        dict(column="MODEL_DATA", complex_value="phase", useweights=True),
        dict(column="FLOAT_DATA"),
        dict(column="FLOAT_DATA", timeaverage=True, timebin="30s"),
        dict(column="WEIGHT_SPECTRUM"),
    ]:
        assert_statistics(actual.statistics(**args), reference.statistics(**args))
    args = dict(column="FLOAT_DATA", useweights=True, useflags=True)
    stats = actual.statistics(**args)[""]
    pinned = reference.statistics(**args)[""]
    weights = 1.0 / np.square(sigma[~flags].astype(np.float64))
    assert stats["npts"] == np.count_nonzero(~flags)
    assert pinned["sumOfWeights"] == pytest.approx(stats["npts"] / 4)
    assert stats["sumOfWeights"] == pytest.approx(weights.sum())
    assert stats["mean"] == pytest.approx(np.average(floating[~flags], weights=weights))
    for tool in (actual, reference):
        assert tool.done()


def test_ms_statistics_reads_default_and_virtual_model_without_materializing(statistics_ms):
    casa = pinned_casa()
    casatasks = pytest.importorskip("casatasks")
    for value in (1.0, 2.5):
        if value != 1.0:
            casatasks.setjy(vis=str(statistics_ms), standard="manual",
                            fluxdensity=[value, 0.0, 0.0, 0.0], usescratch=False)
        expected, actual = casa.ms(), tools.ms()
        records = []
        for tool in (expected, actual):
            assert tool.open(str(statistics_ms))
            record = tool.statistics("MODEL_DATA", "amp")
            assert record[""]["mean"] == value
            with pytest.raises(RuntimeError, match="Model Data column is not present"):
                tool.statisticsold("MODEL_DATA", "amp")
            assert tool.done()
            records.append(record)
        assert_statistics(records[1], records[0])
        table = casa.table()
        assert table.open(str(statistics_ms))
        assert "MODEL_DATA" not in table.colnames()
        assert table.done()


def test_ms_statistics_keeps_separate_field_visits_and_uses_chronological_buffers(statistics_ms):
    casa = pinned_casa()
    tb = casa.table()
    assert tb.open(str(statistics_ms / "FIELD"), nomodify=False)
    source = {name: tb.getcell(name, 0) for name in tb.colnames() if tb.iscelldefined(name, 0)}
    assert tb.addrows(1)
    for name, value in source.items():
        assert tb.putcell(name, 1, value)
    assert tb.done()
    assert tb.open(str(statistics_ms), nomodify=False)
    times = tb.getcol("TIME")
    field_ids = (times == np.unique(times)[1]).astype(np.int32)
    assert tb.putcol("FIELD_ID", field_ids)
    assert tb.done()

    reference, actual = casa.ms(), tools.ms()
    for tool in (reference, actual):
        assert tool.open(str(statistics_ms))
    args = dict(useflags=False, doquantiles=False)
    assert_statistics(actual.statistics("TIME", **args), reference.statistics("TIME", **args))
    average_args = dict(
        timeaverage=True, timebin="100000s", timespan="scan,state",
        useweights=True, useflags=True, doquantiles=False,
    )
    expected = reference.statistics("DATA", "real", **average_args)
    assert next(iter(expected.values()))["npts"] == 240
    assert_statistics(actual.statistics("DATA", "real", **average_args), expected)

    padded = reference.statisticsold("FIELD_ID", useflags=False)["FIELD_ID"]
    assert padded["npts"] == len(field_ids)
    assert padded["sum"] == 1
    physical = actual.statisticsold("FIELD_ID", useflags=False)["FIELD_ID"]
    assert physical["npts"] == len(field_ids)
    assert physical["sum"] == np.sum(field_ids)
    assert physical["mean"] == pytest.approx(np.mean(field_ids))
    for tool in (reference, actual):
        assert tool.done()


def test_ms_statistics_averaged_time_key_uses_the_output_interval(native_dataset):
    casa = pinned_casa()
    tb = casa.table()
    assert tb.open(str(native_dataset), nomodify=False)
    times = tb.getcol("TIME")
    assert tb.putcol("INTERVAL", np.where(times == times.min(), 2.0, 10.0))
    assert tb.done()
    results = []
    for api in (casa, tools):
        tool = api.ms()
        assert tool.open(str(native_dataset))
        value = tool.statistics(
            "DATA", "amp", timeaverage=True, timebin="30s", timespan="scan,state",
            baseline="0&1", useflags=True, useweights=True, doquantiles=False,
        )
        assert list(value) == [f"TIME={times.min() - 3.0:.6f}"]
        results.append(value)
        assert tool.done()
    assert_statistics(results[1], results[0])
