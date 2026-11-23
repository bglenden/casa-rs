"""Independent controls for the public MS FIELD direction surface."""

import numpy as np
import pytest
from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import ephemeris_dataset, native_dataset, measures
from test_tools_simulator import configure


def test_ms_field_direction_polynomial_and_defaults(native_dataset):
    casa = pinned_casa()
    values = {
        "PHASE_DIR": np.array([[-0.4, 0.003], [0.2, 0.002]]),
        "DELAY_DIR": np.array([[-0.3, 0.002], [0.1, 0.003]]),
        "REFERENCE_DIR": np.array([[-0.2, 0.001], [0.3, 0.004]]),
    }
    origin = 4_850_000_000.
    table = casa.table()
    assert table.open(str(native_dataset / "FIELD"), nomodify=False)
    assert table.putcell("NUM_POLY", 0, 1)
    assert table.putcell("TIME", 0, origin)
    for column, coefficients in values.items():
        assert table.putcell(column, 0, coefficients)
    assert table.done()

    empty = np.empty(0, dtype=bool)
    for module in (casa, tools):
        ms = module.ms()
        np.testing.assert_array_equal(ms.getfielddirmeas(), empty, strict=True)
        assert ms.open(str(native_dataset))
        assert ms.getfielddirmeas() == ms.getfielddirmeas("phase_dir", np.int64(0))
        for column in (*values, "EPHEMERIS_DIR"):
            coefficients = values["REFERENCE_DIR" if column == "EPHEMERIS_DIR" else column]
            for seconds, offset in ((0.0, 0.0), (origin, 0.0), (origin + 25.0, 25.0)):
                record = ms.getfielddirmeas(dircolname=column, fieldid=np.int32(0), time=seconds)
                expected = coefficients[:, 0] + offset * coefficients[:, 1]
                assert set(record) == {"m0", "m1", "type", "refer"}
                assert record["type"] == "direction"
                assert record["refer"] == "J2000"
                assert record["m0"]["unit"] == record["m1"]["unit"] == "rad"
                np.testing.assert_allclose([record[key]["value"] for key in ("m0", "m1")],
                                           expected, rtol=0, atol=1e-13)
                lon, lat = expected
                cosines = [np.cos(lon) * np.cos(lat), np.sin(lon) * np.cos(lat), np.sin(lat)]
                expected_text = "Direction: [" + ", ".join(f"{value:.6g}" for value in cosines) + "]"
                assert ms.getfielddirmeas(column, 0, seconds, "String") == expected_text
        np.testing.assert_array_equal(ms.getfielddirmeas(fieldid=-1), empty, strict=True)
        if module is casa:
            assert ms.getfielddirmeas("phase")["m1"]["value"] == np.pi / 2
        else:
            np.testing.assert_array_equal(ms.getfielddirmeas("phase"), empty, strict=True)
        field = native_dataset / "FIELD"
        changed = values["PHASE_DIR"].copy()
        changed[0, 0] = -0.17
        assert table.open(str(field), nomodify=False)
        assert table.putcell("PHASE_DIR", 0, changed)
        assert table.done()
        assert ms.getfielddirmeas()["m0"]["value"] == pytest.approx(-0.17, abs=1e-13)
        assert ms.done()
        assert table.open(str(field), nomodify=False)
        assert table.putcell("PHASE_DIR", 0, values["PHASE_DIR"])
        assert table.done()


def test_ms_field_ephemeris_on_casa_reference_view(ephemeris_dataset):
    casa = pinned_casa()
    dataset, mjd = ephemeris_dataset
    view = dataset.with_name("field-ephemeris-view.ms")
    table = casa.table()
    assert table.open(str(dataset))
    selected = table.selectrows(np.array([0, 2], dtype=np.int64), str(view))
    assert selected.done() and table.done()
    assert not (view / "FIELD").exists()

    results = []
    for module in (casa, tools):
        ms = module.ms()
        assert ms.open(str(view))
        outputs = {}
        for column in ("PHASE_DIR", "REFERENCE_DIR", "DELAY_DIR", "EPHEMERIS_DIR"):
            at_origin = ms.getfielddirmeas(column)
            later = ms.getfielddirmeas(column, time=(mjd + 0.75) * 86400.)
            assert at_origin["m0"]["value"] != later["m0"]["value"]
            outputs[column] = later
            bad = ms.getfielddirmeas(column, time=(mjd + 20.) * 86400.)
            np.testing.assert_array_equal(bad, np.empty(0, dtype=bool), strict=True)
        assert outputs["PHASE_DIR"] != outputs["EPHEMERIS_DIR"]
        assert outputs["REFERENCE_DIR"] != outputs["EPHEMERIS_DIR"]
        assert ms.done()
        results.append(outputs)
    for column, expected in results[0].items():
        actual = results[1][column]
        assert actual["type"] == expected["type"]
        assert actual["refer"] == expected["refer"]
        for angle in ("m0", "m1"):
            assert actual[angle]["unit"] == expected[angle]["unit"]
            assert actual[angle]["value"] == pytest.approx(expected[angle]["value"], abs=2e-11)


def test_ms_spectral_window_info_tracks_used_descriptions(tmp_path, measures):
    casa = pinned_casa()
    dataset = tmp_path / "two-spws.ms"
    simulator = tools.simulator()
    assert simulator.open(str(dataset))
    configure(simulator)
    for name, frequency in (("H", "2GHz"), ("Unused", "3GHz")):
        assert simulator.setspwindow(
            spwname=name, freq=frequency, deltafreq="2MHz", freqresolution="2MHz",
            nchannels=3, stokes="RR LL",
        )
    assert simulator.observe("F", "L", starttime="0s", stoptime="30s")
    assert simulator.observe("F", "H", starttime="30s", stoptime="60s")
    assert simulator.done()

    table = casa.table()
    assert table.open(str(dataset / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.putcell("CHAN_WIDTH", 1, np.full(3, -2e6))
    assert table.putcolkeyword("REF_FREQUENCY", "MEASINFO", {"type": "frequency", "Ref": "BARY"})
    assert table.done()

    results = []
    for module in (casa, tools):
        ms = module.ms()
        assert ms.getspectralwindowinfo() == {}
        assert ms.open(str(dataset))
        info = ms.getspectralwindowinfo()
        assert set(info) == {"0", "1"}
        assert info["1"]["SpectralWindowId"] == 1
        assert info["1"]["NumChan"] == 3
        assert info["1"]["NumCorr"] == 2
        assert info["1"]["ChanWidth"] == -2e6
        assert info["1"]["Frame"] == "BARY"
        assert ms.msselect({"spw": "1"}, onlyparse=True)
        assert ms.getspectralwindowinfo() == info
        assert ms.msselect({"spw": "1"})
        assert ms.getspectralwindowinfo() == {"1": info["1"]}
        spw = dataset / "SPECTRAL_WINDOW"
        assert table.open(str(spw), nomodify=False)
        assert table.putcell("CHAN_WIDTH", 1, np.full(3, -3e6))
        assert table.done()
        assert ms.getspectralwindowinfo()["1"]["ChanWidth"] == -3e6
        assert ms.msselect({"taql": "ANTENNA1 < 0"}) is False
        if module is casa:
            with pytest.raises(RuntimeError, match="whichField"):
                ms.getspectralwindowinfo()
        else:
            assert ms.getspectralwindowinfo() == {}
        assert ms.done()
        assert table.open(str(spw), nomodify=False)
        assert table.putcell("CHAN_WIDTH", 1, np.full(3, -2e6))
        assert table.done()
        results.append(info)
    assert results[0] == results[1]
    for dd in results[0]:
        for key in results[0][dd]:
            assert type(results[0][dd][key]) is type(results[1][dd][key])
