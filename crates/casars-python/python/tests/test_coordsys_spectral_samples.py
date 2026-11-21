"""Public sampled spectral edits and native velocity metadata."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def samples(values, unit):
    return {"value": np.array(values, dtype=float), "unit": unit}


def test_frequency_table_uses_physical_channel_values():
    cs = tools.coordsys().newcoordsys(spectral=True)
    assert cs.setspectral(refcode="bar", frequencies=["1.4GHz", "1420MHz", "1.50GHz"])
    assert cs.getconversiontype("spectral", False) == "BARY"
    np.testing.assert_allclose(cs.toworld([1.5])["numeric"], [1.46e9])
    np.testing.assert_allclose(cs.topixel([1.46e9])["numeric"], [1.5])
    assert cs.setspectral(frequencies={"value": 15.0, "unit": "Hz"})
    np.testing.assert_allclose(cs.toworld([20.0])["numeric"], [15.0])
    np.testing.assert_allclose(cs.topixel([61.0])["numeric"], [0.0])


def test_sampled_spectral_inverse_uses_corrected_pixel_units(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(spectral=True)
        assert cs.setspectral(frequencies=samples([0.0, 1.e-301], "Hz"))
        np.testing.assert_allclose(cs.topixel([5.e-302])["numeric"], [0.5])
        assert cs.setspectral(frequencies=samples([0.0, 1.e-15, 1.0], "Hz"))
        with pytest.raises(RuntimeError, match="repeated x values"):
            cs.topixel([5.e-16])


def test_quantity_text_and_sampled_axis_names(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(spectral=True)
        assert cs.setnames(["SkyFrequency"], "spectral")
        for value, expected in (
            ("1GHz, 2GHz", [1.e9, 2.e9]),
            (["1GHz", "0.2m"], [1.e9, 299792458.0 / 0.2]),
        ):
            assert cs.setspectral(frequencies=value)
            spectral = cs.torecord()["spectral0"]
            assert spectral["name"] == "SkyFrequency"
            np.testing.assert_array_equal(spectral["tabular"]["axes"], ["Frequency"])
            np.testing.assert_allclose(spectral["tabular"]["worldvalues"], expected)
        assert cs.setspectral(doppler="RADIO", restfreq="1.42GHz", velocities="0km/s, 20km/s")
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [1.42e9 * (1 - 20_000 / 299792458.0)])


def test_active_rest_entry_survives_duplicate_values(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(spectral=True)
        assert cs.setrestfrequency(np.array([1.e9, 2.e9]), which=1)
        assert cs.setspectral(restfreq=1.e9)
        assert cs.setspectral(restfreq=3.e9)
        spectral = cs.torecord()["spectral0"]
        assert spectral["restfreq"] == pytest.approx(3.e9)
        np.testing.assert_array_equal(spectral["restfreqs"], [1.e9, 3.e9])
        assert cs.setrestfrequency(np.array([1.e9, 1.e9]), which=1)
        assert cs.setspectral(restfreq={"value": 50.0, "unit": "%"})
        spectral = cs.torecord()["spectral0"]
        assert spectral["restfreq"] == pytest.approx(0.5)
        np.testing.assert_array_equal(spectral["restfreqs"], [1.e9, 0.5])


@pytest.mark.parametrize(("convention", "spec_type", "doppler_code"), [("RADIO", 1, 0), ("OPTICAL", 2, 1), ("BETA", 1, 3)])
def test_sampled_velocity_matches_independent_relations(casa, convention, spec_type, doppler_code):
    velocity = np.array([0.0, 10000.0, 30000.0])
    speed = velocity / 299792458.0
    rest = 1.42e9
    scale = {
        "RADIO": 1.0 - speed,
        "OPTICAL": 1.0 / (1.0 + speed),
        "BETA": np.sqrt((1.0 - speed) / (1.0 + speed)),
    }[convention]
    expected = rest * scale
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(spectral=True)
        assert cs.setspectral(restfreq="1.42GHz", velocities=samples(velocity, "m/s"), doppler=convention)
        record = cs.torecord()["spectral0"]
        assert record["nativeType"] == spec_type
        assert record["velType"] == doppler_code
        np.testing.assert_allclose(record["tabular"]["worldvalues"], expected, atol=1.e-6, rtol=0.0)
        midpoint = (expected[1] + expected[2]) / 2.0
        np.testing.assert_allclose(cs.toworld([1.5])["numeric"], [midpoint], atol=1.e-6, rtol=0.0)
        np.testing.assert_allclose(cs.topixel([midpoint])["numeric"], [1.5], atol=1.e-9)


def test_native_relabelling_and_active_rest_frequency_keep_the_existing_grid(casa):
    source = casa.coordsys().newcoordsys(direction=True, spectral=True)
    assert source.setconversiontype(spectral="BARY")
    record = source.torecord()
    record["spectral1"]["restfreqs"] = np.array([1.40e9, 1.42e9, 1.45e9])
    record["spectral1"]["restfreq"] = 1.42e9
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        assert cs.setspectral(refcode="topo")
        assert cs.getconversiontype("spectral", False) == "TOPO"
        assert cs.getconversiontype("spectral") == "TOPO"
        assert cs.setspectral(restfreq=1.425e9)
        spec = cs.torecord()["spectral1"]
        np.testing.assert_allclose(spec["restfreqs"], [1.40e9, 1.425e9, 1.45e9])
        assert spec["restfreq"] == pytest.approx(1.425e9)
        _assert_value(spec["wcs"], record["spectral1"]["wcs"])


def test_different_rest_frequency_storage_units_remain_physical(casa):
    for api, multiplier in ((tools, 1.e6), (casa, 1.0)):
        cs = api.coordsys().newcoordsys(spectral=True)
        assert cs.setunits(["MHz"])
        old_rest = cs.torecord()["spectral0"]["restfreq"]
        assert cs.setspectral(frequencies=samples([1400.0, 1410.0, 1425.0], "MHz"))
        spectral = cs.torecord()["spectral0"]
        assert spectral["unit"] == "Hz"
        assert spectral["restfreq"] == pytest.approx(old_rest * multiplier)
        np.testing.assert_allclose(spectral["tabular"]["worldvalues"], [1.4e9, 1.41e9, 1.425e9])


def test_invalid_rest_frequency_preserves_previous_value(casa):
    for value, malformed in (("21cm", 0.21), (np.inf, np.inf)):
        oracle = casa.coordsys().newcoordsys(spectral=True)
        assert oracle.setspectral(restfreq=value)
        assert oracle.torecord()["spectral0"]["restfreq"] == pytest.approx(malformed)
        current = tools.coordsys().newcoordsys(spectral=True)
        before = current.torecord()
        with pytest.raises(RuntimeError, match="Rest frequency|rest frequency"):
            current.setspectral(restfreq=value)
        _assert_value(current.torecord(), before)


def test_ignored_and_refused_arguments_leave_the_coordinate_usable(casa, caplog):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(spectral=True)
        before = cs.torecord()
        assert cs.setspectral()
        assert cs.setspectral(frequencies=[1.0, 2.0])
        assert cs.setspectral(frequencies={"unit": "Hz", "value": np.array([1, 2])})
        assert cs.setspectral(frequencies={"unit": "Hz", "value": np.array([[1.0, 2.0]])})
        assert cs.setspectral(refcode="BARY", restfreq="1.5GHz", doppler="RADIO")
        assert cs.setspectral(
            frequencies=samples([1., 3.], "Hz"), velocities=samples([0., 1.], "km/s")
        ) is False
        for parameters, message in (
            ({"refcode": "absent"}, "Invalid frequency reference"),
            ({"refcode": "BARY", "frequencies": "bogus"}, "QuantumHolder::fromString"),
            ({"refcode": "BARY", "frequencies": samples([2.0], "invalid")}, "Illegal unit string"),
            ({"frequencies": samples([1., 3.], "m")}, "Illegal unit for frequencies"),
            ({"velocities": samples([0., 1.], "km/s")}, "must specify the doppler type"),
            ({"velocities": samples([0., 1.], "km/s"), "doppler": "bogus"}, "Invalid doppler"),
            ({"velocities": samples([1., 3.], "Hz"), "doppler": "RADIO"}, "Illegal unit for velocities"),
        ):
            with pytest.raises(RuntimeError, match=message):
                cs.setspectral(**parameters)
        _assert_value(cs.torecord(), before)
    assert "You cannot give both frequencies and velocities" in caplog.text


def test_velocity_axis_survives_all_native_writer_reader_pairs(casa, tmp_path):
    rest = 1.42e9
    expected = rest / (1 + 20_000.0 / 299792458.0)
    for writer in (tools, casa):
        cs = writer.coordsys().newcoordsys(spectral=True)
        assert cs.setspectral(restfreq=rest, velocities=samples([0.0, 20.0, 50.0], "km/s"), doppler="OPTICAL")
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [3], cs.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                spectral = coords.torecord()["spectral0"]
                assert spectral["nativeType"] == 2
                assert spectral["velType"] == 1
                np.testing.assert_allclose(coords.toworld([1.0])["numeric"], [expected], atol=1.e-6, rtol=0.0)
                np.testing.assert_allclose(coords.topixel([expected])["numeric"], [1.0], atol=1.e-9)
            finally:
                image.done()
