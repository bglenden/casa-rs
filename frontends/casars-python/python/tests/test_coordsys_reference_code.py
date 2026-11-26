"""Direction and frequency reference reassignment through the public tool."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def observed(api):
    coords = api.coordsys().newcoordsys(direction=True, spectral=True)
    assert coords.settelescope("VLA")
    assert coords.setepoch({"type": "epoch", "refer": "UTC", "m0": {"value": 59000., "unit": "d"}})
    assert coords.setunits(["deg", "deg"], "direction")
    assert coords.setreferencevalue([24., 32.], "direction")
    return coords


def barycentric(reference):
    longitude, latitude = np.deg2rad([24., 32.])
    vector = [np.cos(latitude) * np.cos(longitude), np.cos(latitude) * np.sin(longitude), np.sin(latitude)]
    # The kinematic LSR's standard 20 km/s apex in J2000 direction cosines.
    radial = 20_000.0 * np.dot([0.0145021, -0.865863, 0.500071], vector)
    return reference * np.sqrt((299792458.0 + radial) / (299792458.0 - radial))


@pytest.mark.parametrize("adjust", [True, False])
@pytest.mark.parametrize("projection", ["SIN", "CAR", "SFL"])
def test_native_direction_reassignment_preserves_projection_and_pc(casa, adjust, projection):
    oracle = observed(casa)
    assert oracle.setprojection(projection)
    assert oracle.setreferencepixel([2.0, 3.0], "direction")
    assert oracle.setincrement([-0.07, 0.12], "direction")
    roll = 0.31
    pc = np.array([[np.cos(roll), -np.sin(roll)], [np.sin(roll), np.cos(roll)]])
    assert oracle.setlineartransform("direction", pc)
    assert oracle.transpose([1, 2, 0])
    current = tools.coordsys()
    assert current.fromrecord(oracle.torecord())
    frame = "ECLIPTIC" if projection == "CAR" else "GALACTIC"
    for coords in (oracle, current):
        assert coords.setreferencecode(frame[:3], "d", adjust)
        np.testing.assert_array_equal(coords.referencecode("direction"), [frame])
        record = coords.torecord()
        np.testing.assert_array_equal(record["pixelmap0"], [2, 0])
        np.testing.assert_array_equal(record["worldmap0"], [2, 0])
        np.testing.assert_array_equal(record["direction0"]["units"], ["rad", "rad"])
        np.testing.assert_allclose(record["direction0"]["pc"], pc)
        if not adjust:
            np.testing.assert_allclose(record["direction0"]["crval"], np.deg2rad([24., 32.]))
    _assert_value(current.torecord()["direction0"], oracle.torecord()["direction0"])
    pixel = [3.25, 0.75, 1.5]
    np.testing.assert_allclose(current.toworld(pixel)["numeric"], oracle.toworld(pixel)["numeric"], atol=1.e-10)


@pytest.mark.parametrize("frame", ["APP", "AZEL"])
def test_direction_adjustment_uses_observation_context(casa, frame):
    angles = []
    for api in (tools, casa):
        coords = observed(api)
        assert coords.setreferencecode(frame)
        angles.append(coords.referencevalue(type="direction")["numeric"])
    np.testing.assert_allclose(*angles, rtol=0, atol=2.e-6)


@pytest.mark.parametrize("adjust", [True, False])
@pytest.mark.parametrize("sampled", [True, False])
def test_native_frequency_reassignment_offsets_the_grid(casa, adjust, sampled):
    reference = 1.4e9
    for api in (tools, casa):
        coords = observed(api)
        if sampled:
            assert coords.setspectral(frequencies=["1.4GHz", "1.41GHz", "1.5GHz"])
        else:
            assert coords.setreferencevalue([reference], "spectral")
        assert coords.setunits(["MHz"], "spectral")
        prior = [coords.toworld([0., 0., pixel])["numeric"][2] for pixel in (0., 1., 2.)]
        assert coords.setreferencecode("bar", "spectral", adjust)
        assert coords.getconversiontype("spectral", False) == "BARY"
        np.testing.assert_array_equal(coords.units("spectral"), ["MHz"])
        after = [coords.toworld([0., 0., pixel])["numeric"][2] for pixel in (0., 1., 2.)]
        shift_mhz = (barycentric(reference) - reference) / 1.e6 if adjust else 0.
        np.testing.assert_allclose(after, np.array(prior) + shift_mhz, rtol=0, atol=2.e-9)


@pytest.mark.parametrize("channel_table", [["1.4GHz"], ["1.4GHz", "1.41GHz", "1.50GHz"]])
def test_adjustment_uses_live_reference_pixel_for_sampled_frequencies(casa, channel_table):
    for api in (tools, casa):
        cs = observed(api)
        assert cs.setspectral(frequencies=channel_table)
        if len(channel_table) > 1:
            assert cs.setreferencepixel([0.5], "spectral")
        pixel = cs.referencepixel()["numeric"]
        reference = cs.referencevalue(type="spectral")["numeric"][0]
        frequency = cs.toworld(pixel)["numeric"][2]
        before = cs.torecord()["spectral1"]["tabular"]["worldvalues"]
        assert cs.setreferencecode("BARY", "spectral")
        adjusted = barycentric(frequency)
        after = cs.torecord()["spectral1"]["tabular"]["worldvalues"]
        assert cs.referencevalue(type="spectral")["numeric"][0] == pytest.approx(adjusted, rel=0, abs=0.002)
        np.testing.assert_allclose(after, before + (adjusted - reference), rtol=0, atol=0.002)


@pytest.mark.parametrize(("unit", "hz_per_unit"), [("Hz", 1.), ("MHz", 1.e6)])
def test_adjusted_legacy_linear_spectral_record_reopens_with_its_new_frequency(casa, unit, hz_per_unit):
    source = observed(casa)
    assert source.setunits([unit], "spectral")
    record = source.torecord()
    spectral = record["spectral1"]
    linear = spectral.pop("wcs")
    spectral.pop("version")
    spectral.pop("unit")
    spectral["tabular"] = {
        **{field: np.array([linear[field]], dtype=float) for field in ("crpix", "crval", "cdelt")},
        "pc": np.eye(1), "axes": ["Frequency"], "units": [unit],
        "pixelvalues": np.array([], dtype=float), "worldvalues": np.array([], dtype=float),
    }
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        assert cs.setreferencecode("BARY", "spectral")
        restored = api.coordsys()
        assert restored.fromrecord(cs.torecord())
        assert restored.getconversiontype("spectral", False) == "BARY"
        reference = restored.referencevalue(type="spectral")["numeric"][0] * hz_per_unit
        assert reference == pytest.approx(barycentric(1.415e9), rel=0, abs=0.002)


def test_adjustment_uses_native_values_when_a_display_frame_is_active(casa):
    longitude, latitude = np.deg2rad([24., 32.])
    obliquity = np.deg2rad(84381.448 / 3600.)
    x, y, z = np.cos(latitude) * np.cos(longitude), np.cos(latitude) * np.sin(longitude), np.sin(latitude)
    projected_y = y * np.cos(obliquity) + z * np.sin(obliquity)
    projected_z = z * np.cos(obliquity) - y * np.sin(obliquity)
    expected = [np.arctan2(projected_y, x), np.arctan2(projected_z, np.hypot(x, projected_y))]
    for api in (tools, casa):
        before = observed(api)
        cs = observed(api)
        assert cs.setconversiontype(direction="GALACTIC", spectral="BARY")
        same_native = cs.torecord()
        assert cs.setreferencecode("LSRK", "spectral")
        assert cs.setreferencecode("J2000")
        _assert_value(cs.torecord(), same_native)
        for coords in (before, cs):
            assert coords.setreferencecode("GEO", "spectral")
            assert coords.setreferencecode("ECLIPTIC")
        baseline = before.referencevalue()["numeric"]
        converted = cs.referencevalue()["numeric"]
        np.testing.assert_allclose(baseline[:2], expected, rtol=0, atol=1.e-10)
        if api is tools:
            np.testing.assert_allclose(converted, baseline, rtol=0, atol=2.e-7)
        else:
            assert np.linalg.norm(converted[:2] - baseline[:2]) > 0.1
            assert abs(converted[2] - baseline[2]) > 100.0


def test_errors_and_noop_preserve_live_frames(casa):
    for api in (tools, casa):
        coords = observed(api)
        prior = coords.torecord()
        for value, kind, adjust, exception, message in (
            ("GALACTIC", "linear", True, RuntimeError, "Direction.*Spectral"),
            ("unknown", "direction", True, RuntimeError, "Invalid direction code"),
            ("unknown", "spectral", True, RuntimeError, "Invalid frequency code"),
            ("REST", "spectral", True, RuntimeError, "MeasFrame"),
            ("Undefined", "spectral", True, RuntimeError, "Transformations"),
            ("GALACTIC", "direction", 1, TypeError, "argument 4 of type 'bool'"),
            ("GALACTIC", "direction", np.bool_(True), TypeError, "argument 4 of type 'bool'"),
        ):
            with pytest.raises(exception, match=message):
                coords.setreferencecode(value, kind, adjust)
            _assert_value(coords.torecord(), prior)
        spectral = api.coordsys().newcoordsys(spectral=True)
        with pytest.raises(RuntimeError, match="No Direction coordinate"):
            spectral.setreferencecode("BARY", "spectral")
        assert spectral.setreferencecode("BARY", "spectral", False)
        assert spectral.getconversiontype("spectral", False) == "BARY"


def test_direction_adjustment_rejects_an_incompatible_celestial_pole(casa):
    for api in (tools, casa):
        source = observed(casa)
        assert source.setprojection("CAR")
        coords = api.coordsys()
        assert coords.fromrecord(source.torecord())
        before = coords.torecord()
        with pytest.raises(RuntimeError, match="Ill-conditioned coordinate transformation"):
            coords.setreferencecode("GALACTIC")
        _assert_value(coords.torecord(), before)


def test_adjusted_frame_preserves_signed_direction_axis_maps(casa):
    source = observed(casa).torecord()
    for domain in ("pixel", "world"):
        source[f"{domain}map0"] = np.array([-1, 0], dtype=np.int32)
        source[f"{domain}map1"] = np.array([1], dtype=np.int32)
    source["pixelreplace0"] = np.array([0.75, 0.0])
    source["worldreplace0"] = np.array([12.0, 0.0])
    coordinates = []
    for api in (tools, casa):
        coords = api.coordsys()
        assert coords.fromrecord(source)
        assert coords.setreferencecode("GALACTIC")
        result = coords.torecord()
        np.testing.assert_array_equal(result["pixelmap0"], [-1, 0])
        np.testing.assert_array_equal(result["worldmap0"], [-1, 0])
        np.testing.assert_allclose(result["worldreplace0"], [np.deg2rad(12.0), 0.0])
        coordinates.append(coords.toworld([1.25, 4.0])["numeric"])
    np.testing.assert_allclose(*coordinates, rtol=1.e-12, atol=1.e-10)


def test_native_frame_change_survives_all_image_writer_reader_pairs(casa, tmp_path):
    expected = [2.33578013, -0.52232373, barycentric(1.415e9)]
    for writer in (tools, casa):
        coords = observed(writer)
        assert coords.setreferencecode("BARY", "spectral")
        assert coords.setreferencecode("GALACTIC")
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [5, 6, 3], coords.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                assert coords.getconversiontype("direction", False) == "GALACTIC"
                assert coords.getconversiontype("spectral", False) == "BARY"
                reference_pixel = coords.referencepixel()["numeric"]
                result = coords.toworld(reference_pixel)["numeric"]
                np.testing.assert_allclose(result[:2], expected[:2], rtol=0, atol=1.e-8)
                assert result[2] == pytest.approx(expected[2], abs=0.003, rel=0)
                np.testing.assert_allclose(coords.topixel(result)["numeric"], reference_pixel, atol=1.e-8)
            finally:
                image.done()
