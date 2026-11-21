"""CASA projection replacement and cylindrical projection geometry."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def setup_direction(api):
    coords = api.coordsys().newcoordsys(direction=True)
    assert coords.setunits(["rad", "rad"])
    assert coords.setreferencevalue([0., 0.])
    assert coords.setincrement([0.1, 0.1])
    assert coords.setreferencepixel([0., 0.])
    return coords


@pytest.mark.parametrize(("name", "parameters", "stored", "expected"), [
    ("SIN", [-1.0], "SIN", [0., 0.]),
    ("SIN", [0.2], "SIN", [0., 0.]),
    ("SIN", [0.2, 0.3, 0.4], "SIN", [0.2, 0.3, 0.4]),
    ("AZP", [], "AZP", [0., 0.]),
    ("SZP", [2.0], "SZP", [0., 0., 90.]),
    ("AIR", [], "AIR", [90.]),
    ("TAN", [2.0], "TAN", [2.0]),
    ("GLS", [], "SFL", []),
    ("CYP", [], "CYP", [1., 1.]),
    ("CYP", [2.], "CYP", [0., 1.]),
    ("CEA", [], "CEA", [1.]),
])
def test_setprojection_rebuilds_the_direction_and_preserves_pixel_geometry(casa, name, parameters, stored, expected):
    source = setup_direction(casa)
    assert source.setunits(["deg", "arcmin"])
    assert source.setnames(["First", "Second"])
    assert source.setreferencepixel([2., 3.])
    assert source.setconversiontype(direction="GALACTIC")
    matrix = np.array([[0.8, -0.6], [0.6, 0.8]])
    assert source.setlineartransform("direction", matrix)
    original = source.torecord()
    for api in (casa, tools):
        coords = api.coordsys()
        assert coords.fromrecord(original)
        assert coords.setprojection(name, parameters)
        actual = coords.torecord()["direction0"]
        assert actual["system"] == actual["conversionSystem"] == "J2000"
        assert actual["projection"] == stored
        np.testing.assert_array_equal(actual["projection_parameters"], expected)
        np.testing.assert_array_equal(actual["axes"], ["Right Ascension", "Declination"])
        for field in ("crpix", "crval", "cdelt", "pc", "units"):
            _assert_value(actual[field], original["direction0"][field])


@pytest.mark.parametrize(("name", "parameters"), [("CEA", [0.7]), ("CYP", [2.0, 1.5])])
def test_cylindrical_projections_match_independent_equations(casa, name, parameters):
    longitude, latitude = 0.2, 0.3
    if name == "CEA":
        plane = [longitude, np.sin(latitude) / parameters[0]]
    else:
        mu, lam = parameters
        plane = [lam * longitude, (mu + lam) * np.sin(latitude) / (mu + np.cos(latitude))]
    expected_pixel = np.array(plane) / 0.1
    for api in (tools, casa):
        coords = setup_direction(api)
        assert coords.setprojection(name, parameters)
        np.testing.assert_allclose(coords.topixel([longitude, latitude])["numeric"], expected_pixel, atol=1.e-12, rtol=0)
        np.testing.assert_allclose(coords.toworld(expected_pixel)["numeric"], [longitude, latitude], atol=1.e-12, rtol=0)


def test_projection_errors_keep_the_previous_transform(casa):
    for api in (tools, casa):
        coords = setup_direction(api)
        prior = coords.torecord()
        for name, parameters in (
            ("sin", []), ("NCP", []), ("BOGUS", []), ("CEA", [1.5]), ("CYP", [1., 0.]),
            ("BON", []), ("COD", [30.]), ("COE", [0., 5.]), ("HPX", [0., 2.]), ("ZPN", []),
            ("COD", [180., 0.]), ("COP", [180., 0.]), ("COD", [30., 180.]),
        ):
            with pytest.raises(RuntimeError):
                coords.setprojection(name, parameters)
            _assert_value(coords.torecord(), prior)


def test_every_casa_projection_is_selectable(casa):
    reference = setup_direction(casa)
    choices = reference.projection("all")["types"]
    for name in choices:
        parameters = {
            "ZPN": [0., 1.] + [0.] * 18,
            "COP": [30., 10.], "COD": [30., 10.],
            "COE": [30., 10.], "COO": [30., 10.], "BON": [30.],
        }.get(name, [])
        for api in (tools, casa):
            coords = setup_direction(api)
            assert coords.setprojection(name, parameters)
            assert coords.projection()["type"] == name


@pytest.mark.parametrize("name", ["COP", "COD", "COE", "COO"])
@pytest.mark.parametrize("sign", [1, -1])
def test_conic_projections_match_pinned_casa_away_from_the_pole(casa, name, sign):
    source = setup_direction(casa)
    assert source.setreferencevalue([0., np.deg2rad(30 * sign)])
    assert source.setprojection(name, [30. * sign, 10.])
    current = setup_direction(tools)
    assert current.setreferencevalue([0., np.deg2rad(30 * sign)])
    assert current.setprojection(name, [30. * sign, 10.])
    _assert_value(current.torecord()["direction0"], source.torecord()["direction0"])
    for pixel in ([1., 1.5], [-1., -2.], [3., 2.5]):
        expected = source.toworld(pixel)["numeric"]
        np.testing.assert_allclose(current.toworld(pixel)["numeric"], expected, rtol=0, atol=3.e-12)
        np.testing.assert_allclose(current.topixel(expected)["numeric"], pixel, rtol=0, atol=3.e-12)


@pytest.mark.parametrize("latitude", [-0.9, 0.1, 0.9])
def test_healpix_and_bonne_accept_agreeing_polar_and_equatorial_points(casa, latitude):
    longitude = 0.3
    for name, parameters in (("HPX", []), ("HPX", [4., 4.]), ("BON", [0.]), ("BON", [35.])):
        positions = []
        for api in (tools, casa):
            coords = setup_direction(api)
            assert coords.setprojection(name, parameters)
            pixel = coords.topixel([longitude, latitude])["numeric"]
            positions.append(pixel)
            np.testing.assert_allclose(coords.toworld(pixel)["numeric"], [longitude, latitude], rtol=0, atol=2.e-11)
        np.testing.assert_allclose(*positions, rtol=0, atol=2.e-11)
        if name == "BON":
            central = coords.topixel([0., latitude])["numeric"]
            np.testing.assert_allclose(central, [0., latitude / 0.1], rtol=0, atol=2.e-11)


def test_zero_latitude_bonne_rejects_pixels_outside_the_sky(casa):
    for api in (tools, casa):
        coords = setup_direction(api)
        assert coords.setprojection("BON", [0.])
        for pixel in ([40., 0.], [0., 20.]):
            with pytest.raises(RuntimeError):
                coords.toworld(pixel)


@pytest.mark.parametrize(("name", "parameters", "invalid_latitude"), [
    ("CEA", [1.], -0.5), ("CYP", [1., 1.], -0.5), ("BON", [0.], -0.5),
    ("HPX", [], -0.5), ("COP", [30., 10.], 0.9),
])
def test_failed_reference_edit_keeps_the_projected_grid(casa, name, parameters, invalid_latitude):
    for api in (tools, casa):
        coords = setup_direction(api)
        assert coords.setprojection(name, parameters)
        before = coords.torecord()
        world_before = coords.toworld([0., 0.])["numeric"]
        with pytest.raises(RuntimeError, match="coordinate transformation"):
            coords.setreferencevalue([0., invalid_latitude])
        if api is tools:
            _assert_value(coords.torecord(), before)
            # A zero-longitude WCS coordinate can be represented by 2π.
            np.testing.assert_array_equal(coords.toworld([0., 0.])["numeric"], world_before)
        else:
            assert coords.torecord()["direction0"]["crval"][1] == pytest.approx(invalid_latitude)


def test_conic_frame_adjustment_rejects_an_unusable_native_pole(casa):
    for api in (tools, casa):
        coords = setup_direction(api)
        # J2000 coordinates of Galactic north require a different native
        # pole when changing the reference code of this conic plane.
        assert coords.setreferencevalue(np.deg2rad([192.85948, 27.12825]))
        assert coords.setprojection("COD", [30., 10.])
        before = coords.torecord()
        with pytest.raises(RuntimeError, match="coordinate transformation"):
            coords.setreferencecode("GALACTIC")
        _assert_value(coords.torecord(), before)


@pytest.mark.parametrize("high_order", [False, True])
def test_zpn_coefficients_and_results_survive_fits_writer_reader_pairs(casa, tmp_path, high_order):
    from astropy.io import fits
    from astropy.wcs import WCS

    if high_order:
        parameters = np.array([0.0, 1.0] + [0.0] * 18 + [0.1])
        pixel = [0.0, -11.0]
    else:
        parameters = np.array([0.02, 1.0, 0.1, -0.02] + [0.0] * 16)
        pixel = [1.0, 1.6]
    for writer in (tools, casa):
        coords = setup_direction(writer)
        assert coords.setreferencevalue([0., np.pi / 2.])
        assert coords.setprojection("ZPN", parameters)
        world = coords.toworld(pixel)["numeric"]
        zenith_distance = np.pi / 2. - world[1]
        radius = np.hypot(0.1 * pixel[0], 0.1 * pixel[1])
        assert np.polynomial.polynomial.polyval(zenith_distance, parameters) == pytest.approx(radius, abs=1.e-12, rel=0)
        image = writer.image()
        assert image.fromshape("", [8, 8], coords.torecord())
        path = str(tmp_path / f"{writer.__name__}.fits")
        assert image.tofits(path, history=False)
        image.done()
        with fits.open(path, mode="update") as hdus:
            header = hdus[0].header
            if writer is casa:
                # CASA writes every coefficient one slot too high. Move its
                # recorded values to the FITS WCS indices before readback.
                assert "PV2_0" not in header
                np.testing.assert_allclose([header.pop(f"PV2_{n + 1}") for n in range(len(parameters))], parameters)
                for n, coefficient in enumerate(parameters):
                    if not high_order or coefficient != 0.0:
                        header[f"PV2_{n}"] = coefficient
            else:
                np.testing.assert_allclose([header[f"PV2_{n}"] for n in range(len(parameters))], parameters)
            np.testing.assert_allclose(WCS(header).pixel_to_world_values(*pixel), np.rad2deg(world), rtol=0, atol=3.e-8)
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                reopened = image.coordsys()
                assert reopened.projection()["type"] == "ZPN"
                if reader is tools:
                    np.testing.assert_allclose(reopened.projection()["parameters"], parameters)
                np.testing.assert_allclose(reopened.toworld(pixel)["numeric"], world, rtol=0, atol=3.e-10)
            finally:
                image.done()


def test_cylindrical_projection_roundtrips_native_image_writer_reader_pairs(casa, tmp_path):
    for writer in (tools, casa):
        coords = setup_direction(writer)
        assert coords.setprojection("CEA", [0.7])
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [8, 8], coords.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                assert coords.projection()["type"] == "CEA"
                np.testing.assert_allclose(coords.toworld([2., 3.])["numeric"], [0.2, np.arcsin(0.21)], atol=1.e-12, rtol=0)
            finally:
                image.done()
