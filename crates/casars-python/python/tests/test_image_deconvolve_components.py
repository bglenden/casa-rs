"""Deconvolve component lists against the pinned CASA release."""

import copy

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            compare(actual[key], expected[key])
    elif isinstance(expected, (float, np.ndarray)):
        np.testing.assert_allclose(actual, expected, rtol=2e-12, atol=2e-13)
    else:
        assert actual == expected


def components(module, shape="Gaussian", major="10arcsec", minor="7arcsec", pa="0deg"):
    tool = module.componentlist()
    assert tool.addcomponent(
        flux=3.125,
        fluxunit="mJy",
        dir="J2000 01h00m00s +02d00m00s",
        shape=shape,
        majoraxis=major,
        minoraxis=minor,
        positionangle=pa,
        spectrumtype="spectral index",
        index=-0.7,
        freq="1.3GHz",
    )
    assert tool.setlabel(0, "resolved source")
    result = tool.torecord()
    tool.done()
    return result


def test_detached_and_binding(casa):
    for module in (tools, casa):
        image = module.image()
        assert image.deconvolvecomponentlist({}) == {}
        assert image.deconvolvecomponentlist({}, True, np.int64(1)) == {}
        with pytest.raises(TypeError, match="_complist is not a dictionary"):
            image.deconvolvecomponentlist([])
        with pytest.raises(TypeError, match="_channel must be an integer"):
            image.deconvolvecomponentlist({}, 0.5)
        with pytest.raises(TypeError, match="_polarization must be an integer"):
            image.deconvolvecomponentlist({}, polarization="1")
        for invalid in (None, 2**128, np.array([1]), np.float32(1), (1,)):
            with pytest.raises(SystemError, match="returned a result with an exception set"):
                image.deconvolvecomponentlist({}, invalid)
        assert image.fromshape(shape=[4, 5])
        with pytest.raises(RuntimeError, match="does not have a restoring beam"):
            image.deconvolvecomponentlist({}, np.array([1]))
        assert image.done()


def test_aligned_gaussian_matches_analytic_widths():
    image = tools.image()
    assert image.fromshape(shape=[5, 6])
    assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg")
    result = image.deconvolvecomponentlist(components(tools))["component0"]["shape"]
    assert result["majoraxis"] == {"unit": "arcsec", "value": pytest.approx(np.sqrt(84))}
    assert result["minoraxis"] == {"unit": "arcsec", "value": pytest.approx(np.sqrt(40))}
    assert image.done()


def test_point_and_rotated_gaussian_keep_component_metadata(casa):
    actual, expected = tools.image(), casa.image()
    for image in (actual, expected):
        assert image.fromshape(shape=[5, 6])
        assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg")
    gauss = components(casa, pa="-20deg")
    point = components(tools, shape="Point")["component0"]
    gauss["nelements"] = 2
    gauss["component1"] = point
    for component in (gauss["component0"], point):
        component["shape"]["direction"]["error"]["longitude"] = {
            "value": 3.0,
            "unit": "arcmin",
        }
    gauss["component0"]["shape"]["majoraxiserror"] = {"value": 0.5, "unit": "arcsec"}
    original = copy.deepcopy(gauss)
    deconvolved = actual.deconvolvecomponentlist(gauss, channel=100, polarization=-100)
    compare(deconvolved, expected.deconvolvecomponentlist(gauss, 100, -100))
    compare(gauss, original)
    compare(deconvolved["component1"], point)
    assert deconvolved["component0"]["shape"]["majoraxiserror"] == {
        "value": 0.0,
        "unit": "arcmin",
    }
    assert deconvolved["component0"]["shape"]["direction"]["error"]["longitude"] == {
        "value": 0.0,
        "unit": "rad",
    }
    for module in (tools, casa):
        cl = module.componentlist()
        assert cl.fromrecord(deconvolved)
        compare(cl.getcomponent(1), point)
        cl.done()
    for image in (actual, expected):
        assert image.done()


@pytest.mark.parametrize(
    "major,minor,pa,error",
    [
        ("10arcsec", "7arcsec", "0deg", None),
        ("2arcmin", "120arcsec", "0deg", None),
        ("4arcsec", "3arcsec", "0deg", None),
        ("3.9arcsec", "2.95arcsec", "0deg", None),
        ("4.0001arcsec", "3.00001arcsec", "0deg", None),
        ("10arcsec", "3arcsec", "0deg", None),
        ("4arcsec", "4arcsec", "0deg", "axialRatio must be"),
        ("1arcsec", "1arcsec", "0deg", "resolved in one direction"),
        ("7arcsec", "2arcsec", "0deg", "resolved in one direction"),
    ],
)
def test_gaussians_at_the_resolution_boundary(casa, major, minor, pa, error):
    actual, expected = tools.image(), casa.image()
    for image in (actual, expected):
        assert image.fromshape(shape=[5, 6])
        assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg")
    record = components(casa, major=major, minor=minor, pa=pa)
    if error:
        for image in (actual, expected):
            with pytest.raises(RuntimeError, match=error):
                image.deconvolvecomponentlist(record)
    else:
        result = actual.deconvolvecomponentlist(record)
        compare(result, expected.deconvolvecomponentlist(record))
    for image in (actual, expected):
        assert image.done()


@pytest.mark.parametrize(
    "major,minor,pa",
    [
        ("4arcsec", "3arcsec", "0deg"),
        ("4arcsec", "3arcsec", "45deg"),
        ("3arcsec", "1arcsec", "15deg"),
        ("3arcsec", "1arcsec", "90deg"),
    ],
)
def test_equal_beams_with_records_from_both_tools(casa, major, minor, pa):
    actual, expected = tools.image(), casa.image()
    for image in (actual, expected):
        assert image.fromshape(shape=[5, 6])
        assert image.setrestoringbeam(major, minor, pa)
    for module in (tools, casa):
        record = components(module, major=major, minor=minor, pa=pa)
        compare(actual.deconvolvecomponentlist(record), expected.deconvolvecomponentlist(record))
    for image in (actual, expected):
        assert image.done()


@pytest.mark.parametrize(
    "value", [[10.0], np.array([10], dtype=np.float32), np.array([10], dtype=np.int32)]
)
def test_single_element_quantity_arrays(casa, value):
    actual, expected = tools.image(), casa.image()
    for image in (actual, expected):
        assert image.fromshape(shape=[5, 6])
        assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg")
    record = components(casa, pa="45deg")
    record["component0"]["shape"]["majoraxis"]["value"] = value
    compare(actual.deconvolvecomponentlist(record), expected.deconvolvecomponentlist(record))
    for image in (actual, expected):
        assert image.done()


@pytest.mark.parametrize("shape,kind", [([4, 5, 2, 3], "c"), ([4, 5, 1, 3], "f")])
def test_per_plane_beams(casa, shape, kind):
    actual, expected = tools.image(), casa.image()
    multiple_stokes = shape[2] > 1
    for image in (actual, expected):
        assert image.fromshape(shape=shape, type=kind)
        assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg", channel=0, polarization=0)
        assert image.setrestoringbeam(
            "3arcsec", "2arcsec", "10deg", channel=1, polarization=int(multiple_stokes)
        )
    record = components(casa, pa="15deg")
    first = actual.deconvolvecomponentlist(record, 0, 0)
    second = actual.deconvolvecomponentlist(record, 1, int(multiple_stokes))
    compare(first, expected.deconvolvecomponentlist(record, 0, 0))
    compare(second, expected.deconvolvecomponentlist(record, 1, int(multiple_stokes)))
    assert first["component0"]["shape"]["majoraxis"] != second["component0"]["shape"]["majoraxis"]
    compare(first, actual.deconvolvecomponentlist(record, 2**32, 2**32))
    if not multiple_stokes:
        compare(first, actual.deconvolvecomponentlist(record, 0, -1))
        compare(first, expected.deconvolvecomponentlist(record, 0, -1))
    for image in (actual, expected):
        for channel, pol in [(-1, -1), (-1, 0), (3, 0)]:
            with pytest.raises(RuntimeError, match="Failed AlwaysAssert"):
                image.deconvolvecomponentlist(record, channel, pol)
        assert image.done()


@pytest.mark.parametrize("writer_is_casa", [False, True])
def test_deconvolves_native_and_fits_beams(casa, tmp_path, writer_is_casa):
    native = str(tmp_path / "source.im")
    fits = str(tmp_path / "source.fits")
    writer = (casa if writer_is_casa else tools).image()
    assert writer.fromshape(native, [5, 6])
    assert writer.setrestoringbeam("0.07arcmin", "2arcsec", "10deg")
    assert writer.done()
    exporter = casa.image()
    assert exporter.open(native)
    assert exporter.tofits(fits)
    assert exporter.done()
    record = components(casa, pa="45deg")
    for path in (native, fits):
        actual, expected = tools.image(), casa.image()
        for image in (actual, expected):
            assert image.open(path)
        compare(actual.deconvolvecomponentlist(record), expected.deconvolvecomponentlist(record))
        for image in (actual, expected):
            assert image.done()


def test_sparse_and_malformed_records(casa):
    actual, expected = tools.image(), casa.image()
    for image in (actual, expected):
        assert image.fromshape(shape=[5, 6])
        assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg")
    for record in ({}, {"ignored": 1}, {"nelements": 0}, {"nelements": 1, "component0": {"label": "default"}}):
        compare(actual.deconvolvecomponentlist(record), expected.deconvolvecomponentlist(record))
    for module, image in ((tools, actual), (casa, expected)):
        for record, error in [
            ({"nelements": 1}, "field component0 is unknown"),
            ({"nelements": -1}, "field component0 is unknown"),
            ({"nelements": 1, "component0": 3}, "incorrect data type Record"),
            ({"nelements": 1, "component0": {"shape": 3}}, "shape' field must be a record"),
            ({"nelements": 1.2}, "asuInt - invalid data type"),
            (components(module, "Disk"), "Cannot deconvolve components of type Disk"),
        ]:
            with pytest.raises(RuntimeError, match=error):
                image.deconvolvecomponentlist(record)
        assert image.done()


@pytest.mark.parametrize(
    "kind,linear,beam,error",
    [
        ("f", False, False, "does not have a restoring beam"),
        ("f", True, True, "does not contain a direction coordinate"),
        ("d", False, True, "double precision pixel values"),
        ("cd", False, True, "complex<double> precision pixel values"),
    ],
)
def test_requires_supported_image_beam_and_direction(casa, kind, linear, beam, error):
    for module in (tools, casa):
        image = module.image()
        assert image.fromshape(shape=[4, 5], type=kind, linear=linear)
        if beam:
            assert image.setrestoringbeam("4arcsec", "3arcsec", "0deg")
        with pytest.raises(RuntimeError, match=error):
            image.deconvolvecomponentlist({})
        assert image.done()
