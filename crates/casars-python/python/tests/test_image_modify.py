"""Image component edits against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def model(module, *, gaussian=False):
    cl = module.componentlist()
    assert cl.addcomponent(flux=[2.0, 0.5, 0.0, 0.0], dir="J2000 00h00m00s 00d00m00s")
    if gaussian:
        assert cl.addcomponent(
            flux=1.25,
            dir="J2000 00h00m00s 00d00m00s",
            shape="Gaussian",
            majoraxis="3arcmin",
            minoraxis="2arcmin",
            positionangle="27deg",
        )
    record = cl.torecord()
    cl.done()
    return record


def test_point_subtracts_by_default_and_round_trips():
    image = tools.image()
    assert image.fromshape(shape=[7, 7])
    assert image.setbrightnessunit("Jy/pixel")
    point = model(tools)
    expected = np.zeros((7, 7), dtype=np.float32)
    expected[3, 3] = -2.0
    assert image.modify(point, list=False)
    np.testing.assert_array_equal(image.getchunk(), expected)
    assert image.modify(point, subtract=False)
    np.testing.assert_array_equal(image.getchunk(), np.zeros_like(expected))
    assert image.done()


@pytest.mark.parametrize("brightness", ["", "Jy/pixel", "mJy/pixel", "Jy/beam"])
def test_gaussian_and_stokes_planes_in_native_image(casa, tmp_path, brightness):
    actual, expected = tools.image(), casa.image()
    native = str(tmp_path / "native.im")
    record = model(casa, gaussian=True)
    for image, path in ((actual, native), (expected, "")):
        assert image.fromshape(path, [7, 7, 2, 3])
        assert image.setbrightnessunit(brightness)
        assert image.setrestoringbeam("4arcmin", "3arcmin", "7deg")
        assert image.modify(record, subtract=False, list=False)
        assert image.history()[-2:] == [
            "Ran ia.modify",
            'ia.modify(model={...}, region={...}, mask="", subtract=false, list=false, stretch=false)',
        ]
    result = expected.getchunk()
    assert actual.getchunk().dtype == result.dtype == np.float64
    assert result[3, 3, 0, 0] > result[3, 3, 1, 0]
    np.testing.assert_allclose(actual.getchunk(), result, rtol=1e-5, atol=1e-8)
    assert actual.done()
    assert expected.done()
    oracle = casa.image()
    assert oracle.open(native)
    np.testing.assert_allclose(oracle.getchunk(), result, rtol=1e-5, atol=1e-8)
    assert oracle.done()


@pytest.mark.parametrize(
    "shape,direction,spectral",
    [
        ("Point", "J2000 12h00m00s +00d00m00s", False),
        ("Disk", "J2000 00h00m00s +00d00m00s", False),
        ("Limbdarkeneddisk", "J2000 00h00m00s +00d00m00s", False),
        ("Point", "J2000 00h00m00s +00d00m00s", True),
    ],
)
def test_direction_disk_and_spectral_index(casa, shape, direction, spectral):
    cl = casa.componentlist()
    assert cl.addcomponent(
        flux=[2.0, 0.5, 0.0, 0.0],
        dir=direction,
        shape="Disk" if shape == "Limbdarkeneddisk" else shape,
        majoraxis="4arcmin",
        minoraxis="3arcmin",
        positionangle="35deg",
        spectrumtype="spectral index" if spectral else "constant",
        freq="1.3GHz",
        index=-0.8,
    )
    record = cl.torecord()
    cl.done()
    if shape == "Limbdarkeneddisk":
        record["component0"]["shape"]["type"] = shape
        record["component0"]["optionalParameters"] = np.array([0.1])
    results = []
    for module in (tools, casa):
        image = module.image()
        assert image.fromshape(shape=[7, 7, 2, 3])
        assert image.setbrightnessunit("Jy/pixel")
        assert image.modify(record, subtract=False)
        results.append(image.getchunk())
        assert image.done()
    np.testing.assert_allclose(*results, rtol=1e-6, atol=1e-8)
    if spectral:
        assert results[0][3, 3, 0, 0] != results[0][3, 3, 0, 2]
    elif shape == "Disk":
        assert np.count_nonzero(results[0]) > 6
    elif shape == "Limbdarkeneddisk":
        assert results[0][3, 3, 0, 0] == pytest.approx(2.0 / (np.pi * 4.0 * 3.0))
    else:
        assert np.count_nonzero(results[0]) == 0


def test_region_bounds_and_mask_validation(casa, tmp_path):
    region = casa.regionmanager().box([2, 2], [5, 5])
    mask_path = str(tmp_path / "mask.im")
    mask = casa.image()
    assert mask.fromshape(mask_path, [7, 7])
    assert mask.done()
    expression = f'"{mask_path}" > 0'
    record = model(casa)
    for module in (tools, casa):
        image = module.image()
        assert image.fromshape(shape=[7, 7, 2, 3])
        assert image.setbrightnessunit("Jy/pixel")
        assert image.calcmask("F", "invalid")
        initial_history = image.history()
        with pytest.raises(RuntimeError, match="mask shape"):
            image.modify(record, mask=expression, stretch=False)
        with pytest.raises(RuntimeError, match="Input mask specification is incorrect"):
            image.modify(record, mask="1+2")
        np.testing.assert_array_equal(image.getchunk(), 0.0)
        assert image.history() == initial_history
        assert image.modify(record, region=region, mask=expression, subtract=False, stretch=True)
        expected = np.zeros((7, 7, 2, 3), dtype=np.float32)
        expected[3, 3, 0, :] = 2.0
        expected[3, 3, 1, :] = 0.5
        np.testing.assert_array_equal(image.getchunk(), expected)
        np.testing.assert_array_equal(image.getchunk(getmask=True), False)
        assert image.done()


def test_beam_grid_requires_a_single_selected_plane(casa):
    record = model(casa)
    plane = casa.regionmanager().box([0, 0, 0, 0], [6, 6, 0, 0])
    expected = 2.0 * np.pi / (4.0 * np.log(2.0)) * 4.0 * 3.0
    for module in (tools, casa):
        image = module.image()
        assert image.fromshape(shape=[7, 7, 2, 3])
        assert image.setbrightnessunit("Jy/beam")
        assert image.setrestoringbeam("4arcmin", "3arcmin", "7deg", channel=0, polarization=0)
        with pytest.raises(RuntimeError, match="cannot extend lattice without coordinates"):
            image.modify(record, subtract=False)
        np.testing.assert_array_equal(image.getchunk(), 0.0)
        assert image.modify(record, region=plane, subtract=False)
        edited = image.getchunk()
        assert edited[3, 3, 0, 0] == pytest.approx(expected)
        assert (edited != 0).sum() == 1
        assert image.done()


def test_polygon_bounding_section_and_verbosity(casa, caplog):
    actual, expected = tools.image(), casa.image()
    for image in (actual, expected):
        assert image.fromshape(shape=[7, 7])
        assert image.setbrightnessunit("Jy/pixel")
    coordinate = expected.coordsys()
    coordinates = coordinate.torecord()
    assert coordinate.done()
    regionmanager = casa.regionmanager()
    triangle = regionmanager.fromtext(
        "#CRTFv0\npoly[[0pix,0pix],[6pix,0pix],[0pix,5pix]]",
        [7, 7],
        coordinates,
    )
    union = regionmanager.fromtext(
        "#CRTFv0\nbox[[0pix,0pix],[1pix,1pix]]\nbox[[5pix,5pix],[6pix,6pix]]",
        [7, 7],
        coordinates,
    )
    record = model(casa)
    for image in (actual, expected):
        with pytest.raises(RuntimeError, match="Only a single n-dimensional rectangular region"):
            image.modify(record, region=union)
        assert image.dohistory(False)
        history = image.history()
        with caplog.at_level("INFO", logger="casars.tools.image"):
            assert image.modify(record, region=triangle, subtract=False, list=True)
        assert image.getchunk()[3, 3] == 2.0
        assert image.history() == history
        assert image.done()
    assert "Selected bounding box [0, 0] to [6, 5]" in caplog.text


def test_rejected_component_records_and_image_types(casa):
    for module in (tools, casa):
        image = module.image()
        assert image.modify({}) is False
        assert image.fromshape(shape=[7, 7], type="c")
        with pytest.raises(RuntimeError, match="only supports float valued images"):
            image.modify({})
        assert image.done()
        assert image.fromshape(shape=[7, 7])
        with pytest.raises(RuntimeError, match="no components in the model componentlist"):
            image.modify({})
        with pytest.raises(RuntimeError, match="invalid componentlist record"):
            image.modify({"nelements": 1, "component0": {"shape": 3}})
        assert image.done()
        assert image.fromshape(shape=[7, 7], linear=True)
        with pytest.raises(RuntimeError, match="does not have required coordinate Direction"):
            image.modify(model(casa))
        assert image.done()


def test_fits_edit_is_rejected_without_changing_data(casa, tmp_path):
    path = str(tmp_path / "input.fits")
    writer = casa.image()
    assert writer.fromshape(shape=[7, 7])
    assert writer.tofits(path)
    assert writer.done()
    original = (tmp_path / "input.fits").read_bytes()
    record = model(casa)
    for module in (tools, casa):
        image = module.image()
        assert image.open(path)
        history = image.history()
        with pytest.raises(RuntimeError, match=r"Failed AlwaysAssert to\.isWritable"):
            image.modify(record)
        np.testing.assert_array_equal(image.getchunk(), 0.0)
        assert image.history() == history
        assert image.done()
    assert (tmp_path / "input.fits").read_bytes() == original
