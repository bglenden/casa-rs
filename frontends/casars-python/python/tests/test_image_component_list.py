"""Lazily sample temporary component-list images through the public tool."""

import numpy as np
import pytest

from test_image_removal import _backend


def _component_record(api, kind="point", **parameters):
    tool = api.componentlist()
    try:
        tool.addcomponent(
            flux=[2., 0., 0., 0.],
            fluxunit="Jy",
            dir="J2000 0deg 0deg",
            shape=kind,
            freq="1.415GHz",
            **parameters,
        )
        return tool.torecord()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("cache", [False, True])
@pytest.mark.parametrize("shape", [(6, 8), (5, 7), (6, 8, 3), (6, 8, 2, 3)])
def test_temporary_component_list_point(tmp_path, backend, cache, shape):
    api = _backend(backend)
    record = _component_record(api)
    tool = api.image()
    center = tuple(n // 2 for n in shape[:2])
    expected = np.zeros(shape)
    index = center + ((0,) if len(shape) >= 3 else ())
    expected[index] = 2.
    try:
        assert tool.fromcomplist(shape=list(shape), cl=record, log=False, cache=cache)
        assert tool.name() == "Temporary ComponentListImage"
        assert tool.type() == "image" and tool.pixeltype() == "float"
        assert tool.brightnessunit() == "Jy/pixel"
        assert not tool.ispersistent()
        np.testing.assert_array_equal(tool.shape(), shape)
        np.testing.assert_array_equal(tool.haslock(), [True, True])
        # Mutating the caller's record does not change the owned component list.
        record.clear()
        pixels = tool.getchunk()
        assert pixels.dtype == np.float64
        np.testing.assert_array_equal(pixels, expected)
        np.testing.assert_array_equal(tool.getchunk(getmask=True), np.ones(shape, dtype=bool))
        repeated_scale = 2 if backend == "casa" and not cache else 1
        np.testing.assert_array_equal(tool.getchunk(), expected * repeated_scale)
        with pytest.raises(RuntimeError, match="pixel values cannot be modified"):
            tool.putchunk(np.zeros((1, 1), dtype=np.float32), [0] * len(shape))
        assert tool.isopen()
        history = tool.history()
        assert history[0].startswith("Created ComponentListImage")
        assert history[1] == "Ran ia.fromcomplist"
        assert not list(tmp_path.iterdir())
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("cache", [False, True])
def test_temporary_component_list_gaussian(backend, cache):
    api = _backend(backend)
    record = _component_record(
        api, "Gaussian", majoraxis="4arcmin", minoraxis="2arcmin", positionangle="0deg",
    )
    tool = api.image()
    try:
        assert tool.fromcomplist(shape=[16, 18], cl=record, cache=cache)
        pixels = tool.getchunk()
        peak = (2. * 4. * np.log(2.)) / (np.pi * 4. * 2.)
        assert pixels[8, 9] == pytest.approx(peak, rel=1e-6)
        assert pixels[8, 11] == pytest.approx(peak / 2., rel=1e-6)
        assert pixels[9, 9] == pytest.approx(peak / 2., rel=1e-6)
        assert pixels.sum() == pytest.approx(2., rel=1e-5)
        np.testing.assert_array_equal(tool.getchunk(), pixels)
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("shape, error", [
    ([], "must have 2, 3, or 4 dimensions"),
    ([4], "must have 2, 3, or 4 dimensions"),
    ([3, 3, 1, 1, 1], "must have 2, 3, or 4 dimensions"),
    ([0, 4], "shape has to be > 0"),
])
def test_temporary_component_list_rejects_invalid_shape(backend, shape, error):
    api = _backend(backend)
    record = _component_record(api)
    tool = api.image()
    try:
        assert tool.fromshape(shape=[2, 2])
        with pytest.raises(RuntimeError, match=error):
            tool.fromcomplist(shape=shape, cl=record)
        assert not tool.isopen()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_temporary_component_list_coordinate_edits_and_metadata(backend):
    api = _backend(backend)
    record = _component_record(api)
    coords = api.coordsys().newcoordsys(direction=True)
    tool = api.image()
    try:
        assert coords.setreferencepixel([1., 2.])
        assert tool.fromcomplist(shape=[6, 8], cl=record, csys=coords.torecord())
        expected = np.zeros((6, 8))
        expected[1, 2] = 2.
        np.testing.assert_array_equal(tool.getchunk(), expected)
        assert coords.setreferencepixel([3., 4.])
        assert tool.setcoordsys(coords.torecord())
        expected[1, 2] = 0.
        expected[3, 4] = 2.
        np.testing.assert_array_equal(tool.getchunk(), expected)
        assert tool.setmiscinfo({"observer": "test"})
        assert tool.miscinfo() == {"observer": "test"}
        with pytest.raises(RuntimeError, match="units must be Jy/pixel"):
            tool.setbrightnessunit("Jy/beam")
        assert tool.brightnessunit() == "Jy/pixel"
        with pytest.raises(RuntimeError, match="ComponentListImage may not have a beam"):
            tool.setrestoringbeam(major="2arcsec", minor="1arcsec", pa="0deg")
        assert tool.isopen()
        np.testing.assert_array_equal(tool.getchunk(), expected)
    finally:
        coords.done()
        tool.done()


@pytest.mark.parametrize("cache", [False, True])
def test_temporary_component_list_samples_only_a_selected_section(cache):
    api = _backend("casars")
    record = _component_record(api)
    tool = api.image()
    try:
        assert tool.fromcomplist(shape=[50_000, 50_000], cl=record, cache=cache)
        expected = np.zeros((3, 3))
        expected[1, 1] = 2.
        for _ in range(2):
            pixels = tool.getchunk(blc=[24_998, 24_998], trc=[25_002, 25_002], inc=[2, 2])
            np.testing.assert_array_equal(pixels, expected)
    finally:
        tool.done()


@pytest.mark.parametrize(
    ("backend", "index"),
    [
        pytest.param("casa", [-3., 0., 0., 0.], id="casa-four-indices"),
        pytest.param("casars", [-3., 0., 0., 0.], id="casars-four-indices"),
        pytest.param("casars", -3., id="casars-scalar-index"),
    ],
)
def test_temporary_component_list_samples_spectral_index_in_stokes_planes(backend, index):
    api = _backend(backend)
    components = api.componentlist()
    tool = api.image()
    try:
        components.addcomponent(
            flux=[2., 1., 0., 0.], fluxunit="Jy", dir="J2000 0deg 0deg",
            shape="point", freq="1.415GHz", spectrumtype="spectral index", index=index,
        )
        assert tool.fromcomplist(shape=[6, 8, 2, 3], cl=components.torecord())
        frequencies = np.array([1_414_999_000., 1_415_000_000., 1_415_001_000.])
        expected = np.zeros((6, 8, 2, 3))
        expected[3, 4] = np.array([[2.], [1.]]) * (frequencies / 1_415_000_000.) ** -3.
        np.testing.assert_allclose(tool.getchunk(), expected, rtol=1e-7)
    finally:
        tool.done()
        components.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("geometry", ["rotated", "outside_horizon"])
def test_point_component_is_assigned_to_one_pixel(backend, geometry):
    api = _backend(backend)
    coords = api.coordsys().newcoordsys(direction=True)
    tool = api.image()
    try:
        if geometry == "rotated":
            assert coords.setreferencepixel([3.48, 4.])
            assert coords.setlineartransform("direction", [[0.8, -0.6], [0.6, 0.8]])
        else:
            assert coords.setreferencepixel([3., 4.])
            assert coords.setunits(["deg", "deg"])
            assert coords.setincrement([-20., 20.])
        assert tool.fromcomplist(shape=[7, 9], csys=coords.torecord(), cl=_component_record(api))
        expected = np.zeros((7, 9))
        expected[3, 4] = 2.
        np.testing.assert_array_equal(tool.getchunk(), expected)
    finally:
        coords.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("spectrum", ["constant", "spectral index"])
def test_component_list_mixed_frequency_frames(backend, spectrum):
    api = _backend(backend)
    source = api.componentlist()
    tool = api.image()
    try:
        for _ in range(2):
            source.addcomponent(
                flux=[2., 0., 0., 0.], fluxunit="Jy", dir="J2000 0deg 0deg",
                shape="point", freq="1.415GHz", spectrumtype=spectrum, index=-2.,
            )
        record = source.torecord()
        record["component1"]["spectrum"]["frequency"]["refer"] = "TOPO"
        assert tool.fromcomplist(shape=[4, 5], cl=record)
        if spectrum == "constant":
            expected = np.zeros((4, 5))
            expected[2, 2] = 4.
            np.testing.assert_array_equal(tool.getchunk(), expected)
        else:
            with pytest.raises(RuntimeError, match="Cannot transform frequency from LSRK to TOPO"):
                tool.getchunk()
        assert tool.isopen()
    finally:
        tool.done()
        source.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_empty_component_list_with_spectral_coordinate(backend):
    api = _backend(backend)
    tool = api.image()
    try:
        assert tool.fromcomplist(shape=[4, 5, 6], cl={"nelements": 0})
        np.testing.assert_array_equal(tool.getchunk(), np.zeros((4, 5, 6)))
        assert tool.isopen()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_component_list_checks_stokes_labels_beyond_image_shape(backend):
    api = _backend(backend)
    coords = api.coordsys().newcoordsys(direction=True, stokes=["I", "RR"])
    tool = api.image()
    try:
        with pytest.raises(RuntimeError, match="Unsupported stokes type RR"):
            tool.fromcomplist(shape=[7, 9, 1], csys=coords.torecord(), cl=_component_record(api))
        assert not tool.isopen()
    finally:
        tool.done()
        coords.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_temporary_component_list_rejects_creation_of_named_mask(tmp_path, backend):
    api = _backend(backend)
    tool, operand = api.image(), api.image()
    path = tmp_path / "mask-input.im"
    try:
        assert operand.fromarray(str(path), np.ones((6, 8)))
        assert operand.done()
        assert tool.fromcomplist(shape=[6, 8], cl=_component_record(api))
        with pytest.raises(RuntimeError, match="Cannot make requested mask.*Temporary ComponentListImage"):
            tool.calcmask(f'"{path}" > 0', "valid")
        assert tool.isopen()
        assert tool.getchunk(getmask=True).all()
    finally:
        operand.done()
        tool.done()
