"""Pixel and world conversions through the public image tool."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa_image():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module.image


def assert_record(actual, reference):
    assert actual.keys() == reference.keys()
    for field, value in reference.items():
        item = actual[field]
        if isinstance(value, dict):
            assert_record(item, value)
        elif isinstance(value, np.ndarray) and value.dtype.kind == "f":
            assert item.dtype == value.dtype
            np.testing.assert_allclose(item, value, rtol=2e-8, atol=1e-10)
        elif isinstance(value, np.ndarray):
            assert item.dtype.kind == value.dtype.kind
            np.testing.assert_array_equal(item, value)
        elif isinstance(value, float):
            assert isinstance(item, float)
            assert item == pytest.approx(value, rel=2e-8, abs=1e-10)
        else:
            assert item == value


def test_detached_coordinate_conversions():
    image = tools.image()
    assert image.topixel() == {}
    assert image.toworld() == {}
    assert image.coordmeasures() == {}


@pytest.mark.parametrize("pixel_type", ["f", "d", "c", "cd"])
@pytest.mark.parametrize("pixel", [[2, 3, 1, 4], [8, 8, 1, 8]])
def test_toworld_formats_and_inverse(casa_image, pixel_type, pixel):
    reference = casa_image()
    image = tools.image()
    try:
        for tool in [reference, image]:
            assert tool.fromshape(shape=[10, 12, 3, 12], type=pixel_type)
        expected = reference.toworld(pixel, format="nQsM")
        actual = image.toworld(pixel, format="nQsM")
        assert_record(actual, expected)
        for key in expected:
            direct = expected[key] if key in {"numeric", "string"} else {key: expected[key]}
            for value in [direct, {key: actual[key]}]:
                converted = image.topixel(value)
                assert converted.keys() == {"ar_type", "pw_type", "numeric"}
                assert converted["ar_type"] == "absolute"
                assert converted["pw_type"] == "pixel"
                np.testing.assert_allclose(converted["numeric"], pixel, rtol=0, atol=2e-7)
        assert_record(
            image.toworld(pixel, format="m", dovelocity=False),
            reference.toworld(pixel, format="m", dovelocity=False),
        )
    finally:
        image.done()
        reference.done()


@pytest.mark.parametrize(
    "position",
    [
        None, [], np.array([], dtype=np.float64), [2], [2, 3, 1, 4, 99], [[1, 2], [3, 4]],
        {"numeric": [2., 3.]}, {"numeric": np.array([8, 8], dtype=np.float32)},
    ],
)
def test_toworld_reference_fill_and_record_forms(casa_image, position):
    reference = casa_image()
    image = tools.image()
    try:
        for tool in [reference, image]:
            assert tool.fromshape(shape=[10, 12, 3, 12])
        assert_record(image.toworld(value=position), reference.toworld(value=position))
        assert image.toworld(position, format="") == {}
    finally:
        image.done()
        reference.done()


def test_linear_world_supplied_in_quantities_and_strings(casa_image, tmp_path):
    path = str(tmp_path / "linear.im")
    reference = casa_image()
    image = tools.image()
    try:
        assert reference.fromshape(path, [10, 12], linear=True)
        coords = reference.coordsys()
        assert coords.setunits(["m", "s"], type="linear", overwrite=True)
        assert coords.setreferencevalue([10., 3.])
        assert coords.setincrement([2., -0.5])
        assert reference.setcoordsys(coords.torecord())
        assert coords.done()
        assert reference.done()
        assert image.open(path)
        assert reference.open(path)
        for value in [[12., 2.], ["0.012km", "2000ms"], "0.012km 2000ms"]:
            assert_record(image.topixel(value), reference.topixel(value))
        quantities = reference.toworld([2.5, 1.5], "q")["quantity"]
        quantities["*2"]["value"] = [quantities["*2"]["value"]]
        reversed_quantities = dict(reversed(quantities.items()))
        for value in [
            {"quantity": reversed_quantities},
            {"measure": {"linear": reversed_quantities}},
        ]:
            assert_record(image.topixel(value), reference.topixel(value))
            np.testing.assert_allclose(image.topixel(value)["numeric"], [2.5, 1.5])
        assert_record(image.toworld([2.5, 1.5], "nqsm"), reference.toworld([2.5, 1.5], "nqsm"))
    finally:
        image.done()
        reference.done()


def test_toworld_galactic_conversion_layer(casa_image):
    reference = casa_image()
    image = tools.image()
    try:
        for tool in [reference, image]:
            assert tool.fromshape(shape=[10, 12, 3, 12])
        coords = reference.coordsys()
        assert coords.setconversiontype(direction="GALACTIC")
        for tool in [reference, image]:
            assert tool.setcoordsys(coords.torecord())
        assert coords.done()
        pixel = [5, 6, 0, 6]
        actual = image.toworld(pixel, "nqm")
        expected = reference.toworld(pixel, "nqm")
        assert_record(actual, expected)
        # The inverse celestial frame transform is accurate to better
        # than a millionth of a pixel in both tools for this WCS.
        for tool, world in [(image, actual), (reference, expected)]:
            np.testing.assert_allclose(tool.topixel(world)["numeric"], pixel, rtol=0, atol=1e-6)
        assert image.toworld(pixel, "s")["string"][0].count(":") == 2
    finally:
        image.done()
        reference.done()


def test_coordinate_conversion_errors_and_reopen(casa_image, tmp_path):
    path = str(tmp_path / "world.fits")
    producer = casa_image()
    assert producer.fromshape(shape=[9, 8, 3])
    assert producer.tofits(path)
    assert producer.done()
    for image_type in [casa_image, tools.image]:
        image = image_type()
        try:
            assert image.open(path)
            with pytest.raises(RuntimeError, match="Unsupported data type"):
                image.toworld(2)
            with pytest.raises(RuntimeError, match="incorrect data type"):
                image.toworld({"numeric": [2, 3]})
            with pytest.raises(RuntimeError, match="relative, not absolute"):
                image.topixel({"numeric": [0., 0.], "ar_type": "relative"})
            position = image.toworld([1.5, 2.5, 2])
            np.testing.assert_allclose(image.topixel(position)["numeric"], [1.5, 2.5, 2], atol=2e-7)
            assert image.coordmeasures([1.5, 2.5, 2])["mask"] is True
        finally:
            image.done()


def test_toworld_rejects_multidimensional_numeric_record():
    image = tools.image()
    try:
        assert image.fromshape(shape=[4, 4])
        for convert in [image.toworld, image.topixel]:
            with pytest.raises(RuntimeError, match="numeric record must be one-dimensional"):
                convert({"numeric": np.array([[1., 2.], [3., 4.]])})
        assert image.toworld([1, 2])["numeric"].shape == (2,)
    finally:
        image.done()


@pytest.mark.parametrize("complex_pixels", [False, True])
def test_coordmeasures_intensity_at_fractional_pixel_and_outside(casa_image, tmp_path, complex_pixels):
    reference = casa_image()
    image = tools.image()
    path = str(tmp_path / "intensity.im")
    data = np.arange(5 * 6 * 2 * 6, dtype=np.float32).reshape(5, 6, 2, 6)
    if complex_pixels:
        data = data.astype(np.complex64) + 2j
    try:
        assert reference.fromarray(path, data)
        assert reference.setbrightnessunit("Jy/pixel")
        if not complex_pixels:
            assert reference.calcmask(f'"{path}">150')
        assert reference.done()
        assert reference.open(path)
        assert image.open(path)
        for args in [(), ([-1],), ([2.9, 3.4],), ([-0.8, 1],), ([-2, 1],), ([10, 1],)]:
            assert_record(image.coordmeasures(*args), reference.coordmeasures(*args))
        assert image.coordmeasures([2.9, 3.4, 1.3, 2.7])["intensity"]["value"] == data[2, 3, 1, 2]
        assert "mask" not in image.coordmeasures([10, 1])
    finally:
        image.done()
        reference.done()


def test_coordmeasures_selects_direction_and_spectral_frames(casa_image):
    reference = casa_image()
    image = tools.image()
    try:
        for tool in [reference, image]:
            assert tool.fromshape(shape=[5, 6, 2, 6])
        pixel = [2.5, 3.1, 1.25, 3.4]
        for direction, frequency in [
            ("GALACTIC", "BARY"), ("native", "native"), ("cl", "LSRK"), ("J20", "BAR"),
        ]:
            assert_record(
                image.coordmeasures(pixel, dframe=direction, sframe=frequency),
                reference.coordmeasures(pixel, dframe=direction, sframe=frequency),
            )
        for tool in [reference, image]:
            with pytest.raises(RuntimeError, match="Unknown direction reference frame"):
                tool.coordmeasures(pixel, dframe="bogus")
            with pytest.raises(RuntimeError, match="Unknown frequency reference frame"):
                tool.coordmeasures(pixel, sframe="bogus")
    finally:
        image.done()
        reference.done()


@pytest.mark.parametrize("shape", [[1, 8, 6], [8, 1, 6]])
def test_direction_measure_on_a_slice_with_a_dropped_axis(casa_image, tmp_path, shape):
    image = tools.image()
    try:
        assert image.fromshape(shape=shape)
        path = str(tmp_path / "sliced.im")
        rust_slice = image.subimage(path, dropdeg=True)
        ref_slice = casa_image()
        try:
            assert ref_slice.open(path)
            pixel = [2, 1]
            world = ref_slice.toworld(pixel, "nqsm")
            assert_record(rust_slice.toworld(pixel, "nqsm"), world)
            assert_record(rust_slice.coordmeasures(pixel), ref_slice.coordmeasures(pixel))
            # The raw measure inverse in CASA writes outside its vector when
            # a direction axis was removed. Verify the surviving pixel here.
            for representation in world:
                converted = rust_slice.topixel({representation: world[representation]})
                np.testing.assert_allclose(converted["numeric"], pixel, atol=2e-7)
        finally:
            rust_slice.done()
            ref_slice.done()
    finally:
        image.done()


def test_single_coordinate_variants_and_direction_string_units(casa_image):
    reference = casa_image()
    image = tools.image()
    try:
        for tool in [reference, image]:
            assert tool.fromshape(shape=[6, 8])
            with pytest.raises(RuntimeError, match="Failed to convert string formatted world direction"):
                tool.topixel(["1m", "0d"])
        for value in [
            np.ones((2, 2), dtype=np.float16), np.ones((2, 2), dtype=object),
            {"numeric": [0., 0.], "ar_type": 1}, ["0d", "0d"], "0 rad",
            np.array([[b"0rad", b"0rad"]]), np.array("0rad"),
        ]:
            assert_record(image.topixel(value), reference.topixel(value))
        for value in [
            np.array(1.), np.array(1, dtype=np.int32), np.ones((2, 2), dtype=np.float16),
        ]:
            assert_record(image.toworld(value), reference.toworld(value))
    finally:
        image.done()
        reference.done()


def test_tabular_world_quantities_and_measures(casa_image):
    from casatools import coordsys

    coords = coordsys().newcoordsys(tabular=True)
    reference = casa_image()
    image = tools.image()
    try:
        assert coords.settabular([0., 1., 2., 3.], [0., 2., 6., 12.])
        assert coords.setunits(["m"], type="tabular", overwrite=True)
        record = coords.torecord()
        for tool in [reference, image]:
            assert tool.fromshape(shape=[4], csys=record)
        assert_record(image.toworld([1.5], "nqsm"), reference.toworld([1.5], "nqsm"))
        value = {"measure": {"tabular": {"unit": "m", "value": 4.0}}}
        assert_record(image.topixel(value), reference.topixel(value))
        for tool in [reference, image]:
            with pytest.raises(RuntimeError, match="Illegal Quantum record"):
                tool.topixel(tool.toworld([1.5], "m"))
    finally:
        image.done()
        reference.done()
        coords.done()


@pytest.mark.parametrize("frequency", ["CMB", "LGROUP", "BARY"])
def test_coordmeasures_frequency_uses_the_physical_direction(frequency):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    reference = casa.image()
    image = tools.image()
    measures = casa.measures()
    try:
        for source in (reference, image):
            assert source.fromshape(shape=[20, 20, 20])
            coords = source.coordsys()
            try:
                assert coords.setconversiontype(direction="B1950", spectral="CMB")
                assert source.setcoordsys(coords.torecord())
            finally:
                coords.done()

        native = image.coordmeasures(dframe="native", sframe="native")["measure"]
        assert native["direction"]["refer"] == "J2000"
        assert native["direction"]["m0"]["value"] == 0
        assert native["direction"]["m1"]["value"] == 0
        lsrk = native["spectral"]["frequency"]
        assert lsrk["refer"] == "LSRK"
        assert lsrk["m0"] == {"value": 1.415e9, "unit": "Hz"}

        assert measures.doframe(measures.direction("J2000", "0rad", "0rad"))
        physical = measures.measure(lsrk, frequency)
        casa_report = reference.coordmeasures(dframe="cl", sframe=frequency)["measure"]
        angles = casa_report["direction"]
        assert angles["refer"] == "B1950"
        assert measures.doframe(measures.direction("J2000", angles["m0"], angles["m1"]))
        mislabeled = measures.measure(lsrk, frequency)
        assert casa_report["spectral"]["frequency"]["m0"]["value"] == pytest.approx(
            mislabeled["m0"]["value"], abs=1e-3, rel=0
        )
        assert abs(mislabeled["m0"]["value"] - physical["m0"]["value"]) > 500

        for direction, frame in (("cl", "B1950"), ("native", "J2000"),
                                 ("GALACTIC", "GALACTIC")):
            actual = image.coordmeasures(dframe=direction, sframe=frequency)["measure"]
            measured = actual["spectral"]["frequency"]
            assert actual["direction"]["refer"] == frame
            assert measured["refer"] == frequency
            assert measured["m0"]["unit"] == physical["m0"]["unit"] == "Hz"
            assert measured["m0"]["value"] == pytest.approx(physical["m0"]["value"], abs=1e-3, rel=0)
    finally:
        measures.done()
        reference.done()
        image.done()
