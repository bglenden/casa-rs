"""Image coordinate queries retain selected axes in CASA coordinate-group order."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.fixture
def source_record(casa):
    cs = casa.coordsys().newcoordsys(direction=True, stokes=["I", "Q"], spectral=True)
    try:
        assert cs.setunits(["deg", "deg", "", "Hz"])
        assert cs.setreferencevalue([41.0, 22.0, 1.0, 1.403e9])
        assert cs.setreferencepixel([7.0, 8.0, 0.0, 2.0])
        assert cs.setincrement([-0.2, 0.15, 1.0, 1000.0])
        assert cs.setobserver("Source observer")
        return cs.torecord()
    finally:
        cs.done()


@pytest.mark.parametrize("requested, retained, kinds, maps", [
    ([0], [0], ["Direction"], [[0, -2]]),
    ([1], [1], ["Direction"], [[-1, 0]]),
    ([0, 3], [0, 3], ["Direction", "Spectral"], [[0, -2], [1]]),
    ([3, 0], [3, 0], ["Spectral", "Direction"], [[0], [1, -3]]),
    ([3, 1, 0], [3, 0, 1], ["Spectral", "Direction"], [[0], [1, 2]]),
    ([3, 2], [3, 2], ["Spectral", "Stokes"], [[0], [1]]),
    ([0, 0], [0], ["Direction"], [[0, -2]]),
])
def test_image_coordsys_selects_pixel_axes_with_signed_maps(
    casa, source_record, tmp_path, requested, retained, kinds, maps,
):
    reference_pixel = np.array([7.0, 8.0, 0.0, 2.0])
    reference_world = np.array([41.0, 22.0, 1.0, 1.403e9])
    names = np.array(["Right Ascension", "Declination", "Stokes", "Frequency"])
    source_shape = np.array([17, 19, 2, 5])
    for writer in (casa, tools):
        path = str(tmp_path / f"{writer.__name__}-input.im")
        output = writer.image()
        try:
            assert output.fromshape(path, source_shape.tolist(), source_record)
        finally:
            output.done()
        for reader in (casa, tools):
            image = reader.image()
            selected = None
            original = None
            try:
                assert image.open(path)
                selected = image.coordsys(axes=requested)
                assert list(selected.coordinatetype()) == kinds
                np.testing.assert_array_equal(selected.names(), names[retained])
                np.testing.assert_allclose(selected.referencevalue()["numeric"], reference_world[retained], atol=1e-10)
                world = selected.toworld(reference_pixel[retained])["numeric"]
                np.testing.assert_allclose(world, reference_world[retained], atol=1e-10, rtol=0)
                np.testing.assert_allclose(selected.topixel(world)["numeric"], reference_pixel[retained], atol=1e-9)
                record = selected.torecord()
                for index, mapping in enumerate(maps):
                    for domain in ("pixel", "world"):
                        np.testing.assert_array_equal(record[f"{domain}map{index}"], mapping)
                assert selected.setobserver("Detached selection")
                original = image.coordsys()
                assert original.observer() == "Source observer"
            finally:
                if original is not None:
                    original.done()
                if selected is not None:
                    selected.done()
                image.done()


@pytest.mark.parametrize("axes, invalid", [([4], 4), ([-1, 3], -1)])
def test_image_coordsys_reports_an_invalid_requested_axis(casa, axes, invalid):
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromshape(shape=[17, 19, 2, 5])
            with pytest.raises(RuntimeError, match=f"zero-based pixel axis {invalid} is not a valid pixel axis"):
                image.coordsys(axes=axes)
            np.testing.assert_array_equal(image.shape(), [17, 19, 2, 5])
        finally:
            image.done()


def test_image_coordsys_requires_a_world_axis_for_each_selected_pixel_axis(casa):
    seed = casa.coordsys().newcoordsys(linear=2)
    try:
        record = seed.torecord()
    finally:
        seed.done()
    record["worldmap0"] = np.array([-1, 0], dtype=np.int32)
    for api in (casa, tools):
        image = api.image()
        selected = None
        try:
            assert image.fromshape(shape=[5, 7], csys=record)
            with pytest.raises(RuntimeError, match="World axis corresponding to zero-based pixel axis 0 has been removed"):
                image.coordsys(axes=[0])
            selected = image.coordsys(axes=[1])
            assert list(selected.names()) == ["LinAxis1"]
            np.testing.assert_array_equal(selected.toworld([4.0])["numeric"], [4.0])
        finally:
            if selected is not None:
                selected.done()
            image.done()


def test_reordered_partial_coordinate_record_survives_native_image_exchange(casa, source_record, tmp_path):
    for writer in (casa, tools):
        source, product = writer.image(), writer.image()
        selected = None
        name = str(tmp_path / f"{writer.__name__}-selected.im")
        try:
            assert source.fromshape(shape=[17, 19, 2, 5], csys=source_record)
            selected = source.coordsys(axes=[3, 0])
            assert product.fromshape(name, [5, 17], selected.torecord())
        finally:
            if selected is not None:
                selected.done()
            product.done()
            source.done()
        for reader in (casa, tools):
            image = reader.image()
            coords = None
            try:
                assert image.open(name)
                coords = image.coordsys()
                np.testing.assert_array_equal(image.shape(), [5, 17])
                np.testing.assert_array_equal(coords.names(), ["Frequency", "Right Ascension"])
                np.testing.assert_allclose(coords.toworld([2.0, 7.0])["numeric"], [1.403e9, 41.0], atol=1e-10)
                record = coords.torecord()
                np.testing.assert_array_equal(record["pixelmap1"], [1, -3])
                np.testing.assert_array_equal(record["worldmap1"], [1, -3])
            finally:
                if coords is not None:
                    coords.done()
                image.done()
