"""World region operations select the same pixels in copy, edit and export."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("operation", ["difference", "intersection", "complement"])
@pytest.mark.parametrize("method", ["subimage", "set", "tofits"])
def test_compound_region_preserves_geometry_across_tools(casa, tmp_path, method, operation):
    data = np.arange(15 * 17, dtype=np.float32).reshape(15, 17)
    reference = casa.image()
    manager = casa.regionmanager()
    coordinates = None
    try:
        assert reference.fromarray(pixels=data)
        coordinates = reference.coordsys()
        record = coordinates.torecord()
        outer = manager.wbox(["3pix", "4pix"], ["10pix", "11pix"], [0, 1], record)
        inner = manager.wbox(["5pix", "6pix"], ["8pix", "9pix"], [0, 1], record)
        if operation == "difference":
            region = manager.difference(outer, inner)
            window = np.s_[3:11, 4:12]
            selected = np.zeros(data.shape, dtype=bool)
            selected[window] = True
            selected[5:9, 6:10] = False
        elif operation == "intersection":
            region = manager.intersection({"0": outer, "1": inner})
            window = np.s_[5:9, 6:10]
            selected = np.zeros(data.shape, dtype=bool)
            selected[window] = True
        else:
            region = manager.complement(inner)
            window = np.s_[:, :]
            selected = np.ones(data.shape, dtype=bool)
            selected[5:9, 6:10] = False
        lower = [axis.start or 0 for axis in window]
        assert coordinates.setunits(['rad', 'rad'])
        origin = coordinates.toworld(lower)["numeric"]
    finally:
        if coordinates is not None:
            coordinates.done()
        manager.done()
        reference.done()

    for api in (casa, tools):
        image = api.image()
        product = None
        coordinates = None
        try:
            assert image.fromarray(pixels=data)
            if method == "set":
                assert image.set(pixels="-7", region=region)
                np.testing.assert_array_equal(image.getchunk(), np.where(selected, -7, data))
                continue
            if method == "subimage":
                product = image.subimage(region=region)
            else:
                path = str(tmp_path / f"{api.__name__}.fits")
                assert image.tofits(path, region=region)
                product = casa.image()
                assert product.open(path)
            mask = selected[window]
            np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
            np.testing.assert_array_equal(product.getchunk()[mask], data[window][mask])
            coordinates = product.coordsys()
            assert coordinates.setunits(['rad', 'rad'])
            np.testing.assert_allclose(
                coordinates.toworld([0, 0])["numeric"], origin, rtol=1e-8, atol=1e-7,
            )
        finally:
            if coordinates is not None:
                coordinates.done()
            if product is not None:
                product.done()
            image.done()
