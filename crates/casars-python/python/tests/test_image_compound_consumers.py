"""Compound regions preserve geometry and the selection rules of each tool."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.fixture(params=["difference", "intersection", "complement"])
def compound(casa, request):
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
        selected = np.zeros(data.shape, dtype=bool)
        if request.param == "difference":
            region = manager.difference(outer, inner)
            window = np.s_[3:11, 4:12]
            selected[window] = True
            selected[5:9, 6:10] = False
        elif request.param == "intersection":
            region = manager.intersection({"0": outer, "1": inner})
            window = np.s_[5:9, 6:10]
            selected[window] = True
        else:
            region = manager.complement(inner)
            window = np.s_[:, :]
            selected[window] = True
            selected[5:9, 6:10] = False
        direction = "J2000 " + " ".join(coordinates.toworld([3, 4], "s")["string"])
        assert coordinates.setunits(["rad", "rad"])
        lower = [axis.start or 0 for axis in window]
        origin = coordinates.toworld(lower)["numeric"]
        return data, region, window, selected, origin, direction
    finally:
        if coordinates is not None:
            coordinates.done()
        manager.done()
        reference.done()


@pytest.mark.parametrize("method", ["subimage", "tofits", "makecomplex"])
def test_compound_products_preserve_pixels_and_masks(casa, tmp_path, compound, method):
    data, region, window, selected, origin, _ = compound
    for api in (casa, tools):
        image = api.image()
        product = None
        coordinates = None
        try:
            assert image.fromarray(pixels=data)
            if method == "subimage":
                product = image.subimage(region=region)
            elif method == "makecomplex":
                imag = str(tmp_path / f"{api.__name__}-imag.im")
                path = str(tmp_path / f"{api.__name__}-complex.im")
                writer = api.image()
                try:
                    assert writer.fromarray(imag, np.full(data.shape, 3.0, dtype=np.float32))
                finally:
                    writer.done()
                assert image.makecomplex(path, imag, region=region)
                product = casa.image()
                assert product.open(path)
            else:
                path = str(tmp_path / f"{api.__name__}.fits")
                assert image.tofits(path, region=region)
                product = casa.image()
                assert product.open(path)
            mask = selected[window]
            expected = data[window]
            if method == "makecomplex":
                expected = expected + 3j
            np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
            np.testing.assert_array_equal(product.getchunk()[mask], expected[mask])
            coordinates = product.coordsys()
            assert coordinates.setunits(["rad", "rad"])
            np.testing.assert_allclose(
                coordinates.toworld([0, 0])["numeric"], origin, rtol=1e-8, atol=1e-7,
            )
        finally:
            if coordinates is not None:
                coordinates.done()
            if product is not None:
                product.done()
            image.done()


@pytest.mark.parametrize("method", ["set", "addnoise", "insert"])
def test_compound_edits_match_membership_or_bounds(casa, tmp_path, compound, method):
    data, region, window, selected, _, _ = compound
    path = str(tmp_path / "input.im")
    if method == "insert":
        writer = casa.image()
        try:
            assert writer.fromarray(path, np.full(data.shape, -7, dtype=np.float32))
        finally:
            writer.done()
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromarray(pixels=data)
            if method == "set":
                assert image.set(pixels="-7", region=region)
                np.testing.assert_array_equal(image.getchunk(), np.where(selected, -7, data))
            elif method == "addnoise":
                assert image.addnoise(region=region, seeds=[19, 31])
                changed = np.zeros(data.shape, dtype=bool)
                changed[window] = True
                np.testing.assert_array_equal(image.getchunk() != data, changed)
            else:
                assert image.insert(infile=path, region=region, locate=[0.0, 0.0])
                expected = data.copy()
                width, height = data[window].shape
                expected[:width, :height] = -7
                np.testing.assert_array_equal(image.getchunk(), expected)
        finally:
            image.done()


@pytest.mark.parametrize("method", ["modify", "replacemaskedpixels"])
def test_compound_rejections_preserve_pixels_mask_and_locks(casa, tmp_path, compound, method):
    data, region, _, _, _, direction = compound
    for api in (casa, tools):
        image = api.image()
        components = None
        path = str(tmp_path / f"{api.__name__}-image.im")
        try:
            assert image.fromarray(path, data)
            if method == "modify":
                assert image.setbrightnessunit("Jy/pixel")
                components = casa.componentlist()
                assert components.addcomponent(dir=direction, flux=1.0)
            else:
                assert image.calcmask("F", "invalid")
            assert image.done()
            assert image.open(path)
            np.testing.assert_array_equal(image.haslock(), [True, False])
            with pytest.raises(RuntimeError, match="Only a single n-dimensional rectangular region"):
                if method == "modify":
                    image.modify(components.torecord(), region=region, subtract=False, list=False)
                else:
                    image.replacemaskedpixels(pixels="-7", region=region)
            np.testing.assert_array_equal(image.haslock(), [True, False])
            np.testing.assert_array_equal(image.getchunk(getmask=True), method == "modify")
            np.testing.assert_array_equal(image.getchunk(), data)
        finally:
            if components is not None:
                components.done()
            image.done()
