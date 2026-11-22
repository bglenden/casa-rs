"""Saved extension and concatenation regions through image edits and products."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.fixture(params=["extension", "concatenation"])
def saved_region(casa, request):
    shape = (15, 17, 1, 5)
    data = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    selection = np.zeros(shape, dtype=bool)
    reference = casa.image()
    manager = casa.regionmanager()
    coordinates = None
    try:
        assert reference.fromarray(pixels=data)
        coordinates = reference.coordsys()
        csys = coordinates.torecord()
        if request.param == "extension":
            region = manager.fromtext(
                "circle[[8pix,8pix], 3pix], range=[1pix,2pix], corr=[I]",
                list(shape), csys,
            )
            assert region["name"] == "WCExtension"
            window = np.s_[5:12, 5:12, :, 1:3]
            x, y = np.ogrid[:15, :17]
            circle = (x - 8) ** 2 + (y - 8) ** 2 <= 9
            selection[:, :, 0, 1:3] = circle[:, :, None]
        else:
            first = manager.wbox(["3pix", "4pix"], ["5pix", "6pix"], [0, 1], csys)
            second = manager.wbox(["8pix", "9pix"], ["10pix", "11pix"], [0, 1], csys)
            channels = manager.wbox(["1pix"], ["2pix"], [3], csys)
            region = manager.concatenation(channels, {"0": first, "1": second})
            assert region["name"] == "WCConcatenation"
            # CASA's factory encodes the first member twice. Supply the
            # distinct second member to exercise channel-dependent selection.
            region["regions"]["*2"] = second
            window = np.s_[3:11, 4:12, :, 1:3]
            selection[3:6, 4:7, :, 1] = True
            selection[8:11, 9:12, :, 2] = True
        start = [axis.start or 0 for axis in window]
        direction = "J2000 " + " ".join(coordinates.toworld(start, "s")["string"][:2])
        assert coordinates.setunits(["rad", "rad", "", "Hz"])
        origin = coordinates.toworld(start)["numeric"]
        return data, region, window, selection, origin, direction
    finally:
        if coordinates is not None:
            coordinates.done()
        manager.done()
        reference.done()


@pytest.mark.parametrize("method", ["subimage", "makecomplex", "tofits"])
def test_saved_region_products_preserve_masks_and_coordinates(casa, tmp_path, saved_region, method):
    data, region, window, selection, origin, _ = saved_region
    for writer in (casa, tools):
        source = writer.image()
        prefix = tmp_path / writer.__name__
        product = str(prefix) + (".fits" if method == "tofits" else ".im")
        try:
            assert source.fromarray(pixels=data)
            if method == "subimage":
                created = source.subimage(product, region)
                assert created.done()
            elif method == "makecomplex":
                imaginary = str(prefix) + "-imag.im"
                other = writer.image()
                try:
                    assert other.fromarray(imaginary, np.full(data.shape, -2.0, dtype=np.float32))
                finally:
                    other.done()
                assert source.makecomplex(product, imaginary, region=region)
            else:
                assert source.tofits(product, region=region, stokeslast=False)
        finally:
            source.done()
        for reader in (casa, tools):
            reopened = reader.image()
            coordinates = None
            try:
                assert reopened.open(product)
                mask = selection[window]
                expected = data[window]
                if method == "makecomplex":
                    expected = expected - 2j
                np.testing.assert_array_equal(reopened.getchunk(getmask=True), mask)
                np.testing.assert_array_equal(reopened.getchunk()[mask], expected[mask])
                coordinates = reopened.coordsys()
                assert coordinates.setunits(["rad", "rad", "", "Hz"])
                np.testing.assert_allclose(
                    coordinates.toworld([0, 0, 0, 0])["numeric"], origin, rtol=1e-8, atol=1e-7,
                )
            finally:
                if coordinates is not None:
                    coordinates.done()
                reopened.done()


@pytest.mark.parametrize(
    "method", ["fft", "decompose", "deviation", "rebin", "regrid", "rotate", "twopointcorrelation"],
)
def test_analysis_rejects_saved_compound_regions_with_mixed_precision_mask(
    casa, tmp_path, saved_region, method,
):
    data, region, _, _, _, _ = saved_region
    primary, secondary = (tmp_path / name for name in ("single.im", "double.im"))
    writer = casa.image()
    try:
        assert writer.fromarray(str(primary), data)
        assert writer.done()
        weights = np.indices(data.shape).sum(axis=0).astype(np.float64) * 0.5
        assert writer.fromarray(str(secondary), weights, type="d")
        assert writer.done()
    finally:
        writer.done()
    expression = f'("{primary}" + "{secondary}") > 650'
    for backend in (casa, tools):
        image = backend.image()
        output = tmp_path / f"{backend.__name__}-output.im"
        try:
            assert image.open(str(primary))
            if method == "regrid":
                coordinates = image.coordsys()
                try:
                    csys = coordinates.torecord()
                finally:
                    coordinates.done()
                options = {"outfile": str(output), "shape": list(data.shape), "csys": csys,
                           "axes": [0, 1], "decimate": 1}
            else:
                options = {
                    "fft": {"complex": str(output), "axes": [0, 1]},
                    "decompose": {"simple": True, "fit": False, "threshold": 650.},
                    "deviation": {"outfile": str(output), "grid": [2, 2], "xlength": "3pix",
                                  "ylength": "3pix", "interp": "nearest", "stattype": "mean"},
                    "rebin": {"outfile": str(output), "bin": [2, 2, 1, 1]},
                    "rotate": {"outfile": str(output), "pa": "0deg", "method": "nearest"},
                    "twopointcorrelation": {"outfile": str(output), "axes": [0, 1]},
                }[method]
            with pytest.raises(RuntimeError, match="Only a single n-dimensional rectangular region"):
                getattr(image, method)(region=region, mask=expression, **options)
            assert image.isopen()
            assert not output.exists()
            np.testing.assert_array_equal(image.getchunk(), data)
        finally:
            image.done()


@pytest.mark.parametrize("method", ["set", "addnoise", "insert", "modify", "replacemaskedpixels"])
def test_saved_region_edits_honor_membership_bounds_or_rejection(casa, tmp_path, saved_region, method):
    data, region, window, selection, _, direction = saved_region
    insert = str(tmp_path / "insert.im")
    if method == "insert":
        writer = casa.image()
        try:
            assert writer.fromarray(insert, np.full(data.shape, -2.0, dtype=np.float32))
        finally:
            writer.done()
    for api in (casa, tools):
        image = api.image()
        components = None
        try:
            path = str(tmp_path / f"{api.__name__}.im")
            assert image.fromarray(path, data)
            if method == "set":
                assert image.set(pixels="-2", region=region)
                np.testing.assert_array_equal(image.getchunk(), np.where(selection, -2, data))
            elif method == "addnoise":
                assert image.addnoise(region=region, seeds=[19, 31])
                changed = np.zeros(data.shape, dtype=bool)
                changed[window] = True
                np.testing.assert_array_equal(image.getchunk() != data, changed)
            elif method == "insert":
                assert image.insert(infile=insert, region=region, locate=[0.0] * 4)
                expected = data.copy()
                expected[tuple(slice(0, size) for size in data[window].shape)] = -2
                np.testing.assert_array_equal(image.getchunk(), expected)
            else:
                if method == "modify":
                    assert image.setbrightnessunit("Jy/pixel")
                    components = casa.componentlist()
                    assert components.addcomponent(dir=direction, flux=1.0)
                else:
                    assert image.calcmask("F", "invalid")
                assert image.done()
                assert image.open(path)
                missing_members = {key: value for key, value in region.items() if key != "regions"}
                cases = (
                    (region, "Only a single n-dimensional rectangular region"),
                    (missing_members, "RecordInterface: field regions is unknown"),
                )
                for value, message in cases:
                    np.testing.assert_array_equal(image.haslock(), [True, False])
                    with pytest.raises(RuntimeError, match=message):
                        if method == "modify":
                            image.modify(components.torecord(), region=value, subtract=False, list=False)
                        else:
                            image.replacemaskedpixels(pixels="-2", region=value)
                    np.testing.assert_array_equal(image.haslock(), [True, False])
                np.testing.assert_array_equal(image.getchunk(), data)
                np.testing.assert_array_equal(image.getchunk(getmask=True), method == "modify")
        finally:
            if components is not None:
                components.done()
            image.done()
