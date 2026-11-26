"""World, relative, and nonrectangular regions through the read-only image tools."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.fixture
def selected_regions(casa, tmp_path):
    shape = (21, 23, 1, 3)
    source, manager = casa.image(), casa.regionmanager()
    coords = None
    try:
        assert source.fromshape(shape=list(shape))
        coords = source.coordsys()
        native = coords.torecord()
        text = "box[[4pix,5pix],[10pix,12pix]]"
        polygon = "poly[[4pix,5pix],[10pix,5pix],[4pix,12pix]]"
        saved = tmp_path / "selection.crtf"
        saved.write_text(f"#CRTFv0\n{text}\n")
        outer = manager.wbox(["4pix", "5pix"], ["10pix", "12pix"], [0, 1], native)
        inner = manager.wbox(["6pix", "7pix"], ["9pix", "10pix"], [0, 1], native)
        return {
            "fractional": manager.box([0.2, 0.25], [0.6, 0.7], frac=True),
            "relative": manager.box([-2, -2], [3, 4], absrel="relref"),
            "worldbox": outer,
            "text": text,
            "file": str(saved),
            "polygon": manager.fromtext(polygon, list(shape), native),
            "difference": manager.difference(outer, inner),
        }
    finally:
        if coords is not None:
            coords.done()
        manager.done()
        source.done()


@pytest.mark.parametrize(
    "kind", ["fractional", "relative", "worldbox", "text", "file", "polygon", "difference"],
)
def test_read_region_values_and_statistics_match_the_selected_pixels(
    casa, selected_regions, tmp_path, kind,
):
    x, y, _, frequency = np.ogrid[:21, :23, :1, :3]
    pixels = (x + 10 * y + 100 * frequency).astype(np.float32)
    region = selected_regions[kind]
    start, stop = {
        "fractional": ((4, 6), (12, 15)),
        "relative": ((8, 9), (13, 15)),
    }.get(kind, ((4, 5), (10, 12)))
    raw = pixels[start[0]:stop[0] + 1, start[1]:stop[1] + 1]
    x, y = np.ogrid[start[0]:stop[0] + 1, start[1]:stop[1] + 1]
    if kind == "polygon":
        valid = 7 * (x - 4) + 6 * (y - 5) <= 42
    elif kind == "difference":
        valid = (x < 6) | (x > 9) | (y < 7) | (y > 10)
    else:
        valid = np.ones(raw.shape[:2], dtype=bool)
    mask = np.broadcast_to(valid[..., None, None], raw.shape)
    selected = raw[valid, 0, :]
    for writer in (casa, tools):
        source = writer.image()
        name = str(tmp_path / f"{writer.__name__}-source.im")
        try:
            assert source.fromarray(name, pixels=pixels)
        finally:
            source.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(name)
                np.testing.assert_array_equal(image.getregion(region=region), raw)
                np.testing.assert_array_equal(image.getregion(region=region, getmask=True), mask)
                collapsed = image.getregion(region=region, axes=[0, 1])
                np.testing.assert_allclose(collapsed.reshape(-1), selected.mean(axis=0), rtol=3e-7)
                statistics = image.statistics(region=region, axes=[0, 1])
                np.testing.assert_array_equal(statistics["npts"], np.full(3, valid.sum()))
                np.testing.assert_array_equal(statistics["sum"], selected.sum(axis=0, dtype=np.float64))
                np.testing.assert_array_equal(statistics["blc"], [*start, 0, 0])
                np.testing.assert_array_equal(statistics["trc"], [*stop, 0, 2])
                if kind in {"polygon", "difference"}:
                    expression = f'"{name}" > 80'
                    accepted = mask & (raw > 80)
                    np.testing.assert_array_equal(
                        image.getregion(region=region, mask=expression, getmask=True), accepted,
                    )
                    filtered = image.statistics(region=region, mask=expression, axes=[0, 1])
                    np.testing.assert_array_equal(filtered["npts"], accepted.sum(axis=(0, 1, 2)))
                    np.testing.assert_array_equal(
                        filtered["sum"], np.where(accepted, raw, 0).sum(axis=(0, 1, 2)),
                    )
            finally:
                image.done()


@pytest.mark.parametrize("kind", ["f", "d", "c", "cd"])
def test_getregion_accepts_a_world_box_for_every_pixel_type(casa, selected_regions, kind):
    x, y, _, frequency = np.ogrid[:21, :23, :1, :3]
    pixels = x + 10 * y + 100 * frequency
    if kind.startswith("c"):
        pixels = pixels * (1 - 2j)
    expected = pixels[4:11, 5:13]
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromshape(shape=list(pixels.shape), type=kind)
            assert image.putchunk(pixels)
            result = image.getregion(region=selected_regions["worldbox"])
            assert result.dtype == image.getchunk().dtype
            np.testing.assert_array_equal(result, expected)
        finally:
            image.done()


@pytest.mark.parametrize("kind", ["LCSlicer", "LCBox"])
def test_pixel_region_on_an_image_with_one_direction_axis(casa, kind):
    x, frequency = np.ogrid[:9, :5]
    pixels = (x + 10 * frequency).astype(np.float32)
    if kind == "LCSlicer":
        manager = casa.regionmanager()
        try:
            section = manager.box([1, 1], [7, 4], [2, 2])
        finally:
            manager.done()
        selected = pixels[1:8:2, 1:5:2]
    else:
        section = {
            "name": "LCBox",
            "isRegion": 1,
            "comment": "",
            "oneRel": True,
            "blc": np.array([2, 2], dtype=np.float32),
            "trc": np.array([8, 5], dtype=np.float32),
            "shape": np.array(pixels.shape, dtype=np.int32),
        }
        selected = pixels[1:8, 1:5]
    for api in (casa, tools):
        image = api.image()
        view = None
        try:
            assert image.fromarray(pixels=pixels[:, None, None, :])
            view = image.subimage(dropdeg=True, list=False)
            np.testing.assert_array_equal(view.shape(), [9, 5])
            np.testing.assert_array_equal(view.getregion(region=section), selected)
            np.testing.assert_array_equal(view.getregion(region=section, getmask=True), True)
        finally:
            if view is not None:
                view.done()
            image.done()


def test_lattice_box_must_match_the_image_shape(casa):
    pixels = np.arange(20, dtype=np.float32).reshape(4, 5)
    box = {
        "name": "LCBox",
        "isRegion": 1,
        "comment": "",
        "oneRel": True,
        "blc": np.array([2, 2], dtype=np.float32),
        "trc": np.array([3, 4], dtype=np.float32),
        "shape": np.array([11, 12], dtype=np.int32),
    }
    selected = pixels[1:3, 1:4]
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromarray(pixels=pixels)
            for read in (image.getregion, image.statistics):
                with pytest.raises(RuntimeError, match="mismatches lattice shape in region"):
                    read(region=box)
            matched = dict(box, shape=np.array(pixels.shape, dtype=np.int32))
            np.testing.assert_array_equal(image.getregion(region=matched), selected)
            assert image.statistics(region=matched)["sum"].item() == selected.sum()
        finally:
            image.done()
