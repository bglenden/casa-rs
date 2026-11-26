"""CASA 6.7.6.14 public tool and native-image subimage interoperability."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().startswith("6.7.6-14")
    return module


@pytest.mark.parametrize("source_casa,copy_casa", [(False, False), (False, True), (True, False), (True, True)])
def test_subimage_native_round_trip(casa, tmp_path, source_casa, copy_casa):
    source_factory = casa.image if source_casa else tools.image
    copy_factory = casa.image if copy_casa else tools.image
    source = str(tmp_path / "source.im")
    target = str(tmp_path / "target.im")
    pixels = np.arange(20, dtype=np.float32).reshape(4, 5, 1)
    original = source_factory()
    original.fromarray(source, pixels, linear=True)
    original.setbrightnessunit("Jy/beam")
    original.setmiscinfo({"testfield": 17})
    original.sethistory("test", ["parent marker"])
    original.done()

    region = casa.regionmanager().box([1, 1, 0], [3, 4, 0])
    reader = copy_factory()
    assert reader.open(source)
    before = reader.history()
    child = reader.subimage(target, region, dropdeg=True, list=False)
    assert child is not None
    assert child.name() == target
    np.testing.assert_array_equal(child.getchunk(), pixels[1:, 1:, 0])
    np.testing.assert_array_equal(child.coordsys().referencepixel()["numeric"], [1, 1])
    assert child.brightnessunit() == "Jy/beam"
    assert child.miscinfo()["testfield"] == 17
    assert child.history()[:-2] == before
    assert "Ran ia.subimage" == child.history()[-2]
    assert child.history()[-1].startswith("ia.subimage(")
    np.testing.assert_array_equal(reader.getchunk(), pixels)
    assert reader.history() == before
    child.done()
    reader.done()

    reopened = source_factory()
    assert reopened.open(target)
    np.testing.assert_array_equal(reopened.getchunk(), pixels[1:, 1:, 0])
    assert reopened.history()[-2] == "Ran ia.subimage"
    reopened.done()


def test_subimage_mask_and_preserved_degenerate_axis(casa, tmp_path):
    source = str(tmp_path / "mask.im")
    values = np.arange(20, dtype=np.float32).reshape(4, 5, 1)
    writer = casa.image()
    writer.fromarray(source, values)
    writer.calcmask(f'"{source}" < 18', name="input_valid", asdefault=True)
    writer.done()

    region = 'box[[1pix, 1pix], [3pix, 3pix]]'
    children = []
    for factory in (casa.image, tools.image):
        parent = factory()
        assert parent.open(source)
        parent.dohistory(False)
        child = parent.subimage(
            region=region,
            mask=f'"{source}" > 8',
            dropdeg=True,
            keepaxes=[2],
            list=False,
        )
        children.append((child.getchunk(), child.getchunk(getmask=True), child.history()))
        child.done()
        parent.done()
    np.testing.assert_array_equal(children[0][0], values[1:4, 1:4, :])
    np.testing.assert_array_equal(children[0][0], children[1][0])
    np.testing.assert_array_equal(children[0][1], children[1][1])
    np.testing.assert_array_equal(children[1][1], (values[1:4, 1:4, :] > 8) & (values[1:4, 1:4, :] < 18))
    assert children[0][2] == children[1][2]


def test_subimage_numeric_lattice_mask_expression(casa, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mask_path = tmp_path / "mask.im"
    mask_pixels = np.array([-2, 0, 0.1, 0.49, 0.5, 0.6, 1, 4], dtype=np.float32).reshape(2, 4)
    writer = casa.image()
    assert writer.fromarray(str(mask_path), mask_pixels)
    writer.done()

    for factory in (casa.image, tools.image):
        source = factory()
        assert source.fromshape(shape=[2, 4])
        for expression in ("mask.im", f'"{mask_path}"', "mask.im * 1"):
            child = source.subimage(mask=expression, list=False)
            np.testing.assert_array_equal(child.getchunk(getmask=True), mask_pixels >= 0.5)
            child.done()
        with pytest.raises(RuntimeError):
            source.subimage(mask="1.0", list=False)
        source.done()


def test_subimage_publish_only_and_failed_overwrite(tmp_path):
    source = str(tmp_path / "in.im")
    target = str(tmp_path / "out.im")
    writer = tools.image()
    assert writer.fromshape(source, [3, 4, 1])
    for outfile in ("", target):
        detached_tool = writer.subimage(outfile=outfile, wantreturn=False)
        assert not detached_tool.isopen() and detached_tool.done()
        assert writer.isopen()
    with pytest.raises(RuntimeError, match="already exists|exists"):
        writer.subimage(outfile=target)
    with pytest.raises(RuntimeError, match="keepaxes"):
        writer.subimage(outfile=target, dropdeg=True, keepaxes=[3], overwrite=True)
    with pytest.raises(RuntimeError, match="matches or contains input"):
        writer.subimage(outfile=source, overwrite=True)
    reader = tools.image()
    assert reader.open(target)
    assert list(reader.shape()) == [3, 4, 1]
    np.testing.assert_array_equal(reader.getchunk(), np.zeros((3, 4, 1)))
    reader.done()
    writer.done()


def test_subimage_cannot_overwrite_its_source_history_table(tmp_path):
    source = tmp_path / "source.im"
    writer = tools.image()
    assert writer.fromshape(str(source), [3, 4])
    assert writer.sethistory("test", ["keep the source history"])
    before = writer.history()
    alias = tmp_path / "source-alias.im"
    alias.symlink_to(source, target_is_directory=True)
    for target in (source / "logtable", alias / "logtable"):
        with pytest.raises(RuntimeError, match="inside input image"):
            writer.subimage(str(target), overwrite=True, list=False)
        assert writer.history() == before
    writer.done()


def test_subimage_can_create_a_new_image_inside_its_source(casa, tmp_path):
    for factory, name in ((casa.image, "casa.im"), (tools.image, "rust.im")):
        source = tmp_path / name
        parent = factory()
        assert parent.fromshape(str(source), [3, 4])
        before = parent.history()
        child = parent.subimage(str(source / "new-image"), list=False)
        assert list(child.shape()) == [3, 4]
        assert parent.history() == before
        child.done()
        parent.done()


@pytest.mark.parametrize("pixeltype", ["f", "d", "c", "cd"])
def test_subimage_preserves_all_pixel_types(casa, pixeltype):
    pixels = np.arange(12, dtype=np.float64).reshape(3, 4, 1)
    if pixeltype.startswith("c"):
        pixels = pixels + 1j * (12 - pixels)
    region = casa.regionmanager().box([0, 1, 0], [2, 3, 0])
    expected = []
    for factory in (casa.image, tools.image):
        parent = factory()
        assert parent.fromshape(shape=[3, 4, 1], linear=True, type=pixeltype)
        assert parent.putchunk(pixels)
        child = parent.subimage(region=region, dropdeg=True, list=False)
        expected.append((child.pixeltype(), child.getchunk()))
        child.done()
        parent.done()
    assert expected[0][0] == expected[1][0]
    np.testing.assert_array_equal(expected[1][1], pixels[:, 1:, 0])
    np.testing.assert_array_equal(expected[0][1], expected[1][1])


def test_subimage_crops_stokes_and_per_plane_beams(casa):
    region = casa.regionmanager().box([0, 0, 1, 0], [2, 3, 1, 1])
    for factory in (casa.image, tools.image):
        parent = factory()
        assert parent.fromshape(shape=[3, 4, 3, 2])
        # A nonidentical beam in the selected Stokes plane distinguishes
        # copying the selected plane from reusing the full input beam set.
        assert parent.setrestoringbeam("5arcsec", "4arcsec", "0deg", channel=0, polarization=0)
        assert parent.setrestoringbeam(
            "3arcsec", "2arcsec", "0deg", channel=0, polarization=1,
        )
        child = parent.subimage(region=region, list=False)
        assert list(child.shape()) == [3, 4, 1, 2]
        assert list(child.coordsys().stokes()) == ["Q"]
        major = child.restoringbeam(channel=0, polarization=0)["major"]
        assert major["unit"] == "arcsec"
        assert major["value"] == pytest.approx(3.0)
        child.done()
        parent.done()


@pytest.mark.parametrize("selection", ["default", "partial", "center", "reference", "fractional", "clip"])
def test_subimage_resolves_lcslicer_against_parent_axes(casa, selection):
    pixels = np.arange(8 * 10 * 2, dtype=np.float32).reshape(8, 10, 2)
    box = casa.regionmanager().box
    if selection == "default":
        region, view = box(), pixels
    elif selection == "partial":
        region, view = box([1, 1], [3, 3]), pixels[1:4, 1:4, :]
    elif selection == "center":
        region, view = box([1, 1, 0], [2, 2, 0], absrel="relcen"), pixels[5:7, 6:8, 1:]
    elif selection == "reference":
        region, view = box([1, 1], [2, 2], absrel="relref"), pixels[5:7, 6:8, :]
    elif selection == "fractional":
        region, view = box([0, 0], [1, 1]), pixels[2:6, 2:7, :]
        region["blc"] = np.array([0.25, 0.2], dtype=np.float32)
        region["trc"] = np.array([0.75, 0.7], dtype=np.float32)
        region["fracblc"] = np.array([True, True])
        region["fractrc"] = np.array([True, True])
    else:
        region, view = box([1, 1], [20, 20]), pixels[1:, 1:, :]
    for factory in (casa.image, tools.image):
        source = factory()
        assert source.fromarray("", pixels, linear=True)
        result = source.subimage(region=region, list=False)
        np.testing.assert_array_equal(result.getchunk(), view)
        result.done()
        source.done()
