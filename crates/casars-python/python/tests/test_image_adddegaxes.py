"""Public image.adddegaxes behavior and CASA 6.7.6.14 image interchange."""

import os
import select
import subprocess
import sys

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_adddegaxes_copies_beams_from_its_open_view(casa, tmp_path):
    for backend in (casa, tools):
        source = str(tmp_path / f"{backend.__name__}.im")
        writer, reader = backend.image(), backend.image()
        assert writer.fromshape(source, [3, 4])
        assert writer.setrestoringbeam("4arcsec", "2arcsec", "12deg")
        assert reader.open(source)
        expected = reader.restoringbeam()
        assert writer.setrestoringbeam("8arcsec", "4arcsec", "27deg")
        assert reader.unlock()
        result = reader.adddegaxes(spectral=True)
        assert result.restoringbeam() == expected
        np.testing.assert_array_equal(reader.haslock(), [True, False])
        assert result.done() and reader.done() and writer.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_adddegaxes_reads_a_lazy_operand(casa, tmp_path):
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    for backend in (casa, tools):
        path = str(tmp_path / f"{backend.__name__}.im")
        source = backend.image()
        assert source.fromarray(path, pixels)
        assert source.calcmask(f'"{path}" > 4', name="valid")
        assert source.done()
        lazy = source.imagecalc(pixels=f'"{path}" + 1')
        assert lazy.unlock()
        if backend is tools:
            with pytest.raises(RuntimeError, match="matches or contains input image"):
                lazy.adddegaxes(path, spectral=True, overwrite=True)
        result = lazy.adddegaxes(spectral=True)
        np.testing.assert_array_equal(lazy.haslock(), [True, False])
        np.testing.assert_array_equal(result.getchunk(), pixels[..., None] + 1)
        assert result.getchunk(getmask=True).all()
        assert result.done() and lazy.done()


@pytest.mark.parametrize(
    "linear,kw,added",
    [
        (True, {"direction": True}, 2),
        (True, {"spectral": True}, 1),
        (True, {"stokes": "q"}, 1),
        (True, {"stokes": "QU"}, 1),
        (True, {"tabular": True}, 1),
        (False, {"linear": True}, 1),
        (True, {"direction": True, "spectral": True, "stokes": "XX", "tabular": True}, 5),
        (False, {"direction": True, "linear": True, "silent": True}, 1),
        (False, {"direction": True, "silent": True}, 0),
    ],
)
def test_adddegaxes_coordinate_records(casa, linear, kw, added):
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    results = []
    for factory in (casa.image, tools.image):
        parent = factory()
        assert parent.fromarray(pixels=pixels, linear=linear)
        child = parent.adddegaxes(**kw)
        assert child.isopen()
        assert not child.ispersistent()
        values = child.getchunk()
        np.testing.assert_array_equal(values, pixels.reshape((3, 2) + (1,) * added))
        source_record = parent.coordsys().torecord()
        record = child.coordsys().torecord()
        for name in ("direction0", "linear0", "worldmap0", "pixelmap0"):
            if name in source_record:
                _assert_value(record[name], source_record[name], name)
        results.append((record, values.dtype, child.history()[-2:]))
        child.done()
        np.testing.assert_array_equal(parent.getchunk(), pixels)
        parent.done()

    oracle, actual = results
    assert actual[1:] == oracle[1:]
    coordinate_keys = [
        name for name in oracle[0] if name.rstrip("0123456789") != name
    ]
    for name in coordinate_keys:
        _assert_value(actual[0][name], oracle[0][name], name)


@pytest.mark.parametrize("code,pixeltype,dtype", [
    ("f", "float", np.float32), ("d", "double", np.float64),
    ("c", "complex", np.complex64), ("cd", "dcomplex", np.complex128),
])
def test_adddegaxes_preserves_pixel_types(code, pixeltype, dtype):
    pixels = np.arange(1, 7, dtype=np.float64).reshape(3, 2) + 2**-40
    if code in ("c", "cd"):
        pixels = pixels + (2 * pixels - 2**-42) * 1j
    pixels = pixels.astype(dtype)
    source = tools.image()
    assert source.fromshape(shape=pixels.shape, type=code)
    assert source.putchunk(pixels)
    child = source.adddegaxes("", False, False, "Q", False, False, False, False)
    assert child.pixeltype() == pixeltype
    np.testing.assert_array_equal(child.getchunk(), pixels[..., None])
    assert child.coordsys().stokes() == ["Q"]
    assert child.done()
    assert source.done()


def test_adddegaxes_publishes_a_native_copy_with_all_masks(casa, tmp_path):
    source = str(tmp_path / "source.im")
    target = str(tmp_path / "target.im")
    pixels = np.arange(20, dtype=np.float32).reshape(5, 4)
    writer = casa.image()
    assert writer.fromarray(source, pixels)
    assert writer.calcmask(f'"{source}" < 17', name="valid", asdefault=True)
    assert writer.calcmask(f'"{source}" > 3', name="other", asdefault=False)
    assert writer.setbrightnessunit("Jy/beam")
    assert writer.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="20deg")
    beam = writer.restoringbeam()
    assert writer.setmiscinfo({"provenance": 42})
    assert writer.sethistory("test", ["input history"])
    writer.done()

    reader = tools.image()
    assert reader.open(source)
    reader.dohistory(False)
    assert reader.adddegaxes(target, spectral=True, stokes="V").done()
    with pytest.raises(RuntimeError):
        reader.adddegaxes(source, linear=True, overwrite=True)
    np.testing.assert_array_equal(reader.getchunk(), pixels)
    reader.done()
    reader = casa.image()
    assert reader.open(target)
    assert reader.ispersistent()
    assert reader.brightnessunit() == "Jy/beam"
    _assert_value(reader.restoringbeam(), beam)
    assert reader.miscinfo() == {"provenance": 42}
    assert reader.history()[-1] == "input history"
    np.testing.assert_array_equal(reader.getchunk(), pixels[..., None, None])
    assert set(reader.maskhandler("get")) == {"valid", "other"}
    assert reader.maskhandler("default") == ["valid"]
    np.testing.assert_array_equal(reader.getchunk(getmask=True), (pixels < 17)[..., None, None])
    assert reader.maskhandler("set", ["other"])
    np.testing.assert_array_equal(reader.getchunk(getmask=True), (pixels > 3)[..., None, None])
    reader.done()


def test_adddegaxes_rejections_and_detached_lifecycle():
    assert not tools.image().adddegaxes().isopen()
    source = tools.image()
    assert source.fromshape(shape=[3, 2])
    with pytest.raises(RuntimeError, match="No degenerate axes specified"):
        source.adddegaxes()
    with pytest.raises(RuntimeError, match="already contains a DirectionCoordinate"):
        source.adddegaxes(direction=True)
    assert source.isopen()
    assert source.done()


def test_adddegaxes_checks_existing_output_before_new_axes(casa, tmp_path):
    target = tmp_path / "existing"
    target.write_text("preserved", encoding="utf-8")
    for factory in (casa.image, tools.image):
        source = factory()
        assert source.fromshape(shape=[3, 2])
        with pytest.raises(RuntimeError, match="exists, and the user does not want to remove it"):
            source.adddegaxes(str(target))
        assert source.done()
    assert target.read_text(encoding="utf-8") == "preserved"


@pytest.mark.parametrize("persistent", [False, True])
def test_adddegaxes_copies_pixels_and_masks_across_block_boundaries(tmp_path, persistent):
    shape = (1025, 1025)
    pixels = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    source = tools.image()
    assert source.fromarray(pixels=pixels)
    assert source.calcmask("T", name="boundary")
    output = str(tmp_path / "large.im") if persistent else ""
    child = source.adddegaxes(output, spectral=True)
    np.testing.assert_array_equal(child.getchunk()[..., 0], pixels)
    assert child.maskhandler("default") == ["boundary"]
    np.testing.assert_array_equal(child.getchunk(getmask=True)[..., 0], np.ones(shape, dtype=bool))
    assert child.done()
    assert source.done()


@pytest.mark.parametrize("persistent", [False, True])
def test_adddegaxes_from_direct_fits(casa, tmp_path, persistent):
    source = tmp_path / "source.fits"
    content = _hdu(True, [1, np.nan, 3, 4], bitpix=-32)
    source.write_bytes(content)
    products = []
    for module in (casa, tools):
        parent = module.image()
        assert parent.open(str(source))
        output = str(tmp_path / f"{module.__name__}.im") if persistent else ""
        child = parent.adddegaxes(output, stokes="V")
        assert child.ispersistent() == persistent
        assert child.coordsys().stokes() == ["V"]
        products.append((child.getchunk(), child.getchunk(getmask=True)))
        child.done()
        parent.done()

    expected = np.array([[1, 3], [np.nan, 4]], dtype=np.float64)[..., None]
    np.testing.assert_array_equal(products[0][0], expected)
    _assert_value(products[1][0], products[0][0])
    np.testing.assert_array_equal(products[1][1], np.ones(expected.shape, dtype=bool))
    _assert_value(products[1][1], products[0][1])
    assert source.read_bytes() == content


def test_adddegaxes_preserves_an_older_coordinate_record():
    source = tools.image()
    assert source.fromshape(shape=[3, 2], linear=True)
    record = source.coordsys().torecord()
    record["coordinate0"] = record.pop("linear0")
    record["coordinate0"]["coordinate_type"] = "Linear"
    assert source.setcoordsys(record)
    child = source.adddegaxes(spectral=True)
    np.testing.assert_array_equal(child.shape(), [3, 2, 1])
    assert child.coordsys().axiscoordinatetypes() == ["Linear", "Linear", "Spectral"]
    assert child.done()
    assert source.done()


def test_adddegaxes_after_rejected_coordinate_record(tmp_path):
    target = tmp_path / "copy.im"
    source = tools.image()
    assert source.fromshape(shape=[3, 2])
    record = source.coordsys().torecord()
    original = source.coordsys().torecord()
    record["quality1"] = {"quality": ["DATA"]}
    # Quality coordinates are supported since #1870; this incomplete record
    # is rejected when installed, before adddegaxes ever sees it.
    with pytest.raises(RuntimeError, match="Missing Quality axis name"):
        source.setcoordsys(record)
    assert not target.exists()
    _assert_value(source.coordsys().torecord(), original)
    child = source.adddegaxes(str(target), spectral=True)
    np.testing.assert_array_equal(child.getchunk(), np.zeros((3, 2, 1), dtype=np.float32))
    assert child.done()
    np.testing.assert_array_equal(source.shape(), [3, 2])
    assert source.done()


def test_adddegaxes_rejects_unread_coordinate_fields(tmp_path):
    target = tmp_path / "copy.im"
    source = tools.image()
    assert source.fromshape(shape=[3, 2], linear=True)
    record = source.coordsys().torecord()
    record["coordinate0"] = record.pop("linear0")
    record["coordinate0"]["coordinate_type"] = "Linear"
    # The modern coordinate entry takes precedence when reading the record;
    # adddegaxes must still refuse the additional, unread legacy coordinate.
    record["quality1"] = {"axes": ["Quality"], "quality": ["DATA"]}
    assert source.setcoordsys(record)
    original = source.coordsys().torecord()
    with pytest.raises(RuntimeError, match="Image coordinates do not describe all pixel axes"):
        source.adddegaxes(str(target), spectral=True)
    assert not target.exists()
    _assert_value(source.coordsys().torecord(), original)
    np.testing.assert_array_equal(source.getchunk(), np.zeros((3, 2), dtype=np.float32))
    assert source.done()


@pytest.mark.parametrize("keyword,match", [
    ("masks", "Invalid mask name"), ("ATTRGROUPS", "attribute groups.*#1625"),
])
def test_adddegaxes_rejects_unsupported_mask_or_attribute_metadata(tmp_path, keyword, match):
    source = str(tmp_path / "source.im")
    target = tmp_path / "target.im"
    image = tools.image()
    assert image.fromshape(source, [3, 2])
    assert image.calcmask("T", name="valid")
    assert image.done()
    table = tools.table()
    assert table.open(source, nomodify=False)
    if keyword == "masks":
        value = {"nested/name": table.getkeyword("masks")["valid"]}
    else:
        value = {"example": {"units": "Jy"}}
    table.putkeyword(keyword, value)
    assert table.done()
    assert image.open(source)
    with pytest.raises(RuntimeError, match=match):
        image.adddegaxes(str(target), spectral=True)
    assert not target.exists()
    assert image.done()


@pytest.mark.parametrize("external", [False, True])
def test_adddegaxes_cannot_overwrite_an_open_table(tmp_path, external):
    target = str(tmp_path / "target.im")
    writer = tools.image()
    pixels = np.full((3, 2), 17.0, dtype=np.float32)
    assert writer.fromarray(target, pixels)
    assert writer.done()
    assert writer.fromshape(shape=[3, 2])
    holder = None
    table = None
    try:
        if external:
            holder = subprocess.Popen(
                [sys.executable, "-u", "-c", """
import sys
import numpy as np
from casars import tools
image = tools.image()
assert image.open(sys.argv[1])
print('READY', flush=True)
sys.stdin.readline()
np.testing.assert_array_equal(image.getchunk(), np.full((3, 2), 17.0))
assert image.done()
""", target], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
            )
            assert select.select([holder.stdout], [], [], 15)[0]
            assert holder.stdout.readline().strip() == "READY"
        else:
            table = tools.table()
            assert table.open(target)
        # Pinned CASA reports an already-open table for same-process holders.
        message = "still open in another process" if external else "already opened"
        with pytest.raises(RuntimeError, match=message):
            writer.adddegaxes(target, spectral=True, overwrite=True)
        reader = tools.image()
        assert reader.open(target)
        np.testing.assert_array_equal(reader.getchunk(), pixels)
        assert reader.done()
        if table is not None:
            np.testing.assert_array_equal(table.getcell("map", 0), pixels)
    finally:
        if holder is not None:
            _, stderr = holder.communicate("\n", timeout=15)
            assert holder.returncode == 0, stderr
        if table is not None:
            assert table.done()
    assert writer.adddegaxes(target, spectral=True, overwrite=True).done()
    assert writer.done()
