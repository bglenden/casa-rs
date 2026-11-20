"""Pinned CASA coverage for native and lazy image calculator products."""

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def product_sources(tmp_path, monkeypatch):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    monkeypatch.chdir(tmp_path)
    pixels = np.arange(1, 13, dtype=np.float32).reshape(3, 4)
    source = casa.image()
    assert source.fromarray("real.im", pixels)
    assert source.calcmask('"real.im">6', name="selected")
    assert source.setbrightnessunit("Jy/beam")
    assert source.setmiscinfo({"object": "expression source"})
    assert source.setrestoringbeam(major="4arcmin", minor="3arcmin", pa="15deg")
    real_beam = source.restoringbeam()
    assert source.tofits("real.fits")
    assert source.done()
    assert source.fromarray("complex.im", pixels.astype(np.complex64) + 2j)
    assert source.calcmask('real("complex.im")>6', name="selected")
    assert source.done()
    assert source.fromarray("metadata.im", np.zeros_like(pixels))
    assert source.setbrightnessunit("K")
    assert source.setmiscinfo({"object": "metadata source"})
    coordinates = source.coordsys()
    assert coordinates.setreferencepixel([5, 4])
    assert source.setcoordsys(coordinates.torecord())
    assert coordinates.done()
    assert source.done()
    return casa, pixels, real_beam


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
@pytest.mark.parametrize("named_mask", [False, True])
def test_imagecalc_lazy_releases_and_reacquires_native_input_lock(product_sources, named_mask):
    casa, _, _ = product_sources
    writer = """
import sys
from casatools import image
from numpy import full
source = image()
assert source.open(sys.argv[1])
acquired = source.lock(writelock=True, nattempts=1)
assert acquired == (sys.argv[2] == "write")
if acquired:
    assert source.putchunk(full((3, 4), 7.0))
assert source.done()
"""
    for backend in (casa, tools):
        name = f"{backend.__name__}.im"
        factory = backend.image()
        assert factory.fromshape(name, [3, 4])
        assert factory.calcmask("T", name="selected")
        assert factory.done()
        operand = f"{name}:selected" if named_mask else name
        result = factory.imagecalc(pixels=f'"{operand}" + 1')
        try:
            np.testing.assert_array_equal(result.haslock(), [True, False])
            subprocess.run([sys.executable, "-c", writer, name, "blocked"],
                           check=True, capture_output=True, timeout=10)
            assert result.unlock()
            np.testing.assert_array_equal(result.haslock(), [False, False])
            subprocess.run([sys.executable, "-c", writer, name, "write"],
                           check=True, capture_output=True, timeout=10)
            np.testing.assert_array_equal(result.getchunk(), np.full((3, 4), 8.0))
            np.testing.assert_array_equal(result.haslock(), [True, False])
            assert result.lock(writelock=True, nattempts=1)
            np.testing.assert_array_equal(result.haslock(), [True, True])
        finally:
            assert result.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_lazy_imagecalc_metadata_preserves_native_input_lock_state(product_sources):
    casa, _, _ = product_sources
    for backend in (casa, tools):
        name = backend.__name__
        source, creator = backend.image(), backend.image()
        assert source.fromshape(f"{name}.im", [3, 4])
        result = creator.imagecalc(pixels=f'"{name}.im" + 1')
        try:
            assert result.unlock()
            assert result.setmiscinfo({"source": "expression"})
            assert result.setbrightnessunit("Jy")
            assert result.setrestoringbeam("3arcsec", "2arcsec", "12deg")
            assert result.summary(list=False)["unit"] == "Jy"
            assert result.pixelvalue([99, 1]) == {}
            with pytest.raises(RuntimeError, match="ImageExpr is not writable"):
                result.putchunk(np.zeros((3, 4)))
            np.testing.assert_array_equal(result.haslock(), [False, False])
            np.testing.assert_array_equal(source.haslock(), [False, False])

            assert result.pixelvalue([1, 1])["value"]["value"] == 1
            np.testing.assert_array_equal(result.haslock(), [True, False])
            np.testing.assert_array_equal(source.haslock(), [True, False])
            assert result.unlock()
            assert result.subimage().done()
            np.testing.assert_array_equal(result.haslock(), [True, False])
            assert result.unlock()
            assert result.torecord()
            np.testing.assert_array_equal(result.haslock(), [True, False])
        finally:
            assert result.done()
            assert source.done()


@pytest.mark.parametrize(
    "source,prec,kind", [("real", "FLOAT", "float"), ("real", "d", "double"),
                         ("complex", "f", "complex"), ("complex", "DoUbLe", "dcomplex")],
)
def test_imagecalc_native_types_masks_history_and_casa_readback(product_sources, source, prec, kind):
    casa, pixels, real_beam = product_sources
    expected = pixels * 2 if source == "real" else (pixels.astype(np.complex64) + 2j) * 2
    expression = f'"{source}.im" * 2'
    for backend, name, reader in [(casa, "reference.im", tools), (tools, "rust.im", casa)]:
        factory = backend.image()
        image = factory.imagecalc(name, expression, prec=prec)
        try:
            assert not factory.isopen()
            assert Path(name).is_dir()
            if kind in ("float", "complex"):
                assert image.ispersistent()
            assert image.pixeltype() == kind
            np.testing.assert_array_equal(image.getchunk(), expected)
            np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 6)
            assert image.maskhandler("default") == ["mask0"]
            if source == "real":
                assert image.brightnessunit() == "Jy/beam"
                assert image.restoringbeam() == real_beam
                if kind == "float":
                    assert image.miscinfo() == {"object": "expression source"}
            assert image.history(list=False)[-2:] == [
                "Ran ia.imagecalc",
                f'ia.imagecalc(outfile="{name}", pixels="{expression}", overwrite=false, imagemd="")',
            ]
        finally:
            image.done()
            factory.done()
        opened = reader.image()
        try:
            assert opened.open(name)
            assert opened.pixeltype() == kind
            np.testing.assert_array_equal(opened.getchunk(), expected)
            np.testing.assert_array_equal(opened.getchunk(getmask=True), pixels > 6)
        finally:
            opened.done()


def test_imagecalc_lazy_reads_live_sections_and_rejects_pixel_writes(product_sources):
    casa, pixels, _ = product_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        writer = backend.image()
        assert writer.fromarray(name, pixels)
        assert writer.calcmask(f'"{name}">6')
        assert writer.done()
        factory = backend.image()
        result = factory.imagecalc(pixels=f'"{name}" * 2')
        try:
            assert not result.ispersistent()
            assert result.name() == ""
            assert result.maskhandler("get") == ["T"]
            assert result.maskhandler("default") == [""]
            np.testing.assert_array_equal(result.getchunk([0, 0], [2, 3], [2, 2]), (pixels * 2)[::2, ::2])
            np.testing.assert_array_equal(result.getchunk(getmask=True), pixels > 6)
            assert result.summary(list=False)["hasmask"]
            assert result.setmiscinfo({"object": "lazy view"})
            assert result.miscinfo() == {"object": "lazy view"}
            assert result.setbrightnessunit("Jy")
            assert result.brightnessunit() == "Jy"
            with pytest.raises(RuntimeError, match="ImageExpr is not writable"):
                result.putchunk(np.zeros_like(pixels))
            with pytest.raises(RuntimeError, match=r"to\.isWritable\(\)"):
                result.calc("2", False)
            with pytest.raises(RuntimeError, match="Cannot make requested mask.*ImageExpr"):
                result.calcmask("T")
            assert writer.open(name)
            assert writer.putchunk(np.full_like(pixels, 5))
            assert writer.done()
            np.testing.assert_array_equal(result.getchunk(), 10)
            np.testing.assert_array_equal(result.getchunk(getmask=True), pixels > 6)
        finally:
            result.done()
            factory.done()


def test_imagecalc_lazy_result_retains_native_inputs_until_closed(product_sources):
    casa, pixels, _ = product_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        writer = backend.image()
        assert writer.fromarray(name, pixels)
        assert writer.done()
        factory = backend.image()
        result = factory.imagecalc(pixels=f'"{name}" * 2')
        try:
            assert writer.open(name)
            with pytest.raises(RuntimeError, match="still open"):
                writer.remove()
            if backend is tools:
                assert writer.open(name)
                with pytest.raises(RuntimeError, match="still open"):
                    writer.rename("moved.im")
                assert not Path("moved.im").exists()
            assert Path(name).is_dir()
            np.testing.assert_array_equal(result.getchunk(), pixels * 2)
        finally:
            result.done()
            writer.done()
            factory.done()
        assert writer.open(name)
        assert writer.remove()
        assert not Path(name).exists()
        writer.done()


@pytest.mark.parametrize("read_before_move", [False, True])
def test_imagecalc_lazy_fits_reader_keeps_its_open_source(product_sources, read_before_move):
    casa, pixels, _ = product_sources
    expected = np.where(pixels > 6, pixels * 2, np.nan)
    for backend, name in [(casa, "reference.fits"), (tools, "rust.fits")]:
        path = Path(name)
        path.write_bytes(Path("real.fits").read_bytes())
        factory = backend.image()
        image = factory.imagecalc(pixels=f'"{name}" * 2')
        try:
            if read_before_move:
                np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 6)
            path.rename(path.with_suffix(".moved"))
            if read_before_move:
                np.testing.assert_array_equal(image.getchunk(), expected)
                np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 6)
            else:
                with pytest.raises(RuntimeError):
                    image.getchunk()
        finally:
            image.done()
            factory.done()


@pytest.mark.parametrize("extension", ["im", "fits"])
def test_imagecalc_resolves_relative_operands_at_construction(product_sources, monkeypatch, extension):
    casa, _, _ = product_sources
    start = Path.cwd()
    for dirname, value in [("original", 2), ("other", 21)]:
        directory = start / dirname
        directory.mkdir()
        monkeypatch.chdir(directory)
        writer = casa.image()
        assert writer.fromarray("input.im", np.full((2, 2), value, dtype=np.float32))
        assert writer.setbrightnessunit("K" if value == 2 else "Jy/beam")
        assert writer.tofits("input.fits")
        assert writer.done()
    for backend in (casa, tools):
        monkeypatch.chdir(start / "original")
        factory = backend.image()
        image = factory.imagecalc(pixels=f'"input.{extension}" * 3')
        try:
            monkeypatch.chdir(start / "other")
            expected = 63 if backend is casa and extension == "fits" else 6
            np.testing.assert_array_equal(image.getchunk(), expected)
            assert image.brightnessunit() == "K"
        finally:
            image.done()
            factory.done()


@pytest.mark.parametrize("persistent", [False, True])
def test_imagecalc_applies_imagemd_and_honors_history_switch(product_sources, persistent):
    casa, pixels, _ = product_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        assert factory.dohistory(False)
        image = factory.imagecalc(name if persistent else "", '"real.fits" * 2', imagemd="metadata.im")
        try:
            assert image.brightnessunit() == "K"
            assert image.miscinfo() == {"object": "metadata source"}
            coordinates = image.coordsys()
            np.testing.assert_array_equal(coordinates.referencepixel()["numeric"], [5, 4])
            coordinates.done()
            assert "Ran ia.imagecalc" not in image.history(list=False)
            np.testing.assert_array_equal(image.getchunk(), np.where(pixels > 6, pixels * 2, np.nan))
            np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 6)
        finally:
            image.done()
            factory.done()


def test_imagecalc_rejects_invalid_expressions_precision_and_alias(product_sources):
    casa, pixels, _ = product_sources
    for backend in (casa, tools):
        factory = backend.image()
        try:
            for expression, message in [
                ("", "provide an expression"), ("2", "have no coordinates"),
                ("[1,2]", "have no coordinates"), ("T", "have no coordinates"),
                ('"real.im">0', "Bool expression cannot be converted"),
            ]:
                with pytest.raises(RuntimeError, match=message):
                    factory.imagecalc(pixels=expression)
            with pytest.raises(RuntimeError, match="Unsupported value for type"):
                factory.imagecalc(pixels='"real.im"', prec="complex")
            with pytest.raises(RuntimeError, match="Caught TableError"):
                factory.imagecalc("real.im", '"real.im" + 1', overwrite=True)
            with pytest.raises(RuntimeError, match="File real.im exists"):
                factory.imagecalc("real.im", '"real.im" + 1')
            with pytest.raises(RuntimeError, match="File real.im exists"):
                factory.imagecalc("real.im", "2")
            assert factory.open("real.im")
            np.testing.assert_array_equal(factory.getchunk(), pixels)
        finally:
            factory.done()


def test_imagecalc_reads_the_new_products_metadata_when_imagemd_is_the_output(product_sources):
    casa, pixels, _ = product_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        assert factory.fromarray(name, pixels + 100)
        assert factory.setbrightnessunit("K")
        assert factory.setmiscinfo({"prior": "metadata"})
        assert factory.done()
        image = factory.imagecalc(name, '"real.im" * 2', overwrite=True, imagemd=name)
        try:
            assert image.brightnessunit() == ""
            assert image.miscinfo() == {}
            np.testing.assert_array_equal(image.getchunk(), pixels * 2)
            np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 6)
        finally:
            image.done()
            factory.done()


def test_imagecalc_new_output_can_supply_its_own_metadata_by_absolute_path(product_sources):
    casa, pixels, _ = product_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        image = factory.imagecalc(name, '"real.im" * 2', imagemd=str(Path(name).absolute()))
        try:
            assert image.brightnessunit() == ""
            assert image.miscinfo() == {}
            np.testing.assert_array_equal(image.getchunk(), pixels * 2)
        finally:
            image.done()
            factory.done()


@pytest.mark.parametrize("persistent", [False, True])
def test_imagecalc_conditional_keeps_predicate_coordinates(product_sources, persistent):
    casa, pixels, beam = product_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        image = factory.imagecalc(name if persistent else "", 'iif("real.im">6,1,0)')
        try:
            assert image.brightnessunit() == "Jy/beam"
            assert image.miscinfo() == {"object": "expression source"}
            assert image.restoringbeam() == beam
            coordinates = image.coordsys()
            np.testing.assert_array_equal(coordinates.referencepixel()["numeric"], [1, 2])
            coordinates.done()
            np.testing.assert_array_equal(image.getchunk(), pixels > 6)
        finally:
            image.done()
            factory.done()


@pytest.mark.parametrize("persistent", [False, True])
def test_imagecalc_keeps_pixels_wcs_when_imagemd_has_a_different_rank(product_sources, persistent):
    casa, pixels, _ = product_sources
    writer = casa.image()
    assert writer.fromshape("rank3.im", [3, 4, 1])
    assert writer.setbrightnessunit("K")
    assert writer.setmiscinfo({"metadata": "rank three"})
    assert writer.done()
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        result = factory.imagecalc(name if persistent else "", '"real.im"*2', imagemd="rank3.im")
        try:
            assert result.brightnessunit() == "K"
            assert result.miscinfo() == {"metadata": "rank three"}
            coords = result.coordsys()
            assert coords.naxes() == 2
            np.testing.assert_array_equal(coords.referencepixel()["numeric"], [1, 2])
            coords.done()
            np.testing.assert_array_equal(result.getchunk(), pixels * 2)
        finally:
            result.done()
            factory.done()


@pytest.mark.parametrize("persistent", [False, True])
def test_imagecalc_applies_position_angle_metadata_after_copy(product_sources, persistent):
    casa, _, _ = product_sources
    writer = casa.image()
    for name, value in [("u.im", 4), ("q.im", 3), ("md.im", 0)]:
        assert writer.fromarray(name, np.full((2, 2, 1), value, dtype=np.float32))
        assert writer.setbrightnessunit("K")
        assert writer.setmiscinfo({"input": name})
        assert writer.done()
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        for spelling, unit, stokes in [("pa ", "deg", "Pangle"), ("PA", "K", "I")]:
            result = factory.imagecalc(name if persistent else "", f'{spelling}("u.im","q.im")',
                                       overwrite=True, imagemd="md.im")
            try:
                assert result.brightnessunit() == unit
                assert result.miscinfo() == {"input": "md.im"}
                coords = result.coordsys()
                assert list(coords.stokes()) == [stokes]
                coords.done()
                np.testing.assert_allclose(result.getchunk(), 0.5 * np.rad2deg(np.arctan2(4, 3)), rtol=1e-6)
            finally:
                result.done()
        factory.done()


@pytest.mark.parametrize("persistent", [False, True])
def test_imagecalc_projects_direct_fits_miscinfo(product_sources, persistent):
    casa, pixels, _ = product_sources
    fits = pytest.importorskip("astropy.io.fits")
    input_hdu = fits.PrimaryHDU(pixels.T)
    input_hdu.header["OBJECT"] = "example"
    input_hdu.header["FOOBAR"] = 73
    input_hdu.writeto("header.fits")
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        factory = backend.image()
        result = factory.imagecalc(name if persistent else "", '"header.fits"*2')
        try:
            assert result.miscinfo() == {"foobar": 73}
            np.testing.assert_array_equal(result.getchunk(), pixels * 2)
        finally:
            result.done()
            factory.done()


def test_imagecalc_validates_metadata_before_publishing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    factory = tools.image()
    pixels = np.arange(4, dtype=np.float32).reshape(2, 2)
    try:
        assert factory.fromarray("input.im", pixels)
        assert factory.done()
        assert factory.fromarray("owned.im", pixels + 100)
        assert factory.done()
        for outfile in ("new.im", "owned.im"):
            with pytest.raises(RuntimeError, match="Cannot access missing.im"):
                factory.imagecalc(outfile, '"input.im" * 3', overwrite=True, imagemd="missing.im")
        assert not Path("new.im").exists()
        assert factory.open("owned.im")
        np.testing.assert_array_equal(factory.getchunk(), pixels + 100)
    finally:
        factory.done()
