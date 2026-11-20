"""Paged image concatenation through the public CASA-style factory."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def write_linear(module, path, offset, payload, kind="f", blank=False):
    image = module.image()
    assert image.fromshape(str(path), [2, 3], linear=True, type=kind)
    assert image.putchunk(np.full((2, 3), payload))
    coord = image.coordsys()
    record = coord.torecord()
    assert coord.done()
    record["linear0"]["crval"][0] = offset
    assert image.setcoordsys(record)
    assert image.setmiscinfo({"label": str(payload)})
    if kind in ("f", "c"):
        assert image.setbrightnessunit("K" if blank else "Jy/pixel")
    assert image.sethistory("input", [f"pixels {payload}"])
    if blank:
        assert image.calcmask("F", "input_mask")
    assert image.done()


def test_linear_product_returns_an_independent_temporary_image(casa, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_linear(casa, "first.im", 0.0, 1.0)
    write_linear(casa, "second.im", 2.0, 2.0, blank=True)
    for module in (tools, casa):
        factory = module.image()
        assert factory.fromshape(shape=[3, 4])
        result = factory.imageconcat(infiles="first.im,second.im", axis=0)
        assert result.ispersistent() is False
        assert result.name() == "Temporary_Image"
        np.testing.assert_array_equal(result.shape(), [4, 3])
        assert result.brightnessunit() == "Jy/pixel"
        assert result.miscinfo() == {"label": "1.0", "label_1": "2.0"}
        assert result.maskhandler("get") == ["mask0"]
        assert result.maskhandler("default") == ["mask0"]
        np.testing.assert_array_equal(result.getchunk()[:, 0], [1, 1, 2, 2])
        np.testing.assert_array_equal(result.getchunk(getmask=True)[:, 0], [True, True, False, False])
        assert "pixels 1.0" in result.history()
        assert "pixels 2.0" in result.history()
        assert result.history()[-2:] == [
            "Ran ia.imageconcat on Temporary_Image",
            "ia.imageconcat(outfile='', infiles='first.im,second.im', axis=0, relax=false, tempclose=true, overwrite=false, reorder=false, mode='paged')",
        ]
        assert result.done()
        np.testing.assert_array_equal(factory.shape(), [3, 4])
        assert factory.done()


def test_repeated_input_preserves_pixel_array_order(tmp_path):
    first, second = tmp_path / "first.im", tmp_path / "second.im"
    write_linear(tools, first, 0.0, 3.0)
    write_linear(tools, second, 2.0, 5.0)
    outfile = str(tmp_path / "joined.im")
    image = tools.image().imageconcat(
        outfile, np.asarray([str(first), str(second), str(first)]), axis=0, relax=True,
    )
    assert image.done()
    reader = tools.image()
    assert reader.open(outfile)
    pixels = reader.getchunk()
    np.testing.assert_array_equal(pixels, np.repeat([[3], [3], [5], [5], [3], [3]], 3, axis=1))
    assert reader.done()


@pytest.mark.parametrize("kind", ["f", "d", "c", "cd"])
def test_native_products_reopen_in_casa_for_every_type(casa, tmp_path, kind):
    first, second = tmp_path / "file 1.im", tmp_path / "file 2.im"
    real = kind not in ("c", "cd")
    a, b = (1.0, 2.0) if real else (1 + 3j, 2 - 4j)
    write_linear(casa, first, 0.0, a, kind)
    write_linear(tools, second, 2.0, b, kind)
    files = np.array([str(first), str(second)])
    for module in (tools, casa):
        outfile = str(tmp_path / f"product-{module.__name__}.im")
        factory = module.image()
        image = factory.imageconcat(outfile, files, 0, tempclose=False)
        if kind in ("f", "c"):
            assert image.ispersistent()
            assert image.name() == outfile
        assert image.pixeltype() == {"f": "float", "d": "double", "c": "complex", "cd": "dcomplex"}[kind]
        assert not factory.isopen()
        assert image.maskhandler("default") == [""]
        assert image.done()
        for reader in (tools, casa):
            product = reader.image()
            assert product.open(outfile)
            np.testing.assert_array_equal(product.getchunk()[:, 0], [a, a, b, b])
            assert product.done()


def test_spectral_order_and_separate_beam_planes(casa, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    paths = [Path("plane01.im"), Path("plane02.im")]
    for index, path in enumerate(paths):
        writer = casa.image()
        assert writer.fromshape(str(path), [3, 4, 1, 2])
        assert writer.putchunk(np.full((3, 4, 1, 2), float(index + 1)))
        cs = writer.coordsys()
        record = cs.torecord()
        assert cs.done()
        spectral = next(value for name, value in record.items() if name.startswith("spectral"))
        spectral["wcs"]["crval"] += 2 * index * spectral["wcs"]["cdelt"]
        assert writer.setcoordsys(record)
        if index == 0:
            assert writer.setrestoringbeam("4arcmin", "2arcsec", "12deg", channel=0)
            assert writer.setrestoringbeam("240arcsec", "2arcsec", "12deg", channel=1)
        else:
            assert writer.setrestoringbeam("5arcmin", "2arcsec", "12deg")
        assert writer.done()
    files = [str(paths[1]), str(paths[0])]
    for module in (tools, casa):
        factory = module.image()
        assert factory.dohistory(False)
        image = factory.imageconcat(infiles=files, reorder=True)
        np.testing.assert_array_equal(image.shape(), [3, 4, 1, 4])
        np.testing.assert_array_equal(image.getchunk()[0, 0, 0, :], [1, 1, 2, 2])
        assert image.restoringbeam()["nChannels"] == 4
        for channel, (width, unit) in enumerate([(4, "arcmin"), (240, "arcsec"), (5, "arcmin"), (5, "arcmin")]):
            beam = image.restoringbeam(channel=channel)
            assert beam["major"]["unit"] == unit
            assert beam["major"]["value"] == pytest.approx(width)
        assert not any("Ran ia.imageconcat" in row for row in image.history())
        assert image.done()
        wildcard = factory.imageconcat(infiles="plane*.im", reorder=True)
        np.testing.assert_array_equal(wildcard.getchunk()[0, 0, 0, :], [1, 1, 2, 2])
        assert wildcard.done()


def test_strict_gap_rejected_and_relaxed_geometry_matches_casa(casa, tmp_path):
    first, second = tmp_path / "first.im", tmp_path / "second.im"
    write_linear(casa, first, 0.0, 1.0)
    write_linear(casa, second, 6.0, 2.0)
    files = [str(first), str(second)]
    results = []
    for module in (tools, casa):
        factory = module.image()
        with pytest.raises(RuntimeError, match="not contiguous along the concatenation axis"):
            factory.imageconcat(infiles=files, axis=0)
        output = factory.imageconcat(infiles=files, axis=0, relax=True)
        np.testing.assert_array_equal(output.getchunk()[:, 0], [1, 1, 2, 2])
        coord = output.coordsys()
        results.append(coord.torecord()["linear0"])
        assert coord.done()
        assert output.done()
    for key in results[0]:
        np.testing.assert_equal(results[0][key], results[1][key])


def test_relaxed_spectral_gap_uses_the_input_world_frequencies(casa, tmp_path):
    paths, saved = [], []
    for index in (0, 1):
        path = str(tmp_path / f"spectrum{index}.im")
        paths.append(path)
        image = casa.image()
        assert image.fromshape(path, [3, 4, 1, 2])
        cs = image.coordsys()
        record = cs.torecord()
        assert cs.done()
        spectral = next(value for key, value in record.items() if key.startswith("spectral"))
        spectral["wcs"]["crval"] += 6 * index * spectral["wcs"]["cdelt"]
        base = spectral["wcs"]["crval"] - spectral["wcs"]["cdelt"] * spectral["wcs"]["crpix"]
        saved.extend([base, base + spectral["wcs"]["cdelt"]])
        assert image.setcoordsys(record)
        assert image.done()
    frequencies = []
    for module in (tools, casa):
        image = module.image().imageconcat(infiles=paths, relax=True)
        cs = image.coordsys()
        record = cs.torecord()
        assert cs.done()
        spectral = next(value for key, value in record.items() if key.startswith("spectral"))
        frequencies.append(spectral["tabular"]["worldvalues"])
        assert image.done()
    for actual in frequencies:
        np.testing.assert_allclose(actual, saved, rtol=1e-14, atol=0)


def test_reorder_decreasing_axis_keeps_descending_world_order(casa, tmp_path):
    files = [str(tmp_path / name) for name in ("first.im", "second.im")]
    for index, path in enumerate(files):
        write_linear(casa, path, -2.0 * index, 1.0 + index)
        image = casa.image()
        assert image.open(path)
        cs = image.coordsys()
        record = cs.torecord()
        assert cs.done()
        record["linear0"]["cdelt"][0] = -1.0
        assert image.setcoordsys(record)
        assert image.done()
    for module in (tools, casa):
        output = module.image().imageconcat(infiles=files[::-1], axis=0, reorder=True)
        np.testing.assert_array_equal(output.getchunk()[:, 0], [1, 1, 2, 2])
        cs = output.coordsys()
        assert cs.torecord()["linear0"]["cdelt"][0] == -1.0
        assert cs.done()
        assert output.done()


def test_fits_inputs_and_blanks(casa, tmp_path):
    for index in range(2):
        native = tmp_path / f"source{index}.im"
        write_linear(casa, native, 2.0 * index, index + 1.0, blank=index == 1)
        writer = casa.image()
        assert writer.open(str(native))
        assert writer.tofits(str(tmp_path / f"source{index}.fits"))
        assert writer.done()
    for other, relax in (("source1.fits", False), ("source1.im", True)):
        files = [str(tmp_path / name) for name in ("source0.fits", other)]
        rows = []
        for module in (tools, casa):
            output = module.image().imageconcat(infiles=files, axis=0, relax=relax)
            assert output.maskhandler("get") == ["mask0"]
            np.testing.assert_array_equal(output.getchunk(getmask=True)[:, 0], [1, 1, 0, 0])
            rows.append(output.getchunk()[:, 0])
            assert output.done()
        np.testing.assert_array_equal(*rows)


def test_stokes_concatenation_extends_the_polarization_coordinate(casa, tmp_path):
    files = []
    for name, value in (("I", 1.0), ("Q", 2.0)):
        path = str(tmp_path / f"{name}.im")
        image = casa.image()
        assert image.fromshape(path, [3, 4, 1, 2])
        assert image.putchunk(np.full([3, 4, 1, 2], value))
        cs = image.coordsys()
        record = cs.torecord()
        assert cs.done()
        stokes = next(value for key, value in record.items() if key.startswith("stokes"))
        stokes["stokes"] = [name]
        assert image.setcoordsys(record)
        assert image.done()
        files.append(path)
    for module in (tools, casa):
        output = module.image().imageconcat(infiles=files, axis=2)
        np.testing.assert_array_equal(output.shape(), [3, 4, 2, 2])
        np.testing.assert_array_equal(output.getchunk()[0, 0, :, 0], [1, 2])
        cs = output.coordsys()
        assert cs.stokes() == ["I", "Q"]
        assert cs.done()
        assert output.done()


def test_argument_errors_and_existing_product(casa, tmp_path):
    first, second = tmp_path / "first.im", tmp_path / "second.im"
    write_linear(casa, first, 0.0, 1.0)
    write_linear(casa, second, 2.0, 2.0)
    files = [str(first), str(second)]
    for module in (tools, casa):
        factory = module.image()
        with pytest.raises(RuntimeError, match="at least two images"):
            factory.imageconcat(infiles=[str(first), str(tmp_path / "missing.im")], axis=0)
        with pytest.raises(RuntimeError, match="Directory"):
            factory.imageconcat(infiles=f"{first}, {second}", axis=0)
        with pytest.raises(RuntimeError, match="has no spectral axis"):
            factory.imageconcat(infiles=files)
        with pytest.raises(RuntimeError, match="axis exceeds number of dimensions"):
            factory.imageconcat(infiles=files, axis=2)
        with pytest.raises(RuntimeError, match="Unsupported mode"):
            factory.imageconcat(infiles=files, axis=0, mode="unknown")
        with pytest.raises(RuntimeError, match="exists and overwrite is false"):
            factory.imageconcat(str(first), files, 0)
        assert not factory.isopen()
        assert factory.open(str(first))
        np.testing.assert_array_equal(factory.getchunk(), 1.0)
        assert factory.done()


def test_paged_overwrite_replaces_an_output_and_preserves_an_aliased_input(casa, tmp_path):
    first, second = tmp_path / "first.im", tmp_path / "second.im"
    write_linear(casa, first, 0.0, 1.0)
    write_linear(casa, second, 2.0, 2.0)
    files = [str(first), str(second)]
    for module in (tools, casa):
        outfile = str(tmp_path / f"existing-{module.__name__}.im")
        write_linear(casa, outfile, 0.0, 9.0)
        instance = module.image()
        output = instance.imageconcat(outfile, files, axis=0, overwrite=True)
        np.testing.assert_array_equal(output.getchunk()[:, 0], [1, 1, 2, 2])
        assert output.done()
        with pytest.raises(RuntimeError, match="already opened"):
            instance.imageconcat(str(first), files, axis=0, overwrite=True)
        assert instance.open(str(first))
        np.testing.assert_array_equal(instance.shape(), [2, 3])
        np.testing.assert_array_equal(instance.getchunk(), 1.0)
        assert instance.done()
