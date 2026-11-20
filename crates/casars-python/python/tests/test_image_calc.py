"""The public image calculator against native products from pinned CASA."""

import os
from pathlib import Path

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def calculator_sources(tmp_path, monkeypatch):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    monkeypatch.chdir(tmp_path)
    source = np.arange(1, 13, dtype=np.float32).reshape(3, 4)
    image = casa.image()
    assert image.fromarray("source.im", source)
    assert image.calcmask('"source.im">6', name="source_validity")
    assert image.tofits("source.fits")
    assert image.done()
    return casa, source


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_calc_takes_native_write_lock_after_unlock(calculator_sources):
    casa, _ = calculator_sources
    for backend in (casa, tools):
        image = backend.image()
        assert image.fromshape(f"{backend.__name__}.im", [2, 2])
        assert image.unlock()
        np.testing.assert_array_equal(image.haslock(), [False, False])
        assert image.calc("1", verbose=False)
        np.testing.assert_array_equal(image.haslock(), [True, True])
        assert image.done()


@pytest.mark.parametrize("complex_target", [False, True])
def test_calc_retains_destination_mask_coordinates_and_history(calculator_sources, complex_target):
    casa, source = calculator_sources
    original = source + 15
    expression = '"source.im" * 2'
    expected_pixels = source * 2
    mask_predicate = '"{name}.im" > 20'
    if complex_target:
        original = original.astype(np.complex64) + 2j
        expression = 'complex("source.im" * 2, "source.im" + 1)'
        expected_pixels = expected_pixels.astype(np.complex64) + 1j * (source + 1)
        mask_predicate = 'real("{name}.im") > 20'
    expected_mask = source > 5
    expected = np.where(expected_mask, expected_pixels, original)
    for backend, name, reader in [(casa, "reference", tools), (tools, "rust", casa)]:
        image = backend.image()
        initial_coordinates = None
        try:
            assert image.fromarray(name + ".im", original)
            coordinates = image.coordsys()
            initial_coordinates = coordinates.referencepixel()["numeric"]
            coordinates.done()
            assert image.calcmask(mask_predicate.format(name=name), name="target_validity")
            assert image.calc(pixels=expression, verbose=False)
            np.testing.assert_array_equal(image.getchunk(), expected)
            np.testing.assert_array_equal(image.getchunk(getmask=True), expected_mask)
            assert image.history(list=False)[-2:] == ["Ran ia.calc", f'ia.calc(expr="{expression}", verbose=false)']
        finally:
            image.done()
        check = reader.image()
        try:
            assert check.open(name + ".im")
            np.testing.assert_array_equal(check.getchunk(), expected)
            np.testing.assert_array_equal(check.getchunk(getmask=True), expected_mask)
            assert check.maskhandler("default") == ["target_validity"]
            coordinates = check.coordsys()
            np.testing.assert_array_equal(coordinates.referencepixel()["numeric"], initial_coordinates)
            coordinates.done()
        finally:
            check.done()


def test_calc_creates_valid_destination_mask_and_reads_an_alias_before_writing(calculator_sources):
    casa, source = calculator_sources
    for backend, name in [(casa, "reference"), (tools, "rust")]:
        image = backend.image()
        try:
            assert image.fromarray(name + ".im", source * 0)
            assert image.calcmask("F", name="mask0", asdefault=False)
            assert image.calc('"source.im" * 2', False)
            assert image.maskhandler("default") == ["mask1"]
            np.testing.assert_array_equal(image.getchunk(getmask=True), True)
            np.testing.assert_array_equal(image.getchunk(), source * 2)
            assert image.calc(f'"{name}.im" + mean("{name}.im")', False)
            np.testing.assert_array_equal(image.getchunk(), source * 2 + 13)
        finally:
            image.done()


def test_calc_self_reference_uses_the_original_image_across_tiles(calculator_sources):
    casa, _ = calculator_sources
    extent = 1025
    pixels = np.arange(extent**2, dtype=np.float32).reshape(extent, extent)
    expected = pixels + np.float32((extent**2 - 1) / 2)
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        image = backend.image()
        try:
            assert image.fromarray(name, pixels)
            assert image.calc(f'"{name}" + mean("{name}")', False)
            np.testing.assert_array_equal(image.getchunk(), expected)
        finally:
            image.done()


@pytest.mark.parametrize("collision", ["file", "directory", "dangling symlink"])
def test_calc_preserves_unregistered_mask_path_and_pixels(calculator_sources, collision):
    casa, source = calculator_sources
    for backend, name in [(casa, "reference.im"), (tools, "rust.im")]:
        image = backend.image()
        initial = source + 100
        try:
            assert image.fromarray(name, initial)
            before = image.history(list=False)
            path = Path(name) / "mask0"
            if collision == "directory":
                path.mkdir()
                path /= "keep.txt"
            if collision == "dangling symlink":
                path.symlink_to("missing.txt")
            else:
                path.write_text("preserve", encoding="utf-8")
            with pytest.raises(RuntimeError, match="mask0 already exists"):
                image.calc('"source.im"', False)
            np.testing.assert_array_equal(image.getchunk(), initial)
            assert image.maskhandler("get") == ["T"]
            assert image.history(list=False) == before
            if collision == "dangling symlink":
                assert path.readlink() == Path("missing.txt")
            else:
                assert path.read_text(encoding="utf-8") == "preserve"
        finally:
            image.done()


def test_calc_and_calcmask_accept_integer_rebin_factors(calculator_sources):
    casa, _ = calculator_sources
    writer = casa.image()
    assert writer.fromarray("rebin.im", np.arange(1, 17, dtype=np.float32).reshape(4, 4))
    assert writer.done()
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 2])
            assert image.calc('rebin("rebin.im", [2,2])', False)
            np.testing.assert_array_equal(image.getchunk(), [[3.5, 5.5], [11.5, 13.5]])
            assert image.calcmask('rebin("rebin.im", [2,2]) > 8')
            np.testing.assert_array_equal(image.getchunk(getmask=True), [[False, False], [True, True]])
        finally:
            image.done()


def test_calc_retains_single_precision_through_conditionals_and_replacement(calculator_sources):
    casa, _ = calculator_sources
    writer = casa.image()
    assert writer.fromarray("large.im", np.full((2, 2), 16777216, dtype=np.float32))
    assert writer.done()
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 2])
            for expression in [
                '(iif("large.im">0,"large.im",0)+1)-"large.im"',
                '(replace("large.im",0)+1)-"large.im"',
            ]:
                assert image.calc(expression, False)
                np.testing.assert_array_equal(image.getchunk(), 0)
        finally:
            image.done()


def test_calc_scalar_temporary_and_rejections_keep_pixels(calculator_sources):
    casa, source = calculator_sources
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.calc("2", False) is False
            assert image.fromshape(shape=list(source.shape))
            assert image.calc("2", False)
            np.testing.assert_array_equal(image.getchunk(), 2)
            for expression, message in [
                ("", "must specify an expression"),
                ("T", "Unsupported node data type Bool"),
                ("complex(2,3)", "attached image is real valued"),
                ("[1,2]", "shape of the expression does not conform"),
            ]:
                with pytest.raises(RuntimeError, match=message):
                    image.calc(expression, False)
                np.testing.assert_array_equal(image.getchunk(), 2)
            assert image.done()
            assert image.fromshape(shape=[2])
            assert image.calc("[1.5,2.5]", False)
            np.testing.assert_array_equal(image.getchunk(), [1.5, 2.5])
            assert image.calc("[1,2]+1", False)
            np.testing.assert_array_equal(image.getchunk(), [2, 3])
            for expression, value in [("mean([1,2])", 1.5), ("mean(1)", 1), ("variance(1)", 0)]:
                assert image.calc(expression, False)
                np.testing.assert_array_equal(image.getchunk(), value)
            with pytest.raises(RuntimeError, match="Unsupported node data type Bool"):
                image.calc("[T,F]", False)
        finally:
            image.done()


@pytest.mark.parametrize("kind", ["d", "cd"])
def test_calc_rejects_double_precision_target(calculator_sources, kind):
    casa, _ = calculator_sources
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3], type=kind)
            with pytest.raises(RuntimeError, match="does not support images with .* precision pixel values"):
                image.calc("2")
            np.testing.assert_array_equal(image.getchunk(), 0)
        finally:
            image.done()


def test_calc_rejects_fits_target_without_writing(calculator_sources):
    casa, source = calculator_sources
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.open("source.fits")
            initial = image.getchunk().copy()
            assert initial.shape == source.shape
            with pytest.raises(RuntimeError, match=r"to\.isWritable\(\)"):
                image.calc("2", False)
            np.testing.assert_array_equal(image.getchunk(), initial)
        finally:
            image.done()


def test_calc_writes_fits_source_values_and_creates_a_destination_mask(calculator_sources):
    casa, source = calculator_sources
    expected = np.where(source > 6, source * 2, np.nan)
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=list(source.shape))
            assert image.calc('"source.fits" * 2', False)
            np.testing.assert_array_equal(image.getchunk(), expected)
            np.testing.assert_array_equal(image.getchunk(getmask=True), True)
            assert image.maskhandler("default") == ["mask0"]
        finally:
            image.done()


@pytest.mark.parametrize("kind", ["f", "d"])
def test_calc_reduces_an_all_invalid_image_to_a_numeric_scalar(calculator_sources, kind):
    casa, source = calculator_sources
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromarray("all_invalid.im", source, type=kind, overwrite=True)
            assert image.calcmask("F")
            assert image.done()
            assert image.fromshape(shape=list(source.shape))
            for name in ("min", "max", "mean", "median"):
                assert image.calc(f'{name}("all_invalid.im")', False)
                expected = -np.finfo(np.float32).tiny if kind == "f" else -0.0
                result = image.getchunk()
                np.testing.assert_array_equal(result, expected)
                assert np.signbit(result).all()
                assert image.maskhandler("default") == [""]
            for name in ("sum", "variance", "stddev", "avdev"):
                assert image.calc(f'{name}("all_invalid.im")', False)
                np.testing.assert_array_equal(image.getchunk(), 0)
        finally:
            image.done()


def test_calc_promotes_mixed_precision_vector_elements(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    image = tools.image()
    try:
        assert image.fromshape(shape=[3])
        assert image.calc("[1, 2.5, 4] + 1", False)
        np.testing.assert_array_equal(image.getchunk(), [2, 3.5, 5])
        assert image.calc("[-1.25, -2e1, +3.2]", False)
        np.testing.assert_allclose(image.getchunk(), [-1.25, -20, 3.2], rtol=1e-6)
    finally:
        image.done()
