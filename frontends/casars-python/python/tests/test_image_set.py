"""Public image.set edits and native readback against CASA 6.7.6.14."""

import os

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_set_edits_the_region_and_preserves_the_default_mask(casa, tmp_path):
    values = np.arange(20, dtype=np.float32).reshape(4, 5)
    region = casa.regionmanager().box([1, 1], [2, 3])
    expected_pixels = values.copy()
    expected_pixels[1:3, 1:4] = 3.25
    expected_mask = np.ones(values.shape, dtype=bool)
    expected_mask[1:3, 1:4] = False
    expected_mask[2, 2] = True
    one_pixel = casa.regionmanager().box([2, 2], [2, 2])
    for module in (casa, tools):
        tool = module.image()
        output = str(tmp_path / f"{module.__name__}.im")
        assert tool.set(pixels=5) is False
        assert tool.fromarray(output, values, linear=True)
        assert tool.set() is False
        assert tool.set(3.25, 0, region, False)
        assert tool.history()[-1] == "ia.set(pixels=3.250000, pixelmask=0, region={...}, list=false)"
        assert tool.set(pixelmask=3, region=one_pixel)
        assert tool.maskhandler("default") == ["mask0"]
        assert tool.done()
        for reader in (casa.image(), tools.image()):
            assert reader.open(output)
            np.testing.assert_array_equal(reader.getchunk(), expected_pixels)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), expected_mask)
            assert reader.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_set_reacquires_write_lock_for_a_self_expression(casa, tmp_path):
    for backend, peer in ((casa, tools), (tools, casa)):
        source = str(tmp_path / f"{backend.__name__}.im")
        image = backend.image()
        assert image.fromarray(source, np.arange(12, dtype=np.float32).reshape(3, 4))
        assert image.unlock()
        assert image.set(f'mean("{source}") + 1', pixelmask=1)
        np.testing.assert_array_equal(image.haslock(), [True, True])
        assert image.done()
        reader = peer.image()
        assert reader.open(source)
        np.testing.assert_array_equal(reader.getchunk(), np.full((3, 4), 6.5))
        assert reader.getchunk(getmask=True).all()
        assert reader.done()


def test_set_accepts_scalar_lattice_expressions(casa, tmp_path):
    source = tmp_path / "sample.im"
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    writer = casa.image()
    assert writer.fromarray(str(source), values)
    assert writer.done()
    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=[3, 4])
        assert image.set(pixels="2.0^3 + 3")
        np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 11))
        assert image.set(pixels="real(complex(1,2))")
        np.testing.assert_array_equal(image.getchunk(), np.ones((3, 4)))
        assert image.set(pixels="real(complex(real(complex(1,2)),3))")
        np.testing.assert_array_equal(image.getchunk(), np.ones((3, 4)))
        assert image.set(pixels="float(16777217) - 16777216")
        np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
        assert image.set(pixels="float(16777216) + 1e0 - 16777216")
        np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
        assert image.set(pixels="float(16777216) + 1. - 16777216")
        np.testing.assert_array_equal(image.getchunk(), np.ones((3, 4)))
        assert image.set(pixels="16777217 - 16777216")
        np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
        assert image.set(pixels=1.2345678e-7)
        np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
        assert image.set(pixels="sqrt(-1)")
        assert np.isnan(image.getchunk()).all()
        for value in ("1>0", "complex(1,2)"):
            with pytest.raises(RuntimeError, match="conversion to Float not possible"):
                image.set(pixels=value)
        assert image.set(pixels=f'mean("{source}") * 2')
        np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 11))
        with pytest.raises(RuntimeError, match="must be scalar"):
            image.set(pixels=f'"{source}"')
        with pytest.raises(RuntimeError, match="must be scalar"):
            image.set(pixels=[1, 2])
        np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 11))
        assert image.done()


def test_set_distinguishes_empty_masked_reductions_from_valid_zero(casa, tmp_path):
    source = str(tmp_path / "fully_masked.im")
    partial = str(tmp_path / "partly_masked.im")
    for name, expression in [(source, "> 99"), (partial, "> 2")]:
        writer = casa.image()
        assert writer.fromarray(name, np.arange(12, dtype=np.float32).reshape(3, 4))
        assert writer.calcmask(f'"{name}" {expression}')
        assert writer.done()

    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=[3, 4])
        assert image.set(11)
        for expression in [
            f'mean("{source}") + 2',
            f'min("{source}")',
            f'fractile("{source}", 0.25)',
            f'real(complex(mean("{source}"),3))',
        ]:
            with pytest.raises(RuntimeError, match="scalar pixels expression is invalid"):
                image.set(expression)
            np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 11))
        assert image.calc(f'mean("{source}") + 2', verbose=False)
        np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 2))
        assert image.set(f'sum("{source}")')
        np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
        assert image.set(f'real(complex(mean("{partial}"),3))')
        np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 7))
        assert image.done()


def test_set_checks_text_and_record_regions_in_casa_order(casa):
    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=[3, 4])
        before = image.maskhandler("default")
        with pytest.raises(RuntimeError) as text_error:
            image.set("invalid operand", pixelmask=0, region="bad region")
        assert "unknown lattice or image" not in str(text_error.value)
        assert image.maskhandler("default") == before
        with pytest.raises(RuntimeError) as record_error:
            image.set("invalid operand", pixelmask=0, region={"name": "LCBox"})
        assert "unknown lattice or image" not in str(record_error.value)
        assert image.maskhandler("default") == ["mask0"]
        np.testing.assert_array_equal(image.getchunk(getmask=True), np.ones((3, 4), dtype=bool))
        assert image.done()


def test_set_keeps_an_all_valid_mask_after_invalid_pixels(casa):
    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=[3, 4])
        with pytest.raises(RuntimeError, match="unknown lattice or image"):
            image.set(pixels="invalid", pixelmask=0)
        assert image.maskhandler("default") == ["mask0"]
        np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
        np.testing.assert_array_equal(image.getchunk(getmask=True), np.ones((3, 4), dtype=bool))
        assert image.done()


def test_set_leaves_an_attached_fits_file_intact(tmp_path):
    path = tmp_path / "image.fits"
    original = _hdu(True, [1, 2, 3, 4])
    path.write_bytes(original)
    image = tools.image()
    assert image.open(str(path))
    with pytest.raises(RuntimeError, match="not writable"):
        image.set(pixels=2)
    with pytest.raises(RuntimeError, match="Cannot make requested mask"):
        image.set(pixelmask=0)
    assert path.read_bytes() == original
    assert image.done()


def test_set_mixed_scalar_operands_with_fits_and_double_images(casa, tmp_path):
    fits = tmp_path / "source.fits"
    fits.write_bytes(_hdu(True, [1, 2, 3, 4]))
    double = str(tmp_path / "double.im")
    writer = casa.image()
    assert writer.fromarray(double, np.array([[0.25, 1.25], [2.25, 3.25]]), type="d")
    assert writer.done()
    expression = f'mean("{fits}") + mean("{double}")'
    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=[3, 4])
        assert image.set(pixels=expression)
        np.testing.assert_array_equal(image.getchunk(), np.full((3, 4), 4.25))
        assert image.done()


def test_set_respects_membership_inside_an_ellipsoid_bounding_box(casa):
    shape = [7, 7]
    template = casa.image()
    assert template.fromshape(shape=shape)
    coordinates = template.coordsys()
    region = casa.regionmanager().fromtext(
        "ellipse[[3pix, 3pix], [2pix, 1pix], 0deg]", shape, coordinates.torecord()
    )
    assert coordinates.done()
    assert template.done()
    results = []
    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=shape)
        assert image.set(pixels=7, pixelmask=0, region=region)
        results.append((image.getchunk(), image.getchunk(getmask=True)))
        assert image.done()
    expected_pixels, expected_mask = results[0]
    assert expected_mask.any() and not expected_mask.all()
    np.testing.assert_array_equal(results[1][0], expected_pixels)
    np.testing.assert_array_equal(results[1][1], expected_mask)


def test_set_reports_an_error_for_complex_output_without_editing_it():
    image = tools.image()
    assert image.fromshape(shape=[3, 4], type="c")
    with pytest.raises(RuntimeError, match="only supports float"):
        image.set(pixels=2)
    np.testing.assert_array_equal(image.getchunk(), np.zeros((3, 4)))
    assert image.done()
