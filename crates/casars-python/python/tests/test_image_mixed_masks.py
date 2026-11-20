"""Native, complex and FITS inputs for the public image.calcmask method."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def reference_images(tmp_path, monkeypatch):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    monkeypatch.chdir(tmp_path)
    real = np.arange(6, dtype=np.float32).reshape(2, 3) + 1
    writer = casa.image()
    for name, precision, pixels in [
        ("real.im", "f", real),
        ("double.im", "d", real),
        ("complex.im", "f", real.astype(np.complex64) + (6 - real) * 1j),
        ("dcomplex.im", "d", real.astype(np.complex128) + (6 - real) * 1j),
    ]:
        assert writer.fromarray(name, pixels, type=precision)
        if name == "real.im":
            assert writer.calcmask('"real.im">3', name="stored")
            assert writer.tofits("real.fits")
        if name == "complex.im":
            assert writer.calcmask('real("complex.im")>3', name="stored")
        assert writer.done()
    return casa, real


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('real("complex.im") > 2', lambda data: data > 2),
        ('imag("dcomplex.im") > 2', lambda data: (6 - data) > 2),
        ('"complex.im" > 2', lambda data: np.hypot(data, 6 - data) > 2),
        ('("real.im"+"double.im") > 4', lambda data: data > 2),
        ('"real.fits" > 2', lambda data: data > 3),
        ('mask("real.fits")', lambda data: data > 3),
        ('replace("real.fits",123)>120', lambda data: data <= 3),
        ('mask("real.im:stored")', lambda data: data > 3),
        ('mask("complex.im:stored")', lambda data: data > 3),
        ('mask("real.im:NoMask")', lambda data: np.ones_like(data, dtype=bool)),
        ('mask("real.fits:nomask")', lambda data: np.ones_like(data, dtype=bool)),
        ('mask("real.im" + "double.im")', lambda data: data > 3),
        ('mask("real.im" > 99)', lambda data: data > 3),
        ('mask(("real.im">0) || F)', lambda data: data > 3),
        ('mask(("real.im">0) && T)', lambda data: data > 3),
        ('"real.im" > mean("real.im")', lambda data: data > 5),
        ('mask(1)', lambda data: np.ones_like(data, dtype=bool)),
        ('isnan(complex(1,2))', lambda data: np.zeros_like(data, dtype=bool)),
    ],
)
def test_calcmask_compiles_mixed_pixel_operands_and_saves_validity(
    reference_images, expression, expected,
):
    casa, real = reference_images
    reference = casa.image()
    image = tools.image()
    try:
        assert reference.fromshape("reference.im", [2, 3])
        assert image.fromshape("output.im", [2, 3])
        for tool in (reference, image):
            assert tool.calcmask(expression, name="selection", asdefault=True)
            np.testing.assert_array_equal(tool.getchunk(getmask=True), expected(real))
        assert image.history(list=False)[-2:] == reference.history(list=False)[-2:]
        assert reference.done()
        assert image.done()
        assert reference.open("output.im")
        assert "selection" in reference.maskhandler("get")
        np.testing.assert_array_equal(reference.getchunk(getmask=True), expected(real))
    finally:
        image.done()
        reference.done()


def test_calcmask_requires_boolean_output_and_preserves_previous_mask(reference_images):
    casa, real = reference_images
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            assert image.calcmask('real("complex.im")>3', name="selection")
            with pytest.raises(RuntimeError, match="The expression type must be Boolean"):
                image.calcmask('real("complex.im")', name="selection")
            with pytest.raises(RuntimeError, match="shape of the expression does not conform"):
                image.calcmask('indexin(0,[1])', name="selection")
            with pytest.raises(RuntimeError, match="shapes of operands mismatch"):
                image.calcmask('indexin(0,[1]) && "real.im">1', name="selection")
            with pytest.raises(RuntimeError, match="unsupported data type"):
                image.calcmask('mask("dcomplex.im:stored")', name="selection")
            np.testing.assert_array_equal(image.getchunk(getmask=True), real > 3)
        finally:
            image.done()


def test_calcmask_rejects_tile_compressed_fits(reference_images):
    casa, real = reference_images
    fits = pytest.importorskip("astropy.io.fits")
    fits.HDUList([fits.PrimaryHDU(), fits.CompImageHDU(real.T)]).writeto("compressed.fits")
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            with pytest.raises(RuntimeError, match="No data in the zeroth or first extension"):
                image.calcmask('"compressed.fits" > 4')
            assert image.maskhandler("get") == ["T"]
        finally:
            image.done()


def test_calcmask_abbreviates_long_history_names(reference_images):
    casa, _ = reference_images
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            long_name = "q" * 301
            assert image.calcmask("T", name=long_name)
            assert image.maskhandler("default") == [long_name]
            assert image.history(list=False)[-1] == 'ia.calcmask(mask="T", name="...", asdefault=true)'
        finally:
            image.done()


def test_calcmask_boolean_validity_uses_a_decisive_known_operand(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    image = tools.image()
    try:
        pixels = np.arange(1, 7, dtype=np.float32).reshape(2, 3)
        assert image.fromarray("source.im", pixels)
        assert image.calcmask('"source.im" > 3')
        assert image.done()
        assert image.fromshape(shape=[2, 3])
        for expression in [
            'mask(("source.im" > 0) || T)',
            'mask(F && ("source.im" > 0))',
            'mask(("source.im" > 3) || (value("source.im") <= 3))',
        ]:
            assert image.calcmask(expression, name="known")
            np.testing.assert_array_equal(image.getchunk(getmask=True), True)
        assert image.calcmask('mask(("source.im" > 3) || (value("source.im") <= 2))', name="known")
        np.testing.assert_array_equal(image.getchunk(getmask=True), pixels != 3)
    finally:
        image.done()


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('("large.im"+1+"double.im")>16777217.5', False),
        ('real("large_complex.im"+complex(1,0))>16777216', False),
        ('real("large_complex.im"+complex("double.im",0))>16777216', True),
    ],
)
def test_calcmask_preserves_arithmetic_precision_at_promotion(reference_images, expression, expected):
    casa, _ = reference_images
    writer = casa.image()
    for name, precision, value, dtype in [
        ("large.im", "f", 16777216, np.float32),
        ("double.im", "d", 1, np.float64),
        ("large_complex.im", "f", 16777216 + 6j, np.complex64),
    ]:
        assert writer.fromarray(name, np.full((2, 3), value, dtype=dtype),
                                type=precision, overwrite=True)
        assert writer.done()
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            assert image.calcmask(expression)
            np.testing.assert_array_equal(image.getchunk(getmask=True), expected)
        finally:
            image.done()
