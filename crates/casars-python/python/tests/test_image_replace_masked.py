"""Public masked pixel replacements and native readback against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_detached_replace_returns_false():
    assert tools.image().replacemaskedpixels() is False


@pytest.mark.parametrize("form", ["inline", "file"])
@pytest.mark.parametrize("case", ["qualified", "compound", "clipped"])
def test_text_region_admission_preserves_mask_edit_state(casa, tmp_path, form, case):
    shape = (15, 17, 1, 5)
    box = "box[[2pix,3pix],[11pix,12pix]]"
    text = {
        "qualified": "circle[[8pix,8pix], 3pix], range=[1pix,2pix], corr=[I]",
        "compound": box + "\nbox[[4pix,5pix],[7pix,8pix]]",
        "clipped": box + "\nbox[[100pix,100pix],[110pix,111pix]]",
    }[case]
    if form == "file":
        path = tmp_path / "region.crtf"
        path.write_text(f"#CRTFv0\n{text}\n")
        region = str(path)
    else:
        region = text
    path = tmp_path / "mask.im"
    mask = casa.image()
    try:
        assert mask.fromarray(str(path), np.ones(shape, dtype=np.float64), type="d")
    finally:
        mask.done()
    expression = f'"{path}" > 0'
    for api in (casa, tools):
        name = str(tmp_path / f"{api.__name__}-input.im")
        image = api.image()
        valid = np.zeros(shape, dtype=bool)
        try:
            assert image.fromshape(name, list(shape))
            assert image.calcmask("F", "initial")
            assert image.done() and image.open(name)
            before = list(image.history(False))
            if case == "clipped":
                assert image.replacemaskedpixels(8, region=region, mask=expression, update=True)
                valid[2:12, 3:13] = True
            else:
                np.testing.assert_array_equal(image.haslock(), [True, False])
                with pytest.raises(RuntimeError, match="Only a single n-dimensional rectangular region"):
                    image.replacemaskedpixels(8, region=region, mask=expression, update=True)
                np.testing.assert_array_equal(image.haslock(), [True, False])
                assert list(image.history(False)) == before
            assert image.isopen()
            np.testing.assert_array_equal(image.getchunk(), np.where(valid, 8., 0.))
            np.testing.assert_array_equal(image.getchunk(getmask=True), valid)
            assert image.done() and image.open(name)
            np.testing.assert_array_equal(image.getchunk(), np.where(valid, 8., 0.))
            np.testing.assert_array_equal(image.getchunk(getmask=True), valid)
        finally:
            image.done()


def test_replacement_updates_pixels_and_mask_inside_region(casa, tmp_path):
    old = np.arange(20, dtype=np.float32).reshape(4, 5)
    source_path = str(tmp_path / "operands.im")
    source = casa.image()
    assert source.fromarray(source_path, old + 100)
    assert source.done()
    region = casa.regionmanager().box([1, 1], [3, 3])
    selection = np.s_[1:4, 1:4]
    expected = old.copy()
    expected[selection] = np.where(old[selection] <= 11, 77, old[selection])
    original_mask = old > 11
    updated_mask = original_mask.copy()
    updated_mask[selection] = True

    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromarray(name, old)
        assert image.calcmask(f'"{name}">11')
        before_history = list(image.history())
        with pytest.raises(RuntimeError, match="shapes of operands mismatch"):
            image.replacemaskedpixels(f'"{source_path}"', region=region, update=True)
        np.testing.assert_array_equal(image.getchunk(), old)
        np.testing.assert_array_equal(image.getchunk(getmask=True), original_mask)
        assert list(image.history()) == before_history

        assert image.replacemaskedpixels(77, region=region, update=True)
        assert image.history()[-1] == (
            "ia.replacemaskedpixels(pixels=77, region={...}, mask=\"\", "
            "update=true, list=false, stretch=false)"
        )
        assert image.done()
        for reader in [casa.image(), tools.image()]:
            assert reader.open(name)
            np.testing.assert_array_equal(reader.getchunk(), expected)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), updated_mask)
            assert reader.done()


def test_replacement_ignores_mask_on_rhs_pixels(casa, tmp_path):
    original = np.arange(16, dtype=np.float32).reshape(4, 4)
    rhs = str(tmp_path / "rhs.im")
    source = casa.image()
    assert source.fromarray(rhs, original + 100)
    assert source.calcmask(f'"{rhs}">104')
    assert source.done()
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromarray(name, original)
        assert image.calcmask(f'"{name}">7')
        assert image.replacemaskedpixels(f'"{rhs}" - 2')
        np.testing.assert_array_equal(
            image.getchunk(), np.where(original > 7, original, original + 98)
        )
        np.testing.assert_array_equal(image.getchunk(getmask=True), original > 7)
        assert image.replacemaskedpixels(4, update=True)
        with pytest.raises(RuntimeError, match="has no bad pixels"):
            image.replacemaskedpixels(9)
        assert image.done()


def test_float_replacement_retains_rounding_before_double_promotion(casa, tmp_path):
    source = str(tmp_path / "float.im")
    writer = casa.image()
    assert writer.fromarray(source, np.full((2, 2), 16777216, dtype=np.float32))
    assert writer.done()
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromshape(name, [2, 2])
        assert image.calcmask(f'"{name}">1')
        for expression, expected in [
            (f'"{source}" + 1e0 - 16777216', 0),
            (f'("{source}" + 1e0) - 16777216.', 0),
            (f'"{source}" + 1. - 16777216', 1),
            (f'mean("{source}") + 1 - 16777216', 0),
        ]:
            assert image.replacemaskedpixels(expression)
            np.testing.assert_array_equal(image.getchunk(), np.full((2, 2), expected))
        assert image.done()


def test_float_reduction_retains_small_values_in_its_accumulator(casa, tmp_path):
    source = str(tmp_path / "sum.im")
    writer = casa.image()
    values = np.array([16777216, 1, -16777216, 1], dtype=np.float32)
    assert writer.fromarray(source, values, linear=True)
    assert writer.done()
    for module in [casa, tools]:
        path = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromshape(path, [4], linear=True)
        assert image.calcmask(f'"{path}">1')
        assert image.replacemaskedpixels(f'sum("{source}")')
        np.testing.assert_array_equal(image.getchunk(), np.full(4, 2))
        assert image.done()


def test_replacement_accepts_a_conforming_numeric_vector(casa, tmp_path):
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromshape(name, [2], linear=True)
        assert image.calcmask(f'"{name}">1')
        for expression, expected in [
            ([10, 20], [10, 20]),
            ([10.5, 20.5], [10.5, 20.5]),
            ("[3.25,4.25]", [3.25, 4.25]),
        ]:
            assert image.replacemaskedpixels(expression)
            np.testing.assert_array_equal(image.getchunk(), expected)
        with pytest.raises(RuntimeError, match="shapes of operands mismatch"):
            image.replacemaskedpixels([1, 2, 3])
        np.testing.assert_array_equal(image.getchunk(), [3.25, 4.25])
        assert image.done()


def test_replacement_constructs_complex_values_from_two_images(casa, tmp_path):
    real, imag = str(tmp_path / "real.im"), str(tmp_path / "imag.im")
    complex_target = np.full((3, 4), 1 + 1j, dtype=np.complex64)
    real_values = np.arange(12, dtype=np.float32).reshape(3,4)
    for path, data in [(real, real_values), (imag, real_values + 20)]:
        writer = casa.image()
        assert writer.fromarray(path, data)
        assert writer.done()
    for module in [casa, tools]:
        path = str(tmp_path / f"{module.__name__}.im")
        writer = casa.image()
        assert writer.fromarray(path, complex_target)
        assert writer.calcmask(f'real("{path}")>2')
        assert writer.done()
        image = module.image()
        assert image.open(path)
        assert image.replacemaskedpixels(f'complex("{real}", "{imag}")')
        np.testing.assert_array_equal(image.getchunk(), real_values + 1j * (real_values + 20))
        assert image.done()


def test_replacement_using_an_external_mask_without_native_mask(casa, tmp_path):
    original = np.arange(16, dtype=np.float32).reshape(4, 4)
    name = str(tmp_path / "selector.im")
    selector = casa.image()
    assert selector.fromarray(name, original)
    assert selector.done()
    for module in [casa, tools]:
        image = module.image()
        assert image.fromarray("", original)
        with pytest.raises(RuntimeError, match="has no mask"):
            image.replacemaskedpixels(99)
        with pytest.raises(RuntimeError, match="no pixelmask available"):
            image.replacemaskedpixels(99, mask=f'"{name}">7', update=True)
        np.testing.assert_array_equal(image.getchunk(), np.where(original > 7, original, 99))
        assert image.getchunk(getmask=True).all()
        assert image.done()


def test_replacement_accepts_complex_scalar_and_keeps_history_setting(casa, tmp_path):
    real = np.arange(12, dtype=np.float32).reshape(3, 4)
    original = (real + 1j * real).astype(np.complex64)
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        # Prepare the complex mask through CASA for both replacement calls.
        writer = casa.image()
        assert writer.fromarray(name, original)
        assert writer.calcmask(f'real("{name}")>7')
        assert writer.done()
        image = module.image()
        assert image.open(name)
        image.dohistory(False)
        history = list(image.history())
        assert image.replacemaskedpixels("complex(2,3)", update=True)
        np.testing.assert_array_equal(image.getchunk(), np.where(real > 7, original, 2 + 3j))
        assert image.getchunk(getmask=True).all()
        assert list(image.history()) == history
        assert image.done()
        for reader in [casa.image(), tools.image()]:
            assert reader.open(name)
            np.testing.assert_array_equal(
                reader.getchunk(), np.where(real > 7, original, 2 + 3j)
            )
            assert reader.getchunk(getmask=True).all()
            assert reader.done()


def test_replacement_uses_region_validity_even_without_a_default_mask(casa):
    original = np.arange(49, dtype=np.float32).reshape(7, 7)
    results = []
    for module in [casa, tools]:
        image = module.image()
        assert image.fromarray("", original)
        coords = image.coordsys()
        region = casa.regionmanager().fromtext(
            "ellipse[[3pix, 3pix], [2pix, 1pix], 0deg]", list(original.shape), coords.torecord()
        )
        assert coords.done()
        assert image.replacemaskedpixels(99, region=region)
        array = image.getchunk()
        assert np.count_nonzero(array != original) == 8
        results.append(array)
        assert image.done()
    np.testing.assert_array_equal(*results)


def test_stretch_applies_only_to_the_external_mask(casa, tmp_path):
    spatial = np.arange(16, dtype=np.float32).reshape(4, 4)
    original = np.stack([spatial, spatial + 1, spatial + 2], axis=-1)
    mask_path = str(tmp_path / "mask-selector.im")
    writer = casa.image()
    assert writer.fromarray(mask_path, spatial)
    assert writer.done()
    for module in [casa, tools]:
        image = module.image()
        assert image.fromarray("", original)
        mask = f'"{mask_path}">7'
        with pytest.raises(RuntimeError, match="mask should not be extended"):
            image.replacemaskedpixels(99, mask=mask)
        np.testing.assert_array_equal(image.getchunk(), original)
        with pytest.raises(RuntimeError, match="shapes of operands mismatch"):
            image.replacemaskedpixels(f'"{mask_path}"', mask=mask, stretch=True)
        np.testing.assert_array_equal(image.getchunk(), original)
        assert image.replacemaskedpixels(99, mask=mask, stretch=True)
        np.testing.assert_array_equal(image.getchunk(), np.where(spatial[..., None] > 7, original, 99))
        assert image.done()


def test_attached_fits_replacement_is_read_only(casa, tmp_path):
    path = tmp_path / "source.fits"
    original = _hdu(True, [1, 2, 3, 4])
    path.write_bytes(original)
    for module in [casa, tools]:
        image = module.image()
        assert image.open(str(path))
        with pytest.raises(RuntimeError, match="not writable"):
            image.replacemaskedpixels(99)
        assert image.done()
    assert path.read_bytes() == original
