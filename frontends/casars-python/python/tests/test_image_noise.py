"""Seeded distributions and region updates through the public image tool."""

import os

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize(
    "kind,parameters",
    [
        ("binomial", [4., 0.4]),
        ("discreteuniform", [-3., 11.]),
        ("erlang", [3., 2.]),
        ("geometric", [0.4]),
        ("hypergeometric", [0.5, 2.]),
        ("Normal", [0., 1.]),
        ("lognormal", [2., 1.]),
        ("negativeexponential", [2.]),
        ("poisson", [2.]),
        ("uniform", [-0.5, 1.5]),
        ("weibull", [2., 3.]),
    ],
)
def test_addnoise_distributions_are_seeded_like_casa(casa, kind, parameters):
    reference = casa.image()
    image = tools.image()
    try:
        for tool in (reference, image):
            assert tool.fromshape(shape=[4, 3], type="d")
            assert tool.addnoise(kind, parameters, zero=True, seeds=[1, 2])
        expected = reference.getchunk()
        assert expected.dtype == image.getchunk().dtype == np.float64
        np.testing.assert_allclose(image.getchunk(), expected, rtol=2e-13, atol=1e-14)
        for tool in (reference, image):
            assert tool.addnoise(kind, parameters, seeds=[-323, 81, 999])
        np.testing.assert_allclose(image.getchunk(), reference.getchunk(), rtol=2e-13, atol=1e-14)
    finally:
        image.done()
        reference.done()


@pytest.mark.parametrize("pixel_type", ["f", "d", "c", "cd"])
def test_addnoise_normal_defaults_and_component_precision(casa, pixel_type):
    reference = casa.image()
    image = tools.image()
    try:
        for tool in (reference, image):
            assert tool.fromshape(shape=[4, 3], type=pixel_type)
            assert tool.addnoise(seeds=[32, -29], zero=True)
        actual, expected = image.getchunk(), reference.getchunk()
        assert actual.dtype == expected.dtype
        np.testing.assert_allclose(actual, expected, rtol=2e-13, atol=1e-14)
        assert image.history(list=False)[-2:] == reference.history(list=False)[-2:]
        if pixel_type in {"c", "cd"}:
            assert np.any(actual.real != actual.imag)
    finally:
        image.done()
        reference.done()


@pytest.mark.parametrize("region_kind", ["slicer", "circle", "text"])
def test_addnoise_writes_the_region_lattice_and_preserves_mask(casa, tmp_path, region_kind):
    original = np.arange(49, dtype=np.float32).reshape(7, 7) + 100
    reference_path = str(tmp_path / "reference.im")
    result_path = str(tmp_path / "result.im")
    reference = casa.image()
    image = tools.image()
    try:
        for tool, path in [(reference, reference_path), (image, result_path)]:
            assert tool.fromarray(path, original)
            assert tool.calcmask(f'"{path}">110')
        changed = np.zeros(original.shape, dtype=bool)
        if region_kind == "slicer":
            region = casa.regionmanager().box([1, 1], [5, 5])
            region["inc"] = np.array([2, 2], dtype=np.float32)
            changed[1:6:2, 1:6:2] = True
        else:
            region = "circle[[3pix,3pix], 2pix]"
            if region_kind == "circle":
                coords = reference.coordsys()
                region = casa.regionmanager().fromtext(region, [7, 7], coords.torecord())
                assert coords.done()
            changed[1:6, 1:6] = True
        for tool in (reference, image):
            assert tool.addnoise("uniform", [0, 1], region=region, zero=True, seeds=[1, 2])
        expected = reference.getchunk()
        np.testing.assert_array_equal(expected != original, changed)
        assert reference.done()
        assert image.done()
        for tool in (reference, image):
            assert tool.open(result_path)
            np.testing.assert_array_equal(tool.getchunk(), expected)
            np.testing.assert_array_equal(tool.getchunk(getmask=True), original > 110)
            assert tool.history()[-2] == "Ran ia.addnoise"
            assert tool.done()
    finally:
        image.done()
        reference.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_addnoise_reacquires_native_write_lock(casa, tmp_path):
    for backend, peer in ((casa, tools), (tools, casa)):
        path = str(tmp_path / f"{backend.__name__}.im")
        image = backend.image()
        assert image.fromshape(path, [3, 4])
        assert image.unlock()
        assert image.addnoise("uniform", [2, 3], seeds=[1, 2])
        np.testing.assert_array_equal(image.haslock(), [True, True])
        values = image.getchunk()
        assert ((values >= 2) & (values <= 3)).all()
        assert image.done()
        reader = peer.image()
        assert reader.open(path)
        np.testing.assert_array_equal(reader.getchunk(), values)
        assert reader.done()


def test_addnoise_reads_and_writes_multiple_tiles_with_casa_cursor_order(casa, tmp_path):
    reference = casa.image()
    image = tools.image()
    try:
        assert reference.fromshape(str(tmp_path / "reference.im"), [211, 193])
        assert image.fromshape(str(tmp_path / "rust.im"), [211, 193])
        for tool in (reference, image):
            assert tool.addnoise(seeds=[991, 22])
        np.testing.assert_array_equal(image.getchunk(), reference.getchunk())
    finally:
        image.done()
        reference.done()


def test_addnoise_writes_a_sparse_section_across_a_wide_window(casa):
    reference = casa.image()
    image = tools.image()
    try:
        region = casa.regionmanager().box([0, 0], [2048, 2048])
        region["inc"] = np.array([2048, 2048], dtype=np.float32)
        for tool in (reference, image):
            assert tool.fromshape(shape=[2049, 2049])
            assert tool.addnoise("uniform", [0, 1], region=region, seeds=[1, 2])
        actual, expected = image.getchunk(), reference.getchunk()
        assert np.count_nonzero(expected) == 4
        np.testing.assert_array_equal(actual, expected)
    finally:
        image.done()
        reference.done()


def test_addnoise_refuses_readonly_fits_attachments(casa, tmp_path):
    path = tmp_path / "noise.fits"
    producer = casa.image()
    assert producer.fromshape(shape=[4, 3])
    assert producer.tofits(str(path))
    assert producer.done()
    original = path.read_bytes()
    for image_type in (casa.image, tools.image):
        image = image_type()
        try:
            assert image.open(str(path))
            for zero in (False, True):
                with pytest.raises(RuntimeError, match="lattice is not writable"):
                    image.addnoise(zero=zero, seeds=[1, 2])
        finally:
            image.done()
    assert path.read_bytes() == original


def test_invalid_addnoise_keeps_previous_pixels_even_with_zero_requested():
    image = tools.image()
    assert image.addnoise() is False
    original = np.arange(9, dtype=np.float32).reshape(3, 3)
    try:
        assert image.fromarray(pixels=original)
        previous = image.history()
        for kind, parameters, error in [
            ("bogus", [], "called function 'set'"),
            ("normal", [0., -1.], "distribution parameters are illegal"),
            ("geometric", [0.], "distribution parameters are illegal"),
            ("poisson", [800.], "distribution parameters are illegal"),
            ("lognormal", [1e12, 1e24], "AlwaysAssert itsVariance > 0"),
            ("discreteuniform", [0., 1e30], "AlwaysAssert low <= high"),
        ]:
            with pytest.raises(RuntimeError, match=error):
                image.addnoise(kind, parameters, zero=True, seeds=[1, 2])
            np.testing.assert_array_equal(image.getchunk(), original)
            assert image.history() == previous
        for seeds in (None, [2**63, 3]):
            with pytest.raises(SystemError, match="returned a result with an exception set"):
                image.addnoise(zero=True, seeds=seeds)
            np.testing.assert_array_equal(image.getchunk(), original)
            assert image.history() == previous
    finally:
        image.done()


@pytest.mark.parametrize(
    "parameters,seeds",
    [
        (np.array([[0., 1.]]), np.array([[1.4, 2.], [3., 4.]])),
        ([False, True], [2**62 + 17, 3]),
        (["-1.5foo", "0x1p2"], ["1e2", "2"]),
        (["0", "1"], [1, float("nan")]),
    ],
)
def test_addnoise_argument_forms_preserve_seed_values_and_history(casa, parameters, seeds):
    reference = casa.image()
    image = tools.image()
    try:
        for tool in (reference, image):
            assert tool.fromshape(shape=[4, 3])
            assert tool.addnoise("uniform", parameters, seeds=seeds, zero=True)
        np.testing.assert_array_equal(image.getchunk(), reference.getchunk())
        assert image.history(list=False)[-2:] == reference.history(list=False)[-2:]
    finally:
        image.done()
        reference.done()


def test_addnoise_history_keeps_empty_string_and_limits_long_seed_arguments(casa):
    reference = casa.image()
    image = tools.image()
    try:
        for tool in (reference, image):
            assert tool.fromshape(shape=[4, 3])
            assert tool.addnoise(region="", seeds=list(range(200)))
        assert image.history(list=False)[-2:] == reference.history(list=False)[-2:]
        assert 'region="", zeroit=false, seeds=...' in image.history(list=False)[-1]
    finally:
        image.done()
        reference.done()


@pytest.mark.parametrize("compound", [False, True])
def test_addnoise_fills_the_bounds_of_polygon_and_compound_regions(casa, compound):
    reference = casa.image()
    image = tools.image()
    try:
        shape = [9, 9, 3]
        for tool in (reference, image):
            assert tool.fromshape(shape=shape)
        manager = casa.regionmanager()
        coords = reference.coordsys()
        record = coords.torecord()
        if compound:
            region = manager.makeunion({
                "first": manager.fromtext("circle[[2pix,2pix], 1pix]", shape, record),
                "second": manager.fromtext("circle[[6pix,6pix], 1pix]", shape, record),
            })
        else:
            region = manager.fromtext("poly[[2pix,2pix],[3pix,5pix],[5pix,3pix]]", shape, record)
        assert coords.done()
        for tool in (reference, image):
            assert tool.addnoise("uniform", [0, 1], region=region, seeds=[1, 2])
        np.testing.assert_array_equal(image.getchunk(), reference.getchunk())
        if compound:
            assert image.getchunk()[4, 4, 0] > 0
    finally:
        image.done()
        reference.done()
