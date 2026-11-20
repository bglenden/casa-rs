"""Native and FITS image.insert geometry and mask parity with CASA 6.7.6.14."""

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_insert_reacquires_a_native_target_write_lock(casa, tmp_path):
    for backend, peer in ((casa, tools), (tools, casa)):
        source = str(tmp_path / f"{backend.__name__}-source.im")
        target = str(tmp_path / f"{backend.__name__}-target.im")
        values = np.array([[1, 2], [3, 4]], dtype=np.float32)
        image = backend.image()
        assert image.fromarray(source, values) and image.done()
        assert image.fromshape(target, [4, 4])
        assert image.unlock()
        assert image.insert(source, locate=[1, 1])
        np.testing.assert_array_equal(image.haslock(), [True, True])
        assert image.done()
        reader = peer.image()
        assert reader.open(target)
        expected = np.zeros((4, 4), dtype=np.float32)
        expected[1:3, 1:3] = values
        np.testing.assert_array_equal(reader.getchunk(), expected)
        assert reader.done()


def test_insert_reads_one_generation_during_a_local_source_edit(tmp_path):
    path = str(tmp_path / "source.im")
    shape = (1536, 1536)
    first = np.full(shape, 1, dtype=np.float32)
    second = np.full(shape, 2, dtype=np.float32)
    source, target = tools.image(), tools.image()
    assert source.fromarray(path, first)
    assert target.fromshape(shape=shape)
    start = Barrier(2)

    def edit_source():
        start.wait(timeout=10)
        for _ in range(10):
            assert source.putchunk(second, locking=False)
            assert source.putchunk(first, locking=False)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            editing = pool.submit(edit_source)
            start.wait(timeout=10)
            for _ in range(4):
                assert target.insert(path, locate=[0, 0])
                pixels = target.getchunk()
                assert pixels.min() == pixels.max() and pixels.flat[0] in (1, 2)
            editing.result(timeout=30)
    finally:
        assert target.done() and source.done()


def test_insert_detached_and_empty_intersection(casa, tmp_path):
    source = str(tmp_path / "source.im")
    writer = casa.image()
    assert writer.fromshape(source, [2, 2])
    assert writer.done()
    for module in [casa, tools]:
        image = module.image()
        assert image.insert(source) is False
        assert image.fromshape(shape=[4, 4])
        for locate in [[10, 10], [-10, -10]]:
            assert image.insert(source, locate=locate)
            np.testing.assert_array_equal(image.getchunk(), np.zeros((4, 4)))
        assert image.done()


@pytest.mark.parametrize(
    "locate, destination, input_section",
    [
        (None, np.s_[1:3, 1:3], np.s_[:, :]),
        ([0.6], np.s_[1:3, 1:3], np.s_[:, :]),
        ([-1, -1], np.s_[:2, :2], np.s_[:, :]),
        ([-2, -2], np.s_[:1, :1], np.s_[1:2, 1:2]),
    ],
)
def test_insert_aligns_centers_or_clips_and_transfers_validity(
    casa, tmp_path, locate, destination, input_section,
):
    source = str(tmp_path / "source.im")
    data = np.arange(4, dtype=np.float32).reshape(2, 2) + 10
    writer = casa.image()
    assert writer.fromarray(source, data)
    assert writer.calcmask(f'"{source}">11')
    assert writer.done()
    expected = np.zeros((4, 4), dtype=np.float32)
    expected[destination] = data[input_section]
    validity = np.zeros((4, 4), dtype=bool)
    validity[destination] = data[input_section] > 11
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromshape(name, [4, 4])
        assert image.calcmask(f'"{name}">0')
        kwargs = {} if locate is None else {"locate": locate}
        assert image.insert(source, **kwargs)
        assert image.done()
        for reader in [casa.image(), tools.image()]:
            assert reader.open(name)
            np.testing.assert_array_equal(reader.getchunk(), expected)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), validity)
            assert reader.history()[-2] == "Ran ia.insert"
            assert reader.done()


def test_insert_crops_complex_pixels_and_handles_overlapping_same_image(casa, tmp_path):
    data = np.arange(16, dtype=np.float32).reshape(4, 4).astype(np.complex64) + 3j
    expected = data.copy()
    expected[1:, 1:] = data[:3, :3]
    crop = casa.regionmanager().box([0, 0], [2, 2])
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromarray(name, data)
        assert image.insert(name, region=crop, locate=[1, 1])
        np.testing.assert_array_equal(image.getchunk(), expected)
        assert image.getchunk(getmask=True).all()
        assert image.done()


@pytest.mark.parametrize("locate,expected_start", [([0, 0], (0, 0)), ([-1], (2, 1))])
@pytest.mark.parametrize("saved", [False, True])
def test_insert_samples_strided_source_pixels_and_mask(
    casa, tmp_path, locate, expected_start, saved,
):
    data = np.arange(15, dtype=np.float32).reshape(5, 3) + 1
    source = str(tmp_path / "strided-source.im")
    writer = casa.image()
    assert writer.fromarray(source, data)
    assert writer.calcmask(f'"{source}">7')
    assert writer.done()
    regions = casa.regionmanager()
    selection = regions.box([0, 0], [4, 2], [2, 1])
    if saved:
        region_file = str(tmp_path / "strided.rgn")
        assert regions.tofile(region_file, selection)
        selection = region_file
    for module in [casa, tools]:
        path = str(tmp_path / f"{module.__name__}.strided.im")
        image = module.image()
        assert image.fromshape(path, [6, 5])
        assert image.calcmask(f'"{path}">0')
        assert image.insert(source, region=selection, locate=locate)
        x, y = expected_start
        expected = np.zeros((6, 5), dtype=np.float32)
        expected[x:x + 3, y:y + 3] = data[::2]
        np.testing.assert_array_equal(image.getchunk(), expected)
        np.testing.assert_array_equal(image.getchunk(getmask=True), expected > 7)
        assert image.done()


@pytest.mark.parametrize(
    "text,section",
    [
        ("circle[[4pix, 4pix], 1pix]", np.s_[1:4, 3:6]),
        ("box[[3pix, 3pix], [5pix, 5pix]]", np.s_[1:4, 3:6]),
        ("ellipse[[4pix, 4pix], [2pix, 1pix], 0deg]", np.s_[1:4, 2:7]),
    ],
)
def test_insert_interprets_text_in_destination_coordinates(casa, tmp_path, text, section):
    data = np.arange(81, dtype=np.float32).reshape(9, 9)
    source = str(tmp_path / "world-source.im")
    writer = casa.image()
    assert writer.fromarray(source, data)
    assert writer.done()
    count = data[section].shape
    expected = np.zeros_like(data)
    expected[:count[0], :count[1]] = data[section]
    for module in [casa, tools]:
        image = module.image()
        assert image.fromshape(shape=[9, 9])
        coords = image.coordsys()
        assert coords.setreferencepixel([6., 4.])
        assert image.setcoordsys(coords.torecord())
        assert coords.done()
        assert image.insert(source, region=text, locate=[0, 0])
        np.testing.assert_array_equal(image.getchunk(), expected)
        assert image.done()


def test_insert_interprets_crtf_channel_numbers_in_destination_coordinates(casa, tmp_path):
    data = np.arange(450, dtype=np.float32).reshape(5, 5, 2, 9) + 1
    source = str(tmp_path / "spectral-source.im")
    writer = casa.image()
    assert writer.fromarray(source, data)
    assert writer.done()
    expected = np.zeros((7, 7, 2, 9), dtype=np.float32)
    expected[:3, :3, :, :2] = data[:3, :3, :, 4:6]
    for module in [casa, tools]:
        image = module.image()
        assert image.fromshape(shape=[7, 7, 2, 9])
        coords = image.coordsys()
        assert coords.setreferencepixel([3., 3., 0., 6.])
        assert image.setcoordsys(coords.torecord())
        assert coords.done()
        text = "box[[1pix,1pix],[3pix,3pix]], range=[6pix,7pix]"
        assert image.insert(source, region=text, locate=[0, 0, 0, 0])
        np.testing.assert_array_equal(image.getchunk(), expected)
        assert image.done()


def test_insert_snapshots_overlapping_native_pixels_above_memory_budget(casa, tmp_path):
    size = 1510
    data = np.arange(size * size, dtype=np.float32).reshape(size, size)
    path = str(tmp_path / "overlap.im")
    image = tools.image()
    assert image.fromarray(path, data)
    region = casa.regionmanager().box([0, 0], [size - 2, size - 2])
    assert image.insert(path, region=region, locate=[1, 1])
    assert image.done()
    data[1:, 1:] = data[:-1, :-1].copy()
    reference_reader = casa.image()
    assert reference_reader.open(path)
    np.testing.assert_array_equal(reference_reader.getchunk(), data)
    assert reference_reader.done()


def test_insert_preserves_the_readonly_target_mask(casa, tmp_path):
    source = str(tmp_path / "writable-pixels.im")
    data = np.ones((2, 2), dtype=np.float32)
    writer = casa.image()
    assert writer.fromarray(source, data)
    assert writer.done()
    for module in [casa, tools]:
        path = tmp_path / f"{module.__name__}.readonly-mask.im"
        image = module.image()
        assert image.fromshape(str(path), [4, 4])
        assert image.calcmask(f'"{path}">0', name="blocked_mask")
        assert image.done()
        mask_file = path / "blocked_mask" / "table.dat"
        mode = mask_file.stat().st_mode
        mask_file.chmod(0o444)
        try:
            if os.access(mask_file, os.W_OK):
                pytest.skip("requires an account without write access to the mask")
            assert image.open(str(path))
            assert image.insert(source, locate=[1, 1])
            expected = np.zeros((4, 4), dtype=np.float32)
            expected[1:3, 1:3] = data
            np.testing.assert_array_equal(image.getchunk(), expected)
            assert not image.getchunk(getmask=True).any()
            assert image.done()
        finally:
            mask_file.chmod(mode)


def test_insert_reads_fits_pixels_and_masks_without_changing_the_source(casa, tmp_path):
    native = str(tmp_path / "source.im")
    path = tmp_path / "source.fits"
    data = np.arange(16, dtype=np.float32).reshape(4, 4)
    writer = casa.image()
    assert writer.fromarray(native, data)
    assert writer.calcmask(f'"{native}">7')
    assert writer.tofits(str(path))
    assert writer.done()
    original = path.read_bytes()
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromshape(name, [4, 4])
        assert image.calcmask(f'"{name}">0')
        assert image.insert(str(path), locate=[0, 0])
        np.testing.assert_array_equal(image.getchunk(), np.where(data > 7, data, np.nan))
        np.testing.assert_array_equal(image.getchunk(getmask=True), data > 7)
        assert image.done()
    assert path.read_bytes() == original


def test_insert_clips_the_validity_window_of_a_saved_ellipsoid(casa, tmp_path):
    data = np.arange(49, dtype=np.float32).reshape(7, 7)
    source = str(tmp_path / "source.im")
    writer = casa.image()
    assert writer.fromarray(source, data)
    coords = writer.coordsys()
    region = casa.regionmanager().fromtext(
        "ellipse[[3pix, 3pix], [2pix, 1pix], 0deg]", list(data.shape), coords.torecord()
    )
    assert coords.done()
    assert writer.done()
    results = []
    for module in [casa, tools]:
        name = str(tmp_path / f"{module.__name__}.im")
        image = module.image()
        assert image.fromshape(name, [3, 3])
        assert image.calcmask(f'"{name}">0')
        assert image.insert(source, region=region, locate=[0, -2])
        results.append((image.getchunk(), image.getchunk(getmask=True)))
        assert image.done()
    (expected_pixels, expected_mask), (pixels, mask) = results
    assert expected_mask.any() and not expected_mask.all()
    np.testing.assert_array_equal(pixels, expected_pixels)
    np.testing.assert_array_equal(mask, expected_mask)


def test_insert_rejects_one_empty_clipped_axis_without_editing(tmp_path):
    name = str(tmp_path / "source.im")
    source = tools.image()
    assert source.fromshape(name, [2, 2])
    assert source.done()
    target = tools.image()
    assert target.fromshape(shape=[4, 4])
    for locate in [[10, 1], [-10, 1]]:
        with pytest.raises(RuntimeError, match="Slicer error:end<start"):
            target.insert(name, locate=locate)
    np.testing.assert_array_equal(target.getchunk(), np.zeros((4, 4)))
    assert target.done()
