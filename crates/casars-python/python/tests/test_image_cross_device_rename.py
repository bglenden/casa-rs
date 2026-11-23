"""Native image rename preserves table data and ownership across filesystems."""

import os
import tempfile
from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_image_removal import _backend


@pytest.fixture
def other_filesystem(tmp_path):
    build = Path(__file__).resolve().parents[4] / "target"
    if not build.is_dir() or build.stat().st_dev == tmp_path.stat().st_dev:
        pytest.skip("the build and temporary directories share a filesystem")
    with tempfile.TemporaryDirectory(dir=build) as path:
        yield Path(path)


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("destination", ["new", "table", "file", "symlink"])
def test_native_rename_cross_device_keeps_table_layout_and_pixels(
    tmp_path, other_filesystem, writer, destination,
):
    casa = _backend("casa")
    api = casa if writer == "casa" else tools
    source = tmp_path / "source.im"
    target = other_filesystem / "target.im"
    referent = other_filesystem / "referent.im"
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    image, previous = api.image(), api.image()
    try:
        assert image.fromarray(str(source), pixels)
        assert image.calcmask(f'"{source}" > 4', "valid")
        assert image.setbrightnessunit("Jy/pixel")
        assert image.sethistory("probe", ["input provenance"])
        if destination == "file":
            target.write_text("original")
        elif destination in ("table", "symlink"):
            path = target if destination == "table" else referent
            assert previous.fromarray(str(path), pixels + 60)
            assert previous.done()
            if destination == "symlink":
                target.symlink_to(referent, target_is_directory=True)
        assert image.rename(str(target), overwrite=destination != "new")
        assert not source.exists()
        assert image.name() == str(target)
        assert target.is_dir() and not target.is_symlink()
        np.testing.assert_array_equal(image.getchunk(), pixels)
        np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 4)
        assert image.brightnessunit() == "Jy/pixel"
        assert image.history()[-4] == "input provenance"
        assert image.history()[-1] == f"original name was {source}"
        if destination == "symlink":
            assert previous.open(str(referent))
            np.testing.assert_array_equal(previous.getchunk(), pixels + 60)
        assert image.putchunk(np.array([[123.0]], dtype=np.float32), blc=[0, 0])
    finally:
        previous.done()
        image.done()

    pixels[0, 0] = 123.0
    expected_mask = pixels > 4
    expected_mask[0, 0] = False
    for reader in (casa, tools):
        reopened = reader.image()
        try:
            assert reopened.open(str(target))
            np.testing.assert_array_equal(reopened.getchunk(), pixels)
            np.testing.assert_array_equal(reopened.getchunk(getmask=True), expected_mask)
            assert reopened.brightnessunit() == "Jy/pixel"
        finally:
            reopened.done()


def test_cross_device_failed_history_write_restores_both_tables(tmp_path, other_filesystem):
    source = tmp_path / "source.im"
    target = other_filesystem / "target.im"
    owner, previous = tools.image(), tools.image()
    try:
        assert owner.fromarray(str(source), np.full((3, 4), 7, dtype=np.float32))
        assert previous.fromarray(str(target), np.full((3, 4), 9, dtype=np.float32))
        assert previous.done()
        log = source / "logtable" / "table.dat"
        content = log.read_bytes()
        log.write_bytes(b"invalid CASA table")
        try:
            with pytest.raises(RuntimeError):
                owner.rename(str(target), overwrite=True)
            assert not owner.isopen()
            assert log.read_bytes() == b"invalid CASA table"
        finally:
            log.write_bytes(content)
        assert owner.open(str(source))
        assert previous.open(str(target))
        np.testing.assert_array_equal(owner.getchunk(), 7)
        np.testing.assert_array_equal(previous.getchunk(), 9)
    finally:
        previous.done()
        owner.done()


def test_cross_device_move_preserves_sparse_tile_storage(tmp_path, other_filesystem):
    source = tmp_path / "sparse.im"
    target = other_filesystem / "sparse.im"
    image = tools.image()
    try:
        assert image.fromshape(str(source), [512, 512, 32])
        assert image.putchunk(np.array([[[18.0]]], dtype=np.float32), blc=[4, 7, 15])
        tiled = max((path for path in source.iterdir() if path.is_file()),
                    key=lambda path: path.stat().st_size)
        original = tiled.stat()
        if original.st_blocks * 512 >= original.st_size // 2:
            pytest.skip("the source filesystem did not create a sparse tiled file")
        assert image.rename(str(target))
        assert not source.exists()
        result = (target / tiled.name).stat()
        assert result.st_size == original.st_size
        assert result.st_blocks * 512 < result.st_size // 2
        assert image.getchunk(blc=[4, 7, 15], trc=[4, 7, 15]).item() == 18.0
        assert image.getchunk(blc=[100, 100, 20], trc=[100, 100, 20]).item() == 0.0
    finally:
        image.done()


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0,
                    reason="requires ordinary Unix directory permissions")
def test_cross_device_removal_preflight_preserves_both_tables(tmp_path, other_filesystem):
    source = tmp_path / "source.im"
    target = other_filesystem / "target.im"
    image, previous = tools.image(), tools.image()
    locked = source / "extra"
    try:
        assert image.fromarray(str(source), np.full((3, 4), 7, dtype=np.float32))
        assert previous.fromarray(str(target), np.full((3, 4), 9, dtype=np.float32))
        assert previous.done()
        locked.mkdir()
        (locked / "keep").write_text("recovery data")
        locked.chmod(0o575)
        with pytest.raises(RuntimeError, match="Cannot move native image.*Permission denied"):
            image.rename(str(target), overwrite=True)
        assert not image.isopen()
        assert (locked / "keep").read_text() == "recovery data"
        assert image.open(str(source))
        assert previous.open(str(target))
        np.testing.assert_array_equal(image.getchunk(), 7.0)
        np.testing.assert_array_equal(previous.getchunk(), 9.0)
    finally:
        if locked.exists():
            locked.chmod(0o755)
        image.done()
        previous.done()
