"""Rename directly attached FITS files through the public image tool."""

import os
import tempfile
from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_rename_fits_reopens_owner_and_retains_a_peer_stream(request, backend, overwrite, tmp_path):
    module = request.getfixturevalue("casa") if backend == "casa" else tools
    source = tmp_path / "input.fits"
    target = tmp_path / "renamed.fits"
    content = _hdu(True, [1, 2, 3, 4])
    source.write_bytes(content)
    if overwrite:
        target.write_bytes(b"existing target")
    owner, peer, dormant = (module.image() for _ in range(3))
    for tool in (owner, peer, dormant):
        assert tool.open(str(source))
    assert owner.setbrightnessunit("Jy")
    assert owner.setmiscinfo({"temporary": True})
    assert owner.sethistory("test", ["attachment-only"])
    assert peer.ispersistent()
    pixels = [[1.0, 3.0], [2.0, 4.0]]
    np.testing.assert_array_equal(peer.getchunk(), pixels)
    assert owner.rename(str(target), overwrite=overwrite)
    assert not source.exists()
    assert target.read_bytes() == content
    assert owner.isopen() and owner.ispersistent()
    assert owner.name() == str(target)
    assert owner.brightnessunit() == "Jy/beam"
    assert owner.miscinfo() == {"foobar": 73}
    assert owner.history() == [
        "first custom history",
        "Ran ia.rename",
        f'ia.rename(name="{target}", overwrite={str(overwrite).lower()})',
        f"original name was {source}",
    ]
    assert peer.name() == str(source)
    np.testing.assert_array_equal(peer.getchunk(), pixels)
    with pytest.raises(RuntimeError):
        dormant.getchunk()
    for tool in (owner, peer, dormant):
        assert tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("case", ["collision", "same", "directory", "missing_parent", "missing_source"])
def test_rename_fits_error_state_and_paths(request, backend, case, tmp_path):
    module = request.getfixturevalue("casa") if backend == "casa" else tools
    source = tmp_path / "input.fits"
    target = tmp_path / "target.fits"
    content = _hdu(True, [1, 2, 3, 4])
    source.write_bytes(content)
    owner = module.image()
    assert owner.open(str(source))
    overwrite = True
    if case == "collision":
        target.write_bytes(b"existing target")
        overwrite = False
        error = "exists, and the user does not want to remove it"
    elif case == "same":
        target = source
        error = "same as the current image name"
    elif case == "directory":
        target.mkdir()
        error = "Unable to open image"
    elif case == "missing_parent":
        target = tmp_path / "missing" / "target.fits"
        error = "target directory .* is not writable"
    else:
        source.unlink()
        error = "image to be renamed no longer exists"

    with pytest.raises(RuntimeError, match=error):
        owner.rename(str(target), overwrite=overwrite)
    assert not owner.isopen()
    if case == "directory":
        assert not source.exists()
        assert (target / source.name).read_bytes() == content
    elif case != "missing_source":
        assert source.read_bytes() == content
    if case == "collision":
        assert target.read_bytes() == b"existing target"


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_fits_moves_the_source_link_target_and_replaces_the_output_link(request, backend, tmp_path):
    module = request.getfixturevalue("casa") if backend == "casa" else tools
    original = tmp_path / "original.fits"
    source = tmp_path / "source.fits"
    destination = tmp_path / "destination.fits"
    old_target = tmp_path / "old-target"
    content = _hdu(True, [1, 2, 3, 4])
    original.write_bytes(content)
    old_target.write_bytes(b"previous destination")
    source.symlink_to(original)
    destination.symlink_to(old_target)
    owner = module.image()
    assert owner.open(str(source))
    assert owner.dohistory(False)
    assert owner.rename(str(destination), overwrite=True)
    assert not original.exists() and source.is_symlink() and not source.exists()
    assert not destination.is_symlink()
    assert destination.read_bytes() == content
    assert old_target.read_bytes() == b"previous destination"
    assert owner.history() == ["first custom history"]
    assert owner.done()


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_fits_between_filesystems(request, tmp_path, overwrite, backend):
    module = request.getfixturevalue("casa") if backend == "casa" else tools
    target_root = Path(__file__).resolve().parents[4] / "target"
    if not target_root.is_dir() or target_root.stat().st_dev == tmp_path.stat().st_dev:
        pytest.skip("the build directory and temporary files share a filesystem")
    source = tmp_path / "input.fits"
    content = _hdu(True, [1, 2, 3, 4])
    source.write_bytes(content)
    owner = module.image()
    assert owner.open(str(source))
    with tempfile.TemporaryDirectory(dir=target_root) as directory:
        target = Path(directory) / "target.fits"
        if overwrite:
            target.write_bytes(b"existing target")
        assert owner.rename(str(target), overwrite=overwrite)
        assert not source.exists()
        assert target.read_bytes() == content
        np.testing.assert_array_equal(owner.getchunk(), [[1.0, 3.0], [2.0, 4.0]])
        assert owner.done()


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs unprivileged file permissions")
def test_cross_filesystem_rename_keeps_destination_when_source_cannot_be_removed(tmp_path):
    target_root = Path(__file__).resolve().parents[4] / "target"
    if not target_root.is_dir() or target_root.stat().st_dev == tmp_path.stat().st_dev:
        pytest.skip("the build directory and temporary files share a filesystem")
    source = tmp_path / "input.fits"
    content = _hdu(True, [1, 2, 3, 4])
    source.write_bytes(content)
    owner = tools.image()
    assert owner.open(str(source))
    with tempfile.TemporaryDirectory(dir=target_root) as directory:
        target = Path(directory) / "target.fits"
        target.write_bytes(b"existing target")
        tmp_path.chmod(0o555)
        try:
            with pytest.raises(RuntimeError):
                owner.rename(str(target), overwrite=True)
        finally:
            tmp_path.chmod(0o700)
        assert not owner.isopen()
        assert source.read_bytes() == content
        assert target.read_bytes() == b"existing target"


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs unprivileged file permissions")
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_fits_checks_effective_target_write_access(request, backend, tmp_path):
    module = request.getfixturevalue("casa") if backend == "casa" else tools
    source = tmp_path / "input.fits"
    target = tmp_path / "target.fits"
    content = _hdu(True, [1, 2, 3, 4])
    source.write_bytes(content)
    target.write_bytes(b"existing target")
    owner = module.image()
    assert owner.open(str(source))
    # The owner permission class has no write bit. Group bits cannot grant
    # access to the owner even though some write bits are set on the file.
    target.chmod(0o024)
    try:
        with pytest.raises(RuntimeError, match="already exists and is not writable"):
            owner.rename(str(target), overwrite=True)
    finally:
        target.chmod(0o600)
    assert not owner.isopen()
    assert source.read_bytes() == content
    assert target.read_bytes() == b"existing target"
