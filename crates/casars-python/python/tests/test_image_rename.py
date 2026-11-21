"""Native table rename semantics through the image tool."""

import numpy as np
import pytest

from casars import tools
from test_image_removal import _backend


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("kind", ["f", "c"])
def test_rename_persistent_image_and_reopen(tmp_path, writer, kind):
    casa = _backend("casa")
    module, other = (casa, tools) if writer == "casa" else (tools, casa)
    old, new = tmp_path / "source.im", tmp_path / "renamed.im"
    image = module.image()
    assert image.fromshape(str(old), [2, 2], type=kind)
    data = np.arange(4, dtype=np.float64).reshape(2, 2)
    if kind == "c":
        data = data + 2j
    assert image.putchunk(data)
    assert image.rename(str(new))
    assert image.name() == str(new)
    assert not old.exists() and new.is_dir()
    assert image.history()[-3:] == [
        "Ran ia.rename", f'ia.rename(name="{new}", overwrite=false)', f"original name was {old}",
    ]
    assert image.done()
    reader = other.image()
    assert reader.open(str(new))
    np.testing.assert_array_equal(reader.getchunk(), data)
    assert reader.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("target_kind", ["image", "directory", "file"])
def test_rename_overwrite_only_after_explicit_request(tmp_path, monkeypatch, backend, target_kind):
    module = _backend(backend)
    source, output = tmp_path / "original.im", tmp_path / "existing"
    image = module.image()
    assert image.fromshape(str(source), [2, 2])
    if target_kind == "image":
        other = module.image()
        assert other.fromshape(str(output), [3, 2])
        assert other.done()
    elif target_kind == "directory":
        output.mkdir()
        (output / "keep").write_text("before rename", encoding="utf-8")
    else:
        output.write_text("before rename", encoding="utf-8")
    with pytest.raises(RuntimeError, match="does not want to remove"):
        image.rename(str(output))
    assert not image.isopen()
    assert source.is_dir() and output.exists()
    if target_kind == "directory":
        assert (output / "keep").read_text(encoding="utf-8") == "before rename"
    elif target_kind == "file":
        assert output.read_text(encoding="utf-8") == "before rename"
    assert image.open(str(source))
    assert image.rename(str(output), True)
    assert image.shape().tolist() == [2, 2]
    with pytest.raises(RuntimeError, match="same as the current image name"):
        image.rename(str(output), overwrite=True)
    assert not image.isopen()
    assert image.open(str(output))
    monkeypatch.chdir(tmp_path)
    assert image.rename(output.name, overwrite=True)
    assert not source.exists() and output.is_dir()
    assert image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_attachment_failures(tmp_path, backend):
    module = _backend(backend)
    tool = module.image()
    target = str(tmp_path / "target.im")
    assert tool.rename(target) is False
    assert tool.fromshape(shape=[2, 2])
    with pytest.raises(RuntimeError, match="cannot be renamed"):
        tool.rename(target)
    assert not tool.isopen()
    assert tool.fromshape(str(tmp_path / "double.im"), [2, 2], type="d")
    with pytest.raises(RuntimeError, match="double precision"):
        tool.rename(target)
    assert tool.isopen()
    assert tool.done()


def test_rename_refuses_a_move_while_another_tool_is_attached(tmp_path):
    old, new = tmp_path / "shared.im", tmp_path / "renamed.im"
    first, second = tools.image(), tools.image()
    assert first.fromshape(str(old), [2, 2])
    assert second.open(str(old))
    with pytest.raises(RuntimeError, match="still open"):
        first.rename(str(new))
    assert not first.isopen()
    assert old.is_dir() and not new.exists()
    assert second.shape().tolist() == [2, 2]
    assert second.done()


def test_rename_restores_both_images_if_history_write_fails(tmp_path):
    old, new = tmp_path / "source.im", tmp_path / "destination.im"
    source, destination = tools.image(), tools.image()
    assert source.fromshape(str(old), [2, 2])
    assert destination.fromshape(str(new), [3, 2])
    assert destination.done()
    log = old / "logtable" / "table.dat"
    original_log = log.read_bytes()
    log.write_bytes(b"invalid CASA table")
    with pytest.raises(RuntimeError):
        source.rename(str(new), overwrite=True)
    assert not source.isopen()
    assert old.is_dir() and new.is_dir()
    assert log.read_bytes() == b"invalid CASA table"
    log.write_bytes(original_log)
    assert source.open(str(old))
    assert source.shape().tolist() == [2, 2]
    assert source.done()
    assert destination.open(str(new))
    assert destination.shape().tolist() == [3, 2]
    assert destination.done()


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("target_link", ["separate", "source"])
def test_rename_native_image_moves_past_links_without_changing_referent(
    tmp_path, writer, target_link,
):
    casa = _backend("casa")
    api = casa if writer == "casa" else tools
    source = tmp_path / "source.im"
    alias = tmp_path / "source-link.im"
    output = tmp_path / "output.im"
    referent = tmp_path / "existing.im"
    owner = api.image()
    peer = api.image()
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    try:
        assert owner.fromarray(str(source), pixels)
        assert owner.calcmask(f'"{source}" > 4', "valid")
        assert owner.setbrightnessunit("Jy/pixel")
        assert owner.done()
        alias.symlink_to(source, target_is_directory=True)
        if target_link == "separate":
            assert peer.fromarray(str(referent), pixels + 50)
            assert peer.done()
            assert peer.open(str(referent))
            output.symlink_to(referent, target_is_directory=True)
        else:
            output.symlink_to(source, target_is_directory=True)
        assert owner.open(str(alias))
        assert owner.rename(str(output), overwrite=True)
        assert owner.name() == str(output)
        assert not source.exists()
        assert alias.is_symlink() and not alias.exists()
        assert output.is_dir() and not output.is_symlink()
        if target_link == "separate":
            assert referent.is_dir()
            np.testing.assert_array_equal(peer.getchunk(), pixels + 50)
        assert owner.history()[-3:] == [
            "Ran ia.rename", f'ia.rename(name="{output}", overwrite=true)',
            f"original name was {alias}",
        ]
    finally:
        owner.done()
        peer.done()

    for reader in (casa, tools):
        reopened = reader.image()
        try:
            assert reopened.open(str(output))
            np.testing.assert_array_equal(reopened.getchunk(), pixels)
            np.testing.assert_array_equal(reopened.getchunk(getmask=True), pixels > 4)
            assert reopened.brightnessunit() == "Jy/pixel"
        finally:
            reopened.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_rejects_a_dangling_destination_link(tmp_path, backend):
    api = _backend(backend)
    source = tmp_path / "source.im"
    output = tmp_path / "output.im"
    missing = tmp_path / "absent.im"
    output.symlink_to(missing, target_is_directory=True)
    image = api.image()
    try:
        assert image.fromarray(str(source), np.ones((3, 4), dtype=np.float32))
        with pytest.raises(RuntimeError, match="target file .* already exists and is not writable"):
            image.rename(str(output), overwrite=True)
        assert not image.isopen()
        assert output.is_symlink() and output.readlink() == missing
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), 1)
    finally:
        image.done()


def test_rename_restores_source_and_target_links_after_history_failure(tmp_path):
    source = tmp_path / "source.im"
    alias = tmp_path / "source-link.im"
    output = tmp_path / "output.im"
    referent = tmp_path / "existing.im"
    owner = tools.image()
    other = tools.image()
    try:
        assert owner.fromarray(str(source), np.full((3, 4), 7, dtype=np.float32))
        assert owner.done()
        assert other.fromarray(str(referent), np.full((3, 4), 9, dtype=np.float32))
        alias.symlink_to(source, target_is_directory=True)
        output.symlink_to(referent, target_is_directory=True)
        assert owner.open(str(alias))
        log = source / "logtable" / "table.dat"
        original = log.read_bytes()
        log.write_bytes(b"invalid CASA table")
        try:
            with pytest.raises(RuntimeError):
                owner.rename(str(output), overwrite=True)
            assert not owner.isopen()
            assert alias.is_symlink() and alias.resolve() == source
            assert output.is_symlink() and output.resolve() == referent
            assert source.is_dir() and referent.is_dir()
        finally:
            log.write_bytes(original)
        assert owner.open(str(source))
        np.testing.assert_array_equal(owner.getchunk(), 7)
        np.testing.assert_array_equal(other.getchunk(), 9)
    finally:
        owner.done()
        other.done()
