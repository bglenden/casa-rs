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
