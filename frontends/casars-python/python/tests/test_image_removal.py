"""Native CASA image removal and refused deletion through the public tools."""

import subprocess
import sys

import numpy as np
import pytest

from casars import tools


def _backend(name):
    if name == "casars":
        return tools
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    return casa


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("method, flags", [("remove", {}), ("remove", {"done": True}), ("done", {"remove": True})])
def test_delete_image_and_detach_tool(tmp_path, backend, method, flags):
    module = _backend(backend)
    target = tmp_path / "attached.im"
    tool = module.image()
    assert tool.remove() is False
    assert tool.fromshape(str(target), [2, 2], type="cd")
    assert getattr(tool, method)(verbose=False, **flags) is True
    assert not target.exists()
    assert not tool.isopen()
    assert tool.fromshape(shape=[2, 2])
    with pytest.raises(RuntimeError, match="cannot be deleted"):
        getattr(tool, method)(verbose=False, **flags)
    assert not tool.isopen()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_image_alias_blocks_removal_and_survives(tmp_path, backend):
    module = _backend(backend)
    target = str(tmp_path / "shared.im")
    output, alias = module.image(), module.image()
    expected = np.arange(4, dtype=np.float64).reshape(2, 2)
    assert output.fromarray(target, expected)
    assert alias.open(target)
    assert output.removefile(target) is False
    with pytest.raises(RuntimeError, match="table is still open in this process"):
        output.remove()
    assert not output.isopen()
    np.testing.assert_array_equal(alias.getchunk(), expected)
    assert alias.done()
    assert output.removefile(target) is True
    assert output.removefile(target) is False


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_removefile_handles_table_alias_and_refuses_other_files(tmp_path, backend):
    module = _backend(backend)
    path = tmp_path / "plain"
    path.mkdir()
    (path / "keep").write_text("keep me", encoding="utf-8")
    tool = module.image()
    assert tool.removefile("") is False
    assert tool.removefile(str(path)) is False
    assert tool.removefile(str(path / "keep")) is False
    assert (path / "keep").read_text(encoding="utf-8") == "keep me"
    target, alias = tmp_path / "native.im", tmp_path / "alias.im"
    assert tool.fromshape(str(target), [2, 2])
    assert tool.done()
    alias.symlink_to(target.name, target_is_directory=True)
    table = module.table()
    assert table.open(str(target))
    assert tool.removefile(str(target)) is False
    if backend == "casars":
        assert tool.removefile(str(alias)) is False
    assert target.is_dir() and alias.is_symlink()
    assert table.done()
    assert tool.removefile(str(alias)) is True
    assert not target.exists() and not alias.is_symlink()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_removefile_respects_an_unlocked_process(tmp_path, backend):
    module = _backend(backend)
    path = tmp_path / "external.im"
    source = module.image()
    assert source.fromshape(str(path), [2, 2])
    assert source.done()
    holder = subprocess.Popen(
        [sys.executable, "-u", "-c", """
import importlib
import sys
module = importlib.import_module(sys.argv[2])
holder = module.image()
assert holder.open(sys.argv[1])
if sys.argv[2] == "casatools":
    assert holder.unlock()
print('READY', flush=True)
sys.stdin.readline()
assert holder.done()
""", str(path), "casatools" if backend == "casa" else "casars.tools"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    tool = tools.image()
    try:
        assert holder.stdout.readline().strip() == "READY"
        assert tool.removefile(str(path)) is False
        assert path.is_dir()
    finally:
        _, stderr = holder.communicate("\n", timeout=15)
        assert holder.returncode == 0, stderr
    assert tool.removefile(str(path)) is True
    assert not path.exists()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_read_only_native_control_file_blocks_removal(tmp_path, backend):
    module = _backend(backend)
    image = module.image()
    path = tmp_path / "readonly.im"
    assert image.fromshape(str(path), [2, 2])
    assert image.done()
    control = path / "table.dat"
    mode = control.stat().st_mode & 0o777
    control.chmod(0o444)
    try:
        assert image.removefile(str(path)) is False
        assert control.is_file()
    finally:
        control.chmod(mode)
    assert image.removefile(str(path)) is True


def test_removefile_preserves_an_open_log_subtable(tmp_path):
    path = tmp_path / "parent.im"
    image = tools.image()
    assert image.fromshape(str(path), [2, 2])
    assert image.done()
    table = tools.table()
    assert table.open(str(path / "logtable"))
    assert image.removefile(str(path)) is False
    assert table.nrows() > 0
    assert table.done()
    assert image.removefile(str(path)) is True


def test_removefile_preserves_table_used_by_an_attached_ancestor(tmp_path):
    path = tmp_path / "holder.im"
    image = tools.image()
    assert image.fromshape(str(path), [2, 2])
    original = image.history()
    assert image.removefile(str(path / "logtable")) is False
    assert image.history() == original
    assert image.done()


def test_removal_preserves_attached_descendants(tmp_path):
    root = tmp_path / "parent.im"
    leaf = root / "child.im"
    image, child = tools.image(), tools.image()
    assert image.fromshape(str(root), [2, 2])
    assert child.fromshape(str(leaf), [3, 2])
    assert image.removefile(str(root)) is False
    with pytest.raises(RuntimeError, match="still open in this process"):
        image.remove()
    assert not image.isopen()
    assert child.getchunk().shape == (3, 2)
    assert leaf.is_dir()
    assert child.done()
    assert image.removefile(str(root)) is True


def test_attached_removal_checks_the_directory_identity(tmp_path):
    original, moved, replacement = (tmp_path / name for name in ("original.im", "moved.im", "replacement.im"))
    image, other = tools.image(), tools.image()
    assert image.fromshape(str(original), [2, 2])
    assert other.fromshape(str(replacement), [3, 2])
    assert other.done()
    original.rename(moved)
    replacement.rename(original)
    with pytest.raises(RuntimeError, match="image at that path has changed"):
        image.remove()
    assert not image.isopen()
    assert other.open(str(original))
    assert other.shape().tolist() == [3, 2]
    assert other.done()
    assert other.open(str(moved))
    assert other.shape().tolist() == [2, 2]
    assert other.done()


def test_unwritable_alias_parent_does_not_remove_the_target(tmp_path):
    target = tmp_path / "native.im"
    tool = tools.image()
    assert tool.fromshape(str(target), [2, 2])
    assert tool.done()
    folder = tmp_path / "readonly"
    folder.mkdir()
    alias = folder / "alias.im"
    alias.symlink_to(target, target_is_directory=True)
    folder.chmod(0o555)
    try:
        assert tool.removefile(str(alias)) is False
        assert target.is_dir() and alias.is_symlink()
    finally:
        folder.chmod(0o755)
