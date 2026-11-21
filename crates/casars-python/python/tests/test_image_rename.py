"""Native table rename semantics through the image tool."""

import getpass
import os
import re
from pathlib import Path

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


def _rename_source(api, directory, storage):
    image = api.image()
    native = directory / "source.im"
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    assert image.fromarray(str(native), values)
    if storage == "native":
        return image, native, values
    fits = native.with_suffix(".fits")
    assert image.tofits(str(fits))
    assert image.done()
    assert image.open(str(fits))
    return image, fits, values


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("reader_backend", ["casa", "casars"])
@pytest.mark.parametrize("storage", ["native", "fits"])
@pytest.mark.parametrize("spelling", ["simple", "braced", "nested", "named-home", "undefined"])
def test_rename_expands_the_destination_and_keeps_the_user_spelling(
    tmp_path, monkeypatch, backend, reader_backend, storage, spelling,
):
    reader_api = _backend(reader_backend)
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "expanded"
    dest.mkdir()
    (tmp_path / "$CASARS_RENAME_DIRECTORY").mkdir()
    monkeypatch.setenv("CASARS_RENAME_DIRECTORY", str(dest))
    monkeypatch.setenv("CASARS_RENAME_NESTED", "$CASARS_RENAME_DIRECTORY")
    monkeypatch.delenv("CASARS_RENAME_LITERAL", raising=False)
    image, source, values = _rename_source(_backend(backend), tmp_path, storage)
    target = dest / f"target{source.suffix}"
    arguments = {
        "simple": f"$CASARS_RENAME_DIRECTORY/{target.name}",
        "braced": "$" + "{CASARS_RENAME_DIRECTORY}/" + target.name,
        "nested": f"$CASARS_RENAME_NESTED/{target.name}",
    }
    try:
        if spelling == "named-home":
            prefix = f"~{getpass.getuser()}"
            home = os.path.expanduser(prefix)
            if home == prefix or not Path(home).is_dir():
                pytest.skip("the account has no resolvable home directory")
            arguments[spelling] = f"{prefix}/{os.path.relpath(target, home)}"
        elif spelling == "undefined":
            target = tmp_path / "$CASARS_RENAME_LITERAL" / target.name
            target.parent.mkdir()
            arguments[spelling] = str(target)
        argument = arguments[spelling]
        assert image.rename(argument)
        assert not source.exists() and target.exists()
        assert not (tmp_path / "$CASARS_RENAME_DIRECTORY" / target.name).exists()
        assert image.name() == str(target)
        np.testing.assert_array_equal(image.getchunk(), values)
        assert image.history()[-3:] == [
            "Ran ia.rename", f'ia.rename(name="{argument}", overwrite=false)',
            f"original name was {source}",
        ]
        assert image.done()
        reader = reader_api.image()
        try:
            assert reader.open(str(target))
            np.testing.assert_array_equal(reader.getchunk(), values)
        finally:
            reader.done()
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("storage", ["native", "fits"])
def test_expanded_rename_errors_leave_both_paths_intact(tmp_path, monkeypatch, backend, storage):
    image, source, values = _rename_source(_backend(backend), tmp_path, storage)
    dest = tmp_path / "expanded"
    dest.mkdir()
    target = dest / f"target{source.suffix}"
    target.write_bytes(b"existing data")
    monkeypatch.setenv("CASARS_RENAME_DIRECTORY", str(dest))
    monkeypatch.setenv("CASARS_RENAME_CYCLE", "$CASARS_RENAME_CYCLE")
    collision = f"$CASARS_RENAME_DIRECTORY/{target.name}"
    missing = f"$CASARS_RENAME_DIRECTORY/missing/{target.name}"
    try:
        for argument, diagnostic in (
            (collision, rf"File {re.escape(collision)} exists"),
            (missing, rf"{re.escape(str(dest / 'missing'))} is not writable"),
            (f"$CASARS_RENAME_CYCLE/{target.name}", "recursive environment variable"),
        ):
            with pytest.raises(RuntimeError, match=diagnostic):
                image.rename(argument)
            assert not image.isopen()
            assert source.exists()
            assert target.read_bytes() == b"existing data"
            assert image.open(str(source))
            np.testing.assert_array_equal(image.getchunk(), values)
        assert image.rename(collision, overwrite=True)
        assert not source.exists()
        assert image.name() == str(target)
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_checks_the_attachment_before_expanding_the_name(tmp_path, monkeypatch, backend):
    monkeypatch.setenv("CASARS_RENAME_CYCLE", "$CASARS_RENAME_CYCLE")
    argument = "$CASARS_RENAME_CYCLE/target.im"
    image = _backend(backend).image()
    try:
        assert image.rename(argument) is False
        assert image.fromshape(shape=[2, 2])
        with pytest.raises(RuntimeError, match="cannot be renamed"):
            image.rename(argument)
        assert not image.isopen()
        assert image.fromshape(str(tmp_path / "double.im"), [2, 2], type="d")
        with pytest.raises(RuntimeError, match="double precision"):
            image.rename(argument)
        assert image.isopen()
    finally:
        image.done()


@pytest.mark.parametrize("storage", ["native", "fits"])
def test_rename_refuses_a_destination_with_two_possible_parents(tmp_path, monkeypatch, storage):
    left = tmp_path / "lexical"
    right = tmp_path / "physical"
    left.mkdir()
    nested = right / "nested"
    nested.mkdir(parents=True)
    (left / "alias").symlink_to(nested, target_is_directory=True)
    monkeypatch.setenv("CASARS_RENAME_DIRECTORY", str(left))
    image, source, values = _rename_source(tools, tmp_path, storage)
    name = f"target{source.suffix}"
    target = right / name
    target.write_bytes(b"existing data")
    try:
        history = image.history()
        with pytest.raises(RuntimeError, match="resolves to a different directory"):
            image.rename(f"$CASARS_RENAME_DIRECTORY/alias/../{name}", overwrite=True)
        assert not image.isopen()
        assert target.read_bytes() == b"existing data"
        assert not (left / name).exists()
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), values)
        assert image.history() == history
    finally:
        image.done()


@pytest.mark.parametrize("storage", ["native", "fits"])
def test_rename_rejects_a_terminal_parent_component(tmp_path, storage):
    parent = tmp_path / "parent"
    victim = parent / "victim"
    victim.mkdir(parents=True)
    (victim / "alias").symlink_to(parent, target_is_directory=True)
    marker = victim / "keep"
    marker.write_bytes(b"existing data")
    image, source, values = _rename_source(tools, tmp_path, storage)
    try:
        with pytest.raises(RuntimeError, match="ends with"):
            image.rename(str(victim / "alias" / ".."), overwrite=True)
        assert not image.isopen()
        assert marker.read_bytes() == b"existing data"
        assert (victim / "alias").is_symlink()
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        image.done()


def test_fits_rename_keeps_the_directory_requirement_of_a_trailing_slash(tmp_path):
    (tmp_path / "sub").mkdir()
    target = tmp_path / "keep.fits"
    target.write_bytes(b"existing data")
    image, source, values = _rename_source(tools, tmp_path, "fits")
    try:
        with pytest.raises(RuntimeError, match="RegularFile::move"):
            image.rename(f"{tmp_path}/sub/../keep.fits/", overwrite=True)
        assert not image.isopen()
        assert target.read_bytes() == b"existing data"
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_rename_rejects_a_destination_inside_the_source(tmp_path, backend):
    image = _backend(backend).image()
    source = tmp_path / "source.im"
    target = source / "nested.im"
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    try:
        assert image.fromarray(str(source), pixels)
        history = image.history()
        with pytest.raises(RuntimeError, match="Directory::move error.*Invalid argument"):
            image.rename(str(target))
        assert not image.isopen()
        assert not target.exists()
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert image.history() == history
    finally:
        image.done()


@pytest.mark.parametrize("target_kind", ["directory", "table"])
def test_rename_refuses_to_overwrite_a_directory_containing_the_source(tmp_path, target_kind):
    image = tools.image()
    target = tmp_path / "containing.im"
    source = target / "source.im"
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    try:
        if target_kind == "table":
            assert image.fromarray(str(target), pixels + 50)
            assert image.done()
        else:
            target.mkdir()
        sibling = target / "unrelated"
        sibling.write_text("keep", encoding="utf-8")
        assert image.fromarray(str(source), pixels)
        history = image.history()
        with pytest.raises(RuntimeError, match="destination contains the source image"):
            image.rename(str(target), overwrite=True)
        assert not image.isopen()
        assert sibling.read_text(encoding="utf-8") == "keep"
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert image.history() == history
        if target_kind == "table":
            assert image.done()
            assert image.open(str(target))
            np.testing.assert_array_equal(image.getchunk(), pixels + 50)
    finally:
        image.done()


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


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("read_before_rename", [False, True])
def test_rename_preserves_a_peer_opened_through_the_replaced_link(
    tmp_path, backend, read_before_rename,
):
    api = _backend(backend)
    source = tmp_path / "source.im"
    referent = tmp_path / "original-referent.im"
    output = tmp_path / "output.im"
    owner, peer = api.image(), api.image()
    try:
        assert owner.fromarray(str(source), np.full((3, 4), 7, dtype=np.float32))
        assert owner.sethistory("probe", ["source provenance"])
        assert peer.fromarray(str(referent), np.full((3, 4), 9, dtype=np.float32))
        assert peer.sethistory("probe", ["referent provenance"])
        assert peer.done()
        output.symlink_to(referent, target_is_directory=True)
        assert peer.open(str(output))
        if read_before_rename:
            np.testing.assert_array_equal(peer.getchunk(), 9)
        assert owner.rename(str(output), overwrite=True)
        assert peer.name() == str(output)
        peer_value = 7 if backend == "casa" and not read_before_rename else 9
        owner_value = 9 if backend == "casa" and read_before_rename else 7
        np.testing.assert_array_equal(peer.getchunk(), peer_value)
        np.testing.assert_array_equal(owner.getchunk(), owner_value)
        assert "source provenance" in owner.history()
        if backend == "casa":
            assert "source provenance" in peer.history()
        else:
            assert "referent provenance" in peer.history()
            assert "source provenance" not in peer.history()
            assert peer.remove()
            assert output.is_dir()
            np.testing.assert_array_equal(owner.getchunk(), 7)
    finally:
        owner.done()
        peer.done()


@pytest.mark.parametrize("parent_tool", ["image", "table"])
def test_rename_preserves_links_in_a_live_containing_table(tmp_path, parent_tool):
    source = tmp_path / "source.im"
    parent = tmp_path / "containing.im"
    referent = tmp_path / "referent.im"
    output = parent / "output.im"
    owner, writer = tools.image(), tools.image()
    guard = getattr(tools, parent_tool)()
    try:
        assert owner.fromarray(str(source), np.full((3, 4), 7, dtype=np.float32))
        assert writer.fromshape(str(parent), [3, 4])
        assert writer.done()
        assert writer.fromshape(str(referent), [3, 4])
        assert writer.done()
        output.symlink_to(referent, target_is_directory=True)
        assert guard.open(str(parent))
        with pytest.raises(RuntimeError, match="still open"):
            owner.rename(str(output), overwrite=True)
        assert source.is_dir() and referent.is_dir()
        assert output.is_symlink() and output.readlink() == referent
    finally:
        guard.done()
        writer.done()
        owner.done()
