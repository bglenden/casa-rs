"""Directory lifecycle of saved, lazily evaluated image expressions."""

import json

import numpy as np
import pytest

from test_image_removal import _backend
from test_image_saved_metadata import PIXELS, _fixture


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("through_alias", [False, True])
def test_saved_rename_keeps_persisted_metadata_and_resets_live_updates(tmp_path, monkeypatch, backend, through_alias):
    api = _backend(backend)
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    name = source
    if through_alias:
        name = tmp_path / "alias.im"
        name.symlink_to(source, target_is_directory=True)
    destination = tmp_path / "renamed.im"
    destination.mkdir()
    (destination / "discard").write_text("old destination", encoding="utf-8")
    image, peer = api.image(), api.image()
    try:
        assert image.open(str(name))
        assert image.setmiscinfo({"retained": "through rename"})
        original = (source / "imageexpr.json").read_bytes()
        assert peer.open(str(name))
        assert image.setbrightnessunit("K")
        assert image.sethistory("test", ["only live"])
        assert image.rename(str(destination), overwrite=True)
        assert image.name() == str(destination)
        assert image.brightnessunit() == ""
        assert image.miscinfo() == {"retained": "through rename"}
        assert len(image.history()) == 3
        assert image.history()[0] == "Ran ia.rename"
        assert image.history()[1] == f'ia.rename(name="{destination}", overwrite=true)'
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
        np.testing.assert_array_equal(peer.getchunk(), PIXELS * 2)
        assert not source.exists()
        if through_alias:
            assert name.is_symlink() and not name.exists()
        assert (destination / "imageexpr.json").read_bytes() == original
        assert not (destination / "discard").exists()
        assert image.done()
        assert image.open(str(destination))
        assert image.history() == []
        assert image.miscinfo() == {"retained": "through rename"}
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()
        peer.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("done", [False, True])
def test_saved_remove_refuses_and_detaches(tmp_path, monkeypatch, backend, done):
    api = _backend(backend)
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    original = (source / "imageexpr.json").read_bytes()
    image = api.image()
    try:
        assert image.open(str(source))
        assert image.removefile(str(source)) is False and image.isopen()
        with pytest.raises(RuntimeError, match="table is not writable"):
            image.remove(done=done, verbose=False)
        assert not image.isopen()
        assert (source / "imageexpr.json").read_bytes() == original
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()


@pytest.mark.parametrize("remote_change", ["cwd", "replacement"])
def test_saved_rename_stays_bound_to_the_captured_directory(tmp_path, monkeypatch, remote_change):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    original = (source / "imageexpr.json").read_bytes()
    target = tmp_path / "renamed.im"
    decoy_contents = json.dumps({"ImageExpr": '"input.im" + 1'})
    image = api.image()
    try:
        assert image.open("expression.im")
        if remote_change == "cwd":
            elsewhere = tmp_path / "elsewhere"
            elsewhere.mkdir()
            decoy = _fixture(elsewhere, api)
            (decoy / "imageexpr.json").write_text(decoy_contents, encoding="utf-8")
            decoy_operand = api.image()
            try:
                assert decoy_operand.open(str(elsewhere / "input.im"))
                assert decoy_operand.putchunk(PIXELS + 100)
            finally:
                decoy_operand.done()
            monkeypatch.chdir(elsewhere)
            assert image.rename(str(target))
            np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
            assert (target / "imageexpr.json").read_bytes() == original
            assert (decoy / "imageexpr.json").read_text(encoding="utf-8") == decoy_contents
            monkeypatch.chdir(tmp_path)
            assert image.done() and image.open(str(target))
            np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
        else:
            original_path = tmp_path / "externally-moved.im"
            source.rename(original_path)
            source.mkdir()
            (source / "imageexpr.json").write_text(decoy_contents, encoding="utf-8")
            with pytest.raises(RuntimeError, match="Saved image expression.*has changed"):
                image.rename(str(target))
            assert not image.isopen()
            assert not target.exists()
            assert (source / "imageexpr.json").read_text(encoding="utf-8") == decoy_contents
            assert (original_path / "imageexpr.json").read_bytes() == original
    finally:
        image.done()


@pytest.mark.parametrize("conflict", ["ancestor", "operand", "contained_alias"])
def test_saved_rename_preserves_its_graph_on_an_unsafe_move(tmp_path, monkeypatch, conflict):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    manifest = source / "imageexpr.json"
    operand = tmp_path / "input.im"
    monkeypatch.chdir(tmp_path)
    if conflict == "ancestor":
        target = tmp_path
    elif conflict == "operand":
        target = operand
    else:
        target = tmp_path / "renamed.im"
        (source / "alias.im").symlink_to(operand, target_is_directory=True)
        body = json.loads(manifest.read_text(encoding="utf-8"))
        body["ImageExpr"] = '"expression.im/alias.im" * 2'
        manifest.write_text(json.dumps(body), encoding="utf-8")
    original, input_data = manifest.read_bytes(), (operand / "table.dat").read_bytes()
    image = api.image()
    try:
        assert image.open(str(source))
        with pytest.raises(RuntimeError, match="(?:captured operand|inside|contain)"):
            image.rename(str(target), overwrite=True)
        assert not image.isopen()
        assert manifest.read_bytes() == original
        assert (operand / "table.dat").read_bytes() == input_data
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()
