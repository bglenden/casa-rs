"""Pinned behavior of the public image tool on a saved Float expression."""

import json

import numpy as np
import pytest

from test_image_removal import _backend


def _operand(api, path, pixels):
    writer = api.image()
    try:
        assert writer.fromarray(outfile=str(path), pixels=pixels)
        assert writer.setbrightnessunit("Jy/pixel")
        assert writer.setmiscinfo({"operand_tag": "inherited from the operand"})
        assert writer.calcmask(mask=f'"{path}" > 4')
        coordinates = writer.coordsys()
        try:
            return coordinates.toworld([2, 3])["numeric"].copy()
        finally:
            coordinates.done()
    finally:
        writer.done()


def _manifest(path, **fields):
    path.mkdir(parents=True)
    (path / "imageexpr.json").write_text(
        json.dumps({"ImageExpr": '"input.im" * 3', **fields}), encoding="utf-8"
    )


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("method", ["open", "newimage", "newimagefromfile"])
def test_open_saved_float_expression_keeps_its_original_operand(tmp_path, monkeypatch, backend, method):
    api = _backend(backend)
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    expected_world = _operand(api, tmp_path / "input.im", pixels)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    _operand(api, elsewhere / "input.im", pixels + 100)
    path = tmp_path / "manifests" / "expression.im"
    _manifest(path, Version=17, DataType="this field is ignored")
    root = api.image()
    opened = root
    monkeypatch.chdir(tmp_path)
    try:
        assert root.fromarray(pixels=[41., 42.])
        result = getattr(root, method)(str(path.relative_to(tmp_path)))
        if method == "open":
            assert result is True
        else:
            opened = result
            np.testing.assert_array_equal(root.getchunk(), [41., 42.])
        assert opened.isopen() and opened.ispersistent()
        assert opened.name() == str(path)
        assert opened.name(True) == path.name
        assert opened.pixeltype() == "float"
        assert opened.brightnessunit() == "Jy/pixel"
        assert opened.miscinfo() == {"operand_tag": "inherited from the operand"}
        assert opened.history() == []

        monkeypatch.chdir(elsewhere)
        validity = opened.getchunk(getmask=True)
        np.testing.assert_array_equal(validity, pixels > 4)
        np.testing.assert_array_equal(opened.getchunk()[validity], (pixels * 3)[validity])
        assert opened.getchunk(blc=[2, 3], trc=[2, 3]).item() == 33.
        coordinates = opened.coordsys()
        try:
            np.testing.assert_allclose(coordinates.toworld([2, 3])["numeric"], expected_world)
        finally:
            coordinates.done()
        with pytest.raises(RuntimeError, match="not writable"):
            opened.putchunk([[9.]], blc=[2, 3])
        assert opened.isopen()
        assert opened.getchunk(blc=[2, 3], trc=[2, 3]).item() == 33.
    finally:
        opened.done()
        if opened is not root:
            root.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("record", [{}, {"nested": {"size": 3}, "title": "saved expression"}])
def test_saved_expression_miscinfo_overrides_the_operand(tmp_path, monkeypatch, backend, record):
    api = _backend(backend)
    _operand(api, tmp_path / "input.im", np.arange(12, dtype=np.float32).reshape(3, 4))
    path = tmp_path / "expression.im"
    _manifest(path, MiscInfo=record)
    image = api.image()
    monkeypatch.chdir(tmp_path)
    try:
        assert image.open(str(path))
        assert image.miscinfo() == record
        assert image.getchunk(blc=[2, 3], trc=[2, 3]).item() == 33.
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_saved_expression_nonrecord_miscinfo_preserves_the_factory_receiver(tmp_path, monkeypatch, backend):
    api = _backend(backend)
    _operand(api, tmp_path / "input.im", np.arange(12, dtype=np.float32).reshape(3, 4))
    path = tmp_path / "expression.im"
    _manifest(path, MiscInfo=None)
    image = api.image()
    monkeypatch.chdir(tmp_path)
    try:
        assert image.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match="JsonValue::getValueMap - invalid data type"):
            image.newimagefromfile(str(path))
        np.testing.assert_array_equal(image.getchunk(), [41., 42.])
    finally:
        image.done()
