"""Pinned behavior of the public image tool on a saved Float expression."""

import json

import numpy as np
import pytest

from test_image_miriad_input import _sky
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
    path.mkdir(parents=True, exist_ok=True)
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
        reported_name = elsewhere / path.relative_to(tmp_path) if backend == "casa" else path
        assert opened.name() == str(reported_name)
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


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("backing", ["native", "miriad", "concat"])
def test_saved_expression_factory_discriminator_precedence(tmp_path, monkeypatch, backend, backing):
    api = _backend(backend)
    _operand(api, tmp_path / "input.im", np.arange(12, dtype=np.float32).reshape(3, 4))
    path = tmp_path / "also-an-image.im"
    if backing == "native":
        writer = api.image()
        assert writer.fromarray(outfile=str(path), pixels=np.full((3, 4), 111., dtype=np.float32))
        assert writer.done()
    elif backing == "miriad":
        _sky(path)
    else:
        path.mkdir()
        (path / "imageconcat.json").write_text("not a JSON record", encoding="ascii")
    _manifest(path)
    monkeypatch.chdir(tmp_path)
    image = api.image()
    try:
        if backing == "concat":
            message = "Json parse error" if backend == "casa" else "#1749"
            with pytest.raises(RuntimeError, match=message):
                image.open(str(path))
            assert not image.isopen()
        else:
            assert image.open(str(path))
            assert image.getchunk(blc=[2, 3], trc=[2, 3]).item() == 33.
            with pytest.raises(RuntimeError, match="not writable"):
                image.putchunk([[99.]], blc=[2, 3])
            assert image.getchunk(blc=[2, 3], trc=[2, 3]).item() == 33.
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_saved_expression_miscinfo_preserves_nested_vectors(tmp_path, monkeypatch, backend):
    api = _backend(backend)
    _operand(api, tmp_path / "input.im", np.arange(12, dtype=np.float32).reshape(3, 4))
    path = tmp_path / "vectors.im"
    vectors = {"channels": [2, 3], "samples": [2, 3.5], "flags": [True, False], "names": ["a", "b"]}
    _manifest(path, MiscInfo={"nested": vectors})
    monkeypatch.chdir(tmp_path)
    image = api.image()
    try:
        assert image.open(str(path))
        record = image.miscinfo()
        assert set(record) == {"nested"}
        assert set(record["nested"]) == set(vectors)
        for name, expected in vectors.items():
            actual = record["nested"][name]
            np.testing.assert_array_equal(actual, expected)
            assert np.asarray(actual).dtype.kind == np.asarray(expected).dtype.kind
    finally:
        image.done()


@pytest.mark.parametrize(("backend", "malformed", "message"), [
    ("casa", [1, "string"], "vector of mixed data types"),
    ("casars", [1, "string"], "vector of mixed data types"),
    ("casars", None, "JsonValue::getValueHolder - invalid data type"),
])
def test_saved_expression_rejects_an_unusable_miscinfo_field(tmp_path, backend, malformed, message):
    api = _backend(backend)
    source = tmp_path / "input.im"
    _operand(api, source, np.arange(12, dtype=np.float32).reshape(3, 4))
    image = api.image()
    path = tmp_path / "malformed.im"
    _manifest(path, ImageExpr=f'"{source}" * 3', MiscInfo={"invalid": malformed})
    try:
        assert image.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match=message):
            image.newimagefromfile(str(path))
        np.testing.assert_array_equal(image.getchunk(), [41., 42.])
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_saved_expression_reports_missing_operand_before_miscinfo(tmp_path, backend):
    image = _backend(backend).image()
    path = tmp_path / "invalid.im"
    source = tmp_path / "absent.im"
    _manifest(path, ImageExpr=f'"{source}" * 3', MiscInfo={"invalid": [1, "text"]})
    try:
        assert image.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match="unknown lattice or image"):
            image.newimagefromfile(str(path))
        np.testing.assert_array_equal(image.getchunk(), [41., 42.])
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("operation", ["rebin", "spectralindex"])
def test_saved_expression_infers_float_function_results(tmp_path, backend, operation):
    api = _backend(backend)
    writer = api.image()
    shape = (3, 4, 1, 1)
    low = tmp_path / "low.im"
    high = tmp_path / "high.im"
    try:
        for path, value, frequency in [(low, 2., 1.e9), (high, 8., 2.e9)]:
            assert writer.fromarray(outfile=str(path), pixels=np.full(shape, value, dtype=np.float32))
            coordinates = writer.coordsys()
            try:
                assert coordinates.setreferencevalue(value=[frequency], type="spectral")
                assert writer.setcoordsys(coordinates.torecord())
            finally:
                coordinates.done()
            assert writer.done()
        text = (f'rebin("{low}", [1,1,1,1])' if operation == "rebin"
                else f'spectralindex("{high}", "{low}")')
        manifest = tmp_path / "expression.im"
        _manifest(manifest, ImageExpr=text, DataType="double")
        assert writer.open(str(manifest))
        assert writer.pixeltype() == "float"
        np.testing.assert_array_equal(writer.getchunk(), np.full(shape, 2.))
    finally:
        writer.done()
