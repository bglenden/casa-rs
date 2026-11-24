"""Pinned behavior of the public image tool on a saved expression."""

import json
import os

import numpy as np
import pytest

from test_image_miriad_input import _sky
from test_image_removal import _backend


@pytest.mark.parametrize("name", ["pa.im", "spectralindex.im", "pa(operand).im", "spectralindex(operand).im"])
@pytest.mark.parametrize("persistent", [False, True])
def test_imagecalc_function_names_in_paths_preserve_metadata(tmp_path, name, persistent):
    api = _backend("casars")
    path = tmp_path / name
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    expected_world = _operand(api, path, pixels)
    source = api.image()
    output = None
    try:
        output = source.imagecalc(
            pixels=f'"{path}" + 3',
            outfile=str(tmp_path / "result.im") if persistent else "",
        )
        valid = pixels > 4
        np.testing.assert_array_equal(output.getchunk()[valid], (pixels + 3)[valid])
        assert output.brightnessunit() == "Jy/pixel"
        coordinates = output.coordsys()
        try:
            np.testing.assert_allclose(coordinates.toworld([2, 3])["numeric"], expected_world)
        finally:
            coordinates.done()
    finally:
        if output is not None:
            output.done()
        source.done()


@pytest.mark.parametrize("function", ["pa", "PA"])
def test_imagecalc_polarization_angle_call_keeps_its_metadata(tmp_path, function):
    api = _backend("casars")
    source = api.image()
    output = None
    try:
        for name, value in [("u", 4.), ("q", 3.)]:
            assert source.fromarray(str(tmp_path / f"{name}.im"),
                                    np.full((1, 1, 1, 1), value, dtype=np.float32))
            assert source.done()
        output = source.imagecalc(pixels=f'{function} ("{tmp_path}/u.im", "{tmp_path}/q.im")')
        np.testing.assert_allclose(output.getchunk(), np.degrees(0.5 * np.arctan2(4., 3.)), rtol=1e-6)
        assert output.brightnessunit() == "deg"
        coordinates = output.coordsys()
        try:
            assert coordinates.stokes() == ["Pangle"]
        finally:
            coordinates.done()
    finally:
        if output is not None:
            output.done()
        source.done()


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
@pytest.mark.parametrize("method", ["open", "newimage", "newimagefromfile"])
def test_nested_saved_expressions_capture_a_shared_operand(tmp_path, monkeypatch, backend, method):
    api = _backend(backend)
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    expected_world = _operand(api, tmp_path / "input.im", pixels)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    _operand(api, elsewhere / "input.im", pixels + 100)
    _manifest(tmp_path / "operands" / "inner.im", MiscInfo={"origin": "inner"})
    (tmp_path / "alias.im").symlink_to("operands/inner.im", target_is_directory=True)
    _manifest(tmp_path / "left.im", ImageExpr='"operands/inner.im" + 2')
    _manifest(tmp_path / "right.im", ImageExpr='"alias.im" + 3')
    _manifest(tmp_path / "outer.im", ImageExpr='"left.im" + "right.im"')
    receiver = api.image()
    opened = receiver
    monkeypatch.chdir(tmp_path)
    try:
        assert receiver.fromarray(pixels=[41., 42.])
        result = getattr(receiver, method)("outer.im")
        if method == "open":
            assert result is True
        else:
            opened = result
            np.testing.assert_array_equal(receiver.getchunk(), [41., 42.])
        monkeypatch.chdir(elsewhere)
        assert opened.miscinfo() == {"origin": "inner"}
        assert opened.brightnessunit() == "Jy/pixel"
        assert opened.history() == []
        assert opened.pixeltype() == "float"
        validity = opened.getchunk(getmask=True)
        np.testing.assert_array_equal(validity, pixels > 4)
        np.testing.assert_array_equal(opened.getchunk()[validity], (6 * pixels + 5)[validity])
        coordinates = opened.coordsys()
        try:
            np.testing.assert_allclose(coordinates.toworld([2, 3])["numeric"], expected_world)
        finally:
            coordinates.done()
    finally:
        opened.done()
        if opened is not receiver:
            receiver.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("selector", ["nomask", "mask0"])
def test_saved_expression_operand_rejects_a_selector_outside_its_quotes(tmp_path, monkeypatch, backend, selector):
    api = _backend(backend)
    _operand(api, tmp_path / "input.im", np.arange(12, dtype=np.float32).reshape(3, 4))
    _manifest(tmp_path / "inner.im")
    _manifest(tmp_path / "outer.im", ImageExpr=f'"inner.im":{selector} + 2')
    monkeypatch.chdir(tmp_path)
    receiver = api.image()
    try:
        assert receiver.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match=f"Parse error at or near ':{selector}'"):
            receiver.newimagefromfile("outer.im")
        np.testing.assert_array_equal(receiver.getchunk(), [41., 42.])
    finally:
        receiver.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("operand", ['"inner.im:nomask"', "inner.im:no_such_mask"])
def test_saved_expression_operand_keeps_intrinsic_validity(tmp_path, monkeypatch, backend, operand):
    api = _backend(backend)
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    _operand(api, tmp_path / "input.im", pixels)
    _manifest(tmp_path / "inner.im")
    _manifest(tmp_path / "outer.im", ImageExpr=f"{operand} + 2")
    monkeypatch.chdir(tmp_path)
    receiver = api.image()
    try:
        assert receiver.open("outer.im")
        valid = receiver.getchunk(getmask=True)
        np.testing.assert_array_equal(valid, pixels > 4)
        np.testing.assert_array_equal(receiver.getchunk()[valid], (pixels * 3 + 2)[valid])
    finally:
        receiver.done()


@pytest.mark.parametrize("second_operand", ["original.im", "alias.im"])
def test_saved_expression_cycle_preserves_the_factory_receiver(tmp_path, monkeypatch, second_operand):
    # Pinned CASA overflows its stack opening this graph. The factory must fail safely.
    _manifest(tmp_path / "original.im", ImageExpr='"other.im" + 1')
    _manifest(tmp_path / "other.im", ImageExpr=f'"{second_operand}" + 2')
    (tmp_path / "alias.im").symlink_to("original.im", target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    receiver = _backend("casars").image()
    try:
        assert receiver.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match="Cycle in saved image expression"):
            receiver.newimagefromfile("original.im")
        np.testing.assert_array_equal(receiver.getchunk(), [41., 42.])
        with pytest.raises(RuntimeError, match="Cycle in saved image expression"):
            receiver.fromimage(outfile="must-not-exist.im", infile="original.im")
        np.testing.assert_array_equal(receiver.getchunk(), [41., 42.])
        assert not (tmp_path / "must-not-exist.im").exists()
    finally:
        receiver.done()


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
            message = "Json parse error" if backend == "casa" else "imageconcat.json"
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


def _typed_operand(api, path, pixeltype):
    dtype = {"float": np.float32, "double": np.float64,
             "complex": np.complex64, "dcomplex": np.complex128}[pixeltype]
    values = np.arange(12.).reshape(3, 4)
    if pixeltype.startswith("d"):
        values += 2. ** -37
    if "complex" in pixeltype:
        values = values + 2j * (values + 1.)
    values = values.astype(dtype)
    source = api.image()
    try:
        assert source.fromarray(outfile=str(path), pixels=values,
                                type="d" if pixeltype.startswith("d") else "f")
        assert source.pixeltype() == pixeltype
    finally:
        source.done()
    return values


@pytest.mark.parametrize("writer_backend", ["casa", "casars"])
@pytest.mark.parametrize("reader_backend", ["casa", "casars"])
@pytest.mark.parametrize("pixeltype", ["float", "double", "complex", "dcomplex"])
@pytest.mark.parametrize("mode", ["paged", "nomove", "copy", "move"])
def test_concat_nested_saved_expressions_in_every_numeric_type(
    tmp_path, writer_backend, reader_backend, pixeltype, mode,
):
    writer_api, reader_api = _backend(writer_backend), _backend(reader_backend)
    leaf, inner, expression, native, output = (
        tmp_path / name for name in ("leaf.im", "inner.im", "outer.im", "native.im", "joined.im")
    )
    pixels = _typed_operand(writer_api, leaf, pixeltype)
    other = _typed_operand(writer_api, native, pixeltype)
    _manifest(inner, ImageExpr=f'"{leaf}" * 3', DataType="ignored")
    _manifest(expression, ImageExpr=f'"{inner}" + 1', MiscInfo={"origin": "nested"})
    expected = np.concatenate([pixels * 3 + 1, other])
    writer, reader = writer_api.image(), reader_api.image()
    joined = None
    try:
        joined = writer.imageconcat(str(output), [str(expression), str(native)], axis=0, relax=True,
                                    mode=mode, tempclose=True)
        assert joined.pixeltype() == pixeltype
        np.testing.assert_array_equal(joined.getchunk(), expected)
        assert joined.done()
        assert reader.open(str(output))
        assert reader.pixeltype() == pixeltype
        np.testing.assert_array_equal(reader.shape(), expected.shape)
        actual = reader.getchunk()
        assert actual.dtype == (np.complex128 if "complex" in pixeltype else np.float64)
        np.testing.assert_array_equal(actual, expected)
        assert expression.exists() == (mode != "move")
        assert native.exists() == (mode != "move")
        assert leaf.is_dir() and inner.is_dir()
        if pixeltype in {"float", "complex"} and mode != "paged":
            np.testing.assert_array_equal(reader.haslock(), [True, False])
            assert reader.unlock()
            np.testing.assert_array_equal(reader.haslock(), [False, False])
            assert reader.lock(nattempts=1)
            np.testing.assert_array_equal(reader.haslock(), [True, False])
    finally:
        reader.done()
        if joined is not None:
            joined.done()
        writer.done()


@pytest.mark.parametrize("mode", ["paged", "nomove", "copy", "move"])
@pytest.mark.parametrize("wrapped", [False, True])
def test_saved_expression_concat_protects_its_transitive_table(tmp_path, mode, wrapped):
    api = _backend("casars")
    source, inner, expression, companion = (
        tmp_path / name for name in ("leaf.im", "inner.im", "outer.im", "companion.im")
    )
    pixels = _typed_operand(api, source, "float")
    _typed_operand(api, companion, "float")
    _manifest(inner, ImageExpr=f'"{source}" * 3')
    _manifest(expression, ImageExpr=f'"{inner}" + 1')
    image = api.image()
    try:
        operand = expression
        if wrapped:
            operand = tmp_path / "wrapped.im"
            product = image.imageconcat(str(operand), [str(expression), str(companion)],
                                        axis=0, relax=True, mode="nomove", tempclose=True)
            assert product.done()
        assert image.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match="input|overlap|overwrite"):
            image.imageconcat(str(source), [str(operand), str(companion)],
                              axis=0, relax=True, mode=mode, overwrite=True)
        np.testing.assert_array_equal(image.getchunk(), [41., 42.])
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert companion.is_dir() and inner.is_dir() and expression.is_dir()
    finally:
        image.done()


def test_saved_expression_concat_move_keeps_a_required_leaf(tmp_path):
    api = _backend("casars")
    source, expression, output = (tmp_path / name for name in ("leaf.im", "expression.im", "joined.im"))
    pixels = _typed_operand(api, source, "float")
    _manifest(expression, ImageExpr=f'"{source}" * 3')
    image = api.image()
    try:
        assert image.fromarray(str(output), [33.]) and image.done()
        with pytest.raises(RuntimeError, match="Cannot relocate virtual concatenation"):
            image.imageconcat(str(output), [str(expression), str(source)],
                              axis=0, relax=True, overwrite=True, mode="move")
        assert image.open(str(output))
        np.testing.assert_array_equal(image.getchunk(), [33.])
        assert image.open(str(expression))
        np.testing.assert_array_equal(image.getchunk(), pixels * 3)
    finally:
        image.done()


@pytest.mark.parametrize("mode", ["paged", "copy"])
@pytest.mark.parametrize("nested", [False, True])
def test_concat_refuses_to_replace_a_linked_manifest_referent(tmp_path, mode, nested):
    api = _backend("casars")
    output = tmp_path / "output.im"
    leaf = tmp_path / "leaf.im"
    other = tmp_path / "other.im"
    original = _typed_operand(api, output, "float")
    pixels = _typed_operand(api, leaf, "float")
    _typed_operand(api, other, "float")
    referent = output / "expression.json"
    document = json.dumps({"ImageExpr": f'"{leaf}" * 3'})
    referent.write_text(document)
    linked = tmp_path / "linked.im"
    linked.mkdir()
    (linked / "imageexpr.json").symlink_to(referent)
    if nested:
        expression = tmp_path / "outer.im"
        _manifest(expression, ImageExpr=f'"{linked}" + 1')
    else:
        expression = linked
    image = api.image()
    try:
        with pytest.raises(RuntimeError, match="overlaps a saved image expression input"):
            image.imageconcat(str(output), [str(expression), str(other)],
                              axis=0, relax=True, mode=mode, overwrite=True)
        assert referent.read_text() == document
        assert image.open(str(output))
        np.testing.assert_array_equal(image.getchunk(), original)
        assert image.open(str(expression))
        np.testing.assert_array_equal(image.getchunk(), pixels * 3 + int(nested))
    finally:
        image.done()


def test_concat_refuses_to_replace_a_directory_containing_an_operand_alias(tmp_path):
    api = _backend("casars")
    output = tmp_path / "output.im"
    leaf = tmp_path / "leaf.im"
    other = tmp_path / "other.im"
    original = _typed_operand(api, output, "float")
    pixels = _typed_operand(api, leaf, "float")
    _typed_operand(api, other, "float")
    alias = output / "alias.im"
    alias.symlink_to(leaf, target_is_directory=True)
    expression = tmp_path / "expression.im"
    _manifest(expression, ImageExpr=f'"{alias}" * 3')
    image = api.image()
    try:
        with pytest.raises(RuntimeError, match="overlaps a saved image expression input"):
            image.imageconcat(str(output), [str(expression), str(other)],
                              axis=0, relax=True, overwrite=True)
        assert alias.is_symlink()
        assert image.open(str(output))
        np.testing.assert_array_equal(image.getchunk(), original)
        assert image.open(str(expression))
        np.testing.assert_array_equal(image.getchunk(), pixels * 3)
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_live_saved_expression_concat_holds_the_operand_for_removal(tmp_path, backend):
    api = _backend(backend)
    source, expression, companion, output = (
        tmp_path / name for name in ("leaf.im", "expression.im", "companion.im", "joined.im")
    )
    pixels = _typed_operand(api, source, "float")
    _typed_operand(api, companion, "float")
    _manifest(expression, ImageExpr=f'"{source}" * 3')
    image = api.image()
    joined = image.imageconcat(str(output), [str(expression), str(companion)],
                               axis=0, relax=True, mode="nomove", tempclose=True)
    try:
        assert image.open(str(source))
        with pytest.raises(RuntimeError, match="still open in this process"):
            image.remove(done=True)
        assert source.is_dir()
        np.testing.assert_array_equal(joined.getchunk()[:3], pixels * 3)
        assert joined.done()
        assert image.open(str(source))
        assert image.remove(done=True)
        assert not source.exists()
    finally:
        joined.done()
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("inner_tempclose", [False, True])
def test_outer_tempclose_keeps_nested_saved_expression_leases(tmp_path, backend, inner_tempclose):
    api = _backend(backend)
    names = ("leaf.im", "expression.im", "first.im", "second.im", "inner.im", "outer.im")
    leaf, expression, first, second, inner, outer = (tmp_path / name for name in names)
    pixels = _typed_operand(api, leaf, "float")
    first_pixels = _typed_operand(api, first, "float")
    second_pixels = _typed_operand(api, second, "float")
    _manifest(expression, ImageExpr=f'"{leaf}" * 3')
    image = api.image()
    joined = image.imageconcat(str(inner), [str(expression), str(first)],
                               axis=0, relax=True, tempclose=inner_tempclose, mode="nomove")
    joined.done()
    joined = image.imageconcat(str(outer), [str(inner), str(second)],
                               axis=0, relax=True, tempclose=True, mode="nomove")
    try:
        np.testing.assert_array_equal(joined.haslock(), [True, False])
        np.testing.assert_array_equal(joined.getchunk(),
                                      np.concatenate([pixels * 3, first_pixels, second_pixels]))
        np.testing.assert_array_equal(joined.haslock(), [True, False])
        assert joined.unlock()
        np.testing.assert_array_equal(joined.haslock(), [False, False])
        assert joined.lock(False, 1)
        np.testing.assert_array_equal(joined.haslock(), [True, False])
    finally:
        joined.done()
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_copy_concat_materializes_a_relocated_expression_manifest_link(tmp_path, backend):
    api = _backend(backend)
    source, companion, referent = (
        tmp_path / name for name in ("leaf.im", "companion.im", "oracle.json")
    )
    pixels = _typed_operand(api, source, "float")
    other = _typed_operand(api, companion, "float")
    expression = tmp_path / "holder" / "expression.im"
    expression.mkdir(parents=True)
    referent.write_text(json.dumps({"ImageExpr": f'"{source}" * 3'}))
    link = expression / "imageexpr.json"
    link.symlink_to("../../oracle.json")
    output = tmp_path / "products" / "joined.im"
    output.parent.mkdir()
    maker, reader = api.image(), api.image()
    product = maker.imageconcat(str(output), [str(expression), str(companion)],
                                axis=0, relax=True, mode="copy")
    try:
        assert product.done()
        assert link.is_symlink()
        assert reader.open(str(output))
        np.testing.assert_array_equal(reader.getchunk(), np.concatenate([pixels * 3, other]))
    finally:
        product.done()
        reader.done()
        maker.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["paged", "nomove", "copy", "move"])
@pytest.mark.parametrize("tempclose", [False, True])
def test_concat_saved_expression_keeps_the_source_mask(tmp_path, backend, mode, tempclose):
    api = _backend(backend)
    source, expression, companion, output = (
        tmp_path / name for name in ("leaf.im", "expression.im", "companion.im", "joined.im")
    )
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    _operand(api, source, pixels)
    _typed_operand(api, companion, "float")
    _manifest(expression, ImageExpr=f'"{source}" * 3')
    tool = api.image()
    product = tool.imageconcat(str(output), [str(expression), str(companion)],
                               axis=0, relax=True, mode=mode, tempclose=tempclose)
    try:
        np.testing.assert_array_equal(product.getchunk(getmask=True),
                                      np.concatenate([pixels > 4, np.ones_like(pixels, dtype=bool)]))
        np.testing.assert_array_equal(product.getchunk()[:3][pixels > 4], (pixels * 3)[pixels > 4])
    finally:
        product.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("selector", ["", ":nomask", ":unknown"])
def test_saved_expression_uses_virtual_concat_pixels_and_mask(tmp_path, backend, selector):
    api = _backend(backend)
    masked, valid, virtual, expression, output = (
        tmp_path / name for name in ("masked.im", "valid.im", "virtual.im", "expression.im", "out.im")
    )
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    _operand(api, masked, values)
    other = _typed_operand(api, valid, "float")
    tool = api.image()
    joined = tool.imageconcat(str(virtual), [str(masked), str(valid)], axis=0, relax=True, mode="nomove")
    coords = joined.coordsys()
    coordinate_record = coords.torecord()
    coords.done()
    assert joined.done()
    _manifest(expression, ImageExpr=f'"{virtual}{selector}" + 2')
    expected = np.concatenate([values, other]) + 2
    mask = np.concatenate([values > 4, np.ones_like(values, dtype=bool)])
    try:
        assert tool.open(str(expression))
        coords = tool.coordsys()
        try:
            record = coords.torecord()
            for key in coordinate_record:
                if key.startswith(("worldreplace", "pixelreplace", "worldmap", "pixelmap")):
                    np.testing.assert_array_equal(record[key], coordinate_record[key])
        finally:
            coords.done()
        np.testing.assert_array_equal(tool.getchunk(getmask=True), mask)
        np.testing.assert_array_equal(tool.getchunk()[mask], expected[mask])
        assert tool.haslock().tolist() == [False, False]
        assert tool.done()
        joined = tool.imageconcat(str(output), [str(expression), str(valid)],
                                   axis=0, relax=True, mode="nomove")
        np.testing.assert_array_equal(joined.getchunk(getmask=True)[:6], mask)
        np.testing.assert_array_equal(joined.getchunk()[:6][mask], expected[mask])
    finally:
        joined.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("pixeltype", ["double", "complex", "dcomplex"])
def test_saved_expression_preserves_virtual_operand_precision(tmp_path, backend, pixeltype):
    api = _backend(backend)
    left, right, virtual, expression = (tmp_path / name for name in
                                        ("left.im", "right.im", "virtual.im", "expr.im"))
    first = _typed_operand(api, left, pixeltype)
    second = _typed_operand(api, right, pixeltype)
    tool = api.image()
    product = tool.imageconcat(str(virtual), [str(left), str(right)], axis=0,
                                relax=True, mode="nomove")
    assert product.done()
    _manifest(expression, ImageExpr=f'"{virtual}" + 2')
    try:
        assert tool.open(str(expression))
        assert tool.pixeltype() == pixeltype
        np.testing.assert_array_equal(tool.getchunk(), np.concatenate([first, second]) + 2)
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("selector", ["", ":nomask", ":unknown"])
def test_saved_expression_reads_miriad_leaves(tmp_path, backend, selector):
    api = _backend(backend)
    miriad, expression, native, output = (tmp_path / name for name in
                                        ("sky.im", "expression.im", "native.im", "out.im"))
    pixels = _sky(miriad)
    _manifest(expression, ImageExpr=f'"{miriad}{selector}" + 2')
    tool = api.image()
    try:
        if selector == ":unknown":
            with pytest.raises(RuntimeError, match="has no named masks"):
                tool.open(str(expression))
            return
        assert tool.open(str(expression))
        np.testing.assert_array_equal(tool.getchunk(), pixels + 2)
        assert tool.haslock().tolist() == [True, True]
        assert tool.done()
        calculated = tool.imagecalc(pixels=f'"{miriad}{selector}" + 2')
        try:
            np.testing.assert_array_equal(calculated.getchunk(), pixels + 2)
        finally:
            calculated.done()
        assert tool.fromarray(str(native), pixels)
        assert tool.done()
        joined = tool.imageconcat(str(output), [str(expression), str(native)],
                                   axis=0, relax=True, mode="nomove")
        try:
            np.testing.assert_array_equal(joined.getchunk(), np.concatenate([pixels + 2, pixels]))
        finally:
            joined.done()
    finally:
        tool.done()


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="needs POSIX file permissions")
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_concat_saved_expression_reads_readonly_native_without_table_lock(tmp_path, backend):
    api = _backend(backend)
    leaf, other, expression, output = (tmp_path / name for name in
                                      ("leaf.im", "other.im", "expression.im", "out.im"))
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    _operand(api, leaf, pixels)
    _typed_operand(api, other, "float")
    lock_file = leaf / "table.lock"
    lock_file.unlink()
    paths = [leaf, *leaf.rglob("*")]
    permissions = {path: path.stat().st_mode & 0o777 for path in paths}
    for path in reversed(paths):
        path.chmod(0o555 if path.is_dir() else 0o444)
    _manifest(expression, ImageExpr=f'"{leaf}:nomask" + 2')
    tool = api.image()
    try:
        for method in ("open", "newimage", "newimagefromfile", "newimagefromimage"):
            result = getattr(tool, method)(str(leaf))
            reader = tool if method == "open" else result
            try:
                np.testing.assert_array_equal(reader.getchunk(), pixels)
                np.testing.assert_array_equal(reader.getchunk(getmask=True), pixels > 4)
                assert reader.haslock().tolist() == [True, True]
                if method != "newimagefromimage":
                    with pytest.raises(RuntimeError):
                        reader.putchunk(np.ones_like(pixels))
                    np.testing.assert_array_equal(reader.getchunk(), pixels)
            finally:
                reader.done()
        assert tool.fromimage(str(tmp_path / "copied.im"), str(leaf))
        np.testing.assert_array_equal(tool.getchunk(), pixels)
        assert tool.done()
        assert not lock_file.exists()
        joined_native = tool.imageconcat(str(tmp_path / "native-joined.im"), [str(leaf), str(other)],
                                          axis=0, relax=True, mode="nomove", tempclose=True)
        try:
            np.testing.assert_array_equal(joined_native.getchunk()[:3], pixels)
            assert joined_native.haslock().tolist() == [False, False]
        finally:
            joined_native.done()
        assert tool.open(str(expression))
        np.testing.assert_array_equal(tool.getchunk(), pixels + 2)
        np.testing.assert_array_equal(tool.getchunk(getmask=True), np.ones_like(pixels, dtype=bool))
        assert tool.done()
        joined = tool.imageconcat(str(output), [str(expression), str(other)],
                                   axis=0, relax=True, mode="nomove", tempclose=True)
        try:
            np.testing.assert_array_equal(joined.getchunk()[:3], pixels + 2)
            assert joined.haslock().tolist() == [True, True]
            assert not lock_file.exists()
        finally:
            joined.done()
    finally:
        tool.done()
        for path, mode in permissions.items():
            path.chmod(mode)


def test_open_rejects_a_cycle_across_saved_expression_and_concat(tmp_path):
    api = _backend("casars")
    expression, virtual, leaf, output = (tmp_path / name for name in
                                        ("expression.im", "virtual.im", "leaf.im", "out.im"))
    _typed_operand(api, leaf, "float")
    _manifest(expression, ImageExpr=f'"{virtual}" + 2')
    virtual.mkdir()
    (virtual / "imageconcat.json").write_text(json.dumps({
        "DataType": "float", "Axis": 0, "Images": ["./expression.im", "./leaf.im"],
    }))
    tool = api.image()
    try:
        for path in (expression, virtual):
            with pytest.raises(RuntimeError, match="Cycle in"):
                tool.open(str(path))
        with pytest.raises(RuntimeError, match="Cycle in"):
            tool.imageconcat(str(output), [str(expression), str(leaf)], axis=0, mode="nomove")
        assert not output.exists()
    finally:
        tool.done()


def test_move_protects_a_table_used_through_an_external_virtual_operand(tmp_path):
    api = _backend("casars")
    first, second, virtual, expression, output = (
        tmp_path / name for name in ("first.im", "second.im", "virtual.im", "expr.im", "output.im")
    )
    first_pixels = _typed_operand(api, first, "float")
    second_pixels = _typed_operand(api, second, "float")
    original = _typed_operand(api, output, "float")
    tool = api.image()
    joined = tool.imageconcat(str(virtual), [str(first), str(second)], axis=0, relax=True, mode="nomove")
    assert joined.done()
    _manifest(expression, ImageExpr=f'"{virtual}" + 2')
    try:
        with pytest.raises(RuntimeError, match="Cannot relocate virtual concatenation"):
            tool.imageconcat(str(output), [str(expression), str(first)], axis=0,
                             relax=True, mode="move", overwrite=True)
        assert first.is_dir() and expression.is_dir()
        assert tool.open(str(output))
        np.testing.assert_array_equal(tool.getchunk(), original)
        assert tool.open(str(expression))
        np.testing.assert_array_equal(tool.getchunk(),
                                      np.concatenate([first_pixels, second_pixels]) + 2)
    finally:
        joined.done()
        tool.done()


@pytest.mark.parametrize("dependency", ["repeated-alias", "concat-manifest", "miriad-header", "miriad-image"])
def test_overlap_check_keeps_linked_leaf_files_and_repeated_aliases(tmp_path, dependency):
    api = _backend("casars")
    output, leaf, source, expression, other = (
        tmp_path / name for name in ("out.im", "leaf.im", "source.im", "expr.im", "other.im")
    )
    original = _typed_operand(api, output, "float")
    tool = api.image()
    if dependency.startswith("miriad"):
        pixels = _sky(source)
        link = source / dependency.split("-")[1]
        contained = output / "payload"
        link.rename(contained)
        link.symlink_to(contained)
    else:
        one = _typed_operand(api, leaf, "float")
        pixels = np.concatenate([one, one])
        if dependency == "repeated-alias":
            link = output / "alias.im"
            link.symlink_to(leaf, target_is_directory=True)
            contained = link
            operands = [str(leaf), str(link)]
        else:
            operands = [str(leaf), str(leaf)]
        tool.imageconcat(str(source), operands, axis=0, relax=True, mode="nomove").done()
        if dependency == "concat-manifest":
            link = source / "imageconcat.json"
            contained = output / "manifest.json"
            link.rename(contained)
            link.symlink_to(contained)
    assert tool.fromarray(str(other), np.zeros_like(pixels))
    assert tool.done()
    _manifest(expression, ImageExpr=f'"{source}" + 1')
    try:
        with pytest.raises(RuntimeError, match="overlaps a saved image expression input"):
            tool.imageconcat(str(output), [str(expression), str(other)], axis=0, relax=True, overwrite=True)
        assert contained.exists() and link.is_symlink()
        assert tool.open(str(output))
        np.testing.assert_array_equal(tool.getchunk(), original)
        assert tool.open(str(expression))
        np.testing.assert_array_equal(tool.getchunk(), pixels + 1)
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("tempclose", [False, True])
def test_saved_expression_section_reads_only_selected_fixed_members(tmp_path, backend, tempclose):
    api = _backend(backend)
    first, second, virtual, expression = (tmp_path / name for name in
                                         ("first.im", "second.im", "virtual.im", "expr.im"))
    _typed_operand(api, first, "float")
    _typed_operand(api, second, "float")
    tool = api.image()
    tool.imageconcat(str(virtual), [str(first), str(second)], axis=0,
                     relax=True, mode="nomove", tempclose=tempclose).done()
    _manifest(expression, ImageExpr=f'"{virtual}" + 2')
    assert tool.open(str(expression))
    moved = second.with_suffix(".moved")
    second.rename(moved)
    try:
        if tempclose:
            with pytest.raises(RuntimeError):
                tool.getchunk([0, 0], [0, 0])
        else:
            np.testing.assert_array_equal(tool.getchunk([0, 0], [0, 0]), [[2.]])
            np.testing.assert_array_equal(tool.getchunk([0, 0], [0, 0], getmask=True), [[True]])
    finally:
        moved.rename(second)
        tool.done()


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="needs POSIX file permissions")
@pytest.mark.parametrize("table_files_writable", [False, True])
def test_concat_beam_write_does_not_modify_members_without_a_physical_lease(tmp_path, table_files_writable):
    api = _backend("casars")
    writable, readonly, output = (tmp_path / name for name in ("rw.im", "ro.im", "out.im"))
    for path in (writable, readonly):
        _typed_operand(api, path, "float")
    (readonly / "table.lock").unlink()
    paths = [readonly, *readonly.rglob("*")]
    permissions = {path: path.stat().st_mode & 0o777 for path in paths}
    if not table_files_writable:
        for path in paths[1:]:
            path.chmod(0o555 if path.is_dir() else 0o444)
    readonly.chmod(0o555)
    tool = api.image()
    joined = None
    try:
        joined = tool.imageconcat(str(output), [str(writable), str(readonly)], axis=0,
                                   relax=True, mode="nomove", tempclose=False)
        with pytest.raises(RuntimeError, match="cannot be opened for read/write"):
            joined.setrestoringbeam("3arcsec", "2arcsec", "1deg")
        assert joined.done()
        for path in (writable, readonly):
            assert tool.open(str(path))
            assert tool.restoringbeam() == {}
            assert tool.done()
        assert not (readonly / "table.lock").exists()
    finally:
        if joined is not None:
            joined.done()
        tool.done()
        for path, mode in permissions.items():
            path.chmod(mode)


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("pixeltype", ["float", "double", "complex", "dcomplex"])
@pytest.mark.parametrize("factory", ["open", "newimage", "newimagefromfile"])
def test_saved_expression_direct_factories_infer_all_pixel_types(tmp_path, backend, pixeltype, factory):
    api = _backend(backend)
    source = tmp_path / "operand.im"
    pixels = _typed_operand(api, source, pixeltype)
    expression = tmp_path / "manifest.im"
    _manifest(expression, ImageExpr=f'"{source}" * 3', DataType="unknown")
    image = api.image()
    opened = image
    try:
        result = getattr(image, factory)(str(expression))
        if factory == "open":
            assert result is True
        else:
            opened = result
        assert opened.isopen()
        if pixeltype.startswith("d"):
            with pytest.raises(RuntimeError, match="ispersistent does not support images"):
                opened.ispersistent()
            with pytest.raises(RuntimeError, match="name does not support images"):
                opened.name()
        else:
            assert opened.ispersistent() and opened.name() == str(expression)
        assert opened.pixeltype() == pixeltype
        values = opened.getchunk()
        assert values.dtype == (np.complex128 if "complex" in pixeltype else np.float64)
        np.testing.assert_array_equal(values, pixels * 3.)
    finally:
        opened.done()
        if opened is not image:
            image.done()


@pytest.mark.parametrize("writer_backend", ["casa", "casars"])
@pytest.mark.parametrize("reader_backend", ["casa", "casars"])
@pytest.mark.parametrize("pixeltype", ["float", "double", "complex", "dcomplex"])
@pytest.mark.parametrize("factory", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("nested", [False, True], ids=["direct", "nested"])
def test_saved_expression_creates_interoperable_native_copies(tmp_path, writer_backend, reader_backend, pixeltype, factory, nested):
    writer_api, reader_api = _backend(writer_backend), _backend(reader_backend)
    source = tmp_path / "operand.im"
    expected = _typed_operand(writer_api, source, pixeltype) * 3.
    expression = tmp_path / "manifest.im"
    _manifest(expression, ImageExpr=f'"{source}" * 3', DataType="unknown")
    if nested:
        outer = tmp_path / "outer.im"
        _manifest(outer, ImageExpr=f'"{expression}" + 1')
        expression = outer
        expected += 1.
    output = tmp_path / "copied.im"
    writer, reader = writer_api.image(), reader_api.image()
    result = writer
    try:
        created = getattr(writer, factory)(infile=str(expression), outfile=str(output))
        if factory == "fromimage":
            assert created is True
        else:
            result = created
        assert result.done()
        assert reader.open(str(output))
        assert reader.pixeltype() == pixeltype
        np.testing.assert_array_equal(reader.getchunk(), expected)
    finally:
        reader.done()
        result.done()
        if result is not writer:
            writer.done()


@pytest.mark.parametrize("factory", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("location", ["same", "enclosing"])
def test_nested_saved_copy_does_not_overwrite_an_operand(tmp_path, factory, location):
    api = _backend("casars")
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    source = tmp_path / "operand.im"
    destination = tmp_path / "existing.im"
    _operand(api, source, pixels)
    _operand(api, destination, pixels + 100)
    inner = destination if location == "same" else destination / "inner.im"
    _manifest(inner, ImageExpr=f'"{source}" * 3')
    outer = tmp_path / "outer.im"
    _manifest(outer, ImageExpr=f'"{inner}" + 1')
    table = destination / "table.dat"
    manifest = inner / "imageexpr.json"
    original_table, original_manifest = table.read_bytes(), manifest.read_bytes()
    image = api.image()
    try:
        assert image.fromarray(pixels=[41., 42.])
        with pytest.raises(RuntimeError, match="overlaps a saved image expression operand"):
            getattr(image, factory)(infile=str(outer), outfile=str(destination), overwrite=True)
        assert table.read_bytes() == original_table
        assert manifest.read_bytes() == original_manifest
        np.testing.assert_array_equal(image.getchunk(), [41., 42.])
        assert image.open(str(outer))
        valid = image.getchunk(getmask=True)
        np.testing.assert_array_equal(valid, pixels > 4)
        np.testing.assert_array_equal(image.getchunk()[valid], (pixels * 3 + 1)[valid])
    finally:
        image.done()


@pytest.mark.parametrize("source_kind", ["saved", "virtual"])
@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("named", [False, True])
def test_inline_imagecalc_uses_bound_expression_operands(tmp_path, source_kind, backend, named):
    api = _backend(backend)
    other_api = _backend("casars" if backend == "casa" else "casa") if named else api
    leaf, extra, source, output = (tmp_path / name for name in
                                  ("leaf.im", "extra.im", "source.im", "output.im"))
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    _operand(api, leaf, values)
    extra_values = _typed_operand(api, extra, "float")
    tool = api.image()
    if source_kind == "saved":
        _manifest(source, ImageExpr=f'"{leaf}" * 3')
        expected = values * 3 + 2
        validity = values > 4
    else:
        tool.imageconcat(str(source), [str(leaf), str(extra)], axis=0, relax=True,
                         mode="nomove", tempclose=False).done()
        expected = np.concatenate([values, extra_values]) + 2
        validity = np.concatenate([values > 4, np.ones_like(extra_values, dtype=bool)])
    expression = tool.imagecalc(outfile=str(output) if named else "", pixels=f'"{source}" + 2')
    reader = expression
    try:
        if named:
            assert expression.done()
            reader = other_api.image()
            assert reader.open(str(output))
        np.testing.assert_array_equal(reader.getchunk(getmask=True), validity)
        np.testing.assert_array_equal(reader.getchunk()[validity], expected[validity])
        assert reader.pixeltype() == "float"
    finally:
        reader.done()
        if reader is not expression:
            expression.done()
        tool.done()


@pytest.mark.parametrize("source_kind", ["saved", "virtual"])
@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("operation", ["calc", "calcmask"])
def test_image_edits_use_bound_expression_operands(tmp_path, source_kind, backend, operation):
    api = _backend(backend)
    leaf, extra, source = (tmp_path / name for name in ("leaf.im", "extra.im", "source.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    for path in (leaf, extra):
        assert image.fromarray(str(path), pixels) and image.done()
    if source_kind == "saved":
        _manifest(source, ImageExpr=f'"{leaf}" * 3')
        values = pixels * 3
        # The named expression reads the very table being edited.
        assert image.open(str(leaf))
    else:
        image.imageconcat(str(source), [str(leaf), str(extra)], axis=0, relax=True,
                          mode="nomove", tempclose=False).done()
        values = np.concatenate([pixels, pixels])
        assert image.fromshape(shape=list(values.shape))
    try:
        if operation == "calc":
            assert image.calc(f'"{source}" + 1')
            np.testing.assert_array_equal(image.getchunk(), values + 1)
        else:
            assert image.calcmask(f'"{source}" > 4', name="chosen")
            np.testing.assert_array_equal(image.getchunk(getmask=True), values > 4)
    finally:
        assert image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_imagecalc_uses_saved_image_for_explicit_metadata(tmp_path, backend):
    api = _backend(backend)
    leaf, metadata = (tmp_path / name for name in ("leaf.im", "metadata.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    assert image.fromarray(str(leaf), pixels)
    assert image.setbrightnessunit("Jy/pixel")
    assert image.done()
    _manifest(metadata, ImageExpr=f'"{leaf}" * 3', MiscInfo={"station": "alma"})
    output = image.imagecalc(pixels=f'"{leaf}" + 1', imagemd=str(metadata))
    try:
        np.testing.assert_array_equal(output.getchunk(), pixels + 1)
        assert output.brightnessunit() == "Jy/pixel"
        assert output.miscinfo()["station"] == "alma"
    finally:
        assert output.done() and image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("previous", [False, True], ids=["new", "replace"])
@pytest.mark.parametrize("kind", ["saved", "virtual"])
def test_imagecalc_reads_its_output_through_metadata_image(tmp_path, backend, previous, kind):
    api = _backend(backend)
    leaf, output, metadata = (tmp_path / name for name in ("leaf.im", "output.im", "metadata.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    assert image.fromarray(str(leaf), pixels) and image.done()
    if previous or kind == "virtual":
        assert image.fromarray(str(output), pixels * 5)
        assert image.setbrightnessunit("K") and image.done()
    if kind == "saved":
        _manifest(metadata, ImageExpr=f'"{output}" * 3', MiscInfo={"station": "alma"})
    else:
        joined = image.imageconcat(str(metadata), [str(output), str(leaf)], axis=0,
                                   mode="nomove", relax=True)
        assert joined.setmiscinfo({"station": "alma"}) and joined.done()
        if not previous:
            assert image.open(str(output)) and image.remove() and image.done()
    result = image.imagecalc(str(output), f'"{leaf}" + 13', overwrite=previous, imagemd=str(metadata))
    try:
        np.testing.assert_array_equal(result.getchunk(), pixels + 13)
        assert result.brightnessunit() == ""
        assert result.miscinfo()["station"] == "alma"
    finally:
        assert result.done() and image.done()


def test_imagecalc_does_not_replace_output_that_contains_its_metadata_operand(tmp_path):
    api = _backend("casars")
    leaf, output, metadata = (tmp_path / name for name in ("leaf.im", "output.im", "metadata.im"))
    inner = output / "inner.im"
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    for path in (leaf, output, inner):
        assert image.fromarray(str(path), pixels) and image.done()
    _manifest(metadata, ImageExpr=f'"{inner}" * 3')
    try:
        with pytest.raises(RuntimeError, match="overlaps a saved image expression input"):
            image.imagecalc(str(output), f'"{leaf}" + 13', overwrite=True, imagemd=str(metadata))
        for path in (output, inner):
            assert image.open(str(path))
            np.testing.assert_array_equal(image.getchunk(), pixels)
            assert image.done()
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_imagecalc_validates_metadata_type_against_replacement_precision(tmp_path, backend):
    api = _backend(backend)
    leaf, output, metadata = (tmp_path / name for name in ("leaf.im", "output.im", "metadata.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    for path in (leaf, output):
        assert image.fromarray(str(path), pixels) and image.done()
    _manifest(metadata, ImageExpr=f'"{output}" * 3')
    try:
        with pytest.raises(RuntimeError, match="Unsupported image pixel data type"):
            image.imagecalc(str(output), f'"{leaf}" + 13', overwrite=True,
                            imagemd=str(metadata), prec="double")
        assert image.open(str(output))
        if backend == "casars":
            assert image.pixeltype() == "float"
            np.testing.assert_array_equal(image.getchunk(), pixels)
        else:
            assert image.pixeltype() == "double"
            np.testing.assert_array_equal(image.getchunk(), pixels + 13)
    finally:
        image.done()


def test_lazy_imagecalc_refreshes_a_previously_read_saved_operand_section(tmp_path):
    api = _backend("casars")
    leaf, manifest = (tmp_path / name for name in ("leaf.im", "expression.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    assert image.fromarray(str(leaf), pixels) and image.done()
    _manifest(manifest, ImageExpr=f'"{leaf}" * 3')
    result = image.imagecalc(pixels=f'"{manifest}" + 1')
    try:
        np.testing.assert_array_equal(result.getchunk([0, 0], [0, 0]), [[1.]])
        assert result.unlock()
        assert image.open(str(leaf)) and image.putchunk(pixels + 50) and image.done()
        np.testing.assert_array_equal(result.getchunk([0, 0], [0, 0]), [[151.]])
    finally:
        result.done()
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("method", ["imagecalc", "fromimage", "newimagefromimage"])
def test_image_expression_can_create_a_new_child_of_its_operand(tmp_path, backend, method):
    api = _backend(backend)
    leaf, source = (tmp_path / name for name in ("leaf.im", "source.im"))
    output = leaf / "new.im"
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    assert image.fromarray(str(leaf), pixels) and image.done()
    _manifest(source, ImageExpr=f'"{leaf}" * 3')
    if method == "imagecalc":
        result = image.imagecalc(str(output), f'"{source}" + 1', overwrite=True)
    else:
        result = getattr(image, method)(infile=str(leaf), outfile=str(output),
                                        mask=f'"{source}" > 4', overwrite=True)
    if method == "fromimage":
        assert result is True
        result = image
    try:
        np.testing.assert_array_equal(result.getchunk(), pixels * 3 + 1 if method == "imagecalc" else pixels)
        if method != "imagecalc":
            np.testing.assert_array_equal(result.getchunk(getmask=True), pixels * 3 > 4)
        assert result.done()
        assert image.open(str(leaf))
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()


@pytest.mark.parametrize("source_kind", ["saved", "virtual"])
@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("factory", ["fromimage", "newimagefromimage"])
def test_image_copy_uses_bound_expression_mask(tmp_path, source_kind, backend, factory):
    api = _backend(backend)
    other_api = _backend("casars" if backend == "casa" else "casa")
    leaf, extra, source, output = (tmp_path / name for name in
                                  ("leaf.im", "extra.im", "source.im", "output.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    reader = other_api.image()
    for path in (leaf, extra):
        assert image.fromarray(str(path), pixels) and image.done()
    if source_kind == "saved":
        _manifest(source, ImageExpr=f'"{leaf}" * 3')
        values = pixels * 3
    else:
        image.imageconcat(str(source), [str(leaf), str(extra)], axis=0, relax=True,
                          mode="nomove", tempclose=False).done()
        values = np.concatenate([pixels, pixels])
    result = getattr(image, factory)(infile=str(source), outfile=str(output), mask=f'"{source}" > 4')
    if factory == "fromimage":
        assert result is True
        result = image
    try:
        assert result.done()
        assert reader.open(str(output))
        np.testing.assert_array_equal(reader.getchunk(), values)
        np.testing.assert_array_equal(reader.getchunk(getmask=True), values > 4)
    finally:
        reader.done()
        image.done()


@pytest.mark.parametrize("factory", ["fromimage", "newimagefromimage"])
def test_image_copy_preserves_a_transitive_mask_operand(tmp_path, factory):
    api = _backend("casars")
    source, output, mask = (tmp_path / name for name in ("source.im", "output.im", "mask.im"))
    values = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    assert image.fromarray(str(source), values * 10) and image.done()
    assert image.fromarray(str(output), values) and image.done()
    _manifest(mask, ImageExpr=f'"{output}" * 1')
    try:
        with pytest.raises(RuntimeError, match="overlaps a saved image expression input"):
            getattr(image, factory)(infile=str(source), outfile=str(output),
                                    mask=f'"{mask}" > 4', overwrite=True)
        assert image.open(str(output))
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        assert image.done()


@pytest.mark.parametrize("source_kind", ["saved", "virtual"])
def test_inline_imagecalc_keeps_transitive_source_before_overwrite(tmp_path, source_kind):
    api = _backend("casars")
    leaf, source, extra = (tmp_path / name for name in ("leaf.im", "source.im", "extra.im"))
    values = _typed_operand(api, leaf, "float")
    other = _typed_operand(api, extra, "float")
    tool = api.image()
    if source_kind == "saved":
        _manifest(source, ImageExpr=f'"{leaf}" * 3')
    else:
        tool.imageconcat(str(source), [str(leaf), str(extra)], axis=0,
                         relax=True, mode="nomove").done()
    try:
        with pytest.raises(RuntimeError, match="Caught TableError"):
            tool.imagecalc(outfile=str(leaf), pixels=f'"{source}" + 1', overwrite=True)
        assert tool.open(str(leaf))
        np.testing.assert_array_equal(tool.getchunk(), values)
        assert tool.open(str(source))
        expected = values * 3 if source_kind == "saved" else np.concatenate([values, other])
        np.testing.assert_array_equal(tool.getchunk(), expected)
    finally:
        tool.done()
