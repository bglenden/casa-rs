"""Pinned behavior of the public image tool on a saved expression."""

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
