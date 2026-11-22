import re

import numpy as np
import pytest

from casars import tools
from test_image_removal import _backend

METHODS = ("fromshape", "fromarray", "newimagefromshape", "newimagefromarray")


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader_backend", ["casa", "casars"])
@pytest.mark.parametrize("method", METHODS)
def test_named_creation_resolves_output_and_keeps_the_argument(
    tmp_path, monkeypatch, writer, reader_backend, method,
):
    api = _backend(writer)
    reader_api = _backend(reader_backend)
    dest = tmp_path / "expanded"
    dest.mkdir()
    monkeypatch.chdir(tmp_path)
    literal = tmp_path / "$CASARS_CREATE_PATH"
    literal.mkdir()
    monkeypatch.setenv("CASARS_CREATE_PATH", str(dest))
    target = dest / "target.im"
    argument = "$CASARS_CREATE_PATH/target.im"
    image = api.image()
    product = image
    expected = np.arange(12, dtype=np.float32).reshape(3, 4)
    if method.endswith("shape"):
        supplied = {"shape": [3, 4]}
        expected = np.zeros((3, 4), dtype=np.float32)
    else:
        supplied = {"pixels": expected}
    try:
        assert image.fromarray(pixels=[8., 9.])
        result = getattr(image, method)(outfile=argument, **supplied)
        if method.startswith("new"):
            product = result
            np.testing.assert_array_equal(image.getchunk(), [8., 9.])
        else:
            assert result is True
        assert product.name() == str(target)
        assert not (literal / target.name).exists()
        np.testing.assert_array_equal(product.getchunk(), expected)
        assert f"Created Paged image '{argument}'" in "\n".join(product.history())
        assert product.done()
        reader = reader_api.image()
        try:
            assert reader.open(str(target))
            np.testing.assert_array_equal(reader.getchunk(), expected)
        finally:
            reader.done()
    finally:
        if product is not image:
            product.done()
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("method", METHODS)
def test_named_creation_errors_keep_the_destination_and_release_the_caller(tmp_path, monkeypatch, backend, method):
    api = _backend(backend)
    image = api.image()
    target = tmp_path / "target.im"
    monkeypatch.setenv("CASARS_CREATE_PATH", str(tmp_path))
    monkeypatch.setenv("CASARS_CREATE_CYCLE", "$CASARS_CREATE_CYCLE")
    supplied = {"shape": [3, 4]} if method.endswith("shape") else {"pixels": [[1., 2.], [3., 4.]]}
    collision = "$CASARS_CREATE_PATH/target.im"
    missing = "$CASARS_CREATE_PATH/missing/target.im"
    try:
        assert image.fromarray(str(target), [31., 32.])
        assert image.done()
        for path, diagnostic in (
            (collision, rf"File {re.escape(collision)} exists"),
            ("$CASARS_CREATE_CYCLE/target.im", "recursive environment variable"),
            (missing, "does not exist and cannot be created"),
        ):
            assert image.fromarray(pixels=[8., 9.])
            with pytest.raises(RuntimeError, match=diagnostic):
                getattr(image, method)(outfile=path, **supplied)
            assert image.isopen() == method.startswith("new")
            if image.isopen():
                np.testing.assert_array_equal(image.getchunk(), [8., 9.])
            assert image.open(str(target))
            np.testing.assert_array_equal(image.getchunk(), [31., 32.])
            assert image.done()
        assert not (tmp_path / "missing").exists()
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("physical_exists", [False, True])
def test_named_creation_targets_the_lexical_path(tmp_path, monkeypatch, backend, physical_exists):
    api = _backend(backend)
    left, right = tmp_path / "lexical", tmp_path / "physical"
    left.mkdir()
    (right / "nested").mkdir(parents=True)
    (left / "alias").symlink_to(right / "nested", target_is_directory=True)
    monkeypatch.setenv("CASARS_CREATE_PATH", str(left))
    image = api.image()
    physical_target = right / "target.im"
    argument = "$CASARS_CREATE_PATH/alias/../target.im"
    try:
        if physical_exists:
            assert image.fromarray(str(physical_target), [41., 42.])
            assert image.done()
            with pytest.raises(RuntimeError, match=rf"File {re.escape(argument)} exists"):
                image.fromshape(outfile=argument, shape=[2, 3])
            assert not image.isopen()
            assert not (left / "target.im").exists()
        assert image.fromshape(outfile=argument, shape=[2, 3], overwrite=physical_exists)
        assert image.name() == str(left / "target.im")
        np.testing.assert_array_equal(image.getchunk(), np.zeros((2, 3), dtype=np.float32))
        if physical_exists:
            assert image.open(str(physical_target))
            np.testing.assert_array_equal(image.getchunk(), [41., 42.])
        else:
            assert not physical_target.exists()
    finally:
        image.done()


def test_named_creation_preserves_the_lexical_target_when_overwrite_is_false(tmp_path, monkeypatch):
    left, right = tmp_path / "lexical", tmp_path / "physical"
    left.mkdir()
    (right / "nested").mkdir(parents=True)
    (left / "alias").symlink_to(right / "nested", target_is_directory=True)
    monkeypatch.setenv("CASARS_CREATE_PATH", str(left))
    argument = "$CASARS_CREATE_PATH/alias/../target.im"
    target = left / "target.im"
    image = tools.image()
    try:
        assert image.fromarray(str(target), [6., 13.])
        original_history = image.history()
        assert image.done()
        with pytest.raises(RuntimeError, match=rf"File {re.escape(argument)} exists"):
            image.fromshape(outfile=argument, shape=[2, 3])
        assert not image.isopen()
        assert image.open(str(target))
        np.testing.assert_array_equal(image.getchunk(), [6., 13.])
        assert image.history() == original_history
        assert not (right / "target.im").exists()
        assert image.fromshape(outfile=argument, shape=[2, 3], overwrite=True)
        np.testing.assert_array_equal(image.getchunk(), np.zeros((2, 3), dtype=np.float32))
        assert not (right / "target.im").exists()
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("suffix", ["", "/", "/."])
def test_named_creation_through_an_output_symlink_keeps_the_link(tmp_path, monkeypatch, backend, suffix):
    api = _backend(backend)
    real = tmp_path / "real.im"
    alias = tmp_path / "alias.im"
    alias.symlink_to(real, target_is_directory=True)
    monkeypatch.setenv("CASARS_CREATE_PATH", str(tmp_path))
    argument = "$CASARS_CREATE_PATH/alias.im" + suffix
    image = api.image()
    try:
        with pytest.raises(RuntimeError, match="is not a true table directory"):
            image.fromshape(outfile=argument, shape=[3], overwrite=True)
        assert alias.is_symlink() and not real.exists()
        assert image.fromarray(str(real), [18., 34.])
        assert image.done()
        product = image.newimagefromshape(outfile=argument, shape=[3], overwrite=True)
        try:
            assert alias.is_symlink()
            assert product.name() == str(alias)
            assert f"Created Paged image '{argument}'" in "\n".join(product.history())
            np.testing.assert_array_equal(product.getchunk(), [0., 0., 0.])
        finally:
            product.done()
        for path in (real, alias):
            assert image.open(str(path))
            np.testing.assert_array_equal(image.getchunk(), [0., 0., 0.])
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("method", ["fromshape", "newimagefromshape"])
def test_invalid_pixel_type_takes_precedence_over_output_errors(tmp_path, monkeypatch, backend, method):
    api = _backend(backend)
    image = api.image()
    target = tmp_path / "existing.im"
    monkeypatch.setenv("CASARS_CREATE_PATH", str(tmp_path))
    monkeypatch.setenv("CASARS_CREATE_CYCLE", "$CASARS_CREATE_CYCLE")
    try:
        assert image.fromarray(str(target), [18., 34.])
        assert image.done()
        for argument in ("$CASARS_CREATE_PATH/existing.im", "$CASARS_CREATE_CYCLE/existing.im"):
            assert image.fromarray(pixels=[8., 9.])
            with pytest.raises(RuntimeError, match="type must be either"):
                getattr(image, method)(outfile=argument, shape=[3], type="bad")
            assert image.isopen() == method.startswith("new")
            if image.isopen():
                np.testing.assert_array_equal(image.getchunk(), [8., 9.])
        assert image.open(str(target))
        np.testing.assert_array_equal(image.getchunk(), [18., 34.])
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("method", METHODS)
def test_invalid_coordinate_record_takes_precedence_over_output_errors(tmp_path, monkeypatch, backend, method):
    api = _backend(backend)
    image = api.image()
    target = tmp_path / "existing.im"
    monkeypatch.setenv("CASARS_CREATE_PATH", str(tmp_path))
    monkeypatch.setenv("CASARS_CREATE_CYCLE", "$CASARS_CREATE_CYCLE")
    record = api.coordsys().newcoordsys(linear=2).torecord()
    record["pixelmap0"] = ["invalid", "invalid"]
    supplied = {"shape": [3, 4]} if method.endswith("shape") else {"pixels": [[1., 2.], [3., 4.]]}
    try:
        assert image.fromarray(str(target), [18., 34.])
        assert image.done()
        for argument in ("$CASARS_CREATE_PATH/existing.im", "$CASARS_CREATE_CYCLE/existing.im"):
            assert image.fromarray(pixels=[8., 9.])
            with pytest.raises(RuntimeError, match="Invalid coordinate system record|incorrect data type"):
                getattr(image, method)(outfile=argument, csys=record, **supplied)
            assert image.isopen() == method.startswith("new")
            if image.isopen():
                np.testing.assert_array_equal(image.getchunk(), [8., 9.])
        assert image.open(str(target))
        np.testing.assert_array_equal(image.getchunk(), [18., 34.])
    finally:
        image.done()
