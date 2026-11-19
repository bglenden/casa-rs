"""Public native mask management using masks created by pinned CASA."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture
def masked_image(tmp_path):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    path = str(tmp_path / "mask-source.im")
    image = casa.image()
    assert image.fromarray(path, np.array([[-1.0, 0.0], [1.0, 2.0]]))
    assert image.calcmask(f'"{path}" > 0', "z", False)
    assert image.calcmask(f'"{path}" <= 0', "a", True)
    assert image.done()
    return casa, path


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_modify_ordered_named_masks_in_a_native_image(masked_image, backend):
    casa, path = masked_image
    module = casa if backend == "casa" else tools
    image = module.image()
    assert image.open(path)
    assert image.maskhandler("get") == ["z", "a"]
    assert image.maskhandler() == ["a"]
    assert image.maskhandler("set", "z") == ["T"]
    assert image.maskhandler("cop", ["z", "b"]) == ["T"]
    with pytest.raises(RuntimeError, match="Mask b already exists"):
        image.maskhandler("copy", ["z", "b"])
    assert image.maskhandler("REN", ["z", "y"]) == ["T"]
    assert image.maskhandler("def") == ["y"]
    assert image.maskhandler("getall") == ["y", "a", "b"]
    expected = np.array([[False, False], [True, True]])
    _assert_value(image.getchunk(getmask=True), expected)
    assert image.done()

    reader = tools if backend == "casa" else casa
    image = reader.image()
    assert image.open(path)
    assert image.maskhandler("get") == ["y", "a", "b"]
    assert image.maskhandler("default") == ["y"]
    _assert_value(image.getchunk(getmask=True), expected)
    assert image.maskhandler("delete", ["missing", "y"]) == ["T"]
    assert image.maskhandler("default") == [""]
    assert image.maskhandler("get") == ["a", "b"]
    _assert_value(image.getchunk(getmask=True), np.ones((2, 2), dtype=np.bool_))
    assert image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_copy_mask_to_a_temporary_image(masked_image, backend):
    casa, path = masked_image
    module = casa if backend == "casa" else tools
    image = module.image()
    assert image.maskhandler() == []
    assert image.fromshape(shape=[2, 2])
    assert image.maskhandler("get") == ["T"]
    assert image.maskhandler("copy", [f"{path}:z", "first"]) == ["T"]
    assert image.maskhandler("copy", [f"{path}:a", "second"]) == ["T"]
    assert image.maskhandler("get") == ["first", "second"]
    assert image.maskhandler("default") == [""]
    assert image.maskhandler("set", "first") == ["T"]
    _assert_value(image.getchunk(getmask=True), np.array([[False, False], [True, True]]))
    assert image.maskhandler("set") == ["T"]
    assert image.maskhandler("default") == [""]
    assert image.done()
