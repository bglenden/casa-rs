"""Ownership and creation history for public image factory methods."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(params=["casars", "casa"])
def backend(request):
    return tools if request.param == "casars" else pytest.importorskip("casatools")


@pytest.mark.parametrize("method", ["newimage", "newimagefromfile"])
def test_native_file_factory_returns_independent_handle(tmp_path, backend, method):
    path = str(tmp_path / "source.im")
    ia = backend.image()
    assert ia.fromshape(path, [2, 3])
    attached = getattr(ia, method)(path)
    assert type(attached) is backend.image
    assert attached is not ia
    assert attached.isopen()
    _assert_value(attached.getchunk(), np.zeros((2, 3)))
    assert ia.done()
    assert attached.isopen()
    assert attached.putchunk(np.ones((1, 1)), [0, 0])
    assert attached.done(remove=True)
    assert not Path(path).exists()


@pytest.mark.parametrize("method", ["newimagefromshape", "newimagefromarray"])
@pytest.mark.parametrize("history_enabled", [True, False])
def test_creation_factory_keeps_original_image_and_history_policy(
    tmp_path, backend, method, history_enabled
):
    ia = backend.image()
    assert ia.fromshape(str(tmp_path / "source.im"), [1, 2])
    assert ia.dohistory(history_enabled)
    data = np.array([[1.5, -2.0], [3.0, 0.75]])
    kwargs = {"shape": [2, 2]} if method == "newimagefromshape" else {"pixels": data}
    created = getattr(ia, method)(**kwargs)
    assert type(created) is backend.image
    assert created is not ia
    assert ia.isopen()
    _assert_value(created.getchunk(), np.zeros((2, 2)) if "shape" in kwargs else data)
    history = ["Created Temp image  of shape [2, 2] with float valued pixels."]
    if history_enabled:
        arguments = "shape=[2,2]" if "shape" in kwargs else "pixels=[1.500000,3.000000,-2.000000,0.750000]"
        tail = ', type="f"' if "shape" in kwargs else ""
        history += [
            f"Ran ia.{method}",
            f'ia.{method}(outfile="", {arguments}, csys={{...}}, linear=false, overwrite=false, log=true{tail})',
        ]
    _assert_value(created.history(False), history)
    assert created.putchunk(np.ones((1, 1)))
    assert created.history(False)[-2] == "Ran ia.putchunk"
    assert created.done()
    assert ia.isopen()
    _assert_value(ia.shape(), np.array([1, 2], dtype=np.int64))


def test_failed_factory_leaves_original_tool_open(backend):
    ia = backend.image()
    assert ia.fromshape(shape=[2, 3])
    with pytest.raises(RuntimeError, match="File name is empty"):
        ia.newimage("")
    with pytest.raises(RuntimeError, match="must be positive"):
        ia.newimagefromshape(shape=[0, 3])
    assert ia.isopen()
