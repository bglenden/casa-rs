"""Miscellaneous image records and history compared with native CASA products."""

import numpy as np
import pytest

from casars import tools

from test_image_workflow import _assert_value


@pytest.mark.parametrize("writer", ["casars", "casa"])
@pytest.mark.parametrize("reader", ["casars", "casa"])
def test_misc_info_and_history_on_disk(tmp_path, writer, reader):
    casa = pytest.importorskip("casatools") if "casa" in (writer, reader) else None
    backends = {"casars": tools, "casa": casa}
    path = str(tmp_path / "metadata.im")
    image = backends[writer].image()
    assert image.fromshape(path, [2, 3])
    assert image.setmiscinfo({"old": "remove"})
    assert image.dohistory(False)
    info = {
        "flags": np.array([[True, False]], dtype=np.bool_),
        "units": {"label": "test", "index": np.array([[1, 2], [3, 4]], dtype=np.int64)},
    }
    assert image.setmiscinfo(info)
    assert image.sethistory("probe", ["first", "", "second"])
    assert image.sethistory(history="default origin")
    assert image.sethistory("ignored", ("tuple", "items"))
    with pytest.raises(TypeError, match="list _history must contain strings"):
        image.sethistory("invalid", ["text", 3])
    expected = [
        f"Created Paged image '{path}' of shape [2, 3] with float valued pixels.",
        "Ran ia.fromshape",
        f'ia.fromshape(outfile="{path}", shape=[2,3], csys={{...}}, linear=false, overwrite=false, log=true, type="f")',
        "Ran ia.setmiscinfo",
        "ia.setmiscinfo(info={...})",
        "first",
        "second",
        "default origin",
    ]
    _assert_value(image.history(False), expected)
    assert image.done()

    reopened = backends[reader].image()
    assert reopened.open(path)
    _assert_value(reopened.miscinfo(), info)
    _assert_value(reopened.history(False), expected)
    assert reopened.done()

    table = backends[reader].table()
    assert table.open(path + "/logtable")
    _assert_value(table.getcol("LOCATION"), np.array([
        "::ImageFactory::createImage",
        "::image::fromshape",
        "::image::fromshape",
        "::image::setmiscinfo",
        "::image::setmiscinfo",
        "::probe",
        "::probe",
        "ImageHistory::addHistory",
    ]))
    assert table.done()


def test_temporary_misc_info_and_history():
    ia = tools.image()
    assert ia.miscinfo() == {}
    assert not ia.setmiscinfo({})
    assert not ia.sethistory(history=["closed"])
    assert ia.fromshape(shape=[3, 4])
    assert ia.setmiscinfo({"key": "temporary"})
    assert ia.miscinfo() == {"key": "temporary"}
    assert ia.setmiscinfo({})
    assert ia.miscinfo() == {}
    assert ia.sethistory("temp", ["memory only"])
    assert ia.history(False)[-1] == "memory only"
    assert ia.done()
    assert ia.history(False) == []
