# SPDX-License-Identifier: LGPL-3.0-or-later
"""Drive SIGINT through public imstat during calculation and before publication."""

import numpy as np
import pytest

from casars import _core, tasks, tools
from test_native_transform_interrupt import messages


pytestmark = pytest.mark.skipif(
    not hasattr(_core, "_arm_imstat_test"), reason="requires imstat-test-hooks on POSIX"
)


def image_contents(source):
    return {
        path.relative_to(source): path.read_bytes()
        for path in source.rglob("*")
        if path.is_file() and path.name != "table.lock"
    }


@pytest.mark.parametrize(
    ("stage", "append", "existing"),
    [
        pytest.param("plane", False, True, id="plane-overwrite"),
        pytest.param("plane", True, True, id="plane-append"),
        pytest.param("before-logfile", False, False, id="before-logfile-create"),
        pytest.param("before-logfile", False, True, id="before-logfile-overwrite"),
    ],
)
def test_imstat_sigint_preserves_logfile(
    tmp_path, monkeypatch, stage, append, existing
):
    monkeypatch.chdir(tmp_path)
    pixels = np.arange(60, dtype=np.float32).reshape(4, 5, 3)
    image = tools.image()
    assert image.fromarray("cube.im", pixels)
    assert image.done()
    source = tmp_path / "cube.im"
    original = image_contents(source)
    logfile = tmp_path / "statistics.log"
    sentinel = b"Existing statistics report\n"
    if existing:
        logfile.write_bytes(sentinel)
    trace = tmp_path / "imstat.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    parameters = dict(
        imagename="cube.im", axes=[0, 1], logfile="statistics.log",
        append=append, listit=False, verbose=False,
    )

    _core._arm_imstat_test(stage)
    with pytest.raises(KeyboardInterrupt):
        tasks.imstat(**parameters)

    log = messages(trace)
    prefix = "imstat test checkpoint "
    checkpoints = [line.removeprefix(prefix) for line in log if line.startswith(prefix)]
    plane = ["PlaneStarted", "PlaneFinished"]
    assert checkpoints == (plane if stage == "plane" else plane * 3 + ["BeforeLogfile"])
    if existing:
        assert logfile.read_bytes() == sentinel
    else:
        assert not logfile.exists()
    assert "imstat interrupted task=imstat reason=imstat interrupted" in log
    assert image_contents(source) == original
    assert image.open("cube.im")
    np.testing.assert_array_equal(image.getchunk(), pixels)
    assert image.done()

    result = tasks.imstat(**parameters)
    np.testing.assert_array_equal(result["npts"], [20, 20, 20])
    np.testing.assert_allclose(result["mean"], pixels.mean(axis=(0, 1)))
    report = logfile.read_bytes()
    assert b"cube.im" in report
    assert report.startswith(sentinel) == append
