# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt public transform tasks at their opt-in test checkpoints."""

import shutil

import numpy as np
import pytest

from casars import _core, tasks, tools
from _table_readback import open_table
from test_tools_msmetadata import measures, native_dataset


def messages(path):
    log = open_table(path)
    try:
        return list(log.getcol("MESSAGE"))
    finally:
        assert log.done()


def checkpoints(log, stage):
    prefix = f"transform test checkpoint {stage}"
    return [message.removeprefix(prefix) for message in log if message.startswith(prefix)]


@pytest.mark.skipif(
    not hasattr(_core, "_arm_transform_test"), reason="requires transform-test-hooks on POSIX"
)
@pytest.mark.parametrize(
    ("surface", "options", "stage"),
    [
        pytest.param("mstransform", {}, "visibility", id="mstransform"),
        pytest.param("split", {}, "visibility", id="split"),
        pytest.param(
            "mstransform", {"createmms": True, "separationaxis": "scan"},
            "visibility", id="mstransform-mms",
        ),
        pytest.param("mstransform", {}, "scalar", id="scalar"),
    ],
)
def test_sigint_discards_staged_transform_and_allows_retry(
    surface, options, stage, native_dataset, tmp_path, monkeypatch
):
    source = tmp_path / "input.ms"
    shutil.copytree(native_dataset, source)
    output = tmp_path / "output.ms"
    logfile = tmp_path / "interrupted.logtable"
    input_metadata = (source / "table.dat").read_bytes()
    reader = open_table(source)
    rows = reader.nrows()
    input_data = reader.getcol("DATA")
    input_flags = reader.getcol("FLAG")
    assert reader.done()
    assert rows > 1

    task = getattr(tasks, surface)
    parameters = {
        "vis": str(source),
        "outputvis": str(output),
        "datacolumn": "data",
        "spw": "0:1~2",
        **options,
    }
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(logfile))
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    _core._arm_transform_test(stage)
    with pytest.raises(KeyboardInterrupt):
        task(**parameters)

    log = messages(logfile)
    assert any(message.startswith(f"{surface} interrupted") for message in log)
    if stage == "visibility":
        assert checkpoints(log, "VisibilityBatchStarted") == ["(0)"]
        assert checkpoints(log, "VisibilityBatchStaged") == ["(0)"]
    else:
        assert checkpoints(log, "ScalarChunkStaged") == [""]
        assert not checkpoints(log, "VisibilityBatchStarted")
    assert not checkpoints(log, "BeforePublication")
    assert not output.exists()
    assert not list(tmp_path.glob(".output.ms.casa-rs-incomplete-*"))
    assert (source / "table.dat").read_bytes() == input_metadata
    reader = open_table(source)
    assert reader.nrows() == rows
    np.testing.assert_array_equal(reader.getcol("DATA"), input_data)
    np.testing.assert_array_equal(reader.getcol("FLAG"), input_flags)
    assert reader.done()

    retry_log = tmp_path / "retry.logtable"
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(retry_log))
    _core._arm_transform_test("observe")
    task(**parameters)
    assert checkpoints(messages(retry_log), "VisibilityBatchStarted")[:2] == ["(0)", "(1)"]
    assert not list(tmp_path.glob(".output.ms.casa-rs-incomplete-*"))

    out = open_table(output)
    assert out.nrows() == rows
    assert out.getcol("DATA").shape == (input_data.shape[0], 2, rows)
    assert out.getcol("FLAG").shape == (input_flags.shape[0], 2, rows)
    assert out.done()
    metadata = tools.msmetadata()
    assert metadata.open(str(output))
    assert metadata.nchan(0) == 2
    assert metadata.done()
