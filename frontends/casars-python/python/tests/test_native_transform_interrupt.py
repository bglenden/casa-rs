# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt both public transform tasks after they have staged visibility data."""

import shutil
import signal
import sys

import numpy as np
import pytest

from casars import tasks, tools
from _table_readback import open_table
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize(
    ("surface", "options"),
    [
        pytest.param("mstransform", {}, id="mstransform"),
        pytest.param("split", {}, id="split"),
        pytest.param(
            "mstransform", {"createmms": True, "separationaxis": "scan"}, id="mstransform-mms"
        ),
    ],
)
def test_sigint_discards_staged_transform_and_allows_retry(
    surface, options, native_dataset, tmp_path, monkeypatch
):
    source = tmp_path / "input.ms"
    shutil.copytree(native_dataset, source)
    output = tmp_path / "output.ms"
    logfile = tmp_path / "task.logtable"
    input_metadata = (source / "table.dat").read_bytes()
    reader = open_table(source)
    rows = reader.nrows()
    input_data = reader.getcol("DATA")
    input_flags = reader.getcol("FLAG")
    assert reader.done()

    stderr = sys.stderr
    in_flight = []

    class Terminal:
        def write(self, line):
            if "stage=first_visibility_batch " in line and not in_flight:
                staging = list(tmp_path.glob(".output.ms.casa-rs-incomplete-*"))
                in_flight.append((len(staging), output.exists()))
                signal.raise_signal(signal.SIGINT)
            return stderr.write(line)

        def flush(self):
            stderr.flush()

    task = getattr(tasks, surface)
    parameters = {
        "vis": str(source),
        "outputvis": str(output),
        "datacolumn": "data",
        "spw": "0:1~2",
        **options,
    }
    with monkeypatch.context() as patch:
        patch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "DEBUG")
        patch.setenv("CASA_RS_LOG_TABLE", str(logfile))
        patch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
        patch.setattr(sys, "stderr", Terminal())
        with pytest.raises(KeyboardInterrupt):
            task(**parameters)

    assert in_flight == [(1, False)]
    assert not output.exists()
    assert not list(tmp_path.glob(".output.ms.casa-rs-incomplete-*"))
    log = open_table(logfile)
    messages = log.getcol("MESSAGE")
    assert log.done()
    assert any(message.startswith(f"{surface} interrupted") for message in messages)
    assert not any("stage=select_channels " in message for message in messages)
    assert (source / "table.dat").read_bytes() == input_metadata
    reader = open_table(source)
    assert reader.nrows() == rows
    np.testing.assert_array_equal(reader.getcol("DATA"), input_data)
    np.testing.assert_array_equal(reader.getcol("FLAG"), input_flags)
    assert reader.done()

    task(**parameters)
    out = open_table(output)
    assert rows > 0 and out.nrows() == rows
    assert out.getcol("DATA").shape == (input_data.shape[0], 2, rows)
    assert out.getcol("FLAG").shape == (input_flags.shape[0], 2, rows)
    assert out.done()
    metadata = tools.msmetadata()
    assert metadata.open(str(output))
    assert metadata.nchan(0) == 2
    assert metadata.done()
