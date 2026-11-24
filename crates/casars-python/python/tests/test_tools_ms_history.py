"""Persist and replay MS history with independent casacore table readers."""

import logging
import time

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def check_history(api, dataset, expected, start, end, epoch_offset=0):
    table = api.table()
    assert table.open(str(dataset / "HISTORY"))
    assert table.nrows() == len(expected)
    for row, (message, command, origin, app) in enumerate(expected):
        assert table.getcell("MESSAGE", row) == message
        assert table.getcell("CLI_COMMAND", row).tolist() == [command]
        assert table.getcell("APP_PARAMS", row).tolist() == [""]
        assert table.getcell("ORIGIN", row) == origin
        assert table.getcell("APPLICATION", row) == app
        assert table.getcell("PRIORITY", row) == "INFO"
        assert table.getcell("OBSERVATION_ID", row) == -1
        assert table.getcell("OBJECT_ID", row) == 0
    epoch = 40_587 * 86_400 + epoch_offset
    times = table.getcol("TIME")
    assert times.dtype == np.float64
    assert times.shape == (len(expected),)
    assert np.all((epoch + start - 1 <= times) & (times <= epoch + end + 1))
    assert np.all(np.diff(times) >= 0)
    assert table.done()


@pytest.mark.parametrize("implementation", ["casa", "native"])
def test_ms_write_and_list_history_preserves_persisted_rows(native_dataset, implementation, caplog):
    casa = pinned_casa()
    module = casa if implementation == "casa" else tools
    selector = module.ms()
    assert selector.listhistory() is False
    assert selector.writehistory() is False
    assert selector.writehistory_batch() is False
    assert selector.writehistory_batch(None) is False
    with pytest.raises(TypeError, match="argument _message"):
        selector.writehistory(None)
    with pytest.raises(RuntimeError):
        selector.writehistory("detached")

    path = str(native_dataset)
    start = time.time()
    default_origin = "MSHistoryHandler::addMessage()"
    assert selector.writehistory(b"explicit target", "params", "", path, "one")
    assert selector.open(path)
    assert selector.select({"rows": [0]})
    assert selector.writehistory("attached one")
    assert selector.writehistory(parms="command only", origin="caller")
    assert selector.writehistory_batch(["", "two", "three", ""], origin="batch", app="combined")
    assert selector.writehistory_batch("scalar batch argument")
    assert selector.writehistory_batch(["", ""])
    assert selector.writehistory_batch([], parms="no rows")
    assert selector.writehistory_batch(("tuple", "ignored")) is False
    with pytest.raises(TypeError, match="list _messages must contain strings"):
        selector.writehistory_batch(["text", 1])
    end = time.time()
    with caplog.at_level(logging.INFO, logger="casars.tools.ms"):
        assert selector.listhistory()
    expected = [
        ("explicit target", "params", default_origin, "one"),
        ("attached one", "", default_origin, "ms"),
        ("", "command only", "caller", "ms"),
        ("two", "", "batch", "combined"),
        ("three", "", "batch", "combined"),
        ("scalar batch argument", "", default_origin, "ms"),
    ]
    if implementation == "native":
        logs = [entry for entry in caplog.records if entry.name == "casars.tools.ms"]
        assert logs[0].getMessage() == "History table entries: 6"
        assert [entry.levelno for entry in logs] == [logging.INFO] * 7
        assert [entry.getMessage().split("\t", 1)[1] for entry in logs[1:]] == [
            f"::{origin}\t{message}" for message, _, origin, _ in expected
        ]
    assert selector.done()
    for api in (casa, tools):
        check_history(api, native_dataset, expected, start, end)


def test_ms_first_write_creates_and_links_missing_history(native_dataset):
    casa = pinned_casa()
    t = casa.table()
    path = str(native_dataset)
    assert t.open(path, nomodify=False)
    assert t.removekeyword("HISTORY")
    assert t.done()
    (native_dataset / "HISTORY").rename(native_dataset / "original-HISTORY")
    start = time.time()
    native = tools.ms()
    assert native.writehistory("first", msname=path)
    assert t.open(path)
    assert t.getkeywords()["HISTORY"].endswith("/HISTORY")
    assert t.done()
    reference = casa.ms()
    assert reference.open(path)
    assert reference.writehistory("second")
    assert reference.done()
    end = time.time()
    expected = [
        (message, "", "MSHistoryHandler::addMessage()", "ms") for message in ("first", "second")
    ]
    for api in (casa, tools):
        check_history(api, native_dataset, expected, start, end)


@pytest.mark.parametrize(("reference", "offset"), [("UTC", 0), ("TAI", 37)])
def test_ms_write_history_converts_clock_with_retained_write_lock(native_dataset, reference, offset):
    casa = pinned_casa()
    t = casa.table()
    assert t.open(str(native_dataset / "HISTORY"), nomodify=False)
    measure = t.getcolkeyword("TIME", "MEASINFO")
    measure["Ref"] = reference
    assert t.putcolkeyword("TIME", "MEASINFO", measure)
    assert t.done()

    start = time.time()
    messages = []
    for api, writer in ((casa, "reference"), (tools, "native")):
        ms = api.ms()
        path = str(native_dataset)
        assert ms.open(path, nomodify=False, lock=True)
        assert ms.writehistory(f"{writer} attachment")
        assert ms.writehistory_batch([f"{writer} explicit"], msname=path)
        messages.extend((f"{writer} attachment", f"{writer} explicit"))
        assert ms.listhistory()
        assert ms.done()
    end = time.time()
    expected = [
        (message, "", "MSHistoryHandler::addMessage()", "ms")
        for message in messages
    ]
    for api in (casa, tools):
        check_history(api, native_dataset, expected, start, end, epoch_offset=offset)
