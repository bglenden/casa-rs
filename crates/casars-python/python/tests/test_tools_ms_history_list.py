"""Read CASA-written HISTORY through the public MS tool."""

import logging

import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("populated", [False, True])
def test_ms_listhistory_reads_casa_history_and_preserves_attachment(native_dataset, caplog,
                                                                 populated):
    casa = pinned_casa()
    reference = casa.ms()
    actual = tools.ms()
    for tool in (reference, actual):
        assert tool.listhistory() is False
        assert tool.open(str(native_dataset), nomodify=True)
    assert actual.listhistory()
    if populated:
        writer = casa.ms()
        assert writer.open(str(native_dataset), nomodify=False)
        for message in ("first history entry", "second history entry"):
            assert writer.writehistory(message, origin="independent-casa-writer")
        assert writer.done()

    # CASA shares its table cache with the writer in this process. Its open
    # read handle can already report writable by the time the listing starts.
    assert actual.iswritable() is False
    before = [tool.iswritable() for tool in (reference, actual)]
    assert reference.listhistory() is True
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="casars.tools.ms"):
        assert actual.listhistory() is True
    entries = [record.getMessage() for record in caplog.records
               if record.name == "casars.tools.ms"]
    if populated:
        assert entries[0] == "History table entries: 2"
        assert entries[1].endswith("\t::independent-casa-writer\tfirst history entry")
        assert entries[2].endswith("\t::independent-casa-writer\tsecond history entry")
    else:
        assert entries == ["The HISTORY table is empty"]
    if populated:
        writer = casa.table()
        assert writer.open(str(native_dataset / "HISTORY"), nomodify=False)
        assert writer.putcell("MESSAGE", 0, "updated without changing the row count")
        assert writer.done()
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="casars.tools.ms"):
            assert actual.listhistory()
        assert "updated without changing the row count" in caplog.text
    for tool, writable in zip((reference, actual), before):
        assert tool.name() == str(native_dataset)
        assert tool.iswritable() is writable
        assert tool.done()
