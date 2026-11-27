"""Python argument and attachment contract for the ASDM path accessor."""

import pytest

from test_tools_ms_putdata import writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_asdmref_for_detached_and_materialized_measurementsets(native_dataset, writer_api):
    reader = writer_api.ms()
    for attached in (False, True):
        if attached:
            assert reader.open(str(native_dataset))
        assert reader.asdmref() == ""
        assert reader.asdmref(abspath="/") == ""
        assert reader.asdmref(b"unused") == ""
        assert reader.asdmref(b"\xff") == ""
        with pytest.raises(TypeError, match="argument _abspath must be a string"):
            reader.asdmref(None)
    assert reader.done()
