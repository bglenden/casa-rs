"""CASA's unimplemented format getter and setter retain their placeholders."""
import json
import logging
from pathlib import Path

import pytest

from casars.tools import quanta


def test_format_placeholders_match_casa(caplog: pytest.LogCaptureFixture) -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_format_placeholders.json").read_text())
    qa = quanta()
    with caplog.at_level(logging.WARNING, logger="casars.tools.quanta"):
        for case in fixture["cases"]:
            caplog.clear()
            args = [bytes(value["bytes"]) if isinstance(value, dict) and "bytes" in value else value for value in case["args"]]
            method = getattr(qa, case["method"])
            if "error" in case:
                with pytest.raises(TypeError) as error:
                    method(*args)
                assert str(error.value) == case["error"]["message"], case
                assert not caplog.records
            else:
                result = method(*args)
                assert result == case["result"] and type(result) is type(case["result"])
                assert [record.getMessage() for record in caplog.records] == [f'{case["method"]} NOT IMPLEMENTED YET!']
