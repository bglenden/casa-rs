"""CASA 6.7.6.14's direct vector type refusals (#1099)."""

import json
from pathlib import Path

import pytest

from casars import tasks


CASES = json.loads((Path(__file__).parent / "data" / "casa_task_vector_refusals.json").read_text())["cases"]


@pytest.mark.parametrize("case", CASES)
def test_direct_vector_refusal_matches_casa(case: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    values = {name: str(tmp_path) if value == "$EXISTING" else value for name, value in case["args"].items()}
    # Applycal now follows the CASA wrapper's schema exception as well as
    # the message. Other task wrappers retain their native TypeError.
    expected = AssertionError if case["task"] == "applycal" else TypeError
    with pytest.raises(expected) as error:
        getattr(tasks, case["task"])(**values)
    assert str(error.value) == case["message"]
