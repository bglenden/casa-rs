"""CASA 6.7.6.14's unambiguous required-path refusals (#1099)."""

import json
from pathlib import Path

import pytest

from casars import tasks


CASES = json.loads((Path(__file__).parent / "data" / "casa_task_required_paths.json").read_text())["cases"]
IMAGE_CASES = [case for case in CASES if case["task"] in {"imstat", "imhead"}]


@pytest.mark.parametrize("case", IMAGE_CASES)
def test_missing_image_path_matches_casa(case: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    values = {key: str(tmp_path / "missing") if value == "$MISSING" else value
              for key, value in case["args"].items()}
    with pytest.raises(ValueError) as error:
        getattr(tasks, case["task"])(**values)
    assert str(error.value) == case["message"].replace("$TMP", str(tmp_path))
