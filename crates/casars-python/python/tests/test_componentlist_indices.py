"""CASA's componentlist.indices placeholder."""
import json
import logging
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist

CASES = json.loads((Path(__file__).parent / "data" / "casa_componentlist_indices.json").read_text())["cases"]


@pytest.mark.parametrize("case", CASES)
def test_indices_matches_casa(case: dict, caplog: pytest.LogCaptureFixture) -> None:
    if case["select"]:
        pytest.skip("component selection remains unimplemented in casa-rs")
    cl = componentlist()
    for _ in range(case["count"]):
        assert cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        result = cl.indices()
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.dtype(case["dtype"])
    assert result.shape == tuple(case["shape"])
    assert result.tolist() == case["result"]
    assert caplog.messages == ["indices not implemented yet"]
