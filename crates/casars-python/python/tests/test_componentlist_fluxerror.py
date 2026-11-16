"""CASA's component flux-error getter, including a CASA-created table."""

import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist


def test_fluxerror_matches_casa(tmp_path: Path) -> None:
    data = Path(__file__).parent / "data"
    fixture = json.loads((data / "casa_componentlist_fluxerror.json").read_text())
    with tarfile.open(data / "casa_fluxerrors.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    cl = componentlist()
    assert cl.open(str(tmp_path / "fluxerrors.cl"), nomodify=True)
    for case in fixture["cases"]:
        if "error" in case:
            expected = case["error"]
            error_type = {"RuntimeError": RuntimeError, "TypeError": TypeError}[expected["type"]]
            with pytest.raises(error_type) as error:
                cl.getfluxerror(case["which"])
            assert str(error.value) == expected["message"]
        else:
            np.testing.assert_array_equal(cl.getfluxerror(case["which"]), case["result"])
    cl.done()
