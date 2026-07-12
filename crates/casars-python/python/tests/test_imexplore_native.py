from __future__ import annotations

import numpy as np
from pathlib import Path
import os
import pytest

from casars import imexplore


class _FakeImage:
    shape = (2, 3, 2)
    coordinate_system = {
        "coordinate0": {
            "coordinate_type": "Direction",
            "direction_ref": "J2000",
            "projection": "SIN",
            "crval": np.array([1.0, 0.5]),
            "cdelt": np.array([-0.001, 0.001]),
            "crpix": np.array([1.0, 1.0]),
        }
    }
    image_info = {"restoringbeam": {"major": {"value": 1.2, "unit": "arcsec"}}}
    units = "Jy/beam"

    @classmethod
    def open(cls, path: str) -> "_FakeImage":
        assert path == "tutorial.image"
        return cls()

    def get_plane(self, axis: int, index: int) -> np.ndarray:
        assert (axis, index) == (2, 1)
        return np.arange(6, dtype=np.float32).reshape(2, 3)

    def get_mask_slice(self, start: list[int], shape: list[int]) -> np.ndarray:
        assert start == [0, 0, 1]
        assert shape == [2, 3, 1]
        return np.ones((2, 3, 1), dtype=np.bool_)


def test_image_plane_data_includes_wcs_mask_beam_and_overlays(monkeypatch) -> None:
    monkeypatch.setattr(imexplore, "Image", _FakeImage)
    overlay = {"kind": "point", "x": 1.0, "y": 0.5, "label": "peak"}

    result = imexplore.data(
        "tutorial.image",
        axis=2,
        index=1,
        overlays=(overlay,),
        stretch="asinh",
    )

    assert result.values.shape == (2, 3)
    assert result.mask is not None and result.mask.all()
    assert result.coordinate_system["coordinate0"]["projection"] == "SIN"
    assert "restoringbeam" in result.beam_metadata
    assert result.overlays == (overlay,)
    assert result.units == "Jy/beam"
    assert result.stretch == "asinh"


def test_tutorial_image_returns_native_plane_and_wcs_metadata() -> None:
    root = Path(os.environ.get("CASA_RS_TUTORIAL_DATA_ROOT", "~/SoftwareProjects/casa-tutorial-data")).expanduser()
    image = root / "tutorial-parity/alma/first-look/twhya/twhya_cont.image"
    if not image.is_dir():
        pytest.skip("local ALMA first-look tutorial image is unavailable")

    result = imexplore.data(image, axis=2, index=0)

    assert result.values.ndim == 2
    assert result.values.size > 0
    assert result.coordinate_system
    assert result.units
    pytest.importorskip("matplotlib")
    pytest.importorskip("astropy")
    figure, axes = imexplore.imshow(result)
    assert figure is not None
    assert axes.images
