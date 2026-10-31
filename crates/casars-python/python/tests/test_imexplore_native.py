from __future__ import annotations

import numpy as np
from pathlib import Path
import os
import subprocess
import pytest

from casars import data as casars_data
from casars import imexplore

REPO_ROOT = Path(__file__).resolve().parents[4]


class _FakeImage:
    shape = (2, 3, 2)
    # As `CoordinateSystem::to_record` writes a direction over axes 0 and 1.
    coordinate_system = {
        "direction0": {
            "system": "J2000",
            "projection": "SIN",
            "crval": np.array([1.0, 0.5]),
            "cdelt": np.array([-0.001, 0.001]),
            "crpix": np.array([1.0, 1.0]),
            "units": ["rad", "rad"],
        },
        "pixelmap0": np.array([0, 1], dtype=np.int32),
        "worldmap0": np.array([0, 1], dtype=np.int32),
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

    def _direction_wcs(self) -> tuple[int, int, dict[str, object]]:
        return 0, 1, {"NAXIS": 3, "CTYPE1": "RA---SIN", "CTYPE2": "DEC--SIN"}


@pytest.fixture(scope="module")
def fixtures(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The images `make_python_fixtures` writes."""
    root = tmp_path_factory.mktemp("fixtures")
    subprocess.run(
        ["cargo", "run", "-q", "-p", "casars-python", "--example", "make_python_fixtures", "--",
         str(root)],
        cwd=REPO_ROOT,
        check=True,
    )
    return root


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
    assert result.coordinate_system["direction0"]["projection"] == "SIN"
    assert "restoringbeam" in result.beam_metadata
    assert result.overlays == (overlay,)
    assert result.units == "Jy/beam"
    assert result.stretch == "asinh"
    assert result.plane_axes == (0, 1)
    assert result.direction_axes == (0, 1)
    assert result.wcs_header is not None and result.wcs_header["CTYPE1"] == "RA---SIN"


def test_a_direction_plane_after_a_stokes_axis_finds_the_direction(fixtures: Path) -> None:
    # imexplore looked for a `coordinate<N>` record with a `coordinate_type`,
    # which casa-rs never writes, so no plane had a WCS. The fixture's
    # direction is on pixel axes 1 and 2, after Stokes.
    plane = imexplore.data(fixtures / "direction_fixture.image", axis=3, index=1)

    assert plane.plane_axes == (1, 2)
    assert plane.direction_axes == (1, 2)
    assert imexplore._is_direction_plane(plane)
    assert plane.values.shape == (6, 5)
    assert plane.values[3, 2] == 10 * 3 + 2 + 100
    header = plane.wcs_header
    assert header is not None
    assert (header["CTYPE2"], header["CTYPE3"]) == ("RA---SIN", "DEC--SIN")
    assert (header["CRPIX2"], header["CRPIX3"]) == (3.5, 2.5)
    assert header["PC2_3"] == pytest.approx(-np.sin(0.3))
    # exportfits's observation metadata is not part of the WCS.
    assert "DATE-OBS" not in header
    # Drawn as an array is: its first axis across.
    drawn = imexplore._display_values(plane)
    assert drawn.shape == (5, 6)
    assert drawn[2, 3] == plane.values[3, 2]


def test_a_plane_through_one_direction_axis_has_no_wcs(fixtures: Path) -> None:
    # Declination by frequency: one celestial axis has no celestial WCS.
    plane = imexplore.data(fixtures / "direction_fixture.image", axis=1, index=2)

    assert plane.plane_axes == (2, 3)
    assert not imexplore._is_direction_plane(plane)
    assert imexplore._astropy_wcs(plane) is None


def test_coordinates_that_miss_an_axis_give_no_direction_wcs(fixtures: Path) -> None:
    # A direction over two of three pixel axes cannot be written as FITS.
    image = casars_data.Image.open(fixtures / "short_coordinates.image")
    assert image._direction_wcs() is None

    plane = imexplore.data(fixtures / "short_coordinates.image", axis=2, index=0)
    assert plane.direction_axes is None and plane.wcs_header is None
    assert imexplore._astropy_wcs(plane) is None


def test_the_direction_plane_wcs_gives_casa_rs_world_coordinates(fixtures: Path) -> None:
    pytest.importorskip("astropy")
    plane = imexplore.data(fixtures / "direction_fixture.image", axis=3, index=1)
    wcs = imexplore._astropy_wcs(plane)

    assert wcs is not None and wcs.naxis == 2
    expected_at = casars_data.Image.open(fixtures / "direction_fixture.image").misc_info
    pixels = np.asarray(expected_at["expected_pixels"])
    world = np.asarray(expected_at["expected_world"])
    assert pixels.shape == world.shape == (5, 4)
    for pixel, expected in zip(pixels, world):
        # casa-rs's world coordinates at [s, x, y, c]; astropy takes the
        # plane's x and y, both 0-based.
        longitude, latitude = wcs.pixel_to_world_values(pixel[1], pixel[2])
        assert np.mod(longitude, 360.0) == pytest.approx(
            np.mod(np.degrees(expected[1]), 360.0), abs=1e-9
        ), pixel
        assert latitude == pytest.approx(np.degrees(expected[2]), abs=1e-9), pixel


def test_imshow_draws_the_plane_across_on_its_wcs(fixtures: Path) -> None:
    pytest.importorskip("matplotlib")
    pytest.importorskip("astropy")
    plane = imexplore.data(fixtures / "direction_fixture.image", axis=3, index=1)

    figure, axes = imexplore.imshow(plane)

    assert axes.images[0].get_array().shape == (5, 6)
    assert axes.wcs.wcs.ctype[0] == "RA---SIN"


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
