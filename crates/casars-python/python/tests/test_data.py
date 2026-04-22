from __future__ import annotations

import json
from pathlib import Path
import subprocess

import numpy as np

from casars.data import Image, Table, protocol_info, schema_bundle


def test_top_level_lazy_data_surface_imports() -> None:
    import casars

    assert hasattr(casars, "Image")
    assert hasattr(casars, "Table")
    assert casars.data is not None
    assert casars.Image is casars.data.Image
    assert casars.Table is casars.data.Table


def test_data_schema_bundle_reports_object_surface() -> None:
    info = protocol_info()
    assert info.protocol_name == "casars_data_objects"
    assert info.protocol_version == 1
    assert info.surface_kind == "object"

    bundle = schema_bundle()
    assert bundle["protocol"]["protocol_name"] == "casars_data_objects"
    assert bundle["protocol"]["protocol_version"] == 1
    assert bundle["protocol"]["surface_kind"] == "object"
    assert bundle["projections"]["python"]["module"] == "casars.data"
    assert "LogicalInputValue" in bundle["components"]
    assert "LogicalOutputValue" in bundle["components"]
    assert "LogicalComplex128" in bundle["components"]

    objects = {entry["name"]: entry for entry in bundle["semantic"]["objects"]}
    assert set(objects) == {"Image", "Table"}
    assert {prop["name"] for prop in objects["Image"]["properties"]} == {
        "shape",
        "pixel_type",
        "units",
        "image_info",
        "misc_info",
        "mask_names",
        "default_mask_name",
    }
    assert {method["name"] for method in objects["Image"]["methods"]} == {
        "get_slice",
        "put_slice",
        "get_plane",
        "get_mask_slice",
    }
    assert {method["name"] for method in objects["Table"]["methods"]} == {
        "column_keywords",
        "get_cell",
        "set_cell",
        "get_column",
        "put_column",
        "set_column_keywords",
    }

    image_methods = {entry["name"]: entry for entry in objects["Image"]["methods"]}
    mask_result = image_methods["get_mask_slice"]["result_schema"]
    assert "null" in json.dumps(mask_result)


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_image_reads_and_writes(tmp_path: Path) -> None:
    fixture_root = _create_fixtures(tmp_path)
    image = Image.open(fixture_root / "python_fixture.image", writable=True)

    assert image.shape == (4, 3, 2)
    assert image.pixel_type == "float32"
    assert image.units == "Jy/beam"
    assert image.mask_names == ["quality"]
    assert image.default_mask_name == "quality"
    assert image.image_info["objectname"] == "python-fixture"
    assert image.misc_info["purpose"] == "python-tests"

    chunk = image.get_slice((1, 1, 0), (2, 2, 2))
    assert isinstance(chunk, np.ndarray)
    assert chunk.shape == (2, 2, 2)
    assert chunk.dtype == np.float32
    np.testing.assert_array_equal(chunk[:, :, 0], np.array([[8.0, 10.0], [14.0, 16.0]], dtype=np.float32))

    plane = image.get_plane(2, 1)
    assert plane.shape == (4, 3)
    np.testing.assert_array_equal(
        plane,
        np.array(
            [
                [1.0, 3.0, 5.0],
                [7.0, 9.0, 11.0],
                [13.0, 15.0, 17.0],
                [19.0, 21.0, 23.0],
            ],
            dtype=np.float32,
        ),
    )

    mask = image.get_mask_slice((0, 0, 0), (2, 2, 2))
    assert mask is not None
    assert mask.dtype == np.bool_
    np.testing.assert_array_equal(mask[:, :, 0], np.array([[True, True], [True, True]]))

    replacement = np.full((2, 2, 1), 99.0, dtype=np.float32)
    image.put_slice(replacement, (0, 0, 0))
    reread = image.get_slice((0, 0, 0), (2, 2, 1))
    np.testing.assert_array_equal(reread, replacement)


def test_table_reads_and_writes(tmp_path: Path) -> None:
    fixture_root = _create_fixtures(tmp_path)
    table = Table.open(fixture_root / "python_fixture.table", writable=True)

    assert table.row_count == 3
    assert table.column_names == ["id", "label", "gain", "spectrum", "vary", "meta"]
    assert table.keywords["observer"] == "python-fixture"
    assert table.keywords["version"] == 1
    assert table.column_keywords("spectrum") == {"unit": "Jy", "frame": "LSRK"}

    assert table.get_cell(0, "label") == "alpha"
    gain = table.get_cell(1, "gain")
    assert gain == complex(2.0, -2.0)
    assert table.get_cell(2, "meta") == {"label": "gamma", "weight": 30}

    ids = table.get_column("id")
    assert isinstance(ids, np.ndarray)
    np.testing.assert_array_equal(ids, np.array([1, 2, 3], dtype=np.int64))

    gains = table.get_column("gain")
    assert isinstance(gains, np.ndarray)
    np.testing.assert_array_equal(
        gains,
        np.array([1.0 - 1.0j, 2.0 - 2.0j, 3.0 - 3.0j], dtype=np.complex128),
    )

    spectra = table.get_column("spectrum")
    assert isinstance(spectra, np.ndarray)
    assert spectra.shape == (3, 2, 2)
    np.testing.assert_array_equal(
        spectra[0],
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
    )

    varying = table.get_column("vary")
    assert isinstance(varying, list)
    assert [item.shape for item in varying] == [(2,), (3,), (1,)]

    written = table.put_column("label", ["delta", "epsilon", "zeta"])
    assert written == 3
    assert table.get_column("label") == ["delta", "epsilon", "zeta"]

    written = table.put_column("meta", [{"label": "delta", "weight": 100}], start=0)
    assert written == 1
    assert table.get_cell(0, "meta") == {"label": "delta", "weight": 100}

    table.set_column_keywords("gain", {"unit": "arb", "stage": "python"})
    assert table.column_keywords("gain") == {"unit": "arb", "stage": "python"}


def _create_fixtures(tmp_path: Path) -> Path:
    fixture_root = tmp_path / "fixtures"
    subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "casars-python",
            "--example",
            "make_python_fixtures",
            "--",
            str(fixture_root),
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    return fixture_root
