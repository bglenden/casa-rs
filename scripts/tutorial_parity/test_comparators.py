from __future__ import annotations

import importlib.util
import struct
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from tutorial_parity.comparators.imhead import normalize_casa
from tutorial_parity.comparators.imstat import normalize_casa as normalize_imstat
from tutorial_parity.comparators.plot_products import _casa_points, _compare_points, _native_points
from tutorial_parity.workers import fits_compare


def _load_casa_compare_worker():
    worker = Path(__file__).parent / "workers" / "casa_compare.py"
    spec = importlib.util.spec_from_file_location("tutorial_parity_test_casa_compare", worker)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load comparator worker: {worker}")
    module = importlib.util.module_from_spec(spec)
    fake_casatools = types.ModuleType("casatools")
    fake_casatools.image = object
    fake_casatools.table = object
    with patch.dict(sys.modules, {"casatools": fake_casatools}):
        spec.loader.exec_module(module)
    return module


def _write_fits(path: Path, values: list[float], *, bunit: str = "Jy/beam") -> None:
    cards = [
        "SIMPLE  =                    T",
        "BITPIX  =                  -32",
        "NAXIS   =                    1",
        f"NAXIS1  = {len(values):20d}",
        f"BUNIT   = '{bunit}'",
        "END",
    ]
    header = "".join(card.ljust(80) for card in cards).encode("ascii")
    header += b" " * ((-len(header)) % 2880)
    data = struct.pack(f">{len(values)}f", *values)
    data += b"\0" * ((-len(data)) % 2880)
    path.write_bytes(header + data)


class TaskComparatorCharacterizationTests(unittest.TestCase):
    def test_imhead_characterizes_beam_axes_and_masks(self) -> None:
        normalized = normalize_casa({
            "axisnames": ["Right Ascension", "Frequency"],
            "axisunits": ["rad", "Hz"],
            "shape": [250, 1],
            "refpix": [124.0, 0.0],
            "refval": [1.2, 372000000000.0],
            "incr": [-4.8e-7, 62500000.0],
            "unit": "Jy/beam",
            "defaultmask": "mask0",
            "masks": ["mask0"],
            "restoringbeam": {"major": {"value": 1.0}, "minor": {"value": 0.8}, "positionangle": {"value": 45.0}},
        })
        self.assertEqual(normalized["shape"], [250, 1])
        self.assertEqual(normalized["restoring_beam"]["minor_arcsec"], 0.8)
        self.assertEqual(normalized["axes"][1]["unit"], "Hz")

    def test_imstat_characterizes_scalar_and_position_fields(self) -> None:
        normalized = normalize_imstat(
            {"max": [0.619], "rms": [0.017], "flux": [1.25], "maxpos": [125, 126, 0, 0]},
            ["max", "rms", "flux"],
            ["maxpos"],
        )
        self.assertEqual(normalized, {"max": 0.619, "rms": 0.017, "flux": 1.25, "maxpos": [125, 126, 0, 0]})

    def test_plot_products_characterize_uv_and_iterated_panel_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            native = root / "native.txt"
            casa = root / "casa.txt"
            native.write_text(
                "# msexplore-manifest-v1\npanel_key\tpanel_label\tseries_key\tseries_label\tx\ty\nfield-0\tTW Hya\tfield-0\tTW Hya\t20.66267\t8.39639\n",
                encoding="utf-8",
            )
            casa.write_text("# x y chan\n20.6627 8.3964 0\n", encoding="utf-8")
            native_points = _native_points(native, panel_label="TW Hya")
            casa_points = _casa_points(casa)
            self.assertEqual(len(native_points), 1)
            self.assertEqual(_compare_points(native_points, casa_points, atol=0.0011, rtol=0.0005)["status"], "passed")

    def test_image_products_characterize_pixels_masks_and_metadata(self) -> None:
        worker = _load_casa_compare_worker()
        records = [
            {
                "values": np.asarray([1.0, 2.0, 3.0]),
                "mask": np.asarray([True, True, False]),
                "summary": {"unit": "Jy/beam"},
            },
            {
                "values": np.asarray([1.0, 2.0005, 99.0]),
                "mask": np.asarray([True, True, True]),
                "summary": {"unit": "Jy/beam"},
            },
        ]
        with patch.object(worker, "image_record", side_effect=records):
            result = worker.compare_image_pair(
                "native.image",
                "oracle.image",
                {"absolute_tolerance": 0.001, "relative_tolerance": 0.0, "metadata_fields": ["unit"]},
            )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["shared_finite_pixels"], 2)
        self.assertEqual(result["mask_mismatch_pixels"], 1)

    def test_measurement_set_characterizes_rows_fields_data_descriptions_and_scans(self) -> None:
        worker = _load_casa_compare_worker()
        summary = {
            "row_count": 10,
            "field_ids": [0, 1],
            "field_names": ["phase", "target"],
            "data_description_ids": [0],
            "scan_numbers": [1, 2],
        }
        with patch.object(worker, "ms_summary", side_effect=[summary, dict(summary)]):
            result = worker.compare_ms(
                "native.ms",
                "oracle.ms",
                {"fields": list(summary)},
            )
        self.assertEqual(result["status"], "passed")
        self.assertTrue(all(field["matched"] for field in result["fields"].values()))

    def test_fits_products_characterize_primary_pixels_shape_and_headers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            native = root / "native.fits"
            oracle = root / "oracle.fits"
            _write_fits(native, [1.0, 2.0, 3.0])
            _write_fits(oracle, [1.0, 2.0005, 3.0])
            result = fits_compare.compare_pair(
                native,
                oracle,
                {"absolute_tolerance": 0.001, "relative_tolerance": 0.0, "header_fields": ["BUNIT"]},
            )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["native_shape"], [3])
        self.assertTrue(result["headers"]["BUNIT"]["matched"])


if __name__ == "__main__":
    unittest.main()
