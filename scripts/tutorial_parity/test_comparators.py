from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tutorial_parity.comparators.imhead import normalize_casa
from tutorial_parity.comparators.imstat import normalize_casa as normalize_imstat
from tutorial_parity.comparators.plot_products import _casa_points, _compare_points, _native_points


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


if __name__ == "__main__":
    unittest.main()
