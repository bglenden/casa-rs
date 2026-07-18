from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from tutorial_parity.runner import manifests
from tutorial_parity.schema import ContractError, parse_section


class SectionSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        path = Path(__file__).parent / "sections" / "image-analysis-01-imhead.json"
        self.value = json.loads(path.read_text(encoding="utf-8"))

    def test_all_first_look_sections_are_declared(self) -> None:
        loaded = manifests()
        self.assertEqual(len(loaded), 12)
        self.assertEqual(len({(item.pack_id, item.section_id) for item in loaded}), 12)

    def test_characteristic_sections_route_to_checked_comparators(self) -> None:
        actual = {
            item.section_id: (
                item.comparison.plugin,
                tuple(item.comparison.config.get("products", [])),
            )
            for item in manifests()
        }
        self.assertEqual(
            {
                "01-imhead-continuum-header": ("imhead", ()),
                "02-imstat-continuum-statistics": ("imstat", ()),
                "03-immoments-n2hp-moment-map": ("image_products", ("moment0",)),
                "04-exportfits-products": ("fits_products", ("continuum", "n2hp")),
                "01-listobs-calibrated-ms": ("json_fields", ()),
                "02-uv-coverage": ("plot_products", ()),
                "03-amplitude-uvdist-by-field": ("plot_products", ()),
                "04-phase-cal-dirty": ("image_products", ("image", "model", "pb", "psf", "residual", "sumwt")),
                "05-phase-cal-clean": ("image_products", ("image", "model", "pb", "psf", "residual", "sumwt")),
                "06-science-target-split": ("measurement_set", ()),
                "07-science-target-auto-clean": ("image_products", ("image", "model", "pb", "psf", "residual", "sumwt")),
                "08-primary-beam-correction": ("image_products", ("pbcor",)),
            },
            actual,
        )

    def test_unknown_task_is_rejected(self) -> None:
        value = copy.deepcopy(self.value)
        value["surfaces"]["cli"]["operations"][0]["task"] = "python-code"
        with self.assertRaisesRegex(ContractError, "expected one of"):
            parse_section(value)

    def test_absolute_product_path_is_rejected(self) -> None:
        value = copy.deepcopy(self.value)
        value["surfaces"]["cli"]["operations"][0]["outputs"] = ["/tmp/result.json"]
        with self.assertRaisesRegex(ContractError, "relative"):
            parse_section(value)

    def test_every_surface_is_required(self) -> None:
        value = copy.deepcopy(self.value)
        del value["surfaces"]["gui"]
        with self.assertRaisesRegex(ContractError, "must define exactly"):
            parse_section(value)


if __name__ == "__main__":
    unittest.main()
