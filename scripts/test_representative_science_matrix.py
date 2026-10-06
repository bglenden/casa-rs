"""The T52 validity amendment must not widen the default product contract."""

import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "representative_matrix", Path(__file__).with_name("check-representative-science-matrix.py")
)
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class NativeAwValidityTests(unittest.TestCase):
    def setUp(self):
        self.comparison = {
            "exact": {"validity": False},
            "other_product_validity_exact": True,
            "mask_mismatches": {
                product: {
                    "count": 2, "total": 512 * 512,
                    "samples": [{"location": [333, 384, 0, 0]}, {"location": [459, 275, 0, 0]}],
                }
                for product in (".alpha", ".alpha.error")
            },
        }

    def test_exactly_reported_approved_pixels_pass(self):
        self.assertTrue(matrix.native_aw_validity_is_approved(self.comparison))

    def test_unapproved_product_or_wrong_total_fails(self):
        for field, value in (("count", 3), ("count", True), ("count", -1), ("total", 1024 * 1024)):
            candidate = copy.deepcopy(self.comparison)
            candidate["mask_mismatches"][".alpha"][field] = value
            self.assertFalse(matrix.native_aw_validity_is_approved(candidate))
        self.comparison["mask_mismatches"][".pb.tt0"] = self.comparison["mask_mismatches"][".alpha"]
        self.assertFalse(matrix.native_aw_validity_is_approved(self.comparison))

    def test_missing_duplicate_or_invalid_pixel_report_fails(self):
        for locations in ([], [[1, 1, 0, 0]] * 2, [[-1, 0, 0, 0], [2, 2, 0, 0]]):
            candidate = copy.deepcopy(self.comparison)
            candidate["mask_mismatches"][".alpha"]["samples"] = [{"location": p} for p in locations]
            self.assertFalse(matrix.native_aw_validity_is_approved(candidate))

    def test_cannot_claim_exact_validity_or_relax_other_products(self):
        self.comparison["exact"]["validity"] = True
        self.assertFalse(matrix.native_aw_validity_is_approved(self.comparison))
        self.comparison["exact"]["validity"] = False
        self.comparison["other_product_validity_exact"] = False
        self.assertFalse(matrix.native_aw_validity_is_approved(self.comparison))


if __name__ == "__main__":
    unittest.main()
