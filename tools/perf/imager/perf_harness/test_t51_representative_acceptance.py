# SPDX-License-Identifier: LGPL-3.0-or-later

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from .t51_pair_guard import run_pair_pipeline
from .t51_representative_acceptance import comparison_request
from t51_aw_vlass_acceptance import DIRTY_WORKLOAD, CLEAN_WORKLOAD


class RepresentativeAcceptanceTests(unittest.TestCase):
    def test_complete_original_comparison_contract_is_preserved(self):
        for role, path, count in (("dirty", DIRTY_WORKLOAD, 18), ("clean", CLEAN_WORKLOAD, 19)):
            workload = json.loads(path.read_text())
            request = comparison_request(workload, Path("/tmp/t51-test"), role)
            self.assertEqual(len(request["products"]), count)
            for field in ("products", "tolerances", "source_regions", "metadata_contract"):
                self.assertEqual(request.get(field), workload["comparison"].get(field))
            self.assertEqual(request["mode"], "full")
            self.assertTrue(request["require_exact_product_inventory"])
            self.assertTrue(request["require_metadata_parity"])
            self.assertTrue(request["require_direction_wcs_parity"])

    def test_unknown_comparison_fields_fail_closed(self):
        workload = json.loads(DIRTY_WORKLOAD.read_text())
        workload["comparison"]["ignore_failure"] = True
        with self.assertRaisesRegex(ValueError, "unknown field"):
            comparison_request(workload, Path("/tmp/t51-test"), "dirty")

    def test_invalid_deadline_cannot_launch(self):
        with patch("subprocess.Popen") as launch:
            for value in (0, -1, True, 1.5, "1800"):
                with self.assertRaises(ValueError):
                    run_pair_pipeline([], cwd=".", environment={}, log=None, wall_seconds=value)
            launch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
