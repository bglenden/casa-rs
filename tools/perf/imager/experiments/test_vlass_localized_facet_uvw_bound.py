import math
import unittest

import numpy as np

import vlass_localized_facet_uvw_bound as bound


class VlassLocalizedFacetUvwBoundTests(unittest.TestCase):
    def test_phase_center_keeps_raw_w_bound(self) -> None:
        uvw = np.array([[3.0], [4.0], [12.0]])
        frequency = np.array([bound.SPEED_OF_LIGHT_M_S])
        result = bound.uvw_rotation_upper_bound(
            uvw, frequency, facet_offset=(0.0, 0.0)
        )
        self.assertEqual(result["raw_abs_w_lambda_max"], 12.0)
        self.assertEqual(result["rotated_abs_w_lambda_upper_bound"], 12.0)
        self.assertEqual(result["baseline_lambda_max"], 13.0)

    def test_offset_adds_transverse_rotation_bound(self) -> None:
        uvw = np.array([[3.0], [4.0], [12.0]])
        frequency = np.array([bound.SPEED_OF_LIGHT_M_S])
        theta = 0.1
        result = bound.uvw_rotation_upper_bound(
            uvw, frequency, facet_offset=(theta, 0.0)
        )
        self.assertAlmostEqual(
            result["rotated_abs_w_lambda_upper_bound"],
            12.0 + 5.0 * math.sin(theta),
        )

    def test_vlass_facet_offset_matches_frozen_geometry(self) -> None:
        offset = bound.facet_offset_rad(
            (606.5, 2156.5),
            image_reference_pixel=2048.0,
            cell_arcsec=0.6,
        )
        self.assertAlmostEqual(math.hypot(*offset), 0.004205014687292608)


if __name__ == "__main__":
    unittest.main()
