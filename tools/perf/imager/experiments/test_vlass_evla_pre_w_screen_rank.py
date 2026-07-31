import json
import pathlib
import tempfile
import unittest

import numpy as np

import vlass_evla_pre_w_screen_rank as rank


class VlassEvlaPreWScreenRankTests(unittest.TestCase):
    def test_exact_rank_two_family_passes_at_two(self) -> None:
        y, x = np.mgrid[:9, :11]
        basis = np.stack(
            [
                np.exp(1j * x / 9.0),
                (1.0 + y / 10.0) * np.exp(-1j * y / 7.0),
            ]
        ).astype(np.complex64)
        coefficients = np.array(
            [[1.0, 0.0], [0.0, 1.0], [1.5, -0.25], [0.3j, 0.7]],
            dtype=np.complex64,
        )
        family = np.einsum("sr,ryx->syx", coefficients, basis)
        metrics = rank._rank_metrics(
            family, np.ones((9, 11), dtype=bool), max_rank=4
        )
        self.assertGreater(metrics["ranks"][0]["relative_rms"], 1.0e-3)
        self.assertLess(metrics["ranks"][1]["relative_rms"], 1.0e-7)
        classified = rank.classify_family(metrics, "forward")
        self.assertEqual(classified["minimum_passing_rank"], 2)
        self.assertTrue(classified["preferred_gate_passed"])

    def test_forward_domain_uses_output_footprint_and_scale(self) -> None:
        manifest = {
            "derived_sky_increment_rad": [1.0e-3, 1.0e-3],
            "uv_reference_pixel": [8.0, 8.0],
            "crop_start": [4, 4],
            "crop_shape": [8, 8],
        }
        mask = rank.forward_domain_mask(
            manifest, image_side=4, cell_arcsec=206.265, scale_pixels=0
        )
        self.assertEqual(int(mask.sum()), 25)
        self.assertTrue(mask[4, 4])

    def test_reducer_rejects_product_energy_outside_crop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            forward = np.ones((2, 4, 4), dtype=np.complex64)
            normal = np.ones((2, 4, 4), dtype=np.complex64)
            forward_path = root / "forward.c64"
            normal_path = root / "normal.c64"
            forward.tofile(forward_path)
            normal.tofile(normal_path)
            manifest = {
                "schema": rank.SOURCE_SCHEMA,
                "full_shape": [8, 8],
                "crop_start": [2, 2],
                "crop_shape": [4, 4],
                "derived_sky_increment_rad": [1.0e-3, 1.0e-3],
                "uv_reference_pixel": [4.0, 4.0],
                "forward_path": str(forward_path),
                "normal_path": str(normal_path),
                "states": [
                    {
                        "normal_outside_crop_peak": 1.0e-3,
                    },
                    {
                        "normal_outside_crop_peak": 0.0,
                    },
                ],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(rank.RankError, "excludes values"):
                rank.reduce_screens(
                    manifest_path,
                    image_side=2,
                    cell_arcsec=206.264806247,
                    scale_pixels=0,
                    pb_limit=1.0e-4,
                    max_rank=2,
                )

    def test_product_domain_is_union_across_states(self) -> None:
        family = np.zeros((2, 3, 4), dtype=np.complex64)
        family[0, 0, 0] = 2.0e-4
        family[1, 2, 3] = 1.0e-4
        mask = rank.product_domain_mask(family, 1.0e-4)
        self.assertEqual(int(mask.sum()), 2)
        self.assertTrue(mask[0, 0])
        self.assertTrue(mask[2, 3])


if __name__ == "__main__":
    unittest.main()
