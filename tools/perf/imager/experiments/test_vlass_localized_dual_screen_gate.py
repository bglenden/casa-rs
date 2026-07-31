import json
import pathlib
import tempfile
import unittest

import numpy as np

import vlass_localized_dual_screen_gate as gate


class VlassLocalizedDualScreenGateTests(unittest.TestCase):
    def test_facet_embeds_scale_dilated_mask_with_guard(self) -> None:
        bounds = gate.facet_bounds(
            (575, 2125), (638, 2188), largest_scale=12, facet_side=128
        )
        self.assertEqual(bounds["mask_shape"], [64, 64])
        self.assertEqual(bounds["scale_dilated_shape"], [88, 88])
        self.assertEqual(bounds["guard_pixels"], [20, 20])
        self.assertEqual(bounds["start_pixel"], [543, 2093])
        self.assertEqual(bounds["end_pixel"], [670, 2220])

    def test_bilinear_sampling_matches_planar_complex_screen(self) -> None:
        y, x = np.mgrid[:6, :7]
        screen = (2.0 * x + 3.0 * y + 1j * (x - y))[None, :, :].astype(
            np.complex64
        )
        sampled = gate.bilinear_sample(
            screen,
            np.array([1.25, 2.5, 4.75]),
            np.array([0.5, 3.25]),
        )
        yy, xx = np.meshgrid(
            np.array([0.5, 3.25]),
            np.array([1.25, 2.5, 4.75]),
            indexing="ij",
        )
        expected = 2.0 * xx + 3.0 * yy + 1j * (xx - yy)
        np.testing.assert_allclose(sampled[0], expected, rtol=0.0, atol=1.0e-6)

    def test_w_rank_uses_first_omitted_taylor_term(self) -> None:
        self.assertEqual(gate.required_taylor_terms(0.02, 2.0e-5), 3)
        self.assertEqual(gate.required_taylor_terms(0.001, 2.0e-5), 2)

    def test_reducer_requires_explicit_reverse_screen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = {
                "schema": gate.SOURCE_SCHEMA,
                "crop_shape": [4, 4],
                "states": [{"index": 0}],
                "forward_path": str(root / "forward.c64"),
                "normal_path": str(root / "normal.c64"),
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            np.ones((1, 4, 4), dtype=np.complex64).tofile(root / "forward.c64")
            np.ones((1, 4, 4), dtype=np.complex64).tofile(root / "normal.c64")
            with self.assertRaisesRegex(gate.LocalizedGateError, "reverse_path"):
                gate.reduce_gate(
                    manifest_path,
                    mask_min=(0, 0),
                    mask_max=(0, 0),
                    image_side=1,
                    image_reference_pixel=0.0,
                    cell_arcsec=1.0,
                    largest_scale=0,
                    facet_side=1,
                    max_screen_rank=1,
                    maximum_w_lambda=1.0,
                )


if __name__ == "__main__":
    unittest.main()
