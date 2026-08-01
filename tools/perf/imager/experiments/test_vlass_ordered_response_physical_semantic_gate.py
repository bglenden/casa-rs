import importlib.util
import pathlib
import sys
import unittest

import numpy as np


SCRIPT = pathlib.Path(__file__).with_name(
    "vlass_ordered_response_physical_semantic_gate.py"
)
SPEC = importlib.util.spec_from_file_location("physical_semantic_gate", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE
SPEC.loader.exec_module(GATE)


class PhysicalSemanticGateTests(unittest.TestCase):
    def test_multiscale_atoms_are_normalized_and_bounded(self) -> None:
        atom = GATE.compact_multiscale_atom(
            (20, 30),
            5,
            np.float32(0.625),
        )

        self.assertAlmostEqual(
            sum(float(value) for value in atom.values()), 0.625, places=6
        )
        self.assertTrue(all(15 <= x <= 25 and 25 <= y <= 35 for x, y in atom))
        self.assertNotIn((15, 30), atom)
        self.assertNotIn((20, 25), atom)

    def test_active_support_matches_promoted_clean_domain(self) -> None:
        active = GATE.active_support_pixels()

        self.assertEqual(active.shape, (7_304, 2))
        np.testing.assert_array_equal(np.min(active, axis=0), [564, 2114])
        np.testing.assert_array_equal(np.max(active, axis=0), [649, 2199])
        self.assertEqual(len({tuple(pixel) for pixel in active}), 7_304)

    def test_complex_fixture_payload_is_flat_and_shape_bound(self) -> None:
        values = np.asarray(
            [
                [1.0 + 2.0j, -3.0 + 4.0j],
                [5.0 - 6.0j, -7.0 - 8.0j],
            ]
        )

        payload = GATE.complex_array_payload(values)

        self.assertEqual(payload["shape"], [2, 2])
        self.assertEqual(
            payload["values"],
            [[1.0, 2.0], [-3.0, 4.0], [5.0, -6.0], [-7.0, -8.0]],
        )

    def test_screen_coordinates_are_pointing_relative(self) -> None:
        manifest = {
            "uv_reference_pixel": [1024, 1024],
            "crop_start": [768, 768],
            "derived_sky_increment_rad": [40 * GATE.CELL_RAD, 40 * GATE.CELL_RAD],
        }
        pointing = np.asarray([1959.25, 2047.75])
        pixels = np.asarray(
            [
                pointing,
                pointing + np.asarray([40.0, -80.0]),
            ]
        )

        x, y = GATE.screen_coordinates(
            pixels,
            pointing_pixel=pointing,
            manifest=manifest,
        )

        np.testing.assert_allclose(x, [256.0, 257.0])
        np.testing.assert_allclose(y, [256.0, 254.0])

    def test_facet_rotation_preserves_phase_differences(self) -> None:
        pixels = np.asarray(
            [
                [563, 2113],
                [641, 2113],
                [650, 2200],
            ],
            dtype=np.int32,
        )
        uvw = np.asarray(
            [
                [-8196.2, -13477.0, 12544.9],
                [104409.9, -86371.3, -35215.4],
            ],
            dtype=np.float64,
        )
        global_l, global_m, global_eta = GATE.direct_pixel_coordinates(pixels)
        local_l, local_m, local_eta = GATE.facet_pixel_coordinates(pixels)
        local_uvw = GATE.rotate_uvw_to_facet(uvw)
        global_direction = np.column_stack([global_l, global_m, global_eta])
        local_direction = np.column_stack([local_l, local_m, local_eta])

        global_difference = uvw @ (global_direction[2] - global_direction[0])
        local_difference = local_uvw @ (local_direction[2] - local_direction[0])

        np.testing.assert_allclose(
            local_difference,
            global_difference,
            rtol=2.0e-12,
            atol=2.0e-12,
        )

    def test_total_order_two_tracks_exact_w_and_controls_are_sensitive(self) -> None:
        rows = 32
        source_pixels = np.asarray([[20, 20], [21, 20], [20, 21]], dtype=np.int32)
        output_pixels = np.asarray([[20, 20], [23, 24], [25, 19]], dtype=np.int32)
        source_l, source_m, source_eta = GATE.direct_pixel_coordinates(source_pixels)
        output_l, output_m, output_eta = GATE.direct_pixel_coordinates(output_pixels)
        u = np.linspace(-2000.0, 1900.0, rows)
        v = np.linspace(1700.0, -1800.0, rows)
        w = np.linspace(-800.0, 900.0, rows)
        weight = np.linspace(0.4, 1.6, rows)
        taylor = np.linspace(-0.3, 0.4, rows)
        wrong_taylor = 0.35 * taylor + 0.12
        model = np.asarray([1.0, 0.4, -0.2])
        prediction_screen = np.asarray([0.9 + 0.1j, 0.8 - 0.2j, 0.7 + 0.3j])
        wrong_prediction_screen = np.asarray([0.5 - 0.4j, 0.6 + 0.5j, 0.9 - 0.1j])
        left_screen = np.asarray([0.8 - 0.1j, 0.7 + 0.2j, 0.9 - 0.3j])
        prediction_inverse_normalization = np.linspace(0.4, 1.2, rows) + 1j * np.linspace(
            -0.2, 0.3, rows
        )

        exact, candidate, wrong_screen, wrong_taylor_result = GATE.evaluate_pair(
            u=u,
            v=v,
            w=w,
            weight=weight,
            taylor=taylor,
            wrong_taylor=wrong_taylor,
            source_l=source_l,
            source_m=source_m,
            source_eta=source_eta,
            output_l=output_l,
            output_m=output_m,
            output_eta=output_eta,
            model_values=model,
            model_term=1,
            prediction_screen=prediction_screen,
            wrong_prediction_screen=wrong_prediction_screen,
            prediction_inverse_normalization=prediction_inverse_normalization,
            left_screen=left_screen,
            row_chunk=7,
        )
        candidate_metrics = GATE.relative_metrics(candidate, exact)
        wrong_screen_metrics = GATE.relative_metrics(wrong_screen, exact)
        wrong_taylor_metrics = GATE.relative_metrics(wrong_taylor_result, exact)

        self.assertLess(candidate_metrics["relative_l2"], 1.0e-10)
        self.assertGreater(
            wrong_screen_metrics["relative_l2"],
            1.0e4 * candidate_metrics["relative_l2"],
        )
        self.assertGreater(
            wrong_taylor_metrics["relative_l2"],
            1.0e4 * candidate_metrics["relative_l2"],
        )


if __name__ == "__main__":
    unittest.main()
