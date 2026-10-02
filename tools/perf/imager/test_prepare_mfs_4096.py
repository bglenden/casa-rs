#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Small deterministic checks for the workload-design diagnostic, not imaging acceptance."""

from pathlib import Path
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np

import prepare_mfs_4096 as design
import mfs_4096_pilot as pilot


class WorkloadDesignTests(unittest.TestCase):
    def test_simple_casa_case_uses_standard_grid_and_one_term_clark(self):
        task = Mock(return_value={})
        casa = SimpleNamespace(
            casalog=Mock(), tclean=task, version_string=lambda: "test"
        )
        for spw in ("0~31", "0,10,21,31"):
            with self.subTest(spw=spw):
                args = SimpleNamespace(
                    output=Path("unused"),
                    label="simple",
                    terms=1,
                    gridder="standard",
                    spw=spw,
                    niter=10000,
                )
                with (
                    patch.dict("sys.modules", casatasks=casa),
                    patch.object(Path, "mkdir"),
                    patch.object(pilot, "save"),
                    patch("builtins.print"),
                ):
                    pilot.image_pilot(args)
                self.assertEqual(task.call_args.kwargs["spw"], spw)
        request = task.call_args.kwargs
        self.assertEqual(request["gridder"], "standard")
        self.assertEqual(request["wprojplanes"], 1)
        self.assertEqual(request["deconvolver"], "clark")
        self.assertEqual(request["nterms"], 1)
        self.assertEqual(request["imsize"], [4096, 4096])
        self.assertEqual(request["weighting"], "uniform")
        self.assertEqual(request["savemodel"], "none")

    def test_pilot_directions_roundtrip_sin_projection(self):
        for east, north in [(0, 0), (-85, 30), (55, -60), (0, 90)]:
            ra, dec = pilot.direction(east, north)
            east_cosine = np.cos(dec) * np.sin(ra - np.pi)
            north_cosine = np.sin(dec) * np.cos(np.pi / 6) - np.cos(dec) * np.sin(
                np.pi / 6
            ) * np.cos(ra - np.pi)
            self.assertAlmostEqual(east_cosine / design.ARCSEC, east, places=9)
            self.assertAlmostEqual(north_cosine / design.ARCSEC, north, places=9)

    def test_casa_task_numpy_results_are_json_serializable(self):
        task = dict(summary=np.arange(4).reshape(2, 2), peak=np.float32(0.25))
        value = json.loads(json.dumps(task, default=pilot.numpy_json))
        self.assertEqual(value, dict(summary=[[0, 1], [2, 3]], peak=0.25))
        with self.assertRaises(TypeError):
            json.dumps(object(), default=pilot.numpy_json)

    def test_full_polarization_channelization_and_flags(self):
        windows = design.spectral_windows()
        self.assertEqual(len(windows), 32)
        self.assertEqual(sum(w["channels"] for w in windows), 2048)
        for w in windows:
            self.assertEqual(w["channel_width_hz"] * w["channels"], 128e6)
            self.assertTrue({0, 1, 62, 63}.issubset(w["flagged_channels"]))
            centers = w["first_center_hz"] + np.arange(64) * w["channel_width_hz"]
            kept = np.delete(centers, w["flagged_channels"])
            self.assertTrue(np.all((kept >= 4e9) & (kept <= 8e9)))

    def test_sky_is_deterministic_and_contains_distinct_morphologies(self):
        components = design.sky_components()
        self.assertEqual(components, design.sky_components())
        self.assertEqual(sum(c["major_arcsec"] == 0 for c in components), 79)
        for prefix in ("ring-", "jet-", "filament-", "diffuse-"):
            self.assertTrue(any(c["name"].startswith(prefix) for c in components))
        self.assertGreater(len({c["spectral_index"] for c in components}), 50)

    def test_flux_orientation_and_spectral_law(self):
        point = dict(
            east_arcsec=0.225,
            north_arcsec=0.125,
            flux_jy=2,
            spectral_index=-1,
            major_arcsec=0,
            minor_arcsec=0,
            pa_degrees=0,
        )
        image = design.render([point], 4e9, pixels=128)
        self.assertAlmostEqual(float(image.sum()), 3)
        yy, xx = np.indices(image.shape)
        self.assertAlmostEqual(
            float((xx * image).sum() / image.sum()), 64 - 0.225 / design.CELL
        )
        self.assertAlmostEqual(
            float((yy * image).sum() / image.sum()), 64 + 0.125 / design.CELL
        )
        gaussian = dict(point, major_arcsec=0.5, minor_arcsec=0.2, pa_degrees=37)
        image = design.render([gaussian], 6e9, pixels=128)
        self.assertAlmostEqual(float(image.sum()), 2, places=5)

    def test_uvw_rotation_preserves_baseline_length_and_counts(self):
        xyz = np.column_stack(
            (
                np.arange(27) * 100 + -1600000,
                np.arange(27) ** 2 + -5000000,
                np.arange(27) * 10 + 3500000,
            )
        )
        with (
            patch.object(np, "loadtxt", return_value=xyz),
            patch.object(Path, "read_bytes", return_value=b"test"),
        ):
            uvws, details = design.uvw_geometry(Path("unused"), ["A"], 90)
        self.assertEqual(uvws[0].shape, (351 * 30 * 3, 3))
        self.assertEqual(details[0]["time_samples"], 90)
        self.assertEqual(details[0]["integration_seconds"], 2)
        hour_angles = np.array(details[0]["hour_angle_centers_hours"])
        self.assertEqual(hour_angles[0], -3)
        self.assertEqual(hour_angles[-1], 3)
        np.testing.assert_allclose(np.diff(hour_angles), 6 / 89, rtol=1e-12)
        self.assertGreater(details[0]["cadence_seconds"], 2)
        i, j = np.triu_indices(27, 1)
        expected = np.tile(np.linalg.norm(xyz[j] - xyz[i], axis=1), 90)
        np.testing.assert_allclose(
            np.linalg.norm(uvws[0], axis=1), expected, rtol=1e-12
        )

    def test_psf_normalization_and_no_uv_aliasing(self):
        with patch.object(design, "PIXELS", 64):
            psf = design.density_psf([np.array([[1000.0, 2000.0, 0]])], [6e9])
            self.assertEqual(float(psf[32, 32]), 1)
            self.assertTrue(np.isfinite(psf).all())
            with self.assertRaisesRegex(ValueError, "Nyquist"):
                design.density_psf([np.array([[1e10, 0.0, 0.0]])], [6e9])

    def test_float32_beam_cut_is_json_serializable(self):
        cut = np.exp(-np.square(np.arange(64, dtype=np.float32) - 32) / 8)
        width = design.half_width(cut)
        self.assertGreater(width, 0)
        self.assertEqual(json.loads(json.dumps(width)), width)


if __name__ == "__main__":
    unittest.main()
