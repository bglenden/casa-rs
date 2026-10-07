#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Scientific-truth checks for the isolated C-array fixture, not imaging acceptance."""
import unittest
import numpy as np

import stage_t55_c_array as sky


class CArraySkyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.templates = sky.spatial_templates(128, 61.44 / 128)

    def test_line_free_windows_are_empty_everywhere(self):
        for channel in list(range(128)) + list(range(384, 512)):
            _, line, emission = sky.sky_plane(channel, self.templates)
            self.assertEqual(np.count_nonzero(line), 0)
            self.assertEqual(np.count_nonzero(emission), 0)

    def test_total_sky_positive_finite_and_has_no_boundary_flux(self):
        outside = self.templates["support"] == 0
        for channel in range(512):
            continuum, line, _ = sky.sky_plane(channel, self.templates)
            total = continuum + line
            self.assertTrue(np.isfinite(total).all())
            self.assertGreaterEqual(total.min(), 0)
            self.assertEqual(np.count_nonzero(total[outside]), 0)

    def test_continuum_is_physical_frequency_power_law(self):
        for channel in (0, 256, 511):
            continuum, _, _ = sky.sky_plane(channel, self.templates)
            ratio = (44e9 + channel * 2e6) / 44.512e9
            expected = 0.8 + 0.35 * ratio**-0.7 + 0.18 * ratio**-0.8 + 0.12 + 0.4 * ratio**-0.85 + 0.6 * ratio**-0.5
            self.assertAlmostEqual(continuum.sum(), expected, places=12)
        self.assertAlmostEqual(sum(c[1] for c in self.templates["continuum"]), 2.45)

    def test_line_channels_change_morphology_not_only_scale(self):
        _, _, blue = sky.sky_plane(278, self.templates)
        _, _, red = sky.sky_plane(234, self.templates)
        self.assertGreater(np.max(np.abs(blue / blue.max() - red / red.max())), 0.5)
        blue_centroid = np.array([(blue * self.templates[k]).sum() / blue.sum() for k in ("x", "y")])
        red_centroid = np.array([(red * self.templates[k]).sum() / red.sum() for k in ("x", "y")])
        self.assertGreater(np.linalg.norm(blue_centroid - red_centroid), 3)

    def test_absorption_is_local_and_does_not_replace_continuum(self):
        continuum, line, emission = sky.sky_plane(245, self.templates)
        absorption = line - emission
        self.assertLess(absorption.sum(), -0.4)
        self.assertTrue(np.all(absorption <= 1e-15))
        self.assertTrue(np.all(continuum + line >= 0))
        core_support = self.templates["continuum"][0][3] != 0
        self.assertTrue(np.all(absorption[~core_support] == 0))

    def test_fits_wcs_and_channel_shape_are_explicit(self):
        from astropy.io.fits import Card
        header = sky.fits_header(1024, 0.06, 512)
        self.assertEqual(len(header) % 2880, 0)
        cards = {header[i:i+8].decode().strip(): header[i+10:i+80].decode().strip()
                 for i in range(0, len(header), 80)}
        self.assertEqual(int(cards["NAXIS1"]), 1024)
        self.assertEqual(int(cards["NAXIS4"]), 512)
        self.assertEqual(float(cards["CRPIX1"]), 513)
        self.assertAlmostEqual(float(cards["CDELT1"]), -0.06 / 3600)
        self.assertEqual(float(cards["CRVAL4"]), 44e9)
        self.assertEqual(cards["BUNIT"], "'Jy/pixel'")
        self.assertEqual(cards["SPECSYS"], "'LSRK'")
        for offset in range(0, len(header), 80):
            raw = header[offset:offset+80].decode()
            if raw.startswith("END"):
                break
            Card.fromstring(raw).verify("exception")


if __name__ == "__main__":
    unittest.main()
