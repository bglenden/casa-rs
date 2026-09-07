# SPDX-License-Identifier: LGPL-3.0-or-later

import copy
from contextlib import ExitStack
import json
import os
from pathlib import Path
import struct
import unittest

import numpy as np

from .t51_casa_model import image_slab, open_verified, verify_coordinates
from .casa_image_compare import COORDINATE_ABSOLUTE_TOLERANCE, COORDINATE_RELATIVE_TOLERANCE


def bits(value):
    return struct.pack(">d", value).hex()


class RecordCoordinates:
    def fromrecord(self, record):
        self.record = copy.deepcopy(record)
        return True

    def torecord(self):
        return self.record


class CasaModelTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("CASA_RS_T51_COORDINATE_PROBE_ROOT"),
                         "requires retained T51 model metadata and native CASA")
    def test_native_retained_model_coordinates_and_one_ulp_negative_control(self):
        from casatools import image

        root = Path(os.environ["CASA_RS_T51_COORDINATE_PROBE_ROOT"])
        manifest = json.loads((root / "authoritative-model/manifest.json").read_text())
        with ExitStack() as stack:
            for term in (0, 1):
                tool, coordinates = open_verified(stack, root / f"probe.model.tt{term}", manifest, image)
                changed = copy.deepcopy(coordinates)
                changed["direction0"]["crval"][0] = np.nextafter(
                    changed["direction0"]["crval"][0], -np.inf)
                csys = tool.coordsys()
                try:
                    with self.assertRaisesRegex(ValueError, "crval"):
                        verify_coordinates(manifest, tool.shape(), changed, csys)
                finally:
                    csys.done()

    def fixture(self):
        domain = dict(pixels=[4096, 4096], projection="Sin", frame="J2000",
                      reference_direction_rad_bits=list(map(bits, [1.0, -0.5])),
                      reference_pixel_bits=list(map(bits, [2048.0, 2048.0])),
                      increment_rad_bits=list(map(bits, [-0.000001, 0.000001])),
                      pc_bits=[list(map(bits, row)) for row in [[1.0, 0.0], [0.0, 1.0]]],
                      pole_deg_bits=list(map(bits, [180.0, -28.0])))
        spectral = dict(output_frame="Lsrk", reference_pixel_bits=bits(0.0),
                        reference_frequency_hz_bits=bits(3e9), increment_hz_bits=bits(1e9))
        coordinates = dict(
            direction0=dict(system="J2000", conversionSystem="J2000", projection="SIN",
                            projection_parameters=[0.0, 0.0], units=["rad", "rad"],
                            crval=[1.0, -0.5], crpix=[2048.0, 2048.0],
                            cdelt=[-0.000001, 0.000001], pc=[[1.0, 0.0], [0.0, 1.0]],
                            longpole=180.0, latpole=-28.0),
            stokes1=dict(stokes=["I"]),
            spectral2=dict(unit="Hz", system="LSRK", conversion=dict(system="LSRK"),
                           wcs=dict(crpix=0.0, crval=3e9, cdelt=1e9)),
        )
        for prefix in ("pixelmap", "worldmap"):
            for ordinal, axes in enumerate(([0, 1], [2], [3])):
                coordinates[f"{prefix}{ordinal}"] = axes
        return dict(domains=[domain], spectral=spectral), coordinates

    def test_exact_coordinate_law_and_axes(self):
        manifest, coordinates = self.fixture()
        verify_coordinates(manifest, [4096, 4096, 1, 1], coordinates, RecordCoordinates())
        for section, field, value in (
            ("direction0", "crval", [1.0 + 1e-15, -0.5]),
            ("direction0", "units", ["deg", "deg"]),
            ("stokes1", "stokes", ["Q"]),
            ("spectral2", "system", "TOPO"),
        ):
            changed = copy.deepcopy(coordinates)
            changed[section][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                verify_coordinates(manifest, [4096, 4096, 1, 1], changed, RecordCoordinates())
        coordinates["worldmap2"] = [2]
        with self.assertRaises(ValueError):
            verify_coordinates(manifest, [4096, 4096, 1, 1], coordinates, RecordCoordinates())

    def test_native_canonicalization_uses_authority_and_keeps_exact_comparison(self):
        manifest, coordinates = self.fixture()
        canonical_ra = np.nextafter(1.0, 0.0)
        coordinates["direction0"].update(crval=[canonical_ra, -0.5], latpole=-30.0)
        test = self

        class CanonicalCoordinates(RecordCoordinates):
            def fromrecord(self, record):
                test.assertEqual(record["direction0"]["crval"], [1.0, -0.5])
                test.assertEqual(record["direction0"]["latpole"], -28.0)
                super().fromrecord(record)
                self.record["direction0"].update(crval=[canonical_ra, -0.5], latpole=-30.0)
                return True

        verify_coordinates(manifest, [4096, 4096, 1, 1], coordinates, CanonicalCoordinates())
        coordinates["direction0"]["crval"][0] = np.nextafter(canonical_ra, 0.0)
        with self.assertRaisesRegex(ValueError, "crval"):
            verify_coordinates(manifest, [4096, 4096, 1, 1], coordinates, CanonicalCoordinates())

    def test_spectral_numeric_fields_use_existing_coordinate_bounds(self):
        manifest, coordinates = self.fixture()
        for field in ("crpix", "crval", "cdelt"):
            expected = coordinates["spectral2"]["wcs"][field]
            bound = max(COORDINATE_ABSOLUTE_TOLERANCE,
                        abs(expected) * COORDINATE_RELATIVE_TOLERANCE)
            for factor, accepted in ((0.5, True), (2.0, False)):
                changed = copy.deepcopy(coordinates)
                changed["spectral2"]["wcs"][field] = expected + factor * bound
                with self.subTest(field=field, factor=factor):
                    if accepted:
                        verify_coordinates(manifest, [4096, 4096, 1, 1], changed, RecordCoordinates())
                    else:
                        with self.assertRaisesRegex(ValueError, field):
                            verify_coordinates(manifest, [4096, 4096, 1, 1], changed, RecordCoordinates())

    def test_slab_mapping_preserves_x_fastest_and_taylor_planes(self):
        plane = 4096 * 4096
        for offset in (0, 65536, plane, plane + 65536):
            term, blc, trc, slab = image_slab(offset, 65536, (4096, 4096))
            self.assertEqual(term, offset // plane)
            self.assertEqual(blc, [0, (offset % plane) // 4096, 0, 0])
            self.assertEqual(trc[1], blc[1] + 15)
            values = np.arange(65536)
            shaped = values.reshape(slab, order="F")
            self.assertEqual(shaped[1, 0, 0, 0], 1)
            self.assertEqual(shaped[0, 1, 0, 0], 4096)
            np.testing.assert_array_equal(shaped.ravel(order="F"), values)
        for offset, count in ((1, 4096), (0, 4095), (plane - 4096, 8192), (plane * 2, 4096)):
            with self.subTest(offset=offset, count=count), self.assertRaises(ValueError):
                image_slab(offset, count, (4096, 4096))


if __name__ == "__main__":
    unittest.main()
