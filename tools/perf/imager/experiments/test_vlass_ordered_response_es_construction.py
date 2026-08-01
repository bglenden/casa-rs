import importlib.util
import pathlib
import sys
import unittest

import numpy as np


SCRIPT = pathlib.Path(__file__).with_name("vlass_ordered_response_es_construction.py")
SPEC = importlib.util.spec_from_file_location("es_construction", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
ES = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ES
SPEC.loader.exec_module(ES)


class EsConstructionTests(unittest.TestCase):
    def test_kernel_is_symmetric_compact_and_smooth(self) -> None:
        self.assertEqual(ES.es_kernel_weight(0.0, -7), 0.0)
        self.assertEqual(ES.es_kernel_weight(0.0, 7), 0.0)
        self.assertAlmostEqual(
            ES.es_kernel_weight(0.25, 2),
            ES.es_kernel_weight(-0.25, -2),
            places=14,
        )
        self.assertGreater(
            ES.es_kernel_weight(0.0, 0),
            ES.es_kernel_weight(0.0, 3),
        )

    def test_exact_route_offsets_are_not_hundredth_cell_quantized(self) -> None:
        base = ES.load_base_module()
        rows = np.zeros(3, dtype=[("unused", "<f8")])
        facet_uvw = np.asarray(
            [
                [123.456789, -88.765432, 1.0],
                [-321.012345, 43.210987, -2.0],
                [0.125, -0.375, 3.0],
            ]
        )

        _, _, offset_x, offset_y = ES.exact_route_geometry(
            base,
            rows,
            facet_uvw,
        )

        self.assertEqual(offset_x.dtype, np.float32)
        self.assertEqual(offset_y.dtype, np.float32)
        self.assertTrue(np.any(np.abs(offset_x * 100 - np.rint(offset_x * 100)) > 1e-4))
        self.assertTrue(np.any(np.abs(offset_y * 100 - np.rint(offset_y * 100)) > 1e-4))


if __name__ == "__main__":
    unittest.main()
