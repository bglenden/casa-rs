import importlib.util
import pathlib
import sys
import unittest


SCRIPT = pathlib.Path(__file__).with_name(
    "vlass_ordered_response_oversampled_construction.py"
)
SPEC = importlib.util.spec_from_file_location("oversampled_construction", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
OVERSAMPLED = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = OVERSAMPLED
SPEC.loader.exec_module(OVERSAMPLED)


class OversampledConstructionTests(unittest.TestCase):
    def test_wrapper_selects_twice_the_resident_grid(self) -> None:
        base = OVERSAMPLED.load_base_module()

        self.assertEqual(OVERSAMPLED.CONSTRUCTION_SIDE, 2 * 192)
        self.assertEqual(base.SIDE, 384)
        self.assertEqual(base.PIXELS, 384 * 384)
        self.assertEqual(base.SCHEMA, OVERSAMPLED.SCHEMA)


if __name__ == "__main__":
    unittest.main()
