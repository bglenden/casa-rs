"""Small deterministic tests for the publication benchmark's measurement contract."""
import copy
import unittest

import publication_probe as probe


class PublicationProbeTests(unittest.TestCase):
    def test_paired_statistics_and_fixed_design(self):
        pairs = [{'candidate': {'result': {'publication_seconds': 9.0}},
                  'parent': {'result': {'publication_seconds': 10.0}}}] * 6
        stats = probe.paired_statistics(pairs)
        self.assertAlmostEqual(stats['ratio'], 0.9)
        self.assertLess(stats['ci95'][1], 0.98)
        with self.assertRaises(AssertionError):
            probe.paired_statistics(pairs[:3])

    def test_equal_timings_do_not_satisfy_improvement_gate(self):
        pairs = [{'candidate': {'result': {'publication_seconds': 10.0}},
                  'parent': {'result': {'publication_seconds': 10.0}}}] * 4
        self.assertEqual(probe.paired_statistics(pairs)['ci95'], [1.0, 1.0])

    def test_signature_requires_complete_exact_fingerprints(self):
        result = dict(rows=351, image_size=128, major_cycles=2, actual_minor_iterations=29,
            product_fingerprints={name: 'a' * 64 for name in probe.PRODUCTS})
        self.assertEqual(probe.scientific_signature(result), result)
        incomplete = copy.deepcopy(result)
        del incomplete['product_fingerprints']['.mask']
        with self.assertRaises(AssertionError):
            probe.scientific_signature(incomplete)
        malformed = copy.deepcopy(result)
        malformed['product_fingerprints']['.pb'] = 'z' * 64
        with self.assertRaises(AssertionError):
            probe.scientific_signature(malformed)
        changed = copy.deepcopy(result)
        changed['product_fingerprints']['.mask'] = 'b' * 64
        self.assertNotEqual(probe.scientific_signature(changed), probe.scientific_signature(result))


if __name__ == '__main__':
    unittest.main()
