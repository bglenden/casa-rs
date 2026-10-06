# SPDX-License-Identifier: LGPL-3.0-or-later

from pathlib import Path
import tempfile
import unittest

from .t51_cf_reload_cost_probe import STAGES, fields, sample_costs, summarize, evaluate_ownership_controls


def sample():
    value = {name + "_nanos": "1" for name in STAGES}
    value.update(admission_nanos="4", payload_nanos="8", reader_nanos="9", total_nanos="19",
                 completed="true", p_num="1", p_den="32", seed=str(0x7453315f72656c64),
                 session="1", ordinal="1", catalog_index="0", phase="0", reload="0", bytes="1024", **{"pass": "0"})
    return value


def count():
    return dict(session="1", phase="0", reload="0", size_bucket="10", loads="32", bytes="32768", samples="1",
                p_num="1", p_den="32", seed=str(0x7453315f72656c64), catalog_entries="1024", reserved_bytes="65536", aborted="false")


def line(prefix, fields):
    return prefix + " ".join(key + "=" + value for key, value in fields.items()) + "\n"


class ReloadAttributionTests(unittest.TestCase):
    def ownership_log(self, candidate_total=50):
        result = []
        for size in (1139200, 5939200, 19302400):
            for trial, copying in enumerate(("true", "false", "false", "true", "true", "false")):
                identity = dict(payload_bytes=str(size), trial=str(trial), copying=copying)
                result.append(line("t51_transfer_begin ", identity))
                result.append(line("t51_cf_reload_sample ", {
                    **sample(), "bytes": str(size), "p_den": "1", "total_nanos": "100" if copying == "true" else str(candidate_total),
                    "construct_kernels_nanos": "40" if copying == "true" else "10"}))
                reader = dict.fromkeys(("reads", "read_bytes", "loads", "hits", "evicted_bytes", "copied_bytes",
                                        "resident_peak_bytes", "decoder_workspace_peak_bytes", "pinned_peak_bytes", "cells_verified",
                                        "cells_rejected", "digest_failures", "eof_failures", "finite_failures"), "0")
                reader.update(reads="1", loads="1", copied_bytes=str(size), aborted="false")
                result.append(line("imaging_prepared_artifact_reader_summary ", reader))
                result.append(line("t51_transfer_complete ", {**identity, "normal_sha256": "exact"}))
        return "".join(result)

    def test_ownership_controls_require_construction_and_complete_load_separation(self):
        self.assertTrue(evaluate_ownership_controls(self.ownership_log())["passed"])
        self.assertFalse(evaluate_ownership_controls(self.ownership_log(100))["passed"])
        self.assertFalse(evaluate_ownership_controls(self.ownership_log(110))["passed"])

    def test_ownership_controls_fail_closed_on_incomplete_work_and_identity_changes(self):
        log = self.ownership_log()
        for invalid in (log.replace("normal_sha256=exact", "normal_sha256=changed", 1),
                        log.replace("digest_failures=0", "digest_failures=1", 1),
                        log.replace("p_den=1", "p_den=32", 1),
                        log[:log.rfind("t51_transfer_complete ")],
                        log.replace("copying=true", "copying=true copying=false", 1)):
            with self.subTest(invalid=invalid[:60]), self.assertRaises(ValueError):
                evaluate_ownership_controls(invalid)

    def test_libtest_inline_receipt_and_distinct_envelope_are_preserved(self):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root) / "control.log"
            log.write_text("test prepared_artifact::reader::tests::control ... t51_cf_reload_control "
                           "mode=off read_envelope_nanos=315081912 reader_nanos=0\nok\n"
                           "unrelated prose t51_cf_reload_control mode=wrong\n")
            self.assertEqual(fields(log, "t51_cf_reload_control "),
                             [dict(mode="off", read_envelope_nanos="315081912", reader_nanos="0")])

    def test_duplicate_duration_fields_fail_instead_of_overwriting(self):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root) / "control.log"
            log.write_text("test control ... t51_cf_reload_control reader_nanos=315081912 reader_nanos=0\n")
            with self.assertRaisesRegex(ValueError, "duplicate diagnostic field: reader_nanos"):
                fields(log, "t51_cf_reload_control ")

    def run_summary(self, samples, counts):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root) / "probe.log"
            log.write_text("".join(line("t51_cf_reload_sample ", value) for value in samples)
                           + "".join(line("t51_cf_reload_stratum ", value) for value in counts))
            return summarize(log)

    def test_nested_costs_keep_nonnegative_explicit_remainders(self):
        costs = sample_costs(sample())
        self.assertEqual(costs["total_unassigned"], 1)
        self.assertEqual(costs["payload_unassigned"], 0)
        self.assertEqual(costs["collection_decode_construct"], 3)

    def test_nested_overcount_failed_sample_and_wrong_probability_fail_closed(self):
        for changes in ({"payload_nanos": "7"}, {"completed": "false"}, {"p_den": "16"}, {"read_nanos": "-1"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                sample_costs({**sample(), **changes})

    def test_exact_denominators_and_ht_estimate(self):
        result = self.run_summary([sample()], [count()])
        self.assertEqual(result["sessions"]["1"], dict(loads=32, bytes=32768, first_loads=32, reloads=0))
        self.assertAlmostEqual(result["role_estimates"][0]["costs"]["collection_decode_construct"]["ht_total_seconds"], 96e-9)
        self.assertIsNone(result["full_run_materiality"])
        self.assertFalse(result["optimization_authorized"])

    def test_duplicate_or_missing_samples_are_not_accepted(self):
        for samples, counts in (([sample(), sample()], [count()]), ([sample()], [{**count(), "samples": "2"}]),
                                ([sample()], [count(), count()]), ([sample()], [{**count(), "aborted": "true"}])):
            with self.subTest(samples=len(samples), counts=len(counts)), self.assertRaises(ValueError):
                self.run_summary(samples, counts)

    def test_unobserved_stratum_is_explicit_not_a_zero_cost_claim(self):
        missing = {**count(), "size_bucket": "11", "loads": "1", "bytes": "2048", "samples": "0"}
        result = self.run_summary([sample()], [count(), missing])
        self.assertEqual(result["unobserved_strata"], [missing])
        self.assertEqual(result["strata"][1]["estimates"], {})


if __name__ == "__main__":
    unittest.main()
