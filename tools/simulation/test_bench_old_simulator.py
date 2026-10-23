# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of bench_old_simulator.py's translation and tabulation (no CASA needed)."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bench_old_simulator as bench  # noqa: E402


def manifest_entry(predict: bool, usehourangle: bool) -> dict:
    calls = [
        {"op": "open", "resolved": {"ms": "x.ms"}, "returned": True},
        {
            "op": "setconfig",
            "resolved": {
                "telescopename": "ALMA",
                "x": [1.0, 2.0, 3.0],
                "y": [4.0, 5.0, 6.0],
                "z": [7.0, 8.0, 9.0],
                "dishdiameter": [12.0, 12.0, 12.0],
                "antname": ["A1", "A2", "A3"],
                "padname": ["P1", "P2", "P3"],
            },
            "returned": True,
        },
        {
            "op": "setspwindow",
            "resolved": {"spwname": "b6", "freq": 230e9, "deltafreq": 7.8125e6, "nchannels": 4, "stokes": "XX YY"},
        },
        {"op": "settimes", "resolved": {"integrationtime": 10.0, "usehourangle": usehourangle, "referencetime": 61120.0}},
        {"op": "setfield", "resolved": {"sourcename": "t", "sourcedirection": [-2.0, -0.5]}},
        {"op": "observe", "resolved": {"starttime": -30.0, "stoptime": 30.0}},
        {"op": "setseed", "resolved": {"seed": 11111}},
        {"op": "setnoise", "resolved": {"mode": "simplenoise", "simplenoise": 0.1}},
        {"op": "corrupt", "resolved": {}},
    ]
    if predict:
        calls.insert(6, {"op": "predict", "resolved": {"imagename": ["/models/alma-cube.image"]}})
    return {"name": "c", "calls": calls}


class HarnessTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        binary = os.path.join(self.tmp.name, "simobserve")
        open(binary, "w").close()
        self.args = argparse.Namespace(
            cases=os.path.join(bench.HERE, "bench_scenarios.json"),
            mapping=os.path.join(bench.HERE, "bench_old_simulator.json"),
            manifests=self.tmp.name,
            out=os.path.join(self.tmp.name, "out"),
            models=None,
            image_root=None,
            bin=binary,
            casa_python="python3",
            repeats=1,
            variants=["auto"],
            append=False,
            rest_s=0.0,
            casa_time_shift_s=37.0,
            max_load=100.0,
            busy_deadline_s=1.0,
            only=None,
            keep_ms=False,
            reference_results=None,
            nas_dir=None,
            tabulate=False,
        )
        self.harness = bench.Harness(self.args)

    def tearDown(self):
        self.tmp.cleanup()

    def test_noise_only_request_without_hour_angle(self):
        request, info = self.harness.build_request("c", manifest_entry(predict=False, usehourangle=False))
        body = request["request"]
        self.assertEqual(request["kind"], "run")
        self.assertEqual(body["telescope_name"], "ALMA")
        self.assertEqual(body["antennas"][1], {"name": "A2", "station": "P2", "position_m": [2.0, 5.0, 8.0], "dish_diameter_m": 12.0})
        self.assertEqual(body["polarization_setup"], {"basis": "linear", "correlation_count": 2})
        self.assertFalse(body["predict_model"])
        self.assertNotIn("model", body)
        self.assertNotIn("model_peak_jy_per_pixel", body)
        self.assertTrue(body["allow_below_elevation_limit"])
        self.assertAlmostEqual(body["phase_center_rad"][0], 2.0 * math.pi - 2.0)
        self.assertEqual(body["start_time_mjd_seconds"], 61120.0 * 86400.0 - 30.0 + 37.0)
        self.assertEqual(body["duration_seconds"], 60.0)
        self.assertEqual(body["corruption"], {"seed": 11111, "noise": {"mode": "simplenoise", "simplenoise_jy": 0.1}})
        self.assertEqual(info["expected_rows"], 6 * 3)
        self.assertEqual(info["expected_data_shape"], [2, 4])
        self.assertEqual(info["expected_corr_type"], [9, 12])

    def test_predict_request_adds_the_hour_angle_offset_and_the_fits_model(self):
        self.harness.casa = lambda *args: {"offset_s": 100.0}
        request, info = self.harness.build_request("c", manifest_entry(predict=True, usehourangle=True))
        body = request["request"]
        self.assertTrue(body["predict_model"])
        self.assertEqual(body["model"], {"kind": "fits_image", "path": os.path.join(self.harness.models, "alma-cube.fits")})
        self.assertEqual(body["start_time_mjd_seconds"], 61120.0 * 86400.0 + 100.0 - 30.0 + 37.0)
        self.assertEqual(info["expected_time_first"], body["start_time_mjd_seconds"] + 5.0)

    def test_several_fields_are_one_span_and_a_complist_is_an_analytic_model(self):
        entry = manifest_entry(predict=False, usehourangle=False)
        centre = [math.radians(195.0), math.radians(34.0)]
        calls = [call for call in entry["calls"] if call["op"] not in ("setfield", "observe")]
        calls[4:4] = [
            {"op": "setfield", "resolved": {"sourcename": "p0", "sourcedirection": centre}},
            {"op": "setfield", "resolved": {"sourcename": "p1", "sourcedirection": [centre[0] + 1e-4, centre[1]]}},
            {"op": "observe", "resolved": {"sourcename": "p0", "starttime": -30.0, "stoptime": 0.0}},
            {"op": "observe", "resolved": {"sourcename": "p1", "starttime": 0.0, "stoptime": 30.0}},
            {"op": "predict", "resolved": {"imagename": [], "complist": "/out/complists/case4-14pt-3gauss.cl"}},
        ]
        entry["calls"] = calls
        request, info = self.harness.build_request("c", entry)
        body = request["request"]
        self.assertEqual(body["duration_seconds"], 60.0)
        self.assertEqual([field["name"] for field in body["fields"]], ["p0", "p1"])
        self.assertAlmostEqual(body["fields"][1]["phase_center_rad"][0], centre[0] + 1e-4)
        self.assertEqual(info["expected_rows"], 6 * 3)
        self.assertEqual(body["model"], {"kind": "analytic_components", "path": info["model_json"]})
        with open(info["model_json"], encoding="utf-8") as handle:
            components = json.load(handle)["components"]
        self.assertEqual([c["kind"] for c in components].count("gaussian"), 3)
        self.assertEqual(len(components), 17)
        # The first component is at the centre; the second 4.5e-5 and 3.5e-5 rad off it.
        self.assertAlmostEqual(components[0]["l_rad"], 0.0, places=12)
        self.assertAlmostEqual(components[1]["l_rad"], 4.5e-5, places=10)
        self.assertAlmostEqual(components[1]["m_rad"], 3.5e-5, places=10)
        self.assertEqual(components[0]["spectrum"], {"flux_jy": 1.0, "spectral_index": -0.65, "reference_frequency_hz": 43.0e9})
        self.assertAlmostEqual(components[14]["major_fwhm_rad"], 3.5e-5, places=10)
        # A gap between observe calls is not one span.
        calls[7]["resolved"]["starttime"] = 10.0
        with self.assertRaises(ValueError):
            self.harness.build_request("c", entry)

    def test_variants_number_after_earlier_runs(self):
        self.harness.runs = [
            {"case": "c", "kind": "oldsim", "variant": "old-1"},
            {"case": "c", "kind": "oldsim", "variant": "old-2"},
            {"case": "c", "kind": "oldsim", "variant": "old-w1-1"},
            {"case": "d", "kind": "oldsim", "variant": "old-5"},
        ]
        self.assertEqual(self.harness.next_variant("c", "old"), "old-3")
        self.assertEqual(self.harness.next_variant("c", "old-w1"), "old-w1-2")
        self.assertEqual(self.harness.next_variant("e", "old"), "old-1")


class TabulationTest(unittest.TestCase):
    def test_reference_pairs_one_worker_runs_with_one_worker_runs(self):
        runs = {
            "old-1": {"exit": 0, "wall_s": 10.0, "user_s": 40.0},
            "old-2": {"exit": 0, "wall_s": 12.0, "user_s": 40.0},
            "old-w1-1": {"exit": 0, "wall_s": 30.0, "user_s": 29.0},
        }
        reference = {
            "commit": "abc",
            "cases": {"c": {"casa_rs": {"cpu": {"wall_s": 11.0, "user_s": 30.0}, "cpu-w1": {"wall_s": 27.0, "user_s": 26.0}}}},
        }
        lines = bench.reference_lines("c", reference, runs)
        cpu = next(line for line in lines if line.startswith("| cpu |"))
        w1 = next(line for line in lines if line.startswith("| cpu-w1 |"))
        self.assertIn("| old (2) | 11.00 | 40.00 | 1.000 | 0.750 |", cpu)
        self.assertIn("| old-w1 (1) | 30.00 | 29.00 | 0.900 |", w1)

    def test_residual_is_the_total_less_the_listed_stages(self):
        timing = {
            "validate_millis": 1, "setup_millis": 2, "metadata_millis": 3, "model_prepare_millis": 4,
            "save_millis": 5, "total_millis": 1000,
            "main_rows": {"uvw_and_row_setup_millis": 6, "prediction_millis": 7, "corruption_millis": 8,
                          "scalar_column_millis": 9, "data_io_enqueue_millis": 10},
        }
        self.assertAlmostEqual(bench.residual_s(timing), 0.945)
        self.assertIsNone(bench.residual_s({"total_millis": 5}))

    def test_fits_header_reads_axes_and_quoted_values(self):
        cards = ["SIMPLE  =                    T", "BITPIX  =                  -32", "NAXIS   =                    2",
                 "NAXIS1  =                    8", "NAXIS2  =                    9", "CTYPE1  = 'RA---SIN'",
                 "CTYPE2  = 'DEC--SIN'", "BUNIT   = 'Jy/pixel'           / brightness", "END"]
        block = "".join(card.ljust(80) for card in cards).ljust(2880).encode("ascii")
        with tempfile.NamedTemporaryFile(suffix=".fits") as handle:
            handle.write(block)
            handle.flush()
            header = bench.fits_header(handle.name)
        self.assertEqual(header, {"bitpix": -32, "shape": [8, 9], "ctype": ["RA---SIN", "DEC--SIN"], "bunit": "Jy/pixel"})

    def test_mapping_names_every_benchmark_case(self):
        with open(os.path.join(bench.HERE, "bench_scenarios.json"), encoding="utf-8") as handle:
            names = {scenario["name"] for scenario in json.load(handle)["scenarios"]}
        with open(os.path.join(bench.HERE, "bench_old_simulator.json"), encoding="utf-8") as handle:
            mapped = set(json.load(handle)["cases"])
        self.assertEqual(names, mapped)


if __name__ == "__main__":
    unittest.main()
