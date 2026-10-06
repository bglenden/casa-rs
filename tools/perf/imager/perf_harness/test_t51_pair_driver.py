# SPDX-License-Identifier: LGPL-3.0-or-later

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from .t51_pair_driver import (
    CASA_SITE, REPO, SYNTHESIS_MODULE, SYNTHESIS_UUID, drive_lldb,
    interpreter_identity, rust_refresh_receipt,
)
from .t51_pair_casa import selection_receipt


class PairDriverTests(unittest.TestCase):
    def test_cli_rejects_missing_interpreter_or_reused_output_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            marker = root / "existing-receipt.json"
            marker.write_text("preserve")
            command = [sys.executable, "-m", "perf_harness.t51_pair_driver", "--authorized-once"]
            for arguments in (
                ["--output", str(root / "new")],
                ["--output", str(root), "--casa-python", sys.executable],
            ):
                with self.subTest(arguments=arguments):
                    result = subprocess.run(command + arguments, cwd=REPO, capture_output=True,
                                            text=True, timeout=10, check=False)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertNotIn("t51_pair_stage", result.stdout)
                    self.assertEqual(list(root.iterdir()), [marker])
                    self.assertEqual(marker.read_text(), "preserve")

    def test_interpreter_identity_binds_symlink_target_and_framework(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            (root / "bin").mkdir()
            python = root / "bin/python3.12"
            python.write_bytes(b"interpreter")
            framework = root / "Python"
            framework.write_bytes(b"framework")
            alias = root / "venv-python"
            alias.symlink_to(python)
            before = interpreter_identity(alias)
            self.assertEqual(before["path"], str(alias))
            self.assertEqual(before["resolved_path"], str(python))
            framework.write_bytes(b"changed framework")
            self.assertNotEqual(interpreter_identity(alias), before)
            with self.assertRaisesRegex(ValueError, "absolute"):
                interpreter_identity(Path("python3.12"))

    def test_debugger_uses_only_manifested_unchanged_interpreter_and_library(self):
        for mutation in (None, "interpreter", "library"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                identity = {"path": "/isolated/venv/bin/python3.12", "executable_sha256": "expected"}
                request = {"diagnostic_interpreter": identity, "casa_site_packages": str(CASA_SITE),
                           "synthesis_module": str(SYNTHESIS_MODULE), "synthesis_uuid": SYNTHESIS_UUID,
                           "synthesis_sha256": "library"}
                (root / "request.json").write_text(json.dumps(request))
                actual = {**identity, "executable_sha256": "changed"} if mutation == "interpreter" else identity
                with patch.dict(os.environ, {"T51_PAIR_ROOT": str(root)}), patch(
                    "perf_harness.t51_pair_driver.interpreter_identity", return_value=actual
                ), patch("perf_harness.t51_pair_driver.sha256_file",
                         return_value="changed" if mutation == "library" else "library"), patch(
                    "perf_harness.casa_cf_guard.run_guarded", return_value={"exit_status": 0}
                ) as launch, patch("traceback.print_exc"):
                    drive_lldb(object())
                receipt = json.loads((root / "native-guard.json").read_text())
                self.assertEqual(receipt["completed"], mutation is None)
                if mutation is None:
                    self.assertEqual(launch.call_args.kwargs["python"], Path(identity["path"]))
                    self.assertEqual(launch.call_args.kwargs["module_path"], SYNTHESIS_MODULE)
                else:
                    launch.assert_not_called()

    def test_source_root_and_disjoint_rust_timing_generation_binding(self):
        self.assertTrue((REPO / "Cargo.toml").is_file())
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "rust.log"
            path.write_text(
                "imaging_terminal_refresh_envelope model_generation=m elapsed_nanos=3000000000\n"
                "imaging_taylor_normalization_envelope model_generation=m aw_projection=true "
                "preparation_nanos=100000000 residual_model_nanos=200000000\n")
            result = rust_refresh_receipt(path, {"model_generation": "m"})
            self.assertEqual(result["normalized_refresh_seconds"], 3.3)
            with self.assertRaisesRegex(ValueError, "generations differ"):
                rust_refresh_receipt(path, {"model_generation": "foreign"})

    def test_bounded_casa_source_census(self):
        fields = list(range(1107, 1128)) + list(range(1512, 1533)) + list(range(1542, 1563))
        groups = [(field, ddid) for field in fields for ddid in range(16)]
        pairs = [(5, 22), (4, 5), (13, 14), (12, 13), (12, 23)] * 2

        class Selected:
            index = 0
            closed = False
            def open(self, path, nomodify):
                self_outer.assertTrue(nomodify)
                return True
            def msselect(self, selector):
                self_outer.assertEqual(selector["baseline"], "declared")
                return True
            def nrow(self, selected):
                self_outer.assertTrue(selected)
                return 10080
            def iterinit(self, **kwargs):
                self_outer.assertEqual(kwargs["maxrows"], 256)
            def iterorigin(self):
                return True
            def getdata(self, items, ifraxis):
                field, ddid = groups[self.index]
                return {"field_id": np.full(10, field), "data_desc_id": np.full(10, ddid),
                        "antenna1": np.array([p[0] for p in pairs]),
                        "antenna2": np.array([p[1] for p in pairs]),
                        "data": np.ones((4, 64, 10), dtype=np.complex64),
                        "flag": np.zeros((4, 64, 10), dtype=bool), "weight": np.ones((4, 10))}
            def iternext(self):
                self.index += 1
                return self.index < len(groups)
            def done(self):
                self.closed = True

        self_outer = self
        selected = Selected()
        result = selection_receipt(lambda: selected, {"ms": "unused", "baseline_selection": "declared"},
                                   {"field": "fields", "spw": "spws", "uvrange": "<12km", "intent": "intent"})
        self.assertEqual(result["samples"], 2580480)
        self.assertTrue(selected.closed)


if __name__ == "__main__":
    unittest.main()
