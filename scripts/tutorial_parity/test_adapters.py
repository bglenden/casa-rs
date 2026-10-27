from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tutorial_parity.adapters.base import AdapterPlan, execute_plan
from tutorial_parity.adapters.cli import build_operation_argv
from tutorial_parity.model import Operation, RuntimeResources
from tutorial_parity.workers.native_tasks import python_parameters


class CliPlanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.resources = RuntimeResources(
            repo_root=Path("/repo"),
            pack_root=Path("/pack"),
            native_python=None,
            casa_python=None,
            binary_dir=Path("/bin"),
            evidence_root=Path("/evidence"),
            dry_run=True,
        )

    def test_exportfits_positional_arguments_precede_flags(self) -> None:
        operation = Operation("exportfits", {"imagename": "in.image", "fitsimage": "out.fits", "velocity": True, "overwrite": True}, ("out.fits",))
        self.assertEqual(
            build_operation_argv(operation, self.resources),
            ["/bin/exportfits", "in.image", "out.fits", "--velocity", "--overwrite"],
        )

    def test_immoments_uses_checked_list_serialization(self) -> None:
        operation = Operation("immoments", {"imagename": "in.image", "outfile": "mom0", "moments": 0, "includepix": [0.03, 100.0]}, ("mom0",))
        self.assertEqual(
            build_operation_argv(operation, self.resources),
            ["/bin/immoments", "in.image", "--outfile", "mom0", "--moments", "0", "--includepix", "0.03,100.0"],
        )

    def test_split_uses_native_ms_and_out_flags(self) -> None:
        operation = Operation("split", {"vis": "in.ms", "outputvis": "out.ms", "width": 8}, ("out.ms",))
        self.assertEqual(
            build_operation_argv(operation, self.resources),
            ["/bin/mstransform", "--ms", "in.ms", "--out", "out.ms", "--width", "8"],
        )

    def test_python_projection_uses_generated_imager_contract(self) -> None:
        self.assertEqual(
            python_parameters("imager", {"cell_arcsec": 0.1, "threshold_jy": 0.015, "field": 3, "phasecenter_field": 3, "overwrite": True}),
            {"cell": "0.1arcsec", "threshold": "0.015Jy", "field": "3", "phasecenter_field": "3"},
        )


class ExecutionTests(unittest.TestCase):
    def test_a_rerun_starts_without_the_sections_own_outputs(self) -> None:
        # split refuses an existing output, as CASA's does (#931); a rerun
        # in the same pack must still run it.
        with tempfile.TemporaryDirectory() as pack, tempfile.TemporaryDirectory() as elsewhere:
            pack_root = Path(pack)
            product = pack_root / "out.ms"
            (product / "table.dat").parent.mkdir(parents=True)
            (product / "table.dat").write_text("first run", encoding="utf-8")
            report = pack_root / "nested" / "report.txt"
            report.parent.mkdir()
            report.write_text("first run", encoding="utf-8")
            worker_result = Path(elsewhere) / "result.json"
            worker_result.write_text("kept", encoding="utf-8")
            refuse_existing = (
                "import pathlib, sys\n"
                "if pathlib.Path('out.ms').exists(): sys.exit('out.ms already exists')\n"
                "pathlib.Path('out.ms').mkdir()\n"
            )
            plan = AdapterPlan(
                surface="cli",
                argv=(sys.executable, "-c", refuse_existing),
                cwd=pack_root,
                env=None,
                timeout_seconds=60,
                outputs=(worker_result, product, report),
                metadata={},
            )
            result = execute_plan(plan)
            self.assertEqual(result["status"], "completed", result)
            self.assertFalse((product / "table.dat").exists())
            self.assertFalse(report.exists())
            self.assertTrue(report.parent.is_dir())
            self.assertEqual(worker_result.read_text(encoding="utf-8"), "kept")


if __name__ == "__main__":
    unittest.main()
