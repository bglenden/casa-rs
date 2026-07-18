from __future__ import annotations

import unittest
from pathlib import Path

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
            ghostty_capture=None,
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


if __name__ == "__main__":
    unittest.main()
