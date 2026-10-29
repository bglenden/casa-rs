from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tutorial_parity.adapters.base import AdapterPlan, execute_plan
from tutorial_parity.adapters.cli import CliAdapter, build_operation_argv
from tutorial_parity.model import Operation, RuntimeResources
from tutorial_parity.schema import load_section
from tutorial_parity.workers.native_tasks import json_safe, python_parameters

SECTIONS = Path(__file__).resolve().parent / "sections"

# A stand-in for `imexplore`: it answers `imhead --json-run FILE` as the task
# front end does, with the request's values as the header record, and
# refuses a request that sets `mode` to `fail`.
FAKE_IMEXPLORE = """#!{python}
import json, sys
assert sys.argv[1:3] == ["imhead", "--json-run"], sys.argv
request = json.load(open(sys.argv[3]))
values = request["values"]
if values.get("mode") == "fail":
    print(json.dumps({{"status": "failed", "surface": "imhead", "kind": "validation", "message": "bad mode"}}))
    sys.exit(2)
value = {{"kind": "dict", "entries": [
    {{"key": key, "value": {{"kind": "string", "value": text}}}} for key, text in values.items()
] + [{{"key": "shape", "value": {{"kind": "int_array", "dtype": "int64", "shape": [2, 2], "data": [1, 2, 3, 4]}}}}]}}
print(json.dumps({{"status": "succeeded", "surface": request["surface"], "value": value, "result": {{}}}}))
"""


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

    def test_split_runs_mstransforms_split_surface_by_casa_names(self) -> None:
        operation = Operation("split", {"vis": "in.ms", "outputvis": "out.ms", "width": 8}, ("out.ms",))
        self.assertEqual(
            build_operation_argv(operation, self.resources),
            ["/bin/mstransform", "split", "--vis", "in.ms", "--outputvis", "out.ms", "--width", "8"],
        )

    def test_a_return_value_is_run_through_one_json_run_request(self) -> None:
        operation = Operation("imstat", {"imagename": "in.image", "json": True}, ())
        self.assertEqual(
            build_operation_argv(operation, self.resources, Path("/evidence/request.json")),
            ["/bin/imexplore", "imstat", "--json-run", "/evidence/request.json"],
        )
        with self.assertRaisesRegex(ValueError, "one-operation section"):
            build_operation_argv(operation, self.resources)

    def test_tclean_runs_casars_imager_with_casas_values(self) -> None:
        operation = Operation(
            "tclean",
            {"vis": "in.ms", "imagename": "img", "imsize": [250, 250], "cell": ["0.1arcsec"], "pbcor": False, "write_pb": True},
            ("img.image",),
        )
        self.assertEqual(
            build_operation_argv(operation, self.resources),
            [
                "/bin/casars-imager", "--ms", "in.ms", "--imagename", "img", "--imsize", "250,250",
                "--cell", "0.1arcsec", "--no-pbcor", "--write-pb",
            ],
        )

    def test_python_projection_passes_casas_tclean_values_through(self) -> None:
        # tclean has CASA's signature (#931 PR 4e).
        values = {"vis": "in.ms", "cell": ["0.1arcsec"], "threshold": "15mJy", "field": "3"}
        self.assertEqual(python_parameters("tclean", {**values, "json": True}), values)

    def test_python_projection_passes_casas_image_values_through(self) -> None:
        # The image wrappers have CASA's signatures (#931).
        values = {"imagename": "in.image", "moments": [0], "includepix": [0.03, 100.0]}
        self.assertEqual(python_parameters("immoments", values), values)

    def test_a_casa_return_value_with_arrays_is_captured_as_json(self) -> None:
        class Array:
            """What json_safe needs of a NumPy array or scalar."""

            def __init__(self, values: list[object]) -> None:
                self.values = values

            def tolist(self) -> list[object]:
                return self.values

        value = {"npts": Array([16.0]), "maxpos": Array([3, 3, 0, 7])}
        self.assertEqual(json_safe(value), {"npts": [16.0], "maxpos": [3, 3, 0, 7]})


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

    def run_imhead_section(self, root: Path, mode: str) -> tuple[dict, Path, dict]:
        """The committed imhead section's CLI surface, run against the fake
        `imexplore` with `mode` set: the result, the capture and the request."""

        manifest = load_section(SECTIONS / "image-analysis-01-imhead.json")
        operation = manifest.surfaces["cli"].operations[0]
        surface = type(manifest.surfaces["cli"])(
            "cli", (Operation(operation.task, {**operation.parameters, "mode": mode}, operation.outputs, operation.capture_stdout),)
        )
        bin_dir = root / "bin"
        bin_dir.mkdir(exist_ok=True)
        fake = bin_dir / "imexplore"
        fake.write_text(FAKE_IMEXPLORE.format(python=sys.executable), encoding="utf-8")
        fake.chmod(0o755)
        resources = RuntimeResources(
            repo_root=Path(__file__).resolve().parents[2],
            pack_root=root / "pack",
            native_python=None,
            casa_python=None,
            binary_dir=bin_dir,
            evidence_root=root / "evidence",
            dry_run=False,
        )
        resources.pack_root.mkdir(exist_ok=True)
        adapter = CliAdapter()
        plan = adapter.plan(manifest, surface, resources)
        request = json.loads(Path(plan.argv[-1]).read_text(encoding="utf-8"))
        return adapter.execute(plan), Path(plan.metadata["capture_stdout"]), request

    def test_a_return_value_section_captures_the_replys_value(self) -> None:
        # imexplore lost its `--json` display flag (#931): the section's JSON
        # is the task's return value, from its `--json-run` reply.
        with tempfile.TemporaryDirectory() as root:
            result, capture, request = self.run_imhead_section(Path(root), "summary")
            self.assertEqual(result["status"], "completed", result)
            self.assertEqual(
                request,
                {"surface": "imhead", "values": {"imagename": "twhya_cont.image", "mode": "summary"}},
            )
            # The typed value is captured as plain JSON; arrays are nested
            # from their column-major data.
            self.assertEqual(
                json.loads(capture.read_text(encoding="utf-8")),
                {"imagename": "twhya_cont.image", "mode": "summary", "shape": [[1, 3], [2, 4]]},
            )

    def test_a_refused_return_value_section_fails_and_captures_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            result, capture, _ = self.run_imhead_section(Path(root), "fail")
            self.assertEqual(result["status"], "failed", result)
            self.assertIn("command exited 2", result["reason"])
            self.assertFalse(capture.exists())


if __name__ == "__main__":
    unittest.main()
