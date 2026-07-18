#!/usr/bin/env python3
"""Stage and compare CASA ACA/simalma simulation workflows.

This runner is the closeout harness for the simulation breadth issues.  It is
strict by design: CASA is the oracle, native gaps are reported as blockers, and
performance claims are emitted only with the inputs and outputs that produced
them.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import statistics
import sys
import time
from typing import Any

import bench_simobserve
import perf_paths
from perf_harness import (
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    validate_run_result,
)
from perf_harness.casa_protocol import run_json_file_protocol
from perf_harness.ms_compare import compare_measurement_set_pairs
from perf_harness.provenance import capture_provenance
from perf_harness.subprocesses import run_command


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "simobserve"
DEFAULT_IMAGER_BINARY = REPO_ROOT / "target" / "release" / "casars-imager"
DEFAULT_OUTPUT_DIR = perf_paths.artifact_path("simulation-breadth", "aca-simalma")
CASA_SCENARIO_PROGRAM = (
    pathlib.Path(__file__).resolve().parent / "perf_harness" / "casa_aca_scenarios.py"
)
DEFAULT_CASA_PYTHON = os.environ.get(
    "CASA_RS_CASA_PYTHON",
    "/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python",
)
TARGET_NATIVE_MB_PER_SECOND = 500.0
ACA_REFERENCE_FREQUENCY_HZ = 330.076e9
ACA_REFERENCE_CHANNEL_WIDTH_HZ = 50.0e6
ACA_MODEL_PEAK_JY_PER_PIXEL = 0.004
ACA_MODEL_REFERENCE_DIRECTION_RAD = [-2.90888209e-06, -0.610862814]
ACA_MODEL_CELL_SIZE_RAD = [4.84813681e-07, 4.84813681e-07]
# CASA's ACA tutorial uses the image-table model path, while the native harness
# reuses the FITS sampled model. This center is the CASA-parity sampled-model
# registration that keeps 12m, 7m, and TP DATA comparisons aligned.
ACA_TUTORIAL_MODEL_REFERENCE_DIRECTION_RAD = [0.011161128641967588, -0.6060040729347653]
ACA_ALMA_12M_PHASE_CENTER_RAD = [0.011041545398498194, -0.6058904359160557]
ACA_7M_PHASE_CENTER_RAD = [0.011072507978077432, -0.6059093304530385]
ACA_TOTAL_POWER_PHASE_CENTER_RAD = [0.011028274098247087, -0.6058947149437073]

MS_COMPARISON_PAIRS: dict[str, list[dict[str, str]]] = {
    "aca": [
        {
            "id": "alma_12m_interferometric_ms",
            "native_run": "aca-alma-12m-interferometric",
            "casa_ms": "m51c/m51c.alma_0.5arcsec.ms",
        },
        {
            "id": "aca_7m_interferometric_ms",
            "native_run": "aca-7m-interferometric",
            "casa_ms": "m51c/m51c.aca.i.ms",
        },
        {
            "id": "total_power_ms",
            "native_run": "aca-total-power",
            "casa_ms": "m51c/m51c.aca.tp.sd.ms",
        },
    ],
    "simalma": [
        {
            "id": "alma_12m_interferometric_ms",
            "native_run": "simalma-alma-12m",
            "casa_ms": "m51/m51.alma.cycle6.3.ms",
        },
        {
            "id": "aca_7m_interferometric_ms",
            "native_run": "simalma-aca-7m",
            "casa_ms": "m51/m51.aca.cycle6.ms",
        },
        {
            "id": "total_power_ms",
            "native_run": "simalma-aca-total-power-ant0",
            "casa_ms": "m51/m51.aca.tp.sd.ms.Ant0",
        },
        {
            "id": "total_power_ms_ant1",
            "native_run": "simalma-aca-total-power-ant1",
            "casa_ms": "m51/m51.aca.tp.sd.ms.Ant1",
        },
    ],
}


class AcaSimalmaError(Exception):
    """Error that should be shown without a Python traceback."""


SCENARIOS: dict[str, dict[str, Any]] = {
    "simalma": {
        "issue": 181,
        "source": "CASA regression test_regression_simalma_12m_ACA_combination.py",
        "tutorial": "simulation/simalma",
        "inputs": {
            "m51ha_fits": {
                "path": "fits/M51ha.fits",
                "kind": "file",
                "fallback_paths": ["regression/simalma_12m_ACA_combination/M51ha.fits"],
            },
        },
        "configs": ["alma.cycle6.3.cfg", "aca.cycle6.cfg", "aca.tp.cfg"],
        "casa_outputs": [
            "m51.alma.cycle6.3.ms",
            "m51.aca.cycle6.ms",
            "m51.aca.tp.sd.ms",
            "m51.concat.ms",
            "m51.concat.image",
            "m51.feather.image",
        ],
        "native_steps": [
            {
                "id": "alma_12m_interferometric_ms",
                "status": "implemented",
                "detail": "native family run using alma.cycle6.3.cfg",
            },
            {
                "id": "aca_7m_interferometric_ms",
                "status": "implemented",
                "detail": "native family run using aca.cycle6.cfg",
            },
            {
                "id": "total_power_ms",
                "status": "implemented",
                "detail": "native total_power family run using aca.tp.cfg autocorrelation DATA rows",
            },
            {
                "id": "concat_ms",
                "status": "covered",
                "detail": "native combined imaging uses direct multi-MS mosaic input instead of a materialized concat.ms",
            },
            {
                "id": "combined_image_products",
                "status": "implemented",
                "detail": "native casars-imager direct multi-MS mosaic image products",
            },
        ],
    },
    "aca": {
        "issue": 182,
        "source": "CASA regression test_regression_sim_multi_arrays_and_TP.py",
        "tutorial": "simulation/aca",
        "inputs": {
            "m51ha_model": {
                "path": "image/m51ha.model",
                "kind": "dir",
                "fallback_paths": ["regression/sim_multi_arrays_and_TP/m51ha.model"],
            },
            "m51ha_fits": {
                "path": "fits/M51ha.fits",
                "kind": "file",
                "fallback_paths": ["regression/simalma_12m_ACA_combination/M51ha.fits"],
            },
            "m51c_reference": {
                "path": "regression/sim_multi_arrays_and_TP/m51c_reference",
                "kind": "dir",
                "optional": True,
            },
        },
        "configs": ["alma.out07.cfg", "aca.i.cfg", "aca.tp.cfg"],
        "casa_outputs": [
            "m51c.alma_0.5arcsec.ms",
            "m51c.aca.i.ms",
            "m51c.aca.tp.sd.ms",
            "m51c.sd.image",
            "m51c.aca.i.image",
            "m51c.alma_0.5arcsec.image",
            "m51c.alma_0.5arcsec.diff",
        ],
        "native_steps": [
            {
                "id": "aca_7m_interferometric_ms",
                "status": "implemented",
                "detail": "native family run using aca.i.cfg with FITS sampled model",
            },
            {
                "id": "alma_12m_interferometric_ms",
                "status": "implemented",
                "detail": "native family run using CASA's alma;0.5arcsec selection resolved to alma.out07.cfg at 330.076 GHz",
            },
            {
                "id": "total_power_ms",
                "status": "implemented",
                "detail": "native total_power family run using aca.tp.cfg autocorrelation DATA rows",
            },
            {
                "id": "simanalyze_image_products",
                "status": "implemented",
                "detail": "native casars-imager 12m/7m image products plus TP sampled-product diagnostic",
            },
        ],
    },
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=["aca", "simalma", "both"],
        default="both",
        help="tutorial/regression scenario to stage and assess",
    )
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--testdata-root", type=pathlib.Path, default=None)
    parser.add_argument("--config-root", type=pathlib.Path, default=None)
    parser.add_argument("--casars-binary", type=pathlib.Path, default=DEFAULT_BINARY)
    parser.add_argument(
        "--casars-imager-binary",
        type=pathlib.Path,
        default=DEFAULT_IMAGER_BINARY,
        help="casars-imager binary used for native image-product closeout checks",
    )
    parser.add_argument("--casa-python", default=DEFAULT_CASA_PYTHON)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--skip-casa", action="store_true")
    parser.add_argument("--skip-native", action="store_true")
    parser.add_argument(
        "--run-casa",
        action="store_true",
        help="run the CASA oracle workflows instead of only staging them",
    )
    parser.add_argument(
        "--run-native",
        action="store_true",
        help="run native interferometric slices that are currently implemented",
    )
    parser.add_argument("--native-target-gib", type=float, default=0.01)
    parser.add_argument("--native-repeats", type=int, default=1)
    parser.add_argument("--row-workers", type=int, default=None)
    parser.add_argument("--channel-workers", type=int, default=None)
    parser.add_argument(
        "--require-native-throughput-mb-s",
        type=float,
        default=TARGET_NATIVE_MB_PER_SECOND,
    )
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="exit 0 even when the closeout gate reports blockers",
    )
    args = parser.parse_args()

    try:
        result = run_assessment(args)
        result_path = pathlib.Path(result["artifacts"]["result_json"])
        atomic_write_json(result_path, result)
        print(result_path)
        status = result["results"]["aca_simalma"]["closeout_gate"]["status"]
        if status != "passed" and not args.allow_incomplete:
            raise SystemExit(2)
    except AcaSimalmaError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def run_assessment(args: argparse.Namespace) -> dict[str, Any]:
    selected = selected_scenarios(args.scenario)
    testdata_root = resolve_testdata_root(args.testdata_root)
    config_root = resolve_config_root(args.config_root)
    output_dir = args.output_dir.resolve()
    perf_paths.mark_safe_to_delete(perf_paths.default_artifact_root())
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + "-".join(selected)
    run_root = output_dir / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    preflight = build_preflight(selected, testdata_root, config_root)
    staged = stage_all_inputs(preflight, run_root / "staged")
    casa = run_casa_section(args, selected, staged, run_root)
    casa_field_overrides = collect_casa_field_center_overrides(
        args.casa_python, selected, casa
    )
    native = run_native_section(
        args, selected, staged, run_root, casa_field_overrides=casa_field_overrides
    )
    comparisons = build_comparison_summary(selected, casa, native, args.casa_python)
    closeout_gate = evaluate_closeout_gate(
        selected,
        preflight,
        casa,
        native,
        comparisons,
        required_native_throughput_mb_s=args.require_native_throughput_mb_s,
    )

    result_path = run_root / "aca-simalma-benchmark.json"
    details = {
        "selected_scenarios": selected,
        "targets": {
            "native_floor_mb_per_second": TARGET_NATIVE_MB_PER_SECOND,
            "required_native_throughput_mb_s": args.require_native_throughput_mb_s,
        },
        "inputs": {
            "testdata_root": str(testdata_root) if testdata_root is not None else None,
            "config_root": str(config_root) if config_root is not None else None,
            "preflight": preflight,
            "staged": staged,
        },
        "casa": casa,
        "casa_field_overrides": casa_field_overrides,
        "native": native,
        "comparisons": comparisons,
        "closeout_gate": closeout_gate,
    }
    status = aca_evidence_status(closeout_gate, comparisons)
    results: dict[str, Any] = {"aca_simalma": details}
    if status != "completed":
        results["failure"] = {
            "kind": (
                "comparison_tolerance"
                if status == "out_of_tolerance"
                else "comparison"
                if status == "failed_comparison"
                else "evidence_unavailable"
            ),
            "reason": "; ".join(
                str(blocker.get("reason", "blocked"))
                for blocker in closeout_gate["blockers"]
            ),
        }
    result = {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "aca_simalma_benchmark",
        "status": status,
        "run_id": run_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "environment": capture_provenance(
            repo_root=REPO_ROOT,
            executables={
                "native_simobserve": args.casars_binary,
                "native_imager": args.casars_imager_binary,
                "casa_python": args.casa_python,
            },
            datasets={
                "testdata_root": testdata_root,
                "config_root": config_root,
            },
            storage_label="aca-simalma-benchmark",
        ),
        "artifacts": {
            "result_json": str(result_path),
            "run_root": str(run_root),
        },
        "results": results,
    }
    validate_run_result(result, source=str(result_path))
    return result


def aca_evidence_status(
    closeout_gate: dict[str, Any], comparisons: dict[str, Any]
) -> str:
    if closeout_gate["status"] == "passed":
        return "completed"
    if any(
        blocker.get("reason") == "native throughput below floor"
        for blocker in closeout_gate["blockers"]
    ):
        return "out_of_tolerance"
    comparison_statuses = {
        value.get("status") for value in comparisons.values() if isinstance(value, dict)
    }
    if comparison_statuses & {"failed", "failed_execution", "failed_comparison"}:
        return "failed_comparison"
    return "unavailable"


def selected_scenarios(value: str) -> list[str]:
    if value == "both":
        return ["simalma", "aca"]
    return [value]


def resolve_testdata_root(configured: pathlib.Path | None) -> pathlib.Path | None:
    candidates = []
    if configured is not None:
        candidates.append(configured)
    env = os.environ.get("CASA_RS_TESTDATA_ROOT")
    if env:
        candidates.append(pathlib.Path(env))
    candidates.extend(
        [
            REPO_ROOT.parent / "casatestdata",
            pathlib.Path.home() / "SoftwareProjects" / "casatestdata",
        ]
    )
    for candidate in candidates:
        root = candidate.expanduser()
        if root.exists():
            return root
    return None


def resolve_config_root(configured: pathlib.Path | None) -> pathlib.Path | None:
    candidates = []
    if configured is not None:
        candidates.append(configured)
    env = os.environ.get("CASA_RS_SIMOBSERVE_CONFIG_ROOT")
    if env:
        candidates.append(pathlib.Path(env))
    casadata = os.environ.get("CASADATA")
    if casadata:
        candidates.append(pathlib.Path(casadata) / "alma" / "simmos")
    casapath = os.environ.get("CASAPATH")
    if casapath:
        first = casapath.split()[0]
        candidates.append(pathlib.Path(first) / "data" / "alma" / "simmos")
    candidates.append(pathlib.Path.home() / ".casa" / "data" / "alma" / "simmos")
    for candidate in candidates:
        root = candidate.expanduser()
        if root.exists():
            return root
    return None


def build_preflight(
    scenarios: list[str],
    testdata_root: pathlib.Path | None,
    config_root: pathlib.Path | None,
) -> dict[str, Any]:
    return {
        scenario: build_scenario_preflight(scenario, testdata_root, config_root)
        for scenario in scenarios
    }


def build_scenario_preflight(
    scenario: str,
    testdata_root: pathlib.Path | None,
    config_root: pathlib.Path | None,
) -> dict[str, Any]:
    contract = SCENARIOS[scenario]
    inputs = {
        name: resolve_input_spec(testdata_root, spec)
        for name, spec in contract["inputs"].items()
    }
    configs = {
        name: resolve_config_spec(config_root, name) for name in contract["configs"]
    }
    missing_required = [
        f"input:{name}"
        for name, status in inputs.items()
        if status["status"] != "available" and not status["optional"]
    ]
    missing_required.extend(
        f"config:{name}" for name, status in configs.items() if status["status"] != "available"
    )
    return {
        "issue": contract["issue"],
        "tutorial": contract["tutorial"],
        "source": contract["source"],
        "status": "available" if not missing_required else "missing",
        "missing_required": missing_required,
        "inputs": inputs,
        "configs": configs,
        "native_steps": contract["native_steps"],
        "casa_outputs": contract["casa_outputs"],
    }


def resolve_input_spec(
    testdata_root: pathlib.Path | None,
    spec: dict[str, Any],
) -> dict[str, Any]:
    optional = bool(spec.get("optional", False))
    candidates = [spec["path"], *spec.get("fallback_paths", [])]
    if testdata_root is None:
        return {
            "status": "missing",
            "optional": optional,
            "reason": "no casatestdata root found",
            "candidates": candidates,
            "path": None,
            "kind": spec["kind"],
        }
    for relative in candidates:
        path = testdata_root / relative
        if path_matches_kind(path, spec["kind"]):
            return {
                "status": "available",
                "optional": optional,
                "path": str(path),
                "kind": spec["kind"],
                "selected_relative_path": relative,
            }
    return {
        "status": "missing",
        "optional": optional,
        "reason": "none of the candidate paths exist with the expected kind",
        "candidates": [str(testdata_root / relative) for relative in candidates],
        "path": None,
        "kind": spec["kind"],
    }


def resolve_config_spec(
    config_root: pathlib.Path | None,
    name: str,
) -> dict[str, Any]:
    if config_root is None:
        return {
            "status": "missing",
            "reason": "no CASA simmos config root found",
            "path": None,
        }
    path = config_root / name
    if path.is_file():
        return {"status": "available", "path": str(path)}
    return {
        "status": "missing",
        "reason": "config file not found",
        "path": str(path),
    }


def path_matches_kind(path: pathlib.Path, kind: str) -> bool:
    if kind == "file":
        return path.is_file()
    if kind == "dir":
        return path.is_dir()
    raise AcaSimalmaError(f"unknown staged input kind {kind!r}")


def stage_all_inputs(preflight: dict[str, Any], stage_root: pathlib.Path) -> dict[str, Any]:
    if stage_root.exists():
        shutil.rmtree(stage_root)
    stage_root.mkdir(parents=True)
    staged: dict[str, Any] = {}
    for scenario, scenario_preflight in preflight.items():
        scenario_stage = stage_root / scenario
        scenario_stage.mkdir(parents=True)
        staged[scenario] = stage_scenario_inputs(scenario_preflight, scenario_stage)
    return staged


def stage_scenario_inputs(
    scenario_preflight: dict[str, Any],
    scenario_stage: pathlib.Path,
) -> dict[str, Any]:
    staged_inputs = {}
    input_root = scenario_stage / "inputs"
    config_stage_root = scenario_stage / "configs"
    input_root.mkdir()
    config_stage_root.mkdir()
    for name, status in scenario_preflight["inputs"].items():
        if status["status"] != "available":
            staged_inputs[name] = {"status": "missing", "source": status.get("path")}
            continue
        source = pathlib.Path(status["path"])
        target = input_root / name
        link_or_copy(source, target, status["kind"])
        staged_inputs[name] = {
            "status": "staged",
            "source": str(source),
            "path": str(target),
            "kind": status["kind"],
        }
    staged_configs = {}
    for name, status in scenario_preflight["configs"].items():
        if status["status"] != "available":
            staged_configs[name] = {"status": "missing", "source": status.get("path")}
            continue
        source = pathlib.Path(status["path"])
        target = config_stage_root / name
        link_or_copy(source, target, "file")
        staged_configs[name] = {
            "status": "staged",
            "source": str(source),
            "path": str(target),
            "kind": "file",
        }
    return {
        "stage_root": str(scenario_stage),
        "inputs": staged_inputs,
        "configs": staged_configs,
    }


def link_or_copy(source: pathlib.Path, target: pathlib.Path, kind: str) -> None:
    if target.exists() or target.is_symlink():
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        else:
            target.unlink()
    try:
        target.symlink_to(source, target_is_directory=(kind == "dir"))
        return
    except OSError:
        pass
    if kind == "dir":
        shutil.copytree(source, target)
    else:
        shutil.copy2(source, target)


def run_casa_section(
    args: argparse.Namespace,
    scenarios: list[str],
    staged: dict[str, Any],
    run_root: pathlib.Path,
) -> dict[str, Any]:
    if args.skip_casa:
        return {"status": "skipped", "reason": "--skip-casa"}
    casa_python = pathlib.Path(args.casa_python)
    if not casa_python.exists():
        return {
            "status": "skipped",
            "reason": f"CASA Python does not exist: {casa_python}",
        }
    if args.preflight_only or not args.run_casa:
        return {
            "status": "not_run",
            "reason": "CASA oracle staged but --run-casa was not requested",
            "casa_python": str(casa_python),
            "programs": {
                scenario: {
                    "script": str(CASA_SCENARIO_PROGRAM),
                    "status": "checked_in",
                }
                for scenario in scenarios
            },
        }
    results = {}
    for scenario in scenarios:
        results[scenario] = run_casa_scenario(
            str(casa_python),
            scenario,
            staged[scenario],
            run_root / "casa" / scenario,
        )
    status = "passed" if all(value["status"] == "run" for value in results.values()) else "failed"
    return {"status": status, "casa_python": str(casa_python), "scenarios": results}


def run_casa_scenario(
    casa_python: str,
    scenario: str,
    staged: dict[str, Any],
    output_dir: pathlib.Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    environment = os.environ.copy()
    environment.setdefault("QT_QPA_PLATFORM", "offscreen")
    environment.setdefault("MPLBACKEND", "Agg")
    environment.setdefault("DISPLAY", ":99")
    environment.setdefault("MPLCONFIGDIR", str(output_dir / "matplotlib"))
    pathlib.Path(environment["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_SCENARIO_PROGRAM,
        request={"schema_version": 1, "scenario": scenario, "staged": staged},
        request_path=output_dir / "scenario.request.json",
        output_path=output_dir / "scenario.result.json",
        log_path=output_dir / "scenario.log",
        cwd=output_dir,
        environment=environment,
    )
    elapsed = time.perf_counter() - started
    if protocol.status != "completed" or protocol.output is None:
        return {
            "status": protocol.status,
            "elapsed_seconds": elapsed,
            "script": str(CASA_SCENARIO_PROGRAM),
            "reason": protocol.reason,
            "log": str(protocol.log_path),
        }
    size_bytes = directory_size_or_zero(output_dir)
    return {
        "status": "run",
        "elapsed_seconds": elapsed,
        "script": str(CASA_SCENARIO_PROGRAM),
        "output_dir": str(output_dir),
        "size_bytes": size_bytes,
        "mb_per_second": bench_simobserve.mb_per_second(size_bytes, elapsed),
        "payload": protocol.output,
        "log": str(protocol.log_path),
    }


def collect_casa_field_center_overrides(
    casa_python: str,
    scenarios: list[str],
    casa: dict[str, Any],
) -> dict[str, dict[str, list[list[float]]]]:
    if casa.get("status") not in {"passed", "failed"}:
        return {}
    overrides = {}
    for scenario in scenarios:
        casa_run = casa.get("scenarios", {}).get(scenario)
        if casa_run is None or casa_run.get("status") != "run":
            continue
        scenario_overrides = collect_scenario_casa_field_centers(
            casa_python, scenario, pathlib.Path(casa_run["output_dir"])
        )
        if scenario_overrides:
            overrides[scenario] = scenario_overrides
    return overrides


def collect_scenario_casa_field_centers(
    casa_python: str,
    scenario: str,
    casa_output_dir: pathlib.Path,
) -> dict[str, list[list[float]]]:
    specs = []
    for spec in MS_COMPARISON_PAIRS[scenario]:
        casa_ms = casa_output_dir / spec["casa_ms"]
        if casa_ms.exists():
            specs.append({"native_run": spec["native_run"], "casa_ms": str(casa_ms)})
    if not specs:
        return {}
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=bench_simobserve.CASA_MS_TOOLS,
        request={"operation": "field_centers", "specs": specs},
        request_path=casa_output_dir / "field-centers.request.json",
        output_path=casa_output_dir / "field-centers.result.json",
        log_path=casa_output_dir / "field-centers.log",
        cwd=REPO_ROOT,
    )
    if protocol.status != "completed" or protocol.output is None:
        raise AcaSimalmaError(
            f"failed to export CASA FIELD centers: {protocol.status}: {protocol.reason}"
        )
    return {
        str(native_run): centers
        for native_run, centers in protocol.output.items()
        if isinstance(centers, list)
    }


def run_native_section(
    args: argparse.Namespace,
    scenarios: list[str],
    staged: dict[str, Any],
    run_root: pathlib.Path,
    *,
    casa_field_overrides: dict[str, dict[str, list[list[float]]]] | None = None,
) -> dict[str, Any]:
    casa_field_overrides = casa_field_overrides or {}
    if args.skip_native:
        return {"status": "skipped", "reason": "--skip-native"}
    if args.preflight_only or not args.run_native:
        return {
            "status": "not_run",
            "reason": "native implemented slices staged but --run-native was not requested",
            "plans": {
                scenario: native_family_plans(
                    scenario,
                    staged[scenario],
                    run_root / "native" / scenario,
                    target_gib=args.native_target_gib,
                    row_workers=args.row_workers,
                    channel_workers=args.channel_workers,
                    field_overrides=casa_field_overrides.get(scenario, {}),
                )
                for scenario in scenarios
            },
        }
    if not args.casars_binary.exists():
        raise AcaSimalmaError(f"native simobserve binary does not exist: {args.casars_binary}")
    results = {}
    for scenario in scenarios:
        results[scenario] = run_native_scenario(
            args.casars_binary,
            args.casars_imager_binary,
            scenario,
            staged[scenario],
            run_root / "native" / scenario,
            target_gib=args.native_target_gib,
            repeats=args.native_repeats,
            row_workers=args.row_workers,
            channel_workers=args.channel_workers,
            field_overrides=casa_field_overrides.get(scenario, {}),
        )
    status = "passed" if all(value["status"] == "run" for value in results.values()) else "failed"
    return {"status": status, "scenarios": results}


def run_native_scenario(
    binary: pathlib.Path,
    imager_binary: pathlib.Path,
    scenario: str,
    staged: dict[str, Any],
    output_dir: pathlib.Path,
    *,
    target_gib: float,
    repeats: int,
    row_workers: int | None,
    channel_workers: int | None,
    field_overrides: dict[str, list[list[float]]] | None = None,
) -> dict[str, Any]:
    plans = native_family_plans(
        scenario,
        staged,
        output_dir,
        target_gib=target_gib,
        row_workers=row_workers,
        channel_workers=channel_workers,
        field_overrides=field_overrides or {},
    )
    runs = []
    for plan in plans:
        if plan["status"] != "planned":
            runs.append(plan)
            continue
        runs.append(run_native_family_plan(binary, plan, repeats=repeats))
    image_products = run_native_image_products(imager_binary, scenario, output_dir, runs)
    status = (
        "run"
        if all(run.get("status") == "run" for run in runs)
        and image_products.get("status") == "run"
        else "partial"
    )
    return {"status": status, "runs": runs, "image_products": image_products}


def native_family_plans(
    scenario: str,
    staged: dict[str, Any],
    output_dir: pathlib.Path,
    *,
    target_gib: float,
    row_workers: int | None,
    channel_workers: int | None,
    field_overrides: dict[str, list[list[float]]] | None = None,
) -> list[dict[str, Any]]:
    field_overrides = field_overrides or {}
    if scenario == "simalma":
        return [
            native_family_plan(
                "simalma-alma-12m",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="alma.cycle6.3.cfg",
                telescope="ALMA",
                imaging_mode="mosaic",
                observation_mode="interferometric",
                target_gib=target_gib,
                pointing_count=52,
                polarizations=2,
                phase_center_rad=ACA_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=180,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_895_242_500.068084,
                field_phase_centers_rad=field_overrides.get("simalma-alma-12m"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
            native_family_plan(
                "simalma-aca-7m",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="aca.cycle6.cfg",
                telescope="ACA",
                imaging_mode="mosaic",
                observation_mode="interferometric",
                target_gib=target_gib,
                pointing_count=17,
                polarizations=2,
                phase_center_rad=ACA_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=360,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_895_241_600.108311,
                field_phase_centers_rad=field_overrides.get("simalma-aca-7m"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
            native_family_plan(
                "simalma-aca-total-power-ant0",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="aca.tp.cfg",
                telescope="ACA",
                imaging_mode="mosaic",
                observation_mode="total_power",
                target_gib=target_gib,
                pointing_count=225,
                polarizations=2,
                phase_center_rad=ACA_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=720,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_895_239_799.479176,
                total_power_antenna_index=0,
                field_phase_centers_rad=field_overrides.get("simalma-aca-total-power-ant0"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
            native_family_plan(
                "simalma-aca-total-power-ant1",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="aca.tp.cfg",
                telescope="ACA",
                imaging_mode="mosaic",
                observation_mode="total_power",
                target_gib=target_gib,
                pointing_count=225,
                polarizations=2,
                phase_center_rad=ACA_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=720,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_895_239_799.466283,
                total_power_antenna_index=1,
                field_phase_centers_rad=field_overrides.get("simalma-aca-total-power-ant1"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
        ]
    if scenario == "aca":
        return [
            native_family_plan(
                "aca-alma-12m-interferometric",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="alma.out07.cfg",
                telescope="ALMA",
                imaging_mode="mosaic",
                observation_mode="interferometric",
                target_gib=target_gib,
                pointing_count=42,
                polarizations=2,
                phase_center_rad=ACA_ALMA_12M_PHASE_CENTER_RAD,
                model_reference_rad=ACA_TUTORIAL_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=360,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_860_172_992.781919,
                field_phase_centers_rad=field_overrides.get("aca-alma-12m-interferometric"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
            native_family_plan(
                "aca-7m-interferometric",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="aca.i.cfg",
                telescope="ACA",
                imaging_mode="mosaic",
                observation_mode="interferometric",
                target_gib=target_gib,
                pointing_count=14,
                polarizations=2,
                phase_center_rad=ACA_7M_PHASE_CENTER_RAD,
                model_reference_rad=ACA_TUTORIAL_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=42,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_860_174_583.225414,
                field_phase_centers_rad=field_overrides.get("aca-7m-interferometric"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
            native_family_plan(
                "aca-total-power",
                staged,
                output_dir,
                model_key="m51ha_fits",
                config_name="aca.tp.cfg",
                telescope="ACA",
                imaging_mode="mosaic",
                observation_mode="total_power",
                target_gib=target_gib,
                pointing_count=36,
                polarizations=2,
                phase_center_rad=ACA_TOTAL_POWER_PHASE_CENTER_RAD,
                model_reference_rad=ACA_TUTORIAL_MODEL_REFERENCE_DIRECTION_RAD,
                start_frequency_hz=ACA_REFERENCE_FREQUENCY_HZ,
                channel_width_hz=ACA_REFERENCE_CHANNEL_WIDTH_HZ,
                time_sample_count=720,
                integration_seconds=10.0,
                start_time_mjd_seconds=4_860_171_192.7219305,
                field_phase_centers_rad=field_overrides.get("aca-total-power"),
                row_workers=row_workers,
                channel_workers=channel_workers,
            ),
        ]
    raise AcaSimalmaError(f"unknown scenario {scenario!r}")


def native_family_plan(
    name: str,
    staged: dict[str, Any],
    output_dir: pathlib.Path,
    *,
    model_key: str,
    config_name: str,
    telescope: str,
    imaging_mode: str,
    observation_mode: str,
    target_gib: float,
    pointing_count: int,
    polarizations: int,
    phase_center_rad: list[float] | None = None,
    model_reference_rad: list[float] | None = None,
    field_phase_centers_rad: list[list[float]] | None = None,
    start_frequency_hz: float | None = None,
    channel_width_hz: float | None = None,
    total_power_antenna_index: int | None = None,
    time_sample_count: int | None = None,
    integration_seconds: float | None = None,
    start_time_mjd_seconds: float | None = None,
    row_workers: int | None = None,
    channel_workers: int | None = None,
) -> dict[str, Any]:
    model = staged["inputs"].get(model_key, {})
    config = staged["configs"].get(config_name, {})
    if model.get("status") != "staged":
        return {"status": "missing", "name": name, "reason": f"missing model {model_key}"}
    if config.get("status") != "staged":
        return {"status": "missing", "name": name, "reason": f"missing config {config_name}"}
    output_dir.mkdir(parents=True, exist_ok=True)
    output_ms = output_dir / f"{name}.ms"
    request = {
        "kind": "family",
        "request": {
            "source_model": {
                "kind": "fits_image",
                "path": model["path"],
                "model_peak_jy_per_pixel": ACA_MODEL_PEAK_JY_PER_PIXEL,
                "direction_reference_rad": model_reference_rad
                or phase_center_rad
                or ACA_MODEL_REFERENCE_DIRECTION_RAD,
                "cell_size_rad": ACA_MODEL_CELL_SIZE_RAD,
            },
            "telescope": telescope,
            "array_config": config["path"],
            "band": "band7",
            "target_ms_size_gib": target_gib,
            "polarizations": polarizations,
            "ms_channels": 1,
            "image_channels": 1,
            "pointing_count": pointing_count,
            "field_phase_centers_rad": field_phase_centers_rad,
            "phase_center_rad": phase_center_rad,
            "start_frequency_hz": start_frequency_hz,
            "channel_width_hz": channel_width_hz,
            "time_sample_count": time_sample_count,
            "integration_seconds": integration_seconds,
            "start_time_mjd_seconds": start_time_mjd_seconds,
            "imaging_mode": imaging_mode,
            "observation_mode": observation_mode,
            "total_power_antenna_index": total_power_antenna_index,
            "output_ms": str(output_ms),
            "measure_actual_size": True,
            "worker_policy": "auto",
            "row_workers": row_workers,
            "channel_workers": channel_workers,
        },
    }
    return {
        "status": "planned",
        "name": name,
        "request": request,
        "request_path": str(output_dir / f"{name}.request.json"),
        "output_ms": str(output_ms),
    }


def run_native_family_plan(
    binary: pathlib.Path,
    plan: dict[str, Any],
    *,
    repeats: int,
) -> dict[str, Any]:
    timings = []
    last_payload = None
    request_path = pathlib.Path(plan["request_path"])
    output_ms = pathlib.Path(plan["output_ms"])
    for repeat in range(repeats):
        request = json.loads(json.dumps(plan["request"]))
        if repeats > 1:
            output_ms = pathlib.Path(plan["output_ms"]).with_name(
                pathlib.Path(plan["output_ms"]).stem + f"-run-{repeat + 1:02d}.ms"
            )
            request["request"]["output_ms"] = str(output_ms)
        atomic_write_json(request_path, request)
        started = time.perf_counter()
        completed = run_command(
            [str(binary), "--json-run", str(request_path)],
            cwd=REPO_ROOT,
            merge_stderr=False,
        )
        elapsed = time.perf_counter() - started
        stdout_path = request_path.with_suffix(f".run-{repeat + 1:02d}.stdout.json")
        stderr_path = request_path.with_suffix(f".run-{repeat + 1:02d}.stderr.log")
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")
        if completed.returncode != 0:
            return {
                **plan,
                "status": "failed",
                "elapsed_seconds": elapsed,
                "stderr_tail": completed.stderr[-4000:],
            }
        payload = bench_simobserve.parse_json_from_stdout(
            completed.stdout, f"native family {plan['name']}"
        )
        timings.append(elapsed)
        last_payload = payload
    assert last_payload is not None
    size_bytes = directory_size_or_zero(output_ms)
    best_seconds = min(timings)
    return {
        **plan,
        "status": "run",
        "timings_seconds": timings,
        "best_seconds": best_seconds,
        "median_seconds": statistics.median(timings),
        "size_bytes": size_bytes,
        "mb_per_second": bench_simobserve.mb_per_second(size_bytes, best_seconds),
        "payload": last_payload,
    }


def run_native_image_products(
    imager_binary: pathlib.Path,
    scenario: str,
    output_dir: pathlib.Path,
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    run_by_name = {
        run.get("name"): run for run in runs if run.get("status") == "run"
    }
    specs = native_image_product_specs(scenario)
    missing = [
        f"native:{run_name}"
        for spec in specs
        for run_name in spec["run_names"]
        if run_name not in run_by_name
    ]
    if missing:
        return {
            "status": "failed",
            "reason": "missing native MS runs for image products",
            "missing": sorted(set(missing)),
            "products": [],
        }
    if any(spec["kind"] == "imager" for spec in specs) and not imager_binary.exists():
        return {
            "status": "failed",
            "reason": f"native casars-imager binary does not exist: {imager_binary}",
            "products": [],
        }
    products = []
    image_dir = output_dir / "image-products"
    image_dir.mkdir(parents=True, exist_ok=True)
    for spec in specs:
        if spec["kind"] == "imager":
            products.append(
                run_native_imager_product(imager_binary, image_dir, spec, run_by_name)
            )
        elif spec["kind"] == "total_power_sampled":
            products.append(write_total_power_sampled_product(image_dir, spec, run_by_name))
        else:
            products.append(
                {
                    "status": "failed",
                    "name": spec["name"],
                    "reason": f"unknown native image product kind {spec['kind']!r}",
                }
            )
    status = "run" if all(product.get("status") == "run" for product in products) else "failed"
    return {"status": status, "products": products}


def native_image_product_specs(scenario: str) -> list[dict[str, Any]]:
    if scenario == "simalma":
        return [
            {
                "kind": "imager",
                "name": "simalma-combined-mfs",
                "run_names": ["simalma-alma-12m", "simalma-aca-7m"],
                "imsize": 128,
                "cell_arcsec": 0.5,
                "phasecenter": phasecenter_literal(ACA_MODEL_REFERENCE_DIRECTION_RAD),
            },
            {
                "kind": "total_power_sampled",
                "name": "simalma-total-power-ant0-sampled",
                "run_names": ["simalma-aca-total-power-ant0"],
            },
            {
                "kind": "total_power_sampled",
                "name": "simalma-total-power-ant1-sampled",
                "run_names": ["simalma-aca-total-power-ant1"],
            },
        ]
    if scenario == "aca":
        return [
            {
                "kind": "imager",
                "name": "aca-7m-mfs",
                "run_names": ["aca-7m-interferometric"],
                "imsize": 512,
                "cell_arcsec": 0.2,
                "phasecenter": phasecenter_literal(ACA_7M_PHASE_CENTER_RAD),
            },
            {
                "kind": "imager",
                "name": "aca-alma-12m-mfs",
                "run_names": ["aca-alma-12m-interferometric"],
                "imsize": 512,
                "cell_arcsec": 0.2,
                "phasecenter": phasecenter_literal(ACA_ALMA_12M_PHASE_CENTER_RAD),
            },
            {
                "kind": "total_power_sampled",
                "name": "aca-total-power-sampled",
                "run_names": ["aca-total-power"],
            },
        ]
    raise AcaSimalmaError(f"unknown scenario {scenario!r}")


def run_native_imager_product(
    imager_binary: pathlib.Path,
    image_dir: pathlib.Path,
    spec: dict[str, Any],
    run_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    ms_paths = [run_by_name[name]["output_ms"] for name in spec["run_names"]]
    prefix = image_dir / spec["name"]
    command = [
        str(imager_binary),
        "--managed-output",
        "true",
        "--ms",
        ",".join(ms_paths),
        "--imagename",
        str(prefix),
        "--imsize",
        str(spec["imsize"]),
        "--cell-arcsec",
        str(spec["cell_arcsec"]),
        "--specmode",
        "mfs",
        "--weighting",
        "natural",
        "--deconvolver",
        "hogbom",
        "--niter",
        "0",
        "--dirty-only",
        "--write-pb",
        "--pblimit",
        "0.2",
        "--no-preview-pngs",
    ]
    if spec.get("phasecenter"):
        command.extend(["--phasecenter", spec["phasecenter"]])
    started = time.perf_counter()
    completed = run_command(
        command,
        cwd=REPO_ROOT,
        merge_stderr=False,
    )
    elapsed = time.perf_counter() - started
    stdout_path = image_dir / f"{spec['name']}.stdout.json"
    stderr_path = image_dir / f"{spec['name']}.stderr.log"
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        return {
            "status": "failed",
            "kind": "imager",
            "name": spec["name"],
            "elapsed_seconds": elapsed,
            "command": command,
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
        }
    payload = bench_simobserve.parse_json_from_stdout(
        completed.stdout, f"native image product {spec['name']}"
    )
    product_paths = sorted(
        str(path)
        for path in prefix.parent.glob(prefix.name + ".*")
        if path != stdout_path and path != stderr_path
    )
    size_bytes = sum(directory_size_or_zero(pathlib.Path(path)) for path in product_paths)
    return {
        "status": "run",
        "kind": "imager",
        "name": spec["name"],
        "elapsed_seconds": elapsed,
        "command": command,
        "ms_paths": ms_paths,
        "product_paths": product_paths,
        "size_bytes": size_bytes,
        "mb_per_second": bench_simobserve.mb_per_second(size_bytes, elapsed),
        "payload": payload,
    }


def write_total_power_sampled_product(
    image_dir: pathlib.Path,
    spec: dict[str, Any],
    run_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    run = run_by_name[spec["run_names"][0]]
    product_path = image_dir / f"{spec['name']}.json"
    payload = {
        "kind": "total_power_sampled_product",
        "name": spec["name"],
        "source_ms": run["output_ms"],
        "source_ms_size_bytes": run.get("size_bytes"),
        "source_ms_mb_per_second": run.get("mb_per_second"),
        "simobserve_payload": run.get("payload"),
    }
    atomic_write_json(product_path, payload)
    return {
        "status": "run",
        "kind": "total_power_sampled",
        "name": spec["name"],
        "product_path": str(product_path),
        "source_ms": run["output_ms"],
        "size_bytes": directory_size_or_zero(product_path),
    }


def phasecenter_literal(direction_rad: list[float]) -> str:
    return f"J2000 {direction_rad[0]:.17g}rad {direction_rad[1]:.17g}rad"


def build_comparison_summary(
    scenarios: list[str],
    casa: dict[str, Any],
    native: dict[str, Any],
    casa_python: str,
) -> dict[str, Any]:
    summary = {}
    for scenario in scenarios:
        native_runs = (
            native.get("scenarios", {}).get(scenario, {}).get("runs", [])
            if native.get("status") in {"passed", "failed"}
            else []
        )
        casa_run = (
            casa.get("scenarios", {}).get(scenario)
            if casa.get("status") in {"passed", "failed"}
            else None
        )
        casa_available = casa_run is not None and casa_run.get("status") == "run"
        native_by_name = {
            run.get("name"): run for run in native_runs if run.get("status") == "run"
        }
        native_run_count = len(native_by_name)
        if casa_available and native_run_count:
            comparison = collect_ms_comparisons(
                casa_python,
                scenario,
                pathlib.Path(casa_run["output_dir"]),
                native_by_name,
            )
            comparison["casa_available"] = True
            comparison["native_run_count"] = native_run_count
            comparison["native_unsupported_steps"] = unsupported_native_steps(scenario)
            summary[scenario] = comparison
        else:
            summary[scenario] = {
                "status": "not_available",
                "reason": "same-input CASA/native end-to-end comparison has not run",
                "casa_available": casa_available,
                "native_run_count": native_run_count,
                "native_unsupported_steps": unsupported_native_steps(scenario),
            }
    return summary


def collect_ms_comparisons(
    casa_python: str,
    scenario: str,
    casa_output_dir: pathlib.Path,
    native_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    pairs = []
    missing = []
    for spec in MS_COMPARISON_PAIRS[scenario]:
        native_run = native_by_name.get(spec["native_run"])
        if native_run is None:
            missing.append(f"native:{spec['native_run']}")
            continue
        casa_ms = casa_output_dir / spec["casa_ms"]
        if not casa_ms.exists():
            missing.append(f"casa:{spec['casa_ms']}")
            continue
        pairs.append(
            {
                "id": spec["id"],
                "native_ms": native_run["output_ms"],
                "casa_ms": str(casa_ms),
            }
        )
    if missing:
        return {
            "status": "failed",
            "reason": "missing MS outputs for CASA/native comparison",
            "missing": missing,
            "pairs": pairs,
        }
    if not pairs:
        return {
            "status": "not_available",
            "reason": "no CASA/native MS pairs were available for comparison",
        }
    return run_ms_comparison_script(casa_python, pairs)


def run_ms_comparison_script(
    casa_python: str, pairs: list[dict[str, str]]
) -> dict[str, Any]:
    artifact_prefix = (
        pathlib.Path(pairs[0]["native_ms"]).parent.parent
        / "ms-comparison"
        / "aca-pairs"
    )
    artifact_prefix.parent.mkdir(parents=True, exist_ok=True)
    return compare_measurement_set_pairs(
        casa_python=casa_python,
        pairs=pairs,
        artifact_prefix=artifact_prefix,
        cwd=REPO_ROOT,
    )


def unsupported_native_steps(scenario: str) -> list[dict[str, Any]]:
    return [
        step
        for step in SCENARIOS[scenario]["native_steps"]
        if step["status"] in {"unsupported", "partial"}
    ]


def evaluate_closeout_gate(
    scenarios: list[str],
    preflight: dict[str, Any],
    casa: dict[str, Any],
    native: dict[str, Any],
    comparisons: dict[str, Any],
    *,
    required_native_throughput_mb_s: float | None,
) -> dict[str, Any]:
    blockers = []
    for scenario in scenarios:
        if preflight[scenario]["status"] != "available":
            blockers.append(
                {
                    "scenario": scenario,
                    "reason": "required CASA tutorial inputs/configs are missing",
                    "missing_required": preflight[scenario]["missing_required"],
                }
            )
        for step in unsupported_native_steps(scenario):
            blockers.append(
                {
                    "scenario": scenario,
                    "reason": f"native step {step['id']} is {step['status']}",
                    "detail": step["detail"],
                }
            )
        if comparisons[scenario]["status"] != "passed":
            blockers.append(
                {
                    "scenario": scenario,
                    "reason": comparisons[scenario]["reason"],
                }
            )
    if casa.get("status") != "passed":
        blockers.append(
            {
                "scenario": "all",
                "reason": "CASA oracle did not run successfully",
                "casa_status": casa.get("status"),
            }
        )
    if native.get("status") != "passed":
        blockers.append(
            {
                "scenario": "all",
                "reason": "native implemented slices did not run successfully",
                "native_status": native.get("status"),
            }
        )
    throughput_failures = native_throughput_failures(native, required_native_throughput_mb_s)
    blockers.extend(throughput_failures)
    return {
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "casa_status": casa.get("status"),
        "native_status": native.get("status"),
    }


def native_throughput_failures(
    native: dict[str, Any],
    required_native_throughput_mb_s: float | None,
) -> list[dict[str, Any]]:
    if required_native_throughput_mb_s is None:
        return []
    failures = []
    for scenario, result in native.get("scenarios", {}).items():
        for run in result.get("runs", []):
            if run.get("status") != "run":
                continue
            rate = float(run.get("mb_per_second") or 0.0)
            if rate < required_native_throughput_mb_s:
                failures.append(
                    {
                        "scenario": scenario,
                        "reason": "native throughput below floor",
                        "run": run.get("name"),
                        "native_mb_per_second": rate,
                        "required_mb_per_second": required_native_throughput_mb_s,
                    }
                )
    return failures


def directory_size_or_zero(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    return bench_simobserve.directory_size(path)


if __name__ == "__main__":
    main()
