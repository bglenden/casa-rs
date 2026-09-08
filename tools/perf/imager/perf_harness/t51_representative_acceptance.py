# SPDX-License-Identifier: LGPL-3.0-or-later
"""Supervised T51 representative correctness gate; no performance acceptance."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback

from .t51_pair_driver import (
    BASELINES, CASA_PYTHON, CASA_SITE, CF, MS, REPO, SYNTHESIS_MODULE,
    SYNTHESIS_UUID, command, interpreter_identity, records, save, sha256_file,
    source_identity, stable_tree,
)
from .image_compare import normalize_comparison_request, validate_comparison_output
from .tolerances import evaluate_comparison_tolerances


WALL_SECONDS = 1800
TEST = "continuum_request::tests::source_bind_probe::representative_acceptance::t51_aw_subset_full_products"


def stage(name):
    print("t51_representative_stage " + name, flush=True)


def comparison_request(workload, root, role):
    raw = dict(workload["comparison"])
    raw.update(left_prefix=str(root / "rust" / role), right_prefix=str(root / "casa" / role),
               left_label="casa-rs", right_label="CASA",
               panel_dir=str(root / f"{role}-panels"),
               structure_workspace_dir=str(root / f"{role}-structure"),
               require_direction_wcs_parity=True)
    return normalize_comparison_request(raw)


def drive_lldb(debugger):
    from .casa_cf_guard import run_guarded

    root = Path(os.environ["T51_ACCEPTANCE_ROOT"])
    try:
        request = json.loads((root / "request.json").read_text())
        python = Path(request["diagnostic_interpreter"]["path"])
        if interpreter_identity(python) != request["diagnostic_interpreter"]:
            raise ValueError("CASA interpreter identity changed")
        if sha256_file(SYNTHESIS_MODULE) != request["runtime_sha256"][str(SYNTHESIS_MODULE)]:
            raise ValueError("CASA synthesis library identity changed")
        result = run_guarded(
            debugger, python=python,
            bootstrap_args=[str(REPO / "tools/perf/imager/perf_harness/casa_cf_guard.py"),
                            "--bootstrap", str(REPO / "tools/perf/imager/perf_harness/t51_representative_casa.py")],
            environment=os.environ.copy(), working_directory=root,
            stdout_path=root / "casa.log", stderr_path=root / "casa-stderr.log",
            module_path=SYNTHESIS_MODULE, module_uuid=SYNTHESIS_UUID,
        )
        save(root / "native-guard.json", {"completed": True, **result})
    except BaseException as error:
        traceback.print_exc()
        save(root / "native-guard.json", {"completed": False, "error": str(error)})


def pipeline(root, python, binary):
    from .casa_tclean_workflow import verified_mask_identity
    from run_workload import parse_backend_plan_logs
    from t51_aw_vlass_acceptance import (
        DIRTY_WORKLOAD, CLEAN_WORKLOAD, STORAGE_PROFILE_ENV, prepared_store_snapshot,
        validate_manifest_contract, validate_prepared_aw_receipts,
    )

    root.mkdir()
    stage("preflight_and_binding")
    if shutil.disk_usage(REPO).free < 12 << 30 or shutil.disk_usage(root).free < 64 << 30:
        raise ValueError("insufficient local/external headroom")
    workloads = {"dirty": DIRTY_WORKLOAD, "clean": CLEAN_WORKLOAD}
    manifests = {role: json.loads(path.read_text()) for role, path in workloads.items()}
    for manifest in manifests.values():
        validate_manifest_contract(manifest)
    mask = verified_mask_identity(manifests["clean"]["imaging"])
    inputs = {"measurement_set": MS, "raw_cf": CF}
    before = {name: stable_tree(path) for name, path in inputs.items()}
    runtime_files = [CASA_PYTHON, SYNTHESIS_MODULE,
                     CASA_SITE / "casatasks/private/task_tclean.py",
                     CASA_SITE / "casatasks/private/imagerhelpers/imager_base.py"]
    sources = source_identity()
    request = {
        "scope": "approved five-baseline representative correctness; full-dataset correctness and performance remain #625",
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "source_sha256": sources, "binary": str(binary), "binary_sha256": sha256_file(binary),
        "workloads": {role: str(path) for role, path in workloads.items()},
        "workload_sha256": {role: sha256_file(path) for role, path in workloads.items()},
        "runtime_sha256": {str(path): sha256_file(path) for path in runtime_files},
        "diagnostic_interpreter": interpreter_identity(python),
        "ms": str(MS), "cf_clone": str(root / "casa-cf"), "mask": mask["path"],
        "mask_identity": mask, "baseline_selection": BASELINES, "inputs_before": before,
        "wall_cap_seconds": WALL_SECONDS, "rss_cap_bytes": 32 << 30,
        "admission_bytes": 16 << 30, "workers": 1,
        "cf_generation": "forbidden by verified native entry breakpoints",
    }
    save(root / "request.json", request)
    for name in ("rust", "casa", "cache", "casa-cf"):
        (root / name).mkdir()
    stage("isolated_reference_cache")
    cells = sorted(path for path in CF.iterdir() if path.name.startswith(("CFS_", "WTCFS_")))
    names = {path.name for path in cells}
    imaging = {name for name in names if name.startswith("CFS_")}
    if len(imaging) != 1024 or names != imaging | {"WT" + name for name in imaging}:
        raise ValueError("raw catalog does not contain 1024 complete pairs")
    cell_identities = {path.name: stable_tree(path) for path in cells}
    for source in cells:
        if source.is_symlink() or not source.is_dir():
            raise ValueError("raw CF cell is not an owned directory")
        shutil.copytree(source, root / "casa-cf" / source.name,
                        ignore=shutil.ignore_patterns("table.lock"))
    if cell_identities != {name: stable_tree(root / "casa-cf" / name) for name in names}:
        raise ValueError("reference CF clone differs")
    environment = os.environ.copy()
    environment.update(PYTHONPATH=os.pathsep.join((str(CASA_SITE), str(REPO / "tools/perf/imager"))),
                       PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1", T51_ACCEPTANCE_ROOT=str(root))
    stage("independent_casa_dirty_and_clean")
    command(["/usr/bin/xcrun", "lldb", "--batch", "-o",
             "script import sys; sys.path.insert(0, " + repr(str(REPO / "tools/perf/imager"))
             + "); from perf_harness.t51_representative_acceptance import drive_lldb; drive_lldb(lldb.debugger)"],
            root / "lldb.log", environment)
    if not json.loads((root / "native-guard.json").read_text())["completed"]:
        raise ValueError("native CF-generation guard failed")
    if not json.loads((root / "casa-result.json").read_text())["completed"]:
        raise ValueError("independent CASA reference failed")
    if cell_identities != {name: stable_tree(root / "casa-cf" / name) for name in names}:
        raise ValueError("CASA changed raw CF cells")
    reference = {role: {suffix: stable_tree(Path(str(root / "casa" / role) + suffix))
                       for suffix in manifest["comparison"]["products"]}
                 for role, manifest in manifests.items()}
    save(root / "reference-identity.json", reference)
    rust_environment = os.environ.copy()
    rust_environment.update(STORAGE_PROFILE_ENV)
    rust_environment.update(CASA_RS_T51_SOURCE_BIND_MS=str(MS), CASA_RS_T51_SOURCE_BIND_CF_CACHE=str(CF),
                            CASA_RS_T51_ACCEPTANCE_CACHE_PARENT=str(root / "cache"),
                            CASA_RS_T51_SUBSET_MASK=mask["path"], CASA_RS_TRACE_IMAGING_STAGE_TIMING="1",
                            CASA_RS_TRACE_AW_REPLAY_TIMING="1", CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES="1")
    rust_environment.pop("CASA_RS_IMAGING_SCIENCE_PROBE", None)
    cache = root / "cache/.casa-rs-aw-prepared"
    snapshot = None
    acceptance = {}
    for role, manifest in manifests.items():
        stage(f"rust_{role}_{'cold' if role == 'dirty' else 'warm'}")
        rust_environment.update(CASA_RS_T51_ACCEPTANCE_ROLE=role,
                                CASA_RS_T51_ACCEPTANCE_OUTPUT_PREFIX=str(root / "rust" / role))
        log = root / f"rust-{role}.log"
        command([str(binary), TEST, "--exact", "--ignored", "--nocapture"], log, rust_environment)
        completed, = records(log, "t51_subset_full_products_complete ")
        if set(item["name"] for item in completed["products"]) != set(manifest["comparison"]["products"]):
            raise ValueError("Rust product receipt inventory differs")
        aw = validate_prepared_aw_receipts({"backend_plan_logs": parse_backend_plan_logs(log.read_text())},
                                           workload_id=manifest["id"])
        current = prepared_store_snapshot(cache)
        if not current or len(list((cache / "objects-v3").iterdir())) != 1024:
            raise ValueError("cold cache did not publish the complete paired catalog")
        if snapshot is not None and current != snapshot:
            raise ValueError("warm run changed private persistent payload")
        snapshot = current
        save(root / f"{role}-cache-snapshot.json", current)
        stage(f"compare_{role}_all_products")
        comparison = comparison_request(manifest, root, role)
        save(root / f"{role}-comparison-request.json", comparison)
        command([str(python), "-m", "perf_harness.casa_image_compare",
                 str(root / f"{role}-comparison-request.json"), str(root / f"{role}-comparison.json")],
                root / f"{role}-comparison.log", environment)
        output = json.loads((root / f"{role}-comparison.json").read_text())
        validate_comparison_output(output, comparison)
        evaluation = evaluate_comparison_tolerances(output, manifest["comparison"]["tolerances"])
        acceptance[role] = {"rust": completed, "aw": aw, "tolerances": evaluation}
        save(root / f"{role}-acceptance.json", acceptance[role])
        if evaluation["status"] != "passed":
            raise ValueError(f"{role} failed unchanged scientific tolerances")
    stage("final_immutability")
    if (before != {name: stable_tree(path) for name, path in inputs.items()}
            or sources != source_identity() or request["binary_sha256"] != sha256_file(binary)
            or request["runtime_sha256"] != {str(path): sha256_file(path) for path in runtime_files}
            or mask != verified_mask_identity(manifests["clean"]["imaging"])):
        raise ValueError("source, executable, runtime, or protected input changed")
    save(root / "pipeline-result.json", {"completed": True, "acceptance": acceptance,
         "scope": request["scope"], "visual_review_required": True})
    stage("numerical_gate_complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--casa-python", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    if not all(path.is_absolute() for path in (args.output, args.casa_python, args.binary)) or args.output.exists():
        parser.error("absolute paths and a new exclusive output directory are required")
    if args.pipeline:
        pipeline(args.output, args.casa_python, args.binary)
    else:
        from .t51_pair_guard import run_pair_pipeline
        environment = os.environ.copy()
        environment.update(PYTHONPATH=str(REPO / "tools/perf/imager"), PYTHONDONTWRITEBYTECODE="1")
        result = run_pair_pipeline([sys.executable, "-m", "perf_harness.t51_representative_acceptance",
                                   "--pipeline", "--output", str(args.output),
                                   "--casa-python", str(args.casa_python), "--binary", str(args.binary)],
                                  cwd=REPO, environment=environment, log=sys.stdout, wall_seconds=WALL_SECONDS)
        if args.output.is_dir():
            save(args.output / "outer-guard.json", result)
        print(json.dumps(result), flush=True)
        raise SystemExit(not result["complete"])
