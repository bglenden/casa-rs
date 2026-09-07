# SPDX-License-Identifier: LGPL-3.0-or-later
"""Single-attempt T51 fixed-model diagnostic; not a performance acceptance row.

Invoke only with the explicit paired-run approval. The outer guard encloses
this entire pipeline, including compilation, provenance scans and validation.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from .tree_identity import sha256_file, tree_identity


REPO = Path(__file__).resolve().parents[4]
MS = Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537-20260903.thHAWF/data/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms")
CF = Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache/6.7.5.9/c03a1fab375d7f1747bad8cfb3fad38cf4620fea401570ed779d3def3fad1c36")
PREPARED = Path("/Volumes/GLENDENNING/t51-subset-cache-3L96cs")
CASA_PYTHON = Path("/Applications/CASA.app/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12")
SYNTHESIS_MODULE = CASA_PYTHON.parent.parent / "lib/python3.12/site-packages/casatools/__casac__/lib/libcasacpp_synthesis.6.dylib"
SYNTHESIS_UUID = "33923015-7F4A-393E-8319-7D4191ECF9FA"
WORKLOAD = REPO / "tools/perf/imager/workloads/vlass-fragment-all-fields-clean-4096-full-16-spw-casa.json"
BASELINES = "5&22;4&5;13&14;12&13;12&23"
CASA_SITE = CASA_PYTHON.parent.parent / "lib/python3.12/site-packages"


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def stage(root, name):
    print("t51_pair_stage " + json.dumps({"stage": name, "monotonic": time.monotonic(),
                                         "root": str(root)}), flush=True)


def interpreter_identity(python):
    """Identify the explicitly selected diagnostic interpreter, not the frozen CASA runtime."""
    if not python.is_absolute():
        raise ValueError("diagnostic interpreter must be an absolute path")
    executable = python.resolve(strict=True)
    framework = executable.parent.parent / "Python"
    return {"path": str(python), "resolved_path": str(executable),
            "executable_sha256": sha256_file(executable),
            "framework_path": str(framework), "framework_sha256": sha256_file(framework)}


def stable_tree(path):
    identity = tree_identity(path, excluded_names={"table.lock"})
    # Lock creation/removal is not a scientific or persistent payload mutation.
    identity.pop("excluded_count")
    return identity


def source_identity():
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=REPO).decode().split("\0")
    added = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"],
                                    cwd=REPO).decode().split("\0")
    paths = sorted(set(path for path in tracked + added if path and (
        path.startswith("crates/casa-") or path.startswith("tools/perf/imager/")
        or path in ("Cargo.toml", "Cargo.lock", ".cargo/config.toml")
    )))
    return {path: sha256_file(REPO / path) for path in paths if (REPO / path).is_file()}


def command(argv, logfile, environment):
    with logfile.open("x") as stream:
        subprocess.run(argv, cwd=REPO, env=environment, stdout=stream,
                       stderr=subprocess.STDOUT, check=True)


def records(log, prefix):
    with log.open() as stream:
        return [json.loads(line.split(prefix, 1)[1]) for line in stream if line.startswith(prefix)]


def rust_refresh_receipt(log, model):
    prefixes = ("imaging_terminal_refresh_envelope ", "imaging_taylor_normalization_envelope ")
    envelopes = []
    for prefix in prefixes:
        with log.open() as stream:
            matches = [dict(part.split("=", 1) for part in line.split()[1:])
                       for line in stream if line.startswith(prefix)]
        envelope, = matches
        if envelope["model_generation"] != model["model_generation"]:
            raise ValueError("Rust timed envelope and authoritative model generations differ")
        envelopes.append(envelope)
    replay, normalization = envelopes
    if normalization["aw_projection"] != "true":
        raise ValueError("Rust normalization envelope was not AW projection")
    nanos = [int(replay["elapsed_nanos"]), int(normalization["preparation_nanos"]),
             int(normalization["residual_model_nanos"])]
    if any(value <= 0 for value in nanos):
        raise ValueError("Rust emitted an empty timed component interval")
    return {"normalized_refresh_seconds": sum(nanos) / 1e9,
            "replay": replay, "normalization": normalization,
            "boundary": "terminal handoff/planning/replay/reconciliation plus disjoint shared product preparation and residual/model normalization",
            "excludes": "initial pass, minor cycle, PSF/weight products, restoration, publication, export, hashing"}


def pipeline(root, python):
    from .casa_tclean_workflow import verified_mask_identity

    root.mkdir()  # Exclusive: the authorized attempt cannot be overwritten/retried.
    stage(root, "prerequisites_and_identity")
    interpreter = interpreter_identity(python)
    if shutil.disk_usage(REPO).free < 12 << 30 or shutil.disk_usage(root).free < 64 << 30:
        raise ValueError("insufficient build or external artifact headroom")
    workload = json.loads(WORKLOAD.read_text())
    mask = verified_mask_identity(workload["imaging"])
    roots = {"measurement_set": MS, "raw_cf": CF,
             "prepared_cf": PREPARED / ".casa-rs-aw-prepared/objects-v3"}
    before = {name: stable_tree(path) for name, path in roots.items()}
    sources = source_identity()
    runtime_files = [CASA_PYTHON, SYNTHESIS_MODULE,
                     CASA_SITE / "casatasks/private/task_tclean.py",
                     CASA_SITE / "casatasks/private/imagerhelpers/imager_base.py"]
    runtime_hashes = {str(path): sha256_file(path) for path in runtime_files}
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    request = {"scope": "one fixed-model normalized residual refresh pair; not full T51 acceptance",
               "parent_revision": revision, "source_sha256": sources,
               "workload_sha256": sha256_file(WORKLOAD), "inputs_before": before,
               "mask": mask, "rust_workers": 1, "rust_memory_bytes": 16 << 30,
               "baseline_selection": BASELINES, "model_rule": "unchanged Rust accepted CLEAN1 model",
               "diagnostic_interpreter": interpreter,
               "installed_casa_python": str(CASA_PYTHON),
               "casa_site_packages": str(CASA_SITE),
               "synthesis_module": str(SYNTHESIS_MODULE), "synthesis_uuid": SYNTHESIS_UUID,
               "synthesis_sha256": sha256_file(SYNTHESIS_MODULE),
               "runtime_sha256": runtime_hashes,
               "wall_cap_seconds": 900, "rss_cap_bytes": 32 << 30,
               "cf_generation": "forbidden by verified native entry breakpoints",
               "attempts": 1, "automatic_retry": False}
    save(root / "request.json", request)
    environment = os.environ.copy()
    environment.update(CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="2")
    stage(root, "build_and_export_unit_test")
    build_log = root / "build.log"
    command(["cargo", "test", "--release", "-p", "casa-imaging-application", "--lib",
             "--no-run", "--message-format=json"], build_log, environment)
    executables = set()
    for line in build_log.read_text().splitlines():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        if (record.get("reason") == "compiler-artifact" and record.get("executable")
                and record.get("target", {}).get("name") == "casa_imaging_application"
                and record.get("profile", {}).get("test")):
            executables.add(record["executable"])
    if len(executables) != 1:
        raise ValueError("build did not identify exactly one current application test binary")
    binary = Path(executables.pop())
    binary_hash = sha256_file(binary)
    command([str(binary), "model_export_preserves_f64_bits_and_independent_support", "--nocapture"],
            root / "export-unit.log", environment)
    if "1 passed" not in (root / "export-unit.log").read_text():
        raise ValueError("authoritative export unit test did not execute")
    environment.update({
        "CASA_RS_T51_SOURCE_BIND_MS": str(MS), "CASA_RS_T51_SOURCE_BIND_CF_CACHE": str(CF),
        "CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT": "/Volumes/GLENDENNING",
        "CASA_RS_T51_SUBSET_CACHE_RESUME": str(PREPARED), "CASA_RS_T51_SUBSET_MASK": mask["path"],
        "CASA_RS_T51_SUBSET_MEMORY_BYTES": str(16 << 30), "CASA_RS_T51_EXPORT_MODEL": "1",
        "CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND": "3000000000",
        "CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND": "3000000000",
        "CASA_RS_TRACE_IMAGING_STAGE_TIMING": "1", "CASA_RS_TRACE_AW_REPLAY_TIMING": "1",
        "CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES": "1",
    })
    environment.pop("CASA_RS_IMAGING_SCIENCE_PROBE", None)
    stage(root, "rust_clean1_and_immutable_model_export")
    rust_log = root / "rust.log"
    command([str(binary), "t51_aw_subset_clean1", "--ignored", "--nocapture"], rust_log, environment)
    completed, = records(rust_log, "t51_subset_clean1_complete ")
    exported, = records(rust_log, "t51_authoritative_model_export ")
    if (completed["major_passes"] != 2 or completed["actual_components"] != 1
            or completed["nonzero_model_samples"] <= 0):
        raise ValueError("Rust production model rule was not fulfilled")
    model_manifest = Path(exported["manifest"])
    model = json.loads(model_manifest.read_text())
    if model["summary"]["values_sha256"] != completed["model_sha256"]:
        raise ValueError("Rust export and production model digests differ")
    rust_refresh = rust_refresh_receipt(rust_log, model)
    save(root / "rust-refresh.json", rust_refresh)
    stage(root, "isolated_raw_cf_copy")
    clone = root / "casa-cf"
    clone.mkdir()
    cells = sorted(path for path in CF.iterdir() if path.name.startswith(("CFS_", "WTCFS_")))
    names = {path.name for path in cells}
    imaging_names = {name for name in names if name.startswith("CFS_")}
    if len(imaging_names) != 1024 or names != imaging_names | {"WT" + name for name in imaging_names}:
        raise ValueError("raw CF catalog is not the complete 1024 imaging/weight pairs")
    for source in cells:
        if source.is_symlink() or not source.is_dir():
            raise ValueError("CF cell is not an owned regular directory")
        shutil.copytree(source, clone / source.name, ignore=shutil.ignore_patterns("table.lock"))
    clone_before = {source.name: stable_tree(source) for source in cells}
    if clone_before != {source.name: stable_tree(clone / source.name) for source in cells}:
        raise ValueError("isolated CF copy is not byte-identical to the complete raw catalog")
    run = {"model_manifest": str(model_manifest), "rust_prefix": str(Path(completed["root"]) / "probe"),
           "casa_prefix": str(root / "casa"), "cf_clone": str(clone),
           "workload": str(WORKLOAD), "ms": str(MS), "baseline_selection": BASELINES,
           "mask": mask["path"], "binary": str(binary), "binary_sha256": binary_hash,
           "raw_cf_clone_before": clone_before, "rust_completed": completed}
    save(root / "engines.json", run)
    stage(root, "guarded_casa_model_conversion_refresh_and_comparison")
    casa_environment = os.environ.copy()
    casa_environment["PYTHONPATH"] = os.pathsep.join((str(CASA_SITE), str(REPO / "tools/perf/imager")))
    casa_environment["PYTHONNOUSERSITE"] = "1"
    casa_environment["PYTHONDONTWRITEBYTECODE"] = "1"
    casa_environment["T51_PAIR_ROOT"] = str(root)
    casa_environment.pop("CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES", None)
    command(["/usr/bin/xcrun", "lldb", "--batch", "-o",
             "script import sys; sys.path.insert(0, " + repr(str(REPO / "tools/perf/imager"))
             + "); from perf_harness.t51_pair_driver import drive_lldb; drive_lldb(lldb.debugger)"],
            root / "lldb.log", casa_environment)
    guard = json.loads((root / "native-guard.json").read_text())
    if not guard["completed"]:
        raise ValueError("CASA native generation guard did not complete successfully")
    result = json.loads((root / "casa-result.json").read_text())
    if not result["completed"]:
        raise ValueError("CASA fixed-model refresh/comparison did not pass")
    stage(root, "final_input_and_executable_immutability")
    after = {name: stable_tree(path) for name, path in roots.items()}
    if (before != after or source_identity() != sources or sha256_file(binary) != binary_hash
            or {str(path): sha256_file(path) for path in runtime_files} != runtime_hashes
            or interpreter_identity(python) != interpreter
            or verified_mask_identity(workload["imaging"]) != mask):
        raise ValueError("protected input, source, mask, or executable changed")
    # CASA may write its subset avgPB to the isolated root, never into raw cells.
    final_cells = {path.name for path in clone.iterdir() if path.name.startswith(("CFS_", "WTCFS_"))}
    if (final_cells != set(clone_before)
            or clone_before != {name: stable_tree(clone / name) for name in clone_before}):
        raise ValueError("CASA mutated the isolated raw CF catalog")
    save(root / "pipeline-result.json", {"completed": True, "inputs_after": after,
         "binary_sha256": binary_hash, "rust_refresh": rust_refresh, "casa": result,
         "scope": request["scope"], "full_t51_goal_achieved": False})
    stage(root, "pipeline_complete")


def drive_lldb(debugger):
    """LLDB batch entry; the parent also verifies the receipt because LLDB may exit zero on Python errors."""
    import traceback
    from .casa_cf_guard import run_guarded

    root = Path(os.environ["T51_PAIR_ROOT"])
    try:
        request = json.loads((root / "request.json").read_text())
        interpreter = request["diagnostic_interpreter"]
        python = Path(interpreter["path"])
        if interpreter_identity(python) != interpreter:
            raise ValueError("diagnostic interpreter changed before CASA launch")
        if (request["casa_site_packages"] != str(CASA_SITE)
                or request["synthesis_module"] != str(SYNTHESIS_MODULE)
                or request["synthesis_uuid"] != SYNTHESIS_UUID
                or sha256_file(SYNTHESIS_MODULE) != request["synthesis_sha256"]):
            raise ValueError("manifested CASA library identity changed before launch")
        result = run_guarded(
            debugger, python=python,
            bootstrap_args=[str(REPO / "tools/perf/imager/perf_harness/casa_cf_guard.py"),
                            "--bootstrap", str(REPO / "tools/perf/imager/perf_harness/t51_pair_casa.py")],
            environment=os.environ.copy(), working_directory=root,
            stdout_path=root / "casa.log", stderr_path=root / "casa-stderr.log",
            module_path=SYNTHESIS_MODULE, module_uuid=SYNTHESIS_UUID,
        )
        save(root / "native-guard.json", {"completed": True, **result})
    except BaseException as error:
        traceback.print_exc()
        save(root / "native-guard.json", {"completed": False, "error": str(error)})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--pipeline", action="store_true", help=argparse.SUPPRESS)
    mode.add_argument("--authorized-once", action="store_true",
                      help="execute one separately authorized pair; does not grant approval")
    parser.add_argument("--output", type=Path, required=True, help="new exclusive artifact directory")
    parser.add_argument("--casa-python", type=Path, required=True,
                        help="explicit debugger-compatible diagnostic Python 3.12 interpreter")
    args = parser.parse_args()
    if not args.output.is_absolute() or not args.casa_python.is_absolute():
        parser.error("output and interpreter paths must be absolute")
    if args.output.exists():
        parser.error("artifact directory already exists; an attempt cannot be overwritten or retried")
    if args.pipeline:
        pipeline(args.output, args.casa_python)
    else:
        from .t51_pair_guard import run_pair_pipeline

        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPO / "tools/perf/imager")
        result = run_pair_pipeline([sys.executable, "-m", "perf_harness.t51_pair_driver", "--pipeline",
                                   "--output", str(args.output), "--casa-python", str(args.casa_python)],
                                  cwd=REPO, environment=environment, log=sys.stdout)
        if args.output.is_dir() and not (args.output / "outer-guard.json").exists():
            save(args.output / "outer-guard.json", result)
        print("t51_pair_outer_terminal " + json.dumps(result), flush=True)
        raise SystemExit(not result["complete"])
