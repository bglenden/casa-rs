# SPDX-License-Identifier: LGPL-3.0-or-later
"""One authorized Rust-only reload diagnostic; no CASA imaging or optimization.

Builds, controls, setup, imaging, comparison and provenance verification all
run inside the existing nonrenewable 900-second / 32-GiB outer supervisor.
"""

import argparse
from collections import defaultdict
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

from .t51_pair_driver import (
    REPO, MS, CF, PREPARED, WORKLOAD, CASA_SITE, command, records,
    rust_refresh_receipt, save, sha256_file, source_identity, stable_tree,
    interpreter_identity,
)

REFERENCE = Path("/Volumes/GLENDENNING/t51-fixed-model-pair-20260906-attempt4")
COMPARISON_PYTHON = Path("/private/tmp/t51-guard-only-20260906-attempt4/venv/bin/python3.12")
SUFFIXES = (".residual.tt0", ".residual.tt1", ".pb.tt0", ".sumwt.tt0", ".sumwt.tt1", ".sumwt.tt2")
STAGES = (
    "admission", "loading_wait", "workspace_wait", "pin_wait", "eviction",
    "decoder_allocate", "reader", "payload", "open", "buffer_allocate", "read",
    "finite", "payload_hash", "segment_hash", "consumer", "final_integrity",
    "decode_planes", "construct_kernels", "pool_complete", "lease", "total",
)
CHILDREN = {
    "total": ("admission", "decoder_allocate", "reader", "decode_planes", "construct_kernels", "pool_complete", "lease"),
    "admission": ("loading_wait", "workspace_wait", "pin_wait", "eviction"),
    "reader": ("payload",),
    "payload": ("open", "buffer_allocate", "read", "finite", "payload_hash", "segment_hash", "consumer", "final_integrity"),
}


def stage(root, name):
    print("t51_cf_reload_stage " + json.dumps({"stage": name, "monotonic": time.monotonic(), "root": str(root)}), flush=True)


def fields(log, prefix):
    result = []
    for line in log.read_text().splitlines():
        if line.startswith(prefix):
            payload = line[len(prefix):]
        elif line.startswith("test ") and " ... " + prefix in line:
            # With --nocapture, libtest may leave its test-name prefix on the
            # same line as the first stderr receipt. Do not match arbitrary prose.
            payload = line.split(" ... " + prefix, 1)[1]
        else:
            continue
        record = {}
        for part in payload.split():
            key, value = part.split("=", 1)
            if key in record:
                raise ValueError("duplicate diagnostic field: " + key)
            record[key] = value
        result.append(record)
    return result


def sample_costs(sample):
    if (sample["completed"] != "true" or int(sample["p_num"]) != 1 or int(sample["p_den"]) != 32
            or int(sample["seed"]) != 0x7453315f72656c64 or sample["phase"] not in ("0", "1", "2")
            or sample["reload"] not in ("0", "1") or int(sample["bytes"]) <= 0):
        raise ValueError("incomplete sample or changed inclusion probability")
    costs = {name: int(sample[name + "_nanos"]) for name in STAGES}
    if any(value < 0 or value >= (1 << 64) - 1 for value in costs.values()):
        raise ValueError("invalid or saturated diagnostic duration")
    for parent, children in CHILDREN.items():
        remainder = costs[parent] - sum(costs[child] for child in children)
        if remainder < 0:
            raise ValueError("nested timing exceeds parent: " + parent)
        costs[parent + "_unassigned"] = remainder
    costs["collection_decode_construct"] = sum(costs[name] for name in ("consumer", "decode_planes", "construct_kernels"))
    return costs


def summarize(log):
    samples = fields(log, "t51_cf_reload_sample ")
    counts = fields(log, "t51_cf_reload_stratum ")
    if not samples or not counts:
        raise ValueError("missing complete-load samples or exact denominators")
    grouped = defaultdict(list)
    sample_ids = set()
    for sample in samples:
        sample_id = (sample["session"], sample["ordinal"])
        if sample_id in sample_ids:
            raise ValueError("duplicate complete-load sample")
        sample_ids.add(sample_id)
        key = (sample["session"], sample["phase"], sample["reload"], str(int(sample["bytes"]).bit_length() - 1))
        grouped[key].append(sample_costs(sample))
    strata = []
    seen = set()
    totals = defaultdict(lambda: defaultdict(lambda: {"estimate_nanos": 0.0, "poisson_variance_nanos2": 0.0}))
    sessions = defaultdict(lambda: {"loads": 0, "bytes": 0, "first_loads": 0, "reloads": 0})
    for count in counts:
        key = tuple(count[name] for name in ("session", "phase", "reload", "size_bucket"))
        if (key in seen or count["aborted"] != "false" or int(count["p_den"]) != 32 or int(count["p_num"]) != 1
                or int(count["seed"]) != 0x7453315f72656c64):
            raise ValueError("duplicate, aborted, or foreign stratum")
        seen.add(key)
        selected = grouped.pop(key, [])
        if len(selected) != int(count["samples"]):
            raise ValueError("selected-load count and emitted samples differ")
        loads = int(count["loads"])
        if loads <= 0 or len(selected) > loads:
            raise ValueError("invalid exact stratum load count")
        session = sessions[count["session"]]
        session["loads"] += loads
        session["bytes"] += int(count["bytes"])
        session["reloads" if count["reload"] == "1" else "first_loads"] += loads
        estimates = {}
        for name in (selected[0] if selected else ()):  # No invented cost for an unobserved stratum.
            values = [sample[name] for sample in selected]
            total = sum(values) * 32
            variance = 31 * 32 * sum(value * value for value in values)
            estimates[name] = {"ht_total_seconds": total / 1e9,
                               "poisson_se_seconds": math.sqrt(variance) / 1e9,
                               "sample_mean_seconds": statistics.mean(values) / 1e9}
            aggregate = totals[(count["session"], count["phase"])][name]
            aggregate["estimate_nanos"] += total
            aggregate["poisson_variance_nanos2"] += variance
        strata.append({"exact": count, "sample_coverage": bool(selected), "estimates": estimates})
    if grouped:
        raise ValueError("sample without an exact load stratum")
    return {
        "sessions": dict(sessions), "strata": strata,
        "role_estimates": [{"session": session, "phase": phase, "costs": {
            name: {"ht_total_seconds": value["estimate_nanos"] / 1e9,
                   "poisson_se_seconds": math.sqrt(value["poisson_variance_nanos2"]) / 1e9}
            for name, value in costs.items()}} for (session, phase), costs in totals.items()],
        "unobserved_strata": [entry["exact"] for entry in strata if not entry["sample_coverage"]],
        "sampling": "predeclared seed, mixed load ordinal, complete-load p=1/32; not periodic access sampling",
        "roles": {"0": "initial_or_other", "1": "prediction", "2": "accumulation"},
        "uncertainty": "Horvitz-Thompson totals with approximate independent-Poisson standard errors for the pseudorandom design; not a rigorous confidence bound; unobserved strata remain unidentified",
        "accounting": "parent and child intervals overlap; only siblings are additive; each sampled parent has a checked nonnegative unassigned remainder",
        "read_semantics": "counted read wall includes OS page-cache service; it is not physical disk time",
        "full_run_materiality": None,
        "full_run_gap": "completed full-size residual load strata unavailable; no multiplication of initial loads or scaling by records/taps",
        "optimization_authorized": False,
    }


def compare_execution(log, reference_log, attribution):
    current, = records(log, "t51_subset_clean1_complete ")
    reference, = records(reference_log, "t51_subset_clean1_complete ")
    keys = ("model_sha256", "normal_sha256", "major_passes", "actual_components", "nonzero_model_samples",
            "absolute_component_flux", "initial_peak_flux", "final_peak_flux")
    if any(current[key] != reference[key] for key in keys):
        raise ValueError("scientific fingerprint or CLEAN1 trajectory changed")
    reader_keys = (
        "catalog", "logical_bytes", "decoded_ceiling_bytes", "decoder_workspace_ceiling_bytes", "session_validations",
        "reads", "read_bytes", "read_operations", "resident_peak_bytes", "decoder_workspace_peak_bytes",
        "pinned_peak_bytes", "hits", "loads", "evicted_bytes", "copied_bytes", "aborted", "cells_requested",
        "cells_verified", "cells_committed", "cells_rejected", "digest_failures", "eof_failures", "finite_failures",
        "consume_payload_read_bytes", "consume_payload_hashed_bytes", "first_failure_identity",
    )
    actual_readers = fields(log, "imaging_prepared_artifact_reader_summary ")
    reference_readers = fields(reference_log, "imaging_prepared_artifact_reader_summary ")
    if len(actual_readers) != 2 or len(reference_readers) != 2 or len(attribution["sessions"]) != 2:
        raise ValueError("initial and final reader lifetimes were not both completed")
    for (session, exact), actual, prior in zip(sorted(attribution["sessions"].items(), key=lambda row: int(row[0])),
                                               actual_readers, reference_readers, strict=True):
        if any(actual[key] != prior[key] for key in reader_keys):
            raise ValueError("reader work, integrity evidence or internal pool changed in session " + session)
        if exact["loads"] != int(actual["loads"]) or exact["bytes"] != int(actual["copied_bytes"]) or exact["first_loads"] > 1024:
            raise ValueError("exact diagnostic loads/bytes do not reconcile with the reader")
    actual_replay = next(line for line in log.read_text().splitlines() if line.startswith("imaging_gridded_replay_summary "))
    prior_replay = next(line for line in reference_log.read_text().splitlines() if line.startswith("imaging_gridded_replay_summary "))
    for key in ("blocks", "logical_frames", "workers", "worker_threads_started", "partitions_executed", "commits_completed",
                "frames_routed", "encoded_records", "prediction_groups", "degrid_records", "grid_records", "sector_rescans"):
        pattern = rf"\b{key}=([^ ]+)"
        if re.search(pattern, actual_replay)[1] != re.search(pattern, prior_replay)[1]:
            raise ValueError("actual replay work changed: " + key)
    for key in ("executed_work_identity", "committed_work_identity"):
        pattern = rf"\b{key}=(\[[^\]]*\])"
        if re.search(pattern, actual_replay)[1] != re.search(pattern, prior_replay)[1]:
            raise ValueError("replay identity changed: " + key)
    return current


def compare_products(root):
    # Only image reads and the established comparator are imported. No task or
    # synthesis-imager entry point is reachable from this worker.
    from .casa_image_compare import compare_one, estimate_native_beam_info
    from .tolerances import evaluate_comparison_tolerances
    run = json.loads((root / "run.json").read_text())
    workload = json.loads(WORKLOAD.read_text())
    reference_prefix = str(REFERENCE / "casa")
    panel_dir = root / "comparison-panels"
    workspace = root / "comparison-workspace"
    panel_dir.mkdir()
    workspace.mkdir()
    beam = estimate_native_beam_info(reference_prefix + ".psf.tt0", 262144)
    products = {}
    for suffix in SUFFIXES:
        products[suffix] = compare_one(run["rust_prefix"] + suffix, reference_prefix + suffix, 262144,
            str(panel_dir), suffix, beam, mode="full", full_chunk_elements=262144,
            require_direction_wcs_parity=True, require_metadata_parity=True,
            structure_workspace_dir=str(workspace))
        save(root / ("comparison" + suffix + ".json"), products[suffix])
    contract = workload["comparison"]["tolerances"]
    component = {**contract, "products": {key: value for key, value in contract["products"].items() if key in SUFFIXES}}
    comparison = {"comparison_mode": "full", "products": products, "beam_info": beam}
    evaluation = evaluate_comparison_tolerances(comparison, component)
    save(root / "comparison-result.json", {"comparison": comparison, "tolerance_evaluation": evaluation,
                                          "component_contract": component, "casa_imaging_executed": False})
    if evaluation["status"] != "passed":
        raise ValueError("unchanged full-array component contract did not pass")


def control_summary(root, environment, binaries):
    results = []
    for ordinal, mode in enumerate(("off", "sparse", "detailed", "detailed", "sparse", "off", "sparse", "off", "detailed")):
        arm = environment.copy()
        if mode == "off":
            arm.pop("CASA_RS_TRACE_CF_RELOAD_COST", None)
        else:
            arm["CASA_RS_TRACE_CF_RELOAD_COST"] = mode
        arm["CASA_RS_VLASS_CF_CACHE"] = str(CF)
        entry = {"ordinal": ordinal, "mode": mode}
        for name, binary, test, prefix in (
            ("reader", binaries["casa_imaging_runtime"], "t51_cf_reload_reader_observer_control", "t51_cf_reload_control "),
            ("decoder", binaries["casa_imaging_application"], "t51_cf_reload_decoder_observer_control", "t51_cf_decoder_control "),
        ):
            log = root / f"control-{ordinal}-{mode}-{name}.log"
            command([str(binary), test, "--ignored", "--nocapture", "--test-threads=1"], log, arm)
            if "1 passed" not in log.read_text():
                raise ValueError("observer control did not execute")
            entry[name] = fields(log, prefix)
            if len(entry[name]) != (1 if name == "reader" else 3):
                raise ValueError("observer control receipt missing")
        results.append(entry)
    for name in ("reader", "decoder"):
        baseline = results[0][name]
        for result in results:
            for actual, prior in zip(result[name], baseline, strict=True):
                for key in ("payload_bytes", "loads", "payload_sha256"):
                    if actual[key] != prior[key]:
                        raise ValueError("observer control changed the workload or payload")
    save(root / "observer-controls.json", {"cohorts": results,
         "scope": "same production reader and decoder seams; interleaved seconds-scale controls, not an imaging speedup",
         "uncertainty": "report all cohort spread; OFF versus sparse/detailed differences are local observer controls, not a full-workload overhead percentage"})


def built_executables(log, names):
    binaries = {}
    for line in log.read_text().splitlines():
        if line.startswith("{"):
            record = json.loads(line)
            if record.get("reason") == "compiler-artifact" and record.get("executable") and record.get("profile", {}).get("test"):
                name = record["target"]["name"]
                if name in names:
                    if name in binaries:
                        raise ValueError("ambiguous test executable")
                    binaries[name] = Path(record["executable"])
    if set(binaries) != set(names):
        raise ValueError("missing current test executable")
    return binaries


def ownership_control_summary(root, environment, binaries):
    log = root / "ownership-controls.log"
    arm_environment = {**environment, "CASA_RS_TRACE_CF_RELOAD_COST": "detailed",
                       "CASA_RS_VLASS_CF_CACHE": str(CF),
                       "CASA_RS_T51_TRANSFER_CONTROL_ROOT": str(root / "ownership-fixtures"),
                       "CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND": "3000000000",
                       "CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND": "3000000000"}
    command([str(binaries["casa_imaging_application"]), "t51_ownership_transfer_provider_controls",
             "--ignored", "--nocapture", "--test-threads=1"], log, arm_environment)
    report = evaluate_ownership_controls(log.read_text())
    save(root / "ownership-controls.json", report)
    if not report["passed"]:
        raise ValueError("ownership-transfer complete-load benefit did not clear observed control spread; no subset run")


def evaluate_ownership_controls(text):
    cohorts = []
    active = None
    for line in text.splitlines():
        for prefix in ("t51_transfer_begin ", "t51_transfer_complete ",
                       "t51_cf_reload_sample ", "imaging_prepared_artifact_reader_summary "):
            if line.startswith("test ") and " ... " + prefix in line:
                line = line.split(" ... ", 1)[1]
            if not line.startswith(prefix):
                continue
            pairs = [part.split("=", 1) for part in line[len(prefix):].split()]
            fields = dict(pairs)
            if len(fields) != len(pairs):
                raise ValueError("duplicate ownership control field")
            if prefix == "t51_transfer_begin ":
                if active is not None:
                    raise ValueError("overlapping ownership control cohorts")
                active = {**fields, "samples": [], "readers": []}
            elif prefix == "t51_transfer_complete ":
                if active is None or any(fields[key] != active[key] for key in ("payload_bytes", "trial", "copying")):
                    raise ValueError("unmatched ownership control completion")
                active["normal_sha256"] = fields["normal_sha256"]
                cohorts.append(active)
                active = None
            elif active is not None:
                active["samples" if prefix == "t51_cf_reload_sample " else "readers"].append(fields)
    if active is not None or len(cohorts) != 18:
        raise ValueError("incomplete ownership controls")
    comparisons = []
    for size in (1139200, 5939200, 19302400):
        selected = [row for row in cohorts if int(row["payload_bytes"]) == size]
        if [row["copying"] for row in selected] != ["true", "false", "false", "true", "true", "false"]:
            raise ValueError("ownership controls lost interleaved arm order")
        identity = None
        measurements = {"true": [], "false": []}
        for row in selected:
            samples, readers = row["samples"], row["readers"]
            if not samples or len(readers) != 1:
                raise ValueError("ownership control did not complete a production reader session")
            reader = readers[0]
            keys = ("reads", "read_bytes", "loads", "hits", "evicted_bytes", "copied_bytes", "resident_peak_bytes",
                    "decoder_workspace_peak_bytes", "pinned_peak_bytes", "cells_verified", "cells_rejected",
                    "digest_failures", "eof_failures", "finite_failures", "aborted")
            current = (row["normal_sha256"], tuple(reader[key] for key in keys),
                       tuple((sample["catalog_index"], sample["bytes"], sample["reload"]) for sample in samples))
            if identity is not None and identity != current:
                raise ValueError("ownership control changed science, cache order, resource or integrity evidence")
            identity = current
            if (int(reader["loads"]) != len(samples) or int(reader["copied_bytes"]) != size * len(samples)
                    or reader["aborted"] != "false" or any(int(reader[key]) for key in ("cells_rejected", "digest_failures", "eof_failures", "finite_failures"))):
                raise ValueError("ownership control failed exact work/integrity reconciliation")
            for sample in samples:
                if sample["completed"] != "true" or sample["p_num"] != "1" or sample["p_den"] != "1" or int(sample["bytes"]) != size:
                    raise ValueError("ownership control did not time every complete paired load")
            measurements[row["copying"]].append({key: sum(int(sample[key + "_nanos"]) for sample in samples) / len(samples)
                                                 for key in ("construct_kernels", "total")})
        separated = all(min(row[key] for row in measurements["true"]) > max(row[key] for row in measurements["false"])
                        for key in ("construct_kernels", "total"))
        comparisons.append({"payload_bytes": size, "per_load_nanos": measurements, "separated": separated})
    return {"passed": all(row["separated"] for row in comparisons), "comparisons": comparisons, "cohorts": cohorts,
            "decision_rule": "each represented size: candidate construction and complete-load ranges entirely below copying-control ranges; descriptive observed-spread gate, not a statistical confidence bound",
            "scope": "synthetic finite CF payloads with paired dimensions selected from observed production sizes; canonical application/reader/provider lifecycle, fresh sessions and unchanged pool; not full catalog history or end-to-end performance",
            "unrepresented_strata": "all other sizes/layouts and actual full-workload load populations remain unestimated"}


def pipeline(root, ownership_transfer=False):
    from .casa_tclean_workflow import verified_mask_identity
    root.mkdir()
    stage(root, "guarded_preflight_and_source_identity")
    if shutil.disk_usage(REPO).free < 12 << 30 or shutil.disk_usage(root).free < 64 << 30:
        raise ValueError("insufficient build or external artifact headroom")
    environment = os.environ.copy()
    environment.update(CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="2", CARGO_NET_OFFLINE="true")
    environment.pop("CASA_RS_TRACE_CF_RELOAD_COST", None)
    environment.pop("CASA_RS_IMAGING_SCIENCE_PROBE", None)
    command(["rustfmt", "--edition", "2024", "--config", "skip_children=true",
             "crates/casa-imaging-runtime/src/prepared_artifact/reload_probe.rs",
             "crates/casa-imaging-runtime/src/prepared_artifact/reader.rs",
             "crates/casa-imaging-runtime/src/prepared_artifact/transaction.rs",
             "crates/casa-imaging-runtime/src/prepared_artifact.rs",
             "crates/casa-imaging-runtime/src/lib.rs",
             "crates/casa-imaging-runtime/src/complete_data_operator.rs",
             "crates/casa-imaging-application/src/aw_cache.rs",
             "crates/casa-imaging-application/src/aw_cache/encoding_probe.rs",
             "crates/casa-imaging-application/src/aw_cache/ownership_transfer_probe.rs",
             "crates/casa-imaging-application/tests/common/continuum_fixture.rs",
             "crates/casa-imaging-application/tests/continuum_application.rs"], root / "format.log", environment)
    reference_request = json.loads((REFERENCE / "request.json").read_text())
    reference_result = json.loads((REFERENCE / "pipeline-result.json").read_text())
    if not reference_result["completed"] or interpreter_identity(COMPARISON_PYTHON) != reference_request["diagnostic_interpreter"]:
        raise ValueError("frozen reference or image-comparison interpreter is not the accepted one")
    workload = json.loads(WORKLOAD.read_text())
    mask = verified_mask_identity(workload["imaging"])
    roots = {"measurement_set": MS, "raw_cf": CF, "prepared_cf": PREPARED / ".casa-rs-aw-prepared/objects-v3"}
    inputs = {name: stable_tree(path) for name, path in roots.items()}
    if inputs != reference_request["inputs_before"] or mask != reference_request["mask"] or sha256_file(WORKLOAD) != reference_request["workload_sha256"]:
        raise ValueError("frozen input, mask or workload changed")
    reference_products = {suffix: stable_tree(Path(str(REFERENCE / "casa") + suffix)) for suffix in (*SUFFIXES, ".psf.tt0")}
    sources = source_identity()
    save(root / "request.json", {"scope": "one ownership-transfer candidate with conditional subset" if ownership_transfer else "one diagnostic-only Rust CLEAN1 with frozen CASA product reads; no CASA imaging",
         "ownership_transfer_candidate": ownership_transfer, "go_no_go_checkpoint_required": ownership_transfer,
         "reference": str(REFERENCE), "reference_products": reference_products, "inputs_before": inputs,
         "source_sha256": sources, "mask": mask, "workload_sha256": sha256_file(WORKLOAD),
         "parent_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
         "memory_bytes": 16 << 30, "workers": 1, "wall_cap_seconds": 900, "rss_cap_bytes": 32 << 30,
         "automatic_retry": False, "sampling_seed": 0x7453315f72656c64, "inclusion_probability": "1/32",
         "diagnostic_interpreter": interpreter_identity(COMPARISON_PYTHON)})
    command(["git", "diff", "--check"], root / "diff-check.log", environment)
    command([sys.executable, "-m", "unittest", "perf_harness.test_t51_cf_reload_cost_probe"], root / "harness-tests.log", environment)
    stage(root, "build_focused_runtime_and_application_tests")
    build = root / "build.log"
    command(["cargo", "test", "--release", "-p", "casa-imaging-runtime", "-p", "casa-imaging-application",
             "--lib", "--no-run", "--message-format=json"], build, environment)
    binaries = built_executables(build, ("casa_imaging_runtime", "casa_imaging_application"))
    integration_build = root / "integration-build.log"
    command(["cargo", "test", "--release", "-p", "casa-imaging-application", "--test", "continuum_application",
             "--no-run", "--message-format=json"], integration_build, environment)
    binaries.update(built_executables(integration_build, ("continuum_application",)))
    hashes = {name: sha256_file(binary) for name, binary in binaries.items()}
    save(root / "binaries.json", {name: {"path": str(binary), "sha256": hashes[name]} for name, binary in binaries.items()})
    stage(root, "integrity_lifecycle_and_diagnostic_unit_gates")
    for name, target, test in (
        ("reader-negative", "casa_imaging_runtime", "prepared_artifact::reader::tests"),
        ("diagnostic", "casa_imaging_runtime", "prepared_artifact::reload_probe::tests"),
        ("decoder-failure", "casa_imaging_application", "decoded_reader_error_invalidates_residency_and_blocks_later_reuse"),
        ("export", "casa_imaging_application", "model_export_preserves_f64_bits_and_independent_support"),
        ("ownership-transfer", "casa_imaging_application", "aw_cache::ownership_transfer_probe::ownership_transfer_"),
    ):
        log = root / (name + "-tests.log")
        command([str(binaries[target]), test, "--nocapture", "--test-threads=1"], log, environment)
        if re.search(r"test result: ok\. [1-9][0-9]* passed; 0 failed;", log.read_text()) is None:
            raise ValueError("focused gate did not execute: " + name)
    detailed = {**environment, "CASA_RS_TRACE_CF_RELOAD_COST": "detailed"}
    for name, test in (
        ("provider-lifecycle", "t51_lazy_aw_reader_executes_real_science_and_closes_at_its_io_fence"),
        ("provider-recompute", "t51_fixed_memory_aw_clean_preserves_prepared_projection_during_recompute"),
    ):
        log = root / (name + "-tests.log")
        command([str(binaries["continuum_application"]), test, "--nocapture", "--test-threads=1"], log, detailed)
        if "1 passed" not in log.read_text() or "t51_cf_reload_sample " not in log.read_text():
            raise ValueError("instrumented production provider gate did not execute: " + name)
    if ownership_transfer:
        stage(root, "interleaved_copy_vs_ownership_complete_load_controls")
        ownership_control_summary(root, environment, binaries)
    else:
        stage(root, "interleaved_off_sparse_detailed_observer_controls")
        control_summary(root, environment, binaries)
    environment.update({
        "CASA_RS_T51_SOURCE_BIND_MS": str(MS), "CASA_RS_T51_SOURCE_BIND_CF_CACHE": str(CF),
        "CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT": "/Volumes/GLENDENNING",
        "CASA_RS_T51_SUBSET_CACHE_RESUME": str(PREPARED), "CASA_RS_T51_SUBSET_MASK": mask["path"],
        "CASA_RS_T51_SUBSET_MEMORY_BYTES": str(16 << 30), "CASA_RS_T51_EXPORT_MODEL": "1",
        "CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND": "3000000000",
        "CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND": "3000000000",
        "CASA_RS_TRACE_IMAGING_STAGE_TIMING": "1", "CASA_RS_TRACE_AW_REPLAY_TIMING": "1",
        "CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES": "1", "CASA_RS_TRACE_CF_RELOAD_COST": "sparse",
    })
    stage(root, "one_rust_clean1_with_reload_observation")
    log = root / "rust.log"
    command([str(binaries["casa_imaging_application"]), "t51_aw_subset_clean1", "--ignored", "--nocapture"], log, environment)
    attribution = summarize(log)
    completed = compare_execution(log, REFERENCE / "rust.log", attribution)
    exported, = records(log, "t51_authoritative_model_export ")
    model = json.loads(Path(exported["manifest"]).read_text())
    if model["summary"]["values_sha256"] != completed["model_sha256"]:
        raise ValueError("physical model export differs from the production fingerprint")
    refresh = rust_refresh_receipt(log, model)
    save(root / "attribution.json", attribution)
    save(root / "rust-refresh.json", refresh)
    save(root / "run.json", {"rust_prefix": str(Path(completed["root"]) / "probe"), "rust_completed": completed})
    stage(root, "full_array_comparison_to_retained_casa_images")
    comparison_environment = environment.copy()
    comparison_environment.update(PYTHONPATH=os.pathsep.join((str(CASA_SITE), str(REPO / "tools/perf/imager"))),
                                  PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    command([str(COMPARISON_PYTHON), "-m", "perf_harness.t51_cf_reload_cost_probe", "--compare", "--output", str(root)],
            root / "comparison.log", comparison_environment)
    stage(root, "final_provenance_and_frozen_reference_immutability")
    if (inputs != {name: stable_tree(path) for name, path in roots.items()}
            or reference_products != {suffix: stable_tree(Path(str(REFERENCE / "casa") + suffix)) for suffix in (*SUFFIXES, ".psf.tt0")}
            or sources != source_identity() or hashes != {name: sha256_file(binary) for name, binary in binaries.items()}
            or mask != verified_mask_identity(workload["imaging"])
            or interpreter_identity(COMPARISON_PYTHON) != reference_request["diagnostic_interpreter"]):
        raise ValueError("protected source/input/reference/mask/binary/interpreter changed")
    save(root / "pipeline-result.json", {"completed": True, "rust_refresh": refresh, "rust_completed": completed,
         "attribution": attribution, "casa_imaging_executed": False, "optimization_authorized": ownership_transfer,
         "go_no_go_checkpoint_required": ownership_transfer,
         "full_t51_goal_achieved": False, "scientific_fingerprints_unchanged": True})
    stage(root, "complete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--authorized-once", action="store_true")
    modes.add_argument("--pipeline", action="store_true", help=argparse.SUPPRESS)
    modes.add_argument("--compare", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ownership-transfer", action="store_true", help="single approved candidate and conditional subset; no next candidate")
    args = parser.parse_args()
    if not args.output.is_absolute():
        parser.error("output must be absolute")
    if args.compare:
        compare_products(args.output)
    elif args.pipeline:
        pipeline(args.output, args.ownership_transfer)
    else:
        if args.output.exists():
            parser.error("an attempt cannot be overwritten or retried")
        from .t51_pair_guard import run_pair_pipeline
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPO / "tools/perf/imager")
        result = run_pair_pipeline([sys.executable, "-m", "perf_harness.t51_cf_reload_cost_probe", "--pipeline", "--output", str(args.output)] +
                                  (["--ownership-transfer"] if args.ownership_transfer else []),
                                  cwd=REPO, environment=environment, log=sys.stdout)
        if args.output.is_dir():
            save(args.output / "outer-guard.json", result)
        print("t51_cf_reload_outer_terminal " + json.dumps(result), flush=True)
        raise SystemExit(not result["complete"])


if __name__ == "__main__":
    main()
