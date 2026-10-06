# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA half of the explicitly approved fixed-model pair, launched only by LLDB."""

from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys

import numpy as np

from perf_harness.t51_casa_model import check_physical_images, create_start_images
from perf_harness.t51_fixed_model_cycle import fixed_model_cycle
from perf_harness.t51_pair_driver import CASA_SITE, REPO, interpreter_identity, save, sha256_file


def selection_receipt(ms_factory, run, imaging):
    """Bounded CASA source census for the exact declared baseline selection."""
    selected = ms_factory()
    try:
        if not selected.open(run["ms"], nomodify=True):
            raise ValueError("CASA could not open protected MS read-only")
        selector = {"field": imaging["field"], "spw": imaging["spw"],
                    "baseline": run["baseline_selection"], "uvdist": imaging["uvrange"],
                    "scanintent": imaging["intent"]}
        if not selected.msselect(selector):
            raise ValueError("CASA rejected the declared subset selection")
        if selected.nrow(True) != 10080:
            raise ValueError("CASA selected a different row count")
        selected.iterinit(columns=["FIELD_ID", "DATA_DESC_ID"], interval=0.0,
                          maxrows=256, adddefaultsortcolumns=True)
        selected.iterorigin()
        groups = Counter()
        pairs = Counter()
        total_samples = 0
        flagged_samples = 0
        raw_digest = hashlib.sha256()
        while True:
            data = selected.getdata(["antenna1", "antenna2", "field_id", "data_desc_id",
                                     "data", "flag", "weight"], ifraxis=False)
            count = len(data["field_id"])
            if not 0 < count <= 256 or data["data"].shape != (4, 64, count):
                raise ValueError("CASA source traversal changed its bounded sample shape")
            if (data["flag"].shape != data["data"].shape
                    or data["weight"].shape != (4, count)):
                raise ValueError("CASA source flag/weight shape differs")
            for field, ddid, first, second in zip(data["field_id"], data["data_desc_id"],
                                                data["antenna1"], data["antenna2"], strict=True):
                groups[(int(field), int(ddid))] += 1
                pairs[tuple(sorted((int(first), int(second))))] += 1
            for name, dtype in (("field_id", "<i8"), ("data_desc_id", "<i8"),
                                ("antenna1", "<i8"), ("antenna2", "<i8"),
                                ("data", "<c16"), ("flag", "u1"), ("weight", "<f8")):
                raw_digest.update(name.encode() + b"\0")
                raw_digest.update(np.asarray(data[name], dtype=dtype).tobytes(order="F"))
            total_samples += data["data"].size
            flagged_samples += int(np.count_nonzero(data["flag"]))
            if not selected.iternext():
                break
        expected_pairs = {(5, 22), (4, 5), (13, 14), (12, 13), (12, 23)}
        expected_fields = set(range(1107, 1128)) | set(range(1512, 1533)) | set(range(1542, 1563))
        if (len(groups) != 1008 or set(groups.values()) != {10}
                or {field for field, _ in groups} != expected_fields
                or set(pairs) != expected_pairs or total_samples != 2580480):
            raise ValueError("CASA source group/sample census differs from the Rust selection")
        return {"selector": selector, "rows": sum(groups.values()), "field_ddid_groups": len(groups),
                "rows_per_group": 10, "samples": total_samples, "flagged_samples": flagged_samples,
                "bounded_traversal_sha256": raw_digest.hexdigest(), "maxrows": 256,
                "scope": "raw CASA data/flags/weights receipt; common immutable source and declared selectors, not transformed-operator parity"}
    finally:
        selected.done()


def run():
    import casatools
    from casatasks import tclean
    from casatasks.private.imagerhelpers.imager_base import PySynthesisImager
    from perf_harness.casa_tclean import load_validated_recipe, normalize_archived_parameters, parse_literal_assignment_recipe
    from perf_harness.casa_tclean_workflow import validate_recipe_manifest_alignment
    from perf_harness.casa_image_compare import compare_one, estimate_native_beam_info
    from perf_harness.tolerances import evaluate_comparison_tolerances

    root = Path(os.environ["T51_PAIR_ROOT"])
    run = json.loads((root / "engines.json").read_text())
    request = json.loads((root / "request.json").read_text())
    workload = json.loads(Path(run["workload"]).read_text())
    if (sha256_file(Path(run["workload"])) != request["workload_sha256"]
            or interpreter_identity(Path(sys.executable)) != request["diagnostic_interpreter"]
            or Path(casatools.__file__).resolve() != (CASA_SITE / "casatools/__init__.py").resolve()
            or list(casatools.version())[:4] != [6, 7, 6, 14]):
        raise ValueError("CASA runtime or workload identity differs from the approved pair")
    imaging = workload["imaging"]
    source = selection_receipt(casatools.ms, run, imaging)
    save(root / "casa-selection.json", source)
    recipe_path = REPO / workload["casa"]["recipe_path"]
    assignments = parse_literal_assignment_recipe(recipe_path.read_text())
    validate_recipe_manifest_alignment(assignments, imaging)
    recipe = load_validated_recipe({"path": str(recipe_path), "sha256": workload["casa"]["recipe_sha256"],
                                    "task": "tclean", "parameter_names": sorted(set(assignments) - {"taskname"})})
    parameters, normalizations, defaults = normalize_archived_parameters(recipe["archived_parameters"], {
        "vis": run["ms"], "imagename": run["casa_prefix"], "cfcache": run["cf_clone"],
        "field": imaging["field"], "phasecenter": imaging["phasecenter_field"],
        "datacolumn": imaging["datacolumn"], "interactive": False, "parallel": False,
        "restart": False, "niter": imaging["niter"], "imsize": [imaging["imsize"]] * 2,
        "spw": imaging["spw"], "mask": run["mask"],
    })
    # This labelled diagnostic bypasses no frozen-protocol validation: it is a
    # separate direct task invocation, not a benchmark-protocol acceptance row.
    starting = [str(root / f"physical-start.tt{term}") for term in range(2)]
    diagnostic_changes = {"antenna": run["baseline_selection"], "startmodel": starting,
                          "niter": 0, "restoration": False}
    parameters.update(diagnostic_changes)
    save(root / "casa-effective-parameters.json", {"parameters": parameters,
         "archived_normalizations": normalizations, "version_defaults": defaults,
         "diagnostic_changes": diagnostic_changes, "frozen_protocol_acceptance": False})
    # A pointwise bound taken from the existing strictest relative ceiling is
    # only an input guard. The unchanged full-array product contract still gates acceptance.
    input_relative_budget = min(workload["comparison"]["tolerances"]["default"][name] for name in (
        "coherent_block_rms_over_right_rms", "diff_abs_max_over_right_peak", "diff_rms_over_right_rms"))
    budgets = {"absolute_budget": 0.0, "relative_budget": input_relative_budget}
    created = create_start_images(run["model_manifest"],
        templates=[run["rust_prefix"] + f".model.tt{term}" for term in range(2)],
        destinations=starting, image_factory=casatools.image, region_factory=casatools.regionmanager, **budgets)
    serialized = check_physical_images(run["model_manifest"], paths=starting,
                                       image_factory=casatools.image, **budgets)
    save(root / "casa-start-model.json", {"created": created, "readback": serialized,
                                          "input_pointwise_guard": budgets})
    cycle = {}
    def check(imager):
        if imager.allimpars["0"]["imagename"] != run["casa_prefix"]:
            raise ValueError("CASA prediction image store has foreign provenance")
        return check_physical_images(run["model_manifest"],
            paths=[run["casa_prefix"] + f".model.tt{term}" for term in range(2)],
            image_factory=casatools.image, **budgets)
    try:
        with fixed_model_cycle(PySynthesisImager, check_physical_model=check, receipt=cycle):
            tclean(**parameters)
    finally:
        save(root / "casa-cycle.json", cycle)
    suffixes = [".residual.tt0", ".residual.tt1", ".pb.tt0",
                ".sumwt.tt0", ".sumwt.tt1", ".sumwt.tt2"]
    panel_dir = root / "comparison-panels"
    workspace = root / "comparison-workspace"
    panel_dir.mkdir()
    workspace.mkdir()
    beam = estimate_native_beam_info(run["casa_prefix"] + ".psf.tt0", 262144)
    products = {}
    for suffix in suffixes:
        products[suffix] = compare_one(run["rust_prefix"] + suffix, run["casa_prefix"] + suffix,
            262144, str(panel_dir), suffix, beam, mode="full", full_chunk_elements=262144,
            require_direction_wcs_parity=True, require_metadata_parity=True,
            structure_workspace_dir=str(workspace))
        save(root / ("comparison" + suffix + ".json"), products[suffix])
    contract = workload["comparison"]["tolerances"]
    # The diagnostic compares the declared component products only. Their
    # ceilings are unchanged; this is never a replacement for full CLEAN products.
    component_contract = {**contract, "products": {key: value for key, value in contract["products"].items()
                                                   if key in suffixes}}
    comparison = {"comparison_mode": "full", "products": products, "beam_info": beam}
    evaluation = evaluate_comparison_tolerances(comparison, component_contract)
    save(root / "casa-result.json", {"completed": cycle["completed"] and evaluation["status"] == "passed",
         "cycle": cycle, "comparison": comparison, "tolerance_evaluation": evaluation,
         "component_contract": component_contract, "full_workload_contract": contract,
         "scope": "fixed-model normalized residual/PB/sumweight pair; no full CLEAN acceptance"})
    if evaluation["status"] != "passed":
        raise ValueError("unchanged component product tolerances did not pass")


if __name__ == "__main__":
    run()
