# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent representative T51 CASA products, under the outer native CF guard.

Entry: ``python -m perf_harness.t51_representative_casa`` with
``T51_ACCEPTANCE_ROOT`` naming the supervisor's immutable request directory.
"""

import json
import math
import os
from pathlib import Path
import sys
import time

import numpy as np

from perf_harness.casa_image_compare import discover_product_inventory
from perf_harness.casa_tclean import (
    load_validated_recipe,
    normalize_archived_parameters,
    parse_literal_assignment_recipe,
)
from perf_harness.casa_tclean_workflow import (
    validate_recipe_manifest_alignment,
    verified_mask_identity,
)
from perf_harness.t51_pair_casa import selection_receipt
from perf_harness.t51_pair_driver import (
    BASELINES,
    CASA_SITE,
    REPO,
    interpreter_identity,
    save,
    sha256_file,
)


def json_result(value):
    """Preserve CASA summaries, including explicit nonfinite diagnostic values."""
    if isinstance(value, np.ndarray):
        return json_result(value.tolist())
    if isinstance(value, np.generic):
        return json_result(value.item())
    if isinstance(value, dict):
        return {str(key): json_result(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_result(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return {"nonfinite_float": repr(value)}
    if isinstance(value, complex):
        return {"real": json_result(value.real), "imaginary": json_result(value.imag)}
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"unsupported CASA summary value: {type(value).__name__}")


def run():
    import casatools
    from casatasks import tclean

    root = Path(os.environ["T51_ACCEPTANCE_ROOT"]).resolve(strict=True)
    request = json.loads((root / "request.json").read_text())
    if (interpreter_identity(Path(sys.executable)) != request["diagnostic_interpreter"]
            or Path(casatools.__file__).resolve() != (CASA_SITE / "casatools/__init__.py").resolve()
            or list(casatools.version())[:4] != [6, 7, 6, 14]):
        raise ValueError("CASA runtime differs from the approved representative gate")
    if not request["runtime_sha256"]:
        raise ValueError("CASA runtime identity is empty")
    for path, expected in request["runtime_sha256"].items():
        if sha256_file(Path(path)) != expected:
            raise ValueError(f"CASA runtime payload changed: {path}")
    if request["baseline_selection"] != BASELINES:
        raise ValueError("representative baseline selection changed")
    for name in ("ms", "cf_clone", "mask"):
        if not Path(request[name]).is_absolute() or not Path(request[name]).is_dir():
            raise ValueError(f"missing absolute representative input: {name}")

    workloads = {}
    for role in ("dirty", "clean"):
        path = Path(request["workloads"][role])
        if not path.is_absolute() or sha256_file(path) != request["workload_sha256"][role]:
            raise ValueError(f"{role} workload identity changed")
        workloads[role] = json.loads(path.read_text())
    casa_root = root / "casa"
    casa_root.mkdir(exist_ok=True)
    results = {}
    source = None
    for role in ("dirty", "clean"):
        workload = workloads[role]
        imaging = workload["imaging"]
        prefix = casa_root / role
        if prefix.exists() or discover_product_inventory(prefix):
            raise ValueError(f"CASA output prefix is not fresh: {prefix}")
        expected_iterations = 0 if role == "dirty" else 2000
        if (imaging["niter"] != expected_iterations or imaging["minor_cycle_length"] != 2000
                or imaging["restoration"] is not True):
            raise ValueError(f"{role} workload changed the approved iteration/restoration controls")
        selection = selection_receipt(casatools.ms, request, imaging)
        if source is None:
            source = selection
            save(root / "casa-selection.json", source)
        elif selection != source:
            raise ValueError("DIRTY and CLEAN source selections differ")
        recipe_path = REPO / workload["casa"]["recipe_path"]
        assignments = parse_literal_assignment_recipe(recipe_path.read_text())
        validate_recipe_manifest_alignment(assignments, imaging)
        recipe = load_validated_recipe({
            "path": str(recipe_path), "sha256": workload["casa"]["recipe_sha256"],
            "task": "tclean", "parameter_names": sorted(set(assignments) - {"taskname"}),
        })
        overrides = {
            "vis": request["ms"], "imagename": str(prefix), "cfcache": request["cf_clone"],
            "field": imaging["field"], "phasecenter": imaging["phasecenter_field"],
            "datacolumn": imaging["datacolumn"], "interactive": False, "parallel": False,
            "restart": False, "niter": expected_iterations, "imsize": [imaging["imsize"]] * 2,
            "spw": imaging["spw"],
        }
        if role == "clean":
            mask = verified_mask_identity(imaging)
            if mask is None or Path(mask["path"]) != Path(request["mask"]).resolve():
                raise ValueError("CLEAN mask differs from the frozen workload mask")
            overrides["mask"] = mask["path"]
        parameters, normalizations, defaults = normalize_archived_parameters(
            recipe["archived_parameters"], overrides)
        # The approved subset is the sole exception to the original selection.
        parameters["antenna"] = request["baseline_selection"]
        if (parameters.get("startmodel") not in (None, "", [])
                or parameters["restoration"] is not True
                or parameters["savemodel"] != "none"
                or parameters["cycleniter"] != 2000
                or (role == "dirty" and parameters.get("mask") not in (None, "", []))):
            raise ValueError("CASA parameters do not describe independent full-product imaging")
        save(root / f"casa-{role}-effective-parameters.json", {
            "role": role, "parameters": parameters, "selection": selection,
            "archived_normalizations": normalizations, "version_defaults": defaults,
            "approved_selection_override": {"antenna": request["baseline_selection"]},
            "workload_sha256": request["workload_sha256"][role],
        })
        print("t51_representative_casa_start " + json.dumps({
            "role": role, "output_prefix": str(prefix), "parameters": parameters,
        }), flush=True)
        started = time.monotonic()
        returned = tclean(**parameters)
        seconds = time.monotonic() - started
        products = discover_product_inventory(prefix)
        expected = sorted(workload["comparison"]["products"])
        completed = (returned is not False and products == expected
                     and len(expected) == (18 if role == "dirty" else 19)
                     and all(Path(str(prefix) + suffix).is_dir() for suffix in products))
        result = {
            "role": role, "completed": completed, "execution_seconds": seconds,
            "output_prefix": str(prefix), "selection": selection,
            "products": [{"name": suffix, "path": str(prefix) + suffix} for suffix in products],
            "expected_products": expected, "missing_products": sorted(set(expected) - set(products)),
            "unexpected_products": sorted(set(products) - set(expected)),
            "tclean_result": json_result(returned),
        }
        save(root / f"casa-{role}-result.json", result)
        print("t51_representative_casa_complete " + json.dumps(result, allow_nan=False), flush=True)
        if not completed:
            raise ValueError(f"CASA {role} did not complete the exact full-product inventory")
        results[role] = result
    save(root / "casa-result.json", {"completed": True, "roles": results})


if __name__ == "__main__":
    run()
