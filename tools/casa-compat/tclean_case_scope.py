#!/usr/bin/env python3
"""Tag every CASA tclean test case NOW or LATER for the tclean work (#980).

Evidence tooling, not a runtime dependency. NOW is what casa-rs's simulation
tasks need plus very common imaging use; LATER is everything else, refused
with an error naming its issue. The split was decided on 2026-11-02 and is
listed under `Decisions:` on the pull request that adds this file.

NOW by definition: every tclean parameter set CASA's simulation tasks can
issue. `simanalyze` images through `simutil.imtclean` (casatasks 6.7.6.14,
`private/simutil.py`), which calls tclean on one MeasurementSet (several are
concatenated first) with `selectdata=False`, gridder `standard` (one field)
or `mosaic` (several), deconvolver `clark`, `specmode` `mfs` (one channel)
or `cube` (several, default `nchan=-1`), `usemask='user'` with a box list or
a mask image, `restoringbeam='common'`, `fastnoise=True`, `restart=True`,
`calcres`/`calcpsf=True`, `pbmask=0.0`, a single `startmodel` image, and the
caller's `imsize`, `cell`, `phasecenter`, `niter`, `threshold`, `pbcor`,
`uvtaper`, `weighting` (`natural`, `uniform` or `briggs`) and `stokes` (`I`,
`IQUV`, `RRLL`, `QU`, `YY`, `RR`, `IV`, `XX`, `XXYY`, `LL`). `simalma` calls
`simanalyze` with `stokes='I'` on its 12 m, 7 m and concatenated
MeasurementSets. So a cube mosaic weighted `uniform` or `briggs` (with
`mosweight` and `perchanweightdensity` at their defaults, True) and a
full-polarization mosaic are NOW, though they were LATER by default.

Very common use, also NOW: single field and mosaic, ALMA 12 m + 7 m lists;
mfs, mtmfs (nterms >= 2), cube and cubedata over one or several windows and
MeasurementSets; gridders standard, wproject, mosaic and awproject as far as
casa-rs covers them; hogbom, clark, multiscale and mtmfs; natural, uniform,
briggs, briggsbwtaper and the other weightings; perchanweightdensity;
uvtaper; user masks (boxes and CRTF regions) and auto-multithresh, cubes and
Stokes planes included; thresholds and cycle controls; restoringbeam;
pbcor; restart, calcres and calcpsf; savemodel; data selection; mosweight;
usepointing.

A case is LATER when any of its calls matches a LATER family below; each
family gives its reason. Everything else is NOW. Cases CASA's own suite skips
are tagged too and marked `casa_skip`; the matrix
(`tclean_case_matrix.py`) leaves them out, and the totals count both. Usage:

    tclean_case_scope.py --cases resources/casa-compat/tclean-casa-test-cases.json \\
        --output resources/casa-compat/tclean-case-scope.json
"""

import argparse
import collections
import hashlib
import json
import pathlib

OWNER_MOVING = "owner decision 2026-11-02: moving sources are an infrequent use case"
NOT_SIMULATION = "no simulation task issues it and it is not common use"


def calls(case):
    """Each call of a case as (vis, params)."""
    if "sequence" in case:
        return [(step["vis"], step["params"]) for step in case["sequence"]]
    return [(case["vis"], case["params"])]


def text(params, name, default=""):
    value = params.get(name, default)
    return value if isinstance(value, str) else json.dumps(value)


def staged(case):
    return set(case.get("stage", {})) if isinstance(case.get("stage"), dict) else set(
        case.get("stage", [])
    )


def moving(vis, params, _case):
    names = json.dumps(vis)
    phasecenter = text(params, "phasecenter")
    return (
        "ephem" in names
        or phasecenter == "TRACKFIELD"
        or phasecenter.endswith(".tab")
        or params.get("specmode") == "cubesource"
    )


def awproject_cache(vis, params, case):
    """awproject with no CASA cache casa-rs can read: one the test lets CASA
    make (an empty name, or a name the test does not stage)."""
    if params.get("gridder") != "awproject":
        return False
    cache = text(params, "cfcache")
    return cache == "" or cache not in staged(case)


def awproject_cube(_vis, params, _case):
    return params.get("gridder") == "awproject" and params.get("specmode") in (
        "cube",
        "cubedata",
        "cubesource",
    )


def awproject_imported_mosaic(vis, params, case):
    cache = text(params, "cfcache")
    return (
        params.get("gridder") == "awproject"
        and "mosaic" in json.dumps(vis)
        and cache != ""
        and cache in staged(case)
    )


def facets_beyond(_vis, params, _case):
    facets = params.get("facets", 1)
    return isinstance(facets, int) and facets > 1 and (
        params.get("gridder") == "awproject"
        or params.get("deconvolver") == "mtmfs"
        or params.get("specmode", "mfs") != "mfs"
    )


def derived_cube_axis(_vis, params, case):
    spw = text(params, "spw")
    cube = params.get("specmode") in ("cube", "cubedata")
    nchan = params.get("nchan", -1)
    selected = ":" in spw
    stride = "^" in spw
    start, width = text(params, "start"), text(params, "width")
    velocity_width = params.get("veltype") == "optical" and width not in ("", "1")
    return cube and (
        stride
        or (nchan == -1 and (selected or start not in ("", "0") or width not in ("", "1")))
        or velocity_width
    )


SPECTRAL = ("spw", "nchan", "start", "width", "specmode", "outframe", "veltype")


def start_model_spectral_regrid(_vis, params, case):
    """A start model made by an earlier call on another spectral grid."""
    model = params.get("startmodel")
    cube = params.get("specmode") in ("cube", "cubedata")
    if "sequence" not in case or not cube or not isinstance(model, str) or not model:
        return False
    for step in case["sequence"]:
        if model.startswith(step["imagename"] + ".") and step["params"] is not params:
            return any(
                json.dumps(step["params"].get(name)) != json.dumps(params.get(name))
                for name in SPECTRAL
            )
    return False


# (family, predicate, reason). Order is the order reasons are listed.
LATER = [
    ("moving-sources", moving, OWNER_MOVING),
    (
        "gridder-awp2-awphpg-imagemosaic",
        lambda v, p, c: p.get("gridder") in ("awp2", "awphpg", "imagemosaic"),
        "gridders awp2, awphpg and imagemosaic: " + NOT_SIMULATION,
    ),
    (
        "specmode-mvc",
        lambda v, p, c: p.get("specmode") == "mvc",
        "specmode mvc: " + NOT_SIMULATION,
    ),
    (
        "deconvolver-asp-mem-clarkstokes",
        lambda v, p, c: p.get("deconvolver") in ("asp", "mem", "clarkstokes"),
        "deconvolvers asp, mem and clarkstokes: " + NOT_SIMULATION,
    ),
    (
        "pb-mask",
        lambda v, p, c: p.get("usemask") == "pb" or float(p.get("pbmask", 0.0) or 0.0) > 0.0,
        "usemask='pb' and pbmask: simanalyze passes pbmask=0.0 with usemask='user', "
        "and no common-use evidence moved it (decided 2026-11-02)",
    ),
    (
        "normtype-pbsquare",
        lambda v, p, c: p.get("normtype") == "pbsquare",
        "normtype pbsquare: " + NOT_SIMULATION,
    ),
    (
        "psfphasecenter",
        lambda v, p, c: text(p, "psfphasecenter") != "",
        "psfphasecenter: " + NOT_SIMULATION,
    ),
    (
        "facets-beyond-single-term-mfs",
        facets_beyond,
        "facets with AW projection, Taylor terms or cubes: " + NOT_SIMULATION,
    ),
    (
        "projection",
        lambda v, p, c: text(p, "projection", "SIN") not in ("SIN", ""),
        "projections other than SIN: " + NOT_SIMULATION,
    ),
    (
        "mtmfs-nterms-1",
        lambda v, p, c: p.get("deconvolver") == "mtmfs" and p.get("nterms", 2) == 1,
        "nterms=1 with mtmfs: " + NOT_SIMULATION,
    ),
    (
        "polarized-taylor",
        lambda v, p, c: p.get("deconvolver") == "mtmfs" and text(p, "stokes", "I") != "I",
        "Taylor images of Stokes other than I: " + NOT_SIMULATION,
    ),
    (
        "cubic-interpolation",
        lambda v, p, c: p.get("interpolation") == "cubic",
        "cubic interpolation: " + NOT_SIMULATION,
    ),
    (
        "start-model-list",
        lambda v, p, c: isinstance(p.get("startmodel"), list) and len(p["startmodel"]) > 1,
        "start-model lists: simanalyze passes one image; " + NOT_SIMULATION,
    ),
    (
        "start-model-spectral-regrid",
        start_model_spectral_regrid,
        "a start model regridded along its spectral axis: " + NOT_SIMULATION,
    ),
    (
        "pa-steps",
        lambda v, p, c: float(p.get("computepastep", 360.0)) != 360.0
        or float(p.get("rotatepastep", 360.0)) != 360.0,
        "computepastep and rotatepastep: " + NOT_SIMULATION,
    ),
    (
        "awproject-cache-generation",
        awproject_cache,
        "awproject with no CASA CF cache to read: generating one is not native yet",
    ),
    (
        "awproject-cube",
        awproject_cube,
        "AW projection of a cube: awproject stays at its current coverage",
    ),
    (
        "awproject-imported-mosaic",
        awproject_imported_mosaic,
        "AW mosaics on imported CASA caches: awproject stays at its current coverage",
    ),
    (
        "derived-cube-axes",
        derived_cube_axis,
        "tabular, strided and derived cube axes (nchan=-1 with a selection, "
        "start or width; strided channels; optical-velocity widths): " + NOT_SIMULATION,
    ),
]

# What NOW includes that the default list had LATER, with its evidence.
MOVED_TO_NOW = [
    {
        "family": "mosweight-per-channel-density",
        "evidence": "simanalyze images several pointings with gridder='mosaic' and a "
        "multi-channel MeasurementSet as specmode='cube', weighting 'uniform' or "
        "'briggs' allowed, mosweight and perchanweightdensity at tclean's defaults "
        "(True): BriggsCubeWeightor's per-field density (#818)",
    },
    {
        "family": "full-polarization-mosaic",
        "evidence": "simanalyze passes the caller's stokes (IQUV, QU, IV and the "
        "correlations) to gridder='mosaic', and simobserve simulates all four "
        "correlations for an IQUV sky model",
    },
]


def sha256(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def classify(case):
    families = []
    for vis, params in calls(case):
        for family, predicate, _ in LATER:
            if family not in families and predicate(vis, params, case):
                families.append(family)
    return families


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--cases", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    matrix = json.loads(pathlib.Path(args.cases).read_text())
    cases = {}
    counts = collections.Counter()
    by_family = collections.Counter()
    for name, case in sorted(matrix["cases"].items()):
        families = classify(case)
        scope = "LATER" if families else "NOW"
        counts[scope] += 1
        for family in families:
            by_family[family] += 1
        entry = {"scope": scope}
        if families:
            entry["families"] = families
        if "expect_error" in case:
            entry["expect_error"] = case["expect_error"]
        if "casa_skip" in case:
            entry["casa_skip"] = True
        else:
            counts["matrix " + scope] += 1
        cases[name] = entry
    output = {
        "source": {
            "tool": "tools/casa-compat/tclean_case_scope.py",
            "case_file": args.cases,
            "case_file_sha256": sha256(args.cases),
            "decided": "2026-11-02",
        },
        "rule": "A case is LATER when any of its calls matches a LATER family; "
        "otherwise NOW (simulation calls and very common use).",
        "later_families": [
            {"family": family, "reason": reason, "cases": by_family[family]}
            for family, _, reason in LATER
        ],
        "moved_to_now": MOVED_TO_NOW,
        "totals": {
            "cases": len(cases),
            "NOW": counts["NOW"],
            "LATER": counts["LATER"],
            "matrix_cases": counts["matrix NOW"] + counts["matrix LATER"],
            "matrix_NOW": counts["matrix NOW"],
            "matrix_LATER": counts["matrix LATER"],
        },
        "cases": cases,
    }
    pathlib.Path(args.output).write_text(json.dumps(output, indent=1) + "\n")
    print(json.dumps(output["totals"]))
    for item in output["later_families"]:
        print(f"{item['cases']:4d}  {item['family']}")


if __name__ == "__main__":
    main()
