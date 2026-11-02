#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Rank the tclean parameters casa-rs refuses by how many CASA test cases hit them (#980).

Evidence tooling, not a runtime dependency. It reads the case matrix
`extract_tclean_test_cases.py` makes from CASA's tclean tests and, for every
resolved case, the tclean refusals casa-rs would raise, from two sources:

* the catalog's `tclean` surface (`parameter-surfaces.json`): a binding
  marked `unimplemented` refuses any value but its default where its
  `applies_when` holds (`casa-task-runtime/src/unimplemented.rs`), one marked
  `unimplemented_forms` refuses the forms it names, and the value domains of
  `parameter-catalog.json` refuse values of the wrong shape;
* `crates/casa-imaging-application/src/tclean.rs`: every
  `TcleanRefusal::not_yet` and `TcleanRefusal::new` site, mirrored here
  condition by condition (this file names the function of each). A site whose
  message says CASA's tclean refuses the same value is a parity refusal, not
  a gap.

Three more limits casa-rs states before imaging are added and labelled apart:
the restart surface note (`restart`, #944), which bounds a restart into an
existing image to the same grid and family of products, refuses a new or
automatic mask over an existing one when cleaning and outlier fields, and
reads the PSF and residual (`calcpsf`, `calcres`) only with the standard and
W-projection gridders on one facet and only where an earlier call left them
(`casa-imaging-application/src/restart.rs`); the cfcache surface
note, by which awproject reads an existing CASA cache and never writes one,
so a cache the test does not stage from casatestdata is missing; and the
availability gate (`casa-imaging-application/src/availability.rs`, `check`)
for what the parameters alone decide (a polarized Taylor basis, facets beyond
single-term MFS with the standard gridder, AW projection of a cube or of a
non-square grid).

The output lists the gaps (every source but the parity refusals) most-hit
first, with how many cases each blocks alone (`sole_blocker`) and examples;
the parity refusals apart; the cases where CASA's test expects an error,
which are not counted; and every case's refusals.

Rust's reader stops at its first refusal; this mirror collects them all, so a
case lists every gap it would hit and the table can say how many cases each
gap blocks alone. Semantic equality with defaults follows the concepts'
normalizations approximately (trimmed text, numbers by value, a scalar for a
one-item list). It does not run casa-rs, so refusals made later (the imaging
compiler, the restart and start-model laws) are outside it.

    python3 -I tools/casa-compat/classify_tclean_test_cases.py \\
        --cases resources/casa-compat/tclean-casa-test-cases.json \\
        --output resources/casa-compat/tclean-casa-test-gaps.json
"""

import argparse
import collections
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SURFACES = ROOT / "crates/casa-provider-contracts/resources/parameter-surfaces.json"
CATALOG = ROOT / "crates/casa-provider-contracts/resources/parameter-catalog.json"
TCLEAN_RS = ROOT / "crates/casa-imaging-application/src/tclean.rs"

NOT_YET = "tclean.rs not_yet"
CASA_RS = "tclean.rs refusal (casa-rs only)"
PARITY = "tclean.rs refusal CASA shares"
DEFAULT_ONLY = "surface: unimplemented (default only)"
FORMS = "surface: unimplemented_forms"
DOMAIN = "catalog: value domain"
NOTE = "surface note: restart (#944)"
CF_NOTE = "surface note: cfcache"
GATE = "availability.rs gate"


def plain(value):
    """A catalog ParameterValue as Python."""
    kind = value.get("kind")
    if kind == "array":
        return [plain(item) for item in value["value"]]
    return value.get("value")


def same(given, default):
    """Whether `given` is `default` as the concept normalizes it, roughly."""
    if isinstance(default, list):
        given = given if isinstance(given, list) else [given]
        given = (
            [item for item in given if item != ""]
            if all(isinstance(item, str) for item in default)
            else given
        )
        default = [item for item in default if item != ""]
        return len(given) == len(default) and all(map(same, given, default))
    if isinstance(given, list) and len(given) == 1:
        given = given[0]
    if isinstance(default, bool) or isinstance(given, bool):
        return isinstance(given, bool) and given == default
    if isinstance(default, (int, float)):
        try:
            return float(given) == float(default)
        except (TypeError, ValueError):
            return False
    if isinstance(default, str):
        return isinstance(given, (str, int, float)) and str(given).strip() == default.strip()
    return given == default


class Surface:
    def __init__(self):
        surfaces = json.loads(SURFACES.read_text())["surfaces"]
        self.tclean = next(surface for surface in surfaces if surface["id"] == "tclean")
        concepts = {c["id"]: c for c in json.loads(CATALOG.read_text())["concepts"]}
        self.bindings = {binding["name"]: binding for binding in self.tclean["bindings"]}
        self.defaults = {
            name: plain(b["default"]["value"])
            for name, b in self.bindings.items()
            if b["default"]["kind"] == "literal"
        }
        self.concepts = {name: concepts[b["concept"]["id"]] for name, b in self.bindings.items()}

    def predicate(self, predicate, values):
        kind = predicate["kind"]
        if kind == "always":
            return True
        if kind == "never":
            return False
        if kind == "equals":
            return same(values.get(predicate["parameter"]), plain(predicate["value"]))
        if kind == "not":
            return not self.predicate(predicate["predicate"], values)
        if kind == "any":
            return any(self.predicate(item, values) for item in predicate["predicates"])
        if kind == "all":
            return all(self.predicate(item, values) for item in predicate["predicates"])
        return True  # an undecided predicate applies, as in unimplemented.rs

    def domain(self, value, domain, normalization):
        """Why `value` is outside `domain`, or None."""
        kind = domain["kind"]
        number = isinstance(value, (int, float)) and not isinstance(value, bool)
        if kind == "array":
            items = value if isinstance(value, list) else None
            if items is None:
                return (
                    self.domain(value, domain["element"], normalization)
                    if domain.get("allow_scalar")
                    else "not a list"
                )
            if len(items) < domain.get("min_items", 0) or (
                domain.get("max_items") is not None and len(items) > domain["max_items"]
            ):
                return f"a list of {len(items)} items"
            for item in items:
                problem = self.domain(item, domain["element"], normalization)
                if problem:
                    return problem
            return None
        if isinstance(value, list):
            return "a list"
        if kind in ("string", "path"):
            ok = isinstance(value, str) or (normalization == "number_as_text" and number)
        elif kind == "bool":
            ok = isinstance(value, bool)
        elif kind == "integer":
            ok = isinstance(value, int) and not isinstance(value, bool)
        elif kind == "float":
            ok = number
        elif kind == "quantity":
            ok = isinstance(value, str) or number
        elif kind == "choice":
            if isinstance(value, str) and value not in domain["values"]:
                return f"{value!r}, which is not one of CASA's values"
            ok = isinstance(value, str)
        elif kind == "optional":
            ok = value in domain.get("states", []) or not self.domain(
                value, domain["value"], normalization
            )
        else:
            ok = True
        return None if ok else f"a {type(value).__name__}"


class Refusals:
    """The refusals one tclean call meets, as (key, source, issue) rows."""

    def __init__(self, surface, params, vis, imagename, staged=()):
        self.surface = surface
        self.staged = set(staged)
        self.given = dict(params, vis=vis, imagename=imagename)
        self.values = dict(surface.defaults, **self.given)
        self.rows = []
        self.bad_shape = set()

    def add(self, key, source, issue=None):
        row = {"reason": key, "source": source, "issue": issue}
        if row not in self.rows:
            self.rows.append(row)

    def catalog(self):
        for name, value in self.given.items():
            binding = self.surface.bindings.get(name)
            if binding is None:
                self.add(f"{name}: not a tclean parameter", DOMAIN)
                self.bad_shape.add(name)
                continue
            concept = self.surface.concepts[name]
            problem = self.surface.domain(
                value, concept["value_domain"], concept.get("normalization", {}).get("kind")
            )
            if problem:
                self.add(f"{name} given as {problem}", DOMAIN)
                self.bad_shape.add(name)
            marker = binding.get("unimplemented")
            if marker and name not in self.bad_shape:
                applies = self.surface.predicate(
                    marker.get("applies_when", {"kind": "always"}), self.values
                )
                if applies and not same(value, self.surface.defaults[name]):
                    when = self.when(marker.get("applies_when"))
                    default = json.dumps(self.surface.defaults[name])
                    self.add(
                        f"{name}: only the default {default} is implemented{when}",
                        DEFAULT_ONLY,
                        marker.get("issue"),
                    )
        vis = self.given.get("vis")
        if isinstance(vis, list) and len(vis) > 1:
            self.add("vis: a list of more than one MeasurementSet", FORMS, 980)
        for name in ("scan", "observation"):
            text = self.given.get(name)
            if isinstance(text, str) and text != "" and not selector_ids(text):
                self.add(f"{name}: selector beyond numeric IDs, lists and a~b ranges", FORMS, 980)

    @staticmethod
    def when(predicate):
        if not predicate:
            return ""
        text = json.dumps(predicate)
        parameters = sorted(set(re.findall(r'"parameter": "(\w+)"', text)))
        values = re.findall(r'"value": "([\w-]+)"', text)
        negated = predicate.get("kind") == "not"
        relation = " is not " if negated else " is "
        return f" (where {'/'.join(parameters)}{relation}{'/'.join(values)})"

    # tclean.rs, read() ----------------------------------------------------

    def text(self, name):
        value = self.values.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            normal = self.surface.concepts[name].get("normalization", {}).get("kind")
            if normal == "number_as_text":
                return str(value)
        if isinstance(value, str):
            return value.strip()
        if name not in self.bad_shape:
            self.add(f"{name}: expected text (Tclean::text)", CASA_RS)
        return None

    def texts(self, name):
        value = self.values.get(name)
        items = value if isinstance(value, list) else [value]
        if not all(isinstance(item, str) for item in items):
            if name not in self.bad_shape:
                self.add(f"{name}: expected text (Tclean::texts)", CASA_RS)
            return []
        return [item.strip() for item in items if item.strip()]

    def integer(self, name):
        value = self.values.get(name)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if name not in self.bad_shape:
            self.add(f"{name}: expected an integer (Tclean::integer)", CASA_RS)
        return None

    def flag(self, name):
        value = self.values.get(name)
        return value if isinstance(value, bool) else None

    def modes(self):
        """`Tclean::modes`."""
        specmode = self.text("specmode")
        if specmode == "mvc":
            self.add("specmode='mvc'", NOT_YET, 823)
        cube = specmode not in ("cont", "mfs")
        planes = self.values.get("wprojplanes")
        name = self.text("gridder")
        gridder = name
        if name in ("standard", "ft", "gridft"):
            gridder = "standard"
        elif name in ("wproject", "wprojectft", "widefield"):
            if isinstance(planes, int) and (planes > 1 or planes == -1):
                gridder = "wproject"
            elif planes == 1:
                gridder = "standard"
            else:
                self.add("wprojplanes: wproject needs wprojplanes>1 or -1", PARITY)
        elif name in ("mosaic", "mosaicft", "ftmosaic"):
            gridder = "mosaic"
        elif name == "awprojectft":
            self.add("gridder='awprojectft' (CASA's other name for awproject)", NOT_YET, 980)
        elif name != "awproject":
            self.add(f"gridder={name!r}", NOT_YET, 980)
        deconvolver = self.text("deconvolver")
        if deconvolver not in ("hogbom", "clark", "multiscale", "mtmfs"):
            issue = 223 if deconvolver == "asp" else 823
            self.add(f"deconvolver={deconvolver!r}", NOT_YET, issue)
        weighting = self.text("weighting")
        if weighting not in ("natural", "uniform", "briggs", "briggsbwtaper"):
            self.add(f"weighting={weighting!r}", NOT_YET, 980)
        if weighting == "briggsbwtaper" and not cube:
            self.add("weighting='briggsbwtaper' with specmode='mfs'", PARITY)
        usemask = self.text("usemask")
        if usemask == "pb":
            self.add("usemask='pb'", NOT_YET, 823)
        return {
            "cube": cube,
            "gridder": gridder,
            "per_field_density": name == "mosaic",
            "deconvolver": deconvolver,
            "weighting": weighting,
            "density": weighting != "natural",
            "user_mask": usemask == "user",
        }

    def selection(self):
        """`Tclean::selection`."""
        column = (self.text("datacolumn") or "").lower()
        if column not in ("corrected", "data", "observed"):
            self.add(f"datacolumn={column!r} (neither data nor corrected)", CASA_RS)
        for name in ("field", "spw", "uvrange", "intent", "scan", "observation"):
            self.text(name)

    def geometry(self):
        """`Tclean::geometry`."""
        if self.values.get("cell") == []:
            self.add("cell: empty", PARITY)
        self.text("phasecenter")

    def spectral(self, modes):
        """`Tclean::spectral` and `refuse_derived_extent`."""
        nchan = self.integer("nchan")
        if modes["cube"] and isinstance(nchan, int) and nchan < 1 and nchan != -1:
            self.add("nchan below 1 (other than -1)", CASA_RS)
        start, width = self.text("start"), self.text("width")
        outframe, veltype = self.text("outframe"), self.text("veltype")
        restfreq = self.texts("restfreq")
        interpolation = self.text("interpolation")
        frame = (outframe or "").upper()
        valid_frame = frame in ("LSRK", "BARY", "TOPO", "REST")
        if not modes["cube"]:
            if not valid_frame:
                self.add(f"outframe={outframe!r}", NOT_YET, 980)
            elif frame != "LSRK":
                self.add("outframe other than LSRK with specmode='mfs'", NOT_YET, 980)
            if restfreq:
                self.add("restfreq with specmode='mfs'", NOT_YET, 980)
            return
        spw = self.text("spw") or ""
        selected = ":" in spw
        if nchan == -1 and (selected or width not in ("", "1", None) or start not in ("", None)):
            self.add(
                "nchan=-1 with a channel selection, start or width (derived cube extent)",
                NOT_YET,
                980,
            )
        if selected and start == "":
            self.add("start='' with a channel selection in spw", NOT_YET, 980)
        if not valid_frame:
            self.add(f"outframe={outframe!r}", NOT_YET, 980)
        if (veltype or "").lower() not in ("radio", "optical", "z", "beta", "relativistic", "true"):
            self.add(f"veltype={veltype!r}", NOT_YET, 980)
        if len(restfreq) > 1:
            self.add("restfreq: a list of rest frequencies", NOT_YET, 980)
        if interpolation == "cubic":
            self.add("interpolation='cubic'", NOT_YET, 823)

    def gridding(self, modes):
        """`Tclean::gridding` and `aw_terms`."""
        gridder = modes["gridder"]
        facets = self.integer("facets")
        if gridder == "mosaic" and facets not in (1, None):
            self.add("facets>1 with the mosaic gridder", PARITY)
        if gridder in ("wproject", "awproject") and facets not in (1, None):
            self.add(f"facets>1 with gridder={gridder!r}", NOT_YET, 980)
        planes = self.integer("wprojplanes")
        if (
            gridder in ("wproject", "awproject")
            and isinstance(planes, int)
            and planes != -1
            and planes < 1
        ):
            self.add("wprojplanes below 1 (other than -1)", CASA_RS)
        normtype = self.text("normtype")
        if gridder in ("mosaic", "awproject") and normtype == "pbsquare":
            self.add("normtype='pbsquare'", NOT_YET, 823)
        per_channel = (
            modes["cube"]
            and self.flag("perchanweightdensity") is not False
            and modes["weighting"] != "superuniform"
        )
        if (
            self.flag("mosweight")
            and modes["per_field_density"]
            and modes["density"]
            and per_channel
        ):
            self.add(
                "mosweight=True with gridder='mosaic' and a cube's per-channel density",
                NOT_YET,
                818,
            )
        fixed = {
            "awproject": {"aterm": True, "psterm": False, "wbawp": True, "conjbeams": True},
        }.get(gridder, {})
        for name, required in fixed.items():
            value = self.flag(name)
            if value is not None and value != required:
                self.add(f"{name}={value} with gridder={gridder!r}", NOT_YET, 980)
        cfcache = self.text("cfcache")
        if (
            gridder == "awproject"
            and self.values.get("aw_cf_source") == "casa-import"
            and not cfcache
        ):
            self.add(
                "cfcache='' with gridder='awproject' (casa-rs reads an existing CASA "
                "CF cache by default)",
                CASA_RS,
            )
        elif (
            gridder == "awproject"
            and self.values.get("aw_cf_source") == "casa-import"
            and cfcache not in self.staged
        ):
            self.add(
                "cfcache: no existing CASA CF cache staged (the test lets CASA make it; "
                "casa-rs reads one and never writes it)",
                CF_NOTE,
            )

    def deconvolution(self, modes):
        """`Tclean::deconvolution`."""
        if modes["deconvolver"] == "mtmfs" and self.integer("nterms") == 1:
            self.add("nterms=1 with deconvolver='mtmfs'", NOT_YET, 980)
        beam = self.texts("restoringbeam")
        if beam not in ([], ["common"]):
            self.add("restoringbeam: a beam given by its axes", NOT_YET, 980)

    def iteration(self):
        """`Tclean::iteration`."""
        if self.values.get("savemodel") == "virtual":
            self.add("savemodel='virtual'", NOT_YET, 823)
        self.integer("cycleniter")

    def masking(self, modes):
        """`Tclean::masking` and `user_mask`."""
        entries = self.texts("mask")
        if not modes["user_mask"] or not entries:
            return
        text = ",".join(entries)
        if "[" in text:
            if not crtf_pixel_boxes(text):
                self.add("mask: a region other than integer pixel boxes", NOT_YET, 980)
        elif pixel_box_list(text) is None and len(entries) > 1:
            self.add("mask: more than one mask image", NOT_YET, 980)

    def tclean_rs(self):
        modes = self.modes()
        self.selection()
        self.geometry()
        self.spectral(modes)
        self.gridding(modes)
        self.deconvolution(modes)
        self.iteration()
        self.masking(modes)
        self.gate(modes)

    def gate(self, modes):
        """`availability::check`, for what the parameters alone decide."""
        taylor = modes["deconvolver"] == "mtmfs"
        if taylor and str(self.values.get("stokes", "I")).strip() != "I":
            self.add("a Taylor basis (mtmfs) of polarizations other than Stokes I", GATE)
        facets = self.values.get("facets")
        faceted = isinstance(facets, int) and facets > 1
        if faceted and modes["gridder"] in ("standard", "wproject") and (taylor or modes["cube"]):
            self.add("facets beyond single-term MFS", GATE)
        if modes["gridder"] == "awproject":
            if modes["cube"]:
                self.add("AW projection of a cube (#719)", GATE, 719)
            imsize, cell = self.values.get("imsize"), self.values.get("cell")
            imsize = imsize if isinstance(imsize, list) else [imsize]
            cell = cell if isinstance(cell, list) else [cell]
            if len(set(map(str, imsize))) > 1 or len(set(map(str, cell))) > 1:
                self.add("AW projection of a non-square grid", GATE)


def selector_ids(text):
    """CASA's numeric scan/observation selectors casa-rs takes (`a`, `a,b`, `a~b`)."""
    for part in text.split(","):
        match = re.fullmatch(r"\s*(\d+)\s*(?:~\s*(\d+)\s*)?", part)
        if not match or (match.group(2) and int(match.group(2)) <= int(match.group(1))):
            return False
    return True


def pixel_box_list(text):
    boxes = []
    for corners in text.split(";"):
        try:
            values = [int(corner.strip()) for corner in corners.split(",")]
        except ValueError:
            return None
        if len(values) != 4:
            return None
        boxes.append(values)
    return boxes


def crtf_pixel_boxes(text):
    """Whether `crtf_pixel_boxes` reads `text` (only `box[[Xpix,Ypix],[Xpix,Ypix]]`)."""
    for line in (line.strip() for line in text.splitlines()):
        if line.startswith("#"):
            continue
        rest = line
        while rest:
            if not rest.startswith("box"):
                return False
            body = rest[3:].lstrip()
            if not body.startswith("[["):
                return False
            inside, found, after = body[2:].partition("]]")
            if not found:
                return False
            corners = [corner.strip().strip("[] ") for corner in inside.split(",")]
            if len(corners) != 4:
                return False
            for corner in corners:
                number = corner.strip()[:-3].strip() if corner.strip().endswith("pix") else None
                try:
                    if number is None or float(number) != int(float(number)):
                        return False
                except ValueError:
                    return False
            rest = after.lstrip(", \t")
    return True


def mask_given(params):
    """Whether a call names a user mask (tclean reads `mask` with usemask='user')."""
    mask = params.get("mask", "")
    return params.get("usemask", "user") == "user" and mask not in ("", [], [""])


def restart_limits(steps, index, step):
    """The restart surface note (`restart.rs`): what a call continuing on an
    earlier call's products, or reading its PSF or residual, is refused."""
    earlier = [s for s in steps[:index] if s["imagename"] == step["imagename"]]
    params = step["params"]
    rows = []
    reads = params.get("calcpsf", True) is not True or params.get("calcres", True) is not True
    if reads:
        gridder = params.get("gridder", "standard")
        facets = int(params.get("facets", 1) or 1)
        if params.get("outlierfile") or gridder in ("mosaic", "mosaicft", "ftmosaic", "awproject",
                                                    "awp2", "awphpg") or facets > 1:
            rows.append("calcpsf=False or calcres=False with mosaic, A-projection, facets or "
                        "outlier fields")
    continuing = earlier and params.get("restart", True) is True
    if reads and not continuing:
        if params.get("calcpsf", True) is not True or (
            params.get("calcres", True) is not True and int(params.get("niter", 0) or 0) > 0
        ):
            rows.append("calcpsf=False or calcres=False with no earlier PSF or residual")
    if not continuing:
        return [{"reason": row, "source": NOTE, "issue": 944} for row in rows]
    before = earlier[-1]["params"]
    if params.get("outlierfile"):
        rows.append("restart into an existing image with outlier fields")
    family = lambda p: p.get("deconvolver") == "mtmfs"  # noqa: E731
    if family(before) != family(params):
        rows.append("restart into an existing image of another deconvolver family")
    grid = ("imsize", "cell", "phasecenter", "stokes", "specmode", "nchan", "start", "width")
    if any(before.get(key) != params.get(key) for key in grid):
        rows.append("restart into an existing image on a different grid")
    masked = any(int(s["params"].get("niter", 0) or 0) > 0 for s in earlier)
    if masked and int(params.get("niter", 0) or 0) > 0:
        if params.get("usemask") == "auto-multithresh":
            rows.append("auto-multithresh over an existing CLEAN mask")
        elif mask_given(params):
            rows.append("a new mask over an existing CLEAN mask")
    return [{"reason": row, "source": NOTE, "issue": 944} for row in rows]


def classify(surface, name, case):
    steps = case.get("sequence") or [
        {
            "call": int(name.rsplit(".", 1)[1]),
            "vis": case["vis"],
            "imagename": case["imagename"],
            "params": case["params"],
        }
    ]
    rows = []
    for index, step in enumerate(steps):
        refusals = Refusals(
            surface, step["params"], step["vis"], step["imagename"], case.get("stage", ())
        )
        refusals.catalog()
        refusals.tclean_rs()
        found = refusals.rows + restart_limits(steps, index, step)
        for row in found:
            row = dict(row, call=step.get("call", 1))
            rows.append(row)
    return rows


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def revision():
    try:
        return subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def table(cases, results, sources, limit=5):
    hits, sole, examples, meta = (
        collections.Counter(),
        collections.Counter(),
        collections.defaultdict(list),
        {},
    )
    for name, rows in results.items():
        gaps = {row["reason"] for row in rows if row["source"] in sources}
        all_gaps = {row["reason"] for row in rows if row["source"] != PARITY}
        for row in rows:
            if row["source"] in sources:
                meta[row["reason"]] = (row["source"], row["issue"])
        for reason in gaps:
            hits[reason] += 1
            sole[reason] += all_gaps == {reason}
            if len(examples[reason]) < limit:
                examples[reason].append(name)
    return [
        {
            "reason": reason,
            "source": meta[reason][0],
            "issue": meta[reason][1],
            "cases": count,
            "sole_blocker": sole[reason],
            "examples": examples[reason],
        }
        for reason, count in sorted(hits.items(), key=lambda item: (-item[1], item[0]))
    ]


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--cases", type=Path, required=True, help="case-matrix JSON")
    parser.add_argument("--output", type=Path, required=True, help="gap-table JSON")
    args = parser.parse_args()
    surface = Surface()
    matrix = json.loads(args.cases.read_text())
    cases = matrix["cases"]
    results = {name: classify(surface, name, case) for name, case in cases.items()}
    gap_sources = {NOT_YET, CASA_RS, DEFAULT_ONLY, FORMS, DOMAIN, NOTE, CF_NOTE, GATE}
    expecting = {name for name, case in cases.items() if "expect_error" in case}
    counted = {name: rows for name, rows in results.items() if name not in expecting}
    blocked = {
        name for name, rows in counted.items() if any(row["source"] in gap_sources for row in rows)
    }
    parity_only = {name for name, rows in counted.items() if rows and name not in blocked}
    report = {
        "source": {
            "classifier": "tools/casa-compat/classify_tclean_test_cases.py",
            "case_file": str(args.cases),
            "case_file_sha256": sha256(args.cases),
            "casa_rs_revision": revision(),
            "parameter_surfaces_sha256": sha256(SURFACES),
            "parameter_catalog_sha256": sha256(CATALOG),
            "tclean_rs_sha256": sha256(TCLEAN_RS),
            "tclean_contract_version": surface.tclean["contract_version"],
        },
        "totals": {
            "resolved_cases": len(cases),
            "cases_where_casa_expects_an_error": len(expecting),
            "cases_counted": len(counted),
            "cases_with_no_refusal": len(counted) - len(blocked) - len(parity_only),
            "cases_with_a_gap": len(blocked),
            "cases_with_only_refusals_casa_shares": len(parity_only),
            "single_call_cases_with_no_refusal": sum(
                1
                for name in counted
                if name not in blocked and name not in parity_only and "sequence" not in cases[name]
            ),
        },
        "gaps": table(cases, counted, gap_sources),
        "refusals_casa_shares": table(cases, counted, {PARITY}),
        "cases_where_casa_expects_an_error": {
            name: {"expect_error": cases[name]["expect_error"], "casa_rs": results[name]}
            for name in sorted(expecting)
        },
        "cases": {
            name: {"runs_without_refusal": not rows, "refusals": rows}
            for name, rows in results.items()
        },
    }
    args.output.write_text(json.dumps(report, indent=1) + "\n")
    print(json.dumps(report["totals"], indent=1))
    for row in report["gaps"]:
        print(
            f"{row['cases']:4d} {row['sole_blocker']:4d}  {row['reason']}  [{row['source']}"
            f"{', #' + str(row['issue']) if row['issue'] else ''}]"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
