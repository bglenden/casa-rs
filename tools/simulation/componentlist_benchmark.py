#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Time the complete cl surface in one Python runtime; build casars --release first.

Run both backends with CASA 6.7.6.14's Python, with the casars source package
on PYTHONPATH. Setup, validation and cleanup are outside each timed call.
Each sample uses fresh tool state. I/O cases use a small CASA-written table.
"""
import argparse
import gc
import json
import logging
import shutil
import statistics
import subprocess
import sys
import platform
import tempfile
import time
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--backend", choices=["casa", "casars"], required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--workdir", type=Path, required=True)
parser.add_argument("--repeat", type=int, default=100)
parser.add_argument("--samples", type=int, default=5)
parser.add_argument("--cases", nargs="*")
args = parser.parse_args()

from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist as casa_cl, version_string
from casatasks import casalog
casalog.filter("SEVERE")
assert version_string() == "6.7.6-14", version_string()
if args.backend == "casa":
    cltool = casa_cl
    module = "casatools 6.7.6-14"
else:
    from casars.tools import componentlist as cltool
    import casars._core
    module = casars._core.__file__
logging.disable(logging.CRITICAL)
args.workdir.mkdir(parents=True, exist_ok=True)


def populated():
    cl = cltool()
    cl.addcomponent(flux=[2, 0.5, 0.25, 0.125], dir="J2000 10deg 20deg", label="source", shape="Gaussian", majoraxis="4arcsec", minoraxis="2arcsec", positionangle="30deg", freq="LSRK 1.4GHz")
    cl.addcomponent(flux=1, dir="J2000 20deg 30deg", label="other", freq="LSRK 1.4GHz")
    return cl


with tempfile.TemporaryDirectory(prefix="componentlist-bench-", dir=args.workdir) as temporary:
    root = Path(temporary)
    fixture = str(root / "input.cl")
    source = casa_cl()
    source.addcomponent(flux=[2, 0.5, 0.25, 0.125], dir="J2000 10deg 20deg", label="source", shape="Gaussian", majoraxis="4arcsec", minoraxis="2arcsec", positionangle="30deg", freq="LSRK 1.4GHz")
    source.addcomponent(flux=1, dir="J2000 20deg 30deg", label="other", freq="LSRK 1.4GHz")
    component = source.getcomponent(0)
    record = source.torecord()
    source.rename(fixture)
    source.putkeyword("metadata", "benchmark")
    source.done()

    def attached(writable=False):
        cl = cltool()
        cl.open(fixture, nomodify=not writable)
        return cl

    def removed():
        cl = populated()
        cl.remove([0])
        return cl

    def selected():
        cl = populated()
        cl.select([0])
        return cl

    def cleanup(cl):
        if cl is not None:
            cl.done()

    # Entries are (setup, timed operation, disk I/O). Every public method is
    # represented; record and table concatenation are measured separately.
    cases = {}
    def add(name, operation, setup=populated, disk=False):
        cases[name] = (setup, operation, disk)

    for name in ["getcomponent", "getfluxvalue", "getfluxerror", "getfluxunit", "getfluxpol", "getfreq", "getfreqframe", "getfrequnit", "getfreqvalue", "getlabel", "getrefdir", "getrefdirra", "getrefdirdec", "getrefdirframe", "getshape", "getspectrum", "shapetype", "spectrumtype"]:
        add(name, lambda cl, name=name: getattr(cl, name)(0))
    for name in ["length", "torecord", "indices", "iscomponentlist", "isphysical"]:
        add(name, lambda cl, name=name: getattr(cl, name)())
    add("selected", lambda cl: cl.selected(), selected)
    add("select", lambda cl: cl.select([0]))
    add("deselect", lambda cl: cl.deselect([0]), selected)
    add("add", lambda cl: cl.add(component))
    add("addcomponent", lambda cl: cl.addcomponent(flux=2, dir="J2000 10deg 20deg"))
    add("fromrecord", lambda cl: cl.fromrecord(record))
    add("concatenate(record)", lambda cl: cl.concatenate(record))
    add("concatenate(table)", lambda cl: cl.concatenate(fixture), disk=True)
    add("sort(Flux)", lambda cl: cl.sort("Flux"))
    add("sort(Position)", lambda cl: cl.sort("Position"))
    add("sort(Polarization)", lambda cl: cl.sort("Polarization"))
    add("summarize", lambda cl: cl.summarize(0))
    add("setlabel", lambda cl: cl.setlabel(0, "changed"))
    add("setflux", lambda cl: cl.setflux(0, [3, 0.5, 0.25, 0.125]))
    add("setfreq", lambda cl: cl.setfreq(0, 2.0, "GHz"))
    add("setfreqframe", lambda cl: cl.setfreqframe(0, "BARY"))
    add("setrefdir", lambda cl: cl.setrefdir(0, "20deg", "30deg"))
    add("setrefdirframe", lambda cl: cl.setrefdirframe(0, "GALACTIC"))
    add("setshape", lambda cl: cl.setshape(0, "Gaussian", "3arcsec", "1arcsec", "20deg"))
    add("setspectrum", lambda cl: cl.setspectrum(0, type="spectral index", index=-0.7))
    add("setstokesspectrum", lambda cl: cl.setstokesspectrum(0, type="spectral index", index=[-0.7, 0.1, 0.0, 0.1]))
    add("convertfluxpol", lambda cl: cl.convertfluxpol(0, "Linear"))
    add("convertfluxunit", lambda cl: cl.convertfluxunit(0, "mJy"))
    add("convertfrequnit", lambda cl: cl.convertfrequnit(0, "MHz"))
    add("convertrefdir", lambda cl: cl.convertrefdir(0, "GALACTIC"))
    add("convertshape", lambda cl: cl.convertshape(0, "arcmin", "arcmin", "rad"))
    add("simulate", lambda cl: cl.simulate(3))
    add("remove", lambda cl: cl.remove([0]))
    add("recover", lambda cl: cl.recover(), removed)
    add("purge", lambda cl: cl.purge(), removed)
    add("sample", lambda cl: cl.sample("J2000 10deg 20deg", "1arcsec", "1arcsec", "LSRK 1.4GHz"))
    add("edit", lambda cl: cl.edit(0))
    add("replace", lambda cl: cl.replace(0, record))
    add("asciitocomponentlist", lambda cl: cl.asciitocomponentlist("unused", "unused"))
    add("getkeyword", lambda cl: cl.getkeyword("metadata"), attached, True)
    add("haskeyword", lambda cl: cl.haskeyword("metadata"), attached, True)
    add("putkeyword", lambda cl: cl.putkeyword("metadata", "changed"), lambda: attached(True), True)
    add("open", lambda cl: cl.open(fixture), cltool, True)
    add("rename", lambda cl: cl.rename(str(root / "renamed.cl")), disk=True)
    add("close", lambda cl: cl.close(log=False))
    add("done", lambda cl: cl.done())
    add("construction", lambda cl: cleanup(cltool()), lambda: None)
    inventory = json.loads((Path(__file__).resolve().parents[2] / "frontends/casars-python/python/tests/data/casa_componentlist_signatures.json").read_text())["methods"]
    assert {name.split("(")[0] for name in cases if name != "construction"} == set(inventory)
    chosen = args.cases or list(cases)
    results = {}
    gc.disable()
    try:
        for name in chosen:
            setup, operation, disk = cases[name]
            samples = []
            repetitions = min(args.repeat, 10) if disk else args.repeat
            for sample in range(args.samples + 1):
                elapsed = 0
                for _ in range(repetitions):
                    cl = setup()
                    try:
                        start = time.perf_counter_ns()
                        operation(cl)
                        elapsed += time.perf_counter_ns() - start
                    finally:
                        cleanup(cl)
                        if name == "rename" and (root / "renamed.cl").exists():
                            shutil.rmtree(root / "renamed.cl")
                if sample:
                    samples.append(elapsed / repetitions)
            results[name] = {"median_ns": statistics.median(samples), "samples_ns": samples, "calls_per_sample": repetitions}
    finally:
        gc.enable()
    repository = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    changes = subprocess.check_output(["git", "status", "--porcelain", "--", "crates", "libs", "tools/simulation"], cwd=repository, text=True)
    args.out.write_text(json.dumps({"revision": revision, "source_dirty": bool(changes), "python": sys.executable, "platform": platform.platform(), "backend": args.backend, "module": module, "casa_version": version_string(), "command": __import__("sys").argv, "cases": results}, indent=2) + "\n")
    print(f"Wrote {len(results)} cases to {args.out}")
