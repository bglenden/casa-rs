# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent CASA 6.7.6.14 numerical simulation diagnostics (SIM-27/28)."""

import argparse
import json
import math
import os
from pathlib import Path
import inspect

import numpy as np


BEAMS = [
    {"beam": "20arcsec", "cell": "3arcsec", "convsupport": support, "sampling": "5arcsec"}
    for support in (-2, -1, 0, 1, 3, 5)
] + [
    {"beam": "1arcmin", "cell": "8arcsec", "convsupport": 3},
    {"beam": "0.005deg", "cell": "0.5arcsec", "convsupport": 4, "sampling": "0.002deg"},
    {"beam": "20Hz", "cell": "3arcsec", "convsupport": 3},
    {"beam": "20arcsec", "cell": "3s", "convsupport": 3},
    {"beam": "20arcsec", "cell": "3arcsec", "convsupport": 3, "sampling": "5m"},
]


def sensitivity_cases(configs):
    def case(name, cfg, **changes):
        params = dict(freq="230GHz", bandwidth="1GHz", etime="60s", elevation="60deg",
                      pwv=0.5, integration="1s", antennalist=str(configs / cfg))
        params.update(changes)
        return {"name": name, "method": "sensitivity", "params": params}
    return [
        case("alma_100", "alma.out01.cfg", freq="100GHz"),
        case("alma_230", "alma.out01.cfg"),
        case("alma_650_dsb", "alma.out01.cfg", freq="650GHz", pwv=0.2),
        case("vla_manual", "vla.d.cfg", freq="8GHz", bandwidth="20MHz", elevation="45deg",
             method="tsys-manual", tau0=0.065, t_sky=20.0),
        case("vla_atmosphere", "vla.d.cfg", freq="45GHz", elevation="30deg", pwv=2.0),
        case("aca_100", "aca.i.cfg", freq="100GHz", bandwidth="500MHz", etime="120s",
             integration="2s", elevation="40deg", pwv=1.0),
        case("manual_defaults", "vla.d.cfg", freq="8GHz", method="tsys-manual"),
        case("manual_units", "vla.d.cfg", freq={"value": 8.0, "unit": "GHz"},
             bandwidth="0.02GHz", etime="4min", integration="2s", elevation="0.7853981633974483rad",
             method="tsys-manual", tau0=0.065, t_sky=20.0),
        case("image_noise", "vla.d.cfg", freq="8GHz", bandwidth="20MHz", elevation="45deg",
             method="tsys-manual", tau0=0.065, t_sky=20.0, doimnoise=True),
        case("low_elevation", "alma.out01.cfg", elevation="2deg"),
        case("bad_method", "alma.out01.cfg", method="bad"),
        {"name": "explicit_array", "method": "sensitivity", "params": dict(freq="100GHz",
         bandwidth="1GHz", etime="60s", elevation="60deg", telescope="ALMA", diam=12.0, nant=4)},
        {"name": "explicit_manual", "method": "sensitivity", "params": dict(freq="8GHz",
         bandwidth="20MHz", etime="60s", elevation="45deg", telescope="VLA", diam=25.0, nant=27,
         method="tsys-manual", tau0=0.065, t_sky=20.0)},
        {"name": "explicit_aca", "method": "sensitivity", "params": dict(freq="100GHz",
         bandwidth="500MHz", etime="120s", integration="2s", elevation="40deg", pwv=1.0,
         telescope="ACA", diam=7.0, nant=12)},
    ]


def array_reference(params):
    """Independent existing radiometer formula, with CASA ATM and receivers.

    No array positions can be inferred from diameter/count. Evaluate the
    requested elevation directly, as an identical-dish planning estimate.
    """
    from casatools import quanta
    from casatasks.private.simutil import simutil
    from compare_sm_ms import tsys_manual_sigma, tsys_atm_sigma

    qa = quanta()
    value = lambda item, unit: float(np.ravel(qa.convert(item, unit)["value"])[0])
    frequency = value(params["freq"], "Hz")
    bandwidth = abs(value(params["bandwidth"], "Hz"))
    time = value(params["etime"], "s")
    integration = value(params["integration"], "s") if "integration" in params else time * 0.01
    elevation = value(params["elevation"], "rad")
    diameter, count = params["diam"], params["nant"]
    telescope = params["telescope"]
    p, s, b, t, q, receiver = simutil().noisetemp(telescope=telescope, freq=params["freq"])
    term = dict(antefficiency=float(p*s*b*t), spillefficiency=float(s), correfficiency=float(q),
                trx=float(receiver), tground=270.0, tcmb=2.725, senscoeff=0.0,
                rxtype=int(telescope == "ALMA" and frequency > 600e9))
    geometry = dict(chan_freq=[np.array([frequency])], spw=np.array([0]),
                    bandwidth=np.array([bandwidth]), exposure=np.array([integration]),
                    diameters=np.array([[diameter, diameter]]), elevations=np.array([[elevation, elevation]]))
    if params.get("method", "tsys-atm") == "tsys-manual":
        term.update(tau=params["tau0"], tatmos=params.get("t_sky") or 200.0)
        sigma = tsys_manual_sigma(term, np.array([frequency]), geometry["bandwidth"],
            geometry["exposure"], geometry["diameters"], geometry["elevations"])[0]
    else:
        term["pwv"] = params.get("pwv", 2.0)
        sigma = tsys_atm_sigma(term, geometry, "channel")[0,0]
    return float(sigma / np.sqrt(time/integration * 0.5*count*(count-1)*2.0))


def serial(value):
    if isinstance(value, np.ndarray):
        return {"array": value.tolist(), "shape": list(value.shape), "dtype": str(value.dtype)}
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {key: serial(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serial(item) for item in value]
    return value


def native_controls(out, only):
    """Replay the public native API against the independent stored controls.

    Image-noise values additionally require --native-image in CASA Python.
    """
    from casars.simutil import simutil

    out = Path(out).resolve()
    cases = json.loads((out / "report.json").read_text())
    work = out / "native"
    work.mkdir(exist_ok=True)
    os.chdir(work)
    reports = []
    for case in cases:
        if only and case["name"] not in only:
            continue
        params = dict(case["params"])
        if case["name"] == "image_noise":
            params["debug"] = True
        before = set(work.glob("simulation-sensitivity-*"))
        report = {"name": case["name"]}
        try:
            actual = getattr(simutil(), case["method"])(**params)
            expected = case.get("radiometer_control", case.get("scalar_array_control", case.get("result")))
            report.update(ours=actual, reference=expected)
            if case["name"] == "image_noise":
                good = isinstance(actual, tuple) and len(actual) == 2 and all(math.isfinite(x) and x > 0 for x in actual)
                created = set(work.glob("simulation-sensitivity-*")) - before
                assert len(created) == 1
                report.update(diagnostic_directory=str(created.pop()), requires_same_ms_image_control=True)
            elif isinstance(expected, dict):
                good = actual["unit"] == expected["unit"] and math.isclose(actual["value"], expected["value"], rel_tol=1e-9)
            else:
                good = isinstance(expected, (int,float)) and math.isclose(actual, expected, rel_tol=1e-3)
        except Exception as error:
            expected = case.get("scalar_array_control_error", case.get("error", {}))
            report.update(ours_error={"type": type(error).__name__, "message": str(error)}, reference_error=expected)
            good = str(error) == expected.get("message")
            if case["name"] == "manual_defaults":
                good = isinstance(error, ValueError) and str(error) == "tau0 must be supplied for tsys-manual sensitivity"
        report["agree"] = good
        reports.append(report)
        print(case["name"], "agree" if good else report, flush=True)
        (out / "native-report.json").write_text(json.dumps(reports,indent=1)+"\n")
    assert reports and all(report["agree"] for report in reports)


def noise_image_control(native_report):
    """Independent CASA imager on the native diagnostic's input MS.

    Raw simutil has two broken legacy calls; use its declared small Clark
    calculation through the supported tclean task. The native image never
    constructs the reference. Both imagers receive exactly the same DATA.
    """
    from casatasks import tclean
    from casatools import image

    reports = json.loads(Path(native_report).read_text())
    case = next(r for r in reports if r["name"] == "image_noise")
    directory = Path(case["diagnostic_directory"])
    prefix = directory / "casa-noise"
    tclean(vis=str(directory / "track.ms"), imagename=str(prefix), imsize=[128,128],
           cell="3arcsec", datacolumn="data", gridder="standard", deconvolver="clark",
           weighting="natural", stokes="I", niter=100, threshold="0.01mJy", pbcor=True)
    ia = image()
    def read(path):
        ia.open(str(path))
        try:
            return ia.getchunk(), ia.brightnessunit()
        finally:
            ia.close()
    reference, unit = read(prefix.with_suffix(".image"))
    native, native_unit = read(directory / "noise.image")
    assert reference.shape == native.shape and unit == native_unit == "Jy/beam"
    assert np.isfinite(reference).all() and np.isfinite(native).all()
    nrms = float(np.linalg.norm(native-reference)/np.linalg.norm(reference))
    result = dict(shape=list(reference.shape), unit=unit, image_nrms=nrms,
                  limit=1e-3, casa_rms_jy=float(np.sqrt(np.mean(reference.astype(float)**2))),
                  native_rms_jy=case["ours"][1])
    (directory.parent / "image-control.json").write_text(json.dumps(result,indent=1)+"\n")
    print(result,flush=True)
    assert nrms <= 1e-3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--configs")
    parser.add_argument("--native-image", help="verify a retained native noise image using its API report")
    parser.add_argument("--native", action="store_true", help="run in native Python against OUT/report.json")
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()
    if args.native:
        native_controls(args.out, args.only)
        return
    if args.native_image:
        noise_image_control(args.native_image)
        return
    if not args.configs:
        parser.error("--configs is required for task controls")
    from casatasks.private.simutil import simutil
    cases = [{"name": f"beam_{i}", "method": "sfBeam1d", "params": p} for i, p in enumerate(BEAMS)]
    cases += sensitivity_cases(Path(args.configs).resolve())
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    reports = []
    for case in cases:
        if args.only and case["name"] not in args.only:
            continue
        work = out / case["name"]
        work.mkdir(exist_ok=True)
        os.chdir(work)
        report = dict(case)
        try:
            report["result"] = serial(getattr(simutil(), case["method"])(**case["params"]))
        except Exception as error:
            report["error"] = {"type": type(error).__name__, "message": str(error)}
            if case["name"].startswith("explicit_") and type(error).__name__ == "UnboundLocalError":
                report["radiometer_control"] = array_reference(case["params"])
            if case["method"] == "sfBeam1d" and str(error) == "only 0-dimensional arrays can be converted to Python scalars":
                # CASA feeds length-one qa arrays to Python float/arange.
                # Preserve that raw NumPy-2 failure, then extract those
                # three scalars explicitly without altering its mathematics.
                original = simutil.sfBeam1d
                source = "".join(line[4:] if line.startswith("    ") else line
                                 for line in inspect.getsource(original).splitlines(keepends=True))
                for name in ("beam", "cell", "sampling"):
                    expression = f'qa.getvalue(qa.convert({name}, "arcsec"))'
                    assert expression in source
                    source = source.replace(expression, f"float({expression}[0])")
                namespace = dict(original.__globals__)
                exec(compile(source, "CASA-scalar-array-control", "exec"), namespace)
                try:
                    report["scalar_array_control"] = serial(namespace["sfBeam1d"](simutil(), **case["params"]))
                except Exception as controlled:
                    report["scalar_array_control_error"] = {"type": type(controlled).__name__, "message": str(controlled)}
            if case["name"] == "image_noise" and "missing 1 required positional argument: 'stokes'" in str(error):
                original = simutil.sensitivity
                source = "".join(line[4:] if line.startswith("    ") else line
                                 for line in inspect.getsource(original).splitlines(keepends=True))
                old = '"csclean",cellsize,[128,128],'
                assert old in source
                source = source.replace(old, '"csclean","clark",cellsize,[128,128],')
                namespace = dict(original.__globals__)
                exec(compile(source, "CASA-image-noise-argument-control", "exec"), namespace)
                try:
                    report["image_argument_control"] = serial(namespace["sensitivity"](simutil(), **case["params"]))
                except Exception as controlled:
                    report["image_argument_control_error"] = {"type": type(controlled).__name__, "message": str(controlled)}
            # Failed CASA sensitivity can retain the global simulator open.
            from casatasks.private.simutil import sm
            sm.done()
        reports.append(report)
        print(case["name"], report.get("result", report.get("error")), flush=True)
        (out / "report.json").write_text(json.dumps(reports, indent=1) + "\n")


if __name__ == "__main__":
    main()
