#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Sequential, guarded observations of the existing Wave3 MS; no generation."""

import argparse
import datetime
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace

REPO = Path(os.environ.get("CASA_RS_MFS_SOURCE_ROOT", Path(__file__).resolve().parents[3]))
EVIDENCE = Path("/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55")
ROOT = EVIDENCE / "wave3-standard-mfs-4096-20261005-v1"
OUTPUT = Path("/Volumes/GLENDENNING/casa-rs-evidence/t55/wave3-standard-mfs-4096-20261005-v1")
ORIGINAL = Path("/Volumes/GLENDENNING/casa-rs-imperformance/wave3/vla/aw-widefield/medium/ms/wave3-vla-aw-widefield-medium.ms")
INPUT = OUTPUT / "input.ms"
PYTHON = "/Users/brianglendenning/.pyenv/versions/3.13.5/bin/python3"
CASA = "/Applications/CASA.app/Contents/MacOS/python3"
GUARD = EVIDENCE / "c-array-spectral-block-240-271-20260922/guard_stage.py"
FFTW = EVIDENCE / "spectral-full-20260924/fftw-native-3.3.11-simd-v2"
CONFIG = EVIDENCE / "mfs-4096-workload-20260923"
SCRIPT = Path(__file__).resolve()
CASA_LABEL = "casa-serial-v2"


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, default=str)
        stream.write("\n")


def state(status, stage, **details):
    value = dict(status=status, stage=stage, pid=os.getpid(),
                 updated_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                 **details)
    pending = ROOT / "STATE.pending.json"
    pending.write_text(json.dumps(value, indent=2) + "\n")
    pending.replace(ROOT / "STATE.json")
    print(json.dumps(value), flush=True)


def environment():
    env = {**os.environ, "CARGO_INCREMENTAL": "0", "CARGO_BUILD_JOBS": "2",
           "RUST_TEST_THREADS": "1", "RUST_MIN_STACK": "33554432",
           "PKG_CONFIG_PATH": str(FFTW / "lib/pkgconfig"),
           "CASA_RS_MEASURESPATH": str(CONFIG / "pilot-runs/reference/frozen-measures"),
           "CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND": "1000000000",
           "CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND": "1000000000",
           "CASA_RS_TRACE_IMAGING_STAGE_TIMING": "1",
           "CASA_RS_TRACE_CLARK_TIMING": "1", "RUST_LOG": "warn",
           "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
           "VECLIB_MAXIMUM_THREADS": "1", "PYTHONDONTWRITEBYTECODE": "1",
           "CASASITECONFIG": str(CONFIG / "pilot-runs/reference/frozen-casa-config.py"),
           "CASARCFILES": str(CONFIG / "intermediate-90-runs/casa-memory-4g.rc"),
           "PYTHONPATH": str(ROOT / "source"),
           "SDKROOT": "/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX26.2.sdk",
           "CARGO_SWEEP_INTERVAL_SECONDS": "2147483647"}
    for key in ("CASA_RS_MFS_METAL", "CASA_RS_PROFILE_METAL_STAGES"):
        env.pop(key, None)
    return env


def stage(label, command, rss=8, extra=None, cwd=ROOT):
    state("running", label, command=command, rss_cap_gib=rss)
    subprocess.run([PYTHON, str(GUARD), "--records", str(ROOT), "--cwd", str(cwd),
                    "--label", label, "--rss-gib", str(rss), "--", *command],
                   cwd=ROOT, env={**environment(), **(extra or {})}, check=True)


def artifact(label, name, destination=None):
    paths = []
    for line in (ROOT / f"{label}.log").read_text().splitlines():
        if line.startswith("{"):
            item = json.loads(line)
            if item.get("reason") == "compiler-artifact" and item.get("executable") and item["target"]["name"] == name:
                paths.append(item["executable"])
    assert len(paths) == 1, (name, paths)
    target = ROOT / (destination or name)
    assert not target.exists(), target
    shutil.copy2(paths[0], target)
    return target


def casa():
    from casatasks import casalog, tclean, version_string
    output = OUTPUT / CASA_LABEL
    output.mkdir()
    casalog.setlogfile(str(ROOT / "casa-task-v2.log"))
    recipe = dict(vis=str(INPUT), imagename=str(output / "image"),
                  datacolumn="data", field="0", spw="0:0~511",
                  imsize=[4096, 4096], cell="0.8arcsec", stokes="I",
                  specmode="mfs", nterms=1, outframe="LSRK", gridder="standard",
                  weighting="uniform", deconvolver="clark", niter=10000,
                  cycleniter=1000, gain=0.1, threshold="5mJy", cyclefactor=1.0,
                  minpsffraction=0.05, maxpsffraction=0.8,
                  usemask="user", mask="", normtype="flatnoise", pblimit=-0.2,
                  restoration=True, pbcor=False, savemodel="none", parallel=False,
                  restart=False, interactive=False, fullsummary=True)
    save(ROOT / "casa-v2-recipe.json", recipe)
    started = time.perf_counter()
    result = tclean(**recipe)
    seconds = time.perf_counter() - started
    import numpy as np
    def encode(value):
        if isinstance(value, (np.ndarray, np.generic)):
            return value.tolist()
        raise TypeError(type(value).__name__)
    with (ROOT / "casa-result.json").open("x") as stream:
        json.dump(result, stream, indent=2, default=encode)
    save(ROOT / "casa-summary.json", dict(seconds=seconds, workers=1,
         backend="CASA CPU serial", version=version_string(),
         timing_boundary="complete tclean: preparation through publication",
         input=str(INPUT), prefix=str(output / "image"), non_quiescent=True))
    print(f"CASA complete tclean seconds={seconds:.6f}", flush=True)


def preflight():
    from casatools import table
    t = table()
    t.open(str(INPUT), nomodify=True)
    try:
        assert t.nrows() == 4212000
        assert "DATA" in t.colnames() and "CORRECTED_DATA" not in t.colnames()
        cell = t.getcell("DATA", 0)
        assert list(cell.shape) == [2, 512], cell.shape
        main = dict(rows=t.nrows(), data_shape=list(cell.shape), columns=t.colnames(),
                    data_storage_manager=t.getdminfo())
    finally:
        t.done()
    t.open(str(INPUT / "SPECTRAL_WINDOW"), nomodify=True)
    try:
        frequency = t.getcell("CHAN_FREQ", 0)
        assert t.nrows() == 1 and len(frequency) == 512
        assert float(frequency[0]) == 1500000000 and float(frequency[-1]) == 2011000000
        main["frequency_hz"] = [float(frequency[0]), float(frequency[-1])]
        main["frequency_reference_code"] = int(t.getcell("MEAS_FREQ_REF", 0))
    finally:
        t.done()
    save(ROOT / "input-preflight.json", main)
    print(json.dumps(main, default=str), flush=True)


def comparison(label):
    from mfs_4096_pilot import compare
    compare(SimpleNamespace(output=ROOT, label=f"comparison-{label}",
        native=OUTPUT / label, reference=OUTPUT / CASA_LABEL, terms=1,
        niter=10000, threshold_jy=0.005, spw="0:0~511", integrations=12000,
        candidate_label=f"casa-rs {label}", reference_label="CASA serial"))


def science_report(label):
    from perf_harness.tolerances import scientific_beam_metrics
    metrics = json.loads((ROOT / f"comparison-{label}/metrics.json").read_text())
    checks, beams = {}, {}
    inventory = set(metrics) == {"image", "mask", "model", "pb", "psf", "residual", "sumwt"}
    for product, item in metrics.items():
        assert item["status"] == "measured", (product, item["status"])
        expected = 1 if product == "sumwt" else 4096 * 4096
        checks[product] = {
            "relative_l2": item["relative_l2"] is not None and math.isfinite(item["relative_l2"]) and item["relative_l2"] <= 0.001,
            "mask": item["mask_mismatch_pixels"] == 0,
            "complete_pixels": item["compared_pixels"] == item["candidate_valid_pixels"] == item["reference_valid_pixels"] == expected,
            "wcs": item["direction_wcs"]["parity"],
            "peak_pixel": item["native_peak_pixel"] == item["casa_peak_pixel"],
            "metadata": all(item["metadata"]["field_parity"][key] for key in ("shape", "unit", "coordinates", "masks")),
        }
        if product in ("image", "psf"):
            beams[product] = scientific_beam_metrics(item["metadata"])
            checks[product]["beam"] = all(value is not None and math.isfinite(value) and value <= 0.001 for value in beams[product].values())
    unit_psf = metrics["psf"]["native_peak"] == 1
    passed = inventory and unit_psf and all(all(values.values()) for values in checks.values())
    save(ROOT / f"science-{label}.json", dict(pass_checks=passed, checks=checks,
         inventory=inventory, unit_psf=unit_psf, beam_metrics=beams,
         scope="unchanged product checks reported; no intrinsic-sky/full-T55 acceptance",
         panels_inspected=False))


def run(resume=False):
    try:
        if resume:
            assert json.loads((ROOT / "build-application-resource.json").read_text())["complete"]
            assert json.loads((ROOT / "connected-application-resource.json").read_text())["complete"]
            binary = ROOT / "continuum_application"
            stage("input-preflight-v2", [CASA, str(SCRIPT), "preflight-v2"])
            observations(binary)
            return
        state("running", "build")
        stage("build-application", ["cargo", "test", "--manifest-path", str(REPO / "Cargo.toml"),
              "--release", "--locked", "-p", "casa-imaging-application", "--test",
              "continuum_application", "--no-run", "--message-format=json"], cwd=REPO)
        binary = artifact("build-application", "continuum_application")
        links = {str(p): p.read_text() for p in (REPO / "target/release/build").glob("casa-fft-*/output")
                 if f"cargo:rustc-link-search=native={FFTW}/lib" in p.read_text()}
        assert links and any("static=fftw3f" in value and "static=fftw3" in value for value in links.values()), "retained static SIMD-v2 FFTW linkage missing"
        save(ROOT / "fftw-links.json", links)
        stage("connected-application", [str(binary), "uniform_multi_spw_mfs_clark_matches_serial_with_four_admitted_workers",
              "--exact", "--nocapture", "--test-threads=1"])
        assert not INPUT.exists()
        stage("clone-input", ["/bin/cp", "-cR", str(ORIGINAL), str(INPUT)])
        stage("input-preflight", [CASA, str(SCRIPT), "preflight"])
        observations(binary)
    except BaseException as error:
        state("failed", "see-latest-stage-log", error=repr(error))
        raise


def observations(binary):
    stage(CASA_LABEL, ["/usr/bin/caffeinate", "-i", "-s", CASA, str(SCRIPT), "casa"])
    native_observations(binary)


def native_observations(binary, attempt=""):
    receipt = json.loads((ROOT / f"{CASA_LABEL}-resource.json").read_text())
    assert receipt["complete"] and receipt["exit_code"] == 0
    results = {"casa-serial": json.loads((ROOT / "casa-summary.json").read_text())}
    for base, workers, metal in (("cpu-w1", 1, False), ("cpu-w4", 4, False), ("metal-w4", 4, True)):
        label = f"{base}-{attempt}" if attempt else base
        assert not (OUTPUT / label).exists(), label
        extra = dict(CASA_RS_MFS_MS=str(INPUT), CASA_RS_MFS_OUTPUT=str(OUTPUT / label),
                     CASA_RS_MFS_WORKERS=str(workers), CASA_RS_MFS_TERMS="1",
                     CASA_RS_MFS_GRIDDER="standard", CASA_RS_MFS_SPW="0:0~511",
                     CASA_RS_MFS_NITER="10000", CASA_RS_MFS_THRESHOLD_JY="0.005",
                     CASA_RS_MFS_CELL_ARCSEC="0.8")
        if metal:
            extra.update(CASA_RS_MFS_METAL="1", CASA_RS_PROFILE_METAL_STAGES="1")
        stage(label, ["/usr/bin/caffeinate", "-i", "-s", str(binary),
              "t55_mfs_pilot::full_field_application", "--exact", "--ignored",
              "--nocapture", "--test-threads=1"], rss=16, extra=extra)
        summary = json.loads((OUTPUT / label / "summary.json").read_text())
        assert summary["workers"] == workers and summary["spectral_window"] == "0:0~511"
        assert summary["cell_arcsec"] == 0.8 and summary["image_size"] == 4096
        assert math.isfinite(summary["seconds"]) and summary["seconds"] > 0
        save(ROOT / f"{label}-summary.json", summary)
        results[label] = summary
        save(ROOT / f"timings-through-{label}.json", results)
        stage(f"compare-{label}", [CASA, str(SCRIPT), "compare", "--label", label])
        science_report(label)
    save(ROOT / (f"TIMINGS-{attempt}.json" if attempt else "TIMINGS.json"), results)
    state("complete", "timings-and-comparisons", timings={key: value["seconds"] for key, value in results.items()},
          note="non-quiescent single observations; panels still require human/agent inspection")


def resume_native(binary, attempt):
    assert attempt and all(c.isalnum() or c == "-" for c in attempt), attempt
    binary = Path(binary).resolve(strict=True)
    assert binary.parent == ROOT.resolve(strict=True), "use the durably frozen candidate executable"
    try:
        native_observations(binary, attempt)
    except BaseException as error:
        state("failed", "see-latest-stage-log", attempt=attempt, error=repr(error))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "resume", "resume-native", "casa", "preflight", "preflight-v2", "compare"))
    parser.add_argument("--label")
    parser.add_argument("--binary")
    parser.add_argument("--attempt")
    args = parser.parse_args()
    if args.action == "compare":
        comparison(args.label)
    elif args.action == "resume":
        run(resume=True)
    elif args.action == "resume-native":
        resume_native(args.binary, args.attempt)
    elif args.action == "preflight-v2":
        # Keep the original preflight evidence immutable.
        from casatools import table
        t = table()
        t.open(str(INPUT), nomodify=False)
        try:
            assert t.nrows() == 4212000
            assert list(t.getcell("DATA", 0).shape) == [2, 512]
        finally:
            t.done()
        save(ROOT / "input-preflight-v2.json", dict(rows=4212000, channels=512,
             casa_update_open_and_close=True))
    else:
        {"run": run, "casa": casa, "preflight": preflight}[args.action]()
