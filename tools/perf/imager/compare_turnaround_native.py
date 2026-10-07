#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Natural/Clark single-plane streaming-cube previews, with matched CASA runs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import numpy as np

from turnaround_c_array import CHANNELS, read_image
from stage_t55_c_array import CELL_ARCSEC, PIXELS, save

INPUT_STARTS = {0: 0, 256: 256, 511: 510}


def casa(args):
    from casatasks import casalog, tclean, version_string
    output = args.outputs / "turnaround-casa-original"
    output.mkdir()
    records = args.records / "casa-original-input"
    records.mkdir()
    os.chdir(output)
    casalog.setlogfile(str(output / "casa.log"))
    results = []
    for channel, start in INPUT_STARTS.items():
        prefix = output / f"ch{channel:03}"
        kwargs = dict(vis=str(args.input), imagename=str(prefix), datacolumn="data", field="0",
                      spw=f"0:{start}~{start+1}", imsize=PIXELS, cell=f"{CELL_ARCSEC}arcsec", stokes="I",
                      specmode="cube", outframe="LSRK", nchan=1, start=f"{44e9+channel*2e6:.0f}Hz",
                      width="2000000Hz", interpolation="linear", gridder="standard", weighting="natural",
                      perchanweightdensity=True, deconvolver="clark", niter=20000, cycleniter=1000,
                      gain=0.1, threshold="0.0005Jy", usemask="user",
                      mask="circle[[512pix,512pix],27.5arcsec]", interactive=False, calcres=True,
                      calcpsf=True, restoration=True, pbcor=False, savemodel="none", parallel=False,
                      pblimit=-0.2, restart=False, fullsummary=True)
        save(records / f"turnaround-ch{channel:03}-casa-request.json", kwargs)
        print(f"stage=CASA-natural-clark channel={channel} selected_input={start}:{start+1}", flush=True)
        began = time.perf_counter()
        returned = tclean(**kwargs)
        result = dict(channel=channel, input_start=start, display_plane=0,
                      frequency_hz=44e9+channel*2e6, prefix=str(prefix), seconds=time.perf_counter()-began,
                      iterations=int(returned["iterdone"]), majors=int(returned["nmajordone"]),
                      stopcode=int(returned["stopcode"]), casa_version=version_string(), workers=1)
        save(records / f"turnaround-ch{channel:03}-casa-result.json", result)
        results.append(result)
        print(json.dumps(result), flush=True)
    save(records / "turnaround-casa-results.json", results)


def native(args):
    output = args.outputs / args.native_directory
    output.mkdir()
    save(args.records / "turnaround-native-binary.json", dict(
        binary=str(args.binary), sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        staged_input=str(args.input),
        scope="ordinary native streaming cube; matching natural/Clark single output planes; one worker; intermediate sky check"))
    results = []
    for channel in CHANNELS:
        root = output / f"ch{channel:03}"
        environment = dict(os.environ, CASA_RS_C_ARRAY_MS=str(args.input),
                           CASA_RS_C_ARRAY_OUTPUT=str(root),
                           CASA_RS_C_ARRAY_CHANNEL=str(channel),
                           CASA_RS_C_ARRAY_MASK=str(args.outputs / f"turnaround-casa-original/ch{channel:03}.mask"))
        command = [str(args.binary), "t55_c_array_turnaround::first_middle_last_plane",
                   "--exact", "--ignored", "--nocapture", "--test-threads=1"]
        save(args.records / f"turnaround-ch{channel:03}-native-command.json", dict(
            command=command, environment={k: v for k, v in environment.items() if k.startswith("CASA_RS_C_ARRAY_")}))
        print(f"stage=native-application channel={channel}", flush=True)
        with (args.records / f"turnaround-ch{channel:03}-native.log").open("x") as log:
            subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)
        result = json.loads((root / "summary.json").read_text())
        results.append(result)
        print(json.dumps(result), flush=True)
    save(args.records / "turnaround-native-results.json", results)


def compare(args):
    from perf_harness.image_compare import compare_products
    from casatasks import casalog
    from casatools import image, regionmanager
    from astropy.io import fits

    os.chdir(args.outputs / args.native_directory)
    casalog.setlogfile(str(args.outputs / args.native_directory / "comparison-casa.log"))
    native_results = json.loads((args.records / "turnaround-native-results.json").read_text())
    contract = json.loads((args.repo / "tools/perf/imager/workloads/t55-clark-cube-development.json").read_text())["comparison"]
    records = []
    beams = []
    for native_result in native_results:
        channel = native_result["channel"]
        directory = args.records / f"turnaround-native-casa-ch{channel:03}"
        directory.mkdir(exist_ok=True)
        casa_prefix = str(args.outputs / f"turnaround-casa-original/ch{channel:03}")
        result_path = directory / "result.json"
        if result_path.exists():
            result = json.loads(result_path.read_text())
            assert result["left_prefix"] == native_result["prefix"]
            assert result["right_prefix"] == casa_prefix
        else:
            result = compare_products(casa_python=str(args.casa_python), cwd=directory,
                                  artifact_prefix=directory / "comparison", request={
                                      **contract, "left_prefix": native_result["prefix"],
                                      "right_prefix": casa_prefix, "left_label": "casa-rs",
                                      "right_label": "CASA", "panel_dir": str(directory / "products"),
                                      "structure_workspace_dir": str(directory / "structure")})
            save(result_path, result)
        # Failed metadata/tolerance gates must remain visible, but do not block
        # displaying the fully compared arrays for this diagnostic preview.
        assert len(result["products"]) == 7
        assert all(product["full_array"]["coverage_complete"]
                   for product in result["products"].values())
        record = dict(channel=channel, native=native_result, casa_prefix=casa_prefix,
                      comparison_status=result["status"], comparison_reason=result.get("reason"),
                      seven_product_tolerance_status=result["tolerance_evaluation"]["status"],
                      raw_comparison=str(directory / "result.json"))
        for label, prefix in (("native", native_result["prefix"]), ("casa", casa_prefix)):
            ia = image()
            ia.open(prefix + ".image")
            try:
                plane = native_result["display_plane"]
                assert list(ia.shape()) == [PIXELS, PIXELS, 1, 1]
                beam = ia.restoringbeam(channel=plane, polarization=0)
                cs = ia.coordsys()
                try:
                    np.testing.assert_allclose(cs.referencevalue()["numeric"][:2],
                                               [np.pi, np.deg2rad(34.07875)], atol=1e-10)
                    world = cs.toworld([512., 512., 0., float(plane)])["numeric"]
                    assert abs(world[3] - native_result["frequency_hz"]) < 1
                    np.testing.assert_allclose(cs.referencepixel()["numeric"][:2], [512, 512], atol=1e-7)
                    np.testing.assert_allclose(cs.increment()["numeric"][:2],
                                               np.deg2rad([-CELL_ARCSEC/3600, CELL_ARCSEC/3600]))
                finally:
                    cs.done()
                assert beam["major"]["unit"] == "arcsec"
                beams.append(beam["major"]["value"])
                record[label + "_beam"] = beam
                selected_path = args.outputs / args.native_directory / f"ch{channel:03}" / (label + "-selected.image")
                if selected_path.exists():
                    selected = image()
                    selected.open(str(selected_path))
                else:
                    selected = ia.subimage(outfile=str(selected_path),
                                           region=regionmanager().box(blc=[0, 0, 0, plane], trc=[1023, 1023, 0, plane]))
                selected.done()
            finally:
                ia.done()
        records.append(record)
        print(json.dumps(record), flush=True)
    # Match display resolution without altering the raw seven-product checks.
    common = np.ceil(max(beams) * 1.01 * 100) / 100
    for record in records:
        channel = record["channel"]
        directory = args.outputs / args.native_directory / f"ch{channel:03}"
        plane = record["native"]["display_plane"]
        pb_cube, valid_cube, _ = read_image(record["casa_prefix"] + ".pb")
        pb = pb_cube[:, :, :, plane:plane+1]
        ia = image()
        ia.open(str(directory / "casa-selected.image"))
        cs = ia.coordsys()
        coordinates = cs.torecord()
        cs.done()
        ia.done()
        with fits.open(args.sky, memmap=False) as hdus:
            intrinsic = np.asarray(hdus[0].section[channel, 0, :, :], dtype=np.float32).T[:, :, None, None]
        ia = image()
        ia.fromarray(outfile=str(directory / "truth-apparent.image"), pixels=intrinsic*pb, csys=coordinates)
        ia.setbrightnessunit("Jy/pixel")
        ia.done()
        paths = dict(native=str(directory / "native-selected.image"),
                     casa=str(directory / "casa-selected.image"), truth=str(directory / "truth-apparent.image"))
        arrays = {}
        for label, source in paths.items():
            ia = image()
            ia.open(source)
            try:
                matched = ia.convolve2d(outfile=str(directory / (label + "-display.image")),
                                       major=f"{common}arcsec", minor=f"{common}arcsec", pa="0deg",
                                       targetres=label != "truth")
                try:
                    assert matched.brightnessunit() == "Jy/beam"
                    values = matched.getchunk()[:, :, 0, 0].T
                    assert np.isfinite(values).all()
                    arrays[label] = values
                finally:
                    matched.done()
            finally:
                ia.done()
        arrays["difference"] = arrays["native"] - arrays["casa"]
        arrays["pb"] = pb[:, :, 0, 0].T
        arrays["valid"] = valid_cube[:, :, 0, plane].T
        coords = (np.arange(PIXELS)-512)*CELL_ARCSEC
        support = np.hypot(*np.meshgrid(coords, coords)) <= 27.5
        assert arrays["valid"][support].all()
        record["common_display_beam_arcsec"] = common
        record["display_native_casa_rms_jy_beam"] = float(np.sqrt(np.mean(arrays["difference"][support]**2)))
        record["display_native_truth_rms_jy_beam"] = float(np.sqrt(np.mean((arrays["native"]-arrays["truth"])[support]**2)))
        record["display_casa_truth_rms_jy_beam"] = float(np.sqrt(np.mean((arrays["casa"]-arrays["truth"])[support]**2)))
        np.savez_compressed(args.records / f"turnaround-four-way-ch{channel:03}.npz", **arrays)
    save(args.records / "turnaround-four-way-comparison.json", records)


def panels(args):
    os.environ["MPLCONFIGDIR"] = str(args.records / "matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import AsinhNorm

    records = json.loads((args.records / "turnaround-four-way-comparison.json").read_text())
    arrays = [dict(np.load(args.records / f"turnaround-four-way-ch{c:03}.npz")) for c in CHANNELS]
    maximum = max(float(a[k].max()) for a in arrays for k in ("truth", "native", "casa"))*1000
    observed_delta = max(float(np.max(np.abs(a["difference"]))) for a in arrays)*1000
    delta = args.difference_limit_mjy or observed_delta
    signal_norm = AsinhNorm(linear_width=0.3, vmin=-0.5, vmax=maximum)
    difference_norm = AsinhNorm(linear_width=max(delta/20, 0.001), vmin=-delta, vmax=delta)
    coords = (np.arange(PIXELS)-512)*CELL_ARCSEC
    extent = [coords[0]-CELL_ARCSEC/2, coords[-1]+CELL_ARCSEC/2]*2
    paths = []
    for name, record, a in zip(("First", "Middle", "Last"), records, arrays):
        fig, axes = plt.subplots(1, 4, figsize=(18, 5.8), layout="constrained", sharex=True, sharey=True)
        for ax, key, title in zip(axes, ("truth", "native", "casa", "difference"),
                                 ("Simulated apparent sky", "casa-rs", "CASA", "casa-rs − CASA")):
            shown = ax.imshow(np.where(a["valid"], a[key]*1000, np.nan), origin="lower", extent=extent,
                              cmap="RdBu_r" if key == "difference" else "magma",
                              norm=difference_norm if key == "difference" else signal_norm)
            ax.contour(coords, coords, a["pb"], levels=[0.7], colors=["#22d3ee"], linestyles="--", linewidths=1)
            ax.set(title=title, xlabel="West offset (arcsec)")
            ax.set_xticks([-30, -15, 0, 15, 30])
            ax.set_yticks([-30, -15, 0, 15, 30])
            if key == "casa":
                signal = shown
        axes[0].set_ylabel("North offset (arcsec)")
        bar = fig.colorbar(signal, ax=axes[:3], orientation="horizontal", fraction=0.07, pad=0.08,
                           label="mJy/beam · shared signal scale")
        bar.set_ticks([0, 1, 10, 100, 500], labels=["0", "1", "10", "100", "500"])
        bar = fig.colorbar(shown, ax=axes[3], orientation="horizontal", fraction=0.07, pad=0.08,
                           label="mJy/beam · separate signed scale")
        ticks = [-delta, -delta/5, 0, delta/5, delta]
        bar.set_ticks(ticks, labels=[f"{v:.2g}" for v in ticks])
        fig.suptitle(f"{name}: channel {record['channel']} · {record['native']['frequency_hz']/1e9:.3f} GHz\n"
                     f"Natural / Clark · one worker each · common {record['common_display_beam_arcsec']:.2f}″ display beam · dashed: 70% primary beam", fontsize=13)
        path = args.records / f"turnaround-{name.lower()}-four-way{args.panel_suffix}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        paths.append(str(path))
    save(args.records / f"turnaround-four-way-panels{args.panel_suffix}.json", dict(paths=paths,
         difference_limit_mjy_beam=delta, observed_difference_max_mjy_beam=observed_delta,
         signal_limit_mjy_beam=maximum,
         note="Common-beam display only; raw seven-product results remain unmodified. No alignment fit or flux rescaling."))
    print(json.dumps(dict(panels=paths)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("casa", "native", "compare", "panels"))
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--sky", type=Path)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--casa-python", type=Path)
    parser.add_argument("--native-directory", default="turnaround-native-single-fixed")
    parser.add_argument("--difference-limit-mjy", type=float)
    parser.add_argument("--panel-suffix", default="")
    args = parser.parse_args()
    assert Path(args.native_directory).name == args.native_directory
    assert args.difference_limit_mjy is None or args.difference_limit_mjy > 0
    assert args.panel_suffix in ("", "-detail")
    for path in (args.records, args.outputs):
        assert path.is_absolute() and path.is_dir()
        assert not any(part in ("private", "tmp", "temp") for part in path.resolve().parts)
    {"casa": casa, "native": native, "compare": compare, "panels": panels}[args.stage](args)


if __name__ == "__main__":
    main()
