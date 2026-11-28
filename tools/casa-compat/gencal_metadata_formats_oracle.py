#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reopen explicit calibration prescriptions from both MS formats with CASA."""

import argparse
import json
from pathlib import Path
import tarfile

import casatasks as casa
import numpy as np

from calibration_gencal_specify_oracle import compare_tables, generate, product, with_second_window
from calibration_smoothcal_oracle import opened as open_table
from record_casa_returns import make_cal_ms


TRIAL = (
    Path(__file__).resolve().parents[2]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz"
)
CASES = {
    "gain": [
        ("amp", {
            "antenna": "3,0", "spw": "1,0", "pol": "Y,X",
            "parameter": [0.8, 1.1, 1.2, 1.3, 0.9, 1.0, 1.2, 1.4],
        }),
        ("ph", {"antenna": "3", "spw": "1", "pol": "X", "parameter": [20.0]}),
    ],
    "sbd": [("sbd", {"antenna": "1", "spw": "1", "pol": "Y", "parameter": [2.0]})],
    "mbd": [("mbd", {"antenna": "2", "spw": "0", "pol": "X", "parameter": [3.0]})],
    "opacity": [("opac", {"antenna": "1", "spw": "1", "parameter": [0.02]})],
}


def paired_sources(ms_v2, native, msv2_only):
    sources = {"MSv2": ms_v2}
    if not msv2_only:
        # The basename is part of the CASA product, irrespective of format.
        ms_v4 = ms_v2.parents[1] / "MSv4" / ms_v2.name
        ms_v4.parent.mkdir()
        converted = ms_v4.parent / "paired.ps.zarr"
        result = native.msconvert(
            vis=str(ms_v2), outputvis=str(converted), outformat="msv4", withpointing=False,
        )
        assert result.successful, result.describe()
        converted.rename(ms_v4)
        sources["MSv4"] = ms_v4
    return sources


def tropo_keyword(caltable):
    with open_table(caltable) as tool:
        return tool.getkeyword("VLATrDelCorr") if "VLATrDelCorr" in tool.keywordnames() else None


def check_antenna_offsets(root, native, msv2_only):
    source = root / "MSv2" / "offsets.ms"
    source.parent.mkdir()
    make_cal_ms(str(source), telescope="ALMA")
    with_second_window(source)
    sources = paired_sources(source, native, msv2_only)
    results = {}
    for mode in ("antpos", "antposvla"):
        expected = root / f"{mode}-CASA.cal"
        stages = (
            {"antenna": "1,0", "spw": "1", "parameter": [0.17, 0.28, 0.34, 0.4, 0.59, 0.63]},
            {"antenna": "0", "parameter": [0.01, 0.02, 0.03]},
        )
        candidates = {name: root / f"{mode}-{name}.cal" for name in sources}
        cross = root / f"{mode}-CASA-create-MSv4-update.cal"
        for phase, options in zip(("create", "update"), stages, strict=True):
            generate(casa, source, expected, [(mode, options)])
            _, rows, _ = product(expected)
            np.testing.assert_array_equal(rows["ANTENNA1"], np.arange(5))
            np.testing.assert_array_equal(rows["SPECTRAL_WINDOW_ID"], np.zeros(5))
            assert not rows["FLAG"].any() and tropo_keyword(expected) is None
            metrics = {}
            for name, vis in sources.items():
                candidate = candidates[name]
                generate(native, vis, candidate, [(mode, options)])
                assert tropo_keyword(candidate) is None
                metrics[name] = compare_tables(candidate, expected)
            if not msv2_only:
                if phase == "create":
                    generate(casa, source, cross, [(mode, options)])
                else:
                    generate(native, sources["MSv4"], cross, [(mode, options)])
                    metrics["CASA-create-MSv4-update"] = compare_tables(cross, expected)
            results[f"{mode}-{phase}"] = metrics
    if not msv2_only:
        results["VLA-epoch"] = check_vla_epoch_refusal(root, native)
    return results


def check_vla_epoch_refusal(root, native):
    boundary = 57609 * 86400.0
    fixture = TRIAL.with_name("ms_trial_lines.ms.tgz")
    keywords, reconstructed = [], []
    for label, stated_start in (("earlier", -5), ("later", 3)):
        case = root / label
        case.mkdir()
        with tarfile.open(fixture) as archive:
            archive.extractall(case, filter="data")
        vis = case / "ms_trial_lines.ms"
        with open_table(vis, write=True) as main:
            time, interval = main.getcol("TIME"), main.getcol("INTERVAL")
            shift = boundary + 5 - np.min(time - interval / 2)
            for name in ("TIME", "TIME_CENTROID"):
                assert main.putcol(name, main.getcol(name) + shift)
        with open_table(vis / "OBSERVATION", write=True) as observation:
            assert "VLA" in observation.getcell("TELESCOPE_NAME", 0)
            assert observation.putcell("TIME_RANGE", 0, [
                boundary + stated_start, np.max(time + shift + interval / 2),
            ])
        options = dict(caltype="antpos", antenna="0", parameter=[0.1, 0.2, 0.3])
        caltable = case / "CASA.cal"
        assert casa.gencal(vis=str(vis), caltable=str(caltable), **options) is None
        keywords.append(tropo_keyword(caltable))
        processing_set = case / "source.ps.zarr"
        result = native.msconvert(vis=str(vis), outputvis=str(processing_set),
                                  outformat="msv4", withpointing=False)
        assert result.successful, result.describe()
        rebuilt = case / "reopened.ms"
        result = native.msconvert(vis=str(processing_set), outputvis=str(rebuilt), outformat="msv2")
        assert result.successful, result.describe()
        with open_table(rebuilt / "OBSERVATION") as observation:
            reconstructed.append(observation.getcell("TIME_RANGE", 0))
        output = case / "refused.cal"
        try:
            native.gencal(vis=str(processing_set), caltable=str(output), **options)
        except native.TaskParameterError as error:
            assert error.completion.parameter == "vis"
            assert "stated OBSERVATION.TIME_RANGE" in error.completion.message
            assert not output.exists()
        else:
            raise AssertionError("inferred a VLA tropospheric correction from the reconstructed start")
    assert keywords == [None, 1.0]
    np.testing.assert_array_equal(reconstructed[0], reconstructed[1])
    assert reconstructed[0][0] == boundary + 5
    return {"CASA-keywords": keywords, "reconstructed-start-seconds-after-boundary": 5}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--msv2-only", action="store_true")
    parser.add_argument("--antenna-offsets", action="store_true")
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir()
    if args.antenna_offsets:
        print(json.dumps(check_antenna_offsets(root, native, args.msv2_only)))
        return
    ms_v2 = root / "MSv2" / "ms_trial.ms"
    ms_v2.parent.mkdir()
    with tarfile.open(TRIAL) as archive:
        archive.extractall(ms_v2.parent, filter="data")
    with_second_window(ms_v2)
    sources = paired_sources(ms_v2, native, args.msv2_only)

    results = {}
    for case, stages in CASES.items():
        expected = root / f"{case}-CASA.cal"
        generate(casa, ms_v2, expected, stages)
        _, rows, _ = product(expected)
        observed_ids = sorted(zip(rows["SPECTRAL_WINDOW_ID"], rows["ANTENNA1"]))
        assert observed_ids == [(spw, antenna) for spw in range(2) for antenna in range(5)]
        assert not np.asarray(rows["FLAG"]).any()
        results[case] = {}
        for label, source in sources.items():
            candidate = root / f"{case}-{label}.cal"
            generate(native, source, candidate, stages)
            results[case][label] = {
                "rows": len(observed_ids),
                "parameter_nrms": compare_tables(candidate, expected),
            }
    if not args.msv2_only:
        cross = root / "gain-CASA-create-MSv4-append.cal"
        generate(casa, ms_v2, cross, CASES["gain"][:1])
        generate(native, sources["MSv4"], cross, CASES["gain"][1:])
        results["gain"]["CASA-create-MSv4-append"] = {
            "rows": 10,
            "parameter_nrms": compare_tables(cross, root / "gain-CASA.cal"),
        }
    print(json.dumps(results))


if __name__ == "__main__":
    main()
