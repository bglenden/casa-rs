#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair the native calibrater model transitions with installed CASA 6.7.6.14."""

import argparse
import json
import os
from pathlib import Path
import subprocess

import casatasks as casa
from casatools import calibrater
import numpy as np

from bandpass_oracle import source_data
from calibration_ft_oracle import component, coordinates
from calibration_lifecycle_oracle import contents
from gaincal_oracle import relative_rms


def run(root, native_test):
    assert casa.version_string() == "6.7.6.14"
    root.mkdir(parents=True)
    inputs = [root / f"source-{index}.ms" for index in range(2)]
    for ms, flux in zip(inputs, [2., 8.], strict=True):
        source_data(ms)
        model = ms.with_suffix(".cl")
        component(model, coordinates(ms), flux)
        assert casa.ft(str(ms), field="0", complist=str(model)) is None

    outputs = root / "casa"
    outputs.mkdir()
    cb = calibrater()
    assert cb.setvi(old=False, quiet=True)
    assert cb.open(str(inputs[0]), addcorr=False, addmodel=False)
    assert cb.selectvis(field="0")

    def configure(name):
        path = outputs / f"{name}.g"
        assert cb.setsolve(type="G", t="inf", table=str(path), refant="A0", minblperant=1)
        return path

    def solve(name, writes):
        path = configure(name)
        assert cb.solve()
        assert path.exists() == writes, name

    def refused(path):
        try:
            cb.solve()
        except RuntimeError as error:
            assert "Error in Calibrater::solve" in str(error)
        else:
            raise AssertionError(f"solve without a configured term accepted {path}")
        assert not path.exists()

    solve("stored", True)
    assert cb.setptmodel([])
    solve("empty", False)
    assert cb.reset(apply=True, solve=True)
    solve("reset", False)
    assert cb.setptmodel([0., 0., 0., 0.])
    solve("zeros", False)

    selected = configure("selected")
    assert cb.selectvis(field="0")
    refused(selected)
    solve("selected", False)

    abandoned = configure("abandoned")
    try:
        cb.selectvis(field="99999")
    except RuntimeError as error:
        assert "no match for Field ID" in str(error)
    else:
        raise AssertionError("calibrater matched an unknown field")
    refused(abandoned)
    solve("recovered", True)

    assert cb.setptmodel([])
    assert cb.close()
    assert cb.open(str(inputs[1]), addcorr=False, addmodel=False)
    assert cb.selectvis(field="0")
    refused(abandoned)
    solve("reopened", True)
    assert cb.done()

    subprocess.run(
        [str(native_test), "--ignored", "--exact", "installed_casa_model_lifecycle"],
        env=dict(os.environ, CASARS_CALIBRATER_LIFECYCLE_WORK=str(root)),
        check=True,
    )
    evidence = {}
    for name in ["stored", "recovered", "reopened"]:
        expected = contents(outputs / f"{name}.g")
        actual = contents(root / "native" / f"{name}.g")
        for column in ["ANTENNA1", "FIELD_ID", "SPECTRAL_WINDOW_ID", "FLAG"]:
            np.testing.assert_array_equal(actual[column], expected[column], err_msg=name)
        error = relative_rms(actual["CPARAM"], expected["CPARAM"])
        assert error <= 1e-3, (name, error)
        evidence[name] = {"complex_gains": actual["CPARAM"].size, "normalized_rms": error}
    assert set(contents(outputs / "recovered.g")["FIELD_ID"]) == {0, 1}
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-test", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    evidence = run(root, args.native_test.resolve())
    (root / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
