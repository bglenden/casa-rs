#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14's test-mode refusal and its intended deterministic ramp (#1292).

The installed simulator calls AtmosCorruptor::initialize before initializing its
slot times. Record that failure, then evaluate the explicit ramp in that source
against CASA ATM, using the already approved whole-wet-delay/CHAN_FREQ fixes.
"""
import argparse
import json
from pathlib import Path
import tempfile

import numpy as np
from sm_multiple_models_oracle import make_session
from troposphere_stats import at_tool_wet_delays


def verify_native(root):
    from casatools import simulator, table

    cases = sorted(root.glob("*/case.json"))
    assert len(cases) == 1, len(cases)
    for case in cases:
        path, calibration = case.parent / "data.ms", case.parent / "trop.cal"
        tb = table()
        tb.open(str(calibration))
        assert tb.info()["subType"] == "Tf Jones"
        gains = {(float(tb.getcell("TIME", r)), int(tb.getcell("ANTENNA1", r))):
                 tb.getcell("CPARAM", r)[0] for r in range(tb.nrows())}
        tb.close()
        tb.open(str(path))
        expected = []
        for row in range(tb.nrows()):
            time = float(tb.getcell("TIME", row))
            p, q = int(tb.getcell("ANTENNA1", row)), int(tb.getcell("ANTENNA2", row))
            expected.append(tb.getcell("DATA", row) * gains[time, p] * gains[time, q].conj())
        tb.close()
        sm = simulator()
        assert sm.openfromms(str(path))
        assert sm.setapply(table=str(calibration), interp="nearest")
        assert sm.corrupt()
        sm.close()
        tb.open(str(path))
        actual = np.asarray([tb.getcell("DATA", r) for r in range(tb.nrows())])
        tb.close()
        expected = np.asarray(expected)
        nrms = float(np.linalg.norm(actual - expected) / np.linalg.norm(expected))
        assert nrms < 1e-3, nrms
        print("CASA consumed native test ramp MS/Tf table, DATA NRMS", nrms, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--out")
    group.add_argument("--verify-native")
    args = parser.parse_args()
    if args.verify_native:
        verify_native(Path(args.verify_native))
        return
    with tempfile.TemporaryDirectory() as work:
        sm = make_session(Path(work) / "test.ms", "XX XY YX YY")
        try:
            result = {"returned": bool(sm.settrop(mode="test", pwv=0.01))}
        except Exception as error:
            result = {"exception": str(error)}
        finally:
            sm.close()
    frequencies = 230e9 + np.arange(4) * 15.625e6
    dispersive, continuum = at_tool_wet_delays(0.01, frequencies)
    # AtmosCorruptor::initialize(Int): (i/nSim + antenna/nAnt)*mean_pwv*10.
    # Four integrations, eight antennas: offsets 0, 1/4, 1/2, 3/4 and a/8.
    excess = np.array([
        [(np.float32(i / 4) + np.float32(a / 8)) * np.float32(0.01) * np.float32(10)
         for a in range(8)] for i in range(4)
    ])
    phase = excess[:, :, None].astype(float) * (dispersive + continuum)
    values = np.exp(1j * phase)
    output = dict(casa_version="6.7.6.14", original=result, pwv_mm=0.01,
                  frequencies_hz=frequencies.tolist(), excess=excess.tolist(),
                  gains=np.stack((values.real, values.imag), axis=-1).tolist())
    Path(args.out).write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
