#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the public calibrater model operations with installed CASA."""

import argparse
from itertools import product
import json
from pathlib import Path

import casatasks
from casatools import calibrater as reference, table
import numpy as np

from casars.tools import calibrater as native, table as native_table

from bandpass_oracle import source_data
from calibration_scratch_oracle import MAIN, SCRATCH, prepare, state
from calibration_uvmodelfit_oracle import component, fixture, numeric_record


def layout(path):
    tool = table()
    assert tool.open(str(path))
    try:
        return {
            str(name): (entry["TYPE"], entry["NAME"], entry["SPEC"].get("DEFAULTTILESHAPE"))
            for entry in tool.getdminfo().values()
            for name in entry["COLUMNS"]
            if str(name).startswith(SCRATCH)
        }
    finally:
        tool.close()


def compare(expected, actual):
    assert expected[0].keys() == actual[0].keys()
    for key in expected[0]:
        if key in SCRATCH:
            np.testing.assert_allclose(actual[0][key], expected[0][key], rtol=2e-5, atol=2e-5)
        else:
            assert key in MAIN
            np.testing.assert_array_equal(actual[0][key], expected[0][key], err_msg=key)
    np.testing.assert_equal(actual[1], expected[1])
    return sum(actual[0][key].size for key in SCRATCH if key in actual[0])


def rejects(call, error_type, message):
    try:
        call()
    except error_type as error:
        assert str(error).splitlines()[0] == message
    else:
        raise AssertionError("calibrater accepted a rejected input")


def open_matrix(root, seed):
    result = {}
    for old, compress, addcorr, addmodel in product((False, True), repeat=4):
        name = f"{int(old)}{int(compress)}{int(addcorr)}{int(addmodel)}"
        reports = []
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"open-{name}-{label}.ms"
            prepare(seed, path, virtual=True)
            tool = factory()
            assert tool.close() and tool.done() and tool.setvi(old, True)
            assert tool.open(str(path), compress, addcorr, addmodel), (name, label)
            assert not tool.setvi(not old, True)
            assert tool.close()
            first = state(path)
            assert tool.open(str(path), not compress, addcorr, addmodel)
            assert tool.done()
            compare(first, state(path))
            reports.append((first, layout(path)))
        if compress:
            np.testing.assert_equal(reports[1][1], reports[0][1])
        result[name] = compare(reports[0][0], reports[1][0])
    return result


def open_float_data(root, seed):
    result = {}
    for form, compress in product(("both", "only"), (False, True)):
        snapshots = []
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"float-{form}-{compress}-{label}.ms"
            prepare(seed, path, float_data=form)
            tool = factory()
            assert tool.open(str(path), compress, True, True)
            assert tool.done()
            snapshots.append(state(path))
        result[f"{form}-{compress}"] = compare(*snapshots)
    return result


def open_packed_data(root, seed):
    result = {}
    for compress in (False, True):
        snapshots = []
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"packed-data-{compress}-{label}.ms"
            prepare(seed, path)
            maker = reference()
            assert maker.open(str(path), True, True, False)
            assert maker.done()
            tb = table()
            assert tb.open(str(path), nomodify=False)
            try:
                data = tb.getcol("DATA")
                tb.renamecol("DATA", "ORIGINAL_DATA")
                tb.renamecol("CORRECTED_DATA", "DATA")
                tb.putcol("DATA", data)
            finally:
                tb.close()
            tool = factory()
            # The packed DATA fixture retains the former CORRECTED_DATA backing
            # names. Add compressed MODEL_DATA and exercise both ordinary additions.
            assert tool.open(str(path), compress, not compress, True)
            assert tool.done()
            snapshots.append(state(path))
        result[str(compress)] = compare(*snapshots)
    return result


def scratch_column_keywords(root, seed):
    for label, factory in (("casa", reference), ("native", native)):
        path = root / f"scratch-keywords-{label}.ms"
        prepare(seed, path)
        tb = table()
        assert tb.open(str(path), nomodify=False)
        try:
            tb.putcolkeyword("DATA", "UNIT", "Jy")
            tb.putcolkeyword("DATA", "USER_TAG", "observed")
            observed = tb.getcolkeywords("DATA")
        finally:
            tb.close()
        tool = factory()
        assert tool.open(str(path), False, True, True)
        assert tool.done()
        assert tb.open(str(path))
        try:
            np.testing.assert_equal(tb.getcolkeywords("MODEL_DATA"), {})
            np.testing.assert_equal(tb.getcolkeywords("CORRECTED_DATA"), observed)
        finally:
            tb.close()
    return 2


def failed_column_publication(root, seed):
    for name in ("MODEL_DATA_SCALE", "CORRECTED_DATA_SCALE"):
        path = root / f"collision-{name}.ms"
        prepare(seed, path, virtual=True)
        tb = table()
        assert tb.open(str(path), nomodify=False)
        try:
            assert tb.addcols({name: {"valueType": "float"}})
            values = np.full(tb.nrows(), 113., dtype=np.float32)
            tb.putcol(name, values)
        finally:
            tb.close()
        before = state(path)
        assert tb.open(str(path))
        try:
            columns = tb.colnames()
            managers = tb.getdminfo()
        finally:
            tb.close()
        tool = native()
        try:
            tool.open(str(path), True, True, True)
        except RuntimeError as error:
            assert name in str(error), str(error)
        else:
            raise AssertionError(f"calibrater published scratch columns over {name}")
        assert tool.done()
        compare(before, state(path))
        assert tb.open(str(path))
        try:
            assert tb.colnames() == columns
            np.testing.assert_equal(tb.getdminfo(), managers)
            np.testing.assert_array_equal(tb.getcol(name), values)
        finally:
            tb.close()

    path = root / "generic-compression-engine.ms"
    prepare(seed, path, virtual=True)
    before = state(path)
    tb = table()
    assert tb.open(str(root / "open-0111-casa.ms"))
    try:
        descriptor = tb.getcoldesc("MODEL_DATA")
    finally:
        tb.close()
    tool = native_table()
    assert tool.open(str(path), nomodify=False)
    try:
        tool.addcols({"BROKEN": descriptor}, {"TYPE": "CompressComplex", "NAME": "BROKEN"})
    except RuntimeError as error:
        assert "CompressComplex" in str(error), str(error)
    else:
        raise AssertionError("table.addcols accepted a compression engine without backing columns")
    finally:
        assert tool.close()
    assert tb.open(str(path))
    try:
        assert "BROKEN" not in tb.colnames()
    finally:
        tb.close()
    compare(before, state(path))
    return 3


def model_transitions(root, seed):
    reports = []
    for label, factory in (("casa", reference), ("native", native)):
        path = root / f"transitions-{label}.ms"
        prepare(seed, path, corrected=True, model=True, virtual=True)
        initial = state(path)
        tool = factory()
        assert tool.open(str(path), False, False, False)
        rejects(lambda: tool.selectvis(chanmode="channel"), RuntimeError,
                "Error in Calibrater::selectvis(): Old-fashioned chanmode selection is no longer supported!")
        assert tool.close()
        assert tool.setvi(old=True, quiet=True)
        assert tool.open(str(path))
        compare(initial, state(path))
        assert tool.delmod()
        for params, expected in [
            ({"chanmode": "velocity"},
             (RuntimeError, "Error in Calibrater::selectvis(): Exception: Method not implemented.")),
            ({"chanmode": "opticalvelocity"},
             (RuntimeError, "Error in Calibrater::selectvis(): Exception: Method not implemented.")),
            ({"mstart": "1GHz"}, (RuntimeError, "Quantum::assure non-conforming unit type 'GHz'")),
            ({"mstep": 3}, (TypeError, "_mstep is not a dictionary")),
        ]:
            rejects(lambda: tool.selectvis(**params), *expected)
        assert tool.selectvis(mstart={}, mstep={})
        assert tool.selectvis(mstart={"value": 3.}, mstep={"unit": "km/s"})
        assert tool.selectvis(mstart={"unit": "m/s"}, mstep={"value": 3.})
        assert tool.selectvis(field=0, spw="0", chanmode="channel", nchan=3, start=2, step=2)
        assert tool.initcalset(0)
        channel_mode = state(path)
        assert tool.selectvis(field=[0], spw="0:2~6^2", msselect="ANTENNA1!=ANTENNA2",
                              nchan=4, start=2, step=2, mstart="2km/s",
                              mstep={"value": 3., "unit": "m/s"})
        assert tool.setptmodel(1.) and tool.setptmodel([])
        assert tool.reset(False, True)
        assert tool.initcalset(0)
        selected = state(path)
        assert tool.delmod(True, "1", "0", False)
        deleted = state(path)
        assert tool.delmod(scr=True)
        assert tool.close() and tool.done()
        assert tool.open(str(path), False, False, False)
        assert tool.selectvis(field="1")
        assert tool.initcalset(1)
        finished = state(path)
        assert tool.done()
        reports.append((channel_mode, selected, deleted, finished))
    return [compare(expected, actual)
            for expected, actual in zip(reports[0], reports[1], strict=True)]


def fit_components(root, seed):
    result = {}
    for shape, code in (("Point", "P"), ("Gaussian", "G"), ("Disk", "D")):
        source, _ = fixture(root, seed, shape)
        pars = [2., .1, -.1] if code == "P" else [2., .1, -.1, 1.1, .8, 24.]
        fits = []
        before = state(source)
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"fit-{shape}-{label}.cl"
            tool = factory()
            assert tool.setvi(True, True)
            assert tool.open(str(source), addcorr=False, addmodel=False)
            assert tool.selectvis(field="0")
            np.testing.assert_allclose(tool.modelfit(True, -1, code, pars), pars, atol=1e-12, rtol=0)
            if code == "P":
                for mask in (0, 1, [1, 0, 0], np.array([1, 0, 0])):
                    np.testing.assert_allclose(tool.modelfit(mask, -1, code, pars),
                                               pars, atol=1e-12, rtol=0)
            values = tool.modelfit([True] * len(pars), 50, code, pars, str(path))
            assert isinstance(values, np.ndarray) and values.shape == (len(pars),)
            assert tool.done()
            fits.append((values, numeric_record(component(path))))
            compare(before, state(source))
        for actual, expected in zip(fits[1], fits[0], strict=True):
            np.testing.assert_allclose(actual, expected, rtol=1e-3, atol=1e-6, err_msg=shape)
        result[shape] = float(np.max(np.abs(fits[1][0] - fits[0][0])))
    return result


def reset_compressed(root, seed):
    results = []
    for label, factory in (("casa", reference), ("native", native)):
        path = root / f"compressed-reset-{label}.ms"
        prepare(seed, path)
        tool = factory()
        assert tool.setvi(True, True)
        assert tool.open(str(path), True, True, True)
        assert tool.close()
        tb = table()
        assert tb.open(str(path), nomodify=False)
        try:
            for key in SCRATCH:
                data = tb.getcol(key)
                tb.putcol(key, np.full_like(data, 13. + 7.j))
            fields = tb.getcol("FIELD_ID")
        finally:
            tb.close()
        before = state(path)[0]
        assert tool.open(str(path), False, False, False)
        assert tool.selectvis(field=0, spw="0:2~4")
        assert tool.initcalset(0)
        assert tool.done()
        after = state(path)
        for key in SCRATCH:
            np.testing.assert_array_equal(after[0][key][..., fields != 0],
                                          before[key][..., fields != 0])
            outside = [0, 1, 5, 6, 7, 8, 9, 10, 11]
            # A new per-row scale can round the retained samples by one step.
            np.testing.assert_allclose(after[0][key][:, outside, :], before[key][:, outside, :],
                                       atol=2.1e-4, rtol=0)
            assert np.any(after[0][key][:, 2:5, fields == 0] != before[key][:, 2:5, fields == 0])
        results.append(after)
    return compare(*results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    root = args.work.resolve()
    root.mkdir(parents=True)
    seed = root / "seed.ms"
    source_data(seed, stokes="XX XY YX YY")
    result = {
        "open_scratch_samples": open_matrix(root, seed),
        "float_data_scratch_samples": open_float_data(root, seed),
        "packed_data_scratch_samples": open_packed_data(root, seed),
        "scratch_column_keyword_cases": scratch_column_keywords(root, seed),
        "failed_column_publication_cases": failed_column_publication(root, seed),
        "model_transition_samples": model_transitions(root, seed),
        "compressed_reset_samples": reset_compressed(root, seed),
        "fit_max_parameter_difference": fit_components(root, seed),
    }
    (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
