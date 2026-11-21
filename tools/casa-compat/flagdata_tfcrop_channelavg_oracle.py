#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check TFCrop channel averaging and its physical flag destinations."""

from contextlib import chdir
from json import dumps, loads
from pathlib import Path
from shutil import copytree
from signal import SIGABRT
from subprocess import run
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_fit_oracle import make_fixture
from flagdata_tfcrop_sources_oracle import (
    check_science_state, put_samples, science_state, seed_fourier_models,
)
from ms_selection_tasks_oracle import add_second_window, casa_table


FREQUENCY_FIT = dict(mode="tfcrop", antenna="0&1", datacolumn="data", flagdimension="freq",
                     freqcutoff=1.5, maxnpieces=3, channelavg=True, extendflags=False)


def check_products(libraries):
    empty, narrow, broad = make_fixture("averaging-source.ms")
    with table("averaging-source.ms", readonly=False, ack=False) as main:
        main.putcol("CORRECTED_DATA", main.getcol("DATA"))
    cases = (
        (0, "freq", "", "data", "", False, (16,)),
        (1, "freq", "", "data", "", False, ()),
        ([], "freq", "", "data", "", False, ()),
        (2, "freq", "", "data", "", False, (16, 17)),
        ([2], "freq", "", "data", "", False, (16, 17)),
        (4, "freq", "", "data", "", False, ()),
        (5, "freq", "", "data", "", False, ()),
        (2, "freq", "0:8~31", "data", "", False, (16, 17)),
        (2, "freq", "0:8~15;16~31", "data", "", False, (16, 17)),
        (2, "freq", "0:6~15;18~31", "data", "", False, ()),
        (2, "time", "0:8~19", "data", "", False, range(8, 20)),
        (4, "time", "0:8~19", "data", "", False, range(8, 20)),
        (1, "time", "", "corrected", "", False, range(32)),
        (2, "freq", "", "data", "preflag", False, ()),
        (2, "freq", "", "data", "SIGMA_SPECTRUM", False, ()),
        (2, "freq", "", "data", "WEIGHT_SPECTRUM", False, (16, 17)),
        (2, "freq", "", "corrected", "WEIGHT_SPECTRUM", False, ()),
        (2, "freq", "", "corrected", "SIGMA_SPECTRUM", False, (16, 17)),
        (2, "freq", "", "data", "phase-cancellation", False, ()),
        (2, "freq", "", "data", "", True, (16, 17)),
        (2, "freq", "0:8~31", "data", "", True, (16, 17)),
        (2, "freq", "0:8~23", "data", "", True, ()),
    )
    for index, (width, direction, spw, column, seed, extend, channels) in enumerate(cases):
        for name, task in libraries:
            path = f"average-{index}-{name}.ms"
            copytree("averaging-source.ms", path)
            seeded = empty.copy()
            with table(path, readonly=False, ack=False) as main:
                if seed == "preflag":
                    seeded[narrow, 16, 1] = True
                    main.putcol("FLAG", seeded)
                elif seed.endswith("_SPECTRUM"):
                    values = np.ones(empty.shape, dtype=np.float32)
                    values[narrow, 16, 1] = 1e10 if seed.startswith("SIGMA") else 1e-12
                    put_samples(main, seed, values)
                elif seed == "phase-cancellation":
                    data = main.getcol("DATA")
                    data[narrow, 17, 1] = -28.
                    main.putcol("DATA", data)
            expected = seeded.copy()
            for channel in channels:
                if direction == "time":
                    expected[broad, channel, 0] = True
                elif extend:
                    expected[narrow, channel, :] = True
                else:
                    expected[narrow, channel, 1] = True
            options = dict(vis=path, mode="tfcrop", antenna="0&1", datacolumn=column, spw=spw,
                           flagdimension=direction, channelavg=True, chanbin=width, freqcutoff=1.5,
                           timecutoff=2.5, timefit="poly", maxnpieces=3, extendflags=extend)
            run_product(name, task, options, seeded, expected)
    return len(cases), broad


def run_product(name, task, options, seeded, expected):
    path = options["vis"]
    original, auxiliary = science_state(path), auxiliary_content(path)
    assert task(action="calculate", **options) == {}
    if name == "native":
        calculated = tasks.run("flagdata", action="calculate", **options)
        result = loads(calculated.stdout)["result"]
        assert result["changed_samples"] == expected.sum() - seeded.sum(), options
    verify_flags(path, seeded)
    check_science_state(path, original)
    assert auxiliary_content(path) == auxiliary
    assert task(**options) == {}
    try:
        verify_flags(path, expected)
    except AssertionError as error:
        raise AssertionError((name, options)) from error
    check_science_state(path, original)
    verify_flags(str(Path(path + ".flagversions", "flags.flagdata_1")), seeded)


def check_windows(libraries):
    source = "multiple-windows.ms"
    copytree("averaging-source.ms", source)
    add_second_window(Path(source))
    with table(source, readonly=False, ack=False) as main:
        data = main.getcol("DATA")
        matched = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        data[matched, 16, 1] = 30.
        main.putcol("DATA", data)
        desc = main.getcol("DATA_DESC_ID")
        seeded = main.getcol("FLAG")
    cases = (
        ([2, 0], "", ((16, 17), (16,))),
        ([0, 2], "", ((16,), (16, 17))),
        ([2, 2], "0:8~15;16~31,1:6~15;18~31", ((16, 17), ())),
    )
    for index, (width, spw, expected_channels) in enumerate(cases):
        expected = seeded.copy()
        for window, channels in enumerate(expected_channels):
            for channel in channels:
                expected[matched & (desc == window), channel, 1] = True
        for name, task in libraries:
            path = f"windows-{index}-{name}.ms"
            copytree(source, path)
            run_product(name, task, FREQUENCY_FIT | dict(vis=path, chanbin=width, spw=spw),
                        seeded, expected)
            if index == 0:
                with casa_table(path) as casa:
                    np.testing.assert_array_equal(casa.getcol("FLAG").transpose(2, 1, 0), expected)
    return len(cases)


def check_other_sources(libraries):
    source = "averaged-sources.ms"
    copytree("averaging-source.ms", source)
    with table(source, readonly=False, ack=False) as main:
        data = main.getcol("DATA")
        narrow = data[:, 16, 1] == 30.
        broad = data[:, 0, 0] == 30.
        main.putcol("CORRECTED_DATA", data + 1.)
        model = np.ones_like(data)
        model[narrow, 16, 1] = -28.
        main.putcol("MODEL_DATA", model)
        weights = np.ones(data.shape, dtype=np.float32)
        weights[narrow, 16, 1] = 30.
        put_samples(main, "WEIGHT_SPECTRUM", weights)
        seeded = main.getcol("FLAG")
    float_source = "averaged-float.ms"
    copytree(source, float_source)
    with table(float_source, readonly=False, ack=False) as main:
        put_samples(main, "FLOAT_DATA", data.real.copy())
        main.removecols("DATA")
    cases = (
        ("model", "", narrow, (1,)),
        ("residual", "", narrow, (1,)),
        ("residual_data", "", narrow, ()),
        ("weight", "REAL_YY", narrow, (1,)),
        ("weight_spectrum", "REAL_YY", narrow, (1,)),
        ("float_data", "REAL_YY", narrow, (1,)),
        ("data", "ABS_I", narrow & ~broad, (0, 1)),
    )
    for index, (column, expression, rows, planes) in enumerate(cases):
        expected = seeded.copy()
        for plane in planes:
            expected[rows, 16:18, plane] = True
        for name, task in libraries:
            path = f"averaged-source-{index}-{name}.ms"
            copytree(float_source if column == "float_data" else source, path)
            options = dict(vis=path, chanbin=2, datacolumn=column, correlation=expression)
            run_product(name, task, FREQUENCY_FIT | options, seeded, expected)
    return len(cases)


def check_refusals(libraries):
    kind = "Error parsing ChannelAverageTVI configuration"
    cases = (
        ({"chanbin": [2, 3]}, kind),
        ({"chanbin": [2, 3], "maxnpieces": 0, "extendflags": True}, kind),
        ({"chanbin": np.array(2)}, kind),
        ({"channelavg": [True], "chanbin": 2}, "RecordInterface::asBool - invalid data type"),
    )
    for index, (values, message) in enumerate(cases):
        for name, task in libraries:
            path = f"averaged-refusal-{index}-{name}.ms"
            copytree("averaging-source.ms", path)
            seeded, _ = flags(path)
            original, auxiliary = science_state(path), auxiliary_content(path)
            for action in ("calculate", "apply"):
                try:
                    task(**(FREQUENCY_FIT | dict(vis=path, action=action) | values))
                except RuntimeError as error:
                    assert str(error) == message, (name, values, error)
                else:
                    raise AssertionError((name, values, action))
                verify_flags(path, seeded)
                check_science_state(path, original)
                if action == "calculate":
                    assert auxiliary_content(path) == auxiliary
                else:
                    verify_flags(str(Path(path + ".flagversions", "flags.flagdata_1")), seeded)
    return len(cases)


def check_extension_iterator(libraries, broad):
    for index, overrides in enumerate((dict(chanbin=0, spw="0:0~4"), dict(chanbin=2, maxnpieces=0))):
        for name, task in libraries:
            path = f"average-extend-{index}-{name}.ms"
            copytree("averaging-source.ms", path)
            with table(path, readonly=False, ack=False) as main:
                main.putcol("DATA", np.ones_like(main.getcol("DATA")))
                seeded = main.getcol("FLAG")
                if index == 0:
                    seeded[broad, :4, :] = True
                else:
                    seeded[broad, 16, 0] = True
                main.putcol("FLAG", seeded)
            expected = seeded.copy()
            if index == 0:
                expected[broad, 4, :] = True
            options = FREQUENCY_FIT | dict(vis=path, extendflags=True, freqcutoff=1e5) | overrides
            run_product(name, task, options, seeded, expected)


def check_fourier_excluded_channel(libraries):
    empty, narrow, model = seed_fourier_models()
    row = np.flatnonzero(narrow)[0]
    assert abs(model[row, 6, 0]) > 0.
    seeded = empty.copy()
    seeded[row, 6, 0] = True
    expected = seeded.copy()
    expected[narrow, 16:18, 0] = True
    for source in ("scratch", "virtual"):
        for excluded_amplitude in (0., 1e12):
            for name, task in libraries:
                path = f"averaged-fourier-{source}-{excluded_amplitude:g}-{name}.ms"
                copytree(f"model-{source}.ms", path)
                with table(path, readonly=False, ack=False) as main:
                    observed = model + 1.
                    observed[narrow, 16, 0] += 30.
                    observed[row, 6, 0] += excluded_amplitude
                    main.putcol("DATA", observed)
                    main.putcol("FLAG", seeded)
                options = dict(vis=path, field="0", chanbin=2, datacolumn="residual_data",
                               correlation="ABS_XX")
                run_product(name, task, FREQUENCY_FIT | options, seeded, expected)


def verify_flags(path, expected):
    actual, rows = flags(path)
    np.testing.assert_array_equal(actual, expected)
    assert not rows.any()


def check_stride(broad, native):
    safe = "explicit-channel-list.ms"
    copytree("averaging-source.ms", safe)
    original, _ = flags(safe)
    expected = original.copy()
    for channel in range(8, 24, 2):
        expected[broad, channel, 0] = True
    common = dict(mode="tfcrop", antenna="0&1", datacolumn="data", flagdimension="time",
                  timefit="poly", timecutoff=2.5, maxnpieces=3, channelavg=True, chanbin=2,
                  extendflags=False, flagbackup=False)
    assert casatasks.flagdata(vis=safe, spw="0:8;10;12;14;16;18;20;22", **common) == {}
    verify_flags(safe, expected)

    failing = "pinned-stride.ms"
    copytree("averaging-source.ms", failing)
    execute = """
import json
import resource
import sys
from casatasks import flagdata

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
flagdata(**json.loads(sys.argv[1]))
"""
    completed = run([sys.executable, "-c", execute, dumps(dict(vis=failing, spw="0:8~23^2", **common))],
                    capture_output=True, text=True, timeout=30)
    assert completed.returncode == -SIGABRT and "ArraySlicerError" in completed.stderr, completed.stderr

    if native:
        path = "native-stride.ms"
        copytree("averaging-source.ms", path)
        assert native(vis=path, spw="0:8~23^2", **common) == {}
        verify_flags(path, expected)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-channelavg-") as root, chdir(root):
        products, broad = check_products(libraries)
        products += check_windows(libraries) + check_other_sources(libraries)
        refusals = check_refusals(libraries)
        check_extension_iterator(libraries, broad)
        check_fourier_excluded_channel(libraries)
        check_stride(broad, tasks.flagdata)
    print(f"{products} TFCrop channel averaging products, {refusals} refusals, "
          "two extension iterator controls, four Fourier controls and a strided selection verified")


if __name__ == "__main__":
    main()
