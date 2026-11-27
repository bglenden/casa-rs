# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise explicit output formats through the installed Python task."""

import json
from pathlib import Path
import sys
import tarfile

import numpy as np
import pytest

from casars import tasks
from casars.tools import table


TRIAL = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)
TRIAL_STORES = {"msv2": "ms_trial.ms", "msv4": "ms_trial.ps.zarr"}
MAIN_COLUMNS = ("TIME", "ANTENNA1", "ANTENNA2", "DATA", "FLAG", "WEIGHT")
WINDOW_COLUMNS = ("CHAN_FREQ", "REF_FREQUENCY")


def read_columns(path, names):
    reader = table()
    assert reader.open(str(path))
    try:
        return {name: reader.getcol(name) for name in names}
    finally:
        reader.close()


def sorted_main_columns(path):
    columns = read_columns(path, MAIN_COLUMNS)
    order = np.lexsort((columns["ANTENNA2"], columns["ANTENNA1"], columns["TIME"]))
    return {name: values[..., order] for name, values in columns.items()}


def assert_msv2(path, main, window):
    for name, actual in sorted_main_columns(path).items():
        np.testing.assert_array_equal(actual, main[name], err_msg=name)
    for name, actual in read_columns(path / "SPECTRAL_WINDOW", WINDOW_COLUMNS).items():
        np.testing.assert_array_equal(actual, window[name], err_msg=name)


def assert_msv4(path, main, window):
    root = json.loads((path / "zarr.json").read_text())
    assert root["attributes"]["type"] == "processing_set"
    dataset = path / "ms_trial_0"
    frequency = json.loads((dataset / "frequency/zarr.json").read_text())
    assert frequency["attributes"]["reference_frequency"]["data"] == window["REF_FREQUENCY"][0]

    # This trial store has a complete 12-time by 10-baseline grid with eight
    # channels and four correlations. Read one uncompressed Zarr chunk per
    # variable directly to compare with the independently stored MSv2.
    shape = (12, 10, 8, 4)
    weight = np.broadcast_to(main["WEIGHT"][:, None, :], main["DATA"].shape)
    for name, values, dtype in (
        ("VISIBILITY", main["DATA"], "<c8"),
        ("FLAG", main["FLAG"], "?"),
        ("WEIGHT", weight, "<f4"),
    ):
        array = dataset / name
        metadata = json.loads((array / "zarr.json").read_text())
        assert tuple(metadata["shape"]) == shape
        assert tuple(metadata["chunk_grid"]["configuration"]["chunk_shape"]) == shape
        assert metadata["dimension_names"] == ["time", "baseline_id", "frequency", "polarization"]
        assert [codec["name"] for codec in metadata["codecs"]] == ["bytes"]
        actual = np.fromfile(array / "c/0/0/0/0", dtype=dtype).reshape(shape)
        expected = values.transpose(2, 1, 0).reshape(shape)
        np.testing.assert_array_equal(actual, expected, err_msg=name)


@pytest.mark.parametrize("informat", TRIAL_STORES)
@pytest.mark.parametrize("outformat", TRIAL_STORES)
def test_msconvert_selects_new_store_format(tmp_path, monkeypatch, informat, outformat):
    # The MSv4 input was produced by XRADIO from this same MSv2 fixture.
    for name in {TRIAL_STORES[informat], TRIAL_STORES["msv2"]}:
        with tarfile.open(TRIAL / f"{name}.tgz") as archive:
            options = {"filter": "data"} if sys.version_info >= (3, 12) else {}
            archive.extractall(tmp_path, **options)
    monkeypatch.chdir(tmp_path)
    reference = Path(TRIAL_STORES["msv2"])
    main = sorted_main_columns(reference)
    window = read_columns(reference / "SPECTRAL_WINDOW", WINDOW_COLUMNS)

    if outformat == "msv2":
        output = Path("output.ms")
        options = {"padding": "omitted"}
    else:
        output = Path("output.ps.zarr")
        options = {"withpointing": False, "codec": "none", "chunks": [0, 0, 0, 0]}
    result = tasks.msconvert(
        vis=TRIAL_STORES[informat], outputvis=output, outformat=outformat, **options
    )
    assert result.successful, result.describe()
    assert result.value["informat"] == informat
    assert result.value["outformat"] == outformat
    assert Path(result.value["outputvis"]) == output.resolve()
    if informat == outformat == "msv2":
        assert "HISTORY is not carried" in result.value["losses"]
    if outformat == "msv2":
        assert_msv2(output, main, window)
    else:
        assert_msv4(output, main, window)
