# SPDX-License-Identifier: LGPL-3.0-or-later
"""Native regressions from CASA 6.7.6.14's test_task_simobserve.py."""
import os
from pathlib import Path
import tarfile

import numpy as np
import pytest

from casars import tasks
from _table_readback import open_table, read_column
from casars.tasks import _runner


@pytest.fixture(scope="module")
def sky_inputs(tmp_path_factory):
    try:
        _runner._task_executable("simobserve")
    except tasks.TaskBinaryNotFoundError as error:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(str(error))
    root = tmp_path_factory.mktemp("upstream-simobserve")
    fixtures = Path(__file__).resolve().parents[3] / "casa-simulation/tests/data"
    with tarfile.open(fixtures / "skymodel_inputs.tar.gz") as archive:
        options = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        archive.extractall(root, **options)
    config = root / "small.cfg"
    config.write_text(
        "# observatory=ALMA\n# coordsys=XYZ\n"
        "2225021.163600 -5440082.369940 -2481681.150540 12 A00\n"
        "2225091.163600 -5440097.369940 -2481681.150540 12 A01\n"
        "2225121.163600 -5440047.369940 -2481681.150540 12 A02\n"
        "2224981.163600 -5440127.369940 -2481681.150540 12 A03\n"
    )
    return root / "inputs", config


@pytest.mark.parametrize("case", ["testBad_incell", "test_notComp", "test_notImage"])
def test_upstream_bad_sky_inputs_report_casas_error(sky_inputs, tmp_path, case):
    inputs, config = sky_inputs
    image, components = inputs / "dfs_cube.im", inputs / "core5ps.cl"
    if case == "testBad_incell":
        parameters = dict(skymodel=str(image), incell="5bad")
        expected = ('Error in QuantumHolder::fromString with input string "5bad": '
                    'Illegal input units or format')
    elif case == "test_notComp":
        parameters = dict(complist=str(image))
        expected = f"{image} is non existant or is not a componentlist table"
    else:
        parameters = dict(skymodel=str(components))
        expected = f"Unable to open image {components}."
    with pytest.raises(tasks.TaskExecutionError) as error:
        tasks.run("simobserve", workspace=tmp_path, project="bad-input",
                  antennalist=str(config), graphics="none", thermalnoise="",
                  **parameters)
    # Keep the upstream substring assertions, including CASA's spelling.
    assert expected in str(error.value)


@pytest.mark.parametrize("parameter,value", [
    ("maptype", "badname"), ("obsmode", "badname"),
    ("thermalnoise", "badname"), ("graphics", "badname"),
    ("user_pwv", -1.), ("t_ground", -1.), ("t_sky", -1.),
    ("tau0", -1.), ("leakage", -1.),
])
def test_upstream_disabled_parameter_checks_refuse_invalid_values(parameter, value):
    """The nine disabled testBad_* cases expect an obsolete CASA message.

    CASA 6.7.6.14 now rejects these values with a parameter-error dictionary;
    casa-rs rejects them with ValueError naming the invalid parameter.
    """
    with pytest.raises(ValueError, match=f"invalid value for {parameter}:"):
        tasks.simobserve(**{parameter: value})


@pytest.mark.parametrize("case", ["testComp_sdObs", "testComp_sdAll", "testSC_sdObs", "testSC_sdAll"])
def test_upstream_disabled_component_sd_cases_preserve_casas_refusal(sky_inputs, tmp_path, case):
    inputs, config = sky_inputs
    parameters = dict(complist=str(inputs / "core5ps.cl"))
    if case.startswith("testSC"):
        parameters["skymodel"] = str(inputs / "dfs_cube.im")
    # CAS-5095 is still an explicit CASA task refusal, rather than a
    # working reference. testComp_sdObs also has an undefined comp_nchan.
    with pytest.raises(tasks.TaskExecutionError, match="Single dish simulation has a flux recovery issue"):
        tasks.run("simobserve", workspace=tmp_path, project="component-sd",
                  obsmode="sd", sdantlist=str(config), graphics="none", **parameters)


@pytest.mark.parametrize("sky", ["image", "components", "both"])
def test_upstream_leakage_cases_apply_the_saved_jones_matrices(sky_inputs, tmp_path, sky):
    """testSky_intLeak and the two disabled intLeak cases, without RNG quantiles."""
    inputs, config = sky_inputs
    parameters = {}
    if sky != "components":
        parameters["skymodel"] = str(inputs / "dfs_cube.im")
    if sky != "image":
        parameters["complist"] = str(inputs / "core5ps.cl")
    tasks.run("simobserve", workspace=tmp_path, project="leak", antennalist=str(config),
              graphics="none", thermalnoise="", leakage=.1 if sky == "image" else .5,
              indirection="J2000 19h00m00 -23d00m00", mapsize="1arcsec",
              incell="1arcsec", incenter="345GHz", inwidth="1GHz", compwidth="1GHz",
              totaltime="20s", integration="10s", **parameters)
    root = tmp_path / "leak"
    clean = open_table(root / "leak.small.ms")
    leaked = open_table(root / "leak.small.fullpol.ms")
    solutions = open_table(root / "leak.small.fullpol.cal")
    dterms = np.asarray(read_column(solutions, "CPARAM"))[:, :, 0]
    antennas = np.asarray(read_column(solutions, "ANTENNA1"))
    leakages = {int(ant): d for ant, d in zip(antennas, dterms)}
    before = np.asarray(read_column(clean, "DATA"))
    after = np.asarray(read_column(leaked, "DATA"))
    assert before.shape[1] == 4 and np.any(before != 0)
    expected = before.copy()
    # CASA JonesGenLin: first-order D leakage changes cross hands only.
    for row, (a, b) in enumerate(zip(read_column(clean, "ANTENNA1"), read_column(clean, "ANTENNA2"))):
        da, db = leakages[int(a)], leakages[int(b)]
        expected[row, 1] += da[0] * before[row, 3] + db[1].conjugate() * before[row, 0]
        expected[row, 2] += da[1] * before[row, 0] + db[0].conjugate() * before[row, 3]
    np.testing.assert_allclose(after, expected, rtol=2e-6, atol=1e-7)
    assert np.any(after != before)
