# SPDX-License-Identifier: LGPL-3.0-or-later
"""Runnable native counterpart of upstream's disabled combined-imaging case."""
import gzip
import json
import os
from pathlib import Path
import tarfile

import numpy as np
import pytest

from casars import tasks
from casars._native_image import NativeImageView
from casars.tasks import _runner


def test_upstream_combined_imaging_produces_if_tp_and_feather_images(tmp_path):
    """Preserve the intended IF/TP checks and compare the feather calculation.

    CASA 6.7.6.14 disables its 1458-pixel/128-channel test, whose feather
    binding fails and whose .image0 assertion omits the actual .sd prefix.
    Use the existing compact, independently CASA-generated scenario and
    numerical reference, within the regular test machine's memory budget.
    """
    try:
        _runner._task_executable("simanalyze")
    except tasks.TaskBinaryNotFoundError as error:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(str(error))
    fixtures = Path(__file__).resolve().parents[4] / "crates" / "casa-simulation/tests/data"
    with tarfile.open(fixtures / "simanalyze_multi_inputs.tar.gz") as archive:
        options = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        archive.extractall(tmp_path, **options)
    with gzip.open(fixtures / "simanalyze_multi_casa.json.gz", "rt") as handle:
        reference = json.load(handle)
    tasks.run("simanalyze", workspace=tmp_path, project="seed", image=True,
              vis="seed.small.ms,seed.small.sd.ms", modelimage="", imsize=[128, 128],
              niter=0, weighting="natural", pbcor=True, analyze=False,
              graphics="none", overwrite=True)
    project = tmp_path / "seed"
    for suffix in ("small.image", "sd.image", "sd.image0", "sd.image0.scaled", "sd.image0.weight"):
        assert (project / f"seed.{suffix}").is_dir()
    expected = reference["cases"]["feather"]["images"]["feather.small.feather.image"]
    image = NativeImageView.open(project / "seed.small.feather.image")
    assert list(image.shape) == expected["shape"]
    actual = image.get_slice([0] * len(image.shape), list(image.shape))
    mask = image.get_mask_slice([0] * len(image.shape), list(image.shape))
    np.testing.assert_array_equal(mask, np.asarray(expected["mask"], dtype=bool))
    wanted = np.asarray(expected["pixels"])
    nrms = np.linalg.norm((actual - wanted)[mask]) / np.linalg.norm(wanted[mask])
    assert nrms <= 1e-3
