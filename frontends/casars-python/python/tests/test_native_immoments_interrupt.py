# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt the public task during profile calculation and product publication."""

import numpy as np
import pytest

from casars import _core, tasks, tools
from test_native_imstat_interrupt import image_contents
from test_native_transform_interrupt import messages


pytestmark = pytest.mark.skipif(
    not hasattr(_core, "_arm_immoments_test"), reason="requires immoments-test-hooks on POSIX"
)


def read_image(path):
    reader = tools.image()
    assert reader.open(str(path))
    try:
        return reader.getchunk(), reader.getchunk(getmask=True)
    finally:
        assert reader.done()


def assert_moment_products(average, maximum, pixels, valid):
    averaged, average_mask = read_image(average)
    peaked, peak_mask = read_image(maximum)
    counts = valid.sum(axis=2, keepdims=True)
    good = counts > 0
    np.testing.assert_array_equal(average_mask, good)
    np.testing.assert_array_equal(peak_mask, good)
    totals = np.where(valid, pixels, 0).sum(axis=2, keepdims=True, dtype=np.float64)
    np.testing.assert_allclose(averaged[good], totals[good] / counts[good])
    # The only unmasked NaN follows a finite sample. CASA's comparison
    # against the existing extremum therefore keeps that finite candidate.
    expected_peaks = np.fmax.reduce(np.where(valid, pixels, -np.inf), axis=2, keepdims=True)
    np.testing.assert_array_equal(peaked[good], expected_peaks[good])
    assert np.isnan(averaged[1, 0, 0])


@pytest.mark.parametrize("stage", ["profile-batch", "before-publication", "during-publication"])
def test_immoments_sigint_respects_publication(tmp_path, monkeypatch, stage):
    monkeypatch.chdir(tmp_path)
    pixels = np.arange(8 * 12 * 24, dtype=np.float32).reshape(8, 12, 24)
    pixels[1, 0, 7] = np.nan
    valid = np.ones(pixels.shape, dtype=bool)
    valid[0, 0, :] = False
    valid[2, 3, -1] = False
    source = tmp_path / "cube.im"
    average = tmp_path / "moments.im.average"
    maximum = tmp_path / "moments.im.maximum"
    writer = tools.image()
    assert writer.fromarray(str(source), pixels)
    assert writer.putregion(pixelmask=valid)
    assert writer.done()
    assert writer.fromarray(str(average), np.full((8, 12, 1), -7, dtype=np.float32))
    assert writer.done()
    original_input = image_contents(source)
    original_average = image_contents(average)

    trace = tmp_path / "immoments.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    parameters = dict(imagename="cube.im", outfile="moments.im", moments=[-1, 8], axis=2,
                      overwrite=True)
    _core._arm_immoments_test(stage)
    with pytest.raises(KeyboardInterrupt):
        tasks.immoments(**parameters)

    log = messages(trace)
    prefix = "immoments test checkpoint "
    checkpoints = [line.removeprefix(prefix) for line in log if line.startswith(prefix)]
    if stage == "during-publication":
        assert checkpoints[-3:] == ["BeforePublication", "ProductPublished(0)", "ProductPublished(1)"]
        assert "immoments completed task=immoments" in log
        assert_moment_products(average, maximum, pixels, valid)
    else:
        assert image_contents(average) == original_average
        assert not maximum.exists()
        if stage == "profile-batch":
            assert checkpoints == ["ProfileBatchStarted(0)", "ProfileBatchFinished(0)"]
        else:
            assert checkpoints[-1] == "BeforePublication"
            assert "ProfileBatchStarted(32)" in checkpoints
        assert "immoments interrupted task=immoments reason=immoments interrupted" in log
    assert not list(tmp_path.glob(".tmp*"))
    assert image_contents(source) == original_input
    actual, actual_mask = read_image(source)
    np.testing.assert_array_equal(actual, pixels)
    np.testing.assert_array_equal(actual_mask, valid)

    assert tasks.immoments(**parameters) is None
    assert_moment_products(average, maximum, pixels, valid)
    assert not list(tmp_path.glob(".tmp*"))
