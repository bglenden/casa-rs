# SPDX-License-Identifier: LGPL-3.0-or-later
"""Public SIGINT during real coordinate mapping, resampling and publication."""

import numpy as np
import pytest

from casars import _core, tasks, tools
from test_native_imstat_interrupt import image_contents
from test_native_transform_interrupt import messages


pytestmark = pytest.mark.skipif(
    not hasattr(_core, "_arm_imregrid_test"), reason="requires imregrid-test-hooks on POSIX"
)


def assert_regrid(output, template, pixels, valid):
    reader = tools.image()
    assert reader.open(str(output))
    try:
        np.testing.assert_array_equal(reader.getchunk(getmask=True), valid)
        np.testing.assert_allclose(reader.getchunk()[valid], pixels[valid], atol=1e-6, rtol=0)
    finally:
        assert reader.done()
    result = tasks.imregrid(str(output), template="get")
    np.testing.assert_array_equal(result["shap"], template["shap"])
    for field in ("crpix", "crval", "cdelt", "pc"):
        np.testing.assert_array_equal(
            result["csys"]["direction0"][field], template["csys"]["direction0"][field]
        )


@pytest.mark.parametrize(
    ("stage", "existing"),
    [
        ("coordinate-batch", False),
        ("interpolation-batch", True),
        pytest.param("before-publication", False, id="before-publication-create"),
        pytest.param("before-publication", True, id="before-publication-overwrite"),
        ("publication-accepted", True),
    ],
)
def test_imregrid_sigint_respects_calculation_and_publication(tmp_path, monkeypatch, stage, existing):
    monkeypatch.chdir(tmp_path)
    # Three full 4096-pixel batches plus a tail, with a fractional WCS shift
    # that requires both coordinate mapping and bilinear interpolation.
    x, y = np.indices((160, 80))
    pixels = (2 * x + 3 * y).astype(np.float32)
    valid = np.ones(pixels.shape, dtype=bool)
    valid[35, 40] = False
    source = tmp_path / "source.im"
    output = tmp_path / "output.im"
    writer = tools.image()
    assert writer.fromarray(str(source), pixels)
    assert writer.putregion(pixelmask=valid)
    assert writer.done()
    original_source = image_contents(source)
    if existing:
        assert writer.fromarray(str(output), np.full((12, 16), -7, dtype=np.float32))
        assert writer.done()
        original_output = image_contents(output)

    template = tasks.imregrid(str(source), template="get")
    template["csys"]["direction0"]["crpix"] += [0.75, -0.25]
    # The input plane is affine, so a bilinear sample has this analytic
    # value. Only the four samples using the masked input pixel are invalid.
    expected = 2 * (x - 0.75) + 3 * (y + 0.25)
    expected_mask = np.ones(pixels.shape, dtype=bool)
    expected_mask[35:37, 39:41] = False

    log_path = tmp_path / "imregrid.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(log_path))
    parameters = dict(imagename=str(source), template=template, output=str(output),
                      interpolation="linear", decimate=0, overwrite=existing)
    _core._arm_imregrid_test(stage)
    with pytest.raises(KeyboardInterrupt):
        tasks.imregrid(**parameters)

    log = messages(log_path)
    prefix = "imregrid test checkpoint "
    checkpoints = [line.removeprefix(prefix) for line in log if line.startswith(prefix)]
    if stage == "publication-accepted":
        assert checkpoints[-2:] == ["BeforePublication", "PublicationAccepted"]
        assert "imregrid completed task=imregrid" in log
        assert_regrid(output, template, expected, expected_mask)
    else:
        if stage == "coordinate-batch":
            assert checkpoints == ["CoordinateBatchStarted(0)", "CoordinateBatchFinished(0)"]
        elif stage == "interpolation-batch":
            interpolation = [label for label in checkpoints if label.startswith("Interpolation")]
            assert interpolation == ["InterpolationBatchStarted(0)", "InterpolationBatchFinished(0)"]
            assert checkpoints[-1] == "InterpolationBatchFinished(0)"
            assert "CoordinateBatchStarted(4096)" in checkpoints
        else:
            assert checkpoints[-1] == "BeforePublication"
            assert "InterpolationBatchStarted(4096)" in checkpoints
        assert "imregrid interrupted task=imregrid reason=imregrid interrupted" in log
        if existing:
            assert image_contents(output) == original_output
        else:
            assert not output.exists()

    assert not list(tmp_path.glob(".tmp*"))
    assert image_contents(source) == original_source
    assert tasks.imregrid(**parameters) is None
    assert_regrid(output, template, expected, expected_mask)
    assert not list(tmp_path.glob(".tmp*"))


def test_imregrid_sigint_stops_spectral_grid_comparison(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    pixels = np.arange(2 * 3 * 8193, dtype=np.float32).reshape(2, 3, 8193)
    writer = tools.image()
    assert writer.fromarray("source.im", pixels)
    assert writer.done()
    template = tasks.imregrid("source.im", template="get")

    trace = tmp_path / "spectral.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    # Equal spectral grids skip interpolation only after comparing every channel.
    request = dict(imagename="source.im", output="output.im", template=template,
                   axes=[2], asvelocity=False)
    _core._arm_imregrid_test("coordinate-batch")
    with pytest.raises(KeyboardInterrupt):
        tasks.imregrid(**request)
    log = messages(trace)
    prefix = "imregrid test checkpoint "
    assert [line.removeprefix(prefix) for line in log if line.startswith(prefix)] == [
        "CoordinateBatchStarted(0)", "CoordinateBatchFinished(0)",
    ]
    assert "imregrid interrupted task=imregrid reason=imregrid interrupted" in log
    assert not (tmp_path / "output.im").exists()
    assert not list(tmp_path.glob(".tmp*"))
    assert tasks.imregrid(**request) is None
    assert_regrid(tmp_path / "output.im", template, pixels, np.ones(pixels.shape, dtype=bool))
