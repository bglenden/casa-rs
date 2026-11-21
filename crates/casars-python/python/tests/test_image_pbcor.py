"""Primary-beam arithmetic, masks, and native CASA interchange."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("mode,cutoff", [("D", 0.25), ("multiply", 0.25), (" divide ", -1.)])
@pytest.mark.parametrize("form", ["array", "list"])
def test_array_beam_arithmetic_and_cutoff_boundaries(casa, mode, cutoff, form):
    values = np.array([[2, 4, 8], [10, 12, 14]], dtype=np.float32)
    beam = np.array([[1., .5, 0.], [-.125, .25, 2.]], dtype=np.float32)
    division = mode.strip().lower().startswith("d")
    with np.errstate(divide="ignore"):
        expected = values / beam if division else values * beam
    validity = np.ones_like(beam, dtype=bool)
    if cutoff >= 0:
        validity = beam >= cutoff if division else beam <= cutoff
    for api in (tools, casa):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.sethistory("fixture", ["source marker"])
        result = source.pbcor(beam if form == "array" else beam.tolist(), mode=mode, cutoff=cutoff)
        try:
            np.testing.assert_array_equal(result.getchunk(), expected)
            np.testing.assert_array_equal(result.getchunk(getmask=True), validity)
            assert result.history()[-3] == "source marker"
            assert result.history()[-2] == "Ran ia.pbcor"
            assert "cutoff=" in result.history()[-1]
            np.testing.assert_array_equal(source.getchunk(), values)
        finally:
            result.done()
            source.done()


def test_direction_plane_beam_matches_cube_broadcast_and_strided_selection(casa):
    data = np.arange(5 * 6 * 2 * 4, dtype=np.float32).reshape(5, 6, 2, 4) + 1
    beam = np.arange(1, 31, dtype=np.float32).reshape(5, 6) / 32.
    full = data * beam[:, :, None, None]
    regions = casa.regionmanager()
    try:
        section = regions.box([0, 1, 0, 0], [4, 5, 1, 3], [2, 2, 1, 2])
    finally:
        regions.done()
    for api in (tools, casa):
        source = api.image()
        assert source.fromarray(pixels=data)
        selected = source.pbcor(beam, region=section, mode="multiply")
        try:
            np.testing.assert_array_equal(selected.getchunk(), full[::2, 1::2, :, ::2])
            assert selected.getchunk(getmask=True).all()
            before = source.coordsys()
            after = selected.coordsys()
            try:
                np.testing.assert_allclose(
                    after.toworld([1, 2, 1, 1])["numeric"],
                    before.toworld([2, 5, 1, 2])["numeric"],
                    rtol=1e-11, atol=1e-11,
                )
            finally:
                before.done()
                after.done()
        finally:
            selected.done()
            source.done()


def test_named_beam_native_writer_reader_pairs_and_mask_history(casa, tmp_path):
    data = np.arange(1, 21, dtype=np.float32).reshape(4, 5)
    beam = np.arange(1, 21, dtype=np.float32).reshape(4, 5) / 32.
    expected_mask = (data > 3) & (data != 11) & (beam >= .25)
    for writer in (tools, casa):
        source = writer.image()
        beam_image = writer.image()
        input_path = str(tmp_path / f"input-{writer.__name__}.im")
        beam_path = str(tmp_path / f"beam-{writer.__name__}.im")
        product_path = str(tmp_path / f"pbcor-{writer.__name__}.im")
        assert source.fromarray(input_path, data)
        assert source.setbrightnessunit("Jy/beam")
        assert source.sethistory("fixture", ["source marker"])
        assert source.calcmask(f'"{input_path}" > 3')
        assert beam_image.fromarray(beam_path, beam)
        assert beam_image.sethistory("fixture", ["beam marker"])
        assert beam_image.calcmask(f'"{beam_path}" != 0.34375')
        assert beam_image.done()
        product = source.pbcor(beam_path.encode(), product_path, cutoff=.25)
        product.done()
        source.done()
        for reader in (tools, casa):
            output = reader.image()
            assert output.open(product_path)
            try:
                np.testing.assert_array_equal(output.getchunk(), data / beam)
                np.testing.assert_array_equal(output.getchunk(getmask=True), expected_mask)
                assert output.brightnessunit() == "Jy/beam"
                history = output.history()
                assert history[0] == f"History of image {input_path}"
                assert "source marker" in history and "beam marker" in history
                assert history[-1] == f"End history of image {beam_path}"
            finally:
                output.done()


def test_pbcor_validation_and_alias_read(casa, tmp_path):
    data = np.arange(1, 16, dtype=np.float32).reshape(3, 5)
    for api in (tools, casa):
        tool = api.image()
        with pytest.raises(RuntimeError, match="Unable to create image"):
            tool.pbcor()
        path = str(tmp_path / f"source-{api.__name__}.im")
        assert tool.fromarray(path, data)
        try:
            cases = [
                ({}, "image filename is empty"),
                ({"pbimage": np.ones_like(data, dtype=np.int32)}, "Unsupported type intvec"),
                ({"pbimage": [[1] * 5] * 3}, "Unsupported type intvec"),
                ({"pbimage": [1.0]}, "Primary beam array is of wrong shape"),
                ({"pbimage": np.ones_like(data), "mode": "wrong"}, "Unknown mode wrong"),
                ({"pbimage": np.ones_like(data), "box": "0,0,1,1"}, "box and regionPtr"),
            ]
            for options, message in cases:
                with pytest.raises(RuntimeError, match=message):
                    tool.pbcor(**options)
            self_corrected = tool.pbcor(path)
            try:
                np.testing.assert_array_equal(self_corrected.getchunk(), np.ones_like(data))
            finally:
                self_corrected.done()
            np.testing.assert_array_equal(tool.getchunk(), data)
        finally:
            tool.done()


def test_pbcor_with_numpy_floating_arrays_and_pixel_box():
    data = np.arange(1, 21, dtype=np.float32).reshape(4, 5)
    beam = np.broadcast_to(np.array([0., .125, .25, .5, 1.], dtype=np.float64), data.shape)
    source = tools.image()
    assert source.fromarray(pixels=data)
    result = source.pbcor(beam, region=None, box="1,1,3,3", mode="m", cutoff=.25)
    try:
        selected = np.s_[1:4, 1:4]
        np.testing.assert_array_equal(result.getchunk(), (beam * data)[selected])
        np.testing.assert_array_equal(result.getchunk(getmask=True), (beam <= .25)[selected])
    finally:
        result.done()
        source.done()
