"""Direction-plane padding, streaming masks, and native ownership."""

import subprocess
import sys

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("padmask", [False, True])
@pytest.mark.parametrize("source_mask", [False, True])
def test_padding_keeps_interior_values_and_validity(casa, tmp_path, padmask, source_mask):
    data = np.arange(5 * 6 * 2 * 4, dtype=np.float32).reshape(5, 6, 2, 4)
    width = 2
    border = ((width, width), (width, width), (0, 0), (0, 0))
    expected_data = np.pad(data, border, constant_values=-13.)
    expected_mask = np.pad(
        data > 10 if source_mask else np.ones_like(data, dtype=bool),
        border,
        constant_values=padmask,
    )
    for api in (tools, casa):
        parent = api.image()
        source = str(tmp_path / f"source-{api.__name__}.im")
        assert parent.fromarray(source, data)
        if source_mask:
            assert parent.calcmask(f'"{source}" > 10', name="valid")
        result = parent.pad(npixels=width, value=-13., padmask=padmask)
        try:
            np.testing.assert_array_equal(result.getchunk(), expected_data)
            np.testing.assert_array_equal(result.getchunk(getmask=True), expected_mask)
            assert result.maskhandler("get") == ["T"]
            assert result.maskhandler("default") == [""]
            assert result.summary(list=False)["hasmask"] == bool(not expected_mask.all())
            assert result.history()[-4] == "Ran ia.pad"
            assert result.history()[-2] == f"Original {source} size => [5, 6, 2, 4]"
            assert result.history()[-1] == "New  size => [9, 10, 2, 4]"
            root = parent.coordsys()
            grown = result.coordsys()
            try:
                np.testing.assert_allclose(
                    grown.toworld([3, 4, 1, 2])["numeric"],
                    root.toworld([1, 2, 1, 2])["numeric"],
                    rtol=1e-11, atol=1e-11,
                )
            finally:
                root.done()
                grown.done()
            np.testing.assert_array_equal(parent.getchunk(), data)
        finally:
            result.done()
            parent.done()


def test_native_products_pad_the_mapped_direction_axes(casa, tmp_path):
    system = casa.coordsys().newcoordsys(direction=True, spectral=True, linear=1)
    assert system.transpose([2, 0, 3, 1])
    record = system.torecord()
    data = np.arange(3 * 4 * 2 * 5, dtype=np.float32).reshape(3, 4, 2, 5)
    border = ((0, 0), (1, 1), (0, 0), (1, 1))
    for writer in (tools, casa):
        parent = writer.image()
        assert parent.fromarray(pixels=data, csys=record)
        assert parent.setbrightnessunit("Jy/beam")
        assert parent.setrestoringbeam(major="4arcsec", minor="3arcsec", pa="23deg")
        path = str(tmp_path / f"pad-{writer.__name__}.im")
        result = parent.pad(path, value=5.5)
        result.done()
        parent.done()
        for reader in (tools, casa):
            result = reader.image()
            assert result.open(path)
            try:
                np.testing.assert_array_equal(result.getchunk(), np.pad(data, border, constant_values=5.5))
                expected_mask = np.pad(np.ones_like(data, dtype=bool), border, constant_values=False)
                np.testing.assert_array_equal(result.getchunk(getmask=True), expected_mask)
                assert result.maskhandler("get") == ["mask0"]
                assert result.brightnessunit() == "Jy/beam"
                assert result.restoringbeam()["major"] == {"value": 4., "unit": "arcsec"}
                cs = result.coordsys()
                try:
                    np.testing.assert_allclose(
                        cs.toworld([1, 2, 1, 3])["numeric"],
                        system.toworld([1, 1, 1, 2])["numeric"],
                        rtol=1e-11, atol=1e-11,
                    )
                finally:
                    cs.done()
            finally:
                result.done()


@pytest.mark.parametrize("selection", ["named", "strided", "stretch"])
def test_padding_selects_before_adding_the_border(casa, tmp_path, selection):
    data = np.arange(5 * 6 * 2 * 4, dtype=np.float32).reshape(5, 6, 2, 4)
    for api in (tools, casa):
        parent = api.image()
        source = str(tmp_path / f"section-{api.__name__}.im")
        assert parent.fromarray(source, data)
        if selection == "named":
            for channel in range(4):
                for polarization in range(2):
                    assert parent.setrestoringbeam(
                        major=f"{3 + channel + polarization / 10}arcsec",
                        minor="2arcsec", pa="23deg",
                        channel=channel, polarization=polarization,
                    )
            kwargs = {"region": "", "box": "1,1,3,3", "chans": "1~2", "stokes": "Q"}
            selected = data[1:4, 1:4, 1:2, 1:3]
        elif selection == "strided":
            maker = casa.regionmanager()
            try:
                region = maker.box([0, 1, 0, 0], [4, 5, 1, 3], [2, 2, 1, 2])
            finally:
                maker.done()
            kwargs = {"region": region}
            selected = data[::2, 1::2, :, ::2]
        else:
            one_plane = str(tmp_path / f"little-{api.__name__}.im")
            little = api.image()
            assert little.fromarray(one_plane, data[:, :, :, :1])
            little.done()
            kwargs = {"mask": f'"{one_plane}" > 100', "stretch": True}
            selected = data
        product = parent.pad(**kwargs)
        try:
            border = ((1, 1), (1, 1), (0, 0), (0, 0))
            mask = np.broadcast_to(data[:, :, :, :1] > 100, data.shape) if selection == "stretch" else np.ones_like(selected, dtype=bool)
            np.testing.assert_array_equal(product.getchunk(), np.pad(selected, border))
            np.testing.assert_array_equal(product.getchunk(getmask=True), np.pad(mask, border))
            if selection == "named":
                for channel in range(2):
                    output_beam = product.restoringbeam(channel=channel, polarization=0)
                    source_beam = parent.restoringbeam(channel=channel + 1, polarization=1)
                    assert output_beam == source_beam
        finally:
            product.done()
            parent.done()


def test_pad_return_and_rejected_edits_preserve_images(casa, tmp_path):
    source = np.arange(15, dtype=np.float32).reshape(3, 5)
    for api in (tools, casa):
        parent = api.image()
        infile = str(tmp_path / f"input-{api.__name__}.im")
        output = str(tmp_path / f"output-{api.__name__}.im")
        assert parent.fromarray(infile, source)
        detached = parent.pad(output, npixels=np.int32(2), wantreturn=False)
        assert detached.shape().size == 0
        detached.done()
        child = api.image()
        assert child.open(output)
        kept = child.getchunk()
        child.done()
        for options, error, message in (
            ({"npixels": 0}, RuntimeError, "npixels must be greater than zero"),
            ({"npixels": 1.5}, TypeError, None),
            ({"box": "0,0,2,2"}, RuntimeError, "box and regionPtr"),
            ({"outfile": output}, RuntimeError, "Replaceable flag is false.*already a file or directory"),
        ):
            with pytest.raises(error, match=message):
                parent.pad(**options)
        with pytest.raises(RuntimeError):
            parent.pad(output, overwrite=True, mask="missing_pad_mask_lattice")
        assert child.open(output)
        np.testing.assert_array_equal(child.getchunk(), kept)
        child.done()
        replaced = parent.pad(output, overwrite=True, padmask=True)
        np.testing.assert_array_equal(replaced.getchunk(), np.pad(source, ((1, 1), (1, 1))))
        assert replaced.getchunk(getmask=True).all()
        replaced.done()
        product = parent.pad(region=None, box="0,0,2,2", npixels=True)
        assert product.shape().tolist() == [5, 5]
        product.done()
        np.testing.assert_array_equal(parent.getchunk(), source)
        parent.done()


def test_pad_rejects_unsupported_pixel_or_coordinate_types(casa):
    data = np.arange(12, dtype=np.float32).reshape(3, 4)
    for api in (tools, casa):
        for pixels, linear, message in (
            (data.astype(np.complex64), False, "only supports Float"),
            (data, True, "required coordinate Direction"),
        ):
            parent = api.image()
            assert parent.fromarray(pixels=pixels, linear=linear)
            with pytest.raises(RuntimeError, match=message):
                parent.pad()
            np.testing.assert_array_equal(parent.getchunk(), pixels)
            parent.done()


def test_nan_fill_and_selectors_on_absent_axes_match_casa(casa):
    data = np.arange(12, dtype=np.float32).reshape(3, 4)
    for api in (tools, casa):
        parent = api.image()
        assert parent.fromarray(pixels=data)
        product = parent.pad(region="", chans="1", stokes="Q", value=float("nan"), padmask=True)
        try:
            expected = np.pad(data, ((1, 1), (1, 1)), constant_values=np.nan)
            np.testing.assert_array_equal(product.getchunk(), expected)
            assert product.getchunk(getmask=True).all()
            assert "value=nan" in product.history()[-3]
        finally:
            product.done()
            parent.done()


def test_analytic_temporary_padding_with_a_numpy_width():
    data = np.arange(12, dtype=np.float32).reshape(3, 4)
    source = tools.image()
    assert source.fromarray(pixels=data)
    product = source.pad(npixels=np.int64(3), value=np.float64(-7.))
    try:
        assert product.shape().tolist() == [9, 10]
        np.testing.assert_array_equal(product.getchunk(), np.pad(data, 3, constant_values=-7.))
        expected = np.pad(np.ones_like(data, dtype=bool), 3)
        np.testing.assert_array_equal(product.getchunk(getmask=True), expected)
        assert product.maskhandler("get") == ["T"]
        assert product.maskhandler("default") == [""]
    finally:
        product.done()
        source.done()


@pytest.mark.parametrize("keyword", ["padmask", "overwrite", "stretch", "wantreturn"])
def test_padding_flags_require_python_bool(casa, keyword):
    for api in (tools, casa):
        source = api.image()
        assert source.fromshape(shape=[3, 5])
        try:
            for raw in (np.bool_(True), np.bool_(False), 1, 0):
                with pytest.raises(TypeError):
                    source.pad(**{keyword: raw})
            for value in (True, False):
                product = source.pad(**{keyword: value})
                product.done()
        finally:
            source.done()


def test_pad_and_alias_rename_share_a_native_source(tmp_path):
    # Use a child process so a lock-order regression cannot hang the suite.
    script = """
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
from threading import Barrier

from casars import tools

scratch = Path(sys.argv[1])
path = scratch / "source.im"
parent = tools.image()
renamer = tools.image()
assert parent.fromshape(str(path), [96, 96])
assert renamer.open(str(path))
other_spelling = str(path / ".." / path.name)
barrier = Barrier(2)

def pad():
    for index in range(12):
        barrier.wait()
        result = parent.pad(str(scratch / f"output{index}.im"))
        assert result.shape().tolist() == [98, 98]
        result.done()

def rename():
    for _ in range(12):
        barrier.wait()
        assert renamer.rename(other_spelling, overwrite=True)

with ThreadPoolExecutor(max_workers=2) as pool:
    tasks = [pool.submit(pad), pool.submit(rename)]
    for task in tasks:
        task.result()
assert parent.shape().tolist() == [96, 96]
assert renamer.shape().tolist() == [96, 96]
renamer.done()
parent.done()
"""
    subprocess.run([sys.executable, "-c", script, str(tmp_path)], check=True, timeout=20)
