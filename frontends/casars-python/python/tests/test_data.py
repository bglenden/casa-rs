from __future__ import annotations

import os
from pathlib import Path
from shutil import copytree
import subprocess
import sys
import tarfile
import threading
import time

import numpy as np
import pytest

from casars import data
from casars._native_image import NativeImageView
from casars.data import protocol_info, schema_bundle
from casars.tools import image as image_tool, table as table_tool


def test_top_level_lazy_data_surface_imports() -> None:
    import casars

    assert not hasattr(casars, "Image")
    assert not hasattr(casars, "Table")
    assert casars.data is not None
    assert not hasattr(casars.data, "Image")
    assert not hasattr(casars.data, "Table")
    assert not hasattr(casars._core, "Image")


def test_data_image_reads_a_native_width_slice_through_the_tool(tmp_path: Path) -> None:
    path = tmp_path / "pixels.im"
    tool = image_tool()
    assert tool.fromshape(str(path), [3, 4])
    assert tool.done()

    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    writer = NativeImageView.open(path, writable=True)
    writer.put_slice(pixels, (0, 0))
    image = NativeImageView.open(path)
    assert isinstance(image._inner, image_tool)
    np.testing.assert_array_equal(image.get_slice((1, 1), (2, 3)), pixels[1:, 1:], strict=True)


@pytest.mark.parametrize("kind,dtype", [("f", "float32"), ("d", "float64"), ("c", "complex64"), ("cd", "complex128")])
def test_compatibility_image_keeps_native_pixel_precision(tmp_path: Path, kind: str, dtype: str) -> None:
    path = tmp_path / "typed.im"
    tool = image_tool()
    assert tool.fromshape(str(path), [3, 4], type=kind)
    assert tool.done()
    readonly = NativeImageView.open(path)
    writable = NativeImageView.open(path, writable=True)
    pixels = np.arange(12).reshape(3, 4).astype(dtype)
    with pytest.raises(ValueError, match="opened read-only"):
        readonly.put_slice(pixels, (0, 0))
    writable.put_slice(pixels, (0, 0))
    assert readonly.pixel_type == dtype
    assert readonly.get_slice((0, 0), (2, 2), stride=(2, 2)).dtype == np.dtype(dtype)
    np.testing.assert_array_equal(readonly.get_slice((0, 0), (2, 2), stride=(2, 2)), pixels[::2, ::2])
    np.testing.assert_array_equal(readonly.get_plane(1, 1), pixels[:, 1])
    wrong = pixels.astype(np.bool_)
    with pytest.raises(TypeError):
        writable.put_slice(wrong, (0, 0))
    del readonly, writable
    assert tool.open(str(path))
    np.testing.assert_array_equal(tool.getchunk(), pixels)
    assert tool.done()


def test_open_data_writer_sees_units_committed_after_reader_opened(tmp_path: Path) -> None:
    path = tmp_path / "source.im"
    tool = image_tool()
    assert tool.fromshape(str(path), [2, 3])
    assert tool.done()
    reader = NativeImageView.open(path)
    assert reader.units == ""
    table = table_tool()
    assert table.open(str(path), nomodify=False)
    table.putkeyword("units", "Jy/beam")
    assert table.done()
    writer = NativeImageView.open(path, writable=True)
    assert writer.units == "Jy/beam"
    writer.put_slice(np.ones((1, 1), dtype=np.float32), (0, 0))
    del writer, reader
    reloaded = NativeImageView.open(path)
    assert reloaded.units == "Jy/beam"
    assert reloaded.get_slice((0, 0), (1, 1)).item() == 1.0


@pytest.mark.skipif(os.name != "posix", reason="Directory symlinks require POSIX permissions")
@pytest.mark.parametrize("alias_reader", ["data", "tool"])
def test_data_writer_uses_resolved_image_even_after_alias_retarget(tmp_path: Path, alias_reader: str) -> None:
    source, separate, alias = [tmp_path / f"{name}.im" for name in ("source", "other", "alias")]
    tool = image_tool()
    assert tool.fromshape(str(source), [2, 3])
    assert tool.done()
    assert tool.fromshape(str(separate), [2, 3])
    assert tool.setbrightnessunit("Jy/beam")
    assert tool.done()
    alias.symlink_to(source, target_is_directory=True)
    if alias_reader == "data":
        reader = NativeImageView.open(alias)
        assert reader.shape == (2, 3)
    else:
        assert tool.open(str(alias))
    alias.unlink()
    alias.symlink_to(separate, target_is_directory=True)
    writer = NativeImageView.open(source, writable=True)
    writer.put_slice(np.ones((1, 1), dtype=np.float32), (0, 0))
    del writer
    assert tool.done()
    assert NativeImageView.open(source).get_slice((0, 0), (1, 1)).item() == 1.0
    other = NativeImageView.open(separate)
    assert other.get_slice((0, 0), (1, 1)).item() == 0.0
    assert other.units == "Jy/beam"


@pytest.mark.skipif(os.name != "posix", reason="The fixture uses POSIX directory permissions")
def test_readonly_image_opens_without_a_table_lock_file(tmp_path: Path) -> None:
    path = tmp_path / "readonly.im"
    tool = image_tool()
    assert tool.fromshape(str(path), [2, 3])
    assert tool.done()
    lock = path / "table.lock"
    lock.unlink()
    path.chmod(0o555)
    try:
        can_create_lock = os.access(path, os.W_OK)
        reader = NativeImageView.open(path)
        np.testing.assert_array_equal(reader.get_slice((0, 0), (2, 3)), np.zeros((2, 3)))
        if not can_create_lock:
            assert not lock.exists()
    finally:
        path.chmod(0o755)


@pytest.mark.skipif(os.name != "posix", reason="This tests replacing a still-open directory")
def test_data_write_refuses_a_replaced_directory(tmp_path: Path) -> None:
    path, moved = tmp_path / "image.im", tmp_path / "original.im"
    tool = image_tool()
    assert tool.fromshape(str(path), [2, 3])
    assert tool.done()
    writer = NativeImageView.open(path, writable=True)
    path.rename(moved)
    assert tool.fromshape(str(path), [3, 4])
    assert tool.done()
    with pytest.raises(RuntimeError, match="image at .* has changed"):
        writer.put_slice(np.ones((1, 1), dtype=np.float32), (0, 0))
    assert NativeImageView.open(path).get_slice((0, 0), (1, 1)).item() == 0.0
    assert NativeImageView.open(moved).get_slice((0, 0), (1, 1)).item() == 0.0


def test_data_schema_bundle_reports_object_surface() -> None:
    info = protocol_info()
    assert info.protocol_name == "casars_data_objects"
    assert info.protocol_version == 1
    assert info.surface_kind == "object"

    bundle = schema_bundle()
    assert bundle["protocol"]["protocol_name"] == "casars_data_objects"
    assert bundle["protocol"]["protocol_version"] == 1
    assert bundle["protocol"]["surface_kind"] == "object"
    assert bundle["projections"]["python"]["module"] == "casars.data"
    assert "LogicalInputValue" in bundle["components"]
    assert "LogicalOutputValue" in bundle["components"]
    assert "LogicalComplex128" in bundle["components"]

    objects = {entry["name"]: entry for entry in bundle["semantic"]["objects"]}
    # The CASA tools of casars.tools share the bundle (test_tools_*.py).
    assert set(objects) == {
        "simulator", "measures", "quanta", "componentlist",
        "image", "coordsys", "table", "msmetadata", "ms", "calibrater", "imager",
        "agentflagger", "wvr", "singledishms", "calanalysis", "atmosphere",
    }
    assert "Image" not in bundle["annotations"]["python"]["objects"]
    assert all(obj["name"] != "Image" for obj in bundle["projections"]["python"]["objects"])


def test_measurement_set_plot_releases_the_gil(tmp_path: Path) -> None:
    with tarfile.open(
        REPO_ROOT / "libs/casacore/casa-ms/tests/fixtures/mssel_test_small.ms.tgz", "r:gz"
    ) as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    measurement_set = tmp_path / "mssel_test_small.ms"
    data.measurement_set_plot(measurement_set, preset="uv_coverage")

    ticks: list[float] = []
    done = threading.Event()

    def tick() -> None:
        while not done.is_set():
            ticks.append(time.perf_counter())

    switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-4)
    try:
        ticker = threading.Thread(target=tick)
        ticker.start()
        windows = []
        for _ in range(5):
            started = time.perf_counter()
            data.measurement_set_plot(measurement_set, preset="uv_coverage")
            windows.append((started, time.perf_counter()))
        done.set()
        ticker.join()
    finally:
        sys.setswitchinterval(switch_interval)

    # Holding the GIL for the whole call, the ticker could run only at its
    # edges; released, it ticks through the middle of the call.
    inside = [
        sum(1 for moment in ticks if start + (end - start) / 4 < moment < end - (end - start) / 4)
        for start, end in windows
    ]
    assert sum(inside) > 0, inside


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_image_reads_and_writes(tmp_path: Path) -> None:
    fixture_root = _create_fixtures(tmp_path)
    image = NativeImageView.open(fixture_root / "python_fixture.image", writable=True)

    assert image.shape == (4, 3, 2)
    assert image.pixel_type == "float32"
    assert image.units == "Jy/beam"
    assert image.mask_names == ["quality"]
    assert image.default_mask_name == "quality"
    assert image.image_info["objectname"] == "python-fixture"
    assert image.misc_info["purpose"] == "python-tests"

    chunk = image.get_slice((1, 1, 0), (2, 2, 2))
    assert isinstance(chunk, np.ndarray)
    assert chunk.shape == (2, 2, 2)
    assert chunk.dtype == np.float32
    np.testing.assert_array_equal(chunk[:, :, 0], np.array([[8.0, 10.0], [14.0, 16.0]], dtype=np.float32))

    plane = image.get_plane(2, 1)
    assert plane.shape == (4, 3)
    np.testing.assert_array_equal(
        plane,
        np.array(
            [
                [1.0, 3.0, 5.0],
                [7.0, 9.0, 11.0],
                [13.0, 15.0, 17.0],
                [19.0, 21.0, 23.0],
            ],
            dtype=np.float32,
        ),
    )

    mask = image.get_mask_slice((0, 0, 0), (2, 2, 2))
    assert mask is not None
    assert mask.dtype == np.bool_
    np.testing.assert_array_equal(mask[:, :, 0], np.array([[True, True], [True, True]]))

    replacement = np.full((2, 2, 1), 99.0, dtype=np.float32)
    image.put_slice(replacement, (0, 0, 0))
    reread = image.get_slice((0, 0, 0), (2, 2, 1))
    np.testing.assert_array_equal(reread, replacement)


def test_linearmosaic_writes_flux_and_coverage(tmp_path: Path) -> None:
    fixture_root = _create_fixtures(tmp_path)
    pointing_path = fixture_root / "direction_fixture.image"
    beam_path = tmp_path / "pointing.pb"
    copytree(pointing_path, beam_path)
    pointing = NativeImageView.open(pointing_path)
    beam = NativeImageView.open(beam_path, writable=True)
    start = (0,) * len(pointing.shape)
    beam.put_slice(np.full(pointing.shape, 0.5, dtype=np.float32), start)
    output = tmp_path / "mosaic.image"
    weight = tmp_path / "mosaic.weight"

    data.linearmosaic([(pointing_path, beam_path)], template=pointing_path,
                      output=output, output_weight=weight)

    np.testing.assert_array_equal(NativeImageView.open(output).get_slice(start, pointing.shape),
                                  2.0 * pointing.get_slice(start, pointing.shape))
    np.testing.assert_array_equal(NativeImageView.open(weight).get_slice(start, pointing.shape),
                                  np.full(pointing.shape, 0.25, dtype=np.float32))


def test_table_reads_and_writes(tmp_path: Path) -> None:
    fixture_root = _create_fixtures(tmp_path)
    table = table_tool()
    assert table.open(str(fixture_root / "python_fixture.table"), nomodify=False)

    assert table.nrows() == 3
    assert table.colnames() == ["id", "label", "gain", "spectrum", "vary", "meta"]
    assert table.getkeyword("observer") == "python-fixture"
    assert table.getkeyword("version") == 1
    assert table.getcolkeywords("spectrum") == {"unit": "Jy", "frame": "LSRK"}

    assert table.getcell("label", 0) == "alpha"
    gain = table.getcell("gain", 1)
    assert gain == complex(2.0, -2.0)
    assert table.getcell("meta", 2) == {"label": "gamma", "weight": 30}

    ids = table.getcol("id")
    assert isinstance(ids, np.ndarray)
    np.testing.assert_array_equal(ids, np.array([1, 2, 3], dtype=np.int64))

    gains = table.getcol("gain")
    assert isinstance(gains, np.ndarray)
    np.testing.assert_array_equal(
        gains,
        np.array([1.0 - 1.0j, 2.0 - 2.0j, 3.0 - 3.0j], dtype=np.complex128),
    )

    spectra = table.getcol("spectrum")
    assert isinstance(spectra, np.ndarray)
    assert spectra.shape == (2, 2, 3)
    np.testing.assert_array_equal(
        spectra[:, :, 0],
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
    )

    varying = table.getvarcol("vary")
    assert [np.squeeze(item, axis=-1).shape for item in varying.values()] == [(2,), (3,), (1,)]

    assert table.putcol("label", ["delta", "epsilon", "zeta"])
    assert np.asarray(table.getcol("label")).tolist() == ["delta", "epsilon", "zeta"]

    assert table.putcell("meta", 0, {"label": "delta", "weight": 100})
    assert table.getcell("meta", 0) == {"label": "delta", "weight": 100}

    table.putcolkeywords("gain", {"unit": "arb", "stage": "python"})
    assert table.getcolkeywords("gain") == {"unit": "arb", "stage": "python"}

    # A 2-D keyword array reads back, from disk, with element (i, j) where it
    # was, whatever numpy's memory order (#952).
    matrix = np.arange(6, dtype=np.float64).reshape(2, 3)
    table.putcolkeywords(
        "spectrum",
        {"c_order": matrix, "f_order": np.asfortranarray(matrix), "strided": matrix[:, ::2]},
    )
    assert table.close()
    assert table.open(str(fixture_root / "python_fixture.table"))
    stored = table.getcolkeywords("spectrum")
    np.testing.assert_array_equal(stored["c_order"], matrix)
    np.testing.assert_array_equal(stored["f_order"], matrix)
    np.testing.assert_array_equal(stored["strided"], matrix[:, ::2])


def _create_fixtures(tmp_path: Path) -> Path:
    fixture_root = tmp_path / "fixtures"
    subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "casars-python",
            "--example",
            "make_python_fixtures",
            "--",
            str(fixture_root),
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    return fixture_root
