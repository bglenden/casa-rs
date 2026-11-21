"""Public image creation, editing, persistence, and readback against CASA."""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def workflow():
    path = Path(__file__).parent / "data/casa_image_workflow.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["casa_version"].replace("-", ".") == "6.7.6.14"
    return data["scripts"]["native_workflow"]


def _decode(value, path, bindings):
    if isinstance(value, dict):
        if set(value) == {"ref"}:
            return bindings[value["ref"]]
        if "ndarray" in value:
            return np.array(value["ndarray"], dtype=value["dtype_descriptor"]).reshape(
                value["shape"]
            )
        return {key: _decode(item, path, bindings) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item, path, bindings) for item in value]
    if isinstance(value, str):
        return value.replace("{tmp}", str(path))
    return value


def _assert_value(actual, expected, key="result"):
    if isinstance(expected, np.ndarray):
        assert isinstance(actual, np.ndarray), key
        assert actual.dtype == expected.dtype, key
        assert actual.shape == expected.shape, key
        if expected.dtype.kind in "fc":
            np.testing.assert_allclose(actual, expected, rtol=5e-10, atol=1e-12, err_msg=key)
        else:
            np.testing.assert_array_equal(actual, expected, err_msg=key)
    elif isinstance(expected, dict):
        assert isinstance(actual, dict), key
        assert actual.keys() == expected.keys(), (key, actual.keys(), expected.keys())
        for field in expected:
            _assert_value(actual[field], expected[field], f"{key}.{field}")
    elif isinstance(expected, float):
        assert type(actual) is float, key
        assert actual == pytest.approx(expected, rel=5e-10, abs=1e-12), key
    else:
        assert type(actual) is type(expected), key
        assert actual == expected, key


def test_casa_recorded_native_workflow(tmp_path, workflow):
    bindings = {"ia": tools.image()}
    for call in workflow:
        method = getattr(bindings[call["target"]], call["method"])
        args = _decode(call["args"], tmp_path, bindings)
        if "raises" in call:
            kind, message = _decode(call["raises"], tmp_path, bindings)
            assert kind == "RuntimeError"
            with pytest.raises(RuntimeError) as failure:
                method(**args)
            assert str(failure.value).splitlines()[0] == message
            continue
        result = method(**args)
        if "as" in call:
            bindings[call["as"]] = result
        expected = _decode(call["returns"], tmp_path, bindings)
        if isinstance(expected, dict) and "tool" in expected:
            assert type(result) is getattr(tools, expected["tool"])
        else:
            _assert_value(result, expected, f"{call['target']}.{call['method']}")


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("kind", ["directory", "generic-table"])
def test_image_factories_refuse_nonimage_directories_without_creating_a_lock(tmp_path, backend, kind):
    if backend == "casa":
        api = pytest.importorskip("casatools")
        assert api.version_string().replace("-", ".") == "6.7.6.14"
    else:
        api = tools
    path = tmp_path / "nonimage"
    if kind == "directory":
        path.mkdir()
        (path / "unrelated").write_text("keep", encoding="utf-8")
    else:
        table = tools.table()
        try:
            assert table.create(str(path), {"ID": {"valueType": "int"}}, nrow=2)
            assert table.putcol("ID", [4, 7])
        finally:
            table.done()
    before = {child.name: child.read_bytes() for child in path.iterdir()}
    for method in ("open", "newimage", "newimagefromfile", "fromimage", "newimagefromimage"):
        image = api.image()
        try:
            assert image.fromarray(pixels=[4., 7., 11.])
            with pytest.raises(RuntimeError, match="Unable to open image"):
                getattr(image, method)(infile=str(path))
            assert image.isopen() == (method != "open")
            if image.isopen():
                np.testing.assert_array_equal(image.getchunk(), [4., 7., 11.])
        finally:
            image.done()
        assert {child.name: child.read_bytes() for child in path.iterdir()} == before


@pytest.mark.parametrize("writer", ["casars", "casa"])
@pytest.mark.parametrize("reader", ["casars", "casa"])
@pytest.mark.parametrize("linear", [False, True])
def test_native_image_interop(tmp_path, writer, reader, linear):
    casa = pytest.importorskip("casatools") if "casa" in (reader, writer) else None
    if casa is not None:
        assert casa.version_string().replace("-", ".") == "6.7.6.14"
    backends = {"casars": tools, "casa": casa}
    path = str(tmp_path / "interop.im")
    pixels = np.array([[0.25, 1.0, 2.0], [-1.0, 4.5, 7.0]], dtype=np.float32)
    output = backends[writer].image()
    assert output.fromarray(path, pixels, linear=linear)
    coord = output.coordsys()
    assert coord.setreferencepixel([0.75, 1.25])
    assert output.setcoordsys(coord.torecord())
    assert output.setbrightnessunit("Jy/pixel")
    assert output.putchunk(np.array([[5.75]], dtype=np.float32), [1, 2])
    expected_record = output.coordsys().torecord()
    assert output.done()

    opened = backends[reader].image()
    assert opened.open(path)
    pixels[1, 2] = 5.75
    _assert_value(opened.getchunk(), pixels.astype(np.float64))
    _assert_value(opened.getchunk(getmask=True), np.ones(pixels.shape, dtype=np.bool_))
    assert opened.brightnessunit() == "Jy/pixel"
    _assert_value(opened.coordsys().referencepixel()["numeric"], np.array([0.75, 1.25]))
    _assert_value(opened.coordsys().torecord(), expected_record)
    assert opened.done()


@pytest.mark.parametrize(
    "shape_type,array_type,input_dtype,pixel_type,result_dtype",
    [
        ("f", "f", np.float32, "float", np.float64),
        ("d", "d", np.float64, "double", np.float64),
        ("c", "f", np.complex64, "complex", np.complex128),
        ("cd", "d", np.complex128, "dcomplex", np.complex128),
    ],
)
def test_temporary_image_pixel_kinds(shape_type, array_type, input_dtype, pixel_type, result_dtype):
    ia = tools.image()
    pixels = np.arange(12, dtype=input_dtype).reshape(3, 4)
    if np.iscomplexobj(pixels):
        pixels += 1j * (pixels[::-1] + 1)
    assert ia.fromshape(shape=[3, 4], type=shape_type)
    assert ia.putchunk(pixels)
    _assert_value(ia.getchunk(), pixels.astype(result_dtype))
    assert ia.fromarray(pixels=pixels, type=array_type)
    assert ia.isopen()
    assert ia.pixeltype() == pixel_type
    assert not ia.ispersistent()
    _assert_value(ia.getchunk(), pixels.astype(result_dtype))
    assert ia.done()
    assert not ia.isopen()


def test_failed_creation_and_pixel_edit_preserve_existing_image(tmp_path):
    ia = tools.image()
    path = str(tmp_path / "kept.im")
    pixels = np.arange(12, dtype=np.float64).reshape(3, 4)
    assert ia.fromarray(path, pixels)
    with pytest.raises(RuntimeError, match="extends beyond"):
        ia.putchunk(np.ones((2, 2)), blc=[2, 3])
    _assert_value(ia.getchunk(), pixels)
    with pytest.raises(RuntimeError, match="positive"):
        ia.fromshape(path, [0, 4], overwrite=True)
    assert not ia.isopen()
    assert ia.open(path)
    _assert_value(ia.getchunk(), pixels)
    assert ia.done(remove=True)
    assert not Path(path).exists()


def test_getchunk_releases_interpreter_while_reading_pixels(tmp_path):
    ia = tools.image()
    assert ia.fromshape(str(tmp_path / "gil.im"), [1536, 1536])
    ready = threading.Event()
    read_start = threading.Event()
    progressed = threading.Event()

    def worker():
        ready.set()
        read_start.wait()
        progressed.set()

    other = threading.Thread(target=worker)
    other.start()
    assert ready.wait(timeout=5)
    interval = sys.getswitchinterval()
    try:
        # Force the other thread to rely on the image method detaching.
        sys.setswitchinterval(60.0)
        read_start.set()
        assert ia.getchunk().shape == (1536, 1536)
        assert progressed.is_set()
    finally:
        sys.setswitchinterval(interval)
        read_start.set()
        other.join(timeout=5)
        ia.done()


def test_explicit_coordinates_and_existing_non_table_directory(tmp_path):
    image = tools.image()
    assert image.fromshape(shape=[3, 4], linear=True)
    coord = image.coordsys()
    assert coord.setreferencepixel([2.25, 1], type="linear")
    directory = tmp_path / "unrelated"
    directory.mkdir()
    preserve = directory / "preserve"
    preserve.write_text("important", encoding="utf-8")
    with pytest.raises(RuntimeError, match="not a true table directory"):
        image.fromshape(str(directory), [3, 4], overwrite=True)
    assert preserve.read_text(encoding="utf-8") == "important"
    assert not image.isopen()

    path = str(tmp_path / "explicit.im")
    record = coord.torecord()
    assert image.fromshape(path, [3, 4], {"coordinates": record})
    assert image.done()
    assert image.open(path, False)
    _assert_value(image.coordsys().torecord(), record)
    assert image.done()


def test_brightness_unit_spelling_and_history():
    ia = tools.image()
    assert ia.fromshape(shape=[2, 3])
    assert ia.setbrightnessunit(" Jy beam-1 ")
    assert ia.brightnessunit() == "Jy.beam-1"
    assert ia.history(False)[-1] == 'ia.setbrightnessunit(unit=" Jy beam-1 ")'


@pytest.mark.parametrize("backend", ["casars", "casa"])
def test_open_handle_protects_native_image_from_replacement(tmp_path, backend):
    module = tools if backend == "casars" else pytest.importorskip("casatools")
    path = str(tmp_path / "attached.im")
    owner = module.image()
    second = module.image()
    assert owner.fromshape(path, [2, 3])
    with pytest.raises(RuntimeError, match="already opened"):
        second.fromshape(path, [4, 5], overwrite=True)
    assert not second.isopen()
    assert second.open(path)
    with pytest.raises(RuntimeError, match="still open in this process"):
        second.done(remove=True)
    assert not second.isopen()
    assert Path(path).exists()
    _assert_value(owner.getchunk(), np.zeros((2, 3)))
    assert owner.done()
    assert second.fromshape(path, [4, 5], overwrite=True)
    assert second.done(remove=True)
    assert not Path(path).exists()



@pytest.mark.parametrize("backend", ["casars", "casa"])
def test_image_handles_share_metadata_and_pixel_edits(tmp_path, backend):
    module = tools if backend == "casars" else pytest.importorskip("casatools")
    path = str(tmp_path / "shared.im")
    first = module.image()
    second = module.image()
    assert first.fromshape(path, [2, 2])
    assert second.open(path)
    assert first.setbrightnessunit("Jy")
    assert first.putchunk(np.array([[10.0]]), [0, 0])
    assert second.putchunk(np.array([[20.0]]), [1, 1])
    assert second.done()
    assert first.done()
    assert second.open(path)
    assert second.brightnessunit() == "Jy"
    _assert_value(second.getchunk(), np.array([[10.0, 0.0], [0.0, 20.0]]))
    assert second.done()


def test_replacement_keeps_the_image_path_visible(tmp_path):
    ia = tools.image()
    path = tmp_path / "published.im"
    assert ia.fromshape(str(path), [2, 3])
    assert ia.done()
    ready = threading.Event()
    finished = threading.Event()
    observed = []

    def reader():
        while not finished.is_set():
            observed.append(path.is_dir())
            ready.set()

    watcher = threading.Thread(target=reader)
    watcher.start()
    try:
        assert ready.wait(timeout=5)
        for _ in range(16):
            assert ia.fromshape(str(path), [3, 2], overwrite=True)
            assert ia.done()
    finally:
        finished.set()
        watcher.join(timeout=5)
    assert observed and all(observed)


@pytest.mark.parametrize("backend", ["casars", "casa"])
def test_boolean_array_cannot_replace_or_edit_native_pixels(tmp_path, backend):
    module = tools if backend == "casars" else pytest.importorskip("casatools")
    ia = module.image()
    path = str(tmp_path / "numeric.im")
    data = np.array([1.5, -2.0], dtype=np.float64)
    assert ia.fromarray(path, data)
    boolean = np.array([True, False])
    with pytest.raises(RuntimeError, match="Unsupported type for pixels"):
        ia.putchunk(boolean)
    _assert_value(ia.getchunk(), data)
    with pytest.raises(RuntimeError, match="pixels is not understood"):
        ia.fromarray(path, boolean, overwrite=True)
    assert ia.open(path)
    _assert_value(ia.getchunk(), data)
    assert ia.done()


def test_putchunk_progresses_during_ndarray_input_capture():
    ia = tools.image()
    assert ia.fromshape(shape=[2, 2])
    pixels = np.ones((2048, 2048), dtype=np.float64)
    # The array cannot fit, so the binding performs no disk or pixel writes.
    # Observe when another Python thread first runs relative to the rejection.
    ratios = []
    for _ in range(3):
        ready = threading.Event()
        start = threading.Event()
        progressed = []

        def worker():
            ready.set()
            start.wait()
            progressed.append(time.perf_counter())

        other = threading.Thread(target=worker)
        other.start()
        interval = sys.getswitchinterval()
        try:
            assert ready.wait(timeout=5)
            sys.setswitchinterval(60.0)
            before = time.perf_counter()
            start.set()
            with pytest.raises(RuntimeError, match="extends beyond"):
                ia.putchunk(pixels)
            after = time.perf_counter()
            if progressed:
                ratios.append((progressed[0] - before) / (after - before))
        finally:
            sys.setswitchinterval(interval)
            start.set()
            other.join(timeout=5)
    assert ratios and min(ratios) < 0.5


@pytest.mark.parametrize("kind,dtype,pixel_type", [
    ("d", np.float64, "double"),
    ("f", np.complex64, "complex"),
    ("d", np.complex128, "dcomplex"),
])
@pytest.mark.parametrize("writer", ["casars", "casa"])
def test_native_pixel_types_cross_reader(tmp_path, kind, dtype, pixel_type, writer):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    modules = {"casa": casa, "casars": tools}
    reader = "casa" if writer == "casars" else "casars"
    path = str(tmp_path / "typed.im")
    pixels = np.array([[0.25, 1.0], [-1.0, 4.5]], dtype=dtype)
    if np.iscomplexobj(pixels):
        pixels += 1j * pixels[::-1]
    image = modules[writer].image()
    assert image.fromarray(path, pixels, type=kind)
    assert image.done()
    opened = modules[reader].image()
    assert opened.open(path)
    assert opened.pixeltype() == pixel_type
    expected = pixels.astype(np.complex128 if np.iscomplexobj(pixels) else np.float64)
    _assert_value(opened.getchunk(), expected)
    assert opened.done()
