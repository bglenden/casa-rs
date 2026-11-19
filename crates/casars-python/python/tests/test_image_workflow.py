"""Public image creation, editing, persistence, and readback against CASA."""

from __future__ import annotations

import json
import sys
import threading
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


@pytest.mark.parametrize("writer", ["casars", "casa"])
@pytest.mark.parametrize("reader", ["casars", "casa"])
def test_native_image_interop(tmp_path, writer, reader):
    casa = pytest.importorskip("casatools") if "casa" in (reader, writer) else None
    if casa is not None:
        assert casa.version_string().replace("-", ".") == "6.7.6.14"
    backends = {"casars": tools, "casa": casa}
    path = str(tmp_path / "interop.im")
    pixels = np.array([[0.25, 1.0, 2.0], [-1.0, 4.5, 7.0]], dtype=np.float32)
    output = backends[writer].image()
    assert output.fromarray(path, pixels)
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
