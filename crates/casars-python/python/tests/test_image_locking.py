"""Image lock state and casacore lock-file interoperability."""

import os
import subprocess
import sys
import time

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


def assert_locks(tool, read, write):
    locks = tool.haslock()
    assert isinstance(locks, np.ndarray) and locks.dtype == np.bool_
    np.testing.assert_array_equal(locks, [read, write])


@pytest.mark.parametrize("kind", ["f", "c"])
def test_public_image_lock_lifecycle(casa, tmp_path, kind):
    for module in (casa, tools):
        disk = str(tmp_path / f"{module.__name__}.im")
        dst = str(tmp_path / f"{module.__name__}-renamed.im")
        ia = module.image()
        assert ia.haslock().shape == (0,) and ia.haslock().dtype == np.bool_
        assert ia.lock() is False and ia.unlock() is False
        with pytest.raises(TypeError):
            ia.lock(writelock=np.bool_(True))
        with pytest.raises(TypeError):
            ia.lock(nattempts=1.0)
        assert ia.fromshape("", [2, 2], type=kind)
        assert_locks(ia, True, True)
        assert ia.unlock() and ia.lock(nattempts=-1)
        assert_locks(ia, True, True)
        assert ia.fromshape(disk, [2, 2], type=kind)
        assert_locks(ia, True, True)
        assert ia.done()
        assert ia.open(disk)
        assert_locks(ia, True, False)
        assert ia.unlock()
        assert ia.unlock()
        assert_locks(ia, False, False)
        assert ia.shape().tolist() == [2, 2]
        assert ia.miscinfo() == {}
        assert ia.pixelvalue([20, 20]) == {}
        assert_locks(ia, False, False)
        ia.getchunk()
        assert_locks(ia, True, False)
        assert ia.lock(writelock=True, nattempts=np.int64(1))
        assert ia.lock(writelock=False, nattempts=1)
        assert_locks(ia, True, True)
        assert ia.putchunk(np.eye(2), locking=True)
        assert_locks(ia, False, False)
        assert ia.putchunk(np.eye(2), locking=False)
        assert_locks(ia, True, True)
        assert ia.unlock()
        assert ia.sethistory(history=["recording a lock test"])
        assert_locks(ia, False, False)
        assert ia.setbrightnessunit("Jy")
        assert_locks(ia, True, True)
        assert ia.rename(dst)
        assert_locks(ia, True, False)
        assert ia.done()
        assert ia.haslock().size == 0


@pytest.mark.parametrize("kind", ["d", "cd"])
def test_explicit_lock_methods_refuse_double_images(casa, kind):
    for module in (casa, tools):
        ia = module.image()
        assert ia.fromshape("", [2, 2], type=kind)
        for method in ("haslock", "lock", "unlock"):
            with pytest.raises(RuntimeError, match="does not support"):
                getattr(ia, method)()
        assert ia.done()


@pytest.mark.parametrize("method", ["remove", "rename"])
def test_lifecycle_operation_preserves_external_replacement(tmp_path, method):
    path = tmp_path / "attached.im"
    other = tmp_path / "replacement.im"
    backup = tmp_path / "original.im"
    original, replacement = tools.image(), tools.image()
    assert original.fromshape(str(path), [2, 2])
    assert original.setmiscinfo({"identity": "original"})
    assert replacement.fromshape(str(other), [2, 2])
    assert replacement.setmiscinfo({"identity": "replacement"})
    assert replacement.done()
    assert_locks(original, True, True)
    path.rename(backup)
    other.rename(path)
    arguments = (str(tmp_path / "unexpected.im"),) if method == "rename" else ()
    with pytest.raises(RuntimeError, match="has changed"):
        getattr(original, method)(*arguments)
    assert replacement.open(str(path))
    assert replacement.miscinfo() == {"identity": "replacement"}
    assert replacement.done()


def test_locked_complex_native_image_keeps_the_casa_tiled_layout(casa, tmp_path):
    path = str(tmp_path / "tiled-complex.im")
    creator = casa.image()
    assert creator.fromshape(path, [200, 90, 4, 6], type="c")
    assert creator.done()

    def tile_shape():
        table = casa.table()
        assert table.open(path)
        result = table.getdminfo()["*1"]["SPEC"]["HYPERCUBES"]["*1"]["TileShape"].tolist()
        assert table.done()
        return result

    expected = tile_shape()
    assert expected == [100, 45, 2, 3]
    tool = tools.image()
    assert tool.open(path)
    assert tool.lock(writelock=True, nattempts=1)
    assert tool.unlock()
    assert tool.done()
    assert tile_shape() == expected


def test_image_observes_a_same_process_table_writer(casa, tmp_path):
    pixels = np.full((2, 2), 7, dtype=np.float32)
    for module in (tools, casa):
        path = str(tmp_path / f"{module.__name__}-shared.im")
        image = module.image()
        assert image.fromshape(path, [2, 2])
        assert image.done() and image.open(path)
        np.testing.assert_array_equal(image.getchunk(), np.zeros((2, 2)))
        table = module.table()
        assert table.open(path, nomodify=False, lockoptions={"option": "auto"})
        assert table.lock(True, 1)
        assert table.putcell("map", 0, pixels)
        assert table.flush()
        assert table.unlock() and table.done()
        assert image.lock(nattempts=1)
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert image.done()


def test_opening_a_shared_image_reacquires_its_read_lock(casa, tmp_path):
    for module in (tools, casa):
        path = str(tmp_path / f"{module.__name__}-reopen.im")
        first, second = module.image(), module.image()
        assert first.fromshape(path, [2, 2]) and first.unlock()
        assert_locks(first, False, False)
        assert second.open(path)
        assert_locks(first, True, False)
        assert_locks(second, True, False)
        assert second.done() and first.done()


def test_noop_write_lock_preserves_native_generation(casa, tmp_path):
    path = tmp_path / "unchanged.im"
    creator = casa.image()
    assert creator.fromshape(str(path), [2, 2]) and creator.done()
    for module in (casa, tools):
        tool = module.image()
        assert tool.open(str(path))
        table_date = (path / "table.dat").stat().st_mtime_ns
        lock_contents = (path / "table.lock").read_bytes()
        assert tool.lock(writelock=True, nattempts=1)
        assert tool.unlock()
        assert (path / "table.dat").stat().st_mtime_ns == table_date
        assert (path / "table.lock").read_bytes() == lock_contents
        assert tool.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
@pytest.mark.parametrize("operation", ["open", "imageconcat"])
@pytest.mark.parametrize("backend", ["casars.tools", "casatools"])
def test_native_image_read_waits_for_external_writer(casa, tmp_path, backend, operation):
    path = tmp_path / "wait-open.im"
    ready = tmp_path / "opening"
    owner = tools.image()
    assert owner.fromshape(str(path), [2, 2], linear=True)
    child = subprocess.Popen(
        [sys.executable, "-c", """
import sys
from importlib import import_module
from pathlib import Path
image = import_module(sys.argv[3]).image()
Path(sys.argv[2]).touch()
if sys.argv[4] == "open":
    assert image.open(sys.argv[1])
    assert image.haslock().tolist() == [True, False]
else:
    product = image.imageconcat(infiles=[sys.argv[1], sys.argv[1]], axis=0, relax=True)
    assert product.shape().tolist() == [4, 2]
    assert product.done()
assert image.done()
""", str(path), str(ready), backend, operation],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.03)
        assert ready.exists()
        time.sleep(0.2)
        assert child.poll() is None, child.stderr.read() if child.poll() is not None else ""
        assert owner.unlock()
        assert child.wait(timeout=10) == 0, child.stderr.read()
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)
        owner.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
@pytest.mark.parametrize("operation", ["subimage", "replace", "modify"])
@pytest.mark.parametrize("backend", ["casars.tools", "casatools"])
def test_native_expression_waits_for_its_source_writer(casa, tmp_path, backend, operation):
    owner, target = tools.image(), tools.image()
    assert owner.fromshape(str(tmp_path / "source.im"), [4, 4])
    assert target.fromshape(str(tmp_path / "target.im"), [4, 4])
    if operation == "replace":
        assert target.calcmask("F")
    assert target.done()
    ready = tmp_path / "ready"
    child = subprocess.Popen(
        [sys.executable, "-c", """
import sys
from importlib import import_module
from pathlib import Path
backend = import_module(sys.argv[1])
tool = backend.image()
assert tool.open("target.im")
Path("ready").touch()
if sys.argv[2] == "subimage":
    result = tool.subimage(mask='"source.im">1')
    assert result.getchunk(getmask=True).all()
    assert result.done()
elif sys.argv[2] == "replace":
    assert tool.replacemaskedpixels('"source.im"', update=True)
    assert (tool.getchunk() == 2).all()
else:
    model = backend.componentlist()
    assert model.addcomponent(flux=1.0, dir="J2000 00h00m00s 00d00m00s")
    assert tool.modify(model.torecord(), mask='"source.im">1')
    assert model.done()
assert tool.done()
""", backend, operation],
        cwd=tmp_path, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
        time.sleep(0.2)
        assert child.poll() is None, child.stderr.read() if child.poll() is not None else ""
        assert owner.putchunk(np.full((4, 4), 2.0), locking=False)
        assert owner.unlock()
        assert child.wait(timeout=10) == 0, child.stderr.read()
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)
        owner.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_tempclosed_concat_rejects_a_new_input_generation(tmp_path):
    path = tmp_path / "first.im"
    owner, other = tools.image(), tools.image()
    assert other.fromshape(str(path), [3, 3], linear=True)
    assert other.setbrightnessunit("Jy")
    assert other.done()
    assert owner.fromshape(str(tmp_path / "second.im"), [3, 3], linear=True)
    product = tmp_path / "product.im"
    assert other.fromshape(str(product), [2, 2])
    assert other.putchunk(np.full((2, 2), 5.0))
    assert other.done()
    ready = tmp_path / "ready"
    child = subprocess.Popen(
        [sys.executable, "-c", """
from pathlib import Path
from casars import tools
tool = tools.image()
Path("ready").touch()
try:
    tool.imageconcat("product.im", ["first.im", "second.im"], axis=0,
                     tempclose=True, relax=True, overwrite=True)
except RuntimeError as error:
    assert "first.im changed while it was being concatenated" in str(error), error
else:
    raise AssertionError("the concatenation accepted a changed input")
assert tool.done()
"""],
        cwd=tmp_path, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
        time.sleep(0.2)
        assert child.poll() is None
        assert other.open(str(path))
        assert other.lock(writelock=True, nattempts=1)
        assert other.setbrightnessunit("K")
        assert other.putchunk(np.full((3, 3), 23.0), locking=False)
        assert other.done()
        assert owner.unlock()
        assert child.wait(timeout=10) == 0, child.stderr.read()
        assert other.open(str(product))
        np.testing.assert_array_equal(other.getchunk(), np.full((2, 2), 5.0))
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)
        other.done()
        owner.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_isconform_self_uses_one_coordinate_generation(casa, tmp_path):
    path = str(tmp_path / "coordinates.im")
    tool = tools.image()
    assert tool.fromshape(path, [4, 4], linear=True)
    assert tool.unlock()
    subprocess.run(
        [sys.executable, "-c", """
import sys
from casatools import image
tool = image()
assert tool.open(sys.argv[1])
coordinates = tool.coordsys()
assert coordinates.setreferencepixel([8, 9], type="linear")
assert tool.setcoordsys(coordinates.torecord())
coordinates.done()
assert tool.done()
""", path],
        check=True, timeout=10, capture_output=True,
    )
    assert tool.isconform(path)
    assert tool.isconform(path)
    assert_locks(tool, True, False)
    assert tool.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
@pytest.mark.parametrize(
    "operation,read,write",
    [
        ("modify", True, True),
        ("replacemaskedpixels", True, True),
        ("subimage", True, False),
        ("makecomplex", True, False),
        ("summary", True, False),
        ("isconform_self", True, False),
        ("isconform_other", False, False),
    ],
)
def test_image_operation_locks_after_unlock(casa, tmp_path, operation, read, write):
    model = casa.componentlist()
    assert model.addcomponent(flux=1.0, dir="J2000 00h00m00s 00d00m00s")
    components = model.torecord()
    assert model.done()
    other = str(tmp_path / "other.im")
    peer = casa.image()
    assert peer.fromshape(other, [7, 7])
    assert peer.done()

    for backend in (casa, tools):
        path = str(tmp_path / f"{backend.__name__}.im")
        instance = backend.image()
        assert instance.fromshape(path, [7, 7])
        if operation == "replacemaskedpixels":
            assert instance.calcmask("F", "invalid")
        assert instance.unlock()
        assert_locks(instance, False, False)
        if operation == "modify":
            assert instance.modify(components, subtract=False)
        elif operation == "replacemaskedpixels":
            assert instance.replacemaskedpixels(2.0)
        elif operation == "subimage":
            assert instance.subimage().done()
        elif operation == "makecomplex":
            assert instance.makecomplex(str(tmp_path / f"complex-{backend.__name__}.im"), other)
        elif operation == "summary":
            assert instance.summary(list=False)
        else:
            assert instance.isconform(path if operation == "isconform_self" else other)
        assert_locks(instance, read, write)
        assert instance.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_native_image_exchanges_locks_and_generations_with_casa(casa, tmp_path):
    path = tmp_path / "exchange.im"
    ia = tools.image()
    assert ia.fromshape(str(path), [2, 2])
    assert ia.putchunk(np.eye(2))
    np.testing.assert_array_equal(ia.getchunk(), np.eye(2))

    def run(source):
        result = subprocess.run(
            [sys.executable, "-c", source, str(path)],
            capture_output=True, text=True, timeout=20,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    probe = """
import sys
from casatools import table
tb = table()
assert tb.open(sys.argv[1], nomodify=False, lockoptions={'option': 'user'})
tb.lock(write=True, nattempts=1)
print('LOCK', tb.haslock(write=True))
tb.close()
"""
    assert "LOCK False" in run(probe)
    assert ia.lock(writelock=True, nattempts=1)
    assert "LOCK False" in run("""
import fcntl
import sys
from pathlib import Path
with (Path(sys.argv[1]) / 'table.lock').open('rb') as lock:
    try:
        fcntl.lockf(lock, fcntl.LOCK_SH | fcntl.LOCK_NB, 1, 0)
    except BlockingIOError:
        print('LOCK False')
""")
    assert ia.unlock()
    assert "LOCK True" in run(probe)

    run("""
import sys
import numpy as np
from casatools import image
ia = image()
assert ia.open(sys.argv[1])
assert ia.lock(writelock=True, nattempts=1)
assert ia.putchunk(np.full((2, 2), 7.0), locking=False)
assert ia.setmiscinfo({'writer': 'CASA'})
assert ia.unlock() and ia.done()
""")
    assert ia.lock(nattempts=1)
    assert ia.miscinfo() == {"writer": "CASA"}
    np.testing.assert_array_equal(ia.getchunk(), np.full((2, 2), 7.0))
    assert ia.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_image_pixel_reads_yield_automatic_lock_to_casa(casa, tmp_path):
    path = tmp_path / "wait.im"
    ready = tmp_path / "reader-ready"
    ia = tools.image()
    assert ia.fromshape(str(path), [2, 2])
    assert ia.unlock()
    ia.getchunk()
    assert_locks(ia, True, False)
    child = subprocess.Popen(
        [sys.executable, "-c", """
import sys
from pathlib import Path
from casatools import image
ia = image()
assert ia.open(sys.argv[1])
Path(sys.argv[2]).touch()
assert ia.lock(writelock=True, nattempts=14)
assert ia.unlock() and ia.done()
""", str(path), str(ready)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 12
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.03)
        assert ready.exists()
        assert ia.lock(writelock=True, nattempts=1) is False
        assert_locks(ia, True, False)
        while child.poll() is None and time.monotonic() < deadline:
            ia.getchunk()
            if not ia.haslock()[0]:
                break
            time.sleep(0.03)
        assert_locks(ia, False, False)
        assert child.wait(timeout=5) == 0, child.stderr.read()
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)
        ia.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_putregion_yields_automatic_write_lock_to_casa_reader(casa, tmp_path):
    path = tmp_path / "edit.im"
    ready = tmp_path / "reader-ready"
    start = tmp_path / "request-lock"
    image = tools.image()
    values = np.full((2, 2), 7, dtype=np.float32)
    assert image.fromshape(str(path), [2, 2])
    assert image.unlock()
    child = subprocess.Popen(
        [sys.executable, "-c", """
import sys
import time
from pathlib import Path
import numpy as np
from casatools import table
reader = table()
assert reader.open(sys.argv[1], nomodify=False, lockoptions={'option': 'user'})
reader.unlock()
Path(sys.argv[2]).touch()
deadline = time.monotonic() + 12
while not Path(sys.argv[3]).exists() and time.monotonic() < deadline:
    time.sleep(0.03)
assert Path(sys.argv[3]).exists()
reader.lock(write=False, nattempts=14)
assert reader.haslock(write=False)
np.testing.assert_array_equal(reader.getcell('map', 0), np.full((2, 2), 7))
reader.close()
""", str(path), str(ready), str(start)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 12
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.03)
        assert ready.exists(), child.stderr.read() if child.poll() is not None else "CASA reader did not open"
        assert image.putregion(values)
        assert_locks(image, True, True)
        start.touch()
        while child.poll() is None and time.monotonic() < deadline:
            assert image.putregion(values)
            if not image.haslock()[1]:
                break
            time.sleep(0.03)
        assert_locks(image, False, False)
        assert child.wait(timeout=5) == 0, child.stderr.read()
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)
        image.done()
