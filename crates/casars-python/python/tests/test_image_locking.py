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
