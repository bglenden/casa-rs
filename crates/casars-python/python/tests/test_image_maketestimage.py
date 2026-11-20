"""The CASA image tool's demonstration FITS image factory."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


SAMPLE = "demo/Images/imagetestimage.fits"


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def write_sample(directory):
    sample = directory / SAMPLE
    sample.parent.mkdir(parents=True, exist_ok=True)
    content = _hdu(True, [1, -999, 3, 4])
    sample.write_bytes(content)
    return sample, content


def snapshot(image):
    return {
        "shape": image.shape(),
        "pixels": image.getchunk(),
        "mask": image.getchunk(getmask=True),
        "coordinates": image.coordsys().torecord(),
        "unit": image.brightnessunit(),
        "misc": image.miscinfo(),
        "history": image.history(),
    }


@pytest.mark.parametrize("persistent", [False, True])
def test_maketestimage_matches_casa_and_reopens_native(casa, tmp_path, monkeypatch, persistent):
    monkeypatch.chdir(tmp_path)
    sample, content = write_sample(tmp_path)
    results = []
    for module in (casa, tools):
        image = module.image()
        assert image.fromshape(shape=[7])
        outfile = f"{module.__name__}.im" if persistent else ""
        assert image.maketestimage(outfile=outfile)
        assert image.ispersistent() == persistent
        observed = snapshot(image)
        assert observed["history"][-2:] == [
            "Ran ia.maketestimage",
            f'ia.maketestimage(outfile="{outfile}", overwrite=false)',
        ]
        observed["coordinates"].pop("parentName", None)
        observed["history"].pop()
        results.append(observed)
        assert image.done()
        if persistent:
            reader = casa.image()
            assert reader.open(outfile)
            _assert_value(reader.getchunk(), observed["pixels"])
            _assert_value(reader.getchunk(getmask=True), observed["mask"])
            assert reader.done()
    _assert_value(results[1], results[0])
    np.testing.assert_array_equal(results[1]["mask"], [[True, True], [False, True]])
    assert sample.read_bytes() == content


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_maketestimage_overwrite_and_history(request, backend, tmp_path, monkeypatch):
    module = request.getfixturevalue("casa") if backend == "casa" else tools
    monkeypatch.chdir(tmp_path)
    sample, _ = write_sample(tmp_path)
    image = module.image()
    outfile = "test.im"
    assert image.maketestimage(outfile)
    with pytest.raises(RuntimeError, match="exists, and the user does not want to remove it"):
        image.maketestimage(outfile)
    assert not image.isopen()
    assert image.open(outfile)
    np.testing.assert_array_equal(image.getchunk(), [[1, 3], [np.nan, 4]])
    assert image.done()
    sample.write_bytes(_hdu(True, [9, 8, 7, 6]))
    assert image.dohistory(False)
    assert image.maketestimage(outfile, True)
    np.testing.assert_array_equal(image.getchunk(), [[9, 7], [8, 6]])
    assert image.history() == ["first custom history"]
    assert image.done()


@pytest.mark.parametrize("variable", ["CASA_RS_MEASURESPATH", "CASAPATH", "AIPSPATH"])
def test_maketestimage_searches_available_data_roots(tmp_path, monkeypatch, variable):
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    root = tmp_path / "install"
    for name in ("CASA_RS_MEASURESPATH", "CASAPATH", "AIPSPATH"):
        monkeypatch.delenv(name, raising=False)
    sample_root = root if variable == "CASA_RS_MEASURESPATH" else root / "data"
    write_sample(sample_root)
    monkeypatch.setenv(variable, str(root))
    image = tools.image()
    assert image.maketestimage()
    np.testing.assert_array_equal(image.shape(), [2, 2])
    assert image.done()


def test_maketestimage_with_missing_sample_detaches_without_writing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for name in ("CASA_RS_MEASURESPATH", "CASAPATH", "AIPSPATH"):
        monkeypatch.delenv(name, raising=False)
    image = tools.image()
    assert image.fromshape(shape=[3])
    with pytest.raises(RuntimeError, match=r"demo/Images/imagetestimage\.fits.*not readable"):
        image.maketestimage("untouched.im")
    assert not image.isopen()
    assert not (tmp_path / "untouched.im").exists()
