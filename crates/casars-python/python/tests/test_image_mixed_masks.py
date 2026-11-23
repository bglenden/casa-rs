"""Native, complex and FITS inputs for the public image.calcmask method."""

import json

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def reference_images(tmp_path, monkeypatch):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    monkeypatch.chdir(tmp_path)
    real = np.arange(6, dtype=np.float32).reshape(2, 3) + 1
    writer = casa.image()
    for name, precision, pixels in [
        ("real.im", "f", real),
        ("double.im", "d", real),
        ("complex.im", "f", real.astype(np.complex64) + (6 - real) * 1j),
        ("dcomplex.im", "d", real.astype(np.complex128) + (6 - real) * 1j),
    ]:
        assert writer.fromarray(name, pixels, type=precision)
        if name == "real.im":
            assert writer.calcmask('"real.im">3', name="stored")
            assert writer.tofits("real.fits")
        if name == "complex.im":
            assert writer.calcmask('real("complex.im")>3', name="stored")
        assert writer.done()
    return casa, real


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('real("complex.im") > 2', lambda data: data > 2),
        ('imag("dcomplex.im") > 2', lambda data: (6 - data) > 2),
        ('"complex.im" > 2', lambda data: np.hypot(data, 6 - data) > 2),
        ('("real.im"+"double.im") > 4', lambda data: data > 2),
        ('"real.fits" > 2', lambda data: data > 3),
        ('mask("real.fits")', lambda data: data > 3),
        ('replace("real.fits",123)>120', lambda data: data <= 3),
        ('mask("real.im:stored")', lambda data: data > 3),
        ('mask("complex.im:stored")', lambda data: data > 3),
        ('mask("real.im:NoMask")', lambda data: np.ones_like(data, dtype=bool)),
        ('mask("real.fits:nomask")', lambda data: np.ones_like(data, dtype=bool)),
        ('mask("real.im" + "double.im")', lambda data: data > 3),
        ('mask("real.im" > 99)', lambda data: data > 3),
        ('mask(("real.im">0) || F)', lambda data: data > 3),
        ('mask(("real.im">0) && T)', lambda data: data > 3),
        ('"real.im" > mean("real.im")', lambda data: data > 5),
        ('mask(1)', lambda data: np.ones_like(data, dtype=bool)),
        ('isnan(complex(1,2))', lambda data: np.zeros_like(data, dtype=bool)),
    ],
)
def test_calcmask_compiles_mixed_pixel_operands_and_saves_validity(
    reference_images, expression, expected,
):
    casa, real = reference_images
    reference = casa.image()
    image = tools.image()
    try:
        assert reference.fromshape("reference.im", [2, 3])
        assert image.fromshape("output.im", [2, 3])
        for tool in (reference, image):
            assert tool.calcmask(expression, name="selection", asdefault=True)
            np.testing.assert_array_equal(tool.getchunk(getmask=True), expected(real))
        assert image.history(list=False)[-2:] == reference.history(list=False)[-2:]
        assert reference.done()
        assert image.done()
        assert reference.open("output.im")
        assert "selection" in reference.maskhandler("get")
        np.testing.assert_array_equal(reference.getchunk(getmask=True), expected(real))
    finally:
        image.done()
        reference.done()


def test_calcmask_requires_boolean_output_and_preserves_previous_mask(reference_images):
    casa, real = reference_images
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            assert image.calcmask('real("complex.im")>3', name="selection")
            with pytest.raises(RuntimeError, match="The expression type must be Boolean"):
                image.calcmask('real("complex.im")', name="selection")
            with pytest.raises(RuntimeError, match="shape of the expression does not conform"):
                image.calcmask('indexin(0,[1])', name="selection")
            with pytest.raises(RuntimeError, match="shapes of operands mismatch"):
                image.calcmask('indexin(0,[1]) && "real.im">1', name="selection")
            with pytest.raises(RuntimeError, match="unsupported data type"):
                image.calcmask('mask("dcomplex.im:stored")', name="selection")
            np.testing.assert_array_equal(image.getchunk(getmask=True), real > 3)
        finally:
            image.done()


def test_calcmask_rejects_tile_compressed_fits(reference_images):
    casa, real = reference_images
    fits = pytest.importorskip("astropy.io.fits")
    fits.HDUList([fits.PrimaryHDU(), fits.CompImageHDU(real.T)]).writeto("compressed.fits")
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            with pytest.raises(RuntimeError, match="No data in the zeroth or first extension"):
                image.calcmask('"compressed.fits" > 4')
            assert image.maskhandler("get") == ["T"]
        finally:
            image.done()


def test_calcmask_abbreviates_long_history_names(reference_images):
    casa, _ = reference_images
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            long_name = "q" * 301
            assert image.calcmask("T", name=long_name)
            assert image.maskhandler("default") == [long_name]
            assert image.history(list=False)[-1] == 'ia.calcmask(mask="T", name="...", asdefault=true)'
        finally:
            image.done()


def test_calcmask_boolean_validity_uses_a_decisive_known_operand(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    image = tools.image()
    try:
        pixels = np.arange(1, 7, dtype=np.float32).reshape(2, 3)
        assert image.fromarray("source.im", pixels)
        assert image.calcmask('"source.im" > 3')
        assert image.done()
        assert image.fromshape(shape=[2, 3])
        for expression in [
            'mask(("source.im" > 0) || T)',
            'mask(F && ("source.im" > 0))',
            'mask(("source.im" > 3) || (value("source.im") <= 3))',
        ]:
            assert image.calcmask(expression, name="known")
            np.testing.assert_array_equal(image.getchunk(getmask=True), True)
        assert image.calcmask('mask(("source.im" > 3) || (value("source.im") <= 2))', name="known")
        np.testing.assert_array_equal(image.getchunk(getmask=True), pixels != 3)
    finally:
        image.done()


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('("large.im"+1+"double.im")>16777217.5', False),
        ('real("large_complex.im"+complex(1,0))>16777216', False),
        ('real("large_complex.im"+complex("double.im",0))>16777216', True),
    ],
)
def test_calcmask_preserves_arithmetic_precision_at_promotion(reference_images, expression, expected):
    casa, _ = reference_images
    writer = casa.image()
    for name, precision, value, dtype in [
        ("large.im", "f", 16777216, np.float32),
        ("double.im", "d", 1, np.float64),
        ("large_complex.im", "f", 16777216 + 6j, np.complex64),
    ]:
        assert writer.fromarray(name, np.full((2, 3), value, dtype=dtype),
                                type=precision, overwrite=True)
        assert writer.done()
    for backend in (casa, tools):
        image = backend.image()
        try:
            assert image.fromshape(shape=[2, 3])
            assert image.calcmask(expression)
            np.testing.assert_array_equal(image.getchunk(getmask=True), expected)
        finally:
            image.done()


@pytest.mark.parametrize("backend", ["casatools", "casars"])
@pytest.mark.parametrize("kind", ["saved", "virtual", "nested"])
@pytest.mark.parametrize("method", ["statistics", "getregion", "subimage", "moments", "histograms"])
def test_analysis_mask_accepts_persisted_expression_operands(tmp_path, backend, kind, method):
    api = tools if backend == "casars" else pytest.importorskip("casatools")
    if backend == "casatools":
        assert api.version_string().replace("-", ".") == "6.7.6.14"
    leaf, peer, operand, output = (tmp_path / name for name in
                                   ("leaf.im", "peer.im", "operand.im", "output.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    tool = api.image()
    reader = None
    try:
        assert tool.fromarray(str(leaf), pixels, linear=True) and tool.done()
        if kind == "saved":
            operand.mkdir()
            (operand / "imageexpr.json").write_text(json.dumps({"ImageExpr": f'"{leaf}" * 3'}))
            valid = pixels * 3 > 4
        else:
            assert tool.fromarray(str(peer), pixels, linear=True) and tool.done()
            first, joined_path = leaf, operand
            if kind == "nested":
                first, joined_path = tmp_path / "inner.im", tmp_path / "joined.im"
                first.mkdir()
                (first / "imageexpr.json").write_text(json.dumps({"ImageExpr": f'"{leaf}" + 1'}))
            joined = tool.imageconcat(str(joined_path), [str(first), str(peer)], axis=0,
                                      mode="nomove", relax=True)
            assert joined.done()
            values = np.concatenate([pixels + (kind == "nested"), pixels])
            if kind == "nested":
                operand.mkdir()
                (operand / "imageexpr.json").write_text(
                    json.dumps({"ImageExpr": f'"{joined_path}" * 3'})
                )
                values *= 3
            pixels = np.concatenate([pixels, pixels])
            valid = values > 4
        assert tool.fromarray(pixels=pixels, linear=True)
        mask = f'"{operand}" > 4'
        if method == "statistics":
            stats = tool.statistics(mask=mask)
            np.testing.assert_array_equal(stats["npts"], [valid.sum()])
            np.testing.assert_array_equal(stats["sum"], [pixels[valid].sum()])
        elif method == "getregion":
            np.testing.assert_array_equal(tool.getregion(mask=mask, getmask=True), valid)
        elif method == "subimage":
            result = tool.subimage(outfile=str(output), mask=mask)
            assert result.done()
            reader_api = tools if backend == "casatools" else api
            if backend == "casars":
                import importlib.util
                if importlib.util.find_spec("casatools") is not None:
                    import casatools
                    reader_api = casatools
            reader = reader_api.image()
            assert reader.open(str(output))
            np.testing.assert_array_equal(reader.getchunk(getmask=True), valid)
            np.testing.assert_array_equal(reader.getchunk()[valid], pixels[valid])
        elif method == "moments":
            reader = tool.moments(moments=[-1], axis=0, mask=mask)
            accepted = valid.any(axis=0)
            expected = [pixels[:, i][valid[:, i]].mean() for i in np.flatnonzero(accepted)]
            np.testing.assert_array_equal(reader.getchunk(getmask=True), accepted)
            np.testing.assert_allclose(reader.getchunk()[accepted], expected)
        else:
            distribution = tool.histograms(mask=mask)
            assert distribution["counts"].sum() == valid.sum()
    finally:
        if reader is not None:
            reader.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casatools", "casars"])
@pytest.mark.parametrize("method", ["subimage", "moments", "collapse"])
def test_analysis_keeps_an_existing_transitive_mask_operand(tmp_path, backend, method):
    api = tools if backend == "casars" else pytest.importorskip("casatools")
    if backend == "casatools":
        assert api.version_string().replace("-", ".") == "6.7.6.14"
    source, destination, mask = (tmp_path / name for name in ("source.im", "output.im", "mask.im"))
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    image = api.image()
    try:
        assert image.fromarray(str(destination), pixels, linear=True) and image.done()
        assert image.fromarray(str(source), pixels * 5, linear=True)
        mask.mkdir()
        (mask / "imageexpr.json").write_text(json.dumps({"ImageExpr": f'"{destination}" * 3'}))
        arguments = dict(outfile=str(destination), mask=f'"{mask}" > 4', overwrite=True)
        if method == "moments":
            arguments.update(moments=[-1], axis=0)
        elif method == "collapse":
            arguments.update(function="mean", axes=[0])
        with pytest.raises(RuntimeError, match="already opened|table cache|overlaps|matches"):
            getattr(image, method)(**arguments)
        assert image.isopen()
        np.testing.assert_array_equal(image.getchunk(), pixels * 5)
        assert image.done() and image.open(str(destination))
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()


def test_analysis_rejects_a_cycle_between_mask_operand_formats(tmp_path):
    leaf, peer, saved, virtual = (tmp_path / name for name in
                                  ("leaf.im", "peer.im", "saved.im", "virtual.im"))
    image = tools.image()
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    try:
        for path in (leaf, peer):
            assert image.fromarray(str(path), pixels, linear=True) and image.done()
        joined = image.imageconcat(str(virtual), [str(leaf), str(peer)], axis=0, mode="nomove", relax=True)
        assert joined.done()
        saved.mkdir()
        (saved / "imageexpr.json").write_text(json.dumps({"ImageExpr": f'"{virtual}" * 1'}))
        descriptor = virtual / "imageconcat.json"
        manifest = json.loads(descriptor.read_text())
        manifest["Images"][0] = str(saved)
        descriptor.write_text(json.dumps(manifest))

        assert image.open(str(leaf))
        with pytest.raises(RuntimeError, match="Cycle in image expression or concatenation"):
            image.statistics(mask=f'"{saved}" > 1')
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()


def test_named_analysis_protects_mask_operand_opened_after_waiting_for_source(tmp_path):
    import subprocess
    import sys
    from concurrent.futures import ThreadPoolExecutor
    from time import monotonic, sleep

    def wait_for(ready, future=None):
        deadline = monotonic() + 10
        while monotonic() < deadline:
            if ready():
                return
            if future is not None and future.done():
                break
            sleep(0.02)
        pytest.fail("The staged image did not reach its expected synchronization point")

    source, destination, other, mask, held, release = (
        tmp_path / name for name in ("source.im", "destination.im", "other.im", "mask.im", "held", "release")
    )
    image = tools.image()
    pixels = np.arange(6, dtype=np.float32).reshape(3, 2)
    for path, values in ((source, pixels * 5), (destination, pixels), (other, pixels + 10)):
        assert image.fromarray(str(path), values) and image.done()
    assert image.open(str(source)) and image.unlock()
    mask.mkdir()
    descriptor = mask / "imageexpr.json"
    descriptor.write_text(json.dumps({"ImageExpr": f'"{other}" * 3'}))
    child = subprocess.Popen(
        [sys.executable, "-c", """
import sys
from pathlib import Path
from time import monotonic, sleep
from casars import tools
source, held, release = map(Path, sys.argv[1:])
image = tools.image()
assert image.open(str(source))
assert image.lock(writelock=True, nattempts=5)
held.touch()
deadline = monotonic() + 14
while not release.exists() and monotonic() < deadline:
    sleep(0.02)
image.done()
""", str(source), str(held), str(release)]
    )
    try:
        wait_for(held.exists)
        with ThreadPoolExecutor() as pool:
            future = pool.submit(image.subimage, outfile=str(destination),
                                 overwrite=True, mask=f'"{mask}" > 4')
            try:
                wait_for(lambda: any(tmp_path.glob(".tmp*")), future)
                next_descriptor = mask / "next.json"
                next_descriptor.write_text(json.dumps({"ImageExpr": f'"{destination}" * 3'}))
                next_descriptor.replace(descriptor)
            finally:
                release.touch()
            with pytest.raises(RuntimeError, match="matches or contains input image|overlaps"):
                future.result(timeout=10)
        assert image.isopen()
        np.testing.assert_array_equal(image.getchunk(), pixels * 5)
        assert image.done() and image.open(str(destination))
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        release.touch()
        if child.poll() is None:
            child.wait(timeout=15)
        image.done()
