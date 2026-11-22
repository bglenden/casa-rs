"""Read independent MIRIAD directories through the public image tool."""

import math
import struct

import numpy as np
import pytest

from casars import tools
from test_image_removal import _backend


def _text(value):
    return struct.pack(">I", 1) + value.encode("ascii")


def _integer(value):
    return struct.pack(">Ii", 2, value)


def _real(value):
    return struct.pack(">I4xd", 5, value)


def _write_cache(path, items):
    entries = list(items.items())
    with path.open("wb") as cache:
        for index, (name, value) in enumerate(entries):
            cache.write(struct.pack(">15sB", name.encode("ascii"), len(value)))
            cache.write(value)
            if index + 1 < len(entries):
                cache.write(bytes(-len(value) % 16))


def _sky(path, layout="cached", external_pixels=True, cube=None, rest_frequency=1.420405751786):
    path.mkdir()
    shape = (128, 16)
    values = np.arange(shape[0])[:, None] + 10 * np.arange(shape[1])[None, :]
    items = {
        "naxis": _integer(2),
        "naxis1": _integer(shape[0]),
        "naxis2": _integer(shape[1]),
        "ctype1": _text("RA---SIN"),
        "ctype2": _text("DEC--SIN"),
        "crval1": _real(1.),
        "crval2": _real(.25),
        "crpix1": _real(2.),
        "crpix2": _real(2.),
        "cdelt1": _real(-.0001),
        "cdelt2": _real(.0001),
        "epoch": _real(2000.),
        "bunit": _text("Jy/pixel"),
    }
    if cube:
        values = (
            values[:, :, None, None]
            + 1000 * np.arange(5)[None, None, :, None]
            + 10_000 * np.arange(2)[None, None, None, :]
        )
        items.update({
            "naxis": _integer(4),
            "naxis3": _integer(5),
            "naxis4": _integer(2),
            "ctype3": _text(cube),
            "ctype4": _text("STOKES"),
            "crpix3": _real(1.),
            "crpix4": _real(1.),
            "crval3": _real(1.412 if cube.startswith("FREQ") else 1000.),
            "crval4": _real(1.),
            "cdelt3": _real(.002 if cube.startswith("FREQ") else 2000.),
            "cdelt4": _real(1.),
            "bmaj": _real(.0002),
            "bmin": _real(.0001),
            "bpa": _real(35.),
            "telescop": _text("VLA"),
            "obstime": _real(2459000.5),
        })
        if rest_frequency is not None:
            items["restfreq"] = _real(rest_frequency)
    if layout == "external":
        del items["naxis1"]
        (path / "naxis1").write_bytes(struct.pack(">I4xq", 8, shape[0]))
        (path / "crval2").write_bytes(struct.pack(">If", 4, .25))
        del items["crval2"]

    encoded = struct.pack(">I", 4) + values.astype(">f4").tobytes(order="F")
    if external_pixels:
        (path / "image").write_bytes(encoded)
    else:
        items["image"] = encoded[:20]
    _write_cache(path / "header", items)
    flags = [0x7fffffff] * math.ceil(values.size / 31)
    reject = shape[0] + 2
    flags[reject // 31] &= ~(1 << (reject % 31))
    (path / "mask").write_bytes(struct.pack(f">I{len(flags)}I", 2, *flags))
    (path / "history").write_text("Independent MIRIAD history line\n", encoding="ascii")
    return values.astype(np.float32)


def _sin_pixel(x, y):
    l = -(x - 1) * .0001
    m = (y - 1) * .0001
    n = math.sqrt(1 - l*l - m*m)
    reference = .25
    longitude = 1. + math.atan2(l, n * math.cos(reference) - m * math.sin(reference))
    latitude = math.asin(m * math.cos(reference) + n * math.sin(reference))
    return [longitude, latitude]


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize(("method", "kwargs"), [
    ("open", {"cache": True}), ("open", {"cache": False}),
    ("newimage", {}), ("newimagefromfile", {}),
])
@pytest.mark.parametrize("layout", ["cached", "external"])
def test_direct_miriad_file_factories(tmp_path, monkeypatch, backend, method, kwargs, layout):
    path = tmp_path / "input.mir"
    pixels = _sky(path, layout)
    root = _backend(backend).image()
    image = root
    monkeypatch.chdir(tmp_path)
    try:
        assert root.fromarray(pixels=[31., 42.])
        result = getattr(root, method)(path.name, **kwargs)
        if method == "open":
            assert result is True
        else:
            image = result
            np.testing.assert_array_equal(root.getchunk(), [31., 42.])
        assert image.isopen() and image.ispersistent()
        assert image.name() == str(path) and image.name(True) == path.name
        np.testing.assert_array_equal(image.shape(), pixels.shape)
        assert image.pixeltype() == "float"
        assert image.brightnessunit() == "Jy/pixel"
        selection = dict(blc=[23, 3], trc=[44, 15], inc=[3, 4])
        read = image.getchunk(**selection)
        assert read.dtype == np.float64
        np.testing.assert_array_equal(read, pixels[23:45:3, 3:16:4])
        assert image.getchunk(blc=[2, 1], trc=[2, 1], getmask=True).item()
        assert image.getchunk(blc=[2, 1], trc=[2, 1]).item() == 12.
        assert image.history() == []
        coordinates = image.coordsys()
        try:
            assert coordinates.setunits(["rad", "rad"])
            np.testing.assert_allclose(coordinates.toworld([40, 8])["numeric"], _sin_pixel(40, 8), rtol=0, atol=1e-10)
        finally:
            coordinates.done()
    finally:
        image.done()
        if root is not image:
            root.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("kind", ["FREQ-LSR", "VELO-LSR", "FELO-LSR"])
def test_miriad_frequency_stokes_and_beam_metadata(tmp_path, backend, kind):
    path = tmp_path / "frequency.mir"
    pixels = _sky(path, cube=kind)
    image = _backend(backend).image()
    try:
        assert image.open(str(path))
        point = [6, 2, 3, 1]
        assert image.getchunk(blc=point, trc=point).item() == pixels[6, 2, 3, 1]
        coordinates = image.coordsys()
        try:
            assert coordinates.setunits(["rad", "rad", "Hz", ""])
            assert coordinates.stokes() == ["I", "Q"]
            assert coordinates.referencecode("spectral")[0] == "LSRK"
            assert coordinates.telescope() == "VLA"
            rest = np.asarray(coordinates.restfrequency()["value"]).item()
            rest_hz = 1.420405751786e9
            np.testing.assert_allclose(rest, rest_hz, rtol=1e-12)
            numeric = coordinates.toworld(point)["numeric"]
            np.testing.assert_allclose(numeric[:2], _sin_pixel(*point[:2]), rtol=0, atol=1e-10)
            speed_of_light = 299_792_458.
            if kind.startswith("VELO"):
                expected_frequency = rest_hz * (1. - 7e6 / speed_of_light)
            elif kind.startswith("FELO"):
                reference = rest_hz / (1. + 1e6 / speed_of_light)
                expected_frequency = reference * (1. - 6e6 / (speed_of_light + 1e6))
            else:
                expected_frequency = 1.418e9
            np.testing.assert_allclose(numeric[2:], [expected_frequency, 2.], rtol=1e-12)
        finally:
            coordinates.done()
        beam = image.restoringbeam()
        scales = {"rad": 1., "deg": math.pi / 180, "arcsec": math.pi / 648000}
        for key, expected in (("major", .0002), ("minor", .0001), ("positionangle", math.radians(35.))):
            quantity = beam[key]
            assert quantity["value"] * scales[quantity["unit"]] == pytest.approx(expected)
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("rest_frequency", [None, 0.])
def test_miriad_velocity_needs_a_nonzero_rest_frequency(tmp_path, backend, rest_frequency):
    path = tmp_path / "velocity.mir"
    _sky(path, cube="VELO-LSR", rest_frequency=rest_frequency)
    image = _backend(backend).image()
    try:
        assert image.fromarray(pixels=[31., 42.])
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            image.newimagefromfile(str(path))
        np.testing.assert_array_equal(image.getchunk(), [31., 42.])
    finally:
        image.done()


@pytest.mark.parametrize("writer_backend", ["casa", "casars"])
@pytest.mark.parametrize("reader_backend", ["casa", "casars"])
@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
def test_miriad_copies_are_independently_writable_native_images(
    tmp_path, writer_backend, reader_backend, method,
):
    writer = _backend(writer_backend)
    reader = _backend(reader_backend)
    source = tmp_path / "source.mir"
    target = tmp_path / "copied.im"
    expected = _sky(source)
    original_bytes = (source / "image").read_bytes()
    image = writer.image()
    product = image
    restored = reader.image()
    try:
        assert image.fromshape(shape=[3])
        result = getattr(image, method)(infile=str(source), outfile=str(target))
        if method == "fromimage":
            assert result is True
        else:
            product = result
            np.testing.assert_array_equal(image.shape(), [3])
        assert product.name() == str(target)
        assert product.brightnessunit() == "Jy/pixel"
        assert product.history()[-2] == f"Ran ia.{method}"
        np.testing.assert_array_equal(product.getchunk(), expected)
        assert np.all(product.getchunk(getmask=True))
        assert product.putchunk(np.asarray([[-7.]], dtype=np.float32), blc=[2, 1])
        assert product.done()
        expected[2, 1] = -7.
        assert restored.open(str(target))
        np.testing.assert_array_equal(restored.getchunk(), expected)
        assert restored.brightnessunit() == "Jy/pixel"
        assert np.all(restored.getchunk(getmask=True))
        coordinates = restored.coordsys()
        try:
            assert coordinates.setunits(["rad", "rad"])
            np.testing.assert_allclose(coordinates.toworld([40, 8])["numeric"], _sin_pixel(40, 8), rtol=0, atol=1e-10)
        finally:
            coordinates.done()
        assert (source / "image").read_bytes() == original_bytes
    finally:
        restored.done()
        if product is not image:
            product.done()
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_pixels_and_masks_cannot_be_written(tmp_path, backend):
    path = tmp_path / "read-only.mir"
    pixels = _sky(path)
    image = _backend(backend).image()
    try:
        assert image.open(str(path))
        with pytest.raises(RuntimeError, match="not writable"):
            image.putchunk(np.asarray([[99.]], dtype=np.float32), blc=[2, 1])
        with pytest.raises(RuntimeError, match="not writable"):
            image.putregion(pixels=np.full(pixels.shape, 99., dtype=np.float32))
        with pytest.raises(RuntimeError, match=r"to\.isWritable\(\)"):
            image.set(pixels=99., pixelmask=0)
        with pytest.raises(RuntimeError, match="no pixelmask available"):
            image.set(pixelmask=0)
        assert image.isopen()
        assert image.history() == []
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert np.all(image.getchunk(getmask=True))
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_admission_needs_external_pixels_and_preserves_table_precedence(tmp_path, backend):
    cached = tmp_path / "cached.mir"
    _sky(cached, external_pixels=False)
    source = tmp_path / "source.mir"
    _sky(source)
    table_path = tmp_path / "plain-table"
    table = tools.table()
    try:
        assert table.create(str(table_path), {"ID": {"valueType": "int"}}, nrow=1)
    finally:
        table.done()
    for name in ("header", "image"):
        (table_path / name).write_bytes((source / name).read_bytes())
    image = _backend(backend).image()
    try:
        for path in (cached, table_path):
            assert image.fromshape(shape=[3])
            with pytest.raises(RuntimeError, match="Unable to open image"):
                image.open(str(path))
            assert not image.isopen()
    finally:
        image.done()


def test_malformed_miriad_payload_preserves_the_factory_receiver(tmp_path):
    path = tmp_path / "short.mir"
    _sky(path)
    (path / "image").write_bytes(struct.pack(">If", 4, 99.))
    image = tools.image()
    try:
        assert image.fromarray(pixels=[31., 42.])
        with pytest.raises(RuntimeError, match="invalid MIRIAD Float pixel payload"):
            image.newimagefromfile(str(path))
        np.testing.assert_array_equal(image.getchunk(), [31., 42.])
    finally:
        image.done()
