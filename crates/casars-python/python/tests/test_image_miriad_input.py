"""Read independent MIRIAD directories through the public image tool."""

import math
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

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


def _replace_cached_number(path, name, original, replacement):
    header = path / "header"
    data = header.read_bytes()
    tag = struct.pack(">15sB", name.encode("ascii"), len(_real(original)))
    assert tag + _real(original) in data
    header.write_bytes(data.replace(tag + _real(original), tag + _real(replacement)))


def _sky(path, layout="cached", external_pixels=True, cube=None, rest_frequency=1.420405751786,
         stokes_planes=2):
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
            + 10_000 * np.arange(stokes_planes)[None, None, None, :]
        )
        items.update({
            "naxis": _integer(4),
            "naxis3": _integer(5),
            "naxis4": _integer(stokes_planes),
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


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
@pytest.mark.parametrize("tempclose", [False, True])
@pytest.mark.parametrize("mode", ["paged", "nomove", "copy", "move"])
def test_miriad_concatenates_with_a_masked_native_image(tmp_path, writer, reader, tempclose, mode):
    source = _backend(writer).image()
    restored = _backend(reader).image()
    external = tmp_path / "external.mir"
    native = tmp_path / "native.im"
    output = tmp_path / "joined.im"
    external_pixels = _sky(external)
    external_bytes = (external / "image").read_bytes()
    original_directory = external.stat()
    native_pixels = np.full_like(external_pixels, 5.)
    native_pixels[7, 3] = -5.
    expected = np.concatenate([external_pixels, native_pixels])
    valid = np.concatenate([np.ones_like(external_pixels, dtype=bool), native_pixels > 0])
    joined = None
    try:
        assert source.open(str(external))
        coords = source.coordsys()
        record = coords.torecord()
        coords.done()
        assert source.done()
        assert source.fromarray(str(native), native_pixels, csys=record)
        assert source.setbrightnessunit("Jy/pixel")
        assert source.calcmask(f'"{native}" > 0', name="valid")
        assert source.done()

        joined = source.imageconcat(str(output), [str(external), str(native)], axis=0,
                                    relax=True, mode=mode, tempclose=tempclose)
        np.testing.assert_array_equal(joined.shape(), expected.shape)
        np.testing.assert_array_equal(joined.getchunk(), expected)
        assert [path.exists() for path in (external, native)] == [mode != "move"] * 2
        member = output / external.name if mode in {"copy", "move"} else external
        assert (member / "image").read_bytes() == external_bytes
        if mode == "move" and writer == "casars":
            assert os.path.samestat(original_directory, member.stat())
        assert joined.done()

        assert restored.open(str(output))
        assert restored.pixeltype() == "float"
        assert restored.brightnessunit() == "Jy/pixel"
        pixels = restored.getchunk()
        assert pixels.dtype == np.float64
        np.testing.assert_array_equal(pixels, expected)
        np.testing.assert_array_equal(restored.getchunk(getmask=True), valid)
        coords = restored.coordsys()
        try:
            assert coords.setunits(["rad", "rad"])
            np.testing.assert_allclose(coords.toworld([40, 8])["numeric"], _sin_pixel(40, 8),
                                       rtol=0, atol=1e-10)
        finally:
            coords.done()
        patch = np.array([[73.]], dtype=np.float32)
        if mode == "paged":
            assert restored.putchunk(patch, blc=[5, 1])
            assert restored.getchunk([5, 1], [5, 1]).item() == 73.
        else:
            assert restored.unlock()
            np.testing.assert_array_equal(restored.haslock(), [True, True])
            with pytest.raises(RuntimeError, match="not writable"):
                restored.putchunk(patch, blc=[5, 1])
            np.testing.assert_array_equal(restored.getchunk(), expected)
        assert (member / "image").read_bytes() == external_bytes
    finally:
        restored.done()
        if joined is not None:
            joined.done()
        source.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["copy", "move"])
def test_miriad_owned_concat_ignores_unrelated_table_control(tmp_path, backend, mode):
    image = _backend(backend).image()
    paths = [tmp_path / "left.mir", tmp_path / "right.mir"]
    expected = np.concatenate([_sky(path) for path in paths])
    for path in paths:
        (path / "table.dat").write_text("unrelated sidecar")
    output = tmp_path / "joined.im"
    joined = image.imageconcat(str(output), list(map(str, paths)), axis=0, relax=True, mode=mode)
    try:
        np.testing.assert_array_equal(joined.getchunk(), expected)
        for path in paths:
            locations = [output / path.name]
            if mode == "copy":
                locations.append(path)
            for location in locations:
                assert (location / "table.dat").read_text() == "unrelated sidecar"
                assert not (location / "table.lock").exists()
    finally:
        joined.done()
        image.done()


@pytest.mark.parametrize(("backend", "mode", "same_binding"), [
    ("casa", "copy", False), ("casars", "copy", False), ("casars", "move", False),
    ("casa", "move", True), ("casars", "move", True),
])
def test_miriad_owned_concat_keeps_linked_payload_and_header(tmp_path, backend, mode, same_binding):
    image = _backend(backend).image()
    source_parent = tmp_path / "source"
    source_parent.mkdir()
    first, original, replacement = (source_parent / "a.mir", source_parent / "b.mir", tmp_path / "b.mir")
    _sky(first)
    values = _sky(original)
    swapped = _sky(replacement) + 1000
    (replacement / "image").write_bytes(struct.pack(">I", 4) + swapped.astype(">f4").tobytes(order="F"))
    _replace_cached_number(replacement, "crval1", 1., 2.)
    for name in ("image", "header"):
        (first / name).unlink()
        (first / name).symlink_to(Path("../b.mir") / name)
    output = tmp_path / "joined.im"
    args = dict(axis=0, relax=True, mode=mode)
    try:
        if mode == "copy" or same_binding:
            second = original if same_binding else replacement
            joined = image.imageconcat(str(output), [str(first), str(second)], **args)
            try:
                want = values if same_binding else swapped
                np.testing.assert_array_equal(joined.getchunk(), np.concatenate([values, want]))
                np.testing.assert_allclose(joined.toworld([1, 1])["numeric"], [1., .25], atol=1e-12)
            finally:
                joined.done()
        else:
            assert image.fromarray(str(output), [17.]) and image.done()
            with pytest.raises(RuntimeError, match="MIRIAD image file .* different file"):
                image.imageconcat(str(output), [str(first), str(replacement)], overwrite=True, **args)
            assert image.open(str(output))
            np.testing.assert_array_equal(image.getchunk(), [17.])
        for name in ("image", "header"):
            member = output / first.name if same_binding else first
            referent = output / original.name if same_binding else original
            assert (member / name).is_symlink()
            assert (member / name).resolve() == referent / name
        assert replacement.is_dir()
    finally:
        image.done()


@pytest.mark.skipif(os.name != "posix", reason="requires casacore POSIX access locks")
def test_nested_miriad_concat_keeps_advisory_locks_and_restores_physical_locks(tmp_path):
    external, first, last, inner_path, outer_path = (
        tmp_path / name for name in ("sky.mir", "inner.im", "last.im", "nested.im", "outer.im")
    )
    pixels = _sky(external)
    tool = tools.image()
    assert tool.open(str(external))
    coords = tool.coordsys()
    record = coords.torecord()
    coords.done()
    assert tool.done()
    for path in (first, last):
        assert tool.fromarray(str(path), np.full_like(pixels, 3.), csys=record)
        assert tool.done()
    args = dict(axis=0, relax=True, mode="nomove", tempclose=False)
    inner = tool.imageconcat(str(inner_path), [str(external), str(first)], **args)
    assert inner.done()
    outer = tool.imageconcat(str(outer_path), [str(inner_path), str(last)], **args)
    assert outer.unlock()
    np.testing.assert_array_equal(outer.haslock(), [True, True])
    probe = """
import fcntl
from pathlib import Path
import sys
with (Path(sys.argv[1]) / 'table.lock').open('r+b') as data:
    fcntl.lockf(data, fcntl.LOCK_SH | fcntl.LOCK_NB, 1, 0)
    if sys.argv[2] == 'wait':
        print('ready', flush=True)
        sys.stdin.readline()
"""
    blocker = subprocess.Popen([sys.executable, "-c", probe, str(last), "wait"],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        assert blocker.stdout.readline().strip() == "ready"
        assert not outer.lock(writelock=True, nattempts=1)
        # Both reported flags are advisory. The inner native table must have
        # released the real write lock it acquired before the last member failed.
        np.testing.assert_array_equal(outer.haslock(), [True, True])
        subprocess.run([sys.executable, "-c", probe, str(first), "check"],
                       check=True, capture_output=True, text=True, timeout=10)
    finally:
        blocker.communicate(input="done\n", timeout=10)
        outer.done()
        tool.done()


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
            np.testing.assert_array_equal(coordinates.projection()["parameters"], [0., 0.])
            np.testing.assert_allclose(coordinates.toworld([40, 8])["numeric"], _sin_pixel(40, 8), rtol=0, atol=1e-10)
        finally:
            coordinates.done()
    finally:
        image.done()
        if root is not image:
            root.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_b1950_direction_frame(tmp_path, backend):
    source = tmp_path / "b1950.mir"
    _sky(source)
    _replace_cached_number(source, "epoch", 2000., 1950.)
    image = _backend(backend).image()
    try:
        assert image.open(str(source))
        coordinate = image.coordsys()
        try:
            assert coordinate.referencecode("direction")[0] == "B1950"
            assert coordinate.setunits(["rad", "rad"])
            np.testing.assert_allclose(
                coordinate.toworld([40, 8])["numeric"], _sin_pixel(40, 8), rtol=0, atol=1e-10
            )
        finally:
            coordinate.done()
    finally:
        image.done()


def test_miriad_non_ascii_coordinate_type_preserves_the_factory_receiver(tmp_path):
    source = tmp_path / "non-ascii-axis.mir"
    _sky(source)
    header = source / "header"
    header.write_bytes(header.read_bytes().replace(b"RA---SIN", b"RA-\xc3\xa9SIN"))
    image = tools.image()
    try:
        assert image.fromarray(pixels=[31., 42.])
        with pytest.raises(RuntimeError, match="CTYPE1 must be ASCII"):
            image.newimagefromfile(str(source))
        np.testing.assert_array_equal(image.getchunk(), [31., 42.])
    finally:
        image.done()


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


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_rejects_more_than_four_stokes_planes(tmp_path, backend):
    path = tmp_path / "too-many-stokes.mir"
    _sky(path, cube="FREQ-LSR", stokes_planes=5)
    image = _backend(backend).image()
    try:
        assert image.fromarray(pixels=[31., 42.])
        with pytest.raises(RuntimeError, match="Stokes axis longer than 4 pixels"):
            image.newimagefromfile(str(path))
        np.testing.assert_array_equal(image.getchunk(), [31., 42.])
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize(("code", "expected"), [(-8., "YX"), (5., "PFlinear"),
                                                (6., "PFlinear"), (7., "Pangle"), (8., "??")])
def test_miriad_stokes_codes(tmp_path, backend, code, expected):
    path = tmp_path / "polarization.mir"
    _sky(path, cube="FREQ-LSR", stokes_planes=1)
    _replace_cached_number(path, "crval4", 1., code)
    image = _backend(backend).image()
    try:
        assert image.open(str(path))
        coordinates = image.coordsys()
        try:
            assert coordinates.stokes() == [expected]
        finally:
            coordinates.done()
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("case", ["direction", "reference-pixel-one", "frequency", "beam"])
def test_miriad_validates_calibrated_metadata_before_attaching(tmp_path, backend, case):
    path = tmp_path / "validation.mir"
    _sky(path, cube="FREQ-LSR" if case in {"frequency", "beam"} else None)
    message = "Linear transformation matrix is singular"
    if case == "beam":
        _replace_cached_number(path, "bmaj", .0002, .00005)
        message = "(?i)major axis.*minor axis"
    elif case == "frequency":
        _replace_cached_number(path, "cdelt3", .002, 0.)
    else:
        _replace_cached_number(path, "cdelt1", -.0001, 0.)
        if case == "reference-pixel-one":
            _replace_cached_number(path, "crpix1", 2., 1.)
    image = _backend(backend).image()
    try:
        assert image.fromarray(pixels=[31., 42.])
        if case == "reference-pixel-one":
            assert image.open(str(path))
            coordinates = image.coordsys()
            try:
                assert coordinates.setunits(["rad", "rad"])
                np.testing.assert_allclose(coordinates.increment()["numeric"], [1., .0001])
            finally:
                coordinates.done()
        else:
            with pytest.raises(RuntimeError, match=message):
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
            image.putchunk(np.asarray([[99.]], dtype=np.float32), replicate=True)
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


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires a POSIX named pipe")
def test_miriad_admission_does_not_open_a_nonregular_table_control_file(tmp_path):
    path = tmp_path / "pipe-control.mir"
    _sky(path)
    os.mkfifo(path / "table.dat")
    script = """
import sys
from casars import tools

image = tools.image()
assert image.open(sys.argv[1])
assert image.getchunk(blc=[2, 1], trc=[2, 1]).item() == 12.
assert image.done()
"""
    subprocess.run([sys.executable, "-c", script, str(path)], check=True, timeout=10)


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


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("existing", [None, "directory", "file"])
def test_rename_miriad_reopens_pixels_and_header_metadata(tmp_path, backend, existing):
    source = tmp_path / "source.mir"
    destination = tmp_path / "destination.mir"
    pixels = _sky(source)
    if existing == "directory":
        destination.mkdir()
        (destination / "previous-output").write_bytes(b"previous contents")
    elif existing == "file":
        destination.write_bytes(b"a previous regular file")
    overwrite = existing is not None
    image = _backend(backend).image()
    try:
        assert image.open(str(source))
        assert image.setbrightnessunit("K")
        assert image.rename(str(destination), overwrite=overwrite)
        assert not source.exists()
        assert not (destination / "previous-output").exists()
        assert image.name() == str(destination)
        assert image.brightnessunit() == "Jy/pixel"
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert image.history() == [
            "Ran ia.rename",
            f'ia.rename(name="{destination}", overwrite={str(overwrite).lower()})',
            f"original name was {source}",
        ]
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_rename_miriad_between_filesystems(tmp_path, backend, overwrite):
    target_root = Path(__file__).resolve().parents[4] / "target"
    if not target_root.is_dir() or target_root.stat().st_dev == tmp_path.stat().st_dev:
        pytest.skip("the build directory and temporary files share a filesystem")
    source = tmp_path / "source.mir"
    pixels = _sky(source)
    original_items = {item.name: item.read_bytes() for item in source.iterdir()}
    with tempfile.TemporaryDirectory(dir=target_root) as directory:
        target = Path(directory) / "target.mir"
        if overwrite:
            target.mkdir()
            (target / "old-item").touch()
        image = _backend(backend).image()
        try:
            assert image.open(str(source))
            assert image.rename(str(target), overwrite=overwrite)
            assert not source.exists()
            assert image.name() == str(target)
            assert {item.name: item.read_bytes() for item in target.iterdir()} == original_items
            np.testing.assert_array_equal(image.getchunk(), pixels)
        finally:
            image.done()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory permissions")
@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_rename_miriad_can_move_a_read_only_directory(tmp_path, backend, overwrite):
    source = tmp_path / "read-only.mir"
    target = tmp_path / "renamed.mir"
    pixels = _sky(source)
    if overwrite:
        target.mkdir()
        (target / "previous-output").touch()
    source.chmod(0o555)
    image = _backend(backend).image()
    try:
        assert image.open(str(source))
        assert image.rename(str(target), overwrite=overwrite)
        assert not source.exists()
        assert not (target / "previous-output").exists()
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()
        (target if target.exists() else source).chmod(0o755)


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0,
                    reason="requires unprivileged POSIX directory permissions")
def test_miriad_cross_device_refusal_restores_read_only_source_and_previous_output(tmp_path):
    target_root = Path(__file__).resolve().parents[4] / "target"
    if not target_root.is_dir() or target_root.stat().st_dev == tmp_path.stat().st_dev:
        pytest.skip("the build directory and temporary files share a filesystem")
    source = tmp_path / "source.mir"
    pixels = _sky(source)
    source.chmod(0o555)
    with tempfile.TemporaryDirectory(dir=target_root) as directory:
        target = Path(directory) / "destination.mir"
        _sky(target)
        marker = target / "old-output"
        marker.write_bytes(b"keep the prior output")
        image = tools.image()
        try:
            assert image.open(str(source))
            with pytest.raises(RuntimeError, match="Cannot move MIRIAD image .* across filesystems"):
                image.rename(str(target), overwrite=True)
            assert not image.isopen()
            assert source.stat().st_mode & 0o777 == 0o555
            assert marker.read_bytes() == b"keep the prior output"
            assert image.open(str(source))
            np.testing.assert_array_equal(image.getchunk(), pixels)
        finally:
            image.done()
            if source.exists():
                source.chmod(0o755)


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0,
                    reason="requires unprivileged POSIX directory permissions")
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_open_only_needs_search_permission_on_directory(tmp_path, backend):
    source = tmp_path / "source.mir"
    pixels = _sky(source)
    source.chmod(0o111)
    image = _backend(backend).image()
    try:
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()
        source.chmod(0o755)


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_rename_refusal_and_removal_detach_without_deleting(tmp_path, backend):
    source = tmp_path / "source.mir"
    destination = tmp_path / "destination.mir"
    _sky(source)
    _sky(destination)
    original = (source / "image").read_bytes()
    replacement = (destination / "image").read_bytes()
    image = _backend(backend).image()
    try:
        assert not image.removefile(str(source))
        for target, overwrite, message in (
            (source, True, "same as the current image name"),
            (destination, False, "does not want to remove it"),
        ):
            assert image.open(str(source))
            with pytest.raises(RuntimeError, match=message):
                image.rename(str(target), overwrite=overwrite)
            assert not image.isopen()
            assert (source / "image").read_bytes() == original
            assert (destination / "image").read_bytes() == replacement
        assert image.open(str(source))
        assert not image.removefile(str(source))
        assert image.isopen()
        with pytest.raises(RuntimeError, match="table is not writable"):
            image.remove()
        assert not image.isopen()
        assert (source / "image").read_bytes() == original
    finally:
        image.done()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX symbolic links")
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_rename_moves_source_referent_and_replaces_destination_link(tmp_path, backend):
    source = tmp_path / "source.mir"
    source_alias = tmp_path / "source-alias.mir"
    destination = tmp_path / "destination.mir"
    previous = tmp_path / "previous.mir"
    pixels = _sky(source)
    _sky(previous)
    marker = previous / "keep-me"
    marker.write_bytes(b"the destination link's original referent")
    source_alias.symlink_to(source.name, target_is_directory=True)
    destination.symlink_to(previous.name, target_is_directory=True)
    image = _backend(backend).image()
    try:
        assert image.open(str(source_alias))
        assert image.rename(str(destination), overwrite=True)
        assert source_alias.is_symlink() and not source_alias.exists()
        assert not source.exists()
        assert destination.is_dir() and not destination.is_symlink()
        assert marker.read_bytes() == b"the destination link's original referent"
        assert image.history()[-1] == f"original name was {source_alias}"
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()


def test_miriad_rename_never_removes_an_ancestor_of_the_source(tmp_path):
    parent = tmp_path / "parent"
    parent.mkdir()
    source = parent / "source.mir"
    pixels = _sky(source)
    preserved = parent / "keep-me"
    preserved.write_bytes(b"another file in the parent")
    image = tools.image()
    try:
        assert image.open(str(source))
        with pytest.raises(RuntimeError, match="destination contains the source image"):
            image.rename(str(parent), overwrite=True)
        assert not image.isopen()
        assert preserved.read_bytes() == b"another file in the parent"
        assert image.open(str(source))
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_rename_keeps_only_the_peer_stream_that_was_already_read(tmp_path, backend):
    source = tmp_path / "source.mir"
    destination = tmp_path / "destination.mir"
    pixels = _sky(source)
    writer, cold_peer, warm_peer = [_backend(backend).image() for _ in range(3)]
    try:
        for image in (writer, cold_peer, warm_peer):
            assert image.open(str(source))
        assert warm_peer.getchunk(blc=[2, 1], trc=[2, 1]).item() == 12.
        assert writer.rename(str(destination))
        assert cold_peer.isopen() and cold_peer.name() == str(source)
        with pytest.raises(RuntimeError, match="could not be opened"):
            cold_peer.getchunk(blc=[2, 1], trc=[2, 1])
        assert warm_peer.name() == str(source)
        np.testing.assert_array_equal(warm_peer.getchunk(), pixels)
        np.testing.assert_array_equal(writer.getchunk(), pixels)
    finally:
        for image in (writer, cold_peer, warm_peer):
            image.done()


def test_miriad_rename_and_read_do_not_follow_replaced_source(tmp_path):
    source = tmp_path / "source.mir"
    elsewhere = tmp_path / "elsewhere.mir"
    destination = tmp_path / "destination.mir"
    pixels = _sky(source)
    reader, writer = tools.image(), tools.image()
    try:
        assert writer.open(str(source))
        assert reader.open(str(source))
        source.rename(elsewhere)
        _sky(source)
        marker = source / "replacement"
        marker.write_text("new directory", encoding="ascii")
        with (source / "image").open("r+b") as replacement:
            replacement.seek(4 + 4 * (pixels.shape[0] + 2))
            replacement.write(struct.pack(">f", 998.))
        with pytest.raises(RuntimeError, match="MIRIAD pixel file .* has changed"):
            reader.getchunk(blc=[2, 1], trc=[2, 1])
        with pytest.raises(RuntimeError, match="image at that path has changed"):
            writer.rename(str(destination))
        assert not writer.isopen() and not destination.exists()
        assert marker.read_text(encoding="ascii") == "new directory"
        assert writer.open(str(elsewhere))
        np.testing.assert_array_equal(writer.getchunk(), pixels)
        assert writer.open(str(source))
        assert writer.getchunk(blc=[2, 1], trc=[2, 1]).item() == 998.
    finally:
        reader.done()
        writer.done()


@pytest.mark.parametrize("inside_parent", ["source", "destination"])
def test_miriad_rename_preserves_child_entries_of_an_open_table(tmp_path, inside_parent):
    parent = tmp_path / "parent.im"
    source = (parent if inside_parent == "source" else tmp_path) / "source.mir"
    destination = (parent if inside_parent == "destination" else tmp_path) / "destination.mir"
    referent = tmp_path / "referent.mir"
    writer, mover, guard = tools.image(), tools.image(), tools.table()
    try:
        assert writer.fromshape(str(parent), [3, 4])
        assert writer.done()
        pixels = _sky(source)
        _sky(referent)
        destination.symlink_to(referent, target_is_directory=True)
        assert guard.open(str(parent))
        guard.unlock()
        assert mover.open(str(source))
        with pytest.raises(RuntimeError, match="table in either path is open"):
            mover.rename(str(destination), overwrite=True)
        assert destination.is_symlink() and destination.readlink() == referent
        assert mover.open(str(source))
        np.testing.assert_array_equal(mover.getchunk(), pixels)
    finally:
        guard.done()
        mover.done()
        writer.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_miscinfo_only_records_the_call_in_history(tmp_path, backend):
    source = tmp_path / "source.mir"
    fits = tmp_path / "export.fits"
    _sky(source)
    image = _backend(backend).image()
    exported = _backend(backend).image()
    try:
        assert image.open(str(source))
        assert image.setmiscinfo({"marker": "in memory only", "run": 17})
        assert image.miscinfo() == {}
        assert image.history() == ["Ran ia.setmiscinfo", "ia.setmiscinfo(info={...})"]
        assert image.tofits(str(fits))
        assert exported.open(str(fits))
        assert not (exported.miscinfo().keys() & {"marker", "run"})
    finally:
        exported.done()
        image.done()


@pytest.mark.parametrize("writer_backend", ["casa", "casars"])
@pytest.mark.parametrize("reader_backend", ["casa", "casars"])
@pytest.mark.parametrize("cropped", [False, True])
def test_miriad_fits_export_round_trip(tmp_path, writer_backend, reader_backend, cropped):
    source = tmp_path / "source.mir"
    destination = tmp_path / "export.fits"
    pixels = _sky(source)
    region = {}
    origin = (0, 0)
    if cropped:
        region_tool = _backend("casa").regionmanager()
        try:
            region = {"region": region_tool.box(blc=[4, 1], trc=[11, 6])}
        finally:
            region_tool.done()
        origin = (4, 1)
        pixels = pixels[4:12, 1:7]

    writer = _backend(writer_backend).image()
    reader = _backend(reader_backend).image()
    try:
        assert writer.open(str(source))
        assert writer.tofits(str(destination), **region)
        assert reader.open(str(destination))
        assert reader.brightnessunit() == "Jy/pixel"
        np.testing.assert_array_equal(reader.getchunk(), pixels)
        coordinate = reader.coordsys()
        try:
            assert coordinate.setunits(["rad", "rad"])
            np.testing.assert_allclose(
                coordinate.toworld([1, 1])["numeric"],
                _sin_pixel(origin[0] + 1, origin[1] + 1),
                rtol=0,
                atol=1e-10,
            )
        finally:
            coordinate.done()
    finally:
        reader.done()
        writer.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_north_celestial_pole_projection(tmp_path, backend):
    path = tmp_path / "ncp.mir"
    _sky(path)
    header = path / "header"
    data = header.read_bytes()
    header.write_bytes(
        data.replace(_text("RA---SIN"), _text("RA---NCP"))
            .replace(_text("DEC--SIN"), _text("DEC--NCP"))
    )
    tool = _backend(backend).image()
    try:
        assert tool.open(str(path))
        coords = tool.coordsys()
        try:
            assert coords.setunits(["rad", "rad"])
            projection = coords.projection()
            assert projection["type"] == "SIN"
            np.testing.assert_allclose(
                projection["parameters"], [0., 1. / math.tan(.25)], rtol=0, atol=1e-13
            )
            for x, y in ([1, 1], [40, 8], [126, 15]):
                m, l = (y - 1) * .0001, -(x - 1) * .0001
                denominator = math.cos(.25) - m * math.sin(.25)
                right_ascension = 1. + math.atan2(l, denominator)
                declination = math.acos(math.hypot(l, denominator))
                np.testing.assert_allclose(
                    coords.toworld([x, y])["numeric"], [right_ascension, declination],
                    rtol=0, atol=1e-10,
                )
        finally:
            coords.done()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize(("qualifier", "frame"), [
    ("", "BARY"), ("-HEL", "BARY"), ("-OBS", "TOPO"), ("-LSD", "LSRD"),
    ("-GEO", "GEO"), ("-SOU", "REST"), ("-GAL", "GALACTO"), ("-LSRK", "LSRK"),
    ("-MYS", "TOPO"),
])
def test_miriad_legacy_spectral_frame_qualifiers(tmp_path, backend, qualifier, frame):
    path = tmp_path / "frequency.mir"
    _sky(path, cube=f"FREQ{qualifier}")
    tool = _backend(backend).image()
    try:
        assert tool.open(str(path))
        coords = tool.coordsys()
        try:
            assert coords.referencecode("spectral")[0] == frame
            np.testing.assert_allclose(coords.toworld([1, 1, 2, 0])["numeric"][2], 1.416e9)
        finally:
            coords.done()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize(("longitude", "latitude", "frame"), [
    ("GLON-SIN", "GLAT-SIN", "GALACTIC"),
    ("ELON-SIN", "ELAT-SIN", "J2000"),
    ("SLON-SIN", "SLAT-SIN", "J2000"),
    ("LL---SIN", "MM---SIN", "J2000"),
    ("RA---GLS", "DEC--GLS", "J2000"),
])
def test_miriad_direction_names_and_projection_alias(tmp_path, backend, longitude, latitude, frame):
    path = tmp_path / "direction.mir"
    _sky(path)
    header = path / "header"
    header.write_bytes(
        header.read_bytes().replace(_text("RA---SIN"), _text(longitude))
        .replace(_text("DEC--SIN"), _text(latitude))
    )
    projection = "SFL" if longitude.endswith("GLS") else "SIN"
    if projection == "SFL":
        _replace_cached_number(path, "crval2", .25, 0.)
    image = _backend(backend).image()
    try:
        assert image.open(str(path))
        coordinates = image.coordsys()
        try:
            assert coordinates.referencecode("direction")[0] == frame
            assert coordinates.projection()["type"] == projection
            assert coordinates.setunits(["rad", "rad"])
            pixel = [40, 1] if projection == "SFL" else [40, 8]
            expected = [1. - 39 * .0001, 0.] if projection == "SFL" else _sin_pixel(*pixel)
            np.testing.assert_allclose(coordinates.toworld(pixel)["numeric"], expected, rtol=0, atol=1e-10)
        finally:
            coordinates.done()
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize(("case", "expected_axes"), [
    ("linear", []),
    ("unprojected", []),
    ("extra-linear", ["Direction", "Direction", "Spectral"]),
    ("duplicate-stokes", ["Direction", "Direction", "Spectral"]),
    ("multiple-stokes", ["Direction", "Direction", "Stokes"]),
    ("fits-only-spectral", ["Direction", "Direction", "Stokes"]),
    ("lowercase-stokes", ["Direction", "Direction", "Spectral"]),
])
def test_miriad_retains_shape_when_header_has_fewer_decoded_axes(tmp_path, backend, case, expected_axes):
    source = tmp_path / "incomplete-axes.mir"
    cube = "FREQ-LSR" if expected_axes else None
    pixels = _sky(source, cube=cube)
    header = source / "header"
    replacements = {
        "linear": [("RA---SIN", "LINEAR11"), ("DEC--SIN", "LINEAR22")],
        "unprojected": [("RA---SIN", "RA---   "), ("DEC--SIN", "DEC--   ")],
        "extra-linear": [("STOKES", "LINPAR")],
        "multiple-stokes": [("FREQ-LSR", "STOKES  ")],
        "fits-only-spectral": [("FREQ-LSR", "VRAD    ")],
        "lowercase-stokes": [("STOKES", "stokes")],
    }
    if case == "duplicate-stokes":
        _replace_cached_number(source, "crval4", 1., 5.)
    else:
        contents = header.read_bytes()
        for matched, replacement in replacements[case]:
            contents = contents.replace(_text(matched), _text(replacement))
        header.write_bytes(contents)
    image = _backend(backend).image()
    output = tmp_path / "unpublished.im"
    try:
        assert image.open(str(source))
        np.testing.assert_array_equal(image.shape(), pixels.shape)
        coords = image.coordsys()
        try:
            assert coords.axiscoordinatetypes() == expected_axes
            assert coords.naxes() == len(expected_axes)
        finally:
            coords.done()
        with pytest.raises(RuntimeError, match="referencePixel and newLatticeShape"):
            image.getchunk()
        with pytest.raises(RuntimeError, match="Failed AlwaysAssert setCoordinateInfo"):
            image.fromimage(outfile=str(output), infile=str(source))
        assert output.exists() == (backend == "casa")
        if case == "lowercase-stokes":
            second_output = tmp_path / "unpublished-new.im"
            for method, name in [
                ("fromimage", ""), ("newimagefromimage", ""),
                ("newimagefromimage", str(second_output)),
            ]:
                with pytest.raises(RuntimeError, match="Failed AlwaysAssert setCoordinateInfo"):
                    getattr(image, method)(outfile=name, infile=str(source))
            assert second_output.exists() == (backend == "casa")
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_miriad_omitted_axis_keeps_the_casacore_coordinate_order(tmp_path, backend):
    path = tmp_path / "multiple-stokes.mir"
    path.mkdir()
    shape = (8, 6, 2, 2, 5)
    items = {"naxis": _integer(len(shape)), **{
        f"naxis{axis}": _integer(length) for axis, length in enumerate(shape, 1)
    }}
    axes = [
        ("RA---SIN", 1., -.0001), ("DEC--SIN", .25, .0001),
        ("STOKES", 1., 1.), ("STOKES", 1., 1.), ("FREQ-LSR", 1.412, .002),
    ]
    for axis, (kind, reference, increment) in enumerate(axes, 1):
        items[f"ctype{axis}"] = _text(kind)
        items[f"crval{axis}"] = _real(reference)
        items[f"crpix{axis}"] = _real(1.)
        items[f"cdelt{axis}"] = _real(increment)
    _write_cache(path / "header", items)
    (path / "image").write_bytes(struct.pack(">I", 4) + bytes(4 * math.prod(shape)))
    image = _backend(backend).image()
    try:
        assert image.open(str(path))
        np.testing.assert_array_equal(image.shape(), shape)
        coordinates = image.coordsys()
        try:
            assert coordinates.axiscoordinatetypes() == ["Direction", "Direction", "Spectral", "Stokes"]
            np.testing.assert_allclose(coordinates.referencevalue()["numeric"], [1., .25, 1.412e9, 1.])
            assert coordinates.stokes() == ["I", "Q"]
        finally:
            coordinates.done()
    finally:
        image.done()
