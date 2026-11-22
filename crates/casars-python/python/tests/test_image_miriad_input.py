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
def test_rename_miriad_can_move_a_read_only_directory(tmp_path, backend):
    source = tmp_path / "read-only.mir"
    target = tmp_path / "renamed.mir"
    pixels = _sky(source)
    source.chmod(0o555)
    image = _backend(backend).image()
    try:
        assert image.open(str(source))
        assert image.rename(str(target))
        assert not source.exists()
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()
        (target if target.exists() else source).chmod(0o755)


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
