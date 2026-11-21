"""Pixel and axis permutations through the public image tool."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def data():
    x, y, z = np.indices((4, 6, 5))
    return (x**2 + 10 * y + 100 * z).astype(np.float32)


def source_image(api, path="", linear=False):
    image = api.image()
    assert image.fromarray(path, data(), linear=linear)
    mask = np.ones(data().shape, dtype=bool)
    mask[1, 3, 4] = False
    assert image.putregion(pixelmask=mask)
    assert image.setbrightnessunit("Jy")
    return image, mask


@pytest.mark.parametrize(
    "order, axes, flip",
    [
        (201, (2, 0, 1), None),
        (b"201", (2, 0, 1), None),
        ("021", (0, 2, 1), None),
        (21, (0, 2, 1), None),
        (["Frequency", "Right", "Decl"], (2, 0, 1), None),
        (np.array(["Frequency", "Right", "Decl"]), (2, 0, 1), None),
        (["fr", "-decl", "R"], (2, 1, 0), 1),
        ("2-01", (2, 0, 1), 0),
        ("-201", (2, 0, 1), 2),
    ],
)
def test_permutation_and_single_axis_reflection(casa, order, axes, flip):
    point = np.array([1.0, 2.0, 3.0])
    reflected = point.copy()
    for api in (tools, casa):
        src, mask = source_image(api)
        try:
            if flip is None:
                pixels = data()
            else:
                pixels = np.flip(data(), axis=flip)
                mask = np.flip(mask, axis=flip)
                reflected[flip] = pixels.shape[flip] - 1 - point[flip]
            output = src.transpose(order=order)
            try:
                np.testing.assert_array_equal(output.getchunk(), pixels.transpose(axes))
                np.testing.assert_array_equal(output.getchunk(getmask=True), mask.transpose(axes))
                assert output.maskhandler("get") == ["mask0"]
                assert output.brightnessunit() == "Jy"
                assert output.history()[-2] == "Ran ia.transpose"
                in_coords = src.coordsys()
                out_coords = output.coordsys()
                assert out_coords.names() == [in_coords.names()[axis] for axis in axes]
                np.testing.assert_allclose(
                    out_coords.toworld(reflected[list(axes)])["numeric"],
                    in_coords.toworld(point)["numeric"][list(axes)], rtol=0, atol=2e-8,
                )
            finally:
                output.done()
        finally:
            src.done()


def test_all_native_writer_reader_pairs_keep_masks_and_history(casa, tmp_path):
    for writer in (tools, casa):
        src, mask = source_image(writer, str(tmp_path / f"{writer.__name__}-input.im"))
        path = str(tmp_path / f"{writer.__name__}-output.im")
        try:
            assert src.sethistory(origin="test", history=["input line"])
            out = src.transpose(path, np.int32(201))
            try:
                assert src.done()
                np.testing.assert_array_equal(out.getchunk(), data().transpose(2, 0, 1))
            finally:
                out.done()
        finally:
            src.done()
        for reader in (tools, casa):
            opened = reader.image()
            assert opened.open(path)
            try:
                np.testing.assert_array_equal(opened.getchunk(), data().transpose(2, 0, 1))
                np.testing.assert_array_equal(opened.getchunk(getmask=True), mask.transpose(2, 0, 1))
                assert opened.brightnessunit() == "Jy"
                assert "input line" in opened.history()
                assert opened.history()[-2] == "Ran ia.transpose"
            finally:
                opened.done()


def test_coupled_direction_reflection_preserves_each_sample_world():
    image, mask = source_image(tools)
    try:
        coords = image.coordsys()
        assert coords.setlineartransform("direction", [[1.0, 0.2], [-0.3, 1.0]])
        assert image.setcoordsys(coords.torecord())
        output = image.transpose(order="2-01")
        try:
            np.testing.assert_array_equal(output.getchunk(), data()[::-1].transpose(2, 0, 1))
            np.testing.assert_array_equal(output.getchunk(getmask=True), mask[::-1].transpose(2, 0, 1))
            for x, y, z in ([0., 0., 0.], [1., 2., 3.], [3., 5., 4.]):
                old = image.coordsys().toworld([x, y, z])["numeric"]
                new = output.coordsys().toworld([z, 3 - x, y])["numeric"]
                np.testing.assert_allclose(new, old[[2, 0, 1]], rtol=0, atol=1e-8)
        finally:
            output.done()
    finally:
        image.done()


def test_reflected_spectral_and_stokes_planes_keep_their_labels_and_beams():
    image = tools.image()
    shape = (4, 5, 2, 6)
    assert image.fromshape("", list(shape))
    pixels = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    assert image.putchunk(pixels)
    try:
        for channel in range(shape[3]):
            for pol in range(shape[2]):
                major = 2.0 + channel + 0.1 * pol
                assert image.setrestoringbeam(
                    major=f"{major}arcsec", minor="1arcsec", pa="0deg",
                    channel=channel, polarization=pol,
                )
        before = image.coordsys()
        output = image.transpose(order="-30-21")
        try:
            np.testing.assert_array_equal(output.getchunk(), pixels[:, :, ::-1, ::-1].transpose(3, 0, 2, 1))
            after = output.coordsys()
            assert list(after.stokes()) == list(before.stokes())[::-1]
            for channel in (0, 5):
                for pol in range(2):
                    beam = output.restoringbeam(channel=channel, polarization=pol)
                    assert beam["major"]["value"] == pytest.approx(2.0 + (5 - channel) + 0.1 * (1 - pol))
                    # The world coordinate follows the selected source plane
                    # together with its restoring beam.
                    want = before.toworld([0, 0, 1 - pol, 5 - channel])["numeric"]
                    actual = after.toworld([channel, 0, pol, 0])["numeric"]
                    np.testing.assert_allclose(actual, want[[3, 0, 2, 1]], rtol=0, atol=1e-8)
        finally:
            output.done()
    finally:
        image.done()


@pytest.mark.parametrize("kind", ["tabular", "spectral"])
def test_reflection_preserves_the_sampled_axis_world_values(kind):
    cs = tools.coordsys().newcoordsys(linear=1, tabular=kind == "tabular", spectral=kind == "spectral")
    if kind == "tabular":
        assert cs.settabular(pixel=[0.0, 1.0, 3.0, 5.0], world=[100.0, 101.0, 111.0, 120.0])
        expected = np.array([100.0, 101.0, 106.0, 111.0, 115.5, 120.0])
    else:
        expected = np.array([1.415e9, 1.415001e9, 1.415004e9, 1.415009e9, 1.415016e9, 1.415025e9])
        assert cs.setspectral(frequencies={"value": expected, "unit": "Hz"})
    axis = cs.findcoordinate(kind)["pixel"][0]
    shape = [4, 4]
    shape[axis] = expected.size
    image = tools.image()
    assert image.fromshape("", shape, cs.torecord())
    try:
        points = np.zeros((2, expected.size))
        points[axis] = np.arange(expected.size)
        at = image.coordsys().toworldmany(points)["numeric"][axis]
        np.testing.assert_allclose(at, expected, rtol=0, atol=1e-6)
        output = image.transpose(order="-01" if axis == 0 else "0-1")
        try:
            kinds = output.coordsys().axiscoordinatetypes()
            assert kinds[axis] == kind.title()
            reflected = output.coordsys().toworldmany(points)["numeric"][axis]
            np.testing.assert_allclose(reflected, expected[::-1], rtol=0, atol=1e-6)
        finally:
            output.done()
    finally:
        image.done()


def test_invalid_orders_and_outfile_collision_preserve_inputs(casa, tmp_path):
    for api in (tools, casa):
        with pytest.raises(RuntimeError, match="No image specified to transpose"):
            api.image().transpose(order=201)
        source, mask = source_image(api)
        path = str(tmp_path / f"{api.__name__}.im")
        try:
            for order, message in [
                (None, "Unsupported type"),
                ([2, 0, 1], "Unsupported type"),
                (201.0, "Unsupported type"),
                ("20", "multiple times"),
                ("2011", "Number of axes to reorder"),
                (["Frequency", "RA", "Declination"], "No axis matches requested axis ra"),
            ]:
                with pytest.raises(RuntimeError, match=message):
                    source.transpose(order=order)
            out = source.transpose(path, 201)
            out.done()
            with pytest.raises(RuntimeError, match="exists"):
                source.transpose(path, "012")
            assert source.shape().tolist() == list(data().shape)
            np.testing.assert_array_equal(source.getchunk(getmask=True), mask)
            out = api.image()
            assert out.open(path)
            try:
                np.testing.assert_array_equal(out.getchunk(), data().transpose(2, 0, 1))
            finally:
                out.done()
        finally:
            source.done()
        complex_image = api.image()
        assert complex_image.fromshape("", [4, 6, 5], type="c")
        try:
            with pytest.raises(RuntimeError, match="only supports Float"):
                complex_image.transpose(order=201)
        finally:
            complex_image.done()


def test_reflection_trims_stokes_labels_to_the_physical_planes(casa):
    cs = casa.coordsys().newcoordsys(direction=True, stokes="I,Q,U,V", spectral=True)
    image = tools.image()
    assert image.fromshape("", [4, 5, 2, 6], cs.torecord())
    try:
        product = image.transpose(order="01-23")
        try:
            output = product.coordsys()
            assert list(output.stokes()) == ["Q", "I"]
            assert output.referencevalue()["numeric"][2] == 2.0
            saved = output.torecord()
            assert saved["stokes1"]["crval"][0] == 2.0
            assert saved["worldreplace1"][0] == 2.0
        finally:
            product.done()
    finally:
        image.done()


def test_named_reflection_uses_the_physical_axis_on_independent_maps(casa):
    cs = casa.coordsys().newcoordsys(stokes="I,Q", spectral=True)
    assert cs.setreferencepixel([0.0, 0.0])
    assert cs.setreferencevalue([1.0, 1.4e9])
    assert cs.setincrement([1.0, 2e6])
    record = cs.torecord()
    record["worldmap0"] = np.array([1], dtype=np.int32)
    record["worldmap1"] = np.array([0], dtype=np.int32)
    pixels = np.array([[1, 2, 3], [10, 20, 30]], dtype=np.float32)
    image = tools.image()
    assert image.fromarray("", pixels, csys=record)
    try:
        output = image.transpose(order=["-Frequency", "Stokes"])
        try:
            np.testing.assert_array_equal(output.getchunk(), [[3, 30], [2, 20], [1, 10]])
            assert output.coordsys().names() == ["Frequency", "Stokes"]
            np.testing.assert_allclose(output.toworld([0, 0])["numeric"], [1.404e9, 1.0], rtol=0, atol=1e-6)
        finally:
            output.done()
    finally:
        image.done()


def test_named_product_preserves_the_live_rebin_coordinate(casa, tmp_path):
    cs = casa.coordsys().newcoordsys(direction=True, spectral=True)
    frequency = np.array([1, 1.1, 1.4, 2.1, 2.2, 2.5, 2.6]) * 1e9
    assert cs.setspectral(frequencies={"value": frequency, "unit": "Hz"})
    region = casa.regionmanager().box([0, 0, 0], [3, 3, 6])
    for api in (tools, casa):
        src = api.image()
        assert src.fromshape("", [4, 4, 7], cs.torecord())
        try:
            rebinned = src.rebin(str(tmp_path / f"{api.__name__}-rebin.im"), [1, 1, 3], region=region)
            path = str(tmp_path / f"{api.__name__}-out.im")
            try:
                output = rebinned.transpose(path, order=201)
                try:
                    assert output.coordsys().referencepixel()["numeric"][0] == pytest.approx(-1 / 3)
                finally:
                    output.done()
                reopened = api.image()
                assert reopened.open(path)
                try:
                    assert reopened.coordsys().referencepixel()["numeric"][0] == pytest.approx(0)
                finally:
                    reopened.done()
            finally:
                rebinned.done()
        finally:
            src.done()


def test_existing_output_is_reported_before_lazy_fits_open(tmp_path):
    src, _ = source_image(tools)
    path = tmp_path / "source.fits"
    try:
        assert src.tofits(outfile=str(path))
    finally:
        src.done()
    assert src.open(str(path))
    existing = tmp_path / "preserved.im"
    existing.write_text("preserve the existing output")
    path.unlink()
    try:
        with pytest.raises(RuntimeError, match="exists"):
            src.transpose(str(existing), 201)
        assert existing.read_text() == "preserve the existing output"
    finally:
        src.done()
