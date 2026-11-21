"""The legacy factories use different coordinate contexts for CRTF text."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("text_source", ["inline", "file"])
def test_copy_text_uses_the_casa_origin_even_when_pixel_types_differ(
    casa, tmp_path, method, text_source,
):
    x, y, _, channel = np.ogrid[:21, :23, :1, :3]
    pixels = (x + 10 * y + 100 * channel).astype(np.float32)
    lower = (2, 2) if method == "fromimage" else (4, 5)
    name = str(tmp_path / "source.im")
    source = casa.image()
    coordinates = None
    try:
        assert source.fromarray(name, pixels)
        coordinates = source.coordsys()
        expected_origin = coordinates.toworld([*lower, 0, 0])["numeric"]
        reference = coordinates.referencepixel()["numeric"]
        assert coordinates.setreferencepixel(reference + [2, 3, 0, 0])
        attachment = coordinates.torecord()
    finally:
        if coordinates is not None:
            coordinates.done()
        source.done()

    text = "box[[4pix,5pix],[10pix,12pix]]"
    if text_source == "file":
        path = tmp_path / "region.crtf"
        path.write_text(f"#CRTFv0\n{text}\n")
        text = str(path)
    expected = pixels[lower[0]:lower[0] + 7, lower[1]:lower[1] + 8]

    for api in (casa, tools):
        destination = str(tmp_path / f"{api.__name__}-copy.im")
        active = api.image()
        product = None
        try:
            assert active.fromshape(shape=list(pixels.shape), csys=attachment, type="c")
            result = getattr(active, method)(
                infile=name, outfile=destination, region=text,
            )
            if method == "fromimage":
                assert result is True
                product = active
            else:
                product = result
                np.testing.assert_array_equal(active.shape(), pixels.shape)
                assert active.pixeltype() == "complex"
            np.testing.assert_array_equal(product.getchunk(), expected)
            np.testing.assert_array_equal(product.getchunk(getmask=True), True)
        finally:
            if product is not None and product is not active:
                product.done()
            active.done()
        for reader_api in (casa, tools):
            reader = reader_api.image()
            coordinates = None
            try:
                assert reader.open(destination)
                coordinates = reader.coordsys()
                np.testing.assert_allclose(
                    coordinates.toworld([0, 0, 0, 0])["numeric"],
                    expected_origin, rtol=1e-10, atol=2e-8,
                )
                np.testing.assert_array_equal(reader.getchunk(), expected)
                np.testing.assert_array_equal(reader.getchunk(getmask=True), True)
            finally:
                if coordinates is not None:
                    coordinates.done()
                reader.done()


@pytest.mark.parametrize("method, error", [
    ("fromimage", "No attached image"),
    ("newimagefromimage", "Logic error"),
])
def test_copy_text_on_detached_tool_rejects_before_creating_an_output(casa, tmp_path, method, error):
    path = str(tmp_path / "source.im")
    source = casa.image()
    try:
        assert source.fromshape(path, [8, 9])
    finally:
        source.done()
    output = tmp_path / "failed.im"
    for api in (casa, tools):
        image = api.image()
        try:
            with pytest.raises(RuntimeError, match=error):
                getattr(image, method)(
                    infile=path, outfile=str(output), region="box[[1pix,1pix],[4pix,4pix]]",
                )
            assert not output.exists()
        finally:
            image.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
def test_copy_named_region_uses_the_attachment_table(casa, tmp_path, method):
    manager = casa.regionmanager()
    fixture = casa.image()
    pixels = np.arange(21 * 23, dtype=np.float32).reshape(21, 23)
    source = str(tmp_path / "source.im")
    attachment = str(tmp_path / "active.im")
    try:
        for name, lower, upper in [
            (source, [4, 5], [10, 12]),
            (attachment, [2, 3], [11, 14]),
        ]:
            assert fixture.fromarray(name, pixels)
            coordinates = fixture.coordsys()
            try:
                saved = manager.wbox(
                    [f"{value}pix" for value in lower],
                    [f"{value}pix" for value in upper],
                    [0, 1], coordinates.torecord(),
                )
            finally:
                coordinates.done()
            assert fixture.done()
            assert manager.fromrecordtotable(name, "selected", saved) == "selected"
    finally:
        fixture.done()
        manager.done()

    for api in (casa, tools):
        active = api.image()
        product = None
        try:
            assert active.open(attachment)
            result = getattr(active, method)(infile=source, region="selected")
            product = active if result is True else result
            np.testing.assert_array_equal(product.getchunk(), pixels[2:12, 3:15])
        finally:
            if product is not None and product is not active:
                product.done()
            active.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("argument", ["", [True, False]])
def test_copy_empty_text_and_boolean_vector_have_casa_attachment_rules(
    casa, tmp_path, method, argument,
):
    source = str(tmp_path / "source.im")
    fixture = casa.image()
    pixels = np.arange(8 * 9, dtype=np.float32).reshape(8, 9)
    try:
        assert fixture.fromarray(source, pixels)
    finally:
        fixture.done()
    for api in (casa, tools):
        active = api.image()
        product = None
        try:
            if method == "fromimage" and argument == "":
                with pytest.raises(RuntimeError, match="No attached image"):
                    active.fromimage(infile=source, region=argument)
                assert active.fromshape(shape=[1])
            result = getattr(active, method)(infile=source, region=argument)
            product = active if result is True else result
            np.testing.assert_array_equal(product.getchunk(), pixels)
        finally:
            if product is not None and product is not active:
                product.done()
            active.done()


def test_fromimage_ignores_crtf_shape_fully_outside_its_attachment(casa, tmp_path):
    source = str(tmp_path / "source.im")
    fixture = casa.image()
    pixels = np.arange(20 * 24, dtype=np.float32).reshape(20, 24)
    try:
        assert fixture.fromarray(source, pixels)
    finally:
        fixture.done()
    for api in (casa, tools):
        active = api.image()
        try:
            assert active.fromshape(shape=[10, 10])
            with pytest.raises(RuntimeError, match="No regions found"):
                active.fromimage(infile=source, region="box[[11pix,11pix],[14pix,15pix]]")
            np.testing.assert_array_equal(active.shape(), [10, 10])
            assert active.fromimage(infile=source, region="box[[5pix,5pix],[14pix,15pix]]")
            np.testing.assert_array_equal(active.getchunk(), pixels[10:20, 12:23])
        finally:
            active.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("leading_difference", [False, True])
def test_copy_crtf_applies_additions_and_subtractions_in_document_order(
    casa, tmp_path, method, leading_difference,
):
    source = str(tmp_path / "source.im")
    shape = (21, 23, 1, 3)
    x, y, _, channel = np.ogrid[:21, :23, :1, :3]
    pixels = (x + 10 * y + 100 * channel).astype(np.float32)
    fixture = casa.image()
    try:
        assert fixture.fromarray(source, pixels)
    finally:
        fixture.done()
    if leading_difference:
        path = tmp_path / "negative.crtf"
        path.write_text(
            "#CRTFv0\n-box[[2pix,3pix],[4pix,5pix]]\n"
            "box[[6pix,7pix],[8pix,9pix]]\n",
        )
        text = str(path)
        expected = pixels[:9, :10]
        mask = np.zeros(expected.shape, dtype=bool)
        mask[0, 0, 0, 0] = True
        mask[6:, 7:] = True
    else:
        text = (
            "box[[4pix,5pix],[10pix,12pix]], range=[1pix,2pix], corr=[I]\n"
            "-box[[4pix,5pix],[6pix,12pix]]\n"
            "box[[5pix,6pix],[5pix,6pix]], range=[1pix,1pix]"
        )
        expected = pixels[4:11, 5:13, :, 1:]
        mask = np.zeros(expected.shape, dtype=bool)
        mask[3:] = True
        mask[1, 1, 0, 0] = True
    for api in (casa, tools):
        active = api.image()
        product = None
        try:
            assert active.fromshape(shape=list(shape))
            result = getattr(active, method)(infile=source, region=text)
            product = active if result is True else result
            np.testing.assert_array_equal(product.getchunk(), expected)
            np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
        finally:
            if product is not None and product is not active:
                product.done()
            active.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("case", ["linear", "extra_statement", "bare_pixels"])
def test_copy_reports_unsupported_crtf_and_preserves_parent(casa, tmp_path, method, case):
    source = str(tmp_path / "source.im")
    fixture = casa.image()
    linear = case == "linear"
    shape = [21, 23, 1, 3]
    text, error = {
        "linear": ("box[[4pix,5pix],[10pix,12pix]]", "has no direction coordinate"),
        "extra_statement": (
            "box[[4pix,5pix],[10pix,12pix]]; box[[18pix,18pix],[20pix,22pix]]",
            "Illegal extra characters on line",
        ),
        "bare_pixels": ("4,5,10,12", "Unable to open region file or region table"),
    }[case]
    try:
        assert fixture.fromshape(source, shape, linear=linear)
    finally:
        fixture.done()
    for api in (casa, tools):
        active = api.image()
        try:
            assert active.fromshape(shape=shape, linear=linear)
            with pytest.raises(RuntimeError, match=error):
                getattr(active, method)(infile=source, region=text)
            np.testing.assert_array_equal(active.shape(), shape)
        finally:
            active.done()
