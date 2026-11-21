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
            assert f'region="{text}"' in product.history()[-1]
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
            history = '""' if isinstance(argument, str) else "[true,false]"
            assert f"region={history}" in product.history()[-1]
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
@pytest.mark.parametrize("case", ["linear", "extra_statement", "bare_pixels", "extra_token", "headerless"])
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
        "extra_token": (
            "box[[4pix,5pix],[10pix,12pix]], garbage",
            "Illegal extra characters on line",
        ),
        "headerless": (
            "box[[4pix,5pix],[10pix,12pix]]", "nor a valid region text file",
        ),
    }[case]
    if case == "headerless":
        path = tmp_path / "region.crtf"
        path.write_text(text + "\n")
        text = str(path)
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


@pytest.mark.parametrize("pixel_type", ["c", "d", "cd"])
@pytest.mark.parametrize("region", ["", "box[[1pix,1pix],[4pix,4pix]]"])
def test_new_copy_string_region_opens_only_float_sources(casa, tmp_path, pixel_type, region):
    source = str(tmp_path / "source.im")
    fixture = casa.image()
    try:
        assert fixture.fromshape(source, [7, 9], type=pixel_type)
    finally:
        fixture.done()
    for api in (casa, tools):
        active = api.image()
        product = None
        try:
            assert active.fromshape(shape=[7, 9])
            destination = tmp_path / f"{api.__name__}-rejected.im"
            with pytest.raises(RuntimeError, match="has a different data type"):
                active.newimagefromimage(infile=source, outfile=str(destination), region=region)
            np.testing.assert_array_equal(active.shape(), [7, 9])
            assert not destination.exists()

            product = active.newimagefromimage(infile=source, region={})
            assert product.pixeltype() != "float"
            np.testing.assert_array_equal(product.shape(), [7, 9])
            assert active.fromimage(infile=source, region=region)
            assert active.pixeltype() == product.pixeltype()
            expected_shape = [4, 4] if region else [7, 9]
            np.testing.assert_array_equal(active.shape(), expected_shape)
        finally:
            if product is not None:
                product.done()
            active.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("planes", ["outside", "no_axes"])
def test_copy_crtf_tests_qualifiers_in_the_definition_lattice(casa, tmp_path, method, planes):
    source = str(tmp_path / "source.im")
    channels = 8 if method == "fromimage" else 2
    shape = (20, 20, 1, channels) if planes == "outside" else (20, 20)
    pixels = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    fixture = casa.image()
    coordinates = None
    try:
        assert fixture.fromarray(source, pixels)
        coordinates = fixture.coordsys()
        definition = coordinates.torecord()
    finally:
        if coordinates is not None:
            coordinates.done()
        fixture.done()

    if planes == "outside":
        attachment_shape = [20, 20, 1, 2]
        region = (
            "box[[1pix,1pix],[3pix,3pix]], range=[4pix,5pix]\n"
            "box[[6pix,6pix],[7pix,7pix]], range=[0pix,1pix]"
        )
        expected = pixels[6:8, 6:8, :, :2]
    else:
        attachment_shape = list(shape)
        region = "box[[1pix,1pix],[3pix,3pix]], range=[1pix,2pix], corr=[I]"
        expected = pixels[1:4, 1:4]

    for api in (casa, tools):
        active = api.image()
        product = None
        try:
            assert active.fromshape(shape=attachment_shape, csys=definition)
            result = getattr(active, method)(infile=source, region=region)
            product = active if result is True else result
            np.testing.assert_array_equal(product.getchunk(), expected)
            np.testing.assert_array_equal(product.getchunk(getmask=True), True)
        finally:
            if product is not None and product is not active:
                product.done()
            active.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
def test_copy_checks_the_lattice_shape_of_named_pixel_regions(casa, tmp_path, method):
    source = str(tmp_path / "source.im")
    attachment = str(tmp_path / "attached.im")
    fixture = casa.image()
    manager = casa.regionmanager()
    region = {
        "name": "LCBox", "isRegion": 1, "comment": "", "oneRel": True,
        "blc": np.array([2, 3], dtype=np.float32),
        "trc": np.array([5, 7], dtype=np.float32), "shape": np.array([10, 10], dtype=np.int32),
    }
    try:
        assert fixture.fromshape(source, [20, 20])
        assert fixture.done()
        assert fixture.fromshape(attachment, [10, 10])
        assert fixture.done()
        assert manager.fromrecordtotable(attachment, "saved_box", region) == "saved_box"
    finally:
        fixture.done()
        manager.done()

    for api in (casa, tools):
        active = api.image()
        destination = tmp_path / f"{api.__name__}-rejected.im"
        try:
            assert active.open(attachment)
            for argument in ("saved_box", region):
                with pytest.raises(RuntimeError, match="mismatches lattice shape in region"):
                    getattr(active, method)(
                        infile=source, outfile=str(destination), region=argument,
                    )
                np.testing.assert_array_equal(active.shape(), [10, 10])
                assert not destination.exists()
        finally:
            active.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("unit, radii", [("arcsec", (4, 4)), ("pix", (2, 8))])
def test_copy_crtf_circles_on_unequal_direction_increments(casa, tmp_path, method, unit, radii):
    pixels = np.arange(31 * 33, dtype=np.float32).reshape(31, 33)
    source = str(tmp_path / "source.im")
    fixture = casa.image()
    coordinates = None
    try:
        assert fixture.fromshape(shape=list(pixels.shape))
        coordinates = fixture.coordsys()
        assert coordinates.setincrement([-1 / 60, 2 / 60])
        record = coordinates.torecord()
        assert fixture.fromarray(source, pixels, record)
    finally:
        if coordinates is not None:
            coordinates.done()
        fixture.done()

    rx, ry = radii
    x, y = np.ogrid[-rx:rx + 1, -ry:ry + 1]
    expected = pixels[15 - rx:16 + rx, 16 - ry:17 + ry]
    mask = (x / rx) ** 2 + (y / ry) ** 2 <= 1
    for api in (casa, tools):
        active = api.image()
        product = None
        try:
            assert active.fromshape(shape=list(pixels.shape), csys=record)
            result = getattr(active, method)(infile=source, region=f"circle[[15pix,16pix],4{unit}]")
            product = active if result is True else result
            np.testing.assert_array_equal(product.getchunk(), expected)
            np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
        finally:
            if product is not None and product is not active:
                product.done()
            active.done()
