"""Public profile aggregation and axes against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _data(complex_image=False):
    data = np.arange(4 * 5 * 7, dtype=np.float32).reshape(4, 5, 1, 7)
    if complex_image:
        data = (data + 1j * (data + 1)).astype(np.complex64)
    mask = np.ones(data.shape, dtype=bool)
    mask[..., 2] = False
    mask[2, 3, :, 3] = False
    return data, mask


def _attached(api, data, mask=None, *, csys=None):
    image = api.image()
    assert image.fromarray(pixels=data, **({} if csys is None else {"csys": csys}))
    if mask is not None:
        assert image.putregion(pixelmask=mask)
    assert image.setbrightnessunit("K")
    return image


def _compare(native, pinned):
    assert native.keys() == pinned.keys()
    assert native["xUnit"] == pinned["xUnit"]
    assert native["yUnit"] == pinned["yUnit"]
    for field in ("mask", "npix", "planes", "coords", "values"):
        assert native[field].dtype == pinned[field].dtype
        assert native[field].shape == pinned[field].shape
        if field in {"mask", "npix", "planes"}:
            np.testing.assert_array_equal(native[field], pinned[field])
        else:
            np.testing.assert_allclose(native[field], pinned[field], rtol=2e-7, atol=2e-7)


@pytest.mark.parametrize("complex_image", [False, True])
@pytest.mark.parametrize("function", ["mean", "sum", "npts", "zero", "max", "med"])
def test_image_reductions_share_values_masks_and_counts(casa, function, complex_image):
    data, mask = _data(complex_image)
    records = []
    for api in (casa, tools):
        image = _attached(api, data, mask)
        try:
            records.append(image.getprofile(axis=3, function=function, unit="pixels"))
        finally:
            image.done()
    native, pinned = records[1], records[0]
    _compare(native, pinned)
    count = mask.sum(axis=(0, 1, 2))
    np.testing.assert_array_equal(native["npix"], count)
    np.testing.assert_array_equal(native["coords"], np.arange(data.shape[3]))
    if function == "mean":
        expected = np.where(mask, data, 0.).sum(axis=(0, 1, 2)) / np.maximum(count, 1)
        np.testing.assert_allclose(native["values"], expected, rtol=1e-7)
    elif function == "npts":
        np.testing.assert_array_equal(native["values"], count)
    elif function == "zero":
        assert native["mask"].all()
        assert not native["values"].any()


@pytest.mark.parametrize("unit,kind,rest", [
    ("", "default", ""), ("GHz", "default", ""), ("km/s", "rad", "1.42GHz"),
    ("m/s", "optical velocity", ""), ("km/s", "beta", ""),
    ("cm", "wave", ""), ("cm", "air", ""),
])
def test_spectral_units_and_doppler_conventions(casa, unit, kind, rest):
    data, mask = _data()
    out = []
    for api in (casa, tools):
        image = _attached(api, data, mask)
        try:
            out.append(image.getprofile(3, "mean", unit=unit, spectype=kind, restfreq=rest))
        finally:
            image.done()
    _compare(out[1], out[0])
    frequency = 1.415e9 + (np.arange(7.) - 3.) * 1e3
    if kind == "rad":
        np.testing.assert_allclose(out[1]["coords"], 299792.458*(1-frequency/1.42e9), atol=2e-6)
    elif kind == "wave":
        np.testing.assert_allclose(out[1]["coords"], 29979245800.0/frequency, atol=1e-10)


def test_selected_profile_and_logfile(casa, tmp_path):
    data, mask = _data()
    regions = casa.regionmanager()
    region = regions.box(blc=[1, 1, 0, 1], trc=[3, 4, 0, 5])
    regions.done()
    expected = np.array([88.5, 0., 90.181816, 91.5, 92.5])
    outputs = []
    texts = []
    for api in (casa, tools):
        image = _attached(api, data, mask)
        log = tmp_path / f"{api.__name__}.txt"
        log.write_text("prior content\n")
        try:
            outputs.append(image.getprofile(axis=3, region=region, unit="Pix", logfile=str(log)))
            texts.append(log.read_text().splitlines())
        finally:
            image.done()
    _compare(outputs[1], outputs[0])
    np.testing.assert_allclose(outputs[1]["values"], expected, atol=3e-6)
    np.testing.assert_array_equal(outputs[1]["planes"], [1., 2., 3., 4., 5.])
    np.testing.assert_array_equal(outputs[1]["mask"], [True, False, True, True, True])
    assert texts[1] == texts[0]
    assert len(texts[1]) == 9


def test_spectral_frame_changes_on_selected_original_planes(casa):
    data, mask = _data()
    regions = casa.regionmanager()
    region = regions.box(blc=[1,1,0,1], trc=[3,4,0,5])
    regions.done()
    original = _attached(casa, data, mask)
    native = _attached(tools, data, mask)
    try:
        full = original.getprofile(3, frame="BARY", unit="Hz")
        selected = original.getprofile(3, frame="BARY", unit="Hz", region=region)
        result = native.getprofile(3, frame="BARY", unit="Hz", region=region)
        np.testing.assert_allclose(result["coords"], full["coords"][1:6], rtol=0, atol=.001)
        # The pinned conversion walks the original coordinate system from zero.
        np.testing.assert_allclose(selected["coords"], full["coords"][:5], atol=.001)
        assert abs(result["coords"][0] - selected["coords"][0]) > 100.
    finally:
        native.done()
        original.done()


def test_default_and_invalid_profile_requests(casa):
    data, mask = _data()
    for api in (casa, tools):
        image = api.image()
        with pytest.raises(RuntimeError, match="No image attached"):
            image.getprofile(3)
        assert image.fromarray(pixels=data)
        assert image.putregion(pixelmask=mask)
        try:
            with pytest.raises(RuntimeError, match="Axis must be greater than 0"):
                image.getprofile()
            with pytest.raises(RuntimeError, match="less than"):
                image.getprofile(4)
            with pytest.raises(RuntimeError, match="Ambiguous"):
                image.getprofile(3, unit="pixel", spectype="r")
            with pytest.raises(RuntimeError, match="conform"):
                image.getprofile(0, unit="m")
            np.testing.assert_array_equal(image.getchunk(), data)
        finally:
            image.done()


def test_writable_report_destination_does_not_alias_input(tmp_path):
    image = tools.image()
    data, mask = _data()
    path = tmp_path / "science.im"
    assert image.fromarray(str(path), data)
    try:
        with pytest.raises(RuntimeError):
            image.getprofile(3, logfile=str(path / "metadata-report"))
        assert not (path / "metadata-report").exists()
        np.testing.assert_array_equal(image.getchunk(), data)
    finally:
        image.done()


def test_retained_direction_or_stokes_axis(casa):
    data, mask = _data()
    reference = _attached(casa, data, mask)
    native = _attached(tools, data, mask)
    try:
        _compare(native.getprofile(axis=0), reference.getprofile(axis=0))
        _compare(native.getprofile(axis=2), reference.getprofile(axis=2))
        with pytest.raises(RuntimeError, match="newWorldOrder"):
            reference.getprofile(axis=1, unit="deg")
        latitude = native.getprofile(axis=1, unit="deg")
        assert latitude["xUnit"] == "deg"
        expected = np.rad2deg(np.arcsin(np.deg2rad((np.arange(5.)-2.)/60)))
        np.testing.assert_allclose(latitude["coords"], expected, atol=2e-7)
        np.testing.assert_array_equal(latitude["npix"], mask.sum(axis=(0, 2, 3)))
    finally:
        native.done()
        reference.done()


def test_casa_native_source_writer_reader_matrix(casa, tmp_path):
    base, mask = _data()
    for writer in (casa, tools):
        product = tmp_path / f"{writer.__name__}.im"
        image = writer.image()
        assert image.fromarray(str(product), base)
        assert image.putregion(pixelmask=mask)
        assert image.setbrightnessunit("K")
        assert image.done()
        for reader in (casa, tools):
            consumer = reader.image()
            assert consumer.open(str(product))
            try:
                measured = consumer.getprofile(axis=3, function="sum", unit="GHz")
                np.testing.assert_array_equal(measured["values"], np.where(mask, base, 0).sum(axis=(0,1,2)))
                np.testing.assert_array_equal(measured["npix"], mask.sum(axis=(0,1,2)))
                np.testing.assert_array_equal(measured["mask"], mask.any(axis=(0,1,2)))
                np.testing.assert_allclose(measured["coords"], 1.415 + (np.arange(7.)-3)/1e6)
                assert measured["yUnit"] == "K"
            finally:
                consumer.done()
