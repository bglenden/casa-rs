"""Local statistic images against CASA 6.7.6.14 and analytic neighborhoods."""

import math

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _analytic(values, name):
    ordered = sorted(values.astype(float).ravel())
    q1, q3 = (ordered[math.ceil(len(ordered) * q) - 1] for q in (.25, .75))
    mad = np.median(np.abs(values - np.median(values)))
    return {
        "iqr": q3 - q1,
        "max": max(ordered),
        "mean": np.mean(values),
        "medabsdevmed": mad,
        "median": np.median(ordered),
        "min": min(ordered),
        "npts": len(ordered),
        "q1": q1,
        "q3": q3,
        "rms": np.sqrt(np.mean(values**2)),
        "std": np.std(values, ddof=1),
        "sumsq": np.sum(values**2),
        "sum": np.sum(values),
        "var": np.var(values, ddof=1),
        "xmadm": 1.482602218505602 * mad,
    }[name]


@pytest.mark.parametrize("name", [
    "iqr", "max", "mean", "medabsdevmed", "median", "min", "npts", "q1",
    "q3", "rms", "std", "sumsq", "sum", "var", "xmadm",
])
def test_deviation_classical_statistics(casa, name):
    x, y = np.indices((15, 17))
    values = (x*x + 2*y + x % 3).astype(np.float32)
    for api in (casa, tools):
        image = api.image()
        assert image.fromarray(pixels=values)
        try:
            result = image.deviation(xlength=3, ylength="2arcmin", stattype=name)
            try:
                actual = result.getchunk()
                assert actual.shape == values.shape
                assert actual.dtype == np.float64
                assert result.getchunk(getmask=True).all()
                for i, j in ((0, 0), (6, 7), (9, 8), (14, 16)):
                    sample = values[max(0, i - 1):i + 2, max(0, j - 1):j + 2]
                    np.testing.assert_allclose(actual[i, j], _analytic(sample, name), rtol=2e-6)
            finally:
                result.done()
        finally:
            image.done()


def test_deviation_default_and_single_point_circle(casa):
    values = np.ones((15, 17), dtype=np.float32)
    for api in (casa, tools):
        source = api.image()
        detached = source.deviation()
        assert detached.shape().size == 0
        detached.done()
        assert source.fromarray(pixels=values)
        try:
            output = source.deviation()
            try:
                np.testing.assert_array_equal(output.getchunk(), 0.)
                assert output.history()[-2:] == [
                    "Ran ia.deviation",
                    'ia.deviation(outfile="", region="", mask="", overwrite=false, stretch=false, '
                    'grid=[1,1], anchor="ref", xlength="1pix", ylength="1pix", interp="cubic", '
                    'stattype="sigma", statalg="classic", zscore=-1.000000, maxiter=-1)',
                ]
            finally:
                output.done()
            for height in ("", None):
                output = source.deviation(xlength=2, ylength=height, stattype="npts")
                try:
                    count = np.full_like(values, 5)
                    count[[0, -1], :] -= 1
                    count[:, [0, -1]] -= 1
                    np.testing.assert_array_equal(output.getchunk(), count)
                finally:
                    output.done()
        finally:
            source.done()


def test_deviation_circle_with_a_full_rectangular_bounding_mask():
    source = tools.image()
    values = np.ones((15, 17), dtype=np.float32)
    assert source.fromarray(pixels=values)
    try:
        output = source.deviation(xlength=3, ylength="", stattype="npts")
        try:
            count = np.full_like(values, 9.)
            count[[0, -1], :] = 6.
            count[:, [0, -1]] = 6.
            count[np.ix_([0, -1], [0, -1])] = 4.
            np.testing.assert_array_equal(output.getchunk(), count)
            assert output.getchunk(getmask=True).all()
        finally:
            output.done()
    finally:
        source.done()


def test_deviation_even_rectangle_includes_float_rounded_edges(casa):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[15, 17])
        try:
            output = source.deviation(xlength=4, ylength=4, stattype="npts")
            try:
                actual = output.getchunk()
                assert actual[7, 8] == 25.
                assert actual[0, 0] == actual[-1, -1] == 9.
                assert actual[7, 0] == actual[7, -1] == 15.
            finally:
                output.done()
        finally:
            source.done()


def test_deviation_boolean_circle_and_casa_history(casa):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[15, 17])
        try:
            output = source.deviation(anchor=[1, 2], xlength=2,
                                      ylength=[True, False], stattype="npts" + "x"*310)
            try:
                assert output.getchunk()[7, 8] == 5.
                assert 'anchor=[1,2], xlength=2, ylength=[true,false]' in output.history()[-1]
                assert 'stattype="..."' in output.history()[-1]
            finally:
                output.done()
        finally:
            source.done()


@pytest.mark.parametrize("interp", ["nearest", "linear", "cubic"])
@pytest.mark.parametrize("masked", [False, True])
def test_deviation_grid_interpolation_across_tiles(casa, interp, masked):
    x, y = np.indices((79, 72), dtype=np.float32)
    values = 0.14 * x*x + 0.025 * y*y + y + (x % 4)
    valid = np.ones_like(values, dtype=bool)
    valid[33:35, 18:56] = False
    seen = []
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            if masked:
                source.putregion(pixelmask=valid)
            output = source.deviation(
                xlength=3, ylength=3, grid=[4, 3], anchor=np.array([1, 1]),
                interp=interp, stattype="mean",
            )
            try:
                seen.append((output.getchunk(), output.getchunk(getmask=True)))
            finally:
                output.done()
        finally:
            source.done()
    (oracle, oracle_valid), (actual, actual_valid) = seen
    np.testing.assert_array_equal(actual_valid, oracle_valid)
    np.testing.assert_allclose(actual[actual_valid], oracle[oracle_valid], rtol=3e-6, atol=3e-6)
    assert actual[29, 31] == pytest.approx(values[28:31, 30:33].mean(), rel=2e-6)


def test_deviation_chauvenet_rejects_a_local_outlier(casa):
    values = np.full((15, 17), 2., dtype=np.float32)
    values[7, 8] = 250.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            count = source.deviation(xlength=3, ylength=3, stattype="npts",
                                     statalg="Chauvenet", zscore=1.2, maxiter=2)
            mean = source.deviation(xlength=3, ylength=3, stattype="mean",
                                    statalg="Chauvenet", zscore=1.2, maxiter=2)
            try:
                assert count.getchunk()[7, 8] == 8.
                np.testing.assert_array_equal(mean.getchunk(), 2.)
            finally:
                mean.done()
                count.done()
        finally:
            source.done()


@pytest.mark.parametrize("offset, expected", [(0, 4.), (1, 0.)])
def test_deviation_chauvenet_can_recover_samples_after_empty_clipping(casa, offset, expected):
    x, y = np.indices((15, 17))
    values = ((x+y) % 2 + offset).astype(np.float32)
    options = dict(xlength=3, ylength=3, statalg="ch", zscore=.1, maxiter=1)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            output = source.deviation(stattype="npts", **options)
            try:
                assert output.getchunk()[7, 8] == expected
                assert output.getchunk(getmask=True).all()
            finally:
                output.done()
            if offset:
                with pytest.raises(RuntimeError, match="No valid data found"):
                    source.deviation(stattype="median", **options)
        finally:
            source.done()


def test_deviation_reuses_single_grid_value_across_output_tiles(casa):
    x, y, z = np.indices((128, 128, 2))
    values = (x + 2*y + 13*z).astype(np.float32)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            output = source.deviation(grid=[256, 256], anchor=[64, 64], interp="nearest",
                                      xlength=51, ylength=51, stattype="mean")
            try:
                expected = np.broadcast_to(values[64, 64, :], values.shape)
                np.testing.assert_array_equal(output.getchunk(), expected)
            finally:
                output.done()
        finally:
            source.done()


def test_deviation_stretches_an_expression_mask_across_planes(casa, tmp_path):
    x, y, z = np.indices((15, 17, 2))
    values = (x*x + 2*y + 40*z).astype(np.float32)
    path = str(tmp_path / "spatial-mask.im")
    operand = casa.image()
    assert operand.fromarray(path, (x[:, :, 0] >= 7).astype(np.float32))
    operand.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            output = source.deviation(mask=f"'{path}' > 0", stretch=True,
                                      xlength=3, ylength=3, stattype="mean")
            try:
                np.testing.assert_array_equal(output.getchunk(getmask=True), x >= 7)
                expected = np.mean(values[7:9, 7:10, :], axis=(0, 1))
                np.testing.assert_array_equal(output.getchunk()[7, 8], expected)
            finally:
                output.done()
        finally:
            source.done()


def test_deviation_tracks_direction_axes_in_a_transposed_cube(casa):
    x, y, z = np.indices((15, 17, 2))
    values = (x*x + 2*y + 40*z).astype(np.float32)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        transposed = source.transpose(order=201)
        try:
            output = transposed.deviation(xlength=3, ylength=3, stattype="mean")
            try:
                np.testing.assert_array_equal(output.shape(), [2, 15, 17])
                expected = np.mean(values[6:9, 7:10, :], axis=(0, 1))
                np.testing.assert_allclose(output.getchunk()[:, 7, 8], expected, rtol=1e-7)
            finally:
                output.done()
        finally:
            transposed.done()
            source.done()


def test_deviation_region_product_native_interoperability(casa, tmp_path):
    x, y, z = np.indices((20, 21, 2))
    values = (x + 2*y + 100*z).astype(np.float32)
    region_tool = casa.regionmanager()
    region = region_tool.box([2, 3, 0], [17, 18, 1])
    region_tool.done()
    seen = []
    for index, maker in enumerate((casa, tools)):
        source = maker.image()
        assert source.fromarray(pixels=values)
        source.setbrightnessunit("Jy/beam")
        source.setrestoringbeam(major="6arcsec", minor="4arcsec", pa="10deg")
        source_coords = source.coordsys()
        expected_world = source_coords.toworld([2, 3, 0])["numeric"]
        source_coords.done()
        outfile = str(tmp_path / f"deviation-{index}.im")
        output = source.deviation(outfile, region, xlength=3, ylength=3, stattype="mean")
        output.done()
        np.testing.assert_array_equal(source.getchunk(), values)
        source.done()
        for reader in (casa, tools):
            output = reader.image()
            assert output.open(outfile)
            try:
                np.testing.assert_array_equal(output.shape(), [16, 16, 2])
                assert output.brightnessunit() == "Jy/beam"
                assert output.restoringbeam()["major"]["value"] == pytest.approx(6.)
                coords = output.coordsys()
                try:
                    actual_world = coords.toworld([0, 0, 0])["numeric"]
                    np.testing.assert_allclose(actual_world, expected_world, rtol=1e-9, atol=1e-9)
                finally:
                    coords.done()
                assert output.getchunk(getmask=True).all()
                assert any("ia.deviation(" in line for line in output.history())
                seen.append(output.getchunk())
            finally:
                output.done()
    for actual in seen:
        np.testing.assert_allclose(actual, seen[0], rtol=2e-6)
        np.testing.assert_array_equal(actual[3, 4], values[5, 7])


@pytest.mark.parametrize("kwargs, match", [
    ({"grid": [1]}, "grid must have exactly two positive integer values"),
    ({"grid": [0, 1]}, "grid value"),
    ({"anchor": [1., 2.]}, "Unsupported type for anchor"),
    ({"xlength": 3.0}, "must be pixel or angular"),
    ({"xlength": {"value": 3., "unit": "pix"}}, "not a valid quantity"),
    ({"interp": "spline"}, "Interpolation algorithm"),
    ({"statalg": "biweight"}, "Unsupported stats algorithm"),
    ({"stattype": "mode"}, "Statistic mode not supported"),
])
def test_deviation_rejects_invalid_options_before_output(casa, tmp_path, kwargs, match):
    values = np.arange(15 * 17, dtype=np.float32).reshape((15, 17))
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        outfile = tmp_path / f"{api.__name__}-unused.im"
        try:
            with pytest.raises(RuntimeError, match=match):
                source.deviation(str(outfile), **kwargs)
            assert not outfile.exists()
            np.testing.assert_array_equal(source.getchunk(), values)
        finally:
            source.done()


def test_deviation_preserves_an_existing_output_after_interpolation_failure(tmp_path):
    source = tools.image()
    values = np.arange(15 * 17, dtype=np.float32).reshape((15, 17))
    assert source.fromarray(pixels=values)
    outfile = str(tmp_path / "existing.im")
    result = source.deviation(outfile, stattype="mean")
    result.done()
    try:
        with pytest.raises(RuntimeError, match="bad interpolation"):
            source.deviation(outfile, overwrite=True, grid=[3, 4], anchor=[14, 16],
                             interp="nearest", stattype="mean")
        reopened = tools.image()
        assert reopened.open(outfile)
        try:
            np.testing.assert_array_equal(reopened.getchunk(), values)
        finally:
            reopened.done()
    finally:
        source.done()


def test_deviation_rejects_output_aliasing_named_mask(casa, tmp_path):
    values = np.full((15, 17), 3., dtype=np.float32)
    for index, api in enumerate((casa, tools)):
        source = api.image()
        assert source.fromarray(pixels=values)
        path = tmp_path / f"protected-mask-{index}.im"
        mask = api.image()
        assert mask.fromarray(str(path), np.ones_like(values))
        mask.done()
        before = {child.relative_to(path): child.read_bytes() for child in path.rglob("*")
                  if child.is_file() and child.name != "table.lock"}
        try:
            with pytest.raises(RuntimeError):
                source.deviation(outfile=str(path), mask=f'"{path}" > 0',
                                 overwrite=True, stattype="mean")
            assert before
            assert all((path / name).read_bytes() == contents for name, contents in before.items())
        finally:
            source.done()
