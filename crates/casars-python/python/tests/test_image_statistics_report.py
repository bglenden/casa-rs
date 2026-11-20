"""Image statistics reports against pinned CASA and independent reductions."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("list_results,robust", [(False, False), (True, True)])
def test_global_statistics_log_can_create_append_and_replace(casa, tmp_path, list_results, robust):
    values = np.arange(24, dtype=np.float32).reshape((4, 3, 2), order="F")
    results = []
    reports = []
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.fromarray(pixels=values)
        assert image.setbrightnessunit("Jy/pixel")
        logfile = tmp_path / f"stats-{index}.txt"
        options = dict(list=list_results, robust=robust, logfile=str(logfile))
        results.append(image.statistics(**options))
        report = logfile.read_text()
        assert image.statistics(**options)["sum"][0] == values.sum()
        assert logfile.read_text() == report * 2
        assert image.statistics(append=False, **options)["sum"][0] == values.sum()
        assert logfile.read_text() == report
        reports.append(report)
        assert image.done()
    _assert_value(*results)
    assert reports[0] == reports[1]


def test_plane_statistics_report_after_native_reopen(casa, tmp_path):
    shape = (4, 3, 2, 3)
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape, order="F")
    reports = []
    for index, writer in enumerate((tools, casa)):
        path = str(tmp_path / f"stats-{index}.im")
        image = writer.image()
        cs = writer.coordsys().newcoordsys(direction=True, stokes="I,Q", spectral=True)
        assert image.fromarray(path, pixels=values, csys=cs.torecord())
        assert image.setbrightnessunit("Jy/pixel")
        assert image.done()
        for reader_index, reader in enumerate((tools, casa)):
            image = reader.image()
            assert image.open(path)
            logfile = tmp_path / f"planes-{index}-{reader_index}.txt"
            actual = image.statistics(axes=[0, 1], logfile=str(logfile))
            np.testing.assert_allclose(actual["sum"], values.sum(axis=(0, 1)))
            report = logfile.read_text()
            lines = report.splitlines()
            assert lines[0] == "#VELOCITY column unit = km/s"
            rows = [line.split() for line in lines if not line.startswith("#")]
            assert [row[2] for row in rows] == ["I", "Q"] * shape[3]
            np.testing.assert_allclose([float(row[5]) for row in rows], values.sum(axis=(0, 1)).ravel(order="F"))
            reports.append(report)
            assert image.done()
    assert all(report == reports[0] for report in reports)


@pytest.mark.parametrize("maxiter,counts", [(-1, [2, 1]), (1, [1, 1]), (0, [0, 0])])
def test_chauvenet_reports_iterations_per_plane(casa, tmp_path, maxiter, counts):
    values = np.zeros((5, 5, 2), dtype=np.float32)
    values[2, 3, 0] = 1000
    values[2, 2, :] = 30
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.fromarray(pixels=values)
        logfile = tmp_path / f"iterations-{index}.txt"
        result = image.statistics(axes=[0, 1], algorithm="chauvenet", maxiter=maxiter, logfile=str(logfile))
        np.testing.assert_array_equal(result["npts"], [23 if maxiter != 0 else 24, 24])
        lines = logfile.read_text().splitlines()
        assert any(line.endswith("N Iter") for line in lines)
        assert [int(line.split()[-1]) for line in lines if line.startswith(" ")] == counts
        assert image.done()


def test_empty_and_unwritable_reports_preserve_numeric_results(casa, tmp_path):
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.fromarray(pixels=np.arange(6, dtype=np.float32).reshape(3, 2))
        logfile = tmp_path / f"empty-{index}.txt"
        result = image.statistics(excludepix=[0, 5], logfile=str(logfile))
        assert result["npts"].size == 0
        assert logfile.read_text() == "NO UNMASKED POINTS FOUND, NO STATISTICS WERE COMPUTED"
        result = image.statistics(logfile=str(tmp_path / "missing" / "log.txt"))
        assert result["sum"][0] == 15
        assert image.done()


def test_statistics_only_writes_a_report_when_a_path_is_supplied(tmp_path):
    image = tools.image()
    assert image.fromarray(pixels=np.array([[1.0, 2.0]], dtype=np.float32))
    logfile = tmp_path / "report.txt"
    image.statistics(logfile=str(logfile))
    text = "existing report text\n"
    logfile.write_text(text)
    image.statistics(append=False)
    assert logfile.read_text() == text
    assert image.done()


@pytest.mark.parametrize("shape,unit", [((4, 3), "Jy"), ((4, 3, 1, 4), "Jy.km/s")])
def test_statistics_report_labels_the_flux_and_its_units(casa, tmp_path, shape, unit):
    values = np.ones(shape, dtype=np.float32)
    flux_lines = []
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.fromarray(pixels=values)
        assert image.setbrightnessunit("Jy/beam")
        assert image.setrestoringbeam(major="2arcmin", minor="1arcmin", pa="0deg")
        logfile = tmp_path / f"flux-{index}.txt"
        result = image.statistics(logfile=str(logfile), list=True)
        line, = (line for line in logfile.read_text().splitlines() if "[flux]" in line)
        assert line.endswith(" " + unit)
        assert float(line.split()[-2]) == pytest.approx(result["flux"][0], rel=5e-6)
        flux_lines.append(line)
        assert image.done()
    assert flux_lines[0] == flux_lines[1]


def test_report_uses_native_velocity_preferences_and_world_axis_order(casa, tmp_path):
    path = str(tmp_path / "galactic.im")
    source = casa.coordsys().newcoordsys(direction=True, spectral=True)
    assert source.setreferencecode("GALACTIC", type="direction", adjust=False)
    record = source.torecord()
    record["spectral1"]["velUnit"] = "m/s"
    record["spectral1"]["velType"] = 1
    image = casa.image()
    values = np.arange(60, dtype=np.float32).reshape((3, 4, 5), order="F")
    assert image.fromarray(path, pixels=values, csys=record)
    assert image.done()
    for axis in (0, 2):
        reports = []
        for index, backend in enumerate((tools, casa)):
            image = backend.image()
            assert image.open(path)
            logfile = tmp_path / f"galactic-{axis}-{index}.txt"
            stats = image.statistics(axes=[axis], logfile=str(logfile))
            np.testing.assert_array_equal(stats["sum"], values.sum(axis=axis))
            reports.append(logfile.read_text())
            assert image.done()
        assert reports[0] == reports[1]
        if axis == 0:
            assert reports[0].startswith("#VELOCITY column unit = m/s\n")


def test_statistics_skips_empty_planes_and_omits_selected_axis_summary(casa, tmp_path):
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.fromarray(pixels=np.arange(27, dtype=np.float32).reshape((3, 3, 3), order="F"))
        logfile = tmp_path / f"empty-plane-{index}.txt"
        result = image.statistics(axes=[0, 1], excludepix=[9, 17], logfile=str(logfile))
        np.testing.assert_array_equal(result["npts"], [9, 0, 9])
        assert [row.split()[0] for row in logfile.read_text().splitlines() if row.startswith(" ")] == ["I", "U"]
        assert image.done()
        assert image.fromarray(pixels=np.arange(5, dtype=np.float32).reshape((1, 1, 5)))
        image.statistics(axes=[2], list=True, logfile=str(logfile), append=False)
        assert "# Regions" in logfile.read_text()
        assert "# Values" not in logfile.read_text()
        assert image.done()


def test_linear_report_labels_have_no_unit_suffix(casa, tmp_path):
    path = str(tmp_path / "linear.im")
    cs = casa.coordsys().newcoordsys(direction=True, linear=1)
    assert cs.setunits(["km"], type="linear", overwrite=True)
    image = casa.image()
    assert image.fromshape(path, [3, 4, 5], csys=cs.torecord())
    assert image.done()
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.open(path)
        logfile = tmp_path / f"linear-{index}.txt"
        image.statistics(axes=[0, 1], logfile=str(logfile))
        assert [row.split()[0] for row in logfile.read_text().splitlines() if row.startswith(" ")] == [str(i) for i in range(5)]
        assert image.done()


def test_optical_flux_uses_the_stored_velocity_unit(casa, tmp_path):
    path = str(tmp_path / "optical.im")
    cs = casa.coordsys().newcoordsys(direction=True, spectral=True)
    record = cs.torecord()
    spectral = record["spectral1"]
    spectral["velUnit"] = "m/s"
    spectral["velType"] = 1
    image = casa.image()
    assert image.fromarray(path, pixels=np.ones((4, 3, 3), dtype=np.float32), csys=record)
    assert image.setbrightnessunit("Jy/beam")
    assert image.setrestoringbeam(major="2arcmin", minor="1arcmin", pa="0deg")
    assert image.done()
    freq = spectral["wcs"]["crval"]
    delta = spectral["wcs"]["cdelt"]
    rest = spectral["restfreq"]
    channel_width = 299792458.0 * rest * abs(1 / (freq + delta) - 1 / freq)
    beam_area_in_pixels = np.pi / (2 * np.log(2))
    expected = 36 * channel_width / beam_area_in_pixels
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.open(path)
        logfile = tmp_path / f"optical-{index}.txt"
        stats = image.statistics(list=True, logfile=str(logfile))
        np.testing.assert_allclose(stats["flux"], [expected], rtol=1e-8)
        assert "3378.53 Jy.m/s" in logfile.read_text()
        assert image.done()


def test_strided_report_labels_absolute_selected_planes(casa, tmp_path):
    image = tools.image()
    cs = tools.coordsys().newcoordsys(direction=True, spectral=True)
    values = np.arange(108, dtype=np.float32).reshape((3, 6, 6), order="F")
    assert image.fromarray(pixels=values, csys=cs.torecord())
    region = casa.regionmanager().box([0, 0, 1], [2, 5, 5], [1, 1, 2])
    logfile = tmp_path / "strided.txt"
    stats = image.statistics(axes=[0, 1], region=region, logfile=str(logfile))
    np.testing.assert_array_equal(stats["sum"], values[:, :, 1::2].sum(axis=(0, 1)))
    rows = [row.split() for row in logfile.read_text().splitlines() if row.startswith(" ")]
    assert [int(row[1]) for row in rows] == [1, 3, 5]
    assert image.done()
