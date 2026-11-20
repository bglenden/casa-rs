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
        assert logfile.read_text() == "NO UNMASKED POINTS FOUND, NO STATISTICS WERE COMPUTED\n"
        result = image.statistics(logfile=str(tmp_path / "missing" / "log.txt"))
        assert result["sum"][0] == 15
        assert image.done()


def test_statistics_only_writes_a_report_when_a_path_is_supplied(tmp_path):
    image = tools.image()
    assert image.fromarray(pixels=np.array([[1.0, 2.0]], dtype=np.float32))
    logfile = tmp_path / "report.txt"
    image.statistics(logfile=str(logfile))
    text = logfile.read_text()
    image.statistics(append=False)
    assert logfile.read_text() == text
    assert image.done()
