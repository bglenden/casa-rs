"""Public simulation plotting against installed CASA 6.7.6.14 artists."""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
import sys
import tarfile

import numpy as np
import pytest

pytest.importorskip("matplotlib")
from matplotlib import pyplot as plt

from casars.simutil import simutil

REPOSITORY = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPOSITORY / "tools/simulation"))
from check_simobserve_rendering import compare as compare_artists
from simobserve_graphics_oracle import artists

with gzip.open(Path(__file__).with_name("data") / "casa_simutil_graphics.json.gz", "rt") as source:
    REFERENCE = json.load(source)


def compare(actual, expected):
    maximum = compare_artists(actual, expected)
    output = os.environ.get("CASA_RS_SIMUTIL_GRAPHICS_RECEIPT")
    if output:
        images = []
        for panel, reference in zip(actual, expected, strict=True):
            for image, truth in zip(panel["images"], reference["images"], strict=True):
                pixels, values = np.asarray(image["pixels"]), np.asarray(truth["pixels"])
                images.append(float(np.linalg.norm(pixels - values) / np.linalg.norm(values)))
        with open(output, "a", encoding="utf-8") as destination:
            destination.write(json.dumps({"case": os.environ["PYTEST_CURRENT_TEST"], "max_coordinate_difference_in_display_units": maximum, "image_nrms": images}) + "\n")
    return maximum


@pytest.fixture(autouse=True)
def figures():
    plt.close("all")
    yield
    plt.close("all")
    plt.ioff()


@pytest.fixture(scope="module")
def images(tmp_path_factory):
    root = tmp_path_factory.mktemp("simutil-plots")
    with tarfile.open(REPOSITORY / "crates/casa-simulation/tests/data/simanalyze_analysis_inputs.tar.gz") as archive:
        archive.extractall(root, filter="data")
    with tarfile.open(Path(__file__).with_name("data") / "simutil_graphics_observation.tar.gz") as archive:
        archive.extractall(root, filter="data")
    return root


def test_figure_lifecycle_and_caller_list_match_casa(tmp_path):
    utility = simutil()
    multi = [2, 2, 1]
    selected = plt.figure(17)
    assert utility.newfig(multi, show=False) is None
    assert plt.gcf() is selected
    assert utility.pmulti is multi
    transitions = []
    for _ in range(5):
        assert utility.nextfig() is None
        transitions.append({"multi": multi.copy(), "axes": len(plt.gcf().axes), "subplot": list(plt.gca().get_subplotspec().get_geometry())})
    assert transitions == REFERENCE["lifecycle"]["transitions"]
    output = tmp_path / "lifecycle.png"
    assert utility.endfig(show=False, filename=str(output)) is None
    assert not plt.isinteractive() and output.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    assert plt.gca().get_xticklabels()[0].get_fontsize() == REFERENCE["lifecycle"]["tick_size"]
    assert utility.newfig(show=True) is None
    assert plt.isinteractive()
    utility.endfig()
    assert not plt.isinteractive()


@pytest.mark.parametrize("case", REFERENCE["antennas"], ids=lambda c: f"{len(c['names'])}-d{c['diameters'][0]}")
def test_every_antenna_and_dish_matches_casa(case):
    utility = simutil()
    utility.newfig(show=False)
    x, y, z = np.asarray(case["positions"]).T
    assert utility.plotants(x, y, z, case["diameters"], case["names"]) is None
    compare(artists(plt.gcf()), case["artists"])


@pytest.mark.parametrize("method", ["plotants", "ephemeris"])
def test_plotants_draws_on_the_selected_panel_and_preserves_its_labels(method):
    utility = simutil()
    utility.newfig([2, 2, 1], show=False)
    utility.nextfig()
    axis = plt.gca()
    axis.set(title="existing title", xlabel="existing x", ylabel="existing y")
    axis.title.set(fontsize=23, color="red", fontweight="bold")
    if method == "plotants":
        case = REFERENCE["antennas"][1]
        x, y, z = np.asarray(case["positions"]).T
        utility.plotants(x, y, z, case["diameters"], case["names"])
        assert (axis.get_xlabel(), axis.get_ylabel()) == ("existing x", "existing y")
        assert len(axis.patches) == len(case["names"])
    else:
        utility.ephemeris("2014/01/01", direction="J2000 10h00m00 -30d00m00", telescope="ALMA")
        assert axis.get_xlabel().startswith("hours relative to ")
        assert axis.get_ylabel() == "elevation" and len(axis.lines) == 2
    assert plt.gca() is axis and len(plt.gcf().axes) == 2
    assert axis.get_title() == "existing title"
    assert axis.title.get_fontsize() == 23
    assert axis.title.get_color() == "red" and axis.title.get_fontweight() == "bold"


@pytest.mark.parametrize("case", REFERENCE["images"], ids=lambda c: f"{c['image']}-{c['options']}")
def test_statim_complete_plane_controls_and_statistics_match_casa(case, images, monkeypatch):
    monkeypatch.chdir(images)
    utility = simutil()
    utility.newfig(show=False)
    options = json.loads(json.dumps(case["options"]))
    result = utility.statim(f"inputs/{case['image']}", **options)
    np.testing.assert_allclose([float(v) for v in result[:3]], case["statistics"][:3], rtol=1e-6, atol=1e-9)
    assert result[3] == case["statistics"][3]
    if isinstance(options.get("disprange"), list):
        np.testing.assert_allclose(options["disprange"], case["options_after"]["disprange"], rtol=1e-6, atol=1e-9)
    compare(artists(plt.gcf()), case["artists"])


@pytest.mark.parametrize("case", REFERENCE["ephemerides"])
@pytest.mark.parametrize("explicit", [False, True])
def test_public_ephemeris_matches_casa(case, explicit, images, monkeypatch):
    monkeypatch.chdir(images)
    utility = simutil(direction=case["direction"], totaltime=case["totaltime"])
    utility.telescopename = case["telescope"]
    utility.newfig(show=False)
    extra = {name: case[name] for name in ("ms", "cofa") if name in case}
    if explicit:
        assert simutil().ephemeris(case["date"], usehourangle=case["usehourangle"], direction=case["direction"], telescope=case["telescope"], **extra) is None
    else:
        assert utility.ephemeris(case["date"], usehourangle=case["usehourangle"], **extra) is None
    compare(artists(plt.gcf()), case["artists"])


@pytest.mark.parametrize("duration", [None, {"value": 40.0, "unit": "s"}])
def test_ephemeris_accepts_its_default_quantity_and_draws_the_requested_duration(duration):
    case = REFERENCE["ephemerides"][0]
    utility = simutil(direction=case["direction"], **({"totaltime": duration} if duration is not None else {}))
    utility.telescopename = case["telescope"]
    utility.newfig(show=False)
    utility.ephemeris(case["date"])
    expected = json.loads(json.dumps(case["artists"]))
    if duration is not None:
        # Independent CASA transit plus the analytic 40-second width. CASA's
        # raw dict > int failure is retained in the fixture, never accepted
        # as a passing plot.
        transit = expected[0]["lines"][1]["x"][0]
        expected[0]["lines"].append({"x": [transit - 20 / 3600, transit + 20 / 3600], "y": [80, 80], "color": "r", "marker": "None", "linestyle": "-", "line_width": 1.5})
    compare(artists(plt.gcf()), expected)
    if duration is not None:
        x = plt.gca().lines[-1].get_xdata()
        assert x[1] - x[0] == pytest.approx(40 / 3600, abs=1e-12)


@pytest.mark.parametrize("case", REFERENCE["errors"])
def test_ephemeris_refuses_an_unknown_or_unobservable_site_as_casa_does(case):
    exception = ValueError if case["type"] == "ValueError" else RuntimeError
    with pytest.raises(exception) as error:
        simutil(totaltime=0.0).ephemeris("2014/01/01", **case["options"])
    assert str(error.value) == case["message"]
    assert plt.get_fignums() == []
