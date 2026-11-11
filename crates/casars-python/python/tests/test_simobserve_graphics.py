"""A real task process, native plot payload, renderer and product completion."""

import os
from pathlib import Path
import subprocess

import pytest

pytest.importorskip("matplotlib")
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
from matplotlib.figure import Figure

from casars import tasks
from casars.tasks import _runner


@pytest.fixture(scope="module")
def simulation_inputs(tmp_path_factory):
    try:
        _runner._task_executable("simobserve")
    except tasks.TaskBinaryNotFoundError as error:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(str(error))
    root = tmp_path_factory.mktemp("simulation-inputs")
    repository = Path(__file__).resolve().parents[4]
    subprocess.run(["cargo", "run", "-q", "-p", "casars-python", "--example", "make_python_fixtures", "--", str(root)], cwd=repository, check=True)
    config = root / "small.cfg"
    config.write_text("# observatory=ALMA\n# coordsys=XYZ\n" + "\n".join([
        "2225021.163600 -5440082.369940 -2481681.150540 12 A00",
        "2225091.163600 -5440097.369940 -2481681.150540 12 A01",
        "2225121.163600 -5440047.369940 -2481681.150540 12 A02",
        "2225011.163600 -5440012.369940 -2481681.150540 12 A03",
        "2225071.163600 -5439997.369940 -2481681.150540 12 A04",
        "2225061.163600 -5440057.369940 -2481681.150540 12 A05",
        "2224981.163600 -5440127.369940 -2481681.150540 12 A06",
        "2225151.163600 -5439977.369940 -2481681.150540 12 A07",
    ]) + "\n")
    return root / "python_fixture.image", config


@pytest.mark.parametrize("graphics", ["file", "screen", "both", "none"])
def test_simobserve_renders_and_cleans_its_payload_before_completion(simulation_inputs, graphics, tmp_path, monkeypatch):
    model, config = simulation_inputs
    payloads, shown = [], []
    popen = subprocess.Popen

    def launch(*args, **kwargs):
        payload = kwargs.get("env", {}).get("CASARS_PLOT_PAYLOAD")
        if payload:
            payloads.append(Path(payload))
        return popen(*args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", launch)
    monkeypatch.setattr(Figure, "show", lambda figure, **kwargs: shown.append(figure))
    before = set(plt.get_fignums())
    try:
        completion = tasks.run(
            "simobserve", workspace=tmp_path, project=f"p-{graphics}",
            skymodel=str(model), antennalist=str(config), mapsize="1arcsec",
            incell="1arcsec", indirection="J2000 10h00m00 -30d00m00",
            incenter="230GHz", inwidth="1GHz", integration="10s", totaltime="40s",
            thermalnoise="", graphics=graphics,
        )
        assert completion.successful and completion.value is None
        pngs = [product for product in completion.products if product.path.endswith(".png")]
        assert len(pngs) == (2 if graphics in {"file", "both"} else 0)
        for product in pngs:
            assert product.exists
            assert Path(product.path).read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
        assert all(product.exists for product in completion.products)
        assert len(shown) == (2 if graphics in {"screen", "both"} else 0)
        assert len(payloads) == (0 if graphics == "none" else 1)
        assert all(not payload.parent.exists() for payload in payloads)
    finally:
        for number in set(plt.get_fignums()) - before:
            plt.close(number)
