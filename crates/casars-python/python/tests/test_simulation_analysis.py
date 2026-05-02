from __future__ import annotations

from casars.tasks import simulation_analysis


def test_vla_ppdisk_dirty_analysis_composes_task_surfaces(monkeypatch) -> None:
    calls: list[tuple[str, tuple, dict]] = []

    def fake_mfs(*args, **kwargs):
        calls.append(("mfs", args, kwargs))
        return {"task": "imager"}

    def fake_imhead(*args, **kwargs):
        calls.append(("imhead", args, kwargs))
        return {"task": "imhead"}

    def fake_imstat(*args, **kwargs):
        calls.append(("imstat", args, kwargs))
        return {"task": "imstat"}

    def fake_plot(*args, **kwargs):
        calls.append(("plot", args, kwargs))
        return {"task": "plot"}

    monkeypatch.setattr(simulation_analysis.imager, "mfs", fake_mfs)
    monkeypatch.setattr(simulation_analysis.image_analysis, "imhead", fake_imhead)
    monkeypatch.setattr(simulation_analysis.image_analysis, "imstat", fake_imstat)
    monkeypatch.setattr(simulation_analysis.msexplore, "plot", fake_plot)

    result = simulation_analysis.vla_ppdisk_dirty_analysis(
        "ppdisk.synthetic.ms",
        "products/ppdisk",
        plot_output="amp-time.png",
        imager_binary="imager-bin",
        imexplore_binary="imexplore-bin",
        msexplore_binary="msexplore-bin",
    )

    assert result == {
        "imaging": {"task": "imager"},
        "imhead": {"task": "imhead"},
        "imstat": {"task": "imstat"},
        "plot": {"task": "plot"},
    }
    assert calls[0] == (
        "mfs",
        ("ppdisk.synthetic.ms", "products/ppdisk"),
        {
            "image_size": 257,
            "cell_arcsec": 0.00311,
            "data_column": "data",
            "dirty_only": True,
            "niter": 0,
            "binary": "imager-bin",
        },
    )
    assert calls[1] == (
        "imhead",
        ("products/ppdisk.image",),
        {"binary": "imexplore-bin"},
    )
    assert calls[2] == (
        "imstat",
        ("products/ppdisk.image",),
        {"binary": "imexplore-bin"},
    )
    assert calls[3][0] == "plot"
    assert calls[3][1] == ("ppdisk.synthetic.ms", "amp-time.png")
    assert calls[3][2]["binary"] == "msexplore-bin"
