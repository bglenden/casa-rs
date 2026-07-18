from __future__ import annotations

from pathlib import Path
import json
import os
import stat
import textwrap

import pytest
import numpy as np

from casars import _task_runtime
from casars.tasks import msexplore


@pytest.fixture(autouse=True)
def reset_msexplore_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    msexplore.configure(binary=None)
    monkeypatch.delenv("CASARS_MSEXPLORE_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_binary_lookup_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_binary = _write_stub_binary(tmp_path / "env" / "msexplore", version="env")
    configured_binary = _write_stub_binary(
        tmp_path / "configured" / "msexplore",
        version="configured",
    )
    explicit_binary = _write_stub_binary(
        tmp_path / "explicit" / "msexplore",
        version="explicit",
    )

    monkeypatch.setenv("CASARS_MSEXPLORE_BIN", str(env_binary))
    msexplore.configure(binary=configured_binary)

    assert msexplore.protocol_info().binary_version == "configured"
    assert msexplore.protocol_info(binary=explicit_binary).binary_version == "explicit"

    msexplore.configure(binary=None)
    assert msexplore.protocol_info().binary_version == "env"


def test_suite_root_env_precedes_repo_local(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _write_stub_binary(suite_root / "bin" / "msexplore", version="suite")

    monkeypatch.setenv("CASARS_SUITE_ROOT", str(suite_root))

    assert msexplore.protocol_info().binary_version == "suite"


def test_protocol_mismatch_fails_fast(tmp_path: Path) -> None:
    binary = _write_stub_binary(
        tmp_path / "bad" / "msexplore",
        version="bad",
        protocol_version=99,
    )

    with pytest.raises(RuntimeError, match="expected protocol version"):
        msexplore.run({"spec": {"ms_path": "dataset.ms"}}, binary=binary)


def test_protocol_info_subprocess_failures_raise_msexplore_invocation_error(
    tmp_path: Path,
) -> None:
    binary = _write_failing_protocol_binary(tmp_path / "bad-protocol" / "msexplore")

    with pytest.raises(_task_runtime.MsExploreInvocationError, match="protocol-info crashed"):
        msexplore.protocol_info(binary=binary)


def test_summary_wrapper_encodes_pythonic_arguments(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "msexplore", version="ok")

    result = msexplore.summary(
        "twhya_calibrated.ms",
        format="json",
        output_path="summary.json",
        overwrite=True,
        selection={"field": "5", "spw": "0"},
        binary=binary,
    )

    assert result["kind"] == "run"
    request = result["result"]["request"]
    assert request["summary_output_path"] == "summary.json"
    assert request["overwrite_outputs"] is True
    spec = request["spec"]
    assert spec["ms_path"] == "twhya_calibrated.ms"
    assert spec["summary_format"] == "Json"
    assert spec["selection"]["field"] == "5"
    assert spec["selection"]["spw"] == "0"
    assert spec["selection"]["selectdata"] is True
    assert spec["plots"] == []


def test_plot_wrapper_encodes_plot_export_request(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "msexplore", version="ok")

    result = msexplore.plot(
        "ppdisk.synthetic.ms",
        "amp-time.png",
        preset="uv_coverage",
        x_axis="Time",
        y_axis="Amplitude",
        data_column="data",
        color_by="Field",
        avgchannel=10000,
        avgtime=1e9,
        avgspw=False,
        avgscan=False,
        title="Synthetic amplitudes",
        binary=binary,
    )

    request = result["result"]["request"]
    assert request["plot_export"] == {
        "output_path": "amp-time.png",
        "format": "png",
        "width": 1200,
        "height": 800,
    }
    plot = request["spec"]["plots"][0]
    assert plot["preset"] == "uv_coverage"
    assert plot["x_axis"] == "time"
    assert plot["y_axes"] == ["amplitude"]
    assert plot["data_column"] == "data"
    assert plot["color_by"] == "field"
    assert plot["averaging"]["avgchannel"] == 10000
    assert plot["averaging"]["avgtime"] == 1e9
    assert plot["averaging"]["avgspw"] is False
    assert plot["averaging"]["avgscan"] is False
    assert plot["style"]["title"] == "Synthetic amplitudes"


def test_plot_wrapper_uses_preset_axes_when_axes_are_omitted(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "msexplore", version="ok")

    uv_result = msexplore.plot(
        "twhya_calibrated.ms",
        "uv-coverage.txt",
        preset="uv_coverage",
        format="txt",
        binary=binary,
    )
    uv_plot = uv_result["result"]["request"]["spec"]["plots"][0]
    assert uv_plot["preset"] == "uv_coverage"
    assert uv_plot["x_axis"] == "u"
    assert uv_plot["y_axes"] == ["v"]

    result = msexplore.plot(
        "twhya_calibrated.ms",
        "amp-uvdist.png",
        preset="amplitude_vs_uv_distance",
        binary=binary,
    )

    plot = result["result"]["request"]["spec"]["plots"][0]
    assert plot["preset"] == "amplitude_vs_uv_distance"
    assert plot["x_axis"] == "uv_distance"
    assert plot["y_axes"] == ["amplitude"]


def test_native_data_returns_numpy_without_reading_a_rendered_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, dict[str, object], dict[str, object]]] = []

    def fake_data(path: str, selection_json: str, plot_json: str) -> str:
        captured.append((path, json.loads(selection_json), json.loads(plot_json)))
        return json.dumps({
            "schema_version": 1,
            "title": "Amplitude vs time",
            "summary": "2 selected visibility samples",
            "header_lines": [],
            "show_legend": True,
            "panels": [{
                "id": "main",
                "title": "Amplitude vs time",
                "axes": [
                    {"id": "x", "label": "Time (s)", "unit": "s", "lower": 1.0, "upper": 2.0},
                    {"id": "y", "label": "Amplitude (Jy)", "unit": "Jy", "lower": 3.0, "upper": 4.0},
                ],
                "series": [{
                    "label": "field 0",
                    "color_group": "field-0",
                    "y_axis_id": "y",
                    "x": [1.0, 2.0],
                    "y": [3.0, 4.0],
                    "provenance": [
                        {"row": 4, "corr": 0, "chan_start": 0, "chan_end": 1},
                        {"row": 5, "corr": 0, "chan_start": 0, "chan_end": 1},
                    ],
                }],
            }],
        })

    monkeypatch.setattr(msexplore._core, "msexplore_plot_data_json", fake_data)
    result = msexplore.data(
        "tutorial.ms",
        preset="amplitude_vs_time",
        selection={"field": "0"},
    )

    assert result.measurement_set == "tutorial.ms"
    assert np.array_equal(result.panels[0].series[0].x, np.array([1.0, 2.0]))
    assert np.array_equal(result.panels[0].series[0].y, np.array([3.0, 4.0]))
    assert result.panels[0].series[0].provenance[1]["row"] == 5
    assert captured[0][1]["field"] == "0"
    assert captured[0][2]["preset"] == "amplitude_vs_time"


def test_tutorial_measurement_set_returns_native_numeric_plot_data() -> None:
    root = Path(os.environ.get("CASA_RS_TUTORIAL_DATA_ROOT", "~/SoftwareProjects/casa-tutorial-data")).expanduser()
    measurement_set = root / "tutorial-parity/alma/first-look/twhya/imaging/alma-first-look-imaging.pack/twhya_calibrated.ms"
    if not measurement_set.is_dir():
        pytest.skip("local ALMA first-look tutorial MeasurementSet is unavailable")

    result = msexplore.data(
        measurement_set,
        preset="amplitude_vs_time",
        selection={"field": "0", "spw": "0"},
    )

    assert result.panels
    assert result.panels[0].series
    series = result.panels[0].series[0]
    assert series.x.size == series.y.size > 0
    assert np.isfinite(series.x).any()
    assert np.isfinite(series.y).any()
    pytest.importorskip("matplotlib")
    figure, axes = msexplore.plot_matplotlib(result)
    assert figure is not None
    assert axes.collections


def _write_stub_binary(
    path: Path,
    *,
    version: str,
    protocol_version: int = 1,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    script = textwrap.dedent(
        f"""\
        #!/usr/bin/env python3
        import json
        import sys

        if "--protocol-info" in sys.argv:
            print(json.dumps({{
                "protocol_name": "casa_msexplore_task",
                "protocol_version": {protocol_version},
                "surface_kind": "task",
                "binary_version": {version!r},
            }}))
            raise SystemExit(0)

        if "--json-run" in sys.argv:
            payload = json.load(sys.stdin)
            print(json.dumps({{
                "kind": payload["kind"],
                "result": {{
                    "request": payload["request"],
                    "binary_version": {version!r},
                }},
            }}))
            raise SystemExit(0)

        if "--json-schema" in sys.argv:
            print(json.dumps({{
                "request_schema": {{"definitions": {{}}}},
                "result_schema": {{}},
                "protocol": {{
                    "protocol_name": "casa_msexplore_task",
                    "protocol_version": {protocol_version},
                    "surface_kind": "task",
                    "binary_version": {version!r},
                }}
            }}))
            raise SystemExit(0)

        raise SystemExit("unexpected argv: " + " ".join(sys.argv[1:]))
        """
    )
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _write_failing_protocol_binary(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    script = textwrap.dedent(
        """\
        #!/usr/bin/env python3
        import sys

        if "--protocol-info" in sys.argv:
            print("protocol-info crashed", file=sys.stderr)
            raise SystemExit(7)

        raise SystemExit("unexpected argv: " + " ".join(sys.argv[1:]))
        """
    )
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path
