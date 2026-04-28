from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import textwrap

import pytest

from casars import _task_runtime
from casars.tasks import calibrate


REPO_ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(autouse=True)
def reset_calibrate_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    calibrate.configure(binary=None)
    monkeypatch.delenv("CASARS_CALIBRATE_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_binary_lookup_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_binary = _write_stub_binary(tmp_path / "env" / "calibrate", version="env")
    configured_binary = _write_stub_binary(tmp_path / "configured" / "calibrate", version="configured")
    explicit_binary = _write_stub_binary(tmp_path / "explicit" / "calibrate", version="explicit")

    monkeypatch.setenv("CASARS_CALIBRATE_BIN", str(env_binary))
    calibrate.configure(binary=configured_binary)

    assert calibrate.protocol_info().binary_version == "configured"
    assert calibrate.protocol_info(binary=explicit_binary).binary_version == "explicit"

    calibrate.configure(binary=None)
    assert calibrate.protocol_info().binary_version == "env"


def test_suite_root_env_precedes_repo_local(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _write_stub_binary(suite_root / "bin" / "calibrate", version="suite")

    monkeypatch.setenv("CASARS_SUITE_ROOT", str(suite_root))

    assert calibrate.protocol_info().binary_version == "suite"


def test_package_relative_suite_layout_is_discovered(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    binary = _write_stub_binary(suite_root / "bin" / "calibrate", version="suite-relative")
    module_file = suite_root / "python" / "site-packages" / "casars" / "_task_runtime.py"
    module_file.parent.mkdir(parents=True, exist_ok=True)
    module_file.write_text("# suite layout test\n", encoding="utf-8")

    assert _task_runtime._find_installed_suite_binary(module_file=module_file) == str(binary)


def test_standard_suite_root_is_discovered(tmp_path: Path) -> None:
    home = tmp_path / "home"
    binary = _write_stub_binary(
        home / ".local" / "opt" / "casa-rs" / "current" / "bin" / "calibrate",
        version="standard-root",
    )
    module_file = tmp_path / "venv" / "lib" / "python3.14" / "site-packages" / "casars" / "_task_runtime.py"
    module_file.parent.mkdir(parents=True, exist_ok=True)
    module_file.write_text("# standard root fallback test\n", encoding="utf-8")

    assert _task_runtime._find_installed_suite_binary(module_file=module_file, home=home) == str(binary)


def test_protocol_mismatch_fails_fast(tmp_path: Path) -> None:
    binary = _write_stub_binary(
        tmp_path / "bad" / "calibrate",
        version="bad",
        protocol_version=99,
    )

    with pytest.raises(RuntimeError, match="expected protocol version"):
        calibrate.summary(["phase.gcal"], binary=binary)


def test_protocol_info_subprocess_failures_raise_calibration_invocation_error(
    tmp_path: Path,
) -> None:
    binary = _write_failing_protocol_binary(tmp_path / "bad-protocol" / "calibrate")

    with pytest.raises(_task_runtime.CalibrationInvocationError, match="protocol-info crashed"):
        calibrate.protocol_info(binary=binary)


def test_wrapper_encodes_pythonic_arguments(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "calibrate", version="ok")

    export_result = calibrate.export_corrected_data(
        "calibrated.ms",
        "selfcal.ms",
        selection=calibrate.Selection(field="5", spw="0"),
        binary=binary,
    )
    export_request = export_result["report"]["request"]
    assert export_result["kind"] == "export_corrected_data"
    assert export_request["input_ms"] == "calibrated.ms"
    assert export_request["output_ms"] == "selfcal.ms"
    assert export_request["selection"]["field"] == "5"
    assert export_request["selection"]["spw"] == "0"

    contsub_result = calibrate.continuum_subtract(
        "selfcal.ms",
        "selfcal.contsub.ms",
        fit_spw="0:0~1;3~4",
        fit_order=1,
        data_column="data",
        selection=calibrate.Selection(field="5", spw="0"),
        binary=binary,
    )
    contsub_request = contsub_result["report"]["request"]
    assert contsub_result["kind"] == "continuum_subtract"
    assert contsub_request["input_ms"] == "selfcal.ms"
    assert contsub_request["output_ms"] == "selfcal.contsub.ms"
    assert contsub_request["fit_spw"] == "0:0~1;3~4"
    assert contsub_request["fit_order"] == 1
    assert contsub_request["data_column"] == "Data"
    assert contsub_request["selection"]["field"] == "5"

    result = calibrate.solve_gain(
        "dataset.ms",
        "gain.gcal",
        refant="ea01",
        selection=calibrate.Selection(field="0", spw="0,1"),
        gain_type="t",
        solve_mode="ap",
        solve_interval=30.0,
        combine=calibrate.SolveCombine(scans=True, fields=False),
        prior_calibration_tables=[
            calibrate.CalibrationTableSpec("phase.gcal", gainfield="nearest", calwt=True)
        ],
        parang=True,
        model_source="model_column",
        min_snr=2.5,
        smodel=(1.0, 0.0, 0.0, 0.0),
        binary=binary,
    )

    assert result["kind"] == "solve_gain"
    request = result["report"]["request"]
    assert request["measurement_set"] == "dataset.ms"
    assert request["output_table"] == "gain.gcal"
    assert request["gain_type"] == "T"
    assert request["solve_mode"] == "AmplitudePhase"
    assert request["solve_interval"] == {"Seconds": 30.0}
    assert request["combine"] == {"scans": True, "fields": False}
    assert request["refant"] == {"AntennaName": "ea01"}
    assert request["selection"]["field"] == "0"
    assert request["selection"]["spw"] == "0,1"
    assert request["prior_calibration_tables"][0]["gainfield"] == "Nearest"
    assert request["prior_calibration_tables"][0]["calwt"] is True
    assert request["model_source"] == "ModelColumn"
    assert request["min_snr"] == 2.5


def test_signature_parity_against_rust_schema() -> None:
    calibrate_binary = _build_calibrate_binary()
    calibrate.validate_signature_parity(binary=calibrate_binary)


def _build_calibrate_binary() -> str:
    subprocess.run(
        ["cargo", "build", "-q", "-p", "casa-calibration", "--bin", "calibrate"],
        cwd=REPO_ROOT,
        check=True,
    )
    suffix = ".exe" if os.name == "nt" else ""
    return str(REPO_ROOT / "target" / "debug" / f"calibrate{suffix}")


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
                "protocol_name": "casa_calibration_task",
                "protocol_version": {protocol_version},
                "surface_kind": "task",
                "binary_version": {version!r},
            }}))
            raise SystemExit(0)

        if "--json-run" in sys.argv:
            payload = json.load(sys.stdin)
            print(json.dumps({{
                "kind": payload["kind"],
                "report": {{
                    "request": payload["request"],
                    "binary_version": {version!r},
                }},
            }}))
            raise SystemExit(0)

        if "--json-schema" in sys.argv:
            print(json.dumps({{"request_schema": {{"definitions": {{}}}}, "result_schema": {{}}, "protocol": {{
                "protocol_name": "casa_calibration_task",
                "protocol_version": {protocol_version},
                "surface_kind": "task",
                "binary_version": {version!r},
            }}}}))
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
