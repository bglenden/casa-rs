from __future__ import annotations

from pathlib import Path
import stat
import textwrap

import pytest

from casars import _task_runtime
from casars.tasks import simobserve


@pytest.fixture(autouse=True)
def reset_simobserve_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    simobserve.configure(binary=None)
    monkeypatch.delenv("CASARS_SIMOBSERVE_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_binary_lookup_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_binary = _write_stub_binary(tmp_path / "env" / "simobserve", version="env")
    configured_binary = _write_stub_binary(
        tmp_path / "configured" / "simobserve", version="configured"
    )
    explicit_binary = _write_stub_binary(
        tmp_path / "explicit" / "simobserve", version="explicit"
    )

    monkeypatch.setenv("CASARS_SIMOBSERVE_BIN", str(env_binary))
    simobserve.configure(binary=configured_binary)

    assert simobserve.protocol_info().binary_version == "configured"
    assert simobserve.protocol_info(binary=explicit_binary).binary_version == "explicit"

    simobserve.configure(binary=None)
    assert simobserve.protocol_info().binary_version == "env"


def test_protocol_mismatch_fails_fast(tmp_path: Path) -> None:
    binary = _write_stub_binary(
        tmp_path / "bad" / "simobserve",
        version="bad",
        protocol_version=99,
    )

    with pytest.raises(RuntimeError, match="expected protocol version"):
        simobserve.vla_ppdisk("model.fits", "out.ms", binary=binary)


def test_protocol_info_subprocess_failures_raise_simobserve_invocation_error(
    tmp_path: Path,
) -> None:
    binary = _write_failing_protocol_binary(tmp_path / "bad-protocol" / "simobserve")

    with pytest.raises(_task_runtime.SimobserveInvocationError, match="protocol-info crashed"):
        simobserve.protocol_info(binary=binary)


def test_wrapper_encodes_pythonic_arguments(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "simobserve", version="ok")

    result = simobserve.vla_ppdisk(
        "ppdisk.fits",
        "ppdisk.ms",
        overwrite=True,
        model_peak_jy_per_pixel=3.0e-5,
        phase_center_rad=(1.0, -0.5),
        duration_seconds=30.0,
        integration_seconds=10.0,
        channel_count=4,
        predict_model=False,
        corruption={
            "seed": 42,
            "noise_stddev_jy": 0.001,
            "gain_phase": {
                "amplitude_stddev": 0.05,
                "phase_stddev_rad": 0.02,
            },
            "bandpass": {
                "amplitude_stddev": 0.03,
                "phase_stddev_rad": 0.04,
            },
            "polarization_leakage": {"amplitude": 0.01},
            "pointing": {"offset_rad": [1.0e-5, -2.0e-5]},
        },
        binary=binary,
    )

    assert result["kind"] == "run"
    request = result["result"]["request"]
    assert request["model_image"] == "ppdisk.fits"
    assert request["model_peak_jy_per_pixel"] == 3.0e-5
    assert request["output_ms"] == "ppdisk.ms"
    assert request["overwrite"] is True
    assert request["phase_center_rad"] == [1.0, -0.5]
    assert request["duration_seconds"] == 30.0
    assert request["integration_seconds"] == 10.0
    assert request["spectral_setup"]["channel_count"] == 4
    assert request["predict_model"] is False
    assert request["corruption"]["seed"] == 42
    assert request["corruption"]["noise_stddev_jy"] == 0.001
    assert request["corruption"]["gain_phase"]["phase_stddev_rad"] == 0.02
    assert request["corruption"]["bandpass"]["amplitude_stddev"] == 0.03
    assert request["corruption"]["polarization_leakage"]["amplitude"] == 0.01
    assert request["corruption"]["pointing"]["offset_rad"] == [1.0e-5, -2.0e-5]


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
                "protocol_name": "casa_simobserve_task",
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
                    "protocol_name": "casa_simobserve_task",
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
