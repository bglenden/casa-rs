from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import textwrap

import pytest

from casars import _task_runtime
from casars.tasks import importvla


REPO_ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(autouse=True)
def reset_importvla_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    importvla.configure(binary=None)
    monkeypatch.delenv("CASARS_IMPORTVLA_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_binary_lookup_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_binary = _write_stub_binary(
        tmp_path / "env" / "casars-importvla",
        version="env",
    )
    configured_binary = _write_stub_binary(
        tmp_path / "configured" / "casars-importvla",
        version="configured",
    )
    explicit_binary = _write_stub_binary(
        tmp_path / "explicit" / "casars-importvla",
        version="explicit",
    )

    monkeypatch.setenv("CASARS_IMPORTVLA_BIN", str(env_binary))
    importvla.configure(binary=configured_binary)

    assert importvla.protocol_info().binary_version == "configured"
    assert importvla.protocol_info(binary=explicit_binary).binary_version == "explicit"

    importvla.configure(binary=None)
    assert importvla.protocol_info().binary_version == "env"


def test_suite_root_env_precedes_repo_local(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _write_stub_binary(suite_root / "bin" / "casars-importvla", version="suite")

    monkeypatch.setenv("CASARS_SUITE_ROOT", str(suite_root))

    assert importvla.protocol_info().binary_version == "suite"


def test_package_relative_suite_layout_is_discovered(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    binary = _write_stub_binary(
        suite_root / "bin" / "casars-importvla",
        version="suite-relative",
    )
    module_file = suite_root / "python" / "site-packages" / "casars" / "_task_runtime.py"
    module_file.parent.mkdir(parents=True, exist_ok=True)
    module_file.write_text("# suite layout test\n", encoding="utf-8")

    assert _task_runtime._find_installed_suite_binary(
        "casars-importvla", module_file=module_file
    ) == str(binary)


def test_protocol_mismatch_fails_fast(tmp_path: Path) -> None:
    binary = _write_stub_binary(
        tmp_path / "bad" / "casars-importvla",
        version="bad",
        protocol_version=99,
    )

    with pytest.raises(RuntimeError, match="expected protocol version"):
        importvla.scan(["archive.exp"], binary=binary)


def test_protocol_info_subprocess_failures_raise_importvla_invocation_error(
    tmp_path: Path,
) -> None:
    binary = _write_failing_protocol_binary(tmp_path / "bad-protocol" / "casars-importvla")

    with pytest.raises(_task_runtime.ImportVlaInvocationError, match="protocol-info crashed"):
        importvla.protocol_info(binary=binary)


def test_wrapper_encodes_pythonic_arguments(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "casars-importvla", version="ok")

    result = importvla.import_archive(
        ["first.exp", "second.xp1"],
        "out.ms",
        bandname="ka",
        frequencytol_hz=12345.0,
        project="AG189",
        starttime="1985/05/01/00:00:00",
        stoptime="1985/05/01/12:00:00",
        applytsys=False,
        autocorr=True,
        antnamescheme="old",
        keepblanks=True,
        evlabands=True,
        binary=binary,
    )

    assert result["kind"] == "import"
    request = result["result"]["request"]["options"]
    assert request["archivefiles"] == ["first.exp", "second.xp1"]
    assert request["vis"] == "out.ms"
    assert request["bandname"] == "Ka"
    assert request["frequencytol_hz"] == 12345.0
    assert request["project"] == "AG189"
    assert request["applytsys"] is False
    assert request["autocorr"] is True
    assert request["antnamescheme"] == "Old"
    assert request["keepblanks"] is True
    assert request["evlabands"] is True


def test_signature_parity_against_rust_schema() -> None:
    binary = _build_importvla_binary()
    importvla.validate_signature_parity(binary=binary)


def _build_importvla_binary() -> str:
    subprocess.run(
        ["cargo", "build", "-q", "-p", "casars-importvla", "--bin", "casars-importvla"],
        cwd=REPO_ROOT,
        check=True,
    )
    suffix = ".exe" if os.name == "nt" else ""
    return str(REPO_ROOT / "target" / "debug" / f"casars-importvla{suffix}")


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
                "protocol_name": "casa_importvla_task",
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
                    "protocol_name": "casa_importvla_task",
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
