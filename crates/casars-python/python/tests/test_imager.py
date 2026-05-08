from __future__ import annotations

from pathlib import Path
import stat
import textwrap

import pytest

from casars import _task_runtime
from casars.tasks import imager


@pytest.fixture(autouse=True)
def reset_imager_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    imager.configure(binary=None)
    monkeypatch.delenv("CASARS_IMAGER_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_binary_lookup_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_binary = _write_stub_binary(tmp_path / "env" / "casars-imager", version="env")
    configured_binary = _write_stub_binary(
        tmp_path / "configured" / "casars-imager",
        version="configured",
    )
    explicit_binary = _write_stub_binary(
        tmp_path / "explicit" / "casars-imager",
        version="explicit",
    )

    monkeypatch.setenv("CASARS_IMAGER_BIN", str(env_binary))
    imager.configure(binary=configured_binary)

    assert imager.protocol_info().binary_version == "configured"
    assert imager.protocol_info(binary=explicit_binary).binary_version == "explicit"

    imager.configure(binary=None)
    assert imager.protocol_info().binary_version == "env"


def test_suite_root_env_precedes_repo_local(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _write_stub_binary(suite_root / "bin" / "casars-imager", version="suite")

    monkeypatch.setenv("CASARS_SUITE_ROOT", str(suite_root))

    assert imager.protocol_info().binary_version == "suite"


def test_package_relative_suite_layout_is_discovered(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    binary = _write_stub_binary(
        suite_root / "bin" / "casars-imager",
        version="suite-relative",
    )
    module_file = suite_root / "python" / "site-packages" / "casars" / "_task_runtime.py"
    module_file.parent.mkdir(parents=True, exist_ok=True)
    module_file.write_text("# suite layout test\n", encoding="utf-8")

    assert _task_runtime._find_installed_suite_binary(
        "casars-imager", module_file=module_file
    ) == str(binary)


def test_protocol_mismatch_fails_fast(tmp_path: Path) -> None:
    binary = _write_stub_binary(
        tmp_path / "bad" / "casars-imager",
        version="bad",
        protocol_version=99,
    )

    with pytest.raises(RuntimeError, match="expected protocol version"):
        imager.run({"measurement_set": "dataset.ms"}, binary=binary)


def test_protocol_info_subprocess_failures_raise_imager_invocation_error(
    tmp_path: Path,
) -> None:
    binary = _write_failing_protocol_binary(tmp_path / "bad-protocol" / "casars-imager")

    with pytest.raises(_task_runtime.ImagerInvocationError, match="protocol-info crashed"):
        imager.protocol_info(binary=binary)


def test_mfs_wrapper_encodes_pythonic_arguments(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "casars-imager", version="ok")

    result = imager.mfs(
        "twhya_calibrated.ms",
        "products/twhya",
        image_size=256,
        cell_arcsec=0.1,
        field_ids=[5],
        spw="0:10~20",
        data_column="corrected_data",
        save_model="modelcolumn",
        start_model="products/seed.model",
        outlier_file="products/outliers.txt",
        correlation="I",
        weighting="briggs",
        robust=0.25,
        use_pointing=True,
        niter=100,
        threshold_jy=0.001,
        use_mask="auto-multithresh",
        auto_mask={"sidelobe_threshold": 2.0, "noise_threshold": 4.25},
        mask_boxes=[(100, 100, 150, 150)],
        binary=binary,
    )

    assert result["kind"] == "run"
    request = result["result"]["request"]
    assert request["measurement_set"] == "twhya_calibrated.ms"
    assert request["image_name"] == "products/twhya"
    assert request["image_size"] == 256
    assert request["cell_arcsec"] == 0.1
    assert request["field_ids"] == [5]
    assert request["spw_selector"] == "0:10~20"
    assert request["data_column"] == "corrected_data"
    assert request["save_model"] == "modelcolumn"
    assert request["start_model"] == "products/seed.model"
    assert request["outlier_file"] == "products/outliers.txt"
    assert request["correlation"] == "I"
    assert request["spectral_mode"] == "mfs"
    assert request["weighting"] == {"kind": "briggs", "robust": 0.25}
    assert request["use_pointing"] is True
    assert request["niter"] == 100
    assert request["threshold_jy"] == 0.001
    assert request["use_mask"] == "auto-multithresh"
    assert request["auto_mask"]["sidelobe_threshold"] == 2.0
    assert request["auto_mask"]["noise_threshold"] == 4.25
    assert request["mask_boxes"] == [[100, 100, 150, 150]]


def test_wrapper_encodes_briggs_bandwidth_taper(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "casars-imager", version="ok")

    result = imager.mfs(
        "twhya_calibrated.ms",
        "products/twhya",
        image_size=128,
        cell_arcsec=0.1,
        weighting="briggsbwtaper",
        robust=-0.5,
        binary=binary,
    )

    request = result["result"]["request"]
    assert request["weighting"] == {"kind": "briggs_bw_taper", "robust": -0.5}


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
                "protocol_name": "casa_imager_task",
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
                    "protocol_name": "casa_imager_task",
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
