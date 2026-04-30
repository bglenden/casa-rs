from __future__ import annotations

from pathlib import Path
import stat
import textwrap

import pytest

from casars import _task_runtime
from casars.tasks import image_analysis


@pytest.fixture(autouse=True)
def reset_image_analysis_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    image_analysis.configure(
        imexplore_binary=None,
        immoments_binary=None,
        impv_binary=None,
        exportfits_binary=None,
        importfits_binary=None,
    )
    monkeypatch.delenv("CASARS_IMEXPLORE_BIN", raising=False)
    monkeypatch.delenv("CASARS_IMMOMENTS_BIN", raising=False)
    monkeypatch.delenv("CASARS_IMPV_BIN", raising=False)
    monkeypatch.delenv("CASARS_EXPORTFITS_BIN", raising=False)
    monkeypatch.delenv("CASARS_IMPORTFITS_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_imhead_wrapper_uses_imexplore_json_subcommand(tmp_path: Path) -> None:
    binary = _write_imexplore_stub(tmp_path / "bin" / "imexplore")

    result = image_analysis.imhead("twhya_cont.image", binary=binary)

    assert result["subcommand"] == "imhead"
    assert result["imagename"] == "twhya_cont.image"


def test_imstat_wrapper_encodes_box_and_channels(tmp_path: Path) -> None:
    binary = _write_imexplore_stub(tmp_path / "bin" / "imexplore")

    result = image_analysis.imstat(
        "twhya_n2hp.image",
        box="100,100,150,150",
        chans="0~4",
        binary=binary,
    )

    assert result["subcommand"] == "imstat"
    assert result["box"] == "100,100,150,150"
    assert result["chans"] == "0~4"


def test_immoments_wrapper_encodes_task_request(tmp_path: Path) -> None:
    binary = _write_task_stub(tmp_path / "bin" / "immoments", version="ok")

    result = image_analysis.immoments(
        "twhya_n2hp.image",
        outfile="twhya_n2hp.mom0",
        moments=0,
        chans="4~12",
        includepix=(0.03, 100.0),
        overwrite=True,
        binary=binary,
    )

    assert result["kind"] == "immoments"
    request = result["result"]["request"]
    assert request["imagename"] == "twhya_n2hp.image"
    assert request["outfile"] == "twhya_n2hp.mom0"
    assert request["moments"] == 0
    assert request["chans"] == "4~12"
    assert request["includepix"] == [0.03, 100.0]
    assert request["overwrite"] is True


def test_exportfits_wrapper_encodes_task_request(tmp_path: Path) -> None:
    binary = _write_task_stub(tmp_path / "bin" / "exportfits", version="ok")

    result = image_analysis.exportfits(
        "twhya_n2hp.image",
        "twhya_n2hp.fits",
        velocity=True,
        overwrite=True,
        binary=binary,
    )

    assert result["kind"] == "exportfits"
    request = result["result"]["request"]
    assert request["imagename"] == "twhya_n2hp.image"
    assert request["fitsimage"] == "twhya_n2hp.fits"
    assert request["velocity"] is True
    assert request["overwrite"] is True


def test_impv_wrapper_encodes_task_request(tmp_path: Path) -> None:
    binary = _write_task_stub(tmp_path / "bin" / "impv", version="ok")

    result = image_analysis.impv(
        "IRC10216_HC3N.cube.image",
        outfile="IRC10216_HC3N.pv.image",
        start="120,140",
        end="180,160",
        width=3,
        chans="11~40",
        overwrite=True,
        binary=binary,
    )

    assert result["kind"] == "impv"
    request = result["result"]["request"]
    assert request["imagename"] == "IRC10216_HC3N.cube.image"
    assert request["outfile"] == "IRC10216_HC3N.pv.image"
    assert request["start"] == "120,140"
    assert request["end"] == "180,160"
    assert request["width"] == 3
    assert request["chans"] == "11~40"
    assert request["overwrite"] is True


def test_importfits_wrapper_encodes_task_request(tmp_path: Path) -> None:
    binary = _write_task_stub(tmp_path / "bin" / "importfits", version="ok")

    result = image_analysis.importfits(
        "twhya_cont.fits",
        "twhya_cont.image",
        overwrite=True,
        binary=binary,
    )

    assert result["kind"] == "importfits"
    request = result["result"]["request"]
    assert request["fitsimage"] == "twhya_cont.fits"
    assert request["imagename"] == "twhya_cont.image"
    assert request["overwrite"] is True


def test_protocol_mismatch_fails_fast(tmp_path: Path) -> None:
    binary = _write_task_stub(tmp_path / "bad" / "immoments", version="bad", protocol_version=99)

    with pytest.raises(RuntimeError, match="expected protocol version"):
        image_analysis.immoments("x.image", outfile="x.mom0", binary=binary)


def _write_imexplore_stub(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    script = textwrap.dedent(
        """\
        #!/usr/bin/env python3
        import json
        import sys

        subcommand = sys.argv[1]
        payload = {
            "subcommand": subcommand,
            "imagename": sys.argv[2],
            "json": "--json" in sys.argv,
        }
        if "--box" in sys.argv:
            payload["box"] = sys.argv[sys.argv.index("--box") + 1]
        if "--chans" in sys.argv:
            payload["chans"] = sys.argv[sys.argv.index("--chans") + 1]
        print(json.dumps(payload))
        """
    )
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _write_task_stub(
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
                "protocol_name": "casa_image_analysis_task",
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

        raise SystemExit("unexpected argv: " + " ".join(sys.argv[1:]))
        """
    )
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path
