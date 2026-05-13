from __future__ import annotations

from pathlib import Path
import stat
import textwrap

import pytest

from casars import _task_runtime
from casars.tasks import split


@pytest.fixture(autouse=True)
def reset_split_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    split.configure(binary=None)
    monkeypatch.delenv("CASARS_MSTRANSFORM_BIN", raising=False)
    monkeypatch.delenv("CASARS_SUITE_ROOT", raising=False)


def test_split_wrapper_encodes_tutorial_arguments(tmp_path: Path) -> None:
    binary = _write_stub_binary(tmp_path / "ok" / "mstransform")

    result = split.split(
        "twhya_calibrated.ms",
        "twhya_smoothed.ms",
        field="5",
        width=8,
        datacolumn="data",
        binary=binary,
    )

    assert result["output_ms"] == "twhya_smoothed.ms"
    assert result["width"] == 8
    assert result["argv"] == [
        "--vis",
        "twhya_calibrated.ms",
        "--outputvis",
        "twhya_smoothed.ms",
        "--datacolumn",
        "DATA",
        "--width",
        "8",
        "--field",
        "5",
        "--keepflags",
    ]


def test_split_wrapper_raises_transform_invocation_error(tmp_path: Path) -> None:
    binary = _write_failing_binary(tmp_path / "bad" / "mstransform")

    with pytest.raises(_task_runtime.MsTransformInvocationError, match="split failed"):
        split.split("input.ms", "output.ms", binary=binary)


def _write_stub_binary(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    script = textwrap.dedent(
        """\
        #!/usr/bin/env python3
        import json
        import sys

        print(json.dumps({
            "output_ms": sys.argv[sys.argv.index("--outputvis") + 1],
            "width": int(sys.argv[sys.argv.index("--width") + 1]),
            "argv": sys.argv[1:],
        }))
        """
    )
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _write_failing_binary(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    script = textwrap.dedent(
        """\
        #!/usr/bin/env python3
        import sys
        print("split failed", file=sys.stderr)
        raise SystemExit(4)
        """
    )
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path
