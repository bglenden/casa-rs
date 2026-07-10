from __future__ import annotations

from pathlib import Path

import pytest

from casars.parameters import SessionParameters
from casars.sessions import SessionProtocolError, open as open_session
from casars.sessions import open_imexplore, open_tablebrowser


def _fake_session_binary(path: Path) -> Path:
    path.write_text(
        """#!/usr/bin/env python3
import json
import sys
for line in sys.stdin:
    request = json.loads(line)
    print(json.dumps({
        "version": 1,
        "response": {"response": "snapshot", "echo": request["command"]},
    }), flush=True)
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _fake_rejecting_tablebrowser(path: Path) -> Path:
    path.write_text(
        """#!/usr/bin/env python3
import json
import sys
for line in sys.stdin:
    request = json.loads(line)
    command = request["command"]
    if (
        command["command"] == "configure"
        and command["parameters"]["content_mode"] == "detailed"
    ):
        response = {
            "version": 1,
            "response": {
                "response": "error",
                "code": "rejected_configuration",
                "message": "detailed mode rejected",
            },
        }
    else:
        response = {
            "version": 1,
            "response": {"response": "snapshot", "echo": command},
        }
    print(json.dumps(response), flush=True)
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _fake_stateful_rejecting_imexplore(path: Path) -> Path:
    path.write_text(
        """#!/usr/bin/env python3
import json
import sys
state = {"contentmode": "raster", "region": {"kind": "none"}}
for line in sys.stdin:
    request = json.loads(line)
    command = request["command"]
    error = None
    if command["command"] == "set_plane_content_mode":
        state["contentmode"] = command["mode"]
    elif command["command"] == "set_selection_references":
        mask = command.get("mask")
        if mask is not None and mask.get("kind") == "expression":
            error = {
                "response": "error",
                "code": "unsupported_mask_expression",
                "message": "mask expression rejected after an earlier command",
            }
        elif command.get("region") is not None:
            state["region"] = command["region"]
    response = error or {
        "response": "snapshot",
        "contentmode": state["contentmode"],
        "region": state["region"],
        "echo": command,
    }
    print(json.dumps({"version": 1, "response": response}), flush=True)
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def test_open_imexplore_resolves_profile_and_records_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "imexplore")
    image = tmp_path / "test.image"

    with open_imexplore(
        image,
        start="defaults",
        workspace=tmp_path,
        binary=binary,
        overrides={"stretch": "manual", "clip_low": "1", "clip_high": "2"},
    ) as session:
        assert session.presentation == {
            "contentmode": "raster",
            "colormap": "gray",
            "movieaxis": "auto",
            "fps": 1,
            "loop": False,
        }
        command = session.first_response["response"]["echo"]
        assert command["command"] == "open_root"
        assert command["path"] == str(image)
        assert command["parameters"]["stretch"] == "manual"
        assert command["parameters"]["clip_low"] == "1"
        response = session.request({"command": "get_snapshot"})
        assert response["response"]["echo"]["command"] == "get_snapshot"
        assert session.warnings == []

    remembered = SessionParameters.last("imexplore", workspace=tmp_path)
    assert remembered["image"] == str(image)
    assert remembered["stretch"] == "manual"


def test_open_tablebrowser_uses_existing_jsonl_open_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "tablebrowser")
    table = tmp_path / "test.table"

    with open_tablebrowser(
        table,
        start="defaults",
        workspace=tmp_path,
        binary=binary,
        width=90,
        height=28,
    ) as session:
        command = session.first_response["response"]["echo"]
        assert command == {
            "command": "open_root",
            "path": str(table),
            "viewport": {"width": 90, "height": 28, "inspector_height": 10},
        }
        configured = session.startup_responses[0]["response"]["echo"]
        assert configured["command"] == "configure"
        assert configured["parameters"] == {
            "view": "overview",
            "row_start": 0,
            "row_count": 100,
            "linked_table": None,
            "bookmark": None,
            "content_mode": "auto",
        }

    remembered = SessionParameters.last("tablebrowser", workspace=tmp_path)
    assert remembered["table"] == str(table)


def test_imexplore_applies_declarative_startup_view_region_and_mask(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "imexplore")

    with open_imexplore(
        tmp_path / "test.image",
        start="defaults",
        workspace=tmp_path,
        binary=binary,
        overrides={
            "view": "coordinates",
            "contentmode": "spreadsheet",
            "region": "saved-roi",
            "mask": "science-mask",
        },
    ) as session:
        commands = [response["response"]["echo"] for response in session.startup_responses]
        assert [command["command"] for command in commands] == [
            "cycle_view",
            "cycle_view",
            "cycle_view",
            "set_plane_content_mode",
            "set_selection_references",
        ]
        assert commands[-2]["mode"] == "spreadsheet"
        assert commands[-1]["region"] == {
            "kind": "definition",
            "name": "saved-roi",
        }
        assert commands[-1]["mask"] == {"kind": "name", "name": "science-mask"}


def test_tablebrowser_applies_initial_row_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "tablebrowser")

    with open_tablebrowser(
        tmp_path / "test.table",
        start="defaults",
        workspace=tmp_path,
        binary=binary,
        height=28,
        overrides={"view": "rows", "rowstart": 3, "nrow": 5},
    ) as session:
        opened = session.first_response["response"]["echo"]
        assert opened["viewport"]["height"] == 28
        commands = [response["response"]["echo"] for response in session.startup_responses]
        assert [command["command"] for command in commands] == ["configure"]
        assert commands[0]["parameters"]["view"] == "cells"
        assert commands[0]["parameters"]["row_start"] == 3
        assert commands[0]["parameters"]["row_count"] == 5


def test_session_updates_use_typed_durable_protocol_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    image_binary = _fake_session_binary(tmp_path / "imexplore")
    with open_imexplore(
        "test.image", workspace=tmp_path, binary=image_binary
    ) as image_session:
        original_pid = image_session.pid
        image_session.update_parameters(
            contentmode="spreadsheet",
            region="box[[1pix,2pix],[3pix,4pix]]",
            mask="mask0 > 0.5",
        )
        assert image_session.pid != original_pid
        assert image_session.parameters["contentmode"] == "spreadsheet"
        commands = [
            response["response"]["echo"]
            for response in image_session.startup_responses[-2:]
        ]
        assert commands[0] == {
            "command": "set_plane_content_mode",
            "mode": "spreadsheet",
        }
        command = commands[1]
        assert command == {
            "command": "set_selection_references",
            "region": {
                "kind": "expression",
                "expression": "box[[1pix,2pix],[3pix,4pix]]",
            },
            "mask": {"kind": "expression", "expression": "mask0 > 0.5"},
        }

    table_binary = _fake_session_binary(tmp_path / "tablebrowser")
    with open_tablebrowser(
        "test.table", workspace=tmp_path, binary=table_binary
    ) as table_session:
        table_session.update_parameters(
            view="rows",
            rowstart=4,
            nrow=12,
            bookmark="cell:4:DATA",
            contentmode="detailed",
        )
        command = table_session.startup_responses[-1]["response"]["echo"]
        assert command["command"] == "configure"
        assert command["parameters"] == {
            "view": "cells",
            "row_start": 4,
            "row_count": 12,
            "linked_table": None,
            "bookmark": {"kind": "cell", "row": 4, "column": "DATA"},
            "content_mode": "detailed",
        }


@pytest.mark.parametrize(
    ("region", "expected"),
    [
        ("none", {"kind": "none"}),
        ("definition:saved", {"kind": "definition", "name": "saved"}),
        ("file:regions/roi.crtf", {"kind": "file", "path": "regions/roi.crtf"}),
        (
            "box[[1pix,2pix],[3pix,4pix]]",
            {
                "kind": "expression",
                "expression": "box[[1pix,2pix],[3pix,4pix]]",
            },
        ),
    ],
)
def test_imexplore_region_reference_protocol_variants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    region: str,
    expected: dict[str, str],
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "imexplore")
    with open_imexplore(
        "test.image",
        workspace=tmp_path,
        binary=binary,
        overrides={"region": region},
        save_last=False,
    ) as session:
        command = session.startup_responses[-1]["response"]["echo"]
        assert command == {
            "command": "set_selection_references",
            "region": expected,
            "mask": None,
        }


@pytest.mark.parametrize(
    ("bookmark", "expected"),
    [
        ("cell:4:DATA", {"kind": "cell", "row": 4, "column": "DATA"}),
        (
            "table-keyword:MEASINFO.Ref",
            {"kind": "table_keyword", "path": ["MEASINFO", "Ref"]},
        ),
        (
            "column-keyword:DATA:MEASINFO/Ref",
            {
                "kind": "column_keyword",
                "column": "DATA",
                "path": ["MEASINFO", "Ref"],
            },
        ),
        ("subtable:FIELD", {"kind": "subtable", "name": "FIELD"}),
    ],
)
def test_tablebrowser_bookmark_protocol_variants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bookmark: str,
    expected: dict[str, object],
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "tablebrowser")
    with open_tablebrowser(
        "test.table",
        workspace=tmp_path,
        binary=binary,
        overrides={"bookmark": bookmark},
        save_last=False,
    ) as session:
        command = session.startup_responses[-1]["response"]["echo"]
        assert command["command"] == "configure"
        assert command["parameters"]["bookmark"] == expected


def test_rejected_durable_update_preserves_parameters_and_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_rejecting_tablebrowser(tmp_path / "tablebrowser")

    with open_tablebrowser(
        "test.table", workspace=tmp_path, binary=binary
    ) as session:
        assert session.parameters["contentmode"] == "auto"
        with pytest.raises(SessionProtocolError, match="rejected_configuration"):
            session.update_parameters(contentmode="detailed")
        assert session.parameters["contentmode"] == "auto"

    remembered = SessionParameters.last("tablebrowser", workspace=tmp_path)
    assert remembered["contentmode"] == "auto"


def test_rejected_multicommand_imexplore_update_preserves_backend_parameters_and_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_stateful_rejecting_imexplore(tmp_path / "imexplore")

    with open_imexplore("test.image", workspace=tmp_path, binary=binary) as session:
        original_pid = session.pid
        with pytest.raises(SessionProtocolError, match="unsupported_mask_expression"):
            session.update_parameters(
                contentmode="spreadsheet",
                region="box[[1pix,2pix],[3pix,4pix]]",
                mask="mask0 > 0.5",
            )

        assert session.pid == original_pid
        assert session.parameters["contentmode"] == "raster"
        assert session.parameters["region"] == "none"
        assert session.parameters["mask"] == "none"
        backend = session.request({"command": "get_snapshot"})["response"]
        assert backend["contentmode"] == "raster"
        assert backend["region"] == {"kind": "none"}

    remembered = SessionParameters.last("imexplore", workspace=tmp_path)
    assert remembered["contentmode"] == "raster"
    assert remembered["region"] == "none"
    assert remembered["mask"] == "none"


def test_live_root_resource_changes_require_reopen_and_preserve_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)

    image_binary = _fake_session_binary(tmp_path / "imexplore")
    with open_imexplore(
        "original.image", workspace=tmp_path, binary=image_binary
    ) as image_session:
        original_pid = image_session.pid
        with pytest.raises(ValueError, match="changing 'image' requires opening"):
            image_session.update_parameters(image="replacement.image")
        assert image_session.pid == original_pid
        assert image_session.parameters["image"] == "original.image"
        assert image_session.first_response["response"]["echo"]["path"] == "original.image"
    assert SessionParameters.last("imexplore", workspace=tmp_path)["image"] == "original.image"

    table_binary = _fake_session_binary(tmp_path / "tablebrowser")
    with open_tablebrowser(
        "original.table", workspace=tmp_path, binary=table_binary
    ) as table_session:
        original_pid = table_session.pid
        with pytest.raises(ValueError, match="changing 'table' requires opening"):
            table_session.update_parameters(table="replacement.table")
        assert table_session.pid == original_pid
        assert table_session.parameters["table"] == "original.table"
        assert table_session.first_response["response"]["echo"]["path"] == "original.table"
    assert SessionParameters.last("tablebrowser", workspace=tmp_path)["table"] == "original.table"


def test_failed_session_open_preserves_previous_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    previous = SessionParameters.defaults("tablebrowser", workspace=tmp_path)
    previous["table"] = "previous.table"
    previous.write_last()

    binary = _fake_rejecting_tablebrowser(tmp_path / "tablebrowser")
    with pytest.raises(SessionProtocolError, match="rejected_configuration"):
        open_tablebrowser(
            "failed.table",
            workspace=tmp_path,
            binary=binary,
            overrides={"contentmode": "detailed"},
        )

    remembered = SessionParameters.last("tablebrowser", workspace=tmp_path)
    assert remembered["table"] == "previous.table"


def test_generic_session_open_dispatches_from_typed_parameters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "imexplore")
    parameters = SessionParameters.defaults("imexplore", workspace=tmp_path)
    parameters["image"] = "generic.image"
    with open_session("imexplore", parameters=parameters, binary=binary) as session:
        assert session.first_response["response"]["echo"]["path"] == "generic.image"


def test_session_sources_are_mutually_exclusive_and_missing_last_is_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "imexplore")
    parameters = SessionParameters.defaults("imexplore", workspace=tmp_path)
    parameters["image"] = "generic.image"

    with pytest.raises(ValueError, match="mutually exclusive"):
        open_session(
            "imexplore",
            parameters=parameters,
            start="last",
            binary=binary,
        )
    with pytest.raises(FileNotFoundError):
        open_session(
            "imexplore",
            start="last",
            workspace=tmp_path,
            binary=binary,
        )


def test_session_no_save_last_and_durable_update_tracking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    binary = _fake_session_binary(tmp_path / "imexplore")

    with open_imexplore(
        "unsaved.image",
        workspace=tmp_path,
        binary=binary,
        save_last=False,
    ):
        pass
    with pytest.raises(FileNotFoundError):
        SessionParameters.last("imexplore", workspace=tmp_path)

    with open_imexplore(
        "durable.image",
        workspace=tmp_path,
        binary=binary,
    ) as session:
        session.request({"command": "set_cursor", "x": 5, "y": 7})
        session.update_parameters(colormap="inferno", loop=True, fps=7)
        assert session.presentation["colormap"] == "inferno"
        assert session.presentation["loop"] is True

    remembered = SessionParameters.last("imexplore", workspace=tmp_path)
    assert remembered["image"] == "durable.image"
    assert remembered["colormap"] == "inferno"
    assert remembered["loop"] is True
    assert remembered["fps"] == 7
