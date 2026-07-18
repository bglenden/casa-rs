"""Profile-aware Python clients for the existing browser JSONL protocols."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from os import PathLike
from pathlib import Path
import subprocess
import threading
from typing import Any, Literal, TypeAlias

from . import _core
from ._task_runtime import _resolve_task_binary, resolve_imexplore_binary
from .parameters import ParameterData, SessionParameters

StrPath: TypeAlias = str | PathLike[str]
SessionBaseSource: TypeAlias = Literal["defaults", "last"]
_SESSION_LIFECYCLE = _core.ParameterSessionLifecycle()


class SessionProtocolError(RuntimeError):
    """Raised when a browser process fails or returns a protocol error."""


class JsonlSession:
    """Synchronous request/response client for one long-lived browser process.

    Profile parameters are resolved during startup. Raw :meth:`request` calls
    are transient protocol commands and are deliberately not inferred as
    profile intent; :meth:`update_parameters` is the explicit durable path.
    """

    def __init__(
        self,
        process: subprocess.Popen[str],
        parameters: SessionParameters,
        first_response: dict[str, Any],
        *,
        save_last: bool,
        viewport: Mapping[str, int],
        binary: str,
    ) -> None:
        self._process = process
        self._binary = binary
        self.parameters = parameters
        self.presentation = _presentation_settings(parameters)
        self.first_response = first_response
        self.startup_responses: list[dict[str, Any]] = []
        self.warnings: list[str] = []
        self._lock = threading.Lock()
        self._save_last = save_last
        self._viewport = dict(viewport)
        self._opened = False
        self._last_response = first_response

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def closed(self) -> bool:
        return self._process.poll() is not None

    def request(self, command: Mapping[str, Any]) -> dict[str, Any]:
        """Send one transient protocol command and return its decoded response.

        Raw navigation is never inferred to be profile intent. Use
        :meth:`update_parameters` for an intentional durable setting change.
        """

        if "version" in command and "command" in command:
            envelope = dict(command)
        else:
            envelope = {"version": 1, "command": dict(command)}
        return self._exchange(envelope)

    def update_parameters(self, **values: ParameterData) -> None:
        """Apply validated durable settings and debounce their managed Last save.

        The typed draft is committed only after every required backend command
        succeeds. Root ``image``/``table`` changes require opening a new
        session and are rejected here.
        """

        candidate = self.parameters.copy()
        candidate.update(**values)
        root_name = "image" if candidate.surface == "imexplore" else "table"
        if root_name in values and candidate[root_name] != self.parameters[root_name]:
            raise ValueError(f"changing {root_name!r} requires opening a new session")
        if candidate.surface == "imexplore":
            _apply_imexplore_parameter_update(self, candidate, set(values))
        elif candidate.surface == "tablebrowser":
            _apply_tablebrowser_parameter_update(self, candidate, set(values))
        else:  # pragma: no cover - SessionParameters rejects other surfaces
            raise ValueError(f"unsupported session surface {candidate.surface!r}")
        self.parameters = candidate
        self.presentation = _presentation_settings(candidate)
        self._queue_last_save()

    def close(self) -> None:
        self.warnings.extend(
            _SESSION_LIFECYCLE.flush(self.parameters.surface, self.parameters.workspace)
        )
        _close_process(self._process)

    def __enter__(self) -> "JsonlSession":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _exchange(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self.closed:
                raise SessionProtocolError("browser session process is closed")
            assert self._process.stdin is not None
            assert self._process.stdout is not None
            self._process.stdin.write(
                json.dumps(envelope, separators=(",", ":")) + "\n"
            )
            self._process.stdin.flush()
            line = self._process.stdout.readline()
            if not line:
                stderr = ""
                if (
                    self._process.stderr is not None
                    and self._process.poll() is not None
                ):
                    stderr = self._process.stderr.read().strip()
                detail = f": {stderr}" if stderr else ""
                raise SessionProtocolError(
                    f"browser session ended without a response{detail}"
                )
            try:
                response = json.loads(line)
            except json.JSONDecodeError as error:
                raise SessionProtocolError(
                    f"browser returned invalid JSON: {error}"
                ) from error
            payload = response.get("response", {})
            if isinstance(payload, dict) and payload.get("response") == "error":
                raise SessionProtocolError(
                    f"{payload.get('code', 'session_error')}: "
                    f"{payload.get('message', 'browser command failed')}"
                )
            self._last_response = response
            return response

    def _startup_exchange(self, command: Mapping[str, Any]) -> dict[str, Any]:
        response = self._exchange({"version": 1, "command": dict(command)})
        self.startup_responses.append(response)
        return response

    def _latest_response(self) -> dict[str, Any]:
        return self._last_response

    def _mark_opened(self) -> None:
        self._opened = True
        try:
            self.warnings.extend(
                _SESSION_LIFECYCLE.opened(
                    self.parameters.surface,
                    self.parameters.workspace,
                    self.parameters._resolved_values_json(),
                    self._save_last,
                )
            )
        except Exception as error:  # automatic state failure must not fail valid science
            self.warnings.append(f"could not save Last parameters: {error}")

    def _queue_last_save(self) -> None:
        if not self._save_last or not self._opened:
            return
        try:
            self.warnings.extend(
                _SESSION_LIFECYCLE.accepted_durable_change(
                    self.parameters.surface,
                    self.parameters.workspace,
                    self.parameters._resolved_values_json(),
                    self._save_last,
                )
            )
        except Exception as error:  # automatic state failure must not fail valid science
            self.warnings.append(f"could not save Last parameters: {error}")


def open(
    surface: str,
    *,
    parameters: SessionParameters | None = None,
    profile: StrPath | None = None,
    start: SessionBaseSource = "defaults",
    save_last: bool = True,
    **options: Any,
) -> JsonlSession:
    """Open either catalog session with one common source-selection API."""

    if surface == "imexplore":
        return open_imexplore(
            parameters=parameters,
            profile=profile,
            start=start,
            save_last=save_last,
            **options,
        )
    if surface == "tablebrowser":
        return open_tablebrowser(
            parameters=parameters,
            profile=profile,
            start=start,
            save_last=save_last,
            **options,
        )
    raise ValueError(
        f"unknown session surface {surface!r}; expected 'imexplore' or 'tablebrowser'"
    )


def open_imexplore(
    image: StrPath | None = None,
    *,
    parameters: SessionParameters | None = None,
    profile: StrPath | None = None,
    start: SessionBaseSource = "defaults",
    save_last: bool = True,
    workspace: StrPath | None = None,
    binary: StrPath | None = None,
    width: int = 120,
    height: int = 36,
    inspector_height: int = 10,
    plane_width: int = 512,
    plane_height: int = 384,
    overrides: Mapping[str, ParameterData] | None = None,
) -> JsonlSession:
    """Resolve an ``imexplore`` startup profile and open its JSONL session."""

    resolved = _resolve_session_parameters(
        "imexplore", parameters, profile, start, workspace, overrides
    )
    if image is not None:
        resolved["image"] = os.fspath(image)
    viewport = _image_viewport(
        width, height, inspector_height, plane_width, plane_height
    )
    view_parameters = {
        name: str(resolved[name])
        for name in (
            "blc",
            "trc",
            "inc",
            "stretch",
            "autoscale",
            "clip_low",
            "clip_high",
        )
    }
    command = {
        "version": 1,
        "command": {
            "command": "open_root",
            "path": str(resolved["image"]),
            "viewport": viewport,
            "parameters": view_parameters,
        },
    }
    resolved_binary = resolve_imexplore_binary(binary)
    process = _spawn_session(resolved_binary, resolved.workspace)
    session = JsonlSession(
        process,
        resolved,
        {},
        save_last=save_last,
        viewport=viewport,
        binary=resolved_binary,
    )
    try:
        session.first_response = session._exchange(command)
        _apply_imexplore_startup_profile(session)
    except Exception:
        session.close()
        raise
    session._mark_opened()
    return session


def open_tablebrowser(
    table: StrPath | None = None,
    *,
    parameters: SessionParameters | None = None,
    profile: StrPath | None = None,
    start: SessionBaseSource = "defaults",
    save_last: bool = True,
    workspace: StrPath | None = None,
    binary: StrPath | None = None,
    width: int = 120,
    height: int = 32,
    inspector_height: int = 10,
    overrides: Mapping[str, ParameterData] | None = None,
) -> JsonlSession:
    """Resolve a ``tablebrowser`` startup profile and open its JSONL session."""

    resolved = _resolve_session_parameters(
        "tablebrowser", parameters, profile, start, workspace, overrides
    )
    if table is not None:
        resolved["table"] = os.fspath(table)
    viewport = _table_viewport(width, height, inspector_height)
    command = {
        "version": 1,
        "command": {
            "command": "open_root",
            "path": str(resolved["table"]),
            "viewport": viewport,
        },
    }
    resolved_binary = _resolve_tablebrowser_binary(binary)
    process = _spawn_session(resolved_binary, resolved.workspace)
    session = JsonlSession(
        process,
        resolved,
        {},
        save_last=save_last,
        viewport=viewport,
        binary=resolved_binary,
    )
    try:
        session.first_response = session._exchange(command)
        _apply_tablebrowser_startup_profile(session)
    except Exception:
        session.close()
        raise
    session._mark_opened()
    return session


def _apply_imexplore_startup_profile(session: JsonlSession) -> None:
    """Apply non-window declarative settings before accepting the open."""

    view_steps = {
        "plane": 0,
        "spectrum": 1,
        "metadata": 2,
        "coordinates": 3,
    }
    for _ in range(view_steps[str(session.parameters["view"])]):
        session._startup_exchange({"command": "cycle_view", "forward": True})

    if session.parameters.states["contentmode"].explicit:
        session._startup_exchange(
            {
                "command": "set_plane_content_mode",
                "mode": str(session.parameters["contentmode"]),
            }
        )

    snapshot = _snapshot_payload(
        session.startup_responses[-1]
        if session.startup_responses
        else session.first_response
    )
    profile_axis = str(session.parameters["profileaxis"])
    if profile_axis != "auto" and snapshot is not None:
        axis = _resolve_image_axis(snapshot, profile_axis)
        session._startup_exchange(
            {"command": "set_selected_non_display_axis", "axis": axis}
        )

    states = session.parameters.states
    if states["region"].explicit or states["mask"].explicit:
        session._startup_exchange(
            _image_selection_command(
                session.parameters,
                include_region=states["region"].explicit,
                include_mask=states["mask"].explicit,
            )
        )


def _apply_imexplore_parameter_update(
    session: JsonlSession,
    candidate: SessionParameters,
    changed: set[str],
) -> None:
    backend_names = {
        "blc",
        "trc",
        "inc",
        "stretch",
        "autoscale",
        "clip_low",
        "clip_high",
        "view",
        "contentmode",
        "profileaxis",
        "region",
        "mask",
    }
    if changed & backend_names:
        _replace_imexplore_backend(session, candidate)

    # colormap, movieaxis, fps, and loop are durable client presentation
    # settings. The JSONL backend has no corresponding rendering/playback
    # command; successful typed validation is the client-side acceptance.


def _replace_imexplore_backend(
    session: JsonlSession, candidate: SessionParameters
) -> None:
    """Stage a complete durable profile and commit it with one process swap."""

    view_names = (
        "blc",
        "trc",
        "inc",
        "stretch",
        "autoscale",
        "clip_low",
        "clip_high",
    )
    open_command = {
        "version": 1,
        "command": {
            "command": "open_root",
            "path": str(candidate["image"]),
            "viewport": dict(session._viewport),
            "parameters": {name: str(candidate[name]) for name in view_names},
        },
    }
    staged = JsonlSession(
        _spawn_session(session._binary, candidate.workspace),
        candidate,
        {},
        save_last=False,
        viewport=session._viewport,
        binary=session._binary,
    )
    try:
        staged.first_response = staged._exchange(open_command)
        _apply_imexplore_startup_profile(staged)
    except Exception:
        staged.close()
        raise

    old_process = session._process
    session._process = staged._process
    session._last_response = staged._last_response
    session.startup_responses.extend(staged.startup_responses)
    _close_process(old_process)


def _apply_tablebrowser_startup_profile(session: JsonlSession) -> None:
    """Apply the declarative table target before accepting the open."""
    session._startup_exchange(
        {
            "command": "configure",
            "parameters": _tablebrowser_parameters(
                session.parameters, include_linked=True
            ),
        }
    )


def _apply_tablebrowser_parameter_update(
    session: JsonlSession,
    candidate: SessionParameters,
    changed: set[str],
) -> None:
    if changed & {
        "view",
        "bookmark",
        "rowstart",
        "nrow",
        "linkedtable",
        "contentmode",
    }:
        # Re-applying an unchanged linked-table selector would attempt to open
        # it relative to the table that is already open. Only carry that field
        # when this update intentionally changes the target.
        session._startup_exchange(
            {
                "command": "configure",
                "parameters": _tablebrowser_parameters(
                    candidate, include_linked="linkedtable" in changed
                ),
            }
        )


def _image_selection_command(
    parameters: SessionParameters,
    *,
    include_region: bool,
    include_mask: bool,
) -> dict[str, Any]:
    return {
        "command": "set_selection_references",
        "region": (
            _image_region_reference(str(parameters["region"]))
            if include_region
            else None
        ),
        "mask": (
            _image_mask_reference(str(parameters["mask"]))
            if include_mask
            else None
        ),
    }


def _looks_like_image_expression(value: str) -> bool:
    return any(character in "[](){}&|=<>!*+;" for character in value)


def _image_region_reference(value: str) -> dict[str, str]:
    value = value.strip()
    if value.casefold() in {"none", ""}:
        return {"kind": "none"}
    if value.startswith("file:"):
        path = value.removeprefix("file:").strip()
        if not path:
            raise ValueError("imexplore region file reference cannot be empty")
        return {"kind": "file", "path": path}
    if value.startswith("definition:"):
        name = value.removeprefix("definition:").strip()
        if not name:
            raise ValueError("imexplore saved region definition cannot be empty")
        return {"kind": "definition", "name": name}
    if _looks_like_image_expression(value):
        return {"kind": "expression", "expression": value}
    if (
        "/" in value
        or "\\" in value
        or value.endswith((".crtf", ".reg", ".region"))
    ):
        return {"kind": "file", "path": value}
    return {"kind": "definition", "name": value}


def _image_mask_reference(value: str) -> dict[str, str]:
    value = value.strip()
    if value.casefold() in {"none", ""}:
        return {"kind": "none"}
    if value.startswith("name:"):
        name = value.removeprefix("name:").strip()
        if not name:
            raise ValueError("imexplore mask name cannot be empty")
        return {"kind": "name", "name": name}
    if _looks_like_image_expression(value):
        return {"kind": "expression", "expression": value}
    return {"kind": "name", "name": value}


def _tablebrowser_parameters(
    parameters: SessionParameters, *, include_linked: bool
) -> dict[str, Any]:
    view = {
        "summary": "overview",
        "columns": "columns",
        "keywords": "keywords",
        "rows": "cells",
    }[str(parameters["view"])]
    linked = str(parameters["linkedtable"]).strip()
    return {
        "view": view,
        "row_start": int(parameters["rowstart"]),
        "row_count": int(parameters["nrow"]),
        "linked_table": (
            linked
            if include_linked and linked.casefold() not in {"none", ""}
            else None
        ),
        "bookmark": _table_bookmark(str(parameters["bookmark"])),
        "content_mode": str(parameters["contentmode"]),
    }


def _table_bookmark(value: str) -> dict[str, Any] | None:
    value = value.strip()
    if value.casefold() in {"none", ""}:
        return None
    if value.startswith("cell:"):
        parts = value.removeprefix("cell:").split(":", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].strip():
            return {
                "kind": "cell",
                "row": int(parts[0]),
                "column": parts[1].strip(),
            }
    elif value.startswith("table-keyword:"):
        path = _bookmark_path(value.removeprefix("table-keyword:"))
        if path:
            return {"kind": "table_keyword", "path": path}
    elif value.startswith("column-keyword:"):
        parts = value.removeprefix("column-keyword:").split(":", 1)
        if len(parts) == 2 and parts[0].strip():
            path = _bookmark_path(parts[1])
            if path:
                return {
                    "kind": "column_keyword",
                    "column": parts[0].strip(),
                    "path": path,
                }
    elif value.startswith("subtable:"):
        name = value.removeprefix("subtable:").strip()
        if name:
            return {"kind": "subtable", "name": name}
    raise ValueError(
        "bookmark must be cell:ROW:COLUMN, table-keyword:PATH, "
        "column-keyword:COLUMN:PATH, or subtable:NAME"
    )


def _bookmark_path(value: str) -> list[str]:
    return [
        part.strip()
        for slash_part in value.split("/")
        for part in slash_part.split(".")
        if part.strip()
    ]

def _snapshot_payload(response: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = response.get("response")
    if not isinstance(payload, dict) or payload.get("response") != "snapshot":
        return None
    if "echo" in payload:
        return None
    return payload


def _resolve_image_axis(snapshot: Mapping[str, Any], requested: str) -> int:
    folded = requested.casefold()
    axes = snapshot.get("non_display_axes", [])
    for entry in axes if isinstance(axes, list) else []:
        if not isinstance(entry, dict):
            continue
        axis = int(entry.get("axis", -1))
        if requested == str(axis) or str(entry.get("label", "")).casefold() == folded:
            return axis
    raise SessionProtocolError(
        f"profile axis {requested!r} is not one of the opened image's non-display axes"
    )


def _resolve_session_parameters(
    surface: str,
    parameters: SessionParameters | None,
    profile: StrPath | None,
    start: SessionBaseSource,
    workspace: StrPath | None,
    overrides: Mapping[str, ParameterData] | None,
) -> SessionParameters:
    source = start.lower().replace("-", "_")
    if source not in {"defaults", "last"}:
        raise ValueError("session source must be 'defaults' or 'last'")
    if parameters is not None and profile is not None:
        raise ValueError("pass either parameters or profile, not both")
    if (parameters is not None or profile is not None) and source != "defaults":
        raise ValueError(
            "parameters/profile and a non-default source are mutually exclusive"
        )
    if parameters is not None:
        if parameters.surface != surface:
            raise ValueError(
                f"parameters target {parameters.surface!r}, expected {surface!r}"
            )
        resolved = parameters.copy(workspace=workspace)
    elif profile is not None:
        resolved = SessionParameters.load(surface, profile, workspace=workspace)
    elif source == "defaults":
        resolved = SessionParameters.defaults(surface, workspace=workspace)
    else:
        resolved = SessionParameters.last(surface, workspace=workspace)
    if overrides:
        resolved.set_many(overrides)
    return resolved


def _presentation_settings(parameters: SessionParameters) -> dict[str, ParameterData]:
    names = (
        ("contentmode", "colormap", "movieaxis", "fps", "loop")
        if parameters.surface == "imexplore"
        else ("contentmode",)
    )
    return {name: parameters[name] for name in names}


def _spawn_session(binary: str, workspace: Path) -> subprocess.Popen[str]:
    try:
        return subprocess.Popen(
            [binary, "--session"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=workspace,
        )
    except OSError as error:
        raise SessionProtocolError(f"failed to start {binary!r}: {error}") from error


def _close_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if process.stdin is not None:
        process.stdin.close()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def _resolve_tablebrowser_binary(binary: StrPath | None) -> str:
    return _resolve_task_binary(
        application_id="tablebrowser",
        binary=binary,
        configured_binary=None,
        missing_error_cls=FileNotFoundError,
        description="tablebrowser",
    )


def _image_viewport(
    width: int,
    height: int,
    inspector_height: int,
    plane_width: int,
    plane_height: int,
) -> dict[str, int]:
    values = (width, height, inspector_height, plane_width, plane_height)
    if any(value < 0 for value in values) or width == 0 or height == 0:
        raise ValueError(
            "viewport width/height must be positive and other dimensions nonnegative"
        )
    return {
        "width": width,
        "height": height,
        "inspector_height": inspector_height,
        "plane_pixel_width": plane_width,
        "plane_pixel_height": plane_height,
    }


def _table_viewport(width: int, height: int, inspector_height: int) -> dict[str, int]:
    if width <= 0 or height <= 0 or inspector_height < 0:
        raise ValueError(
            "viewport width/height must be positive and inspector height nonnegative"
        )
    return {"width": width, "height": height, "inspector_height": inspector_height}


__all__ = [
    "JsonlSession",
    "SessionBaseSource",
    "SessionProtocolError",
    "open",
    "open_imexplore",
    "open_tablebrowser",
]

# Generated CASA-named conveniences remain a projection of the checked Rust
# definitions; no defaults or validators are maintained in Python.
from ._session_catalog import imexplore, tablebrowser

__all__.extend(["imexplore", "tablebrowser"])
