"""Unified CASA-named task and session parameters.

The Rust parameter catalog remains the only source of defaults, validation,
normalization, migrations, origins, and sparse TOML rendering.  This module is
an idiomatic mutable Python view over that runtime; it does not duplicate task
signatures or defaults.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
import json
from os import PathLike, fspath
from pathlib import Path
from typing import Any, ClassVar, TypeAlias

StrPath: TypeAlias = str | PathLike[str]
ParameterScalar: TypeAlias = bool | int | float | str | PathLike[str]
ParameterData: TypeAlias = (
    ParameterScalar
    | list["ParameterData"]
    | tuple["ParameterData", ...]
    | dict[str, "ParameterData"]
)


class ParameterOrigin(str, Enum):
    """Winning source of a resolved value."""

    DEFAULT = "default"
    BASE_PROFILE = "base_profile"
    CONTEXT = "context"
    OVERRIDE = "override"


@dataclass(frozen=True, slots=True)
class ParameterState:
    """Resolved value and UI-relevant state for one canonical CASA name."""

    value: ParameterData | None
    origin: ParameterOrigin
    active: bool
    required: bool
    explicit: bool


@dataclass(frozen=True, slots=True)
class ParameterDiagnostic:
    """One structured warning or error emitted by profile resolution."""

    level: str
    code: str
    message: str
    parameter: str | None = None
    line: int | None = None
    column: int | None = None
    suggestions: tuple[str, ...] = ()


def _core() -> Any:
    return import_module("._core", __package__)


def _json_result(payload: str) -> dict[str, Any]:
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise TypeError("parameter bridge returned a non-object JSON payload")
    return value


def _encode_value(value: ParameterData) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"kind": "bool", "value": value}
    if isinstance(value, int):
        return {"kind": "integer", "value": value}
    if isinstance(value, float):
        return {"kind": "float", "value": value}
    if isinstance(value, str):
        return {"kind": "string", "value": value}
    if isinstance(value, PathLike):
        path = fspath(value)
        if not isinstance(path, str):
            raise TypeError("parameter paths must resolve to text, not bytes")
        return {"kind": "string", "value": path}
    if isinstance(value, (list, tuple)):
        return {"kind": "array", "value": [_encode_value(item) for item in value]}
    if isinstance(value, Mapping):
        table: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("parameter table keys must be strings")
            table[key] = _encode_value(item)
        return {"kind": "table", "value": table}
    raise TypeError(f"unsupported parameter value {value!r}")


def _decode_value(value: Mapping[str, Any]) -> ParameterData:
    kind = value.get("kind")
    payload = value.get("value")
    if kind in {"bool", "integer", "float", "string"}:
        return payload
    if kind == "array":
        return [_decode_value(item) for item in payload]
    if kind == "table":
        return {name: _decode_value(item) for name, item in payload.items()}
    raise ValueError(f"unknown tagged parameter value kind {kind!r}")


def _encoded_values(values: Mapping[str, ParameterData]) -> str:
    return json.dumps({name: _encode_value(value) for name, value in values.items()})


def catalog() -> dict[str, Any]:
    """Return the checked aggregate concept and surface catalog."""

    return _json_result(_core().parameter_catalog_json())


def definition(surface: str) -> dict[str, Any]:
    """Return one task or session definition from the built-in catalog."""

    return _json_result(_core().parameter_surface_definition_json(surface))


def contract_bundle(surface: str) -> dict[str, Any]:
    """Return a self-contained definition with exactly its referenced concepts."""

    return _json_result(_core().parameter_surface_bundle_json(surface))


def documented_template(surface: str) -> str:
    """Render a commented TOML reference without activating default values."""

    return _core().parameter_template_toml(surface)


class SurfaceParameters(MutableMapping[str, ParameterData]):
    """Mutable resolved parameters for exactly one catalog surface.

    Mapping keys are canonical CASA parameter names.  Assignments are
    normalized and validated immediately by Rust.  A failed assignment leaves
    the prior draft unchanged.
    """

    expected_kind: ClassVar[str | None] = None

    def __init__(
        self,
        surface: str,
        *,
        base_source: str = "defaults",
        profile_toml: str | None = None,
        profile_path: StrPath | None = None,
        workspace: StrPath | None = None,
    ) -> None:
        self.surface = surface
        self._definition = definition(surface)
        actual_kind = self._definition["kind"]
        if self.expected_kind is not None and actual_kind != self.expected_kind:
            raise TypeError(
                f"{type(self).__name__} requires a {self.expected_kind} surface; "
                f"{surface!r} is {actual_kind}"
            )
        self._base_source = base_source
        self._profile_toml = profile_toml
        self._profile_path = None if profile_path is None else Path(profile_path)
        self._workspace = Path.cwd() if workspace is None else Path(workspace)
        self._overrides: dict[str, ParameterData] = {}
        self._unset: set[str] = set()
        self._snapshot: dict[str, Any] = {}
        self._refresh()

    @classmethod
    def defaults(
        cls, surface: str, *, workspace: StrPath | None = None
    ) -> "SurfaceParameters":
        """Start from the current contract defaults."""

        return cls(surface, workspace=workspace)

    @classmethod
    def load(
        cls,
        surface: str,
        path: StrPath,
        *,
        workspace: StrPath | None = None,
    ) -> "SurfaceParameters":
        """Load one sparse TOML file as the mutually exclusive base source."""

        profile_path = Path(path)
        return cls(
            surface,
            base_source="file",
            profile_toml=profile_path.read_text(encoding="utf-8"),
            profile_path=profile_path,
            workspace=workspace,
        )

    @classmethod
    def last(
        cls,
        surface: str,
        *,
        workspace: StrPath | None = None,
    ) -> "SurfaceParameters":
        """Start from the most recently persisted valid intent."""

        root = Path.cwd() if workspace is None else Path(workspace)
        profile = _core().parameter_managed_profile_toml(surface, root, False)
        if profile is None:
            raise FileNotFoundError(f"no Last parameter profile exists for {surface!r}")
        return cls(
            surface,
            base_source="last",
            profile_toml=profile,
            workspace=root,
        )

    @classmethod
    def last_successful(
        cls,
        surface: str,
        *,
        workspace: StrPath | None = None,
    ) -> "SurfaceParameters":
        """Start from the most recently successful task invocation."""

        root = Path.cwd() if workspace is None else Path(workspace)
        profile = _core().parameter_managed_profile_toml(surface, root, True)
        if profile is None:
            raise FileNotFoundError(
                f"no Last Successful parameter profile exists for {surface!r}"
            )
        return cls(
            surface,
            base_source="last_successful",
            profile_toml=profile,
            workspace=root,
        )

    @property
    def kind(self) -> str:
        return self._definition["kind"]

    @property
    def contract_version(self) -> int:
        return int(self._definition["contract_version"])

    @property
    def base_source(self) -> str:
        return self._base_source

    @property
    def workspace(self) -> Path:
        """Workspace used to resolve managed Last parameter slots."""

        return self._workspace

    @property
    def is_dirty(self) -> bool:
        return bool(self._snapshot["dirty"])

    @property
    def states(self) -> dict[str, ParameterState]:
        result: dict[str, ParameterState] = {}
        for name, state in self._snapshot["states"].items():
            tagged = state.get("value")
            result[name] = ParameterState(
                value=None if tagged is None else _decode_value(tagged),
                origin=ParameterOrigin(state["origin"]),
                active=bool(state["active"]),
                required=bool(state["required"]),
                explicit=bool(state["explicit"]),
            )
        return result

    @property
    def origins(self) -> dict[str, ParameterOrigin]:
        return {name: state.origin for name, state in self.states.items()}

    @property
    def diagnostics(self) -> tuple[ParameterDiagnostic, ...]:
        result = []
        for diagnostic in self._snapshot.get("diagnostics", []):
            location = diagnostic.get("location") or {}
            result.append(
                ParameterDiagnostic(
                    level=diagnostic["level"],
                    code=diagnostic["code"],
                    message=diagnostic["message"],
                    parameter=diagnostic.get("parameter"),
                    line=location.get("line"),
                    column=location.get("column"),
                    suggestions=tuple(diagnostic.get("suggestions", ())),
                )
            )
        return tuple(result)

    def __getitem__(self, name: str) -> ParameterData:
        state = self.states.get(name)
        if state is None or state.value is None:
            raise KeyError(name)
        return state.value

    def __setitem__(self, name: str, value: ParameterData) -> None:
        previous_value = self._overrides.get(name, _MISSING)
        was_unset = name in self._unset
        self._overrides[name] = value
        self._unset.discard(name)
        try:
            self._refresh()
        except Exception:
            if previous_value is _MISSING:
                self._overrides.pop(name, None)
            else:
                self._overrides[name] = previous_value
            if was_unset:
                self._unset.add(name)
            raise

    def __delitem__(self, name: str) -> None:
        self.reset(name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._snapshot["states"])

    def __len__(self) -> int:
        return len(self._snapshot["states"])

    def set_many(self, values: Mapping[str, ParameterData]) -> None:
        """Apply several mutations atomically."""

        old_overrides = self._overrides.copy()
        old_unset = self._unset.copy()
        self._overrides.update(values)
        self._unset.difference_update(values)
        try:
            self._refresh()
        except Exception:
            self._overrides = old_overrides
            self._unset = old_unset
            raise

    def reset(self, name: str) -> None:
        """Discard base/context/explicit intent for one name and expose its default."""

        if name not in self._snapshot["states"]:
            raise KeyError(name)
        old_overrides = self._overrides.copy()
        old_unset = self._unset.copy()
        self._overrides.pop(name, None)
        self._unset.add(name)
        try:
            self._refresh()
        except Exception:
            self._overrides = old_overrides
            self._unset = old_unset
            raise

    def revert(self) -> None:
        """Discard explicit edits while retaining the selected base profile."""

        self._overrides.clear()
        self._unset.clear()
        self._refresh()

    def copy(self, *, workspace: StrPath | None = None) -> SurfaceParameters:
        """Return an independent draft with the same base source and edits.

        ``workspace`` may intentionally rebase relative data/product paths and
        managed Last storage without changing the sparse parameter intent.
        """

        copied = type(self)(
            self.surface,
            base_source=self._base_source,
            profile_toml=self._profile_toml,
            profile_path=self._profile_path,
            workspace=self._workspace if workspace is None else workspace,
        )
        copied._overrides = self._overrides.copy()
        copied._unset = self._unset.copy()
        copied._refresh()
        return copied

    def reload(self) -> None:
        """Reload the selected file or managed slot and discard explicit edits."""

        if self._base_source == "file":
            assert self._profile_path is not None
            self._profile_toml = self._profile_path.read_text(encoding="utf-8")
        elif self._base_source in {"last", "last_successful"}:
            profile = _core().parameter_managed_profile_toml(
                self.surface,
                self._workspace,
                self._base_source == "last_successful",
            )
            if profile is None:
                raise FileNotFoundError(
                    f"managed parameter profile disappeared for {self.surface!r}"
                )
            self._profile_toml = profile
        self.revert()

    def to_toml(self) -> str:
        """Render required values and semantic differences from current defaults."""

        return _core().parameter_render_toml(self.surface, self._resolved_values_json())

    def save(self, path: StrPath) -> Path:
        """Atomically save this draft as sparse current-contract TOML."""

        target = Path(path)
        _core().parameter_save_toml(self.surface, self._resolved_values_json(), target)
        return target

    def write_last(self, *, successful: bool = False) -> Path:
        """Explicitly update Last or Last Successful for this workspace."""

        path = _core().parameter_write_managed(
            self.surface,
            self._workspace,
            self._resolved_values_json(),
            successful,
        )
        return Path(path)

    def _resolved_values_json(self) -> str:
        values = {
            name: state.value
            for name, state in self.states.items()
            if state.value is not None
        }
        return _encoded_values(values)

    def _refresh(self) -> None:
        patch = {
            "values": {
                name: _encode_value(value) for name, value in self._overrides.items()
            },
            "unset": sorted(self._unset),
        }
        payload = _core().parameter_resolve_json(
            self.surface,
            self._base_source,
            self._profile_toml,
            self._profile_path,
            json.dumps(patch),
        )
        self._snapshot = _json_result(payload)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(surface={self.surface!r}, "
            f"base_source={self.base_source!r}, dirty={self.is_dirty})"
        )


class TaskParameters(SurfaceParameters):
    """Mutable parameters constrained to a one-shot task definition."""

    expected_kind = "task"

    def run(self, **options: Any) -> Any:
        """Run this draft through the canonical catalog-driven task runner."""

        if {"parameters", "profile"} & options.keys():
            raise TypeError("TaskParameters.run supplies its own parameters draft")
        from .tasks import run

        return run(self.surface, parameters=self, **options)


class SessionParameters(SurfaceParameters):
    """Mutable startup/profile parameters constrained to a session definition."""

    expected_kind = "session"

    def open(self, **options: Any) -> Any:
        """Open this draft through the matching catalog session client."""

        if {"parameters", "profile"} & options.keys():
            raise TypeError("SessionParameters.open supplies its own parameters draft")
        from .sessions import open as open_session

        return open_session(self.surface, parameters=self, **options)


def _surface_class(surface: str) -> type[SurfaceParameters]:
    kind = definition(surface)["kind"]
    if kind == "task":
        return TaskParameters
    if kind == "session":
        return SessionParameters
    raise ValueError(f"unsupported surface kind {kind!r}")


def defaults(surface: str, *, workspace: StrPath | None = None) -> SurfaceParameters:
    """Start the appropriate task/session parameter class from defaults."""

    return _surface_class(surface).defaults(surface, workspace=workspace)


def load(
    surface_or_path: str | PathLike[str],
    path: StrPath | None = None,
    *,
    workspace: StrPath | None = None,
) -> SurfaceParameters:
    """Load a sparse TOML profile, inferring its surface from the header.

    The two-argument ``load(surface, path)`` spelling remains available for
    callers that want to assert the expected surface explicitly.
    """

    if path is None:
        profile_path = Path(surface_or_path)
        source = profile_path.read_text(encoding="utf-8")
        surface = _core().parameter_profile_surface(source)
    else:
        surface = str(surface_or_path)
        profile_path = Path(path)
    return _surface_class(surface).load(surface, profile_path, workspace=workspace)


def last(surface: str, *, workspace: StrPath | None = None) -> SurfaceParameters:
    """Load Last for a task or session."""

    return _surface_class(surface).last(surface, workspace=workspace)


def last_successful(
    surface: str, *, workspace: StrPath | None = None
) -> TaskParameters:
    """Load Last Successful for a one-shot task."""

    cls = _surface_class(surface)
    if cls is not TaskParameters:
        raise TypeError(f"session surface {surface!r} does not have Last Successful")
    return TaskParameters.last_successful(surface, workspace=workspace)


_MISSING = object()


__all__ = [
    "ParameterData",
    "ParameterDiagnostic",
    "ParameterOrigin",
    "ParameterState",
    "SessionParameters",
    "SurfaceParameters",
    "TaskParameters",
    "catalog",
    "contract_bundle",
    "defaults",
    "definition",
    "documented_template",
    "last",
    "last_successful",
    "load",
]
