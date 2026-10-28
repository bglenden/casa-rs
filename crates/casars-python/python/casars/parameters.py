"""CASA-named task parameters.

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
from os import PathLike
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


@dataclass(frozen=True, slots=True)
class ProviderInvocation:
    """Versioned canonical provider request projected by Rust."""

    protocol_name: str | None
    protocol_version: int | None
    args: tuple[str, ...]
    stdin: str | None


def _native() -> Any:
    """The native extension, loaded on first use so `import casars` stays light."""

    return import_module("._core", __package__)


def catalog() -> dict[str, Any]:
    """Return the checked aggregate concept and surface catalog."""

    return _native().parameter_catalog()


def definition(surface: str) -> dict[str, Any]:
    """Return one task definition from the built-in catalog."""

    return _native().parameter_surface_bundle(surface)["surface"]


def contract_bundle(surface: str) -> dict[str, Any]:
    """Return a self-contained definition with exactly its referenced concepts."""

    return _native().parameter_surface_bundle(surface)


def documented_template(surface: str) -> str:
    """Render a commented TOML reference without activating default values."""

    return _native().parameter_template_toml(surface)


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
        self._snapshot: Any = None
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
        snapshot = _native().parameter_last(surface, str(root), False)
        if snapshot is None or snapshot["profile_toml"] is None:
            raise FileNotFoundError(f"no Last parameter profile exists for {surface!r}")
        return cls(
            surface,
            base_source="last",
            profile_toml=snapshot["profile_toml"],
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
        snapshot = _native().parameter_last(surface, str(root), True)
        if snapshot is None or snapshot["profile_toml"] is None:
            raise FileNotFoundError(
                f"no Last Successful parameter profile exists for {surface!r}"
            )
        return cls(
            surface,
            base_source="last_successful",
            profile_toml=snapshot["profile_toml"],
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
            result[name] = ParameterState(
                value=state["value"],
                origin=ParameterOrigin(state["origin"]),
                active=state["active"],
                required=state["required"],
                explicit=state["explicit"],
            )
        return result

    @property
    def origins(self) -> dict[str, ParameterOrigin]:
        return {name: state.origin for name, state in self.states.items()}

    @property
    def diagnostics(self) -> tuple[ParameterDiagnostic, ...]:
        result = []
        for diagnostic in self._snapshot["diagnostics"]:
            location = diagnostic["location"]
            result.append(
                ParameterDiagnostic(
                    level=diagnostic["level"],
                    code=diagnostic["code"],
                    message=diagnostic["message"],
                    parameter=diagnostic["parameter"],
                    line=None if location is None else location["line"],
                    column=None if location is None else location["column"],
                    suggestions=tuple(diagnostic["suggestions"]),
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

    # The mapping holds the parameters that have a value, so that a loaded
    # profile runs as `tasks.gaincal(**parameters.load(path))`.
    def __iter__(self) -> Iterator[str]:
        return iter(self._resolved_values())

    def __len__(self) -> int:
        return len(self._resolved_values())

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
            snapshot = _native().parameter_last(
                self.surface,
                str(self._workspace),
                self._base_source == "last_successful",
            )
            if snapshot is None or snapshot["profile_toml"] is None:
                raise FileNotFoundError(
                    f"managed parameter profile disappeared for {self.surface!r}"
                )
            self._profile_toml = snapshot["profile_toml"]
        self.revert()

    def to_toml(self) -> str:
        """Render required values and semantic differences from current defaults."""

        return _native().parameter_render_toml(self.surface, self._resolved_values())

    def save(self, path: StrPath) -> Path:
        """Atomically save this draft as sparse current-contract TOML."""

        target = Path(path)
        _native().parameter_save(self.surface, self._resolved_values(), str(target))
        return target

    def write_last(self, *, successful: bool = False) -> Path:
        """Explicitly update Last or Last Successful for this workspace."""

        outcome = _native().parameter_write_managed(
            self.surface,
            str(self._workspace),
            self._resolved_values(),
            successful,
        )
        return Path(outcome["path"])

    def provider_invocation(self) -> ProviderInvocation:
        """Project this task through the canonical provider-owned request seam."""

        projected = _native().parameter_provider_invocation(
            self.surface, self._resolved_values()
        )
        return ProviderInvocation(
            protocol_name=projected["protocol_name"],
            protocol_version=projected["protocol_version"],
            args=tuple(projected["args"]),
            stdin=projected["stdin"],
        )

    def _resolved_values(self) -> dict[str, ParameterData]:
        return {
            name: state["value"]
            for name, state in self._snapshot["states"].items()
            if state["value"] is not None
        }

    def _refresh(self) -> None:
        if self._base_source not in {"defaults", "file", "last", "last_successful"}:
            raise KeyError(self._base_source)
        self._snapshot = _native().parameter_resolve(
            self.surface,
            self._base_source,
            self._profile_toml,
            None if self._profile_path is None else str(self._profile_path),
            dict(self._overrides),
            sorted(self._unset),
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(surface={self.surface!r}, "
            f"base_source={self.base_source!r}, dirty={self.is_dirty})"
        )


class TaskParameters(SurfaceParameters):
    """Mutable parameters constrained to a one-shot task definition."""

    expected_kind = "task"

    def run(self, **options: Any) -> Any:
        """Run this draft: :func:`casars.tasks.run` with it as the base.

        ``options`` are CASA-named values to set on top, and ``workspace``.
        """

        from .tasks import run

        return run(self.surface, self, **options)


def defaults(surface: str, *, workspace: StrPath | None = None) -> SurfaceParameters:
    """Start a task's parameters from its current contract defaults."""

    return TaskParameters.defaults(surface, workspace=workspace)


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
        surface = _native().parameter_profile_surface(source)
    else:
        surface = str(surface_or_path)
        profile_path = Path(path)
    return TaskParameters.load(surface, profile_path, workspace=workspace)


def last(surface: str, *, workspace: StrPath | None = None) -> SurfaceParameters:
    """Load Last for a task."""

    return TaskParameters.last(surface, workspace=workspace)


def last_successful(
    surface: str, *, workspace: StrPath | None = None
) -> SurfaceParameters:
    """Load Last Successful for a task."""

    return TaskParameters.last_successful(surface, workspace=workspace)


_MISSING = object()


__all__ = [
    "ParameterData",
    "ParameterDiagnostic",
    "ParameterOrigin",
    "ParameterState",
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
