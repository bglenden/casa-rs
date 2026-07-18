"""Explicit launch resolution shared by Python tasks and sessions."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from os import PathLike
from pathlib import Path
from typing import TypeAlias

from .parameters import _frontend

StrPath: TypeAlias = str | PathLike[str]

CASARS_SUITE_ROOT_ENVVAR = "CASARS_SUITE_ROOT"
CASARS_LAUNCH_MODE_ENVVAR = "CASARS_LAUNCH_MODE"
CASARS_DEVELOPMENT_WORKSPACE_ENVVAR = "CASARS_DEVELOPMENT_WORKSPACE"


@dataclass(frozen=True, slots=True)
class ApplicationLaunch:
    """Launch metadata projected from the generated application catalog."""

    executable: str
    cargo_package: str
    override_env: str


def resolve_imexplore_binary(binary: StrPath | None = None) -> str:
    """Resolve the image-explorer session through the canonical launch policy."""

    return _resolve_task_binary(
        application_id="imexplore",
        binary=binary,
        configured_binary=None,
        missing_error_cls=FileNotFoundError,
        description="imexplore",
    )


def _resolve_task_binary(
    *,
    application_id: str,
    binary: StrPath | None,
    configured_binary: str | None,
    missing_error_cls: type[FileNotFoundError],
    description: str,
) -> str:
    """Resolve one executable from exactly one explicit launch mode."""

    if binary is not None:
        return _require_binary(
            os.fspath(binary),
            source="explicit function override",
            missing_error_cls=missing_error_cls,
            description=description,
        )
    if configured_binary is not None:
        return _require_binary(
            configured_binary,
            source="module configuration",
            missing_error_cls=missing_error_cls,
            description=description,
        )

    launch = _application_launch(application_id)
    mode = os.environ.get(CASARS_LAUNCH_MODE_ENVVAR, "installed_suite")
    if mode == "installed_suite":
        explicit = os.environ.get(launch.override_env)
        if explicit:
            return _require_binary(
                explicit,
                source=f"${launch.override_env}",
                missing_error_cls=missing_error_cls,
                description=description,
            )
        suite_root = Path(
            os.environ.get(
                CASARS_SUITE_ROOT_ENVVAR,
                Path.home() / ".local" / "opt" / "casa-rs" / "current",
            )
        )
        return _require_binary(
            str(suite_root / "bin" / _binary_name(launch.executable)),
            source="installed-suite launch mode",
            missing_error_cls=missing_error_cls,
            description=description,
        )
    if mode == "development_workspace":
        workspace = os.environ.get(CASARS_DEVELOPMENT_WORKSPACE_ENVVAR)
        if not workspace:
            raise missing_error_cls(
                f"development-workspace launch mode requires "
                f"{CASARS_DEVELOPMENT_WORKSPACE_ENVVAR} for {description}"
            )
        return _require_binary(
            str(Path(workspace) / "target" / "debug" / _binary_name(launch.executable)),
            source="development-workspace launch mode",
            missing_error_cls=missing_error_cls,
            description=description,
        )
    raise ValueError(
        f"invalid {CASARS_LAUNCH_MODE_ENVVAR} {mode!r}; expected "
        "'installed_suite' or 'development_workspace'"
    )


@lru_cache(maxsize=1)
def _application_launches() -> dict[str, ApplicationLaunch]:
    catalog = _frontend().application_catalog()
    return {
        application.id: ApplicationLaunch(
            executable=application.executable,
            cargo_package=application.cargo_package,
            override_env=application.override_env,
        )
        for application in catalog.applications
    }


def _application_launch(application_id: str) -> ApplicationLaunch:
    try:
        return _application_launches()[application_id]
    except KeyError as error:
        raise RuntimeError(
            f"canonical application catalog has no entry for {application_id!r}"
        ) from error


def _require_binary(
    candidate: str,
    *,
    source: str,
    missing_error_cls: type[FileNotFoundError],
    description: str,
) -> str:
    resolved = Path(candidate).expanduser()
    if not resolved.is_file():
        raise missing_error_cls(
            f"{source} did not resolve to an existing {description} binary: {candidate}"
        )
    return str(resolved)


def _binary_name(binary_name: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    return f"{binary_name}{suffix}"


__all__ = ["resolve_imexplore_binary"]
