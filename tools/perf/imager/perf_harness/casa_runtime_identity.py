#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture the CASA runtime and data/model identity used by imaging evidence."""

from __future__ import annotations

import ast
import copy
import hashlib
import importlib.metadata
import json
import pathlib
import platform
import re
import sys
import tempfile
from typing import Any

try:
    from .tree_identity import sha256_file, tree_identity
except ImportError:  # Executed directly by the CASA Python interpreter.
    from tree_identity import sha256_file, tree_identity


SCHEMA_VERSION = 1
REQUEST_KIND = "casa_runtime_identity_request"
RESULT_KIND = "casa_runtime_identity_result"
VOLATILE_DATA_NAMES = {"table.lock", "data_update.lock"}
IDENTITY_FIELDS = {
    "schema_version",
    "python",
    "modules",
    "configuration",
    "data_versions",
    "data_trees",
}
STABLE_IDENTITY_FIELDS = IDENTITY_FIELDS - {"configuration"}
MODULE_NAMES = {"casatasks", "casatools", "casaconfig", "casadata"}
DATA_TREE_NAMES = {"geodetic", "vla"}
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class IdentityError(ValueError):
    """The CASA installation cannot produce the required stable identity."""


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stable_identity_projection(identity: dict[str, Any]) -> dict[str, Any]:
    """Remove host locators while preserving every executable/data digest.

    Absolute installation and CASA-data paths are useful provenance, but they
    are not scientific identity.  Keeping them out of this projection allows a
    byte-identical frozen runtime to be verified on a larger host.
    """

    validate_identity(identity, stable=False)
    projected = copy.deepcopy(identity)
    projected.pop("configuration", None)
    python = projected.get("python")
    if isinstance(python, dict):
        python.pop("executable", None)
    modules = projected.get("modules")
    if isinstance(modules, dict):
        for module in modules.values():
            if isinstance(module, dict):
                module.pop("module_file", None)
    trees = projected.get("data_trees")
    if isinstance(trees, dict):
        for tree in trees.values():
            if isinstance(tree, dict):
                tree.pop("path", None)
    validate_identity(projected, stable=True)
    return projected


def stable_identity_sha256(identity: dict[str, Any]) -> str:
    if isinstance(identity, dict) and "configuration" not in identity:
        validate_identity(identity, stable=True)
        return canonical_sha256(identity)
    return canonical_sha256(stable_identity_projection(identity))


def validate_identity(identity: Any, *, stable: bool) -> None:
    """Validate the complete full or path-independent frozen identity shape."""

    if not isinstance(identity, dict):
        raise IdentityError("CASA runtime identity must be an object")
    _exact_fields(
        identity,
        STABLE_IDENTITY_FIELDS if stable else IDENTITY_FIELDS,
        "runtime identity",
    )
    if identity.get("schema_version") != SCHEMA_VERSION or isinstance(
        identity.get("schema_version"), bool
    ):
        raise IdentityError(f"runtime identity schema_version must be {SCHEMA_VERSION}")

    python = _object(identity.get("python"), "runtime identity.python")
    python_fields = {"version", "implementation", "executable_sha256"}
    if not stable:
        python_fields.add("executable")
    _exact_fields(python, python_fields, "runtime identity.python")
    _string(python.get("version"), "runtime identity.python.version")
    _string(python.get("implementation"), "runtime identity.python.implementation")
    _sha256(
        python.get("executable_sha256"),
        "runtime identity.python.executable_sha256",
    )
    if not stable:
        _absolute_path_string(
            python.get("executable"), "runtime identity.python.executable"
        )

    modules = _object(identity.get("modules"), "runtime identity.modules")
    _exact_fields(modules, MODULE_NAMES, "runtime identity.modules")
    for name in sorted(MODULE_NAMES):
        module = _object(modules.get(name), f"runtime identity.modules.{name}")
        module_fields = {
            "distribution_version",
            "reported_version",
            "module_file_sha256",
            "code_tree",
        }
        if not stable:
            module_fields.add("module_file")
        _exact_fields(module, module_fields, f"runtime identity.modules.{name}")
        for field in ("distribution_version", "reported_version"):
            value = module.get(field)
            if value is not None:
                _string(value, f"runtime identity.modules.{name}.{field}")
        if name == "casatasks":
            _string(
                module.get("reported_version"),
                "runtime identity.modules.casatasks.reported_version",
            )
        if not stable:
            _absolute_path_string(
                module.get("module_file"),
                f"runtime identity.modules.{name}.module_file",
            )
        _sha256(
            module.get("module_file_sha256"),
            f"runtime identity.modules.{name}.module_file_sha256",
        )
        code_tree = _object(
            module.get("code_tree"), f"runtime identity.modules.{name}.code_tree"
        )
        _exact_fields(
            code_tree,
            {"tree_sha256", "file_count", "size_bytes", "policy"},
            f"runtime identity.modules.{name}.code_tree",
        )
        _sha256(
            code_tree.get("tree_sha256"),
            f"runtime identity.modules.{name}.code_tree.tree_sha256",
        )
        _nonnegative_int(
            code_tree.get("file_count"),
            f"runtime identity.modules.{name}.code_tree.file_count",
            positive=True,
        )
        _nonnegative_int(
            code_tree.get("size_bytes"),
            f"runtime identity.modules.{name}.code_tree.size_bytes",
        )
        if code_tree.get("policy") != "package_files_without_bytecode_v1":
            raise IdentityError(
                f"runtime identity.modules.{name}.code_tree.policy is invalid"
            )

    if not stable:
        configuration = _object(
            identity.get("configuration"), "runtime identity.configuration"
        )
        _exact_fields(
            configuration,
            {"measurespath", "datapath"},
            "runtime identity.configuration",
        )
        _absolute_path_string(
            configuration.get("measurespath"),
            "runtime identity.configuration.measurespath",
        )
        datapath = configuration.get("datapath")
        if not isinstance(datapath, list) or not datapath:
            raise IdentityError(
                "runtime identity.configuration.datapath must be a non-empty list"
            )
        for index, value in enumerate(datapath):
            _absolute_path_string(
                value, f"runtime identity.configuration.datapath[{index}]"
            )

    versions = _object(identity.get("data_versions"), "runtime identity.data_versions")
    _exact_fields(
        versions, {"casarundata", "measures"}, "runtime identity.data_versions"
    )
    for name in ("casarundata", "measures"):
        info = versions.get(name)
        if info is None:
            continue
        info = _object(info, f"runtime identity.data_versions.{name}")
        if not info or not set(info) <= {"version", "date", "site"}:
            raise IdentityError(
                f"runtime identity.data_versions.{name} fields are invalid"
            )
        for field, value in info.items():
            if value is not None and not isinstance(value, str):
                raise IdentityError(
                    f"runtime identity.data_versions.{name}.{field} must be a string or null"
                )

    trees = _object(identity.get("data_trees"), "runtime identity.data_trees")
    _exact_fields(trees, DATA_TREE_NAMES, "runtime identity.data_trees")
    for name in sorted(DATA_TREE_NAMES):
        tree = _object(trees.get(name), f"runtime identity.data_trees.{name}")
        tree_fields = {
            "tree_sha256",
            "file_count",
            "size_bytes",
            "excluded_names",
            "excluded_count",
        }
        if not stable:
            tree_fields.add("path")
        _exact_fields(tree, tree_fields, f"runtime identity.data_trees.{name}")
        if not stable:
            _absolute_path_string(
                tree.get("path"), f"runtime identity.data_trees.{name}.path"
            )
        _sha256(
            tree.get("tree_sha256"),
            f"runtime identity.data_trees.{name}.tree_sha256",
        )
        _nonnegative_int(
            tree.get("file_count"),
            f"runtime identity.data_trees.{name}.file_count",
            positive=True,
        )
        _nonnegative_int(
            tree.get("size_bytes"),
            f"runtime identity.data_trees.{name}.size_bytes",
        )
        _nonnegative_int(
            tree.get("excluded_count"),
            f"runtime identity.data_trees.{name}.excluded_count",
        )
        if tree.get("excluded_names") != sorted(VOLATILE_DATA_NAMES):
            raise IdentityError(
                f"runtime identity.data_trees.{name}.excluded_names is invalid"
            )


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise IdentityError(f"{field} must be an object")
    return value


def _exact_fields(value: dict[str, Any], expected: set[str], field: str) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise IdentityError(
            f"{field} field mismatch; missing={missing or 'none'}; "
            f"unknown={unknown or 'none'}"
        )


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise IdentityError(f"{field} must be a non-empty string")
    return value


def _absolute_path_string(value: Any, field: str) -> str:
    value = _string(value, field)
    if not pathlib.Path(value).is_absolute():
        raise IdentityError(f"{field} must be an absolute path")
    return value


def _sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise IdentityError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _nonnegative_int(value: Any, field: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise IdentityError(f"{field} must be an integer")
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "non-negative"
        raise IdentityError(f"{field} must be {qualifier}")
    return value


def capture_runtime_identity() -> dict[str, Any]:
    """Return a deterministic identity for CASA code and VLASS beam inputs."""

    import casaconfig  # type: ignore[import-not-found]
    import casadata  # type: ignore[import-not-found]
    import casatasks  # type: ignore[import-not-found]
    import casatools  # type: ignore[import-not-found]

    config = _parse_config(casaconfig.get_config())
    measures_path = _required_directory(config.get("measurespath"), "measurespath")
    data_paths = config.get("datapath")
    if not isinstance(data_paths, list):
        raise IdentityError("casaconfig datapath must be a list")
    data_root = next(
        (
            pathlib.Path(value).expanduser().resolve()
            for value in data_paths
            if isinstance(value, str)
            and (pathlib.Path(value).expanduser() / "nrao" / "VLA").is_dir()
        ),
        None,
    )
    if data_root is None:
        raise IdentityError("no casaconfig datapath contains nrao/VLA")

    executable = pathlib.Path(sys.executable).resolve()
    identity = {
        "schema_version": SCHEMA_VERSION,
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": str(executable),
            "executable_sha256": sha256_file(executable),
        },
        "modules": {
            "casatasks": _module_identity(
                casatasks,
                distribution="casatasks",
                reported_version=str(casatasks.version_string()),
            ),
            "casatools": _module_identity(
                casatools,
                distribution="casatools",
                reported_version=str(casatools.version_string()),
            ),
            "casaconfig": _module_identity(casaconfig, distribution="casaconfig"),
            "casadata": _module_identity(casadata, distribution="casadata"),
        },
        "configuration": {
            "measurespath": str(measures_path),
            "datapath": [
                str(pathlib.Path(value).expanduser().resolve()) for value in data_paths
            ],
        },
        "data_versions": {
            "casarundata": _compact_data_info(
                casaconfig.get_data_info(type="casarundata")
            ),
            "measures": _compact_data_info(casaconfig.get_data_info(type="measures")),
        },
        "data_trees": {
            "geodetic": _required_tree(measures_path / "geodetic", "geodetic"),
            "vla": _required_tree(data_root / "nrao" / "VLA", "nrao/VLA"),
        },
    }
    validate_identity(identity, stable=False)
    return identity


def validate_result(value: Any, *, expected_casa_version: str | None = None) -> None:
    if not isinstance(value, dict):
        raise IdentityError("runtime identity result must be an object")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise IdentityError(f"runtime identity schema_version must be {SCHEMA_VERSION}")
    if value.get("kind") != RESULT_KIND or value.get("status") != "completed":
        raise IdentityError("runtime identity result is not completed")
    _exact_fields(
        value,
        {"schema_version", "kind", "status", "identity", "identity_sha256"},
        "runtime identity result",
    )
    identity = value.get("identity")
    if not isinstance(identity, dict):
        raise IdentityError("runtime identity result.identity must be an object")
    validate_identity(identity, stable=False)
    digest = value.get("identity_sha256")
    if digest != stable_identity_sha256(identity):
        raise IdentityError("runtime identity digest does not match its payload")
    if expected_casa_version is not None:
        actual = (
            identity.get("modules", {}).get("casatasks", {}).get("reported_version")
        )
        if actual != expected_casa_version:
            raise IdentityError(
                f"CASA version mismatch: expected {expected_casa_version}, got {actual}"
            )


def _module_identity(
    module: Any, *, distribution: str, reported_version: str | None = None
) -> dict[str, Any]:
    module_path = pathlib.Path(module.__file__).resolve()
    try:
        distribution_version = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        distribution_version = None
    return {
        "distribution_version": distribution_version,
        "reported_version": reported_version,
        "module_file": str(module_path),
        "module_file_sha256": sha256_file(module_path),
        "code_tree": _distribution_code_identity(module, distribution=distribution),
    }


def _distribution_code_identity(module: Any, *, distribution: str) -> dict[str, Any]:
    """Hash installed package code/config by distribution-relative path."""

    try:
        metadata = importlib.metadata.distribution(distribution)
    except importlib.metadata.PackageNotFoundError as error:
        raise IdentityError(
            f"CASA distribution is not installed: {distribution}"
        ) from error
    files = metadata.files
    if files is None:
        raise IdentityError(f"CASA distribution has no file inventory: {distribution}")
    package = str(module.__name__).split(".", 1)[0]
    digest = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    for relative in sorted(files, key=lambda value: value.as_posix()):
        parts = relative.parts
        if not parts or parts[0] != package:
            continue
        if "__pycache__" in parts or relative.suffix in {".pyc", ".pyo"}:
            continue
        # The casadata wheel is mostly a second copy of runtime data.  Its two
        # package entry points are code; the configured measures/VLA trees are
        # hashed independently below and are the data actually used by CASA.
        if distribution == "casadata" and len(parts) > 2:
            continue
        path = pathlib.Path(metadata.locate_file(relative))
        if not path.is_file() or path.is_symlink():
            raise IdentityError(
                f"CASA distribution inventory is not a regular file: {path}"
            )
        size = path.stat().st_size
        file_digest = sha256_file(path)
        digest.update(f"{relative.as_posix()}\0{size}\0{file_digest}\n".encode("utf-8"))
        file_count += 1
        total_bytes += size
    if file_count == 0:
        raise IdentityError(f"CASA distribution code tree is empty: {distribution}")
    return {
        "tree_sha256": digest.hexdigest(),
        "file_count": file_count,
        "size_bytes": total_bytes,
        "policy": "package_files_without_bytecode_v1",
    }


def _parse_config(lines: Any) -> dict[str, Any]:
    if not isinstance(lines, list):
        raise IdentityError("casaconfig.get_config() did not return a list")
    result: dict[str, Any] = {}
    for line in lines:
        if not isinstance(line, str) or "=" not in line:
            continue
        name, encoded = line.split("=", 1)
        try:
            result[name.strip()] = ast.literal_eval(encoded.strip())
        except (SyntaxError, ValueError):
            continue
    return result


def _required_directory(value: Any, label: str) -> pathlib.Path:
    if not isinstance(value, str) or not value:
        raise IdentityError(f"CASA {label} is unset")
    path = pathlib.Path(value).expanduser().resolve()
    if not path.is_dir():
        raise IdentityError(f"CASA {label} is not a directory: {path}")
    return path


def _required_tree(path: pathlib.Path, label: str) -> dict[str, Any]:
    if not path.is_dir():
        raise IdentityError(f"CASA {label} tree is missing: {path}")
    return {
        "path": str(path.resolve()),
        **tree_identity(path, excluded_names=VOLATILE_DATA_NAMES),
    }


def _compact_data_info(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise IdentityError("casaconfig data info must be an object or null")
    return {key: value.get(key) for key in ("version", "date", "site") if key in value}


def _load_request(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise IdentityError(f"cannot read identity request {path}: {error}") from error
    if not isinstance(value, dict):
        raise IdentityError("identity request must be an object")
    expected = {"schema_version", "kind", "expected_casa_version"}
    if set(value) != expected:
        raise IdentityError("identity request fields do not match the protocol")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise IdentityError(f"identity request schema_version must be {SCHEMA_VERSION}")
    if value.get("kind") != REQUEST_KIND:
        raise IdentityError(f"identity request kind must be {REQUEST_KIND!r}")
    if not isinstance(value.get("expected_casa_version"), str):
        raise IdentityError("identity request expected_casa_version must be a string")
    return value


def _write_json_atomic(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = pathlib.Path(handle.name)
        json.dump(value, handle, allow_nan=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2:
        print(
            "usage: casa_runtime_identity.py REQUEST.json RESULT.json", file=sys.stderr
        )
        return 2
    output_path = pathlib.Path(arguments[1])
    try:
        request = _load_request(pathlib.Path(arguments[0]))
        identity = capture_runtime_identity()
        result = {
            "schema_version": SCHEMA_VERSION,
            "kind": RESULT_KIND,
            "status": "completed",
            "identity": identity,
            "identity_sha256": stable_identity_sha256(identity),
        }
        validate_result(result, expected_casa_version=request["expected_casa_version"])
    except Exception as error:
        result = {
            "schema_version": SCHEMA_VERSION,
            "kind": RESULT_KIND,
            "status": "failed",
            "failure": {
                "kind": "runtime_identity",
                "reason": str(error),
                "exception_type": type(error).__name__,
            },
        }
    _write_json_atomic(output_path, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
