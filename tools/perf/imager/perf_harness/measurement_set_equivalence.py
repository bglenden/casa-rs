# SPDX-License-Identifier: LGPL-3.0-or-later
"""Strict read-only proof for relocated, owner-initialized MeasurementSets.

Scientific storage remains byte-identical. Only MAIN's owner keyword and
append-only HISTORY provenance may differ semantically; table locks are volatile.
This is an internal reuse check, not an alternative dataset identity contract.
"""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import sys
import tempfile
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from perf_harness.casa_protocol import run_json_file_protocol
from perf_harness.errors import HarnessError
from perf_harness.tree_identity import sha256_file


OWNER_KEYWORD = "CASA_RS_IMAGING_OWNER_MANIFEST"
SEMANTIC_FILES = frozenset({"table.dat", "HISTORY/table.f0"})


def _inventory(root: pathlib.Path) -> tuple[dict[str, tuple[int, ...]], set[str], int]:
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise HarnessError(
            f"MS equivalence requires an absolute real directory: {root}"
        )
    files = {}
    directories = set()
    locks = 0
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise HarnessError(f"MS equivalence rejects symlinks: {path}")
        if path.is_dir():
            directories.add(relative)
        elif not path.is_file():
            raise HarnessError(f"MS equivalence rejects non-regular files: {path}")
        elif path.name == "table.lock":
            locks += 1
        else:
            stat = path.stat()
            files[relative] = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )
    return files, directories, locks


def _normalize_table_keywords(value: Any, root: pathlib.Path) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_table_keywords(item, root) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_table_keywords(item, root) for item in value]
    if isinstance(value, str) and value.startswith("Table: "):
        prefix = "Table: " + str(root)
        if value != prefix and not value.startswith(prefix + "/"):
            raise HarnessError(
                f"MS equivalence table reference is outside the dataset: {value}"
            )
        suffix = value[len(prefix) :]
        target = pathlib.Path(value[len("Table: ") :])
        if (
            ".." in suffix.split("/")
            or not target.is_dir()
            or not target.resolve().is_relative_to(root.resolve())
        ):
            raise HarnessError(
                f"MS equivalence rejects escaping or missing table reference: {value}"
            )
        return "Table: <MS_ROOT>" + suffix
    return value


def _encoded(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def _table_metadata(
    metadata: dict[str, Any], root: pathlib.Path, *, main: bool = False
) -> dict[str, Any]:
    value = copy.deepcopy(metadata)
    keyword_records = [value["keywords"], value["description"].get("_keywords_", {})]
    if main:
        for keywords in keyword_records:
            keywords.pop(OWNER_KEYWORD, None)
    keyword_records.extend(
        description["keywords"]
        for name, description in value["description"].items()
        if name in value["columns"] and "keywords" in description
    )
    for keywords in keyword_records:
        normalized = _normalize_table_keywords(keywords, root)
        keywords.clear()
        keywords.update(normalized)
    return value


def _history(
    current: dict[str, Any], historical: dict[str, Any]
) -> list[dict[str, Any]]:
    current_rows = current["rows"]
    historical_rows = historical["rows"]
    current_metadata = copy.deepcopy(current["metadata"])
    historical_metadata = copy.deepcopy(historical["metadata"])
    if (
        current_metadata.pop("rows") != len(current_rows)
        or historical_metadata.pop("rows") != len(historical_rows)
        or len(current_rows) < len(historical_rows)
        or _encoded(current_rows[: len(historical_rows)]) != _encoded(historical_rows)
    ):
        raise HarnessError(
            "MS equivalence HISTORY common rows are not an unchanged prefix"
        )
    # Appending HISTORY rows can grow its existing StandardStMan index, but
    # cannot change its columns, keywords, manager kind, or other manager state.
    for key, old in historical_metadata["data_managers"].items():
        new = current_metadata["data_managers"].get(key)
        if (
            new is not None
            and old.get("TYPE") == new.get("TYPE") == "StandardStMan"
            and len(current_rows) > len(historical_rows)
        ):
            old_index = old.get("SPEC", {}).get("IndexLength")
            new_index = new.get("SPEC", {}).get("IndexLength")
            if (
                isinstance(old_index, int)
                and isinstance(new_index, int)
                and new_index >= old_index
            ):
                new["SPEC"]["IndexLength"] = old_index
    if _encoded(current_metadata) != _encoded(historical_metadata):
        raise HarnessError(
            "MS equivalence HISTORY schema, keywords, or data managers differ"
        )
    return current_rows[len(historical_rows) :]


def validate_measurement_set_equivalence(
    current: pathlib.Path, historical: pathlib.Path, *, casa_python: str
) -> dict[str, Any]:
    """Verify complete storage and semantic equality before accepting relocation."""
    try:
        before = [_inventory(root) for root in (current, historical)]
        if set(before[0][0]) != set(before[1][0]) or before[0][1] != before[1][1]:
            raise HarnessError("MS equivalence file/directory inventory differs")
        if not {"table.dat", "HISTORY/table.dat"}.issubset(before[0][0]):
            raise HarnessError("MS equivalence requires MAIN and HISTORY metadata")
        digest = hashlib.sha256()
        files = size = 0
        for relative in sorted(before[0][0]):
            if relative in SEMANTIC_FILES:
                continue
            left_size, right_size = before[0][0][relative][2], before[1][0][relative][2]
            if left_size != right_size:
                raise HarnessError(
                    f"MS equivalence scientific payload size differs: {relative}"
                )
            left_hash = sha256_file(current / relative)
            if left_hash != sha256_file(historical / relative):
                raise HarnessError(
                    f"MS equivalence scientific payload differs: {relative}"
                )
            digest.update(f"{relative}\0{left_size}\0{left_hash}\n".encode())
            files += 1
            size += left_size
        metadata = [_read_metadata(root, casa_python) for root in (current, historical)]
        main = [
            _table_metadata(value["main"], root, main=True)
            for value, root in zip(metadata, (current, historical))
        ]
        if _encoded(main[0]) != _encoded(main[1]):
            raise HarnessError(
                "MS equivalence MAIN rows, schema, keywords, or data managers differ"
            )
        histories = [
            {
                "metadata": _table_metadata(value["history"]["metadata"], root),
                "rows": value["history"]["rows"],
            }
            for value, root in zip(metadata, (current, historical))
        ]
        appended = _history(*histories)
        for root, snapshot in zip((current, historical), before):
            after = _inventory(root)
            if after[:2] != snapshot[:2]:
                raise HarnessError(
                    f"MS changed while checking content equivalence: {root}"
                )
        return {
            "current_path": str(current),
            "historical_path": str(historical),
            "scientific_payload_files": files,
            "scientific_payload_bytes": size,
            "scientific_payload_sha256": digest.hexdigest(),
            "main_metadata_sha256": hashlib.sha256(_encoded(main[0])).hexdigest(),
            "history_original_rows": len(histories[1]["rows"]),
            "history_current_rows": len(histories[0]["rows"]),
            "history_appended_rows": appended,
            "excluded_lock_files": {
                "current": before[0][2],
                "historical": before[1][2],
            },
        }
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise HarnessError(
            f"cannot establish MeasurementSet equivalence: {error}"
        ) from error


def _read_metadata(path: pathlib.Path, casa_python: str) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="casa-ms-equivalence-") as directory:
        scratch = pathlib.Path(directory)
        result = run_json_file_protocol(
            casa_python=casa_python,
            script=pathlib.Path(__file__),
            request={"path": str(path)},
            request_path=scratch / "request.json",
            output_path=scratch / "output.json",
            log_path=scratch / "metadata.log",
            cwd=scratch,
            timeout_seconds=120,
        )
        if result.status != "completed" or result.output is None:
            raise HarnessError(
                f"read-only MS metadata comparison failed: {result.reason}"
            )
        return result.output


def _inspect(path: pathlib.Path) -> dict[str, Any]:
    import numpy as np
    from casatools import table

    def plain(value: Any) -> Any:
        if isinstance(value, np.ndarray):
            return {
                "dtype": str(value.dtype),
                "shape": list(value.shape),
                "values": plain(value.tolist()),
            }
        if isinstance(value, np.generic):
            return plain(value.item())
        if isinstance(value, dict):
            return {key: plain(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [plain(item) for item in value]
        if isinstance(value, complex):
            return {"complex": [value.real, value.imag]}
        return value

    result = {}
    for name, selected in (("main", path), ("history", path / "HISTORY")):
        handle = table()
        handle.open(str(selected), nomodify=True)
        try:
            metadata = plain(
                {
                    "rows": handle.nrows(),
                    "columns": handle.colnames(),
                    "description": handle.getdesc(),
                    "keywords": handle.getkeywords(),
                    "data_managers": handle.getdminfo(),
                }
            )
            if name == "main":
                result[name] = metadata
            else:
                if handle.nrows() > 100_000:
                    raise HarnessError(
                        "bounded MS equivalence rejects HISTORY larger than 100000 rows"
                    )
                result[name] = {
                    "metadata": metadata,
                    "rows": [
                        plain(
                            {
                                column: handle.getcell(column, row)
                                for column in handle.colnames()
                            }
                        )
                        for row in range(handle.nrows())
                    ],
                }
        finally:
            handle.close()
    return result


if __name__ == "__main__":
    request = json.loads(pathlib.Path(sys.argv[1]).read_text())
    if set(request) != {"path"}:
        raise HarnessError("MS metadata request must name exactly one path")
    pathlib.Path(sys.argv[2]).write_bytes(
        _encoded(_inspect(pathlib.Path(request["path"])))
    )
