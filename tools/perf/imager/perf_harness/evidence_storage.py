# SPDX-License-Identifier: LGPL-3.0-or-later
"""Internal durable-storage contracts for imaging evidence workloads.

Workload manifests already carry a human-readable ``run.storage_label``.  This
module binds selected labels to machine-enforced storage requirements without
putting dataset-specific path branches in the generic workload runner.
"""

from __future__ import annotations

from dataclasses import dataclass
import pathlib
from typing import Any, Iterable

from .errors import HarnessError


REQUIREMENT_SCHEMA_VERSION = 1
REQUIREMENT_KIND = "imaging_evidence_storage_requirement"


@dataclass(frozen=True)
class _StoragePolicy:
    required_root: pathlib.Path
    minimum_free_bytes: int
    forbidden_path_parts: tuple[str, ...]

    def record(self, policy_id: str) -> dict[str, Any]:
        return {
            "schema_version": REQUIREMENT_SCHEMA_VERSION,
            "kind": REQUIREMENT_KIND,
            "policy_id": policy_id,
            "required_root": str(self.required_root),
            "minimum_free_bytes": self.minimum_free_bytes,
            "forbidden_path_parts": list(self.forbidden_path_parts),
        }


# This is contract data owned by the evidence workload, not selection logic in
# the generic runner.  Keep the exact issue boundary and stop threshold stable.
_POLICIES = {
    "GLENDENNING-vlass-archive": _StoragePolicy(
        required_root=pathlib.Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446"),
        minimum_free_bytes=1 << 40,
        forbidden_path_parts=("_tmp_safe_to_delete",),
    ),
}
_DATASET_POLICY_PREFIXES = {
    "vlass-fragment": "GLENDENNING-vlass-archive",
}


def requirement_for_storage_label(storage_label: str) -> dict[str, Any] | None:
    """Return an immutable-plan record for a known durable storage label."""

    policy = _POLICIES.get(storage_label)
    return None if policy is None else policy.record(storage_label)


def requirement_for_workload(
    *, dataset_key: str, storage_label: str
) -> dict[str, Any] | None:
    """Bind protected dataset families to their manifest storage policy."""

    expected_policy = next(
        (
            policy_id
            for prefix, policy_id in _DATASET_POLICY_PREFIXES.items()
            if dataset_key.startswith(prefix)
        ),
        None,
    )
    if expected_policy is not None and storage_label != expected_policy:
        raise HarnessError(
            f"dataset {dataset_key!r} requires storage policy {expected_policy!r}"
        )
    return requirement_for_storage_label(storage_label)


def _canonical_requirement(
    value: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if value is None:
        return None
    policy_id = value.get("policy_id")
    if not isinstance(policy_id, str) or policy_id not in _POLICIES:
        raise HarnessError("unknown imaging evidence storage policy")
    expected = _POLICIES[policy_id].record(policy_id)
    if value != expected:
        raise HarnessError(
            f"imaging evidence storage requirement {policy_id!r} was modified"
        )
    return expected


def validate_requirement_paths(
    value: dict[str, Any] | None, *, paths: Iterable[pathlib.Path]
) -> None:
    """Validate evidence paths against a canonical requirement."""

    expected = _canonical_requirement(value)
    if expected is None:
        return

    required_root = pathlib.Path(expected["required_root"])
    forbidden_parts = set(expected["forbidden_path_parts"])
    for path in paths:
        if path != required_root and required_root not in path.parents:
            raise HarnessError(
                f"durable evidence path must be under {required_root}: {path}"
            )
        forbidden = sorted(forbidden_parts & set(path.parts))
        if forbidden:
            raise HarnessError(
                f"durable evidence path is disposable ({', '.join(forbidden)}): {path}"
            )


def validate_requirement_capacity(
    value: dict[str, Any] | None, *, available_bytes: int
) -> None:
    """Validate current free space against a canonical requirement."""

    expected = _canonical_requirement(value)
    if expected is None:
        return
    minimum_free_bytes = int(expected["minimum_free_bytes"])
    if available_bytes < minimum_free_bytes:
        raise HarnessError(
            f"evidence volume has {available_bytes} free bytes; stop threshold is "
            f"{minimum_free_bytes} bytes"
        )
