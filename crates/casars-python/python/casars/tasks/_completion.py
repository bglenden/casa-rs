"""Records of a successful task's declared products.

Rust decodes a task's result and checks its products; these classes carry
that decoding to Python as ``TaskCompletion.result``.
"""

from __future__ import annotations

import enum
from typing import Any


class TaskProductRole(enum.Enum):
    """What a product is to its task."""

    PRIMARY = 0
    AUXILIARY = 1
    PREVIEW = 2


class TaskProductKind(enum.Enum):
    """The kind of data a product holds."""

    MEASUREMENT_SET = 0
    CASA_IMAGE = 1
    CASA_TABLE = 2
    FITS_IMAGE = 3
    FILE = 4


class TaskCompletionProduct:
    """One product a task declared, with whether it exists on disk."""

    id: str
    role: TaskProductRole
    resource_kind: TaskProductKind
    label: str
    path: str
    exists: bool
    preview_path: str | None
    preview_exists: bool
    diagnostic: str | None

    def __init__(
        self,
        *,
        id: str,
        role: TaskProductRole,
        resource_kind: TaskProductKind,
        label: str,
        path: str,
        exists: bool,
        preview_path: str | None,
        preview_exists: bool,
        diagnostic: str | None,
    ) -> None:
        self.id = id
        self.role = role
        self.resource_kind = resource_kind
        self.label = label
        self.path = path
        self.exists = exists
        self.preview_path = preview_path
        self.preview_exists = preview_exists
        self.diagnostic = diagnostic

    def __str__(self) -> str:
        return (
            f"TaskCompletionProduct(id={self.id}, role={self.role}, "
            f"resource_kind={self.resource_kind}, label={self.label}, path={self.path}, "
            f"exists={self.exists}, preview_path={self.preview_path}, "
            f"preview_exists={self.preview_exists}, diagnostic={self.diagnostic})"
        )

    def __eq__(self, other: Any) -> bool:
        return (
            self.id == other.id
            and self.role == other.role
            and self.resource_kind == other.resource_kind
            and self.label == other.label
            and self.path == other.path
            and self.exists == other.exists
            and self.preview_path == other.preview_path
            and self.preview_exists == other.preview_exists
            and self.diagnostic == other.diagnostic
        )

    __hash__ = None  # type: ignore[assignment]


class TaskCompletionProjection:
    """A successful task's summary, declared products and diagnostics."""

    surface_id: str
    summary: str
    products: list[TaskCompletionProduct]
    diagnostics: list[str]

    def __init__(
        self,
        *,
        surface_id: str,
        summary: str,
        products: list[TaskCompletionProduct],
        diagnostics: list[str],
    ) -> None:
        self.surface_id = surface_id
        self.summary = summary
        self.products = products
        self.diagnostics = diagnostics

    def __str__(self) -> str:
        return (
            f"TaskCompletionProjection(surface_id={self.surface_id}, summary={self.summary}, "
            f"products={self.products}, diagnostics={self.diagnostics})"
        )

    def __eq__(self, other: Any) -> bool:
        return (
            self.surface_id == other.surface_id
            and self.summary == other.summary
            and self.products == other.products
            and self.diagnostics == other.diagnostics
        )

    __hash__ = None  # type: ignore[assignment]


def completion_projection(record: dict[str, Any]) -> TaskCompletionProjection:
    """The records of a completion that Rust decoded."""

    return TaskCompletionProjection(
        surface_id=record["surface_id"],
        summary=record["summary"],
        products=[
            TaskCompletionProduct(
                **{
                    **product,
                    "role": TaskProductRole[product["role"]],
                    "resource_kind": TaskProductKind[product["resource_kind"]],
                }
            )
            for product in record["products"]
        ],
        diagnostics=record["diagnostics"],
    )
