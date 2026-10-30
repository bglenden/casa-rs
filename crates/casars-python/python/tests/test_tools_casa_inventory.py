"""The CASA tools #1025 adds to `casars.tools`: `image`, `coordsys`, `table`,
`msmetadata` and `ms` (ADR-0019).

Each class has every method of CASA 6.7.6.14's installed binding, with its
signature (`data/casa_<tool>_signatures.json`, written by
`tools/simulation/dump_casa_tool_signatures.py`, and a live CASA when one is
importable), and the object-surface bundle describes the same methods. A
method casa-rs does not implement yet raises `NotImplementedError` naming the
issue that records it. `IMPLEMENTED` grows as #1025's parts land; each
tool's own test file checks what an implemented method does.
"""

from __future__ import annotations

import inspect

import pytest

import casars
from casars import tools

from _casa_tools import (
    assert_bundle_describes,
    assert_casa_signatures,
    assert_live_signatures,
    casa_signatures,
)

#: Each tool, and the issue its refused methods name.
ISSUES = {"image": 1027, "coordsys": 1027, "table": 1028, "msmetadata": 1029, "ms": 1030}

#: The methods each tool implements.
IMPLEMENTED: dict[str, set[str]] = {tool: set() for tool in ISSUES}


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_the_class_is_casas(tool: str) -> None:
    cls = getattr(tools, tool)
    assert cls is getattr(casars._core, tool)
    assert cls.__name__ == tool
    assert cls.__module__ == "casars.tools"
    assert tool in tools.__all__


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_every_casa_method_exists_with_casas_signature(tool: str) -> None:
    assert_casa_signatures(getattr(tools, tool), casa_signatures(tool))


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_signatures_match_a_live_casa(tool: str) -> None:
    casatools = pytest.importorskip("casatools")
    assert_live_signatures(getattr(tools, tool), getattr(casatools, tool))


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_the_bundle_describes_each_method_and_what_casa_rs_does_with_it(tool: str) -> None:
    methods = set(casa_signatures(tool)["methods"])
    assert_bundle_describes(
        getattr(tools, tool),
        tool,
        {
            "implemented": IMPLEMENTED[tool],
            "not_yet_implemented": methods - IMPLEMENTED[tool],
        },
    )


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_a_method_casa_rs_lacks_names_its_issue(tool: str) -> None:
    cls = getattr(tools, tool)
    instance = cls()
    for name in sorted(set(casa_signatures(tool)["methods"]) - IMPLEMENTED[tool]):
        required = [
            parameter.name
            for parameter in inspect.signature(getattr(cls, name)).parameters.values()
            if parameter.name != "self" and parameter.default is inspect.Parameter.empty
        ]
        with pytest.raises(NotImplementedError, match=rf"^{tool}\.{name} .*\(#{ISSUES[tool]}\)$"):
            getattr(instance, name)(**{parameter: None for parameter in required})
