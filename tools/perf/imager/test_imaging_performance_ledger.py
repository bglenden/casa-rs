#!/usr/bin/env python3
"""Tests for the broad imaging performance ledger validator."""

from __future__ import annotations

import json
import pathlib

import pytest

import imaging_performance_ledger as ledger_tool


def test_default_ledger_validates_and_covers_wave_issues() -> None:
    ledger = ledger_tool.load_ledger(ledger_tool.LEDGER_PATH)
    summary = ledger_tool.summarize(ledger)

    assert set(summary["wave_issues"]) == {56, 343, 262, 352}
    assert summary["workload_group_count"] >= 5
    assert summary["run_count"] >= 1


def test_ledger_rejects_missing_required_issue(tmp_path: pathlib.Path) -> None:
    ledger = json.loads(ledger_tool.LEDGER_PATH.read_text(encoding="utf-8"))
    ledger["wave_issues"] = [56, 343, 352]
    path = tmp_path / "ledger.json"
    path.write_text(json.dumps(ledger), encoding="utf-8")

    with pytest.raises(ledger_tool.LedgerError, match="wave_issues"):
        ledger_tool.load_ledger(path)
