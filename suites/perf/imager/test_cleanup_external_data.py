#!/usr/bin/env python3
"""Tests for generated external-data cleanup planning."""

from __future__ import annotations

import pathlib
import shutil
import sys
import tempfile
import unittest


TOOL_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import cleanup_external_data as cleanup  # noqa: E402


class CleanupExternalDataTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = pathlib.Path(tempfile.mkdtemp())
        self.root = self.tmpdir / "GLENDENNING"
        self.parity_runs = (
            self.root
            / "casa-rs"
            / "tutorial-data"
            / "tutorial-parity"
            / "vla"
            / "flagging"
            / "parity-runs"
        )
        self.parity_runs.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_collects_only_dated_flagging_runs_and_trace_data_by_default(self) -> None:
        dated = self.parity_runs / "20260506-perf"
        dated.mkdir()
        keep_source = self.parity_runs / "README"
        keep_source.mkdir()
        io_trace = self.root / "casa-rs-imperformance" / "io-trace"
        io_trace.mkdir(parents=True)
        issue175 = self.root / "casa-rs" / "issue175-runs"
        issue175.mkdir(parents=True)

        candidates = cleanup.collect_candidates(self.root, include_issue175_runs=False)

        self.assertEqual(
            [dated, io_trace],
            [candidate.path for candidate in candidates],
        )

    def test_issue175_runs_are_opt_in(self) -> None:
        issue175 = self.root / "casa-rs" / "issue175-runs"
        issue175.mkdir(parents=True)

        candidates = cleanup.collect_candidates(self.root, include_issue175_runs=True)

        self.assertEqual([issue175], [candidate.path for candidate in candidates])

    def test_m100_split_parity_keeps_latest_run(self) -> None:
        work = (
            self.root
            / "casa-rs"
            / "tutorial-data"
            / "tutorial-parity"
            / "alma"
            / "m100"
            / "band3-combine"
            / "work"
        )
        old = work / "split-parity-20260504T025221Z"
        latest = work / "split-parity-20260504T132927Z"
        scratch = work / "scratch"
        old.mkdir(parents=True)
        latest.mkdir()
        scratch.mkdir()

        candidates = cleanup.collect_candidates(self.root, include_issue175_runs=False)

        self.assertEqual([old], [candidate.path for candidate in candidates])

    def test_keep_filter_accepts_basename(self) -> None:
        first = cleanup.Candidate(self.parity_runs / "20260506-perf", "test")
        second = cleanup.Candidate(self.parity_runs / "20260506-release", "test")

        filtered = cleanup.apply_keep_filters([first, second], ["20260506-perf"])

        self.assertEqual([second], filtered)


if __name__ == "__main__":
    unittest.main()
