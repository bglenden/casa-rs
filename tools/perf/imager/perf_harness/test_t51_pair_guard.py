# SPDX-License-Identifier: LGPL-3.0-or-later

import unittest
from unittest.mock import Mock, patch

from .t51_pair_guard import RSS_BYTES, process_scope, run_pair_pipeline


class PairGuardTests(unittest.TestCase):
    def test_scope_tracks_escaped_debugger_descendants_and_reparenting(self):
        rows = "10 1 10 20\n11 10 11 30\n12 11 11 40\n20 1 20 1000\n"
        owned, rss = process_scope(rows, 10, set())
        self.assertEqual(owned, {10, 11, 12})
        self.assertEqual(rss, 90 * 1024)
        owned, rss = process_scope("12 1 11 45\n20 1 20 1000\n", 10, owned)
        self.assertEqual((owned, rss), ({12}, 45 * 1024))

    def test_malformed_census_fails(self):
        for rows in ("10 1 10 -1", "10 1 10", "10 1 10 1\n10 1 10 1"):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                process_scope(rows, 10, set())

    def test_total_deadline_is_not_reset_after_launch(self):
        proc = Mock(pid=10, returncode=-9)
        proc.poll.return_value = None
        with patch("subprocess.Popen", return_value=proc) as launch, \
                patch("time.monotonic", side_effect=[100, 1000, 1000.01]), \
                patch("os.killpg") as kill, patch("os.kill"), \
                patch("subprocess.check_output") as census:
            receipt = run_pair_pipeline(["approved-pipeline"], cwd=".", environment={}, log=None)
        launch.assert_called_once()
        census.assert_not_called()
        kill.assert_called_once_with(10, 9)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["stop_reason"], "wall_cap")

    def test_rss_equal_to_cap_is_a_failure(self):
        proc = Mock(pid=10, returncode=-9)
        proc.poll.return_value = None
        with patch("subprocess.Popen", return_value=proc), \
                patch("time.monotonic", side_effect=[0, 1, 2]), \
                patch("subprocess.check_output", return_value=f"10 1 10 {RSS_BYTES // 1024}"), \
                patch("os.killpg") as kill, patch("os.kill"):
            receipt = run_pair_pipeline(["approved-pipeline"], cwd=".", environment={}, log=None)
        kill.assert_called_once_with(10, 9)
        self.assertEqual(receipt["stop_reason"], "rss_cap")

    def test_guardian_error_kills_child(self):
        proc = Mock(pid=10)
        proc.poll.return_value = None
        with patch("subprocess.Popen", return_value=proc), \
                patch("subprocess.check_output", side_effect=ValueError("census failed")), \
                patch("os.killpg") as kill, patch("os.kill"):
            with self.assertRaisesRegex(ValueError, "census failed"):
                run_pair_pipeline(["approved-pipeline"], cwd=".", environment={}, log=None)
        self.assertTrue(kill.called)
        proc.wait.assert_called_once()


if __name__ == "__main__":
    unittest.main()
