# SPDX-License-Identifier: LGPL-3.0-or-later
"""Terminal usage must cover the child, including memory released before exit."""

import pathlib
import subprocess
import sys
import unittest


class TerminalUsageTests(unittest.TestCase):
    def run_child(self, code):
        wrapper = pathlib.Path(__file__).with_name("subprocesses.py")
        return subprocess.run(
            [sys.executable, str(wrapper), "--", sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=15,
        )

    def test_terminal_high_water_includes_released_allocation(self):
        result = self.run_child(
            "data = bytearray(64 * 1024 * 1024); "
            "data[::4096] = b'x' * (len(data) // 4096); "
            "del data; raise SystemExit(7)"
        )
        self.assertEqual(result.returncode, 7)
        lines = [line for line in result.stderr.splitlines()
                 if line.startswith("imager_bench_process_resource ")]
        self.assertEqual(len(lines), 1)
        fields = dict(item.split("=", 1) for item in lines[0].split()[1:])
        self.assertEqual(fields["exit_code"], "7")
        self.assertEqual(fields["source"], "wait4")
        self.assertEqual(fields["scope"], "terminal_child")
        self.assertGreaterEqual(int(fields["peak_rss_bytes"]), 64 * 1024 * 1024)
        self.assertLess(int(fields["peak_rss_bytes"]), 512 * 1024 * 1024)

    def test_signal_exit_is_not_success(self):
        result = self.run_child("import os, signal; os.kill(os.getpid(), signal.SIGTERM)")
        self.assertEqual(result.returncode, 143)
        self.assertIn("exit_code=-15", result.stderr)


if __name__ == "__main__":
    unittest.main()
