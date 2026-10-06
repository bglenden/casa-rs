# SPDX-License-Identifier: LGPL-3.0-or-later
"""Policy tests do not substitute for the installed native trap preflight."""

from pathlib import Path
import signal
import sys
import unittest
from unittest.mock import MagicMock, patch

from .casa_cf_guard import (
    CfGuardFailure, GENERATION_ENTRIES, bootstrap, resolved_entries, run_guarded,
)


class CfGuardTests(unittest.TestCase):
    def breakpoints(self):
        points = []
        for name in GENERATION_ENTRIES:
            point = MagicMock()
            point.GetHitCount.return_value = 0
            point.GetNumLocations.return_value = 1
            location = point.GetLocationAtIndex.return_value
            address = location.GetAddress.return_value
            module = address.GetModule.return_value
            module.GetFileSpec.return_value.GetDirectory.return_value = "/tmp"
            module.GetFileSpec.return_value.GetFilename.return_value = "synthesis.dylib"
            module.GetUUIDString.return_value = "expected-uuid"
            address.GetSymbol.return_value.GetName.return_value = name
            address.GetFileAddress.return_value = 123
            address.GetLoadAddress.return_value = 456
            start = address.GetSymbol.return_value.GetStartAddress.return_value
            start.GetFileAddress.return_value = 123
            points.append(point)
        return points

    def test_every_entry_requires_an_enabled_unhit_resolved_exact_module_start(self):
        target = object()
        points = self.breakpoints()
        records = resolved_entries(target, points, "/tmp/synthesis.dylib", "expected-uuid")
        self.assertEqual([record["entry"] for record in records], list(GENERATION_ENTRIES))
        location = "GetLocationAtIndex.return_value."
        address = location + "GetAddress.return_value."
        mutations = {
            "IsValid.return_value": False,
            "IsEnabled.return_value": False,
            "GetHitCount.return_value": 1,
            "GetNumLocations.return_value": 0,
            location + "IsEnabled.return_value": False,
            location + "IsResolved.return_value": False,
            address + "GetModule.return_value.GetUUIDString.return_value": "foreign",
            address + "GetFileAddress.return_value": 124,
        }
        for attribute, value in mutations.items():
            with self.subTest(attribute=attribute):
                points = self.breakpoints()
                points[-1].configure_mock(**{attribute: value})
                with self.assertRaises(CfGuardFailure):
                    resolved_entries(target, points, "/tmp/synthesis.dylib", "expected-uuid")
        with self.assertRaises(CfGuardFailure):
            resolved_entries(target, points[:-1], "/tmp/synthesis.dylib", "expected-uuid")
        with self.assertRaises(CfGuardFailure):
            resolved_entries(target, self.breakpoints(), "/tmp/foreign.dylib", "expected-uuid")

    def test_only_verified_bootstrap_stop_can_continue_and_later_stops_are_killed(self):
        cases = ((2, 0, False, True), (1, 0, False, False),
                 (1, 1, False, False), (1, 0, True, False))
        for terminal, hits, unresolved, accepted in cases:
            with self.subTest(terminal=terminal, hits=hits, unresolved=unresolved):
                api = MagicMock(eStateStopped=1, eStateExited=2, eStateDetached=3,
                                eStateInvalid=4, eStopReasonNone=0, eStopReasonSignal=5)
                debugger = MagicMock()
                target = debugger.CreateTarget.return_value
                points = self.breakpoints()
                target.BreakpointCreateByName.side_effect = points
                process = target.Launch.return_value
                process.GetState.side_effect = [1, terminal, terminal]
                process.GetExitStatus.return_value = 0
                stopped = MagicMock()
                stopped.GetStopReason.return_value = 5
                stopped.GetStopReasonDataAtIndex.return_value = signal.SIGSTOP
                process.__iter__.return_value = iter([stopped])
                points[0].GetHitCount.return_value = hits
                arguments = dict(
                    python=Path("/python"), bootstrap_args=["bootstrap.py"],
                    environment={}, working_directory="/tmp", stdout_path="/tmp/out",
                    stderr_path="/tmp/err", module_path="/tmp/synthesis.dylib",
                    module_uuid="expected-uuid",
                )
                with patch.dict(sys.modules, {"lldb": api}), patch(
                    "perf_harness.casa_cf_guard.resolved_entries", return_value=[]
                ) as verify:
                    if unresolved:
                        verify.side_effect = CfGuardFailure("unresolved")
                    if accepted:
                        self.assertEqual(run_guarded(debugger, **arguments)["exit_status"], 0)
                    else:
                        with self.assertRaises(CfGuardFailure):
                            run_guarded(debugger, **arguments)
                    verify.assert_called_once()
                self.assertEqual(process.Continue.call_count, int(not unresolved))
                self.assertEqual(process.Kill.call_count, int(terminal == 1))

    def test_bootstrap_stops_before_entering_workload(self):
        calls = []
        with patch.dict(sys.modules, {"casatools": MagicMock()}), patch(
            "os.kill", side_effect=lambda pid, sig: calls.append(("stop", sig))
        ), patch("runpy.run_path", side_effect=lambda *a, **k: calls.append(("run", a, k))):
            bootstrap("manifested.py")
        self.assertEqual(calls, [
            ("stop", signal.SIGSTOP),
            ("run", ("manifested.py",), {"run_name": "__main__"}),
        ])

    def test_initial_exec_must_be_followed_by_verified_bootstrap_stop(self):
        for next_reason, next_signal, accepted in (
            (5, signal.SIGSTOP, True), (7, 0, False),
            (5, signal.SIGSEGV, False), (3, 0, False),
        ):
            with self.subTest(next_reason=next_reason, next_signal=next_signal):
                api = MagicMock(eStateStopped=1, eStateExited=2, eStateDetached=3,
                                eStateInvalid=4, eStopReasonNone=0,
                                eStopReasonSignal=5, eStopReasonExec=7)
                debugger = MagicMock()
                target = debugger.CreateTarget.return_value
                points = self.breakpoints()
                target.BreakpointCreateByName.side_effect = points
                process = target.Launch.return_value
                initial = MagicMock()
                initial.GetStopReason.return_value = 7
                bootstrap_stop = MagicMock()
                bootstrap_stop.GetStopReason.return_value = next_reason
                bootstrap_stop.GetStopReasonDataAtIndex.return_value = next_signal
                process.__iter__.side_effect = lambda: iter(
                    [initial if process.Continue.call_count == 0 else bootstrap_stop]
                )
                process.GetState.side_effect = lambda: (
                    2 if accepted and process.Continue.call_count == 2 else 1
                )
                process.GetExitStatus.return_value = 0
                with patch.dict(sys.modules, {"lldb": api}), patch(
                    "perf_harness.casa_cf_guard.resolved_entries", return_value=[]
                ) as verify:
                    arguments = dict(
                        python=Path("/python"), bootstrap_args=["bootstrap.py"],
                        environment={}, working_directory="/tmp", stdout_path="/tmp/out",
                        stderr_path="/tmp/err", module_path="/tmp/synthesis.dylib",
                        module_uuid="expected-uuid",
                    )
                    if accepted:
                        self.assertEqual(run_guarded(debugger, **arguments)["exit_status"], 0)
                        verify.assert_called_once()
                    else:
                        with self.assertRaises(CfGuardFailure):
                            run_guarded(debugger, **arguments)
                        verify.assert_not_called()
                self.assertEqual(process.Continue.call_count, 2 if accepted else 1)
                self.assertEqual(process.Kill.call_count, int(not accepted))


if __name__ == "__main__":
    unittest.main()
