# SPDX-License-Identifier: LGPL-3.0-or-later
"""LLDB-side no-CF-generation guard for a separately supervised diagnostic.

The caller owns the single outer wall/RSS budget and immutable input manifest.
Its Python bootstrap imports CASA, raises SIGSTOP, then invokes the diagnostic
only after continuation. This module never supplies an imaging recipe or
changes the frozen CASA benchmark protocol. Import it inside LLDB; ordinary
Python can inspect the policy without importing CASA or LLDB.
"""

from pathlib import Path
import signal
import sys


# Guard both implementations, including native paths that bypass Python fill
# wrappers. These names must resolve in the exact manifested synthesis module.
GENERATION_ENTRIES = (
    "casa::AWConvFunc::makeConvFunction",
    "casa::AWConvFunc::fillConvFuncBuffer",
    "casa::refim::AWConvFunc::makeConvFunction",
    "casa::refim::AWConvFunc::makeConvFunction2",
    "casa::refim::AWConvFunc::fillConvFuncBuffer",
    "casa::refim::AWConvFunc::fillConvFuncBuffer2",
    "casa::SynthesisImagerVi2::dryGridding",
    "casa::SynthesisImagerVi2::fillCFCache",
)


class CfGuardFailure(RuntimeError):
    """The diagnostic cannot continue with verified native guard coverage."""


def resolved_entries(target, breakpoints, module_path, module_uuid):
    """Require enabled, unhit, entry-address traps in the exact loaded module."""
    expected_path = Path(module_path).resolve()
    records = []
    if len(breakpoints) != len(GENERATION_ENTRIES):
        raise CfGuardFailure("generation entry inventory is incomplete")
    for name, breakpoint in zip(GENERATION_ENTRIES, breakpoints):
        if not breakpoint.IsValid() or not breakpoint.IsEnabled():
            raise CfGuardFailure(f"generation breakpoint is disabled: {name}")
        if breakpoint.GetHitCount() or breakpoint.GetNumLocations() == 0:
            raise CfGuardFailure(f"generation entry was hit or unresolved: {name}")
        for index in range(breakpoint.GetNumLocations()):
            location = breakpoint.GetLocationAtIndex(index)
            address = location.GetAddress()
            module = address.GetModule()
            spec = module.GetFileSpec()
            actual_path = Path(spec.GetDirectory()) / spec.GetFilename()
            symbol = address.GetSymbol()
            if (
                not location.IsEnabled()
                or not location.IsResolved()
                or actual_path.resolve() != expected_path
                or module.GetUUIDString() != module_uuid
                or not symbol.IsValid()
                or address.GetFileAddress() != symbol.GetStartAddress().GetFileAddress()
            ):
                raise CfGuardFailure(f"unverified native entry trap: {name}")
            records.append({
                "entry": name,
                "symbol": symbol.GetName(),
                "file_address": address.GetFileAddress(),
                "load_address": address.GetLoadAddress(target),
                "module_uuid": module.GetUUIDString(),
            })
    return records


def run_guarded(debugger, *, python, bootstrap_args, environment, working_directory,
                stdout_path, stderr_path, module_path, module_uuid):
    """Run once under an external guardian, validating before workload entry.

    One initial interpreter exec stop may continue to the bootstrap SIGSTOP.
    Any native generation breakpoint, unexpected stop, unresolved guard, launch
    error or nonzero child exit fails. A stopped child is killed, never resumed
    to execute a forbidden body. Success requires normal child termination and
    zero guard hits; it does not establish CF compatibility or numerical parity.
    """
    import lldb

    debugger.SetAsync(False)
    command = lldb.SBCommandReturnObject()
    debugger.GetCommandInterpreter().HandleCommand(
        "settings set target.skip-prologue false", command
    )
    if not command.Succeeded():
        raise CfGuardFailure(command.GetError())
    target = debugger.CreateTarget(str(python))
    if not target.IsValid():
        raise CfGuardFailure("invalid CASA Python target")
    breakpoints = [
        target.BreakpointCreateByName(name, Path(module_path).name)
        for name in GENERATION_ENTRIES
    ]
    error = lldb.SBError()
    process = target.Launch(
        debugger.GetListener(), list(bootstrap_args),
        [f"{key}={value}" for key, value in sorted(environment.items())],
        "/dev/null", str(stdout_path), str(stderr_path), str(working_directory),
        0, False, error,
    )
    try:
        if not error.Success() or not process.IsValid():
            raise CfGuardFailure(f"CASA debugger launch failed: {error}")
        for startup_stop in range(2):
            if process.GetState() != lldb.eStateStopped:
                raise CfGuardFailure("bootstrap did not stop before imaging")
            stops = [
                thread for thread in process
                if thread.GetStopReason() != lldb.eStopReasonNone
            ]
            if (startup_stop == 0 and len(stops) == 1
                    and stops[0].GetStopReason() == lldb.eStopReasonExec):
                if any(not point.IsValid() or not point.IsEnabled()
                       or point.GetHitCount() for point in breakpoints):
                    raise CfGuardFailure("generation guard changed before interpreter exec")
                continued = process.Continue()
                if not continued.Success():
                    raise CfGuardFailure(f"bootstrap startup continuation failed: {continued}")
                continue
            break
        if len(stops) != 1 or (
            stops[0].GetStopReason() != lldb.eStopReasonSignal
            or stops[0].GetStopReasonDataAtIndex(0) != signal.SIGSTOP
        ):
            descriptions = [
                {"reason": thread.GetStopReason(),
                 "description": thread.GetStopDescription(1024)}
                for thread in stops
            ]
            raise CfGuardFailure(f"unexpected stop before guard validation: {descriptions}")
        entries = resolved_entries(target, breakpoints, module_path, module_uuid)
        # Do not deliver the bootstrap's stop signal a second time.
        if not process.GetUnixSignals().SetShouldSuppress(signal.SIGSTOP, True):
            raise CfGuardFailure("cannot suppress the bootstrap stop signal")
        continued = process.Continue()
        if not continued.Success():
            raise CfGuardFailure(f"guarded continuation failed: {continued}")
        hits = {
            name: breakpoint.GetHitCount()
            for name, breakpoint in zip(GENERATION_ENTRIES, breakpoints)
        }
        if any(hits.values()):
            raise CfGuardFailure(f"native CF generation attempted: {hits}")
        if process.GetState() != lldb.eStateExited or process.GetExitStatus() != 0:
            raise CfGuardFailure("guarded diagnostic stopped or exited unsuccessfully")
        return {"native_entries": entries, "generation_hits": hits, "exit_status": 0}
    finally:
        if process.IsValid() and process.GetState() not in (
            lldb.eStateExited, lldb.eStateDetached, lldb.eStateInvalid,
        ):
            killed = process.Kill()
            if not killed.Success():
                message = f"outer guardian must terminate child: {killed}"
                failure = sys.exc_info()[1]
                if failure is not None:
                    raise CfGuardFailure(message) from failure
                else:
                    raise CfGuardFailure(message)


def bootstrap(workload):
    """Load the native library, then wait for LLDB's guard validation."""
    import os
    import runpy
    import casatools  # noqa: F401 - loading the installed native library is the prerequisite

    os.kill(os.getpid(), signal.SIGSTOP)
    runpy.run_path(str(workload), run_name="__main__")


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--bootstrap":
        raise SystemExit("LLDB bootstrap only: --bootstrap <manifested-workload.py>")
    workload = Path(sys.argv[2]).resolve(strict=True)
    # Do not leak bootstrap arguments into the separately manifested workload.
    sys.argv = [str(workload)]
    bootstrap(workload)
