"""LLDB callbacks for the two-point installed CASA AW division trace."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import lldb


EXPECTED_HELPER_VMADDR = 0xCB2F00
EXPECTED_RETURN_VMADDR = 0xB7C604
MODULE_BASENAME = "libcasacpp_synthesis.6.dylib"
MODULE_UUID = "DAFE5981-5FBA-39BB-B616-E28B1B2BAEEB"
_state: dict[str, Any] | None = None


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise RuntimeError(f"refusing to overwrite LLDB trace: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _load_state() -> dict[str, Any]:
    global _state
    if _state is not None:
        return _state
    manifest_path = Path(os.environ["CASA_VLASS_CALLSITE_MANIFEST"])
    output_path = Path(os.environ["CASA_VLASS_CALLSITE_RAW_TRACE"])
    if output_path.exists():
        raise RuntimeError(f"refusing to overwrite LLDB trace: {output_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    targets = {
        int(target["helper_call_index"]): target for target in manifest["targets"]
    }
    if set(targets) != {0, 2892}:
        raise RuntimeError("LLDB manifest helper call indices changed")
    _state = {
        "manifest_path": str(manifest_path),
        "output_path": output_path,
        "targets_by_call": targets,
        "next_call_index": 0,
        "captured": [],
        "pending": None,
        "trace_thread_id": None,
        "completed": False,
        "invalid": None,
    }
    return _state


def _filespec_path(filespec: lldb.SBFileSpec) -> str:
    directory = filespec.GetDirectory()
    filename = filespec.GetFilename()
    if not filename:
        raise RuntimeError("LLDB file specification has no filename")
    return str(Path(directory, filename)) if directory else filename


def _module_identity(address: lldb.SBAddress) -> tuple[str, str, int]:
    module = address.GetModule()
    if not module.IsValid():
        raise RuntimeError("stopped address has no module")
    path = _filespec_path(module.GetFileSpec())
    uuid = module.GetUUIDString()
    return path, uuid, address.GetFileAddress()


def _register_u32(frame: lldb.SBFrame, name: str) -> int:
    register = frame.FindRegister(name)
    if not register.IsValid():
        raise RuntimeError(f"LLDB register {name} is unavailable")
    error = lldb.SBError()
    value = register.GetData().GetUnsignedInt32(error, 0)
    if error.Fail():
        raise RuntimeError(f"read LLDB register {name}: {error}")
    return int(value)


def _register_u64(frame: lldb.SBFrame, name: str) -> int:
    register = frame.FindRegister(name)
    if not register.IsValid():
        raise RuntimeError(f"LLDB register {name} is unavailable")
    error = lldb.SBError()
    value = register.GetValueAsUnsigned(error, 0)
    if error.Fail():
        raise RuntimeError(f"read LLDB register {name}: {error}")
    return int(value)


def _invalidate(frame: lldb.SBFrame, error: BaseException) -> bool:
    state = _load_state()
    state["invalid"] = str(error)
    output = {
        "schema": "casa-rs-vlass-casa-aw-divsc3-callsite-raw-trace-v1",
        "status": "invalid",
        "error": str(error),
        "next_call_index": state["next_call_index"],
        "targets": state["captured"],
    }
    try:
        _atomic_json(state["output_path"], output)
    except BaseException:
        pass
    return True


def helper_entry_callback(
    frame: lldb.SBFrame,
    _breakpoint_location: lldb.SBBreakpointLocation,
    _internal_dict: dict[str, Any],
) -> bool:
    try:
        state = _load_state()
        target = frame.GetThread().GetProcess().GetTarget()
        helper_path, helper_uuid, helper_vmaddr = _module_identity(frame.GetPCAddress())
        if (
            Path(helper_path).name != MODULE_BASENAME
            or helper_uuid != MODULE_UUID
            or helper_vmaddr != EXPECTED_HELPER_VMADDR
        ):
            raise RuntimeError("LLDB helper breakpoint resolved to the wrong image")

        link_register = _register_u64(frame, "x30")
        return_address = target.ResolveLoadAddress(link_register)
        return_path, return_uuid, return_vmaddr = _module_identity(return_address)
        if Path(return_path).name != MODULE_BASENAME or return_uuid != MODULE_UUID:
            return False
        if return_vmaddr != EXPECTED_RETURN_VMADDR:
            return False

        call_index = state["next_call_index"]
        state["next_call_index"] += 1
        if call_index > 2892:
            raise RuntimeError("LLDB trace passed the target call without stopping")
        target_identity = state["targets_by_call"].get(call_index)
        if target_identity is None:
            return False

        thread_id = int(frame.GetThread().GetThreadID())
        if state["trace_thread_id"] is None:
            state["trace_thread_id"] = thread_id
        elif state["trace_thread_id"] != thread_id:
            raise RuntimeError("CASA AW division targets ran on different threads")
        if state["pending"] is not None:
            raise RuntimeError("LLDB reached another target before the prior return")

        capture = {
            "source_ordinal": int(target_identity["source_ordinal"]),
            "helper_call_index": call_index,
            "role": target_identity["role"],
            "term": target_identity["term"],
            "thread_id": thread_id,
            "pre_bits": [_register_u32(frame, f"s{index}") for index in range(4)],
            "fpcr_before": _register_u64(frame, "fpcr"),
            "fpsr_before": _register_u64(frame, "fpsr"),
        }
        state["pending"] = capture
        breakpoint = target.BreakpointCreateByAddress(link_register)
        if not breakpoint.IsValid() or breakpoint.GetNumLocations() != 1:
            raise RuntimeError("LLDB could not create the one-shot return breakpoint")
        breakpoint.SetOneShot(True)
        breakpoint.SetThreadID(thread_id)
        breakpoint.SetScriptCallbackFunction(
            "vlass_casa_aw_divsc3_callsite_trace_lldb.return_callback"
        )
        return False
    except BaseException as error:
        return _invalidate(frame, error)


def return_callback(
    frame: lldb.SBFrame,
    breakpoint_location: lldb.SBBreakpointLocation,
    _internal_dict: dict[str, Any],
) -> bool:
    try:
        breakpoint = breakpoint_location.GetBreakpoint()
        if not breakpoint.IsValid():
            raise RuntimeError("LLDB return callback has no valid breakpoint")
        breakpoint.SetEnabled(False)
        state = _load_state()
        capture = state["pending"]
        if capture is None:
            raise RuntimeError("LLDB return breakpoint has no pending target")
        _, uuid, vmaddr = _module_identity(frame.GetPCAddress())
        if uuid != MODULE_UUID or vmaddr != EXPECTED_RETURN_VMADDR:
            raise RuntimeError("LLDB return breakpoint resolved to the wrong address")
        if int(frame.GetThread().GetThreadID()) != capture["thread_id"]:
            raise RuntimeError("LLDB return breakpoint changed threads")
        capture["post_bits"] = [
            _register_u32(frame, "s0"),
            _register_u32(frame, "s1"),
        ]
        capture["fpcr_after"] = _register_u64(frame, "fpcr")
        capture["fpsr_after"] = _register_u64(frame, "fpsr")
        state["captured"].append(capture)
        state["pending"] = None
        if capture["source_ordinal"] != 1446:
            return False

        target = frame.GetThread().GetProcess().GetTarget()
        module = frame.GetPCAddress().GetModule()
        slide = (
            frame.GetPCAddress().GetLoadAddress(target)
            - frame.GetPCAddress().GetFileAddress()
        )
        output = {
            "schema": "casa-rs-vlass-casa-aw-divsc3-callsite-raw-trace-v1",
            "status": "completed-at-source1446-return",
            "manifest": state["manifest_path"],
            "library_path": _filespec_path(module.GetFileSpec()),
            "library_uuid": module.GetUUIDString(),
            "image_slide": f"0x{slide:016x}",
            "helper_vmaddr": f"0x{EXPECTED_HELPER_VMADDR:016x}",
            "callsite_vmaddr": f"0x{EXPECTED_RETURN_VMADDR - 4:016x}",
            "return_vmaddr": f"0x{EXPECTED_RETURN_VMADDR:016x}",
            "callsite_disassembly": [
                "0000000000b7c5f8 fmov s0, s8",
                "0000000000b7c5fc fmov s1, s9",
                "0000000000b7c600 bl ___divsc3",
                "0000000000b7c648 stp s0, s1, [x8]",
            ],
            "trace_thread_id": state["trace_thread_id"],
            "filtered_call_count_through_target": state["next_call_index"],
            "targets": state["captured"],
            "stop_boundary": (
                "source1446-rr-tt0-immediately-after-divsc3-before-result-store"
            ),
        }
        _atomic_json(state["output_path"], output)
        state["completed"] = True
        return True
    except BaseException as error:
        return _invalidate(frame, error)


def require_complete(
    _debugger: lldb.SBDebugger,
    _command: str,
    result: lldb.SBCommandReturnObject,
    _internal_dict: dict[str, Any],
) -> None:
    try:
        state = _load_state()
        if not state["completed"]:
            raise RuntimeError(f"LLDB call-site trace incomplete: {state['invalid']}")
        result.AppendMessage("VLASS CASA AW call-site trace complete")
    except BaseException as error:
        result.SetError(str(error))


def install_helper_breakpoint(
    debugger: lldb.SBDebugger,
    _command: str,
    result: lldb.SBCommandReturnObject,
    _internal_dict: dict[str, Any],
) -> None:
    try:
        _load_state()
        target = debugger.GetSelectedTarget()
        matching_modules = [
            module
            for module in target.modules
            if Path(_filespec_path(module.GetFileSpec())).name == MODULE_BASENAME
            and module.GetUUIDString().upper() == MODULE_UUID
        ]
        if len(matching_modules) != 1:
            raise RuntimeError(
                "attached process does not contain exactly one frozen "
                "CASA synthesis image"
            )
        helper_address = matching_modules[0].ResolveFileAddress(EXPECTED_HELPER_VMADDR)
        helper_load_address = helper_address.GetLoadAddress(target)
        if helper_load_address == lldb.LLDB_INVALID_ADDRESS:
            raise RuntimeError("could not resolve loaded CASA helper address")
        prior_count = target.GetNumBreakpoints()
        command_result = lldb.SBCommandReturnObject()
        debugger.GetCommandInterpreter().HandleCommand(
            f"breakpoint set -H -a 0x{helper_load_address:x}",
            command_result,
        )
        if not command_result.Succeeded():
            raise RuntimeError(
                "could not install hardware helper breakpoint: "
                + command_result.GetError()
            )
        if target.GetNumBreakpoints() != prior_count + 1:
            raise RuntimeError("hardware helper breakpoint count changed")
        breakpoint = target.GetBreakpointAtIndex(prior_count)
        if not breakpoint.IsValid() or breakpoint.GetNumLocations() != 1:
            raise RuntimeError(
                "hardware helper breakpoint did not resolve exactly once"
            )
        breakpoint.SetScriptCallbackFunction(
            "vlass_casa_aw_divsc3_callsite_trace_lldb.helper_entry_callback"
        )
        result.AppendMessage(
            f"installed frozen CASA helper breakpoint at 0x{helper_load_address:x}"
        )
    except BaseException as error:
        result.SetError(str(error))


def __lldb_init_module(
    debugger: lldb.SBDebugger,
    _internal_dict: dict[str, Any],
) -> None:
    debugger.HandleCommand(
        "command script add -f "
        "vlass_casa_aw_divsc3_callsite_trace_lldb.install_helper_breakpoint "
        "vlass-install-callsite-breakpoint"
    )
    debugger.HandleCommand(
        "command script add -f "
        "vlass_casa_aw_divsc3_callsite_trace_lldb.require_complete "
        "vlass-require-callsite-trace"
    )
