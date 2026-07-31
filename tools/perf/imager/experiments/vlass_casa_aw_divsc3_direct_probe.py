#!/usr/bin/env python3
"""Invoke the exact image-local CASA AW complex-division helper.

The probe calls the private ``___divsc3`` branch target used by the installed
CASA 6.7.5.18 ``refim::AWVisResampler::GridToData``.  It executes two fixed
ordinary-finite operand sets and no CASA task or imaging operation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import subprocess
import tempfile
from pathlib import Path

import vlass_casa_aw_division_codegen_audit as codegen


EXPECTED_LIBRARY_SHA256 = (
    "0e86c46963025b4deac2bd2b795788dac46f333b4c72a966846b96a8afb2f697"
)
EXPECTED_IMAGE_UUID = "DAFE5981-5FBA-39BB-B616-E28B1B2BAEEB"
EXPECTED_CALLSITE_VMADDR = 0xB7C600
EXPECTED_HELPER_VMADDR = 0xCB2F00
EXPECTED_HELPER_RETURN_VMADDR = 0xCB2F80
EXPECTED_SOURCE_ZERO = [0x3DA31298, 0x3DD885FC]
EXPECTED_SOURCE_1446 = [0xBB7D5A3C, 0xBE9077C1]
EXPECTED_RUST_SOURCE_1446 = [0xBB7D5A3D, 0xBE9077C1]
LC_SEGMENT_64 = 0x19
LC_UUID = 0x1B
MH_MAGIC_64 = 0xFEEDFACF


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def decode_aarch64_bl(callsite: int, instruction: int) -> int:
    if instruction & 0xFC000000 != 0x94000000:
        raise RuntimeError("official CASA call site is not an AArch64 BL")
    immediate = instruction & 0x03FFFFFF
    if immediate & (1 << 25):
        immediate -= 1 << 26
    return callsite + immediate * 4


def macho_metadata(data: bytes) -> dict[str, object]:
    if len(data) < 32:
        raise RuntimeError("installed CASA library is not a Mach-O 64 image")
    magic, _, _, _, ncmds, sizeofcmds, _, _ = struct.unpack_from("<IIIIIIII", data, 0)
    if magic != MH_MAGIC_64 or len(data) < 32 + sizeofcmds:
        raise RuntimeError("installed CASA library has an invalid Mach-O header")
    cursor = 32
    segments: list[dict[str, int | str]] = []
    image_uuid: str | None = None
    for _ in range(ncmds):
        command, command_size = struct.unpack_from("<II", data, cursor)
        if command_size < 8 or cursor + command_size > len(data):
            raise RuntimeError("installed CASA library has an invalid load command")
        if command == LC_SEGMENT_64:
            (
                _,
                _,
                raw_name,
                vmaddr,
                vmsize,
                fileoff,
                filesize,
                _,
                _,
                _,
                _,
            ) = struct.unpack_from("<II16sQQQQIIII", data, cursor)
            segments.append(
                {
                    "name": raw_name.split(b"\0", 1)[0].decode("ascii"),
                    "vmaddr": vmaddr,
                    "vmsize": vmsize,
                    "fileoff": fileoff,
                    "filesize": filesize,
                }
            )
        elif command == LC_UUID:
            value = data[cursor + 8 : cursor + 24]
            hex_value = value.hex().upper()
            image_uuid = (
                f"{hex_value[0:8]}-{hex_value[8:12]}-{hex_value[12:16]}-"
                f"{hex_value[16:20]}-{hex_value[20:32]}"
            )
        cursor += command_size
    if image_uuid is None:
        raise RuntimeError("installed CASA library has no LC_UUID")
    return {"uuid": image_uuid, "segments": segments}


def vmaddr_to_file_offset(metadata: dict[str, object], vmaddr: int) -> int:
    segments = metadata["segments"]
    assert isinstance(segments, list)
    for segment in segments:
        assert isinstance(segment, dict)
        start = int(segment["vmaddr"])
        filesize = int(segment["filesize"])
        if start <= vmaddr < start + filesize:
            return int(segment["fileoff"]) + vmaddr - start
    raise RuntimeError(f"vmaddr 0x{vmaddr:x} is not backed by file bytes")


def classify(source_zero: list[int], source_1446: list[int]) -> str:
    if source_zero != EXPECTED_SOURCE_ZERO:
        return "invalid-source0-control-fails"
    if source_1446 == EXPECTED_SOURCE_1446:
        return "installed-helper-reproduces-official-source1446"
    if source_1446 == EXPECTED_RUST_SOURCE_1446:
        return "installed-helper-matches-rust-helper"
    return "installed-helper-returns-other-result"


def normalized_disassembly(
    instructions: list[dict[str, str]],
) -> list[str]:
    return [
        f"{instruction['address']} {instruction['mnemonic']} "
        f"{instruction['operands']}".rstrip()
        for instruction in instructions
    ]


def probe(library: Path, output: Path, compiler: str) -> dict[str, object]:
    if output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {output}")
    library_data = library.read_bytes()
    library_sha256 = sha256_bytes(library_data)
    if library_sha256 != EXPECTED_LIBRARY_SHA256:
        raise RuntimeError("installed CASA synthesis dylib checksum changed")
    metadata = macho_metadata(library_data)
    if metadata["uuid"] != EXPECTED_IMAGE_UUID:
        raise RuntimeError("installed CASA synthesis dylib UUID changed")

    audit = codegen.audit(library)
    call = audit["grid_to_data"]["divsc3_call"]
    callsite_vmaddr = int(call["address"], 16)
    if callsite_vmaddr != EXPECTED_CALLSITE_VMADDR:
        raise RuntimeError("official CASA AW division call site changed")
    helper_instructions = codegen.disassemble_functions(
        library, {codegen.DIVSC3_SYMBOL}
    )[codegen.DIVSC3_SYMBOL]
    return_indices = [
        index
        for index, instruction in enumerate(helper_instructions)
        if instruction["mnemonic"] == "ret"
    ]
    if not return_indices:
        raise RuntimeError("official CASA division helper has no return")
    helper_through_return = helper_instructions[: return_indices[0] + 1]
    helper_vmaddr = int(helper_through_return[0]["address"], 16)
    helper_return_vmaddr = int(helper_through_return[-1]["address"], 16)
    if (
        helper_vmaddr != EXPECTED_HELPER_VMADDR
        or helper_return_vmaddr != EXPECTED_HELPER_RETURN_VMADDR
    ):
        raise RuntimeError("official CASA division helper extent changed")

    callsite_offset = vmaddr_to_file_offset(metadata, callsite_vmaddr)
    helper_offset = vmaddr_to_file_offset(metadata, helper_vmaddr)
    callsite_bytes = library_data[callsite_offset : callsite_offset + 4]
    helper_bytes = library_data[
        helper_offset : helper_offset + helper_return_vmaddr - helper_vmaddr + 4
    ]
    call_instruction = struct.unpack("<I", callsite_bytes)[0]
    if decode_aarch64_bl(callsite_vmaddr, call_instruction) != helper_vmaddr:
        raise RuntimeError("official CASA BL does not target the audited helper")

    source = Path(__file__).with_suffix(".c")
    analyzer = Path(__file__).resolve()
    with tempfile.TemporaryDirectory(
        prefix="casa-rs-vlass-divsc3-direct-probe-"
    ) as temporary:
        executable = Path(temporary) / "vlass-casa-aw-divsc3-direct-probe"
        compile_command = [
            compiler,
            "-std=c11",
            "-O2",
            "-Wall",
            "-Wextra",
            "-Werror",
            str(source),
            "-o",
            str(executable),
        ]
        subprocess.run(compile_command, check=True)
        executable_sha256 = sha256_file(executable)
        environment = os.environ.copy()
        prior_library_path = environment.get("DYLD_LIBRARY_PATH")
        environment["DYLD_LIBRARY_PATH"] = (
            str(library.parent)
            if not prior_library_path
            else f"{library.parent}:{prior_library_path}"
        )
        completed = subprocess.run(
            [
                str(executable),
                str(library),
                hex(callsite_vmaddr),
                hex(helper_vmaddr),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        runtime = json.loads(completed.stdout)

    if runtime["image_uuid"] != EXPECTED_IMAGE_UUID:
        raise RuntimeError("runtime image UUID does not match the frozen image")
    if int(runtime["callsite_vmaddr"], 16) != callsite_vmaddr:
        raise RuntimeError("runtime call-site vmaddr changed")
    if int(runtime["helper_vmaddr"], 16) != helper_vmaddr:
        raise RuntimeError("runtime helper vmaddr changed")
    if int(runtime["call_instruction"], 16) != call_instruction:
        raise RuntimeError("runtime call-site bytes differ from file bytes")
    if int(runtime["decoded_target"], 16) != int(runtime["helper_runtime"], 16):
        raise RuntimeError("runtime BL target does not equal the invoked helper")
    if (
        runtime["fpcr_before"] != runtime["fpcr_after"]
        or runtime["fegetround_before"] != runtime["fegetround_after"]
        or runtime["fegetround_before"] != 0
    ):
        raise RuntimeError("floating-point environment is not stable FE_TONEAREST")

    source_zero = runtime["source_zero"]
    source_1446 = runtime["source_1446"]
    classification = classify(source_zero, source_1446)
    valid = classification != "invalid-source0-control-fails"
    result = {
        "schema": "casa-rs-vlass-casa-aw-divsc3-direct-probe-v1",
        "classification": classification,
        "valid": valid,
        "casa_version": "6.7.5.18",
        "casa_source_commit": "418bb1a26df7c4aba663ff123b038b75a6fa0295",
        "library": {
            "path": str(library),
            "sha256": library_sha256,
            "bytes": len(library_data),
            "uuid": metadata["uuid"],
        },
        "static_identity": {
            "callsite_vmaddr": f"0x{callsite_vmaddr:016x}",
            "callsite_instruction": f"0x{call_instruction:08x}",
            "callsite_bytes_sha256": sha256_bytes(callsite_bytes),
            "decoded_helper_vmaddr": f"0x{helper_vmaddr:016x}",
            "helper_return_vmaddr": f"0x{helper_return_vmaddr:016x}",
            "helper_code_bytes": len(helper_bytes),
            "helper_code_sha256": sha256_bytes(helper_bytes),
            "callsite_disassembly": [
                f"{call['address']} {call['mnemonic']} {call['operands']}"
            ],
            "helper_disassembly": normalized_disassembly(helper_through_return),
        },
        "runtime_identity": runtime,
        "argument_mapping": {
            "s0": "numerator.re",
            "s1": "numerator.im",
            "s2": "normalizer.re",
            "s3": "normalizer.im",
            "return_s0": "quotient.re",
            "return_s1": "quotient.im",
        },
        "fixed_evidence_points": {
            "source_zero": {
                "numerator": [0x3DA00F0F, 0x3DC30CDE],
                "normalizer": [0x3F6E1694, 0xBD1ED44B],
                "required": EXPECTED_SOURCE_ZERO,
                "actual": source_zero,
            },
            "source_1446": {
                "numerator": [0x39C7D0F4, 0xBE8D50A9],
                "normalizer": [0x3F7A5C92, 0x3C71A8AE],
                "official_required": EXPECTED_SOURCE_1446,
                "rust_helper": EXPECTED_RUST_SOURCE_1446,
                "actual": source_1446,
                "c_equivalent_wide_graph": runtime["rust_equivalent_wide_graph"],
            },
        },
        "probe": {
            "source": str(source),
            "source_sha256": sha256_file(source),
            "analyzer": str(analyzer),
            "analyzer_sha256": sha256_file(analyzer),
            "compiler": compiler,
            "compile_flags": [
                "-std=c11",
                "-O2",
                "-Wall",
                "-Wextra",
                "-Werror",
            ],
            "executable_sha256": executable_sha256,
        },
        "authorization": (
            "native-rust-port-of-exact-installed-helper-instruction-graph-"
            "for-ordinary-finite-operands-only"
            if classification == "installed-helper-reproduces-official-source1446"
            else "no-production-arithmetic-change"
        ),
        "prohibited_work": {
            "casa_task_executed": False,
            "measurement_set_read": False,
            "prediction_executed": False,
            "grid_executed": False,
            "fft_executed": False,
            "products_formed": False,
            "clean_executed": False,
        },
    }
    if not valid:
        raise RuntimeError("source-0 validity control failed")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--compiler", default="clang")
    args = parser.parse_args()
    if not args.library.is_file():
        raise RuntimeError(f"installed CASA library is missing: {args.library}")
    result = probe(args.library.resolve(), args.output, args.compiler)
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
