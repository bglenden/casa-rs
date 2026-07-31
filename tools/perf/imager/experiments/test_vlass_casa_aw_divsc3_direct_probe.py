#!/usr/bin/env python3

from __future__ import annotations

import struct
import unittest

import vlass_casa_aw_divsc3_direct_probe as subject


class DirectDivsc3ProbeTests(unittest.TestCase):
    def test_decodes_forward_aarch64_bl(self) -> None:
        callsite = 0xB7C600
        target = 0xCB2F00
        immediate = (target - callsite) // 4
        instruction = 0x94000000 | (immediate & 0x03FFFFFF)

        self.assertEqual(subject.decode_aarch64_bl(callsite, instruction), target)

    def test_rejects_non_bl_instruction(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not an AArch64 BL"):
            subject.decode_aarch64_bl(0x1000, 0xD65F03C0)

    def test_classifies_fixed_outcomes(self) -> None:
        self.assertEqual(
            subject.classify(
                subject.EXPECTED_SOURCE_ZERO,
                subject.EXPECTED_SOURCE_1446,
            ),
            "installed-helper-reproduces-official-source1446",
        )
        self.assertEqual(
            subject.classify(
                subject.EXPECTED_SOURCE_ZERO,
                subject.EXPECTED_RUST_SOURCE_1446,
            ),
            "installed-helper-matches-rust-helper",
        )
        self.assertEqual(
            subject.classify(subject.EXPECTED_SOURCE_ZERO, [1, 2]),
            "installed-helper-returns-other-result",
        )
        self.assertEqual(
            subject.classify([1, 2], subject.EXPECTED_SOURCE_1446),
            "invalid-source0-control-fails",
        )

    def test_maps_vmaddr_through_file_backed_segment(self) -> None:
        metadata = {
            "segments": [
                {
                    "name": "__TEXT",
                    "vmaddr": 0x1000,
                    "vmsize": 0x400,
                    "fileoff": 0x200,
                    "filesize": 0x300,
                }
            ]
        }
        self.assertEqual(subject.vmaddr_to_file_offset(metadata, 0x1120), 0x320)
        with self.assertRaisesRegex(RuntimeError, "not backed by file bytes"):
            subject.vmaddr_to_file_offset(metadata, 0x1400)

    def test_parses_minimal_macho_metadata(self) -> None:
        uuid = bytes.fromhex("DAFE59815FBA39BBB616E28B1B2BAEEB")
        segment = struct.pack(
            "<II16sQQQQIIII",
            subject.LC_SEGMENT_64,
            72,
            b"__TEXT\0\0\0\0\0\0\0\0\0\0",
            0,
            0x2000,
            0,
            0x1000,
            7,
            5,
            0,
            0,
        )
        uuid_command = struct.pack("<II16s", subject.LC_UUID, 24, uuid)
        header = struct.pack(
            "<IIIIIIII",
            subject.MH_MAGIC_64,
            0,
            0,
            6,
            2,
            len(segment) + len(uuid_command),
            0,
            0,
        )

        metadata = subject.macho_metadata(header + segment + uuid_command)

        self.assertEqual(metadata["uuid"], subject.EXPECTED_IMAGE_UUID)
        self.assertEqual(metadata["segments"][0]["name"], "__TEXT")


if __name__ == "__main__":
    unittest.main()
