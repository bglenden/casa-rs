#!/usr/bin/env python3

from __future__ import annotations

import unittest

import vlass_casa_aw_division_codegen_audit as subject


class DisassemblyParserTests(unittest.TestCase):
    def test_extracts_only_requested_functions(self) -> None:
        lines = [
            "_ignored:\n",
            "0000000000000000 ret\n",
            f"{subject.GRID_TO_DATA_SYMBOL}:\n",
            "0000000000000004 bl ___divsc3\n",
            f"{subject.DIVSC3_SYMBOL}:\n",
            "0000000000000008 fcvt d31, s3\n",
            "_after:\n",
            "000000000000000c ret\n",
        ]

        functions = subject.parse_functions(
            lines,
            {subject.GRID_TO_DATA_SYMBOL, subject.DIVSC3_SYMBOL},
        )

        self.assertEqual(set(functions), {subject.GRID_TO_DATA_SYMBOL, "___divsc3"})
        self.assertEqual(
            functions[subject.GRID_TO_DATA_SYMBOL],
            [
                {
                    "address": "0000000000000004",
                    "mnemonic": "bl",
                    "operands": "___divsc3",
                }
            ],
        )
        self.assertEqual(
            functions[subject.DIVSC3_SYMBOL][0]["operands"],
            "d31, s3",
        )


if __name__ == "__main__":
    unittest.main()
