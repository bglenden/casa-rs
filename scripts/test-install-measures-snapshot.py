#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check that only the verified measures archive receives scientific provenance."""

import hashlib
import io
import json
import runpy
import tarfile
import tempfile
import unittest
from pathlib import Path


installer = runpy.run_path(str(Path(__file__).with_name("install-measures-snapshot.py")))
install = installer["install"]
PROVENANCE_NAME = installer["PROVENANCE_NAME"]


class SnapshotInstallerTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.bundle = self.root / "snapshot.tar.gz"
        self.manifest = self.root / "snapshot.json"
        self.destination = self.root / "installed"
        self.provenance = {
            "source": "demonstration snapshot",
            "release": "test",
            "license": "test",
            "included_paths": ["table"],
            "excluded_paths": [],
        }
        contents = b"original data\n"
        with tarfile.open(self.bundle, "w:gz") as bundle:
            member = tarfile.TarInfo("table")
            member.size = len(contents)
            bundle.addfile(member, io.BytesIO(contents))
        packed = self.bundle.read_bytes()
        self.release = dict(
            self.provenance,
            archive_size_bytes=len(packed),
            archive_sha256=hashlib.sha256(packed).hexdigest(),
        )
        self.manifest.write_text(json.dumps(self.release))

    def test_install_and_replace_legacy_cache(self):
        self.destination.mkdir()
        marker = self.destination / PROVENANCE_NAME
        marker.write_text(json.dumps(self.release))
        (self.destination / "table").write_bytes(b"stale data")

        self.assertEqual(install(self.bundle, self.manifest, self.destination, reuse=True), self.provenance)
        self.assertEqual(json.loads(marker.read_text()), self.provenance)
        self.assertEqual((self.destination / "table").read_bytes(), b"original data\n")

    def test_same_size_corruption_cannot_be_stamped_or_replace_cache(self):
        install(self.bundle, self.manifest, self.destination)
        packed = bytearray(self.bundle.read_bytes())
        packed[-1] ^= 1
        self.bundle.write_bytes(packed)

        fresh = self.root / "fresh-install"
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            install(self.bundle, self.manifest, fresh)
        self.assertFalse(fresh.exists())
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            install(self.bundle, self.manifest, self.destination, reuse=True)
        self.assertEqual(json.loads((self.destination / PROVENANCE_NAME).read_text()), self.provenance)
        self.assertEqual((self.destination / "table").read_bytes(), b"original data\n")


if __name__ == "__main__":
    unittest.main()
