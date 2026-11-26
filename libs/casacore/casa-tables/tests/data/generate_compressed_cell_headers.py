# SPDX-License-Identifier: LGPL-3.0-or-later
"""Generate metadata-only CompressComplex fixtures with python-casacore 3.8.1.

The omitted TSM payload contains nine 32-byte packed Int32 tiles. Reading one
cell needs a 32-byte decoded tile and a 288-byte contiguous read-ahead buffer.
"""
import gzip
import io
from pathlib import Path
import tarfile
import tempfile

import numpy as np
from casacore.tables import makearrcoldesc, maketabdesc, table


archive = io.BytesIO()
with tempfile.TemporaryDirectory() as directory, tarfile.open(fileobj=archive, mode="w") as output:
    for manager in ("CompressComplex", "CompressComplexSD"):
        path = Path(directory) / manager
        description = maketabdesc([
            makearrcoldesc("DATA", 0j, ndim=2, shape=[9, 1], valuetype="complex"),
            makearrcoldesc("PACKED", 0, ndim=2, shape=[9, 1], valuetype="int"),
        ])
        managers = {
            "packed": {"TYPE": "TiledShapeStMan", "NAME": "packed", "COLUMNS": ["PACKED"],
                       "SPEC": {"DEFAULTTILESHAPE": np.array([1, 1, 8], dtype=np.int32)}},
            "complex": {"TYPE": manager, "NAME": "complex", "COLUMNS": ["DATA"],
                        "SPEC": {"SOURCENAME": "DATA", "TARGETNAME": "PACKED",
                                 "SCALE": 1., "OFFSET": 0., "AUTOSCALE": False}},
        }
        with table(str(path), description, nrow=1, dminfo=managers, ack=False) as source:
            source.putcell("DATA", 0, np.zeros((9, 1), dtype=np.complex64))
            np.testing.assert_array_equal(source.getcell("DATA", 0), np.zeros((9, 1)))
        for name in ("table.dat", "table.f0", "table.info"):
            data = (path / name).read_bytes()
            entry = tarfile.TarInfo(f"{manager}/{name}")
            entry.size = len(data)
            entry.mode = 0o644
            output.addfile(entry, io.BytesIO(data))
Path(__file__).with_name("compressed-cell-headers.tar.gz").write_bytes(
    gzip.compress(archive.getvalue(), mtime=0)
)
