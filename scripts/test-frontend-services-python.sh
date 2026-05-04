#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

out_root="$repo_root/target/frontend-bindings"
python_out="$out_root/python"

scripts/generate-frontend-bindings.sh "$out_root"

python3 - "$python_out" "$repo_root/crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz" <<'PY'
from __future__ import annotations

import sys
import tarfile
import tempfile
from pathlib import Path

bindings_dir = Path(sys.argv[1])
fixture_archive = Path(sys.argv[2])
sys.path.insert(0, str(bindings_dir))

import casars_frontend_services as frontend

with tempfile.TemporaryDirectory() as tmp:
    project_root = Path(tmp)
    with tarfile.open(fixture_archive, "r:gz") as archive:
        extract_kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(project_root, **extract_kwargs)

    ms_path = project_root / "mssel_test_small.ms"
    project = frontend.probe_project(str(project_root))
    datasets = {dataset.path: dataset for dataset in project.datasets}

    assert str(ms_path) in datasets
    dataset = datasets[str(ms_path)]
    assert dataset.kind == frontend.DatasetKind.MEASUREMENT_SET
    assert dataset.fields
    assert dataset.spectral_windows
    assert dataset.antennas
    assert dataset.data_columns == ["DATA"]
    assert "ANTENNA (required)" in dataset.subtables

    direct_probe = frontend.probe_path(str(ms_path))
    assert direct_probe is not None
    assert direct_probe.kind == frontend.DatasetKind.MEASUREMENT_SET
    assert direct_probe.data_columns == ["DATA"]

print("frontend services Python UniFFI smoke passed")
PY
