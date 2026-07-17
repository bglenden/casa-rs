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

    plot = frontend.build_measurement_set_plot(
        frontend.MeasurementSetPlotRequest(
            dataset_path=str(ms_path),
            preset=frontend.MeasurementSetPlotPreset.AMPLITUDE_VS_FREQUENCY,
            field=None,
            spectral_window=None,
            timerange=None,
            uvrange=None,
            antenna=None,
            scan=None,
            correlation=None,
            array=None,
            observation=None,
            intent=None,
            feed=None,
            msselect=None,
            data_column="DATA",
            color_by=None,
            avgchannel=None,
            avgtime=None,
            avgscan=False,
            avgfield=False,
            avgbaseline=False,
            avgantenna=False,
            avgspw=False,
            scalar=False,
            iteraxis=None,
            width=640,
            height=420,
            max_plot_points=10000,
        )
    )
    assert plot.preset == frontend.MeasurementSetPlotPreset.AMPLITUDE_VS_FREQUENCY
    assert plot.render.image_format == "none"
    assert plot.image_bytes == b""
    assert plot.x_axis.label
    assert plot.y_axis.label
    assert plot.series
    assert plot.sampling.rendered_point_count > 0

print("frontend services Python UniFFI smoke passed")
PY
