// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use casa_imaging::{Deconvolver, HogbomIterationMode, RestoringBeamMode, WTermMode, WeightingMode};
use casa_ms::CubeAxisConfig;
use casa_test_support::{casatestdata_path, discover_casa_python};
use casars_imager::{
    CliConfig, SelectedRowTrace, SpectralMode, build_prepare_geometry_trace_from_config,
};
use serde::Deserialize;
use tempfile::tempdir;

fn casa_fixvis_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Deserialize)]
struct CasaGeometryOracle {
    selected_rows: Vec<SelectedRowTrace>,
    rows: Vec<CasaGeometryOracleRow>,
}

#[derive(Debug, Deserialize)]
struct CasaGeometryOracleRow {
    row_index: usize,
    input_field_id: usize,
    phase_center_field_id: Option<usize>,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    pointing_id: Option<i32>,
    antenna1_pointing_row: Option<usize>,
    antenna1_pointing_direction_rad: [f64; 2],
    antenna1_pointing_used_fallback: bool,
    antenna2_pointing_row: Option<usize>,
    antenna2_pointing_direction_rad: [f64; 2],
    antenna2_pointing_used_fallback: bool,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    raw_uvw_m: [f64; 3],
    imaging_uvw_m: [f64; 3],
}

#[test]
fn prepare_geometry_matches_casa_fixvis_on_ngc5921_native_phase_center() {
    let Some(ms_path) = dataset_path("measurementset/vla/ngc5921.ms") else {
        eprintln!(
            "Wave 2 geometry parity skipped: missing measurementset/vla/ngc5921.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let config = base_config(ms_path, SpectralMode::Mfs);
    assert_geometry_matches_fixvis_oracle(&config, "ngc5921.ms");
}

#[test]
fn prepare_geometry_matches_casa_fixvis_on_n2403_multifield_cube_wproject() {
    let Some(ms_path) = dataset_path("measurementset/vla/n2403.short.ms") else {
        eprintln!(
            "Wave 2 geometry parity skipped: missing measurementset/vla/n2403.short.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let mut config = base_config(ms_path, SpectralMode::Cube);
    config.field_ids = Some(vec![0, 1]);
    config.phasecenter_field = Some(0);
    config.w_term_mode = WTermMode::WProject;
    config.w_project_planes = Some(8);
    assert_geometry_matches_fixvis_oracle(&config, "n2403.short.ms");
}

#[test]
fn prepare_geometry_matches_casa_pointing_resolution_on_papersky_mosaic() {
    let Some(ms_path) = dataset_path("measurementset/evla/papersky_mosaic.ms") else {
        eprintln!(
            "Wave 14 pointing parity skipped: missing measurementset/evla/papersky_mosaic.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let mut config = base_config(ms_path, SpectralMode::Mfs);
    config.field_ids = Some((0..25).collect());
    config.phasecenter_field = Some(0);
    config.imsize = 128;
    config.cell_arcsec = 8.0;
    config.spw = Some(0);
    config.channel_start = Some(0);
    config.channel_count = Some(1);
    assert_geometry_matches_fixvis_oracle(&config, "papersky_mosaic.ms");
}

#[test]
fn prepare_geometry_matches_casa_fixvis_on_oneshiftpoint_mosaic() {
    let Some(ms_path) = dataset_path("measurementset/evla/refim_oneshiftpoint.mosaic.ms") else {
        eprintln!(
            "Wave 14 geometry parity skipped: missing measurementset/evla/refim_oneshiftpoint.mosaic.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let mut config = base_config(ms_path, SpectralMode::Mfs);
    config.field_ids = Some(vec![0, 1]);
    config.phasecenter_field = None;
    config.phasecenter = Some("J2000 5.233697011339747rad 0.7097745013495772rad".to_string());
    config.imsize = 1024;
    config.cell_arcsec = 10.0;
    config.spw = Some(0);
    config.channel_start = Some(0);
    config.channel_count = Some(1);
    assert_geometry_matches_fixvis_oracle(&config, "refim_oneshiftpoint.mosaic.ms");
}

fn assert_geometry_matches_fixvis_oracle(config: &CliConfig, staged_name: &str) {
    let Some(casa) = discover_casa_python() else {
        eprintln!("Wave 2 geometry parity skipped: no CASA-capable Python interpreter found");
        return;
    };
    if !fixvis_available(&casa.program) {
        eprintln!("Wave 2 geometry parity skipped: CASA Python has no casatasks.fixvis");
        return;
    }
    let _lock = casa_fixvis_lock().lock().unwrap();
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&config.ms, temp.path(), staged_name).expect("stage measurement set");
    let mut staged_config = config.clone();
    staged_config.ms = staged_ms_path.clone();
    let rust_trace = build_prepare_geometry_trace_from_config(&staged_config)
        .expect("build rust geometry trace");
    let casa_oracle =
        run_fixvis_geometry_oracle(&casa.program, &staged_ms_path, &rust_trace, &staged_config)
            .expect("run CASA fixvis geometry oracle");

    assert_eq!(rust_trace.selected_rows, casa_oracle.selected_rows);
    assert_eq!(rust_trace.rows.len(), casa_oracle.rows.len());
    for (rust_row, casa_row) in rust_trace.rows.iter().zip(casa_oracle.rows.iter()) {
        assert_eq!(rust_row.row_index, casa_row.row_index);
        assert_eq!(rust_row.input_field_id, casa_row.input_field_id);
        assert_eq!(
            rust_row.phase_center_field_id,
            casa_row.phase_center_field_id
        );
        assert_eq!(rust_row.ddid, casa_row.ddid);
        assert_eq!(rust_row.spw_id, casa_row.spw_id);
        assert_eq!(rust_row.polarization_id, casa_row.polarization_id);
        assert_eq!(rust_row.pointing_id, casa_row.pointing_id);
        assert_eq!(
            rust_row.antenna1_pointing_row,
            casa_row.antenna1_pointing_row
        );
        assert_eq!(
            rust_row.antenna1_pointing_used_fallback,
            casa_row.antenna1_pointing_used_fallback
        );
        assert_eq!(
            rust_row.antenna2_pointing_row,
            casa_row.antenna2_pointing_row
        );
        assert_eq!(
            rust_row.antenna2_pointing_used_fallback,
            casa_row.antenna2_pointing_used_fallback
        );
        assert_eq!(rust_row.antenna1_id, casa_row.antenna1_id);
        assert_eq!(rust_row.antenna2_id, casa_row.antenna2_id);
        assert_eq!(rust_row.is_cross, casa_row.is_cross);
        assert_vector2_close(
            rust_row.row_index,
            "antenna1_pointing_direction_rad",
            &rust_row.antenna1_pointing_direction_rad,
            &casa_row.antenna1_pointing_direction_rad,
            1.0e-9,
        );
        assert_vector2_close(
            rust_row.row_index,
            "antenna2_pointing_direction_rad",
            &rust_row.antenna2_pointing_direction_rad,
            &casa_row.antenna2_pointing_direction_rad,
            1.0e-9,
        );
        assert_vector_close(
            rust_row.row_index,
            "raw_uvw_m",
            &rust_row.raw_uvw_m,
            &casa_row.raw_uvw_m,
            1.0e-9,
        );
        assert_vector_close(
            rust_row.row_index,
            "imaging_uvw_m",
            &rust_row.imaging_uvw_m,
            &casa_row.imaging_uvw_m,
            1.0e-6,
        );
    }
}

fn assert_vector_close(
    row_index: usize,
    label: &str,
    actual: &[f64; 3],
    expected: &[f64; 3],
    tolerance: f64,
) {
    for (axis, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            (*actual - *expected).abs() <= tolerance,
            "row {row_index} {label}[{axis}] mismatch: actual={actual} expected={expected} tolerance={tolerance}"
        );
    }
}

fn assert_vector2_close(
    row_index: usize,
    label: &str,
    actual: &[f64; 2],
    expected: &[f64; 2],
    tolerance: f64,
) {
    for (axis, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
        let delta = if axis == 0 {
            circular_angle_delta(*actual, *expected)
        } else {
            *actual - *expected
        };
        assert!(
            delta.abs() <= tolerance,
            "row {row_index} {label}[{axis}] mismatch: actual={actual} expected={expected} tolerance={tolerance}"
        );
    }
}

fn circular_angle_delta(actual: f64, expected: f64) -> f64 {
    let two_pi = std::f64::consts::TAU;
    let mut delta = (actual - expected) % two_pi;
    if delta > std::f64::consts::PI {
        delta -= two_pi;
    } else if delta < -std::f64::consts::PI {
        delta += two_pi;
    }
    delta
}

fn base_config(ms_path: PathBuf, spectral_mode: SpectralMode) -> CliConfig {
    CliConfig {
        ms: ms_path,
        imagename: PathBuf::from("unused"),
        imsize: 64,
        cell_arcsec: 20.0,
        field_ids: Some(vec![0]),
        phasecenter_field: Some(0),
        phasecenter: None,
        ddid: None,
        spw: None,
        spw_selector: None,
        channel_start: None,
        channel_count: None,
        datacolumn: None,
        save_model: casars_imager::SaveModelMode::None,
        correlation: None,
        spectral_mode,
        cube_axis: CubeAxisConfig::default(),
        weighting: WeightingMode::Natural,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 0,
        gain: 0.1,
        threshold_jy: 0.0,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: 2,
        cyclefactor: 1.0,
        min_psf_fraction: 0.1,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::Strict,
        use_mask: Default::default(),
        auto_mask: Default::default(),
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only: true,
        write_preview_pngs: false,
    }
}

fn dataset_path(relative: &str) -> Option<PathBuf> {
    casatestdata_path(relative).filter(|path| path.exists())
}

fn stage_measurement_set(ms_path: &Path, temp_root: &Path, name: &str) -> Result<PathBuf, String> {
    let staged = temp_root.join(name);
    if staged.exists() {
        std::fs::remove_dir_all(&staged)
            .map_err(|error| format!("remove existing {}: {error}", staged.display()))?;
    }
    let copy_status = Command::new("cp")
        .arg("-R")
        .arg(ms_path)
        .arg(&staged)
        .status()
        .map_err(|error| format!("spawn cp -R for {}: {error}", ms_path.display()))?;
    if !copy_status.success() {
        return Err(format!(
            "cp -R {} {} failed with status {}",
            ms_path.display(),
            staged.display(),
            copy_status
        ));
    }
    Ok(staged)
}

fn fixvis_available(program: &Path) -> bool {
    Command::new(program)
        .arg("-c")
        .arg("import casatasks\nprint('1' if hasattr(casatasks, 'fixvis') else '0')\n")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .is_some_and(|stdout| stdout.trim() == "1")
}

fn run_fixvis_geometry_oracle(
    python: &Path,
    staged_ms_path: &Path,
    rust_trace: &casars_imager::PreparedGeometryTraceBundle,
    config: &CliConfig,
) -> Result<CasaGeometryOracle, String> {
    let output_path = staged_ms_path.with_extension("geometry.json");
    let shifted_ms_path = staged_ms_path.with_extension("fixvis.ms");
    let phasecenter = format!(
        "{} {}rad {}rad",
        rust_trace.phase_center.reference,
        rust_trace.phase_center.angles_rad[0],
        rust_trace.phase_center.angles_rad[1]
    );
    let field_selector = config
        .field_ids
        .as_ref()
        .map(|field_ids| {
            field_ids
                .iter()
                .map(|field_id| field_id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|| {
            rust_trace
                .phase_center
                .field_id
                .expect("geometry oracle requires a field-backed phase center")
                .to_string()
        });
    let script = r#"
import json
import os
import shutil
from casatasks import fixvis
from casatools import msmetadata, table

vis = os.environ["CASA_VIS"]
outputvis = os.environ["CASA_OUTPUTVIS"]
output_json = os.environ["CASA_OUTPUT_JSON"]
field_selector = os.environ["CASA_FIELD_SELECTOR"]
phasecenter_field_id = os.environ.get("CASA_PHASECENTER_FIELD_ID")
phasecenter = os.environ["CASA_PHASECENTER"]
explicit_ddid = os.environ.get("CASA_DDID")
explicit_spw = os.environ.get("CASA_SPW")

if os.path.exists(outputvis):
    shutil.rmtree(outputvis)
fixvis(
    vis=vis,
    outputvis=outputvis,
    field=field_selector,
    phasecenter=phasecenter,
    refcode="J2000",
    reuse=False,
    datacolumn="all",
)

tb = table()
tb.open(os.path.join(vis, "DATA_DESCRIPTION"))
ddid_to_spw = tb.getcol("SPECTRAL_WINDOW_ID").tolist()
ddid_to_pol = tb.getcol("POLARIZATION_ID").tolist()
tb.close()

tb.open(vis)
input_field = tb.getcol("FIELD_ID").tolist()
input_ddid = tb.getcol("DATA_DESC_ID").tolist()
input_time = tb.getcol("TIME").tolist() if "TIME" in tb.colnames() else None
input_uvw = tb.getcol("UVW")
input_ant1 = tb.getcol("ANTENNA1").tolist()
input_ant2 = tb.getcol("ANTENNA2").tolist()
input_pointing = tb.getcol("POINTING_ID").tolist() if "POINTING_ID" in tb.colnames() else None
tb.close()

tb.open(os.path.join(vis, "POINTING"))
pointing_ant = tb.getcol("ANTENNA_ID").tolist()
pointing_time = tb.getcol("TIME").tolist()
pointing_interval = tb.getcol("INTERVAL").tolist()
pointing_direction = tb.getcol("DIRECTION")
tb.close()

tb.open(outputvis)
output_uvw = tb.getcol("UVW")
tb.close()

md = msmetadata()
md.open(vis)

selected_fields = {int(value) for value in field_selector.split(",") if value}

pointing_rows_by_antenna = {}
for row_index, antenna_id in enumerate(pointing_ant):
    pointing_rows_by_antenna.setdefault(int(antenna_id), []).append(row_index)
for rows_for_antenna in pointing_rows_by_antenna.values():
    rows_for_antenna.sort(key=lambda row_index: float(pointing_time[row_index]))

field_phase_centers = {}
for field_id in selected_fields:
    phase_center = md.phasecenter(field_id)
    field_phase_centers[field_id] = [
        float(phase_center["m0"]["value"]),
        float(phase_center["m1"]["value"]),
    ]

def resolve_pointing_row(antenna_id, time_mjd_seconds):
    rows_for_antenna = pointing_rows_by_antenna.get(int(antenna_id), [])
    for row_index in rows_for_antenna:
        center = float(pointing_time[row_index])
        half_width = float(pointing_interval[row_index])
        if time_mjd_seconds >= center - half_width and time_mjd_seconds <= center + half_width:
            return row_index
    return None

def direction_from_pointing_row(row_index):
    return [
        float(pointing_direction[0][0][row_index]),
        float(pointing_direction[1][0][row_index]),
    ]

selected_rows = []
rows = []
for row_index, (field_id, ddid) in enumerate(zip(input_field, input_ddid)):
    if ddid < 0:
        continue
    if field_id not in selected_fields:
        continue
    if explicit_ddid is not None and ddid != int(explicit_ddid):
        continue
    spw_id = int(ddid_to_spw[ddid])
    if explicit_spw is not None and spw_id != int(explicit_spw):
        continue
    pol_id = int(ddid_to_pol[ddid])
    row_time_mjd_seconds = float(input_time[row_index]) if input_time is not None else None
    row_phase_center = field_phase_centers[int(field_id)]
    antenna1_pointing_row = None if row_time_mjd_seconds is None else resolve_pointing_row(input_ant1[row_index], row_time_mjd_seconds)
    antenna2_pointing_row = None if row_time_mjd_seconds is None else resolve_pointing_row(input_ant2[row_index], row_time_mjd_seconds)
    antenna1_pointing_used_fallback = antenna1_pointing_row is None
    antenna2_pointing_used_fallback = antenna2_pointing_row is None
    antenna1_pointing_direction = (
        row_phase_center
        if antenna1_pointing_used_fallback
        else direction_from_pointing_row(antenna1_pointing_row)
    )
    antenna2_pointing_direction = (
        row_phase_center
        if antenna2_pointing_used_fallback
        else direction_from_pointing_row(antenna2_pointing_row)
    )
    selected_rows.append({
        "row_index": row_index,
        "field_id": int(field_id),
        "ddid": int(ddid),
        "spw_id": spw_id,
        "polarization_id": pol_id,
        "time_mjd_seconds": row_time_mjd_seconds,
    })
    rows.append({
        "row_index": row_index,
        "input_field_id": int(field_id),
        "phase_center_field_id": None if phasecenter_field_id is None else int(phasecenter_field_id),
        "ddid": int(ddid),
        "spw_id": spw_id,
        "polarization_id": pol_id,
        "pointing_id": None if input_pointing is None else int(input_pointing[row_index]),
        "antenna1_pointing_row": antenna1_pointing_row,
        "antenna1_pointing_direction_rad": antenna1_pointing_direction,
        "antenna1_pointing_used_fallback": antenna1_pointing_used_fallback,
        "antenna2_pointing_row": antenna2_pointing_row,
        "antenna2_pointing_direction_rad": antenna2_pointing_direction,
        "antenna2_pointing_used_fallback": antenna2_pointing_used_fallback,
        "antenna1_id": int(input_ant1[row_index]),
        "antenna2_id": int(input_ant2[row_index]),
        "is_cross": int(input_ant1[row_index]) != int(input_ant2[row_index]),
        "raw_uvw_m": [float(input_uvw[0][row_index]), float(input_uvw[1][row_index]), float(input_uvw[2][row_index])],
        "imaging_uvw_m": [float(output_uvw[0][row_index]), float(output_uvw[1][row_index]), float(output_uvw[2][row_index])],
    })

md.close()

with open(output_json, "w", encoding="utf-8") as handle:
    json.dump({"selected_rows": selected_rows, "rows": rows}, handle, sort_keys=True)
"#;
    let mut command = Command::new(python);
    command.arg("-c").arg(script);
    command.env("CASA_VIS", staged_ms_path);
    command.env("CASA_OUTPUTVIS", &shifted_ms_path);
    command.env("CASA_OUTPUT_JSON", &output_path);
    command.env("CASA_FIELD_SELECTOR", field_selector);
    if let Some(phase_center_field_id) = rust_trace.phase_center.field_id {
        command.env(
            "CASA_PHASECENTER_FIELD_ID",
            phase_center_field_id.to_string(),
        );
    }
    command.env("CASA_PHASECENTER", phasecenter);
    if let Some(ddid) = config.ddid {
        command.env("CASA_DDID", ddid.to_string());
    }
    if let Some(spw) = config.spw {
        command.env("CASA_SPW", spw.to_string());
    }
    let output = command
        .output()
        .map_err(|error| format!("spawn CASA fixvis oracle: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "CASA fixvis oracle failed with status {}:\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let bytes = std::fs::read(&output_path).map_err(|error| {
        format!(
            "read CASA geometry oracle {}: {error}",
            output_path.display()
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "parse CASA geometry oracle {}: {error}",
            output_path.display()
        )
    })
}
