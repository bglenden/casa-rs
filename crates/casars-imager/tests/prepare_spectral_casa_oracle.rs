// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use casa_imaging::{Deconvolver, HogbomIterationMode, RestoringBeamMode, WTermMode, WeightingMode};
use casa_ms::{CubeAxisConfig, CubeAxisValue, CubeInterpolation, CubeSpecMode};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier, discover_casa_python};
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::FrequencyRef;
use casars_imager::{
    CliConfig, PreparedSampleRejectionReason, SelectedRowTrace, SpectralMode, WeightSourceKind,
    build_prepare_spectral_trace_from_config,
};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

fn casa_spectral_oracle_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Deserialize)]
struct CasaSpectralOracle {
    selected_rows: Vec<SelectedRowTrace>,
    source_channel_indices: Vec<usize>,
    source_channel_frequencies_hz: Vec<f64>,
    source_channel_widths_hz: Vec<f64>,
    output_channel_frequencies_hz: Vec<f64>,
    samples: Vec<CasaPreparedSampleTrace>,
    rejected_samples: Vec<CasaRejectedPreparedSampleTrace>,
}

#[derive(Debug, Deserialize)]
struct CasaPreparedSampleTrace {
    row_index: usize,
    input_field_id: usize,
    phase_center_field_id: usize,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    correlation_indices: Vec<usize>,
    output_channel_index: Option<usize>,
    output_frequency_hz: f64,
    visibility_re: f32,
    visibility_im: f32,
    weight: f32,
    weight_source: WeightSourceKind,
    sumwt_factor: f32,
    gridable: bool,
}

#[derive(Debug, Deserialize)]
struct CasaRejectedPreparedSampleTrace {
    row_index: usize,
    input_field_id: usize,
    phase_center_field_id: usize,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    correlation_indices: Vec<usize>,
    output_channel_index: Option<usize>,
    output_frequency_hz: f64,
    first_weight: f32,
    second_weight: f32,
    first_weight_source: WeightSourceKind,
    second_weight_source: WeightSourceKind,
    first_flagged: bool,
    second_flagged: bool,
    rejection_reason: PreparedSampleRejectionReason,
}

#[derive(Debug, Serialize)]
struct RustTraceSeed {
    phase_center_field_id: usize,
    selected_rows: Vec<SelectedRowTrace>,
    samples: Vec<RustSampleSeed>,
    rejected_samples: Vec<RustRejectedSampleSeed>,
}

#[derive(Debug, Serialize)]
struct RustChannelContributionSeed {
    source_channel_index: usize,
    factor: f32,
}

#[derive(Debug, Serialize)]
struct RustSampleSeed {
    row_index: usize,
    input_field_id: usize,
    phase_center_field_id: usize,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    correlation_indices: Vec<usize>,
    output_channel_index: Option<usize>,
    output_frequency_hz: f64,
    gridable: bool,
    source_contributions: Vec<RustChannelContributionSeed>,
}

#[derive(Debug, Serialize)]
struct RustRejectedSampleSeed {
    row_index: usize,
    input_field_id: usize,
    phase_center_field_id: usize,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    correlation_indices: Vec<usize>,
    output_channel_index: Option<usize>,
    output_frequency_hz: f64,
    source_contributions: Vec<RustChannelContributionSeed>,
}

#[test]
fn prepare_spectral_matches_casa_on_refim_point_withline_cube() {
    let Some(ms_path) = dataset_path("measurementset/vla/refim_point_withline.ms") else {
        eprintln!(
            "Wave 3 spectral parity skipped: missing measurementset/vla/refim_point_withline.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let mut config = base_config(ms_path, SpectralMode::Cube);
    config.channel_start = Some(0);
    config.channel_count = Some(20);
    config.cube_axis = CubeAxisConfig::default();
    assert_spectral_matches_casa_oracle(&config, "refim_point_withline.ms");
}

#[test]
fn prepare_spectral_matches_casa_on_refim_cband_g37line_cube() {
    let Some(ms_path) = dataset_path("measurementset/evla/refim_Cband.G37line.ms") else {
        eprintln!(
            "Wave 3 spectral parity skipped: missing measurementset/evla/refim_Cband.G37line.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let mut config = base_config(ms_path, SpectralMode::Cube);
    config.field_ids = Some(vec![1]);
    config.phasecenter_field = Some(1);
    config.channel_start = Some(105);
    config.channel_count = Some(30);
    config.cube_axis = CubeAxisConfig {
        specmode: CubeSpecMode::Cube,
        outframe: FrequencyRef::LSRK,
        veltype: DopplerRef::RADIO,
        interpolation: CubeInterpolation::Linear,
        rest_frequency_hz: Some(1.25e9),
        start: Some(CubeAxisValue::Channel(105)),
        width: Some(CubeAxisValue::Channel(1)),
    };
    assert_spectral_matches_casa_oracle(&config, "refim_Cband.G37line.ms");
}

#[test]
fn prepare_spectral_matches_casa_on_refim_cband_g37line_cubedata() {
    let Some(ms_path) = dataset_path("measurementset/evla/refim_Cband.G37line.ms") else {
        eprintln!(
            "Wave 3 spectral parity skipped: missing measurementset/evla/refim_Cband.G37line.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    };
    let mut config = base_config(ms_path, SpectralMode::Cubedata);
    config.field_ids = Some(vec![1]);
    config.phasecenter_field = Some(1);
    config.channel_start = Some(105);
    config.channel_count = Some(30);
    config.cube_axis = CubeAxisConfig {
        specmode: CubeSpecMode::Cubedata,
        outframe: FrequencyRef::LSRK,
        veltype: DopplerRef::RADIO,
        interpolation: CubeInterpolation::Linear,
        rest_frequency_hz: Some(1.25e9),
        start: Some(CubeAxisValue::Channel(105)),
        width: Some(CubeAxisValue::Channel(1)),
    };
    assert_spectral_matches_casa_oracle(&config, "refim_Cband.G37line.ms");
}

fn assert_spectral_matches_casa_oracle(config: &CliConfig, staged_name: &str) {
    let Some(casa) = discover_casa_python() else {
        eprintln!("Wave 3 spectral parity skipped: no CASA-capable Python interpreter found");
        return;
    };
    if !spectral_oracle_available(&casa.program) {
        eprintln!(
            "Wave 3 spectral parity skipped: CASA Python lacks required mstransform/casatools modules"
        );
        return;
    }
    let _lock = casa_spectral_oracle_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&config.ms, temp.path(), staged_name).expect("stage measurement set");
    let mut staged_config = config.clone();
    staged_config.ms = staged_ms_path.clone();
    let rust_trace = build_prepare_spectral_trace_from_config(&staged_config)
        .expect("build rust spectral trace");
    let casa_oracle =
        run_casa_spectral_oracle(&casa.program, &staged_ms_path, &rust_trace, &staged_config)
            .expect("run CASA spectral oracle");

    assert_eq!(rust_trace.selected_rows, casa_oracle.selected_rows);
    assert_eq!(
        rust_trace.source_channel_indices,
        casa_oracle.source_channel_indices
    );
    assert_close_slice(
        "source_channel_frequencies_hz",
        &rust_trace.source_channel_frequencies_hz,
        &casa_oracle.source_channel_frequencies_hz,
        1.0e-3,
    );
    assert_close_slice(
        "source_channel_widths_hz",
        &rust_trace.source_channel_widths_hz,
        &casa_oracle.source_channel_widths_hz,
        1.0e-3,
    );
    assert_close_slice(
        "output_channel_frequencies_hz",
        &rust_trace.output_channel_frequencies_hz,
        &casa_oracle.output_channel_frequencies_hz,
        0.1,
    );
    assert_eq!(rust_trace.samples.len(), casa_oracle.samples.len());
    for (index, (rust_sample, casa_sample)) in rust_trace
        .samples
        .iter()
        .zip(casa_oracle.samples.iter())
        .enumerate()
    {
        assert_eq!(
            rust_sample.row_index, casa_sample.row_index,
            "sample[{index}] row_index"
        );
        assert_eq!(
            rust_sample.input_field_id, casa_sample.input_field_id,
            "sample[{index}] input_field_id"
        );
        assert_eq!(
            rust_sample.phase_center_field_id,
            Some(casa_sample.phase_center_field_id),
            "sample[{index}] phase_center_field_id"
        );
        assert_eq!(rust_sample.ddid, casa_sample.ddid, "sample[{index}] ddid");
        assert_eq!(
            rust_sample.spw_id, casa_sample.spw_id,
            "sample[{index}] spw_id"
        );
        assert_eq!(
            rust_sample.polarization_id, casa_sample.polarization_id,
            "sample[{index}] polarization_id"
        );
        assert_eq!(
            rust_sample.antenna1_id, casa_sample.antenna1_id,
            "sample[{index}] antenna1_id"
        );
        assert_eq!(
            rust_sample.antenna2_id, casa_sample.antenna2_id,
            "sample[{index}] antenna2_id"
        );
        assert_eq!(
            rust_sample.is_cross, casa_sample.is_cross,
            "sample[{index}] is_cross"
        );
        assert_eq!(
            rust_sample.correlation_indices, casa_sample.correlation_indices,
            "sample[{index}] correlation_indices"
        );
        assert_eq!(
            rust_sample.output_channel_index, casa_sample.output_channel_index,
            "sample[{index}] output_channel_index"
        );
        assert_eq!(
            rust_sample.gridable, casa_sample.gridable,
            "sample[{index}] gridable"
        );
        assert_eq!(
            rust_sample.weight_source, casa_sample.weight_source,
            "sample[{index}] weight_source"
        );
        assert_close_f64(
            &format!("sample[{index}] output_frequency_hz"),
            rust_sample.output_frequency_hz,
            casa_sample.output_frequency_hz,
            1.0,
        );
        // Small residual spread remains from float-level interpolation
        // differences against CASA's `mstransform` output visibilities.
        assert_close_f32_rel(
            &format!("sample[{index}] visibility_re"),
            rust_sample.visibility_re,
            casa_sample.visibility_re,
            5.0e-4,
            5.0e-4,
        );
        assert_close_f32_rel(
            &format!("sample[{index}] visibility_im"),
            rust_sample.visibility_im,
            casa_sample.visibility_im,
            5.0e-4,
            5.0e-4,
        );
        assert_close_f32(
            &format!("sample[{index}] weight"),
            rust_sample.weight,
            casa_sample.weight,
            1.0e-4,
        );
        assert_close_f32(
            &format!("sample[{index}] sumwt_factor"),
            rust_sample.sumwt_factor,
            casa_sample.sumwt_factor,
            1.0e-6,
        );
    }
    assert_eq!(
        rust_trace.rejected_samples.len(),
        casa_oracle.rejected_samples.len()
    );
    for (index, (rust_sample, casa_sample)) in rust_trace
        .rejected_samples
        .iter()
        .zip(casa_oracle.rejected_samples.iter())
        .enumerate()
    {
        assert_eq!(
            rust_sample.row_index, casa_sample.row_index,
            "rejected[{index}] row_index"
        );
        assert_eq!(
            rust_sample.input_field_id, casa_sample.input_field_id,
            "rejected[{index}] input_field_id"
        );
        assert_eq!(
            rust_sample.phase_center_field_id,
            Some(casa_sample.phase_center_field_id),
            "rejected[{index}] phase_center_field_id"
        );
        assert_eq!(rust_sample.ddid, casa_sample.ddid, "rejected[{index}] ddid");
        assert_eq!(
            rust_sample.spw_id, casa_sample.spw_id,
            "rejected[{index}] spw_id"
        );
        assert_eq!(
            rust_sample.polarization_id, casa_sample.polarization_id,
            "rejected[{index}] polarization_id"
        );
        assert_eq!(
            rust_sample.antenna1_id, casa_sample.antenna1_id,
            "rejected[{index}] antenna1_id"
        );
        assert_eq!(
            rust_sample.antenna2_id, casa_sample.antenna2_id,
            "rejected[{index}] antenna2_id"
        );
        assert_eq!(
            rust_sample.is_cross, casa_sample.is_cross,
            "rejected[{index}] is_cross"
        );
        assert_eq!(
            rust_sample.correlation_indices, casa_sample.correlation_indices,
            "rejected[{index}] correlation_indices"
        );
        assert_eq!(
            rust_sample.output_channel_index, casa_sample.output_channel_index,
            "rejected[{index}] output_channel_index"
        );
        assert_eq!(
            rust_sample.rejection_reason, casa_sample.rejection_reason,
            "rejected[{index}] rejection_reason"
        );
        assert_eq!(
            rust_sample.first_flagged, casa_sample.first_flagged,
            "rejected[{index}] first_flagged"
        );
        assert_eq!(
            rust_sample.second_flagged, casa_sample.second_flagged,
            "rejected[{index}] second_flagged"
        );
        assert_eq!(
            rust_sample.first_weight_source, casa_sample.first_weight_source,
            "rejected[{index}] first_weight_source"
        );
        assert_eq!(
            rust_sample.second_weight_source, casa_sample.second_weight_source,
            "rejected[{index}] second_weight_source"
        );
        assert_close_f64(
            &format!("rejected[{index}] output_frequency_hz"),
            rust_sample.output_frequency_hz,
            casa_sample.output_frequency_hz,
            1.0,
        );
        assert_close_f32(
            &format!("rejected[{index}] first_weight"),
            rust_sample.first_weight,
            casa_sample.first_weight,
            1.0e-4,
        );
        assert_close_f32(
            &format!("rejected[{index}] second_weight"),
            rust_sample.second_weight,
            casa_sample.second_weight,
            1.0e-4,
        );
    }
}

fn assert_close_slice(label: &str, actual: &[f64], expected: &[f64], tolerance: f64) {
    assert_eq!(actual.len(), expected.len(), "{label} length mismatch");
    for (index, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            (*actual - *expected).abs() <= tolerance,
            "{label}[{index}] mismatch: actual={actual} expected={expected} tolerance={tolerance}"
        );
    }
}

fn assert_close_f64(label: &str, actual: f64, expected: f64, tolerance: f64) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{label} mismatch: actual={actual} expected={expected} tolerance={tolerance}"
    );
}

fn assert_close_f32(label: &str, actual: f32, expected: f32, tolerance: f32) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{label} mismatch: actual={actual} expected={expected} tolerance={tolerance}"
    );
}

fn assert_close_f32_rel(
    label: &str,
    actual: f32,
    expected: f32,
    absolute_tolerance: f32,
    relative_tolerance: f32,
) {
    let difference = (actual - expected).abs();
    let scale = actual.abs().max(expected.abs()).max(1.0);
    assert!(
        difference <= absolute_tolerance || difference / scale <= relative_tolerance,
        "{label} mismatch: actual={actual} expected={expected} abs_diff={difference} abs_tol={absolute_tolerance} rel_tol={relative_tolerance}"
    );
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
        spw: Some(0),
        spw_selector: None,
        channel_start: None,
        channel_count: None,
        datacolumn: None,
        save_model: casars_imager::SaveModelMode::None,
        start_model: None,
        outlier_file: None,
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
        nmajor: None,
        fullsummary: false,
        gain: 0.1,
        threshold_jy: 0.0,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        write_pb: false,
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
        force_standard_gridder: false,
        w_project_planes: None,
        dirty_only: true,
        standard_mfs_acceleration: Default::default(),
        standard_mfs_backend: None,
        standard_mfs_grid_threads: None,
        standard_mfs_tile_anchor: None,
        standard_mfs_residual_backend: None,
        standard_mfs_initial_dirty_backend: None,
        standard_mfs_metal_grouped_input_cache: None,
        standard_mfs_memory_target_mb: None,
        standard_mfs_prepare_buffer_mb: None,
        imaging_memory_target_mb: None,
        imaging_prepare_buffer_mb: None,
        imaging_row_block_rows: None,
        imaging_prepare_workers: None,
        write_preview_pngs: false,
    }
}

fn dataset_path(relative: &str) -> Option<PathBuf> {
    casatestdata_path_for_tier(CasaTestDataTier::SlowParity, relative).filter(|path| path.exists())
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

fn spectral_oracle_available(program: &Path) -> bool {
    Command::new(program)
        .arg("-c")
        .arg(
            "import casatasks\nfrom casatools import table\nprint('1' if hasattr(casatasks, 'mstransform') else '0')\n",
        )
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .is_some_and(|stdout| stdout.trim() == "1")
}

fn run_casa_spectral_oracle(
    python: &Path,
    staged_ms_path: &Path,
    rust_trace: &casars_imager::PreparedVisibilityTraceBundle,
    config: &CliConfig,
) -> Result<CasaSpectralOracle, String> {
    let output_path = staged_ms_path.with_extension("spectral.json");
    let trace_seed_path = staged_ms_path.with_extension("rust_trace_seed.json");
    let outputvis_path = staged_ms_path.with_extension("mstransform.ms");
    let trace_seed = RustTraceSeed {
        phase_center_field_id: rust_trace
            .phase_center
            .field_id
            .expect("spectral CASA oracle expects a field phase center"),
        selected_rows: rust_trace.selected_rows.clone(),
        samples: rust_trace
            .samples
            .iter()
            .map(|sample| RustSampleSeed {
                row_index: sample.row_index,
                input_field_id: sample.input_field_id,
                phase_center_field_id: sample
                    .phase_center_field_id
                    .expect("spectral CASA oracle expects sample phase center field"),
                ddid: sample.ddid,
                spw_id: sample.spw_id,
                polarization_id: sample.polarization_id,
                antenna1_id: sample.antenna1_id,
                antenna2_id: sample.antenna2_id,
                is_cross: sample.is_cross,
                correlation_indices: sample.correlation_indices.clone(),
                output_channel_index: sample.output_channel_index,
                output_frequency_hz: sample.output_frequency_hz,
                gridable: sample.gridable,
                source_contributions: sample
                    .source_contributions
                    .iter()
                    .map(|contribution| RustChannelContributionSeed {
                        source_channel_index: contribution.source_channel_index,
                        factor: contribution.factor,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>(),
        rejected_samples: rust_trace
            .rejected_samples
            .iter()
            .map(|sample| RustRejectedSampleSeed {
                row_index: sample.row_index,
                input_field_id: sample.input_field_id,
                phase_center_field_id: sample
                    .phase_center_field_id
                    .expect("spectral CASA oracle expects rejected sample phase center field"),
                ddid: sample.ddid,
                spw_id: sample.spw_id,
                polarization_id: sample.polarization_id,
                antenna1_id: sample.antenna1_id,
                antenna2_id: sample.antenna2_id,
                is_cross: sample.is_cross,
                correlation_indices: sample.correlation_indices.clone(),
                output_channel_index: sample.output_channel_index,
                output_frequency_hz: sample.output_frequency_hz,
                source_contributions: sample
                    .source_contributions
                    .iter()
                    .map(|contribution| RustChannelContributionSeed {
                        source_channel_index: contribution.source_channel_index,
                        factor: contribution.factor,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>(),
    };
    std::fs::write(
        &trace_seed_path,
        serde_json::to_vec(&trace_seed)
            .map_err(|error| format!("serialize rust trace seed: {error}"))?,
    )
    .map_err(|error| {
        format!(
            "write rust trace seed {}: {error}",
            trace_seed_path.display()
        )
    })?;

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
                .expect("spectral CASA oracle expects a field phase center")
                .to_string()
        });
    let script = r#"
import json
import os
import shutil
from collections import defaultdict
from casatasks import mstransform
from casatools import table

vis = os.environ["CASA_VIS"]
output_json = os.environ["CASA_OUTPUT_JSON"]
trace_seed_path = os.environ["CASA_TRACE_SEED_JSON"]
field_selector = os.environ["CASA_FIELD_SELECTOR"]
phase_center_field_id = int(os.environ["CASA_PHASECENTER_FIELD_ID"])
spectral_mode = os.environ["CASA_SPECTRAL_MODE"]
spw_id = int(os.environ["CASA_SPW"])
nchan = int(os.environ["CASA_NCHAN"])
start_channel = int(os.environ["CASA_START_CHANNEL"])
width_channel = int(os.environ["CASA_WIDTH_CHANNEL"])
outframe = os.environ["CASA_OUTFRAME"]
interpolation = os.environ["CASA_INTERPOLATION"]
veltype = os.environ["CASA_VELTYPE"]
restfreq = os.environ.get("CASA_RESTFREQ")
outputvis = os.environ.get("CASA_OUTPUTVIS")

with open(trace_seed_path, "r", encoding="utf-8") as handle:
    rust_trace = json.load(handle)

tb = table()
tb.open(os.path.join(vis, "DATA_DESCRIPTION"))
ddid_to_spw = tb.getcol("SPECTRAL_WINDOW_ID").tolist()
ddid_to_pol = tb.getcol("POLARIZATION_ID").tolist()
tb.close()

tb.open(os.path.join(vis, "SPECTRAL_WINDOW"))
all_chan_freqs = tb.getcell("CHAN_FREQ", spw_id).tolist()
all_chan_widths = tb.getcell("CHAN_WIDTH", spw_id).tolist()
tb.close()

tb.open(vis)
input_field = tb.getcol("FIELD_ID").tolist()
input_ddid = tb.getcol("DATA_DESC_ID").tolist()
input_time = tb.getcol("TIME").tolist() if "TIME" in tb.colnames() else None
input_data = [tb.getcell("DATA", row) for row in range(tb.nrows())]
input_flag = [tb.getcell("FLAG", row) for row in range(tb.nrows())]
input_weight = [tb.getcell("WEIGHT", row) for row in range(tb.nrows())]
def optional_array_column_cells(table_tool, column_name):
    if column_name not in table_tool.colnames():
        return None
    values = []
    for row in range(table_tool.nrows()):
        if hasattr(table_tool, "iscelldefined") and not table_tool.iscelldefined(column_name, row):
            values.append(None)
            continue
        try:
            values.append(table_tool.getcell(column_name, row))
        except RuntimeError:
            values.append(None)
    return values

input_weight_spectrum = optional_array_column_cells(tb, "WEIGHT_SPECTRUM")
input_ant1 = tb.getcol("ANTENNA1").tolist()
input_ant2 = tb.getcol("ANTENNA2").tolist()
tb.close()

selected_fields = {int(value) for value in field_selector.split(",") if value}
selected_rows = []
selected_row_indices = []
for row_index, (field_id, ddid) in enumerate(zip(input_field, input_ddid)):
    if ddid < 0:
        continue
    if field_id not in selected_fields:
        continue
    if int(ddid_to_spw[ddid]) != spw_id:
        continue
    selected_rows.append({
        "row_index": row_index,
        "field_id": int(field_id),
        "ddid": int(ddid),
        "spw_id": int(ddid_to_spw[ddid]),
        "polarization_id": int(ddid_to_pol[ddid]),
        "time_mjd_seconds": float(input_time[row_index]) if input_time is not None else None,
    })
    selected_row_indices.append(row_index)

def collect_source_channel_indices(trace):
    indices = set()
    for seed in trace["samples"]:
        for contribution in seed["source_contributions"]:
            indices.add(int(contribution["source_channel_index"]))
    for seed in trace["rejected_samples"]:
        for contribution in seed["source_contributions"]:
            indices.add(int(contribution["source_channel_index"]))
    return sorted(indices)

source_channel_indices = collect_source_channel_indices(rust_trace)
if not source_channel_indices:
    if spectral_mode == "cubedata":
        source_channel_indices = [
            start_channel + index * width_channel for index in range(nchan)
        ]
    else:
        source_channel_indices = list(range(len(all_chan_freqs)))
source_channel_frequencies_hz = [
    float(all_chan_freqs[channel_index]) for channel_index in source_channel_indices
]
source_channel_widths_hz = [
    float(all_chan_widths[channel_index]) for channel_index in source_channel_indices
]

def resolve_weight_with_source(weight_row, weight_spectrum_row, corr_index, channel_index):
    if weight_spectrum_row is not None and corr_index < weight_spectrum_row.shape[0] and channel_index < weight_spectrum_row.shape[1]:
        return float(weight_spectrum_row[corr_index, channel_index]), "weight_spectrum"
    return float(weight_row[corr_index]), "weight"

def combine_weight_source(first, second):
    return first if first == second else "mixed"

def row_identity_key(field_id, ddid, time_value, antenna1_id, antenna2_id):
    if time_value is None:
        time_key = None
    else:
        time_key = format(float(time_value), ".16e")
    return (
        int(field_id),
        int(ddid),
        time_key,
        int(antenna1_id),
        int(antenna2_id),
    )

def relaxed_row_identity_key(field_id, time_value, antenna1_id, antenna2_id):
    if time_value is None:
        time_key = None
    else:
        time_key = format(float(time_value), ".16e")
    return (
        int(field_id),
        time_key,
        int(antenna1_id),
        int(antenna2_id),
    )

def minimal_row_identity_key(time_value, antenna1_id, antenna2_id):
    if time_value is None:
        time_key = None
    else:
        time_key = format(float(time_value), ".16e")
    return (
        time_key,
        int(antenna1_id),
        int(antenna2_id),
    )

def single_status_from_seed(seed, flag_row, weight_row, weight_spectrum_row):
    flagged = False
    weight = 0.0
    weight_source = None
    sumwt_factor = 0.0
    corr = seed["correlation_indices"][0]
    for contribution in seed["source_contributions"]:
        factor = float(contribution["factor"])
        channel_index = int(contribution["source_channel_index"])
        flagged = flagged or bool(flag_row[corr, channel_index])
        source_weight, source_weight_source = resolve_weight_with_source(
            weight_row,
            weight_spectrum_row,
            corr,
            channel_index,
        )
        weight += source_weight * factor
        weight_source = source_weight_source if weight_source is None else combine_weight_source(weight_source, source_weight_source)
        sumwt_factor += factor
    return {
        "flagged": flagged,
        "weight": weight,
        "weight_source": weight_source or "weight",
        "sumwt_factor": sumwt_factor,
    }

def pair_status_from_seed(seed, source_flag_row, weight_row, weight_spectrum_row):
    first_corr, second_corr = seed["correlation_indices"]
    first_weight = 0.0
    second_weight = 0.0
    first_weight_source = None
    second_weight_source = None
    first_flagged = False
    second_flagged = False
    for contribution in seed["source_contributions"]:
        factor = float(contribution["factor"])
        channel_index = int(contribution["source_channel_index"])
        source_first_weight, source_first_weight_source = resolve_weight_with_source(
            weight_row,
            weight_spectrum_row,
            first_corr,
            channel_index,
        )
        source_second_weight, source_second_weight_source = resolve_weight_with_source(
            weight_row,
            weight_spectrum_row,
            second_corr,
            channel_index,
        )
        first_weight += source_first_weight * factor
        second_weight += source_second_weight * factor
        first_weight_source = source_first_weight_source if first_weight_source is None else combine_weight_source(first_weight_source, source_first_weight_source)
        second_weight_source = source_second_weight_source if second_weight_source is None else combine_weight_source(second_weight_source, source_second_weight_source)
        first_flagged = first_flagged or bool(source_flag_row[first_corr, channel_index])
        second_flagged = second_flagged or bool(source_flag_row[second_corr, channel_index])
    return {
        "first_weight": first_weight,
        "second_weight": second_weight,
        "first_weight_source": first_weight_source or "weight",
        "second_weight_source": second_weight_source or "weight",
        "first_flagged": first_flagged,
        "second_flagged": second_flagged,
    }

def build_single_sample(seed, data_row, visibility_channel_index, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz):
    corr = seed["correlation_indices"][0]
    status = single_status_from_seed(
        seed,
        source_flag_row,
        weight_row,
        weight_spectrum_row,
    )
    if bool(status["flagged"]):
        raise RuntimeError(f"unexpected flagged accepted single sample row={seed['row_index']} chan={visibility_channel_index}")
    visibility = data_row[corr, visibility_channel_index]
    return {
        "row_index": seed["row_index"],
        "input_field_id": seed["input_field_id"],
        "phase_center_field_id": seed["phase_center_field_id"],
        "ddid": seed["ddid"],
        "spw_id": seed["spw_id"],
        "polarization_id": seed["polarization_id"],
        "antenna1_id": seed["antenna1_id"],
        "antenna2_id": seed["antenna2_id"],
        "is_cross": seed["is_cross"],
        "correlation_indices": seed["correlation_indices"],
        "output_channel_index": seed["output_channel_index"],
        "output_frequency_hz": float(output_frequency_hz),
        "visibility_re": float(visibility.real),
        "visibility_im": float(visibility.imag),
        "weight": float(status["weight"]),
        "weight_source": status["weight_source"],
        "sumwt_factor": float(status["sumwt_factor"]),
        "gridable": bool(seed["gridable"]),
    }

def build_pair_sample(seed, data_row, visibility_channel_index, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz):
    status = pair_status_from_seed(seed, source_flag_row, weight_row, weight_spectrum_row)
    if status["first_flagged"] or status["second_flagged"]:
        return {
            "row_index": seed["row_index"],
            "input_field_id": seed["input_field_id"],
            "phase_center_field_id": seed["phase_center_field_id"],
            "ddid": seed["ddid"],
            "spw_id": seed["spw_id"],
            "polarization_id": seed["polarization_id"],
            "antenna1_id": seed["antenna1_id"],
            "antenna2_id": seed["antenna2_id"],
            "is_cross": seed["is_cross"],
            "correlation_indices": seed["correlation_indices"],
            "output_channel_index": seed["output_channel_index"],
            "output_frequency_hz": float(output_frequency_hz),
            "first_weight": float(status["first_weight"]),
            "second_weight": float(status["second_weight"]),
            "first_weight_source": status["first_weight_source"],
            "second_weight_source": status["second_weight_source"],
            "first_flagged": bool(status["first_flagged"]),
            "second_flagged": bool(status["second_flagged"]),
            "rejection_reason": "flagged_correlation",
        }
    if not (status["first_weight"] > 0.0 and status["second_weight"] > 0.0):
        return {
            "row_index": seed["row_index"],
            "input_field_id": seed["input_field_id"],
            "phase_center_field_id": seed["phase_center_field_id"],
            "ddid": seed["ddid"],
            "spw_id": seed["spw_id"],
            "polarization_id": seed["polarization_id"],
            "antenna1_id": seed["antenna1_id"],
            "antenna2_id": seed["antenna2_id"],
            "is_cross": seed["is_cross"],
            "correlation_indices": seed["correlation_indices"],
            "output_channel_index": seed["output_channel_index"],
            "output_frequency_hz": float(output_frequency_hz),
            "first_weight": float(status["first_weight"]),
            "second_weight": float(status["second_weight"]),
            "first_weight_source": status["first_weight_source"],
            "second_weight_source": status["second_weight_source"],
            "first_flagged": bool(status["first_flagged"]),
            "second_flagged": bool(status["second_flagged"]),
            "rejection_reason": "non_positive_weight",
        }
    first_corr, second_corr = seed["correlation_indices"]
    first_visibility = data_row[first_corr, visibility_channel_index]
    second_visibility = data_row[second_corr, visibility_channel_index]
    visibility = 0.5 * (first_visibility + second_visibility)
    return {
        "row_index": seed["row_index"],
        "input_field_id": seed["input_field_id"],
        "phase_center_field_id": seed["phase_center_field_id"],
        "ddid": seed["ddid"],
        "spw_id": seed["spw_id"],
        "polarization_id": seed["polarization_id"],
        "antenna1_id": seed["antenna1_id"],
        "antenna2_id": seed["antenna2_id"],
        "is_cross": seed["is_cross"],
        "correlation_indices": seed["correlation_indices"],
        "output_channel_index": seed["output_channel_index"],
        "output_frequency_hz": float(output_frequency_hz),
        "visibility_re": float(visibility.real),
        "visibility_im": float(visibility.imag),
        "weight": 0.5 * (float(status["first_weight"]) + float(status["second_weight"])),
        "weight_source": combine_weight_source(status["first_weight_source"], status["second_weight_source"]),
        "sumwt_factor": 2.0,
        "gridable": bool(seed["gridable"]),
    }

if spectral_mode == "cube":
    if outputvis is None:
        raise RuntimeError("missing CASA_OUTPUTVIS for cube spectral oracle")
    if os.path.exists(outputvis):
        shutil.rmtree(outputvis)
    transform_kwargs = dict(
        vis=vis,
        outputvis=outputvis,
        field=field_selector,
        phasecenter=int(phase_center_field_id),
        spw=str(spw_id),
        datacolumn="DATA",
        keepflags=True,
        usewtspectrum=True,
        regridms=True,
        mode="channel",
        nchan=nchan,
        start=start_channel,
        width=width_channel,
        interpolation=interpolation,
        outframe=outframe,
        veltype=veltype,
    )
    if restfreq is not None:
        transform_kwargs["restfreq"] = restfreq
    mstransform(**transform_kwargs)
    tb.open(os.path.join(outputvis, "SPECTRAL_WINDOW"))
    output_channel_frequencies_hz = [float(value) for value in tb.getcell("CHAN_FREQ", 0).tolist()]
    tb.close()
    tb.open(outputvis)
    output_field = tb.getcol("FIELD_ID").tolist()
    output_ddid = tb.getcol("DATA_DESC_ID").tolist()
    output_time = tb.getcol("TIME").tolist() if "TIME" in tb.colnames() else None
    output_ant1 = tb.getcol("ANTENNA1").tolist()
    output_ant2 = tb.getcol("ANTENNA2").tolist()
    output_data = [tb.getcell("DATA", row) for row in range(tb.nrows())]
    output_flag = [tb.getcell("FLAG", row) for row in range(tb.nrows())]
    output_weight = [tb.getcell("WEIGHT", row) for row in range(tb.nrows())]
    output_weight_spectrum = optional_array_column_cells(tb, "WEIGHT_SPECTRUM")
    tb.close()
    output_positions_by_key = defaultdict(list)
    output_positions_by_relaxed_key = defaultdict(list)
    output_positions_by_minimal_key = defaultdict(list)
    for position, field_id in enumerate(output_field):
        time_value = None if output_time is None else output_time[position]
        output_positions_by_key[
            row_identity_key(
                field_id,
                output_ddid[position],
                time_value,
                output_ant1[position],
                output_ant2[position],
            )
        ].append(position)
        output_positions_by_relaxed_key[
            relaxed_row_identity_key(
                field_id,
                time_value,
                output_ant1[position],
                output_ant2[position],
            )
        ].append(position)
        output_positions_by_minimal_key[
            minimal_row_identity_key(
                time_value,
                output_ant1[position],
                output_ant2[position],
            )
        ].append(position)
    row_position = {}
    used_output_positions = set()
    for selected_row in selected_rows:
        row_index = selected_row["row_index"]
        time_value = selected_row["time_mjd_seconds"]
        key = row_identity_key(
            selected_row["field_id"],
            selected_row["ddid"],
            time_value,
            input_ant1[row_index],
            input_ant2[row_index],
        )
        positions = output_positions_by_key.get(key)
        while positions and positions[0] in used_output_positions:
            positions.pop(0)
        if positions:
            position = positions.pop(0)
            used_output_positions.add(position)
            row_position[row_index] = position
            continue
        relaxed_key = relaxed_row_identity_key(
            selected_row["field_id"],
            time_value,
            input_ant1[row_index],
            input_ant2[row_index],
        )
        positions = output_positions_by_relaxed_key.get(relaxed_key)
        while positions and positions[0] in used_output_positions:
            positions.pop(0)
        if positions:
            position = positions.pop(0)
            used_output_positions.add(position)
            row_position[row_index] = position
            continue
        minimal_key = minimal_row_identity_key(
            time_value,
            input_ant1[row_index],
            input_ant2[row_index],
        )
        positions = output_positions_by_minimal_key.get(minimal_key)
        while positions and positions[0] in used_output_positions:
            positions.pop(0)
        if not positions:
            raise RuntimeError(f"missing output row for selected input row {row_index} and key {key}")
        position = positions.pop(0)
        used_output_positions.add(position)
        row_position[row_index] = position
    samples = []
    rejected_samples = []
    for seed in rust_trace["samples"]:
        channel_index = seed["output_channel_index"]
        output_frequency_hz = output_channel_frequencies_hz[channel_index]
        row_pos = row_position[seed["row_index"]]
        data_row = output_data[row_pos]
        source_flag_row = input_flag[seed["row_index"]]
        weight_row = input_weight[seed["row_index"]]
        weight_spectrum_row = None if input_weight_spectrum is None else input_weight_spectrum[seed["row_index"]]
        if len(seed["correlation_indices"]) == 1:
            samples.append(build_single_sample(seed, data_row, channel_index, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz))
        else:
            result = build_pair_sample(seed, data_row, channel_index, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz)
            if "rejection_reason" in result:
                rejected_samples.append(result)
            else:
                samples.append(result)
    for seed in rust_trace["rejected_samples"]:
        channel_index = seed["output_channel_index"]
        output_frequency_hz = output_channel_frequencies_hz[channel_index]
        row_pos = row_position[seed["row_index"]]
        data_row = output_data[row_pos]
        source_flag_row = input_flag[seed["row_index"]]
        weight_row = input_weight[seed["row_index"]]
        weight_spectrum_row = None if input_weight_spectrum is None else input_weight_spectrum[seed["row_index"]]
        rejected_samples.append(build_pair_sample(seed, data_row, channel_index, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz))
else:
    output_channel_frequencies_hz = [float(value) for value in source_channel_frequencies_hz]
    samples = []
    rejected_samples = []
    for seed in rust_trace["samples"]:
        source_channel = start_channel + seed["output_channel_index"] * width_channel
        output_frequency_hz = output_channel_frequencies_hz[seed["output_channel_index"]]
        data_row = input_data[seed["row_index"]]
        source_flag_row = input_flag[seed["row_index"]]
        weight_row = input_weight[seed["row_index"]]
        weight_spectrum_row = None if input_weight_spectrum is None else input_weight_spectrum[seed["row_index"]]
        if len(seed["correlation_indices"]) == 1:
            samples.append(build_single_sample(seed, data_row, source_channel, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz))
        else:
            result = build_pair_sample(seed, data_row, source_channel, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz)
            if "rejection_reason" in result:
                rejected_samples.append(result)
            else:
                samples.append(result)
    for seed in rust_trace["rejected_samples"]:
        source_channel = start_channel + seed["output_channel_index"] * width_channel
        output_frequency_hz = output_channel_frequencies_hz[seed["output_channel_index"]]
        data_row = input_data[seed["row_index"]]
        source_flag_row = input_flag[seed["row_index"]]
        weight_row = input_weight[seed["row_index"]]
        weight_spectrum_row = None if input_weight_spectrum is None else input_weight_spectrum[seed["row_index"]]
        rejected_samples.append(build_pair_sample(seed, data_row, source_channel, source_flag_row, weight_row, weight_spectrum_row, output_frequency_hz))

with open(output_json, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "selected_rows": selected_rows,
            "source_channel_indices": source_channel_indices,
            "source_channel_frequencies_hz": source_channel_frequencies_hz,
            "source_channel_widths_hz": source_channel_widths_hz,
            "output_channel_frequencies_hz": output_channel_frequencies_hz,
            "samples": samples,
            "rejected_samples": rejected_samples,
        },
        handle,
        sort_keys=True,
    )
"#;
    let mut command = Command::new(python);
    command.arg("-c").arg(script);
    command.env("CASA_VIS", staged_ms_path);
    command.env("CASA_OUTPUT_JSON", &output_path);
    command.env("CASA_TRACE_SEED_JSON", &trace_seed_path);
    command.env("CASA_FIELD_SELECTOR", field_selector);
    command.env(
        "CASA_PHASECENTER_FIELD_ID",
        rust_trace
            .phase_center
            .field_id
            .expect("spectral CASA oracle expects a field phase center")
            .to_string(),
    );
    command.env(
        "CASA_SPECTRAL_MODE",
        match config.spectral_mode {
            SpectralMode::Cube => "cube",
            SpectralMode::Cubedata => "cubedata",
            SpectralMode::Mfs => "mfs",
        },
    );
    if let Some(spw) = config.spw {
        command.env("CASA_SPW", spw.to_string());
    }
    command.env(
        "CASA_NCHAN",
        config
            .channel_count
            .ok_or_else(|| "Wave 3 oracle requires channel_count".to_string())?
            .to_string(),
    );
    let start_channel = match config.cube_axis.start.as_ref() {
        Some(CubeAxisValue::Channel(channel)) => *channel,
        None => config.channel_start.map(|value| value as i32).unwrap_or(0),
        Some(other) => {
            return Err(format!(
                "Wave 3 CASA spectral oracle currently supports channel-mode starts only, found {other:?}"
            ));
        }
    };
    let width_channel = match config.cube_axis.width.as_ref() {
        Some(CubeAxisValue::Channel(channel)) => *channel,
        None => 1,
        Some(other) => {
            return Err(format!(
                "Wave 3 CASA spectral oracle currently supports channel-mode widths only, found {other:?}"
            ));
        }
    };
    command.env("CASA_START_CHANNEL", start_channel.to_string());
    command.env("CASA_WIDTH_CHANNEL", width_channel.to_string());
    command.env("CASA_OUTFRAME", config.cube_axis.outframe.as_str());
    command.env(
        "CASA_INTERPOLATION",
        match config.cube_axis.interpolation {
            CubeInterpolation::Nearest => "nearest",
            CubeInterpolation::Linear => "linear",
            CubeInterpolation::Cubic => "cubic",
        },
    );
    command.env("CASA_VELTYPE", config.cube_axis.veltype.as_str());
    if let Some(rest_frequency_hz) = config.cube_axis.rest_frequency_hz {
        command.env("CASA_RESTFREQ", format!("{rest_frequency_hz}Hz"));
    }
    if matches!(config.spectral_mode, SpectralMode::Cube) {
        command.env("CASA_OUTPUTVIS", &outputvis_path);
    }
    let output = command
        .output()
        .map_err(|error| format!("spawn CASA spectral oracle: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "CASA spectral oracle failed with status {}:\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let bytes = std::fs::read(&output_path).map_err(|error| {
        format!(
            "read CASA spectral oracle {}: {error}",
            output_path.display()
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "parse CASA spectral oracle {}: {error}",
            output_path.display()
        )
    })
}
