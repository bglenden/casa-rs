// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use casa_images::PagedImage;
use casa_images::beam::{GaussianBeam, ImageBeamSet};
use casa_imaging::{
    CleanStopReason, Deconvolver, GaussianUvTaper, HogbomIterationMode, RestoringBeamMode,
    UvTaperSize, WTermMode, WeightingMode, estimate_psf_sidelobe_from_psf,
    fit_restoring_beam_from_psf,
};
use casa_test_support::{
    casa_source_root, casacore_source_root, casatestdata_path, discover_casa_python,
    git_head_commit, gridder_interop::cpp_convolve_gridder_make_dirty_image_2d,
    hogbom_interop::cpp_hogbom_clean_minor_cycle_2d,
};
use casa_types::measures::frequency::FrequencyRef;
use casars_imager::{
    CliConfig, RunSummary, build_prepare_plane_trace_from_config, run_from_config,
    trace_cube_channel_residual_refresh_from_config,
    trace_cube_channel_residual_refresh_from_config_with_model_cube,
    trace_cube_channel_residual_refresh_from_config_with_model_cube_model_channel_lambda,
};
use ndarray::{Array2, ArrayD, IxDyn};
use serde_json::Value;
use tempfile::tempdir;

fn casa_tclean_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Clone, Copy)]
struct ParityCase<'a> {
    dataset_rel: &'a str,
    field_ids: &'a [i32],
    phasecenter_field: Option<i32>,
    spw: i32,
    channel_start: usize,
    channel_count: usize,
    correlation: Option<&'a str>,
    weighting: WeightingMode,
    imsize: usize,
    cell_arcsec: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StagedFieldLayout {
    SingleField,
    SharedPhaseMultiField,
    DistinctPhaseMultiField { ra_offset_rad: f64 },
}

impl<'a> ParityCase<'a> {
    fn field_selector(self) -> String {
        if self.field_ids.is_empty() {
            return "*".to_string();
        }
        self.field_ids
            .iter()
            .map(|field_id| field_id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn default_phasecenter_field(self) -> i32 {
        self.phasecenter_field
            .or_else(|| self.field_ids.first().copied())
            .expect("parity cases require at least one field id")
    }

    fn robust(self) -> Option<f32> {
        match self.weighting {
            WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
                Some(robust)
            }
            _ => None,
        }
    }

    fn casa_weighting(self) -> &'static str {
        match self.weighting {
            WeightingMode::Natural => "natural",
            WeightingMode::Uniform => "uniform",
            WeightingMode::Briggs { .. } => "briggs",
            WeightingMode::BriggsBwTaper { .. } => "briggsbwtaper",
        }
    }
}

impl<'a> ParityCase<'a> {
    fn stokes(self) -> &'a str {
        self.correlation.unwrap_or("I")
    }

    fn cube_channel_spw_selector(self) -> String {
        if self.channel_count == 0 {
            self.spw.to_string()
        } else {
            let end = self.channel_start + self.channel_count - 1;
            format!("{}:{}~{}", self.spw, self.channel_start, end)
        }
    }

    fn channel_start_option(self) -> Option<usize> {
        (self.channel_count != 0).then_some(self.channel_start)
    }

    fn channel_count_option(self) -> Option<usize> {
        (self.channel_count != 0).then_some(self.channel_count)
    }

    fn center(self) -> usize {
        self.imsize / 2
    }
}

#[test]
fn dirty_products_track_casa_headers_and_pixels() {
    if !parity_available() {
        eprintln!("{}", skip_reason());
        return;
    }

    let ms_path = ngc5921_ms_path().expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "ngc5921.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-ngc5921");
    let casa_prefix = temp.path().join("casa-ngc5921");

    run_rust_imager(&staged_ms_path, &rust_prefix, true).expect("run rust imager");
    run_casa_tclean(&staged_ms_path, &casa_prefix, 0).expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    for (x, y) in [(64, 64), (64, 65), (63, 64), (60, 60)] {
        assert_close(
            sample(&rust_psf, x, y),
            sample(&casa_psf, x, y),
            0.12,
            0.2,
            &format!("psf[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y) in [(64, 64), (63, 64), (64, 63), (60, 60)] {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            0.15,
            0.35,
            &format!("residual[{x},{y}]"),
        );
    }

    let rust_sumwt = read_scalar_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_scalar_image(&casa_product(&casa_prefix, "sumwt"));
    assert!(
        rust_sumwt.is_finite() && rust_sumwt > 0.0,
        "expected positive Rust sumwt"
    );
    assert!(
        casa_sumwt.is_finite() && casa_sumwt > 0.0,
        "expected positive CASA sumwt"
    );
}

#[test]
fn multi_channel_dirty_products_track_casa_headers_and_pixels() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_twochan.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 2,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 64,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_twochan.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-twochan");
    let casa_prefix = temp.path().join("casa-refim-twochan");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
    assert_dirty_case_matches(
        case,
        &rust_prefix,
        &casa_prefix,
        0.15,
        0.35,
        0.08,
        0.1,
        true,
    );
}

#[test]
fn dirty_cube_products_track_casa_on_simulated_jet() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/sim_data_VLA_jet.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 5,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 12.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "sim_data_VLA_jet.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-simjet-cube-dirty");
    let casa_prefix = temp.path().join("casa-simjet-cube-dirty");

    run_rust_imager_cube_dirty(case, &staged_ms_path, &rust_prefix).expect("run rust imager");
    run_casa_tclean_cube_dirty_case(
        case,
        &staged_ms_path,
        &casa_prefix,
        CubeAxisStep::Text("1.0GHz"),
        CubeAxisStep::Text("0.2GHz"),
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    for chan in [0usize, 4usize] {
        for (x, y) in [(256, 256), (255, 256), (256, 255)] {
            assert_close(
                sample_channel(&rust_psf, x, y, chan),
                sample_channel(&casa_psf, x, y, chan),
                0.12,
                0.2,
                &format!("cube psf[{x},{y},chan={chan}]"),
            );
        }
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y, chan) in [
        (256, 209, 0usize),
        (275, 330, 0usize),
        (256, 209, 4usize),
        (275, 330, 4usize),
    ] {
        assert_close(
            sample_channel(&rust_residual, x, y, chan),
            sample_channel(&casa_residual, x, y, chan),
            0.15,
            0.35,
            &format!("cube residual[{x},{y},chan={chan}]"),
        );
    }

    let rust_sumwt = read_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_image(&casa_product(&casa_prefix, "sumwt"));
    for chan in [0usize, 4usize] {
        assert_close(
            sample_scalar_channel(&rust_sumwt, chan),
            sample_scalar_channel(&casa_sumwt, chan),
            1.0,
            0.05,
            &format!("cube sumwt[chan={chan}]"),
        );
    }
}

#[test]
fn parity_case_spw_selector_preserves_explicit_channel_ranges() {
    let case = ParityCase {
        dataset_rel: "unused.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 3,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 64,
        cell_arcsec: 1.0,
    };
    assert_eq!(case.cube_channel_spw_selector(), "3:0~0");

    let later = ParityCase {
        channel_start: 5,
        channel_count: 4,
        ..case
    };
    assert_eq!(later.cube_channel_spw_selector(), "3:5~8");
}

#[test]
fn dirty_cube_products_track_casa_on_refim_cband_g37line() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_Cband.G37line.ms",
        field_ids: &[1],
        phasecenter_field: Some(1),
        spw: 0,
        channel_start: 105,
        channel_count: 30,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 0.6,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_Cband.G37line.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-cband-cube-dirty");
    let casa_prefix = temp.path().join("casa-refim-cband-cube-dirty");

    run_rust_imager_cube_dirty(case, &staged_ms_path, &rust_prefix).expect("run rust imager");
    run_casa_tclean_cube_dirty_case(
        case,
        &staged_ms_path,
        &casa_prefix,
        CubeAxisStep::Channel(105),
        CubeAxisStep::Channel(1),
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let psf_points = [0usize, 18usize, 20usize]
        .into_iter()
        .flat_map(|chan| {
            [
                (128usize, 128usize),
                (127usize, 128usize),
                (128usize, 127usize),
            ]
            .into_iter()
            .map(move |(x, y)| [x, y, 0usize, chan])
        })
        .collect::<Vec<_>>();
    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = sample_image_points_in_casa(&casa_product(&casa_prefix, "psf"), &psf_points)
        .expect("sample CASA psf");
    for (index, [x, y, _, chan]) in psf_points.iter().enumerate() {
        assert_close(
            sample_channel(&rust_psf, *x, *y, *chan),
            casa_psf[index],
            0.1,
            0.1,
            &format!("g37line cube psf[{x},{y},chan={chan}]"),
        );
    }

    let residual_points = [
        (128usize, 128usize, 18usize),
        (128usize, 128usize, 20usize),
        (127usize, 128usize, 18usize),
        (128usize, 127usize, 20usize),
    ];
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = sample_image_points_in_casa(
        &casa_product(&casa_prefix, "residual"),
        &residual_points
            .into_iter()
            .map(|(x, y, chan)| [x, y, 0usize, chan])
            .collect::<Vec<_>>(),
    )
    .expect("sample CASA residual");
    for (index, (x, y, chan)) in residual_points.into_iter().enumerate() {
        assert_close(
            sample_channel(&rust_residual, x, y, chan),
            casa_residual[index],
            0.75,
            0.05,
            &format!("g37line cube residual[{x},{y},chan={chan}]"),
        );
    }

    let rust_sumwt = read_image(&rust_product(&rust_prefix, "sumwt"));
    let sumwt_channels = [0usize, 18usize, 20usize, 29usize];
    let casa_sumwt = sample_image_points_in_casa(
        &casa_product(&casa_prefix, "sumwt"),
        &sumwt_channels
            .into_iter()
            .map(|chan| [0usize, 0usize, 0usize, chan])
            .collect::<Vec<_>>(),
    )
    .expect("sample CASA sumwt");
    for (index, chan) in sumwt_channels.into_iter().enumerate() {
        assert_close(
            sample_scalar_channel(&rust_sumwt, chan),
            casa_sumwt[index],
            5.0,
            0.05,
            &format!("g37line cube sumwt[chan={chan}]"),
        );
    }
}

#[test]
fn dirty_cube_products_track_casa_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 5,
        channel_count: 3,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-point-withline-cube-dirty");
    let casa_prefix = temp.path().join("casa-refim-point-withline-cube-dirty");

    run_rust_imager_cube_dirty(case, &staged_ms_path, &rust_prefix).expect("run rust imager");
    run_casa_tclean_cube_dirty_case(
        case,
        &staged_ms_path,
        &casa_prefix,
        CubeAxisStep::Channel(case.channel_start as i32),
        CubeAxisStep::Channel(1),
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_sumwt = read_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_image(&casa_product(&casa_prefix, "sumwt"));
    let center = case.center();

    for chan in [0usize, 2usize] {
        assert_eq!(
            peak_location_in_plane(&extract_channel_plane(&rust_psf, chan)),
            Some((center, center)),
            "rust cube psf peak moved at chan={chan}"
        );
        assert_eq!(
            peak_location_in_plane(&extract_channel_plane(&casa_psf, chan)),
            Some((center, center)),
            "casa cube psf peak moved at chan={chan}"
        );
        assert_close(
            sample_channel(&rust_psf, center, center, chan),
            sample_channel(&casa_psf, center, center, chan),
            1.0e-4,
            1.0e-4,
            &format!("point_withline cube psf center[chan={chan}]"),
        );

        let rust_peak = peak_location_in_plane(&extract_channel_plane(&rust_residual, chan))
            .expect("rust peak");
        let casa_peak = peak_location_in_plane(&extract_channel_plane(&casa_residual, chan))
            .expect("casa peak");
        assert_eq!(
            rust_peak, casa_peak,
            "point_withline cube residual peak pixel changed at chan={chan}"
        );
        assert_close(
            sample_channel(&rust_residual, rust_peak.0, rust_peak.1, chan),
            sample_channel(&casa_residual, casa_peak.0, casa_peak.1, chan),
            5.0e-4,
            5.0e-4,
            &format!("point_withline cube residual peak[chan={chan}]"),
        );

        let stats = plane_difference_stats(
            &extract_channel_plane(&rust_residual, chan),
            &extract_channel_plane(&casa_residual, chan),
        );
        eprintln!(
            "point_withline source-backed dirty residual chan={chan}: rms={:.9e} max_abs={:.9e} corr={:.9e}",
            stats.rms, stats.max_abs, stats.correlation
        );
        assert!(
            stats.rms <= 5.0e-5,
            "point_withline cube residual RMS diff too large at chan={chan}: {stats:?}"
        );
        assert!(
            stats.max_abs <= 5.0e-4,
            "point_withline cube residual max diff too large at chan={chan}: {stats:?}"
        );
        assert!(
            stats.correlation >= 0.999_999,
            "point_withline cube residual correlation too small at chan={chan}: {stats:?}"
        );

        assert_close(
            sample_scalar_channel(&rust_sumwt, chan),
            sample_scalar_channel(&casa_sumwt, chan),
            0.1,
            1.0e-4,
            &format!("point_withline cube sumwt[chan={chan}]"),
        );
    }
}

#[test]
fn alma_mosaic_dirty_products_diagnostics() {
    let case = ParityCase {
        dataset_rel: "measurementset/alma/refim_alma_mosaic.ms",
        field_ids: &[0, 1, 2],
        phasecenter_field: Some(1),
        spw: 0,
        channel_start: 0,
        channel_count: 3,
        correlation: Some("XX"),
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 0.25,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_alma_mosaic.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-alma-mosaic-dirty");
    let casa_prefix = temp.path().join("casa-refim-alma-mosaic-dirty");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case_with_mosaic(case, &staged_ms_path, &casa_prefix, 0)
        .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_sumwt = read_scalar_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_scalar_image(&casa_product(&casa_prefix, "sumwt"));
    let psf_stats = image_difference_stats(&rust_psf, &casa_psf);
    let residual_stats = image_difference_stats(&rust_residual, &casa_residual);
    let rust_peak = peak_location_in_plane(&extract_channel_plane(&rust_residual, 0));
    let casa_peak = peak_location_in_plane(&extract_channel_plane(&casa_residual, 0));

    eprintln!(
        "alma mosaic dirty diagnostic: residual_rms={:.9e} residual_max_abs={:.9e} residual_corr={:.9e} \
         psf_rms={:.9e} psf_max_abs={:.9e} psf_corr={:.9e} rust_peak={rust_peak:?} casa_peak={casa_peak:?} \
         rust_sumwt={:.9e} casa_sumwt={:.9e}",
        residual_stats.rms,
        residual_stats.max_abs,
        residual_stats.correlation,
        psf_stats.rms,
        psf_stats.max_abs,
        psf_stats.correlation,
        rust_sumwt,
        casa_sumwt,
    );
    assert!(
        residual_stats.rms <= 1.2e-2,
        "alma residual RMS diff too large: {residual_stats:?}"
    );
    assert!(
        residual_stats.max_abs <= 3.5e-1,
        "alma residual max diff too large: {residual_stats:?}"
    );
    assert!(
        residual_stats.correlation >= 0.98,
        "alma residual correlation too small: {residual_stats:?}"
    );
    assert!(
        psf_stats.rms <= 5.0e-3,
        "alma PSF RMS diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.max_abs <= 1.0e-1,
        "alma PSF max diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.correlation >= 0.97,
        "alma PSF correlation too small: {psf_stats:?}"
    );
    assert_eq!(rust_peak, casa_peak, "alma mosaic residual peak shifted");
    assert_close(rust_sumwt, casa_sumwt, 200.0, 1.0e-3, "alma mosaic sumwt");
}

#[test]
fn papersky_mosaic_dirty_products_diagnostics() {
    let field_ids = (0..25).collect::<Vec<_>>();
    let case = ParityCase {
        dataset_rel: "measurementset/evla/papersky_mosaic.ms",
        field_ids: &field_ids,
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: Some("I"),
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "papersky_mosaic.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-papersky-mosaic-dirty");
    let casa_prefix = temp.path().join("casa-papersky-mosaic-dirty");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case_with_mosaic(case, &staged_ms_path, &casa_prefix, 0)
        .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_sumwt = read_scalar_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_scalar_image(&casa_product(&casa_prefix, "sumwt"));
    let psf_stats = image_difference_stats(&rust_psf, &casa_psf);
    let residual_stats = image_difference_stats(&rust_residual, &casa_residual);
    let rust_peak = peak_location_in_plane(&extract_channel_plane(&rust_residual, 0));
    let casa_peak = peak_location_in_plane(&extract_channel_plane(&casa_residual, 0));

    eprintln!(
        "papersky mosaic dirty diagnostic: residual_rms={:.9e} residual_max_abs={:.9e} residual_corr={:.9e} \
         psf_rms={:.9e} psf_max_abs={:.9e} psf_corr={:.9e} rust_peak={rust_peak:?} casa_peak={casa_peak:?} \
         rust_sumwt={:.9e} casa_sumwt={:.9e}",
        residual_stats.rms,
        residual_stats.max_abs,
        residual_stats.correlation,
        psf_stats.rms,
        psf_stats.max_abs,
        psf_stats.correlation,
        rust_sumwt,
        casa_sumwt,
    );
    assert!(
        residual_stats.rms <= 2.0e-3,
        "papersky residual RMS diff too large: {residual_stats:?}"
    );
    assert!(
        residual_stats.max_abs <= 1.0e-2,
        "papersky residual max diff too large: {residual_stats:?}"
    );
    assert!(
        residual_stats.correlation >= 0.94,
        "papersky residual correlation too small: {residual_stats:?}"
    );
    assert!(
        psf_stats.rms <= 3.0e-3,
        "papersky PSF RMS diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.max_abs <= 1.5e-2,
        "papersky PSF max diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.correlation >= 0.99,
        "papersky PSF correlation too small: {psf_stats:?}"
    );
    assert_close(rust_sumwt, casa_sumwt, 0.1, 1.0e-4, "papersky mosaic sumwt");
}

#[test]
fn oneshiftpoint_mosaic_dirty_products_diagnostics() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_oneshiftpoint.mosaic.ms",
        field_ids: &[0, 1],
        phasecenter_field: None,
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: Some("I"),
        weighting: WeightingMode::Natural,
        imsize: 1024,
        cell_arcsec: 10.0,
    };
    let explicit_phasecenter = "J2000 5.233697011339747rad 0.7097745013495772rad";
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_oneshiftpoint.mosaic.ms")
            .expect("stage ms");
    let rust_prefix = temp.path().join("rust-oneshiftpoint-mosaic-dirty");
    let casa_prefix = temp.path().join("casa-oneshiftpoint-mosaic-dirty");

    run_rust_imager_case_with_explicit_phasecenter_and_w_term_mode(
        case,
        &staged_ms_path,
        &rust_prefix,
        true,
        0,
        explicit_phasecenter,
        WTermMode::None,
        None,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_mosaic_and_explicit_phasecenter(
        case,
        &staged_ms_path,
        &casa_prefix,
        0,
        explicit_phasecenter,
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_sumwt = read_scalar_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_scalar_image(&casa_product(&casa_prefix, "sumwt"));
    let psf_stats = image_difference_stats(&rust_psf, &casa_psf);
    let residual_stats = image_difference_stats(&rust_residual, &casa_residual);
    let rust_peak = peak_location_in_plane(&extract_channel_plane(&rust_residual, 0));
    let casa_peak = peak_location_in_plane(&extract_channel_plane(&casa_residual, 0));

    eprintln!(
        "oneshiftpoint mosaic dirty diagnostic: residual_rms={:.9e} residual_max_abs={:.9e} residual_corr={:.9e} \
         psf_rms={:.9e} psf_max_abs={:.9e} psf_corr={:.9e} rust_peak={rust_peak:?} casa_peak={casa_peak:?} \
         rust_sumwt={:.9e} casa_sumwt={:.9e}",
        residual_stats.rms,
        residual_stats.max_abs,
        residual_stats.correlation,
        psf_stats.rms,
        psf_stats.max_abs,
        psf_stats.correlation,
        rust_sumwt,
        casa_sumwt,
    );
    assert!(
        residual_stats.rms <= 2.0e-3,
        "oneshiftpoint residual RMS diff too large: {residual_stats:?}"
    );
    assert!(
        residual_stats.max_abs <= 2.5e-1,
        "oneshiftpoint residual max diff too large: {residual_stats:?}"
    );
    assert!(
        residual_stats.correlation >= 0.93,
        "oneshiftpoint residual correlation too small: {residual_stats:?}"
    );
    assert!(
        psf_stats.rms <= 2.5e-3,
        "oneshiftpoint PSF RMS diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.max_abs <= 5.0e-2,
        "oneshiftpoint PSF max diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.correlation >= 0.9,
        "oneshiftpoint PSF correlation too small: {psf_stats:?}"
    );
    assert_eq!(rust_peak, casa_peak, "oneshiftpoint residual peak shifted");
    assert_close(
        rust_sumwt,
        casa_sumwt,
        0.1,
        1.0e-4,
        "oneshiftpoint mosaic sumwt",
    );
}

#[test]
fn wproject_dirty_cube_products_track_casa_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 6,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    assert_wproject_dirty_cube_products_track_casa_on_refim_point_withline(
        case,
        "singlefield",
        StagedFieldLayout::SingleField,
    );
}

#[test]
fn wproject_dirty_cube_products_track_casa_on_refim_point_withline_shared_phase_multifield() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0, 1],
        phasecenter_field: None,
        spw: 0,
        channel_start: 0,
        channel_count: 6,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    assert_wproject_dirty_cube_products_track_casa_on_refim_point_withline(
        case,
        "shared-phase-multifield",
        StagedFieldLayout::SharedPhaseMultiField,
    );
}

fn assert_wproject_dirty_cube_products_track_casa_on_refim_point_withline(
    case: ParityCase<'_>,
    label: &str,
    field_layout: StagedFieldLayout,
) {
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_name = format!("refim_point_withline-{label}.ms");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), &staged_name)
        .expect("stage refim_point_withline");
    match field_layout {
        StagedFieldLayout::SingleField => {}
        StagedFieldLayout::SharedPhaseMultiField => {
            promote_staged_measurement_set_to_multifield(&staged_ms_path, case.field_ids, None)
                .expect("promote staged cube MS to shared-phase multi-field");
        }
        StagedFieldLayout::DistinctPhaseMultiField { .. } => {
            panic!("cube helper does not support distinct-phase staging")
        }
    }
    let rust_prefix = temp
        .path()
        .join(format!("rust-refim-point-withline-cube-wproject-{label}"));
    let casa_prefix = temp
        .path()
        .join(format!("casa-refim-point-withline-cube-wproject-{label}"));

    run_rust_imager_cube_dirty_with_w_term_mode(
        case,
        &staged_ms_path,
        &rust_prefix,
        casars_imager::SpectralMode::Cube,
        WTermMode::WProject,
        Some(8),
    )
    .expect("run rust wproject cube");
    run_casa_tclean_cube_dirty_case_with_wproject(
        case,
        &staged_ms_path,
        &casa_prefix,
        CubeAxisStep::Channel(0),
        CubeAxisStep::Channel(1),
        8,
    )
    .expect("run casa wproject cube");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "sumwt"),
        &casa_product(&casa_prefix, "sumwt"),
        "",
        false,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_sumwt = read_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_image(&casa_product(&casa_prefix, "sumwt"));
    let center = case.center();

    for chan in [0usize, case.channel_count - 1] {
        let rust_peak = peak_location_in_plane(&extract_channel_plane(&rust_residual, chan))
            .expect("rust residual peak");
        let casa_peak = peak_location_in_plane(&extract_channel_plane(&casa_residual, chan))
            .expect("casa residual peak");
        assert_eq!(
            rust_peak, casa_peak,
            "wproject cube peak pixel changed at chan={chan}"
        );
        assert_eq!(
            peak_location_in_plane(&extract_channel_plane(&rust_psf, chan)),
            Some((center, center)),
            "rust cube psf peak moved at chan={chan}"
        );
        assert_eq!(
            peak_location_in_plane(&extract_channel_plane(&casa_psf, chan)),
            Some((center, center)),
            "casa cube psf peak moved at chan={chan}"
        );
        assert_close(
            sample_channel(&rust_residual, rust_peak.0, rust_peak.1, chan),
            sample_channel(&casa_residual, casa_peak.0, casa_peak.1, chan),
            0.08,
            0.12,
            &format!("wproject cube residual peak[chan={chan}]"),
        );
        assert_close(
            sample_channel(&rust_psf, center, center, chan),
            sample_channel(&casa_psf, center, center, chan),
            1.0e-3,
            1.0e-3,
            &format!("wproject cube psf center[chan={chan}]"),
        );
        let stats = plane_difference_stats(
            &extract_channel_plane(&rust_residual, chan),
            &extract_channel_plane(&casa_residual, chan),
        );
        assert!(
            stats.rms <= 0.01,
            "wproject cube residual RMS diff too large at chan={chan}: {stats:?}"
        );
        assert!(
            stats.max_abs <= 0.2,
            "wproject cube residual max diff too large at chan={chan}: {stats:?}"
        );
        assert_close(
            sample_scalar_channel(&rust_sumwt, chan),
            sample_scalar_channel(&casa_sumwt, chan),
            1.0,
            0.05,
            &format!("wproject cube sumwt[chan={chan}]"),
        );
    }
}

#[test]
fn dirty_cubedata_products_track_casa_on_refim_cband_g37line() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_Cband.G37line.ms",
        field_ids: &[1],
        phasecenter_field: Some(1),
        spw: 0,
        channel_start: 105,
        channel_count: 30,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 0.6,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_Cband.G37line.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-cband-cubedata-dirty");
    let casa_prefix = temp.path().join("casa-refim-cband-cubedata-dirty");

    run_rust_imager_spectral_cube_case_with_options_and_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        casars_imager::SpectralMode::Cubedata,
        CubeCaseOptions {
            spw_selector: "0:105~135",
            nchan: 30,
            start: Some(CubeAxisStep::Channel(105)),
            width: Some(CubeAxisStep::Channel(1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        true,
        0,
        CubeWeightingOptions::default(),
    )
    .expect("run rust imager");
    run_casa_tclean_cubedata_case_with_options(
        case,
        &staged_ms_path,
        &casa_prefix,
        0,
        "hogbom",
        &[],
        0.0,
        false,
        CubeCaseOptions {
            spw_selector: "0:105~135",
            nchan: 30,
            start: Some(CubeAxisStep::Channel(105)),
            width: Some(CubeAxisStep::Channel(1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        "0Jy",
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );
    let (rust_frame, _) = spectral_header_summary(&rust_product(&rust_prefix, "image"));
    let (casa_frame, _) = spectral_header_summary(&casa_product(&casa_prefix, "image"));
    assert_eq!(rust_frame, casa_frame, "cubedata spectral frame");

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = sample_image_points_in_casa(
        &casa_product(&casa_prefix, "image"),
        &[[128usize, 128usize, 0usize, 18usize]],
    )
    .expect("sample CASA image");
    let rust_peak = sample_channel(&rust_image, 128, 128, 18);
    let casa_peak = casa_image[0];
    assert_close(
        rust_peak,
        86.254,
        1.5,
        0.02,
        "Rust cubedata image[128,128,18]",
    );
    assert_close(
        casa_peak,
        86.254,
        1.5,
        0.02,
        "CASA cubedata image[128,128,18]",
    );
    assert_close(
        rust_peak,
        casa_peak,
        1.0,
        0.02,
        "Rust/CASA cubedata image[128,128,18]",
    );
}

#[test]
fn dirty_cubedata_briggs_products_track_casa_on_refim_cband_g37line() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_Cband.G37line.ms",
        field_ids: &[1],
        phasecenter_field: Some(1),
        spw: 0,
        channel_start: 105,
        channel_count: 30,
        correlation: None,
        weighting: WeightingMode::Briggs { robust: 0.5 },
        imsize: 256,
        cell_arcsec: 0.6,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_Cband.G37line.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-cband-cubedata-briggs-dirty");
    let casa_prefix = temp.path().join("casa-refim-cband-cubedata-briggs-dirty");

    run_rust_imager_spectral_cube_case_with_options_and_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        casars_imager::SpectralMode::Cubedata,
        CubeCaseOptions {
            spw_selector: "0:105~135",
            nchan: 30,
            start: Some(CubeAxisStep::Channel(105)),
            width: Some(CubeAxisStep::Channel(1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        true,
        0,
        CubeWeightingOptions {
            per_channel_weight_density: true,
            ..CubeWeightingOptions::default()
        },
    )
    .expect("run rust imager");
    run_casa_tclean_cubedata_case_with_options(
        case,
        &staged_ms_path,
        &casa_prefix,
        0,
        "hogbom",
        &[],
        0.0,
        true,
        CubeCaseOptions {
            spw_selector: "0:105~135",
            nchan: 30,
            start: Some(CubeAxisStep::Channel(105)),
            width: Some(CubeAxisStep::Channel(1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        "0Jy",
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );
    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = sample_image_points_in_casa(
        &casa_product(&casa_prefix, "image"),
        &[[128usize, 128usize, 0usize, 18usize]],
    )
    .expect("sample CASA image");
    let rust_peak = sample_channel(&rust_image, 128, 128, 18);
    let casa_peak = casa_image[0];
    assert_close(
        rust_peak,
        92.63,
        1.5,
        0.02,
        "Rust cubedata briggs image[128,128,18]",
    );
    assert_close(
        casa_peak,
        92.63,
        1.5,
        0.02,
        "CASA cubedata briggs image[128,128,18]",
    );
    assert_close(
        rust_peak,
        casa_peak,
        1.0,
        0.02,
        "Rust/CASA cubedata briggs image[128,128,18]",
    );
}

#[test]
fn cube_perchanweight_briggs_tracks_casa_on_refim_point_withline() {
    let natural_case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(natural_case) {
        eprintln!("{}", skip_reason_for_case(natural_case));
        return;
    }

    let briggs0_case = ParityCase {
        weighting: WeightingMode::Briggs { robust: 0.0 },
        ..natural_case
    };
    let briggs_2_case = ParityCase {
        weighting: WeightingMode::Briggs { robust: -2.0 },
        ..natural_case
    };
    let ms_path = dataset_path(natural_case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let natural_rust_prefix = temp.path().join("rust-refim-point-withline-cube-natural");
    let natural_casa_prefix = temp.path().join("casa-refim-point-withline-cube-natural");
    run_rust_imager_cube_task_default_case_with_weighting(
        natural_case,
        &staged_ms_path,
        &natural_rust_prefix,
        false,
        1,
        CubeWeightingOptions::default(),
    )
    .expect("run rust natural cube");
    run_casa_tclean_cube_task_default_case_with_weighting(
        natural_case,
        &staged_ms_path,
        &natural_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        "0Jy",
    )
    .expect("run casa natural cube");

    let briggs0_rust_prefix = temp.path().join("rust-refim-point-withline-cube-briggs0");
    let briggs0_casa_prefix = temp.path().join("casa-refim-point-withline-cube-briggs0");
    let per_plane = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::PerPlane,
    };
    run_rust_imager_cube_task_default_case_with_weighting(
        briggs0_case,
        &staged_ms_path,
        &briggs0_rust_prefix,
        false,
        1,
        per_plane.clone(),
    )
    .expect("run rust briggs0 cube");
    run_casa_tclean_cube_task_default_case_with_weighting(
        briggs0_case,
        &staged_ms_path,
        &briggs0_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        per_plane.clone(),
        "0Jy",
    )
    .expect("run casa briggs0 cube");

    let briggs_2_rust_prefix = temp.path().join("rust-refim-point-withline-cube-briggs-2");
    let briggs_2_casa_prefix = temp.path().join("casa-refim-point-withline-cube-briggs-2");
    run_rust_imager_cube_task_default_case_with_weighting(
        briggs_2_case,
        &staged_ms_path,
        &briggs_2_rust_prefix,
        false,
        1,
        per_plane.clone(),
    )
    .expect("run rust briggs-2 cube");
    run_casa_tclean_cube_task_default_case_with_weighting(
        briggs_2_case,
        &staged_ms_path,
        &briggs_2_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        per_plane.clone(),
        "0Jy",
    )
    .expect("run casa briggs-2 cube");

    let briggs0_taper_rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-briggs0-taper");
    let briggs0_taper_casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-briggs0-taper");
    let taper = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &["50arcsec"],
        restoring_beam_mode: RestoringBeamMode::PerPlane,
    };
    run_rust_imager_cube_task_default_case_with_weighting(
        briggs0_case,
        &staged_ms_path,
        &briggs0_taper_rust_prefix,
        false,
        1,
        taper.clone(),
    )
    .expect("run rust briggs0+taper cube");
    run_casa_tclean_cube_task_default_case_with_weighting(
        briggs0_case,
        &staged_ms_path,
        &briggs0_taper_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        taper.clone(),
        "0Jy",
    )
    .expect("run casa briggs0+taper cube");

    let rust_nat = image_beam_areas_arcsec2(&rust_product(&natural_rust_prefix, "image"));
    let rust_briggs0 = image_beam_areas_arcsec2(&rust_product(&briggs0_rust_prefix, "image"));
    let rust_briggs_2 = image_beam_areas_arcsec2(&rust_product(&briggs_2_rust_prefix, "image"));
    let rust_briggs0_taper =
        image_beam_areas_arcsec2(&rust_product(&briggs0_taper_rust_prefix, "image"));

    let casa_nat = image_beam_areas_arcsec2(&casa_product(&natural_casa_prefix, "image"));
    let casa_briggs0 = image_beam_areas_arcsec2(&casa_product(&briggs0_casa_prefix, "image"));
    let casa_briggs_2 = image_beam_areas_arcsec2(&casa_product(&briggs_2_casa_prefix, "image"));
    let casa_briggs0_taper =
        image_beam_areas_arcsec2(&casa_product(&briggs0_taper_casa_prefix, "image"));

    assert_beam_area_relation(
        &rust_briggs0,
        &rust_nat,
        |lhs, rhs| lhs < rhs,
        "Rust briggs0 < natural",
    );
    assert_beam_area_relation(
        &rust_briggs_2,
        &rust_briggs0,
        |lhs, rhs| lhs < rhs,
        "Rust briggs-2 < briggs0",
    );
    assert_beam_area_relation(
        &rust_briggs0,
        &rust_briggs0_taper,
        |lhs, rhs| lhs < rhs,
        "Rust briggs0 < briggs0+taper",
    );

    assert_beam_area_relation(
        &casa_briggs0,
        &casa_nat,
        |lhs, rhs| lhs < rhs,
        "CASA briggs0 < natural",
    );
    assert_beam_area_relation(
        &casa_briggs_2,
        &casa_briggs0,
        |lhs, rhs| lhs < rhs,
        "CASA briggs-2 < briggs0",
    );
    assert_beam_area_relation(
        &casa_briggs0,
        &casa_briggs0_taper,
        |lhs, rhs| lhs < rhs,
        "CASA briggs0 < briggs0+taper",
    );
}

#[test]
fn cube_weighting_taper_common_beam_tracks_casa_on_refim_point_withline() {
    let uniform_case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(uniform_case) {
        eprintln!("{}", skip_reason_for_case(uniform_case));
        return;
    }
    let natural_case = ParityCase {
        weighting: WeightingMode::Natural,
        ..uniform_case
    };
    let briggs_2_case = ParityCase {
        weighting: WeightingMode::Briggs { robust: -2.0 },
        ..uniform_case
    };
    let ms_path = dataset_path(uniform_case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let common = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let common_50_arcsec = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &["50arcsec"],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let common_500_arcsec = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &["500arcsec"],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let briggs_common_50_arcsec = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &["50arcsec"],
        restoring_beam_mode: RestoringBeamMode::Common,
    };

    let uniform_rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-uniform-common");
    let uniform_casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-uniform-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        uniform_case,
        &staged_ms_path,
        &uniform_rust_prefix,
        false,
        1,
        common.clone(),
    )
    .expect("run rust uniform common beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        uniform_case,
        &staged_ms_path,
        &uniform_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        common.clone(),
        "0Jy",
    )
    .expect("run casa uniform common beam");

    let uniform_taper_rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-uniform-taper-common");
    let uniform_taper_casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-uniform-taper-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        uniform_case,
        &staged_ms_path,
        &uniform_taper_rust_prefix,
        false,
        1,
        common_50_arcsec.clone(),
    )
    .expect("run rust uniform taper common beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        uniform_case,
        &staged_ms_path,
        &uniform_taper_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        common_50_arcsec.clone(),
        "0Jy",
    )
    .expect("run casa uniform taper common beam");

    let natural_taper_rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-natural-taper-common");
    let natural_taper_casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-natural-taper-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        natural_case,
        &staged_ms_path,
        &natural_taper_rust_prefix,
        false,
        1,
        common_500_arcsec.clone(),
    )
    .expect("run rust natural taper common beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        natural_case,
        &staged_ms_path,
        &natural_taper_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        common_500_arcsec.clone(),
        "0Jy",
    )
    .expect("run casa natural taper common beam");

    let briggs_taper_rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-briggs-taper-common");
    let briggs_taper_casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-briggs-taper-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        briggs_2_case,
        &staged_ms_path,
        &briggs_taper_rust_prefix,
        false,
        1,
        briggs_common_50_arcsec.clone(),
    )
    .expect("run rust briggs taper common beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        briggs_2_case,
        &staged_ms_path,
        &briggs_taper_casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        briggs_common_50_arcsec.clone(),
        "0Jy",
    )
    .expect("run casa briggs taper common beam");

    let rust_uniform = single_beam_summary(&rust_product(&uniform_rust_prefix, "image"));
    let casa_uniform = single_beam_summary(&casa_product(&uniform_casa_prefix, "image"));
    let rust_uniform_taper =
        single_beam_summary(&rust_product(&uniform_taper_rust_prefix, "image"));
    let casa_uniform_taper =
        single_beam_summary(&casa_product(&uniform_taper_casa_prefix, "image"));
    let rust_natural_taper =
        single_beam_summary(&rust_product(&natural_taper_rust_prefix, "image"));
    let casa_natural_taper =
        single_beam_summary(&casa_product(&natural_taper_casa_prefix, "image"));
    let rust_briggs_taper = single_beam_summary(&rust_product(&briggs_taper_rust_prefix, "image"));
    let casa_briggs_taper = single_beam_summary(&casa_product(&briggs_taper_casa_prefix, "image"));

    assert_close(
        rust_uniform.0 as f32,
        70.00,
        3.0,
        0.05,
        "Rust common uniform major",
    );
    assert_close(
        rust_uniform.1 as f32,
        51.07,
        3.0,
        0.05,
        "Rust common uniform minor",
    );
    assert_close(
        casa_uniform.0 as f32,
        70.00,
        3.0,
        0.05,
        "CASA common uniform major",
    );
    assert_close(
        casa_uniform.1 as f32,
        51.07,
        3.0,
        0.05,
        "CASA common uniform minor",
    );

    assert_close(
        rust_uniform_taper.0 as f32,
        76.31,
        3.0,
        0.05,
        "Rust common uniform taper major",
    );
    assert_close(
        rust_uniform_taper.1 as f32,
        63.06,
        3.0,
        0.05,
        "Rust common uniform taper minor",
    );
    assert_close(
        casa_uniform_taper.0 as f32,
        76.31,
        3.0,
        0.05,
        "CASA common uniform taper major",
    );
    assert_close(
        casa_uniform_taper.1 as f32,
        63.06,
        3.0,
        0.05,
        "CASA common uniform taper minor",
    );

    assert_close(
        rust_natural_taper.0 as f32,
        488.31,
        10.0,
        0.05,
        "Rust common natural taper major",
    );
    assert_close(
        rust_natural_taper.1 as f32,
        469.05,
        10.0,
        0.05,
        "Rust common natural taper minor",
    );
    assert_close(
        casa_natural_taper.0 as f32,
        488.31,
        10.0,
        0.05,
        "CASA common natural taper major",
    );
    assert_close(
        casa_natural_taper.1 as f32,
        469.05,
        10.0,
        0.05,
        "CASA common natural taper minor",
    );

    assert_close(
        rust_briggs_taper.0 as f32,
        76.31,
        3.0,
        0.05,
        "Rust common briggs taper major",
    );
    assert_close(
        rust_briggs_taper.1 as f32,
        63.06,
        3.0,
        0.05,
        "Rust common briggs taper minor",
    );
    assert_close(
        casa_briggs_taper.0 as f32,
        76.31,
        3.0,
        0.05,
        "CASA common briggs taper major",
    );
    assert_close(
        casa_briggs_taper.1 as f32,
        63.06,
        3.0,
        0.05,
        "CASA common briggs taper minor",
    );
}

#[test]
fn cube_badchannel_restoringbeam_tracks_casa_on_refim_point() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let rust_perplane_prefix = temp.path().join("rust-refim-point-badchannel-perplane");
    let casa_perplane_prefix = temp.path().join("casa-refim-point-badchannel-perplane");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_perplane_prefix,
        false,
        10,
        CubeWeightingOptions::default(),
    )
    .expect("run rust badchannel per-plane");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_perplane_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        "0Jy",
    )
    .expect("run casa badchannel per-plane");

    let common = CubeWeightingOptions {
        per_channel_weight_density: false,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_common_prefix = temp.path().join("rust-refim-point-badchannel-common");
    let casa_common_prefix = temp.path().join("casa-refim-point-badchannel-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_common_prefix,
        false,
        10,
        common.clone(),
    )
    .expect("run rust badchannel common");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_common_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa badchannel common");

    compare_image_headers(
        &rust_product(&rust_perplane_prefix, "image"),
        &casa_product(&casa_perplane_prefix, "image"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_common_prefix, "image"),
        &casa_product(&casa_common_prefix, "image"),
        "Jy/beam",
        true,
    );

    let rust_perplane_image = read_image(&rust_product(&rust_perplane_prefix, "image"));
    let casa_perplane_image = read_image(&casa_product(&casa_perplane_prefix, "image"));
    let rust_perplane_residual = read_image(&rust_product(&rust_perplane_prefix, "residual"));
    let casa_perplane_residual = read_image(&casa_product(&casa_perplane_prefix, "residual"));
    let rust_common_image = read_image(&rust_product(&rust_common_prefix, "image"));
    let casa_common_image = read_image(&casa_product(&casa_common_prefix, "image"));
    let rust_common_residual = read_image(&rust_product(&rust_common_prefix, "residual"));
    let casa_common_residual = read_image(&casa_product(&casa_common_prefix, "residual"));

    assert_close(
        sample_channel(&rust_perplane_image, 54, 50, 0),
        0.889,
        0.03,
        0.05,
        "Rust badchannel per-plane image[54,50,0,0]",
    );
    assert_close(
        sample_channel(&casa_perplane_image, 54, 50, 0),
        0.889,
        0.03,
        0.05,
        "CASA badchannel per-plane image[54,50,0,0]",
    );
    assert_close(
        sample_channel(&rust_perplane_image, 54, 50, 19),
        0.0602,
        0.02,
        0.1,
        "Rust badchannel per-plane image[54,50,0,19]",
    );
    assert_close(
        sample_channel(&casa_perplane_image, 54, 50, 19),
        0.0602,
        0.02,
        0.1,
        "CASA badchannel per-plane image[54,50,0,19]",
    );
    assert_close(
        sample_channel(&rust_perplane_residual, 54, 50, 19),
        0.033_942,
        0.01,
        0.1,
        "Rust badchannel per-plane residual[54,50,0,19]",
    );
    assert_close(
        sample_channel(&casa_perplane_residual, 54, 50, 19),
        0.033_942,
        0.01,
        0.1,
        "CASA badchannel per-plane residual[54,50,0,19]",
    );

    assert_close(
        sample_channel(&rust_common_image, 54, 50, 0),
        0.8906,
        0.03,
        0.05,
        "Rust badchannel common image[54,50,0,0]",
    );
    assert_close(
        sample_channel(&casa_common_image, 54, 50, 0),
        0.8906,
        0.03,
        0.05,
        "CASA badchannel common image[54,50,0,0]",
    );
    assert_close(
        sample_channel(&rust_common_image, 54, 50, 19),
        0.519_77,
        0.03,
        0.08,
        "Rust badchannel common image[54,50,0,19]",
    );
    assert_close(
        sample_channel(&casa_common_image, 54, 50, 19),
        0.519_77,
        0.03,
        0.08,
        "CASA badchannel common image[54,50,0,19]",
    );
    assert_close(
        sample_channel(&rust_common_residual, 54, 50, 19),
        0.033_942,
        0.01,
        0.1,
        "Rust badchannel common residual[54,50,0,19]",
    );
    assert_close(
        sample_channel(&casa_common_residual, 54, 50, 19),
        0.033_942,
        0.01,
        0.1,
        "CASA badchannel common residual[54,50,0,19]",
    );
}

#[test]
fn cube_common_restoringbeam_tracks_casa_on_refim_point() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let common = CubeWeightingOptions {
        per_channel_weight_density: false,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_prefix = temp.path().join("rust-refim-point-common-restoringbeam");
    let casa_prefix = temp.path().join("casa-refim-point-common-restoringbeam");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        10,
        common.clone(),
    )
    .expect("run rust common restoring beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa common restoring beam");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );
    let rust_beam = single_beam_summary(&rust_product(&rust_prefix, "image"));
    let casa_beam = single_beam_summary(&casa_product(&casa_prefix, "image"));
    assert_close(
        rust_beam.0 as f32,
        casa_beam.0 as f32,
        0.1,
        0.01,
        "common restoring beam major",
    );
    assert_close(
        rust_beam.1 as f32,
        casa_beam.1 as f32,
        0.1,
        0.01,
        "common restoring beam minor",
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    assert_close(
        sample_channel(&rust_image, 54, 50, 1),
        0.770_450,
        0.03,
        0.05,
        "Rust common restoring beam image[54,50,0,1]",
    );
    assert_close(
        sample_channel(&casa_image, 54, 50, 1),
        0.770_450,
        0.03,
        0.05,
        "CASA common restoring beam image[54,50,0,1]",
    );
    assert_close(
        sample_channel(&rust_image, 54, 50, 15),
        0.567_246,
        0.03,
        0.05,
        "Rust common restoring beam image[54,50,0,15]",
    );
    assert_close(
        sample_channel(&casa_image, 54, 50, 15),
        0.567_246,
        0.03,
        0.05,
        "CASA common restoring beam image[54,50,0,15]",
    );
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 common-beam parity on refim_point"]
fn cube_common_restoringbeam_algorithm_matches_casa_on_casa_beamset_refim_point() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let common = CubeWeightingOptions {
        per_channel_weight_density: false,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let casa_prefix = temp.path().join("casa-refim-point-common-restoringbeam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa common restoring beam");

    let per_plane_beams = image_beam_set(&casa_product(&casa_prefix, "psf"));
    assert_eq!(
        per_plane_beams.shape(),
        (20, 1),
        "CASA PSF beamset parsed with unexpected shape"
    );
    let derived_common = per_plane_beams
        .common_beam()
        .expect("derive common beam from CASA PSF beamset");
    let image_common = single_beam_summary(&casa_product(&casa_prefix, "image"));

    assert_close(
        derived_common
            .major_in("arcsec")
            .expect("derived major arcsec") as f32,
        image_common.0 as f32,
        0.1,
        0.01,
        "derived common beam major from CASA beamset",
    );
    assert_close(
        derived_common
            .minor_in("arcsec")
            .expect("derived minor arcsec") as f32,
        image_common.1 as f32,
        0.1,
        0.01,
        "derived common beam minor from CASA beamset",
    );
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 compare Rust and CASA derived common beamsets on refim_point"]
fn cube_common_restoringbeam_algorithm_compares_rust_and_casa_beamsets_on_refim_point() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let common = CubeWeightingOptions {
        per_channel_weight_density: false,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_prefix = temp.path().join("rust-refim-point-common-restoringbeam");
    let casa_prefix = temp.path().join("casa-refim-point-common-restoringbeam");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        10,
        common.clone(),
    )
    .expect("run rust common restoring beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa common restoring beam");

    let rust_psf_beamset = image_beam_set(&rust_product(&rust_prefix, "psf"));
    let casa_psf_beamset = image_beam_set(&casa_product(&casa_prefix, "psf"));
    let rust_common = rust_psf_beamset
        .common_beam()
        .expect("derive common beam from Rust PSF beamset");
    let casa_common = casa_psf_beamset
        .common_beam()
        .expect("derive common beam from CASA PSF beamset");
    let rust_image_common = single_beam_summary(&rust_product(&rust_prefix, "image"));
    let casa_image_common = single_beam_summary(&casa_product(&casa_prefix, "image"));

    eprintln!(
        "Rust derived common beam: major={} minor={} pa={}",
        rust_common.major_in("arcsec").expect("rust derived major"),
        rust_common.minor_in("arcsec").expect("rust derived minor"),
        rust_common
            .position_angle_in("deg")
            .expect("rust derived position angle")
    );
    eprintln!(
        "CASA derived common beam: major={} minor={} pa={}",
        casa_common.major_in("arcsec").expect("casa derived major"),
        casa_common.minor_in("arcsec").expect("casa derived minor"),
        casa_common
            .position_angle_in("deg")
            .expect("casa derived position angle")
    );
    eprintln!("Rust image common beam: {:?}", rust_image_common);
    eprintln!("CASA image common beam: {:?}", casa_image_common);
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 compare uniform common-beam PSF beamsets on refim_point_withline"]
fn cube_uniform_common_beam_diagnostics_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");

    let common = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-uniform-common");
    let casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-uniform-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        1,
        common.clone(),
    )
    .expect("run rust uniform common beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa uniform common beam");

    let rust_psf_beamset = image_beam_set(&rust_product(&rust_prefix, "psf"));
    let casa_psf_beamset = image_beam_set(&casa_product(&casa_prefix, "psf"));
    let rust_common = rust_psf_beamset
        .common_beam()
        .expect("derive common beam from Rust PSF beamset");
    let casa_common = casa_psf_beamset
        .common_beam()
        .expect("derive common beam from CASA PSF beamset");
    let rust_image_common = single_beam_summary(&rust_product(&rust_prefix, "image"));
    let casa_image_common = single_beam_summary(&casa_product(&casa_prefix, "image"));
    eprintln!(
        "Rust derived uniform common beam: major={} minor={} pa={}",
        rust_common.major_in("arcsec").expect("rust derived major"),
        rust_common.minor_in("arcsec").expect("rust derived minor"),
        rust_common
            .position_angle_in("deg")
            .expect("rust derived position angle")
    );
    eprintln!(
        "CASA derived uniform common beam: major={} minor={} pa={}",
        casa_common.major_in("arcsec").expect("casa derived major"),
        casa_common.minor_in("arcsec").expect("casa derived minor"),
        casa_common
            .position_angle_in("deg")
            .expect("casa derived position angle")
    );
    eprintln!("Rust uniform image common beam: {:?}", rust_image_common);
    eprintln!("CASA uniform image common beam: {:?}", casa_image_common);
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 per-plane PSF beam parity on refim_point_withline"]
fn cube_uniform_per_plane_psf_beam_diagnostics_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");

    let common = CubeWeightingOptions {
        per_channel_weight_density: true,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_prefix = temp
        .path()
        .join("rust-refim-point-withline-cube-uniform-common");
    let casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-uniform-common");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        1,
        common.clone(),
    )
    .expect("run rust uniform common beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa uniform common beam");

    let rust_psf_beams = image_beam_summaries(&rust_product(&rust_prefix, "psf"));
    let casa_psf_beams = image_beam_summaries(&casa_product(&casa_prefix, "psf"));
    assert_eq!(
        rust_psf_beams.len(),
        casa_psf_beams.len(),
        "PSF beam count mismatch"
    );

    let mut max_major = (0usize, 0.0f64, 0.0f64, 0.0f64);
    let mut max_minor = (0usize, 0.0f64, 0.0f64, 0.0f64);
    let mut max_pa = (0usize, 0.0f64, 0.0f64, 0.0f64);
    for (
        (rust_chan, rust_stokes, rust_major, rust_minor, rust_pa),
        (_, _, casa_major, casa_minor, casa_pa),
    ) in rust_psf_beams.iter().zip(casa_psf_beams.iter())
    {
        assert_eq!(*rust_stokes, 0, "unexpected stokes index");
        let major_delta = (rust_major - casa_major).abs();
        let minor_delta = (rust_minor - casa_minor).abs();
        let pa_delta = (rust_pa - casa_pa).abs();
        if major_delta > max_major.1 {
            max_major = (*rust_chan, major_delta, *rust_major, *casa_major);
        }
        if minor_delta > max_minor.1 {
            max_minor = (*rust_chan, minor_delta, *rust_minor, *casa_minor);
        }
        if pa_delta > max_pa.1 {
            max_pa = (*rust_chan, pa_delta, *rust_pa, *casa_pa);
        }
        eprintln!(
            "chan {rust_chan}: rust=({rust_major:.6}, {rust_minor:.6}, {rust_pa:.6}) casa=({casa_major:.6}, {casa_minor:.6}, {casa_pa:.6})"
        );
    }

    eprintln!(
        "max major delta at chan {}: delta={} rust={} casa={}",
        max_major.0, max_major.1, max_major.2, max_major.3
    );
    eprintln!(
        "max minor delta at chan {}: delta={} rust={} casa={}",
        max_minor.0, max_minor.1, max_minor.2, max_minor.3
    );
    eprintln!(
        "max pa delta at chan {}: delta={} rust={} casa={}",
        max_pa.0, max_pa.1, max_pa.2, max_pa.3
    );
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 compare Rust fitter against CASA PSF beam headers on refim_point_withline"]
fn cube_uniform_psf_fit_diagnostics_on_casa_psfs_for_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");

    let casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-uniform-common");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        1,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions {
            per_channel_weight_density: true,
            uvtaper: &[],
            restoring_beam_mode: RestoringBeamMode::Common,
        },
        "0Jy",
    )
    .expect("run casa uniform common beam");

    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let casa_psf_beams = image_beam_summaries(&casa_product(&casa_prefix, "psf"));
    assert_eq!(
        casa_psf_beams.len(),
        case.channel_count,
        "PSF beam count mismatch"
    );

    let cell_size_rad = [
        case.cell_arcsec.to_radians() / 3600.0,
        case.cell_arcsec.to_radians() / 3600.0,
    ];
    let mut max_major = (0usize, 0.0f64, 0.0f64, 0.0f64);
    let mut max_minor = (0usize, 0.0f64, 0.0f64, 0.0f64);
    let mut max_pa = (0usize, 0.0f64, 0.0f64, 0.0f64);
    for channel in 0..case.channel_count {
        let plane = extract_channel_plane(&casa_psf, channel);
        let fitted = fit_restoring_beam_from_psf(&plane, cell_size_rad, 0.35);
        let fitted_beam = fitted.beam.expect("Rust fitter returns beam on CASA PSF");
        let (_, _, casa_major, casa_minor, casa_pa) = casa_psf_beams[channel];
        let rust_major = fitted_beam.major_fwhm_rad.to_degrees() * 3600.0;
        let rust_minor = fitted_beam.minor_fwhm_rad.to_degrees() * 3600.0;
        let rust_pa = fitted_beam.position_angle_rad.to_degrees();
        let major_delta = (rust_major - casa_major).abs();
        let minor_delta = (rust_minor - casa_minor).abs();
        let pa_delta = (rust_pa - casa_pa).abs();
        if major_delta > max_major.1 {
            max_major = (channel, major_delta, rust_major, casa_major);
        }
        if minor_delta > max_minor.1 {
            max_minor = (channel, minor_delta, rust_minor, casa_minor);
        }
        if pa_delta > max_pa.1 {
            max_pa = (channel, pa_delta, rust_pa, casa_pa);
        }
        eprintln!(
            "chan {channel}: rust-fit-on-casa=({rust_major:.6}, {rust_minor:.6}, {rust_pa:.6}) casa-header=({casa_major:.6}, {casa_minor:.6}, {casa_pa:.6}) debug={:?}",
            fitted.debug
        );
    }
    eprintln!(
        "max fitter-vs-casa-header major delta at chan {}: delta={} rust={} casa={}",
        max_major.0, max_major.1, max_major.2, max_major.3
    );
    eprintln!(
        "max fitter-vs-casa-header minor delta at chan {}: delta={} rust={} casa={}",
        max_minor.0, max_minor.1, max_minor.2, max_minor.3
    );
    eprintln!(
        "max fitter-vs-casa-header pa delta at chan {}: delta={} rust={} casa={}",
        max_pa.0, max_pa.1, max_pa.2, max_pa.3
    );
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 per-plane PSF beam parity on refim_point"]
fn cube_psf_beamset_tracks_casa_on_refim_point_common_beam_case() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let common = CubeWeightingOptions {
        per_channel_weight_density: false,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_prefix = temp.path().join("rust-refim-point-common-restoringbeam");
    let casa_prefix = temp.path().join("casa-refim-point-common-restoringbeam");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        10,
        common.clone(),
    )
    .expect("run rust common restoring beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa common restoring beam");

    let rust_psf_beams = image_beam_summaries(&rust_product(&rust_prefix, "psf"));
    let casa_psf_beams = image_beam_summaries(&casa_product(&casa_prefix, "psf"));
    assert_eq!(
        rust_psf_beams.len(),
        casa_psf_beams.len(),
        "PSF beam count mismatch"
    );
    for (
        (rust_chan, rust_stokes, rust_major, rust_minor, rust_pa),
        (_, _, casa_major, casa_minor, casa_pa),
    ) in rust_psf_beams.iter().zip(casa_psf_beams.iter())
    {
        assert_close(
            *rust_major as f32,
            *casa_major as f32,
            0.1,
            0.01,
            &format!("Rust PSF beam major[chan={rust_chan}, stokes={rust_stokes}]"),
        );
        assert_close(
            *rust_minor as f32,
            *casa_minor as f32,
            0.1,
            0.01,
            &format!("Rust PSF beam minor[chan={rust_chan}, stokes={rust_stokes}]"),
        );
        assert_close(
            *rust_pa as f32,
            *casa_pa as f32,
            0.5,
            0.01,
            &format!("Rust PSF beam pa[chan={rust_chan}, stokes={rust_stokes}]"),
        );
    }
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 cube PSF beam fitting on refim_point"]
fn cube_psf_beam_fit_matches_casa_on_refim_point_channel_zero() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let casa_prefix = temp.path().join("casa-refim-point-common-restoringbeam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions {
            per_channel_weight_density: false,
            uvtaper: &[],
            restoring_beam_mode: RestoringBeamMode::Common,
        },
        "0Jy",
    )
    .expect("run casa common restoring beam");

    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let fitted = fit_restoring_beam_from_psf(
        &extract_channel_plane(&casa_psf, 0),
        [
            case.cell_arcsec.to_radians() / 3600.0,
            case.cell_arcsec.to_radians() / 3600.0,
        ],
        0.35,
    );
    let casa_psf_beams = image_beam_summaries(&casa_product(&casa_prefix, "psf"));
    let (_, _, casa_major, casa_minor, casa_pa) = casa_psf_beams[0];
    let beam = fitted.beam.expect("Rust fitter returns a beam on CASA PSF");
    assert_close(
        beam.major_fwhm_rad.to_degrees() as f32 * 3600.0,
        casa_major as f32,
        0.1,
        0.01,
        "Rust fitter major on CASA PSF channel 0",
    );
    assert_close(
        beam.minor_fwhm_rad.to_degrees() as f32 * 3600.0,
        casa_minor as f32,
        0.1,
        0.01,
        "Rust fitter minor on CASA PSF channel 0",
    );
    assert_close(
        beam.position_angle_rad.to_degrees() as f32,
        casa_pa as f32,
        0.5,
        0.01,
        "Rust fitter PA on CASA PSF channel 0",
    );
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 cube PSF beam metadata on refim_point"]
fn cube_rust_psf_metadata_matches_rust_fit_on_refim_point_channel_zero() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let rust_prefix = temp.path().join("rust-refim-point-common-restoringbeam");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        10,
        CubeWeightingOptions {
            per_channel_weight_density: false,
            uvtaper: &[],
            restoring_beam_mode: RestoringBeamMode::Common,
        },
    )
    .expect("run rust common restoring beam");

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let fitted = fit_restoring_beam_from_psf(
        &extract_channel_plane(&rust_psf, 0),
        [
            case.cell_arcsec.to_radians() / 3600.0,
            case.cell_arcsec.to_radians() / 3600.0,
        ],
        0.35,
    );
    let rust_psf_beams = image_beam_summaries(&rust_product(&rust_prefix, "psf"));
    let (_, _, rust_major, rust_minor, rust_pa) = rust_psf_beams[0];
    let beam = fitted.beam.expect("Rust fitter returns a beam on Rust PSF");
    assert_close(
        beam.major_fwhm_rad.to_degrees() as f32 * 3600.0,
        rust_major as f32,
        0.1,
        0.01,
        "Rust fitter major on Rust PSF channel 0",
    );
    assert_close(
        beam.minor_fwhm_rad.to_degrees() as f32 * 3600.0,
        rust_minor as f32,
        0.1,
        0.01,
        "Rust fitter minor on Rust PSF channel 0",
    );
    assert_close(
        beam.position_angle_rad.to_degrees() as f32,
        rust_pa as f32,
        0.5,
        0.01,
        "Rust fitter PA on Rust PSF channel 0",
    );
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_selected_cases() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 10,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let case0_expected = [(50usize, 50usize, 0usize, 1.50002f32)];
    let case1_expected = [(50usize, 50usize, 0usize, 1.50002f32)];
    let case2_expected = [(50usize, 50usize, 0usize, 1.4643f32)];
    let case3_expected = [(50usize, 50usize, 0usize, 1.2000f32)];
    let case5_expected = [(50usize, 50usize, 0usize, 1.4643f32)];
    let case6_expected = [(50usize, 50usize, 0usize, 1.36365354f32)];
    let case7_expected = [
        (50usize, 50usize, 0usize, 0.0f32),
        (50usize, 50usize, 3usize, 1.2000f32),
    ];
    let case8_expected = [(50usize, 50usize, 9usize, 1.42858946f32)];
    let case9_expected = [(50usize, 50usize, 9usize, 1.46184647f32)];
    let case10_expected = [(50usize, 50usize, 0usize, 1.46184647f32)];
    let case11_expected = [(50usize, 50usize, 4usize, 1.50001776f32)];
    let case12_expected = [(50usize, 50usize, 4usize, 1.50001931f32)];
    let case14_expected = [(50usize, 50usize, 0usize, 1.25000215f32)];
    let case15_expected = [(50usize, 50usize, 0usize, 1.25001216f32)];
    let case16_expected = [(50usize, 50usize, 4usize, 1.50001776f32)];
    let case18_expected = [(50usize, 50usize, 9usize, 1.50001764f32)];
    let case20_expected = [(50usize, 50usize, 4usize, 1.5000546f32)];
    let case21_expected = [
        (50usize, 50usize, 0usize, 1.2500016f32),
        (50usize, 50usize, 6usize, 0.0f32),
        (50usize, 50usize, 7usize, 0.0f32),
    ];
    let case22_expected = [(50usize, 50usize, 0usize, 1.5000546f32)];
    let case23_expected = [(50usize, 50usize, 0usize, 1.2500156f32)];

    struct RefimCase<'a> {
        suffix: &'a str,
        options: CubeCaseOptions<'a>,
        expected_frame: &'a str,
        expected_ref_hz: f64,
        expected_voxels: &'a [(usize, usize, usize, f32)],
    }

    let cases = [
        RefimCase {
            suffix: "cube0",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Channel(0)),
                width: Some(CubeAxisStep::Channel(1)),
                outframe: "LSRK",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 999_988_750.0,
            expected_voxels: &case0_expected,
        },
        RefimCase {
            suffix: "cube1",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Channel(0)),
                width: Some(CubeAxisStep::Channel(1)),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 999_999_990.0,
            expected_voxels: &case1_expected,
        },
        RefimCase {
            suffix: "cube2",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Channel(0)),
                width: Some(CubeAxisStep::Channel(2)),
                outframe: "LSRK",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 1.024_988_46e9,
            expected_voxels: &case2_expected,
        },
        RefimCase {
            suffix: "cube3",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Channel(5)),
                width: Some(CubeAxisStep::Channel(1)),
                outframe: "LSRK",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 1.249_985_937e9,
            expected_voxels: &case3_expected,
        },
        RefimCase {
            suffix: "cube5",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: None,
                width: Some(CubeAxisStep::Text("100MHz")),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.025e9,
            expected_voxels: &case5_expected,
        },
        RefimCase {
            suffix: "cube6",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("1.1GHz")),
                width: None,
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.1e9,
            expected_voxels: &case6_expected,
        },
        RefimCase {
            suffix: "cube7",
            options: CubeCaseOptions {
                spw_selector: "0:4~19",
                nchan: 10,
                start: Some(CubeAxisStep::Text("1.1GHz")),
                width: None,
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.1e9,
            expected_voxels: &case7_expected,
        },
        RefimCase {
            suffix: "cube8",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("1.5GHz")),
                width: Some(CubeAxisStep::Text("-50MHz")),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.5e9,
            expected_voxels: &case8_expected,
        },
        RefimCase {
            suffix: "cube9",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: None,
                width: Some(CubeAxisStep::Text("23983.4km/s")),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.925e9,
            expected_voxels: &case9_expected,
        },
        RefimCase {
            suffix: "cube10",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: None,
                width: Some(CubeAxisStep::Text("-23983.4km/s")),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.025e9,
            expected_voxels: &case10_expected,
        },
        RefimCase {
            suffix: "cube11",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("11991.7km/s")),
                width: None,
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.2e9,
            expected_voxels: &case11_expected,
        },
        RefimCase {
            suffix: "cube12",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("11977.6km/s")),
                width: None,
                outframe: "BARY",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "BARY",
            expected_ref_hz: 1.200_058_783e9,
            expected_voxels: &case12_expected,
        },
        RefimCase {
            suffix: "cube14",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("1.2GHz")),
                width: None,
                outframe: "LSRK",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 1.2e9,
            expected_voxels: &case14_expected,
        },
        RefimCase {
            suffix: "cube15",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("1.199989GHz")),
                width: None,
                outframe: "LSRK",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 1.199_989e9,
            expected_voxels: &case15_expected,
        },
        RefimCase {
            suffix: "cube16",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::Text("11991.7km/s")),
                width: None,
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.2e9,
            expected_voxels: &case16_expected,
        },
        RefimCase {
            suffix: "cube17",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::FramedValue {
                    python_literal: "_me.radialvelocity('BARY', _qa.quantity('11977.6km/s'))",
                    value: casa_ms::CubeAxisValue::VelocityMs {
                        ms: 11_977_600.0,
                        frame: Some(FrequencyRef::BARY),
                    },
                }),
                width: None,
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "BARY",
            expected_ref_hz: 1.200_058_783e9,
            expected_voxels: &case12_expected,
        },
        RefimCase {
            suffix: "cube18",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: None,
                width: Some(CubeAxisStep::Text("11991.7km/s")),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.45e9,
            expected_voxels: &case18_expected,
        },
        RefimCase {
            suffix: "cube19",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: None,
                width: Some(CubeAxisStep::FramedValue {
                    python_literal: "_me.radialvelocity('TOPO', _qa.quantity('11991.7km/s'))",
                    value: casa_ms::CubeAxisValue::VelocityMs {
                        ms: 11_991_700.0,
                        frame: Some(FrequencyRef::TOPO),
                    },
                }),
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.45e9,
            expected_voxels: &case18_expected,
        },
        RefimCase {
            suffix: "cube20",
            options: CubeCaseOptions {
                spw_selector: "0",
                nchan: 10,
                start: Some(CubeAxisStep::FramedValue {
                    python_literal: "_me.todoppler('radio', _me.frequency('LSRK', _qa.quantity('1.199989GHz')), _qa.quantity('1.25GHz'))",
                    value: casa_ms::CubeAxisValue::VelocityMs {
                        ms: 11_994_336.493_630_42,
                        frame: None,
                    },
                }),
                width: None,
                outframe: "LSRK",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 1.199_989_152e9,
            expected_voxels: &case20_expected,
        },
        RefimCase {
            suffix: "cube21",
            options: CubeCaseOptions {
                spw_selector: "0:4~9;12~14",
                nchan: 10,
                start: Some(CubeAxisStep::Channel(4)),
                width: None,
                outframe: "LSRK",
                interpolation: "nearest",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 1.199_986_500e9,
            expected_voxels: &case21_expected,
        },
        RefimCase {
            suffix: "cube22",
            options: CubeCaseOptions {
                spw_selector: "0:0~10^2",
                nchan: 10,
                start: Some(CubeAxisStep::Channel(0)),
                width: None,
                outframe: "LSRK",
                interpolation: "nearest",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "LSRK",
            expected_ref_hz: 0.999_988_750_387e9,
            expected_voxels: &case22_expected,
        },
        RefimCase {
            suffix: "cube23",
            options: CubeCaseOptions {
                spw_selector: "0:4~13",
                nchan: 10,
                start: None,
                width: None,
                outframe: "TOPO",
                interpolation: "linear",
                veltype: "radio",
                restfreq: "1.25GHz",
            },
            expected_frame: "TOPO",
            expected_ref_hz: 1.20e9,
            expected_voxels: &case23_expected,
        },
    ];
    for spec in cases {
        let rust_prefix = temp.path().join(format!("rust-{}", spec.suffix));
        let casa_prefix = temp.path().join(format!("casa-{}", spec.suffix));
        let _summary = run_rust_imager_cube_case_with_options(
            case,
            &staged_ms_path,
            &rust_prefix,
            spec.options.clone(),
            false,
            10,
        )
        .unwrap_or_else(|error| panic!("run rust refim_point cube case {}: {error}", spec.suffix));
        run_casa_tclean_cube_case_with_options(
            case,
            &staged_ms_path,
            &casa_prefix,
            10,
            "hogbom",
            &[],
            0.0,
            spec.options.clone(),
            "0Jy",
        )
        .expect("run casa refim_point cube case");

        compare_image_headers(
            &rust_product(&rust_prefix, "image"),
            &casa_product(&casa_prefix, "image"),
            "Jy/beam",
            true,
        );
        let (rust_frame, rust_ref_hz) =
            spectral_header_summary(&rust_product(&rust_prefix, "image"));
        let (casa_frame, casa_ref_hz) =
            spectral_header_summary(&casa_product(&casa_prefix, "image"));
        assert_eq!(
            rust_frame, spec.expected_frame,
            "Rust spectral frame {}",
            spec.suffix
        );
        assert_eq!(
            casa_frame, spec.expected_frame,
            "CASA spectral frame {}",
            spec.suffix
        );
        assert_close(
            rust_ref_hz as f32,
            spec.expected_ref_hz as f32,
            5.0e4,
            1.0e-5,
            &format!("Rust spectral ref {}", spec.suffix),
        );
        assert_close(
            casa_ref_hz as f32,
            spec.expected_ref_hz as f32,
            5.0e4,
            1.0e-5,
            &format!("CASA spectral ref {}", spec.suffix),
        );

        let rust_image = read_image(&rust_product(&rust_prefix, "image"));
        let casa_image = read_image(&casa_product(&casa_prefix, "image"));
        for (x, y, chan, expected) in spec.expected_voxels {
            assert_close(
                sample_channel(&rust_image, *x, *y, *chan),
                *expected,
                0.04,
                0.06,
                &format!("Rust {} image[{x},{y},chan={chan}]", spec.suffix),
            );
            assert_close(
                sample_channel(&casa_image, *x, *y, *chan),
                *expected,
                0.04,
                0.06,
                &format!("CASA {} image[{x},{y},chan={chan}]", spec.suffix),
            );
            assert_close(
                sample_channel(&rust_image, *x, *y, *chan),
                sample_channel(&casa_image, *x, *y, *chan),
                0.04,
                0.06,
                &format!("Rust/CASA {} image[{x},{y},chan={chan}]", spec.suffix),
            );
        }
    }
}

#[derive(Clone)]
struct DirectCubeParityCase<'a> {
    suffix: &'a str,
    options: CubeCaseOptions<'a>,
    center_spectrum_abs_tol: f32,
    center_spectrum_rel_tol: f32,
    image_rms_tol: f32,
    image_max_abs_tol: f32,
    image_correlation_min: f32,
}

fn run_refim_point_direct_cube_case(spec: DirectCubeParityCase<'_>) {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: spec.options.nchan,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");
    let rust_prefix = temp.path().join(format!("rust-{}", spec.suffix));
    let casa_prefix = temp.path().join(format!("casa-{}", spec.suffix));

    let _summary = run_rust_imager_cube_case_with_options(
        case,
        &staged_ms_path,
        &rust_prefix,
        spec.options.clone(),
        false,
        10,
    )
    .unwrap_or_else(|error| panic!("run rust refim_point cube case {}: {error}", spec.suffix));
    run_casa_tclean_cube_case_with_options(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        spec.options.clone(),
        "0Jy",
    )
    .expect("run casa refim_point cube case");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );

    let rust_summary = spectral_header_summary_full(&rust_product(&rust_prefix, "image"));
    let casa_summary = spectral_header_summary_full(&casa_product(&casa_prefix, "image"));
    assert_eq!(
        rust_summary.frequency_ref, casa_summary.frequency_ref,
        "spectral frame {}",
        spec.suffix
    );
    assert_close(
        rust_summary.reference_value_hz as f32,
        casa_summary.reference_value_hz as f32,
        5.0e4,
        1.0e-5,
        &format!("spectral ref {}", spec.suffix),
    );
    assert_close(
        rust_summary.increment_hz as f32,
        casa_summary.increment_hz as f32,
        5.0e4,
        1.0e-5,
        &format!("spectral delta {}", spec.suffix),
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for chan in 0..spec.options.nchan {
        assert_close(
            sample_channel(&rust_image, 50, 50, chan),
            sample_channel(&casa_image, 50, 50, chan),
            spec.center_spectrum_abs_tol,
            spec.center_spectrum_rel_tol,
            &format!("Rust/CASA {} image[50,50,chan={chan}]", spec.suffix),
        );
    }

    let stats = image_difference_stats(&rust_image, &casa_image);
    assert!(
        stats.rms <= spec.image_rms_tol,
        "image RMS {} too large: {} > {}",
        spec.suffix,
        stats.rms,
        spec.image_rms_tol
    );
    assert!(
        stats.max_abs <= spec.image_max_abs_tol,
        "image max abs {} too large: {} > {}",
        spec.suffix,
        stats.max_abs,
        spec.image_max_abs_tol
    );
    assert!(
        stats.correlation >= spec.image_correlation_min,
        "image correlation {} too low: {} < {}",
        spec.suffix,
        stats.correlation,
        spec.image_correlation_min
    );
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_case_20_direct_parity() {
    run_refim_point_direct_cube_case(DirectCubeParityCase {
        suffix: "cube20-direct",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::FramedValue {
                python_literal: "_me.todoppler('radio', _me.frequency('LSRK', _qa.quantity('1.199989GHz')), _qa.quantity('1.25GHz'))",
                value: casa_ms::CubeAxisValue::VelocityMs {
                    ms: 11_994_336.493_630_42,
                    frame: None,
                },
            }),
            width: None,
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        center_spectrum_abs_tol: 0.04,
        center_spectrum_rel_tol: 0.08,
        image_rms_tol: 0.03,
        image_max_abs_tol: 0.30,
        image_correlation_min: 0.993,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_case_22_direct_parity() {
    run_refim_point_direct_cube_case(DirectCubeParityCase {
        suffix: "cube22-direct",
        options: CubeCaseOptions {
            spw_selector: "0:0~10^2",
            nchan: 10,
            start: Some(CubeAxisStep::Channel(0)),
            width: None,
            outframe: "LSRK",
            interpolation: "nearest",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        center_spectrum_abs_tol: 0.04,
        center_spectrum_rel_tol: 0.08,
        image_rms_tol: 0.03,
        image_max_abs_tol: 0.15,
        image_correlation_min: 0.995,
    });
}

#[test]
#[ignore = "CASA test_task_tclean.py leaves cube4 assertions commented out; not a validated oracle"]
fn channel_mode_cube_products_track_casa_on_refim_point_case_4_direct_parity() {
    run_refim_point_direct_cube_case(DirectCubeParityCase {
        suffix: "cube4",
        options: CubeCaseOptions {
            spw_selector: "0:5~19",
            nchan: 10,
            start: Some(CubeAxisStep::Channel(0)),
            width: Some(CubeAxisStep::Channel(1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        center_spectrum_abs_tol: 0.04,
        center_spectrum_rel_tol: 0.08,
        image_rms_tol: 0.03,
        image_max_abs_tol: 0.15,
        image_correlation_min: 0.995,
    });
}

#[test]
#[ignore = "CASA test_task_tclean.py marks cube13 as not quite properly working and leaves assertions commented out"]
fn channel_mode_cube_products_track_casa_on_refim_point_case_13_direct_parity() {
    run_refim_point_direct_cube_case(DirectCubeParityCase {
        suffix: "cube13",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 8,
            start: Some(CubeAxisStep::Text("-41347.8km/s")),
            width: Some(CubeAxisStep::Text("20000km/s")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "optical",
            restfreq: "1.25GHz",
        },
        center_spectrum_abs_tol: 0.04,
        center_spectrum_rel_tol: 0.08,
        image_rms_tol: 0.03,
        image_max_abs_tol: 0.15,
        image_correlation_min: 0.995,
    });
}

#[derive(Clone)]
struct RefimDescendingCase<'a> {
    suffix: &'a str,
    options: CubeCaseOptions<'a>,
    expected_frame: &'a str,
    expected_ref_hz: f64,
    expected_delta_hz: f64,
}

fn run_refim_point_descendingfreqs_case(spec: RefimDescendingCase<'_>) {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_descendingfreqs.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 10,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_descending.ms")
        .expect("stage refim_point_descendingfreqs");
    let rust_prefix = temp.path().join(format!("rust-{}", spec.suffix));
    let casa_prefix = temp.path().join(format!("casa-{}", spec.suffix));
    let _summary = run_rust_imager_cube_case_with_options(
        case,
        &staged_ms_path,
        &rust_prefix,
        spec.options.clone(),
        false,
        10,
    )
    .unwrap_or_else(|error| {
        panic!(
            "run rust refim_point_descendingfreqs cube case {}: {error}",
            spec.suffix
        )
    });
    run_casa_tclean_cube_case_with_options(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        spec.options.clone(),
        "0Jy",
    )
    .expect("run casa refim_point_descendingfreqs cube case");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );
    let rust_summary = spectral_header_summary_full(&rust_product(&rust_prefix, "image"));
    let casa_summary = spectral_header_summary_full(&casa_product(&casa_prefix, "image"));
    for (label, summary) in [("Rust", &rust_summary), ("CASA", &casa_summary)] {
        assert_eq!(
            summary.frequency_ref, spec.expected_frame,
            "{label} spectral frame {}",
            spec.suffix
        );
        assert_close(
            summary.reference_value_hz as f32,
            spec.expected_ref_hz as f32,
            5.0e4,
            1.0e-5,
            &format!("{label} spectral ref {}", spec.suffix),
        );
        assert_close(
            summary.increment_hz as f32,
            spec.expected_delta_hz as f32,
            5.0e4,
            1.0e-5,
            &format!("{label} spectral delta {}", spec.suffix),
        );
    }
    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    assert_close(
        sample_channel(&rust_image, 50, 50, 0),
        sample_channel(&casa_image, 50, 50, 0),
        0.04,
        0.06,
        &format!("Rust/CASA {} image[50,50,chan=0]", spec.suffix),
    );
}

fn run_refim_point_descendingfreqs_dirty_case(spec: RefimDescendingCase<'_>) {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_descendingfreqs.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 10,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_descending.ms")
        .expect("stage refim_point_descendingfreqs");
    let rust_prefix = temp.path().join(format!("rust-dirty-{}", spec.suffix));
    let casa_prefix = temp.path().join(format!("casa-dirty-{}", spec.suffix));
    let _summary = run_rust_imager_cube_case_with_options(
        case,
        &staged_ms_path,
        &rust_prefix,
        spec.options.clone(),
        true,
        0,
    )
    .unwrap_or_else(|error| {
        panic!(
            "run rust refim_point_descendingfreqs dirty cube case {}: {error}",
            spec.suffix
        )
    });
    let rust_residual_before_casa = read_image(&rust_product(&rust_prefix, "residual"));
    assert_close(
        sample_channel(&rust_residual_before_casa, 50, 50, 0),
        0.0,
        0.05,
        0.2,
        &format!("dirty {} residual[50,50,chan=0] before CASA", spec.suffix),
    );
    run_casa_tclean_cube_case_with_options(
        case,
        &staged_ms_path,
        &casa_prefix,
        0,
        "hogbom",
        &[],
        0.0,
        spec.options.clone(),
        "0Jy",
    )
    .expect("run casa refim_point_descendingfreqs dirty cube case");

    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for chan in 0..10usize {
        assert_close(
            sample_channel(&rust_residual, 50, 50, chan),
            sample_channel(&casa_residual, 50, 50, chan),
            0.05,
            0.2,
            &format!("dirty {} residual[50,50,chan={chan}]", spec.suffix),
        );
    }
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f1() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF1",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: None,
            width: None,
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.949_978e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f2() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF2",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Channel(5)),
            width: None,
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.699_981e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f3() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF3",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Channel(5)),
            width: Some(CubeAxisStep::Channel(1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.699_981e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f4() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF4",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Channel(9)),
            width: Some(CubeAxisStep::Channel(-1)),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.499_983_125e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f5() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF5",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Text("1.499983125GHz")),
            width: None,
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.499_983_125e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f6() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF6",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Text("1.499983125GHz")),
            width: Some(CubeAxisStep::Text("0.049999438GHz")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.499_983_125e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f7() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF7",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Text("1.699981GHz")),
            width: Some(CubeAxisStep::Text("-0.049999438GHz")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.699_981e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f8() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF8",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: None,
            width: Some(CubeAxisStep::Text("0.049999438GHz")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 0.999_989e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f9() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF9",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: None,
            width: Some(CubeAxisStep::Text("-0.049999438GHz")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.449_983_692_63e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f10() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF10",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Text("-107920.698km/s")),
            width: None,
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.699_980_875e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f11() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF11",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Text("-107920.698km/s")),
            width: Some(CubeAxisStep::Text("1.1991563418e4km/s")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.699_980_875e9,
        expected_delta_hz: -0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f12() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF12",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: Some(CubeAxisStep::Text("-59954.444km/s")),
            width: Some(CubeAxisStep::Text("-1.1991563418e4km/s")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.499_983_125_58e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f13() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF13",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: None,
            width: Some(CubeAxisStep::Text("1.1991563418e4km/s")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 1.449_983_688e9,
        expected_delta_hz: -0.049_999_437_519_4e9,
    });
}

#[test]
fn channel_mode_cube_products_track_casa_on_refim_point_descendingfreqs_case_f14() {
    run_refim_point_descendingfreqs_case(RefimDescendingCase {
        suffix: "descendF14",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: None,
            width: Some(CubeAxisStep::Text("-1.1991563418e4km/s")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 0.999_988_750_387e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn dirty_cube_products_track_casa_on_refim_point_descendingfreqs_case_f14() {
    run_refim_point_descendingfreqs_dirty_case(RefimDescendingCase {
        suffix: "descendF14",
        options: CubeCaseOptions {
            spw_selector: "0",
            nchan: 10,
            start: None,
            width: Some(CubeAxisStep::Text("-1.1991563418e4km/s")),
            outframe: "LSRK",
            interpolation: "linear",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        expected_frame: "LSRK",
        expected_ref_hz: 0.999_988_750_387e9,
        expected_delta_hz: 0.049_999_438e9,
    });
}

#[test]
fn hogbom_cube_products_track_casa_on_refim_eptwochan() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_eptwochan.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 3,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 200,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_eptwochan.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-eptwochan-cube-hogbom");
    let casa_prefix = temp.path().join("casa-refim-eptwochan-cube-hogbom");

    let rust_summary = run_rust_imager_cube_case_with_deconvolver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        100,
        Deconvolver::Hogbom,
        0.1,
        100,
    )
    .expect("run rust imager");
    let casa_summary = run_casa_tclean_cube_case(
        case,
        &staged_ms_path,
        &casa_prefix,
        100,
        "hogbom",
        CubeAxisStep::Channel(0),
        CubeAxisStep::Channel(1),
        "0.1Jy",
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );

    assert_eq!(
        rust_summary.major_cycles, 2,
        "expected Rust cube major cycles"
    );
    assert_eq!(
        casa_summary["nmajordone"].as_u64(),
        Some(2),
        "expected CASA cube major cycles"
    );
    assert_eq!(
        rust_summary.minor_iterations, 116,
        "expected Rust cube total minor iterations"
    );
    assert_eq!(
        casa_summary["iterdone"].as_u64(),
        Some(116),
        "expected CASA cube total minor iterations"
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y, chan, expected) in [
        (100usize, 100usize, 0usize, 0.939f32),
        (100usize, 100usize, 2usize, 0.282f32),
    ] {
        assert_close(
            sample_channel(&rust_image, x, y, chan),
            expected,
            0.03,
            0.05,
            &format!("rust cube image[{x},{y},chan={chan}]"),
        );
        assert_close(
            sample_channel(&casa_image, x, y, chan),
            expected,
            0.03,
            0.05,
            &format!("casa cube image[{x},{y},chan={chan}]"),
        );
        assert_close(
            sample_channel(&rust_image, x, y, chan),
            sample_channel(&casa_image, x, y, chan),
            0.03,
            0.05,
            &format!("cube image[{x},{y},chan={chan}]"),
        );
    }
}

#[test]
fn clark_cube_products_track_casa_on_refim_eptwochan() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_eptwochan.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 3,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 200,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_eptwochan.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-eptwochan-cube-clark");
    let casa_prefix = temp.path().join("casa-refim-eptwochan-cube-clark");

    let rust_summary = run_rust_imager_cube_case_with_deconvolver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        100,
        Deconvolver::Clark,
        0.1,
        100,
    )
    .expect("run rust imager");
    let casa_summary = run_casa_tclean_cube_case(
        case,
        &staged_ms_path,
        &casa_prefix,
        100,
        "clark",
        CubeAxisStep::Channel(0),
        CubeAxisStep::Channel(1),
        "0.1Jy",
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );

    assert_eq!(
        rust_summary.major_cycles, 2,
        "expected Rust Clark cube major cycles"
    );
    assert_eq!(
        casa_summary["nmajordone"].as_u64(),
        Some(2),
        "expected CASA Clark cube major cycles"
    );
    assert_eq!(
        rust_summary.minor_iterations, 114,
        "expected Rust Clark cube total minor iterations"
    );
    assert_eq!(
        casa_summary["iterdone"].as_u64(),
        Some(114),
        "expected CASA Clark cube total minor iterations"
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y, chan, expected) in [
        (100usize, 100usize, 0usize, 0.935f32),
        (100usize, 100usize, 2usize, 0.282f32),
    ] {
        assert_close(
            sample_channel(&rust_image, x, y, chan),
            expected,
            0.03,
            0.05,
            &format!("rust Clark cube image[{x},{y},chan={chan}]"),
        );
        assert_close(
            sample_channel(&casa_image, x, y, chan),
            expected,
            0.03,
            0.05,
            &format!("casa Clark cube image[{x},{y},chan={chan}]"),
        );
        assert_close(
            sample_channel(&rust_image, x, y, chan),
            sample_channel(&casa_image, x, y, chan),
            0.03,
            0.05,
            &format!("Clark cube image[{x},{y},chan={chan}]"),
        );
    }
}

#[test]
fn multiscale_cube_products_track_casa_on_refim_eptwochan() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_eptwochan.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 3,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 200,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_eptwochan.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-eptwochan-cube-multiscale");
    let casa_prefix = temp.path().join("casa-refim-eptwochan-cube-multiscale");
    let scales = [0.0, 6.0, 10.0, 20.0, 40.0];
    let small_scale_bias = 0.6f32;

    let rust_summary = run_rust_imager_cube_case_with_solver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        100,
        Deconvolver::Multiscale,
        &scales,
        small_scale_bias,
        0.1,
        100,
        casars_imager::SpectralMode::Cube,
        CubeWeightingOptions::default(),
    )
    .expect("run rust imager");
    let casa_summary = run_casa_tclean_cube_case_with_solver(
        case,
        &staged_ms_path,
        &casa_prefix,
        100,
        "multiscale",
        &scales,
        small_scale_bias,
        CubeAxisStep::Channel(0),
        CubeAxisStep::Channel(1),
        "0.1Jy",
    )
    .expect("run casa tclean");
    eprintln!(
        "RUST cube multiscale summaries: {:?}",
        rust_summary.channel_summaries
    );
    eprintln!("CASA cube multiscale summary: {:?}", casa_summary);
    eprintln!(
        "RUST beams: {:?}",
        image_beam_summaries(&rust_product(&rust_prefix, "image"))
    );
    eprintln!(
        "CASA beams: {:?}",
        image_beam_summaries(&casa_product(&casa_prefix, "image"))
    );
    eprintln!(
        "RUST PSF beams: {:?}",
        image_beam_summaries(&rust_product(&rust_prefix, "psf"))
    );
    eprintln!(
        "CASA PSF beams: {:?}",
        image_beam_summaries(&casa_product(&casa_prefix, "psf"))
    );
    eprintln!(
        "RUST PSF half-max widths: {:?}",
        psf_half_max_widths(&rust_product(&rust_prefix, "psf"))
    );
    eprintln!(
        "CASA PSF half-max widths: {:?}",
        psf_half_max_widths(&casa_product(&casa_prefix, "psf"))
    );
    eprintln!(
        "RUST PSF spectral header: {:?}",
        spectral_header_summary_full(&rust_product(&rust_prefix, "psf"))
    );
    eprintln!(
        "CASA PSF spectral header: {:?}",
        spectral_header_summary_full(&casa_product(&casa_prefix, "psf"))
    );
    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_psf_sidelobes = (0..3)
        .map(|channel| {
            estimate_psf_sidelobe_from_psf(
                &extract_channel_plane(&rust_psf, channel),
                [
                    case.cell_arcsec.to_radians() / 3600.0,
                    case.cell_arcsec.to_radians() / 3600.0,
                ],
                0.35,
            )
        })
        .collect::<Vec<_>>();
    let casa_psf_sidelobes = (0..3)
        .map(|channel| {
            estimate_psf_sidelobe_from_psf(
                &extract_channel_plane(&casa_psf, channel),
                [
                    case.cell_arcsec.to_radians() / 3600.0,
                    case.cell_arcsec.to_radians() / 3600.0,
                ],
                0.35,
            )
        })
        .collect::<Vec<_>>();
    let casa_psf_chan0 = extract_channel_plane(&casa_psf, 0);
    let casa_psf_fit = fit_restoring_beam_from_psf(
        &casa_psf_chan0,
        [
            case.cell_arcsec.to_radians() / 3600.0,
            case.cell_arcsec.to_radians() / 3600.0,
        ],
        0.35,
    );
    eprintln!("Rust fitter on CASA PSF channel 0: {:?}", casa_psf_fit);
    eprintln!("Rust PSF sidelobes: {:?}", rust_psf_sidelobes);
    eprintln!(
        "CASA PSF sidelobes via Rust estimator: {:?}",
        casa_psf_sidelobes
    );

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        true,
    );

    assert_eq!(
        rust_summary.major_cycles, 3,
        "expected Rust cube multiscale major cycles"
    );
    assert_eq!(
        casa_summary["nmajordone"].as_u64(),
        Some(3),
        "expected CASA cube multiscale major cycles"
    );
    assert_eq!(
        rust_summary.minor_iterations, 100,
        "expected Rust cube multiscale total minor iterations"
    );
    assert_eq!(
        casa_summary["iterdone"].as_u64(),
        Some(100),
        "expected CASA cube multiscale total minor iterations"
    );
    let casa_traces = extract_casa_cube_minor_cycle_traces(&casa_summary).expect("CASA traces");
    eprintln!("CASA cube multiscale traces: {:?}", casa_traces);
    assert_eq!(
        rust_summary.channel_summaries.len(),
        casa_traces.len(),
        "expected one CASA trace set per Rust cube channel"
    );
    for (rust_channel, casa_channel) in rust_summary.channel_summaries.iter().zip(&casa_traces) {
        assert_eq!(
            rust_channel.minor_cycle_traces.len(),
            casa_channel.len(),
            "expected matching multiscale block counts for channel {}",
            rust_channel.channel_index
        );
        for (rust_trace, casa_trace) in rust_channel.minor_cycle_traces.iter().zip(casa_channel) {
            assert_eq!(
                rust_trace.start_reported_iteration, casa_trace.start_reported_iteration,
                "expected matching cube multiscale block start iteration for channel {} block {}",
                rust_channel.channel_index, rust_trace.cycle_index
            );
            assert_eq!(
                rust_trace.reported_updates, casa_trace.reported_updates,
                "expected matching cube multiscale reported iterations for channel {} block {}",
                rust_channel.channel_index, rust_trace.cycle_index
            );
            assert_close(
                rust_trace.start_peak_residual_jy_per_beam,
                casa_trace.start_peak_residual_jy_per_beam,
                1.0e-4,
                1.0e-4,
                &format!(
                    "cube multiscale start peak channel {} block {}",
                    rust_channel.channel_index, rust_trace.cycle_index
                ),
            );
            assert_close(
                rust_trace.end_peak_residual_jy_per_beam,
                casa_trace.end_peak_residual_jy_per_beam,
                1.0e-4,
                1.0e-4,
                &format!(
                    "cube multiscale end peak channel {} block {}",
                    rust_channel.channel_index, rust_trace.cycle_index
                ),
            );
        }
    }

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y, chan, expected) in [
        (100usize, 100usize, 0usize, 0.888f32),
        (100usize, 100usize, 2usize, 0.1601f32),
    ] {
        assert_close(
            sample_channel(&rust_image, x, y, chan),
            expected,
            0.04,
            0.06,
            &format!("rust cube multiscale image[{x},{y},chan={chan}]"),
        );
        assert_close(
            sample_channel(&casa_image, x, y, chan),
            expected,
            0.04,
            0.06,
            &format!("casa cube multiscale image[{x},{y},chan={chan}]"),
        );
        assert_close(
            sample_channel(&rust_image, x, y, chan),
            sample_channel(&casa_image, x, y, chan),
            0.04,
            0.06,
            &format!("cube multiscale image[{x},{y},chan={chan}]"),
        );
    }
}

#[test]
fn clark_cube_iteration_controls_track_casa_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    struct IterbotCase<'a> {
        suffix: &'a str,
        threshold: &'a str,
        expected_iterdone: u64,
        expected_nmajordone: u64,
        expected_clean_stop_reason: Option<CleanStopReason>,
        expected_model_voxels: &'a [(usize, usize, usize, f32)],
    }

    let cases = [
        IterbotCase {
            suffix: "iterbot-cube-1",
            threshold: "0.75Jy",
            expected_iterdone: 90,
            expected_nmajordone: 2,
            expected_clean_stop_reason: None,
            expected_model_voxels: &[],
        },
        IterbotCase {
            suffix: "iterbot-cube-2",
            threshold: "1.75Jy",
            expected_iterdone: 12,
            expected_nmajordone: 2,
            expected_clean_stop_reason: None,
            expected_model_voxels: &[
                (50usize, 50usize, 7usize, 1.73f32),
                (50usize, 50usize, 3usize, 0.0f32),
            ],
        },
        IterbotCase {
            suffix: "iterbot-cube-3",
            threshold: "3.5Jy",
            expected_iterdone: 0,
            expected_nmajordone: 1,
            expected_clean_stop_reason: Some(CleanStopReason::GlobalThresholdReached),
            expected_model_voxels: &[],
        },
    ];

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");

    for iter_case in cases {
        let rust_prefix = temp
            .path()
            .join(format!("rust-refim-point-withline-{}", iter_case.suffix));
        let casa_prefix = temp
            .path()
            .join(format!("casa-refim-point-withline-{}", iter_case.suffix));
        let clean = CubeCleanControls {
            threshold_jy: parse_threshold_jy_text(iter_case.threshold).expect("parse threshold"),
            minor_cycle_length: 10,
            ..CubeCleanControls::default()
        };

        let rust_summary = run_rust_imager_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &rust_prefix,
            false,
            10,
            Deconvolver::Clark,
            &[],
            0.0,
            clean,
            CubeWeightingOptions::default(),
        )
        .expect("run rust iterbot cube case");
        let casa_summary = run_casa_tclean_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &casa_prefix,
            10,
            "clark",
            &[],
            0.0,
            CubeWeightingOptions::default(),
            clean,
            iter_case.threshold,
        )
        .expect("run casa iterbot cube case");

        assert_eq!(
            rust_summary.minor_iterations as u64, iter_case.expected_iterdone,
            "expected Rust iterdone for {}",
            iter_case.suffix
        );
        assert_eq!(
            casa_summary["iterdone"].as_u64(),
            Some(iter_case.expected_iterdone),
            "expected CASA iterdone for {}",
            iter_case.suffix
        );
        assert_eq!(
            rust_summary.major_cycles as u64, iter_case.expected_nmajordone,
            "expected Rust nmajordone for {}",
            iter_case.suffix
        );
        assert_eq!(
            casa_summary["nmajordone"].as_u64(),
            Some(iter_case.expected_nmajordone),
            "expected CASA nmajordone for {}",
            iter_case.suffix
        );
        if let Some(expected_reason) = iter_case.expected_clean_stop_reason {
            assert_eq!(
                rust_summary.clean_stop_reason,
                Some(expected_reason),
                "expected Rust clean stop reason for {}",
                iter_case.suffix
            );
        }

        if !iter_case.expected_model_voxels.is_empty() {
            let rust_model = read_image(&rust_product(&rust_prefix, "model"));
            let casa_model = read_image(&casa_product(&casa_prefix, "model"));
            for (x, y, chan, expected) in iter_case.expected_model_voxels {
                assert_close(
                    sample_channel(&rust_model, *x, *y, *chan),
                    *expected,
                    0.03,
                    0.05,
                    &format!("Rust model[{x},{y},chan={chan}] for {}", iter_case.suffix),
                );
                assert_close(
                    sample_channel(&casa_model, *x, *y, *chan),
                    *expected,
                    0.03,
                    0.05,
                    &format!("CASA model[{x},{y},chan={chan}] for {}", iter_case.suffix),
                );
                assert_close(
                    sample_channel(&rust_model, *x, *y, *chan),
                    sample_channel(&casa_model, *x, *y, *chan),
                    0.03,
                    0.05,
                    &format!("model[{x},{y},chan={chan}] parity for {}", iter_case.suffix),
                );
            }
        }
    }
}

#[test]
fn hogbom_cube_threshold_tolerance_tracks_casa_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let rust_prefix = temp.path().join("rust-refim-point-withline-cube-tol");
    let casa_prefix = temp.path().join("casa-refim-point-withline-cube-tol");
    let clean = CubeCleanControls {
        threshold_jy: 0.50001,
        minor_cycle_length: 5,
        ..CubeCleanControls::default()
    };

    let rust_summary = run_rust_imager_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        1_000_000,
        Deconvolver::Hogbom,
        &[],
        0.0,
        clean,
        CubeWeightingOptions::default(),
    )
    .expect("run rust cube tol case");
    let casa_summary = run_casa_tclean_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &casa_prefix,
        1_000_000,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        clean,
        "0.50001Jy",
    )
    .expect("run casa cube tol case");

    assert_eq!(
        rust_summary.minor_iterations, 151,
        "expected Rust cube tol iterdone"
    );
    assert_eq!(
        casa_summary["iterdone"].as_u64(),
        Some(151),
        "expected CASA cube tol iterdone"
    );
    assert_eq!(
        rust_summary.major_cycles, 4,
        "expected Rust cube tol nmajordone"
    );
    assert_eq!(
        casa_summary["nmajordone"].as_u64(),
        Some(4),
        "expected CASA cube tol nmajordone"
    );
    assert_eq!(
        casa_summary["stopcode"].as_i64(),
        Some(2),
        "expected CASA cube tol stopcode"
    );
}

#[test]
#[ignore = "issue #44: late major-cycle residual-refresh divergence still drives the final two extra nsigma updates on refim_point_withline"]
fn hogbom_cube_nsigma_stopping_tracks_casa_on_refim_point_withline() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let rust_prefix = temp.path().join("rust-refim-point-withline-cube-nsigma");
    let casa_prefix = temp.path().join("casa-refim-point-withline-cube-nsigma");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    let rust_summary = run_rust_imager_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        1_000_000,
        Deconvolver::Hogbom,
        &[],
        0.0,
        clean,
        CubeWeightingOptions::default(),
    )
    .expect("run rust cube nsigma case");
    let casa_summary = run_casa_tclean_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &casa_prefix,
        1_000_000,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        clean,
        "0.000001Jy",
    )
    .expect("run casa cube nsigma case");
    let casa_traces = extract_casa_cube_minor_cycle_traces(&casa_summary).expect("CASA traces");
    let rust_block_count = rust_summary
        .channel_summaries
        .iter()
        .map(|channel| channel.minor_cycle_traces.len())
        .max()
        .unwrap_or(0);
    for block_index in 0..rust_block_count {
        let mut rust_updates = Vec::new();
        let mut rust_max_peak = 0.0f32;
        let mut rust_cycle_threshold = None::<f32>;
        for channel in &rust_summary.channel_summaries {
            if let Some(trace) = channel.minor_cycle_traces.get(block_index) {
                rust_updates.push(trace.reported_updates);
                rust_max_peak = rust_max_peak.max(trace.start_peak_residual_jy_per_beam);
                rust_cycle_threshold = Some(trace.cycle_threshold_jy_per_beam);
            } else {
                rust_updates.push(0);
            }
        }
        let mut casa_updates = Vec::new();
        let mut casa_max_peak = 0.0f32;
        let mut casa_cycle_threshold = None::<f32>;
        for channel in &casa_traces {
            if let Some(trace) = channel.get(block_index) {
                casa_updates.push(trace.reported_updates);
                casa_max_peak = casa_max_peak.max(trace.start_peak_residual_jy_per_beam);
                casa_cycle_threshold = Some(trace.cycle_threshold_jy_per_beam);
            } else {
                casa_updates.push(0);
            }
        }
        eprintln!(
            "cube nsigma block {block_index}: rust max_peak={rust_max_peak:.9} cycle_threshold={:?} updates={rust_updates:?}",
            rust_cycle_threshold
        );
        eprintln!(
            "cube nsigma block {block_index}: casa max_peak={casa_max_peak:.9} cycle_threshold={:?} updates={casa_updates:?}",
            casa_cycle_threshold
        );
    }
    for (channel_index, (rust_channel, casa_channel)) in rust_summary
        .channel_summaries
        .iter()
        .zip(&casa_traces)
        .enumerate()
    {
        for (block_index, (rust_trace, casa_trace)) in rust_channel
            .minor_cycle_traces
            .iter()
            .zip(casa_channel)
            .enumerate()
        {
            if rust_trace.reported_updates != casa_trace.reported_updates {
                eprintln!(
                    "cube nsigma mismatch channel={channel_index} block={block_index}: rust(start_peak={:.9}, end_peak={:.9}, cycle_threshold={:.9}, nsigma_threshold={:.9}, updates={}) casa(start_peak={:.9}, end_peak={:.9}, cycle_threshold={:.9}, updates={})",
                    rust_trace.start_peak_residual_jy_per_beam,
                    rust_trace.end_peak_residual_jy_per_beam,
                    rust_trace.cycle_threshold_jy_per_beam,
                    rust_trace.nsigma_threshold_jy_per_beam,
                    rust_trace.reported_updates,
                    casa_trace.start_peak_residual_jy_per_beam,
                    casa_trace.end_peak_residual_jy_per_beam,
                    casa_trace.cycle_threshold_jy_per_beam,
                    casa_trace.reported_updates,
                );
            }
        }
    }

    assert_eq!(
        rust_summary.minor_iterations, 407,
        "expected Rust cube nsigma iterdone"
    );
    assert_eq!(
        casa_summary["iterdone"].as_u64(),
        Some(407),
        "expected CASA cube nsigma iterdone"
    );
    assert_eq!(
        rust_summary.major_cycles, 11,
        "expected Rust cube nsigma nmajordone"
    );
    assert_eq!(
        casa_summary["nmajordone"].as_u64(),
        Some(11),
        "expected CASA cube nsigma nmajordone"
    );
    assert_eq!(
        rust_summary.clean_stop_reason,
        Some(CleanStopReason::NsigmaThresholdReached),
        "expected Rust cube nsigma clean stop reason"
    );
    assert_eq!(
        casa_summary["stopcode"].as_i64(),
        Some(8),
        "expected CASA cube nsigma stopcode"
    );
}

#[test]
#[ignore = "diagnostic for Wave 8 late-block CASA restart residual parity"]
fn hogbom_cube_nsigma_late_block_residual_planes_track_casa_restart() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let rust_prefix = temp.path().join("rust-refim-point-withline-cube-nsigma");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    for &(channel_index, block_index) in &[(7usize, 8usize), (5usize, 9usize)] {
        let capture_dir = temp.path().join(format!(
            "capture-channel-{channel_index}-block-{block_index}"
        ));
        unsafe {
            std::env::set_var("CASA_RS_CUBE_CAPTURE_CHANNEL", channel_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_BLOCK", block_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_DIR", &capture_dir);
        }
        let _ = run_rust_imager_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &rust_prefix,
            false,
            1_000_000,
            Deconvolver::Hogbom,
            &[],
            0.0,
            clean,
            CubeWeightingOptions::default(),
        )
        .expect("rerun rust cube nsigma capture case");
        unsafe {
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_CHANNEL");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_BLOCK");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_DIR");
        }
        let captured = read_captured_cube_minor_cycle_plane(&capture_dir);

        let casa_prefix = temp.path().join(format!(
            "casa-refim-point-withline-cube-preblock-{channel_index}-{block_index}"
        ));
        let mut casa_summary = None::<Value>;
        for step in 0..block_index {
            casa_summary = Some(
                run_casa_tclean_cube_task_default_case_with_clean_controls(
                    case,
                    &staged_ms_path,
                    &casa_prefix,
                    clean.minor_cycle_length,
                    "hogbom",
                    &[],
                    0.0,
                    CubeWeightingOptions::default(),
                    clean,
                    "0.000001Jy",
                )
                .expect("run casa cube nsigma restart step"),
            );
            let completed = casa_summary
                .as_ref()
                .and_then(|summary| summary["iterdone"].as_u64())
                .map(|value| value as usize)
                .unwrap_or(0);
            let step_channel_trace = casa_summary
                .as_ref()
                .and_then(|summary| extract_casa_cube_minor_cycle_traces(summary).ok())
                .and_then(|channels| channels.get(channel_index).cloned())
                .and_then(|traces| traces.last().cloned());
            let step_casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
            let step_casa_residual_plane =
                extract_channel_plane(&step_casa_residual, channel_index);
            let step_residual_stats =
                plane_difference_stats(&captured.residual, &step_casa_residual_plane);
            let step_casa_model = read_image(&casa_product(&casa_prefix, "model"));
            let step_casa_model_plane = extract_channel_plane(&step_casa_model, channel_index);
            let step_model_stats = plane_difference_stats(&captured.model, &step_casa_model_plane);
            eprintln!(
                "cube nsigma restart step channel={channel_index} block={block_index} step={step}: requested_niter={} completed_iterdone={completed} channel_start_iter={:?} channel_updates={:?} residual_rms={:.9e} residual_max_abs={:.9e} residual_corr={:.9e} model_rms={:.9e} model_max_abs={:.9e} model_corr={:.9e}",
                clean.minor_cycle_length,
                step_channel_trace
                    .as_ref()
                    .map(|trace| trace.start_reported_iteration),
                step_channel_trace
                    .as_ref()
                    .map(|trace| trace.reported_updates),
                step_residual_stats.rms,
                step_residual_stats.max_abs,
                step_residual_stats.correlation,
                step_model_stats.rms,
                step_model_stats.max_abs,
                step_model_stats.correlation,
            );
        }
        let casa_summary = casa_summary.expect("casa restart boundary summary");

        let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
        let casa_plane = extract_channel_plane(&casa_residual, channel_index);
        let stats = plane_difference_stats(&captured.residual, &casa_plane);
        let casa_model = read_image(&casa_product(&casa_prefix, "model"));
        let casa_model_plane = extract_channel_plane(&casa_model, channel_index);
        let model_stats = plane_difference_stats(&captured.model, &casa_model_plane);
        eprintln!(
            "cube nsigma pre-block residual channel={channel_index} block={block_index}: casa_iterdone={:?} rms={:.9e} max_abs={:.9e} correlation={:.9e}",
            casa_summary["iterdone"].as_u64(),
            stats.rms,
            stats.max_abs,
            stats.correlation,
        );
        eprintln!(
            "cube nsigma pre-block model channel={channel_index} block={block_index}: rms={:.9e} max_abs={:.9e} correlation={:.9e}",
            model_stats.rms, model_stats.max_abs, model_stats.correlation,
        );
    }
}

#[test]
#[ignore = "diagnostic for Wave 8 exact CASA inputres/inputmod late-block parity"]
fn hogbom_cube_nsigma_late_block_inputs_track_casa_minor_cycle_snapshots() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    let mut cube_axis = casa_ms::CubeAxisConfig::default();
    cube_axis.specmode = casa_ms::CubeSpecMode::Cube;
    let config = CliConfig {
        ms: staged_ms_path.clone(),
        imagename: temp
            .path()
            .join("rust-trace-refim-point-withline-cube-snapshots"),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: Some(case.spw.to_string()),
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Cube,
        cube_axis,
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 1_000_000,
        gain: clean.gain,
        threshold_jy: clean.threshold_jy,
        nsigma: clean.nsigma,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: clean.minor_cycle_length,
        cyclefactor: clean.cyclefactor,
        min_psf_fraction: clean.min_psf_fraction,
        max_psf_fraction: clean.max_psf_fraction,
        hogbom_iteration_mode: clean.hogbom_iteration_mode,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only: false,
        write_preview_pngs: false,
    };

    for &(channel_index, block_index) in &[(7usize, 8usize), (5usize, 9usize), (9usize, 9usize)] {
        let rust_prefix = temp.path().join(format!(
            "rust-refim-point-withline-cube-nsigma-{channel_index}-{block_index}"
        ));
        let casa_prefix = temp.path().join(format!(
            "casa-refim-point-withline-cube-nsigma-snapshots-{channel_index}-{block_index}"
        ));
        let capture_dir = temp.path().join(format!(
            "capture-exact-channel-{channel_index}-block-{block_index}"
        ));
        unsafe {
            std::env::set_var("CASA_RS_CUBE_CAPTURE_CHANNEL", channel_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_BLOCK", block_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_DIR", &capture_dir);
        }
        let _ = run_rust_imager_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &rust_prefix,
            false,
            1_000_000,
            Deconvolver::Hogbom,
            &[],
            0.0,
            clean,
            CubeWeightingOptions::default(),
        )
        .expect("rerun rust cube nsigma capture case");
        unsafe {
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_CHANNEL");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_BLOCK");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_DIR");
            std::env::set_var("SAVE_ALL_RESIMS", "true");
        }
        let casa_summary = run_casa_tclean_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &casa_prefix,
            1_000_000,
            "hogbom",
            &[],
            0.0,
            CubeWeightingOptions::default(),
            clean,
            "0.000001Jy",
        )
        .expect("run casa cube nsigma snapshot case");
        unsafe {
            std::env::remove_var("SAVE_ALL_RESIMS");
        }
        let casa_traces = extract_casa_cube_minor_cycle_traces(&casa_summary).expect("CASA traces");

        let captured = read_captured_cube_minor_cycle_plane(&capture_dir);
        let cycle_number = block_index + 1;
        let casa_psf_plane =
            read_channel_plane_in_casa(&casa_product(&casa_prefix, "psf"), channel_index)
                .expect("read CASA psf plane");
        let casa_residual_plane = read_channel_plane_in_casa(
            &PathBuf::from(format!("{}.inputres{cycle_number}", casa_prefix.display())),
            channel_index,
        )
        .expect("read CASA inputres plane");
        let casa_model_plane = read_channel_plane_in_casa(
            &PathBuf::from(format!("{}.inputmod{cycle_number}", casa_prefix.display())),
            channel_index,
        )
        .expect("read CASA inputmod plane");
        let casa_model_cube_path =
            PathBuf::from(format!("{}.inputmod{cycle_number}", casa_prefix.display()));
        let casa_model_cube = if casa_model_cube_path.exists() {
            read_cube_planes_in_casa(&casa_model_cube_path).expect("read CASA inputmod cube")
        } else {
            (0..case.channel_count)
                .map(|_| Array2::<f32>::zeros((case.imsize, case.imsize)))
                .collect::<Vec<_>>()
        };
        let residual_stats = plane_difference_stats(&captured.residual, &casa_residual_plane);
        let model_stats = plane_difference_stats(&captured.model, &casa_model_plane);
        let rust_trace = trace_cube_channel_residual_refresh_from_config_with_model_cube(
            &config,
            channel_index,
            &casa_model_cube,
        )
        .expect("trace rust residual refresh from CASA inputmod");
        let rust_trace_model_channel_lambda =
            trace_cube_channel_residual_refresh_from_config_with_model_cube_model_channel_lambda(
                &config,
                channel_index,
                &casa_model_cube,
            )
            .expect("trace rust residual refresh from CASA inputmod with model-channel lambda");
        let same_model_stats =
            plane_difference_stats(&rust_trace.residual_image, &casa_residual_plane);
        let model_channel_lambda_stats = plane_difference_stats(
            &rust_trace_model_channel_lambda.residual_image,
            &casa_residual_plane,
        );
        let casa_trace = casa_traces
            .get(channel_index)
            .and_then(|channel| channel.get(block_index))
            .expect("CASA trace for channel/block");
        let casa_threshold = casa_trace
            .cycle_threshold_jy_per_beam
            .max(clean.threshold_jy);
        let casa_residual_flat = casa_residual_plane.iter().copied().collect::<Vec<_>>();
        let casa_psf_flat = casa_psf_plane.iter().copied().collect::<Vec<_>>();
        let rust_replay = replay_rust_hogbom_minor_cycle_2d(
            &casa_psf_flat,
            &casa_residual_flat,
            [case.imsize, case.imsize],
            clean.gain,
            casa_threshold,
            clean.minor_cycle_length,
        );
        let cpp_replay = match cpp_hogbom_clean_minor_cycle_2d(
            &casa_psf_flat,
            &casa_residual_flat,
            [case.imsize, case.imsize],
            clean.gain,
            casa_threshold,
            clean.minor_cycle_length,
        ) {
            Ok(result) => Some(result),
            Err(error) if error == "casacore C++ backend unavailable" => None,
            Err(error) => panic!("run CASA snapshot hclean interop: {error}"),
        };
        eprintln!(
            "cube nsigma exact input snapshot channel={channel_index} block={block_index}: residual_rms={:.9e} residual_max_abs={:.9e} residual_corr={:.9e} model_rms={:.9e} model_max_abs={:.9e} model_corr={:.9e} same_model_residual_rms={:.9e} same_model_residual_max_abs={:.9e} same_model_residual_corr={:.9e} model_channel_lambda_residual_rms={:.9e} model_channel_lambda_residual_max_abs={:.9e} model_channel_lambda_residual_corr={:.9e} casa_start_peak={:.9e} casa_end_peak={:.9e} casa_cycle_threshold={:.9e} rust_replay_iterdone={} cpp_replay_iterdone={:?}",
            residual_stats.rms,
            residual_stats.max_abs,
            residual_stats.correlation,
            model_stats.rms,
            model_stats.max_abs,
            model_stats.correlation,
            same_model_stats.rms,
            same_model_stats.max_abs,
            same_model_stats.correlation,
            model_channel_lambda_stats.rms,
            model_channel_lambda_stats.max_abs,
            model_channel_lambda_stats.correlation,
            casa_trace.start_peak_residual_jy_per_beam,
            casa_trace.end_peak_residual_jy_per_beam,
            casa_trace.cycle_threshold_jy_per_beam,
            rust_replay.iterdone,
            cpp_replay.as_ref().map(|result| result.iterdone),
        );
    }
}

#[test]
#[ignore = "diagnostic for Wave 8 same-model residual refresh at CASA restart boundaries"]
fn hogbom_cube_nsigma_same_model_residual_refresh_tracks_casa_restart() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    let mut cube_axis = casa_ms::CubeAxisConfig::default();
    cube_axis.specmode = casa_ms::CubeSpecMode::Cube;
    let config = CliConfig {
        ms: staged_ms_path.clone(),
        imagename: temp
            .path()
            .join("rust-trace-refim-point-withline-cube-nsigma"),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: Some(case.spw.to_string()),
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Cube,
        cube_axis,
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 1_000_000,
        gain: clean.gain,
        threshold_jy: clean.threshold_jy,
        nsigma: clean.nsigma,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: clean.minor_cycle_length,
        cyclefactor: clean.cyclefactor,
        min_psf_fraction: clean.min_psf_fraction,
        max_psf_fraction: clean.max_psf_fraction,
        hogbom_iteration_mode: clean.hogbom_iteration_mode,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only: false,
        write_preview_pngs: false,
    };

    for &(channel_index, block_index) in &[(7usize, 8usize), (5usize, 9usize), (9usize, 9usize)] {
        let casa_prefix = temp.path().join(format!(
            "casa-refim-point-withline-cube-same-model-{channel_index}-{block_index}"
        ));
        for _ in 0..block_index {
            run_casa_tclean_cube_task_default_case_with_clean_controls(
                case,
                &staged_ms_path,
                &casa_prefix,
                clean.minor_cycle_length,
                "hogbom",
                &[],
                0.0,
                CubeWeightingOptions::default(),
                clean,
                "0.000001Jy",
            )
            .expect("run casa cube nsigma restart step");
        }

        let casa_residual_plane =
            read_channel_plane_in_casa(&casa_product(&casa_prefix, "residual"), channel_index)
                .expect("read CASA residual plane");
        let casa_model_plane =
            read_channel_plane_in_casa(&casa_product(&casa_prefix, "model"), channel_index)
                .expect("read CASA model plane");
        let rust_trace = trace_cube_channel_residual_refresh_from_config(
            &config,
            channel_index,
            &casa_model_plane,
        )
        .expect("trace rust residual refresh from casa model");
        let stats = plane_difference_stats(&rust_trace.residual_image, &casa_residual_plane);
        eprintln!(
            "cube nsigma same-model residual channel={channel_index} block={block_index}: rms={:.9e} max_abs={:.9e} correlation={:.9e} normalization_sumwt={:.9e} reported_sumwt={:.9e} psf_peak={:.9e}",
            stats.rms,
            stats.max_abs,
            stats.correlation,
            rust_trace.normalization_sumwt,
            rust_trace.reported_sumwt,
            rust_trace.psf_peak,
        );
    }
}

#[test]
#[ignore = "diagnostic for Wave 8 internal-model residual refresh consistency"]
fn hogbom_cube_nsigma_internal_model_residual_refresh_matches_captured_state() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let rust_prefix = temp.path().join("rust-refim-point-withline-internal-model");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    let mut cube_axis = casa_ms::CubeAxisConfig::default();
    cube_axis.specmode = casa_ms::CubeSpecMode::Cube;
    let config = CliConfig {
        ms: staged_ms_path.clone(),
        imagename: rust_prefix.clone(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: Some(case.spw.to_string()),
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Cube,
        cube_axis,
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 1_000_000,
        gain: clean.gain,
        threshold_jy: clean.threshold_jy,
        nsigma: clean.nsigma,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: clean.minor_cycle_length,
        cyclefactor: clean.cyclefactor,
        min_psf_fraction: clean.min_psf_fraction,
        max_psf_fraction: clean.max_psf_fraction,
        hogbom_iteration_mode: clean.hogbom_iteration_mode,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only: false,
        write_preview_pngs: false,
    };

    for &(channel_index, block_index) in &[(7usize, 8usize), (5usize, 9usize)] {
        let capture_dir = temp.path().join(format!(
            "capture-internal-channel-{channel_index}-block-{block_index}"
        ));
        unsafe {
            std::env::set_var("CASA_RS_CUBE_CAPTURE_CHANNEL", channel_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_BLOCK", block_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_DIR", &capture_dir);
        }
        let _ = run_rust_imager_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &rust_prefix,
            false,
            1_000_000,
            Deconvolver::Hogbom,
            &[],
            0.0,
            clean,
            CubeWeightingOptions::default(),
        )
        .expect("rerun rust cube nsigma capture case");
        unsafe {
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_CHANNEL");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_BLOCK");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_DIR");
        }

        let captured = read_captured_cube_minor_cycle_plane(&capture_dir);
        let rust_trace = trace_cube_channel_residual_refresh_from_config(
            &config,
            channel_index,
            &captured.model,
        )
        .expect("trace rust residual refresh from internal model");
        let stats = plane_difference_stats(&rust_trace.residual_image, &captured.residual);
        let rust_model_image = read_image(&rust_product(&rust_prefix, "model"));
        let rust_model_plane = extract_channel_plane(&rust_model_image, channel_index);
        let file_model_stats = plane_difference_stats(&rust_model_plane, &captured.model);
        let rust_file_trace = trace_cube_channel_residual_refresh_from_config(
            &config,
            channel_index,
            &rust_model_plane,
        )
        .expect("trace rust residual refresh from rust file model");
        let file_residual_stats =
            plane_difference_stats(&rust_file_trace.residual_image, &captured.residual);
        eprintln!(
            "cube nsigma internal-model residual channel={channel_index} block={block_index}: rms={:.9e} max_abs={:.9e} correlation={:.9e}",
            stats.rms, stats.max_abs, stats.correlation,
        );
        eprintln!(
            "cube nsigma rust-file model channel={channel_index} block={block_index}: model_rms={:.9e} model_max_abs={:.9e} model_corr={:.9e} residual_rms={:.9e} residual_max_abs={:.9e} residual_corr={:.9e}",
            file_model_stats.rms,
            file_model_stats.max_abs,
            file_model_stats.correlation,
            file_residual_stats.rms,
            file_residual_stats.max_abs,
            file_residual_stats.correlation,
        );
    }
}

#[test]
#[ignore = "diagnostic for Wave 9 full-cube restart consistency on refim_point_withline"]
fn hogbom_cube_nsigma_full_cube_model_context_explains_late_restart_gap() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    let mut cube_axis = casa_ms::CubeAxisConfig::default();
    cube_axis.specmode = casa_ms::CubeSpecMode::Cube;
    let config = CliConfig {
        ms: staged_ms_path.clone(),
        imagename: temp
            .path()
            .join("rust-trace-refim-point-withline-cube-full-context"),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: Some(case.spw.to_string()),
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Cube,
        cube_axis,
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 1_000_000,
        gain: clean.gain,
        threshold_jy: clean.threshold_jy,
        nsigma: clean.nsigma,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: clean.minor_cycle_length,
        cyclefactor: clean.cyclefactor,
        min_psf_fraction: clean.min_psf_fraction,
        max_psf_fraction: clean.max_psf_fraction,
        hogbom_iteration_mode: clean.hogbom_iteration_mode,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only: false,
        write_preview_pngs: false,
    };

    let channel_index = 9usize;
    unsafe {
        std::env::set_var("SAVE_ALL_RESIMS", "true");
    }
    let casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-full-context");
    let _ = run_casa_tclean_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &casa_prefix,
        1_000_000,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        clean,
        "0.000001Jy",
    )
    .expect("run casa cube nsigma snapshot case");
    unsafe {
        std::env::remove_var("SAVE_ALL_RESIMS");
    }
    for &block_index in &[0usize, 8usize, 9usize] {
        let cycle_number = block_index + 1;
        let capture_dir = temp.path().join(format!(
            "capture-full-context-channel-{channel_index}-block-{block_index}"
        ));
        unsafe {
            std::env::set_var("CASA_RS_CUBE_CAPTURE_CHANNEL", channel_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_BLOCK", block_index.to_string());
            std::env::set_var("CASA_RS_CUBE_CAPTURE_DIR", &capture_dir);
        }
        let rust_prefix = temp.path().join(format!(
            "rust-refim-point-withline-cube-full-context-{block_index}"
        ));
        let _ = run_rust_imager_cube_task_default_case_with_clean_controls(
            case,
            &staged_ms_path,
            &rust_prefix,
            false,
            1_000_000,
            Deconvolver::Hogbom,
            &[],
            0.0,
            clean,
            CubeWeightingOptions::default(),
        )
        .expect("rerun rust cube nsigma capture case");
        unsafe {
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_CHANNEL");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_BLOCK");
            std::env::remove_var("CASA_RS_CUBE_CAPTURE_DIR");
        }

        let captured = read_captured_cube_minor_cycle_plane(&capture_dir);
        let captured_model_cube = read_captured_cube_model_cube(&capture_dir);
        let captured_trace = trace_cube_channel_residual_refresh_from_config_with_model_cube(
            &config,
            channel_index,
            &captured_model_cube,
        )
        .expect("trace residual refresh from captured full model cube");
        let captured_trace_stats =
            plane_difference_stats(&captured_trace.residual_image, &captured.residual);

        let casa_model_cube_path =
            PathBuf::from(format!("{}.inputmod{cycle_number}", casa_prefix.display()));
        let casa_model_cube = if casa_model_cube_path.exists() {
            read_cube_planes_in_casa(&casa_model_cube_path).expect("read CASA inputmod cube")
        } else {
            (0..case.channel_count)
                .map(|_| Array2::<f32>::zeros((case.imsize, case.imsize)))
                .collect::<Vec<_>>()
        };
        let casa_residual_plane = read_channel_plane_in_casa(
            &PathBuf::from(format!("{}.inputres{cycle_number}", casa_prefix.display())),
            channel_index,
        )
        .expect("read CASA inputres plane");
        let casa_trace = trace_cube_channel_residual_refresh_from_config_with_model_cube(
            &config,
            channel_index,
            &casa_model_cube,
        )
        .expect("trace residual refresh from CASA full model cube");
        let casa_trace_vs_captured =
            plane_difference_stats(&casa_trace.residual_image, &captured.residual);
        let casa_trace_vs_casa =
            plane_difference_stats(&casa_trace.residual_image, &casa_residual_plane);

        for compared_channel in [8usize, 9usize, 10usize] {
            let stats = plane_difference_stats(
                &captured_model_cube[compared_channel],
                &casa_model_cube[compared_channel],
            );
            eprintln!(
                "cube nsigma full-context block={block_index} model channel={compared_channel}: rms={:.9e} max_abs={:.9e} correlation={:.9e}",
                stats.rms, stats.max_abs, stats.correlation,
            );
        }
        eprintln!(
            "cube nsigma full-context residual channel={channel_index} block={block_index}: captured_full_trace_rms={:.9e} captured_full_trace_max_abs={:.9e} captured_full_trace_corr={:.9e} casa_full_trace_vs_captured_rms={:.9e} casa_full_trace_vs_captured_max_abs={:.9e} casa_full_trace_vs_captured_corr={:.9e} casa_full_trace_vs_casa_rms={:.9e} casa_full_trace_vs_casa_max_abs={:.9e} casa_full_trace_vs_casa_corr={:.9e}",
            captured_trace_stats.rms,
            captured_trace_stats.max_abs,
            captured_trace_stats.correlation,
            casa_trace_vs_captured.rms,
            casa_trace_vs_captured.max_abs,
            casa_trace_vs_captured.correlation,
            casa_trace_vs_casa.rms,
            casa_trace_vs_casa.max_abs,
            casa_trace_vs_casa.correlation,
        );
    }
}

#[test]
#[ignore = "diagnostic for Wave 9 channel-9 block-0 nearest-vs-linear dirty parity"]
fn hogbom_cube_nsigma_block0_channel9_nearest_vs_linear_dirty_against_casa() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };

    unsafe {
        std::env::set_var("SAVE_ALL_RESIMS", "true");
    }
    let casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-block0-nearest-linear");
    let _ = run_casa_tclean_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &casa_prefix,
        1_000_000,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        clean,
        "0.000001Jy",
    )
    .expect("run casa cube nsigma snapshot case");
    unsafe {
        std::env::remove_var("SAVE_ALL_RESIMS");
    }

    let channel_index = 9usize;
    let casa_residual_plane = read_channel_plane_in_casa(
        &PathBuf::from(format!("{}.inputres1", casa_prefix.display())),
        channel_index,
    )
    .expect("read CASA inputres1 plane");

    let make_config = |suffix: &str, interpolation| {
        let mut cube_axis = casa_ms::CubeAxisConfig::default();
        cube_axis.specmode = casa_ms::CubeSpecMode::Cube;
        cube_axis.interpolation = interpolation;
        CliConfig {
            ms: staged_ms_path.clone(),
            imagename: temp
                .path()
                .join(format!("rust-refim-point-withline-cube-block0-{suffix}")),
            imsize: case.imsize,
            cell_arcsec: case.cell_arcsec,
            field_ids: Some(case.field_ids.to_vec()),
            phasecenter_field: case.phasecenter_field,
            phasecenter: None,
            ddid: None,
            spw: Some(case.spw),
            spw_selector: Some(case.spw.to_string()),
            channel_start: case.channel_start_option(),
            channel_count: case.channel_count_option(),
            datacolumn: Some("DATA".to_string()),
            save_model: casars_imager::SaveModelMode::None,
            correlation: case.correlation.map(str::to_string),
            spectral_mode: casars_imager::SpectralMode::Cube,
            cube_axis,
            weighting: case.weighting,
            per_channel_weight_density: false,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: clean.gain,
            threshold_jy: clean.threshold_jy,
            nsigma: clean.nsigma,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: clean.minor_cycle_length,
            cyclefactor: clean.cyclefactor,
            min_psf_fraction: clean.min_psf_fraction,
            max_psf_fraction: clean.max_psf_fraction,
            hogbom_iteration_mode: clean.hogbom_iteration_mode,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    };

    let linear_config = make_config("linear", casa_ms::CubeInterpolation::Linear);
    let nearest_config = make_config("nearest", casa_ms::CubeInterpolation::Nearest);
    let _ = run_from_config(&linear_config).expect("run rust linear dirty cube");
    let _ = run_from_config(&nearest_config).expect("run rust nearest dirty cube");

    let linear_residual = extract_channel_plane(
        &read_image(&rust_product(&linear_config.imagename, "residual")),
        channel_index,
    );
    let nearest_residual = extract_channel_plane(
        &read_image(&rust_product(&nearest_config.imagename, "residual")),
        channel_index,
    );
    let linear_sumwt = sample_scalar_channel(
        &read_image(&rust_product(&linear_config.imagename, "sumwt")),
        channel_index,
    );
    let nearest_sumwt = sample_scalar_channel(
        &read_image(&rust_product(&nearest_config.imagename, "sumwt")),
        channel_index,
    );
    let casa_sumwt = sample_scalar_channel(
        &read_image(&casa_product(&casa_prefix, "sumwt")),
        channel_index,
    );
    let linear_stats = plane_difference_stats(&linear_residual, &casa_residual_plane);
    let nearest_stats = plane_difference_stats(&nearest_residual, &casa_residual_plane);
    eprintln!(
        "cube nsigma block0 channel9 dirty parity: linear_rms={:.9e} linear_max_abs={:.9e} linear_corr={:.9e} nearest_rms={:.9e} nearest_max_abs={:.9e} nearest_corr={:.9e} linear_sumwt={:.9e} nearest_sumwt={:.9e} casa_sumwt={:.9e}",
        linear_stats.rms,
        linear_stats.max_abs,
        linear_stats.correlation,
        nearest_stats.rms,
        nearest_stats.max_abs,
        nearest_stats.correlation,
        linear_sumwt,
        nearest_sumwt,
        casa_sumwt,
    );
}

#[test]
#[ignore = "diagnostic for Wave 9 block-0 cube dirty isolation with CASA mstransform output"]
fn hogbom_cube_nsigma_block0_channel9_casa_regridded_ms_isolates_spectral_seam() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let casa_regridded_ms = temp.path().join("refim_point_withline.mstransform.ms");
    run_casa_mstransform_default_cube_case(&staged_ms_path, &casa_regridded_ms, case, "linear")
        .expect("run casa mstransform cube regrid");

    let clean = CubeCleanControls {
        gain: 0.5,
        threshold_jy: 0.000001,
        nsigma: 10.0,
        minor_cycle_length: 10,
        ..CubeCleanControls::default()
    };
    let casa_prefix = temp
        .path()
        .join("casa-refim-point-withline-cube-block0-casa-regridded");
    unsafe {
        std::env::set_var("SAVE_ALL_RESIMS", "true");
    }
    let _ = run_casa_tclean_cube_task_default_case_with_clean_controls(
        case,
        &staged_ms_path,
        &casa_prefix,
        1_000_000,
        "hogbom",
        &[],
        0.0,
        CubeWeightingOptions::default(),
        clean,
        "0.000001Jy",
    )
    .expect("run casa cube nsigma snapshot case");
    unsafe {
        std::env::remove_var("SAVE_ALL_RESIMS");
    }

    let channel_index = 9usize;
    let casa_residual_plane = read_channel_plane_in_casa(
        &PathBuf::from(format!("{}.inputres1", casa_prefix.display())),
        channel_index,
    )
    .expect("read CASA inputres1 plane");

    let mut cubedata_axis = casa_ms::CubeAxisConfig::default();
    cubedata_axis.specmode = casa_ms::CubeSpecMode::Cubedata;
    let cubedata_config = CliConfig {
        ms: casa_regridded_ms.clone(),
        imagename: temp
            .path()
            .join("rust-refim-point-withline-cubedata-from-casa-regrid"),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: Some(case.spw.to_string()),
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Cubedata,
        cube_axis: cubedata_axis,
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 0,
        gain: clean.gain,
        threshold_jy: clean.threshold_jy,
        nsigma: clean.nsigma,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: clean.minor_cycle_length,
        cyclefactor: clean.cyclefactor,
        min_psf_fraction: clean.min_psf_fraction,
        max_psf_fraction: clean.max_psf_fraction,
        hogbom_iteration_mode: clean.hogbom_iteration_mode,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only: true,
        write_preview_pngs: false,
    };
    let _ = run_from_config(&cubedata_config).expect("run rust cubedata dirty cube");

    let cubedata_residual = extract_channel_plane(
        &read_image(&rust_product(&cubedata_config.imagename, "residual")),
        channel_index,
    );
    let cubedata_stats = plane_difference_stats(&cubedata_residual, &casa_residual_plane);
    let casa_regrid_spw_selector = case.spw.to_string();
    let casa_regrid_prefix = temp
        .path()
        .join("casa-refim-point-withline-cubedata-from-casa-regrid");
    let casa_regrid_options = CubeCaseOptions {
        spw_selector: &casa_regrid_spw_selector,
        nchan: case.channel_count,
        start: Some(CubeAxisStep::Channel(case.channel_start as i32)),
        width: Some(CubeAxisStep::Channel(1)),
        outframe: "LSRK",
        interpolation: "nearest",
        veltype: "radio",
        restfreq: "1.25GHz",
    };
    let _ = run_casa_tclean_cubedata_case_with_options(
        case,
        &casa_regridded_ms,
        &casa_regrid_prefix,
        0,
        "hogbom",
        &[],
        0.0,
        false,
        casa_regrid_options,
        "0Jy",
    )
    .expect("run CASA cubedata dirty cube on regridded MS");
    let casa_regrid_residual = extract_channel_plane(
        &read_image(&casa_product(&casa_regrid_prefix, "residual")),
        channel_index,
    );
    let casa_regrid_stats = plane_difference_stats(&casa_regrid_residual, &casa_residual_plane);
    let trace = build_prepare_plane_trace_from_config(&cubedata_config)
        .expect("build prepared cubedata trace from CASA regridded MS");
    let output_frequency_hz = trace.output_channel_frequencies_hz[channel_index];
    let lambda_scale = output_frequency_hz / 299_792_458.0;
    let channel_samples = trace
        .samples
        .iter()
        .filter(|sample| sample.output_channel_index == Some(channel_index))
        .collect::<Vec<_>>();
    let cell_size_rad = case.cell_arcsec / 206_264.806_247;
    let padded_len = |image_len: usize| {
        let padded = (1.2 * image_len as f64 - 0.5).floor() as usize;
        let padded = padded.max(image_len);
        if padded % 2 == 0 { padded } else { padded + 1 }
    };
    let grid_shape = [padded_len(case.imsize), padded_len(case.imsize)];
    let make_cpp_dirty = |grid_shape: [usize; 2]| {
        cpp_convolve_gridder_make_dirty_image_2d(
            grid_shape,
            [case.imsize, case.imsize],
            [
                grid_shape[0] as f64 * cell_size_rad,
                grid_shape[1] as f64 * cell_size_rad,
            ],
            [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0],
            &channel_samples
                .iter()
                .map(|sample| sample.imaging_uvw_m[0] * lambda_scale)
                .collect::<Vec<_>>(),
            &channel_samples
                .iter()
                .map(|sample| sample.imaging_uvw_m[1] * lambda_scale)
                .collect::<Vec<_>>(),
            &channel_samples
                .iter()
                .map(|sample| sample.visibility_re)
                .collect::<Vec<_>>(),
            &channel_samples
                .iter()
                .map(|sample| sample.visibility_im)
                .collect::<Vec<_>>(),
            &channel_samples
                .iter()
                .map(|sample| sample.weight)
                .collect::<Vec<_>>(),
            &channel_samples
                .iter()
                .map(|sample| sample.gridable)
                .collect::<Vec<_>>(),
        )
    };
    let cpp_dirty = match make_cpp_dirty(grid_shape) {
        Ok(image) => image,
        Err(error) if error == "casacore C++ backend unavailable" => {
            eprintln!(
                "cube nsigma block0 channel9 CASA-regridded cubedata parity: casacore C++ gridder shim unavailable"
            );
            return;
        }
        Err(error) => panic!("run casacore dirty-image shim: {error}"),
    };
    let cpp_dirty_plane = Array2::from_shape_vec((case.imsize, case.imsize), cpp_dirty.pixels)
        .expect("shape C++ dirty plane");
    let cpp_stats = plane_difference_stats(&cpp_dirty_plane, &casa_residual_plane);
    let cpp_no_padding = match make_cpp_dirty([case.imsize, case.imsize]) {
        Ok(image) => image,
        Err(error) if error == "casacore C++ backend unavailable" => {
            eprintln!(
                "cube nsigma block0 channel9 CASA-regridded cubedata parity: casacore C++ gridder shim unavailable"
            );
            return;
        }
        Err(error) => panic!("run casacore dirty-image shim: {error}"),
    };
    let cpp_no_padding_plane =
        Array2::from_shape_vec((case.imsize, case.imsize), cpp_no_padding.pixels)
            .expect("shape C++ no-padding dirty plane");
    let cpp_no_padding_stats = plane_difference_stats(&cpp_no_padding_plane, &casa_residual_plane);
    let rust_peak_index =
        peak_location_in_plane(&cubedata_residual).expect("rust cubedata residual peak");
    let casa_peak_index = peak_location_in_plane(&casa_residual_plane).expect("casa residual peak");
    let rust_peak_value = cubedata_residual[(rust_peak_index.0, rust_peak_index.1)];
    let casa_peak_value = casa_residual_plane[(casa_peak_index.0, casa_peak_index.1)];
    let slope = |lhs: &Array2<f32>, rhs: &Array2<f32>| {
        let (mut num, mut den) = (0.0f64, 0.0f64);
        for (&lhs_value, &rhs_value) in lhs.iter().zip(rhs.iter()) {
            num += f64::from(lhs_value) * f64::from(rhs_value);
            den += f64::from(rhs_value) * f64::from(rhs_value);
        }
        if den > 0.0 { num / den } else { f64::NAN }
    };
    let rust_scale_vs_casa = slope(&cubedata_residual, &casa_residual_plane);
    let casa_regrid_scale_vs_casa = slope(&casa_regrid_residual, &casa_residual_plane);
    let cpp_scale_vs_casa = slope(&cpp_dirty_plane, &casa_residual_plane);
    let rust_psf_plane = extract_channel_plane(
        &read_image(&rust_product(&cubedata_config.imagename, "psf")),
        channel_index,
    );
    let casa_psf_plane = extract_channel_plane(
        &read_image(&casa_product(&casa_prefix, "psf")),
        channel_index,
    );
    let rust_psf_center = rust_psf_plane[(case.center(), case.center())];
    let casa_psf_center = casa_psf_plane[(case.center(), case.center())];
    eprintln!(
        "cube nsigma block0 channel9 CASA-regridded cubedata parity: rust_rms={:.9e} rust_max_abs={:.9e} rust_corr={:.9e} casa_regrid_rms={:.9e} casa_regrid_max_abs={:.9e} casa_regrid_corr={:.9e} cpp_rms={:.9e} cpp_max_abs={:.9e} cpp_corr={:.9e} cpp_nopad_rms={:.9e} cpp_nopad_max_abs={:.9e} cpp_nopad_corr={:.9e} rust_peak={} casa_peak={} rust_scale_vs_casa={:.9e} casa_regrid_scale_vs_casa={:.9e} cpp_scale_vs_casa={:.9e} rust_psf_center={} casa_psf_center={}",
        cubedata_stats.rms,
        cubedata_stats.max_abs,
        cubedata_stats.correlation,
        casa_regrid_stats.rms,
        casa_regrid_stats.max_abs,
        casa_regrid_stats.correlation,
        cpp_stats.rms,
        cpp_stats.max_abs,
        cpp_stats.correlation,
        cpp_no_padding_stats.rms,
        cpp_no_padding_stats.max_abs,
        cpp_no_padding_stats.correlation,
        rust_peak_value,
        casa_peak_value,
        rust_scale_vs_casa,
        casa_regrid_scale_vs_casa,
        cpp_scale_vs_casa,
        rust_psf_center,
        casa_psf_center,
    );
}

#[test]
#[ignore = "diagnostic for Wave 9 block-0 cube dirty parity split by correlation product"]
fn hogbom_cube_nsigma_block0_channel9_single_pol_dirty_parity() {
    let base_case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(base_case) {
        eprintln!("{}", skip_reason_for_case(base_case));
        return;
    }

    let ms_path = dataset_path(base_case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "refim_point_withline.ms")
        .expect("stage refim_point_withline");
    let channel_index = 9usize;

    for correlation in ["RR", "LL", "XX", "YY"] {
        let case = ParityCase {
            correlation: Some(correlation),
            ..base_case
        };
        let rust_prefix = temp
            .path()
            .join(format!("rust-refim-point-withline-{correlation}-dirty"));
        let casa_prefix = temp
            .path()
            .join(format!("casa-refim-point-withline-{correlation}-dirty"));
        let rust_summary = run_rust_imager_cube_dirty(case, &staged_ms_path, &rust_prefix);
        if let Err(error) = rust_summary {
            if error.contains("requested correlation") && error.contains("is not present") {
                eprintln!(
                    "cube nsigma block0 channel9 single-pol dirty parity {correlation}: skipped ({error})"
                );
                continue;
            }
            panic!("run rust cube dirty: {error}");
        }
        run_casa_tclean_cube_case(
            case,
            &staged_ms_path,
            &casa_prefix,
            0,
            "hogbom",
            CubeAxisStep::Channel(case.channel_start as i32),
            CubeAxisStep::Channel(1),
            "0Jy",
        )
        .expect("run casa cube dirty");

        let rust_residual = extract_channel_plane(
            &read_image(&rust_product(&rust_prefix, "residual")),
            channel_index,
        );
        let casa_residual = extract_channel_plane(
            &read_image(&casa_product(&casa_prefix, "residual")),
            channel_index,
        );
        let stats = plane_difference_stats(&rust_residual, &casa_residual);
        let rust_sumwt = sample_scalar_channel(
            &read_image(&rust_product(&rust_prefix, "sumwt")),
            channel_index,
        );
        let casa_sumwt = sample_scalar_channel(
            &read_image(&casa_product(&casa_prefix, "sumwt")),
            channel_index,
        );
        eprintln!(
            "cube nsigma block0 channel9 single-pol dirty parity {correlation}: rms={:.9e} max_abs={:.9e} corr={:.9e} rust_sumwt={:.9e} casa_sumwt={:.9e}",
            stats.rms, stats.max_abs, stats.correlation, rust_sumwt, casa_sumwt,
        );
    }
}

#[test]
fn explicit_xx_dirty_products_track_casa_headers_and_pixels() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_linXY.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: Some("XX"),
        weighting: WeightingMode::Natural,
        imsize: 64,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point_linXY.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-refim-linxy");
    let casa_prefix = temp.path().join("casa-refim-linxy");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let center = case.center();
    let points = [
        (center, center),
        (center.saturating_sub(1), center),
        (center, center.saturating_sub(1)),
    ];
    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    for (x, y) in points {
        assert_close(
            sample(&rust_psf, x, y),
            sample(&casa_psf, x, y),
            0.12,
            0.25,
            &format!("psf[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y) in points {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            0.15,
            0.35,
            &format!("residual[{x},{y}]"),
        );
    }

    let rust_sumwt = read_scalar_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_scalar_image(&casa_product(&casa_prefix, "sumwt"));
    assert_close(rust_sumwt, casa_sumwt, 1.0, 0.05, "sumwt");
}

#[test]
fn full_stokes_dirty_products_track_casa_on_refim_point_linxy() {
    let base = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_linXY.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 64,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(base) {
        eprintln!("{}", skip_reason_for_case(base));
        return;
    }

    let ms_path = dataset_path(base.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point_linXY.ms").expect("stage ms");

    for stokes in ["I", "Q", "U", "V"] {
        let case = ParityCase {
            correlation: Some(stokes),
            ..base
        };
        let rust_prefix = temp.path().join(format!("rust-refim-linxy-{stokes}"));
        let casa_prefix = temp.path().join(format!("casa-refim-linxy-{stokes}"));
        run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0)
            .expect("run rust imager");
        run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
        assert_dirty_case_matches(
            case,
            &rust_prefix,
            &casa_prefix,
            0.15,
            0.4,
            0.15,
            0.35,
            true,
        );
    }
}

#[test]
fn full_stokes_dirty_products_track_casa_on_polcal_circular_basis() {
    let base = ParityCase {
        dataset_rel: "measurementset/vla/polcal_CIRCULAR_BASIS.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 0.02,
    };
    if !parity_case_available(base) {
        eprintln!("{}", skip_reason_for_case(base));
        return;
    }

    let ms_path = dataset_path(base.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "polcal_CIRCULAR_BASIS.ms").expect("stage ms");

    for stokes in ["I", "Q", "U", "V"] {
        let case = ParityCase {
            correlation: Some(stokes),
            ..base
        };
        let rust_prefix = temp.path().join(format!("rust-polcal-circular-{stokes}"));
        let casa_prefix = temp.path().join(format!("casa-polcal-circular-{stokes}"));
        run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0)
            .expect("run rust imager");
        run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
        assert_dirty_case_matches(case, &rust_prefix, &casa_prefix, 0.2, 0.45, 0.2, 0.4, true);
    }
}

#[test]
fn full_stokes_hogbom_products_track_casa_on_refim_point_stokes() {
    let base = ParityCase {
        dataset_rel: "measurementset/vla/refim_point_stokes.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 64,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(base) {
        eprintln!("{}", skip_reason_for_case(base));
        return;
    }

    let ms_path = dataset_path(base.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point_stokes.ms").expect("stage ms");

    for stokes in ["I", "Q", "U", "V"] {
        let case = ParityCase {
            correlation: Some(stokes),
            ..base
        };
        let rust_prefix = temp.path().join(format!("rust-refim-stokes-{stokes}"));
        let casa_prefix = temp.path().join(format!("casa-refim-stokes-{stokes}"));
        run_rust_imager_case(case, &staged_ms_path, &rust_prefix, false, 20)
            .expect("run rust imager");
        run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 20).expect("run casa tclean");
        assert_clean_case_matches(
            case,
            &rust_prefix,
            &casa_prefix,
            0.2,
            0.5,
            0.2,
            0.45,
            0.2,
            0.45,
        );
    }
}

#[test]
fn flagged_dirty_products_track_casa_headers_and_pixels() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/ngc5921_with_flags.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 30.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "ngc5921_with_flags.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-ngc5921-flags");
    let casa_prefix = temp.path().join("casa-ngc5921-flags");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
    assert_dirty_case_matches(
        case,
        &rust_prefix,
        &casa_prefix,
        0.15,
        0.35,
        0.12,
        0.2,
        true,
    );
}

#[test]
fn uniform_dirty_products_track_casa_headers_and_pixels() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/ngc5921.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 128,
        cell_arcsec: 30.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "ngc5921_uniform.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-ngc5921-uniform");
    let casa_prefix = temp.path().join("casa-ngc5921-uniform");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
    assert_dirty_case_matches(case, &rust_prefix, &casa_prefix, 0.2, 0.4, 0.2, 0.35, false);
}

#[test]
fn briggs_dirty_products_track_casa_headers_and_pixels() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/ngc5921.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Briggs { robust: 0.5 },
        imsize: 128,
        cell_arcsec: 30.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "ngc5921_briggs.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-ngc5921-briggs");
    let casa_prefix = temp.path().join("casa-ngc5921-briggs");

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
    assert_dirty_case_matches(
        case,
        &rust_prefix,
        &casa_prefix,
        0.2,
        0.45,
        0.2,
        0.35,
        false,
    );
}

fn assert_m51_dirty_products_track_casa_headers_and_pixels(weighting: WeightingMode, label: &str) {
    let case = ParityCase {
        dataset_rel: "measurementset/alma/M51.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting,
        imsize: 256,
        cell_arcsec: 0.05,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "M51.ms").expect("stage ms");
    let rust_prefix = temp.path().join(format!("rust-m51-dirty-{label}"));
    let casa_prefix = temp.path().join(format!("casa-m51-dirty-{label}"));

    run_rust_imager_case(case, &staged_ms_path, &rust_prefix, true, 0).expect("run rust imager");
    run_casa_tclean_case(case, &staged_ms_path, &casa_prefix, 0).expect("run casa tclean");
    assert_dirty_case_matches(
        case,
        &rust_prefix,
        &casa_prefix,
        0.05,
        0.15,
        0.02,
        0.05,
        true,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let psf_stats = image_difference_stats(&rust_psf, &casa_psf);
    let psf_rms_tol = match weighting {
        WeightingMode::Briggs { .. } | WeightingMode::BriggsBwTaper { .. } => 1.1e-3,
        _ => 5.0e-4,
    };
    assert!(
        psf_stats.rms <= psf_rms_tol,
        "M51 dirty PSF RMS diff too large: {psf_stats:?}"
    );
    assert!(
        psf_stats.max_abs <= 5.0e-3,
        "M51 dirty PSF max diff too large: {psf_stats:?}"
    );

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let residual_stats = image_difference_stats(&rust_residual, &casa_residual);
    let rust_peak = peak_location(&rust_residual).expect("rust residual peak");
    let casa_peak = peak_location(&casa_residual).expect("casa residual peak");
    assert_eq!(
        rust_peak, casa_peak,
        "M51 dirty residual peak pixel mismatch"
    );
    let casa_peak_value = sample(&casa_residual, casa_peak.0, casa_peak.1).abs();
    let normalized_rms = residual_stats.rms / casa_peak_value.max(1.0e-6);
    let normalized_max = residual_stats.max_abs / casa_peak_value.max(1.0e-6);
    assert!(
        normalized_rms <= 0.02,
        "M51 dirty residual normalized RMS diff too large: peak={casa_peak_value} stats={residual_stats:?}"
    );
    assert!(
        normalized_max <= 0.2,
        "M51 dirty residual normalized max diff too large: peak={casa_peak_value} stats={residual_stats:?}"
    );
    assert!(
        residual_stats.correlation >= 0.995,
        "M51 dirty residual correlation too low: {residual_stats:?}"
    );
}

#[test]
fn m51_dirty_products_track_casa_headers_and_pixels() {
    assert_m51_dirty_products_track_casa_headers_and_pixels(WeightingMode::Natural, "natural");
}

#[test]
fn m51_briggs_dirty_products_track_casa_headers_and_pixels() {
    assert_m51_dirty_products_track_casa_headers_and_pixels(
        WeightingMode::Briggs { robust: 0.5 },
        "briggs",
    );
}

fn assert_wideband_single_term_field_tracks_casa(
    niter: usize,
    label: &str,
    deconvolver: Deconvolver,
    casa_deconvolver: &str,
) {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/vla_wideband_2ptg_w_squint.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 0,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "vla_wideband_2ptg_w_squint.ms")
            .expect("stage ms");
    let rust_prefix = temp
        .path()
        .join(format!("rust-vla-wideband-field0-{label}"));
    let casa_prefix = temp
        .path()
        .join(format!("casa-vla-wideband-field0-{label}"));

    run_rust_imager_case_with_deconvolver(
        case,
        &staged_ms_path,
        &rust_prefix,
        niter == 0,
        niter,
        deconvolver,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_deconvolver(
        case,
        &staged_ms_path,
        &casa_prefix,
        niter,
        casa_deconvolver,
    )
    .expect("run casa tclean");

    if niter == 0 {
        assert_dirty_case_matches(case, &rust_prefix, &casa_prefix, 0.05, 0.2, 0.05, 0.2, true);

        let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
        let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
        let psf_stats = image_difference_stats(&rust_psf, &casa_psf);
        assert!(
            psf_stats.correlation >= 0.995,
            "wideband nterms=1 dirty PSF correlation too low: {psf_stats:?}"
        );

        let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
        let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
        let residual_stats = image_difference_stats(&rust_residual, &casa_residual);
        assert!(
            residual_stats.correlation >= 0.995,
            "wideband nterms=1 dirty residual correlation too low: {residual_stats:?}"
        );
    } else {
        assert_clean_case_matches(
            case,
            &rust_prefix,
            &casa_prefix,
            0.1,
            0.2,
            0.1,
            0.2,
            0.1,
            0.25,
        );

        let rust_image = read_image(&rust_product(&rust_prefix, "image"));
        let casa_image = read_image(&casa_product(&casa_prefix, "image"));
        let image_stats = image_difference_stats(&rust_image, &casa_image);
        let casa_peak = peak_location(&casa_image).expect("casa image peak");
        let casa_peak_value = sample(&casa_image, casa_peak.0, casa_peak.1).abs();
        let normalized_rms = image_stats.rms / casa_peak_value.max(1.0e-6);
        let normalized_max = image_stats.max_abs / casa_peak_value.max(1.0e-6);
        assert!(
            normalized_rms <= 0.02,
            "wideband nterms=1 clean image normalized RMS diff too large: peak={casa_peak_value} stats={image_stats:?}"
        );
        assert!(
            normalized_max <= 0.2,
            "wideband nterms=1 clean image normalized max diff too large: peak={casa_peak_value} stats={image_stats:?}"
        );
        assert!(
            image_stats.correlation >= 0.97,
            "wideband nterms=1 clean image correlation too low: {image_stats:?}"
        );
    }
}

#[test]
fn wideband_single_term_dirty_products_track_casa_on_vla_field0() {
    assert_wideband_single_term_field_tracks_casa(0, "dirty", Deconvolver::Hogbom, "hogbom");
}

#[test]
fn wideband_single_term_clark_products_track_casa_on_vla_field0() {
    assert_wideband_single_term_field_tracks_casa(10, "clark", Deconvolver::Clark, "clark");
}

fn assert_mtmfs_vlass_tracks_casa(niter: usize, label: &str) {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/ref_vlass_wtsp_creation.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 0,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 2.5,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let rust_prefix = temp.path().join(format!("rust-ref-vlass-mtmfs-{label}"));
    let casa_prefix = temp.path().join(format!("casa-ref-vlass-mtmfs-{label}"));

    run_rust_imager_case_with_mtmfs(case, &ms_path, &rust_prefix, niter == 0, niter, 2)
        .expect("run rust mtmfs");
    run_casa_tclean_case_with_mtmfs(case, &ms_path, &casa_prefix, niter, 2)
        .expect("run casa mtmfs");

    if niter == 0 {
        assert_mtmfs_dirty_products_track_casa(&rust_prefix, &casa_prefix);
    } else {
        assert_mtmfs_clean_products_track_casa(&rust_prefix, &casa_prefix);
    }
}

#[test]
fn mtmfs_dirty_products_track_casa_on_ref_vlass_wtsp_creation() {
    assert_mtmfs_vlass_tracks_casa(0, "dirty");
}

#[test]
fn mtmfs_clean_products_track_casa_on_ref_vlass_wtsp_creation() {
    assert_mtmfs_vlass_tracks_casa(1, "clean");
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_point_wterm_vlad() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_wterm_vlad.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 256,
        cell_arcsec: 80.0,
    };
    assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
        case,
        "uniform-256-80arcsec",
        StagedFieldLayout::SingleField,
        WProjectDirtyParityExpectation {
            rust_wprojplanes: None,
            casa_wprojplanes: 16,
            min_peak_gain: 0.1,
            peak_abs_tol: 6.0e-2,
            peak_rel_tol: 8.0e-2,
            residual_rms_tol: 0.005,
            residual_max_abs_tol: 0.15,
        },
    );
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_point_wterm_vlad_natural_weighting() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_wterm_vlad.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 80.0,
    };
    assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
        case,
        "natural-256-80arcsec",
        StagedFieldLayout::SingleField,
        WProjectDirtyParityExpectation {
            rust_wprojplanes: None,
            casa_wprojplanes: 16,
            min_peak_gain: 0.05,
            peak_abs_tol: 7.0e-2,
            peak_rel_tol: 9.0e-2,
            residual_rms_tol: 0.01,
            residual_max_abs_tol: 0.25,
        },
    );
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_point_wterm_vlad_tighter_field() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_wterm_vlad.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 384,
        cell_arcsec: 60.0,
    };
    assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
        case,
        "uniform-384-60arcsec",
        StagedFieldLayout::SingleField,
        WProjectDirtyParityExpectation {
            rust_wprojplanes: None,
            casa_wprojplanes: 16,
            min_peak_gain: 0.08,
            peak_abs_tol: 8.0e-2,
            peak_rel_tol: 9.0e-2,
            residual_rms_tol: 0.007,
            residual_max_abs_tol: 0.2,
        },
    );
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_point_wterm_vlad_eight_planes() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_wterm_vlad.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 256,
        cell_arcsec: 80.0,
    };
    assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
        case,
        "uniform-256-80arcsec-8planes",
        StagedFieldLayout::SingleField,
        WProjectDirtyParityExpectation {
            rust_wprojplanes: Some(8),
            casa_wprojplanes: 8,
            min_peak_gain: 0.08,
            peak_abs_tol: 7.0e-2,
            peak_rel_tol: 9.0e-2,
            residual_rms_tol: 0.006,
            residual_max_abs_tol: 0.18,
        },
    );
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_point_wterm_vlad_shared_phase_multifield() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_wterm_vlad.ms",
        field_ids: &[0, 1],
        phasecenter_field: None,
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 256,
        cell_arcsec: 80.0,
    };
    assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
        case,
        "uniform-256-80arcsec-multifield",
        StagedFieldLayout::SharedPhaseMultiField,
        WProjectDirtyParityExpectation {
            rust_wprojplanes: None,
            casa_wprojplanes: 16,
            min_peak_gain: 0.1,
            peak_abs_tol: 6.0e-2,
            peak_rel_tol: 8.0e-2,
            residual_rms_tol: 0.005,
            residual_max_abs_tol: 0.15,
        },
    );
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_point_wterm_vlad_distinct_phase_multifield() {
    let case = ParityCase {
        dataset_rel: "unittest/tclean/refim_point_wterm_vlad.ms",
        field_ids: &[0, 1],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Uniform,
        imsize: 256,
        cell_arcsec: 80.0,
    };
    assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
        case,
        "uniform-256-80arcsec-distinct-phase-multifield",
        StagedFieldLayout::DistinctPhaseMultiField {
            ra_offset_rad: 600.0 / 206_264.806_247_096_36,
        },
        WProjectDirtyParityExpectation {
            rust_wprojplanes: None,
            casa_wprojplanes: 16,
            min_peak_gain: 0.05,
            peak_abs_tol: 7.0e-2,
            peak_rel_tol: 9.0e-2,
            residual_rms_tol: 0.01,
            residual_max_abs_tol: 0.25,
        },
    );
}

#[derive(Clone, Copy)]
struct WProjectDirtyParityExpectation {
    rust_wprojplanes: Option<usize>,
    casa_wprojplanes: usize,
    min_peak_gain: f32,
    peak_abs_tol: f32,
    peak_rel_tol: f32,
    residual_rms_tol: f32,
    residual_max_abs_tol: f32,
}

const MAWPROJECT_PHASECENTER: &str = "J2000 19:59:28.500 +40.44.01.50";

fn assert_wproject_dirty_products_track_casa_on_refim_point_wterm_vlad(
    case: ParityCase<'_>,
    label: &str,
    field_layout: StagedFieldLayout,
    expectation: WProjectDirtyParityExpectation,
) {
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_name = format!("refim_point_wterm_vlad-{label}.ms");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), &staged_name).expect("stage ms");
    match field_layout {
        StagedFieldLayout::SingleField => {}
        StagedFieldLayout::SharedPhaseMultiField => {
            promote_staged_measurement_set_to_multifield(&staged_ms_path, case.field_ids, None)
                .expect("promote staged MS to shared-phase multi-field");
        }
        StagedFieldLayout::DistinctPhaseMultiField { ra_offset_rad } => {
            promote_staged_measurement_set_to_multifield(
                &staged_ms_path,
                case.field_ids,
                Some(ra_offset_rad),
            )
            .expect("promote staged MS to distinct-phase multi-field");
        }
    }
    let rust_standard_prefix = temp
        .path()
        .join(format!("rust-refim-point-wterm-standard-{label}"));
    let rust_wproject_prefix = temp
        .path()
        .join(format!("rust-refim-point-wterm-wproject-{label}"));
    let casa_prefix = temp
        .path()
        .join(format!("casa-refim-point-wterm-wproject-{label}"));

    run_rust_imager_case(case, &staged_ms_path, &rust_standard_prefix, true, 0)
        .expect("run rust standard imager");
    run_rust_imager_case_with_w_term_mode(
        case,
        &staged_ms_path,
        &rust_wproject_prefix,
        true,
        0,
        WTermMode::WProject,
        expectation.rust_wprojplanes,
    )
    .expect("run rust wproject imager");
    run_casa_tclean_case_with_wproject(
        case,
        &staged_ms_path,
        &casa_prefix,
        0,
        expectation.casa_wprojplanes,
    )
    .expect("run casa wproject tclean");

    compare_image_headers(
        &rust_product(&rust_wproject_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_wproject_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let center = (case.center(), case.center());
    let rust_standard_residual = read_image(&rust_product(&rust_standard_prefix, "residual"));
    let rust_wproject_residual = read_image(&rust_product(&rust_wproject_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_wproject_psf = read_image(&rust_product(&rust_wproject_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let expected_peak = peak_location(&casa_residual).expect("casa wproject residual peak");

    assert_eq!(
        peak_location(&rust_wproject_residual),
        Some(expected_peak),
        "Rust wproject dirty peak pixel changed for {label}"
    );
    assert_eq!(
        peak_location(&casa_residual),
        Some(expected_peak),
        "CASA wproject dirty peak pixel changed for {label}"
    );
    assert_ne!(
        expected_peak, center,
        "expected off-axis wproject peak for {label}"
    );
    assert_eq!(
        peak_location(&rust_wproject_psf),
        Some(center),
        "Rust wproject PSF peak pixel changed for {label}"
    );
    assert_eq!(
        peak_location(&casa_psf),
        Some(center),
        "CASA wproject PSF peak pixel changed for {label}"
    );

    let rust_standard_peak = sample(&rust_standard_residual, expected_peak.0, expected_peak.1);
    let rust_wproject_peak = sample(&rust_wproject_residual, expected_peak.0, expected_peak.1);
    let casa_peak = sample(&casa_residual, expected_peak.0, expected_peak.1);
    assert!(
        rust_wproject_peak > rust_standard_peak + expectation.min_peak_gain,
        "Rust wproject should materially improve the off-axis source peak for {label}: standard={rust_standard_peak}, wproject={rust_wproject_peak}"
    );
    assert_close(
        rust_wproject_peak,
        casa_peak,
        expectation.peak_abs_tol,
        expectation.peak_rel_tol,
        &format!("wproject residual peak ({label})"),
    );

    assert_close(
        sample(&rust_wproject_psf, center.0, center.1),
        sample(&casa_psf, center.0, center.1),
        1.0e-3,
        1.0e-3,
        "wproject psf center",
    );

    let residual_stats = image_difference_stats(&rust_wproject_residual, &casa_residual);
    assert!(
        residual_stats.rms <= expectation.residual_rms_tol,
        "wproject residual RMS diff too large for {label}: {residual_stats:?}"
    );
    assert!(
        residual_stats.max_abs <= expectation.residual_max_abs_tol,
        "wproject residual max diff too large for {label}: {residual_stats:?}"
    );
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_mawproject() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_mawproject.ms",
        field_ids: &[],
        phasecenter_field: None,
        spw: 1,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 10.0,
    };
    assert_wproject_dirty_products_track_casa_on_mawproject(case, "between-pointings");
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_mawproject_offcenter() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_mawproject_offcenter.ms",
        field_ids: &[],
        phasecenter_field: None,
        spw: 1,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 10.0,
    };
    assert_wproject_dirty_products_track_casa_on_mawproject(case, "offcenter");
}

#[test]
fn wproject_dirty_products_track_casa_on_refim_mawproject_twopointings() {
    let case = ParityCase {
        dataset_rel: "measurementset/evla/refim_mawproject_twopointings.ms",
        field_ids: &[],
        phasecenter_field: None,
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 10.0,
    };
    assert_wproject_dirty_products_track_casa_on_mawproject(case, "twopointings");
}

fn assert_wproject_dirty_products_track_casa_on_mawproject(case: ParityCase<'_>, label: &str) {
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_name = format!("refim-mawproject-{label}.ms");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), &staged_name).expect("stage ms");
    let rust_prefix = temp.path().join(format!("rust-refim-mawproject-{label}"));
    let casa_prefix = temp.path().join(format!("casa-refim-mawproject-{label}"));

    run_rust_imager_case_with_explicit_phasecenter_and_w_term_mode(
        case,
        &staged_ms_path,
        &rust_prefix,
        true,
        0,
        MAWPROJECT_PHASECENTER,
        WTermMode::WProject,
        Some(16),
    )
    .expect("run rust wproject imager");
    run_casa_tclean_case_with_wproject_and_explicit_phasecenter(
        case,
        &staged_ms_path,
        &casa_prefix,
        0,
        16,
        MAWPROJECT_PHASECENTER,
    )
    .expect("run casa wproject tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let casa_peak = peak_location(&casa_residual).expect("casa mawproject peak");
    let rust_peak = peak_location(&rust_residual).expect("rust mawproject peak");
    let center = (case.center(), case.center());

    assert_eq!(
        rust_peak, casa_peak,
        "Rust mawproject peak pixel changed for {label}"
    );
    assert_eq!(
        peak_location(&rust_psf),
        Some(center),
        "Rust mawproject PSF peak pixel changed for {label}"
    );
    assert_eq!(
        peak_location(&casa_psf),
        Some(center),
        "CASA mawproject PSF peak pixel changed for {label}"
    );

    let rust_peak_value = sample(&rust_residual, rust_peak.0, rust_peak.1);
    let casa_peak_value = sample(&casa_residual, casa_peak.0, casa_peak.1);
    assert_close(
        rust_peak_value,
        casa_peak_value,
        8.0e-2,
        1.2e-1,
        &format!("mawproject residual peak ({label})"),
    );
    assert_close(
        sample(&rust_psf, center.0, center.1),
        sample(&casa_psf, center.0, center.1),
        1.0e-3,
        1.0e-3,
        &format!("mawproject psf center ({label})"),
    );

    let residual_stats = image_difference_stats(&rust_residual, &casa_residual);
    eprintln!(
        "mawproject {label}: peak={casa_peak:?} rust_peak={rust_peak_value:.9e} casa_peak={casa_peak_value:.9e} rms={:.9e} max_abs={:.9e} corr={:.9e}",
        residual_stats.rms, residual_stats.max_abs, residual_stats.correlation,
    );
    assert!(
        residual_stats.rms <= 2.0e-2,
        "mawproject residual RMS diff too large for {label}: {residual_stats:?}"
    );
    assert!(
        residual_stats.max_abs <= 4.0e-1,
        "mawproject residual max diff too large for {label}: {residual_stats:?}"
    );
    assert!(
        residual_stats.correlation >= 0.99,
        "mawproject residual correlation too small for {label}: {residual_stats:?}"
    );
}

#[test]
fn clean_products_reopen_in_casa_and_rust() {
    if !parity_available() {
        eprintln!("{}", skip_reason());
        return;
    }

    let ms_path = ngc5921_ms_path().expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "ngc5921.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-clean");
    let casa_prefix = temp.path().join("casa-clean");

    run_rust_imager(&staged_ms_path, &rust_prefix, false).expect("run rust imager");
    run_casa_tclean(&staged_ms_path, &casa_prefix, 4).expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "model"),
        &casa_product(&casa_prefix, "model"),
        "Jy/pixel",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        false,
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y) in [(64, 64), (63, 64), (64, 63), (60, 60)] {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            0.2,
            0.4,
            &format!("image[{x},{y}]"),
        );
    }

    let summary = summarize_image_in_casa(&rust_product(&rust_prefix, "image"))
        .expect("summarize rust-written image in CASA");
    assert_eq!(summary["shape"], Value::from(vec![128, 128, 1, 1]));
    assert_eq!(summary["brightnessunit"], Value::from("Jy/beam"));
    assert_eq!(summary["has_beam"], Value::from(true));
}

#[test]
fn clark_niter_one_matches_casa_component_support() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/sim_data_VLA_jet.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 5,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 12.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "sim_data_VLA_jet.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-simjet-clark");
    let casa_prefix = temp.path().join("casa-simjet-clark");

    run_rust_imager_case_with_deconvolver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        1,
        Deconvolver::Clark,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_deconvolver(case, &staged_ms_path, &casa_prefix, 1, "clark")
        .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "model"),
        &casa_product(&casa_prefix, "model"),
        "Jy/pixel",
        false,
    );

    let rust_model = read_image(&rust_product(&rust_prefix, "model"));
    let casa_model = read_image(&casa_product(&casa_prefix, "model"));
    let rust_nonzero = count_nonzero_pixels(&rust_model, 1.0e-6);
    let casa_nonzero = count_nonzero_pixels(&casa_model, 1.0e-6);
    assert_eq!(
        rust_nonzero, 1,
        "expected one Rust Clark component at niter=1"
    );
    assert_eq!(
        casa_nonzero, 1,
        "expected one CASA Clark component at niter=1"
    );

    let rust_peak = peak_location(&rust_model).expect("rust model peak");
    let casa_peak = peak_location(&casa_model).expect("casa model peak");
    assert_eq!(rust_peak, casa_peak, "Clark model peak pixel mismatch");
    assert_close(
        sample(&rust_model, rust_peak.0, rust_peak.1),
        sample(&casa_model, casa_peak.0, casa_peak.1),
        5.0e-3,
        5.0e-3,
        "clark model peak amplitude",
    );
}

#[test]
fn clark_products_track_casa_on_simulated_jet() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/sim_data_VLA_jet.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 5,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 12.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "sim_data_VLA_jet.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-simjet-clark-deep");
    let casa_prefix = temp.path().join("casa-simjet-clark-deep");

    run_rust_imager_case_with_deconvolver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        50,
        Deconvolver::Clark,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_deconvolver(case, &staged_ms_path, &casa_prefix, 50, "clark")
        .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "model"),
        &casa_product(&casa_prefix, "model"),
        "Jy/pixel",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let points = [(256, 256), (256, 257), (257, 256), (264, 331), (265, 331)];
    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y) in points {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            2.0e-2,
            8.0e-2,
            &format!("clark image[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y) in points {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            2.0e-2,
            1.0e-1,
            &format!("clark residual[{x},{y}]"),
        );
    }

    let rust_model = read_image(&rust_product(&rust_prefix, "model"));
    let casa_model = read_image(&casa_product(&casa_prefix, "model"));
    let rust_peak = peak_location(&rust_model).expect("rust model peak");
    let casa_peak = peak_location(&casa_model).expect("casa model peak");
    assert_eq!(rust_peak, casa_peak, "Clark deep model peak mismatch");
}

#[test]
fn clark_products_track_casa_on_ngc5921() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/ngc5921.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 30.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "ngc5921.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-ngc5921-clark");
    let casa_prefix = temp.path().join("casa-ngc5921-clark");

    run_rust_imager_case_with_deconvolver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        8,
        Deconvolver::Clark,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_deconvolver(case, &staged_ms_path, &casa_prefix, 8, "clark")
        .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "model"),
        &casa_product(&casa_prefix, "model"),
        "Jy/pixel",
        false,
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y) in [(64, 64), (63, 64), (64, 63), (60, 60)] {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            2.0e-1,
            4.0e-1,
            &format!("ngc5921 clark image[{x},{y}]"),
        );
    }

    let rust_model = read_image(&rust_product(&rust_prefix, "model"));
    let casa_model = read_image(&casa_product(&casa_prefix, "model"));
    assert!(
        count_nonzero_pixels(&rust_model, 1.0e-6) > 0,
        "expected Rust Clark model to contain clean components"
    );
    assert!(
        count_nonzero_pixels(&casa_model, 1.0e-6) > 0,
        "expected CASA Clark model to contain clean components"
    );
}

#[test]
fn multiscale_scales_zero_matches_casa_component_support() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/sim_data_VLA_jet.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 5,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 12.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "sim_data_VLA_jet.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-simjet-ms0");
    let casa_prefix = temp.path().join("casa-simjet-ms0");
    let scales = [0.0_f32];

    run_rust_imager_case_with_solver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        1,
        Deconvolver::Multiscale,
        &scales,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_solver(
        case,
        &staged_ms_path,
        &casa_prefix,
        1,
        "multiscale",
        &scales,
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "model"),
        &casa_product(&casa_prefix, "model"),
        "Jy/pixel",
        false,
    );

    let rust_model = read_image(&rust_product(&rust_prefix, "model"));
    let casa_model = read_image(&casa_product(&casa_prefix, "model"));
    let rust_nonzero = count_nonzero_pixels(&rust_model, 1.0e-6);
    let casa_nonzero = count_nonzero_pixels(&casa_model, 1.0e-6);
    assert_eq!(
        rust_nonzero, 1,
        "expected one Rust multiscale(scale=0) component at niter=1"
    );
    assert_eq!(
        casa_nonzero, 1,
        "expected one CASA multiscale(scale=0) component at niter=1"
    );

    let rust_peak = peak_location(&rust_model).expect("rust model peak");
    let casa_peak = peak_location(&casa_model).expect("casa model peak");
    assert_eq!(rust_peak, casa_peak, "multiscale(scale=0) peak mismatch");
    assert_close(
        sample(&rust_model, rust_peak.0, rust_peak.1),
        sample(&casa_model, casa_peak.0, casa_peak.1),
        5.0e-3,
        5.0e-3,
        "multiscale(scale=0) model peak amplitude",
    );
}

#[test]
fn multiscale_products_track_casa_on_simulated_jet() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/sim_data_VLA_jet.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 5,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 512,
        cell_arcsec: 12.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "sim_data_VLA_jet.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-simjet-ms");
    let casa_prefix = temp.path().join("casa-simjet-ms");
    let scales = [0.0_f32, 5.0, 15.0];

    run_rust_imager_case_with_solver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        50,
        Deconvolver::Multiscale,
        &scales,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_solver(
        case,
        &staged_ms_path,
        &casa_prefix,
        50,
        "multiscale",
        &scales,
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "model"),
        &casa_product(&casa_prefix, "model"),
        "Jy/pixel",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let points = [(256, 256), (256, 257), (257, 256), (264, 331), (265, 331)];
    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    for (x, y) in points {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            2.0e-2,
            8.0e-2,
            &format!("multiscale image[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y) in points {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            2.0e-2,
            1.0e-1,
            &format!("multiscale residual[{x},{y}]"),
        );
    }
}

#[test]
fn multiscale_products_track_casa_on_m51_single_field() {
    let case = ParityCase {
        dataset_rel: "measurementset/alma/M51.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 1,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 0.05,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), "M51.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-m51-ms");
    let casa_prefix = temp.path().join("casa-m51-ms");
    let scales = [0.0_f32, 5.0, 15.0];

    run_rust_imager_case_with_solver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        20,
        Deconvolver::Multiscale,
        &scales,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_solver(
        case,
        &staged_ms_path,
        &casa_prefix,
        20,
        "multiscale",
        &scales,
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    let rust_peak = peak_location(&rust_image).expect("rust image peak");
    let casa_peak = peak_location(&casa_image).expect("casa image peak");
    assert_eq!(rust_peak, casa_peak, "M51 multiscale peak pixel mismatch");

    for (x, y) in [
        rust_peak,
        (rust_peak.0.saturating_sub(1), rust_peak.1),
        (rust_peak.0 + 1, rust_peak.1),
        (rust_peak.0, rust_peak.1.saturating_sub(1)),
        (rust_peak.0, rust_peak.1 + 1),
    ] {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            5.0e-2,
            1.5e-1,
            &format!("M51 multiscale image[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y) in [
        rust_peak,
        (rust_peak.0.saturating_sub(1), rust_peak.1),
        (rust_peak.0, rust_peak.1.saturating_sub(1)),
    ] {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            5.0e-2,
            2.0e-1,
            &format!("M51 multiscale residual[{x},{y}]"),
        );
    }
}

#[test]
fn wproject_dirty_cube_products_track_casa_on_n2403_source_backed_multifield() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/n2403.short.ms",
        field_ids: &[0, 1],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 2,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 8.0,
    };
    assert_wproject_dirty_cube_products_track_casa_on_n2403_source_backed_multifield(
        case,
        "phasecenter-0",
    );
}

#[test]
fn wproject_dirty_cube_products_track_casa_on_n2403_source_backed_calibrator_multifield() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/n2403.short.ms",
        field_ids: &[1, 3],
        phasecenter_field: Some(1),
        spw: 0,
        channel_start: 0,
        channel_count: 2,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 128,
        cell_arcsec: 8.0,
    };
    assert_wproject_dirty_cube_products_track_casa_on_n2403_source_backed_multifield(
        case,
        "calibrators-phasecenter-1",
    );
}

fn assert_wproject_dirty_cube_products_track_casa_on_n2403_source_backed_multifield(
    case: ParityCase<'_>,
    label: &str,
) {
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path = stage_measurement_set(&ms_path, temp.path(), &format!("n2403.{label}.ms"))
        .expect("stage ms");
    let rust_prefix = temp
        .path()
        .join(format!("rust-n2403-multifield-cube-wproject-{label}"));
    let casa_prefix = temp
        .path()
        .join(format!("casa-n2403-multifield-cube-wproject-{label}"));

    run_rust_imager_cube_dirty_with_w_term_mode(
        case,
        &staged_ms_path,
        &rust_prefix,
        casars_imager::SpectralMode::Cube,
        WTermMode::WProject,
        Some(8),
    )
    .expect("run rust wproject cube");
    run_casa_tclean_cube_dirty_case_with_wproject(
        case,
        &staged_ms_path,
        &casa_prefix,
        CubeAxisStep::Channel(0),
        CubeAxisStep::Channel(1),
        8,
    )
    .expect("run casa wproject cube");

    compare_image_headers(
        &rust_product(&rust_prefix, "psf"),
        &casa_product(&casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    let rust_sumwt = read_image(&rust_product(&rust_prefix, "sumwt"));
    let casa_sumwt = read_image(&casa_product(&casa_prefix, "sumwt"));
    let center = case.center();

    for chan in [0usize, case.channel_count - 1] {
        let rust_psf_plane = extract_channel_plane(&rust_psf, chan);
        let casa_psf_plane = extract_channel_plane(&casa_psf, chan);
        let rust_residual_plane = extract_channel_plane(&rust_residual, chan);
        let casa_residual_plane = extract_channel_plane(&casa_residual, chan);

        assert_eq!(
            peak_location_in_plane(&rust_psf_plane),
            Some((center, center)),
            "Rust n2403 multifield wproject PSF peak moved at chan={chan}"
        );
        assert_eq!(
            peak_location_in_plane(&casa_psf_plane),
            Some((center, center)),
            "CASA n2403 multifield wproject PSF peak moved at chan={chan}"
        );
        assert_close(
            sample_channel(&rust_psf, center, center, chan),
            sample_channel(&casa_psf, center, center, chan),
            1.0e-3,
            1.0e-3,
            &format!("n2403 multifield wproject psf center[chan={chan}]"),
        );

        for (x, y) in [
            (center, center),
            (center.saturating_sub(1), center),
            (center, center.saturating_sub(1)),
        ] {
            assert_close(
                sample_channel(&rust_residual, x, y, chan),
                sample_channel(&casa_residual, x, y, chan),
                0.1,
                0.2,
                &format!("n2403 multifield wproject residual[{x},{y},chan={chan}]"),
            );
        }

        let stats = plane_difference_stats(&rust_residual_plane, &casa_residual_plane);
        let rust_peak_scale = rust_residual_plane
            .iter()
            .fold(0.0f32, |best, value| best.max(value.abs()));
        let casa_peak_scale = casa_residual_plane
            .iter()
            .fold(0.0f32, |best, value| best.max(value.abs()));
        let peak_scale = rust_peak_scale.max(casa_peak_scale).max(1.0);
        assert!(
            stats.rms / peak_scale <= 0.03,
            "n2403 multifield wproject residual RMS diff too large at chan={chan}: {stats:?}"
        );
        assert!(
            stats.max_abs / peak_scale <= 0.12,
            "n2403 multifield wproject residual max diff too large at chan={chan}: {stats:?}"
        );
        let rust_sumwt_chan = sample_scalar_channel(&rust_sumwt, chan);
        let casa_sumwt_chan = sample_scalar_channel(&casa_sumwt, chan);
        assert!(
            rust_sumwt_chan.is_finite() && rust_sumwt_chan > 0.0,
            "expected positive Rust n2403 multifield wproject sumwt[chan={chan}]"
        );
        assert!(
            casa_sumwt_chan.is_finite() && casa_sumwt_chan > 0.0,
            "expected positive CASA n2403 multifield wproject sumwt[chan={chan}]"
        );
    }
}

#[test]
fn multiscale_products_track_casa_on_n2403() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/n2403.short.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 127,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 256,
        cell_arcsec: 8.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "n2403.short.ms").expect("stage ms");
    let rust_prefix = temp.path().join("rust-n2403-ms");
    let casa_prefix = temp.path().join("casa-n2403-ms");
    let scales = [0.0_f32, 5.0, 15.0];

    run_rust_imager_case_with_solver(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        20,
        Deconvolver::Multiscale,
        &scales,
    )
    .expect("run rust imager");
    run_casa_tclean_case_with_solver(
        case,
        &staged_ms_path,
        &casa_prefix,
        20,
        "multiscale",
        &scales,
    )
    .expect("run casa tclean");

    compare_image_headers(
        &rust_product(&rust_prefix, "image"),
        &casa_product(&casa_prefix, "image"),
        "Jy/beam",
        false,
    );
    compare_image_headers(
        &rust_product(&rust_prefix, "residual"),
        &casa_product(&casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let rust_image = read_image(&rust_product(&rust_prefix, "image"));
    let casa_image = read_image(&casa_product(&casa_prefix, "image"));
    let rust_peak = peak_location(&rust_image).expect("rust image peak");
    let casa_peak = peak_location(&casa_image).expect("casa image peak");
    assert_eq!(rust_peak, casa_peak, "n2403 multiscale peak pixel mismatch");

    for (x, y) in [
        rust_peak,
        (rust_peak.0.saturating_sub(1), rust_peak.1),
        (rust_peak.0 + 1, rust_peak.1),
        (rust_peak.0, rust_peak.1.saturating_sub(1)),
        (rust_peak.0, rust_peak.1 + 1),
    ] {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            7.5e-2,
            2.0e-1,
            &format!("n2403 multiscale image[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(&rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(&casa_prefix, "residual"));
    for (x, y) in [
        rust_peak,
        (rust_peak.0.saturating_sub(1), rust_peak.1),
        (rust_peak.0, rust_peak.1.saturating_sub(1)),
    ] {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            7.5e-2,
            2.5e-1,
            &format!("n2403 multiscale residual[{x},{y}]"),
        );
    }
}

fn parity_available() -> bool {
    discover_casa_python().is_some_and(|python| python.tclean_available)
        && ngc5921_ms_path().is_some()
}

fn parity_case_available(case: ParityCase<'_>) -> bool {
    discover_casa_python().is_some_and(|python| python.tclean_available)
        && dataset_path(case.dataset_rel).is_some()
}

fn skip_reason() -> String {
    match (discover_casa_python(), ngc5921_ms_path()) {
        (None, _) => {
            "CASA imaging parity skipped: no CASA-capable python with tclean was found".to_string()
        }
        (Some(python), _) if !python.tclean_available => format!(
            "CASA imaging parity skipped: {} can import casatasks but does not expose tclean",
            python.program.display()
        ),
        (_, None) => {
            "CASA imaging parity skipped: missing measurementset/vla/ngc5921.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata".to_string()
        }
        _ => "CASA imaging parity skipped".to_string(),
    }
}

fn skip_reason_for_case(case: ParityCase<'_>) -> String {
    match (discover_casa_python(), dataset_path(case.dataset_rel)) {
        (None, _) => {
            "CASA imaging parity skipped: no CASA-capable python with tclean was found".to_string()
        }
        (Some(python), _) if !python.tclean_available => format!(
            "CASA imaging parity skipped: {} can import casatasks but does not expose tclean",
            python.program.display()
        ),
        (_, None) => format!(
            "CASA imaging parity skipped: missing {} under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata",
            case.dataset_rel
        ),
        _ => "CASA imaging parity skipped".to_string(),
    }
}

fn ngc5921_ms_path() -> Option<PathBuf> {
    casatestdata_path("measurementset/vla/ngc5921.ms").filter(|path| path.exists())
}

fn dataset_path(relative: &str) -> Option<PathBuf> {
    dataset_candidates(relative)
        .into_iter()
        .find(|candidate| candidate.exists())
}

fn dataset_candidates(relative: &str) -> Vec<PathBuf> {
    let mut candidates = casatestdata_path(relative).into_iter().collect::<Vec<_>>();
    if relative == "unittest/tclean/refim_eptwochan.ms" {
        candidates.extend(casatestdata_path("measurementset/evla/refim_eptwochan.ms"));
    } else if relative == "unittest/tclean/refim_point.ms" {
        candidates.extend(casatestdata_path("measurementset/vla/refim_point.ms"));
    } else if relative == "unittest/tclean/refim_point_descendingfreqs.ms" {
        candidates.extend(casatestdata_path(
            "measurementset/vla/refim_point_descendingfreqs.ms",
        ));
    }
    candidates
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

fn promote_staged_measurement_set_to_multifield(
    staged_ms_path: &Path,
    field_ids: &[i32],
    ra_offset_rad: Option<f64>,
) -> Result<(), String> {
    if field_ids.len() <= 1 {
        return Ok(());
    }
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let field_ids_text = field_ids
        .iter()
        .map(|field_id| field_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let script = r#"
import os
import numpy as np
from casatools import table

ms_path = os.environ["CASA_VIS"]
field_ids = [int(value) for value in os.environ["CASA_FIELD_IDS"].split(",") if value]
ra_offset_rad = float(os.environ["CASA_RA_OFFSET_RAD"])
if len(field_ids) < 2:
    raise RuntimeError("need at least two field ids for multi-field staging")
if field_ids[0] != 0:
    raise RuntimeError(f"expected first field id 0 for staged multi-field parity, found {field_ids[0]}")

tb = table()
field_path = os.path.join(ms_path, "FIELD")
tb.open(field_path, nomodify=False)
field_colnames = tb.colnames()
if tb.nrows() < 1:
    raise RuntimeError("FIELD table is empty")
row0 = {name: tb.getcell(name, 0) for name in field_colnames}
ra0 = float(row0["PHASE_DIR"][0][0])
dec0 = float(row0["PHASE_DIR"][1][0])
required_rows = max(field_ids) + 1
if tb.nrows() < required_rows:
    tb.addrows(required_rows - tb.nrows())
for field_id in field_ids[1:]:
    for name, value in row0.items():
        tb.putcell(name, field_id, value)
    if ra_offset_rad != 0.0:
        shifted_ra = ra0 + ra_offset_rad * field_id
        for dir_name in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"]:
            if dir_name not in field_colnames:
                continue
            direction = tb.getcell(dir_name, field_id)
            direction[0][0] = shifted_ra
            tb.putcell(dir_name, field_id, direction)
    if "NAME" in field_colnames:
        tb.putcell("NAME", field_id, f"FIELD{field_id}")
tb.close()

tb.open(os.path.join(ms_path, "DATA_DESCRIPTION"))
ddid_to_spw = [int(value) for value in tb.getcol("SPECTRAL_WINDOW_ID").tolist()]
tb.close()

tb.open(os.path.join(ms_path, "SPECTRAL_WINDOW"))
chan_freq_by_spw = [np.asarray(tb.getcell("CHAN_FREQ", row), dtype=np.float64) for row in range(tb.nrows())]
tb.close()

tb.open(ms_path, nomodify=False)
main_field_ids = tb.getcol("FIELD_ID")
if len(main_field_ids) < len(field_ids):
    raise RuntimeError(
        f"need at least {len(field_ids)} MAIN rows to split across fields, found {len(main_field_ids)}"
    )
for row in range(len(main_field_ids)):
    main_field_ids[row] = field_ids[row % len(field_ids)]
tb.putcol("FIELD_ID", main_field_ids)
if ra_offset_rad != 0.0:
    c_m_per_s = 299792458.0
    ddids = tb.getcol("DATA_DESC_ID")
    for row, field_id in enumerate(main_field_ids.tolist()):
        field_id = int(field_id)
        if field_id == 0:
            continue
        shifted_ra = ra0 + ra_offset_rad * field_id
        delta_ra = shifted_ra - ra0
        l = np.cos(dec0) * np.sin(delta_ra)
        m = np.sin(dec0) * np.cos(dec0) - np.cos(dec0) * np.sin(dec0) * np.cos(delta_ra)
        n = np.sin(dec0) * np.sin(dec0) + np.cos(dec0) * np.cos(dec0) * np.cos(delta_ra)
        uvw = np.asarray(tb.getcell("UVW", row), dtype=np.float64)
        spw_id = ddid_to_spw[int(ddids[row])]
        chan_freq_hz = chan_freq_by_spw[spw_id]
        phase = 2.0 * np.pi * (
            (uvw[0] * chan_freq_hz / c_m_per_s) * l
            + (uvw[1] * chan_freq_hz / c_m_per_s) * m
            + (uvw[2] * chan_freq_hz / c_m_per_s) * (n - 1.0)
        )
        rotation = np.exp(1j * phase).astype(np.complex64)
        data = tb.getcell("DATA", row)
        tb.putcell("DATA", row, data * rotation[np.newaxis, :])
tb.close()
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", staged_ms_path)
        .env("CASA_FIELD_IDS", field_ids_text)
        .env(
            "CASA_RA_OFFSET_RAD",
            ra_offset_rad.unwrap_or(0.0).to_string(),
        )
        .output()
        .map_err(|error| {
            format!(
                "spawn CASA python to promote {} to multi-field: {error}",
                staged_ms_path.display()
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "promote {} to multi-field failed: {}",
            staged_ms_path.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

fn run_rust_imager(ms_path: &Path, prefix: &Path, dirty_only: bool) -> Result<(), String> {
    let _ = (casa_source_root(), casacore_source_root());
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: 128,
        cell_arcsec: 30.0,
        field_ids: Some(vec![0]),
        phasecenter_field: Some(0),
        phasecenter: None,
        ddid: None,
        spw: Some(0),
        spw_selector: None,
        channel_start: Some(0),
        channel_count: Some(1),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: None,
        spectral_mode: casars_imager::SpectralMode::Mfs,
        cube_axis: casa_ms::CubeAxisConfig::default(),
        weighting: WeightingMode::Natural,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter: 4,
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
        hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only,
        write_preview_pngs: false,
    })
    .map(|_| ())
}

fn run_rust_imager_case(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
) -> Result<(), String> {
    run_rust_imager_case_with_solver(
        case,
        ms_path,
        prefix,
        dirty_only,
        niter,
        Deconvolver::Hogbom,
        &[],
    )
}

fn run_rust_imager_case_with_deconvolver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
) -> Result<(), String> {
    run_rust_imager_case_with_solver(case, ms_path, prefix, dirty_only, niter, deconvolver, &[])
}

fn run_rust_imager_case_with_solver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
    multiscale_scales: &[f32],
) -> Result<(), String> {
    run_rust_imager_case_with_solver_and_w_term_mode(
        case,
        ms_path,
        prefix,
        dirty_only,
        niter,
        deconvolver,
        multiscale_scales,
        WTermMode::None,
        None,
    )
}

fn run_rust_imager_case_with_w_term_mode(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    w_term_mode: WTermMode,
    w_project_planes: Option<usize>,
) -> Result<(), String> {
    run_rust_imager_case_with_solver_and_w_term_mode(
        case,
        ms_path,
        prefix,
        dirty_only,
        niter,
        Deconvolver::Hogbom,
        &[],
        w_term_mode,
        w_project_planes,
    )
}

fn run_rust_imager_case_with_explicit_phasecenter_and_w_term_mode(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    explicit_phasecenter: &str,
    w_term_mode: WTermMode,
    w_project_planes: Option<usize>,
) -> Result<(), String> {
    let _ = (casa_source_root(), casacore_source_root());
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: (!case.field_ids.is_empty()).then(|| case.field_ids.to_vec()),
        phasecenter_field: None,
        phasecenter: Some(explicit_phasecenter.to_string()),
        ddid: None,
        spw: Some(case.spw),
        spw_selector: None,
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Mfs,
        cube_axis: casa_ms::CubeAxisConfig::default(),
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter,
        gain: 0.1,
        threshold_jy: 0.0,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: niter.max(1),
        cyclefactor: 1.0,
        min_psf_fraction: 0.1,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode,
        w_project_planes,
        dirty_only,
        write_preview_pngs: false,
    })
    .map(|_| ())
}

fn run_rust_imager_case_with_solver_and_w_term_mode(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
    multiscale_scales: &[f32],
    w_term_mode: WTermMode,
    w_project_planes: Option<usize>,
) -> Result<(), String> {
    let _ = (casa_source_root(), casacore_source_root());
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: None,
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Mfs,
        cube_axis: casa_ms::CubeAxisConfig::default(),
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver,
        nterms: 1,
        multiscale_scales: multiscale_scales.to_vec(),
        small_scale_bias: 0.0,
        niter,
        gain: 0.1,
        threshold_jy: 0.0,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: niter.max(1),
        cyclefactor: 1.0,
        min_psf_fraction: 0.1,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode,
        w_project_planes,
        dirty_only,
        write_preview_pngs: false,
    })
    .map(|_| ())
}

fn run_rust_imager_case_with_mtmfs(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    nterms: usize,
) -> Result<(), String> {
    let _ = (casa_source_root(), casacore_source_root());
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: None,
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Mfs,
        cube_axis: casa_ms::CubeAxisConfig::default(),
        weighting: case.weighting,
        per_channel_weight_density: false,
        use_pointing: false,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: Deconvolver::Mtmfs,
        nterms,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter,
        gain: 0.1,
        threshold_jy: 0.0,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: niter.max(1),
        cyclefactor: 1.0,
        min_psf_fraction: 0.1,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only,
        write_preview_pngs: false,
    })
    .map(|_| ())
}

fn run_rust_imager_cube_dirty(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
) -> Result<RunSummary, String> {
    run_rust_imager_cube_dirty_with_w_term_mode(
        case,
        ms_path,
        prefix,
        casars_imager::SpectralMode::Cube,
        WTermMode::None,
        None,
    )
}

fn run_rust_imager_cube_dirty_with_w_term_mode(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    spectral_mode: casars_imager::SpectralMode,
    w_term_mode: WTermMode,
    w_project_planes: Option<usize>,
) -> Result<RunSummary, String> {
    run_rust_imager_cube_case_with_solver_and_w_term_mode(
        case,
        ms_path,
        prefix,
        true,
        0,
        Deconvolver::Hogbom,
        &[],
        0.0,
        0.0,
        2,
        spectral_mode,
        CubeWeightingOptions::default(),
        w_term_mode,
        w_project_planes,
    )
}

fn run_rust_imager_cube_case_with_deconvolver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
    threshold_jy: f32,
    minor_cycle_length: usize,
) -> Result<RunSummary, String> {
    run_rust_imager_cube_case_with_solver_and_w_term_mode(
        case,
        ms_path,
        prefix,
        dirty_only,
        niter,
        deconvolver,
        &[],
        0.0,
        threshold_jy,
        minor_cycle_length,
        casars_imager::SpectralMode::Cube,
        CubeWeightingOptions::default(),
        WTermMode::None,
        None,
    )
}

fn run_rust_imager_cube_task_default_case_with_clean_controls(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    clean: CubeCleanControls,
    weighting_options: CubeWeightingOptions<'_>,
) -> Result<RunSummary, String> {
    let _ = (casa_source_root(), casacore_source_root());
    let mut cube_axis = casa_ms::CubeAxisConfig::default();
    cube_axis.specmode = casa_ms::CubeSpecMode::Cube;
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: None,
        channel_start: None,
        channel_count: None,
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode: casars_imager::SpectralMode::Cube,
        cube_axis,
        weighting: case.weighting,
        per_channel_weight_density: weighting_options.per_channel_weight_density,
        use_pointing: false,
        uv_taper: parse_rust_uv_taper(weighting_options.uvtaper)?,
        restoring_beam_mode: weighting_options.restoring_beam_mode,
        deconvolver,
        nterms: 1,
        multiscale_scales: multiscale_scales.to_vec(),
        small_scale_bias,
        niter,
        gain: clean.gain,
        threshold_jy: clean.threshold_jy,
        nsigma: clean.nsigma,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: clean.minor_cycle_length,
        cyclefactor: clean.cyclefactor,
        min_psf_fraction: clean.min_psf_fraction,
        max_psf_fraction: clean.max_psf_fraction,
        hogbom_iteration_mode: clean.hogbom_iteration_mode,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only,
        write_preview_pngs: false,
    })
}

fn run_rust_imager_cube_case_with_solver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    threshold_jy: f32,
    minor_cycle_length: usize,
    spectral_mode: casars_imager::SpectralMode,
    weighting_options: CubeWeightingOptions<'_>,
) -> Result<RunSummary, String> {
    run_rust_imager_cube_case_with_solver_and_w_term_mode(
        case,
        ms_path,
        prefix,
        dirty_only,
        niter,
        deconvolver,
        multiscale_scales,
        small_scale_bias,
        threshold_jy,
        minor_cycle_length,
        spectral_mode,
        weighting_options,
        WTermMode::None,
        None,
    )
}

fn run_rust_imager_cube_case_with_solver_and_w_term_mode(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    deconvolver: Deconvolver,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    threshold_jy: f32,
    minor_cycle_length: usize,
    spectral_mode: casars_imager::SpectralMode,
    weighting_options: CubeWeightingOptions<'_>,
    w_term_mode: WTermMode,
    w_project_planes: Option<usize>,
) -> Result<RunSummary, String> {
    let _ = (casa_source_root(), casacore_source_root());
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: None,
        channel_start: case.channel_start_option(),
        channel_count: case.channel_count_option(),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode,
        cube_axis: casa_ms::CubeAxisConfig {
            specmode: match spectral_mode {
                casars_imager::SpectralMode::Cubedata => casa_ms::CubeSpecMode::Cubedata,
                _ => casa_ms::CubeSpecMode::Cube,
            },
            outframe: FrequencyRef::LSRK,
            veltype: casa_types::measures::doppler::DopplerRef::RADIO,
            interpolation: casa_ms::CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(casa_ms::CubeAxisValue::Channel(case.channel_start as i32)),
            width: Some(casa_ms::CubeAxisValue::Channel(1)),
        },
        weighting: case.weighting,
        per_channel_weight_density: weighting_options.per_channel_weight_density,
        use_pointing: false,
        uv_taper: parse_rust_uv_taper(weighting_options.uvtaper)?,
        restoring_beam_mode: weighting_options.restoring_beam_mode,
        deconvolver,
        nterms: 1,
        multiscale_scales: multiscale_scales.to_vec(),
        small_scale_bias,
        niter,
        gain: 0.1,
        threshold_jy,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length,
        cyclefactor: 1.0,
        min_psf_fraction: 0.1,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode,
        w_project_planes,
        dirty_only,
        write_preview_pngs: false,
    })
}

fn run_rust_imager_cube_case_with_options(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    options: CubeCaseOptions<'_>,
    dirty_only: bool,
    niter: usize,
) -> Result<RunSummary, String> {
    run_rust_imager_cube_case_with_options_and_weighting(
        case,
        ms_path,
        prefix,
        options,
        dirty_only,
        niter,
        CubeWeightingOptions::default(),
    )
}

fn run_rust_imager_cube_task_default_case_with_weighting(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    dirty_only: bool,
    niter: usize,
    weighting_options: CubeWeightingOptions<'_>,
) -> Result<RunSummary, String> {
    run_rust_imager_cube_task_default_case_with_clean_controls(
        case,
        ms_path,
        prefix,
        dirty_only,
        niter,
        Deconvolver::Hogbom,
        &[],
        0.0,
        CubeCleanControls {
            minor_cycle_length: niter.max(1),
            ..CubeCleanControls::default()
        },
        weighting_options,
    )
}

fn run_rust_imager_cube_case_with_options_and_weighting(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    options: CubeCaseOptions<'_>,
    dirty_only: bool,
    niter: usize,
    weighting_options: CubeWeightingOptions<'_>,
) -> Result<RunSummary, String> {
    run_rust_imager_spectral_cube_case_with_options_and_weighting(
        case,
        ms_path,
        prefix,
        casars_imager::SpectralMode::Cube,
        options,
        dirty_only,
        niter,
        weighting_options,
    )
}

fn run_rust_imager_spectral_cube_case_with_options_and_weighting(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    spectral_mode: casars_imager::SpectralMode,
    options: CubeCaseOptions<'_>,
    dirty_only: bool,
    niter: usize,
    weighting_options: CubeWeightingOptions<'_>,
) -> Result<RunSummary, String> {
    let veltype = options
        .veltype
        .parse::<casa_types::measures::doppler::DopplerRef>()
        .map_err(|error| format!("parse veltype {:?}: {error}", options.veltype))?;
    let outframe = options
        .outframe
        .parse::<FrequencyRef>()
        .map_err(|error| format!("parse outframe {:?}: {error}", options.outframe))?;
    let interpolation = match options.interpolation {
        "nearest" => casa_ms::CubeInterpolation::Nearest,
        "linear" => casa_ms::CubeInterpolation::Linear,
        "cubic" => casa_ms::CubeInterpolation::Cubic,
        other => return Err(format!("unsupported interpolation {other:?}")),
    };
    run_from_config(&CliConfig {
        ms: ms_path.to_path_buf(),
        imagename: prefix.to_path_buf(),
        imsize: case.imsize,
        cell_arcsec: case.cell_arcsec,
        field_ids: Some(case.field_ids.to_vec()),
        phasecenter_field: case.phasecenter_field,
        phasecenter: None,
        ddid: None,
        spw: Some(case.spw),
        spw_selector: Some(options.spw_selector.to_string()),
        channel_start: None,
        channel_count: Some(options.nchan),
        datacolumn: Some("DATA".to_string()),
        save_model: casars_imager::SaveModelMode::None,
        correlation: case.correlation.map(str::to_string),
        spectral_mode,
        cube_axis: casa_ms::CubeAxisConfig {
            specmode: match spectral_mode {
                casars_imager::SpectralMode::Cubedata => casa_ms::CubeSpecMode::Cubedata,
                _ => casa_ms::CubeSpecMode::Cube,
            },
            outframe,
            veltype,
            interpolation,
            rest_frequency_hz: Some(
                casa_ms::CubeAxisValue::parse(
                    options.restfreq,
                    casa_types::measures::doppler::DopplerRef::RADIO,
                )
                .map_err(|error| error.to_string())
                .and_then(|value| match value {
                    casa_ms::CubeAxisValue::FrequencyHz { hz, .. } => Ok(hz),
                    other => Err(format!("restfreq must be a frequency, found {other:?}")),
                })?,
            ),
            start: options.start.map(|value| value.to_rust_value(veltype)),
            width: options.width.map(|value| value.to_rust_value(veltype)),
        },
        weighting: case.weighting,
        per_channel_weight_density: weighting_options.per_channel_weight_density,
        use_pointing: false,
        uv_taper: parse_rust_uv_taper(weighting_options.uvtaper)?,
        restoring_beam_mode: weighting_options.restoring_beam_mode,
        deconvolver: Deconvolver::Hogbom,
        nterms: 1,
        multiscale_scales: Vec::new(),
        small_scale_bias: 0.0,
        niter,
        gain: 0.1,
        threshold_jy: 0.0,
        nsigma: 0.0,
        psf_cutoff: 0.35,
        mosaic_pb_limit: 0.1,
        pbcor: false,
        minor_cycle_length: niter.max(1),
        cyclefactor: 1.0,
        min_psf_fraction: 0.1,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        mask_boxes: Vec::new(),
        mask_image: None,
        w_term_mode: WTermMode::None,
        w_project_planes: None,
        dirty_only,
        write_preview_pngs: false,
    })
}

fn run_casa_tclean(ms_path: &Path, prefix: &Path, niter: usize) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field="0",
    spw="0:0",
    specmode="mfs",
    gridder="standard",
    weighting="natural",
    deconvolver="hogbom",
    imsize=128,
    cell="30arcsec",
    niter=int(os.environ["CASA_NITER"]),
    gain=0.1,
    threshold="0Jy",
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_NITER", niter.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    if let (Some(casa_root), Some(casacore_root)) = (casa_source_root(), casacore_source_root()) {
        eprintln!(
            "CASA imaging parity used casa={} casacore={}",
            git_head_commit(&casa_root).unwrap_or_else(|| "unknown".to_string()),
            git_head_commit(&casacore_root).unwrap_or_else(|| "unknown".to_string())
        );
    }
    Ok(())
}

fn run_casa_mstransform_default_cube_case(
    ms_path: &Path,
    outputvis_path: &Path,
    case: ParityCase<'_>,
    interpolation: &str,
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA mstransform");
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let outputvis_text = outputvis_path
        .to_str()
        .ok_or_else(|| format!("non-utf8 outputvis path {}", outputvis_path.display()))?;
    let script = r#"
import os
import shutil
from casatasks import mstransform

outputvis = os.environ["CASA_OUTPUTVIS"]
if os.path.exists(outputvis):
    shutil.rmtree(outputvis)

mstransform(
    vis=os.environ["CASA_VIS"],
    outputvis=outputvis,
    field=os.environ["CASA_FIELD"],
    phasecenter=int(os.environ["CASA_PHASECENTER"]),
    spw=os.environ["CASA_SPW"],
    datacolumn="DATA",
    keepflags=True,
    usewtspectrum=True,
    regridms=True,
    mode="channel",
    nchan=int(os.environ["CASA_NCHAN"]),
    start=int(os.environ["CASA_START"]),
    width=1,
    interpolation=os.environ["CASA_INTERPOLATION"],
    outframe="LSRK",
    veltype="radio",
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_OUTPUTVIS", outputvis_text)
        .env("CASA_FIELD", case.field_selector())
        .env(
            "CASA_PHASECENTER",
            case.default_phasecenter_field().to_string(),
        )
        .env("CASA_SPW", case.spw.to_string())
        .env("CASA_NCHAN", case.channel_count.to_string())
        .env("CASA_START", case.channel_start.to_string())
        .env("CASA_INTERPOLATION", interpolation)
        .output()
        .map_err(|error| format!("spawn casa mstransform: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "casa mstransform failed with status {}:\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

fn run_casa_tclean_case(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
) -> Result<(), String> {
    run_casa_tclean_case_with_solver(case, ms_path, prefix, niter, "hogbom", &[])
}

fn run_casa_tclean_case_with_deconvolver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
) -> Result<(), String> {
    run_casa_tclean_case_with_solver(case, ms_path, prefix, niter, deconvolver, &[])
}

fn run_casa_tclean_case_with_mtmfs(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    nterms: usize,
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    phasecenter=int(os.environ["CASA_PHASECENTER"]),
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="mfs",
    gridder="standard",
    weighting=os.environ["CASA_WEIGHTING"],
    deconvolver="mtmfs",
    nterms=int(os.environ["CASA_NTERMS"]),
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{os.environ["CASA_CELL_ARCSEC"]}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    cycleniter=int(os.environ["CASA_CYCLENITER"]),
    gain=0.1,
    threshold="0Jy",
    cyclefactor=float(os.environ["CASA_CYCLEFACTOR"]),
    minpsffraction=float(os.environ["CASA_MINPSFFRACTION"]),
    maxpsffraction=float(os.environ["CASA_MAXPSFFRACTION"]),
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env(
            "CASA_PHASECENTER",
            case.default_phasecenter_field().to_string(),
        )
        .env("CASA_SPW", case.cube_channel_spw_selector())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_NTERMS", nterms.to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .env("CASA_NITER", niter.to_string())
        .env("CASA_CYCLENITER", niter.max(1).to_string())
        .env("CASA_CYCLEFACTOR", "1.0")
        .env("CASA_MINPSFFRACTION", "0.1")
        .env("CASA_MAXPSFFRACTION", "0.8")
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    Ok(())
}

fn run_casa_tclean_case_with_wproject(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    wprojplanes: usize,
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    phasecenter=int(os.environ["CASA_PHASECENTER"]),
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="mfs",
    gridder="wproject",
    wprojplanes=int(os.environ["CASA_WPROJPLANES"]),
    weighting=os.environ["CASA_WEIGHTING"],
    deconvolver="hogbom",
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{os.environ["CASA_CELL_ARCSEC"]}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    robust=float(os.environ["CASA_ROBUST"]),
    gain=0.1,
    threshold="0Jy",
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    pblimit=-0.1,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env(
            "CASA_PHASECENTER",
            case.default_phasecenter_field().to_string(),
        )
        .env("CASA_SPW", case.cube_channel_spw_selector())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .env("CASA_NITER", niter.to_string())
        .env("CASA_WPROJPLANES", wprojplanes.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    Ok(())
}

fn run_casa_tclean_case_with_wproject_and_explicit_phasecenter(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    wprojplanes: usize,
    explicit_phasecenter: &str,
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    phasecenter=os.environ["CASA_PHASECENTER"],
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="mfs",
    gridder="wproject",
    wprojplanes=int(os.environ["CASA_WPROJPLANES"]),
    weighting=os.environ["CASA_WEIGHTING"],
    deconvolver="hogbom",
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{os.environ["CASA_CELL_ARCSEC"]}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    robust=float(os.environ["CASA_ROBUST"]),
    gain=0.1,
    threshold="0Jy",
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    pblimit=-0.1,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env("CASA_PHASECENTER", explicit_phasecenter)
        .env("CASA_SPW", case.cube_channel_spw_selector())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .env("CASA_NITER", niter.to_string())
        .env("CASA_WPROJPLANES", wprojplanes.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    Ok(())
}

fn run_casa_tclean_case_with_mosaic_and_explicit_phasecenter(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    explicit_phasecenter: &str,
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    phasecenter=os.environ["CASA_PHASECENTER"],
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="mfs",
    gridder="mosaic",
    weighting=os.environ["CASA_WEIGHTING"],
    deconvolver="hogbom",
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{os.environ["CASA_CELL_ARCSEC"]}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    robust=float(os.environ["CASA_ROBUST"]),
    gain=0.1,
    threshold="0Jy",
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    pblimit=0.1,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env("CASA_PHASECENTER", explicit_phasecenter)
        .env("CASA_SPW", case.cube_channel_spw_selector())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .env("CASA_NITER", niter.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    Ok(())
}

fn run_casa_tclean_case_with_mosaic(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    phasecenter=int(os.environ["CASA_PHASECENTER"]),
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="mfs",
    gridder="mosaic",
    weighting=os.environ["CASA_WEIGHTING"],
    deconvolver="hogbom",
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{os.environ["CASA_CELL_ARCSEC"]}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    robust=float(os.environ["CASA_ROBUST"]),
    gain=0.1,
    threshold="0Jy",
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    pblimit=0.1,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env(
            "CASA_PHASECENTER",
            case.default_phasecenter_field().to_string(),
        )
        .env("CASA_SPW", case.cube_channel_spw_selector())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .env("CASA_NITER", niter.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    Ok(())
}

fn run_casa_tclean_case_with_solver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
) -> Result<(), String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let scales = multiscale_scales
        .iter()
        .map(|scale| {
            if scale.fract() == 0.0 {
                format!("{}", *scale as i32)
            } else {
                scale.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let script = r#"
import os
from casatasks import tclean
tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    phasecenter=int(os.environ["CASA_PHASECENTER"]),
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="mfs",
    gridder="standard",
    weighting=os.environ["CASA_WEIGHTING"],
    deconvolver=os.environ["CASA_DECONVOLVER"],
    nterms=1,
    scales=[] if os.environ["CASA_SCALES"] == "" else [int(float(v)) for v in os.environ["CASA_SCALES"].split(",")],
    smallscalebias=float(os.environ["CASA_SMALLSCALEBIAS"]),
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{os.environ["CASA_CELL_ARCSEC"]}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    cycleniter=int(os.environ["CASA_CYCLENITER"]),
    robust=float(os.environ["CASA_ROBUST"]),
    gain=0.1,
    threshold="0Jy",
    cyclefactor=float(os.environ["CASA_CYCLEFACTOR"]),
    minpsffraction=float(os.environ["CASA_MINPSFFRACTION"]),
    maxpsffraction=float(os.environ["CASA_MAXPSFFRACTION"]),
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
)
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env(
            "CASA_PHASECENTER",
            case.default_phasecenter_field().to_string(),
        )
        .env("CASA_SPW", case.cube_channel_spw_selector())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_DECONVOLVER", deconvolver)
        .env("CASA_SCALES", scales)
        .env("CASA_SMALLSCALEBIAS", "0.0")
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .env("CASA_NITER", niter.to_string())
        .env("CASA_CYCLENITER", niter.max(1).to_string())
        .env("CASA_CYCLEFACTOR", "1.0")
        .env("CASA_MINPSFFRACTION", "0.1")
        .env("CASA_MAXPSFFRACTION", "0.8")
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    Ok(())
}

#[derive(Clone)]
enum CubeAxisStep<'a> {
    Channel(i32),
    Text(&'a str),
    FramedValue {
        python_literal: &'a str,
        value: casa_ms::CubeAxisValue,
    },
}

#[derive(Clone)]
struct CubeCaseOptions<'a> {
    spw_selector: &'a str,
    nchan: usize,
    start: Option<CubeAxisStep<'a>>,
    width: Option<CubeAxisStep<'a>>,
    outframe: &'a str,
    interpolation: &'a str,
    veltype: &'a str,
    restfreq: &'a str,
}

#[derive(Clone, Default)]
struct CubeWeightingOptions<'a> {
    per_channel_weight_density: bool,
    uvtaper: &'a [&'a str],
    restoring_beam_mode: RestoringBeamMode,
}

#[derive(Debug, Clone, Copy)]
struct CubeCleanControls {
    gain: f32,
    threshold_jy: f32,
    nsigma: f32,
    minor_cycle_length: usize,
    cyclefactor: f32,
    min_psf_fraction: f32,
    max_psf_fraction: f32,
    hogbom_iteration_mode: HogbomIterationMode,
}

impl Default for CubeCleanControls {
    fn default() -> Self {
        Self {
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            minor_cycle_length: 1,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CasaMinorCycleTrace {
    channel_index: usize,
    cycle_index: usize,
    start_reported_iteration: usize,
    reported_updates: usize,
    start_peak_residual_jy_per_beam: f32,
    end_peak_residual_jy_per_beam: f32,
    cycle_threshold_jy_per_beam: f32,
    stop_code: i32,
}

fn parse_rust_uv_taper(parts: &[&str]) -> Result<Option<GaussianUvTaper>, String> {
    match parts {
        [] => Ok(None),
        [major] => {
            let size = parse_rust_uv_taper_size(major)?;
            Ok(Some(GaussianUvTaper {
                major: size,
                minor: size,
                position_angle_rad: 0.0,
            }))
        }
        [major, minor] => Ok(Some(GaussianUvTaper {
            major: parse_rust_uv_taper_size(major)?,
            minor: parse_rust_uv_taper_size(minor)?,
            position_angle_rad: 0.0,
        })),
        [major, minor, pa] => Ok(Some(GaussianUvTaper {
            major: parse_rust_uv_taper_size(major)?,
            minor: parse_rust_uv_taper_size(minor)?,
            position_angle_rad: parse_degrees(pa)? * std::f64::consts::PI / 180.0,
        })),
        _ => Err(format!(
            "unsupported UV taper {parts:?}; expected MAJOR[,MINOR[,PA]]"
        )),
    }
}

fn parse_rust_uv_taper_size(text: &str) -> Result<UvTaperSize, String> {
    let trimmed = text.trim();
    let lower = trimmed.to_ascii_lowercase();
    if let Some(value) = lower.strip_suffix("arcsec") {
        let arcsec = value
            .trim()
            .parse::<f64>()
            .map_err(|error| format!("parse uvtaper arcsec {text:?}: {error}"))?;
        return Ok(UvTaperSize::ImageFwhmRad(
            arcsec * std::f64::consts::PI / (180.0 * 3600.0),
        ));
    }
    if let Some(value) = lower.strip_suffix("lambda") {
        let lambda = value
            .trim()
            .parse::<f64>()
            .map_err(|error| format!("parse uvtaper lambda {text:?}: {error}"))?;
        return Ok(UvTaperSize::BaselineHwhmLambda(lambda));
    }
    Err(format!(
        "unsupported uvtaper size {text:?}; expected arcsec or lambda units"
    ))
}

fn parse_degrees(text: &str) -> Result<f64, String> {
    text.trim()
        .strip_suffix("deg")
        .ok_or_else(|| format!("unsupported uvtaper position angle {text:?}; expected deg"))?
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("parse uvtaper position angle {text:?}: {error}"))
}

fn parse_threshold_jy_text(text: &str) -> Result<f32, String> {
    text.trim()
        .strip_suffix("Jy")
        .ok_or_else(|| format!("unsupported threshold {text:?}; expected Jy units"))?
        .trim()
        .parse::<f32>()
        .map_err(|error| format!("parse threshold {text:?}: {error}"))
}

impl CubeAxisStep<'_> {
    fn to_python_literal(&self) -> String {
        match self {
            Self::Channel(value) => value.to_string(),
            Self::Text(value) => format!("{value:?}"),
            Self::FramedValue { python_literal, .. } => (*python_literal).to_string(),
        }
    }

    fn to_rust_value(
        &self,
        veltype: casa_types::measures::doppler::DopplerRef,
    ) -> casa_ms::CubeAxisValue {
        match self {
            Self::Channel(value) => casa_ms::CubeAxisValue::Channel(*value),
            Self::Text(value) => {
                casa_ms::CubeAxisValue::parse(value, veltype).expect("parse cube axis value")
            }
            Self::FramedValue { value, .. } => value.clone(),
        }
    }
}

fn run_casa_tclean_cube_dirty_case(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    start: CubeAxisStep<'_>,
    width: CubeAxisStep<'_>,
) -> Result<(), String> {
    run_casa_tclean_cube_case(case, ms_path, prefix, 0, "hogbom", start, width, "0Jy").map(|_| ())
}

fn run_casa_tclean_cube_dirty_case_with_wproject(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    start: CubeAxisStep<'_>,
    width: CubeAxisStep<'_>,
    wprojplanes: usize,
) -> Result<(), String> {
    run_casa_tclean_cube_case_with_options_internal(
        case,
        ms_path,
        prefix,
        0,
        "hogbom",
        &[],
        0.0,
        "cube",
        false,
        &[],
        "",
        CubeCaseOptions {
            spw_selector: &case.cube_channel_spw_selector(),
            nchan: case.channel_count,
            start: Some(start),
            width: Some(width),
            outframe: "LSRK",
            interpolation: "nearest",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        "0Jy",
        "wproject",
        Some(wprojplanes),
    )
    .map(|_| ())
}

fn run_casa_tclean_cube_case(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    start: CubeAxisStep<'_>,
    width: CubeAxisStep<'_>,
    threshold: &str,
) -> Result<Value, String> {
    run_casa_tclean_cube_case_with_options_internal(
        case,
        ms_path,
        prefix,
        niter,
        deconvolver,
        &[],
        0.0,
        "cube",
        false,
        &[],
        "",
        CubeCaseOptions {
            spw_selector: &case.cube_channel_spw_selector(),
            nchan: case.channel_count,
            start: Some(start),
            width: Some(width),
            outframe: "LSRK",
            interpolation: "nearest",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        threshold,
        "standard",
        None,
    )
}

fn run_casa_tclean_cube_case_with_solver(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    start: CubeAxisStep<'_>,
    width: CubeAxisStep<'_>,
    threshold: &str,
) -> Result<Value, String> {
    run_casa_tclean_cube_case_with_options_internal(
        case,
        ms_path,
        prefix,
        niter,
        deconvolver,
        multiscale_scales,
        small_scale_bias,
        "cube",
        false,
        &[],
        "",
        CubeCaseOptions {
            spw_selector: &case.cube_channel_spw_selector(),
            nchan: case.channel_count,
            start: Some(start),
            width: Some(width),
            outframe: "LSRK",
            interpolation: "nearest",
            veltype: "radio",
            restfreq: "1.25GHz",
        },
        threshold,
        "standard",
        None,
    )
}

fn run_casa_tclean_cube_case_with_options(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    options: CubeCaseOptions<'_>,
    threshold: &str,
) -> Result<Value, String> {
    run_casa_tclean_cube_case_with_options_internal(
        case,
        ms_path,
        prefix,
        niter,
        deconvolver,
        multiscale_scales,
        small_scale_bias,
        "cube",
        false,
        &[],
        "",
        options,
        threshold,
        "standard",
        None,
    )
}

fn run_casa_tclean_cube_task_default_case_with_weighting(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    weighting_options: CubeWeightingOptions<'_>,
    threshold: &str,
) -> Result<Value, String> {
    run_casa_tclean_cube_task_default_case_with_clean_controls(
        case,
        ms_path,
        prefix,
        niter,
        deconvolver,
        multiscale_scales,
        small_scale_bias,
        weighting_options,
        CubeCleanControls {
            minor_cycle_length: niter.max(1),
            threshold_jy: parse_threshold_jy_text(threshold)?,
            ..CubeCleanControls::default()
        },
        threshold,
    )
}

fn run_casa_tclean_cube_task_default_case_with_clean_controls(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    weighting_options: CubeWeightingOptions<'_>,
    clean: CubeCleanControls,
    threshold: &str,
) -> Result<Value, String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let scales = multiscale_scales
        .iter()
        .map(|scale| {
            if scale.fract() == 0.0 {
                format!("{}", *scale as i32)
            } else {
                scale.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let uvtaper_literal = if weighting_options.uvtaper.is_empty() {
        "['']".to_string()
    } else {
        format!(
            "[{}]",
            weighting_options
                .uvtaper
                .iter()
                .map(|value| format!("{value:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let restoring_beam_literal = match weighting_options.restoring_beam_mode {
        RestoringBeamMode::PerPlane => "",
        RestoringBeamMode::Common => "common",
    };
    let script = format!(
        r#"
import os
import json
from casatasks import tclean
ret = tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode="cube",
    gridder="standard",
    weighting=os.environ["CASA_WEIGHTING"],
    robust=float(os.environ["CASA_ROBUST"]),
    perchanweightdensity=os.environ["CASA_PERCHANWEIGHTDENSITY"] == "true",
    uvtaper={uvtaper_literal},
    restoringbeam=os.environ["CASA_RESTORINGBEAM"],
    deconvolver=os.environ["CASA_DECONVOLVER"],
    nterms=1,
    scales=[] if os.environ["CASA_SCALES"] == "" else [int(float(v)) for v in os.environ["CASA_SCALES"].split(",")],
    smallscalebias=float(os.environ["CASA_SMALLSCALEBIAS"]),
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{{os.environ["CASA_CELL_ARCSEC"]}}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    gain=float(os.environ["CASA_GAIN"]),
    threshold=os.environ["CASA_THRESHOLD"],
    nsigma=float(os.environ["CASA_NSIGMA"]),
    cycleniter=int(os.environ["CASA_CYCLENITER"]),
    cyclefactor=float(os.environ["CASA_CYCLEFACTOR"]),
    minpsffraction=float(os.environ["CASA_MINPSFFRACTION"]),
    maxpsffraction=float(os.environ["CASA_MAXPSFFRACTION"]),
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    pblimit=-1e-05,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
    fullsummary=True,
)
print(json.dumps({{
    "iterdone": int(ret.get("iterdone", 0)),
    "nmajordone": int(ret.get("nmajordone", 0)),
    "stopcode": int(ret.get("stopcode", 0)),
    "maxpsfsidelobe": float(ret.get("maxpsfsidelobe", 0.0)),
    "minpsffraction": float(ret.get("minpsffraction", 0.0)),
    "maxpsffraction": float(ret.get("maxpsffraction", 0.0)),
    "cyclethreshold": float(ret.get("cyclethreshold", 0.0)),
    "summaryminor": ret.get("summaryminor"),
}}))
"#
    );
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env("CASA_SPW", case.spw.to_string())
        .env("CASA_STOKES", case.stokes())
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env(
            "CASA_PERCHANWEIGHTDENSITY",
            if weighting_options.per_channel_weight_density {
                "true"
            } else {
                "false"
            },
        )
        .env("CASA_RESTORINGBEAM", restoring_beam_literal)
        .env("CASA_DECONVOLVER", deconvolver)
        .env("CASA_SCALES", scales)
        .env("CASA_SMALLSCALEBIAS", small_scale_bias.to_string())
        .env("CASA_NITER", niter.to_string())
        .env("CASA_GAIN", clean.gain.to_string())
        .env("CASA_THRESHOLD", threshold)
        .env("CASA_NSIGMA", clean.nsigma.to_string())
        .env("CASA_CYCLENITER", clean.minor_cycle_length.to_string())
        .env("CASA_CYCLEFACTOR", clean.cyclefactor.to_string())
        .env("CASA_MINPSFFRACTION", clean.min_psf_fraction.to_string())
        .env("CASA_MAXPSFFRACTION", clean.max_psf_fraction.to_string())
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let summary_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| "missing CASA cube tclean JSON summary".to_string())?;
    serde_json::from_str(summary_line)
        .map_err(|error| format!("decode casa cube tclean summary: {error}; stdout={stdout}"))
}

fn run_casa_tclean_cubedata_case_with_options(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    per_channel_weight_density: bool,
    options: CubeCaseOptions<'_>,
    threshold: &str,
) -> Result<Value, String> {
    run_casa_tclean_cube_case_with_options_internal(
        case,
        ms_path,
        prefix,
        niter,
        deconvolver,
        multiscale_scales,
        small_scale_bias,
        "cubedata",
        per_channel_weight_density,
        &[],
        "",
        options,
        threshold,
        "standard",
        None,
    )
}

fn run_casa_tclean_cube_case_with_options_internal(
    case: ParityCase<'_>,
    ms_path: &Path,
    prefix: &Path,
    niter: usize,
    deconvolver: &str,
    multiscale_scales: &[f32],
    small_scale_bias: f32,
    specmode: &str,
    per_channel_weight_density: bool,
    uvtaper: &[&str],
    restoring_beam: &str,
    options: CubeCaseOptions<'_>,
    threshold: &str,
    gridder: &str,
    wprojplanes: Option<usize>,
) -> Result<Value, String> {
    let _guard = casa_tclean_lock().lock().expect("lock CASA tclean");
    let casa = discover_casa_python().ok_or_else(|| skip_reason_for_case(case))?;
    let prefix_text = prefix
        .to_str()
        .ok_or_else(|| format!("non-utf8 imagename prefix {}", prefix.display()))?;
    let scales = multiscale_scales
        .iter()
        .map(|scale| {
            if scale.fract() == 0.0 {
                format!("{}", *scale as i32)
            } else {
                scale.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let start_literal = options
        .start
        .map(|value| value.to_python_literal())
        .unwrap_or_else(|| "''".to_string());
    let width_literal = options
        .width
        .map(|value| value.to_python_literal())
        .unwrap_or_else(|| "''".to_string());
    let uvtaper_literal = if uvtaper.is_empty() {
        "['']".to_string()
    } else {
        format!(
            "[{}]",
            uvtaper
                .iter()
                .map(|value| format!("{value:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let phasecenter_literal = case.default_phasecenter_field().to_string();
    let wprojplanes_literal = wprojplanes
        .map(|value| value.to_string())
        .unwrap_or_else(|| "0".to_string());
    let script = format!(
        r#"
import os
import json
from casatasks import tclean
from casatools import measures, quanta
_me = measures()
_qa = quanta()
ret = tclean(
    vis=os.environ["CASA_VIS"],
    imagename=os.environ["CASA_IMAGENAME"],
    datacolumn="data",
    field=os.environ["CASA_FIELD"],
    spw=os.environ["CASA_SPW"],
    stokes=os.environ["CASA_STOKES"],
    specmode=os.environ["CASA_SPECMODE"],
    interpolation=os.environ["CASA_INTERPOLATION"],
    nchan=int(os.environ["CASA_NCHAN"]),
    start={start_literal},
    width={width_literal},
    veltype=os.environ["CASA_VELTYPE"],
    outframe=os.environ["CASA_OUTFRAME"],
    restfreq=[os.environ["CASA_RESTFREQ"]],
    gridder=os.environ["CASA_GRIDDER"],
    wprojplanes=int(os.environ["CASA_WPROJPLANES"]),
    weighting=os.environ["CASA_WEIGHTING"],
    robust=float(os.environ["CASA_ROBUST"]),
    perchanweightdensity=os.environ["CASA_PERCHANWEIGHTDENSITY"] == "true",
    uvtaper={uvtaper_literal},
    restoringbeam=os.environ["CASA_RESTORINGBEAM"],
    phasecenter={phasecenter_literal},
    deconvolver=os.environ["CASA_DECONVOLVER"],
    nterms=1,
    scales=[] if os.environ["CASA_SCALES"] == "" else [int(float(v)) for v in os.environ["CASA_SCALES"].split(",")],
    smallscalebias=float(os.environ["CASA_SMALLSCALEBIAS"]),
    imsize=int(os.environ["CASA_IMSIZE"]),
    cell=f'{{os.environ["CASA_CELL_ARCSEC"]}}arcsec',
    niter=int(os.environ["CASA_NITER"]),
    gain=0.1,
    threshold=os.environ["CASA_THRESHOLD"],
    restoration=True,
    calcpsf=True,
    calcres=True,
    restart=True,
    interactive=False,
    parallel=False,
    pbcor=False,
    pblimit=-1e-05,
    usemask="user",
    mask="",
    savemodel="none",
    psfcutoff=0.35,
    fullsummary=True,
)
print(json.dumps({{
    "iterdone": int(ret.get("iterdone", 0)),
    "nmajordone": int(ret.get("nmajordone", 0)),
    "stopcode": int(ret.get("stopcode", 0)),
    "maxpsfsidelobe": float(ret.get("maxpsfsidelobe", 0.0)),
    "minpsffraction": float(ret.get("minpsffraction", 0.0)),
    "maxpsffraction": float(ret.get("maxpsffraction", 0.0)),
    "cyclethreshold": float(ret.get("cyclethreshold", 0.0)),
    "summaryminor": ret.get("summaryminor"),
}}))
"#
    );
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_IMAGENAME", prefix_text)
        .env("CASA_FIELD", case.field_selector())
        .env("CASA_SPW", options.spw_selector)
        .env("CASA_STOKES", case.stokes())
        .env("CASA_SPECMODE", specmode)
        .env("CASA_WEIGHTING", case.casa_weighting())
        .env("CASA_ROBUST", case.robust().unwrap_or(0.5).to_string())
        .env("CASA_RESTORINGBEAM", restoring_beam)
        .env(
            "CASA_PERCHANWEIGHTDENSITY",
            if per_channel_weight_density {
                "true"
            } else {
                "false"
            },
        )
        .env("CASA_NCHAN", options.nchan.to_string())
        .env("CASA_INTERPOLATION", options.interpolation)
        .env("CASA_VELTYPE", options.veltype)
        .env("CASA_OUTFRAME", options.outframe)
        .env("CASA_RESTFREQ", options.restfreq)
        .env("CASA_GRIDDER", gridder)
        .env("CASA_WPROJPLANES", wprojplanes_literal)
        .env("CASA_DECONVOLVER", deconvolver)
        .env("CASA_SCALES", scales)
        .env("CASA_SMALLSCALEBIAS", small_scale_bias.to_string())
        .env("CASA_NITER", niter.to_string())
        .env("CASA_THRESHOLD", threshold)
        .env("CASA_IMSIZE", case.imsize.to_string())
        .env("CASA_CELL_ARCSEC", case.cell_arcsec.to_string())
        .output()
        .map_err(|error| format!("spawn casa tclean: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let summary_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| "missing CASA cube tclean JSON summary".to_string())?;
    serde_json::from_str(summary_line)
        .map_err(|error| format!("decode casa cube tclean summary: {error}; stdout={stdout}"))
}

fn extract_casa_cube_minor_cycle_traces(
    summary: &Value,
) -> Result<Vec<Vec<CasaMinorCycleTrace>>, String> {
    let rank_zero = summary
        .get("summaryminor")
        .and_then(Value::as_object)
        .and_then(|entries| entries.get("0"))
        .and_then(Value::as_object)
        .ok_or_else(|| "missing CASA summaryminor rank 0".to_string())?;
    let max_channel = rank_zero
        .keys()
        .filter_map(|key| key.parse::<usize>().ok())
        .max()
        .map(|value| value + 1)
        .unwrap_or(0);
    let mut traces = Vec::with_capacity(max_channel);
    for channel_index in 0..max_channel {
        let Some(channel_entry) = rank_zero.get(&channel_index.to_string()) else {
            traces.push(Vec::new());
            continue;
        };
        let stokes_zero = channel_entry
            .as_object()
            .and_then(|entries| entries.get("0"))
            .and_then(Value::as_object);
        let Some(channel_summary) = stokes_zero else {
            traces.push(Vec::new());
            continue;
        };
        let iter_done = value_array(channel_summary, "iterDone")?;
        let start_iter_done = value_array(channel_summary, "startIterDone")?;
        let start_peak_res = value_array(channel_summary, "startPeakRes")?;
        let peak_res = value_array(channel_summary, "peakRes")?;
        let cycle_thresh = value_array(channel_summary, "cycleThresh")?;
        let stop_code = value_array(channel_summary, "stopCode")?;
        let ncycles = iter_done.len();
        if ![
            start_iter_done.len(),
            start_peak_res.len(),
            peak_res.len(),
            cycle_thresh.len(),
            stop_code.len(),
        ]
        .into_iter()
        .all(|len| len == ncycles)
        {
            return Err(format!(
                "inconsistent CASA summaryminor lengths for channel {channel_index}"
            ));
        }
        traces.push(
            (0..ncycles)
                .map(|cycle_index| CasaMinorCycleTrace {
                    channel_index,
                    cycle_index,
                    start_reported_iteration: start_iter_done[cycle_index] as usize,
                    reported_updates: iter_done[cycle_index] as usize,
                    start_peak_residual_jy_per_beam: start_peak_res[cycle_index] as f32,
                    end_peak_residual_jy_per_beam: peak_res[cycle_index] as f32,
                    cycle_threshold_jy_per_beam: cycle_thresh[cycle_index] as f32,
                    stop_code: stop_code[cycle_index] as i32,
                })
                .collect(),
        );
    }
    Ok(traces)
}

fn value_array(summary: &serde_json::Map<String, Value>, key: &str) -> Result<Vec<f64>, String> {
    summary
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing CASA summaryminor field {key}"))?
        .iter()
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| format!("non-numeric CASA summaryminor field {key}: {value}"))
        })
        .collect()
}

fn assert_dirty_case_matches(
    case: ParityCase<'_>,
    rust_prefix: &Path,
    casa_prefix: &Path,
    residual_abs_tol: f32,
    residual_rel_tol: f32,
    psf_abs_tol: f32,
    psf_rel_tol: f32,
    expect_exact_sumwt: bool,
) {
    compare_image_headers(
        &rust_product(rust_prefix, "psf"),
        &casa_product(casa_prefix, "psf"),
        "",
        false,
    );
    compare_image_headers(
        &rust_product(rust_prefix, "residual"),
        &casa_product(casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let center = case.center();
    let points = [
        (center, center),
        (center.saturating_sub(1), center),
        (center, center.saturating_sub(1)),
        (center.saturating_sub(4), center.saturating_sub(4)),
    ];

    let rust_psf = read_image(&rust_product(rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(casa_prefix, "psf"));
    for (x, y) in points {
        assert_close(
            sample(&rust_psf, x, y),
            sample(&casa_psf, x, y),
            psf_abs_tol,
            psf_rel_tol,
            &format!("psf[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(casa_prefix, "residual"));
    for (x, y) in points {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            residual_abs_tol,
            residual_rel_tol,
            &format!("residual[{x},{y}]"),
        );
    }

    let rust_sumwt = read_scalar_image(&rust_product(rust_prefix, "sumwt"));
    let casa_sumwt = read_scalar_image(&casa_product(casa_prefix, "sumwt"));
    if expect_exact_sumwt {
        assert_close(rust_sumwt, casa_sumwt, 1.0, 0.05, "sumwt");
    } else {
        assert!(
            rust_sumwt.is_finite() && rust_sumwt > 0.0,
            "expected positive Rust sumwt"
        );
        assert!(
            casa_sumwt.is_finite() && casa_sumwt > 0.0,
            "expected positive CASA sumwt"
        );
    }
}

fn assert_clean_case_matches(
    case: ParityCase<'_>,
    rust_prefix: &Path,
    casa_prefix: &Path,
    model_abs_tol: f32,
    model_rel_tol: f32,
    image_abs_tol: f32,
    image_rel_tol: f32,
    residual_abs_tol: f32,
    residual_rel_tol: f32,
) {
    compare_image_headers(
        &rust_product(rust_prefix, "model"),
        &casa_product(casa_prefix, "model"),
        "Jy/pixel",
        false,
    );
    compare_image_headers(
        &rust_product(rust_prefix, "image"),
        &casa_product(casa_prefix, "image"),
        "Jy/beam",
        false,
    );
    compare_image_headers(
        &rust_product(rust_prefix, "residual"),
        &casa_product(casa_prefix, "residual"),
        "Jy/beam",
        true,
    );

    let center = case.center();
    let points = [
        (center, center),
        (center.saturating_sub(1), center),
        (center, center.saturating_sub(1)),
        (center.saturating_sub(4), center.saturating_sub(4)),
    ];

    let rust_model = read_image(&rust_product(rust_prefix, "model"));
    let casa_model = read_image(&casa_product(casa_prefix, "model"));
    for (x, y) in points {
        assert_close(
            sample(&rust_model, x, y),
            sample(&casa_model, x, y),
            model_abs_tol,
            model_rel_tol,
            &format!("model[{x},{y}]"),
        );
    }

    let rust_image = read_image(&rust_product(rust_prefix, "image"));
    let casa_image = read_image(&casa_product(casa_prefix, "image"));
    for (x, y) in points {
        assert_close(
            sample(&rust_image, x, y),
            sample(&casa_image, x, y),
            image_abs_tol,
            image_rel_tol,
            &format!("image[{x},{y}]"),
        );
    }

    let rust_residual = read_image(&rust_product(rust_prefix, "residual"));
    let casa_residual = read_image(&casa_product(casa_prefix, "residual"));
    for (x, y) in points {
        assert_close(
            sample(&rust_residual, x, y),
            sample(&casa_residual, x, y),
            residual_abs_tol,
            residual_rel_tol,
            &format!("residual[{x},{y}]"),
        );
    }
}

fn assert_mtmfs_dirty_products_track_casa(rust_prefix: &Path, casa_prefix: &Path) {
    for &(suffix, correlation_min, normalized_rms_max, normalized_max_max) in &[
        ("psf.tt0", 0.995, 0.02, 0.10),
        ("psf.tt1", 0.99, 0.05, 0.15),
        ("psf.tt2", 0.99, 0.05, 0.15),
        ("residual.tt0", 0.995, 0.02, 0.15),
        ("residual.tt1", 0.99, 0.05, 0.20),
    ] {
        assert_mtmfs_image_pair(
            rust_product(rust_prefix, suffix),
            casa_product(casa_prefix, suffix),
            correlation_min,
            normalized_rms_max,
            normalized_max_max,
        );
    }

    for suffix in ["sumwt.tt0", "sumwt.tt1", "sumwt.tt2"] {
        let rust_sumwt = read_scalar_image(&rust_product(rust_prefix, suffix));
        let casa_sumwt = read_scalar_image(&casa_product(casa_prefix, suffix));
        assert_close(rust_sumwt, casa_sumwt, 1.0, 0.10, suffix);
    }
}

fn assert_mtmfs_clean_products_track_casa(rust_prefix: &Path, casa_prefix: &Path) {
    for &(suffix, rms_max, max_abs_max, correlation_min) in &[
        ("model.tt0", 1.0e-3, 1.0e-2, None),
        // `tt1` is a low-amplitude, one-iteration Taylor model on this small
        // fixture; keep it bounded by RMS and correlation while allowing a
        // slightly larger localized peak delta across CASA builds.
        ("model.tt1", 1.0e-3, 5.0e-2, Some(0.98)),
    ] {
        let rust_model = read_image(&rust_product(rust_prefix, suffix));
        let casa_model = read_image(&casa_product(casa_prefix, suffix));
        let stats = image_difference_stats(&rust_model, &casa_model);
        if let Some(correlation_min) = correlation_min {
            assert!(
                stats.correlation >= correlation_min,
                "{} correlation too low: {:?}",
                suffix,
                stats
            );
        }
        assert!(
            stats.rms <= rms_max,
            "{} RMS diff too large: {:?}",
            suffix,
            stats
        );
        assert!(
            stats.max_abs <= max_abs_max,
            "{} max diff too large: {:?}",
            suffix,
            stats
        );
    }

    for &(suffix, correlation_min, normalized_rms_max, normalized_max_max) in &[
        ("image.tt0", 0.95, 0.025, 0.15),
        // `tt1` is low-amplitude on this tiny two-channel fixture, so
        // normalized error is the stronger gate than raw correlation.
        ("image.tt1", 0.90, 0.08, 0.30),
        // Residual Taylor terms can be close to noise after a short clean,
        // which makes correlation less stable than bounded normalized error.
        ("residual.tt0", 0.95, 0.03, 0.20),
        ("residual.tt1", 0.95, 0.10, 0.35),
    ] {
        assert_mtmfs_image_pair(
            rust_product(rust_prefix, suffix),
            casa_product(casa_prefix, suffix),
            correlation_min,
            normalized_rms_max,
            normalized_max_max,
        );
    }

    let casa_image_tt0 = read_image(&casa_product(casa_prefix, "image.tt0"));
    let peak = peak_location(&casa_image_tt0).expect("casa mtmfs image.tt0 peak");
    let rust_alpha = read_image(&rust_product(rust_prefix, "alpha"));
    let casa_alpha = read_image(&casa_product(casa_prefix, "alpha"));
    assert_close(
        sample(&rust_alpha, peak.0, peak.1),
        sample(&casa_alpha, peak.0, peak.1),
        0.25,
        0.25,
        "alpha at image.tt0 peak",
    );
    let rust_alpha_error = read_image(&rust_product(rust_prefix, "alpha.error"));
    let casa_alpha_error = read_image(&casa_product(casa_prefix, "alpha.error"));
    assert_close(
        sample(&rust_alpha_error, peak.0, peak.1),
        sample(&casa_alpha_error, peak.0, peak.1),
        0.25,
        0.25,
        "alpha.error at image.tt0 peak",
    );
}

fn assert_mtmfs_image_pair(
    rust_path: PathBuf,
    casa_path: PathBuf,
    correlation_min: f32,
    normalized_rms_max: f32,
    normalized_max_max: f32,
) {
    let rust_image = read_image(&rust_path);
    let casa_image = read_image(&casa_path);
    let stats = image_difference_stats(&rust_image, &casa_image);
    let casa_peak = casa_image
        .iter()
        .map(|value| value.abs())
        .fold(0.0f32, f32::max)
        .max(1.0e-6);
    let normalized_rms = stats.rms / casa_peak;
    let normalized_max = stats.max_abs / casa_peak;
    assert!(
        stats.correlation >= correlation_min,
        "{} correlation too low: {:?}",
        rust_path.display(),
        stats
    );
    assert!(
        normalized_rms <= normalized_rms_max,
        "{} normalized RMS diff too large: peak={} stats={:?}",
        rust_path.display(),
        casa_peak,
        stats
    );
    assert!(
        normalized_max <= normalized_max_max,
        "{} normalized max diff too large: peak={} stats={:?}",
        rust_path.display(),
        casa_peak,
        stats
    );
}

fn summarize_image_in_casa(image_path: &Path) -> Result<Value, String> {
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let script = r#"
import json
import os
from casatools import image

ia = image()
if not ia.open(os.environ["CASA_IMAGE"]):
    raise RuntimeError("failed to open image")
cs = ia.coordsys()
summary = {
    "shape": [int(v) for v in ia.shape()],
    "brightnessunit": ia.brightnessunit(),
    "axisnames": list(cs.names()),
}
try:
    summary["has_beam"] = bool(ia.restoringbeam())
except Exception:
    summary["has_beam"] = False
cs.done()
ia.done()
print(json.dumps(summary))
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_IMAGE", image_path)
        .output()
        .map_err(|error| format!("spawn casa image summary: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode casa image summary: {error}"))
}

fn sample_image_points_in_casa(
    image_path: &Path,
    points: &[[usize; 4]],
) -> Result<Vec<f32>, String> {
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let script = r#"
import json
import os
from casatools import image

ia = image()
if not ia.open(os.environ["CASA_IMAGE"]):
    raise RuntimeError("failed to open image")
points = json.loads(os.environ["CASA_POINTS"])
values = []
for point in points:
    chunk = ia.getchunk(blc=point, trc=point)
    values.append(float(chunk.reshape(-1)[0]))
ia.done()
print(json.dumps(values))
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_IMAGE", image_path)
        .env(
            "CASA_POINTS",
            serde_json::to_string(points)
                .map_err(|error| format!("encode sample points: {error}"))?,
        )
        .output()
        .map_err(|error| format!("spawn casa image sampler: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode casa image samples: {error}"))
}

fn read_channel_plane_in_casa(image_path: &Path, channel: usize) -> Result<Array2<f32>, String> {
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let script = r#"
import json
import os
from casatools import image

ia = image()
if not ia.open(os.environ["CASA_IMAGE"]):
    raise RuntimeError("failed to open image")
shape = ia.shape()
chan = int(os.environ["CASA_CHANNEL"])
chunk = ia.getchunk(
    blc=[0, 0, 0, chan],
    trc=[shape[0] - 1, shape[1] - 1, 0, chan],
)
plane = chunk[:, :, 0, 0]
ia.done()
print(json.dumps(plane.tolist()))
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_IMAGE", image_path)
        .env("CASA_CHANNEL", channel.to_string())
        .output()
        .map_err(|error| format!("spawn casa image plane reader: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    let rows: Vec<Vec<f32>> = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode casa image plane: {error}"))?;
    let nx = rows.len();
    let ny = rows.first().map_or(0, Vec::len);
    if ny == 0 || nx == 0 {
        return Err("CASA image plane was empty".to_string());
    }
    if rows.iter().any(|row| row.len() != ny) {
        return Err("CASA image plane rows had inconsistent lengths".to_string());
    }
    let data = rows.into_iter().flatten().collect::<Vec<_>>();
    Array2::from_shape_vec((nx, ny), data)
        .map_err(|error| format!("reshape CASA image plane: {error}"))
}

fn read_cube_planes_in_casa(image_path: &Path) -> Result<Vec<Array2<f32>>, String> {
    let casa = discover_casa_python().ok_or_else(skip_reason)?;
    let script = r#"
import json
import os
from casatools import image

ia = image()
if not ia.open(os.environ["CASA_IMAGE"]):
    raise RuntimeError("failed to open image")
shape = ia.shape()
chunk = ia.getchunk(
    blc=[0, 0, 0, 0],
    trc=[shape[0] - 1, shape[1] - 1, 0, shape[3] - 1],
)
planes = [chunk[:, :, 0, chan].tolist() for chan in range(shape[3])]
ia.done()
print(json.dumps(planes))
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_IMAGE", image_path)
        .output()
        .map_err(|error| format!("spawn casa cube reader: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }
    let planes: Vec<Vec<Vec<f32>>> = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode casa cube planes: {error}"))?;
    planes
        .into_iter()
        .map(|rows| {
            let nx = rows.len();
            let ny = rows.first().map_or(0, Vec::len);
            if ny == 0 || nx == 0 {
                return Err("CASA cube plane was empty".to_string());
            }
            if rows.iter().any(|row| row.len() != ny) {
                return Err("CASA cube plane rows had inconsistent lengths".to_string());
            }
            let data = rows.into_iter().flatten().collect::<Vec<_>>();
            Array2::from_shape_vec((nx, ny), data)
                .map_err(|error| format!("reshape CASA cube plane: {error}"))
        })
        .collect()
}

fn compare_image_headers(
    rust_path: &Path,
    casa_path: &Path,
    expected_units: &str,
    allow_blank_casa_units: bool,
) {
    let rust = PagedImage::<f32>::open(rust_path).expect("open rust product");
    let casa = PagedImage::<f32>::open(casa_path).expect("open casa product");
    assert_eq!(
        rust.shape(),
        casa.shape(),
        "shape mismatch for {}",
        rust_path.display()
    );
    assert_eq!(
        rust.units(),
        expected_units,
        "unexpected units for {}",
        rust_path.display()
    );
    if !(allow_blank_casa_units && casa.units().is_empty()) {
        assert_eq!(
            casa.units(),
            expected_units,
            "unexpected CASA units for {}",
            casa_path.display()
        );
    }
    assert_eq!(rust.axis_names(), casa.axis_names(), "axis-name mismatch");
    assert_eq!(
        rust.coordinates().n_coordinates(),
        casa.coordinates().n_coordinates(),
        "coordinate-count mismatch"
    );
}

#[derive(Debug)]
struct SpectralHeaderSummary {
    frequency_ref: String,
    reference_value_hz: f64,
    increment_hz: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct CapturedCubeMinorCyclePlane {
    residual: Array2<f32>,
    model: Array2<f32>,
}

#[derive(Debug, Clone, PartialEq)]
struct HogbomReplay2d {
    iterdone: usize,
    residual: Vec<f32>,
}

fn read_captured_cube_minor_cycle_plane(directory: &Path) -> CapturedCubeMinorCyclePlane {
    let meta = fs::read_to_string(directory.join("meta.txt")).expect("read captured meta");
    let mut nx = None::<usize>;
    let mut ny = None::<usize>;
    for line in meta.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "nx" => nx = Some(value.parse().expect("captured nx")),
            "ny" => ny = Some(value.parse().expect("captured ny")),
            _ => {}
        }
    }
    let nx = nx.expect("captured nx");
    let ny = ny.expect("captured ny");
    let values = fs::read_to_string(directory.join("residual.txt"))
        .expect("read captured residual")
        .lines()
        .map(|line| line.parse::<f32>().expect("captured residual value"))
        .collect::<Vec<_>>();
    let residual = Array2::from_shape_vec((nx, ny), values).expect("captured residual shape");
    let model_values = fs::read_to_string(directory.join("model.txt"))
        .expect("read captured model")
        .lines()
        .map(|line| line.parse::<f32>().expect("captured model value"))
        .collect::<Vec<_>>();
    let model = Array2::from_shape_vec((nx, ny), model_values).expect("captured model shape");
    CapturedCubeMinorCyclePlane { residual, model }
}

fn read_captured_cube_model_cube(directory: &Path) -> Vec<Array2<f32>> {
    let meta = fs::read_to_string(directory.join("meta.txt")).expect("read captured meta");
    let mut nx = None::<usize>;
    let mut ny = None::<usize>;
    let mut channel_count = None::<usize>;
    for line in meta.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "nx" => nx = Some(value.parse().expect("captured nx")),
            "ny" => ny = Some(value.parse().expect("captured ny")),
            "channel_count" => channel_count = Some(value.parse().expect("captured channel count")),
            _ => {}
        }
    }
    let nx = nx.expect("captured nx");
    let ny = ny.expect("captured ny");
    let channel_count = channel_count.expect("captured channel count");
    (0..channel_count)
        .map(|channel_index| {
            let values =
                fs::read_to_string(directory.join(format!("model_channel_{channel_index}.txt")))
                    .expect("read captured model channel")
                    .lines()
                    .map(|line| line.parse::<f32>().expect("captured model channel value"))
                    .collect::<Vec<_>>();
            Array2::from_shape_vec((nx, ny), values).expect("captured model channel shape")
        })
        .collect()
}

fn replay_rust_hogbom_minor_cycle_2d(
    psf: &[f32],
    residual: &[f32],
    shape: [usize; 2],
    gain: f32,
    threshold: f32,
    cycle_reported_niter: usize,
) -> HogbomReplay2d {
    let [nx, ny] = shape;
    let mut residual = residual.to_vec();
    let mut cycle_component_updates = 0usize;
    let cycle_component_budget = cycle_reported_niter.saturating_add(1);
    while cycle_component_updates < cycle_component_budget {
        let Some((peak_index, peak_value)) = peak_location_flat_xy(&residual, [nx, ny]) else {
            break;
        };
        if peak_value.abs() < threshold {
            break;
        }
        let component = gain * peak_value;
        subtract_shifted_psf_flat(&mut residual, psf, [nx, ny], peak_index, component);
        cycle_component_updates += 1;
    }
    HogbomReplay2d {
        iterdone: cycle_component_updates.min(cycle_reported_niter),
        residual,
    }
}

fn peak_location_flat_xy(values: &[f32], shape: [usize; 2]) -> Option<((usize, usize), f32)> {
    let [nx, ny] = shape;
    if values.len() != nx * ny {
        return None;
    }
    values
        .iter()
        .copied()
        .enumerate()
        .max_by(|(_, left), (_, right)| {
            left.abs()
                .partial_cmp(&right.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(index, value)| ((index / ny, index % ny), value))
}

fn subtract_shifted_psf_flat(
    residual: &mut [f32],
    psf: &[f32],
    shape: [usize; 2],
    peak_index: (usize, usize),
    component: f32,
) {
    let [nx, ny] = shape;
    let center = (nx / 2, ny / 2);
    for x in 0..nx {
        let psf_x = x as isize + center.0 as isize - peak_index.0 as isize;
        if !(0..nx as isize).contains(&psf_x) {
            continue;
        }
        for y in 0..ny {
            let psf_y = y as isize + center.1 as isize - peak_index.1 as isize;
            if !(0..ny as isize).contains(&psf_y) {
                continue;
            }
            let residual_index = x * ny + y;
            let psf_index = psf_x as usize * ny + psf_y as usize;
            residual[residual_index] -= component * psf[psf_index];
        }
    }
}

fn spectral_header_summary_full(path: &Path) -> SpectralHeaderSummary {
    let image = PagedImage::<f32>::open(path).expect("open image");
    let coords = image.coordinates();
    let index = coords
        .find_coordinate(casa_coordinates::CoordinateType::Spectral)
        .expect("spectral coordinate");
    let coord = coords.coordinate(index);
    let reference_value_hz = coord.reference_value()[0];
    let increment_hz = coord.increment()[0];
    let record = coord.to_record();
    let frequency_ref = match record.get("frequency_ref") {
        Some(casa_types::Value::Scalar(casa_types::ScalarValue::String(value))) => value.clone(),
        other => panic!("unexpected spectral frequency_ref record field: {other:?}"),
    };
    SpectralHeaderSummary {
        frequency_ref,
        reference_value_hz,
        increment_hz,
    }
}

fn spectral_header_summary(path: &Path) -> (String, f64) {
    let summary = spectral_header_summary_full(path);
    (summary.frequency_ref, summary.reference_value_hz)
}

fn image_beam_summaries(path: &Path) -> Vec<(usize, usize, f64, f64, f64)> {
    let image = PagedImage::<f32>::open(path).expect("open image");
    let info = image.image_info().expect("image info");
    let beam_set = info.beam_set;
    let (nchan, nstokes) = beam_set.shape();
    if nchan == 0 || nstokes == 0 {
        return Vec::new();
    }
    let expand = if beam_set.is_single() {
        let beam = *beam_set.get_beam().expect("single beam");
        vec![(0usize, 0usize, beam)]
    } else {
        let mut beams = Vec::with_capacity(nchan * nstokes);
        for chan in 0..nchan {
            for stokes in 0..nstokes {
                beams.push((chan, stokes, *beam_set.beam(chan, stokes)));
            }
        }
        beams
    };
    expand
        .into_iter()
        .map(|(chan, stokes, beam): (usize, usize, GaussianBeam)| {
            (
                chan,
                stokes,
                beam.major_in("arcsec").expect("beam major arcsec"),
                beam.minor_in("arcsec").expect("beam minor arcsec"),
                beam.position_angle_in("deg").expect("beam pa deg"),
            )
        })
        .collect()
}

fn image_beam_areas_arcsec2(path: &Path) -> Vec<(usize, usize, f64)> {
    let beam_area_factor = std::f64::consts::PI / (4.0 * std::f64::consts::LN_2);
    image_beam_summaries(path)
        .into_iter()
        .map(|(chan, stokes, major_arcsec, minor_arcsec, _)| {
            (chan, stokes, beam_area_factor * major_arcsec * minor_arcsec)
        })
        .collect()
}

fn single_beam_summary(path: &Path) -> (f64, f64, f64) {
    let image = PagedImage::<f32>::open(path).expect("open image");
    let info = image.image_info().expect("image info");
    let beam = if let Some(beam) = info.beam_set.single_beam() {
        beam
    } else {
        *info.beam_set.get_beam().expect("single beam")
    };
    (
        beam.major_in("arcsec").expect("beam major arcsec"),
        beam.minor_in("arcsec").expect("beam minor arcsec"),
        beam.position_angle_in("deg").expect("beam pa deg"),
    )
}

fn image_beam_set(path: &Path) -> ImageBeamSet {
    let image = PagedImage::<f32>::open(path).expect("open image");
    image.image_info().expect("image info").beam_set.clone()
}

fn spectral_world_values(path: &Path) -> Vec<f64> {
    let image = PagedImage::<f32>::open(path).expect("open image");
    let shape = image.shape();
    let nchan = shape[3];
    let coords = image.coordinates();
    (0..nchan)
        .map(|chan| {
            coords
                .to_world(&[
                    (shape[0] / 2) as f64,
                    (shape[1] / 2) as f64,
                    0.0,
                    chan as f64,
                ])
                .expect("convert cube pixel to world")[3]
        })
        .collect()
}

fn assert_beam_area_relation(
    lhs: &[(usize, usize, f64)],
    rhs: &[(usize, usize, f64)],
    relation: impl Fn(f64, f64) -> bool,
    label: &str,
) {
    assert_eq!(
        lhs.len(),
        rhs.len(),
        "{label}: channel/stokes beam counts differ ({} vs {})",
        lhs.len(),
        rhs.len()
    );
    for ((lhs_chan, lhs_stokes, lhs_area), (rhs_chan, rhs_stokes, rhs_area)) in lhs.iter().zip(rhs)
    {
        assert_eq!(
            (*lhs_chan, *lhs_stokes),
            (*rhs_chan, *rhs_stokes),
            "{label}: mismatched channel/stokes positions"
        );
        assert!(
            relation(*lhs_area, *rhs_area),
            "{label}: beam area relation failed at chan={lhs_chan}, stokes={lhs_stokes}: lhs={} rhs={}",
            lhs_area,
            rhs_area
        );
    }
}

fn psf_half_max_widths(path: &Path) -> Vec<(usize, usize, usize)> {
    let image = read_image(path);
    let shape = image.shape();
    let nx = shape[0];
    let ny = shape[1];
    let nchan = shape[3];
    let cx = nx / 2;
    let cy = ny / 2;
    let mut widths = Vec::with_capacity(nchan);
    for chan in 0..nchan {
        let peak = sample_channel(&image, cx, cy, chan);
        let half = peak * 0.5;
        let mut x_lo = cx;
        while x_lo > 0 && sample_channel(&image, x_lo, cy, chan) >= half {
            x_lo -= 1;
        }
        let mut x_hi = cx;
        while x_hi + 1 < nx && sample_channel(&image, x_hi, cy, chan) >= half {
            x_hi += 1;
        }
        let mut y_lo = cy;
        while y_lo > 0 && sample_channel(&image, cx, y_lo, chan) >= half {
            y_lo -= 1;
        }
        let mut y_hi = cy;
        while y_hi + 1 < ny && sample_channel(&image, cx, y_hi, chan) >= half {
            y_hi += 1;
        }
        widths.push((chan, x_hi.saturating_sub(x_lo), y_hi.saturating_sub(y_lo)));
    }
    widths
}

fn read_image(path: &Path) -> ArrayD<f32> {
    let image = PagedImage::<f32>::open(path).expect("open image");
    image
        .get_slice(&[0, 0, 0, 0], image.shape())
        .expect("read image slice")
}

fn read_scalar_image(path: &Path) -> f32 {
    let image = PagedImage::<f32>::open(path).expect("open image");
    let slice = image
        .get_slice(&[0, 0, 0, 0], image.shape())
        .expect("read image slice");
    slice[IxDyn(&[0, 0, 0, 0])]
}

fn sample(array: &ArrayD<f32>, x: usize, y: usize) -> f32 {
    array[IxDyn(&[x, y, 0, 0])]
}

fn sample_channel(array: &ArrayD<f32>, x: usize, y: usize, chan: usize) -> f32 {
    array[IxDyn(&[x, y, 0, chan])]
}

fn sample_scalar_channel(array: &ArrayD<f32>, chan: usize) -> f32 {
    array[IxDyn(&[0, 0, 0, chan])]
}

fn extract_channel_plane(array: &ArrayD<f32>, chan: usize) -> Array2<f32> {
    let nx = array.shape()[0];
    let ny = array.shape()[1];
    let mut plane = Array2::<f32>::zeros((nx, ny));
    for x in 0..nx {
        for y in 0..ny {
            plane[(x, y)] = array[IxDyn(&[x, y, 0, chan])];
        }
    }
    plane
}

fn peak_location_in_plane(plane: &Array2<f32>) -> Option<(usize, usize)> {
    let (nx, ny) = plane.dim();
    let mut best = None::<((usize, usize), f32)>;
    for x in 0..nx {
        for y in 0..ny {
            let value = plane[(x, y)];
            match best {
                None => best = Some(((x, y), value)),
                Some((_, best_value)) if value.abs() > best_value.abs() => {
                    best = Some(((x, y), value));
                }
                _ => {}
            }
        }
    }
    best.map(|(index, _)| index)
}

fn plane_difference_stats(left: &Array2<f32>, right: &Array2<f32>) -> ImageDifferenceStats {
    assert_eq!(left.dim(), right.dim(), "plane shape mismatch");
    let len = left.len().max(1) as f64;
    let left_mean = left.iter().map(|value| *value as f64).sum::<f64>() / len;
    let right_mean = right.iter().map(|value| *value as f64).sum::<f64>() / len;
    let mut max_abs = 0.0f32;
    let mut sum_sq = 0.0f64;
    let mut cov = 0.0f64;
    let mut left_var = 0.0f64;
    let mut right_var = 0.0f64;
    let mut count = 0usize;
    for ((x, y), left_value) in left.indexed_iter() {
        let diff = *left_value - right[(x, y)];
        max_abs = max_abs.max(diff.abs());
        sum_sq += (diff as f64) * (diff as f64);
        let left_centered = *left_value as f64 - left_mean;
        let right_centered = right[(x, y)] as f64 - right_mean;
        cov += left_centered * right_centered;
        left_var += left_centered * left_centered;
        right_var += right_centered * right_centered;
        count += 1;
    }
    ImageDifferenceStats {
        max_abs,
        rms: if count == 0 {
            0.0
        } else {
            (sum_sq / count as f64).sqrt() as f32
        },
        correlation: if left_var == 0.0 || right_var == 0.0 {
            if max_abs == 0.0 { 1.0 } else { 0.0 }
        } else {
            (cov / (left_var.sqrt() * right_var.sqrt())) as f32
        },
    }
}

#[test]
#[ignore = "diagnostic for Backlog 11.6 raw cube PSF parity on refim_point common-beam case"]
fn cube_common_restoringbeam_psf_header_and_plane_compare_on_refim_point() {
    let case = ParityCase {
        dataset_rel: "measurementset/vla/refim_point.ms",
        field_ids: &[0],
        phasecenter_field: Some(0),
        spw: 0,
        channel_start: 0,
        channel_count: 20,
        correlation: None,
        weighting: WeightingMode::Natural,
        imsize: 100,
        cell_arcsec: 10.0,
    };
    if !parity_case_available(case) {
        eprintln!("{}", skip_reason_for_case(case));
        return;
    }

    let ms_path = dataset_path(case.dataset_rel).expect("dataset");
    let temp = tempdir().expect("tempdir");
    let staged_ms_path =
        stage_measurement_set(&ms_path, temp.path(), "refim_point.ms").expect("stage refim_point");

    let common = CubeWeightingOptions {
        per_channel_weight_density: false,
        uvtaper: &[],
        restoring_beam_mode: RestoringBeamMode::Common,
    };
    let rust_prefix = temp.path().join("rust-refim-point-common-restoringbeam");
    let casa_prefix = temp.path().join("casa-refim-point-common-restoringbeam");
    run_rust_imager_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &rust_prefix,
        false,
        10,
        common.clone(),
    )
    .expect("run rust common restoring beam");
    run_casa_tclean_cube_task_default_case_with_weighting(
        case,
        &staged_ms_path,
        &casa_prefix,
        10,
        "hogbom",
        &[],
        0.0,
        common,
        "0Jy",
    )
    .expect("run casa common restoring beam");

    eprintln!(
        "RUST image spectral header: {:?}",
        spectral_header_summary_full(&rust_product(&rust_prefix, "image"))
    );
    eprintln!(
        "CASA image spectral header: {:?}",
        spectral_header_summary_full(&casa_product(&casa_prefix, "image"))
    );
    eprintln!(
        "RUST psf spectral header: {:?}",
        spectral_header_summary_full(&rust_product(&rust_prefix, "psf"))
    );
    eprintln!(
        "CASA psf spectral header: {:?}",
        spectral_header_summary_full(&casa_product(&casa_prefix, "psf"))
    );
    eprintln!(
        "RUST image spectral worlds: {:?}",
        spectral_world_values(&rust_product(&rust_prefix, "image"))
    );
    eprintln!(
        "CASA image spectral worlds: {:?}",
        spectral_world_values(&casa_product(&casa_prefix, "image"))
    );
    eprintln!(
        "RUST psf spectral worlds: {:?}",
        spectral_world_values(&rust_product(&rust_prefix, "psf"))
    );
    eprintln!(
        "CASA psf spectral worlds: {:?}",
        spectral_world_values(&casa_product(&casa_prefix, "psf"))
    );
    eprintln!(
        "RUST PSF beams: {:?}",
        image_beam_summaries(&rust_product(&rust_prefix, "psf"))
    );
    eprintln!(
        "CASA PSF beams: {:?}",
        image_beam_summaries(&casa_product(&casa_prefix, "psf"))
    );

    let rust_psf = read_image(&rust_product(&rust_prefix, "psf"));
    let casa_psf = read_image(&casa_product(&casa_prefix, "psf"));
    for &channel in &[0usize, 1, 15, 19] {
        let rust_plane = extract_channel_plane(&rust_psf, channel);
        let casa_plane = extract_channel_plane(&casa_psf, channel);
        let rust_dyn = rust_plane.clone().into_dyn();
        let casa_dyn = casa_plane.clone().into_dyn();
        eprintln!(
            "channel {} psf diff stats: {:?}",
            channel,
            image_difference_stats(&rust_dyn, &casa_dyn)
        );
        eprintln!(
            "channel {} center rust/casa: {} / {}",
            channel,
            rust_plane[(case.imsize / 2, case.imsize / 2)],
            casa_plane[(case.imsize / 2, case.imsize / 2)]
        );
    }

    panic!("diagnostic complete");
}

#[derive(Debug)]
struct ImageDifferenceStats {
    rms: f32,
    max_abs: f32,
    correlation: f32,
}

fn image_difference_stats(left: &ArrayD<f32>, right: &ArrayD<f32>) -> ImageDifferenceStats {
    assert_eq!(left.shape(), right.shape(), "image shape mismatch");
    let len = left.len().max(1) as f64;
    let left_mean = left.iter().map(|value| *value as f64).sum::<f64>() / len;
    let right_mean = right.iter().map(|value| *value as f64).sum::<f64>() / len;
    let mut sum_sq = 0.0f64;
    let mut max_abs = 0.0f64;
    let mut cov = 0.0f64;
    let mut left_var = 0.0f64;
    let mut right_var = 0.0f64;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let delta = *left_value as f64 - *right_value as f64;
        sum_sq += delta * delta;
        max_abs = max_abs.max(delta.abs());
        let left_centered = *left_value as f64 - left_mean;
        let right_centered = *right_value as f64 - right_mean;
        cov += left_centered * right_centered;
        left_var += left_centered * left_centered;
        right_var += right_centered * right_centered;
    }
    let correlation = if left_var == 0.0 || right_var == 0.0 {
        if max_abs == 0.0 { 1.0 } else { 0.0 }
    } else {
        (cov / (left_var.sqrt() * right_var.sqrt())) as f32
    };
    ImageDifferenceStats {
        rms: (sum_sq / len).sqrt() as f32,
        max_abs: max_abs as f32,
        correlation,
    }
}

fn count_nonzero_pixels(image: &ArrayD<f32>, threshold: f32) -> usize {
    image.iter().filter(|value| value.abs() > threshold).count()
}

fn peak_location(image: &ArrayD<f32>) -> Option<(usize, usize)> {
    let mut best = None::<((usize, usize), f32)>;
    for (index, value) in image.indexed_iter() {
        let x = index[0];
        let y = index[1];
        let abs_value = value.abs();
        if best
            .map(|(_, best_value)| abs_value > best_value)
            .unwrap_or(true)
        {
            best = Some(((x, y), abs_value));
        }
    }
    best.map(|(position, _)| position)
}

fn assert_close(left: f32, right: f32, abs_tol: f32, rel_tol: f32, label: &str) {
    let delta = (left - right).abs();
    let limit = abs_tol.max(rel_tol * right.abs().max(left.abs()));
    assert!(
        delta <= limit,
        "{label} mismatch: left={left}, right={right}, delta={delta}, limit={limit}"
    );
}

fn rust_product(prefix: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}.{}", prefix.display(), suffix))
}

fn casa_product(prefix: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}.{}", prefix.display(), suffix))
}
