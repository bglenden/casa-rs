// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native AW request validation runs before any MeasurementSet access.

use super::super::*;

fn native_aw_request(w_plane_count: Option<usize>) -> ContinuumImagingRequest {
    let requirements = vec![
        TaskRequirement::SerialCpu,
        TaskRequirement::AwProjection,
        TaskRequirement::PerChannelWeightDensity,
        TaskRequirement::WProjectionPlanes,
    ];
    ContinuumImagingRequest {
        measurement_set: PathBuf::from("absent.ms"),
        image_name: PathBuf::from("unused-image"),
        image_size: 4096,
        facets: 1,
        cell_arcsec: 0.6,
        phase_center_field: Some(1525),
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![1525]),
        uv_range: Some("<12km".to_string()),
        intent: Some("OBSERVE_TARGET#UNSPECIFIED".to_string()),
        data_description: None,
        spectral_window: Some("2~17".to_string()),
        channel_start: Some(0),
        channel_count: Some(64),
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("data".to_string()),
        polarizations: vec![PolarizationCoordinate::StokesI],
        algorithm: ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0, 5.0, 12.0],
            small_scale_bias: 0.0,
        },
        weighting: ContinuumWeighting::Briggs(1.0),
        iterations: 0,
        cycle_iterations: 1,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: None,
        noise_sigma: Some(5.0),
        cycle_factor: 3.0,
        minimum_psf_fraction: f64::from(0.05_f32),
        maximum_psf_fraction: f64::from(0.8_f32),
        gain: f64::from(0.1_f32),
        threshold_jy: 0.0,
        psf_cutoff: 0.35,
        primary_beam_limit: 0.0001,
        normalization: ProductNormalization::FlatNoise,
        beam_policy: ContinuumBeamPolicy::Common,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: false,
        mosaic_use_pointing: false,
        w_projection_planes: Some(32),
        aw_projection: Some(ContinuumAwProjection {
            source: ContinuumAwCfSource::NativeEvla(NativeEvlaAwCache {
                root: PathBuf::from("unused-cache"),
                surface: PathBuf::from("absent.surface"),
                policy: NativeAwCachePolicy::ReuseOnly,
                working_size: 64,
                oversampling: 4,
                cache_bytes: 1 << 20,
                maximum_cells: 16,
            }),
            resident_bytes: 384 << 20,
            w_plane_count,
            psf_phase_center_direction_rad: None,
            vp_table: None,
            a_term: true,
            ps_term: false,
            wideband: true,
            conjugate_beams: true,
            use_pointing: true,
            pointing_offset_sigdev: vec![0.0],
            mosaic_weighting: false,
            compute_pa_step_deg: 360.0,
            rotate_pa_step_deg: 360.0,
        }),
        resource_policy: resource_policy_for_task_requirements(&requirements),
        task_requirements: requirements,
    }
}

#[test]
fn native_aw_w_plane_count_is_rejected_before_source_io() {
    for (planes, expected) in [
        (None, "at least two W planes"),
        (Some(0), "at least two W planes"),
        (Some(1), "at least two W planes"),
        (Some(usize::MAX), "explicit cell-count bound"),
    ] {
        let error = prepare(native_aw_request(planes))
            .err()
            .expect("invalid W count");
        assert!(error.to_string().contains(expected), "{error}");
    }
}
