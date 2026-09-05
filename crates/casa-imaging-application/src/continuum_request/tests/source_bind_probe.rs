// SPDX-License-Identifier: LGPL-3.0-or-later

//! Full T51 selection admission, stopping before CF preparation or visibility traversal.

use super::super::*;
use std::{fs, os::unix::fs::MetadataExt, process::Command, time::Instant};

#[test]
#[ignore = "requires the owner-initialized T51 MS, full CF path, and external scratch; release only under a 60s external timeout"]
#[allow(clippy::assertions_on_constants)]
fn t51_full_aw_source_bind_only() {
    assert!(!cfg!(debug_assertions), "use a release test binary");
    let started = Instant::now();
    let required_path = |name| {
        PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required")))
            .canonicalize()
            .unwrap()
    };
    let measurement_set = required_path("CASA_RS_T51_SOURCE_BIND_MS");
    let casa_cache = required_path("CASA_RS_T51_SOURCE_BIND_CF_CACHE");
    let external = required_path("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT");
    assert_eq!(
        fs::metadata(&measurement_set).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_eq!(
        fs::metadata(&casa_cache).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_ne!(
        fs::metadata(external.parent().expect("external volume mount"))
            .unwrap()
            .dev(),
        fs::metadata(&external).unwrap().dev(),
        "scratch parent must be the external volume mount, not a retained data/store directory"
    );
    assert!(!external.starts_with(&measurement_set) && !external.starts_with(&casa_cache));
    let scratch = tempfile::Builder::new()
        .prefix("t51-source-bind-only-")
        .tempdir_in(&external)
        .unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(revision.status.success());
    let requirements = vec![
        TaskRequirement::SerialCpu,
        TaskRequirement::AwProjection,
        TaskRequirement::PerChannelWeightDensity,
        TaskRequirement::WProjectionPlanes,
    ];
    // Values follow the pinned full dirty workload and native_application::application_request.
    let request = ContinuumImagingRequest {
        measurement_set,
        image_name: scratch.path().join("diagnostic"),
        image_size: 4096,
        facets: 1,
        cell_arcsec: 0.6,
        phase_center_field: Some(1525),
        phase_center: None,
        outlier_file: None,
        field_ids: Some(
            (1107..=1127)
                .chain(1512..=1532)
                .chain(1542..=1562)
                .collect(),
        ),
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
        primary_beam_cutoff: 0.0001,
        normalization: ProductNormalization::FlatNoise,
        beam_policy: ContinuumBeamPolicy::Common,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: false,
        w_projection_planes: Some(32),
        aw_projection: Some(ContinuumAwProjection {
            casa_cache,
            resident_bytes: 384 << 20,
            w_plane_count: Some(32),
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
    };
    eprintln!(
        "t51_source_bind_header {}",
        serde_json::json!({
            "parent_revision": String::from_utf8(revision.stdout).unwrap().trim(),
            "compiled_prepare_sha256": format!("{:x}", Sha256::digest(include_bytes!("../../continuum_request.rs"))),
            "compiled_probe_sha256": format!("{:x}", Sha256::digest(include_bytes!("source_bind_probe.rs"))),
            "executable": std::env::current_exe().unwrap(),
            "request": format!("{request:?}"),
            "scope": "production metadata prepare + resolve + compile + certify + open; no CF import, visibility traversal, gridding, FFT, or product publication",
            "external_timeout_seconds": 60,
        })
    );

    let stage = Instant::now();
    let prepared = prepare(request).expect("prepare full T51 request");
    eprintln!(
        "t51_source_bind_stage name=prepare seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let mut native = prepared.native.expect("production runtime preparation");
    let runtime = native.runtime.clone();
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    let stage = Instant::now();
    let resolved = casa_ms::resolve_selected_observation(prepared.observation.clone())
        .expect("resolve full T51 selection");
    eprintln!(
        "t51_source_bind_stage name=resolve seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let (snapshot, initial_access) = resolved.into_parts();
    let stage = Instant::now();
    let observation = casa_imaging_model::compile_observation(snapshot).unwrap();
    let problem = casa_imaging_model::compile(casa_imaging_model::ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        casa_imaging_model::ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .expect("compile full T51 problem");
    crate::validate_installed_implementation(&problem, prepared.task_requirements).unwrap();
    eprintln!(
        "t51_source_bind_stage name=compile seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    assert_eq!(source.selection().rows().selected_row_count(), 655_200);
    let budget = initial_access.source_binding().content_budget();
    eprintln!(
        "t51_source_bind_input {}",
        serde_json::json!({
            "selected_rows": source.selection().rows().selected_row_count(),
            "source_identity": source.identity().to_string(),
            "content_available_bytes": budget.available_bytes(),
            "maximum_live_blocks": budget.maximum_live_blocks(),
            "maximum_pointing_polynomial_terms": budget.maximum_pointing_polynomial_terms(),
        })
    );
    let requirements = initial_access
        .content_requirements(&problem)
        .expect("derive full source requirements without opening traversal");
    assert!(
        requirements.plan(budget).is_err(),
        "bootstrap remains insufficient for full execution"
    );
    let (_, small_access) = casa_ms::resolve_selected_observation(prepared.observation.clone())
        .expect("resolve small-cap control")
        .into_parts();
    let small_policy = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: native
            .runtime
            .authority
            .topology()
            .memory_domains
            .iter()
            .map(|domain| (domain.id.clone(), 64 << 20))
            .collect(),
        workers: Some(1),
        ..ResourceOverride::default()
    });
    native.runtime.resource_policy = small_policy;
    let failure = match crate::run_native(
        &problem,
        crate::NativeInput {
            observation: prepared.observation.clone(),
            initial_access: small_access,
            write_model_column: prepared.write_model_column,
            write_corrected_data: prepared.write_corrected_data,
            masks: prepared.masks,
            minor_cycle_image_response: prepared.minor_cycle_image_response,
            native: Ok(native),
        },
    ) {
        Ok(_) => panic!("an insufficient runtime cap must reject before CF preparation"),
        Err(error) => error,
    };
    assert!(
        failure
            .to_string()
            .contains("selected-observation host memory")
    );
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    let stage = Instant::now();
    let initial_access = SelectedObservationSourceResources::finalize_access(
        &problem,
        initial_access,
        &runtime.authority,
        &runtime.resource_policy,
    )
    .expect("select the production source envelope before CF preparation");
    eprintln!(
        "t51_source_bind_stage name=finalize seconds={:.9} budget={:?}",
        stage.elapsed().as_secs_f64(),
        initial_access.source_binding().content_budget(),
    );
    let stage = Instant::now();
    let residency = initial_access.certify_residency(&problem);
    eprintln!(
        "t51_source_bind_stage name=certify seconds={:.9} result={residency:?}",
        stage.elapsed().as_secs_f64()
    );
    let residency = residency.expect("certify full T51 source residency before CF preparation");
    let stage = Instant::now();
    let selected = initial_access.open(&problem);
    eprintln!(
        "t51_source_bind_stage name=open seconds={:.9} total_seconds={:.9} error={:?}",
        stage.elapsed().as_secs_f64(),
        started.elapsed().as_secs_f64(),
        selected.as_ref().err()
    );
    let selected = selected.expect("open full T51 source before CF preparation");
    assert_eq!(selected.residency_certificate(), &residency);
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    assert!(
        started.elapsed().as_secs_f64() < 60.0,
        "external deadline must also bound the probe"
    );
}
