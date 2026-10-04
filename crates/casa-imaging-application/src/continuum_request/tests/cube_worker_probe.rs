// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fixed-work residual diagnostic on the production bulk-cube phase. Bootstrap
//! uses an explicitly selected CPU worker count; it is outside the residual timer. Its
//! retained reservations must permit every measured W. Saved FFTW wisdom holds
//! the seed's numerical work fixed between otherwise independent processes.

use super::super::*;
use crate::{MajorCyclePhase, PhaseContext, PlanningRegistry, execution_policy, run_phase};
use casa_imaging_model::{ImagingRequest, ProblemInputIdentities, compile, compile_observation};
use casa_imaging_reconstruction::{ImageDomainReconstructionMaskPlans, ReconstructionMaskSet};
use casa_imaging_runtime::{CubePhase, ReconstructionCyclePhaseCompletion, SpectralCycleRegistry};
use casa_ms::CubeAxisValue;
use std::io::{Read, Write};
use std::time::Instant;

unsafe extern "C" {
    fn fftw_init_threads() -> std::ffi::c_int;
    fn fftwf_init_threads() -> std::ffi::c_int;
    fn fftw_import_wisdom_from_filename(path: *const std::ffi::c_char) -> std::ffi::c_int;
    fn fftwf_import_wisdom_from_filename(path: *const std::ffi::c_char) -> std::ffi::c_int;
    fn fftw_export_wisdom_to_filename(path: *const std::ffi::c_char) -> std::ffi::c_int;
    fn fftwf_export_wisdom_to_filename(path: *const std::ffi::c_char) -> std::ffi::c_int;
}

fn wisdom(root: &Path, import: bool) {
    let double = CString::new(root.join("fftw-wisdom").as_os_str().as_encoded_bytes()).unwrap();
    let single = CString::new(root.join("fftwf-wisdom").as_os_str().as_encoded_bytes()).unwrap();
    // This isolated test process calls the installed FFTW API with valid,
    // NUL-terminated paths while no transform/planning threads are running.
    let result = unsafe {
        assert_eq!((fftw_init_threads(), fftwf_init_threads()), (1, 1));
        if import {
            (
                fftw_import_wisdom_from_filename(double.as_ptr()),
                fftwf_import_wisdom_from_filename(single.as_ptr()),
            )
        } else {
            (
                fftw_export_wisdom_to_filename(double.as_ptr()),
                fftwf_export_wisdom_to_filename(single.as_ptr()),
            )
        }
    };
    assert_eq!(result, (1, 1), "FFTW diagnostic wisdom transfer");
}

#[test]
#[ignore = "requires C-array MS, durable fresh output and sampled aggregate RSS guard"]
fn fixed_nonzero_residual() {
    let root = PathBuf::from(std::env::var_os("CASA_RS_C_ARRAY_OUTPUT").expect("fresh output"));
    let workers: u64 = std::env::var("CASA_RS_C_ARRAY_WORKERS")
        .unwrap()
        .parse()
        .unwrap();
    let channels: usize = std::env::var("CASA_RS_C_ARRAY_OUTPUT_CHANNELS")
        .unwrap()
        .parse()
        .unwrap();
    let memory: u64 = std::env::var("CASA_RS_C_ARRAY_MEMORY_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    assert!([1, 2, 4, 6, 8].contains(&workers));
    assert!((8..=512).contains(&channels));
    assert!(memory > 0 && memory <= 16 << 30);
    let expected_rows: usize = std::env::var("CASA_RS_C_ARRAY_EXPECTED_ROWS")
        .unwrap_or_else(|_| "168480".into())
        .parse()
        .unwrap();
    assert!(
        [168_480, 4_094_064].contains(&expected_rows),
        "diagnostic must explicitly select one of the approved fixtures"
    );
    std::fs::create_dir(&root).expect("fresh durable output directory");
    let reference = std::env::var_os("CASA_RS_FIXED_RESIDUAL_REFERENCE").map(PathBuf::from);
    let start_channel = std::env::var("CASA_RS_FIXED_RESIDUAL_START_CHANNEL")
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(0);
    assert!(start_channel + channels <= 512);
    let bootstrap_cycles = std::env::var("CASA_RS_FIXED_RESIDUAL_BOOTSTRAP_CYCLES")
        .map(|v| v.parse::<u32>().unwrap())
        .unwrap_or(1);
    assert!((1..=16).contains(&bootstrap_cycles));
    let deep = bootstrap_cycles > 1;
    let metal = std::env::var_os("CASA_RS_FIXED_RESIDUAL_METAL").is_some();
    let bootstrap_workers = std::env::var("CASA_RS_FIXED_RESIDUAL_BOOTSTRAP_WORKERS")
        .map(|v| v.parse::<u64>().unwrap())
        .unwrap_or(8);
    assert!((1..=8).contains(&bootstrap_workers));
    if let Some(reference) = &reference {
        wisdom(reference, true);
    }
    let request = ContinuumImagingRequest {
        measurement_set: PathBuf::from(std::env::var_os("CASA_RS_C_ARRAY_MS").unwrap()),
        image_name: root.join("image"),
        image_size: 1024,
        facets: 1,
        cell_arcsec: 0.06,
        phase_center_field: None,
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: Some("0:0~511".into()),
        channel_start: None,
        channel_count: Some(512),
        spectral_mode: SpectralImagingMode::Cube {
            axis: CubeAxisConfig {
                outframe: FrequencyRef::LSRK,
                interpolation: casa_ms::CubeInterpolation::Linear,
                start: Some(CubeAxisValue::FrequencyHz {
                    hz: 44e9 + start_channel as f64 * 2e6,
                    frame: None,
                }),
                width: Some(CubeAxisValue::FrequencyHz {
                    hz: 2e6,
                    frame: None,
                }),
                ..CubeAxisConfig::default()
            },
            output_channels: Some(channels),
        },
        continuum_subtraction: None,
        data_column: Some("DATA".into()),
        polarizations: vec![PolarizationCoordinate::StokesI],
        algorithm: ContinuumAlgorithm::Clark,
        weighting: ContinuumWeighting::Natural,
        iterations: if deep {
            20_000 * channels
        } else {
            64 * channels
        },
        cycle_iterations: if deep { 1000 } else { 64 },
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: Some(bootstrap_cycles as usize),
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.05,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.0005,
        psf_cutoff: 0.35,
        primary_beam_limit: -0.2,
        normalization: ProductNormalization::FlatNoise,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::Image(PathBuf::from(
            std::env::var_os("CASA_RS_C_ARRAY_MASK").unwrap(),
        )),
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: false,
        w_projection_planes: None,
        aw_projection: None,
        task_requirements: vec![TaskRequirement::PerChannelWeightDensity],
        resource_policy: ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(bootstrap_workers),
            memory_bytes: BTreeMap::from([(
                casa_imaging_runtime::CapacityDomainId::new("host-memory"),
                memory,
            )]),
            ..ResourceOverride::default()
        }),
    };
    let ms = casa_ms::MeasurementSet::open(&request.measurement_set).unwrap();
    let rows = ms.row_count();
    assert_eq!(rows, expected_rows, "explicit diagnostic fixture selection");
    assert_eq!(ms.spectral_window().unwrap().num_chan(0).unwrap(), 512);
    assert_eq!(
        ms.spectral_window().unwrap().meas_freq_ref(0).unwrap(),
        FrequencyRef::LSRK.casacore_code()
    );
    drop(ms);
    std::fs::write(
        root.join("request.txt"),
        format!("{request:#?}\nresidual_workers={workers}\n"),
    )
    .unwrap();
    let setup_started = Instant::now();
    let prepared = prepare(request).unwrap();
    let (snapshot, access) = casa_ms::resolve_selected_observation(prepared.observation.clone())
        .unwrap()
        .into_parts();
    let problem = compile(ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        ProblemInputIdentities::new(compile_observation(snapshot).unwrap()),
        prepared.model_lifecycle,
    ))
    .unwrap();
    assert!(CubePhase::supports(&problem).unwrap());
    let mut runtime = prepared.native.unwrap().runtime;
    let access = SelectedObservationSourceResources::finalize_access(
        &problem,
        access,
        &runtime.authority,
        &runtime.resource_policy,
    )
    .unwrap();
    let observation = prepared
        .observation
        .with_content_budget(access.source_binding().content_budget());
    let residency = access.certify_residency(&problem).unwrap();
    let planning =
        PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), &problem);
    let minor =
        crate::streaming_cube::minor_program(&problem, prepared.minor_cycle_image_response, None)
            .unwrap();
    let mut mask_plans = prepared.masks;
    let (initial_plan, phase, _) = <CubePhase as MajorCyclePhase>::initial(
        PhaseContext {
            problem: &problem,
            runtime: &runtime,
            registry: &planning,
            policy: execution_policy(&runtime, residency.clone(), None),
            minor: Some((mask_plans.clone(), minor)),
            clark_refresh: None,
        },
        access,
        None,
        false,
        casa_ms::SelectedVisibilityWriteTargets::new(false, false),
        &observation,
    )
    .unwrap();
    let registry = SpectralCycleRegistry::new(
        runtime.registry,
        runtime.implementation.clone(),
        &problem,
        phase,
    );
    run_phase(
        &problem,
        &initial_plan,
        &registry,
        &runtime,
        runtime.attempts[0],
    )
    .unwrap();
    let mut replay = registry.implementation().take_replay().unwrap();
    let minor = registry
        .implementation()
        .take_reconstruction_cycle_completion()
        .unwrap();
    let mut components = minor.evidence().iterations();
    let mut controller_iterations = minor.evidence().controller_iterations();
    assert!(components > 0, "fixed residual requires a nonzero model");
    mask_plans = next_masks(&mask_plans, &minor, 1);
    let mut final_input = minor.into_final_major_input();
    drop(registry);
    for ordinal in 1..bootstrap_cycles {
        let program = crate::streaming_cube::minor_program(
            &problem,
            prepared.minor_cycle_image_response,
            Some((20_000 * channels).saturating_sub(controller_iterations)),
        )
        .unwrap();
        let (plan, phase) = <CubePhase as MajorCyclePhase>::refresh(
            PhaseContext {
                problem: &problem,
                runtime: &runtime,
                registry: &planning,
                policy: execution_policy(&runtime, residency.clone(), None),
                minor: Some((mask_plans.clone(), program)),
                clark_refresh: None,
            },
            final_input,
            ordinal,
            replay,
            None,
            &observation,
        )
        .unwrap();
        let registry = SpectralCycleRegistry::new(
            runtime.registry,
            runtime.implementation.clone(),
            &problem,
            phase,
        );
        run_phase(
            &problem,
            &plan,
            &registry,
            &runtime,
            crate::major_cycle_attempt(runtime.attempts[1], ordinal),
        )
        .unwrap();
        replay = registry.implementation().take_replay().unwrap();
        let minor = registry
            .implementation()
            .take_reconstruction_cycle_completion()
            .unwrap();
        components += minor.evidence().iterations();
        controller_iterations += minor.evidence().controller_iterations();
        mask_plans = next_masks(&mask_plans, &minor, ordinal as usize + 1);
        final_input = minor.into_final_major_input();
    }
    if let ResourcePolicy::Explicit(policy) = &mut runtime.resource_policy {
        policy.workers = Some(workers);
    }
    let bootstrap_seconds = setup_started.elapsed().as_secs_f64();
    let plan_started = Instant::now();
    let residual_policy = execution_policy(&runtime, residency, None).with_metal_cube(metal);
    let (residual_plan, phase) = <CubePhase as MajorCyclePhase>::refresh(
        PhaseContext {
            problem: &problem,
            runtime: &runtime,
            registry: &planning,
            policy: residual_policy,
            minor: None,
            clark_refresh: None,
        },
        final_input,
        bootstrap_cycles,
        replay,
        None,
        &observation,
    )
    .unwrap();
    assert_eq!(
        residual_plan
            .execution_dag()
            .resource_alternative()
            .demand
            .workers
            .hard(),
        workers
    );
    let plan_seconds = plan_started.elapsed().as_secs_f64();
    let registry = SpectralCycleRegistry::new(
        runtime.registry,
        runtime.implementation.clone(),
        &problem,
        phase,
    );
    eprintln!(
        "fixed_residual_start workers={workers} channels={channels} bootstrap_seconds={bootstrap_seconds} plan_seconds={plan_seconds} components={components}"
    );
    let started = Instant::now();
    run_phase(
        &problem,
        &residual_plan,
        &registry,
        &runtime,
        crate::major_cycle_attempt(runtime.attempts[1], bootstrap_cycles),
    )
    .unwrap();
    let residual_seconds = started.elapsed().as_secs_f64();
    eprintln!("fixed_residual_finished workers={workers} seconds={residual_seconds}");
    let completed = registry
        .implementation()
        .take_completion()
        .unwrap()
        .into_completion();
    let normal = completed.normal_state();
    assert_eq!(normal.shape(), [1024, 1024]);
    assert_eq!(normal.channel_count(), channels);
    assert_eq!(normal.polarization_count(), 1);
    assert_eq!(normal.coefficient_term_count(), channels);
    wisdom(&root, false);
    let mut baseline = reference.as_ref().map(|path| {
        std::io::BufReader::new(std::fs::File::open(path.join("residual-f64le.bin")).unwrap())
    });
    let mut output = reference.is_none().then(|| {
        std::io::BufWriter::new(std::fs::File::create(root.join("residual-f64le.bin")).unwrap())
    });
    let mut model_digest = Sha256::new();
    let mut model_abs = 0.0;
    let mut residual_energy = 0.0;
    let mut difference_energy = 0.0;
    let mut maximum_difference = 0.0_f64;
    let mut metadata_digest = Sha256::new();
    metadata_digest.update(format!("{:?}", problem.geometry()));
    for &weight in normal.sum_weights() {
        metadata_digest.update(weight.to_le_bytes());
    }
    for channel in 0..channels {
        let model = completed.final_model().read_plane(0, channel, 0).unwrap();
        for sample in model.iter() {
            let value = sample.value().value();
            assert!(value.is_finite());
            model_abs += value.abs();
            model_digest.update(value.to_le_bytes());
            model_digest.update([u8::from(
                sample.support() == casa_imaging_model::ModelSupport::Valid,
            )]);
        }
        drop(model);
        let window = normal.read_window(channel..channel + 1).unwrap();
        let values = window.residual();
        assert_eq!(values.len(), 1024 * 1024);
        let mut bytes = vec![0_u8; values.len() * 16];
        if let Some(baseline) = &mut baseline {
            baseline.read_exact(&mut bytes).unwrap();
        }
        for (value, cell) in values.iter().zip(bytes.chunks_exact_mut(16)) {
            assert!(value.re.is_finite() && value.im.is_finite());
            if baseline.is_some() {
                let re = f64::from_le_bytes(cell[..8].try_into().unwrap());
                let im = f64::from_le_bytes(cell[8..].try_into().unwrap());
                let delta = (value.re - re).hypot(value.im - im);
                difference_energy += delta * delta;
                residual_energy += re * re + im * im;
                maximum_difference = maximum_difference.max(delta);
            } else {
                cell[..8].copy_from_slice(&value.re.to_le_bytes());
                cell[8..].copy_from_slice(&value.im.to_le_bytes());
            }
        }
        if let Some(output) = &mut output {
            output.write_all(&bytes).unwrap();
        }
    }
    if let Some(output) = &mut output {
        output.flush().unwrap();
    }
    if let Some(baseline) = &mut baseline {
        assert_eq!(baseline.read(&mut [0_u8; 1]).unwrap(), 0);
    }
    assert!(model_abs > 0.0);
    let model_sha256 = format!("{:x}", model_digest.finalize());
    let metadata_sha256 = format!("{:x}", metadata_digest.finalize());
    let normalized_rms = (difference_energy / residual_energy.max(f64::MIN_POSITIVE)).sqrt();
    if let Some(reference) = reference {
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(reference.join("result.json")).unwrap()).unwrap();
        assert_eq!(saved["rows"], rows);
        assert_eq!(saved["channels"], channels);
        assert_eq!(
            saved["model_sha256"], model_sha256,
            "same fixed model, not a worker-dependent CLEAN result"
        );
        assert_eq!(saved["metadata_sha256"], metadata_sha256);
        assert!(
            normalized_rms <= 1e-3,
            "cross-worker residual divergence: {normalized_rms}"
        );
    }
    let result = serde_json::json!({
        "scope": "one production residual phase; bootstrap, planning, diagnostics and publication excluded",
        "workers": workers, "channels": channels, "rows": rows, "shape": [1024, 1024],
        "metal": metal, "start_channel": start_channel, "bootstrap_cycles": bootstrap_cycles,
        "memory_bytes": memory, "bootstrap_seconds": bootstrap_seconds, "plan_seconds": plan_seconds,
        "residual_seconds": residual_seconds, "components": components,
        "model_sha256": model_sha256, "model_abs": model_abs, "metadata_sha256": metadata_sha256,
        "residual_normalized_rms": normalized_rms, "residual_maximum_difference": maximum_difference,
    });
    std::fs::write(
        root.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    eprintln!("{result}");
}

fn next_masks(
    plans: &ImageDomainReconstructionMaskPlans,
    minor: &ReconstructionCyclePhaseCompletion,
    cycle: usize,
) -> ImageDomainReconstructionMaskPlans {
    let ReconstructionMaskSet::Domains(masks) = minor.masks() else {
        panic!("cube fixture requires domain masks");
    };
    plans
        .next_cycle(
            masks,
            cycle,
            minor.evidence().cycle_threshold_is_global(),
            &(0..masks.len())
                .map(|ordinal| {
                    minor
                        .domain_auto_mask_evidence(ordinal)
                        .is_some_and(|evidence| evidence.channel_stopped)
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
}
