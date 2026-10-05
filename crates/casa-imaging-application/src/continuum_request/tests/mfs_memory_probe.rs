// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in metadata and initial physical-plan census; never executes imaging.

use super::super::*;
use casa_imaging_model::{ImagingRequest, ProblemInputIdentities, compile, compile_observation};
use casa_imaging_runtime::SpectralCyclePlan;

#[test]
#[ignore = "requires owner-initialized MFS input and durable output; planning only"]
fn standard_clark_initial_memory() {
    let input = PathBuf::from(std::env::var_os("CASA_RS_MFS_MS").expect("input MS"));
    let root = PathBuf::from(std::env::var_os("CASA_RS_MFS_OUTPUT").expect("new evidence root"));
    let workers: u64 = std::env::var("CASA_RS_MFS_WORKERS")
        .unwrap()
        .parse()
        .unwrap();
    assert!([1, 4].contains(&workers));
    let cell_arcsec = std::env::var("CASA_RS_MFS_CELL_ARCSEC")
        .unwrap_or_else(|_| "0.05".into())
        .parse::<f64>()
        .unwrap();
    assert!(cell_arcsec.is_finite() && cell_arcsec > 0.0);
    let spectral_window = std::env::var("CASA_RS_MFS_SPW").unwrap_or_else(|_| "0~31".into());
    std::fs::create_dir(&root).expect("fresh durable directory");
    let request = ContinuumImagingRequest {
        measurement_set: input,
        image_name: root.join("image"),
        image_size: 4096,
        facets: 1,
        cell_arcsec,
        phase_center_field: None,
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: Some(spectral_window.clone()),
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("DATA".into()),
        polarizations: vec![PolarizationCoordinate::StokesI],
        algorithm: ContinuumAlgorithm::Clark,
        weighting: ContinuumWeighting::Uniform,
        iterations: 10000,
        cycle_iterations: 1000,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: None,
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.05,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.005,
        psf_cutoff: 0.35,
        primary_beam_limit: -0.2,
        normalization: ProductNormalization::FlatNoise,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: false,
        w_projection_planes: None,
        aw_projection: None,
        task_requirements: if workers == 1 {
            vec![TaskRequirement::SerialCpu]
        } else {
            vec![]
        },
        resource_policy: ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(workers),
            memory_bytes: BTreeMap::from([(
                casa_imaging_runtime::CapacityDomainId::new("host-memory"),
                16 << 30,
            )]),
            ..ResourceOverride::default()
        }),
    };
    std::fs::write(root.join("request.txt"), format!("{request:#?}\n")).unwrap();
    let prepared = prepare(request).expect("same production request preparation");
    let (snapshot, access) = casa_ms::resolve_selected_observation(prepared.observation.clone())
        .unwrap()
        .into_parts();
    let observation = compile_observation(snapshot).unwrap();
    let problem = compile(ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .unwrap();
    crate::validate_installed_implementation(&problem, prepared.task_requirements).unwrap();
    assert!(!casa_imaging_runtime::CubePhase::supports(&problem).unwrap());
    let runtime = prepared.native.unwrap().runtime;
    let access = SelectedObservationSourceResources::finalize_access(
        &problem,
        access,
        &runtime.authority,
        &runtime.resource_policy,
    )
    .unwrap();
    let residency = access.certify_residency(&problem).unwrap();
    let registry =
        crate::PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), &problem);
    let policy = crate::execution_policy(&runtime, residency, None);
    let plan = SpectralCyclePlan::initial(&problem, &registry, policy).unwrap();
    let _frozen = casa_imaging_runtime::FrozenWeightingReservation::acquire(
        &runtime.authority,
        runtime.resource_policy.clone(),
        plan.weighting_plan().planned_residency(),
        access.replay_proof_retained_heap_bytes(&problem).unwrap(),
    )
    .unwrap();
    let admitted = casa_imaging_runtime::plan(
        &problem,
        casa_imaging_runtime::PlanningBindings::new(
            runtime.registry,
            runtime.resource_policy.clone(),
            runtime.cost_model,
        ),
        &runtime.authority,
        &registry,
        &runtime.receipts,
        |_, _| Ok::<_, std::convert::Infallible>(plan.physical_candidates()),
    )
    .expect("production admission, including live weighting and temporary storage");
    let candidates = plan
        .physical_candidates()
        .into_iter()
        .map(|candidate| {
            let alternative = candidate.execution_dag().resource_alternative();
            serde_json::json!({
                "id": alternative.id.as_str(),
                "workers": alternative.demand.workers.hard(),
                "memory": alternative.demand.memory.iter().map(|claim| serde_json::json!({
                    "allocation": claim.allocation_id, "bytes": claim.hard_bytes,
                })).collect::<Vec<_>>(),
                "overhead": format!("{:?}", alternative.demand.overhead),
                "cache_bytes": alternative.demand.caches.hard_resident_bytes,
                "headroom_bytes": alternative.headroom.memory_bytes.values().sum::<u64>(),
                "storage": alternative.demand.storage.iter().map(|claim| serde_json::json!({
                    "allocation": claim.demand_id, "temporary_bytes": claim.temporary_bytes,
                    "staged_output_bytes": claim.staged_output_bytes, "final_output_bytes": claim.final_output_bytes,
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    assert!(!candidates.is_empty());
    let result = serde_json::json!({
        "scope": "production initial physical-plan reservations, not measured imaging RSS; no imaging or publication",
        "requested_workers": workers, "native_planning_bytes": 16_u64 << 30,
        "cell_arcsec": cell_arcsec, "spectral_window": spectral_window,
        "admitted_alternative": admitted.execution_dag().resource_alternative().id.as_str(),
        "selected_rows": problem.inputs().observation_snapshot().sources()[0].selection().rows().selected_row_count(),
        "minor_workspace_bytes": runtime.minor_cycle_bytes,
        "candidates": candidates,
    });
    std::fs::write(
        root.join("memory-plan.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    eprintln!("{result}");
    assert!(!root.join("image.image").exists());
}
