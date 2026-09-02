// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused frozen-CASA gate for the T43 coupled MT-MFS reconstruction cycle.

#[path = "mtmfs_real_ms_normal.rs"]
mod t42_fixture;

use std::{collections::BTreeMap, convert::Infallible, error::Error, fs, io, path::PathBuf};

use casa_imaging_model::{ProductRole, ProductTerm, ProductUnit};
use casa_imaging_products::{
    AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs,
    ContinuumSourceCatalog, ProductGenerationAuthority, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, ImageDomainReconstructionMaskPlans, MinorCycleProgram,
    MinorCycleStopReason, ModelGeneration, ReconstructionMaskPlan, WeightingExecutionLimits,
};
use casa_imaging_runtime::{
    AttemptBoundObservationCompletion, BuildIdentity, CapacityDomainId, CapacityViewId,
    CpuClassCapacity, ExecutionAttemptId, ExecutionProvenance, ExecutionReceiptStore,
    ExternalPressure, FenceKind, FrozenGriddedNormalReplay, FrozenWeightingArtifact,
    FrozenWeightingReservation, GriddedNormalReplayStorage, HostInventory,
    ImplementationContractMetadata, ImplementationRegistry, ImplementationRegistryId,
    MemoryCapacityDomain, MemoryCapacityKind, MemoryView, MemoryViewKind,
    ObservationReadCompletionContext, PlannerCostModelProfileBootstrap, PlannerCostModelProfileId,
    PlanningBindings, QueueResource, QueueResourceId, RateResource, RateResourceId, RateUnit,
    ReceiptRetention, ResourceAuthority, ResourceOverride, ResourcePolicy, ResourceTopology,
    RunBindings, RunToCompletion, SpectralCycleExecutionPolicy, SpectralCycleExecutor,
    SpectralCyclePassInput, SpectralCyclePlan, SpectralCyclePlanParts, SpectralCycleRegistry,
    StorageDomain, StorageDomainId, StorageIoResourceBinding, WorkExecutionContext,
    WorkImplementation, WorkImplementationId, WorkMeasurements, plan as runtime_plan,
    run as runtime_run,
};
use serde_json::json;

const T43_OUTPUT_ENV: &str = "CASA_RS_T43_RUST_OUTPUT";
const T44_OUTPUT_ENV: &str = "CASA_RS_T44_RUST_OUTPUT";
const IMAGE_SIZE: usize = 128;
const CELLS: usize = IMAGE_SIZE * IMAGE_SIZE;
const IMPLEMENTATION_BYTE: u8 = 0x43;
const HOST_MEMORY_BYTES: u64 = 1 << 30;
const STORAGE_BYTES: u64 = 4 << 30;
const T44_PRODUCT_NAMES: [&str; 19] = [
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".model.tt0",
    ".model.tt1",
    ".image.tt0",
    ".image.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".mask",
    ".pb.tt0",
    ".pb.tt1",
    ".image.tt0.pbcor",
    ".image.tt1.pbcor",
    ".alpha",
    ".alpha.error",
];

#[derive(Debug)]
struct CycleSummary {
    iterations: usize,
    cycle_threshold: f64,
    peak_residual: f64,
    model_flux: Option<f64>,
    stop_reason: &'static str,
    components: Vec<ComponentSummary>,
}

#[derive(Debug)]
struct ComponentSummary {
    coefficient: usize,
    pixel: [usize; 2],
    flux: f64,
    scale_px: f64,
}

fn open_fixture(
    t44_products: bool,
) -> Result<
    (
        tempfile::TempDir,
        casa_imaging_model::CompiledProblem,
        casa_ms::BoundSelectedObservation,
    ),
    Box<dyn Error>,
> {
    let source = casa_test_support::casatestdata_path_for_tier(
        casa_test_support::CasaTestDataTier::SlowParity,
        t42_fixture::DATASET,
    )
    .ok_or("slow-parity casatestdata root is unavailable")?;
    if !source.is_dir() {
        return Err(format!("T43 MeasurementSet is missing at {}", source.display()).into());
    }
    let staging = tempfile::tempdir()?;
    let staged = staging.path().join("ref_vlass_wtsp_creation.ms");
    t42_fixture::copy_measurement_set(&source, &staged)?;
    casa_ms::initialize_measurement_set_owner_manifest(&staged)?;
    let (problem, selected) = if t44_products {
        t42_fixture::build_t44_problem(&staged)?
    } else {
        t42_fixture::build_problem(&staged)?
    };
    Ok((staging, problem, selected))
}

struct CleanRun {
    problem: casa_imaging_model::CompiledProblem,
    final_completion: casa_imaging_reconstruction::MajorCycleCompletion,
    cycles: Vec<CycleSummary>,
}

fn execute_four_cycle_clean(t44_products: bool) -> Result<CleanRun, Box<dyn Error>> {
    let (_staging, problem, selected) = open_fixture(t44_products)?;
    let artifact_directory = tempfile::tempdir()?;
    let receipt_directory = tempfile::tempdir()?;
    let authority = ResourceAuthority::install_production_inventory(runtime_inventory(
        artifact_directory.path(),
        problem_source_root(&problem)?,
    ))?;
    let gridded_normal_directory = artifact_directory.path().join("gridded-normal");
    fs::create_dir_all(&gridded_normal_directory)?;
    let storage = GriddedNormalReplayStorage::bind(
        authority,
        artifact_storage_io(),
        gridded_normal_directory,
    )?;
    let residency = selected.residency_certificate().clone();
    let resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(1),
        ..ResourceOverride::default()
    });
    let planning_registry = PlanningRegistry::new(&problem);
    let execution_policy = || {
        SpectralCycleExecutionPolicy::new(
            implementation_id(),
            WeightingExecutionLimits::new(512, 1).expect("fixed T43/T44 weighting limits"),
            residency.clone(),
            storage_io(),
            1_000,
            16 << 20,
            900_000,
            authority.clone(),
            resource_policy.clone(),
        )
        .with_gridded_normal_storage(storage.clone())
    };
    let receipts = ExecutionReceiptStore::new(
        receipt_directory.path(),
        ReceiptRetention::new(12, 4 << 20)?,
    )?;
    let current = RunBindings::new(problem.inputs().clone(), &resource_policy, cost_model_id());
    let executable = ExecutableModelProblem::from_compiled(problem.clone())?;
    let program = MinorCycleProgram::for_problem(&problem)?.record_component_sequence(16)?;

    let initial = SpectralCyclePlan::initial(&problem, &planning_registry, execution_policy())?;
    let minor_node = initial
        .minor_cycle_node()
        .ok_or("T43/T44 initial plan lacks its reconstruction cycle")?
        .clone();
    let SpectralCyclePlanParts {
        physical,
        weighting,
        complete_data,
        source_resources,
        pass,
        gridded_normal,
        ..
    } = initial.into_parts();
    let frozen_reservation = FrozenWeightingReservation::acquire(
        authority,
        resource_policy.clone(),
        weighting.planned_residency(),
        1 << 20,
    )?;
    let executor = SpectralCycleExecutor::new(
        implementation_id(),
        problem.clone(),
        weighting,
        source_resources,
        pass,
        complete_data,
        selected,
        ExecutableModelProblem::from_compiled(problem.clone())?,
        SpectralCyclePassInput::Initial,
    )
    .with_frozen_weighting_reservation(frozen_reservation)
    .with_planned_gridded_normal_binding(
        gridded_normal.ok_or("T43/T44 initial plan lacks gridded-normal binding")?,
    )?
    .with_reconstruction_cycle(
        minor_node,
        ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        }])?,
        program.clone(),
    );
    let registry =
        SpectralCycleRegistry::new(registry_id(), implementation_id(), &problem, executor);
    let plan = runtime_plan(
        &problem,
        PlanningBindings::new(
            registry_id(),
            resource_policy.clone(),
            PlannerCostModelProfileBootstrap::new(cost_model_id()),
        ),
        authority,
        &registry,
        &receipts,
        move |_, _| Ok::<_, Infallible>(vec![physical]),
    )?;
    runtime_run(
        &executable,
        &plan,
        &current,
        &registry,
        authority,
        &mut RunToCompletion,
        receipts.bind(ExecutionProvenance::new(
            attempt_id(0),
            BuildIdentity::from_sha256([0x43; 32]),
        )),
    )?;
    let mut completion = registry
        .implementation()
        .take_reconstruction_cycle_completion()
        .ok_or("T43/T44 initial reconstruction completion is missing")?;
    let mut frozen_weighting = registry
        .implementation()
        .take_frozen_weighting()
        .ok_or("T43/T44 frozen weighting state is missing")?;
    let mut replay = registry
        .implementation()
        .take_gridded_normal_replay()
        .ok_or("T43/T44 gridded-normal replay is missing")?;
    let mut cycles = vec![cycle_summary(&completion)?];
    let mut input = completion.into_final_major_input();

    for ordinal in 1_u32..=3 {
        let outcome = execute_continuing_cycle(
            &problem,
            &planning_registry,
            execution_policy(),
            &resource_policy,
            authority,
            &receipts,
            &current,
            input,
            frozen_weighting,
            replay,
            program.clone(),
            ordinal,
        )?;
        completion = outcome.0;
        frozen_weighting = outcome.1;
        replay = outcome.2;
        input = completion.into_final_major_input();
        cycles[usize::try_from(ordinal - 1)?].model_flux =
            Some(model_tt0_sum(input.evidence().initial_model()));
        cycles.push(cycle_summary_from_evidence(
            input.evidence().reconstruction_cycle(),
        )?);
    }

    let final_completion = execute_terminal_major(
        &problem,
        &planning_registry,
        execution_policy(),
        &resource_policy,
        authority,
        &receipts,
        &current,
        input,
        frozen_weighting,
        replay,
        4,
    )?;
    cycles[3].model_flux = Some(model_tt0_sum(final_completion.final_model()));
    Ok(CleanRun {
        problem,
        final_completion,
        cycles,
    })
}

#[test]
#[ignore = "requires slow-parity casatestdata and frozen CASA image products"]
fn t43_real_ms_mtmfs_clean_matches_frozen_casa() -> Result<(), Box<dyn Error>> {
    let output = PathBuf::from(
        std::env::var_os(T43_OUTPUT_ENV).ok_or("CASA_RS_T43_RUST_OUTPUT is not set")?,
    );
    let CleanRun {
        problem: _,
        final_completion,
        cycles,
    } = execute_four_cycle_clean(false)?;

    let model = final_completion.final_model().samples();
    if model.len() != 2 * CELLS {
        return Err(format!("T43 final model has {} samples", model.len()).into());
    }
    let normal = final_completion.normal_state();
    let divisor = *normal
        .sum_weights()
        .first()
        .filter(|value| **value > 0.0)
        .ok_or("T43 principal Taylor sum weight is not positive")?;
    let residual = (0..2)
        .map(|term| {
            normal
                .coefficient_term(term)
                .map(|view| {
                    view.residual()
                        .iter()
                        .map(|value| value.re / divisor)
                        .collect::<Vec<_>>()
                })
                .ok_or("T43 final Taylor residual term is missing")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let model_plane = |term: usize| {
        (0..IMAGE_SIZE)
            .flat_map(|x| {
                (0..IMAGE_SIZE)
                    .map(move |y| model[term * CELLS + y * IMAGE_SIZE + x].value().value())
            })
            .collect::<Vec<_>>()
    };
    let artifact = json!({
        "schema": "casa-rs-t43-mtmfs-clean-v1",
        "geometry": {"shape": [IMAGE_SIZE, IMAGE_SIZE], "layout": "x,y"},
        "trajectory": {
            "cycles": cycles.iter().map(|cycle| json!({
                "iterations": cycle.iterations,
                "cycle_threshold": cycle.cycle_threshold,
                "peak_residual": cycle.peak_residual,
                "model_flux": cycle.model_flux.expect("every cycle was reconciled"),
                "stop_reason": cycle.stop_reason,
            })).collect::<Vec<_>>(),
            "total_iterations": cycles.iter().map(|cycle| cycle.iterations).sum::<usize>(),
            "stop_reason": "iteration_limit",
        },
        "products": {
            "model_tt0": model_plane(0),
            "model_tt1": model_plane(1),
            "residual_tt0": residual[0],
            "residual_tt1": residual[1],
        },
        "diagnostics": {
            "component_order_normative": false,
            "cycles": cycles.iter().map(|cycle| cycle.components.iter().map(|component| json!({
                "coefficient": component.coefficient,
                "pixel": component.pixel,
                "flux": component.flux,
                "scale_px": component.scale_px,
            })).collect::<Vec<_>>()).collect::<Vec<_>>(),
        },
    });
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec(&artifact)?)?;
    eprintln!("t43_mtmfs_rust_clean {}", output.display());
    Ok(())
}

#[test]
#[ignore = "requires slow-parity casatestdata and frozen CASA image products"]
fn t44_real_ms_mtmfs_products_match_frozen_casa() -> Result<(), Box<dyn Error>> {
    let output = PathBuf::from(
        std::env::var_os(T44_OUTPUT_ENV).ok_or("CASA_RS_T44_RUST_OUTPUT is not set")?,
    );
    let CleanRun {
        problem,
        final_completion,
        cycles,
    } = execute_four_cycle_clean(true)?;
    let catalog = ContinuumSourceCatalog::from_major_cycle(&problem, &final_completion)?;
    let authority = ProductGenerationAuthority::bind(&problem);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let planned = authority.plan(&catalog, &controls)?;
    if planned.primary_beam_model() != Some(AnalyticPrimaryBeamModel::CasaEvlaCommon) {
        return Err("T44 product plan did not pin the CASA EVLA common beam".into());
    }
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &final_completion)?;
    let produced = produce_continuum_members(&planned, &inputs)?;
    let sealed = authority.authorize(&planned, &produced)?;
    let names = sealed
        .members()
        .iter()
        .map(|member| member.name())
        .collect::<Vec<_>>();
    if names != T44_PRODUCT_NAMES {
        return Err(format!("T44 sealed product set changed: {names:?}").into());
    }
    if names.iter().any(|name| name.starts_with(".weight")) || names.contains(&".alpha.pbcor") {
        return Err("T44 standard product set contains weight or alpha.pbcor".into());
    }
    let common_beam = sealed
        .restoring_beam()
        .ok_or("T44 common restoring beam is missing")?;
    for member in sealed.members().iter().filter(|member| {
        matches!(
            member.contract().role(),
            ProductRole::RestoredImage(_)
                | ProductRole::PbCorrectedImage(_)
                | ProductRole::SpectralIndex
                | ProductRole::SpectralIndexError
        )
    }) {
        if member.resolved_beam() != Some(common_beam) {
            return Err(format!("{} does not carry the common beam", member.name()).into());
        }
    }
    let artifact = json!({
        "schema": "casa-rs-t44-mtmfs-products-v1",
        "geometry": {"shape": [IMAGE_SIZE, IMAGE_SIZE], "layout": "x,y"},
        "trajectory": {
            "cycles": cycles.iter().map(|cycle| json!({
                "iterations": cycle.iterations,
                "cycle_threshold": cycle.cycle_threshold,
                "peak_residual": cycle.peak_residual,
                "model_flux": cycle.model_flux.expect("every cycle was reconciled"),
                "stop_reason": cycle.stop_reason,
            })).collect::<Vec<_>>(),
            "total_iterations": cycles.iter().map(|cycle| cycle.iterations).sum::<usize>(),
            "stop_reason": "iteration_limit",
        },
        "common_beam": beam_json(common_beam),
        "members": sealed.members().iter().map(|member| json!({
            "name": member.name(),
            "role": product_role_name(member.contract().role()),
            "unit": product_unit_name(member.contract().unit()),
            "shape": member.contract().axes().shape(),
            "beam": member.resolved_beam().map(beam_json),
            "payload": member.payload(),
            "validity": member.validity(),
        })).collect::<Vec<_>>(),
    });
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec(&artifact)?)?;
    eprintln!("t44_mtmfs_rust_products {}", output.display());
    Ok(())
}

fn beam_json(beam: &casa_imaging_products::RestoringBeam) -> serde_json::Value {
    json!({
        "major_rad": beam.major_fwhm_rad(),
        "minor_rad": beam.minor_fwhm_rad(),
        "position_angle_rad": beam.position_angle_rad(),
    })
}

fn product_role_name(role: ProductRole) -> String {
    let term = |prefix: &str, term: ProductTerm| match term {
        ProductTerm::Single => prefix.to_string(),
        ProductTerm::Taylor(term) => format!("{prefix}.tt{term}"),
        ProductTerm::Continuum(term) => format!("{prefix}.continuum{term}"),
        ProductTerm::Line => format!("{prefix}.line"),
        ProductTerm::Total => format!("{prefix}.total"),
        ProductTerm::JointNormal { row, column } => {
            format!("{prefix}.normal{row}_{column}")
        }
    };
    match role {
        ProductRole::Psf(value) => term("psf", value),
        ProductRole::Residual(value) => term("residual", value),
        ProductRole::Model(value) => term("model", value),
        ProductRole::RestoredImage(value) => term("restored_image", value),
        ProductRole::SumWeights(value) => term("sum_weights", value),
        ProductRole::CleanMask => "clean_mask".to_string(),
        ProductRole::ContinuumCleanMask => "continuum_clean_mask".to_string(),
        ProductRole::LineCleanMask => "line_clean_mask".to_string(),
        ProductRole::Weight(value) => term("weight", value),
        ProductRole::PrimaryBeam(value) => term("primary_beam", value),
        ProductRole::PrimaryBeamSpectralIndex => "primary_beam_spectral_index".to_string(),
        ProductRole::Sensitivity => "sensitivity".to_string(),
        ProductRole::PbCorrectedImage(value) => term("pb_corrected_image", value),
        ProductRole::TaylorCoefficientSet => "taylor_coefficient_set".to_string(),
        ProductRole::SpectralIndex => "spectral_index".to_string(),
        ProductRole::SpectralIndexError => "spectral_index_error".to_string(),
        ProductRole::PbCorrectedSpectralIndex => "pb_corrected_spectral_index".to_string(),
        ProductRole::BeamMetadata => "beam_metadata".to_string(),
    }
}

const fn product_unit_name(unit: ProductUnit) -> &'static str {
    match unit {
        ProductUnit::NotApplicable => "not_applicable",
        ProductUnit::JyPerBeam => "jy_per_beam",
        ProductUnit::JyPerPixel => "jy_per_pixel",
        ProductUnit::Dimensionless => "dimensionless",
        ProductUnit::VisibilityWeight => "visibility_weight",
    }
}

fn cycle_summary(
    completion: &casa_imaging_runtime::ReconstructionCyclePhaseCompletion,
) -> Result<CycleSummary, Box<dyn Error>> {
    cycle_summary_from_evidence(completion.evidence())
}

fn cycle_summary_from_evidence(
    evidence: &casa_imaging_reconstruction::ReconstructionCycleEvidence,
) -> Result<CycleSummary, Box<dyn Error>> {
    let stop_reason = match evidence.stop_reason() {
        MinorCycleStopReason::IterationBound => "iteration_bound",
        MinorCycleStopReason::ThresholdReached => "threshold_reached",
        MinorCycleStopReason::StalenessBound => "staleness_bound",
        MinorCycleStopReason::MultiscaleDivergence => "multiscale_divergence",
    };
    Ok(CycleSummary {
        iterations: evidence.controller_iterations(),
        cycle_threshold: evidence
            .cycle_threshold()
            .ok_or("T43 cycle threshold evidence is missing")?,
        peak_residual: evidence.final_peak_flux(),
        model_flux: None,
        stop_reason,
        components: evidence
            .recorded_components()
            .map(|component| ComponentSummary {
                coefficient: component.cell().coefficient(),
                pixel: component.cell().pixel(),
                flux: component.flux(),
                scale_px: component.scale_px(),
            })
            .collect(),
    })
}

fn model_tt0_sum(model: &ModelGeneration) -> f64 {
    model.samples()[..CELLS]
        .iter()
        .map(|sample| sample.value().value())
        .sum()
}

#[allow(clippy::too_many_arguments)]
fn execute_continuing_cycle(
    problem: &casa_imaging_model::CompiledProblem,
    planning_registry: &PlanningRegistry,
    policy: SpectralCycleExecutionPolicy,
    resource_policy: &ResourcePolicy,
    authority: &ResourceAuthority,
    receipts: &ExecutionReceiptStore,
    current: &RunBindings,
    input: casa_imaging_runtime::FinalMajorPhaseInput,
    frozen_weighting: FrozenWeightingArtifact,
    replay: FrozenGriddedNormalReplay,
    program: MinorCycleProgram,
    ordinal: u32,
) -> Result<
    (
        casa_imaging_runtime::ReconstructionCyclePhaseCompletion,
        FrozenWeightingArtifact,
        FrozenGriddedNormalReplay,
    ),
    Box<dyn Error>,
> {
    let planned = SpectralCyclePlan::continuing_major(
        problem,
        planning_registry,
        policy,
        &input,
        ordinal,
        replay,
    )?;
    let minor_node = planned
        .minor_cycle_node()
        .ok_or("T43 continuing plan lacks reconstruction cycle")?
        .clone();
    let SpectralCyclePlanParts {
        physical,
        weighting,
        complete_data,
        pass,
        gridded_normal,
        ..
    } = planned.into_parts();
    let executor = SpectralCycleExecutor::new_gridded(
        implementation_id(),
        problem.clone(),
        weighting,
        pass,
        complete_data,
        ExecutableModelProblem::from_compiled(problem.clone())?,
        SpectralCyclePassInput::FinalMajor(input),
        gridded_normal.ok_or("T43 continuing plan lacks gridded replay binding")?,
    )?
    .with_frozen_weighting(frozen_weighting)
    .with_reconstruction_cycle(
        minor_node,
        ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        }])?,
        program,
    );
    let registry =
        SpectralCycleRegistry::new(registry_id(), implementation_id(), problem, executor);
    run_plan(
        problem,
        resource_policy,
        authority,
        receipts,
        current,
        &registry,
        physical,
        ordinal,
    )?;
    Ok((
        registry
            .implementation()
            .take_reconstruction_cycle_completion()
            .ok_or("T43 continuing reconstruction completion is missing")?,
        registry
            .implementation()
            .take_frozen_weighting()
            .ok_or("T43 continuing frozen weighting state is missing")?,
        registry
            .implementation()
            .take_gridded_normal_replay()
            .ok_or("T43 continuing gridded replay is missing")?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn execute_terminal_major(
    problem: &casa_imaging_model::CompiledProblem,
    planning_registry: &PlanningRegistry,
    policy: SpectralCycleExecutionPolicy,
    resource_policy: &ResourcePolicy,
    authority: &ResourceAuthority,
    receipts: &ExecutionReceiptStore,
    current: &RunBindings,
    input: casa_imaging_runtime::FinalMajorPhaseInput,
    frozen_weighting: FrozenWeightingArtifact,
    replay: FrozenGriddedNormalReplay,
    ordinal: u32,
) -> Result<casa_imaging_reconstruction::MajorCycleCompletion, Box<dyn Error>> {
    let planned = SpectralCyclePlan::final_major_at(
        problem,
        planning_registry,
        policy,
        &input,
        ordinal,
        replay,
    )?;
    let SpectralCyclePlanParts {
        physical,
        weighting,
        complete_data,
        pass,
        gridded_normal,
        ..
    } = planned.into_parts();
    let executor = SpectralCycleExecutor::new_gridded(
        implementation_id(),
        problem.clone(),
        weighting,
        pass,
        complete_data,
        ExecutableModelProblem::from_compiled(problem.clone())?,
        SpectralCyclePassInput::FinalMajor(input),
        gridded_normal.ok_or("T43 terminal plan lacks gridded replay binding")?,
    )?
    .with_frozen_weighting(frozen_weighting);
    let registry =
        SpectralCycleRegistry::new(registry_id(), implementation_id(), problem, executor);
    run_plan(
        problem,
        resource_policy,
        authority,
        receipts,
        current,
        &registry,
        physical,
        ordinal,
    )?;
    Ok(registry
        .implementation()
        .take_completion()
        .ok_or("T43 terminal completion is missing")?
        .into_completion())
}

#[allow(clippy::too_many_arguments)]
fn run_plan(
    problem: &casa_imaging_model::CompiledProblem,
    resource_policy: &ResourcePolicy,
    authority: &ResourceAuthority,
    receipts: &ExecutionReceiptStore,
    current: &RunBindings,
    registry: &SpectralCycleRegistry<SpectralCycleExecutor>,
    physical: casa_imaging_runtime::PhysicalWorkBinding,
    ordinal: u32,
) -> Result<(), Box<dyn Error>> {
    let plan = runtime_plan(
        problem,
        PlanningBindings::new(
            registry_id(),
            resource_policy.clone(),
            PlannerCostModelProfileBootstrap::new(cost_model_id()),
        ),
        authority,
        registry,
        receipts,
        move |_, _| Ok::<_, Infallible>(vec![physical]),
    )?;
    let executable = ExecutableModelProblem::from_compiled(problem.clone())?;
    let attempt = attempt_id(ordinal);
    runtime_run(
        &executable,
        &plan,
        current,
        registry,
        authority,
        &mut RunToCompletion,
        receipts.bind(ExecutionProvenance::new(
            attempt,
            BuildIdentity::from_sha256([0x43_u8.wrapping_add(ordinal as u8); 32]),
        )),
    )?;
    Ok(())
}

fn problem_source_root(
    problem: &casa_imaging_model::CompiledProblem,
) -> Result<PathBuf, Box<dyn Error>> {
    let locator = problem.inputs().observation_snapshot().sources()[0]
        .provenance()
        .locator();
    Ok(PathBuf::from(locator)
        .parent()
        .ok_or("T43 source locator has no parent")?
        .to_path_buf())
}

fn runtime_inventory(artifact_root: &std::path::Path, source_root: PathBuf) -> HostInventory {
    let memory_domain = CapacityDomainId::new("host-memory");
    let memory_view = CapacityViewId::new("host-memory");
    let io_rate = RateResourceId::new("io-rate");
    let io_operations = RateResourceId::new("io-operations-rate");
    let io_queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let storage = StorageDomainId::new("atomic-output");
    let source_storage = StorageDomainId::new("prepared-source-secondary");
    HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory_domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: HOST_MEMORY_BYTES,
            }],
            memory_views: vec![MemoryView {
                id: memory_view,
                domain: memory_domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![
                StorageDomain {
                    id: storage.clone(),
                    root: artifact_root.to_path_buf(),
                    capacity_bytes: STORAGE_BYTES,
                    read_rate: io_rate.clone(),
                    write_rate: io_rate.clone(),
                    operations_rate: Some(io_operations.clone()),
                    queue: io_queue.clone(),
                },
                StorageDomain {
                    id: source_storage.clone(),
                    root: source_root,
                    capacity_bytes: STORAGE_BYTES,
                    read_rate: io_rate.clone(),
                    write_rate: io_rate.clone(),
                    operations_rate: Some(io_operations.clone()),
                    queue: io_queue.clone(),
                },
            ],
            rate_resources: vec![
                RateResource::new(io_rate.clone(), RateUnit::BytesPerSecond, STORAGE_BYTES),
                RateResource::new(io_operations.clone(), RateUnit::OperationsPerSecond, 1_024),
                RateResource::new(
                    transaction_rate.clone(),
                    RateUnit::BytesPerSecond,
                    STORAGE_BYTES,
                ),
            ],
            queue_resources: vec![
                QueueResource::new(io_queue.clone(), 8),
                QueueResource::new(transaction_queue.clone(), 4),
            ],
            logical_cpu_threads: 1,
            performance_cpu_cores: CpuClassCapacity::Known(1),
            cache_capacity_bytes: 64 << 20,
            lock_capacity: 8,
            file_descriptor_capacity: 64,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory_domain, HOST_MEMORY_BYTES)]),
            available_cpu_threads: 1,
            storage_available_bytes: BTreeMap::from([
                (storage, STORAGE_BYTES),
                (source_storage, STORAGE_BYTES),
            ]),
            rate_available_per_second: BTreeMap::from([
                (io_rate, STORAGE_BYTES),
                (io_operations, 1_024),
                (transaction_rate, STORAGE_BYTES),
            ]),
            queue_available_slots: BTreeMap::from([(io_queue, 8), (transaction_queue, 4)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 64 << 20,
            available_locks: 8,
            available_file_descriptors: 64,
        },
    }
}

fn storage_io() -> StorageIoResourceBinding {
    StorageIoResourceBinding::new(
        StorageDomainId::new("atomic-output"),
        RateResourceId::new("transaction-io-rate"),
        RateResourceId::new("transaction-io-rate"),
        QueueResourceId::new("transaction-io-queue"),
    )
}

fn artifact_storage_io() -> StorageIoResourceBinding {
    StorageIoResourceBinding::new(
        StorageDomainId::new("atomic-output"),
        RateResourceId::new("io-rate"),
        RateResourceId::new("io-rate"),
        QueueResourceId::new("io-queue"),
    )
}

fn registry_id() -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([IMPLEMENTATION_BYTE; 32])
}

fn implementation_id() -> WorkImplementationId {
    WorkImplementationId::new("issue-529-mtmfs-clean-oracle")
}

fn cost_model_id() -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([0x45; 32])
}

fn attempt_id(ordinal: u32) -> ExecutionAttemptId {
    ExecutionAttemptId::from_sha256([0x50_u8.wrapping_add(ordinal as u8); 32])
}

struct PlanningImplementation {
    id: WorkImplementationId,
}

impl WorkImplementation for PlanningImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, _context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        unreachable!("contract-only implementation is never executed")
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        unreachable!("contract-only implementation owns no fence")
    }

    fn complete_observation_read(
        &self,
        _completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        unreachable!("contract-only implementation owns no observation read")
    }

    fn publish(&self, _context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        unreachable!("contract-only implementation owns no publication")
    }
}

struct PlanningRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    implementation: PlanningImplementation,
}

impl PlanningRegistry {
    fn new(problem: &casa_imaging_model::CompiledProblem) -> Self {
        Self {
            id: registry_id(),
            metadata: ImplementationContractMetadata::new(
                problem.problem_id(),
                problem.numerics_id(),
                problem.required_capabilities().clone(),
            ),
            implementation: PlanningImplementation {
                id: implementation_id(),
            },
        }
    }
}

impl ImplementationRegistry for PlanningRegistry {
    type Implementation = PlanningImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        (id == &self.implementation.id).then_some(&self.implementation)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        (id == &self.implementation.id).then(|| self.metadata.clone())
    }
}
