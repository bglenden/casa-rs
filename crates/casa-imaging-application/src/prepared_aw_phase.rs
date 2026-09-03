// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan/run-owned preparation of a complete AW convolution-function catalog.

use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

use casa_imaging_model::CompiledProblem;
use casa_imaging_reconstruction::PreparedAwProjection;
use casa_imaging_runtime::{
    ArtifactDisposition, ArtifactMeasurement, AttemptBoundObservationCompletion,
    ExecutionAttemptId, ExecutionEvidenceError, ExecutionProvenance, ExecutionReceipt, FenceKind,
    ImplementationContractMetadata, ImplementationRegistry, ImplementationRegistryId, IoBufferKind,
    IoMeasurement, ObservationReadCompletionContext, PlanningBindings, PreparedArtifact,
    PreparedArtifactImportSource, PreparedArtifactOperation, PreparedArtifactPlanFragment,
    PreparedArtifactRegistration, PreparedArtifactReuseOutcome, PreparedArtifactStore,
    ResourceMeasurement, RunBindings, RunError, RunToCompletion, StorageDomain,
    WorkExecutionContext, WorkImplementation, WorkImplementationId, WorkMeasurements, plan, run,
};

use crate::{
    ApplicationAwPreparation, ApplicationError, ApplicationRuntime, CasaAwCache,
    CasaAwPreparedCell, PreparedAwCellProvider, boxed,
};

/// Complete pre-phase result retained by every later major cycle.
pub(crate) struct PreparedAwPhase {
    pub projection: PreparedAwProjection,
    pub receipts: Vec<ExecutionReceipt>,
}

enum OperationResult {
    Artifact(PreparedArtifact),
    Rejected,
    Consumed,
}

struct OperationAdapter {
    id: WorkImplementationId,
    operation: PreparedArtifactOperation,
    store: Arc<PreparedArtifactStore>,
    cache: Arc<CasaAwCache>,
    prepared: CasaAwPreparedCell,
    artifact: Mutex<Option<PreparedArtifact>>,
    source: Option<PreparedArtifactImportSource>,
    provider: PreparedAwCellProvider,
    result: Mutex<Option<OperationResult>>,
}

enum PhaseImplementation {
    Base {
        id: WorkImplementationId,
        sources: Vec<PreparedArtifactImportSource>,
    },
    Operation(Box<OperationAdapter>),
}

struct OperationBatch<'a> {
    cache: Arc<CasaAwCache>,
    store: Arc<PreparedArtifactStore>,
    provider: PreparedAwCellProvider,
    cells: Vec<CasaAwPreparedCell>,
    source_domain: &'a StorageDomain,
    operation: PreparedArtifactOperation,
    artifacts: Vec<(CasaAwPreparedCell, PreparedArtifact)>,
    phase: u64,
}

impl WorkImplementation for PhaseImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        match self {
            Self::Base { id, .. } => id,
            Self::Operation(op) => &op.id,
        }
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let op = match self {
            Self::Base { sources, .. } => return base_measurements(context, sources),
            Self::Operation(op) => op,
        };
        let result = match op.operation {
            PreparedArtifactOperation::Reuse => {
                let (outcome, measurements) = op
                    .prepared
                    .reuse_warm(&op.store, context)
                    .map_err(io::Error::other)?;
                let result = match outcome {
                    PreparedArtifactReuseOutcome::Reused(artifact) => {
                        OperationResult::Artifact(artifact)
                    }
                    PreparedArtifactReuseOutcome::Rejected(_) => OperationResult::Rejected,
                };
                (result, measurements)
            }
            PreparedArtifactOperation::Load => {
                let source = op
                    .source
                    .as_ref()
                    .ok_or_else(|| io::Error::other("AW cold load omitted its source"))?;
                let (artifact, measurements) = op
                    .prepared
                    .import_cold(&op.cache, &op.store, source, context)
                    .map_err(io::Error::other)?;
                (OperationResult::Artifact(artifact), measurements)
            }
            PreparedArtifactOperation::Consume => {
                let artifact = op
                    .artifact
                    .lock()
                    .map_err(|_| io::Error::other("AW artifact state poisoned"))?
                    .take()
                    .ok_or_else(|| io::Error::other("AW consume omitted opaque artifact"))?;
                let measurements = op
                    .provider
                    .consume_cell(&op.store, context, &op.prepared, &artifact)
                    .map_err(io::Error::other)?;
                (OperationResult::Consumed, measurements)
            }
            PreparedArtifactOperation::Generate => {
                return Err(io::Error::other("CASA AW import requires plan-listed Load"));
            }
        };
        *op.result
            .lock()
            .map_err(|_| io::Error::other("AW result state poisoned"))? = Some(result.0);
        Ok(result.1)
    }

    fn failure_measurements<'a>(&'a self, error: &'a Self::Error) -> Option<&'a WorkMeasurements> {
        error
            .get_ref()?
            .downcast_ref::<casa_imaging_runtime::PreparedArtifactError>()?
            .work_measurements()
    }
    fn wait_for_fence(
        &self,
        _: WorkExecutionContext<'_>,
        _: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        Ok(WorkMeasurements::default())
    }
    fn complete_observation_read(
        &self,
        _: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(io::Error::other("AW pre-phase is source-free"))
    }
    fn publish(&self, _: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn base_measurements(
    context: WorkExecutionContext<'_>,
    sources: &[PreparedArtifactImportSource],
) -> Result<WorkMeasurements, io::Error> {
    let resources = context
        .resources()
        .iter()
        .map(|capability| {
            ResourceMeasurement::new(
                capability.resource().clone(),
                capability.lifetime().clone(),
                capability.amount(),
            )
        })
        .collect();
    let io = match context.node().id.as_str() {
        "prepared-phase-commit" => vec![IoMeasurement::new(IoBufferKind::Publication, 0, 0)],
        node if node.starts_with("prepared-release-") => {
            vec![IoMeasurement::new(IoBufferKind::StorageManager, 0, 0)]
        }
        _ => vec![],
    };
    let artifacts = (context.node().id.as_str() == "prepared-phase-producer")
        .then(|| {
            sources
                .iter()
                .map(|source| {
                    ArtifactMeasurement::new(
                        source.identity(),
                        Some(source.identity()),
                        ArtifactDisposition::Loaded,
                        source.source_read_bytes(),
                        None,
                    )
                    .map_err(io::Error::other)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(WorkMeasurements::new(resources, io, artifacts))
}

struct PhaseRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    implementations: BTreeMap<WorkImplementationId, PhaseImplementation>,
    prepared_artifact: PreparedArtifactRegistration,
}

impl ImplementationRegistry for PhaseRegistry {
    type Implementation = PhaseImplementation;
    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }
    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.implementations.get(id)
    }
    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.implementations
            .contains_key(id)
            .then(|| self.metadata.clone())
    }
    fn prepared_artifact_registration(
        &self,
        implementation: &WorkImplementationId,
    ) -> Option<&PreparedArtifactRegistration> {
        (implementation == self.prepared_artifact.implementation())
            .then_some(&self.prepared_artifact)
    }
}

impl PhaseRegistry {
    fn take_results(&self) -> Result<Vec<(CasaAwPreparedCell, OperationResult)>, ApplicationError> {
        self.implementations
            .values()
            .filter_map(|implementation| match implementation {
                PhaseImplementation::Base { .. } => None,
                PhaseImplementation::Operation(op) => Some(
                    op.result
                        .lock()
                        .map_err(|_| boxed("AW result state poisoned"))
                        .and_then(|mut result| {
                            result
                                .take()
                                .map(|result| (op.prepared.clone(), result))
                                .ok_or_else(|| boxed("AW operation did not execute"))
                        }),
                ),
            })
            .collect()
    }
}

pub(crate) fn prepare_aw_projection(
    problem: &CompiledProblem,
    deployment: ApplicationAwPreparation,
    runtime: &ApplicationRuntime,
) -> Result<PreparedAwPhase, ApplicationError> {
    std::fs::create_dir_all(&deployment.private_root)?;
    let cache = Arc::new(CasaAwCache::open(&deployment.casa_cache)?);
    let catalog = cache.prepared_catalog()?;
    let entries = cache.inventory().paired_cells;
    let budget = casa_imaging_runtime::PreparedArtifactBudget::new(
        u64::try_from(deployment.resident_bytes)?
            .saturating_mul(2)
            .saturating_add((entries as u64).saturating_mul(4096)),
        entries,
        8 << 20,
    )?;
    let store = Arc::new(PreparedArtifactStore::open(
        &deployment.private_root,
        &deployment.storage_domain,
        budget,
    )?);
    let owner =
        super::PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    let prepared = cache.prepared_cells(&store, &owner, &runtime.implementation, problem)?;
    let decoded_bytes = prepared
        .iter()
        .try_fold(0_usize, |total, cell| {
            total.checked_add(cell.decoded_resident_bytes()?)
        })
        .ok_or_else(|| boxed("AW decoded catalog residency overflowed"))?;
    if decoded_bytes > deployment.resident_bytes {
        return Err(Box::new(io::Error::other(format!(
            "AW decoded catalog requires {decoded_bytes} bytes, exceeding the {} byte ceiling",
            deployment.resident_bytes
        ))));
    }
    let provider = PreparedAwCellProvider::new(deployment.resident_bytes)?;
    let mut receipts = Vec::new();

    let mut artifacts = Vec::new();
    let mut missing = Vec::new();
    for (ordinal, cell) in prepared.into_iter().enumerate() {
        let reuse = run_operations(
            problem,
            runtime,
            OperationBatch {
                cache: Arc::clone(&cache),
                store: Arc::clone(&store),
                provider: provider.clone(),
                cells: vec![cell],
                source_domain: &deployment.storage_domain,
                operation: PreparedArtifactOperation::Reuse,
                artifacts: Vec::new(),
                phase: ordinal as u64,
            },
        )?;
        receipts.push(reuse.1);
        for (cell, result) in reuse.0 {
            match result {
                OperationResult::Artifact(artifact) => artifacts.push((cell, artifact)),
                OperationResult::Rejected => missing.push(cell),
                OperationResult::Consumed => unreachable!(),
            }
        }
    }
    if !missing.is_empty() {
        let loaded = run_operations(
            problem,
            runtime,
            OperationBatch {
                cache: Arc::clone(&cache),
                store: Arc::clone(&store),
                provider: provider.clone(),
                cells: missing,
                source_domain: &deployment.storage_domain,
                operation: PreparedArtifactOperation::Load,
                artifacts: Vec::new(),
                phase: 1_000_000,
            },
        )?;
        receipts.push(loaded.1);
        for (cell, result) in loaded.0 {
            if let OperationResult::Artifact(artifact) = result {
                artifacts.push((cell, artifact));
            } else {
                return Err(boxed("AW cold load omitted artifact"));
            }
        }
    }
    artifacts.sort_by_key(|(cell, _)| cell.metadata().identity().as_bytes());
    let cells = artifacts.iter().map(|(cell, _)| cell.clone()).collect();
    let consumed = run_operations(
        problem,
        runtime,
        OperationBatch {
            cache,
            store,
            provider: provider.clone(),
            cells,
            source_domain: &deployment.storage_domain,
            operation: PreparedArtifactOperation::Consume,
            artifacts,
            phase: 2_000_000,
        },
    )?;
    if consumed
        .0
        .iter()
        .any(|(_, result)| !matches!(result, OperationResult::Consumed))
    {
        return Err(boxed("AW consume phase returned a non-consume outcome"));
    }
    receipts.push(consumed.1);
    if provider.resident_cells()? != entries {
        return Err(boxed(
            "AW complete catalog did not fit the resident ceiling",
        ));
    }
    let projection = PreparedAwProjection::new(
        catalog,
        provider,
        deployment.conjugate_beams,
        deployment.resident_bytes,
    )?;
    Ok(PreparedAwPhase {
        projection,
        receipts,
    })
}

fn run_operations(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    batch: OperationBatch<'_>,
) -> Result<(Vec<(CasaAwPreparedCell, OperationResult)>, ExecutionReceipt), ApplicationError> {
    let OperationBatch {
        cache,
        store,
        provider,
        cells,
        source_domain,
        operation,
        mut artifacts,
        phase,
    } = batch;
    let metadata = ImplementationContractMetadata::new(
        problem.problem_id(),
        problem.numerics_id(),
        problem.required_capabilities().clone(),
    );
    let producer = casa_imaging_runtime::WorkNodeId::new("prepared-phase-producer");
    let mut implementations = BTreeMap::new();
    let mut sources = Vec::new();
    for cell in cells {
        let id = cell.descriptor().work_implementation_id(operation);
        let artifact = artifacts
            .iter()
            .position(|(candidate, _)| {
                candidate.metadata().identity() == cell.metadata().identity()
            })
            .map(|index| artifacts.swap_remove(index).1);
        let source = (operation == PreparedArtifactOperation::Load)
            .then(|| cell.import_source(&cache, source_domain, producer.clone()))
            .transpose()?;
        if let Some(source) = &source {
            sources.push(source.clone());
        }
        implementations.insert(
            id.clone(),
            PhaseImplementation::Operation(Box::new(OperationAdapter {
                id,
                operation,
                store: Arc::clone(&store),
                cache: Arc::clone(&cache),
                prepared: cell,
                artifact: Mutex::new(artifact),
                source,
                provider: provider.clone(),
                result: Mutex::new(None),
            })),
        );
    }
    implementations.insert(
        runtime.implementation.clone(),
        PhaseImplementation::Base {
            id: runtime.implementation.clone(),
            sources,
        },
    );
    let registry = PhaseRegistry {
        id: runtime.registry,
        metadata,
        implementations,
        prepared_artifact: crate::prepared_aw_registration(runtime.implementation.clone()),
    };
    let mut physical = PreparedArtifactPlanFragment::standalone_base(
        problem,
        &registry,
        runtime.implementation.clone(),
        &runtime.storage_io,
        runtime.stage_nanos,
        runtime.confidence_parts_per_million,
    )?;
    for implementation in registry.implementations.values() {
        if let PhaseImplementation::Operation(op) = implementation {
            let fragment = PreparedArtifactPlanFragment::new(
                op.prepared.descriptor(),
                &store,
                operation,
                producer.clone(),
                casa_imaging_runtime::WorkNodeId::new("prepared-phase-commit"),
                runtime.implementation.clone(),
            );
            let fragment = if let Some(source) = &op.source {
                fragment.with_import_source(source)
            } else {
                fragment
            };
            physical = fragment.compose(&physical)?;
        }
    }
    let execution_plan = plan(
        problem,
        PlanningBindings::new(
            runtime.registry,
            runtime.resource_policy.clone(),
            runtime.cost_model,
        ),
        &runtime.authority,
        &registry,
        &runtime.receipts,
        move |_, _| Ok::<_, std::convert::Infallible>(vec![physical]),
    )?;
    let executable =
        casa_imaging_reconstruction::ExecutableModelProblem::from_compiled(problem.clone())?;
    let current = RunBindings::new(
        problem.inputs().clone(),
        &runtime.resource_policy,
        runtime.cost_model.profile_id(),
    );
    let attempt = aw_attempt(runtime.attempts[0], phase);
    let execution = run(
        &executable,
        &execution_plan,
        &current,
        &registry,
        &runtime.authority,
        &mut RunToCompletion,
        runtime
            .receipts
            .bind(ExecutionProvenance::new(attempt, runtime.build)),
    );
    if let Err(error) = execution {
        if !(operation == PreparedArtifactOperation::Reuse
            && matches!(
                error,
                RunError::Evidence(ExecutionEvidenceError::RejectedArtifact { .. })
            ))
        {
            return Err(Box::new(error));
        }
    }
    Ok((registry.take_results()?, runtime.receipts.open(attempt)?))
}

fn aw_attempt(base: ExecutionAttemptId, phase: u64) -> ExecutionAttemptId {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"casa-rs:aw-preparation-attempt:v1");
    hash.update(base.as_bytes());
    hash.update(phase.to_le_bytes());
    ExecutionAttemptId::from_sha256(hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use casa_imaging_model::{
        AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
        DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
        GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
        InstrumentResponse, MeasurementEquationContract, ModelColumnWrite, ModelInnerProduct,
        NumericPrecision, NumericalStage, NumericsContract, ObservationTransactionRequirements,
        PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
        ProblemSpecification, ProductKind, ProductNormalization, ProductRequirements, Projection,
        ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
        ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy,
        ScientificContract, SkyDirection, SpectralContract, SpectralCoordinateSpec,
        SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, StageErrorBudget,
        UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope, WeightingContract,
        WeightingScheme, compile,
    };
    use casa_imaging_reconstruction::WeightingExecutionLimits;
    use casa_imaging_runtime::{
        BuildIdentity, ExecutionReceiptStore, ManagedSpillStorage, PlannerCostModelProfileId,
        ProductionStorageProfile, ReceiptRetention, ResourceAuthority, ResourcePolicy,
    };
    use tempfile::TempDir;

    use super::*;

    mod common {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
    }

    #[test]
    fn cold_then_warm_preparation_runs_through_plan_and_receipts() {
        let root = TempDir::new().expect("temporary preparation root");
        let casa = root.path().join("casa-cache");
        std::fs::create_dir(&casa).expect("create CASA cache root");
        crate::aw_cache::tests::write_test_cache(&casa);
        let private_root = root.path().join("prepared");
        let profile = ProductionStorageProfile::new(
            root.path(),
            1 << 30,
            1 << 30,
            100 << 20,
            100 << 20,
            4,
            4,
        )
        .expect("test storage profile")
        .with_measured_operations_rate(root.path())
        .expect("measured test storage operations");
        let cold_runtime = runtime(root.path(), &profile);
        let problem = problem();
        let deployment = ApplicationAwPreparation {
            casa_cache: casa,
            private_root,
            storage_domain: profile.storage_domain(),
            resident_bytes: 1 << 20,
            conjugate_beams: true,
        };

        let cold = prepare_aw_projection(&problem, deployment.clone(), &cold_runtime)
            .expect("cold Load then Consume");
        assert_eq!(cold.receipts.len(), 3);
        let operations_rate = profile
            .operations_rate_id()
            .expect("AW profile operations calibration")
            .as_str();
        let load_receipt = &cold.receipts[1];
        let selected = load_receipt.selected_alternative_projection();
        let operation_demands = selected
            .demand
            .storage
            .iter()
            .filter(|demand| demand.operations_rate.hard() > 0)
            .collect::<Vec<_>>();
        assert!(operation_demands.len() >= 2);
        assert!(
            operation_demands
                .iter()
                .all(|demand| demand.demand_id.contains(operations_rate)),
            "private-cache and CASA-source demands retain calibration provenance"
        );
        assert!(load_receipt.plan_node_identities().iter().any(|node| {
            load_receipt
                .stage_actual_io(node, IoBufferKind::StorageManager)
                .is_some_and(|(_, operations)| operations > 0)
        }));
        drop(cold.projection);

        let mut warm_runtime = runtime(root.path(), &profile);
        warm_runtime.attempts = [
            ExecutionAttemptId::from_sha256([7; 32]),
            ExecutionAttemptId::from_sha256([8; 32]),
            ExecutionAttemptId::from_sha256([9; 32]),
        ];
        let warm = prepare_aw_projection(&problem, deployment, &warm_runtime)
            .expect("warm Reuse then Consume");
        assert_eq!(warm.receipts.len(), 2);
        drop(warm.projection);
    }

    fn runtime(root: &Path, profile: &ProductionStorageProfile) -> ApplicationRuntime {
        let authority = ResourceAuthority::detected_with_storage_profile(profile)
            .expect("detected test authority");
        let storage_io = profile.io_resources();
        let spill = root.join("spill");
        std::fs::create_dir_all(&spill).expect("create spill root");
        ApplicationRuntime {
            registry: ImplementationRegistryId::from_sha256([1; 32]),
            implementation: WorkImplementationId::new("aw-preparation-test"),
            weighting_limits: WeightingExecutionLimits::new(16, 1).expect("test weighting limits"),
            stage_nanos: 1_000,
            minor_cycle_bytes: 1 << 20,
            storage_io: storage_io.clone(),
            gridded_normal_storage: ManagedSpillStorage::bind(&authority, storage_io, &spill)
                .expect("bind test spill"),
            confidence_parts_per_million: 900_000,
            resource_policy: ResourcePolicy::Exclusive,
            cost_model: PlannerCostModelProfileId::from_sha256([2; 32]).bootstrap(),
            authority,
            receipts: ExecutionReceiptStore::new(
                root.join("receipts"),
                ReceiptRetention::new(32, 8 << 20).expect("test receipt retention"),
            )
            .expect("test receipt store"),
            build: BuildIdentity::from_sha256([3; 32]),
            attempts: [
                ExecutionAttemptId::from_sha256([4; 32]),
                ExecutionAttemptId::from_sha256([5; 32]),
                ExecutionAttemptId::from_sha256([6; 32]),
            ],
        }
    }

    fn problem() -> CompiledProblem {
        let direction = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [31.0, 31.0],
            [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        );
        let geometry = GeometryInput::new(
            vec![ImageDomainSpec::new(
                ImageDomainRole::Main,
                ImageShape::new(64, 64),
                direction,
                FacetLayout::Single,
                AxisOrder::new([
                    ImageAxis::DirectionLongitude,
                    ImageAxis::DirectionLatitude,
                    ImageAxis::Polarization,
                    ImageAxis::Spectral,
                ]),
            )],
            CentreLaws::new(
                PhaseCentreLaw::Observation,
                DelayCentreLaw::PhaseTrackingCentre,
                PointingCentreLaw::PhaseTrackingCentre,
            ),
            UvwCoordinateLaw::PhaseTrackingCentre,
            SpectralCoordinateSpec::new(
                FrequencyFrame::Topocentric,
                FrequencyFrame::Topocentric,
                SpectralFrameAnchor::NotApplicable,
                SpectralWcs::Linear {
                    channels: 1,
                    reference_pixel: 0.0,
                    reference_frequency_hz: 1.4e9,
                    increment_hz: 1.0e6,
                },
                RestFrequency::NotApplicable,
                DopplerConvention::NotApplicable,
            ),
        );
        let specification = ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
                MeasurementEquationContract::new(
                    InstrumentResponse::Scalar,
                    DeclaredInnerProducts::new(
                        ModelInnerProduct::HermitianEuclidean,
                        VisibilityInnerProduct::HermitianEuclidean,
                    ),
                ),
            ),
            ReconstructionContract::new(
                ReconstructionBasis::Constant,
                ReconstructionAlgorithm::Dirty,
                ReconstructionControls::new(0, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                product_validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::DeterministicPairwise,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-12, 1.0e-3)))
                    .collect(),
            ),
        );
        compile(ImagingRequest::new(
            specification,
            geometry,
            common::problem_inputs(Vec::new()),
            common::model_lifecycle(),
        ))
        .expect("compile AW preparation test problem")
    }

    fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
        casa_imaging_model::ProductValidityPolicies::new(
            casa_imaging_model::PrimaryBeamValidityPolicy::new(
                0.2,
                casa_imaging_model::ProductSupportComparison::StrictlyGreater,
                casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
            )
            .expect("valid primary-beam policy"),
            casa_imaging_model::TaylorValidityPolicy::new(
                casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                0.1,
                casa_imaging_model::ProductSupportComparison::StrictlyGreater,
                casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
            )
            .expect("valid Taylor policy"),
        )
    }
}
