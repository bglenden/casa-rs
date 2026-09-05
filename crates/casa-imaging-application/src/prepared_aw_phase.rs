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
    PreparedArtifactCatalogPlanFragment, PreparedArtifactCatalogReuseOutcome,
    PreparedArtifactExecutionBinding, PreparedArtifactImportSource, PreparedArtifactOperation,
    PreparedArtifactPlanFragment, PreparedArtifactReaderFactory, PreparedArtifactRegistration,
    PreparedArtifactReuseOutcome, PreparedArtifactStore, ResourceMeasurement, RunBindings,
    RunError, RunToCompletion, StorageDomain, WorkExecutionContext, WorkImplementation,
    WorkImplementationId, WorkMeasurements, plan, run,
};

use crate::{
    ApplicationAwPreparation, ApplicationError, ApplicationRuntime, CasaAwCache,
    CasaAwPreparedCell, PreparedAwCellProvider, boxed,
};

/// Complete pre-phase result retained by every later major cycle.
pub(crate) struct PreparedAwPhase {
    catalog: casa_imaging_reconstruction::AwPreparedCatalog,
    prepared: Vec<CasaAwPreparedCell>,
    reader: PreparedArtifactReaderFactory,
    conjugate_beams: bool,
    resident_bytes: usize,
    pub receipts: Vec<ExecutionReceipt>,
}

pub(crate) struct PreparedAwPlanBinding {
    pub projection: PreparedAwProjection,
    pub execution: PreparedArtifactExecutionBinding,
}

impl PreparedAwPhase {
    pub fn bind_plan(&self) -> Result<PreparedAwPlanBinding, ApplicationError> {
        let reader = self.reader.session();
        let provider = PreparedAwCellProvider::new(
            self.resident_bytes,
            Arc::clone(&reader),
            self.prepared.clone(),
        )?;
        let projection = PreparedAwProjection::new(
            self.catalog.clone(),
            provider.clone(),
            self.conjugate_beams,
            self.resident_bytes,
        )?;
        Ok(PreparedAwPlanBinding {
            projection,
            execution: PreparedArtifactExecutionBinding::new(reader, provider),
        })
    }
}

enum OperationResult {
    Artifact(PreparedArtifact),
    Rejected,
}

struct OperationAdapter {
    id: WorkImplementationId,
    operation: PreparedArtifactOperation,
    store: Arc<PreparedArtifactStore>,
    cache: Arc<CasaAwCache>,
    prepared: CasaAwPreparedCell,
    source: Option<PreparedArtifactImportSource>,
    result: Mutex<Option<OperationResult>>,
}

struct CatalogReuseAdapter {
    id: WorkImplementationId,
    store: Arc<PreparedArtifactStore>,
    descriptors: Vec<casa_imaging_runtime::PreparedArtifactDescriptor>,
    result: Mutex<Option<PreparedArtifactCatalogReuseOutcome>>,
}

enum PhaseImplementation {
    Base {
        id: WorkImplementationId,
        sources: Vec<PreparedArtifactImportSource>,
    },
    CatalogReuse(Box<CatalogReuseAdapter>),
    Operation(Box<OperationAdapter>),
}

struct OperationInput<'a> {
    cache: Arc<CasaAwCache>,
    store: Arc<PreparedArtifactStore>,
    cell: CasaAwPreparedCell,
    source_domain: &'a StorageDomain,
    operation: PreparedArtifactOperation,
    phase: u64,
}

impl WorkImplementation for PhaseImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        match self {
            Self::Base { id, .. } => id,
            Self::CatalogReuse(op) => &op.id,
            Self::Operation(op) => &op.id,
        }
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let op = match self {
            Self::Base { sources, .. } => return base_measurements(context, sources),
            Self::CatalogReuse(op) => {
                let (outcome, measurements) = op
                    .store
                    .reuse_catalog(&context, &op.descriptors)
                    .map_err(io::Error::other)?;
                *op.result
                    .lock()
                    .map_err(|_| io::Error::other("AW catalog result state poisoned"))? =
                    Some(outcome);
                return Ok(measurements);
            }
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
                return Err(io::Error::other(
                    "AW preparation cannot eagerly consume prepared payloads",
                ));
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
    fn take_result(&self) -> Result<(CasaAwPreparedCell, OperationResult), ApplicationError> {
        let operation = self
            .implementations
            .values()
            .find_map(|implementation| match implementation {
                PhaseImplementation::Base { .. } | PhaseImplementation::CatalogReuse(_) => None,
                PhaseImplementation::Operation(op) => Some(op),
            })
            .ok_or_else(|| boxed("AW operation is absent"))?;
        let result = operation
            .result
            .lock()
            .map_err(|_| boxed("AW result state poisoned"))?
            .take()
            .ok_or_else(|| boxed("AW operation did not execute"))?;
        Ok((operation.prepared.clone(), result))
    }

    fn take_catalog_result(&self) -> Result<PreparedArtifactCatalogReuseOutcome, ApplicationError> {
        let catalog = self
            .implementations
            .values()
            .find_map(|implementation| match implementation {
                PhaseImplementation::CatalogReuse(op) => Some(op),
                PhaseImplementation::Base { .. } | PhaseImplementation::Operation(_) => None,
            })
            .ok_or_else(|| boxed("AW catalog reuse operation is absent"))?;
        catalog
            .result
            .lock()
            .map_err(|_| boxed("AW catalog result state poisoned"))?
            .take()
            .ok_or_else(|| boxed("AW catalog reuse operation did not execute"))
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
    let inventory = cache.inventory();
    let entries = inventory.paired_cells;
    let prepared_cache_bytes = cache.prepared_cache_bytes()?;
    eprintln!(
        "imaging_aw_cache_inventory_summary paired_cells={} frequencies={} w_values={} mueller_elements={} parallactic_angles={} prepared_cache_bytes={} decoded_resident_ceiling_bytes={}",
        entries,
        inventory.frequencies_hz.len(),
        inventory.w_values_lambda.len(),
        inventory.mueller_elements.len(),
        inventory.parallactic_angles_deg.len(),
        prepared_cache_bytes,
        deployment.resident_bytes,
    );
    let budget =
        casa_imaging_runtime::PreparedArtifactBudget::new(prepared_cache_bytes, entries, 8 << 20)?;
    let store = Arc::new(PreparedArtifactStore::open(
        &deployment.private_root,
        &deployment.storage_domain,
        budget,
    )?);
    let owner =
        super::PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    let prepared = cache.prepared_cells(&store, &owner, &runtime.implementation, problem)?;
    let largest_cell = prepared
        .iter()
        .map(CasaAwPreparedCell::decoded_resident_bytes)
        .collect::<Option<Vec<_>>>()
        .and_then(|bytes| bytes.into_iter().max())
        .ok_or_else(|| boxed("AW decoded cell residency overflowed"))?;
    if largest_cell > deployment.resident_bytes {
        return Err(Box::new(io::Error::other(format!(
            "largest AW decoded cell requires {largest_cell} bytes, exceeding the {} byte ceiling",
            deployment.resident_bytes
        ))));
    }
    let decoder_workspace_bytes = prepared
        .iter()
        .map(CasaAwPreparedCell::decoder_workspace_bytes)
        .collect::<Option<Vec<_>>>()
        .and_then(|bytes| bytes.into_iter().max())
        .ok_or_else(|| boxed("AW decoder workspace overflowed"))?;
    let mut receipts = Vec::new();

    let mut ordered = prepared.to_vec();
    ordered.sort_by_key(|cell| cell.descriptor().identity().as_bytes());
    let descriptors = ordered
        .iter()
        .map(|cell| cell.descriptor().clone())
        .collect::<Vec<_>>();
    let (catalog_outcome, catalog_receipt) =
        run_catalog_reuse(problem, runtime, Arc::clone(&store), descriptors)?;
    receipts.push(catalog_receipt);

    let mut artifacts = Vec::new();
    let mut missing = Vec::new();
    let outcomes = catalog_outcome.into_entries();
    if outcomes.len() != ordered.len() {
        return Err(boxed("AW catalog reuse omitted an entry"));
    }
    for (cell, outcome) in ordered.into_iter().zip(outcomes) {
        if outcome.identity() != cell.descriptor().identity() {
            return Err(boxed("AW catalog reuse changed descriptor order"));
        }
        match outcome.into_outcome() {
            PreparedArtifactReuseOutcome::Reused(artifact) => artifacts.push((cell, artifact)),
            PreparedArtifactReuseOutcome::Rejected(_) => missing.push(cell),
        }
    }
    for (ordinal, cell) in missing.into_iter().enumerate() {
        let loaded = run_operation(
            problem,
            runtime,
            OperationInput {
                cache: Arc::clone(&cache),
                store: Arc::clone(&store),
                cell,
                source_domain: &deployment.storage_domain,
                operation: PreparedArtifactOperation::Load,
                phase: 1_000_000 + ordinal as u64,
            },
        )?;
        receipts.push(loaded.1);
        let (cell, result) = loaded.0;
        if let OperationResult::Artifact(artifact) = result {
            artifacts.push((cell, artifact));
        } else {
            return Err(boxed("AW cold load omitted artifact"));
        }
    }
    artifacts.sort_by_key(|(cell, _)| cell.metadata().identity().as_bytes());
    if artifacts.len() != entries {
        return Err(boxed("AW preparation omitted a catalog artifact"));
    }
    let reader = PreparedArtifactReaderFactory::new(
        Arc::clone(&store),
        artifacts
            .into_iter()
            .map(|(cell, artifact)| (cell.descriptor().clone(), artifact))
            .collect(),
        runtime.implementation.clone(),
        u64::try_from(deployment.resident_bytes)?,
        u64::try_from(decoder_workspace_bytes)?,
    )?;
    Ok(PreparedAwPhase {
        catalog,
        prepared,
        reader,
        conjugate_beams: deployment.conjugate_beams,
        resident_bytes: deployment.resident_bytes,
        receipts,
    })
}

fn run_catalog_reuse(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    store: Arc<PreparedArtifactStore>,
    descriptors: Vec<casa_imaging_runtime::PreparedArtifactDescriptor>,
) -> Result<(PreparedArtifactCatalogReuseOutcome, ExecutionReceipt), ApplicationError> {
    let metadata = ImplementationContractMetadata::new(
        problem.problem_id(),
        problem.numerics_id(),
        problem.required_capabilities().clone(),
    );
    let producer = casa_imaging_runtime::WorkNodeId::new("prepared-phase-producer");
    let id = PreparedArtifactCatalogPlanFragment::implementation_id(&descriptors)?;
    let mut implementations = BTreeMap::from([(
        id.clone(),
        PhaseImplementation::CatalogReuse(Box::new(CatalogReuseAdapter {
            id,
            store: Arc::clone(&store),
            descriptors,
            result: Mutex::new(None),
        })),
    )]);
    implementations.insert(
        runtime.implementation.clone(),
        PhaseImplementation::Base {
            id: runtime.implementation.clone(),
            sources: vec![],
        },
    );
    let registry = PhaseRegistry {
        id: runtime.registry,
        metadata,
        implementations,
        prepared_artifact: crate::prepared_aw_registration(runtime.implementation.clone()),
    };
    let descriptors = registry
        .implementations
        .values()
        .find_map(|implementation| match implementation {
            PhaseImplementation::CatalogReuse(op) => Some(op.descriptors.as_slice()),
            PhaseImplementation::Base { .. } | PhaseImplementation::Operation(_) => None,
        })
        .ok_or_else(|| boxed("AW catalog reuse operation is absent"))?;
    let first = descriptors
        .first()
        .ok_or_else(|| boxed("AW prepared catalog is empty"))?;
    let base = PreparedArtifactPlanFragment::standalone_base(
        problem,
        &registry,
        runtime.implementation.clone(),
        first,
        &store,
        runtime.stage_nanos,
        runtime.confidence_parts_per_million,
    )?;
    let physical = PreparedArtifactCatalogPlanFragment::new(
        descriptors,
        &store,
        producer,
        casa_imaging_runtime::WorkNodeId::new("prepared-phase-commit"),
        runtime.implementation.clone(),
    )?
    .compose(&base)?;
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
    let attempt = aw_attempt(runtime.attempts[0], 0);
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
    if let Err(error) = execution
        && !matches!(
            error,
            RunError::Evidence(ExecutionEvidenceError::RejectedArtifact { .. })
        )
    {
        return Err(Box::new(error));
    }
    Ok((
        registry.take_catalog_result()?,
        runtime.receipts.open(attempt)?,
    ))
}

fn run_operation(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    input: OperationInput<'_>,
) -> Result<((CasaAwPreparedCell, OperationResult), ExecutionReceipt), ApplicationError> {
    let OperationInput {
        cache,
        store,
        cell,
        source_domain,
        operation,
        phase,
    } = input;
    let metadata = ImplementationContractMetadata::new(
        problem.problem_id(),
        problem.numerics_id(),
        problem.required_capabilities().clone(),
    );
    let producer = casa_imaging_runtime::WorkNodeId::new("prepared-phase-producer");
    let mut implementations = BTreeMap::new();
    let id = cell.descriptor().work_implementation_id(operation);
    let source = (operation == PreparedArtifactOperation::Load)
        .then(|| cell.import_source(&cache, source_domain, producer.clone()))
        .transpose()?;
    let sources = source.iter().cloned().collect();
    implementations.insert(
        id.clone(),
        PhaseImplementation::Operation(Box::new(OperationAdapter {
            id,
            operation,
            store: Arc::clone(&store),
            cache: Arc::clone(&cache),
            prepared: cell,
            source,
            result: Mutex::new(None),
        })),
    );
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
    let descriptor = registry
        .implementations
        .values()
        .find_map(|implementation| match implementation {
            PhaseImplementation::Base { .. } | PhaseImplementation::CatalogReuse(_) => None,
            PhaseImplementation::Operation(op) => Some(op.prepared.descriptor()),
        })
        .ok_or_else(|| boxed("AW operation is absent"))?;
    let mut physical = PreparedArtifactPlanFragment::standalone_base(
        problem,
        &registry,
        runtime.implementation.clone(),
        descriptor,
        &store,
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
    Ok((registry.take_result()?, runtime.receipts.open(attempt)?))
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
    mod cold_load_probe;

    use std::{collections::BTreeSet, path::Path};

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
    fn cold_load_with_two_queue_slots_then_warm_reuse_runs_through_plan_and_receipts() {
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
            2,
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

        let cold =
            prepare_aw_projection(&problem, deployment.clone(), &cold_runtime).expect("cold Load");
        assert_eq!(cold.receipts.len(), 2);
        let operations_rate = profile
            .operations_rate_id()
            .expect("AW profile operations calibration")
            .as_str();
        let load_receipt = &cold.receipts[1];
        let selected = load_receipt.selected_alternative_projection();
        assert_eq!(
            selected
                .demand
                .storage
                .iter()
                .map(|demand| demand.queue_slots.hard())
                .sum::<u64>(),
            2,
            "cold load reserves only its concurrent source and cache operations"
        );
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
        drop(cold.bind_plan().expect("fresh cold reader binding"));

        let mut warm_runtime = runtime(root.path(), &profile);
        warm_runtime.attempts = [
            ExecutionAttemptId::from_sha256([7; 32]),
            ExecutionAttemptId::from_sha256([8; 32]),
            ExecutionAttemptId::from_sha256([9; 32]),
        ];
        let warm = prepare_aw_projection(&problem, deployment, &warm_runtime).expect("warm Reuse");
        assert_eq!(warm.receipts.len(), 1);
        drop(warm.bind_plan().expect("fresh warm reader binding"));
    }

    #[test]
    fn two_cell_cold_load_keeps_every_plan_within_two_queue_slots() {
        let root = TempDir::new().expect("temporary preparation root");
        let casa = root.path().join("casa-cache");
        std::fs::create_dir(&casa).expect("create CASA cache root");
        crate::aw_cache::tests::write_two_cell_test_cache(&casa);
        let private_root = root.path().join("prepared");
        let profile = ProductionStorageProfile::new(
            root.path(),
            1 << 30,
            1 << 30,
            100 << 20,
            100 << 20,
            2,
            4,
        )
        .expect("test storage profile")
        .with_measured_operations_rate(root.path())
        .expect("measured test storage operations");
        let runtime = runtime(root.path(), &profile);
        let phase = prepare_aw_projection(
            &problem(),
            ApplicationAwPreparation {
                casa_cache: casa,
                private_root,
                storage_domain: profile.storage_domain(),
                resident_bytes: 1 << 20,
                conjugate_beams: true,
            },
            &runtime,
        )
        .expect("two-cell cold preparation");

        let mut cache_demands = BTreeSet::new();
        let mut cold_load_nodes = BTreeSet::new();
        for receipt in &phase.receipts {
            let selected = receipt.selected_alternative_projection();
            let queue_slots = selected
                .demand
                .storage
                .iter()
                .map(|demand| demand.queue_slots.hard())
                .sum::<u64>();
            assert!(
                queue_slots <= 2,
                "{} reserves {queue_slots} storage queue slots",
                selected.id.as_str()
            );
            cache_demands.extend(
                selected
                    .demand
                    .storage
                    .iter()
                    .filter(|demand| demand.demand_id.starts_with("private-prepared-cache-"))
                    .map(|demand| demand.demand_id.clone()),
            );
            cold_load_nodes.extend(
                receipt
                    .plan_node_identities()
                    .into_iter()
                    .filter(|node| node.as_str().starts_with("prepared-artifact-cold-load-")),
            );
        }
        assert_eq!(cache_demands.len(), 1);
        assert_eq!(cold_load_nodes.len(), 2);
        drop(phase.bind_plan().expect("fresh two-cell reader binding"));
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
