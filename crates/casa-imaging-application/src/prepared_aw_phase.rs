// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan/run-owned preparation of a complete AW convolution-function catalog.

use std::{
    collections::{BTreeMap, BTreeSet},
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
    PreparedArtifactExecutionBinding, PreparedArtifactImportSource, PreparedArtifactPlanFragment,
    PreparedArtifactReaderFactory, PreparedArtifactRegistration, PreparedArtifactReuseOutcome,
    PreparedArtifactStore, ResourceMeasurement, RunBindings, RunError, RunToCompletion,
    StorageDomain, WorkExecutionContext, WorkImplementation, WorkImplementationId,
    WorkMeasurements, plan, run,
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

enum CatalogOutcome {
    Reused(PreparedArtifactCatalogReuseOutcome),
    Imported(Vec<PreparedArtifact>),
}

struct ColdCatalog {
    cache: Arc<CasaAwCache>,
    cells: Vec<CasaAwPreparedCell>,
    sources: Vec<Option<PreparedArtifactImportSource>>,
}

struct CatalogAdapter {
    id: WorkImplementationId,
    store: Arc<PreparedArtifactStore>,
    descriptors: Vec<casa_imaging_runtime::PreparedArtifactDescriptor>,
    cold: Option<ColdCatalog>,
    result: Mutex<Option<CatalogOutcome>>,
}

enum PhaseImplementation {
    Base {
        id: WorkImplementationId,
        sources: Vec<(casa_imaging_runtime::ArtifactIdentity, u64)>,
    },
    Catalog(Box<CatalogAdapter>),
}

enum CatalogInput<'a> {
    Reuse(Vec<casa_imaging_runtime::PreparedArtifactDescriptor>),
    Import {
        cache: Arc<CasaAwCache>,
        cells: Vec<CasaAwPreparedCell>,
        reusable: BTreeSet<casa_imaging_runtime::ArtifactIdentity>,
        source_domain: &'a StorageDomain,
    },
}

enum CatalogPhaseResult {
    Reused(PreparedArtifactCatalogReuseOutcome),
    Imported(Vec<(CasaAwPreparedCell, PreparedArtifact)>),
}

impl WorkImplementation for PhaseImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        match self {
            Self::Base { id, .. } => id,
            Self::Catalog(op) => &op.id,
        }
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let op = match self {
            Self::Base { sources, .. } => return base_measurements(context, sources),
            Self::Catalog(op) => op,
        };
        let (result, measurements) = if let Some(cold) = &op.cold {
            let (artifacts, measurements) = op
                .store
                .import_catalog(&context, &op.descriptors, &cold.sources, |index| {
                    cold.cache.importer(&cold.cells[index]).map_err(|_| {
                        casa_imaging_runtime::PreparedArtifactError::SourceIdentityMismatch
                    })
                })
                .map_err(io::Error::other)?;
            (CatalogOutcome::Imported(artifacts), measurements)
        } else {
            let (outcome, measurements) = op
                .store
                .reuse_catalog(&context, &op.descriptors)
                .map_err(io::Error::other)?;
            (CatalogOutcome::Reused(outcome), measurements)
        };
        *op.result
            .lock()
            .map_err(|_| io::Error::other("AW catalog result state poisoned"))? = Some(result);
        Ok(measurements)
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
    sources: &[(casa_imaging_runtime::ArtifactIdentity, u64)],
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
                .map(|&(identity, bytes)| {
                    ArtifactMeasurement::new(
                        identity,
                        Some(identity),
                        ArtifactDisposition::Loaded,
                        bytes,
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
    fn catalog(&self) -> &CatalogAdapter {
        self.implementations
            .values()
            .find_map(|implementation| match implementation {
                PhaseImplementation::Catalog(op) => Some(op.as_ref()),
                PhaseImplementation::Base { .. } => None,
            })
            .expect("AW phase registry contains exactly one catalog")
    }

    fn into_result(self) -> Result<CatalogPhaseResult, ApplicationError> {
        let catalog = self
            .implementations
            .into_values()
            .find_map(|implementation| match implementation {
                PhaseImplementation::Catalog(op) => Some(op),
                PhaseImplementation::Base { .. } => None,
            })
            .expect("AW phase registry contains exactly one catalog");
        let result = catalog
            .result
            .into_inner()
            .map_err(|_| boxed("AW catalog result state poisoned"))?
            .ok_or_else(|| boxed("AW catalog operation did not complete"))?;
        match (catalog.cold, result) {
            (None, CatalogOutcome::Reused(outcome)) => Ok(CatalogPhaseResult::Reused(outcome)),
            (Some(cold), CatalogOutcome::Imported(artifacts)) => {
                if cold.cells.len() != artifacts.len()
                    || cold
                        .cells
                        .iter()
                        .zip(&artifacts)
                        .any(|(cell, artifact)| cell.descriptor().identity() != artifact.identity())
                {
                    return Err(boxed("AW cold catalog omitted or reordered an artifact"));
                }
                Ok(CatalogPhaseResult::Imported(
                    cold.cells.into_iter().zip(artifacts).collect(),
                ))
            }
            _ => Err(boxed(
                "AW catalog result did not match its planned operation",
            )),
        }
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
    let (catalog_outcome, catalog_receipt) = run_catalog(
        problem,
        runtime,
        Arc::clone(&store),
        CatalogInput::Reuse(descriptors),
    )?;
    receipts.push(catalog_receipt);

    let mut artifacts = Vec::new();
    let mut reusable = BTreeSet::new();
    let CatalogPhaseResult::Reused(catalog_outcome) = catalog_outcome else {
        return Err(boxed("AW reuse phase returned cold artifacts"));
    };
    let outcomes = catalog_outcome.into_entries();
    if outcomes.len() != ordered.len() {
        return Err(boxed("AW catalog reuse omitted an entry"));
    }
    for (cell, outcome) in ordered.iter().zip(outcomes) {
        if outcome.identity() != cell.descriptor().identity() {
            return Err(boxed("AW catalog reuse changed descriptor order"));
        }
        match outcome.into_outcome() {
            PreparedArtifactReuseOutcome::Reused(artifact) => {
                reusable.insert(artifact.identity());
                artifacts.push((cell.clone(), artifact));
            }
            PreparedArtifactReuseOutcome::Rejected(_) => {}
        }
    }
    if reusable.len() != ordered.len() {
        let (loaded, receipt) = run_catalog(
            problem,
            runtime,
            Arc::clone(&store),
            CatalogInput::Import {
                cache: Arc::clone(&cache),
                cells: ordered,
                reusable,
                source_domain: &deployment.storage_domain,
            },
        )?;
        receipts.push(receipt);
        let CatalogPhaseResult::Imported(loaded) = loaded else {
            return Err(boxed("AW cold phase returned reuse outcomes"));
        };
        artifacts = loaded;
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

fn run_catalog(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    store: Arc<PreparedArtifactStore>,
    input: CatalogInput<'_>,
) -> Result<(CatalogPhaseResult, ExecutionReceipt), ApplicationError> {
    let producer = casa_imaging_runtime::WorkNodeId::new("prepared-phase-producer");
    let commit = casa_imaging_runtime::WorkNodeId::new("prepared-phase-commit");
    let (descriptors, cold) = match input {
        CatalogInput::Reuse(descriptors) => (descriptors, None),
        CatalogInput::Import {
            cache,
            cells,
            reusable,
            source_domain,
        } => {
            let sources = cells
                .iter()
                .map(|cell| {
                    (!reusable.contains(&cell.descriptor().identity()))
                        .then(|| cell.import_source(&cache, source_domain, producer.clone()))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let descriptors = cells.iter().map(|cell| cell.descriptor().clone()).collect();
            (
                descriptors,
                Some(ColdCatalog {
                    cache,
                    cells,
                    sources,
                }),
            )
        }
    };
    let reuse = cold.is_none();
    let mut fragment = PreparedArtifactCatalogPlanFragment::new(
        &descriptors,
        &store,
        producer.clone(),
        commit.clone(),
        runtime.implementation.clone(),
    )?;
    if let Some(cold) = &cold {
        fragment = fragment.with_import_sources(&cold.sources)?;
    }
    let id = fragment.work_implementation_id()?;
    let sources = cold
        .iter()
        .flat_map(|cold| &cold.sources)
        .flatten()
        .map(|source| (source.identity(), source.source_read_bytes()))
        .collect();
    let registry = PhaseRegistry {
        id: runtime.registry,
        metadata: ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        implementations: BTreeMap::from([
            (
                id.clone(),
                PhaseImplementation::Catalog(Box::new(CatalogAdapter {
                    id,
                    store: Arc::clone(&store),
                    descriptors,
                    cold,
                    result: Mutex::new(None),
                })),
            ),
            (
                runtime.implementation.clone(),
                PhaseImplementation::Base {
                    id: runtime.implementation.clone(),
                    sources,
                },
            ),
        ]),
        prepared_artifact: crate::prepared_aw_registration(runtime.implementation.clone()),
    };
    let catalog = registry.catalog();
    let base = PreparedArtifactPlanFragment::standalone_base(
        problem,
        &registry,
        runtime.implementation.clone(),
        &catalog.descriptors[0],
        &store,
        runtime.stage_nanos,
        runtime.confidence_parts_per_million,
    )?;
    let mut fragment = PreparedArtifactCatalogPlanFragment::new(
        &catalog.descriptors,
        &store,
        producer,
        commit,
        runtime.implementation.clone(),
    )?;
    if let Some(cold) = &catalog.cold {
        fragment = fragment.with_import_sources(&cold.sources)?;
    }
    let physical = fragment.compose(&base)?;
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
    let attempt = aw_attempt(runtime.attempts[0], u64::from(!reuse));
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
        && !(reuse
            && matches!(
                error,
                RunError::Evidence(ExecutionEvidenceError::RejectedArtifact { .. })
            ))
    {
        return Err(Box::new(error));
    }
    Ok((registry.into_result()?, runtime.receipts.open(attempt)?))
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
    mod catalog_scale_probe;
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
        let cold_catalog = &cold.receipts[0];
        assert_eq!(
            cold_catalog.status(),
            casa_imaging_runtime::ReceiptStatus::Failed
        );
        assert_eq!(
            cold_catalog.failure_kind(),
            Some(casa_imaging_runtime::ReceiptFailureKind::EvidenceContract)
        );
        let artifacts = cold_catalog
            .artifact_identities()
            .into_iter()
            .filter(|artifact| {
                cold_catalog.artifact_role(*artifact)
                    == Some(casa_imaging_runtime::ArtifactRole::Cache)
            })
            .collect::<Vec<_>>();
        assert_eq!(artifacts.len(), 1);
        assert!(artifacts.into_iter().all(|artifact| {
            cold_catalog.artifact_disposition(artifact)
                == Some(casa_imaging_runtime::ArtifactDisposition::RejectedStale)
        }));
        let operations_rate = profile
            .operations_rate_id()
            .expect("AW profile operations calibration")
            .as_str();
        let load_receipt = &cold.receipts[1];
        assert_eq!(
            load_receipt.status(),
            casa_imaging_runtime::ReceiptStatus::Completed
        );
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
        assert_eq!(
            warm.receipts[0].status(),
            casa_imaging_runtime::ReceiptStatus::Completed
        );
        drop(warm.bind_plan().expect("fresh warm reader binding"));
    }

    #[test]
    fn t51_dirty_to_clean_reuses_identical_prepared_cells_without_mutation() {
        use std::{collections::BTreeMap, os::unix::fs::MetadataExt};

        let snapshot = |root: &Path| {
            let mut files = BTreeMap::new();
            let mut pending = vec![root.to_path_buf()];
            while let Some(directory) = pending.pop() {
                for entry in std::fs::read_dir(directory).expect("read prepared cache") {
                    let path = entry.expect("cache entry").path();
                    let metadata = path.metadata().expect("cache metadata");
                    if metadata.is_dir() {
                        pending.push(path);
                    } else if matches!(
                        path.file_name().and_then(|name| name.to_str()),
                        Some("manifest.json" | "payload.bin")
                    ) {
                        files.insert(
                            path.strip_prefix(root)
                                .expect("relative cache path")
                                .to_path_buf(),
                            (
                                metadata.ino(),
                                metadata.mtime(),
                                metadata.mtime_nsec(),
                                std::fs::read(path).expect("immutable cache bytes"),
                            ),
                        );
                    }
                }
            }
            files
        };
        for cell_count in [1, 2] {
            let root = TempDir::new().expect("temporary preparation root");
            let casa = root.path().join("casa-cache");
            std::fs::create_dir(&casa).expect("create CASA cache root");
            if cell_count == 1 {
                crate::aw_cache::tests::write_test_cache(&casa);
            } else {
                crate::aw_cache::tests::write_two_cell_test_cache(&casa);
            }
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
            let deployment = ApplicationAwPreparation {
                casa_cache: casa,
                private_root: root.path().join("prepared"),
                storage_domain: profile.storage_domain(),
                resident_bytes: 1 << 20,
                conjugate_beams: true,
            };
            let dirty = problem();
            let clean = problem_for(ReconstructionAlgorithm::Hogbom, 30);
            assert_ne!(dirty.problem_id(), clean.problem_id());
            assert_eq!(
                dirty.geometry().geometry_id(),
                clean.geometry().geometry_id()
            );
            let cold =
                prepare_aw_projection(&dirty, deployment.clone(), &runtime(root.path(), &profile))
                    .expect("cold DIRTY preparation");
            assert_eq!(cold.receipts.len(), 2);
            drop(cold.bind_plan().expect("bind DIRTY reader"));
            let before = snapshot(&deployment.private_root);
            assert_eq!(
                before.len(),
                cell_count * 2,
                "each paired cell has one manifest and payload"
            );
            let private_root = deployment.private_root.clone();
            let mut clean_runtime = runtime(root.path(), &profile);
            clean_runtime.attempts = [
                ExecutionAttemptId::from_sha256([7; 32]),
                ExecutionAttemptId::from_sha256([8; 32]),
                ExecutionAttemptId::from_sha256([9; 32]),
            ];
            let warm = prepare_aw_projection(&clean, deployment, &clean_runtime)
                .expect("warm CLEAN preparation");
            assert_eq!(
                warm.receipts.len(),
                1,
                "CLEAN must reuse, not cold-load, DIRTY's identical cells"
            );
            drop(warm.bind_plan().expect("bind CLEAN reader"));
            assert_eq!(
                snapshot(&private_root),
                before,
                "warm reuse must not rewrite or replace cache files"
            );
        }
    }

    #[test]
    fn two_cell_cold_load_keeps_every_plan_within_two_queue_slots() {
        use std::os::unix::fs::MetadataExt;
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
        let mut runtime = runtime(root.path(), &profile);
        let deployment = ApplicationAwPreparation {
            casa_cache: casa,
            private_root,
            storage_domain: profile.storage_domain(),
            resident_bytes: 1 << 20,
            conjugate_beams: true,
        };
        let phase = prepare_aw_projection(&problem(), deployment.clone(), &runtime)
            .expect("two-cell cold preparation");

        assert_eq!(
            phase.receipts.len(),
            2,
            "one probe and one cold catalog phase"
        );

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
            cold_load_nodes.extend(receipt.plan_node_identities().into_iter().filter(|node| {
                node.as_str()
                    .starts_with("prepared-artifact-catalog-cold-load-")
            }));
        }
        assert_eq!(cache_demands.len(), 1);
        assert_eq!(cold_load_nodes.len(), 1);
        drop(phase.bind_plan().expect("fresh two-cell reader binding"));

        let missing = phase.prepared[0].descriptor().identity();
        let reused = phase.prepared[1].descriptor().identity();
        let objects = deployment.private_root.join("objects-v3");
        std::fs::rename(
            objects.join(missing.to_string()),
            root.path().join("interrupted-cell"),
        )
        .expect("simulate an absent cell without touching the warm one");
        let snapshot = || {
            ["manifest.json", "payload.bin"].map(|name| {
                let path = objects.join(reused.to_string()).join(name);
                let metadata = std::fs::metadata(&path).expect("warm metadata");
                (
                    metadata.ino(),
                    metadata.mtime(),
                    metadata.mtime_nsec(),
                    std::fs::read(path).expect("warm bytes"),
                )
            })
        };
        let before = snapshot();
        runtime.attempts[0] = ExecutionAttemptId::from_sha256([10; 32]);
        let mixed = prepare_aw_projection(&problem(), deployment, &runtime)
            .expect("mixed cold/warm catalog preparation");
        assert_eq!(mixed.receipts.len(), 2);
        let terminal = &mixed.receipts[1];
        assert_eq!(
            terminal.artifact_disposition(missing),
            Some(ArtifactDisposition::Loaded)
        );
        assert_eq!(
            terminal.artifact_disposition(reused),
            Some(ArtifactDisposition::Reused)
        );
        assert_eq!(
            snapshot(),
            before,
            "mixed preparation must not rewrite the warm member"
        );
        drop(
            mixed
                .bind_plan()
                .expect("complete mixed-catalog reader binding"),
        );
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
        problem_for(ReconstructionAlgorithm::Dirty, 0)
    }

    fn problem_for(algorithm: ReconstructionAlgorithm, iterations: usize) -> CompiledProblem {
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
                algorithm,
                ReconstructionControls::new(iterations, 1.0, 0.0),
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
