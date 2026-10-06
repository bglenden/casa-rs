// SPDX-License-Identifier: LGPL-3.0-or-later

//! Requested native preparation shares the exact store, catalog plan and writer.

use super::*;
use crate::{AllocationId, PhysicalWorkBinding};
use casa_imaging_model::{EvlaAwCellRequest, NativeAwRequest};

const REQUEST_DOMAIN: &[u8] = b"casa-rs/native-prepared/request/v1\0";
const OUTPUT_CONTRACT: &[u8] = b"paired-C32-last-axis-contiguous-LE/even-centered-crop/TM2-recomputed-cropped-FT-coordinates/v1\0";

/// Explicit native catalog action; consuming a catalog never authorizes generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactNativeOperation {
    /// Resolve bounded metadata and revalidate all expected cells, without generating.
    Reuse,
    /// Reuse valid members and explicitly generate missing members.
    Generate,
    /// Explicitly regenerate every member, checking existing immutable publications.
    Regenerate,
}

impl PreparedArtifactNativeOperation {
    const fn tag(self) -> u8 {
        match self {
            Self::Reuse => 0,
            Self::Generate => 1,
            Self::Regenerate => 2,
        }
    }

    const fn ordinary(self) -> PreparedArtifactOperation {
        match self {
            Self::Reuse => PreparedArtifactOperation::Reuse,
            Self::Generate | Self::Regenerate => PreparedArtifactOperation::Generate,
        }
    }
}

/// Only support-selection outputs may vary; all other semantics belong to the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedArtifactNativePlaneLayout {
    /// Even square crop within the full requested working plane.
    pub shape: [u64; 2],
    /// Positive X/Y support extents, in normally sampled grid pixels.
    pub support: [u64; 2],
}

/// Independently selected imaging and weight layouts for a generated pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedArtifactNativeLayout {
    /// Imaging convolution plane.
    pub imaging: PreparedArtifactNativePlaneLayout,
    /// Weight convolution plane.
    pub weight: PreparedArtifactNativePlaneLayout,
}

/// Lazy admitted numeric provider. Construction must not allocate numerical workspaces.
pub trait PreparedArtifactNativeGenerator: PreparedArtifactGenerator {
    /// Generate one pair in the admitted reusable workspace. The store subsequently
    /// streams the current pair using the ordinary bounded generator interface.
    fn generate_cell(
        &mut self,
        index: usize,
        cell: EvlaAwCellRequest,
        workspace_limit: u64,
    ) -> Result<PreparedArtifactNativeLayout, PreparedArtifactError>;

    /// Peak charged numerical workspace: owned numeric capacities plus the
    /// conservative opaque FFT reservation, not an allocator or RSS measurement.
    fn workspace_peak_bytes(&self) -> u64;
}

#[derive(Clone, Debug)]
struct NativeCell {
    identity: ArtifactIdentity,
    scientific: ScientificCommitments,
}

/// Runtime-owned requested identity P, before any realized exact layout D exists.
#[derive(Clone, Debug)]
pub struct PreparedArtifactNativeRequest {
    request: NativeAwRequest,
    owner: PreparedArtifactOwner,
    cache_scope: CacheScope,
    cache_identity: CacheIdentity,
    execution_problem: CompiledProblemId,
    cells: Vec<NativeCell>,
}

impl PreparedArtifactNativeRequest {
    /// Bind a validated model request to this execution, provider and private cache.
    pub fn new<R: ImplementationRegistry>(
        store: &PreparedArtifactStore,
        registry: &R,
        implementation: &WorkImplementationId,
        problem: &CompiledProblem,
        request: NativeAwRequest,
    ) -> Result<Self, PreparedArtifactError> {
        if request.geometry() != problem.geometry().geometry_id() {
            return Err(PreparedArtifactError::ScientificBindingMismatch);
        }
        if request.cell_count() > store.budget.entries {
            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: request.cell_count(),
                budget: store.budget.entries,
            });
        }
        let owner = PreparedArtifactOwner::from_registry(registry, implementation)?;
        let cache_identity = derive_cache_identity(&owner, &store.scope)?;
        let cells = (0..request.cell_count())
            .map(|index| {
                let (_, identity) = request.cell(index).expect("validated expected cell");
                let scientific = ScientificCommitments::from_problem(problem, identity);
                scientific.validate()?;
                let mut digest = Sha256::new();
                digest.update(REQUEST_DOMAIN);
                digest.update(OUTPUT_CONTRACT);
                digest.update(cache_identity.as_bytes());
                digest.update(serde_json::to_vec(&scientific)?);
                Ok(NativeCell {
                    identity: ArtifactIdentity::from_owner_digest(digest.finalize().into()),
                    scientific,
                })
            })
            .collect::<Result<Vec<_>, PreparedArtifactError>>()?;
        Ok(Self {
            request,
            owner,
            cache_scope: store.scope.clone(),
            cache_identity,
            execution_problem: problem.problem_id(),
            cells,
        })
    }

    /// Frozen scientific request, independent of any persisted manifest.
    #[must_use]
    pub const fn request(&self) -> &NativeAwRequest {
        &self.request
    }

    /// Runtime-owned logical identity of one expected cell in model order.
    #[must_use]
    pub fn cell_identity(&self, index: usize) -> Option<ArtifactIdentity> {
        self.cells.get(index).map(|cell| cell.identity)
    }

    fn demand_id(&self, store: &PreparedArtifactStore) -> String {
        let base = format!("private-prepared-cache-{}", self.cache_identity);
        match &store.storage_operations_rate {
            Some(rate) => format!("{base}-operations-{}", rate.as_str()),
            None => base,
        }
    }

    fn digest(&self, operation: PreparedArtifactNativeOperation, domain: &[u8]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(domain);
        digest.update([operation.tag()]);
        for cell in &self.cells {
            digest.update(cell.identity.as_bytes());
        }
        digest.finalize().into()
    }

    fn node(&self, operation: PreparedArtifactNativeOperation) -> WorkNodeId {
        WorkNodeId::new(format!(
            "native-prepared-catalog-{}",
            encode_hex(&self.digest(operation, b"native-prepared-node/v1\0"))
        ))
    }

    fn implementation(&self, operation: PreparedArtifactNativeOperation) -> WorkImplementationId {
        WorkImplementationId::new(format!(
            "native-prepared-catalog-{}",
            encode_hex(&self.digest(operation, b"native-prepared-implementation/v1\0"))
        ))
    }

    fn ledger(&self, index: usize, operation: PreparedArtifactNativeOperation) -> ArtifactIdentity {
        let mut digest = Sha256::new();
        digest.update(b"native-prepared-evictions/v1\0");
        digest.update(self.cells[index].identity.as_bytes());
        digest.update([operation.tag()]);
        ArtifactIdentity::from_owner_digest(digest.finalize().into())
    }

    fn planned(&self, operation: PreparedArtifactNativeOperation) -> Vec<PlannedArtifact> {
        let node = self.node(operation);
        self.cells
            .iter()
            .enumerate()
            .flat_map(|(index, cell)| {
                [
                    PlannedArtifact::new(
                        cell.identity,
                        node.clone(),
                        if operation == PreparedArtifactNativeOperation::Reuse {
                            ArtifactRole::Cache
                        } else {
                            ArtifactRole::Prepared
                        },
                        Some(self.cache_identity),
                    ),
                    PlannedArtifact::new(
                        self.ledger(index, operation),
                        node.clone(),
                        ArtifactRole::Input,
                        None,
                    ),
                ]
            })
            .collect()
    }

    fn workspace_id(&self, operation: PreparedArtifactNativeOperation) -> AllocationId {
        AllocationId::new(format!(
            "native-generation-workspace-{}",
            self.node(operation).as_str()
        ))
    }

    fn workspace_bytes(
        &self,
        operation: PreparedArtifactNativeOperation,
    ) -> Result<u64, PreparedArtifactError> {
        if operation == PreparedArtifactNativeOperation::Reuse {
            return Ok(0);
        }
        self.request
            .generation_workspace_bytes()
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)
            .and_then(|bytes| {
                u64::try_from(bytes).map_err(|_| PreparedArtifactError::ArtifactTooLarge)
            })
    }

    fn metadata_bytes(&self) -> Result<u64, PreparedArtifactError> {
        // Current request, candidates, retained descriptors, outcomes and receipt
        // snapshots coexist. None owns payload; every manifest remains capped.
        (self.cells.len() as u64)
            .checked_mul(4 * MANIFEST_RESIDENT_BYTES)
            .and_then(|bytes| bytes.checked_add(self.request.resident_bytes() as u64))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)
    }

    /// Derive plan bounds from the typed request, without assuming an output crop.
    pub fn reservation(
        &self,
        store: &PreparedArtifactStore,
        _operation: PreparedArtifactNativeOperation,
    ) -> Result<PreparedArtifactReservation, PreparedArtifactError> {
        if self.cache_scope != store.scope {
            return Err(PreparedArtifactError::CachePolicyMismatch);
        }
        let size = self.request.input().grid.size as u64;
        let entry_bytes = size
            .checked_mul(size)
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(MANIFEST_RESERVATION_BYTES))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        // A working FFT maximum is a per-cell staging bound, not the size of
        // every selected crop. Settlement charges actual immutable entries and
        // protects the complete selected catalog from its own evictions.
        let required = entry_bytes;
        if required > store.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required,
                budget: store.budget.cache_bytes,
            });
        }
        let streaming_buffer_bytes = store
            .budget
            .streaming_buffer_bytes
            .min(STREAMING_BUFFER_CEILING as u64);
        if streaming_buffer_bytes < 8 {
            return Err(PreparedArtifactError::StreamingBufferTooSmall {
                required: 8,
                budget: streaming_buffer_bytes,
            });
        }
        let resident_buffer_bytes = streaming_buffer_bytes
            .checked_add(MANIFEST_RESIDENT_BYTES)
            .and_then(|b| {
                b.checked_add(inventory_resident_reservation(&store.cache, store.budget).ok()?)
            })
            .and_then(|b| b.checked_add(self.metadata_bytes().ok()?))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        Ok(PreparedArtifactReservation {
            persistent_cache_bytes: store.budget.cache_bytes,
            entry_bytes,
            // Bounded reconciliation of abandoned staging also runs on reuse.
            temporary_staging_bytes: store.budget.cache_bytes,
            source_read_bytes: 0,
            file_descriptors: 2,
            source_descriptor_bytes: 0,
            streaming_buffer_bytes,
            resident_buffer_bytes,
        })
    }

    fn realize(
        &self,
        index: usize,
        layout: PreparedArtifactNativeLayout,
    ) -> Result<PreparedArtifactDescriptor, PreparedArtifactError> {
        let (cell, _) = self
            .request
            .cell(index)
            .ok_or(PreparedArtifactError::InvalidCellKey)?;
        let plane = |layout: PreparedArtifactNativePlaneLayout| {
            if layout.shape[0] != layout.shape[1]
                || layout
                    .shape
                    .iter()
                    .any(|n| *n > cell.size as u64 || *n % 2 != 0)
            {
                return Err(PreparedArtifactError::InvalidLayout);
            }
            for axis in 0..2 {
                let radius = layout.support[axis]
                    .checked_mul(cell.oversampling as u64)
                    .and_then(|support| support.checked_add((cell.oversampling as u64).div_ceil(2)))
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                let center = layout.shape[axis] / 2;
                if center < radius
                    || center
                        .checked_add(radius)
                        .is_none_or(|last| last >= layout.shape[axis])
                {
                    return Err(PreparedArtifactError::InvalidLayout);
                }
            }
            let uv = PreparedArtifactUvAffine::new(
                [0.0; 2],
                layout.shape.map(|n| (n / 2) as f64),
                [
                    1.0 / (layout.shape[0] as f64 * cell.sky_increment_rad[0]),
                    1.0 / (layout.shape[1] as f64 * cell.sky_increment_rad[1]),
                ],
                [[1.0, 0.0], [0.0, 1.0]],
            )?;
            PreparedArtifactPlaneDescriptor::new(
                layout.shape,
                layout.support,
                cell.oversampling as u64,
                uv,
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::LastAxisContiguousLittleEndian,
            )
        };
        PreparedArtifactDescriptor::from_commitments(
            self.owner.clone(),
            PreparedArtifactKind::ConvolutionFunction,
            self.cells[index].scientific.clone(),
            self.cache_scope.clone(),
            vec![
                plane(layout.imaging)?.into_segment("imaging"),
                plane(layout.weight)?.into_segment("weight"),
            ],
            self.execution_problem,
        )
    }

    fn rebind(
        &self,
        index: usize,
        compatibility: PreparedArtifactCompatibility,
    ) -> Result<PreparedArtifactDescriptor, PreparedArtifactError> {
        let layout = |segment: &PreparedArtifactSegmentDescriptor| -> Result<PreparedArtifactNativePlaneLayout, PreparedArtifactError> {
            Ok(PreparedArtifactNativePlaneLayout {
                shape: segment.shape.as_slice().try_into().map_err(|_| PreparedArtifactError::InvalidLayout)?,
                support: segment.support.as_slice().try_into().map_err(|_| PreparedArtifactError::InvalidLayout)?,
            })
        };
        if compatibility.segments.len() != 2 {
            return Err(PreparedArtifactError::InvalidLayout);
        }
        let descriptor = self.realize(
            index,
            PreparedArtifactNativeLayout {
                imaging: layout(&compatibility.segments[0])?,
                weight: layout(&compatibility.segments[1])?,
            },
        )?;
        if descriptor.compatibility != compatibility {
            return Err(PreparedArtifactError::StaleArtifact);
        }
        Ok(descriptor)
    }
}

/// Native request facade over the ordinary serial catalog planning scaffold.
pub struct PreparedArtifactNativePlanFragment<'a> {
    request: &'a PreparedArtifactNativeRequest,
    store: &'a PreparedArtifactStore,
    operation: PreparedArtifactNativeOperation,
    producer: WorkNodeId,
    publication_commit: WorkNodeId,
    release_implementation: WorkImplementationId,
}

impl<'a> PreparedArtifactNativePlanFragment<'a> {
    /// Bind the exact request and explicit action into one catalog cache phase.
    pub fn new(
        request: &'a PreparedArtifactNativeRequest,
        store: &'a PreparedArtifactStore,
        operation: PreparedArtifactNativeOperation,
        producer: WorkNodeId,
        publication_commit: WorkNodeId,
        release_implementation: WorkImplementationId,
    ) -> Result<Self, PreparedArtifactPlanError> {
        request.reservation(store, operation)?;
        Ok(Self {
            request,
            store,
            operation,
            producer,
            publication_commit,
            release_implementation,
        })
    }

    /// Construct the same source-free receipt-closing base used by exact catalogs.
    pub fn standalone_base<R: ImplementationRegistry>(
        registry: &R,
        implementation: WorkImplementationId,
        request: &PreparedArtifactNativeRequest,
        store: &PreparedArtifactStore,
        stage_nanos: u64,
        confidence_parts_per_million: u32,
    ) -> Result<PhysicalWorkBinding, PreparedArtifactPlanError> {
        PreparedArtifactPlanFragment::standalone_base_for_demand(
            registry,
            implementation,
            request.demand_id(store),
            store,
            stage_nanos,
            confidence_parts_per_million,
        )
    }

    /// Exact native request cache-node identity.
    #[must_use]
    pub fn work_node_id(&self) -> WorkNodeId {
        self.request.node(self.operation)
    }

    /// Exact request adapter selected by the implementation registry.
    #[must_use]
    pub fn work_implementation_id(&self) -> WorkImplementationId {
        self.request.implementation(self.operation)
    }

    /// Compose shared cache claims, catalog allocation, generation data allocation,
    /// release node and receipt artifacts into the existing execution plan.
    pub fn compose(
        self,
        base: &PhysicalWorkBinding,
    ) -> Result<PhysicalWorkBinding, PreparedArtifactPlanError> {
        let reservation = self.request.reservation(self.store, self.operation)?;
        let workspace_bytes = self.request.workspace_bytes(self.operation)?;
        planning::compose_catalog(
            base,
            self.store,
            planning::CatalogPlanInputs {
                reservation,
                node_id: self.work_node_id(),
                implementation: self.work_implementation_id(),
                demand_id: self.request.demand_id(self.store),
                source_demands: BTreeMap::new(),
                source_producers: vec![],
                planned_artifacts: self.request.planned(self.operation),
                io_operations: (self.store.budget.entries as u64)
                    .saturating_mul(self.request.cells.len() as u64)
                    .saturating_add(1),
                workspace: (workspace_bytes > 0)
                    .then(|| (self.request.workspace_id(self.operation), workspace_bytes)),
            },
            self.producer,
            self.publication_commit,
            self.release_implementation,
        )
    }
}

/// Resolution of one expected cell; the order is exactly the model request order.
#[derive(Debug)]
pub enum PreparedArtifactNativeEntryOutcome {
    /// Exact immutable descriptor and validated handle for ordinary reader planning.
    Ready {
        /// Current-execution descriptor D, minted after checking the output contract.
        descriptor: Box<PreparedArtifactDescriptor>,
        /// Ordinary store handle, with no new payload access path.
        artifact: PreparedArtifact,
    },
    /// Explicit missing, stale or corrupt member; no generation was implicit.
    Rejected(PreparedArtifactRejection),
}

/// Closed native request result; a completed prefix is not a complete catalog.
#[derive(Debug)]
pub struct PreparedArtifactNativeCatalogOutcome {
    entries: Vec<PreparedArtifactNativeEntryOutcome>,
    generation_workspace_peak_bytes: u64,
}

impl PreparedArtifactNativeCatalogOutcome {
    /// All expected outcomes in deterministic request order.
    #[must_use]
    pub fn entries(&self) -> &[PreparedArtifactNativeEntryOutcome] {
        &self.entries
    }

    /// Peak charged numeric residency; includes the conservative opaque FFT
    /// allowance. This is not process RSS or a measurement of opaque allocations.
    #[must_use]
    pub const fn generation_workspace_peak_bytes(&self) -> u64 {
        self.generation_workspace_peak_bytes
    }

    /// Require complete coverage before exposing ordinary descriptor/handle pairs.
    pub fn into_complete(
        self,
    ) -> Result<Vec<(PreparedArtifactDescriptor, PreparedArtifact)>, PreparedArtifactError> {
        self.entries
            .into_iter()
            .map(|entry| match entry {
                PreparedArtifactNativeEntryOutcome::Ready {
                    descriptor,
                    artifact,
                } => Ok((*descriptor, artifact)),
                PreparedArtifactNativeEntryOutcome::Rejected(_) => {
                    Err(PreparedArtifactError::IncompleteArtifact)
                }
            })
            .collect()
    }
}

impl PreparedArtifactNativeRequest {
    fn validate_binding(
        &self,
        context: WorkExecutionContext<'_>,
        store: &PreparedArtifactStore,
        operation: PreparedArtifactNativeOperation,
        reservation: PreparedArtifactReservation,
    ) -> Result<(), PreparedArtifactError> {
        if self.owner.implementation_registry != context.implementation_registry_id() {
            return Err(PreparedArtifactError::ImplementationRegistryMismatch);
        }
        if self.execution_problem != context.compiled().problem_id()
            || self.cells.iter().any(|cell| {
                !cell
                    .scientific
                    .matches_context(PreparedArtifactKind::ConvolutionFunction, context)
            })
        {
            return Err(PreparedArtifactError::ScientificBindingMismatch);
        }
        if context.node().kind != WorkKind::Cache
            || context.node().id != self.node(operation)
            || context.node().implementation != self.implementation(operation)
        {
            return Err(PreparedArtifactError::UnplannedOperation);
        }
        let mut expected = self.planned(operation);
        expected.sort_unstable_by_key(PlannedArtifact::identity);
        let actual = context.planned_artifacts().collect::<Vec<_>>();
        if actual.len() != expected.len()
            || actual.iter().zip(&expected).any(|(a, e)| {
                a.identity() != e.identity()
                    || a.node() != e.node()
                    || a.role() != e.role()
                    || a.cache_identity() != e.cache_identity()
            })
        {
            return Err(PreparedArtifactError::UnplannedOperation);
        }
        validate_catalog_resources(
            context,
            store,
            self.demand_id(store),
            BTreeMap::new(),
            reservation,
        )?;
        let workspace_bytes = self.workspace_bytes(operation)?;
        let workspace = context
            .allocations()
            .iter()
            .find(|allocation| allocation.allocation() == &self.workspace_id(operation));
        if workspace_bytes > 0
            && workspace.is_none_or(|allocation| allocation.capacity_bytes() < workspace_bytes)
        {
            return Err(PreparedArtifactError::MissingReservation(
                "native generation workspace",
            ));
        }
        if workspace_bytes == 0 && workspace.is_some() {
            return Err(PreparedArtifactError::UnplannedOperation);
        }
        Ok(())
    }
}

impl PreparedArtifactStore {
    fn resolve_native_locked(
        &self,
        request: &PreparedArtifactNativeRequest,
        evidence: &mut ValidationEvidence,
    ) -> Result<Vec<Option<PreparedArtifactDescriptor>>, PreparedArtifactError> {
        let mut candidates: Vec<Option<PreparedArtifactDescriptor>> =
            vec![None; request.cells.len()];
        with_directory_paths_counted(
            &self.cache,
            evidence,
            root_inventory_limit(self.budget)?,
            |evidence, paths| {
                for path in paths {
                    let name =
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .ok_or_else(|| {
                                PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())
                            })?;
                    let identity = decode_digest(name)
                        .filter(|digest| name == encode_hex(digest))
                        .map(ArtifactIdentity::from_owner_digest)
                        .ok_or_else(|| {
                            PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())
                        })?;
                    let manifest = self.validate_manifest_at_path(
                        path.to_path_buf(),
                        identity,
                        None,
                        evidence,
                    )?;
                    let compatibility = manifest.descriptor;
                    if compatibility.owner != request.owner
                        || compatibility.cache_scope != request.cache_scope
                        || compatibility.kind != PreparedArtifactKind::ConvolutionFunction
                    {
                        continue;
                    }
                    let Some(index) = request
                        .cells
                        .iter()
                        .position(|cell| cell.scientific == compatibility.scientific)
                    else {
                        continue;
                    };
                    let descriptor = request.rebind(index, compatibility)?;
                    if let Some(previous) = &candidates[index] {
                        self.validate_entry_with_evidence(
                            previous.identity(),
                            Some(previous),
                            evidence,
                        )?;
                        self.validate_entry_with_evidence(
                            descriptor.identity(),
                            Some(&descriptor),
                            evidence,
                        )?;
                        return Err(PreparedArtifactError::PublicationConflict);
                    }
                    candidates[index] = Some(descriptor);
                }
                Ok(())
            },
        )?;
        Ok(candidates)
    }

    /// Execute an admitted requested catalog through the ordinary immutable writer.
    /// Metadata resolution and every payload revalidation occur under the private
    /// store lock. Generated layouts become exact descriptors only after admission;
    /// no native reader or cache schema exists. On failure, completed prefix receipt
    /// measurements survive, but no complete catalog is returned.
    pub fn prepare_native_catalog(
        &self,
        context: &WorkExecutionContext<'_>,
        request: &PreparedArtifactNativeRequest,
        operation: PreparedArtifactNativeOperation,
        mut generator: Option<&mut dyn PreparedArtifactNativeGenerator>,
    ) -> Result<(PreparedArtifactNativeCatalogOutcome, WorkMeasurements), PreparedArtifactError>
    {
        let reservation = request.reservation(self, operation)?;
        request.validate_binding(*context, self, operation, reservation)?;
        if (operation == PreparedArtifactNativeOperation::Reuse) != generator.is_none() {
            return Err(PreparedArtifactError::UnplannedOperation);
        }
        let metadata_bytes = request.metadata_bytes()?;
        let make_evidence = || {
            let mut evidence =
                ValidationEvidence::for_operation(self.budget, reservation.resident_buffer_bytes);
            evidence.acquire_resident(metadata_bytes);
            evidence
        };
        let mut aggregate = CatalogMeasurements::new(*context, request.cells.len());
        let mut initial = make_evidence();
        initial.ensure_resident_budget()?;
        let mut lock = self.lock(&mut initial).map_err(|error| {
            error.with_measurements(native_measurements(
                *context, self, request, operation, None, 0, &initial,
            ))
        })?;
        let setup = self
            .remove_orphan_staging(&mut initial)
            .and_then(|()| self.validate_budget_without_eviction(&mut initial))
            .and_then(|_| self.resolve_native_locked(request, &mut initial));
        aggregate.record(native_measurements(
            *context, self, request, operation, None, 0, &initial,
        ))?;
        let candidates = match setup {
            Ok(candidates) => candidates,
            Err(error) => {
                aggregate.record(native_measurements(
                    *context,
                    self,
                    request,
                    operation,
                    Some((0, None)),
                    0,
                    &ValidationEvidence {
                        evictions: std::mem::take(&mut initial.evictions),
                        ..ValidationEvidence::default()
                    },
                ))?;
                let mut terminal = make_evidence();
                let _ = lock.release(&mut terminal);
                aggregate.record(native_measurements(
                    *context, self, request, operation, None, 0, &terminal,
                ))?;
                return Err(error.with_measurements(aggregate.finish()));
            }
        };
        let initial_evictions = std::mem::take(&mut initial.evictions);
        drop(initial);
        let mut retained = candidates.iter().flatten().cloned().collect::<Vec<_>>();
        retained.sort_unstable_by_key(PreparedArtifactDescriptor::identity);
        let mut entries = Vec::with_capacity(request.cells.len());
        let mut peak = 0_u64;
        for (index, candidate) in candidates.into_iter().enumerate() {
            let mut evidence = make_evidence();
            if index == 0 {
                evidence.evictions.extend_from_slice(&initial_evictions);
            }
            evidence.observe_locks(1);
            let result = (|| {
                if context.stop_requested() {
                    return Err(PreparedArtifactError::Interrupted);
                }
                let prior = candidate.as_ref().map(|descriptor| {
                    self.validate_entry_with_evidence(
                        descriptor.identity(),
                        Some(descriptor),
                        &mut evidence,
                    )
                });
                if operation != PreparedArtifactNativeOperation::Regenerate {
                    match prior {
                        Some(Ok(validated)) => {
                            let descriptor = candidate.expect("candidate was validated");
                            let artifact = validated.into_handle(&descriptor);
                            return Ok((
                                PreparedArtifactNativeEntryOutcome::Ready {
                                    descriptor: Box::new(descriptor),
                                    artifact,
                                },
                                ArtifactDisposition::Reused,
                                0,
                            ));
                        }
                        Some(Err(error)) => {
                            let rejection = rejection_for(&error).ok_or(error)?;
                            return Ok((
                                PreparedArtifactNativeEntryOutcome::Rejected(rejection),
                                ArtifactDisposition::RejectedStale,
                                0,
                            ));
                        }
                        None if operation == PreparedArtifactNativeOperation::Reuse => {
                            return Ok((
                                PreparedArtifactNativeEntryOutcome::Rejected(
                                    PreparedArtifactRejection::Missing,
                                ),
                                ArtifactDisposition::RejectedStale,
                                0,
                            ));
                        }
                        None => {}
                    }
                }
                let generator = generator
                    .as_deref_mut()
                    .ok_or(PreparedArtifactError::UnplannedOperation)?;
                let workspace_limit = request.workspace_bytes(operation)?;
                let (cell, _) = request
                    .request
                    .cell(index)
                    .expect("expected request member");
                let generated = generator.generate_cell(index, cell, workspace_limit);
                let charged = generator.workspace_peak_bytes();
                peak = peak.max(charged);
                if (generated.is_ok() && charged == 0) || charged > workspace_limit {
                    return Err(PreparedArtifactError::ResidentBudgetExceeded {
                        required: charged.max(1),
                        budget: workspace_limit,
                    });
                }
                let layout = generated?;
                let descriptor = request.realize(index, layout)?;
                if let Some(previous) = &candidate
                    && previous.identity() != descriptor.identity()
                    && self
                        .validate_entry_with_evidence(
                            previous.identity(),
                            Some(previous),
                            &mut evidence,
                        )
                        .is_ok()
                {
                    return Err(PreparedArtifactError::PublicationConflict);
                }
                let exact_reservation =
                    self.reservation(&descriptor, PreparedArtifactOperation::Generate)?;
                let (validated, disposition, _) = self.publish_bytes_locked(
                    &descriptor,
                    ArtifactDisposition::Built,
                    PreparedArtifactMaterialization::Generate(generator),
                    exact_reservation,
                    transaction::PreparedArtifactPublicationScope {
                        retained: &retained,
                        regeneration_candidate: (operation
                            == PreparedArtifactNativeOperation::Regenerate)
                            .then_some(candidate.as_ref())
                            .flatten(),
                    },
                    &mut evidence,
                )?;
                let artifact = validated.into_handle(&descriptor);
                if retained
                    .binary_search_by_key(
                        &descriptor.identity(),
                        PreparedArtifactDescriptor::identity,
                    )
                    .is_err()
                {
                    retained.push(descriptor.clone());
                    retained.sort_unstable_by_key(PreparedArtifactDescriptor::identity);
                }
                Ok((
                    PreparedArtifactNativeEntryOutcome::Ready {
                        descriptor: Box::new(descriptor),
                        artifact,
                    },
                    disposition,
                    charged,
                ))
            })();
            match result {
                Ok((entry, disposition, charged)) => {
                    aggregate.record(native_measurements(
                        *context,
                        self,
                        request,
                        operation,
                        Some((index, Some((&entry, disposition)))),
                        charged,
                        &evidence,
                    ))?;
                    entries.push(entry);
                }
                Err(error) => {
                    let error = self
                        .rollback_materialized(&mut evidence)
                        .err()
                        .unwrap_or(error);
                    aggregate.record(native_measurements(
                        *context,
                        self,
                        request,
                        operation,
                        Some((index, None)),
                        peak,
                        &evidence,
                    ))?;
                    let mut terminal = make_evidence();
                    let _ = lock.release(&mut terminal);
                    aggregate.record(native_measurements(
                        *context, self, request, operation, None, 0, &terminal,
                    ))?;
                    return Err(error.with_measurements(aggregate.finish()));
                }
            }
        }
        let mut terminal = make_evidence();
        let unlocked = lock.release(&mut terminal);
        aggregate.record(native_measurements(
            *context, self, request, operation, None, 0, &terminal,
        ))?;
        if let Err(error) = unlocked {
            return Err(error.with_measurements(aggregate.finish()));
        }
        Ok((
            PreparedArtifactNativeCatalogOutcome {
                entries,
                generation_workspace_peak_bytes: peak,
            },
            aggregate.finish(),
        ))
    }
}

fn native_measurements(
    context: WorkExecutionContext<'_>,
    store: &PreparedArtifactStore,
    request: &PreparedArtifactNativeRequest,
    operation: PreparedArtifactNativeOperation,
    member: Option<(
        usize,
        Option<(&PreparedArtifactNativeEntryOutcome, ArtifactDisposition)>,
    )>,
    workspace_bytes: u64,
    evidence: &ValidationEvidence,
) -> WorkMeasurements {
    let demand_id = request.demand_id(store);
    let resources = context
        .resources()
        .iter()
        .map(|capability| {
            ResourceMeasurement::new(
                capability.resource().clone(),
                capability.lifetime().clone(),
                observed_resource_peak(
                    capability.resource(),
                    &demand_id,
                    operation.ordinary(),
                    evidence.cache_bytes_peak,
                    0,
                    evidence,
                ),
            )
        })
        .collect();
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    let mut artifacts = Vec::new();
    if let Some((index, outcome)) = member {
        let planned = request.cells[index].identity;
        if let Some((outcome, disposition)) = outcome {
            let (observed, bytes, path) = match outcome {
                PreparedArtifactNativeEntryOutcome::Ready {
                    descriptor,
                    artifact,
                } => {
                    // E commits requested P, exact D, verified C, and charged
                    // workspace evidence. Zero denotes a purely warm realization.
                    let mut digest = Sha256::new();
                    digest.update(b"native-prepared-realization/P-D-C-charged-workspace/v1\0");
                    digest.update(planned.as_bytes());
                    digest.update(descriptor.identity().as_bytes());
                    digest.update(artifact.integrity_identity.as_bytes());
                    digest.update(workspace_bytes.to_le_bytes());
                    (
                        ArtifactIdentity::from_owner_digest(digest.finalize().into()),
                        descriptor.payload_bytes().expect("validated descriptor"),
                        Some(RedactedPath::from_path(
                            store.entry_path(descriptor.identity()),
                        )),
                    )
                }
                PreparedArtifactNativeEntryOutcome::Rejected(rejection) => {
                    (rejection.evidence_identity(planned), 0, None)
                }
            };
            artifacts.push(ArtifactMeasurement::new_store_owned(
                planned,
                Some(observed),
                disposition,
                bytes,
                path,
            ));
        }
        let ledger = request.ledger(index, operation);
        // Failed numerical generation has no realized D. Its admitted charged
        // workspace still belongs in durable evidence beside any evictions.
        let mut ledger_observed = Sha256::new();
        ledger_observed.update(b"native-prepared-evictions-and-charged-workspace/v1\0");
        ledger_observed
            .update(derive_eviction_observed_identity(ledger, &evidence.evictions).as_bytes());
        ledger_observed.update(workspace_bytes.to_le_bytes());
        artifacts.push(ArtifactMeasurement::new_store_owned(
            ledger,
            Some(ArtifactIdentity::from_owner_digest(
                ledger_observed.finalize().into(),
            )),
            ArtifactDisposition::Loaded,
            evidence.evictions.iter().map(|(_, bytes)| bytes).sum(),
            None,
        ));
    }
    WorkMeasurements::new(resources, io, artifacts)
}
