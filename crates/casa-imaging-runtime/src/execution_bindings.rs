// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    path::Path,
};

use casa_imaging_model::{
    CompiledGeometry, CompiledGeometryId, CompiledProblem, CompiledProblemId, NumericsContract,
    NumericsContractId, ObservationProvenanceId, ObservationReadSet, ObservationSnapshotId,
    ObservationTransactionContract, ObservationWriteSet, ProblemInputIdentities, ProductGraphId,
    ProductRequirements, ReconstructionContract, ReferenceDataKind, RequiredCapability,
    ScientificContract, SelectedObservationCommitmentId, WeightingOperatorContract,
};
use casa_imaging_reconstruction::ExecutableModelProblem;
use sha2::{Digest, Sha256};

use crate::{
    AdaptationId, AdaptationTransition, AdmissionInfeasibilityCertificate, AllocationId,
    AlternativeId, ClaimLifetime, DemandAlternatives, ExecutionAttemptId, ExecutionError,
    ExecutionKnobs, ExecutionOutcome, ExecutionReceiptBinding, FenceKind, IoBufferKind,
    LeaseResource, PhysicalSlotId, PublicationLayoutLedger, ReceiptError, ReceiptFailureKind,
    ReceiptStatus, ResourceAuthority, ResourceError, ResourceIdentity, ResourceOverride,
    ResourcePolicy, WorkImplementationId, WorkKind, WorkNodeId,
    cost_model::PlannerCostModelProfileRecord,
    execution::{
        ExecutionDag, ExecutionScheduler, PublicationReservation, SchedulerAction,
        SchedulerTerminal, WorkResult, io_buffer_kind_supports_work_kind, validate_topology,
    },
    observation_transaction::{
        BoundObservationTransaction, ObservationTransactionPlanError, ObservationTransactionWork,
        bind_observation_transaction,
    },
    receipt::{ExecutionReceiptStore, ReceiptFailure, ReceiptRecorder},
};

const EXECUTION_PLAN_IDENTITY_DOMAIN: &[u8] = b"casa-rs-execution-plan";
const EXECUTION_PLAN_IDENTITY_VERSION: u32 = 12;
const RESOURCE_POLICY_IDENTITY_DOMAIN: &[u8] = b"casa-rs-resource-policy";
const RESOURCE_POLICY_IDENTITY_VERSION: u32 = 1;

macro_rules! digest_identity {
    ($name:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Construct an identity from an already computed SHA-256 digest.
            #[must_use]
            pub const fn from_sha256(digest: [u8; 32]) -> Self {
                Self(digest)
            }

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.0)?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.0)
            }
        }
    };
}

digest_identity!(
    ImplementationRegistryId,
    "Stable content identity of one immutable implementation-registry snapshot."
);
digest_identity!(
    PlannerCostModelProfileId,
    "Stable content identity of one reviewed planner cost-model profile."
);

impl PlannerCostModelProfileId {
    /// Mark this deployment-selected identity as the initial planner baseline.
    #[must_use]
    pub const fn bootstrap(self) -> crate::PlannerCostModelProfileBootstrap {
        crate::PlannerCostModelProfileBootstrap::new(self)
    }
}

digest_identity!(
    PhysicalWorkId,
    "Stable content identity of the physical work emitted by planning."
);
digest_identity!(
    ArtifactIdentity,
    "Stable content identity of one plan-visible input, cache, prepared artifact, or output."
);
digest_identity!(
    CacheIdentity,
    "Stable content identity of one cache namespace and compatibility contract."
);

impl ArtifactIdentity {
    pub(crate) const fn from_owner_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }
}

impl CacheIdentity {
    pub(crate) const fn from_owner_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }
}

/// Fixed-point confidence in a conservative planner prediction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PredictionConfidence(u32);

impl PredictionConfidence {
    /// Create confidence in parts per million, where one million is certainty.
    pub fn new(parts_per_million: u32) -> Result<Self, PhysicalWorkBindingError> {
        if parts_per_million > 1_000_000 {
            return Err(PhysicalWorkBindingError::InvalidConfidence(
                parts_per_million,
            ));
        }
        Ok(Self(parts_per_million))
    }

    /// Return confidence in parts per million.
    #[must_use]
    pub const fn parts_per_million(self) -> u32 {
        self.0
    }
}

/// One named contributor to conservative prediction uncertainty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredictionUncertainty {
    identity: String,
    predicted_nanos: u64,
}

impl PredictionUncertainty {
    /// Record a stable uncertainty identity and its time contribution.
    #[must_use]
    pub fn new(identity: impl Into<String>, predicted_nanos: u64) -> Self {
        Self {
            identity: identity.into(),
            predicted_nanos,
        }
    }

    /// Return the stable uncertainty identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Return the conservative time contribution.
    #[must_use]
    pub const fn predicted_nanos(&self) -> u64 {
        self.predicted_nanos
    }
}

/// Conservative predicted I/O volume for one typed category in one stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IoPrediction {
    kind: IoBufferKind,
    bytes: u64,
    operations: u64,
}

impl IoPrediction {
    /// Bind a predicted byte and operation count to one typed I/O category.
    #[must_use]
    pub const fn new(kind: IoBufferKind, bytes: u64, operations: u64) -> Self {
        Self {
            kind,
            bytes,
            operations,
        }
    }

    /// Return the typed I/O category.
    #[must_use]
    pub const fn kind(self) -> IoBufferKind {
        self.kind
    }

    /// Return predicted transferred bytes.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    /// Return predicted operations.
    #[must_use]
    pub const fn operations(self) -> u64 {
        self.operations
    }
}

/// Conservative predicted time for one exact plan node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagePrediction {
    node: WorkNodeId,
    elapsed_nanos: u64,
    io: Vec<IoPrediction>,
}

impl StagePrediction {
    /// Bind a predicted elapsed time to one plan node.
    #[must_use]
    pub const fn new(node: WorkNodeId, elapsed_nanos: u64) -> Self {
        Self {
            node,
            elapsed_nanos,
            io: Vec::new(),
        }
    }

    /// Attach the complete predicted I/O ledger for this stage.
    #[must_use]
    pub fn with_io(mut self, io: Vec<IoPrediction>) -> Self {
        self.io = io;
        self
    }

    /// Return the predicted node identity.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Return predicted elapsed nanoseconds.
    #[must_use]
    pub const fn elapsed_nanos(&self) -> u64 {
        self.elapsed_nanos
    }

    /// Return the complete canonical predicted I/O ledger.
    #[must_use]
    pub fn io(&self) -> &[IoPrediction] {
        &self.io
    }
}

/// Complete conservative time prediction bound into one Execution Plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanPrediction {
    elapsed_nanos: u64,
    confidence: PredictionConfidence,
    uncertainty: Vec<PredictionUncertainty>,
    stages: BTreeMap<WorkNodeId, StagePrediction>,
}

impl PlanPrediction {
    /// Validate and canonicalize a complete plan prediction.
    pub fn new(
        elapsed_nanos: u64,
        confidence: PredictionConfidence,
        mut uncertainty: Vec<PredictionUncertainty>,
        stages: Vec<StagePrediction>,
    ) -> Result<Self, PhysicalWorkBindingError> {
        uncertainty.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        if uncertainty.iter().any(|item| item.identity.is_empty()) {
            return Err(PhysicalWorkBindingError::EmptyUncertaintyIdentity);
        }
        if let Some(duplicate) = uncertainty
            .windows(2)
            .find(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(PhysicalWorkBindingError::DuplicateUncertainty(
                duplicate[0].identity.clone(),
            ));
        }
        let mut stage_map = BTreeMap::new();
        for mut stage in stages {
            let node = stage.node.clone();
            stage.io.sort_unstable_by_key(|prediction| prediction.kind);
            if let Some(duplicate) = stage
                .io
                .windows(2)
                .find(|pair| pair[0].kind == pair[1].kind)
            {
                return Err(PhysicalWorkBindingError::DuplicateIoPrediction {
                    node,
                    kind: duplicate[0].kind,
                });
            }
            if stage_map.insert(node.clone(), stage).is_some() {
                return Err(PhysicalWorkBindingError::DuplicateStagePrediction(node));
            }
        }
        Ok(Self {
            elapsed_nanos,
            confidence,
            uncertainty,
            stages: stage_map,
        })
    }

    /// Return conservative total predicted elapsed nanoseconds.
    #[must_use]
    pub const fn elapsed_nanos(&self) -> u64 {
        self.elapsed_nanos
    }

    /// Return fixed-point prediction confidence.
    #[must_use]
    pub const fn confidence(&self) -> PredictionConfidence {
        self.confidence
    }

    /// Return canonical dominant uncertainty terms.
    #[must_use]
    pub fn uncertainty(&self) -> &[PredictionUncertainty] {
        &self.uncertainty
    }

    /// Conservative total predicted time including every uncertainty term.
    ///
    /// Lexicographic planning compares candidates by this bound so predicted
    /// wall time can never understate committed uncertainty.
    #[must_use]
    pub fn conservative_nanos(&self) -> u64 {
        self.uncertainty
            .iter()
            .fold(self.elapsed_nanos, |total, term| {
                total.saturating_add(term.predicted_nanos)
            })
    }

    /// Return one prediction for every plan node.
    #[must_use]
    pub const fn stages(&self) -> &BTreeMap<WorkNodeId, StagePrediction> {
        &self.stages
    }
}

/// Plan-visible role of one immutable artifact identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactRole {
    /// Immutable execution input.
    Input,
    /// Prepared artifact generated or loaded by the plan.
    Prepared,
    /// Cache-resident artifact candidate.
    Cache,
    /// Scientific or provenance output awaiting publication.
    Output,
}

/// One artifact or cache identity declared by an exact plan node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedArtifact {
    identity: ArtifactIdentity,
    node: WorkNodeId,
    role: ArtifactRole,
    cache: Option<CacheIdentity>,
}

impl PlannedArtifact {
    /// Bind an artifact identity, role, and optional cache identity to one node.
    #[must_use]
    pub const fn new(
        identity: ArtifactIdentity,
        node: WorkNodeId,
        role: ArtifactRole,
        cache: Option<CacheIdentity>,
    ) -> Self {
        Self {
            identity,
            node,
            role,
            cache,
        }
    }

    /// Return the exact artifact identity.
    #[must_use]
    pub const fn identity(&self) -> ArtifactIdentity {
        self.identity
    }

    /// Return the exact owning node.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Return the plan-visible artifact role.
    #[must_use]
    pub const fn role(&self) -> ArtifactRole {
        self.role
    }

    /// Return the cache identity when the artifact participates in a cache.
    #[must_use]
    pub const fn cache_identity(&self) -> Option<CacheIdentity> {
        self.cache
    }
}

/// One-way local path identity safe to persist in an Execution Receipt.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RedactedPath([u8; 32]);

impl RedactedPath {
    /// Hash a local path without retaining its spelling.
    #[must_use]
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"casa-rs-receipt-path\0");
        hasher.update(path.as_ref().as_os_str().as_encoded_bytes());
        Self(hasher.finalize().into())
    }

    /// Return the irreversible path identity.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for RedactedPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RedactedPath(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Observed treatment of one plan-listed artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactDisposition {
    /// Built by the owning node.
    Built,
    /// Loaded from immutable local storage.
    Loaded,
    /// Reused after exact identity and compatibility validation.
    Reused,
    /// Examined but rejected because its identity or compatibility was stale.
    RejectedStale,
    /// Staged as an output and ready for the sole atomic publication operation.
    Staged,
    /// Privately prepared for one independently atomic product replacement.
    PublicationPrepared,
    /// Durably published as an output, as reported only by a completed receipt.
    Published,
    /// A member replacement failed before visibility; the prior member remains visible.
    PublicationFailed,
    /// A member replacement returned without proving whether visibility changed.
    PublicationUncertain,
}

/// Observed peak use of one exact resource claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceMeasurement {
    resource: LeaseResource,
    lifetime: ClaimLifetime,
    peak: u64,
}

impl ResourceMeasurement {
    /// Report the observed peak for one exact planned claim.
    #[must_use]
    pub const fn new(resource: LeaseResource, lifetime: ClaimLifetime, peak: u64) -> Self {
        Self {
            resource,
            lifetime,
            peak,
        }
    }

    /// Return the measured resource.
    #[must_use]
    pub const fn resource(&self) -> &LeaseResource {
        &self.resource
    }

    /// Return the exact planned lifetime of the measured claim.
    #[must_use]
    pub const fn lifetime(&self) -> &ClaimLifetime {
        &self.lifetime
    }

    /// Return observed peak concurrent use.
    #[must_use]
    pub const fn peak(&self) -> u64 {
        self.peak
    }
}

/// Observed bytes and operations for one planned I/O category.
///
/// A measurement covers only the scheduled node that produced the adapter's
/// evidence. Later consumer I/O must execute as a separate plan-listed node
/// with its own measurements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IoMeasurement {
    kind: IoBufferKind,
    bytes: u64,
    operations: u64,
}

impl IoMeasurement {
    /// Report actual bytes and operations for one planned category.
    #[must_use]
    pub const fn new(kind: IoBufferKind, bytes: u64, operations: u64) -> Self {
        Self {
            kind,
            bytes,
            operations,
        }
    }

    /// Return the typed I/O category.
    #[must_use]
    pub const fn kind(self) -> IoBufferKind {
        self.kind
    }

    /// Return actual transferred bytes.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    /// Return actual operations.
    #[must_use]
    pub const fn operations(self) -> u64 {
        self.operations
    }
}

/// Actual result for one plan-listed artifact, containing no raw local path.
///
/// For [`ArtifactDisposition::RejectedStale`], `observed` must contain typed
/// rejection evidence bound to the planned identity rather than a
/// materialized-content identity, and `bytes` is the number of bytes inspected
/// while rejecting the candidate. Missing, arbitrary, or differently bound
/// rejection identities fail the execution-evidence contract before receipt
/// checkpointing. For a successful prepared-artifact operation, the associated
/// I/O measurement covers the private-store lock, scans, metadata,
/// payload/manifest reads and writes, synchronization, validation, and eviction
/// work; consumer copies from the returned handle remain the consumer's
/// separately measured work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactMeasurement {
    planned: ArtifactIdentity,
    observed: Option<ArtifactIdentity>,
    disposition: ArtifactDisposition,
    bytes: u64,
    path: Option<RedactedPath>,
}

/// Rejection of an externally constructed artifact measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactMeasurementError {
    /// The disposition can only be emitted by its owning runtime module.
    StoreOwnedDisposition(ArtifactDisposition),
}

impl fmt::Display for ArtifactMeasurementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreOwnedDisposition(disposition) => write!(
                formatter,
                "artifact disposition {disposition:?} requires store-owned evidence"
            ),
        }
    }
}

impl Error for ArtifactMeasurementError {}

impl ArtifactMeasurement {
    /// Report one plan-listed artifact's observed identity and final disposition.
    ///
    /// [`ArtifactDisposition::RejectedStale`] is deliberately store-owned: a
    /// generic execution adapter cannot mint it even if it reproduces the
    /// deterministic identity stored in an execution receipt.
    pub const fn new(
        planned: ArtifactIdentity,
        observed: Option<ArtifactIdentity>,
        disposition: ArtifactDisposition,
        bytes: u64,
        path: Option<RedactedPath>,
    ) -> Result<Self, ArtifactMeasurementError> {
        if matches!(disposition, ArtifactDisposition::RejectedStale) {
            return Err(ArtifactMeasurementError::StoreOwnedDisposition(disposition));
        }
        Ok(Self {
            planned,
            observed,
            disposition,
            bytes,
            path,
        })
    }

    pub(crate) const fn new_store_owned(
        planned: ArtifactIdentity,
        observed: Option<ArtifactIdentity>,
        disposition: ArtifactDisposition,
        bytes: u64,
        path: Option<RedactedPath>,
    ) -> Self {
        Self {
            planned,
            observed,
            disposition,
            bytes,
            path,
        }
    }

    /// Return the plan-listed artifact identity.
    #[must_use]
    pub const fn planned_identity(self) -> ArtifactIdentity {
        self.planned
    }

    /// Return the observed content or typed rejection-evidence identity.
    #[must_use]
    pub const fn observed_identity(self) -> Option<ArtifactIdentity> {
        self.observed
    }

    /// Return the final artifact disposition.
    #[must_use]
    pub const fn disposition(self) -> ArtifactDisposition {
        self.disposition
    }

    /// Return the observed artifact size.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    /// Return the irreversible path identity, when location evidence exists.
    #[must_use]
    pub const fn path(self) -> Option<RedactedPath> {
        self.path
    }
}

/// Complete actual evidence returned by one successful plan-selected adapter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkMeasurements {
    resources: Vec<ResourceMeasurement>,
    io: Vec<IoMeasurement>,
    artifacts: Vec<ArtifactMeasurement>,
}

impl WorkMeasurements {
    /// Report every resource claim, planned I/O category, and listed artifact.
    #[must_use]
    pub const fn new(
        resources: Vec<ResourceMeasurement>,
        io: Vec<IoMeasurement>,
        artifacts: Vec<ArtifactMeasurement>,
    ) -> Self {
        Self {
            resources,
            io,
            artifacts,
        }
    }

    /// Return actual resource observations.
    #[must_use]
    pub fn resources(&self) -> &[ResourceMeasurement] {
        &self.resources
    }

    /// Return actual I/O observations.
    #[must_use]
    pub fn io(&self) -> &[IoMeasurement] {
        &self.io
    }

    /// Return actual artifact observations.
    #[must_use]
    pub fn artifacts(&self) -> &[ArtifactMeasurement] {
        &self.artifacts
    }
}

/// Invalid prediction or artifact declaration supplied with physical work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhysicalWorkBindingError {
    /// Confidence exceeded one million parts per million.
    InvalidConfidence(u32),
    /// An uncertainty identity was empty.
    EmptyUncertaintyIdentity,
    /// An uncertainty identity was repeated.
    DuplicateUncertainty(String),
    /// More than one prediction named the same node.
    DuplicateStagePrediction(WorkNodeId),
    /// More than one I/O prediction named the same category in one node.
    DuplicateIoPrediction {
        /// Exact predicted node.
        node: WorkNodeId,
        /// Repeated I/O category.
        kind: IoBufferKind,
    },
    /// A plan node had no prediction.
    MissingStagePrediction(WorkNodeId),
    /// A prediction named a node absent from the DAG.
    UnknownStagePrediction(WorkNodeId),
    /// An artifact named a node absent from the DAG.
    UnknownArtifactNode(WorkNodeId),
    /// The same artifact declaration was repeated.
    DuplicateArtifact(ArtifactIdentity, WorkNodeId),
    /// Typed I/O evidence was attached to a node with different semantics.
    IoKindMismatch {
        /// Exact predicted node.
        node: WorkNodeId,
        /// Predicted I/O category.
        kind: IoBufferKind,
        /// Declared work semantics.
        work_kind: WorkKind,
    },
    /// Typed I/O evidence lacked its exact lease claim and logical allocation.
    MissingIoContract {
        /// Exact predicted node.
        node: WorkNodeId,
        /// Predicted I/O category.
        kind: IoBufferKind,
    },
    /// A plan node declared a typed buffer contract without prediction evidence.
    MissingIoPrediction {
        /// Exact declaring node.
        node: WorkNodeId,
        /// Declared I/O-buffer category.
        kind: IoBufferKind,
    },
    /// An output artifact was not owned by a complete publication contract.
    MissingPublicationContract {
        /// Output artifact identity.
        artifact: ArtifactIdentity,
        /// Exact owning node.
        node: WorkNodeId,
    },
    /// The publication-layout ledger disagreed with artifacts, resources, or events.
    InvalidPublicationLayout {
        /// Stable diagnostic for the rejected physical declaration.
        reason: String,
    },
    /// Product publication layouts or completed seal evidence do not match the plan.
    InvalidProductPublication {
        /// Stable diagnostic for the rejected seal binding.
        reason: String,
    },
    /// The registry did not publish a contract for one selected implementation.
    MissingImplementationContract(WorkImplementationId),
    /// Registry-owned implementation contracts disagreed within one physical DAG.
    ConflictingImplementationContract {
        /// Stable diagnostic describing the conflict.
        reason: String,
    },
}

impl fmt::Display for PhysicalWorkBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfidence(value) => {
                write!(
                    formatter,
                    "prediction confidence {value} exceeds 1000000 ppm"
                )
            }
            Self::EmptyUncertaintyIdentity => {
                formatter.write_str("prediction uncertainty identity cannot be empty")
            }
            Self::DuplicateUncertainty(identity) => {
                write!(formatter, "duplicate prediction uncertainty {identity}")
            }
            Self::DuplicateStagePrediction(node) => {
                write!(formatter, "duplicate prediction for node {}", node.as_str())
            }
            Self::DuplicateIoPrediction { node, kind } => write!(
                formatter,
                "duplicate {} I/O prediction for node {}",
                io_buffer_name(*kind),
                node.as_str()
            ),
            Self::MissingStagePrediction(node) => {
                write!(formatter, "missing prediction for node {}", node.as_str())
            }
            Self::UnknownStagePrediction(node) => {
                write!(formatter, "prediction names unknown node {}", node.as_str())
            }
            Self::UnknownArtifactNode(node) => {
                write!(formatter, "artifact names unknown node {}", node.as_str())
            }
            Self::DuplicateArtifact(artifact, node) => write!(
                formatter,
                "artifact {artifact} is repeated for node {}",
                node.as_str()
            ),
            Self::IoKindMismatch {
                node,
                kind,
                work_kind,
            } => write!(
                formatter,
                "{} I/O evidence is incompatible with {work_kind:?} node {}",
                io_buffer_name(*kind),
                node.as_str()
            ),
            Self::MissingIoContract { node, kind } => write!(
                formatter,
                "{} I/O evidence for node {} lacks its exact typed buffer claim and allocation",
                io_buffer_name(*kind),
                node.as_str()
            ),
            Self::MissingIoPrediction { node, kind } => write!(
                formatter,
                "node {} declares a {} buffer contract without predicted evidence",
                node.as_str(),
                io_buffer_name(*kind)
            ),
            Self::MissingPublicationContract { artifact, node } => write!(
                formatter,
                "output artifact {artifact} at node {} lacks an explicit publication buffer, staging, storage, and fence contract",
                node.as_str()
            ),
            Self::InvalidPublicationLayout { reason } => {
                write!(formatter, "invalid publication layout: {reason}")
            }
            Self::InvalidProductPublication { reason } => {
                write!(formatter, "invalid sealed product publication: {reason}")
            }
            Self::MissingImplementationContract(implementation) => write!(
                formatter,
                "implementation registry did not publish a contract for {}",
                implementation.as_str()
            ),
            Self::ConflictingImplementationContract { reason } => {
                write!(formatter, "conflicting implementation contracts: {reason}")
            }
        }
    }
}

impl Error for PhysicalWorkBindingError {}

/// Adapter evidence did not exactly cover the work sealed into the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionEvidenceError {
    /// The adapter reported the same planned resource claim more than once.
    DuplicateResource {
        /// Exact node.
        node: WorkNodeId,
        /// Repeated resource.
        resource: LeaseResource,
        /// Repeated claim lifetime.
        lifetime: ClaimLifetime,
    },
    /// The adapter reported a resource/lifetime pair absent from the node.
    UnplannedResource {
        /// Exact node.
        node: WorkNodeId,
        /// Unlisted resource.
        resource: LeaseResource,
        /// Unlisted claim lifetime.
        lifetime: ClaimLifetime,
    },
    /// The adapter omitted one planned resource claim.
    MissingResource {
        /// Exact node.
        node: WorkNodeId,
        /// Omitted resource.
        resource: LeaseResource,
        /// Omitted claim lifetime.
        lifetime: ClaimLifetime,
    },
    /// Observed peak use exceeded the admitted hard claim.
    ResourcePeakExceeded {
        /// Exact node.
        node: WorkNodeId,
        /// Overused resource.
        resource: LeaseResource,
        /// Admitted concurrent amount.
        planned: u64,
        /// Observed concurrent peak.
        actual: u64,
    },
    /// The adapter repeated one I/O category.
    DuplicateIo {
        /// Exact node.
        node: WorkNodeId,
        /// Repeated category.
        kind: IoBufferKind,
    },
    /// The adapter reported an I/O category absent from the stage prediction.
    UnplannedIo {
        /// Exact node.
        node: WorkNodeId,
        /// Unlisted category.
        kind: IoBufferKind,
    },
    /// The adapter omitted one predicted I/O category.
    MissingIo {
        /// Exact node.
        node: WorkNodeId,
        /// Omitted category.
        kind: IoBufferKind,
    },
    /// The adapter repeated one plan-listed artifact.
    DuplicateArtifact {
        /// Exact node.
        node: WorkNodeId,
        /// Repeated artifact.
        artifact: ArtifactIdentity,
    },
    /// The adapter reported an artifact not owned by the node.
    UnplannedArtifact {
        /// Exact node.
        node: WorkNodeId,
        /// Unlisted artifact.
        artifact: ArtifactIdentity,
    },
    /// The adapter omitted one artifact owned by the node.
    MissingArtifact {
        /// Exact node.
        node: WorkNodeId,
        /// Omitted artifact.
        artifact: ArtifactIdentity,
    },
    /// An artifact disposition contradicted its plan-visible role.
    ArtifactDispositionMismatch {
        /// Exact node.
        node: WorkNodeId,
        /// Plan-listed artifact.
        artifact: ArtifactIdentity,
        /// Plan-visible role.
        role: ArtifactRole,
        /// Observed disposition.
        disposition: ArtifactDisposition,
    },
    /// A cache node rejected its selected prepared artifact instead of
    /// returning a materialized artifact or using an explicitly planned
    /// alternate node.
    RejectedArtifact {
        /// Exact node.
        node: WorkNodeId,
        /// Plan-listed artifact that was rejected.
        artifact: ArtifactIdentity,
    },
}

impl ExecutionEvidenceError {
    fn node(&self) -> &WorkNodeId {
        match self {
            Self::DuplicateResource { node, .. }
            | Self::UnplannedResource { node, .. }
            | Self::MissingResource { node, .. }
            | Self::ResourcePeakExceeded { node, .. }
            | Self::DuplicateIo { node, .. }
            | Self::UnplannedIo { node, .. }
            | Self::MissingIo { node, .. }
            | Self::DuplicateArtifact { node, .. }
            | Self::UnplannedArtifact { node, .. }
            | Self::MissingArtifact { node, .. }
            | Self::ArtifactDispositionMismatch { node, .. }
            | Self::RejectedArtifact { node, .. } => node,
        }
    }
}

impl fmt::Display for ExecutionEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateResource { node, resource, .. } => write!(
                formatter,
                "node {} repeated resource measurement {}",
                node.as_str(),
                lease_resource_name(resource)
            ),
            Self::UnplannedResource { node, resource, .. } => write!(
                formatter,
                "node {} reported unplanned resource {}",
                node.as_str(),
                lease_resource_name(resource)
            ),
            Self::MissingResource { node, resource, .. } => write!(
                formatter,
                "node {} omitted resource measurement {}",
                node.as_str(),
                lease_resource_name(resource)
            ),
            Self::ResourcePeakExceeded {
                node,
                resource,
                planned,
                actual,
            } => write!(
                formatter,
                "node {} measured resource {} peak {actual} above admitted {planned}",
                node.as_str(),
                lease_resource_name(resource)
            ),
            Self::DuplicateIo { node, kind } => write!(
                formatter,
                "node {} repeated {} I/O measurement",
                node.as_str(),
                io_buffer_name(*kind)
            ),
            Self::UnplannedIo { node, kind } => write!(
                formatter,
                "node {} reported unplanned {} I/O",
                node.as_str(),
                io_buffer_name(*kind)
            ),
            Self::MissingIo { node, kind } => write!(
                formatter,
                "node {} omitted {} I/O measurement",
                node.as_str(),
                io_buffer_name(*kind)
            ),
            Self::DuplicateArtifact { node, artifact } => write!(
                formatter,
                "node {} repeated artifact {artifact}",
                node.as_str()
            ),
            Self::UnplannedArtifact { node, artifact } => write!(
                formatter,
                "node {} reported unplanned artifact {artifact}",
                node.as_str()
            ),
            Self::MissingArtifact { node, artifact } => write!(
                formatter,
                "node {} omitted artifact {artifact}",
                node.as_str()
            ),
            Self::ArtifactDispositionMismatch {
                node,
                artifact,
                role,
                disposition,
            } => write!(
                formatter,
                "node {} reported {disposition:?} for {role:?} artifact {artifact}",
                node.as_str()
            ),
            Self::RejectedArtifact { node, artifact } => write!(
                formatter,
                "cache node {} rejected selected prepared artifact {artifact}",
                node.as_str()
            ),
        }
    }
}

impl Error for ExecutionEvidenceError {}

/// Complete physical work emitted by the sole planning seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalWorkBinding {
    implementation_contract: ImplementationContractCommitment,
    execution_dag: ExecutionDag,
    prediction: PlanPrediction,
    artifacts: Vec<PlannedArtifact>,
    observation_transaction: ObservationTransactionWork,
    publication_layouts: PublicationLayoutLedger,
    product_publication: ProductPublicationAuthority,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProductPublicationAuthority {
    None,
    Planned(crate::ProductPublicationPlan),
}

/// Registry-owned science, numerics, and capability metadata for one
/// implementation identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImplementationContractMetadata {
    problem: CompiledProblemId,
    numerics: NumericsContractId,
    required_capabilities: BTreeSet<RequiredCapability>,
}

impl ImplementationContractMetadata {
    /// Describe the contract published by an implementation registry.
    #[must_use]
    pub fn new(
        problem: CompiledProblemId,
        numerics: NumericsContractId,
        required_capabilities: BTreeSet<RequiredCapability>,
    ) -> Self {
        Self {
            problem,
            numerics,
            required_capabilities,
        }
    }

    /// Return the exact compiled problem whose science is implemented.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the exact numerical contract implemented by the candidate.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the compiler-derived capability set implemented by the
    /// candidate.
    #[must_use]
    pub const fn required_capabilities(&self) -> &BTreeSet<RequiredCapability> {
        &self.required_capabilities
    }
}

/// Opaque registry-bound implementation declarations used to bind physical
/// work. Callers can obtain a catalog only by asking an implementation
/// registry for its metadata; there is no public constructor for arbitrary
/// candidate declarations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImplementationContractCatalog {
    registry: ImplementationRegistryId,
    declarations: BTreeMap<WorkImplementationId, ImplementationContractDeclaration>,
}

impl ImplementationContractCatalog {
    /// Capture the immutable contract snapshot for the implementations in a
    /// physical DAG from one exact registry snapshot.
    pub fn from_registry<R, I>(
        registry: &R,
        implementations: I,
    ) -> Result<Self, PhysicalWorkBindingError>
    where
        R: ImplementationRegistry,
        I: IntoIterator<Item = WorkImplementationId>,
    {
        let mut declarations = BTreeMap::new();
        for implementation in implementations {
            if declarations.contains_key(&implementation) {
                continue;
            }
            let metadata = registry
                .implementation_contract(&implementation)
                .ok_or_else(|| {
                    PhysicalWorkBindingError::MissingImplementationContract(implementation.clone())
                })?;
            declarations.insert(
                implementation,
                ImplementationContractDeclaration {
                    problem: metadata.problem,
                    numerics: metadata.numerics,
                    required_capabilities: metadata.required_capabilities,
                },
            );
        }
        Ok(Self {
            registry: registry.registry_id(),
            declarations,
        })
    }

    /// Return the exact registry snapshot that published this catalog.
    #[must_use]
    pub const fn registry_id(&self) -> ImplementationRegistryId {
        self.registry
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ImplementationContractDeclaration {
    problem: CompiledProblemId,
    numerics: NumericsContractId,
    required_capabilities: BTreeSet<RequiredCapability>,
}

/// Exact compiled science, numerics, and capability contract committed to one
/// physical candidate after its execution DAG is sealed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImplementationContractCommitment {
    registry: ImplementationRegistryId,
    problem: CompiledProblemId,
    numerics: NumericsContractId,
    required_capabilities: BTreeSet<RequiredCapability>,
    declarations: BTreeMap<WorkImplementationId, ImplementationContractDeclaration>,
    implementation_ids: BTreeMap<WorkNodeId, WorkImplementationId>,
}

impl ImplementationContractCommitment {
    fn from_catalog(
        catalog: &ImplementationContractCatalog,
        execution_dag: &ExecutionDag,
    ) -> Result<Self, PhysicalWorkBindingError> {
        let mut contract = None;
        for work in execution_dag.nodes().values() {
            let declaration = catalog
                .declarations
                .get(&work.implementation)
                .ok_or_else(|| {
                    PhysicalWorkBindingError::MissingImplementationContract(
                        work.implementation.clone(),
                    )
                })?;
            if let Some((problem, numerics, capabilities)) = &contract {
                if *problem != declaration.problem
                    || *numerics != declaration.numerics
                    || capabilities != &declaration.required_capabilities
                {
                    return Err(
                        PhysicalWorkBindingError::ConflictingImplementationContract {
                            reason: format!(
                                "implementation {} disagrees with the previously selected contract",
                                work.implementation.as_str()
                            ),
                        },
                    );
                }
            } else {
                contract = Some((
                    declaration.problem,
                    declaration.numerics,
                    declaration.required_capabilities.clone(),
                ));
            }
        }
        let Some((problem, numerics, required_capabilities)) = contract else {
            return Err(
                PhysicalWorkBindingError::ConflictingImplementationContract {
                    reason: "physical execution DAG contains no implementation nodes".to_string(),
                },
            );
        };
        Ok(Self {
            registry: catalog.registry,
            problem,
            numerics,
            required_capabilities,
            declarations: catalog.declarations.clone(),
            implementation_ids: implementation_ids(execution_dag),
        })
    }

    pub(crate) fn for_execution_dag(
        &self,
        execution_dag: &ExecutionDag,
    ) -> Result<Self, PhysicalWorkBindingError> {
        let declarations = self.declarations.clone();
        for work in execution_dag.nodes().values() {
            let Some(declaration) = declarations.get(&work.implementation) else {
                if work.kind == WorkKind::Cache {
                    // The prepared cache declaration is supplied by the
                    // authoritative registry during plan sealing.
                    continue;
                }
                return Err(PhysicalWorkBindingError::MissingImplementationContract(
                    work.implementation.clone(),
                ));
            };
            if declaration.problem != self.problem
                || declaration.numerics != self.numerics
                || declaration.required_capabilities != self.required_capabilities
            {
                return Err(
                    PhysicalWorkBindingError::ConflictingImplementationContract {
                        reason: format!(
                            "implementation {} disagrees with the sealed physical contract",
                            work.implementation.as_str()
                        ),
                    },
                );
            }
        }
        Ok(Self {
            registry: self.registry,
            problem: self.problem,
            numerics: self.numerics,
            required_capabilities: self.required_capabilities.clone(),
            declarations,
            implementation_ids: implementation_ids(execution_dag),
        })
    }

    fn bind_registry<R: ImplementationRegistry>(
        &self,
        registry: &R,
        execution_dag: &ExecutionDag,
    ) -> Result<Self, PhysicalWorkBindingError> {
        if self.registry != registry.registry_id()
            || self.implementation_ids != implementation_ids(execution_dag)
        {
            return Err(
                PhysicalWorkBindingError::ConflictingImplementationContract {
                    reason: "registry or final execution DAG differs from the sealed contract"
                        .to_string(),
                },
            );
        }
        let mut declarations = self.declarations.clone();
        for work in execution_dag.nodes().values() {
            let implementation = registry.resolve(&work.implementation).ok_or_else(|| {
                PhysicalWorkBindingError::MissingImplementationContract(work.implementation.clone())
            })?;
            if implementation.implementation_id() != &work.implementation {
                return Err(
                    PhysicalWorkBindingError::ConflictingImplementationContract {
                        reason: format!(
                            "registry resolved {} for requested implementation {}",
                            implementation.implementation_id().as_str(),
                            work.implementation.as_str()
                        ),
                    },
                );
            }
            let observed = registry
                .implementation_contract(&work.implementation)
                .ok_or_else(|| {
                    PhysicalWorkBindingError::MissingImplementationContract(
                        work.implementation.clone(),
                    )
                })?;
            let declaration = ImplementationContractDeclaration {
                problem: observed.problem,
                numerics: observed.numerics,
                required_capabilities: observed.required_capabilities,
            };
            if declaration.problem != self.problem
                || declaration.numerics != self.numerics
                || declaration.required_capabilities != self.required_capabilities
            {
                return Err(
                    PhysicalWorkBindingError::ConflictingImplementationContract {
                        reason: format!(
                            "registry contract for implementation {} disagrees with the sealed science contract",
                            work.implementation.as_str()
                        ),
                    },
                );
            }
            declarations.insert(work.implementation.clone(), declaration);
        }
        Ok(Self {
            registry: self.registry,
            problem: self.problem,
            numerics: self.numerics,
            required_capabilities: self.required_capabilities.clone(),
            declarations,
            implementation_ids: self.implementation_ids.clone(),
        })
    }

    fn validate_registry<R: ImplementationRegistry>(
        &self,
        registry: &R,
        execution_dag: &ExecutionDag,
    ) -> Result<(), PhysicalWorkBindingError> {
        if self.registry != registry.registry_id()
            || self.implementation_ids != implementation_ids(execution_dag)
        {
            return Err(
                PhysicalWorkBindingError::ConflictingImplementationContract {
                    reason: "run registry or final execution DAG differs from the sealed contract"
                        .to_string(),
                },
            );
        }
        for work in execution_dag.nodes().values() {
            let Some(implementation) = registry.resolve(&work.implementation) else {
                return Err(PhysicalWorkBindingError::MissingImplementationContract(
                    work.implementation.clone(),
                ));
            };
            if implementation.implementation_id() != &work.implementation {
                return Err(
                    PhysicalWorkBindingError::ConflictingImplementationContract {
                        reason: format!(
                            "run registry resolved {} for planned implementation {}",
                            implementation.implementation_id().as_str(),
                            work.implementation.as_str()
                        ),
                    },
                );
            }
            let planned = self.declarations.get(&work.implementation).ok_or_else(|| {
                PhysicalWorkBindingError::MissingImplementationContract(work.implementation.clone())
            })?;
            let Some(observed) = registry.implementation_contract(&work.implementation) else {
                return Err(PhysicalWorkBindingError::MissingImplementationContract(
                    work.implementation.clone(),
                ));
            };
            if observed.problem != planned.problem
                || observed.numerics != planned.numerics
                || observed.required_capabilities != planned.required_capabilities
            {
                return Err(
                    PhysicalWorkBindingError::ConflictingImplementationContract {
                        reason: format!(
                            "run registry changed the contract for implementation {}",
                            work.implementation.as_str()
                        ),
                    },
                );
            }
        }
        Ok(())
    }

    /// Return the exact compiled problem whose science is implemented.
    #[must_use]
    pub(crate) const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the registry snapshot that published this contract.
    #[must_use]
    pub(crate) const fn registry_id(&self) -> ImplementationRegistryId {
        self.registry
    }

    /// Return the exact numerical contract implemented by the candidate.
    #[must_use]
    pub(crate) const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the compiler-derived capability set implemented by the candidate.
    #[must_use]
    pub(crate) const fn required_capabilities(&self) -> &BTreeSet<RequiredCapability> {
        &self.required_capabilities
    }

    /// Return the registry-selected implementation identity for each node.
    #[must_use]
    pub(crate) const fn implementation_ids(&self) -> &BTreeMap<WorkNodeId, WorkImplementationId> {
        &self.implementation_ids
    }
}

fn implementation_ids(execution_dag: &ExecutionDag) -> BTreeMap<WorkNodeId, WorkImplementationId> {
    execution_dag
        .nodes()
        .iter()
        .map(|(node, work)| (node.clone(), work.implementation.clone()))
        .collect()
}

impl PhysicalWorkBinding {
    /// Bind a complete immutable physical work DAG, prediction, artifacts,
    /// and transaction to an explicit registry-owned implementation contract.
    pub fn new_reconstruction(
        catalog: ImplementationContractCatalog,
        execution_dag: ExecutionDag,
        prediction: PlanPrediction,
        artifacts: Vec<PlannedArtifact>,
        observation_transaction: ObservationTransactionWork,
        publication_layouts: PublicationLayoutLedger,
    ) -> Result<Self, PhysicalWorkBindingError> {
        if observation_transaction.publication_scope()
            != crate::ObservationTransactionPublicationScope::ReconstructionOnly
        {
            return invalid_product_publication(
                "reconstruction physical work requires ReconstructionOnly transaction scope",
            );
        }
        Self::with_implementation_contract(
            ImplementationContractCommitment::from_catalog(&catalog, &execution_dag)?,
            execution_dag,
            prediction,
            artifacts,
            observation_transaction,
            publication_layouts,
            ProductPublicationAuthority::None,
        )
    }

    /// Bind native product publication through an exact planned generation.
    ///
    /// This is the production construction path for physical work containing
    /// [`crate::PublicationParticipant::Product`] layouts. It validates the
    /// ordinary DAG, resource, artifact, and layout contracts, then requires
    /// the product subset to match the planned-generation publication plan exactly
    /// by compiled problem, Product Graph, member, and artifact identity.
    pub fn new_with_product_publication(
        catalog: ImplementationContractCatalog,
        execution_dag: ExecutionDag,
        prediction: PlanPrediction,
        artifacts: Vec<PlannedArtifact>,
        observation_transaction: ObservationTransactionWork,
        publication_layouts: PublicationLayoutLedger,
        product_publication: &crate::ProductPublicationPlan,
    ) -> Result<Self, PhysicalWorkBindingError> {
        if observation_transaction.publication_scope()
            != crate::ObservationTransactionPublicationScope::ProductPublication
        {
            return invalid_product_publication(
                "native product publication requires ProductPublication transaction scope",
            );
        }
        let binding = Self::with_implementation_contract(
            ImplementationContractCommitment::from_catalog(&catalog, &execution_dag)?,
            execution_dag,
            prediction,
            artifacts,
            observation_transaction,
            publication_layouts,
            ProductPublicationAuthority::Planned(product_publication.clone()),
        )?;
        binding.validate_product_publication(product_publication)?;
        Ok(binding)
    }

    fn validate_product_publication(
        &self,
        product_publication: &crate::ProductPublicationPlan,
    ) -> Result<(), PhysicalWorkBindingError> {
        if product_publication.problem_id() != self.implementation_contract.problem_id() {
            return invalid_product_publication(
                "product generation and implementation contract name different compiled problems",
            );
        }
        let product_layouts = self
            .publication_layouts
            .entries()
            .iter()
            .filter_map(|layout| match layout.participant() {
                crate::PublicationParticipant::Product { graph_id, node_id } => {
                    Some((graph_id, node_id, layout.artifact()))
                }
                crate::PublicationParticipant::ModelData(_) => None,
            })
            .collect::<Vec<_>>();
        if product_layouts.len() != product_publication.entries().len() {
            return invalid_product_publication(
                "product layouts do not exactly cover the planned member set",
            );
        }
        for entry in product_publication.entries() {
            if !product_layouts.iter().any(|(graph_id, node_id, artifact)| {
                *graph_id == product_publication.graph_id()
                    && *node_id == entry.node()
                    && *artifact == entry.artifact()
            }) {
                return invalid_product_publication(format!(
                    "planned product node {} has no exact publication layout",
                    entry.node().ordinal()
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn with_implementation_contract(
        implementation_contract: ImplementationContractCommitment,
        execution_dag: ExecutionDag,
        prediction: PlanPrediction,
        mut artifacts: Vec<PlannedArtifact>,
        observation_transaction: ObservationTransactionWork,
        publication_layouts: PublicationLayoutLedger,
        product_publication: ProductPublicationAuthority,
    ) -> Result<Self, PhysicalWorkBindingError> {
        for node in execution_dag.nodes().keys() {
            if !prediction.stages.contains_key(node) {
                return Err(PhysicalWorkBindingError::MissingStagePrediction(
                    node.clone(),
                ));
            }
        }
        for node in prediction.stages.keys() {
            if !execution_dag.nodes().contains_key(node) {
                return Err(PhysicalWorkBindingError::UnknownStagePrediction(
                    node.clone(),
                ));
            }
        }
        validate_io_contracts(&execution_dag, &prediction)?;
        artifacts.sort_unstable_by(|left, right| {
            (left.identity, &left.node).cmp(&(right.identity, &right.node))
        });
        for artifact in &artifacts {
            let Some(node) = execution_dag.nodes().get(&artifact.node) else {
                return Err(PhysicalWorkBindingError::UnknownArtifactNode(
                    artifact.node.clone(),
                ));
            };
            if artifact.role == ArtifactRole::Output
                && !has_publication_contract(node, &prediction.stages[&artifact.node])
            {
                return Err(PhysicalWorkBindingError::MissingPublicationContract {
                    artifact: artifact.identity,
                    node: artifact.node.clone(),
                });
            }
        }
        if let Some(pair) = artifacts
            .windows(2)
            .find(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(PhysicalWorkBindingError::DuplicateArtifact(
                pair[0].identity,
                pair[0].node.clone(),
            ));
        }
        validate_publication_layouts(
            &execution_dag,
            &prediction,
            &artifacts,
            &publication_layouts,
        )?;
        let has_product_layout = publication_layouts.entries().iter().any(|layout| {
            matches!(
                layout.participant(),
                crate::PublicationParticipant::Product { .. }
            )
        });
        if has_product_layout && matches!(product_publication, ProductPublicationAuthority::None) {
            return invalid_product_publication(
                "Product layouts require an exact planned native generation",
            );
        }
        Ok(Self {
            implementation_contract,
            execution_dag,
            prediction,
            artifacts,
            observation_transaction,
            publication_layouts,
            product_publication,
        })
    }

    /// Return the exact science, numerics, and capability commitment carried
    /// by every selected implementation in this candidate.
    #[must_use]
    pub(crate) const fn implementation_contract(&self) -> &ImplementationContractCommitment {
        &self.implementation_contract
    }

    pub(crate) fn product_publication_authority(&self) -> ProductPublicationAuthority {
        self.product_publication.clone()
    }

    fn bind_registry<R: ImplementationRegistry>(
        self,
        registry: &R,
    ) -> Result<Self, PhysicalWorkBindingError> {
        let contract = self
            .implementation_contract
            .bind_registry(registry, &self.execution_dag)?;
        Self::with_implementation_contract(
            contract,
            self.execution_dag,
            self.prediction,
            self.artifacts,
            self.observation_transaction,
            self.publication_layouts,
            self.product_publication,
        )
    }

    /// Return the stable physical-work identity.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.execution_dag.physical_work_id()
    }

    /// Return the complete immutable physical work DAG.
    #[must_use]
    pub const fn execution_dag(&self) -> &ExecutionDag {
        &self.execution_dag
    }

    /// Return the complete cost-model prediction.
    #[must_use]
    pub const fn prediction(&self) -> &PlanPrediction {
        &self.prediction
    }

    /// Return every plan-visible artifact and cache identity.
    #[must_use]
    pub fn artifacts(&self) -> &[PlannedArtifact] {
        &self.artifacts
    }
}

fn validate_publication_layouts(
    dag: &ExecutionDag,
    prediction: &PlanPrediction,
    artifacts: &[PlannedArtifact],
    layouts: &PublicationLayoutLedger,
) -> Result<(), PhysicalWorkBindingError> {
    let outputs = artifacts
        .iter()
        .filter(|artifact| artifact.role == ArtifactRole::Output)
        .map(|artifact| artifact.identity)
        .collect::<BTreeSet<_>>();
    let layout_artifacts = layouts
        .entries()
        .iter()
        .map(crate::PublicationPhysicalLayout::artifact)
        .collect::<BTreeSet<_>>();
    if outputs != layout_artifacts {
        return invalid_publication_layout("layout artifacts do not exactly match planned outputs");
    }

    let mut staged_by_producer = BTreeMap::<WorkNodeId, u64>::new();
    let mut io_by_node = BTreeMap::<(WorkNodeId, IoBufferKind), u64>::new();
    let mut bytes_by_allocation = BTreeMap::<AllocationId, u64>::new();
    for layout in layouts.entries() {
        let staging = layout.staging();
        let producer = dag.nodes().get(staging.producer()).ok_or_else(|| {
            PhysicalWorkBindingError::InvalidPublicationLayout {
                reason: format!(
                    "artifact {} names absent producer {}",
                    layout.artifact(),
                    staging.producer().as_str()
                ),
            }
        })?;
        if !is_declared_completion(dag, staging.terminal(), Some(&producer.id)) {
            return invalid_publication_layout(format!(
                "artifact {} terminal is not a declared completion of {}",
                layout.artifact(),
                producer.id.as_str()
            ));
        }
        let allocation = dag
            .logical_allocations()
            .get(staging.writer_allocation())
            .ok_or_else(|| PhysicalWorkBindingError::InvalidPublicationLayout {
                reason: format!(
                    "artifact {} names absent writer allocation {}",
                    layout.artifact(),
                    staging.writer_allocation().as_str()
                ),
            })?;
        if allocation.purpose != crate::AllocationPurpose::IoBuffer(staging.writer_buffer_kind())
            || !producer
                .allocations
                .iter()
                .any(|usage| usage.allocation == allocation.id)
            || !allocation
                .lifetime
                .release_after
                .contains(staging.terminal())
        {
            return invalid_publication_layout(format!(
                "artifact {} writer allocation is not producer-owned through its terminal event",
                layout.artifact()
            ));
        }
        *staged_by_producer.entry(producer.id.clone()).or_default() = staged_by_producer
            .get(&producer.id)
            .copied()
            .unwrap_or(0)
            .saturating_add(layout.resource_bounds().staged_storage_bytes());
        *io_by_node
            .entry((producer.id.clone(), staging.writer_buffer_kind()))
            .or_default() = io_by_node
            .get(&(producer.id.clone(), staging.writer_buffer_kind()))
            .copied()
            .unwrap_or(0)
            .saturating_add(layout.resource_bounds().writer_buffer_bytes());
        *bytes_by_allocation
            .entry(allocation.id.clone())
            .or_default() = bytes_by_allocation
            .get(&allocation.id)
            .copied()
            .unwrap_or(0)
            .saturating_add(layout.resource_bounds().writer_buffer_bytes());

        if let Some(mapped) = staging.mapped_page_cache() {
            let mapped_producer = dag.nodes().get(mapped.producer()).ok_or_else(|| {
                PhysicalWorkBindingError::InvalidPublicationLayout {
                    reason: format!(
                        "artifact {} names absent mapped producer {}",
                        layout.artifact(),
                        mapped.producer().as_str()
                    ),
                }
            })?;
            if !is_declared_completion(dag, mapped.terminal(), None) {
                return invalid_publication_layout(format!(
                    "artifact {} mapped terminal is not a declared completion event",
                    layout.artifact()
                ));
            }
            let release_id = dependency_node(mapped.terminal());
            let release = &dag.nodes()[release_id];
            let mapped_allocation = dag
                .logical_allocations()
                .get(mapped.allocation())
                .ok_or_else(|| PhysicalWorkBindingError::InvalidPublicationLayout {
                    reason: format!(
                        "artifact {} names absent mapped allocation {}",
                        layout.artifact(),
                        mapped.allocation().as_str()
                    ),
                })?;
            if mapped_allocation.purpose
                != crate::AllocationPurpose::IoBuffer(IoBufferKind::MappedPageCache)
                || mapped_allocation.lifetime.acquire_at != mapped_producer.id
                || !mapped_producer
                    .allocations
                    .iter()
                    .any(|usage| usage.allocation == mapped_allocation.id)
                || release.kind != WorkKind::Release
                || !release
                    .allocations
                    .iter()
                    .any(|usage| usage.allocation == mapped_allocation.id)
                || !mapped_allocation
                    .lifetime
                    .release_after
                    .contains(mapped.terminal())
            {
                return invalid_publication_layout(format!(
                    "artifact {} mapped allocation is not acquired by its producer and retained through its release event",
                    layout.artifact()
                ));
            }
            let required = layout.resource_bounds().mapped_page_cache_bytes();
            for node in [&mapped_producer.id, &release.id] {
                *io_by_node
                    .entry((node.clone(), IoBufferKind::MappedPageCache))
                    .or_default() = io_by_node
                    .get(&(node.clone(), IoBufferKind::MappedPageCache))
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(required);
            }
            *bytes_by_allocation
                .entry(mapped_allocation.id.clone())
                .or_default() = bytes_by_allocation
                .get(&mapped_allocation.id)
                .copied()
                .unwrap_or(0)
                .saturating_add(required);
        }
    }
    for (producer, required) in staged_by_producer {
        let declared = dag.nodes()[&producer]
            .claims
            .iter()
            .filter_map(|claim| match claim.resource {
                LeaseResource::Storage {
                    use_kind: crate::StorageUseKind::StagedOutput,
                    ..
                } => Some(claim.amount),
                _ => None,
            })
            .fold(0_u64, u64::saturating_add);
        if declared < required {
            return invalid_publication_layout(format!(
                "producer {} stages {required} bytes but declares {declared}",
                producer.as_str()
            ));
        }
    }
    for ((node_id, kind), required) in io_by_node {
        let node = &dag.nodes()[&node_id];
        let claimed = node
            .claims
            .iter()
            .filter(|claim| claim.resource == LeaseResource::IoBuffer(kind))
            .map(|claim| claim.amount)
            .fold(0_u64, u64::saturating_add);
        let predicted = prediction.stages()[&node_id]
            .io()
            .iter()
            .find(|io| io.kind() == kind)
            .map_or(0, |io| io.bytes());
        if claimed < required || predicted < required {
            return invalid_publication_layout(format!(
                "node {} requires {required} {} bytes, claims {claimed}, predicts {predicted}",
                node_id.as_str(),
                io_buffer_name(kind)
            ));
        }
    }
    for (allocation, required) in bytes_by_allocation {
        let declared = dag.logical_allocations()[&allocation].bytes;
        if declared < required {
            return invalid_publication_layout(format!(
                "writer allocation {} requires {required} bytes but declares {declared}",
                allocation.as_str()
            ));
        }
    }
    let demand = &dag.resource_alternative().demand;
    let staged = demand
        .storage
        .iter()
        .map(|storage| storage.staged_output_bytes)
        .fold(0_u64, u64::saturating_add);
    let final_output = demand
        .storage
        .iter()
        .map(|storage| storage.final_output_bytes)
        .fold(0_u64, u64::saturating_add);
    if staged < layouts.staged_storage_bytes() || final_output < layouts.final_storage_bytes() {
        return invalid_publication_layout(format!(
            "resource demand stages {staged}/{} bytes and retains {final_output}/{} final bytes",
            layouts.staged_storage_bytes(),
            layouts.final_storage_bytes()
        ));
    }
    let mapped_demand = demand.io_buffers.bytes(IoBufferKind::MappedPageCache);
    if mapped_demand < layouts.mapped_page_cache_bytes() {
        return invalid_publication_layout(format!(
            "resource demand exposes {mapped_demand}/{} mapped/page-cache bytes",
            layouts.mapped_page_cache_bytes()
        ));
    }
    Ok(())
}

fn is_declared_completion(
    dag: &ExecutionDag,
    event: &crate::WorkDependency,
    expected_node: Option<&WorkNodeId>,
) -> bool {
    let node_id = dependency_node(event);
    if expected_node.is_some_and(|expected| expected != node_id) {
        return false;
    }
    let Some(node) = dag.nodes().get(node_id) else {
        return false;
    };
    match event {
        crate::WorkDependency::Work(_) => node.fences.is_empty(),
        crate::WorkDependency::Fence(fence) => node.fences.contains(&fence.kind()),
    }
}

fn dependency_node(dependency: &crate::WorkDependency) -> &WorkNodeId {
    match dependency {
        crate::WorkDependency::Work(node) => node,
        crate::WorkDependency::Fence(fence) => fence.node(),
    }
}

fn invalid_publication_layout<T>(reason: impl Into<String>) -> Result<T, PhysicalWorkBindingError> {
    Err(PhysicalWorkBindingError::InvalidPublicationLayout {
        reason: reason.into(),
    })
}

fn invalid_product_publication<T>(
    reason: impl Into<String>,
) -> Result<T, PhysicalWorkBindingError> {
    Err(PhysicalWorkBindingError::InvalidProductPublication {
        reason: reason.into(),
    })
}

fn validate_io_contracts(
    execution_dag: &ExecutionDag,
    prediction: &PlanPrediction,
) -> Result<(), PhysicalWorkBindingError> {
    for (node_id, stage) in &prediction.stages {
        let node = &execution_dag.nodes()[node_id];
        for io in stage.io() {
            let kind = io.kind();
            if !io_buffer_kind_supports_work_kind(kind, node.kind) {
                return Err(PhysicalWorkBindingError::IoKindMismatch {
                    node: node_id.clone(),
                    kind,
                    work_kind: node.kind,
                });
            }
            let has_claim = node.claims.iter().any(
                |claim| matches!(claim.resource, LeaseResource::IoBuffer(claimed) if claimed == kind),
            );
            let has_allocation = node.allocations.iter().any(|allocation_use| {
                matches!(
                    execution_dag.logical_allocations()[&allocation_use.allocation].purpose,
                    crate::AllocationPurpose::IoBuffer(allocated) if allocated == kind
                )
            });
            if !has_claim || !has_allocation {
                return Err(PhysicalWorkBindingError::MissingIoContract {
                    node: node_id.clone(),
                    kind,
                });
            }
        }
        let predicted = stage
            .io()
            .iter()
            .map(|io| io.kind())
            .collect::<std::collections::BTreeSet<_>>();
        let declared = node
            .claims
            .iter()
            .filter_map(|claim| match claim.resource {
                LeaseResource::IoBuffer(kind) => Some(kind),
                _ => None,
            })
            .chain(node.allocations.iter().filter_map(|allocation_use| {
                match execution_dag.logical_allocations()[&allocation_use.allocation].purpose {
                    crate::AllocationPurpose::IoBuffer(kind) => Some(kind),
                    crate::AllocationPurpose::Data => None,
                }
            }))
            .collect::<std::collections::BTreeSet<_>>();
        for kind in declared {
            if !predicted.contains(&kind) {
                return Err(PhysicalWorkBindingError::MissingIoPrediction {
                    node: node_id.clone(),
                    kind,
                });
            }
        }
    }
    Ok(())
}

fn has_publication_contract(node: &crate::WorkNode, stage: &StagePrediction) -> bool {
    if node.kind != WorkKind::Publication
        || !node.fences.contains(&FenceKind::Io)
        || !node.fences.contains(&FenceKind::Publication)
        || !stage
            .io()
            .iter()
            .any(|prediction| prediction.kind() == IoBufferKind::Publication)
    {
        return false;
    }
    let staged = node
        .claims
        .iter()
        .filter_map(|claim| match &claim.resource {
            LeaseResource::Storage {
                demand_id,
                use_kind: crate::StorageUseKind::StagedOutput,
            } if claim.amount > 0 => Some(demand_id),
            _ => None,
        });
    let final_outputs = node
        .claims
        .iter()
        .filter_map(|claim| match &claim.resource {
            LeaseResource::Storage {
                demand_id,
                use_kind: crate::StorageUseKind::FinalOutput,
            } if claim.amount > 0 => Some(demand_id),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    staged.into_iter().any(|id| final_outputs.contains(id))
}

impl PhysicalWorkBinding {
    /// Return the exact transaction checkpoints and staging events emitted by planning.
    #[must_use]
    pub const fn observation_transaction(&self) -> &ObservationTransactionWork {
        &self.observation_transaction
    }

    /// Return the complete adapter-derived publication-layout ledger.
    #[must_use]
    pub const fn publication_layouts(&self) -> &PublicationLayoutLedger {
        &self.publication_layouts
    }
}

/// Stable identity of the exact host-use policy bound into a plan.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourcePolicyId([u8; 32]);

impl ResourcePolicyId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = RESOURCE_POLICY_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    pub(crate) const fn from_sha256(digest: [u8; 32]) -> Self {
        Self(digest)
    }
}

impl fmt::Debug for ResourcePolicyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResourcePolicyId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ResourcePolicyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Stable identity of one immutable execution plan and all its bindings.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionPlanId([u8; 32]);

impl ExecutionPlanId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = EXECUTION_PLAN_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ExecutionPlanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExecutionPlanId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ExecutionPlanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Immutable inputs available to the sole physical planning entrypoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanningBindings {
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicy,
    resource_policy_id: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileRecord,
}

impl PlanningBindings {
    /// Bind one registry snapshot, host-use policy, and reviewed cost model.
    #[must_use]
    pub fn new<P>(
        implementation_registry: ImplementationRegistryId,
        resource_policy: ResourcePolicy,
        planner_cost_model_profile: P,
    ) -> Self
    where
        P: Into<PlannerCostModelProfileRecord>,
    {
        let resource_policy_id = resource_policy_id(&resource_policy);
        Self {
            implementation_registry,
            resource_policy,
            resource_policy_id,
            planner_cost_model_profile: planner_cost_model_profile.into(),
        }
    }

    /// Return the exact implementation-registry snapshot identity.
    #[must_use]
    pub const fn implementation_registry_id(&self) -> ImplementationRegistryId {
        self.implementation_registry
    }

    /// Return the selected host-use policy.
    #[must_use]
    pub const fn resource_policy(&self) -> &ResourcePolicy {
        &self.resource_policy
    }

    /// Return the canonical identity of the selected host-use policy.
    #[must_use]
    pub const fn resource_policy_id(&self) -> ResourcePolicyId {
        self.resource_policy_id
    }

    /// Return the exact reviewed cost-model profile identity.
    #[must_use]
    pub const fn planner_cost_model_profile_id(&self) -> PlannerCostModelProfileId {
        self.planner_cost_model_profile.profile_id()
    }

    /// Return the reviewed or deployment-selected profile bound to planning.
    #[must_use]
    pub const fn planner_cost_model_profile(&self) -> &PlannerCostModelProfileRecord {
        &self.planner_cost_model_profile
    }
}

/// Immutable physical execution plan sealed to one complete binding set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    plan_id: ExecutionPlanId,
    problem_id: CompiledProblemId,
    product_graph_id: ProductGraphId,
    problem_inputs: ProblemInputIdentities,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicy,
    resource_policy_id: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
    recorded_receipt_source: crate::receipt::ReceiptEvidenceSource,
    receipt_store: ExecutionReceiptStore,
    implementation_contract: ImplementationContractCommitment,
    execution_dag: ExecutionDag,
    prediction: PlanPrediction,
    artifacts: Vec<PlannedArtifact>,
    observation_transaction: BoundObservationTransaction,
    publication_layouts: PublicationLayoutLedger,
    product_publication: ProductPublicationAuthority,
}

impl ExecutionPlan {
    /// Return the stable identity of this plan and all of its bindings.
    #[must_use]
    pub const fn plan_id(&self) -> ExecutionPlanId {
        self.plan_id
    }

    /// Return a handle to the canonical receipt store bound during planning.
    ///
    /// The returned handle shares the same bounded root and integrity source;
    /// it is the store that must be used when binding the execution receipt.
    #[must_use]
    pub fn receipt_store(&self) -> ExecutionReceiptStore {
        self.receipt_store.clone()
    }

    /// Bind execution provenance to this plan's canonical receipt store.
    #[must_use]
    pub fn bind_receipt(
        &self,
        provenance: crate::ExecutionProvenance,
    ) -> ExecutionReceiptBinding<'_> {
        self.receipt_store.bind(provenance)
    }

    /// Return the exact compiled problem identity.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiler-derived product topology sealed by this plan.
    #[must_use]
    pub const fn product_graph_id(&self) -> ProductGraphId {
        self.product_graph_id
    }

    /// Return the exact observation snapshot identity.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.problem_inputs.observation()
    }

    /// Return the compiler-derived coordinate and image-domain geometry identity.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return the exact numerical-contract identity.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the exact implementation-registry snapshot identity.
    #[must_use]
    pub const fn implementation_registry_id(&self) -> ImplementationRegistryId {
        self.implementation_registry
    }

    /// Return the exact resource-policy identity.
    #[must_use]
    pub const fn resource_policy_id(&self) -> ResourcePolicyId {
        self.resource_policy_id
    }

    /// Return the host-use policy selected during planning.
    #[must_use]
    pub const fn resource_policy(&self) -> &ResourcePolicy {
        &self.resource_policy
    }

    /// Return the exact reviewed cost-model profile identity.
    #[must_use]
    pub const fn planner_cost_model_profile_id(&self) -> PlannerCostModelProfileId {
        self.planner_cost_model_profile
    }

    /// Return the stable identity of the emitted physical work.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.execution_dag.physical_work_id()
    }

    /// Return the complete immutable physical work DAG selected by planning.
    #[must_use]
    pub const fn execution_dag(&self) -> &ExecutionDag {
        &self.execution_dag
    }

    /// Return the complete conservative plan prediction.
    #[must_use]
    pub const fn prediction(&self) -> &PlanPrediction {
        &self.prediction
    }

    /// Return every plan-visible artifact and cache identity.
    #[must_use]
    pub fn artifacts(&self) -> &[PlannedArtifact] {
        &self.artifacts
    }

    /// Return the exact problem-bound observation transaction sealed by planning.
    #[must_use]
    pub const fn observation_transaction(&self) -> &BoundObservationTransaction {
        &self.observation_transaction
    }

    /// Return physical layouts and resource bounds sealed by planning.
    #[must_use]
    pub const fn publication_layouts(&self) -> &PublicationLayoutLedger {
        &self.publication_layouts
    }
}

/// Physical planning or Resource Authority selection failure.
#[derive(Debug)]
pub enum PlanError<E> {
    /// The physical planner could not produce candidates.
    Planner(E),
    /// Durable receipt evidence could not be read or validated.
    Receipt(ReceiptError),
    /// A candidate was structurally incompatible with the authority topology.
    InvalidCandidate(ExecutionError),
    /// The emitted DAG and transaction declaration do not implement the compiled problem.
    ObservationTransaction(ObservationTransactionPlanError),
    /// No candidate could be admitted under current policy, pressure, and reservations.
    Resource(ResourceError),
}

impl<E> PlanError<E> {
    /// Machine-readable proof that no physical candidate fit current policy,
    /// pressure, and reservations.
    #[must_use]
    pub fn infeasibility_certificate(&self) -> Option<&AdmissionInfeasibilityCertificate> {
        match self {
            Self::Resource(ResourceError::NoFeasibleAlternative(certificate)) => Some(certificate),
            _ => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for PlanError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planner(error) => write!(formatter, "physical planner failed: {error}"),
            Self::Receipt(error) => write!(formatter, "execution receipt evidence failed: {error}"),
            Self::InvalidCandidate(error) => {
                write!(formatter, "physical candidate failed: {error}")
            }
            Self::ObservationTransaction(error) => error.fmt(formatter),
            Self::Resource(error) => write!(
                formatter,
                "Resource Authority rejected every physical candidate: {error}"
            ),
        }
    }
}

impl<E: Error + 'static> Error for PlanError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Planner(error) => Some(error),
            Self::Receipt(error) => Some(error),
            Self::InvalidCandidate(error) => Some(error),
            Self::ObservationTransaction(error) => Some(error),
            Self::Resource(error) => Some(error),
        }
    }
}

/// Ask the Resource Authority to select one feasible planner-emitted physical candidate and seal it.
///
/// Selection follows ADR-0010's lexicographic order. Every candidate must
/// preserve identical science, product, numerics, and capability requirements;
/// hard feasibility with reserved headroom is proven only by Resource Authority
/// admission under the bound host-use policy; and among admitted candidates
/// the plan commits to the minimum conservative predicted wall time including
/// uncertainty. Integrity-checked quantitative receipt constraints are passed
/// into that same authority admission, where they produce explicit recorded
/// refusals without entering the cost model. When no candidate fits, the returned
/// [`PlanError::infeasibility_certificate`] reports exactly why each
/// alternative was refused.
pub fn plan<E, R>(
    problem: &CompiledProblem,
    bindings: PlanningBindings,
    authority: &ResourceAuthority,
    registry: &R,
    receipts: &ExecutionReceiptStore,
    planner: impl FnOnce(&CompiledProblem, &PlanningBindings) -> Result<Vec<PhysicalWorkBinding>, E>,
) -> Result<ExecutionPlan, PlanError<E>>
where
    R: ImplementationRegistry,
{
    let recorded_infeasibility =
        RecordedInfeasibility::from_store(receipts).map_err(PlanError::Receipt)?;
    let emitted_candidates = planner(problem, &bindings).map_err(PlanError::Planner)?;
    let mut candidates = Vec::with_capacity(emitted_candidates.len());
    for candidate in emitted_candidates {
        candidates.push(candidate.bind_registry(registry).map_err(|error| {
            PlanError::InvalidCandidate(ExecutionError::InvalidPlan(error.to_string()))
        })?);
    }
    let Some(first) = candidates.first() else {
        return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
            "physical planner emitted no candidates".to_string(),
        )));
    };
    let required_capabilities = first.execution_dag.required_resource_capabilities().clone();
    let reference_products = product_surface(first);
    let reference_transaction = first.observation_transaction.clone();
    let reference_layouts = first.publication_layouts.clone();
    let reference_product_publication = first.product_publication.clone();
    for candidate in &candidates {
        candidate
            .implementation_contract()
            .validate_registry(registry, &candidate.execution_dag)
            .map_err(|error| {
                PlanError::InvalidCandidate(ExecutionError::InvalidPlan(error.to_string()))
            })?;
        let commitment = candidate.implementation_contract();
        if commitment.registry_id() != bindings.implementation_registry {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidate contract was not published by the bound implementation registry"
                    .to_string(),
            )));
        }
        if commitment.problem_id() != problem.problem_id() {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidate implementations are not committed to the compiled problem"
                    .to_string(),
            )));
        }
        if commitment.numerics_id() != problem.numerics_id() {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidate implementations are not committed to the Numerics Contract"
                    .to_string(),
            )));
        }
        if commitment.required_capabilities() != problem.required_capabilities() {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidate implementations do not commit to every compiler-derived capability"
                    .to_string(),
            )));
        }
        let implementation_ids = candidate
            .execution_dag
            .nodes()
            .iter()
            .map(|(node, work)| (node.clone(), work.implementation.clone()))
            .collect::<BTreeMap<_, _>>();
        if commitment.implementation_ids() != &implementation_ids {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidate implementation commitment does not match its execution DAG"
                    .to_string(),
            )));
        }
        if candidate.execution_dag.required_resource_capabilities() != &required_capabilities {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidates disagree on required resource capabilities".to_string(),
            )));
        }
        if product_surface(candidate) != reference_products {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidates disagree on the product artifact surface; lexicographic planning preserves product contracts".to_string(),
            )));
        }
        if candidate.observation_transaction != reference_transaction {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidates disagree on the observation transaction; lexicographic planning preserves science contracts".to_string(),
            )));
        }
        if candidate.publication_layouts != reference_layouts {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidates disagree on publication layouts; lexicographic planning preserves product contracts".to_string(),
            )));
        }
        if candidate.product_publication != reference_product_publication {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidates disagree on product-publication authority; lexicographic planning preserves publication capabilities".to_string(),
            )));
        }
        validate_topology(&candidate.execution_dag, authority.topology())
            .map_err(PlanError::InvalidCandidate)?;
    }
    // Hard feasibility is decidable only by admission, so candidates are offered
    // in ascending conservative-predicted-time order and the authority commits
    // to the first feasible one: the minimum-time feasible candidate.
    candidates.sort_by_key(|candidate| candidate.prediction().conservative_nanos());
    let recorded_constraints = recorded_infeasibility.admission_constraints(
        problem.problem_id().as_bytes(),
        bindings.resource_policy_id.as_bytes(),
        &candidates,
    );
    let lease = match authority.acquire_with_recorded_constraints(
        bindings.resource_policy.clone(),
        DemandAlternatives {
            required_capabilities,
            alternatives: candidates
                .iter()
                .map(|candidate| candidate.execution_dag.resource_alternative().clone())
                .collect(),
        },
        &recorded_constraints,
    ) {
        Ok(lease) => lease,
        Err(error) => return Err(PlanError::Resource(error)),
    };
    let selected = lease.selected_alternative().clone();
    let release = lease.release().map_err(PlanError::Resource)?;
    if !release.is_released() {
        return Err(PlanError::Resource(ResourceError::Invalid(
            "provisional planning lease retained an unexpected fence".to_string(),
        )));
    }
    let physical_work = candidates
        .into_iter()
        .find(|candidate| candidate.execution_dag.resource_alternative().id == selected)
        .ok_or_else(|| {
            PlanError::InvalidCandidate(ExecutionError::InvalidState(
                "Resource Authority selected an absent physical candidate".to_string(),
            ))
        })?;
    let observation_transaction = bind_observation_transaction(
        problem,
        &physical_work.execution_dag,
        physical_work.observation_transaction.clone(),
        &physical_work.publication_layouts,
        &physical_work.artifacts,
    )
    .map_err(PlanError::ObservationTransaction)?;
    let planner_cost_model_profile = bindings.planner_cost_model_profile_id();
    let mut plan = ExecutionPlan {
        plan_id: ExecutionPlanId([0; 32]),
        problem_id: problem.problem_id(),
        product_graph_id: problem.product_graph().graph_id(),
        problem_inputs: problem.inputs().clone(),
        geometry: problem.geometry().geometry_id(),
        numerics: problem.numerics_id(),
        implementation_registry: bindings.implementation_registry,
        resource_policy: bindings.resource_policy,
        resource_policy_id: bindings.resource_policy_id,
        planner_cost_model_profile,
        recorded_receipt_source: recorded_infeasibility.source.clone(),
        receipt_store: receipts.clone(),
        implementation_contract: physical_work.implementation_contract,
        execution_dag: physical_work.execution_dag,
        prediction: physical_work.prediction,
        artifacts: physical_work.artifacts,
        observation_transaction,
        publication_layouts: physical_work.publication_layouts,
        product_publication: physical_work.product_publication,
    };
    plan.plan_id = execution_plan_id(&plan);
    Ok(plan)
}

/// The science- and product-bearing surface one physical candidate commits to.
///
/// Legal physical alternatives may vary resource claims, slots, knobs,
/// adaptations, implementation identities, and execution-only prepared/cache
/// artifacts, but never this surface.
fn product_surface(candidate: &PhysicalWorkBinding) -> BTreeMap<ArtifactIdentity, ArtifactRole> {
    candidate
        .artifacts
        .iter()
        .filter(|artifact| matches!(artifact.role(), ArtifactRole::Input | ArtifactRole::Output))
        .map(|artifact| (artifact.identity(), artifact.role()))
        .collect()
}

/// Recorded terminal failed or aborted executions that constrain planning.
///
/// Each entry is durable receipt evidence that one demand alternative of one
/// compiled problem terminally failed or was aborted. Quantitative receipts are
/// converted to explicit admission constraints owned by Resource Authority;
/// they never enter the performance cost model, which changes only through
/// reviewed profile promotion.
#[derive(Clone, Debug)]
pub(crate) struct RecordedInfeasibility {
    source: crate::receipt::ReceiptEvidenceSource,
    regions: Vec<RegionFailure>,
}

#[derive(Clone, Debug)]
struct RegionFailure {
    problem: [u8; 32],
    physical_work: [u8; 32],
    resource_policy: [u8; 32],
    alternative: AlternativeId,
    attempt: ExecutionAttemptId,
    status: ReceiptStatus,
    resource_identity: ResourceIdentity,
    required: u64,
    available: u64,
}

impl RecordedInfeasibility {
    /// Derive constraints from integrity-checked resource-infeasibility
    /// receipts in `store`.
    ///
    /// Other failed or aborted receipts are deliberately ignored: an
    /// interrupted scheduler, adapter, or evidence-contract failure is not
    /// proof that the candidate's resource region is infeasible.
    fn from_store(store: &crate::ExecutionReceiptStore) -> Result<Self, ReceiptError> {
        let mut regions = Vec::new();
        for attempt in store.attempts()? {
            let receipt = store.open(attempt)?;
            let status = receipt.status();
            if !matches!(
                status,
                ReceiptStatus::Failed | ReceiptStatus::Aborted | ReceiptStatus::Infeasible
            ) || receipt.failure_kind() != Some(ReceiptFailureKind::ResourceInfeasible)
            {
                continue;
            }
            let Some(crate::ReceiptInfeasibilityCertificate::Infeasible {
                resource_identity,
                required,
                available,
                ..
            }) = receipt.infeasibility_certificate()
            else {
                // Capability gaps and references to earlier receipts are not
                // quantitative pressure regions and cannot constrain a later
                // Resource Authority decision.
                continue;
            };
            regions.push(RegionFailure {
                problem: receipt.problem_identity(),
                physical_work: receipt.dag_identity(),
                resource_policy: receipt.resource_policy_identity(),
                alternative: receipt.selected_alternative_projection().id,
                attempt,
                status,
                resource_identity,
                required,
                available,
            });
        }
        Ok(Self {
            source: store.evidence_source(),
            regions,
        })
    }

    fn admission_constraints(
        &self,
        problem: [u8; 32],
        resource_policy: [u8; 32],
        candidates: &[PhysicalWorkBinding],
    ) -> Vec<crate::resource_authority::RecordedAdmissionConstraint> {
        candidates
            .iter()
            .flat_map(|candidate| {
                self.regions
                    .iter()
                    .filter(|region| {
                        region.problem == problem
                            && region.physical_work == candidate.physical_work_id().as_bytes()
                            && region.resource_policy == resource_policy
                            && region.alternative
                                == candidate.execution_dag.resource_alternative().id
                    })
                    .map(
                        |region| crate::resource_authority::RecordedAdmissionConstraint {
                            alternative: region.alternative.clone(),
                            resource: region.resource_identity.clone(),
                            required: region.required,
                            available: region.available,
                            attempt: region.attempt,
                            status: region.status,
                        },
                    )
            })
            .collect()
    }
}

/// Effective identities observed immediately before execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunBindings {
    problem_inputs: ProblemInputIdentities,
    resource_policy: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
}

impl RunBindings {
    /// Capture identities observed immediately before execution.
    #[must_use]
    pub fn new(
        problem_inputs: ProblemInputIdentities,
        resource_policy: &ResourcePolicy,
        planner_cost_model_profile: PlannerCostModelProfileId,
    ) -> Self {
        Self {
            problem_inputs,
            resource_policy: resource_policy_id(resource_policy),
            planner_cost_model_profile,
        }
    }
}

/// Exact binding whose mismatch prevented execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingKind {
    /// Compiled problem identity.
    CompiledProblem,
    /// Compiler-derived product-topology identity.
    ProductGraph,
    /// Observation snapshot identity.
    ObservationSnapshot,
    /// Compiled coordinate and image-domain geometry identity.
    CompiledGeometry,
    /// Canonical reference-data snapshot identities.
    ReferenceDataSnapshots,
    /// Initial model-state identity.
    ModelState,
    /// Implementation-registry snapshot identity.
    ImplementationRegistry,
    /// Resource-policy identity.
    ResourcePolicy,
    /// Planner cost-model profile identity.
    PlannerCostModelProfile,
}

/// Failure from exact plan binding validation, resource-backed scheduling, or
/// one plan-selected work implementation.
#[derive(Debug)]
pub enum RunError<E> {
    /// Durable receipt creation, update, validation, or retention failed.
    Receipt(ReceiptError),
    /// A binding changed after planning; execution was not entered.
    BindingMismatch {
        /// Exact rejected binding.
        binding: BindingKind,
    },
    /// The bound registry snapshot does not contain one selected work implementation.
    ImplementationUnavailable {
        /// Exact selected implementation missing from the registry.
        implementation: WorkImplementationId,
    },
    /// The registry returned an adapter under a different stable identity.
    ImplementationMismatch {
        /// Identity selected by the immutable DAG.
        planned: WorkImplementationId,
        /// Identity reported by the resolved adapter.
        observed: WorkImplementationId,
    },
    /// Resource admission or deterministic scheduling failed.
    Scheduler(ExecutionError),
    /// A successful adapter return omitted, duplicated, or exceeded sealed evidence.
    Evidence(ExecutionEvidenceError),
    /// Completed product authority or staged identities did not match the immutable plan.
    ProductPublication(crate::ProductPublicationError),
    /// One exact plan-owned work node or its asynchronous fence failed.
    Execution {
        /// Node whose adapter reported the failure.
        node: WorkNodeId,
        /// Adapter failure retained as the error source.
        source: E,
    },
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Receipt(error) => write!(formatter, "execution receipt failed: {error}"),
            Self::BindingMismatch { binding } => {
                write!(formatter, "execution plan binding mismatch: {binding:?}")
            }
            Self::ImplementationUnavailable { implementation } => {
                write!(
                    formatter,
                    "bound implementation is unavailable: {}",
                    implementation.as_str()
                )
            }
            Self::ImplementationMismatch { planned, observed } => write!(
                formatter,
                "implementation registry returned {} for planned work adapter {}",
                observed.as_str(),
                planned.as_str()
            ),
            Self::Scheduler(error) => write!(formatter, "execution scheduling failed: {error}"),
            Self::Evidence(error) => write!(formatter, "execution evidence failed: {error}"),
            Self::ProductPublication(error) => {
                write!(
                    formatter,
                    "product publication authorization failed: {error}"
                )
            }
            Self::Execution { node, source } => {
                write!(formatter, "work node {} failed: {source}", node.as_str())
            }
        }
    }
}

impl<E: Error + 'static> Error for RunError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Receipt(error) => Some(error),
            Self::BindingMismatch { .. }
            | Self::ImplementationUnavailable { .. }
            | Self::ImplementationMismatch { .. } => None,
            Self::Scheduler(error) => Some(error),
            Self::Evidence(error) => Some(error),
            Self::ProductPublication(error) => Some(error),
            Self::Execution { source, .. } => Some(source),
        }
    }
}

/// Compiled science visible to every work node without observation-source authority.
#[derive(Clone, Copy, Debug)]
pub struct CompiledWorkContext<'a> {
    problem: &'a CompiledProblem,
}

impl<'a> CompiledWorkContext<'a> {
    /// Return the stable compiled-problem identity.
    #[must_use]
    pub const fn problem_id(self) -> CompiledProblemId {
        self.problem.problem_id()
    }

    /// Return the exact observation snapshot bound into the compiled problem.
    #[must_use]
    pub const fn observation_snapshot_id(self) -> ObservationSnapshotId {
        self.problem.inputs().observation()
    }

    /// Return the exact numerical-contract identity.
    #[must_use]
    pub const fn numerics_id(self) -> NumericsContractId {
        self.problem.numerics_id()
    }

    /// Return compiled output geometry.
    #[must_use]
    pub const fn geometry(self) -> &'a CompiledGeometry {
        self.problem.geometry()
    }

    /// Return the compiled scientific contract.
    #[must_use]
    pub const fn science(self) -> &'a ScientificContract {
        self.problem.science()
    }

    /// Return compiled reconstruction semantics.
    #[must_use]
    pub const fn reconstruction(self) -> &'a ReconstructionContract {
        self.problem.reconstruction()
    }

    /// Return compiled weighting semantics.
    #[must_use]
    pub const fn weighting(self) -> &'a WeightingOperatorContract {
        self.problem.weighting()
    }

    /// Return exact required products.
    #[must_use]
    pub const fn products(self) -> &'a ProductRequirements {
        self.problem.products()
    }

    /// Return the numerical contract.
    #[must_use]
    pub const fn numerics(self) -> &'a NumericsContract {
        self.problem.numerics()
    }

    /// Return derived implementation capabilities.
    #[must_use]
    pub const fn required_capabilities(self) -> &'a BTreeSet<RequiredCapability> {
        self.problem.required_capabilities()
    }
}

/// Capability-scoped compiled inputs supplied to one exact work node.
///
/// Generic work receives compiled science but no MeasurementSet source set.
/// Only the initial consistency check, typed observation reads, model
/// writeback, and atomic publication receive their corresponding transaction
/// authority.
#[derive(Clone, Copy, Debug)]
pub struct WorkExecutionContext<'a> {
    attempt_id: ExecutionAttemptId,
    compiled: CompiledWorkContext<'a>,
    implementation_registry: ImplementationRegistryId,
    scheduled: &'a crate::execution::WorkExecutionContext,
    planned_artifacts: &'a [PlannedArtifact],
    stage_prediction: &'a StagePrediction,
    resource_alternative: &'a crate::DemandAlternative,
    observation_consistency: Option<&'a ObservationTransactionContract>,
    observation_reads: Option<&'a ObservationReadSet>,
    model_writes: Option<&'a ObservationWriteSet>,
    publication: Option<&'a ObservationTransactionContract>,
    publication_resources: Option<PublicationResources<'a>>,
    product_publication: Option<&'a crate::ProductPublicationAuthorization>,
    completed_observation_reads: &'a BTreeMap<WorkNodeId, AttemptBoundObservationCompletion>,
}

impl<'a> WorkExecutionContext<'a> {
    /// Return the execution attempt that dispatched this exact node call.
    #[must_use]
    pub const fn attempt_id(self) -> ExecutionAttemptId {
        self.attempt_id
    }

    /// Return compiled science common to every work node.
    #[must_use]
    pub const fn compiled(self) -> CompiledWorkContext<'a> {
        self.compiled
    }

    /// Return the exact implementation-registry snapshot selected by the plan
    /// and revalidated against the running registry.
    #[must_use]
    pub const fn implementation_registry_id(self) -> ImplementationRegistryId {
        self.implementation_registry
    }

    /// Return the exact planned node declaration.
    #[must_use]
    pub const fn node(self) -> &'a crate::WorkNode {
        self.scheduled.node()
    }

    /// Return the current pre-authorized execution configuration.
    #[must_use]
    pub const fn knobs(self) -> &'a ExecutionKnobs {
        self.scheduled.knobs()
    }

    /// Return the Resource Authority lease epoch issuing this context.
    #[must_use]
    pub const fn lease_epoch(self) -> u64 {
        self.scheduled.lease_epoch()
    }

    /// Return whether the scheduler dispatched this Release node while draining.
    ///
    /// Only a plan-validated [`WorkKind::Release`] node can observe `true`.
    /// Implementations use this distinction to discard externally retained
    /// state after failed predecessor work without accepting an incomplete
    /// success-path lifecycle.
    #[must_use]
    pub const fn is_cleanup(self) -> bool {
        self.scheduled.is_cleanup()
    }

    /// Return the scheduler-issued resource capabilities live for this call.
    #[must_use]
    pub fn resources(self) -> &'a [crate::WorkResourceCapability] {
        self.scheduled.resources()
    }

    /// Return the scheduler-issued allocation capabilities live for this call.
    #[must_use]
    pub fn allocations(self) -> &'a [crate::WorkAllocationCapability] {
        self.scheduled.allocations()
    }

    /// Return the canonical plan-listed artifacts owned by this exact node.
    pub fn planned_artifacts(self) -> impl Iterator<Item = &'a PlannedArtifact> + 'a {
        let node = &self.scheduled.node().id;
        self.planned_artifacts
            .iter()
            .filter(move |artifact| artifact.node() == node)
    }

    pub(crate) fn plan_artifact(self, identity: ArtifactIdentity) -> Option<&'a PlannedArtifact> {
        self.planned_artifacts
            .iter()
            .find(|artifact| artifact.identity() == identity)
    }

    /// Return the canonical prediction for this exact node.
    #[must_use]
    pub const fn stage_prediction(self) -> &'a StagePrediction {
        self.stage_prediction
    }

    /// Return the exact admitted resource alternative that owns this node's lease.
    #[must_use]
    pub const fn resource_alternative(self) -> &'a crate::DemandAlternative {
        self.resource_alternative
    }

    /// Return the expected observation state only for the initial consistency check.
    #[must_use]
    pub const fn observation_consistency(self) -> Option<&'a ObservationTransactionContract> {
        self.observation_consistency
    }

    /// Return exact MeasurementSet sources only for [`WorkKind::ObservationRead`].
    #[must_use]
    pub const fn observation_reads(self) -> Option<&'a ObservationReadSet> {
        self.observation_reads
    }

    /// Return the exact compiled Selected Observation only to an ObservationRead adapter.
    ///
    /// The storage owner needs the snapshot, provenance, geometry, commitment,
    /// and transaction identities together to validate and mint its scientific
    /// completion. Other work kinds receive no such authority.
    #[must_use]
    pub const fn selected_observation(self) -> Option<&'a CompiledProblem> {
        if self.observation_reads.is_some() {
            Some(self.compiled.problem)
        } else {
            None
        }
    }

    /// Return exact model-column writes only for the bound private writeback node.
    #[must_use]
    pub const fn model_writes(self) -> Option<&'a ObservationWriteSet> {
        self.model_writes
    }

    /// Return the complete transaction only for the sole atomic Publication node.
    #[must_use]
    pub const fn publication(self) -> Option<&'a ObservationTransactionContract> {
        self.publication
    }

    /// Return scheduler-owned resources only for the final atomic publish call.
    ///
    /// Presence proves that the admitted lease and every listed physical-memory
    /// permit remain held until [`WorkImplementation::publish`] returns.
    #[must_use]
    pub const fn publication_resources(self) -> Option<PublicationResources<'a>> {
        self.publication_resources
    }

    /// Return the runtime-validated Product Generation seal only to the final publish call.
    #[must_use]
    pub const fn product_publication(self) -> Option<&'a crate::ProductPublicationAuthorization> {
        self.product_publication
    }

    /// Return one scheduler-retained selected-observation completion when its
    /// owning read is an explicit predecessor of this exact node.
    #[must_use]
    pub fn predecessor_observation_completion(
        self,
        owner: &WorkNodeId,
    ) -> Option<&'a AttemptBoundObservationCompletion> {
        self.node()
            .dependencies
            .iter()
            .any(|dependency| match dependency {
                crate::WorkDependency::Work(node) => node == owner,
                crate::WorkDependency::Fence(fence) => fence.node() == owner,
            })
            .then(|| self.completed_observation_reads.get(owner))
            .flatten()
    }
}

/// Read-only proof of resources retained for the final atomic publish call.
#[derive(Clone, Copy, Debug)]
pub struct PublicationResources<'a> {
    reservation: &'a PublicationReservation,
}

/// Fresh runtime authority for finalizing one selected-observation read.
///
/// The runtime creates this affine value only after the owning
/// [`WorkKind::ObservationRead`] completes and every declared fence, if any, has
/// settled successfully. It has no public constructor: the selected-observation
/// adapter must consume it together with its owner-minted scientific completion.
#[derive(Debug)]
pub struct ObservationReadCompletionContext {
    attempt_id: ExecutionAttemptId,
    owner_node: WorkNodeId,
    settled_fences: BTreeSet<FenceKind>,
    lease_epoch: u64,
    problem_id: CompiledProblemId,
    observation_snapshot_id: ObservationSnapshotId,
    observation_provenance_id: ObservationProvenanceId,
    commitment_id: SelectedObservationCommitmentId,
}

impl ObservationReadCompletionContext {
    /// Return the execution attempt to which this fresh authority belongs.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.attempt_id
    }

    /// Return the exact ObservationRead node that owns this authority.
    #[must_use]
    pub const fn owner_node(&self) -> &WorkNodeId {
        &self.owner_node
    }

    /// Return the complete set of successfully settled owner fences, empty for a synchronous read.
    #[must_use]
    pub const fn settled_fences(&self) -> &BTreeSet<FenceKind> {
        &self.settled_fences
    }

    /// Return the live Resource Authority lease epoch used by the owner.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Bind the storage owner's complete selected-observation traversal to this attempt.
    ///
    /// A physical-only read result has no conversion to
    /// [`casa_ms::SelectedObservationCompletion`], so it cannot call this method:
    ///
    /// ```compile_fail
    /// use casa_imaging_runtime::ObservationReadCompletionContext;
    ///
    /// fn physical_only(context: ObservationReadCompletionContext) {
    ///     context.bind(());
    /// }
    /// ```
    pub fn bind(
        self,
        owner_completion: casa_ms::SelectedObservationCompletion,
    ) -> Result<AttemptBoundObservationCompletion, ObservationCompletionBindingError> {
        if owner_completion.problem_id() != self.problem_id
            || owner_completion.observation_snapshot_id() != self.observation_snapshot_id
            || owner_completion.observation_provenance_id() != self.observation_provenance_id
            || owner_completion.commitment_id() != self.commitment_id
        {
            return Err(ObservationCompletionBindingError);
        }
        Ok(AttemptBoundObservationCompletion {
            attempt_id: self.attempt_id,
            owner_node: self.owner_node,
            settled_fences: self.settled_fences,
            lease_epoch: self.lease_epoch,
            owner_completion,
        })
    }
}

/// A storage-owner completion did not match the runtime's exact compiled observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationCompletionBindingError;

impl fmt::Display for ObservationCompletionBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "selected-observation completion does not match the runtime-bound problem and provenance",
        )
    }
}

impl Error for ObservationCompletionBindingError {}

/// Affine selected-observation completion bound to one execution attempt and owning node.
///
/// The contained value is casa-ms's opaque scientific completion. Keeping that
/// concrete affine value inside this proof prevents the runtime from
/// synthesizing science identity or treating physical I/O completion alone as
/// selected-observation completion.
#[derive(Debug)]
pub struct AttemptBoundObservationCompletion {
    attempt_id: ExecutionAttemptId,
    owner_node: WorkNodeId,
    settled_fences: BTreeSet<FenceKind>,
    lease_epoch: u64,
    owner_completion: casa_ms::SelectedObservationCompletion,
}

impl AttemptBoundObservationCompletion {
    /// Return the caller-owned execution-attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.attempt_id
    }

    /// Return the exact ObservationRead node that owned this completion.
    #[must_use]
    pub const fn owner_node(&self) -> &WorkNodeId {
        &self.owner_node
    }

    /// Return the complete set of successfully settled owner fences, empty for a synchronous read.
    #[must_use]
    pub const fn settled_fences(&self) -> &BTreeSet<FenceKind> {
        &self.settled_fences
    }

    /// Return the live Resource Authority lease epoch used by the owning node.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Return the storage owner's opaque scientific completion.
    #[must_use]
    pub const fn owner_completion(&self) -> &casa_ms::SelectedObservationCompletion {
        &self.owner_completion
    }
}

impl<'a> PublicationResources<'a> {
    /// Return the Resource Authority epoch of the still-live execution lease.
    #[must_use]
    pub const fn lease_epoch(self) -> u64 {
        self.reservation.lease_epoch
    }

    /// Return the still-reserved physical slot for one publication allocation.
    #[must_use]
    pub fn allocation_slot(self, allocation: &AllocationId) -> Option<&'a PhysicalSlotId> {
        self.reservation.allocations.get(allocation)
    }
}

/// One exact plan-selected work-node adapter stored in an immutable registry.
pub trait WorkImplementation {
    /// Execution failure.
    type Error: Error + 'static;

    /// Return this adapter's stable plan identity.
    fn implementation_id(&self) -> &WorkImplementationId;

    /// Launch or synchronously execute exactly one scheduled node.
    ///
    /// Returning `Ok` means every fence declared by the node was launched and
    /// the returned evidence completely covers its plan-listed resource, I/O,
    /// and artifact work. Fences can subsequently be joined through
    /// [`Self::wait_for_fence`]. Returning `Err` guarantees that no asynchronous
    /// work escaped.
    /// A transaction-bound [`WorkKind::ObservationRead`] implementation
    /// revalidates the compiled read generations and holds its declared
    /// source-table locks through the complete work or fence event.
    /// A [`WorkKind::Writeback`] implementation writes only private staging.
    /// A [`WorkKind::Publication`] launch revalidates its bound input and write
    /// preconditions while holding its declared locks but does not expose
    /// staging before its publication fence succeeds.
    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error>;

    /// Return completed synchronous evidence retained by an execution error.
    ///
    /// Every implementation must choose explicitly whether its error retains
    /// evidence. An implementation that cannot mutate durable state or
    /// complete I/O before failure returns `None`; all others retain those
    /// measurements in the error and expose them here. The runtime validates
    /// the evidence against the sealed node before writing it into the failed
    /// execution receipt; artifacts not reached before failure may be omitted.
    fn failure_measurements<'error>(
        &'error self,
        error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements>;

    /// Block until one exact fence previously launched by [`Self::execute`]
    /// settles. An error means the fence settled unsuccessfully, so the
    /// scheduler may drain and release resources after recording failure. For
    /// [`WorkKind::Publication`], any launch or fence error leaves the previous
    /// generation solely visible. Successful [`FenceKind::Publication`]
    /// completion establishes publication readiness only; it does not expose
    /// staging.
    fn wait_for_fence(
        &self,
        context: WorkExecutionContext<'_>,
        fence: FenceKind,
    ) -> Result<(), Self::Error>;

    /// Bind the storage owner's selected-observation completion to runtime freshness evidence.
    ///
    /// This is invoked exactly once for an [`WorkKind::ObservationRead`] node,
    /// after synchronous work completion and all of that node's declared fences,
    /// if any, settle successfully, and before dependent work may launch. The
    /// implementation must consume `completion` with an owner-minted
    /// [`casa_ms::SelectedObservationCompletion`] through
    /// [`ObservationReadCompletionContext::bind`]; returning an error fails the
    /// run and prevents dependent work from launching.
    fn complete_observation_read(
        &self,
        completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error>;

    /// Return the Product Generation seal produced by this completed publication node.
    ///
    /// The runtime invokes this after synchronous work and every declared fence
    /// have settled but before it prepares the durable publication receipt or
    /// exposes staging. Native product publication plans require one projection;
    /// implementations without native product members may retain the default.
    fn complete_product_generation(
        &self,
        _context: WorkExecutionContext<'_>,
    ) -> Result<Option<casa_imaging_products::PublicationProjection>, Self::Error> {
        Ok(None)
    }

    /// Atomically activate all staged products and optional model-column output.
    ///
    /// The runtime invokes this exactly once, only after every fence and fallible
    /// scheduler transition has settled successfully, while the transaction
    /// lease, permits, and allocations remain held. Returning an error leaves
    /// the previous generation solely visible; returning success is the final
    /// operation before [`ExecutionOutcome::Succeeded`] and resource release.
    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error>;

    /// Independently publish one authorized product member.
    ///
    /// Returning `None` retains the indivisible [`Self::publish`] path. Native
    /// product publication implementations return one identity-bound outcome;
    /// the runtime checkpoints it before attempting the next member.
    fn publish_product_member(
        &self,
        _context: WorkExecutionContext<'_>,
        _entry: crate::AuthorizedProductPublicationEntry,
    ) -> Option<Result<ArtifactMeasurement, ProductMemberPublicationFailure<Self::Error>>> {
        None
    }
}

/// One failed member promotion together with its receipt evidence.
#[derive(Debug)]
pub struct ProductMemberPublicationFailure<E> {
    source: E,
    measurement: ArtifactMeasurement,
}

impl<E> ProductMemberPublicationFailure<E> {
    /// Retain the adapter failure and exact failed/uncertain member evidence.
    #[must_use]
    pub const fn new(source: E, measurement: ArtifactMeasurement) -> Self {
        Self {
            source,
            measurement,
        }
    }

    /// Consume the failure into its adapter source and member evidence.
    #[must_use]
    pub fn into_parts(self) -> (E, ArtifactMeasurement) {
        (self.source, self.measurement)
    }
}

/// Immutable registry snapshot that resolves selected implementations by identity.
pub trait ImplementationRegistry {
    /// Homogeneous execution interface stored by this registry.
    type Implementation: WorkImplementation;

    /// Return the exact snapshot identity bound during planning.
    fn registry_id(&self) -> ImplementationRegistryId;

    /// Resolve one implementation without substituting another candidate.
    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation>;

    /// Return the immutable science, numerics, and capability contract
    /// published by this exact registry snapshot for one implementation.
    /// Returning `None` fails closed when a planner tries to bind that
    /// implementation into physical work.
    fn implementation_contract(
        &self,
        _implementation: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        None
    }

    /// Resolve the canonical provider/catalog registration for preparation
    /// owned by one implementation in this exact registry snapshot.
    ///
    /// Registries that do not own prepared artifacts leave this absent. A
    /// prepared descriptor can only mint its closed owner through this lookup;
    /// caller-authored provider strings are not accepted by the descriptor.
    fn prepared_artifact_registration(
        &self,
        _implementation: &WorkImplementationId,
    ) -> Option<&crate::prepared_artifact::PreparedArtifactRegistration> {
        None
    }
}

/// Immutable scheduler state exposed to a run controller without exposing the
/// scheduler or its Resource Lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionStatus {
    lease_epoch: u64,
    pressure_changed: bool,
    knobs: ExecutionKnobs,
    applied_adaptations: Vec<AdaptationId>,
    eligible_adaptations: Vec<AdaptationTransition>,
}

impl ExecutionStatus {
    /// Returns the Resource Authority epoch backing this run.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Returns whether external pressure changed since lease admission.
    #[must_use]
    pub const fn pressure_changed(&self) -> bool {
        self.pressure_changed
    }

    /// Returns the exact current plan-authorized execution configuration.
    #[must_use]
    pub const fn knobs(&self) -> &ExecutionKnobs {
        &self.knobs
    }

    /// Returns the transitions already applied by this run.
    #[must_use]
    pub fn applied_adaptations(&self) -> &[AdaptationId] {
        &self.applied_adaptations
    }

    /// Returns only transitions applicable at the current globally idle cut.
    /// The list is empty while work or fences are active.
    #[must_use]
    pub fn eligible_adaptations(&self) -> &[AdaptationTransition] {
        &self.eligible_adaptations
    }
}

/// One controller request interpreted only through the plan-owned scheduler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunDirective {
    /// Continue with the current plan-authorized configuration.
    Continue,
    /// Cancel pending work and drain launched work while publication remains reversible.
    /// The controller is not consulted after atomic publication launches.
    Cancel,
    /// Apply one exact pre-authorized transition at its declared quiescence point.
    Adapt(AdaptationId),
}

/// Scheduling policy consulted by the sole validated [`run`] seam.
pub trait RunController {
    /// Return the next request. The scheduler rejects any unlisted transition
    /// or adaptation outside its exact global quiescence boundary. Successful
    /// atomic Publication launch permanently closes this polling seam.
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective;
}

/// Controller that executes the sealed initial configuration to completion.
#[derive(Clone, Copy, Debug, Default)]
pub struct RunToCompletion;

impl RunController for RunToCompletion {
    fn directive(&mut self, _status: &ExecutionStatus) -> RunDirective {
        RunDirective::Continue
    }
}

enum PendingRunError<E> {
    Receipt(ReceiptError),
    Scheduler(ExecutionError),
    Evidence(ExecutionEvidenceError),
    Execution { node: WorkNodeId, source: E },
}

impl<E> PendingRunError<E> {
    fn into_run_error(self) -> RunError<E> {
        match self {
            Self::Receipt(error) => RunError::Receipt(error),
            Self::Scheduler(error) => RunError::Scheduler(error),
            Self::Evidence(error) => RunError::Evidence(error),
            Self::Execution { node, source } => RunError::Execution { node, source },
        }
    }
}

fn defer_receipt_error<E>(
    scheduler: &mut ExecutionScheduler<'_>,
    pending: &mut Option<PendingRunError<E>>,
    error: ReceiptError,
) {
    if pending.is_none() {
        *pending = Some(PendingRunError::Receipt(error));
    }
    scheduler.cancel_after_error();
}

fn defer_scheduler_error<E>(
    scheduler: &mut ExecutionScheduler<'_>,
    pending: &mut Option<PendingRunError<E>>,
    error: ExecutionError,
) {
    if pending.is_none() {
        *pending = Some(PendingRunError::Scheduler(error));
    }
    scheduler.cancel_after_error();
}

fn terminal_drain_error<E>(
    scheduler: &mut ExecutionScheduler<'_>,
    pending: &mut Option<PendingRunError<E>>,
    invariant: &'static str,
) -> RunError<E> {
    let _ = scheduler.quarantine();
    pending.take().expect(invariant).into_run_error()
}

fn validate_work_measurements(
    plan: &ExecutionPlan,
    work: &WorkExecutionContext,
    measurements: &WorkMeasurements,
) -> Result<(), ExecutionEvidenceError> {
    validate_measurements(plan, work, measurements, true)
}

fn validate_failed_work_measurements(
    plan: &ExecutionPlan,
    work: &WorkExecutionContext,
    measurements: &WorkMeasurements,
) -> Result<(), ExecutionEvidenceError> {
    validate_measurements(plan, work, measurements, false)
}

fn validate_measurements(
    plan: &ExecutionPlan,
    work: &WorkExecutionContext,
    measurements: &WorkMeasurements,
    require_all_artifacts: bool,
) -> Result<(), ExecutionEvidenceError> {
    let node = &work.node().id;
    let claims = work
        .node()
        .claims
        .iter()
        .map(|claim| {
            (
                (claim.resource.clone(), claim.lifetime.clone()),
                claim.amount,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut measured_claims = BTreeMap::new();
    for measurement in measurements.resources() {
        let key = (
            measurement.resource().clone(),
            measurement.lifetime().clone(),
        );
        if measured_claims
            .insert(key.clone(), measurement.peak())
            .is_some()
        {
            return Err(ExecutionEvidenceError::DuplicateResource {
                node: node.clone(),
                resource: key.0,
                lifetime: key.1,
            });
        }
        if !claims.contains_key(&key) {
            return Err(ExecutionEvidenceError::UnplannedResource {
                node: node.clone(),
                resource: key.0,
                lifetime: key.1,
            });
        }
    }
    if let Some(((resource, lifetime), _)) = claims
        .iter()
        .find(|(key, _)| !measured_claims.contains_key(*key))
    {
        return Err(ExecutionEvidenceError::MissingResource {
            node: node.clone(),
            resource: resource.clone(),
            lifetime: lifetime.clone(),
        });
    }

    let predicted_io = plan.prediction.stages[node]
        .io()
        .iter()
        .map(|prediction| (prediction.kind(), *prediction))
        .collect::<BTreeMap<_, _>>();
    let mut measured_io = BTreeMap::new();
    for measurement in measurements.io() {
        if measured_io
            .insert(measurement.kind(), *measurement)
            .is_some()
        {
            return Err(ExecutionEvidenceError::DuplicateIo {
                node: node.clone(),
                kind: measurement.kind(),
            });
        }
        if !predicted_io.contains_key(&measurement.kind()) {
            return Err(ExecutionEvidenceError::UnplannedIo {
                node: node.clone(),
                kind: measurement.kind(),
            });
        }
    }
    if let Some(kind) = predicted_io
        .keys()
        .find(|kind| !measured_io.contains_key(kind))
    {
        return Err(ExecutionEvidenceError::MissingIo {
            node: node.clone(),
            kind: *kind,
        });
    }

    let planned_artifacts = plan
        .artifacts
        .iter()
        .filter(|artifact| artifact.node() == node)
        .map(|artifact| (artifact.identity(), artifact))
        .collect::<BTreeMap<_, _>>();
    let artifact_error = match validate_artifact_measurements(
        node,
        work.node().kind,
        &planned_artifacts,
        measurements,
        require_all_artifacts,
    ) {
        Ok(()) => None,
        Err(error @ ExecutionEvidenceError::RejectedArtifact { .. }) => Some(error),
        Err(error) => return Err(error),
    };

    if let Some(error) = measured_claims.iter().find_map(|(key, actual)| {
        let planned = claims
            .get(key)
            .expect("measured resource was proven plan-listed");
        (*actual > *planned).then(|| ExecutionEvidenceError::ResourcePeakExceeded {
            node: node.clone(),
            resource: key.0.clone(),
            planned: *planned,
            actual: *actual,
        })
    }) {
        return Err(error);
    }

    artifact_error.map_or(Ok(()), Err)
}

fn validate_artifact_measurements(
    node: &WorkNodeId,
    work_kind: WorkKind,
    planned_artifacts: &BTreeMap<ArtifactIdentity, &PlannedArtifact>,
    measurements: &WorkMeasurements,
    require_all_artifacts: bool,
) -> Result<(), ExecutionEvidenceError> {
    let mut measured_artifacts = BTreeMap::new();
    let mut rejected_artifact = None;
    for measurement in measurements.artifacts() {
        let artifact = measurement.planned_identity();
        if measured_artifacts.insert(artifact, *measurement).is_some() {
            return Err(ExecutionEvidenceError::DuplicateArtifact {
                node: node.clone(),
                artifact,
            });
        }
        let Some(planned) = planned_artifacts.get(&artifact) else {
            return Err(ExecutionEvidenceError::UnplannedArtifact {
                node: node.clone(),
                artifact,
            });
        };
        let disposition = measurement.disposition();
        let disposition_matches_role = if planned.role() == ArtifactRole::Output {
            matches!(
                disposition,
                ArtifactDisposition::Staged | ArtifactDisposition::PublicationPrepared
            )
        } else {
            matches!(
                disposition,
                ArtifactDisposition::Built
                    | ArtifactDisposition::Loaded
                    | ArtifactDisposition::Reused
                    | ArtifactDisposition::RejectedStale
            )
        };
        if !disposition_matches_role {
            return Err(ExecutionEvidenceError::ArtifactDispositionMismatch {
                node: node.clone(),
                artifact,
                role: planned.role(),
                disposition,
            });
        }
        if disposition == ArtifactDisposition::RejectedStale
            && measurement.observed_identity().is_none_or(|observed| {
                crate::prepared_artifact::PreparedArtifactRejection::from_evidence_identity(
                    artifact, observed,
                )
                .is_none()
            })
        {
            return Err(ExecutionEvidenceError::ArtifactDispositionMismatch {
                node: node.clone(),
                artifact,
                role: planned.role(),
                disposition,
            });
        }
        if work_kind == WorkKind::Cache && disposition == ArtifactDisposition::RejectedStale {
            rejected_artifact.get_or_insert(artifact);
        }
    }
    if require_all_artifacts
        && let Some(artifact) = planned_artifacts
            .keys()
            .find(|artifact| !measured_artifacts.contains_key(artifact))
    {
        return Err(ExecutionEvidenceError::MissingArtifact {
            node: node.clone(),
            artifact: *artifact,
        });
    }
    if let Some(artifact) = rejected_artifact {
        Err(ExecutionEvidenceError::RejectedArtifact {
            node: node.clone(),
            artifact,
        })
    } else {
        Ok(())
    }
}

fn work_execution_context<'a>(
    attempt_id: ExecutionAttemptId,
    problem: &'a CompiledProblem,
    plan: &'a ExecutionPlan,
    work: &'a crate::execution::WorkExecutionContext,
    completed_observation_reads: &'a BTreeMap<WorkNodeId, AttemptBoundObservationCompletion>,
) -> WorkExecutionContext<'a> {
    let compiled = CompiledWorkContext { problem };
    let transaction_work = plan.observation_transaction.work();
    let common = |observation_consistency,
                  observation_reads,
                  model_writes,
                  publication,
                  publication_resources| WorkExecutionContext {
        attempt_id,
        compiled,
        implementation_registry: plan.implementation_registry,
        scheduled: work,
        planned_artifacts: &plan.artifacts,
        stage_prediction: &plan.prediction.stages[&work.node().id],
        resource_alternative: plan.execution_dag.resource_alternative(),
        observation_consistency,
        observation_reads,
        model_writes,
        publication,
        publication_resources,
        product_publication: None,
        completed_observation_reads,
    };
    if work.node().kind == WorkKind::ObservationRead {
        common(
            None,
            Some(problem.observation_transaction().read_set()),
            None,
            None,
            None,
        )
    } else if transaction_work.model_column_staging() == Some(&work.node().id) {
        common(
            None,
            None,
            Some(problem.observation_transaction().write_set()),
            None,
            None,
        )
    } else if transaction_work.commit() == &work.node().id {
        common(
            None,
            None,
            None,
            Some(problem.observation_transaction()),
            None,
        )
    } else {
        common(
            (transaction_work.initial_consistency_check() == &work.node().id)
                .then_some(problem.observation_transaction()),
            None,
            None,
            None,
            None,
        )
    }
}

fn publication_execution_context<'a>(
    attempt_id: ExecutionAttemptId,
    problem: &'a CompiledProblem,
    plan: &'a ExecutionPlan,
    work: &'a crate::execution::WorkExecutionContext,
    reservation: &'a PublicationReservation,
    product_publication: Option<&'a crate::ProductPublicationAuthorization>,
    completed_observation_reads: &'a BTreeMap<WorkNodeId, AttemptBoundObservationCompletion>,
) -> WorkExecutionContext<'a> {
    let mut context =
        work_execution_context(attempt_id, problem, plan, work, completed_observation_reads);
    context.publication_resources = Some(PublicationResources { reservation });
    context.product_publication = product_publication;
    context
}

/// Persist the bound plan before execution, drive its complete DAG to
/// settlement, and atomically publish typed terminal evidence before returning.
pub fn run<R, C>(
    problem: &ExecutableModelProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
    registry: &R,
    authority: &ResourceAuthority,
    controller: &mut C,
    receipt: ExecutionReceiptBinding<'_>,
) -> Result<ExecutionOutcome, RunError<<R::Implementation as WorkImplementation>::Error>>
where
    R: ImplementationRegistry,
    C: RunController,
{
    let compiled_problem = problem.compiled_problem();
    let binding_result = validate_bindings(compiled_problem, plan, current);
    let receipt_source_matches = plan.recorded_receipt_source == receipt.evidence_source();
    // A valid run must use the store captured by planning.  A stale binding
    // still receives durable mutation evidence, but it is rebound to that
    // canonical store before any receipt file can be created; the caller's
    // noncanonical store is never mutated.
    if binding_result.is_ok() && !receipt_source_matches {
        return Err(RunError::Receipt(ReceiptError::IntegrityMismatch));
    }
    let receipt = if receipt_source_matches {
        receipt
    } else {
        plan.bind_receipt(receipt.provenance().clone())
    };
    let mut receipt = receipt.begin(problem, plan).map_err(RunError::Receipt)?;
    let result = match binding_result {
        Err(error) => Err(error),
        Ok(()) => run_inner(
            compiled_problem,
            plan,
            current,
            registry,
            authority,
            controller,
            &mut receipt,
        ),
    };
    if receipt.is_terminal() {
        return result;
    }
    let status = match &result {
        Ok(ExecutionOutcome::Succeeded) => ReceiptStatus::Completed,
        Ok(ExecutionOutcome::Cancelled) => ReceiptStatus::Cancelled,
        Err(RunError::BindingMismatch { .. }) => ReceiptStatus::Mutation,
        Err(RunError::Scheduler(ExecutionError::Resource(
            crate::ResourceError::Infeasible { .. }
            | crate::ResourceError::NoCapableAlternative
            | crate::ResourceError::NoFeasibleAlternative(_),
        ))) => ReceiptStatus::Infeasible,
        Err(_) => ReceiptStatus::Failed,
    };
    let failure = receipt_failure(&result);
    receipt.finish(status, failure).map_err(RunError::Receipt)?;
    result
}

fn receipt_failure<E>(result: &Result<ExecutionOutcome, RunError<E>>) -> Option<ReceiptFailure> {
    let error = result.as_ref().err()?;
    let failure = match error {
        RunError::BindingMismatch { binding } => ReceiptFailure::new(
            ReceiptFailureKind::BindingMutation,
            None,
            Some(binding_name(*binding).to_string()),
        ),
        RunError::ImplementationUnavailable { implementation } => ReceiptFailure::new(
            ReceiptFailureKind::ImplementationUnavailable,
            None,
            Some(implementation.as_str().to_string()),
        ),
        RunError::ImplementationMismatch { planned, observed } => ReceiptFailure::new(
            ReceiptFailureKind::ImplementationMismatch,
            None,
            Some(format!(
                "planned:{};observed:{}",
                planned.as_str(),
                observed.as_str()
            )),
        ),
        RunError::Evidence(error) => ReceiptFailure::new(
            ReceiptFailureKind::EvidenceContract,
            Some(error.node().clone()),
            None,
        ),
        RunError::ProductPublication(error) => ReceiptFailure::new(
            ReceiptFailureKind::EvidenceContract,
            None,
            Some(error.to_string()),
        ),
        RunError::Execution { node, .. } => {
            ReceiptFailure::new(ReceiptFailureKind::Adapter, Some(node.clone()), None)
        }
        RunError::Scheduler(ExecutionError::Resource(
            error @ (crate::ResourceError::Infeasible { .. }
            | crate::ResourceError::NoCapableAlternative
            | crate::ResourceError::NoFeasibleAlternative(_)),
        )) => ReceiptFailure::infeasible(error),
        RunError::Scheduler(_) | RunError::Receipt(_) => {
            ReceiptFailure::new(ReceiptFailureKind::Scheduler, None, None)
        }
    };
    Some(failure)
}

fn run_inner<R, C>(
    problem: &CompiledProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
    registry: &R,
    authority: &ResourceAuthority,
    controller: &mut C,
    receipt: &mut ReceiptRecorder<'_>,
) -> Result<ExecutionOutcome, RunError<<R::Implementation as WorkImplementation>::Error>>
where
    R: ImplementationRegistry,
    C: RunController,
{
    validate_bindings(problem, plan, current)?;
    if plan.implementation_registry != registry.registry_id() {
        return Err(RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry,
        });
    }
    let mut implementations = BTreeMap::new();
    for identity in plan.execution_dag.selected_implementations() {
        let implementation =
            registry
                .resolve(identity)
                .ok_or_else(|| RunError::ImplementationUnavailable {
                    implementation: identity.clone(),
                })?;
        if implementation.implementation_id() != identity {
            return Err(RunError::ImplementationMismatch {
                planned: identity.clone(),
                observed: implementation.implementation_id().clone(),
            });
        }
        implementations.insert(identity.clone(), implementation);
    }
    plan.implementation_contract
        .validate_registry(registry, &plan.execution_dag)
        .map_err(|_| RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry,
        })?;
    let mut scheduler = ExecutionScheduler::start(
        &plan.execution_dag,
        &plan.resource_policy,
        authority,
        Some(plan.observation_transaction.work().commit()),
    )
    .map_err(RunError::Scheduler)?;
    let mut launched = BTreeMap::<WorkNodeId, crate::execution::WorkExecutionContext>::new();
    let mut settled_observation_fences = BTreeMap::<WorkNodeId, BTreeSet<FenceKind>>::new();
    let mut completed_observation_reads =
        BTreeMap::<WorkNodeId, AttemptBoundObservationCompletion>::new();
    let mut publication_measurements = None;
    let mut pending = None;
    let mut controller_stopped = false;
    loop {
        if pending.is_none() && !controller_stopped {
            let status = match (scheduler.lease_epoch(), scheduler.pressure_changed()) {
                (Some(lease_epoch), Ok(Some(pressure_changed))) => ExecutionStatus {
                    lease_epoch,
                    pressure_changed,
                    knobs: scheduler.knobs().clone(),
                    applied_adaptations: scheduler.applied_adaptations().to_vec(),
                    eligible_adaptations: scheduler.eligible_adaptations(),
                },
                (None, _) | (_, Ok(None)) => {
                    defer_scheduler_error(
                        &mut scheduler,
                        &mut pending,
                        ExecutionError::InvalidState(
                            "active execution cannot observe its Resource Authority lease"
                                .to_string(),
                        ),
                    );
                    controller_stopped = true;
                    continue;
                }
                (_, Err(error)) => {
                    defer_scheduler_error(&mut scheduler, &mut pending, error);
                    controller_stopped = true;
                    continue;
                }
            };
            match controller.directive(&status) {
                RunDirective::Continue => {}
                RunDirective::Cancel => {
                    if let Err(error) = scheduler.cancel() {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                    }
                    controller_stopped = true;
                }
                RunDirective::Adapt(adaptation) => {
                    let eligible = status
                        .eligible_adaptations
                        .iter()
                        .map(|transition| transition.id.clone())
                        .collect::<Vec<_>>();
                    if !eligible.contains(&adaptation) {
                        defer_scheduler_error(
                            &mut scheduler,
                            &mut pending,
                            ExecutionError::IneligibleAdaptation {
                                requested: adaptation,
                                eligible,
                            },
                        );
                        controller_stopped = true;
                    } else if let Err(error) = scheduler.adapt(&adaptation) {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                    } else if let Err(error) = receipt.adaptation_applied(&adaptation) {
                        defer_receipt_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                    }
                }
            }
        }
        let action = match scheduler.next_action() {
            Ok(action) => action,
            Err(error) if pending.is_none() => {
                defer_scheduler_error(&mut scheduler, &mut pending, error);
                controller_stopped = true;
                continue;
            }
            Err(_) => {
                return Err(terminal_drain_error(
                    &mut scheduler,
                    &mut pending,
                    "draining scheduler error has a primary failure",
                ));
            }
        };
        match action {
            SchedulerAction::Work(work) => {
                let work = *work;
                let node_id = work.node().id.clone();
                let implementation = implementations[&work.node().implementation];
                if let Err(error) = receipt.work_started(&node_id) {
                    defer_receipt_error(&mut scheduler, &mut pending, error);
                    controller_stopped = true;
                    let _ = receipt.work_failed(&node_id);
                    if work.node().kind == WorkKind::Release {
                        if scheduler.fail_release_work(&node_id).is_err() {
                            return Err(terminal_drain_error(
                                &mut scheduler,
                                &mut pending,
                                "receipt checkpoint failure is retained",
                            ));
                        }
                        continue;
                    }
                    match scheduler.finish_work(
                        node_id,
                        WorkResult::Failed {
                            message: "execution receipt checkpoint failed".to_string(),
                        },
                    ) {
                        Ok(fences) => {
                            for fence in fences {
                                if scheduler.complete_fence(fence).is_err() {
                                    return Err(terminal_drain_error(
                                        &mut scheduler,
                                        &mut pending,
                                        "receipt checkpoint failure is retained",
                                    ));
                                }
                            }
                        }
                        Err(_) => {
                            return Err(terminal_drain_error(
                                &mut scheduler,
                                &mut pending,
                                "receipt checkpoint failure is retained",
                            ));
                        }
                    }
                    continue;
                }
                let context = work_execution_context(
                    receipt.attempt_id(),
                    problem,
                    plan,
                    &work,
                    &completed_observation_reads,
                );
                match implementation.execute(context) {
                    Ok(measurements) => {
                        if work.node().kind == WorkKind::Publication {
                            controller_stopped = true;
                        }
                        match validate_work_measurements(plan, &context, &measurements) {
                            Ok(()) => {
                                if work.node().kind == WorkKind::Publication {
                                    publication_measurements = Some(measurements.clone());
                                }
                                let mut receipt_error = receipt.fences_launched(&node_id).err();
                                if let Err(error) = receipt.work_completed(&node_id, &measurements)
                                    && receipt_error.is_none()
                                {
                                    receipt_error = Some(error);
                                }
                                if let Some(error) = receipt_error {
                                    defer_receipt_error(&mut scheduler, &mut pending, error);
                                    controller_stopped = true;
                                }
                                let synchronous_observation_read = work.node().kind
                                    == WorkKind::ObservationRead
                                    && work.node().fences.is_empty();
                                let work_lease_epoch = context.lease_epoch();
                                launched.insert(node_id.clone(), work);
                                match scheduler.finish_work(node_id.clone(), WorkResult::Succeeded)
                                {
                                    Ok(_) if synchronous_observation_read => {
                                        let completion = ObservationReadCompletionContext {
                                            attempt_id: receipt.attempt_id(),
                                            owner_node: node_id.clone(),
                                            settled_fences: BTreeSet::new(),
                                            lease_epoch: work_lease_epoch,
                                            problem_id: problem.problem_id(),
                                            observation_snapshot_id: problem
                                                .inputs()
                                                .observation_snapshot()
                                                .snapshot_id(),
                                            observation_provenance_id: problem
                                                .inputs()
                                                .observation_snapshot()
                                                .provenance_id(),
                                            commitment_id: problem
                                                .selected_observation()
                                                .commitment_id(),
                                        };
                                        match implementation.complete_observation_read(completion) {
                                            Ok(completion) => {
                                                if completed_observation_reads
                                                    .insert(node_id.clone(), completion)
                                                    .is_some()
                                                {
                                                    defer_scheduler_error(
                                                        &mut scheduler,
                                                        &mut pending,
                                                        ExecutionError::InvalidState(format!(
                                                            "observation read {} completed more than once",
                                                            node_id.as_str()
                                                        )),
                                                    );
                                                    controller_stopped = true;
                                                }
                                            }
                                            Err(source) => {
                                                if pending.is_none() {
                                                    pending = Some(PendingRunError::Execution {
                                                        node: node_id,
                                                        source,
                                                    });
                                                }
                                                controller_stopped = true;
                                                scheduler.cancel_after_error();
                                            }
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                                        controller_stopped = true;
                                    }
                                }
                            }
                            Err(error) => {
                                let record_failed_measurements = matches!(
                                    &error,
                                    ExecutionEvidenceError::RejectedArtifact { .. }
                                        | ExecutionEvidenceError::ResourcePeakExceeded { .. }
                                );
                                if pending.is_none() {
                                    pending = Some(PendingRunError::Evidence(error));
                                }
                                controller_stopped = true;
                                let mut receipt_error = receipt.fences_launched(&node_id).err();
                                if record_failed_measurements {
                                    // Retain structurally valid completed
                                    // evidence in the durable failure receipt.
                                    // Rejected warm artifacts and uncensored
                                    // resource overruns remain failures rather
                                    // than successful cache results.
                                    if let Err(error) = receipt
                                        .work_failed_with_measurements(&node_id, &measurements)
                                        && receipt_error.is_none()
                                    {
                                        receipt_error = Some(error);
                                    }
                                } else if let Err(error) = receipt.work_failed(&node_id)
                                    && receipt_error.is_none()
                                {
                                    receipt_error = Some(error);
                                }
                                if let Some(error) = receipt_error {
                                    defer_receipt_error(&mut scheduler, &mut pending, error);
                                }
                                launched.insert(node_id.clone(), work);
                                if scheduler
                                    .finish_work(node_id, WorkResult::Succeeded)
                                    .is_err()
                                {
                                    return Err(terminal_drain_error(
                                        &mut scheduler,
                                        &mut pending,
                                        "evidence failure is retained",
                                    ));
                                }
                                scheduler.cancel_after_error();
                            }
                        }
                    }
                    Err(source) => {
                        let diagnostic = source.to_string();
                        match implementation
                            .failure_measurements(&source)
                            .map(|measurements| {
                                (
                                    measurements,
                                    validate_failed_work_measurements(plan, &context, measurements),
                                )
                            }) {
                            Some((measurements, Ok(()))) => {
                                if let Err(error) =
                                    receipt.work_failed_with_measurements(&node_id, measurements)
                                {
                                    defer_receipt_error(&mut scheduler, &mut pending, error);
                                }
                            }
                            Some((measurements, Err(error))) => {
                                let record_failed_measurements = matches!(
                                    &error,
                                    ExecutionEvidenceError::ResourcePeakExceeded { .. }
                                );
                                if pending.is_none() {
                                    pending = Some(PendingRunError::Evidence(error));
                                }
                                let receipt_result = if record_failed_measurements {
                                    receipt.work_failed_with_measurements(&node_id, measurements)
                                } else {
                                    receipt.work_failed(&node_id)
                                };
                                if let Err(error) = receipt_result {
                                    defer_receipt_error(&mut scheduler, &mut pending, error);
                                }
                            }
                            None => {
                                let _ = receipt.work_failed(&node_id);
                            }
                        }
                        if pending.is_none() {
                            pending = Some(PendingRunError::Execution {
                                node: node_id.clone(),
                                source,
                            });
                        }
                        if work.node().kind == WorkKind::Release {
                            controller_stopped = true;
                            if scheduler.fail_release_work(&node_id).is_err() {
                                return Err(terminal_drain_error(
                                    &mut scheduler,
                                    &mut pending,
                                    "release failure is retained",
                                ));
                            }
                            scheduler.cancel_after_error();
                            continue;
                        }
                        controller_stopped = true;
                        match scheduler.finish_work(
                            node_id,
                            WorkResult::Failed {
                                message: diagnostic,
                            },
                        ) {
                            Ok(fences) => {
                                for fence in fences {
                                    if scheduler.complete_fence(fence).is_err() {
                                        return Err(terminal_drain_error(
                                            &mut scheduler,
                                            &mut pending,
                                            "executor failure is retained",
                                        ));
                                    }
                                }
                            }
                            Err(_) => {
                                scheduler.cancel_after_error();
                            }
                        }
                    }
                }
            }
            SchedulerAction::Waiting { .. } => {
                let Some(fence) = scheduler.next_pending_fence() else {
                    let error = ExecutionError::InvalidState(
                        "scheduler reported waiting without an outstanding fence".to_string(),
                    );
                    if pending.is_none() {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                        continue;
                    }
                    return Err(terminal_drain_error(
                        &mut scheduler,
                        &mut pending,
                        "waiting failure has a primary error",
                    ));
                };
                let Some(work) = launched.get(fence.node()) else {
                    let _ = scheduler.quarantine();
                    return Err(pending
                        .take()
                        .unwrap_or_else(|| {
                            PendingRunError::Scheduler(ExecutionError::InvalidState(
                                "outstanding fence has no launched work declaration".to_string(),
                            ))
                        })
                        .into_run_error());
                };
                let implementation = implementations[&work.node().implementation];
                let fence_work = work.for_fence(fence.kind());
                let context = work_execution_context(
                    receipt.attempt_id(),
                    problem,
                    plan,
                    &fence_work,
                    &completed_observation_reads,
                );
                if let Err(source) = implementation.wait_for_fence(context, fence.kind()) {
                    if pending.is_none() {
                        pending = Some(PendingRunError::Execution {
                            node: fence.node().clone(),
                            source,
                        });
                    }
                    let _ = receipt.fence_failed(&fence);
                    controller_stopped = true;
                    if work.node().kind == WorkKind::Release {
                        if scheduler.fail_release_fence(fence).is_err() {
                            return Err(terminal_drain_error(
                                &mut scheduler,
                                &mut pending,
                                "release fence failure is retained",
                            ));
                        }
                        scheduler.cancel_after_error();
                        continue;
                    }
                    if scheduler
                        .fail_fence(fence.clone(), "asynchronous work failed".to_string())
                        .is_err()
                    {
                        return Err(terminal_drain_error(
                            &mut scheduler,
                            &mut pending,
                            "fence failure is retained",
                        ));
                    }
                } else {
                    let observation_completion = if work.node().kind == WorkKind::ObservationRead {
                        let settled = settled_observation_fences
                            .entry(work.node().id.clone())
                            .or_default();
                        settled.insert(fence.kind());
                        if settled == &work.node().fences {
                            Some(ObservationReadCompletionContext {
                                attempt_id: receipt.attempt_id(),
                                owner_node: work.node().id.clone(),
                                settled_fences: settled.clone(),
                                lease_epoch: context.lease_epoch(),
                                problem_id: problem.problem_id(),
                                observation_snapshot_id: problem
                                    .inputs()
                                    .observation_snapshot()
                                    .snapshot_id(),
                                observation_provenance_id: problem
                                    .inputs()
                                    .observation_snapshot()
                                    .provenance_id(),
                                commitment_id: problem.selected_observation().commitment_id(),
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let mut fence_transition_succeeded = true;
                    if let Err(error) = receipt.fence_completed(&fence) {
                        defer_receipt_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                        fence_transition_succeeded = false;
                    }
                    if let Err(error) = scheduler.complete_fence(fence) {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                        fence_transition_succeeded = false;
                    }
                    if fence_transition_succeeded && let Some(completion) = observation_completion {
                        match implementation.complete_observation_read(completion) {
                            Ok(completion) => {
                                if completed_observation_reads
                                    .insert(work.node().id.clone(), completion)
                                    .is_some()
                                {
                                    defer_scheduler_error(
                                        &mut scheduler,
                                        &mut pending,
                                        ExecutionError::InvalidState(format!(
                                            "observation read {} completed more than once",
                                            work.node().id.as_str()
                                        )),
                                    );
                                    controller_stopped = true;
                                }
                            }
                            Err(source) => {
                                if pending.is_none() {
                                    pending = Some(PendingRunError::Execution {
                                        node: work.node().id.clone(),
                                        source,
                                    });
                                }
                                controller_stopped = true;
                                scheduler.cancel_after_error();
                            }
                        }
                    }
                }
            }
            SchedulerAction::PublicationReady {
                node: publication,
                resources,
            } => {
                if let Some(failure) = pending.take() {
                    return Err(failure.into_run_error());
                }
                if &publication != plan.observation_transaction.work().commit() {
                    return Err(RunError::Scheduler(ExecutionError::InvalidState(format!(
                        "scheduler exposed non-transaction publication node {}",
                        publication.as_str()
                    ))));
                }
                let work = launched.get(&publication).ok_or_else(|| {
                    RunError::Scheduler(ExecutionError::InvalidState(
                        "terminal publication has no launched work declaration".to_string(),
                    ))
                })?;
                let implementation = implementations[&work.node().implementation];
                let completion_context = publication_execution_context(
                    receipt.attempt_id(),
                    problem,
                    plan,
                    work,
                    &resources,
                    None,
                    &completed_observation_reads,
                );
                let projection = implementation
                    .complete_product_generation(completion_context)
                    .map_err(|source| RunError::Execution {
                        node: publication.clone(),
                        source,
                    })?;
                let product_publication = match (&plan.product_publication, projection) {
                    (ProductPublicationAuthority::Planned(planned), Some(projection)) => {
                        let authorization = planned
                            .authorize(&projection)
                            .map_err(RunError::ProductPublication)?;
                        let measurements = publication_measurements.as_ref().ok_or_else(|| {
                            RunError::ProductPublication(
                                crate::ProductPublicationError::MissingProjection,
                            )
                        })?;
                        authorization
                            .validate_staging(measurements)
                            .map_err(RunError::ProductPublication)?;
                        Some(authorization)
                    }
                    (ProductPublicationAuthority::Planned(_), None) => {
                        return Err(RunError::ProductPublication(
                            crate::ProductPublicationError::MissingProjection,
                        ));
                    }
                    (_, Some(_)) => {
                        return Err(RunError::ProductPublication(
                            crate::ProductPublicationError::UnexpectedProjection,
                        ));
                    }
                    (_, None) => None,
                };
                let context = publication_execution_context(
                    receipt.attempt_id(),
                    problem,
                    plan,
                    work,
                    &resources,
                    product_publication.as_ref(),
                    &completed_observation_reads,
                );
                if let Some(authorization) = product_publication.as_ref() {
                    receipt
                        .prepare_independent_product_publication()
                        .map_err(RunError::Receipt)?;
                    for entry in authorization.entries() {
                        let outcome = implementation
                            .publish_product_member(context, *entry)
                            .ok_or_else(|| {
                                RunError::ProductPublication(
                                    crate::ProductPublicationError::MissingMemberPublisher,
                                )
                            })?;
                        match outcome {
                            Ok(measurement) => {
                                receipt
                                    .record_publication_measurements(&WorkMeasurements::new(
                                        Vec::new(),
                                        Vec::new(),
                                        vec![measurement],
                                    ))
                                    .map_err(RunError::Receipt)?;
                            }
                            Err(failure) => {
                                let (source, measurement) = failure.into_parts();
                                receipt
                                    .record_publication_measurements(&WorkMeasurements::new(
                                        Vec::new(),
                                        Vec::new(),
                                        vec![measurement],
                                    ))
                                    .map_err(RunError::Receipt)?;
                                receipt
                                    .finish(
                                        ReceiptStatus::Failed,
                                        Some(ReceiptFailure::new(
                                            ReceiptFailureKind::Adapter,
                                            Some(publication.clone()),
                                            None,
                                        )),
                                    )
                                    .map_err(RunError::Receipt)?;
                                return Err(RunError::Execution {
                                    node: publication,
                                    source,
                                });
                            }
                        }
                    }
                    receipt
                        .complete_independent_product_publication()
                        .map_err(RunError::Receipt)?;
                    return Ok(ExecutionOutcome::Succeeded);
                }
                let prepared = receipt.prepare_publication().map_err(RunError::Receipt)?;
                match implementation.publish(context) {
                    Ok(()) => {
                        receipt.complete_publication(prepared);
                        return Ok(ExecutionOutcome::Succeeded);
                    }
                    Err(source) => {
                        drop(prepared);
                        return Err(RunError::Execution {
                            node: publication,
                            source,
                        });
                    }
                }
            }
            SchedulerAction::Complete(terminal) => {
                return match pending.take() {
                    Some(failure) => Err(failure.into_run_error()),
                    None => match terminal {
                        SchedulerTerminal::Succeeded => Ok(ExecutionOutcome::Succeeded),
                        SchedulerTerminal::Cancelled => Ok(ExecutionOutcome::Cancelled),
                        SchedulerTerminal::Failed { .. } => {
                            Err(RunError::Scheduler(ExecutionError::InvalidState(
                                "scheduler reported failure without its adapter error".to_string(),
                            )))
                        }
                    },
                };
            }
        }
    }
}

fn validate_bindings<E>(
    problem: &CompiledProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
) -> Result<(), RunError<E>> {
    let mismatch = if plan.problem_id != problem.problem_id() {
        Some(BindingKind::CompiledProblem)
    } else if plan.product_graph_id != problem.product_graph().graph_id() {
        Some(BindingKind::ProductGraph)
    } else if plan.geometry != problem.geometry().geometry_id() {
        Some(BindingKind::CompiledGeometry)
    } else if plan.problem_inputs.reference_data() != current.problem_inputs.reference_data() {
        Some(BindingKind::ReferenceDataSnapshots)
    } else if plan.problem_inputs.model() != current.problem_inputs.model() {
        Some(BindingKind::ModelState)
    } else if plan.problem_inputs.observation() != current.problem_inputs.observation() {
        Some(BindingKind::ObservationSnapshot)
    } else if plan.resource_policy_id != current.resource_policy {
        Some(BindingKind::ResourcePolicy)
    } else if plan.planner_cost_model_profile != current.planner_cost_model_profile {
        Some(BindingKind::PlannerCostModelProfile)
    } else {
        None
    };
    match mismatch {
        Some(binding) => Err(RunError::BindingMismatch { binding }),
        None => Ok(()),
    }
}

fn binding_name(binding: BindingKind) -> &'static str {
    match binding {
        BindingKind::CompiledProblem => "compiled_problem",
        BindingKind::ProductGraph => "product_graph",
        BindingKind::ObservationSnapshot => "observation_snapshot",
        BindingKind::CompiledGeometry => "compiled_geometry",
        BindingKind::ReferenceDataSnapshots => "reference_data_snapshots",
        BindingKind::ModelState => "model_state",
        BindingKind::ImplementationRegistry => "implementation_registry",
        BindingKind::ResourcePolicy => "resource_policy",
        BindingKind::PlannerCostModelProfile => "planner_cost_model_profile",
    }
}

fn resource_policy_id(policy: &ResourcePolicy) -> ResourcePolicyId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(RESOURCE_POLICY_IDENTITY_DOMAIN);
    encoder.u32(RESOURCE_POLICY_IDENTITY_VERSION);
    match policy {
        ResourcePolicy::Interactive => encoder.u8(0),
        ResourcePolicy::Balanced => encoder.u8(1),
        ResourcePolicy::Exclusive => encoder.u8(2),
        ResourcePolicy::Explicit(overrides) => {
            encoder.u8(3);
            encode_resource_overrides(&mut encoder, overrides);
        }
    }
    ResourcePolicyId(encoder.finish())
}

fn encode_resource_overrides(encoder: &mut CanonicalEncoder, overrides: &ResourceOverride) {
    encoder.usize(overrides.memory_bytes.len());
    for (domain, bytes) in &overrides.memory_bytes {
        encoder.string(domain.as_str());
        encoder.u64(*bytes);
    }
    encoder.optional_u64(overrides.workers);
    encoder.usize(overrides.storage_bytes.len());
    for (domain, bytes) in &overrides.storage_bytes {
        encoder.string(domain.as_str());
        encoder.u64(*bytes);
    }
    encoder.usize(overrides.rates_per_second.len());
    for (resource, rate) in &overrides.rates_per_second {
        encoder.string(resource.as_str());
        encoder.u64(*rate);
    }
    encoder.optional_u64(overrides.cache_bytes);
    encoder.optional_u64(overrides.locks);
    encoder.optional_u64(overrides.file_descriptors);
    encoder.usize(overrides.queue_slots.len());
    for (resource, slots) in &overrides.queue_slots {
        encoder.string(resource.as_str());
        encoder.u64(*slots);
    }
    encoder.usize(overrides.accelerator_slots.len());
    for (accelerator, slots) in &overrides.accelerator_slots {
        encoder.string(accelerator.as_str());
        encoder.u64(*slots);
    }
}

fn execution_plan_id(plan: &ExecutionPlan) -> ExecutionPlanId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(EXECUTION_PLAN_IDENTITY_DOMAIN);
    encoder.u32(EXECUTION_PLAN_IDENTITY_VERSION);
    encoder.digest(plan.problem_id.as_bytes());
    encoder.digest(plan.product_graph_id.as_bytes());
    encoder.digest(plan.problem_inputs.observation().identity().as_bytes());
    encoder.digest(plan.geometry.as_bytes());
    encoder.usize(plan.problem_inputs.reference_data().len());
    for (kind, identity) in plan.problem_inputs.reference_data() {
        encoder.u8(reference_data_tag(*kind));
        encoder.digest(identity.as_bytes());
    }
    match plan.problem_inputs.model() {
        casa_imaging_model::ModelStateIdentity::Empty => encoder.u8(0),
        casa_imaging_model::ModelStateIdentity::Seed(identity) => {
            encoder.u8(1);
            encoder.digest(identity.as_bytes());
        }
        casa_imaging_model::ModelStateIdentity::Generation(identity) => {
            encoder.u8(2);
            encoder.digest(identity.as_bytes());
        }
    }
    encoder.digest(plan.numerics.as_bytes());
    encoder.digest(plan.implementation_registry.as_bytes());
    // The receipt root is process-local execution authority, not portable logical plan identity.
    encoder.digest(plan.resource_policy_id.as_bytes());
    encoder.digest(plan.planner_cost_model_profile.as_bytes());
    encoder.digest(plan.execution_dag.physical_work_id().as_bytes());
    match &plan.product_publication {
        ProductPublicationAuthority::None => encoder.u8(0),
        ProductPublicationAuthority::Planned(publication) => {
            encoder.u8(1);
            encoder.digest(publication.problem_id().as_bytes());
            encoder.digest(publication.graph_id().as_bytes());
            encoder.digest(publication.generation_id().as_bytes());
            encoder.usize(publication.entries().len());
            for entry in publication.entries() {
                encoder.usize(entry.node().ordinal());
                encoder.digest(entry.artifact().as_bytes());
                encoder.u64(entry.payload_bytes());
            }
        }
    }
    encoder.u64(plan.prediction.elapsed_nanos());
    encoder.u32(plan.prediction.confidence().parts_per_million());
    encoder.usize(plan.prediction.uncertainty().len());
    for uncertainty in plan.prediction.uncertainty() {
        encoder.string(uncertainty.identity());
        encoder.u64(uncertainty.predicted_nanos());
    }
    encoder.usize(plan.prediction.stages().len());
    for (node, stage) in plan.prediction.stages() {
        encoder.string(node.as_str());
        encoder.u64(stage.elapsed_nanos());
        encoder.usize(stage.io().len());
        for io in stage.io() {
            encode_io_buffer_kind(&mut encoder, io.kind());
            encoder.u64(io.bytes());
            encoder.u64(io.operations());
        }
    }
    encoder.usize(plan.artifacts.len());
    for artifact in &plan.artifacts {
        encoder.digest(artifact.identity().as_bytes());
        encoder.string(artifact.node().as_str());
        encoder.u8(match artifact.role() {
            ArtifactRole::Input => 0,
            ArtifactRole::Prepared => 1,
            ArtifactRole::Cache => 2,
            ArtifactRole::Output => 3,
        });
        match artifact.cache_identity() {
            Some(cache) => {
                encoder.u8(1);
                encoder.digest(cache.as_bytes());
            }
            None => encoder.u8(0),
        }
    }
    encode_observation_transaction(&mut encoder, &plan.observation_transaction);
    encoder.usize(plan.publication_layouts.entries().len());
    for layout in plan.publication_layouts.entries() {
        match layout.participant() {
            crate::PublicationParticipant::Product { graph_id, node_id } => {
                encoder.u8(0);
                encoder.digest(graph_id.as_bytes());
                encoder.usize(node_id.ordinal());
            }
            crate::PublicationParticipant::ModelData(measurement_set) => {
                encoder.u8(1);
                encoder.digest(measurement_set.identity().as_bytes());
            }
        }
        encoder.digest(layout.artifact().as_bytes());
        encoder.digest(layout.layout_id().as_bytes());
        encoder.string(layout.staging().producer().as_str());
        encode_dependencies(
            &mut encoder,
            &BTreeSet::from([layout.staging().terminal().clone()]),
        );
        encoder.string(io_buffer_name(layout.staging().writer_buffer_kind()));
        encoder.string(layout.staging().writer_allocation().as_str());
        match layout.staging().mapped_page_cache() {
            Some(mapped) => {
                encoder.u8(1);
                encoder.string(mapped.producer().as_str());
                encode_dependencies(&mut encoder, &BTreeSet::from([mapped.terminal().clone()]));
                encoder.string(mapped.allocation().as_str());
            }
            None => encoder.u8(0),
        }
        let bounds = layout.resource_bounds();
        encoder.u64(bounds.staged_storage_bytes());
        encoder.u64(bounds.final_storage_bytes());
        encoder.u64(bounds.writer_buffer_bytes());
        encoder.u64(bounds.mapped_page_cache_bytes());
    }
    ExecutionPlanId(encoder.finish())
}

fn encode_observation_transaction(
    encoder: &mut CanonicalEncoder,
    transaction: &BoundObservationTransaction,
) {
    encoder.u8(match transaction.work().publication_scope() {
        crate::ObservationTransactionPublicationScope::ReconstructionOnly => 0,
        crate::ObservationTransactionPublicationScope::ProductPublication => 1,
    });
    encoder.digest(transaction.problem_id().as_bytes());
    encoder.digest(transaction.product_graph_id().as_bytes());
    encoder.digest(transaction.transaction_id().as_bytes());
    encoder.digest(transaction.physical_work_id().as_bytes());
    let work = transaction.work();
    encoder.string(work.initial_consistency_check().as_str());
    encode_dependencies(encoder, work.observation_reads());
    encoder.string(work.final_reconciliation().as_str());
    encode_dependencies(encoder, work.product_staging());
    match work.model_column_staging() {
        Some(node) => {
            encoder.u8(1);
            encoder.string(node.as_str());
        }
        None => encoder.u8(0),
    }
    encoder.string(work.commit().as_str());
}

fn encode_dependencies(
    encoder: &mut CanonicalEncoder,
    dependencies: &std::collections::BTreeSet<crate::WorkDependency>,
) {
    encoder.usize(dependencies.len());
    for dependency in dependencies {
        match dependency {
            crate::WorkDependency::Work(node) => {
                encoder.u8(0);
                encoder.string(node.as_str());
            }
            crate::WorkDependency::Fence(fence) => {
                encoder.u8(1);
                encoder.string(fence.node().as_str());
                encoder.u8(match fence.kind() {
                    FenceKind::Device => 0,
                    FenceKind::Io => 1,
                    FenceKind::Writeback => 2,
                    FenceKind::Publication => 3,
                });
            }
        }
    }
}

fn reference_data_tag(kind: ReferenceDataKind) -> u8 {
    match kind {
        ReferenceDataKind::Measures => 0,
        ReferenceDataKind::Ephemeris => 1,
        ReferenceDataKind::Observatory => 2,
        ReferenceDataKind::SpectralLines => 3,
        ReferenceDataKind::Instrument => 4,
    }
}

fn encode_io_buffer_kind(encoder: &mut CanonicalEncoder, kind: IoBufferKind) {
    encoder.u8(match kind {
        IoBufferKind::SourceReadAhead => 0,
        IoBufferKind::Decode => 1,
        IoBufferKind::Preparation => 2,
        IoBufferKind::HostToDeviceTransfer => 3,
        IoBufferKind::DeviceToHostTransfer => 4,
        IoBufferKind::SpillRead => 5,
        IoBufferKind::SpillWrite => 6,
        IoBufferKind::Serialization => 7,
        IoBufferKind::StorageManager => 8,
        IoBufferKind::TiledColumnWriter => 9,
        IoBufferKind::ScalarColumnWriter => 10,
        IoBufferKind::Writeback => 11,
        IoBufferKind::Publication => 12,
        IoBufferKind::MappedPageCache => 13,
    });
}

fn io_buffer_name(kind: IoBufferKind) -> &'static str {
    match kind {
        IoBufferKind::SourceReadAhead => "source-read-ahead",
        IoBufferKind::Decode => "decode",
        IoBufferKind::Preparation => "preparation",
        IoBufferKind::HostToDeviceTransfer => "host-to-device-transfer",
        IoBufferKind::DeviceToHostTransfer => "device-to-host-transfer",
        IoBufferKind::SpillRead => "spill-read",
        IoBufferKind::SpillWrite => "spill-write",
        IoBufferKind::Serialization => "serialization",
        IoBufferKind::StorageManager => "storage-manager",
        IoBufferKind::TiledColumnWriter => "tiled-column-writer",
        IoBufferKind::ScalarColumnWriter => "scalar-column-writer",
        IoBufferKind::Writeback => "writeback",
        IoBufferKind::Publication => "publication",
        IoBufferKind::MappedPageCache => "mapped-page-cache",
    }
}

fn lease_resource_name(resource: &LeaseResource) -> String {
    match resource {
        LeaseResource::Memory { allocation_id } => format!("memory:{allocation_id}"),
        LeaseResource::Workers => "workers".to_string(),
        LeaseResource::RuntimeOverhead(kind) => format!(
            "runtime-overhead:{}",
            match kind {
                crate::RuntimeOverheadKind::ThreadStack => "thread-stack",
                crate::RuntimeOverheadKind::AllocatorFragmentation => "allocator-fragmentation",
                crate::RuntimeOverheadKind::ExternalLibrary => "external-library",
                crate::RuntimeOverheadKind::FftWorkspace => "fft-workspace",
                crate::RuntimeOverheadKind::Driver => "driver",
                crate::RuntimeOverheadKind::Jit => "jit",
                crate::RuntimeOverheadKind::CommandBuffer => "command-buffer",
            }
        ),
        LeaseResource::IoBuffer(kind) => format!("io-buffer:{}", io_buffer_name(*kind)),
        LeaseResource::Storage {
            demand_id,
            use_kind,
        } => format!(
            "storage:{demand_id}:{}",
            match use_kind {
                crate::StorageUseKind::Temporary => "temporary",
                crate::StorageUseKind::StagedOutput => "staged-output",
                crate::StorageUseKind::FinalOutput => "final-output",
                crate::StorageUseKind::PersistentCache => "persistent-cache",
            }
        ),
        LeaseResource::StorageReadRate { demand_id } => {
            format!("storage-read-rate:{demand_id}")
        }
        LeaseResource::StorageWriteRate { demand_id } => {
            format!("storage-write-rate:{demand_id}")
        }
        LeaseResource::StorageOperationsRate { demand_id } => {
            format!("storage-operations-rate:{demand_id}")
        }
        LeaseResource::StorageQueue { demand_id } => format!("storage-queue:{demand_id}"),
        LeaseResource::Rate { demand_id } => format!("rate:{demand_id}"),
        LeaseResource::Queue { demand_id } => format!("queue:{demand_id}"),
        LeaseResource::TransferRate { demand_id } => format!("transfer-rate:{demand_id}"),
        LeaseResource::TransferQueue { demand_id } => format!("transfer-queue:{demand_id}"),
        LeaseResource::Accelerator { demand_id } => format!("accelerator:{demand_id}"),
        LeaseResource::AcceleratorCommandQueue { demand_id } => {
            format!("accelerator-command-queue:{demand_id}")
        }
        LeaseResource::ResidentCache => "resident-cache".to_string(),
        LeaseResource::MeasurementSetLock { measurement_set } => {
            format!("measurement-set-lock:{measurement_set}")
        }
        LeaseResource::Locks => "locks".to_string(),
        LeaseResource::FileDescriptors => "file-descriptors".to_string(),
    }
}

pub(crate) struct CanonicalEncoder(Sha256);

impl CanonicalEncoder {
    pub(crate) fn new() -> Self {
        Self(Sha256::new())
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn usize(&mut self, value: usize) {
        self.0.update((value as u128).to_le_bytes());
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    pub(crate) fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    pub(crate) fn digest(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    pub(crate) fn optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod artifact_measurement_tests {
    use super::*;

    #[test]
    fn store_owned_rejection_requires_typed_identity_bound_to_planned_artifact() {
        let node = WorkNodeId::new("cache");
        let identity = ArtifactIdentity::from_sha256([1; 32]);
        let other_identity = ArtifactIdentity::from_sha256([2; 32]);
        let planned = PlannedArtifact::new(identity, node.clone(), ArtifactRole::Cache, None);
        let planned_artifacts = BTreeMap::from([(identity, &planned)]);
        let invalid = [
            None,
            Some(ArtifactIdentity::from_sha256([3; 32])),
            Some(
                crate::prepared_artifact::PreparedArtifactRejection::Missing
                    .evidence_identity(other_identity),
            ),
        ];

        for observed in invalid {
            let measurements = WorkMeasurements::new(
                Vec::new(),
                Vec::new(),
                vec![ArtifactMeasurement::new_store_owned(
                    identity,
                    observed,
                    ArtifactDisposition::RejectedStale,
                    0,
                    None,
                )],
            );
            assert!(matches!(
                validate_artifact_measurements(
                    &node,
                    WorkKind::Cache,
                    &planned_artifacts,
                    &measurements,
                    true,
                ),
                Err(ExecutionEvidenceError::ArtifactDispositionMismatch { artifact, .. })
                    if artifact == identity
            ));
        }

        let valid = WorkMeasurements::new(
            Vec::new(),
            Vec::new(),
            vec![ArtifactMeasurement::new_store_owned(
                identity,
                Some(
                    crate::prepared_artifact::PreparedArtifactRejection::Missing
                        .evidence_identity(identity),
                ),
                ArtifactDisposition::RejectedStale,
                0,
                None,
            )],
        );
        assert!(matches!(
            validate_artifact_measurements(
                &node,
                WorkKind::Cache,
                &planned_artifacts,
                &valid,
                true,
            ),
            Err(ExecutionEvidenceError::RejectedArtifact { artifact, .. })
                if artifact == identity
        ));
    }
}
