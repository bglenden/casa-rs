// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, error::Error, fmt, path::Path};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, NumericsContractId,
    ObservationSnapshotId, ProblemInputIdentities, ReferenceDataKind,
};
use sha2::{Digest, Sha256};

use crate::{
    AdaptationId, AdaptationTransition, ClaimLifetime, DemandAlternatives, ExecutionError,
    ExecutionKnobs, ExecutionOutcome, ExecutionReceiptBinding, FenceKind, IoBufferKind,
    LeaseResource, ReceiptError, ReceiptFailureKind, ReceiptStatus, ResourceAuthority,
    ResourceError, ResourceOverride, ResourcePolicy, WorkExecutionContext, WorkImplementationId,
    WorkKind, WorkNodeId,
    execution::{
        ExecutionDag, ExecutionScheduler, SchedulerAction, SchedulerTerminal, WorkResult,
        io_buffer_kind_supports_work_kind, validate_topology,
    },
    receipt::{ReceiptFailure, ReceiptRecorder},
};

const EXECUTION_PLAN_IDENTITY_DOMAIN: &[u8] = b"casa-rs-execution-plan";
const EXECUTION_PLAN_IDENTITY_VERSION: u32 = 4;
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

/// Final observed treatment of one plan-listed artifact.
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
    /// Durably published as an output.
    Published,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactMeasurement {
    planned: ArtifactIdentity,
    observed: Option<ArtifactIdentity>,
    disposition: ArtifactDisposition,
    bytes: u64,
    path: Option<RedactedPath>,
}

impl ArtifactMeasurement {
    /// Report one plan-listed artifact's observed identity and final disposition.
    #[must_use]
    pub const fn new(
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

    /// Return the observed content identity, when materialized.
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
            | Self::ArtifactDispositionMismatch { node, .. } => node,
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
        }
    }
}

impl Error for ExecutionEvidenceError {}

/// Complete physical work emitted by the sole planning seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalWorkBinding {
    execution_dag: ExecutionDag,
    prediction: PlanPrediction,
    artifacts: Vec<PlannedArtifact>,
}

impl PhysicalWorkBinding {
    /// Bind a complete immutable physical work DAG, prediction, and artifact ledger.
    pub fn new(
        execution_dag: ExecutionDag,
        prediction: PlanPrediction,
        mut artifacts: Vec<PlannedArtifact>,
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
        Ok(Self {
            execution_dag,
            prediction,
            artifacts,
        })
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
    planner_cost_model_profile: PlannerCostModelProfileId,
}

impl PlanningBindings {
    /// Bind one registry snapshot, host-use policy, and reviewed cost model.
    #[must_use]
    pub fn new(
        implementation_registry: ImplementationRegistryId,
        resource_policy: ResourcePolicy,
        planner_cost_model_profile: PlannerCostModelProfileId,
    ) -> Self {
        let resource_policy_id = resource_policy_id(&resource_policy);
        Self {
            implementation_registry,
            resource_policy,
            resource_policy_id,
            planner_cost_model_profile,
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
        self.planner_cost_model_profile
    }
}

/// Immutable physical execution plan sealed to one complete binding set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    plan_id: ExecutionPlanId,
    problem_id: CompiledProblemId,
    problem_inputs: ProblemInputIdentities,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicy,
    resource_policy_id: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
    execution_dag: ExecutionDag,
    prediction: PlanPrediction,
    artifacts: Vec<PlannedArtifact>,
}

impl ExecutionPlan {
    /// Return the stable identity of this plan and all of its bindings.
    #[must_use]
    pub const fn plan_id(&self) -> ExecutionPlanId {
        self.plan_id
    }

    /// Return the exact compiled problem identity.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
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
}

/// Physical planning or Resource Authority selection failure.
#[derive(Debug)]
pub enum PlanError<E> {
    /// The physical planner could not produce candidates.
    Planner(E),
    /// A candidate was structurally incompatible with the authority topology.
    InvalidCandidate(ExecutionError),
    /// No candidate could be admitted under current policy, pressure, and reservations.
    Resource(ResourceError),
}

impl<E: fmt::Display> fmt::Display for PlanError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planner(error) => write!(formatter, "physical planner failed: {error}"),
            Self::InvalidCandidate(error) => {
                write!(formatter, "physical candidate failed: {error}")
            }
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
            Self::InvalidCandidate(error) => Some(error),
            Self::Resource(error) => Some(error),
        }
    }
}

/// Ask the Resource Authority to select one feasible planner-emitted physical candidate and seal it.
pub fn plan<E>(
    problem: &CompiledProblem,
    bindings: PlanningBindings,
    authority: &ResourceAuthority,
    planner: impl FnOnce(&CompiledProblem, &PlanningBindings) -> Result<Vec<PhysicalWorkBinding>, E>,
) -> Result<ExecutionPlan, PlanError<E>> {
    let candidates = planner(problem, &bindings).map_err(PlanError::Planner)?;
    let Some(first) = candidates.first() else {
        return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
            "physical planner emitted no candidates".to_string(),
        )));
    };
    let required_capabilities = first.execution_dag.required_resource_capabilities().clone();
    for candidate in &candidates {
        if candidate.execution_dag.required_resource_capabilities() != &required_capabilities {
            return Err(PlanError::InvalidCandidate(ExecutionError::InvalidPlan(
                "physical candidates disagree on required resource capabilities".to_string(),
            )));
        }
        validate_topology(&candidate.execution_dag, authority.topology())
            .map_err(PlanError::InvalidCandidate)?;
    }
    let lease = authority
        .acquire(
            bindings.resource_policy.clone(),
            DemandAlternatives {
                required_capabilities,
                alternatives: candidates
                    .iter()
                    .map(|candidate| candidate.execution_dag.resource_alternative().clone())
                    .collect(),
            },
        )
        .map_err(PlanError::Resource)?;
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
    let mut plan = ExecutionPlan {
        plan_id: ExecutionPlanId([0; 32]),
        problem_id: problem.problem_id(),
        problem_inputs: problem.inputs().clone(),
        geometry: problem.geometry().geometry_id(),
        numerics: problem.numerics_id(),
        implementation_registry: bindings.implementation_registry,
        resource_policy: bindings.resource_policy,
        resource_policy_id: bindings.resource_policy_id,
        planner_cost_model_profile: bindings.planner_cost_model_profile,
        execution_dag: physical_work.execution_dag,
        prediction: physical_work.prediction,
        artifacts: physical_work.artifacts,
    };
    plan.plan_id = execution_plan_id(&plan);
    Ok(plan)
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
            Self::Execution { source, .. } => Some(source),
        }
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
    fn execute(
        &self,
        problem: &CompiledProblem,
        work: &WorkExecutionContext,
    ) -> Result<WorkMeasurements, Self::Error>;

    /// Block until one exact fence previously launched by [`Self::execute`]
    /// settles. An error means the fence settled unsuccessfully, so the
    /// scheduler may drain and release resources after recording failure.
    fn wait_for_fence(
        &self,
        problem: &CompiledProblem,
        work: &WorkExecutionContext,
        fence: FenceKind,
    ) -> Result<(), Self::Error>;
}

/// Immutable registry snapshot that resolves selected implementations by identity.
pub trait ImplementationRegistry {
    /// Homogeneous execution interface stored by this registry.
    type Implementation: WorkImplementation;

    /// Return the exact snapshot identity bound during planning.
    fn registry_id(&self) -> ImplementationRegistryId;

    /// Resolve one implementation without substituting another candidate.
    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation>;
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
    /// Cancel pending work and drain all launched work and fences.
    Cancel,
    /// Apply one exact pre-authorized transition at its declared quiescence point.
    Adapt(AdaptationId),
}

/// Scheduling policy consulted by the sole validated [`run`] seam.
pub trait RunController {
    /// Return the next request. The scheduler rejects any unlisted transition
    /// or adaptation outside its exact global quiescence boundary.
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
        let Some(planned) = claims.get(&key) else {
            return Err(ExecutionEvidenceError::UnplannedResource {
                node: node.clone(),
                resource: key.0,
                lifetime: key.1,
            });
        };
        if measurement.peak() > *planned {
            return Err(ExecutionEvidenceError::ResourcePeakExceeded {
                node: node.clone(),
                resource: key.0,
                planned: *planned,
                actual: measurement.peak(),
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
    let mut measured_artifacts = BTreeMap::new();
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
        if (planned.role() == ArtifactRole::Output)
            != (disposition == ArtifactDisposition::Published)
        {
            return Err(ExecutionEvidenceError::ArtifactDispositionMismatch {
                node: node.clone(),
                artifact,
                role: planned.role(),
                disposition,
            });
        }
    }
    if let Some(artifact) = planned_artifacts
        .keys()
        .find(|artifact| !measured_artifacts.contains_key(artifact))
    {
        return Err(ExecutionEvidenceError::MissingArtifact {
            node: node.clone(),
            artifact: *artifact,
        });
    }
    Ok(())
}

/// Persist the bound plan before execution, drive its complete DAG to
/// settlement, and atomically publish typed terminal evidence before returning.
pub fn run<R, C>(
    problem: &CompiledProblem,
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
    let mut receipt = receipt.begin(problem, plan).map_err(RunError::Receipt)?;
    let result = run_inner(
        problem,
        plan,
        current,
        registry,
        authority,
        controller,
        &mut receipt,
    );
    let status = match &result {
        Ok(ExecutionOutcome::Succeeded) => ReceiptStatus::Completed,
        Ok(ExecutionOutcome::Cancelled) => ReceiptStatus::Cancelled,
        Err(RunError::BindingMismatch { .. }) => ReceiptStatus::Mutation,
        Err(RunError::Scheduler(ExecutionError::Resource(
            crate::ResourceError::Infeasible { .. } | crate::ResourceError::NoCapableAlternative,
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
        RunError::Execution { node, .. } => {
            ReceiptFailure::new(ReceiptFailureKind::Adapter, Some(node.clone()), None)
        }
        RunError::Scheduler(ExecutionError::Resource(
            error @ (crate::ResourceError::Infeasible { .. }
            | crate::ResourceError::NoCapableAlternative),
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
    let mut scheduler = ExecutionScheduler::start(plan, authority).map_err(RunError::Scheduler)?;
    let mut launched = BTreeMap::<WorkNodeId, WorkExecutionContext>::new();
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
                match implementation.execute(problem, &work) {
                    Ok(measurements) => {
                        match validate_work_measurements(plan, &work, &measurements) {
                            Ok(()) => {
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
                                launched.insert(node_id.clone(), work);
                                if let Err(error) =
                                    scheduler.finish_work(node_id, WorkResult::Succeeded)
                                {
                                    defer_scheduler_error(&mut scheduler, &mut pending, error);
                                    controller_stopped = true;
                                }
                            }
                            Err(error) => {
                                if pending.is_none() {
                                    pending = Some(PendingRunError::Evidence(error));
                                }
                                controller_stopped = true;
                                let _ = receipt.fences_launched(&node_id);
                                let _ = receipt.work_failed(&node_id);
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
                        let _ = receipt.work_failed(&node_id);
                        if work.node().kind == WorkKind::Release {
                            if pending.is_none() {
                                pending = Some(PendingRunError::Execution {
                                    node: node_id.clone(),
                                    source,
                                });
                            }
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
                        let diagnostic = source.to_string();
                        pending = Some(PendingRunError::Execution {
                            node: node_id.clone(),
                            source,
                        });
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
                if let Err(source) =
                    implementation.wait_for_fence(problem, &fence_work, fence.kind())
                {
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
                    if let Err(error) = receipt.fence_completed(&fence) {
                        defer_receipt_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                    }
                    if let Err(error) = scheduler.complete_fence(fence) {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
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
    encoder.digest(plan.resource_policy_id.as_bytes());
    encoder.digest(plan.planner_cost_model_profile.as_bytes());
    encoder.digest(plan.execution_dag.physical_work_id().as_bytes());
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
    ExecutionPlanId(encoder.finish())
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
