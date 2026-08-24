// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use casa_imaging_model::{
    AntennaSelection, CompiledGeometry, CompiledProblem, CorrelationType, DelayCentreLaw,
    DirectionFrame, DopplerConvention, FiniteValuePolicy, FlagPolicy, FrequencyFrame, IdSelection,
    ImageAxis, ImageDomainRole, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementSetIdentity, MetadataTableKind, MissingPointingPolicy, ModelBounds,
    ModelInnerProduct, ModelInputCommitment, ModelInputCommitmentIdentity, ModelReprojectionPolicy,
    ModelStateEncoding, ModelStateIdentity, ModelSupportSemantics, MsColumnKind,
    NormalEquationForm, NormalStateNormalization, NumericPrecision, NumericalStage,
    PairedMeasurementTransform, PairedTransformKind, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationCoordinate, ProblemInputIdentities,
    ProductAxisKind, ProductBeamRule, ProductBlankingPolicy, ProductBoundaryOperation,
    ProductGraphId, ProductKind, ProductNormalization, ProductRole, ProductSchema,
    ProductSupportComparison, ProductTerm, ProductUnit, ProductValidityRule, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReductionPolicy, ReferenceDataKind,
    RequiredCapability, RestFrequency, RestoringBeamPolicy, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, TaylorSupportReference, TimeScale, TimeSelection,
    UvDistanceUnit, UvSelection, UvwAxes, UvwUnit, VisibilityColumn, VisibilityInnerProduct,
    VisibilityPhaseConvention, WeightColumn, WeightDensityScope, WeightingScheme,
    validate_compiled_problem_identity, validate_model_lifecycle_contract_identity,
    validate_model_reprojection_contract_identity,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, validate_reprojected_seed_proof_identity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::{
    AdaptationId, AdaptationTransition, AllocationId, AllocationPurpose, ArtifactDisposition,
    ArtifactIdentity, ArtifactMeasurement, ArtifactRole, CacheIdentity, CapabilityId,
    ClaimLifetime, DemandAlternative, ExecutionKnobs, ExecutionPlan, FenceId, FenceKind,
    InitializationPolicy, IoBufferKind, LeaseResource, PhysicalLayoutId, PhysicalSlotId,
    PublicationParticipant, PublicationResourceBounds, QuiescencePoint, ResourceIdentity,
    ResourcePolicy, StorageMode, WorkDependency, WorkDomain, WorkImplementationId, WorkKind,
    WorkMeasurements, WorkNodeId,
};

const RECEIPT_SCHEMA: &str = "casa-rs-imaging-execution-receipt";
const RECEIPT_SCHEMA_VERSION: u32 = 13;
const COMPILED_PROBLEM_EVIDENCE_VERSION: u32 = 7;
const RECEIPT_SUFFIX: &str = ".receipt.json";
const RECEIPT_STAGING_PREFIX: &str = ".casa-rs-receipt-staging-";
const RECEIPT_STAGING_SUFFIX: &str = ".tmp";
const MAX_FAILURE_SUBJECT_BYTES: usize = 128;

macro_rules! receipt_identity {
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

receipt_identity!(
    ExecutionAttemptId,
    "Stable caller-owned identity of one planning and execution attempt."
);
receipt_identity!(
    BuildIdentity,
    "Stable content identity of the executable build used for an attempt."
);

/// Whole-run owner selected by the authoritative pre-plan migration router.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionRouteDisposition {
    /// Every required migration row is native.
    Native,
    /// At least one required row remains behind the sole legacy whole-run port.
    LegacyWholeRun,
    /// At least one required row has no production implementation.
    TemporarilyUnavailable,
}

/// Kind of authoritative migration-matrix row retained by a receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionRouteRequirementKind {
    /// Scientific or operational capability.
    Capability,
    /// Published scientific product.
    Product,
    /// Reconstruction solver.
    Solver,
    /// User-facing request projection.
    Frontend,
    /// Physical implementation family.
    Backend,
}

/// One exact migration row and its disposition at routing time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRouteRequirement {
    id: String,
    kind: ExecutionRouteRequirementKind,
    disposition: ExecutionRouteDisposition,
    evidence: ExecutionRouteRequirementEvidence,
}

/// Lossless authoritative migration evidence carried by one routed row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRouteRequirementEvidence {
    /// Sole current implementation owner.
    pub current_owner: String,
    /// Accepted transfer tickets.
    pub destination_tickets: Vec<String>,
    /// Authoritative issue evidence.
    pub evidence_issues: Vec<u64>,
    /// Content-pinned baseline manifests.
    pub baseline_manifests: Vec<String>,
    /// Versioned Acceptance Contract identifier.
    pub acceptance_contract: String,
    /// Exact transfer milestone.
    pub transfer_point: String,
    /// Same-merge deletion or quarantine condition.
    pub deletion_condition: String,
    /// Repository source locators supporting the current status.
    pub source_evidence: Vec<String>,
    /// Owning transfer ticket for a non-native row.
    pub obligation_ticket: Option<String>,
    /// Reason a non-native row remains open.
    pub obligation_reason: Option<String>,
}

impl ExecutionRouteRequirement {
    /// Create one stable route row projection.
    pub fn new(
        id: impl Into<String>,
        kind: ExecutionRouteRequirementKind,
        disposition: ExecutionRouteDisposition,
        evidence: ExecutionRouteRequirementEvidence,
    ) -> Result<Self, ReceiptError> {
        let id = id.into();
        if id.is_empty()
            || !id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
        {
            return Err(ReceiptError::InvalidRouteEvidence);
        }
        let required_text = [
            evidence.current_owner.as_str(),
            evidence.acceptance_contract.as_str(),
            evidence.transfer_point.as_str(),
            evidence.deletion_condition.as_str(),
        ];
        let has_complete_evidence = required_text.iter().all(|value| !value.trim().is_empty())
            && !evidence.destination_tickets.is_empty()
            && evidence
                .destination_tickets
                .iter()
                .all(|value| !value.trim().is_empty())
            && !evidence.evidence_issues.is_empty()
            && evidence.evidence_issues.iter().all(|issue| *issue > 0)
            && !evidence.baseline_manifests.is_empty()
            && evidence
                .baseline_manifests
                .iter()
                .all(|value| !value.trim().is_empty())
            && !evidence.source_evidence.is_empty()
            && evidence
                .source_evidence
                .iter()
                .all(|value| !value.trim().is_empty());
        let obligation_is_valid = match (
            disposition,
            evidence.obligation_ticket.as_deref(),
            evidence.obligation_reason.as_deref(),
        ) {
            (ExecutionRouteDisposition::Native, None, None) => true,
            (
                ExecutionRouteDisposition::LegacyWholeRun
                | ExecutionRouteDisposition::TemporarilyUnavailable,
                Some(ticket),
                Some(reason),
            ) => !ticket.trim().is_empty() && !reason.trim().is_empty(),
            _ => false,
        };
        if !has_complete_evidence || !obligation_is_valid {
            return Err(ReceiptError::InvalidRouteEvidence);
        }
        Ok(Self {
            id,
            kind,
            disposition,
            evidence,
        })
    }

    /// Return the canonical migration-row identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the migration-row kind.
    #[must_use]
    pub const fn kind(&self) -> ExecutionRouteRequirementKind {
        self.kind
    }

    /// Return the row's routed disposition.
    #[must_use]
    pub const fn disposition(&self) -> ExecutionRouteDisposition {
        self.disposition
    }

    /// Return the full authoritative migration evidence without truncation.
    #[must_use]
    pub const fn evidence(&self) -> &ExecutionRouteRequirementEvidence {
        &self.evidence
    }
}

/// Exact pre-plan routing evidence bound into every execution receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRouteEvidence {
    matrix_schema_version: u32,
    matrix_contract_revision: u32,
    disposition: ExecutionRouteDisposition,
    requirements: Vec<ExecutionRouteRequirement>,
}

impl ExecutionRouteEvidence {
    /// Bind one canonical migration-matrix decision and its complete row evidence.
    pub fn new(
        matrix_schema_version: u32,
        matrix_contract_revision: u32,
        disposition: ExecutionRouteDisposition,
        requirements: Vec<ExecutionRouteRequirement>,
    ) -> Result<Self, ReceiptError> {
        if matrix_schema_version == 0 || matrix_contract_revision == 0 || requirements.is_empty() {
            return Err(ReceiptError::InvalidRouteEvidence);
        }
        if requirements
            .windows(2)
            .any(|rows| rows[0].id() >= rows[1].id())
        {
            return Err(ReceiptError::InvalidRouteEvidence);
        }
        if disposition != route_disposition_from_requirements(&requirements) {
            return Err(ReceiptError::InvalidRouteEvidence);
        }
        Ok(Self {
            matrix_schema_version,
            matrix_contract_revision,
            disposition,
            requirements,
        })
    }

    /// Return the migration-matrix schema version.
    #[must_use]
    pub const fn matrix_schema_version(&self) -> u32 {
        self.matrix_schema_version
    }

    /// Return the migration-matrix contract revision.
    #[must_use]
    pub const fn matrix_contract_revision(&self) -> u32 {
        self.matrix_contract_revision
    }

    /// Return the selected whole-run owner.
    #[must_use]
    pub const fn disposition(&self) -> ExecutionRouteDisposition {
        self.disposition
    }

    /// Return every canonical route requirement.
    #[must_use]
    pub fn requirements(&self) -> &[ExecutionRouteRequirement] {
        &self.requirements
    }
}

fn route_disposition_from_requirements(
    requirements: &[ExecutionRouteRequirement],
) -> ExecutionRouteDisposition {
    requirements.iter().fold(
        ExecutionRouteDisposition::Native,
        |disposition, requirement| match (disposition, requirement.disposition()) {
            (_, ExecutionRouteDisposition::TemporarilyUnavailable)
            | (
                ExecutionRouteDisposition::TemporarilyUnavailable,
                ExecutionRouteDisposition::LegacyWholeRun | ExecutionRouteDisposition::Native,
            ) => ExecutionRouteDisposition::TemporarilyUnavailable,
            (_, ExecutionRouteDisposition::LegacyWholeRun) => {
                ExecutionRouteDisposition::LegacyWholeRun
            }
            (current, ExecutionRouteDisposition::Native) => current,
        },
    )
}

/// Provenance supplied at the sole execution seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionProvenance {
    attempt: ExecutionAttemptId,
    build: BuildIdentity,
    route: ExecutionRouteEvidence,
}

impl ExecutionProvenance {
    /// Bind one unique attempt to the exact executable build.
    #[must_use]
    pub const fn new(
        attempt: ExecutionAttemptId,
        build: BuildIdentity,
        route: ExecutionRouteEvidence,
    ) -> Self {
        Self {
            attempt,
            build,
            route,
        }
    }

    /// Return the attempt identity used to reopen its receipt.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.attempt
    }

    /// Return the exact executable build identity.
    #[must_use]
    pub const fn build_identity(&self) -> BuildIdentity {
        self.build
    }

    /// Return the exact pre-plan routing evidence for this attempt.
    #[must_use]
    pub const fn route(&self) -> &ExecutionRouteEvidence {
        &self.route
    }
}

/// Hard local-retention ceilings for one receipt store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiptRetention {
    max_receipts: usize,
    max_bytes: u64,
}

impl ReceiptRetention {
    /// Create positive count and byte ceilings.
    pub fn new(max_receipts: usize, max_bytes: u64) -> Result<Self, ReceiptError> {
        if max_receipts == 0 || max_bytes == 0 {
            return Err(ReceiptError::InvalidRetention);
        }
        Ok(Self {
            max_receipts,
            max_bytes,
        })
    }
}

/// Durable overall or per-node completion state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    /// The plan item is declared but has not started.
    Planned,
    /// The attempt or work item is active.
    Running,
    /// All work is settled and durable prepared evidence exists, but external
    /// publication visibility is indeterminate until a terminal receipt exists.
    PublicationPrepared,
    /// A terminal attempt ended before this plan item could start.
    NotStarted,
    /// The attempt or work item completed successfully.
    Completed,
    /// Execution failed after a plan existed.
    Failed,
    /// Explicit cancellation prevented remaining work from starting.
    Cancelled,
    /// Execution stopped without a normal terminal return.
    Aborted,
    /// A bound input changed before execution could begin.
    Mutation,
    /// The selected plan could not be admitted by the Resource Authority.
    Infeasible,
}

impl ReceiptStatus {
    fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Planned | Self::Running | Self::PublicationPrepared
        )
    }
}

/// Stable typed cause retained for a non-successful execution attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiptFailureKind {
    /// A current binding no longer matched the sealed plan.
    BindingMutation,
    /// Resource Authority admission proved the selected alternative infeasible.
    ResourceInfeasible,
    /// A selected implementation was absent from the bound registry snapshot.
    ImplementationUnavailable,
    /// A registry adapter reported a different identity than the selected one.
    ImplementationMismatch,
    /// Adapter evidence was missing, duplicated, unlisted, or above a hard claim.
    EvidenceContract,
    /// Deterministic scheduling failed after admission.
    Scheduler,
    /// A plan-selected adapter or asynchronous fence failed.
    Adapter,
    /// Execution unwound or otherwise ended without its normal terminal path.
    Interrupted,
}

/// Machine-readable Resource Authority proof retained for an infeasible run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptInfeasibilityCertificate {
    /// No declared alternative supported every required capability.
    NoCapableAlternative,
    /// One exact mandatory resource exceeded its available capacity.
    Infeasible {
        /// Stable resource category reported by the Resource Authority.
        resource: String,
        /// Exact typed Resource Authority identity retained independently of
        /// the bounded/redacted display string.
        resource_identity: ResourceIdentity,
        /// Mandatory requested amount.
        required: u64,
        /// Available amount after policy, pressure, and active reservations.
        available: u64,
    },
    /// Planning refused the alternative because a prior execution of it
    /// terminally failed or was aborted; recorded evidence constrains the
    /// infeasible region without entering the cost model.
    RecordedFailure {
        /// Attempt whose terminal receipt recorded the failure.
        attempt: String,
        /// Terminal status retained by that receipt.
        status: ReceiptStatus,
    },
}

/// One plan-authorized adaptation and its durable application evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptAdaptation {
    transition: AdaptationTransition,
    applied_revision: Option<u64>,
}

impl ReceiptAdaptation {
    /// Return the exact execution-only transition projected from the plan.
    #[must_use]
    pub const fn transition(&self) -> &AdaptationTransition {
        &self.transition
    }

    /// Return whether execution applied this listed transition.
    #[must_use]
    pub const fn was_applied(&self) -> bool {
        self.applied_revision.is_some()
    }

    /// Return the receipt revision that first recorded the transition.
    #[must_use]
    pub const fn applied_revision(&self) -> Option<u64> {
        self.applied_revision
    }
}

/// A versioned, validated projection reopened from durable local evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReceipt {
    schema_version: u32,
    body: ReceiptBody,
}

/// Audit-only semantic identity of one coordinated publication member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiptPublicationParticipant {
    /// One compiler-owned product graph node, represented by its graph-local ordinal.
    Product {
        /// SHA-256 identity of the compiler-owned topology containing the node.
        graph_identity: [u8; 32],
        /// Zero-based identity within the compiler-owned Product Graph.
        node_ordinal: usize,
    },
    /// The optional `MODEL_DATA` member for one MeasurementSet.
    ModelData(MeasurementSetIdentity),
}

/// Closed audit projection of the compiler-owned Product Graph.
///
/// Publication validation consumes this typed projection directly. The open
/// Compiled Problem field map remains descriptive audit evidence and is never
/// reconstructed into publication authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProductGraphProjection {
    identity: String,
    schema_version: u32,
    node_ordinals: Vec<usize>,
    publication_member_ordinals: Vec<usize>,
}

impl ProductGraphProjection {
    const AUDIT_NODE_PREFIX: &'static str = "products.graph.nodes.";
    const AUDIT_PUBLICATION_MEMBER_PREFIX: &'static str = "products.graph.publication.members.";

    fn new(problem: &CompiledProblem) -> Self {
        let graph = problem.product_graph();
        Self {
            identity: hex(&graph.graph_id().as_bytes()),
            schema_version: graph.schema_version(),
            node_ordinals: graph
                .nodes()
                .iter()
                .map(|node| node.node_id().ordinal())
                .collect(),
            publication_member_ordinals: graph
                .publication()
                .members()
                .iter()
                .map(|member| member.ordinal())
                .collect(),
        }
    }

    fn validate(&self) -> Result<(), ReceiptError> {
        require_integrity(
            is_digest(&self.identity)
                && self.schema_version == ProductGraphId::SCHEMA_VERSION
                && self
                    .node_ordinals
                    .iter()
                    .copied()
                    .eq(0..self.node_ordinals.len())
                && self
                    .publication_member_ordinals
                    .windows(2)
                    .all(|pair| pair[0] < pair[1])
                && self
                    .publication_member_ordinals
                    .iter()
                    .all(|member| self.node_ordinals.binary_search(member).is_ok()),
        )
    }

    fn validate_audit_fields(
        &self,
        evidence: &CompiledProblemEvidence,
    ) -> Result<(), ReceiptError> {
        let node_ordinal_fields = evidence
            .fields
            .keys()
            .filter(|path| {
                path.strip_prefix(Self::AUDIT_NODE_PREFIX)
                    .and_then(|suffix| suffix.strip_suffix(".ordinal"))
                    .is_some()
            })
            .count();
        require_integrity(node_ordinal_fields == self.node_ordinals.len())?;
        for ordinal in &self.node_ordinals {
            let path = format!("{}{ordinal}.ordinal", Self::AUDIT_NODE_PREFIX);
            require_integrity(evidence.field(&path) == Some(ordinal.to_string().as_str()))?;
        }

        let publication_member_fields = evidence
            .fields
            .keys()
            .filter(|path| {
                path.strip_prefix(Self::AUDIT_PUBLICATION_MEMBER_PREFIX)
                    .is_some()
            })
            .count();
        require_integrity(publication_member_fields == self.publication_member_ordinals.len())?;
        for (index, ordinal) in self.publication_member_ordinals.iter().enumerate() {
            let path = format!("{}{index}", Self::AUDIT_PUBLICATION_MEMBER_PREFIX);
            require_integrity(evidence.field(&path) == Some(ordinal.to_string().as_str()))?;
        }
        Ok(())
    }
}

/// Stable, versioned field projection of one effective Compiled Problem.
///
/// This is audit evidence only. It deliberately cannot be converted back into
/// a [`CompiledProblem`] and must not be used as planning or science authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledProblemEvidence {
    schema_version: u32,
    fields: BTreeMap<String, String>,
}

impl CompiledProblemEvidence {
    /// Project every stable field of an immutable Compiled Problem for audit comparison.
    #[must_use]
    pub fn project(problem: &CompiledProblem) -> Self {
        Self {
            schema_version: COMPILED_PROBLEM_EVIDENCE_VERSION,
            fields: project_problem_fields(problem),
        }
    }

    /// Return the field-projection schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Return one stable projected value by its versioned field path.
    #[must_use]
    pub fn field(&self, path: &str) -> Option<&str> {
        self.fields.get(path).map(String::as_str)
    }

    /// Return the complete canonical field map.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<String, String> {
        &self.fields
    }
}

impl ExecutionReceipt {
    /// Return the stable receipt schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Return the caller-owned identity of this execution attempt.
    #[must_use]
    pub fn attempt_id(&self) -> ExecutionAttemptId {
        self.body.attempt()
    }

    /// Return the exact executable-build identity recorded for this attempt.
    #[must_use]
    pub fn build_identity(&self) -> BuildIdentity {
        BuildIdentity::from_sha256(parse_digest(&self.body.build_identity))
    }

    /// Return the migration-matrix schema version used to route this attempt.
    #[must_use]
    pub const fn route_matrix_schema_version(&self) -> u32 {
        self.body.route.matrix_schema_version
    }

    /// Return the migration-matrix contract revision used to route this attempt.
    #[must_use]
    pub const fn route_matrix_contract_revision(&self) -> u32 {
        self.body.route.matrix_contract_revision
    }

    /// Return the stable routed whole-run disposition.
    #[must_use]
    pub fn route_disposition(&self) -> &str {
        &self.body.route.disposition
    }

    /// Return the canonical routed row identities in receipt order.
    #[must_use]
    pub fn route_requirement_identities(&self) -> Vec<&str> {
        self.body
            .route
            .requirements
            .iter()
            .map(|requirement| requirement.id.as_str())
            .collect()
    }

    /// Reconstruct one complete typed authoritative routed row.
    #[must_use]
    pub fn route_requirement(&self, id: &str) -> Option<ExecutionRouteRequirement> {
        let requirement = self
            .body
            .route
            .requirements
            .iter()
            .find(|requirement| requirement.id == id)?;
        ExecutionRouteRequirement::new(
            requirement.id.clone(),
            execution_route_requirement_kind(&requirement.kind)?,
            execution_route_disposition(&requirement.disposition)?,
            ExecutionRouteRequirementEvidence {
                current_owner: requirement.current_owner.clone(),
                destination_tickets: requirement.destination_tickets.clone(),
                evidence_issues: requirement.evidence_issues.clone(),
                baseline_manifests: requirement.baseline_manifests.clone(),
                acceptance_contract: requirement.acceptance_contract.clone(),
                transfer_point: requirement.transfer_point.clone(),
                deletion_condition: requirement.deletion_condition.clone(),
                source_evidence: requirement.source_evidence.clone(),
                obligation_ticket: requirement.obligation_ticket.clone(),
                obligation_reason: requirement.obligation_reason.clone(),
            },
        )
        .ok()
    }

    /// Return the typed terminal or active status.
    #[must_use]
    pub const fn status(&self) -> ReceiptStatus {
        self.body.status
    }

    /// Return the complete audit-only projection of the effective Compiled Problem.
    #[must_use]
    pub const fn compiled_problem_evidence(&self) -> &CompiledProblemEvidence {
        &self.body.problem.effective
    }

    /// Return the typed failure class, when this attempt failed or aborted.
    #[must_use]
    pub fn failure_kind(&self) -> Option<ReceiptFailureKind> {
        self.body
            .failure
            .as_ref()
            .map(|failure| failure.kind.into())
    }

    /// Return the exact affected plan node when the failure is node-local.
    #[must_use]
    pub fn failure_node(&self) -> Option<WorkNodeId> {
        self.body
            .failure
            .as_ref()?
            .node_id
            .as_ref()
            .map(|node| WorkNodeId::new(node.clone()))
    }

    /// Return the typed Resource Authority infeasibility proof, when present.
    #[must_use]
    pub fn infeasibility_certificate(&self) -> Option<ReceiptInfeasibilityCertificate> {
        self.body
            .failure
            .as_ref()?
            .infeasibility
            .as_ref()
            .map(InfeasibilityProjection::to_runtime)
    }

    /// Return the canonical execution-plan identity.
    #[must_use]
    pub fn plan_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.plan.plan_identity)
    }

    /// Return the canonical compiled-problem identity.
    #[must_use]
    pub fn problem_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.problem.problem_identity)
    }

    /// Return the compiler-derived geometry identity.
    #[must_use]
    pub fn geometry_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.problem.geometry_identity)
    }

    /// Return the compiler-derived product-topology identity.
    #[must_use]
    pub fn product_graph_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.problem.product_graph.identity)
    }

    /// Return the compiler-owned Product Graph schema version.
    #[must_use]
    pub const fn product_graph_schema_version(&self) -> u32 {
        self.body.problem.product_graph.schema_version
    }

    /// Return every graph-local product node ordinal in canonical order.
    #[must_use]
    pub fn product_graph_node_ordinals(&self) -> &[usize] {
        &self.body.problem.product_graph.node_ordinals
    }

    /// Return the exact atomic-publication member ordinals in canonical order.
    #[must_use]
    pub fn product_graph_publication_member_ordinals(&self) -> &[usize] {
        &self.body.problem.product_graph.publication_member_ordinals
    }

    /// Return the observation-snapshot identity.
    #[must_use]
    pub fn observation_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.problem.observation_identity)
    }

    /// Return one compiler-bound reference-data identity, when present.
    #[must_use]
    pub fn reference_identity(&self, kind: ReferenceDataKind) -> Option<[u8; 32]> {
        let kind = reference_kind(kind);
        self.body
            .problem
            .reference_identities
            .iter()
            .find(|reference| reference.kind == kind)
            .map(|reference| parse_digest(&reference.identity))
    }

    /// Return the exact initial-model identity projected from the compiled problem.
    #[must_use]
    pub fn model_identity(&self) -> ModelStateIdentity {
        self.body.problem.model_identity.to_runtime()
    }

    /// Return the exact Numerics Contract identity.
    #[must_use]
    pub fn numerics_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.problem.numerics_identity)
    }

    /// Return the exact compiler-bound model-lifecycle commitment identity.
    #[must_use]
    pub fn model_lifecycle_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.problem.model_lifecycle.identity)
    }

    /// Return the bound implementation-registry identity.
    #[must_use]
    pub fn implementation_registry_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.plan.implementation_registry_identity)
    }

    /// Return the bound Resource Policy identity.
    #[must_use]
    pub fn resource_policy_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.plan.resource_policy_identity)
    }

    /// Return the bound reviewed cost-model profile identity.
    #[must_use]
    pub fn cost_model_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.plan.cost_model_identity)
    }

    /// Return the complete physical DAG identity.
    #[must_use]
    pub fn dag_identity(&self) -> [u8; 32] {
        parse_digest(&self.body.plan.dag_identity)
    }

    /// Return the projected host-use policy as audit evidence.
    ///
    /// This decoded copy is not execution authority; only the immutable plan
    /// and Resource Authority may govern a run.
    #[must_use]
    pub fn projected_resource_policy(&self) -> ResourcePolicy {
        self.body.plan.resource_policy.to_runtime()
    }

    /// Return the complete selected demand alternative as audit evidence.
    ///
    /// Path-shaped identifiers remain irreversibly redacted. The returned copy
    /// must not be used to re-admit or execute work; the immutable plan remains
    /// the sole execution authority.
    #[must_use]
    pub fn selected_alternative_projection(&self) -> DemandAlternative {
        self.body.plan.selected_alternative.to_runtime()
    }

    /// Return all plan-required resource-capability identities.
    #[must_use]
    pub fn required_resource_capability_identities(&self) -> BTreeSet<CapabilityId> {
        self.body
            .plan
            .required_resource_capabilities
            .iter()
            .cloned()
            .map(CapabilityId::new)
            .collect()
    }

    /// Return every implementation identity selected by the plan.
    #[must_use]
    pub fn selected_implementation_identities(&self) -> BTreeSet<WorkImplementationId> {
        self.body
            .plan
            .selected_implementations
            .iter()
            .cloned()
            .map(WorkImplementationId::new)
            .collect()
    }

    /// Return every receipted work-node identity.
    #[must_use]
    pub fn plan_node_identities(&self) -> BTreeSet<WorkNodeId> {
        self.body
            .plan
            .nodes
            .iter()
            .map(|node| WorkNodeId::new(node.node_id.clone()))
            .collect()
    }

    /// Return every logical allocation-generation identity.
    #[must_use]
    pub fn allocation_generation_identities(&self) -> BTreeSet<AllocationId> {
        self.body
            .plan
            .allocation_generations
            .iter()
            .map(|allocation| AllocationId::new(allocation.generation_identity.clone()))
            .collect()
    }

    /// Return every exact allocation generation and access lifetime used by one plan node.
    #[must_use]
    pub fn allocation_uses(
        &self,
        node: &WorkNodeId,
    ) -> Option<BTreeMap<AllocationId, ClaimLifetime>> {
        Some(
            self.node(node)?
                .allocation_uses
                .iter()
                .map(|usage| {
                    (
                        AllocationId::new(usage.generation_identity.clone()),
                        parse_claim_lifetime(&usage.lifetime),
                    )
                })
                .collect(),
        )
    }

    /// Return every plan-owned physical-slot identity.
    #[must_use]
    pub fn physical_slot_identities(&self) -> BTreeSet<PhysicalSlotId> {
        self.body
            .plan
            .physical_slots
            .iter()
            .map(|slot| PhysicalSlotId::new(slot.slot_identity.clone()))
            .collect()
    }

    /// Return every plan-listed artifact identity.
    #[must_use]
    pub fn artifact_identities(&self) -> BTreeSet<ArtifactIdentity> {
        self.body
            .plan
            .artifacts
            .iter()
            .map(|artifact| {
                ArtifactIdentity::from_sha256(parse_digest(&artifact.artifact_identity))
            })
            .collect()
    }

    /// Return the number of exact coordinated publication layouts.
    #[must_use]
    pub fn publication_layout_count(&self) -> usize {
        self.body.plan.publication_layouts.len()
    }

    /// Return the audit-only semantic member bound to one output artifact.
    #[must_use]
    pub fn publication_participant(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<ReceiptPublicationParticipant> {
        Some(self.publication_layout(artifact)?.participant.to_runtime())
    }

    /// Return the exact adapter-selected physical layout identity.
    #[must_use]
    pub fn publication_layout_identity(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<PhysicalLayoutId> {
        Some(PhysicalLayoutId::from_sha256(parse_digest(
            &self.publication_layout(artifact)?.layout_identity,
        )))
    }

    /// Return the exact private-staging producer.
    #[must_use]
    pub fn publication_producer(&self, artifact: ArtifactIdentity) -> Option<WorkNodeId> {
        Some(WorkNodeId::new(
            self.publication_layout(artifact)?.producer.clone(),
        ))
    }

    /// Return the producer completion event required before publication.
    #[must_use]
    pub fn publication_terminal(&self, artifact: ArtifactIdentity) -> Option<WorkDependency> {
        Some(parse_dependency(
            &self.publication_layout(artifact)?.terminal,
        ))
    }

    /// Return the selected private writer-buffer category.
    #[must_use]
    pub fn publication_writer_buffer_kind(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<IoBufferKind> {
        Some(parse_io_buffer(
            &self.publication_layout(artifact)?.writer_buffer_kind,
        ))
    }

    /// Return the producer-owned writer allocation.
    #[must_use]
    pub fn publication_writer_allocation(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<AllocationId> {
        Some(AllocationId::new(
            self.publication_layout(artifact)?.writer_allocation.clone(),
        ))
    }

    /// Return exact staged, final, writer, and mapped resource bounds.
    #[must_use]
    pub fn publication_resource_bounds(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<PublicationResourceBounds> {
        let bounds = &self.publication_layout(artifact)?.resource_bounds;
        PublicationResourceBounds::new(
            bounds.staged_storage_bytes,
            bounds.final_storage_bytes,
            bounds.writer_buffer_bytes,
            bounds.mapped_page_cache_bytes,
        )
        .ok()
    }

    /// Return the mapped/page-cache producer, when the layout retains mapped exposure.
    #[must_use]
    pub fn publication_mapped_producer(&self, artifact: ArtifactIdentity) -> Option<WorkNodeId> {
        Some(WorkNodeId::new(
            self.publication_layout(artifact)?
                .mapped_page_cache
                .as_ref()?
                .producer
                .clone(),
        ))
    }

    /// Return the mapped/page-cache release event, when present.
    #[must_use]
    pub fn publication_mapped_terminal(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<WorkDependency> {
        Some(parse_dependency(
            &self
                .publication_layout(artifact)?
                .mapped_page_cache
                .as_ref()?
                .terminal,
        ))
    }

    /// Return the mapped/page-cache allocation, when present.
    #[must_use]
    pub fn publication_mapped_allocation(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<AllocationId> {
        Some(AllocationId::new(
            self.publication_layout(artifact)?
                .mapped_page_cache
                .as_ref()?
                .allocation
                .clone(),
        ))
    }

    /// Return every plan-listed cache namespace identity.
    #[must_use]
    pub fn cache_identities(&self) -> BTreeSet<CacheIdentity> {
        self.body
            .plan
            .artifacts
            .iter()
            .filter_map(|artifact| artifact.cache_identity.as_deref())
            .map(|identity| CacheIdentity::from_sha256(parse_digest(identity)))
            .collect()
    }

    /// Return the plan's initial execution-only controls.
    #[must_use]
    pub fn initial_execution_knobs(&self) -> ExecutionKnobs {
        self.body.plan.initial_execution_knobs.to_runtime()
    }

    /// Return every plan-authorized adaptation identity.
    #[must_use]
    pub fn adaptation_identities(&self) -> BTreeSet<AdaptationId> {
        self.body
            .plan
            .adaptations
            .iter()
            .map(|adaptation| AdaptationId::new(adaptation.adaptation_identity.clone()))
            .collect()
    }

    /// Return one authorized adaptation and its application evidence.
    #[must_use]
    pub fn adaptation_projection(&self, id: &AdaptationId) -> Option<ReceiptAdaptation> {
        let identity = stable_text(id.as_str());
        self.body
            .plan
            .adaptations
            .iter()
            .find(|adaptation| adaptation.adaptation_identity == identity)
            .map(AdaptationProjection::to_runtime)
    }

    /// Return the number of plan-projected work nodes.
    #[must_use]
    pub fn plan_node_count(&self) -> usize {
        self.body.plan.nodes.len()
    }

    /// Return one plan node's durable state.
    #[must_use]
    pub fn node_status(&self, node: &WorkNodeId) -> Option<ReceiptStatus> {
        let node = stable_text(node.as_str());
        self.body
            .plan
            .nodes
            .iter()
            .find(|item| item.node_id == node)
            .map(|item| item.status)
    }

    /// Return conservative total predicted elapsed nanoseconds.
    #[must_use]
    pub const fn predicted_elapsed_nanos(&self) -> u64 {
        self.body.plan.prediction.elapsed_nanos
    }

    /// Return prediction confidence in parts per million.
    #[must_use]
    pub const fn prediction_confidence_ppm(&self) -> u32 {
        self.body.plan.prediction.confidence_ppm
    }

    /// Return the number of named prediction uncertainty terms.
    #[must_use]
    pub fn prediction_uncertainty_count(&self) -> usize {
        self.body.plan.prediction.uncertainty.len()
    }

    /// Return predicted elapsed nanoseconds for one exact plan node.
    #[must_use]
    pub fn stage_predicted_elapsed_nanos(&self, node: &WorkNodeId) -> Option<u64> {
        self.node(node).map(|item| item.predicted_elapsed_nanos)
    }

    /// Return measured synchronous elapsed nanoseconds for one dispatched node.
    #[must_use]
    pub fn stage_actual_elapsed_nanos(&self, node: &WorkNodeId) -> Option<u64> {
        self.node(node).and_then(|item| item.actual_elapsed_nanos)
    }

    /// Return predicted bytes and operations for one stage I/O category.
    #[must_use]
    pub fn stage_predicted_io(&self, node: &WorkNodeId, kind: IoBufferKind) -> Option<(u64, u64)> {
        let kind = io_buffer(kind);
        self.node(node)?
            .io
            .iter()
            .find(|item| item.kind == kind)
            .map(|item| (item.predicted_bytes, item.predicted_operations))
    }

    /// Return actual bytes and operations for one stage I/O category.
    #[must_use]
    pub fn stage_actual_io(&self, node: &WorkNodeId, kind: IoBufferKind) -> Option<(u64, u64)> {
        let kind = io_buffer(kind);
        let item = self.node(node)?.io.iter().find(|item| item.kind == kind)?;
        Some((item.actual_bytes?, item.actual_operations?))
    }

    /// Return the plan-declared hard amount for one node resource/lifetime pair.
    #[must_use]
    pub fn planned_resource_amount(
        &self,
        node: &WorkNodeId,
        resource: &LeaseResource,
        lifetime: &ClaimLifetime,
    ) -> Option<u64> {
        let resource = lease_resource(resource);
        let lifetime = claim_lifetime(lifetime);
        self.node(node)?
            .claims
            .iter()
            .find(|claim| claim.resource == resource && claim.lifetime == lifetime)
            .map(|claim| claim.amount)
    }

    /// Return the adapter-reported peak for one admitted resource/lifetime pair.
    ///
    /// A failed node retains the uncensored observed peak, including a value
    /// above the admitted amount that caused an evidence-contract failure.
    #[must_use]
    pub fn actual_resource_peak(
        &self,
        node: &WorkNodeId,
        resource: &LeaseResource,
        lifetime: &ClaimLifetime,
    ) -> Option<u64> {
        let resource = lease_resource(resource);
        let lifetime = claim_lifetime(lifetime);
        self.node(node)?
            .claims
            .iter()
            .find(|claim| claim.resource == resource && claim.lifetime == lifetime)
            .and_then(|claim| claim.actual_peak)
    }

    /// Return one declared asynchronous fence's durable completion state.
    #[must_use]
    pub fn fence_status(&self, fence: &FenceId) -> Option<ReceiptStatus> {
        let node = stable_text(fence.node().as_str());
        let kind = fence_kind(fence.kind());
        self.body
            .plan
            .fences
            .iter()
            .find(|item| item.node_id == node && item.kind == kind)
            .map(|item| item.status)
    }

    /// Return measured elapsed nanoseconds for one declared asynchronous fence.
    #[must_use]
    pub fn fence_actual_elapsed_nanos(&self, fence: &FenceId) -> Option<u64> {
        let node = stable_text(fence.node().as_str());
        let kind = fence_kind(fence.kind());
        self.body
            .plan
            .fences
            .iter()
            .find(|item| item.node_id == node && item.kind == kind)
            .and_then(|item| item.actual_elapsed_nanos)
    }

    /// Return the number of plan-listed artifacts in the durable ledger.
    #[must_use]
    pub fn artifact_count(&self) -> usize {
        self.body.plan.artifacts.len()
    }

    /// Return one plan-listed artifact's actual disposition.
    #[must_use]
    pub fn artifact_disposition(&self, artifact: ArtifactIdentity) -> Option<ArtifactDisposition> {
        self.artifact(artifact)?.disposition.map(Into::into)
    }

    /// Return one plan-listed artifact's role in the output/cache manifest.
    #[must_use]
    pub fn artifact_role(&self, artifact: ArtifactIdentity) -> Option<ArtifactRole> {
        match self.artifact(artifact)?.role.as_str() {
            "input" => Some(ArtifactRole::Input),
            "prepared" => Some(ArtifactRole::Prepared),
            "cache" => Some(ArtifactRole::Cache),
            "output" => Some(ArtifactRole::Output),
            _ => unreachable!("validated receipt artifact role"),
        }
    }

    /// Return the exact plan node owning one artifact.
    #[must_use]
    pub fn artifact_node(&self, artifact: ArtifactIdentity) -> Option<WorkNodeId> {
        Some(WorkNodeId::new(self.artifact(artifact)?.node_id.clone()))
    }

    /// Return the actual artifact size reported by its owning implementation.
    #[must_use]
    pub fn artifact_actual_bytes(&self, artifact: ArtifactIdentity) -> Option<u64> {
        self.artifact(artifact)?.actual_bytes
    }

    /// Return one plan-listed artifact's cache namespace identity.
    #[must_use]
    pub fn artifact_cache_identity(&self, artifact: ArtifactIdentity) -> Option<[u8; 32]> {
        self.artifact(artifact)?
            .cache_identity
            .as_deref()
            .map(parse_digest)
    }

    /// Return the actual observed content or rejection-evidence identity.
    #[must_use]
    pub fn artifact_observed_identity(&self, artifact: ArtifactIdentity) -> Option<[u8; 32]> {
        self.artifact(artifact)?
            .observed_identity
            .as_deref()
            .map(parse_digest)
    }

    /// Return the irreversible local-path identity for one artifact.
    #[must_use]
    pub fn artifact_path_identity(&self, artifact: ArtifactIdentity) -> Option<[u8; 32]> {
        self.artifact(artifact)?
            .path_identity
            .as_deref()
            .map(parse_digest)
    }

    fn node(&self, node: &WorkNodeId) -> Option<&NodeProjection> {
        let node = stable_text(node.as_str());
        self.body
            .plan
            .nodes
            .iter()
            .find(|item| item.node_id == node)
    }

    fn artifact(&self, artifact: ArtifactIdentity) -> Option<&ArtifactProjection> {
        let artifact = hex(&artifact.as_bytes());
        self.body
            .plan
            .artifacts
            .iter()
            .find(|item| item.artifact_identity == artifact)
    }

    fn publication_layout(
        &self,
        artifact: ArtifactIdentity,
    ) -> Option<&PublicationLayoutProjection> {
        let artifact = hex(&artifact.as_bytes());
        self.body
            .plan
            .publication_layouts
            .iter()
            .find(|item| item.artifact_identity == artifact)
    }
}

/// Local, bounded owner of atomic imaging Execution Receipts.
#[derive(Debug)]
pub struct ExecutionReceiptStore {
    root: PathBuf,
    state: Arc<ReceiptRootState>,
}

#[derive(Debug)]
struct ReceiptRootState {
    retention: ReceiptRetention,
    mutation: Mutex<()>,
}

static RECEIPT_ROOT_STATES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<ReceiptRootState>>>> =
    OnceLock::new();

fn receipt_root_state(
    root: &Path,
    retention: ReceiptRetention,
) -> Result<Arc<ReceiptRootState>, ReceiptError> {
    let states = RECEIPT_ROOT_STATES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut states = states.lock().map_err(|_| ReceiptError::InvalidStore)?;
    states.retain(|_, state| state.strong_count() != 0);
    if let Some(state) = states.get(root).and_then(Weak::upgrade) {
        if state.retention != retention {
            return Err(ReceiptError::ConflictingRetention);
        }
        return Ok(state);
    }
    let state = Arc::new(ReceiptRootState {
        retention,
        mutation: Mutex::new(()),
    });
    states.insert(root.to_owned(), Arc::downgrade(&state));
    Ok(state)
}

/// One attempt-scoped durable-evidence binding consumed by the run seam.
#[derive(Clone, Debug)]
pub struct ExecutionReceiptBinding<'store> {
    store: &'store ExecutionReceiptStore,
    provenance: ExecutionProvenance,
}

impl<'store> ExecutionReceiptBinding<'store> {
    pub(crate) fn begin(
        self,
        problem: &ExecutableModelProblem,
        plan: &ExecutionPlan,
    ) -> Result<ReceiptRecorder<'store>, ReceiptError> {
        self.store
            .begin(self.provenance, problem.compiled_problem(), plan)
    }
}

impl ExecutionReceiptStore {
    /// Open or create a local store with explicit hard retention ceilings.
    pub fn new(root: impl AsRef<Path>, retention: ReceiptRetention) -> Result<Self, ReceiptError> {
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root).map_err(|source| ReceiptError::Io {
            action: "create receipt directory",
            source,
        })?;
        if !root.is_dir() {
            return Err(ReceiptError::InvalidStore);
        }
        let root = root.canonicalize().map_err(|source| ReceiptError::Io {
            action: "canonicalize receipt directory",
            source,
        })?;
        let state = receipt_root_state(&root, retention)?;
        let store = Self { root, state };
        store.remove_orphaned_staging_files()?;
        Ok(store)
    }

    /// Bind one caller-owned attempt and build identity to this local store.
    #[must_use]
    pub const fn bind(&self, provenance: ExecutionProvenance) -> ExecutionReceiptBinding<'_> {
        ExecutionReceiptBinding {
            store: self,
            provenance,
        }
    }

    /// Return every stored attempt identity in ascending order.
    ///
    /// Identities come from receipt filenames; reopening still validates each
    /// document's integrity.
    pub(crate) fn attempts(&self) -> Result<Vec<ExecutionAttemptId>, ReceiptError> {
        let mut attempts = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(|source| ReceiptError::Io {
            action: "list execution receipts",
            source,
        })? {
            let name = entry
                .map_err(|source| ReceiptError::Io {
                    action: "read execution receipt entry",
                    source,
                })?
                .file_name();
            let name = name.to_string_lossy();
            let Some(stem) = name.strip_suffix(RECEIPT_SUFFIX) else {
                continue;
            };
            if stem.len() == 64 && stem.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                attempts.push(ExecutionAttemptId::from_sha256(parse_digest(stem)));
            }
        }
        attempts.sort();
        Ok(attempts)
    }

    /// Reopen and integrity-check one receipt by its caller-owned attempt identity.
    pub fn open(&self, attempt: ExecutionAttemptId) -> Result<ExecutionReceipt, ReceiptError> {
        let path = self.receipt_path(attempt);
        let bytes = fs::read(&path).map_err(|source| ReceiptError::Io {
            action: "read execution receipt",
            source,
        })?;
        let document = decode_document(&bytes)?;
        if document.receipt.attempt_identity != attempt.to_string() {
            return Err(ReceiptError::AttemptMismatch);
        }
        Ok(ExecutionReceipt {
            schema_version: document.schema.version,
            body: document.receipt,
        })
    }

    pub(crate) fn begin<'store>(
        &'store self,
        provenance: ExecutionProvenance,
        problem: &CompiledProblem,
        plan: &ExecutionPlan,
    ) -> Result<ReceiptRecorder<'store>, ReceiptError> {
        let body = ReceiptBody::new(provenance, problem, plan);
        self.persist(&body, true)?;
        Ok(ReceiptRecorder {
            store: self,
            body,
            active_nodes: BTreeMap::new(),
            active_fences: BTreeMap::new(),
            pending_publications: BTreeSet::new(),
            terminal: false,
        })
    }

    fn receipt_path(&self, attempt: ExecutionAttemptId) -> PathBuf {
        self.root.join(format!("{attempt}{RECEIPT_SUFFIX}"))
    }

    fn remove_orphaned_staging_files(&self) -> Result<(), ReceiptError> {
        let _mutation = self
            .state
            .mutation
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?;
        for entry in fs::read_dir(&self.root).map_err(|source| ReceiptError::Io {
            action: "list execution receipt staging files",
            source,
        })? {
            let path = entry
                .map_err(|source| ReceiptError::Io {
                    action: "read execution receipt staging entry",
                    source,
                })?
                .path();
            if is_receipt_staging_path(&path) {
                fs::remove_file(&path).map_err(|source| ReceiptError::Io {
                    action: "remove orphaned execution receipt staging file",
                    source,
                })?;
            }
        }
        sync_directory(&self.root)
    }

    fn persist(&self, body: &ReceiptBody, is_new: bool) -> Result<(), ReceiptError> {
        let _mutation = self
            .state
            .mutation
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?;
        let bytes = encode_document(body)?;
        let actual_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let reserved_bytes = if body.status.is_terminal() {
            actual_bytes
        } else {
            actual_bytes.max(worst_case_receipt_bytes(body)?)
        };
        self.make_room(body, reserved_bytes)?;
        let path = self.receipt_path(body.attempt());
        if is_new {
            atomic_create(&path, &bytes)
        } else {
            atomic_write(&path, &bytes)
        }
    }

    fn prepare_publication<'store>(
        &'store self,
        prepared: &ReceiptBody,
        completed: ReceiptBody,
    ) -> Result<PreparedPublicationReceipt<'store>, ReceiptError> {
        let mutation = self
            .state
            .mutation
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?;
        let prepared_bytes = encode_document(prepared)?;
        let completed_bytes = encode_document(&completed)?;
        let reserved_bytes = prepared_publication_bytes(
            prepared_bytes.len(),
            completed_bytes.len(),
            worst_case_receipt_bytes(prepared)?,
        )?;
        self.make_room(prepared, reserved_bytes)?;
        let path = self.receipt_path(prepared.attempt());
        let (parent, terminal) = staged_receipt(&path, &completed_bytes)?;
        atomic_write(&path, &prepared_bytes)?;
        Ok(PreparedPublicationReceipt {
            parent: parent.to_owned(),
            path,
            terminal,
            completed,
            _mutation: mutation,
        })
    }

    fn complete_publication(
        &self,
        prepared: PreparedPublicationReceipt,
    ) -> Result<ReceiptBody, ReceiptError> {
        let PreparedPublicationReceipt {
            parent,
            path,
            terminal,
            completed,
            _mutation,
        } = prepared;
        terminal.persist(&path).map_err(|error| ReceiptError::Io {
            action: "promote completed execution receipt",
            source: error.error,
        })?;
        sync_directory(&parent)?;
        Ok(completed)
    }

    fn make_room(&self, body: &ReceiptBody, incoming_bytes: u64) -> Result<(), ReceiptError> {
        if incoming_bytes > self.state.retention.max_bytes {
            return Err(ReceiptError::RetentionExceeded);
        }
        let current_path = self.receipt_path(body.attempt());
        let mut retained = Vec::new();
        let mut total_bytes = 0_u64;
        for entry in fs::read_dir(&self.root).map_err(|source| ReceiptError::Io {
            action: "list execution receipts",
            source,
        })? {
            let entry = entry.map_err(|source| ReceiptError::Io {
                action: "read execution receipt entry",
                source,
            })?;
            let path = entry.path();
            if path == current_path || !is_receipt_path(&path) {
                continue;
            }
            let file_bytes = entry
                .metadata()
                .map_err(|source| ReceiptError::Io {
                    action: "inspect execution receipt",
                    source,
                })?
                .len();
            let receipt = read_receipt_body(&path)?;
            let bytes = if receipt.status.is_terminal() {
                file_bytes
            } else {
                file_bytes.max(worst_case_receipt_bytes(&receipt)?)
            };
            total_bytes = total_bytes.saturating_add(bytes);
            retained.push((
                path,
                bytes,
                receipt.status.is_terminal(),
                receipt
                    .finished_unix_millis
                    .unwrap_or(receipt.started_unix_millis),
                receipt.attempt_identity,
            ));
        }
        retained.sort_unstable_by(|left, right| {
            (left.3, left.4.as_str()).cmp(&(right.3, right.4.as_str()))
        });
        let mut count = retained.len() + 1;
        let mut bytes = total_bytes.saturating_add(incoming_bytes);
        let mut prune = Vec::new();
        for (path, file_bytes, terminal, _, _) in retained {
            if count <= self.state.retention.max_receipts && bytes <= self.state.retention.max_bytes
            {
                break;
            }
            if !terminal {
                continue;
            }
            prune.push(path);
            count -= 1;
            bytes = bytes.saturating_sub(file_bytes);
        }
        if count > self.state.retention.max_receipts || bytes > self.state.retention.max_bytes {
            return Err(ReceiptError::RetentionExceeded);
        }
        for path in prune {
            fs::remove_file(path).map_err(|source| ReceiptError::Io {
                action: "prune retained execution receipt",
                source,
            })?;
        }
        sync_directory(&self.root)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReceiptDocument {
    schema: ReceiptSchema,
    payload_sha256: String,
    receipt: ReceiptBody,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReceiptSchema {
    name: String,
    version: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReceiptBody {
    attempt_identity: String,
    build_identity: String,
    route: RouteProjection,
    revision: u64,
    started_unix_millis: u64,
    finished_unix_millis: Option<u64>,
    status: ReceiptStatus,
    failure: Option<FailureProjection>,
    problem: ProblemProjection,
    plan: PlanProjection,
}

impl ReceiptBody {
    fn new(
        provenance: ExecutionProvenance,
        problem: &CompiledProblem,
        plan: &ExecutionPlan,
    ) -> Self {
        let route = RouteProjection::new(provenance.route());
        Self {
            attempt_identity: provenance.attempt.to_string(),
            build_identity: provenance.build.to_string(),
            route,
            revision: 1,
            started_unix_millis: now_millis(),
            finished_unix_millis: None,
            status: ReceiptStatus::Running,
            failure: None,
            problem: ProblemProjection::new(problem),
            plan: PlanProjection::new(plan),
        }
    }

    fn attempt(&self) -> ExecutionAttemptId {
        ExecutionAttemptId::from_sha256(parse_digest(&self.attempt_identity))
    }

    fn worst_case_terminal(&self) -> Self {
        let mut body = self.clone();
        body.revision = u64::MAX;
        body.finished_unix_millis = Some(u64::MAX);
        body.status = ReceiptStatus::Infeasible;
        let node_id =
            maximum_json_serialized_text(body.plan.nodes.iter().map(|node| node.node_id.as_str()))
                .map(ToOwned::to_owned);
        body.failure = Some(FailureProjection {
            kind: FailureKindProjection::ImplementationUnavailable,
            node_id,
            subject: Some(maximum_json_escaped_evidence()),
            infeasibility: Some(InfeasibilityProjection::Infeasible {
                resource: maximum_json_escaped_evidence(),
                resource_identity: maximum_json_escaped_evidence(),
                required: u64::MAX,
                available: u64::MAX,
            }),
        });
        for node in &mut body.plan.nodes {
            node.status = ReceiptStatus::NotStarted;
            node.actual_elapsed_nanos = Some(u64::MAX);
            for claim in &mut node.claims {
                claim.actual_peak = Some(claim.amount);
            }
            for io in &mut node.io {
                io.actual_bytes = Some(u64::MAX);
                io.actual_operations = Some(u64::MAX);
            }
        }
        for fence in &mut body.plan.fences {
            fence.status = ReceiptStatus::NotStarted;
            fence.actual_elapsed_nanos = Some(u64::MAX);
        }
        for artifact in &mut body.plan.artifacts {
            artifact.observed_identity = Some("f".repeat(64));
            artifact.disposition = Some(ArtifactDispositionProjection::RejectedStale);
            artifact.actual_bytes = Some(u64::MAX);
            artifact.path_identity = Some("f".repeat(64));
        }
        for (index, adaptation) in body.plan.adaptations.iter_mut().enumerate() {
            adaptation.applied_revision =
                Some(u64::MAX.saturating_sub(u64::try_from(index).unwrap_or(u64::MAX)));
        }
        body
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RouteProjection {
    matrix_schema_version: u32,
    matrix_contract_revision: u32,
    disposition: String,
    requirements: Vec<RouteRequirementProjection>,
}

impl RouteProjection {
    fn new(route: &ExecutionRouteEvidence) -> Self {
        Self {
            matrix_schema_version: route.matrix_schema_version(),
            matrix_contract_revision: route.matrix_contract_revision(),
            disposition: route_disposition(route.disposition()).to_string(),
            requirements: route
                .requirements()
                .iter()
                .map(|requirement| RouteRequirementProjection {
                    id: requirement.id().to_string(),
                    kind: route_requirement_kind(requirement.kind()).to_string(),
                    disposition: route_disposition(requirement.disposition()).to_string(),
                    current_owner: requirement.evidence().current_owner.clone(),
                    destination_tickets: requirement.evidence().destination_tickets.clone(),
                    evidence_issues: requirement.evidence().evidence_issues.clone(),
                    baseline_manifests: requirement.evidence().baseline_manifests.clone(),
                    acceptance_contract: requirement.evidence().acceptance_contract.clone(),
                    transfer_point: requirement.evidence().transfer_point.clone(),
                    deletion_condition: requirement.evidence().deletion_condition.clone(),
                    source_evidence: requirement.evidence().source_evidence.clone(),
                    obligation_ticket: requirement.evidence().obligation_ticket.clone(),
                    obligation_reason: requirement.evidence().obligation_reason.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RouteRequirementProjection {
    id: String,
    kind: String,
    disposition: String,
    current_owner: String,
    destination_tickets: Vec<String>,
    evidence_issues: Vec<u64>,
    baseline_manifests: Vec<String>,
    acceptance_contract: String,
    transfer_point: String,
    deletion_condition: String,
    source_evidence: Vec<String>,
    obligation_ticket: Option<String>,
    obligation_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FailureProjection {
    kind: FailureKindProjection,
    node_id: Option<String>,
    subject: Option<String>,
    infeasibility: Option<InfeasibilityProjection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum InfeasibilityProjection {
    NoCapableAlternative,
    Infeasible {
        resource: String,
        resource_identity: String,
        required: u64,
        available: u64,
    },
    RecordedFailure {
        attempt: String,
        status: ReceiptStatus,
    },
}

impl InfeasibilityProjection {
    fn to_runtime(&self) -> ReceiptInfeasibilityCertificate {
        match self {
            Self::NoCapableAlternative => ReceiptInfeasibilityCertificate::NoCapableAlternative,
            Self::Infeasible {
                resource,
                resource_identity,
                required,
                available,
            } => ReceiptInfeasibilityCertificate::Infeasible {
                resource: resource.clone(),
                resource_identity: ResourceIdentity::new(resource_identity.clone()),
                required: *required,
                available: *available,
            },
            Self::RecordedFailure { attempt, status } => {
                ReceiptInfeasibilityCertificate::RecordedFailure {
                    attempt: attempt.clone(),
                    status: *status,
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailureKindProjection {
    BindingMutation,
    ResourceInfeasible,
    ImplementationUnavailable,
    ImplementationMismatch,
    EvidenceContract,
    Scheduler,
    Adapter,
    Interrupted,
}

impl From<ReceiptFailureKind> for FailureKindProjection {
    fn from(value: ReceiptFailureKind) -> Self {
        match value {
            ReceiptFailureKind::BindingMutation => Self::BindingMutation,
            ReceiptFailureKind::ResourceInfeasible => Self::ResourceInfeasible,
            ReceiptFailureKind::ImplementationUnavailable => Self::ImplementationUnavailable,
            ReceiptFailureKind::ImplementationMismatch => Self::ImplementationMismatch,
            ReceiptFailureKind::EvidenceContract => Self::EvidenceContract,
            ReceiptFailureKind::Scheduler => Self::Scheduler,
            ReceiptFailureKind::Adapter => Self::Adapter,
            ReceiptFailureKind::Interrupted => Self::Interrupted,
        }
    }
}

impl From<FailureKindProjection> for ReceiptFailureKind {
    fn from(value: FailureKindProjection) -> Self {
        match value {
            FailureKindProjection::BindingMutation => Self::BindingMutation,
            FailureKindProjection::ResourceInfeasible => Self::ResourceInfeasible,
            FailureKindProjection::ImplementationUnavailable => Self::ImplementationUnavailable,
            FailureKindProjection::ImplementationMismatch => Self::ImplementationMismatch,
            FailureKindProjection::EvidenceContract => Self::EvidenceContract,
            FailureKindProjection::Scheduler => Self::Scheduler,
            FailureKindProjection::Adapter => Self::Adapter,
            FailureKindProjection::Interrupted => Self::Interrupted,
        }
    }
}

pub(crate) struct ReceiptFailure {
    kind: ReceiptFailureKind,
    node: Option<WorkNodeId>,
    subject: Option<String>,
    infeasibility: Option<InfeasibilityProjection>,
}

impl ReceiptFailure {
    pub(crate) fn new(
        kind: ReceiptFailureKind,
        node: Option<WorkNodeId>,
        subject: Option<String>,
    ) -> Self {
        Self {
            kind,
            node,
            subject,
            infeasibility: None,
        }
    }

    pub(crate) fn infeasible(error: &crate::ResourceError) -> Self {
        let infeasibility = match error {
            crate::ResourceError::NoCapableAlternative => {
                InfeasibilityProjection::NoCapableAlternative
            }
            crate::ResourceError::NoFeasibleAlternative(certificate) => {
                // Scheduled admission always offers exactly the plan's sole
                // alternative, so its refusal projects losslessly.
                let rejection = certificate
                    .rejections()
                    .first()
                    .expect("admission certificates are never empty");
                match rejection.reason() {
                    crate::AlternativeRejectionReason::NoCapableAlternative => {
                        InfeasibilityProjection::NoCapableAlternative
                    }
                    crate::AlternativeRejectionReason::Infeasible {
                        resource,
                        required,
                        available,
                    } => InfeasibilityProjection::Infeasible {
                        resource: bounded_evidence_text(resource),
                        resource_identity: resource.clone(),
                        required: *required,
                        available: *available,
                    },
                    crate::AlternativeRejectionReason::RecordedFailure { attempt, status } => {
                        InfeasibilityProjection::RecordedFailure {
                            attempt: hex(&attempt.as_bytes()),
                            status: *status,
                        }
                    }
                }
            }
            crate::ResourceError::Infeasible {
                resource,
                required,
                available,
            } => InfeasibilityProjection::Infeasible {
                resource: bounded_evidence_text(resource),
                resource_identity: resource.clone(),
                required: *required,
                available: *available,
            },
            _ => unreachable!("only admission infeasibility has a receipt certificate"),
        };
        Self {
            kind: ReceiptFailureKind::ResourceInfeasible,
            node: None,
            subject: None,
            infeasibility: Some(infeasibility),
        }
    }

    fn projection(self) -> FailureProjection {
        FailureProjection {
            kind: self.kind.into(),
            node_id: self.node.map(|node| stable_text(node.as_str())),
            subject: self.subject.map(|subject| bounded_evidence_text(&subject)),
            infeasibility: self.infeasibility,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProblemProjection {
    problem_identity: String,
    problem_identity_basis: String,
    geometry_identity: String,
    product_graph: ProductGraphProjection,
    observation_identity: String,
    reference_identities: Vec<ReferenceIdentityProjection>,
    model_identity: ModelIdentityProjection,
    numerics_identity: String,
    model_lifecycle: ModelLifecycleProjection,
    effective: CompiledProblemEvidence,
}

impl ProblemProjection {
    fn new(problem: &CompiledProblem) -> Self {
        let inputs = problem.inputs();
        Self {
            problem_identity: hex(&problem.problem_id().as_bytes()),
            problem_identity_basis: hex(&problem.problem_identity_basis().as_bytes()),
            geometry_identity: hex(&problem.geometry().geometry_id().as_bytes()),
            product_graph: ProductGraphProjection::new(problem),
            observation_identity: hex(&inputs.observation().identity().as_bytes()),
            reference_identities: inputs
                .reference_data()
                .iter()
                .map(|(kind, identity)| ReferenceIdentityProjection {
                    kind: reference_kind(*kind).to_string(),
                    identity: hex(&identity.as_bytes()),
                })
                .collect(),
            model_identity: model_projection(inputs),
            numerics_identity: hex(&problem.numerics_id().as_bytes()),
            model_lifecycle: ModelLifecycleProjection::new(problem),
            effective: CompiledProblemEvidence::project(problem),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ModelLifecycleProjection {
    identity: String,
    numerics_identity: String,
    target_shape_identity: String,
    reprojection: ModelReprojectionProjection,
    bounds: ModelBoundsProjection,
    arithmetic_precision: String,
    state_encoding: String,
    support_semantics: String,
    input: ModelLifecycleInputProjection,
}

impl ModelLifecycleProjection {
    fn new(problem: &CompiledProblem) -> Self {
        let contract = problem.model_lifecycle();
        let bounds = contract.bounds();
        Self {
            identity: hex(&contract.contract_id().as_bytes()),
            numerics_identity: hex(&contract.numerics().as_bytes()),
            target_shape_identity: hex(&contract.target().identity().as_bytes()),
            reprojection: ModelReprojectionProjection::new(problem),
            bounds: ModelBoundsProjection {
                max_model_samples: bounds.max_model_samples(),
                max_source_samples: bounds.max_source_samples(),
                max_reprojection_terms: bounds.max_reprojection_terms(),
                max_delta_terms: bounds.max_delta_terms(),
                max_absolute_model_value_bits: bounds.max_absolute_model_value().to_bits(),
                max_absolute_delta_value_bits: bounds.max_absolute_delta_value().to_bits(),
            },
            arithmetic_precision: numeric_precision(contract.arithmetic_precision()).to_string(),
            state_encoding: model_state_encoding(contract.state_encoding()).to_string(),
            support_semantics: model_support_semantics(contract.support_semantics()).to_string(),
            input: ModelLifecycleInputProjection::new(contract.input()),
        }
    }

    fn validate(
        &self,
        product_graph_identity: &str,
        numerics_identity: &str,
        evidence: &CompiledProblemEvidence,
    ) -> Result<(), ReceiptError> {
        require_integrity(
            is_nonzero_digest(&self.identity)
                && is_nonzero_digest(&self.numerics_identity)
                && is_nonzero_digest(&self.target_shape_identity)
                && self.bounds.is_valid()
                && matches!(self.arithmetic_precision.as_str(), "f32" | "f64")
                && self.state_encoding == "canonical_f64"
                && self.support_semantics == "explicit_validity",
        )?;
        let arithmetic_precision = parse_numeric_precision(&self.arithmetic_precision)
            .ok_or(ReceiptError::IntegrityMismatch)?;
        require_integrity(self.numerics_identity == numerics_identity)?;
        self.reprojection.validate(
            product_graph_identity,
            numerics_identity,
            arithmetic_precision,
            evidence,
        )?;
        self.input.validate()?;
        let bounds = ModelBounds::new(
            self.bounds.max_model_samples,
            self.bounds.max_source_samples,
            self.bounds.max_reprojection_terms,
            self.bounds.max_delta_terms,
            f64::from_bits(self.bounds.max_absolute_model_value_bits),
            f64::from_bits(self.bounds.max_absolute_delta_value_bits),
        )
        .map_err(|_| ReceiptError::IntegrityMismatch)?;
        validate_model_lifecycle_contract_identity(
            LogicalIdentity::from_sha256(parse_digest(&self.identity)),
            LogicalIdentity::from_sha256(parse_digest(&self.numerics_identity)),
            LogicalIdentity::from_sha256(parse_digest(&self.reprojection.identity)),
            LogicalIdentity::from_sha256(parse_digest(&self.target_shape_identity)),
            bounds,
            arithmetic_precision,
            parse_model_state_encoding(&self.state_encoding)
                .ok_or(ReceiptError::IntegrityMismatch)?,
            parse_model_support_semantics(&self.support_semantics)
                .ok_or(ReceiptError::IntegrityMismatch)?,
            &self.input.identity(),
        )
        .map_err(|_| ReceiptError::IntegrityMismatch)?;
        let max_model_samples = self.bounds.max_model_samples.to_string();
        let max_source_samples = self.bounds.max_source_samples.to_string();
        let max_reprojection_terms = self.bounds.max_reprojection_terms.to_string();
        let max_delta_terms = self.bounds.max_delta_terms.to_string();
        let max_absolute_model_value =
            stable_float(f64::from_bits(self.bounds.max_absolute_model_value_bits));
        let max_absolute_delta_value =
            stable_float(f64::from_bits(self.bounds.max_absolute_delta_value_bits));
        require_integrity(
            evidence.field("model_lifecycle.identity") == Some(self.identity.as_str())
                && evidence.field("model_lifecycle.numerics_identity")
                    == Some(self.numerics_identity.as_str())
                && evidence.field("model_lifecycle.target_shape_identity")
                    == Some(self.target_shape_identity.as_str())
                && evidence.field("model_lifecycle.arithmetic_precision")
                    == Some(self.arithmetic_precision.as_str())
                && evidence.field("model_lifecycle.state_encoding")
                    == Some(self.state_encoding.as_str())
                && evidence.field("model_lifecycle.support_semantics")
                    == Some(self.support_semantics.as_str())
                && evidence.field("model_lifecycle.bounds.max_model_samples")
                    == Some(max_model_samples.as_str())
                && evidence.field("model_lifecycle.bounds.max_source_samples")
                    == Some(max_source_samples.as_str())
                && evidence.field("model_lifecycle.bounds.max_reprojection_terms")
                    == Some(max_reprojection_terms.as_str())
                && evidence.field("model_lifecycle.bounds.max_delta_terms")
                    == Some(max_delta_terms.as_str())
                && evidence.field("model_lifecycle.bounds.max_absolute_model_value")
                    == Some(max_absolute_model_value.as_str())
                && evidence.field("model_lifecycle.bounds.max_absolute_delta_value")
                    == Some(max_absolute_delta_value.as_str()),
        )?;
        self.input.validate_audit_fields(evidence)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ModelReprojectionProjection {
    identity: String,
    product_graph_identity: String,
    numerics_identity: String,
    conversion_precision: String,
    direction_registry: String,
    basis_registry: String,
    polarization_registry: String,
    invalid_contributor_policy: String,
    uncovered_target_policy: String,
}

impl ModelReprojectionProjection {
    fn new(problem: &CompiledProblem) -> Self {
        let contract = problem.model_lifecycle();
        let policy = contract.reprojection_policy();
        Self {
            identity: hex(&contract.reprojection_contract_identity().as_bytes()),
            product_graph_identity: hex(&problem.product_graph().graph_id().as_bytes()),
            numerics_identity: hex(&contract.numerics().as_bytes()),
            conversion_precision: numeric_precision(contract.arithmetic_precision()).to_string(),
            direction_registry: policy.direction_registry().as_str().to_owned(),
            basis_registry: policy.basis_registry().as_str().to_owned(),
            polarization_registry: policy.polarization_registry().as_str().to_owned(),
            invalid_contributor_policy: policy.invalid_contributor().as_str().to_owned(),
            uncovered_target_policy: policy.uncovered_target().as_str().to_owned(),
        }
    }

    fn validate(
        &self,
        product_graph_identity: &str,
        numerics_identity: &str,
        conversion_precision: NumericPrecision,
        evidence: &CompiledProblemEvidence,
    ) -> Result<(), ReceiptError> {
        let policy = ModelReprojectionPolicy::canonical();
        require_integrity(
            is_digest(&self.identity)
                && is_digest(&self.product_graph_identity)
                && is_digest(&self.numerics_identity)
                && matches!(self.conversion_precision.as_str(), "f32" | "f64")
                && self.direction_registry == policy.direction_registry().as_str()
                && self.basis_registry == policy.basis_registry().as_str()
                && self.polarization_registry == policy.polarization_registry().as_str()
                && self.invalid_contributor_policy == policy.invalid_contributor().as_str()
                && self.uncovered_target_policy == policy.uncovered_target().as_str()
                && self.product_graph_identity == product_graph_identity
                && self.numerics_identity == numerics_identity
                && self.conversion_precision == numeric_precision(conversion_precision),
        )?;
        validate_model_reprojection_contract_identity(
            LogicalIdentity::from_sha256(parse_digest(&self.identity)),
            LogicalIdentity::from_sha256(parse_digest(product_graph_identity)),
            LogicalIdentity::from_sha256(parse_digest(numerics_identity)),
            conversion_precision,
            policy,
        )
        .map_err(|_| ReceiptError::IntegrityMismatch)?;
        let fields = [
            (
                "model_lifecycle.reprojection.identity",
                self.identity.as_str(),
            ),
            (
                "model_lifecycle.reprojection.product_graph_identity",
                self.product_graph_identity.as_str(),
            ),
            (
                "model_lifecycle.reprojection.numerics_identity",
                self.numerics_identity.as_str(),
            ),
            (
                "model_lifecycle.reprojection.conversion_precision",
                self.conversion_precision.as_str(),
            ),
            (
                "model_lifecycle.reprojection.direction_registry",
                self.direction_registry.as_str(),
            ),
            (
                "model_lifecycle.reprojection.basis_registry",
                self.basis_registry.as_str(),
            ),
            (
                "model_lifecycle.reprojection.polarization_registry",
                self.polarization_registry.as_str(),
            ),
            (
                "model_lifecycle.reprojection.invalid_contributor_policy",
                self.invalid_contributor_policy.as_str(),
            ),
            (
                "model_lifecycle.reprojection.uncovered_target_policy",
                self.uncovered_target_policy.as_str(),
            ),
        ];
        require_integrity(
            fields
                .into_iter()
                .all(|(field, value)| evidence.field(field) == Some(value)),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ModelBoundsProjection {
    max_model_samples: usize,
    max_source_samples: usize,
    max_reprojection_terms: usize,
    max_delta_terms: usize,
    max_absolute_model_value_bits: u64,
    max_absolute_delta_value_bits: u64,
}

impl ModelBoundsProjection {
    fn is_valid(&self) -> bool {
        let model = f64::from_bits(self.max_absolute_model_value_bits);
        let delta = f64::from_bits(self.max_absolute_delta_value_bits);
        self.max_model_samples > 0
            && self.max_source_samples > 0
            && self.max_reprojection_terms > 0
            && self.max_delta_terms > 0
            && model.is_finite()
            && model > 0.0
            && delta.is_finite()
            && delta > 0.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ModelLifecycleInputProjection {
    Empty,
    AlignedSeed {
        source_identity: String,
        support_identity: String,
    },
    ReprojectedSeed {
        source_identity: String,
        source_shape_identity: String,
        preparation_contract_identity: String,
        reprojection_identity: String,
        support_identity: String,
        sample_identity: String,
        stencil_identity: String,
        proof_identity: String,
    },
    Generation {
        generation_identity: String,
    },
}

impl ModelLifecycleInputProjection {
    fn new(input: &ModelInputCommitment) -> Self {
        match input {
            ModelInputCommitment::Empty => Self::Empty,
            ModelInputCommitment::AlignedSeed { source, support } => Self::AlignedSeed {
                source_identity: hex(&source.as_bytes()),
                support_identity: hex(&support.as_bytes()),
            },
            ModelInputCommitment::ReprojectedSeed(commitment) => Self::ReprojectedSeed {
                source_identity: hex(&commitment.source().as_bytes()),
                source_shape_identity: hex(&commitment.source_shape().identity().as_bytes()),
                preparation_contract_identity: hex(&commitment.preparation_contract().as_bytes()),
                reprojection_identity: hex(&commitment.reprojection().as_bytes()),
                support_identity: hex(&commitment.support().as_bytes()),
                sample_identity: hex(&commitment.samples().as_bytes()),
                stencil_identity: hex(&commitment.stencil().as_bytes()),
                proof_identity: hex(&commitment.proof().as_bytes()),
            },
            ModelInputCommitment::Generation(generation) => Self::Generation {
                generation_identity: hex(&generation.as_bytes()),
            },
        }
    }

    fn validate(&self) -> Result<(), ReceiptError> {
        require_integrity(match self {
            Self::Empty => true,
            Self::AlignedSeed {
                source_identity,
                support_identity,
            } => is_nonzero_digest(source_identity) && is_nonzero_digest(support_identity),
            Self::ReprojectedSeed {
                source_identity,
                source_shape_identity,
                preparation_contract_identity,
                reprojection_identity,
                support_identity,
                sample_identity,
                stencil_identity,
                proof_identity,
            } => {
                is_nonzero_digest(source_identity)
                    && is_nonzero_digest(source_shape_identity)
                    && is_nonzero_digest(preparation_contract_identity)
                    && is_nonzero_digest(reprojection_identity)
                    && is_nonzero_digest(support_identity)
                    && is_nonzero_digest(sample_identity)
                    && is_nonzero_digest(stencil_identity)
                    && is_nonzero_digest(proof_identity)
            }
            Self::Generation {
                generation_identity,
            } => is_nonzero_digest(generation_identity),
        })?;
        if let Self::ReprojectedSeed {
            source_identity,
            source_shape_identity,
            preparation_contract_identity,
            reprojection_identity,
            support_identity,
            sample_identity,
            stencil_identity,
            proof_identity,
        } = self
        {
            validate_reprojected_seed_proof_identity(
                LogicalIdentity::from_sha256(parse_digest(proof_identity)),
                LogicalIdentity::from_sha256(parse_digest(source_identity)),
                LogicalIdentity::from_sha256(parse_digest(source_shape_identity)),
                LogicalIdentity::from_sha256(parse_digest(preparation_contract_identity)),
                LogicalIdentity::from_sha256(parse_digest(reprojection_identity)),
                LogicalIdentity::from_sha256(parse_digest(support_identity)),
                LogicalIdentity::from_sha256(parse_digest(sample_identity)),
                LogicalIdentity::from_sha256(parse_digest(stencil_identity)),
            )
            .map_err(|_| ReceiptError::IntegrityMismatch)?;
        }
        Ok(())
    }

    fn identity(&self) -> ModelInputCommitmentIdentity {
        match self {
            Self::Empty => ModelInputCommitmentIdentity::Empty,
            Self::AlignedSeed {
                source_identity,
                support_identity,
            } => ModelInputCommitmentIdentity::AlignedSeed {
                source: LogicalIdentity::from_sha256(parse_digest(source_identity)),
                support: LogicalIdentity::from_sha256(parse_digest(support_identity)),
            },
            Self::ReprojectedSeed {
                source_identity,
                source_shape_identity,
                preparation_contract_identity,
                reprojection_identity,
                support_identity,
                sample_identity,
                stencil_identity,
                proof_identity,
            } => ModelInputCommitmentIdentity::ReprojectedSeed {
                source: LogicalIdentity::from_sha256(parse_digest(source_identity)),
                source_shape: LogicalIdentity::from_sha256(parse_digest(source_shape_identity)),
                preparation_contract: LogicalIdentity::from_sha256(parse_digest(
                    preparation_contract_identity,
                )),
                reprojection: LogicalIdentity::from_sha256(parse_digest(reprojection_identity)),
                support: LogicalIdentity::from_sha256(parse_digest(support_identity)),
                samples: LogicalIdentity::from_sha256(parse_digest(sample_identity)),
                stencil: LogicalIdentity::from_sha256(parse_digest(stencil_identity)),
                proof: LogicalIdentity::from_sha256(parse_digest(proof_identity)),
            },
            Self::Generation {
                generation_identity,
            } => ModelInputCommitmentIdentity::Generation(LogicalIdentity::from_sha256(
                parse_digest(generation_identity),
            )),
        }
    }

    fn validate_audit_fields(
        &self,
        evidence: &CompiledProblemEvidence,
    ) -> Result<(), ReceiptError> {
        let matches = evidence.field("model_lifecycle.input.kind") == Some(self.kind())
            && match self {
                Self::Empty => true,
                Self::AlignedSeed {
                    source_identity,
                    support_identity,
                } => {
                    evidence.field("model_lifecycle.input.source_identity")
                        == Some(source_identity.as_str())
                        && evidence.field("model_lifecycle.input.support_identity")
                            == Some(support_identity.as_str())
                }
                Self::ReprojectedSeed {
                    source_identity,
                    source_shape_identity,
                    preparation_contract_identity,
                    reprojection_identity,
                    support_identity,
                    sample_identity,
                    stencil_identity,
                    proof_identity,
                } => {
                    evidence.field("model_lifecycle.input.source_identity")
                        == Some(source_identity.as_str())
                        && evidence.field("model_lifecycle.input.source_shape_identity")
                            == Some(source_shape_identity.as_str())
                        && evidence.field("model_lifecycle.input.preparation_contract_identity")
                            == Some(preparation_contract_identity.as_str())
                        && evidence.field("model_lifecycle.input.reprojection_identity")
                            == Some(reprojection_identity.as_str())
                        && evidence.field("model_lifecycle.input.support_identity")
                            == Some(support_identity.as_str())
                        && evidence.field("model_lifecycle.input.sample_identity")
                            == Some(sample_identity.as_str())
                        && evidence.field("model_lifecycle.input.stencil_identity")
                            == Some(stencil_identity.as_str())
                        && evidence.field("model_lifecycle.input.proof_identity")
                            == Some(proof_identity.as_str())
                }
                Self::Generation {
                    generation_identity,
                } => {
                    evidence.field("model_lifecycle.input.generation_identity")
                        == Some(generation_identity.as_str())
                }
            };
        require_integrity(matches)
    }

    const fn kind(&self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::AlignedSeed { .. } => "aligned_seed",
            Self::ReprojectedSeed { .. } => "reprojected_seed",
            Self::Generation { .. } => "generation",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReferenceIdentityProjection {
    kind: String,
    identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
enum ModelIdentityProjection {
    Empty,
    Seed(String),
    Generation(String),
}

impl ModelIdentityProjection {
    fn to_runtime(&self) -> ModelStateIdentity {
        match self {
            Self::Empty => ModelStateIdentity::Empty,
            Self::Seed(identity) => {
                ModelStateIdentity::Seed(LogicalIdentity::from_sha256(parse_digest(identity)))
            }
            Self::Generation(identity) => {
                ModelStateIdentity::Generation(LogicalIdentity::from_sha256(parse_digest(identity)))
            }
        }
    }

    fn validate_against(
        &self,
        input: &ModelLifecycleInputProjection,
        evidence: &CompiledProblemEvidence,
    ) -> Result<(), ReceiptError> {
        let valid = match (self, input) {
            (Self::Empty, ModelLifecycleInputProjection::Empty) => {
                evidence.field("observation.model.kind") == Some("empty")
                    && evidence.field("observation.model.identity").is_none()
            }
            (
                Self::Seed(identity),
                ModelLifecycleInputProjection::AlignedSeed {
                    source_identity, ..
                }
                | ModelLifecycleInputProjection::ReprojectedSeed {
                    source_identity, ..
                },
            ) => {
                is_nonzero_digest(identity)
                    && identity == source_identity
                    && evidence.field("observation.model.kind") == Some("seed")
                    && evidence.field("observation.model.identity") == Some(identity.as_str())
            }
            (
                Self::Generation(identity),
                ModelLifecycleInputProjection::Generation {
                    generation_identity,
                },
            ) => {
                is_nonzero_digest(identity)
                    && identity == generation_identity
                    && evidence.field("observation.model.kind") == Some("generation")
                    && evidence.field("observation.model.identity") == Some(identity.as_str())
            }
            _ => false,
        };
        require_integrity(valid)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PlanProjection {
    plan_identity: String,
    dag_identity: String,
    product_graph_identity: String,
    implementation_registry_identity: String,
    resource_policy_identity: String,
    resource_policy: ResourcePolicyProjection,
    cost_model_identity: String,
    required_resource_capabilities: Vec<String>,
    selected_alternative: DemandAlternativeProjection,
    selected_implementations: Vec<String>,
    prediction: PredictionProjection,
    nodes: Vec<NodeProjection>,
    fences: Vec<FenceProjection>,
    allocation_generations: Vec<AllocationProjection>,
    physical_slots: Vec<PhysicalSlotProjection>,
    artifacts: Vec<ArtifactProjection>,
    publication_layouts: Vec<PublicationLayoutProjection>,
    initial_execution_knobs: ExecutionKnobsProjection,
    adaptations: Vec<AdaptationProjection>,
}

impl PlanProjection {
    fn new(plan: &ExecutionPlan) -> Self {
        let dag = plan.execution_dag();
        let nodes = dag
            .nodes()
            .values()
            .map(|node| {
                let prediction = &plan.prediction().stages()[&node.id];
                NodeProjection::new(node, prediction)
            })
            .collect();
        let fences = dag
            .nodes()
            .values()
            .flat_map(|node| {
                node.fences
                    .iter()
                    .map(|kind| FenceProjection::new(&node.id, *kind))
            })
            .collect();
        Self {
            plan_identity: hex(&plan.plan_id().as_bytes()),
            dag_identity: hex(&plan.physical_work_id().as_bytes()),
            product_graph_identity: hex(&plan.product_graph_id().as_bytes()),
            implementation_registry_identity: hex(&plan.implementation_registry_id().as_bytes()),
            resource_policy_identity: hex(&plan.resource_policy_id().as_bytes()),
            resource_policy: ResourcePolicyProjection::new(plan.resource_policy()),
            cost_model_identity: hex(&plan.planner_cost_model_profile_id().as_bytes()),
            required_resource_capabilities: dag
                .required_resource_capabilities()
                .iter()
                .map(|identity| stable_text(identity.as_str()))
                .collect(),
            selected_alternative: DemandAlternativeProjection::new(dag.resource_alternative()),
            selected_implementations: dag
                .selected_implementations()
                .iter()
                .map(|identity| stable_text(identity.as_str()))
                .collect(),
            prediction: PredictionProjection::new(plan.prediction()),
            nodes,
            fences,
            allocation_generations: dag
                .logical_allocations()
                .values()
                .map(AllocationProjection::new)
                .collect(),
            physical_slots: dag
                .physical_slots()
                .values()
                .map(PhysicalSlotProjection::new)
                .collect(),
            artifacts: plan
                .artifacts()
                .iter()
                .map(ArtifactProjection::new)
                .collect(),
            publication_layouts: plan
                .publication_layouts()
                .entries()
                .iter()
                .map(PublicationLayoutProjection::new)
                .collect(),
            initial_execution_knobs: ExecutionKnobsProjection::new(dag.initial_knobs()),
            adaptations: dag
                .adaptations()
                .values()
                .map(AdaptationProjection::new)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PublicationParticipantProjection {
    Product {
        graph_identity: String,
        node_ordinal: usize,
    },
    ModelData {
        measurement_set_identity: String,
    },
}

impl PublicationParticipantProjection {
    fn new(participant: PublicationParticipant) -> Self {
        match participant {
            PublicationParticipant::Product { graph_id, node_id } => Self::Product {
                graph_identity: hex(&graph_id.as_bytes()),
                node_ordinal: node_id.ordinal(),
            },
            PublicationParticipant::ModelData(measurement_set) => Self::ModelData {
                measurement_set_identity: measurement_set.to_string(),
            },
        }
    }

    fn to_runtime(&self) -> ReceiptPublicationParticipant {
        match self {
            Self::Product {
                graph_identity,
                node_ordinal,
            } => ReceiptPublicationParticipant::Product {
                graph_identity: parse_digest(graph_identity),
                node_ordinal: *node_ordinal,
            },
            Self::ModelData {
                measurement_set_identity,
            } => ReceiptPublicationParticipant::ModelData(MeasurementSetIdentity::new(
                LogicalIdentity::from_sha256(parse_digest(measurement_set_identity)),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationLayoutProjection {
    participant: PublicationParticipantProjection,
    artifact_identity: String,
    layout_identity: String,
    producer: String,
    terminal: String,
    writer_buffer_kind: String,
    writer_allocation: String,
    mapped_page_cache: Option<PublicationMappedStagingProjection>,
    resource_bounds: PublicationResourceBoundsProjection,
}

impl PublicationLayoutProjection {
    fn new(layout: &crate::PublicationPhysicalLayout) -> Self {
        Self {
            participant: PublicationParticipantProjection::new(layout.participant()),
            artifact_identity: hex(&layout.artifact().as_bytes()),
            layout_identity: hex(&layout.layout_id().as_bytes()),
            producer: stable_text(layout.staging().producer().as_str()),
            terminal: dependency(layout.staging().terminal()),
            writer_buffer_kind: io_buffer(layout.staging().writer_buffer_kind()).to_string(),
            writer_allocation: stable_text(layout.staging().writer_allocation().as_str()),
            mapped_page_cache: layout.staging().mapped_page_cache().map(|mapped| {
                PublicationMappedStagingProjection {
                    producer: stable_text(mapped.producer().as_str()),
                    terminal: dependency(mapped.terminal()),
                    allocation: stable_text(mapped.allocation().as_str()),
                }
            }),
            resource_bounds: PublicationResourceBoundsProjection::new(layout.resource_bounds()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationMappedStagingProjection {
    producer: String,
    terminal: String,
    allocation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationResourceBoundsProjection {
    staged_storage_bytes: u64,
    final_storage_bytes: u64,
    writer_buffer_bytes: u64,
    mapped_page_cache_bytes: u64,
}

impl PublicationResourceBoundsProjection {
    fn new(bounds: PublicationResourceBounds) -> Self {
        Self {
            staged_storage_bytes: bounds.staged_storage_bytes(),
            final_storage_bytes: bounds.final_storage_bytes(),
            writer_buffer_bytes: bounds.writer_buffer_bytes(),
            mapped_page_cache_bytes: bounds.mapped_page_cache_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PredictionProjection {
    elapsed_nanos: u64,
    confidence_ppm: u32,
    uncertainty: Vec<UncertaintyProjection>,
}

impl PredictionProjection {
    fn new(prediction: &crate::PlanPrediction) -> Self {
        Self {
            elapsed_nanos: prediction.elapsed_nanos(),
            confidence_ppm: prediction.confidence().parts_per_million(),
            uncertainty: prediction
                .uncertainty()
                .iter()
                .map(|item| UncertaintyProjection {
                    identity: stable_text(item.identity()),
                    predicted_nanos: item.predicted_nanos(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UncertaintyProjection {
    identity: String,
    predicted_nanos: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ResourcePolicyProjection {
    Interactive,
    Balanced,
    Exclusive,
    Explicit {
        ceilings: ResourceOverrideProjection,
    },
}

impl ResourcePolicyProjection {
    fn new(policy: &ResourcePolicy) -> Self {
        match policy {
            ResourcePolicy::Interactive => Self::Interactive,
            ResourcePolicy::Balanced => Self::Balanced,
            ResourcePolicy::Exclusive => Self::Exclusive,
            ResourcePolicy::Explicit(ceilings) => Self::Explicit {
                ceilings: ResourceOverrideProjection::new(ceilings),
            },
        }
    }

    fn to_runtime(&self) -> ResourcePolicy {
        match self {
            Self::Interactive => ResourcePolicy::Interactive,
            Self::Balanced => ResourcePolicy::Balanced,
            Self::Exclusive => ResourcePolicy::Exclusive,
            Self::Explicit { ceilings } => ResourcePolicy::Explicit(ceilings.to_runtime()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ResourceOverrideProjection {
    memory_bytes: BTreeMap<String, u64>,
    workers: Option<u64>,
    storage_bytes: BTreeMap<String, u64>,
    rates_per_second: BTreeMap<String, u64>,
    cache_bytes: Option<u64>,
    locks: Option<u64>,
    file_descriptors: Option<u64>,
    queue_slots: BTreeMap<String, u64>,
    accelerator_slots: BTreeMap<String, u64>,
}

impl ResourceOverrideProjection {
    fn new(ceilings: &crate::ResourceOverride) -> Self {
        Self {
            memory_bytes: ceilings
                .memory_bytes
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            workers: ceilings.workers,
            storage_bytes: ceilings
                .storage_bytes
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            rates_per_second: ceilings
                .rates_per_second
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            cache_bytes: ceilings.cache_bytes,
            locks: ceilings.locks,
            file_descriptors: ceilings.file_descriptors,
            queue_slots: ceilings
                .queue_slots
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            accelerator_slots: ceilings
                .accelerator_slots
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
        }
    }

    fn to_runtime(&self) -> crate::ResourceOverride {
        crate::ResourceOverride {
            memory_bytes: self
                .memory_bytes
                .iter()
                .map(|(identity, amount)| (crate::CapacityDomainId::new(identity.clone()), *amount))
                .collect(),
            workers: self.workers,
            storage_bytes: self
                .storage_bytes
                .iter()
                .map(|(identity, amount)| (crate::StorageDomainId::new(identity.clone()), *amount))
                .collect(),
            rates_per_second: self
                .rates_per_second
                .iter()
                .map(|(identity, amount)| (crate::RateResourceId::new(identity.clone()), *amount))
                .collect(),
            cache_bytes: self.cache_bytes,
            locks: self.locks,
            file_descriptors: self.file_descriptors,
            queue_slots: self
                .queue_slots
                .iter()
                .map(|(identity, amount)| (crate::QueueResourceId::new(identity.clone()), *amount))
                .collect(),
            accelerator_slots: self
                .accelerator_slots
                .iter()
                .map(|(identity, amount)| (crate::AcceleratorId::new(identity.clone()), *amount))
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CountDemandProjection {
    hard: u64,
    preferred: u64,
}

impl CountDemandProjection {
    fn new(demand: crate::CountDemand) -> Self {
        Self {
            hard: demand.hard(),
            preferred: demand.preferred(),
        }
    }

    const fn to_runtime(self) -> crate::CountDemand {
        crate::CountDemand::new(self.hard, self.preferred)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RuntimeOverheadProjection {
    thread_stack_bytes: u64,
    allocator_fragmentation_bytes: u64,
    external_library_bytes: u64,
    fft_workspace_bytes: u64,
    driver_bytes: u64,
    jit_bytes: u64,
    command_buffer_bytes: u64,
}

impl RuntimeOverheadProjection {
    const fn new(demand: crate::RuntimeOverheadDemand) -> Self {
        Self {
            thread_stack_bytes: demand.thread_stack_bytes,
            allocator_fragmentation_bytes: demand.allocator_fragmentation_bytes,
            external_library_bytes: demand.external_library_bytes,
            fft_workspace_bytes: demand.fft_workspace_bytes,
            driver_bytes: demand.driver_bytes,
            jit_bytes: demand.jit_bytes,
            command_buffer_bytes: demand.command_buffer_bytes,
        }
    }

    const fn to_runtime(self) -> crate::RuntimeOverheadDemand {
        crate::RuntimeOverheadDemand {
            thread_stack_bytes: self.thread_stack_bytes,
            allocator_fragmentation_bytes: self.allocator_fragmentation_bytes,
            external_library_bytes: self.external_library_bytes,
            fft_workspace_bytes: self.fft_workspace_bytes,
            driver_bytes: self.driver_bytes,
            jit_bytes: self.jit_bytes,
            command_buffer_bytes: self.command_buffer_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IoBufferDemandProjection {
    source_read_ahead_bytes: u64,
    decode_bytes: u64,
    preparation_bytes: u64,
    host_to_device_transfer_bytes: u64,
    device_to_host_transfer_bytes: u64,
    spill_read_bytes: u64,
    spill_write_bytes: u64,
    serialization_bytes: u64,
    storage_manager_bytes: u64,
    tiled_column_writer_bytes: u64,
    scalar_column_writer_bytes: u64,
    writeback_bytes: u64,
    publication_bytes: u64,
    mapped_page_cache_bytes: u64,
}

impl IoBufferDemandProjection {
    const fn new(demand: crate::IoBufferDemand) -> Self {
        Self {
            source_read_ahead_bytes: demand.source_read_ahead_bytes,
            decode_bytes: demand.decode_bytes,
            preparation_bytes: demand.preparation_bytes,
            host_to_device_transfer_bytes: demand.host_to_device_transfer_bytes,
            device_to_host_transfer_bytes: demand.device_to_host_transfer_bytes,
            spill_read_bytes: demand.spill_read_bytes,
            spill_write_bytes: demand.spill_write_bytes,
            serialization_bytes: demand.serialization_bytes,
            storage_manager_bytes: demand.storage_manager_bytes,
            tiled_column_writer_bytes: demand.tiled_column_writer_bytes,
            scalar_column_writer_bytes: demand.scalar_column_writer_bytes,
            writeback_bytes: demand.writeback_bytes,
            publication_bytes: demand.publication_bytes,
            mapped_page_cache_bytes: demand.mapped_page_cache_bytes,
        }
    }

    const fn to_runtime(self) -> crate::IoBufferDemand {
        crate::IoBufferDemand {
            source_read_ahead_bytes: self.source_read_ahead_bytes,
            decode_bytes: self.decode_bytes,
            preparation_bytes: self.preparation_bytes,
            host_to_device_transfer_bytes: self.host_to_device_transfer_bytes,
            device_to_host_transfer_bytes: self.device_to_host_transfer_bytes,
            spill_read_bytes: self.spill_read_bytes,
            spill_write_bytes: self.spill_write_bytes,
            serialization_bytes: self.serialization_bytes,
            storage_manager_bytes: self.storage_manager_bytes,
            tiled_column_writer_bytes: self.tiled_column_writer_bytes,
            scalar_column_writer_bytes: self.scalar_column_writer_bytes,
            writeback_bytes: self.writeback_bytes,
            publication_bytes: self.publication_bytes,
            mapped_page_cache_bytes: self.mapped_page_cache_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CacheDemandProjection {
    hard_resident_bytes: u64,
    preferred_resident_bytes: u64,
}

impl CacheDemandProjection {
    const fn new(demand: crate::CacheDemand) -> Self {
        Self {
            hard_resident_bytes: demand.hard_resident_bytes,
            preferred_resident_bytes: demand.preferred_resident_bytes,
        }
    }

    const fn to_runtime(self) -> crate::CacheDemand {
        crate::CacheDemand {
            hard_resident_bytes: self.hard_resident_bytes,
            preferred_resident_bytes: self.preferred_resident_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MemoryDemandProjection {
    allocation_identity: String,
    hard_bytes: u64,
    preferred_bytes: u64,
    views: Vec<String>,
}

impl MemoryDemandProjection {
    fn new(demand: &crate::MemoryDemand) -> Self {
        Self {
            allocation_identity: stable_text(&demand.allocation_id),
            hard_bytes: demand.hard_bytes,
            preferred_bytes: demand.preferred_bytes,
            views: demand
                .views
                .iter()
                .map(|view| stable_text(view.as_str()))
                .collect(),
        }
    }

    fn to_runtime(&self) -> crate::MemoryDemand {
        crate::MemoryDemand {
            allocation_id: self.allocation_identity.clone(),
            hard_bytes: self.hard_bytes,
            preferred_bytes: self.preferred_bytes,
            views: self
                .views
                .iter()
                .cloned()
                .map(crate::CapacityViewId::new)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StorageDemandProjection {
    demand_identity: String,
    domain_identity: String,
    temporary_bytes: u64,
    staged_output_bytes: u64,
    final_output_bytes: u64,
    persistent_cache_bytes: u64,
    read_rate: CountDemandProjection,
    write_rate: CountDemandProjection,
    operations_rate: CountDemandProjection,
    queue_slots: CountDemandProjection,
}

impl StorageDemandProjection {
    fn new(demand: &crate::StorageDemand) -> Self {
        Self {
            demand_identity: stable_text(&demand.demand_id),
            domain_identity: stable_text(demand.domain.as_str()),
            temporary_bytes: demand.temporary_bytes,
            staged_output_bytes: demand.staged_output_bytes,
            final_output_bytes: demand.final_output_bytes,
            persistent_cache_bytes: demand.persistent_cache_bytes,
            read_rate: CountDemandProjection::new(demand.read_rate),
            write_rate: CountDemandProjection::new(demand.write_rate),
            operations_rate: CountDemandProjection::new(demand.operations_rate),
            queue_slots: CountDemandProjection::new(demand.queue_slots),
        }
    }

    fn to_runtime(&self) -> crate::StorageDemand {
        crate::StorageDemand {
            demand_id: self.demand_identity.clone(),
            domain: crate::StorageDomainId::new(self.domain_identity.clone()),
            temporary_bytes: self.temporary_bytes,
            staged_output_bytes: self.staged_output_bytes,
            final_output_bytes: self.final_output_bytes,
            persistent_cache_bytes: self.persistent_cache_bytes,
            read_rate: self.read_rate.to_runtime(),
            write_rate: self.write_rate.to_runtime(),
            operations_rate: self.operations_rate.to_runtime(),
            queue_slots: self.queue_slots.to_runtime(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RateDemandProjection {
    demand_identity: String,
    resource_identity: String,
    amount: CountDemandProjection,
}

impl RateDemandProjection {
    fn new(demand: &crate::RateDemand) -> Self {
        Self {
            demand_identity: stable_text(&demand.demand_id),
            resource_identity: stable_text(demand.resource.as_str()),
            amount: CountDemandProjection::new(demand.amount),
        }
    }

    fn to_runtime(&self) -> crate::RateDemand {
        crate::RateDemand {
            demand_id: self.demand_identity.clone(),
            resource: crate::RateResourceId::new(self.resource_identity.clone()),
            amount: self.amount.to_runtime(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct QueueDemandProjection {
    demand_identity: String,
    resource_identity: String,
    slots: CountDemandProjection,
}

impl QueueDemandProjection {
    fn new(demand: &crate::QueueDemand) -> Self {
        Self {
            demand_identity: stable_text(&demand.demand_id),
            resource_identity: stable_text(demand.resource.as_str()),
            slots: CountDemandProjection::new(demand.slots),
        }
    }

    fn to_runtime(&self) -> crate::QueueDemand {
        crate::QueueDemand {
            demand_id: self.demand_identity.clone(),
            resource: crate::QueueResourceId::new(self.resource_identity.clone()),
            slots: self.slots.to_runtime(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TransferDemandProjection {
    demand_identity: String,
    link_identity: String,
    rate: CountDemandProjection,
    queue_slots: CountDemandProjection,
}

impl TransferDemandProjection {
    fn new(demand: &crate::TransferDemand) -> Self {
        Self {
            demand_identity: stable_text(&demand.demand_id),
            link_identity: stable_text(demand.link.as_str()),
            rate: CountDemandProjection::new(demand.rate),
            queue_slots: CountDemandProjection::new(demand.queue_slots),
        }
    }

    fn to_runtime(&self) -> crate::TransferDemand {
        crate::TransferDemand {
            demand_id: self.demand_identity.clone(),
            link: crate::TransferLinkId::new(self.link_identity.clone()),
            rate: self.rate.to_runtime(),
            queue_slots: self.queue_slots.to_runtime(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AcceleratorDemandProjection {
    demand_identity: String,
    accelerator_identity: String,
    slots: CountDemandProjection,
    command_queue_slots: CountDemandProjection,
}

impl AcceleratorDemandProjection {
    fn new(demand: &crate::AcceleratorDemand) -> Self {
        Self {
            demand_identity: stable_text(&demand.demand_id),
            accelerator_identity: stable_text(demand.accelerator.as_str()),
            slots: CountDemandProjection::new(demand.slots),
            command_queue_slots: CountDemandProjection::new(demand.command_queue_slots),
        }
    }

    fn to_runtime(&self) -> crate::AcceleratorDemand {
        crate::AcceleratorDemand {
            demand_id: self.demand_identity.clone(),
            accelerator: crate::AcceleratorId::new(self.accelerator_identity.clone()),
            slots: self.slots.to_runtime(),
            command_queue_slots: self.command_queue_slots.to_runtime(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DemandEnvelopeProjection {
    host_memory_view: String,
    memory: Vec<MemoryDemandProjection>,
    workers: CountDemandProjection,
    overhead: RuntimeOverheadProjection,
    storage: Vec<StorageDemandProjection>,
    rates: Vec<RateDemandProjection>,
    caches: CacheDemandProjection,
    locks: CountDemandProjection,
    file_descriptors: CountDemandProjection,
    queues: Vec<QueueDemandProjection>,
    transfers: Vec<TransferDemandProjection>,
    accelerators: Vec<AcceleratorDemandProjection>,
    io_buffers: IoBufferDemandProjection,
}

impl DemandEnvelopeProjection {
    fn new(demand: &crate::DemandEnvelope) -> Self {
        Self {
            host_memory_view: stable_text(demand.host_memory_view.as_str()),
            memory: demand
                .memory
                .iter()
                .map(MemoryDemandProjection::new)
                .collect(),
            workers: CountDemandProjection::new(demand.workers),
            overhead: RuntimeOverheadProjection::new(demand.overhead),
            storage: demand
                .storage
                .iter()
                .map(StorageDemandProjection::new)
                .collect(),
            rates: demand.rates.iter().map(RateDemandProjection::new).collect(),
            caches: CacheDemandProjection::new(demand.caches),
            locks: CountDemandProjection::new(demand.locks),
            file_descriptors: CountDemandProjection::new(demand.file_descriptors),
            queues: demand
                .queues
                .iter()
                .map(QueueDemandProjection::new)
                .collect(),
            transfers: demand
                .transfers
                .iter()
                .map(TransferDemandProjection::new)
                .collect(),
            accelerators: demand
                .accelerators
                .iter()
                .map(AcceleratorDemandProjection::new)
                .collect(),
            io_buffers: IoBufferDemandProjection::new(demand.io_buffers),
        }
    }

    fn to_runtime(&self) -> crate::DemandEnvelope {
        crate::DemandEnvelope {
            host_memory_view: crate::CapacityViewId::new(self.host_memory_view.clone()),
            memory: self
                .memory
                .iter()
                .map(MemoryDemandProjection::to_runtime)
                .collect(),
            workers: self.workers.to_runtime(),
            overhead: self.overhead.to_runtime(),
            storage: self
                .storage
                .iter()
                .map(StorageDemandProjection::to_runtime)
                .collect(),
            rates: self
                .rates
                .iter()
                .map(RateDemandProjection::to_runtime)
                .collect(),
            caches: self.caches.to_runtime(),
            locks: self.locks.to_runtime(),
            file_descriptors: self.file_descriptors.to_runtime(),
            queues: self
                .queues
                .iter()
                .map(QueueDemandProjection::to_runtime)
                .collect(),
            transfers: self
                .transfers
                .iter()
                .map(TransferDemandProjection::to_runtime)
                .collect(),
            accelerators: self
                .accelerators
                .iter()
                .map(AcceleratorDemandProjection::to_runtime)
                .collect(),
            io_buffers: self.io_buffers.to_runtime(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ResourceHeadroomProjection {
    memory_bytes: BTreeMap<String, u64>,
    workers: u64,
    storage_bytes: BTreeMap<String, u64>,
    rates_per_second: BTreeMap<String, u64>,
    cache_bytes: u64,
    locks: u64,
    file_descriptors: u64,
    queue_slots: BTreeMap<String, u64>,
    accelerator_slots: BTreeMap<String, u64>,
}

impl ResourceHeadroomProjection {
    fn new(headroom: &crate::ResourceHeadroom) -> Self {
        Self {
            memory_bytes: headroom
                .memory_bytes
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            workers: headroom.workers,
            storage_bytes: headroom
                .storage_bytes
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            rates_per_second: headroom
                .rates_per_second
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            cache_bytes: headroom.cache_bytes,
            locks: headroom.locks,
            file_descriptors: headroom.file_descriptors,
            queue_slots: headroom
                .queue_slots
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
            accelerator_slots: headroom
                .accelerator_slots
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
        }
    }

    fn to_runtime(&self) -> crate::ResourceHeadroom {
        crate::ResourceHeadroom {
            memory_bytes: self
                .memory_bytes
                .iter()
                .map(|(identity, amount)| (crate::CapacityDomainId::new(identity.clone()), *amount))
                .collect(),
            workers: self.workers,
            storage_bytes: self
                .storage_bytes
                .iter()
                .map(|(identity, amount)| (crate::StorageDomainId::new(identity.clone()), *amount))
                .collect(),
            rates_per_second: self
                .rates_per_second
                .iter()
                .map(|(identity, amount)| (crate::RateResourceId::new(identity.clone()), *amount))
                .collect(),
            cache_bytes: self.cache_bytes,
            locks: self.locks,
            file_descriptors: self.file_descriptors,
            queue_slots: self
                .queue_slots
                .iter()
                .map(|(identity, amount)| (crate::QueueResourceId::new(identity.clone()), *amount))
                .collect(),
            accelerator_slots: self
                .accelerator_slots
                .iter()
                .map(|(identity, amount)| (crate::AcceleratorId::new(identity.clone()), *amount))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ScalingMetadataProjection {
    minimum_workers: u64,
    maximum_workers: u64,
    maximum_batch_size: u64,
    maximum_tile_width: u64,
    maximum_tile_height: u64,
    maximum_slab_depth: u64,
    memory_bytes_per_worker: BTreeMap<String, u64>,
}

impl ScalingMetadataProjection {
    fn new(scaling: &crate::ScalingMetadata) -> Self {
        Self {
            minimum_workers: scaling.minimum_workers,
            maximum_workers: scaling.maximum_workers,
            maximum_batch_size: scaling.maximum_batch_size,
            maximum_tile_width: scaling.maximum_tile_width,
            maximum_tile_height: scaling.maximum_tile_height,
            maximum_slab_depth: scaling.maximum_slab_depth,
            memory_bytes_per_worker: scaling
                .memory_bytes_per_worker
                .iter()
                .map(|(identity, amount)| (stable_text(identity.as_str()), *amount))
                .collect(),
        }
    }

    fn to_runtime(&self) -> crate::ScalingMetadata {
        crate::ScalingMetadata {
            minimum_workers: self.minimum_workers,
            maximum_workers: self.maximum_workers,
            maximum_batch_size: self.maximum_batch_size,
            maximum_tile_width: self.maximum_tile_width,
            maximum_tile_height: self.maximum_tile_height,
            maximum_slab_depth: self.maximum_slab_depth,
            memory_bytes_per_worker: self
                .memory_bytes_per_worker
                .iter()
                .map(|(identity, amount)| (crate::CapacityDomainId::new(identity.clone()), *amount))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DemandAlternativeProjection {
    alternative_identity: String,
    supported_capabilities: Vec<String>,
    demand: DemandEnvelopeProjection,
    headroom: ResourceHeadroomProjection,
    scaling: ScalingMetadataProjection,
    quiescence_points: Vec<String>,
}

impl DemandAlternativeProjection {
    fn new(alternative: &DemandAlternative) -> Self {
        Self {
            alternative_identity: stable_text(alternative.id.as_str()),
            supported_capabilities: alternative
                .capabilities
                .supported
                .iter()
                .map(|identity| stable_text(identity.as_str()))
                .collect(),
            demand: DemandEnvelopeProjection::new(&alternative.demand),
            headroom: ResourceHeadroomProjection::new(&alternative.headroom),
            scaling: ScalingMetadataProjection::new(&alternative.scaling),
            quiescence_points: alternative
                .quiescence_points
                .iter()
                .map(|point| quiescence(*point).to_string())
                .collect(),
        }
    }

    fn to_runtime(&self) -> DemandAlternative {
        DemandAlternative {
            id: crate::AlternativeId::new(self.alternative_identity.clone()),
            capabilities: crate::CapabilityPredicate {
                supported: self
                    .supported_capabilities
                    .iter()
                    .cloned()
                    .map(CapabilityId::new)
                    .collect(),
            },
            demand: self.demand.to_runtime(),
            headroom: self.headroom.to_runtime(),
            scaling: self.scaling.to_runtime(),
            quiescence_points: self
                .quiescence_points
                .iter()
                .map(|point| parse_quiescence(point))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NodeProjection {
    node_id: String,
    kind: String,
    domain: String,
    implementation: String,
    dependencies: Vec<String>,
    claims: Vec<ClaimProjection>,
    allocation_uses: Vec<AllocationUseProjection>,
    fences: Vec<String>,
    quiescence_after: Vec<String>,
    predicted_elapsed_nanos: u64,
    actual_elapsed_nanos: Option<u64>,
    io: Vec<IoProjection>,
    status: ReceiptStatus,
}

impl NodeProjection {
    fn new(node: &crate::WorkNode, prediction: &crate::StagePrediction) -> Self {
        Self {
            node_id: stable_text(node.id.as_str()),
            kind: work_kind(node.kind).to_string(),
            domain: work_domain(&node.domain),
            implementation: stable_text(node.implementation.as_str()),
            dependencies: node.dependencies.iter().map(dependency).collect(),
            claims: node
                .claims
                .iter()
                .map(|claim| ClaimProjection {
                    resource: lease_resource(&claim.resource),
                    amount: claim.amount,
                    lifetime: claim_lifetime(&claim.lifetime),
                    actual_peak: None,
                })
                .collect(),
            allocation_uses: node
                .allocations
                .iter()
                .map(|usage| AllocationUseProjection {
                    generation_identity: stable_text(usage.allocation.as_str()),
                    lifetime: claim_lifetime(&usage.lifetime),
                })
                .collect(),
            fences: node
                .fences
                .iter()
                .map(|kind| fence_kind(*kind).to_string())
                .collect(),
            quiescence_after: node
                .quiescence_after
                .iter()
                .map(|point| quiescence(*point).to_string())
                .collect(),
            predicted_elapsed_nanos: prediction.elapsed_nanos(),
            actual_elapsed_nanos: None,
            io: prediction
                .io()
                .iter()
                .map(|item| IoProjection {
                    kind: io_buffer(item.kind()).to_string(),
                    predicted_bytes: item.bytes(),
                    predicted_operations: item.operations(),
                    actual_bytes: None,
                    actual_operations: None,
                })
                .collect(),
            status: ReceiptStatus::Planned,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AllocationUseProjection {
    generation_identity: String,
    lifetime: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IoProjection {
    kind: String,
    predicted_bytes: u64,
    predicted_operations: u64,
    actual_bytes: Option<u64>,
    actual_operations: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ClaimProjection {
    resource: String,
    amount: u64,
    lifetime: String,
    actual_peak: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FenceProjection {
    node_id: String,
    kind: String,
    status: ReceiptStatus,
    actual_elapsed_nanos: Option<u64>,
}

impl FenceProjection {
    fn new(node: &WorkNodeId, kind: FenceKind) -> Self {
        Self {
            node_id: stable_text(node.as_str()),
            kind: fence_kind(kind).to_string(),
            status: ReceiptStatus::Planned,
            actual_elapsed_nanos: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ArtifactProjection {
    artifact_identity: String,
    node_id: String,
    role: String,
    cache_identity: Option<String>,
    observed_identity: Option<String>,
    disposition: Option<ArtifactDispositionProjection>,
    actual_bytes: Option<u64>,
    path_identity: Option<String>,
}

impl ArtifactProjection {
    fn new(artifact: &crate::PlannedArtifact) -> Self {
        Self {
            artifact_identity: hex(&artifact.identity().as_bytes()),
            node_id: stable_text(artifact.node().as_str()),
            role: match artifact.role() {
                ArtifactRole::Input => "input",
                ArtifactRole::Prepared => "prepared",
                ArtifactRole::Cache => "cache",
                ArtifactRole::Output => "output",
            }
            .to_string(),
            cache_identity: artifact
                .cache_identity()
                .map(|identity| hex(&identity.as_bytes())),
            observed_identity: None,
            disposition: None,
            actual_bytes: None,
            path_identity: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactDispositionProjection {
    Built,
    Loaded,
    Reused,
    RejectedStale,
    Staged,
    Published,
}

impl From<ArtifactDisposition> for ArtifactDispositionProjection {
    fn from(value: ArtifactDisposition) -> Self {
        match value {
            ArtifactDisposition::Built => Self::Built,
            ArtifactDisposition::Loaded => Self::Loaded,
            ArtifactDisposition::Reused => Self::Reused,
            ArtifactDisposition::RejectedStale => Self::RejectedStale,
            ArtifactDisposition::Staged => Self::Staged,
            ArtifactDisposition::Published => Self::Published,
        }
    }
}

impl From<ArtifactDispositionProjection> for ArtifactDisposition {
    fn from(value: ArtifactDispositionProjection) -> Self {
        match value {
            ArtifactDispositionProjection::Built => Self::Built,
            ArtifactDispositionProjection::Loaded => Self::Loaded,
            ArtifactDispositionProjection::Reused => Self::Reused,
            ArtifactDispositionProjection::RejectedStale => Self::RejectedStale,
            ArtifactDispositionProjection::Staged => Self::Staged,
            ArtifactDispositionProjection::Published => Self::Published,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AllocationProjection {
    generation_identity: String,
    bytes: u64,
    purpose: String,
    physical_slot: String,
    acquire_at: String,
    release_after: Vec<String>,
    compatibility: CompatibilityProjection,
}

impl AllocationProjection {
    fn new(allocation: &crate::LogicalAllocation) -> Self {
        Self {
            generation_identity: stable_text(allocation.id.as_str()),
            bytes: allocation.bytes,
            purpose: match allocation.purpose {
                AllocationPurpose::Data => "data".to_string(),
                AllocationPurpose::IoBuffer(kind) => format!("io_buffer:{}", io_buffer(kind)),
            },
            physical_slot: stable_text(allocation.physical_slot.as_str()),
            acquire_at: stable_text(allocation.lifetime.acquire_at.as_str()),
            release_after: allocation
                .lifetime
                .release_after
                .iter()
                .map(dependency)
                .collect(),
            compatibility: CompatibilityProjection::new(&allocation.compatibility),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalSlotProjection {
    slot_identity: String,
    lease_resource: String,
    capacity_bytes: u64,
    compatibility: CompatibilityProjection,
}

impl PhysicalSlotProjection {
    fn new(slot: &crate::PhysicalSlot) -> Self {
        Self {
            slot_identity: stable_text(slot.id.as_str()),
            lease_resource: lease_resource(&slot.lease_resource),
            capacity_bytes: slot.capacity_bytes,
            compatibility: CompatibilityProjection::new(&slot.compatibility),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CompatibilityProjection {
    memory_domain: String,
    views: Vec<String>,
    alignment_bytes: u64,
    storage_mode: String,
    layout: String,
    initialization: String,
    access: String,
}

impl CompatibilityProjection {
    fn new(compatibility: &crate::SlotCompatibility) -> Self {
        Self {
            memory_domain: stable_text(compatibility.memory_domain.as_str()),
            views: compatibility
                .views
                .iter()
                .map(|view| stable_text(view.as_str()))
                .collect(),
            alignment_bytes: compatibility.alignment_bytes,
            storage_mode: match compatibility.storage_mode {
                StorageMode::Host => "host",
                StorageMode::MetalShared => "metal_shared",
            }
            .to_string(),
            layout: stable_text(compatibility.layout.as_str()),
            initialization: match compatibility.initialization {
                InitializationPolicy::Preserve => "preserve",
                InitializationPolicy::ZeroBeforeRead => "zero_before_read",
                InitializationPolicy::OverwriteBeforeRead => "overwrite_before_read",
            }
            .to_string(),
            access: match compatibility.access {
                crate::AllocationAccess::ReadOnly => "read_only",
                crate::AllocationAccess::ReadWrite => "read_write",
                crate::AllocationAccess::WriteOnly => "write_only",
            }
            .to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ExecutionKnobsProjection {
    workers: u64,
    batch_size: u64,
    tile_width: u64,
    tile_height: u64,
    slab_depth: u64,
    io_depth: u64,
    cache_retention_bytes: u64,
    fusion: bool,
    recomputation: bool,
    spill: bool,
    prefetch: bool,
}

impl ExecutionKnobsProjection {
    const fn new(knobs: &ExecutionKnobs) -> Self {
        Self {
            workers: knobs.workers,
            batch_size: knobs.batch_size,
            tile_width: knobs.tile_width,
            tile_height: knobs.tile_height,
            slab_depth: knobs.slab_depth,
            io_depth: knobs.io_depth,
            cache_retention_bytes: knobs.cache_retention_bytes,
            fusion: knobs.fusion,
            recomputation: knobs.recomputation,
            spill: knobs.spill,
            prefetch: knobs.prefetch,
        }
    }

    const fn to_runtime(self) -> ExecutionKnobs {
        ExecutionKnobs {
            workers: self.workers,
            batch_size: self.batch_size,
            tile_width: self.tile_width,
            tile_height: self.tile_height,
            slab_depth: self.slab_depth,
            io_depth: self.io_depth,
            cache_retention_bytes: self.cache_retention_bytes,
            fusion: self.fusion,
            recomputation: self.recomputation,
            spill: self.spill,
            prefetch: self.prefetch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AdaptationProjection {
    adaptation_identity: String,
    from: ExecutionKnobsProjection,
    to: ExecutionKnobsProjection,
    quiescence: String,
    applied_revision: Option<u64>,
}

impl AdaptationProjection {
    fn new(adaptation: &AdaptationTransition) -> Self {
        Self {
            adaptation_identity: stable_text(adaptation.id.as_str()),
            from: ExecutionKnobsProjection::new(&adaptation.from),
            to: ExecutionKnobsProjection::new(&adaptation.to),
            quiescence: quiescence(adaptation.at).to_string(),
            applied_revision: None,
        }
    }

    fn to_runtime(&self) -> ReceiptAdaptation {
        ReceiptAdaptation {
            transition: AdaptationTransition {
                id: AdaptationId::new(self.adaptation_identity.clone()),
                from: self.from.to_runtime(),
                to: self.to.to_runtime(),
                at: parse_quiescence(&self.quiescence),
            },
            applied_revision: self.applied_revision,
        }
    }
}

pub(crate) struct ReceiptRecorder<'store> {
    store: &'store ExecutionReceiptStore,
    body: ReceiptBody,
    active_nodes: BTreeMap<String, Instant>,
    active_fences: BTreeMap<(String, String), Instant>,
    pending_publications: BTreeSet<String>,
    terminal: bool,
}

pub(crate) struct PreparedPublicationReceipt<'store> {
    parent: PathBuf,
    path: PathBuf,
    terminal: NamedTempFile,
    completed: ReceiptBody,
    _mutation: MutexGuard<'store, ()>,
}

impl<'store> ReceiptRecorder<'store> {
    pub(crate) fn attempt_id(&self) -> ExecutionAttemptId {
        self.body.attempt()
    }

    pub(crate) fn work_started(&mut self, node: &WorkNodeId) -> Result<(), ReceiptError> {
        let node_id = stable_text(node.as_str());
        let item = self
            .body
            .plan
            .nodes
            .iter_mut()
            .find(|item| item.node_id == node_id)
            .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "work node" })?;
        item.status = ReceiptStatus::Running;
        self.active_nodes.insert(node_id, Instant::now());
        self.checkpoint()
    }

    pub(crate) fn work_completed(
        &mut self,
        node: &WorkNodeId,
        measurements: &WorkMeasurements,
    ) -> Result<(), ReceiptError> {
        self.record_measurements(node, measurements)?;
        self.finish_node(node, ReceiptStatus::Completed)?;
        self.checkpoint()
    }

    pub(crate) fn work_failed(&mut self, node: &WorkNodeId) -> Result<(), ReceiptError> {
        self.finish_node(node, ReceiptStatus::Failed)?;
        self.checkpoint()
    }

    pub(crate) fn work_failed_with_measurements(
        &mut self,
        node: &WorkNodeId,
        measurements: &WorkMeasurements,
    ) -> Result<(), ReceiptError> {
        self.record_measurements(node, measurements)?;
        self.finish_node(node, ReceiptStatus::Failed)?;
        self.checkpoint()
    }

    pub(crate) fn fences_launched(&mut self, node: &WorkNodeId) -> Result<(), ReceiptError> {
        let node_id = stable_text(node.as_str());
        let expected = self
            .body
            .plan
            .nodes
            .iter()
            .find(|item| item.node_id == node_id)
            .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "work node" })?
            .fences
            .len();
        let mut launched = 0;
        for fence in self
            .body
            .plan
            .fences
            .iter_mut()
            .filter(|fence| fence.node_id == node_id)
        {
            fence.status = ReceiptStatus::Running;
            self.active_fences
                .insert((fence.node_id.clone(), fence.kind.clone()), Instant::now());
            launched += 1;
        }
        if launched != expected {
            return Err(ReceiptError::UnlistedPlanEvidence {
                kind: "asynchronous fence",
            });
        }
        self.checkpoint()
    }

    pub(crate) fn fence_completed(&mut self, fence: &FenceId) -> Result<(), ReceiptError> {
        self.finish_fence(fence, ReceiptStatus::Completed)?;
        self.checkpoint()
    }

    pub(crate) fn fence_failed(&mut self, fence: &FenceId) -> Result<(), ReceiptError> {
        self.finish_fence(fence, ReceiptStatus::Failed)?;
        self.checkpoint()
    }

    pub(crate) fn adaptation_applied(
        &mut self,
        adaptation: &AdaptationId,
    ) -> Result<(), ReceiptError> {
        let item = self
            .body
            .plan
            .adaptations
            .iter_mut()
            .find(|item| item.adaptation_identity == stable_text(adaptation.as_str()))
            .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "adaptation" })?;
        item.applied_revision = Some(self.body.revision.saturating_add(1));
        self.checkpoint()
    }

    pub(crate) fn finish(
        &mut self,
        status: ReceiptStatus,
        failure: Option<ReceiptFailure>,
    ) -> Result<(), ReceiptError> {
        for node in &mut self.body.plan.nodes {
            node.status = terminal_item_status(status, node.status)?;
        }
        for fence in &mut self.body.plan.fences {
            fence.status = terminal_item_status(status, fence.status)?;
        }
        self.body.status = status;
        self.body.failure = failure.map(ReceiptFailure::projection);
        self.body.finished_unix_millis = Some(now_millis());
        self.body.revision = self.body.revision.saturating_add(1);
        self.store.persist(&self.body, false)?;
        self.terminal = true;
        Ok(())
    }

    pub(crate) fn prepare_publication(
        &mut self,
    ) -> Result<PreparedPublicationReceipt<'store>, ReceiptError> {
        if self.body.status != ReceiptStatus::Running
            || !self.active_nodes.is_empty()
            || !self.active_fences.is_empty()
            || self
                .body
                .plan
                .nodes
                .iter()
                .any(|node| node.status != ReceiptStatus::Completed)
            || self
                .body
                .plan
                .fences
                .iter()
                .any(|fence| fence.status != ReceiptStatus::Completed)
        {
            return Err(ReceiptError::IncompleteSuccess);
        }
        let expected_publications = self
            .body
            .plan
            .artifacts
            .iter()
            .filter(|artifact| artifact.role == "output")
            .map(|artifact| artifact.artifact_identity.clone())
            .collect::<BTreeSet<_>>();
        let staged_publications = self
            .body
            .plan
            .artifacts
            .iter()
            .filter(|artifact| artifact.disposition == Some(ArtifactDispositionProjection::Staged))
            .map(|artifact| artifact.artifact_identity.clone())
            .collect::<BTreeSet<_>>();
        if self.pending_publications != expected_publications
            || staged_publications != expected_publications
        {
            return Err(ReceiptError::IncompleteSuccess);
        }

        let mut prepared = self.body.clone();
        prepared.status = ReceiptStatus::PublicationPrepared;
        prepared.failure = None;
        prepared.finished_unix_millis = None;
        prepared.revision = prepared.revision.saturating_add(1);

        let mut completed = prepared.clone();
        completed.status = ReceiptStatus::Completed;
        completed.finished_unix_millis = Some(now_millis());
        completed.revision = completed.revision.saturating_add(1);
        for artifact in completed
            .plan
            .artifacts
            .iter_mut()
            .filter(|artifact| expected_publications.contains(&artifact.artifact_identity))
        {
            artifact.disposition = Some(ArtifactDispositionProjection::Published);
        }

        let publication = self.store.prepare_publication(&prepared, completed)?;
        self.body = prepared;
        Ok(publication)
    }

    pub(crate) fn complete_publication(&mut self, prepared: PreparedPublicationReceipt<'store>) {
        self.terminal = true;
        if let Ok(completed) = self.store.complete_publication(prepared) {
            self.body = completed;
            self.pending_publications.clear();
        }
    }

    pub(crate) const fn is_terminal(&self) -> bool {
        self.terminal
    }

    fn checkpoint(&mut self) -> Result<(), ReceiptError> {
        self.body.revision = self.body.revision.saturating_add(1);
        self.store.persist(&self.body, false)
    }

    fn finish_node(
        &mut self,
        node: &WorkNodeId,
        status: ReceiptStatus,
    ) -> Result<(), ReceiptError> {
        let node_id = stable_text(node.as_str());
        let elapsed = self
            .active_nodes
            .remove(&node_id)
            .map(|started| elapsed_nanos(started.elapsed()));
        let item = self
            .body
            .plan
            .nodes
            .iter_mut()
            .find(|item| item.node_id == node_id)
            .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "work node" })?;
        item.status = status;
        if elapsed.is_some() {
            item.actual_elapsed_nanos = elapsed;
        }
        Ok(())
    }

    fn record_measurements(
        &mut self,
        node: &WorkNodeId,
        measurements: &WorkMeasurements,
    ) -> Result<(), ReceiptError> {
        let node_id = stable_text(node.as_str());
        {
            let item = self
                .body
                .plan
                .nodes
                .iter_mut()
                .find(|item| item.node_id == node_id)
                .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "work node" })?;
            for measurement in measurements.resources() {
                let resource = lease_resource(measurement.resource());
                let lifetime = claim_lifetime(measurement.lifetime());
                let claim = item
                    .claims
                    .iter_mut()
                    .find(|claim| claim.resource == resource && claim.lifetime == lifetime)
                    .ok_or(ReceiptError::UnlistedPlanEvidence {
                        kind: "resource measurement",
                    })?;
                claim.actual_peak = Some(measurement.peak());
            }
            for measurement in measurements.io() {
                let kind = io_buffer(measurement.kind());
                let io = item.io.iter_mut().find(|item| item.kind == kind).ok_or(
                    ReceiptError::UnlistedPlanEvidence {
                        kind: "I/O measurement",
                    },
                )?;
                io.actual_bytes = Some(measurement.bytes());
                io.actual_operations = Some(measurement.operations());
            }
        }
        for measurement in measurements.artifacts() {
            self.record_artifact(*measurement)?;
        }
        Ok(())
    }

    fn record_artifact(&mut self, measurement: ArtifactMeasurement) -> Result<(), ReceiptError> {
        let planned = hex(&measurement.planned_identity().as_bytes());
        let artifact = self
            .body
            .plan
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.artifact_identity == planned)
            .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "artifact" })?;
        artifact.observed_identity = measurement
            .observed_identity()
            .map(|identity| hex(&identity.as_bytes()));
        if measurement.disposition() == ArtifactDisposition::Staged {
            artifact.disposition = Some(ArtifactDispositionProjection::Staged);
            self.pending_publications.insert(planned);
        } else {
            artifact.disposition = Some(measurement.disposition().into());
        }
        artifact.actual_bytes = Some(measurement.bytes());
        artifact.path_identity = measurement.path().map(|path| hex(&path.as_bytes()));
        Ok(())
    }

    fn finish_fence(&mut self, fence: &FenceId, status: ReceiptStatus) -> Result<(), ReceiptError> {
        let node_id = stable_text(fence.node().as_str());
        let kind = fence_kind(fence.kind()).to_string();
        let key = (node_id.clone(), kind.clone());
        let elapsed = self
            .active_fences
            .remove(&key)
            .map(|started| elapsed_nanos(started.elapsed()));
        let item = self
            .body
            .plan
            .fences
            .iter_mut()
            .find(|item| item.node_id == node_id && item.kind == kind)
            .ok_or(ReceiptError::UnlistedPlanEvidence {
                kind: "asynchronous fence",
            })?;
        item.status = status;
        item.actual_elapsed_nanos = elapsed;
        Ok(())
    }
}

impl Drop for ReceiptRecorder<'_> {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        let node = self
            .body
            .plan
            .nodes
            .iter()
            .find(|node| node.status == ReceiptStatus::Running)
            .map(|node| WorkNodeId::new(node.node_id.clone()));
        let _ = self.finish(
            ReceiptStatus::Aborted,
            Some(ReceiptFailure::new(
                ReceiptFailureKind::Interrupted,
                node,
                None,
            )),
        );
    }
}

fn terminal_item_status(
    overall: ReceiptStatus,
    current: ReceiptStatus,
) -> Result<ReceiptStatus, ReceiptError> {
    match (overall, current) {
        (ReceiptStatus::Completed, ReceiptStatus::Completed) => Ok(current),
        (ReceiptStatus::Completed, _) => Err(ReceiptError::IncompleteSuccess),
        (ReceiptStatus::PublicationPrepared, _) => Err(ReceiptError::IncompleteSuccess),
        (ReceiptStatus::Cancelled, ReceiptStatus::Planned | ReceiptStatus::Running) => {
            Ok(ReceiptStatus::Cancelled)
        }
        (ReceiptStatus::Failed, ReceiptStatus::Planned) => Ok(ReceiptStatus::Cancelled),
        (ReceiptStatus::Failed, ReceiptStatus::Running) => Ok(ReceiptStatus::Failed),
        (ReceiptStatus::Mutation | ReceiptStatus::Infeasible, ReceiptStatus::Planned) => {
            Ok(ReceiptStatus::NotStarted)
        }
        (ReceiptStatus::Mutation | ReceiptStatus::Infeasible, ReceiptStatus::Running) => {
            Ok(ReceiptStatus::Failed)
        }
        (ReceiptStatus::Aborted, ReceiptStatus::Planned) => Ok(ReceiptStatus::NotStarted),
        (ReceiptStatus::Aborted, ReceiptStatus::Running) => Ok(ReceiptStatus::Aborted),
        (_, _) => Ok(current),
    }
}

/// Receipt persistence, schema, integrity, or retention failure.
#[derive(Debug)]
pub enum ReceiptError {
    /// A retention ceiling was zero.
    InvalidRetention,
    /// Another live store already owns this canonical root with different ceilings.
    ConflictingRetention,
    /// The configured root is not a directory.
    InvalidStore,
    /// Routing evidence was empty, unordered, or not canonical.
    InvalidRouteEvidence,
    /// An attempt identity already owns a receipt.
    AttemptAlreadyExists,
    /// The reopened file did not match the requested attempt.
    AttemptMismatch,
    /// The receipt schema is unknown.
    UnsupportedSchema {
        /// Persisted schema name.
        name: String,
        /// Persisted schema version.
        version: u32,
    },
    /// The content checksum did not match the receipt payload.
    IntegrityMismatch,
    /// A successful outcome omitted or failed a plan node.
    IncompleteSuccess,
    /// Receipt recording was asked to persist evidence absent from the plan.
    UnlistedPlanEvidence {
        /// Stable evidence category; raw identities and paths are never included.
        kind: &'static str,
    },
    /// Active receipts leave no capacity within the configured retention ceiling.
    RetentionExceeded,
    /// Local filesystem operation failed.
    Io {
        /// Stable operation class without retaining a path.
        action: &'static str,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// JSON encoding or decoding failed.
    Json {
        /// JSON error.
        source: serde_json::Error,
    },
}

impl fmt::Display for ReceiptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRetention => {
                formatter.write_str("receipt retention ceilings must be positive")
            }
            Self::ConflictingRetention => formatter.write_str(
                "receipt store root already has different process-wide retention ceilings",
            ),
            Self::InvalidStore => formatter.write_str("receipt store root is not a directory"),
            Self::InvalidRouteEvidence => {
                formatter.write_str("execution route evidence is not canonical")
            }
            Self::AttemptAlreadyExists => {
                formatter.write_str("execution attempt already has a receipt")
            }
            Self::AttemptMismatch => {
                formatter.write_str("receipt attempt identity does not match its file identity")
            }
            Self::UnsupportedSchema { name, version } => {
                write!(
                    formatter,
                    "unsupported receipt schema {name} version {version}"
                )
            }
            Self::IntegrityMismatch => {
                formatter.write_str("execution receipt integrity check failed")
            }
            Self::IncompleteSuccess => {
                formatter.write_str("successful receipt does not contain every completed plan node")
            }
            Self::UnlistedPlanEvidence { kind } => {
                write!(formatter, "receipt rejected unlisted {kind}")
            }
            Self::RetentionExceeded => {
                formatter.write_str("receipt retention ceiling cannot admit more active evidence")
            }
            Self::Io { action, source } => write!(formatter, "{action}: {source}"),
            Self::Json { source } => write!(formatter, "execution receipt JSON failure: {source}"),
        }
    }
}

impl Error for ReceiptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Json { source } => Some(source),
            _ => None,
        }
    }
}

fn validate_body(body: &ReceiptBody) -> Result<(), ReceiptError> {
    for digest in [
        &body.attempt_identity,
        &body.build_identity,
        &body.problem.problem_identity,
        &body.problem.problem_identity_basis,
        &body.problem.geometry_identity,
        &body.problem.product_graph.identity,
        &body.problem.observation_identity,
        &body.problem.numerics_identity,
        &body.problem.model_lifecycle.identity,
        &body.problem.model_lifecycle.numerics_identity,
        &body.problem.model_lifecycle.target_shape_identity,
        &body.problem.model_lifecycle.reprojection.identity,
        &body
            .problem
            .model_lifecycle
            .reprojection
            .product_graph_identity,
        &body.problem.model_lifecycle.reprojection.numerics_identity,
        &body.plan.plan_identity,
        &body.plan.dag_identity,
        &body.plan.product_graph_identity,
        &body.plan.implementation_registry_identity,
        &body.plan.resource_policy_identity,
        &body.plan.cost_model_identity,
    ] {
        if !is_digest(digest) {
            return Err(ReceiptError::IntegrityMismatch);
        }
    }
    for reference in &body.problem.reference_identities {
        require_integrity(
            reference_kind_is_valid(&reference.kind) && is_digest(&reference.identity),
        )?;
    }
    let reference_kinds = body
        .problem
        .reference_identities
        .iter()
        .map(|reference| reference.kind.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(reference_kinds.len() == body.problem.reference_identities.len())?;
    body.problem.model_lifecycle.validate(
        &body.problem.product_graph.identity,
        &body.problem.numerics_identity,
        &body.problem.effective,
    )?;
    body.problem
        .model_identity
        .validate_against(&body.problem.model_lifecycle.input, &body.problem.effective)?;
    require_integrity(validate_compiled_problem_identity(
        parse_digest(&body.problem.problem_identity),
        LogicalIdentity::from_sha256(parse_digest(&body.problem.problem_identity_basis)),
        body.problem.model_identity.to_runtime(),
        LogicalIdentity::from_sha256(parse_digest(&body.problem.model_lifecycle.identity)),
    ))?;
    validate_problem_evidence(&body.problem)?;
    require_integrity(body.problem.product_graph.identity == body.plan.product_graph_identity)?;
    validate_route_projection(&body.route)?;

    require_integrity(body.revision > 0)?;
    require_integrity(!matches!(
        body.status,
        ReceiptStatus::Planned | ReceiptStatus::NotStarted
    ))?;
    require_integrity(body.status.is_terminal() == body.finished_unix_millis.is_some())?;
    match body.status {
        ReceiptStatus::Completed | ReceiptStatus::PublicationPrepared => {
            require_integrity(body.failure.is_none())?;
            require_integrity(
                body.plan
                    .nodes
                    .iter()
                    .all(|node| node.status == ReceiptStatus::Completed)
                    && body
                        .plan
                        .fences
                        .iter()
                        .all(|fence| fence.status == ReceiptStatus::Completed),
            )?;
        }
        ReceiptStatus::Failed
        | ReceiptStatus::Aborted
        | ReceiptStatus::Mutation
        | ReceiptStatus::Infeasible => require_integrity(body.failure.is_some())?,
        ReceiptStatus::Cancelled | ReceiptStatus::Running => {}
        ReceiptStatus::Planned | ReceiptStatus::NotStarted => unreachable!(),
    }
    if body.status.is_terminal() {
        require_integrity(
            body.plan.nodes.iter().all(|node| node.status.is_terminal())
                && body
                    .plan
                    .fences
                    .iter()
                    .all(|fence| fence.status.is_terminal()),
        )?;
    }

    validate_plan_projection(&body.problem.product_graph, &body.plan, body.revision)?;
    let expected_publications = body
        .plan
        .artifacts
        .iter()
        .filter(|artifact| artifact.role == "output")
        .map(|artifact| artifact.artifact_identity.as_str())
        .collect::<BTreeSet<_>>();
    let published = body
        .plan
        .artifacts
        .iter()
        .filter(|artifact| artifact.disposition == Some(ArtifactDispositionProjection::Published))
        .map(|artifact| artifact.artifact_identity.as_str())
        .collect::<BTreeSet<_>>();
    let staged = body
        .plan
        .artifacts
        .iter()
        .filter(|artifact| artifact.disposition == Some(ArtifactDispositionProjection::Staged))
        .map(|artifact| artifact.artifact_identity.as_str())
        .collect::<BTreeSet<_>>();
    match body.status {
        ReceiptStatus::Completed => {
            require_integrity(published == expected_publications && staged.is_empty())?;
        }
        ReceiptStatus::PublicationPrepared => {
            require_integrity(staged == expected_publications && published.is_empty())?;
        }
        _ => {
            require_integrity(published.is_empty() && staged.is_subset(&expected_publications))?;
        }
    }
    if let Some(failure) = &body.failure {
        if let Some(node) = &failure.node_id {
            require_integrity(body.plan.nodes.iter().any(|item| &item.node_id == node))?;
        }
        if let Some(subject) = &failure.subject {
            require_integrity(
                is_redacted_text(subject) && subject.len() <= MAX_FAILURE_SUBJECT_BYTES,
            )?;
        }
        match (&failure.kind, &failure.infeasibility) {
            (
                FailureKindProjection::ResourceInfeasible,
                Some(InfeasibilityProjection::NoCapableAlternative),
            ) => {}
            (
                FailureKindProjection::ResourceInfeasible,
                Some(InfeasibilityProjection::Infeasible {
                    resource,
                    resource_identity,
                    required,
                    available,
                }),
            ) => require_integrity(
                is_redacted_text(resource)
                    && resource.len() <= MAX_FAILURE_SUBJECT_BYTES
                    && !resource_identity.is_empty()
                    && required > available,
            )?,
            (
                FailureKindProjection::ResourceInfeasible,
                Some(InfeasibilityProjection::RecordedFailure { attempt, status }),
            ) => require_integrity(
                is_digest(attempt)
                    && matches!(
                        status,
                        ReceiptStatus::Failed | ReceiptStatus::Aborted | ReceiptStatus::Infeasible
                    ),
            )?,
            (FailureKindProjection::ResourceInfeasible, None) | (_, Some(_)) => {
                return Err(ReceiptError::IntegrityMismatch);
            }
            (_, None) => {}
        }
    }
    let has_infeasibility = body
        .failure
        .as_ref()
        .is_some_and(|failure| failure.infeasibility.is_some());
    require_integrity(if has_infeasibility {
        matches!(
            body.status,
            ReceiptStatus::Failed | ReceiptStatus::Aborted | ReceiptStatus::Infeasible
        )
    } else {
        body.status != ReceiptStatus::Infeasible
    })?;
    Ok(())
}

fn validate_route_projection(route: &RouteProjection) -> Result<(), ReceiptError> {
    require_integrity(
        route.matrix_schema_version > 0
            && route.matrix_contract_revision > 0
            && route_disposition_is_valid(&route.disposition)
            && !route.requirements.is_empty(),
    )?;
    let mut previous = None;
    let mut derived = "native";
    for requirement in &route.requirements {
        require_integrity(
            !requirement.id.is_empty()
                && requirement.id.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'-')
                })
                && route_requirement_kind_is_valid(&requirement.kind)
                && route_disposition_is_valid(&requirement.disposition)
                && !requirement.current_owner.trim().is_empty()
                && !requirement.destination_tickets.is_empty()
                && requirement
                    .destination_tickets
                    .iter()
                    .all(|value| !value.trim().is_empty())
                && !requirement.evidence_issues.is_empty()
                && requirement.evidence_issues.iter().all(|issue| *issue > 0)
                && !requirement.baseline_manifests.is_empty()
                && requirement
                    .baseline_manifests
                    .iter()
                    .all(|value| !value.trim().is_empty())
                && !requirement.acceptance_contract.trim().is_empty()
                && !requirement.transfer_point.trim().is_empty()
                && !requirement.deletion_condition.trim().is_empty()
                && !requirement.source_evidence.is_empty()
                && requirement
                    .source_evidence
                    .iter()
                    .all(|value| !value.trim().is_empty())
                && match (
                    requirement.disposition.as_str(),
                    requirement.obligation_ticket.as_deref(),
                    requirement.obligation_reason.as_deref(),
                ) {
                    ("native", None, None) => true,
                    (
                        "legacy_whole_run" | "temporarily_unavailable",
                        Some(ticket),
                        Some(reason),
                    ) => !ticket.trim().is_empty() && !reason.trim().is_empty(),
                    _ => false,
                }
                && previous.is_none_or(|id| id < requirement.id.as_str()),
        )?;
        previous = Some(requirement.id.as_str());
        derived = match (derived, requirement.disposition.as_str()) {
            (_, "temporarily_unavailable") => "temporarily_unavailable",
            ("temporarily_unavailable", _) => "temporarily_unavailable",
            (_, "legacy_whole_run") => "legacy_whole_run",
            (current, "native") => current,
            _ => return Err(ReceiptError::IntegrityMismatch),
        };
    }
    require_integrity(route.disposition == derived)
}

fn validate_problem_evidence(problem: &ProblemProjection) -> Result<(), ReceiptError> {
    let evidence = &problem.effective;
    problem.product_graph.validate()?;
    problem.product_graph.validate_audit_fields(evidence)?;
    require_integrity(evidence.schema_version == COMPILED_PROBLEM_EVIDENCE_VERSION)?;
    require_integrity(
        evidence.field("problem.identity") == Some(problem.problem_identity.as_str())
            && evidence.field("problem.identity_basis")
                == Some(problem.problem_identity_basis.as_str())
            && evidence.field("problem.numerics_identity")
                == Some(problem.numerics_identity.as_str())
            && evidence.field("model_lifecycle.identity")
                == Some(problem.model_lifecycle.identity.as_str())
            && evidence.field("geometry.identity") == Some(problem.geometry_identity.as_str())
            && evidence.field("products.graph.identity")
                == Some(problem.product_graph.identity.as_str())
            && evidence
                .field("products.graph.schema_version")
                .and_then(|value| value.parse::<u32>().ok())
                == Some(problem.product_graph.schema_version)
            && evidence.field("observation.snapshot.identity")
                == Some(problem.observation_identity.as_str()),
    )?;
    require_integrity(evidence.fields.iter().all(|(path, value)| {
        !path.is_empty()
            && path.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_')
            })
            && is_redacted_text(value)
    }))?;
    for prefix in [
        "science.",
        "reconstruction.",
        "weighting.",
        "products.",
        "numerics.",
        "model_lifecycle.",
        "required_capabilities.",
        "geometry.",
        "observation.",
    ] {
        require_integrity(evidence.fields.keys().any(|path| path.starts_with(prefix)))?;
    }
    Ok(())
}

fn validate_plan_projection(
    product_graph: &ProductGraphProjection,
    plan: &PlanProjection,
    revision: u64,
) -> Result<(), ReceiptError> {
    require_integrity(is_digest(&plan.product_graph_identity))?;
    let publication_members = &product_graph.publication_member_ordinals;
    let node_ids = plan
        .nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(node_ids.len() == plan.nodes.len())?;
    require_integrity(node_ids.iter().all(|identity| is_redacted_text(identity)))?;

    let implementations = plan
        .selected_implementations
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    require_integrity(implementations.len() == plan.selected_implementations.len())?;
    require_integrity(
        implementations
            .iter()
            .all(|identity| is_redacted_text(identity)),
    )?;
    let node_implementations = plan
        .nodes
        .iter()
        .map(|node| node.implementation.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(implementations == node_implementations)?;

    let required_capabilities = plan
        .required_resource_capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    require_integrity(
        required_capabilities.len() == plan.required_resource_capabilities.len()
            && required_capabilities
                .iter()
                .all(|identity| is_redacted_text(identity)),
    )?;
    validate_alternative_projection(&plan.selected_alternative)?;
    let supported_capabilities = plan
        .selected_alternative
        .supported_capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    require_integrity(required_capabilities.is_subset(&supported_capabilities))?;
    validate_resource_policy_projection(&plan.resource_policy)?;

    let fence_keys = plan
        .fences
        .iter()
        .map(|fence| (fence.node_id.as_str(), fence.kind.as_str()))
        .collect::<BTreeSet<_>>();
    require_integrity(fence_keys.len() == plan.fences.len())?;
    require_integrity(plan.fences.iter().all(|fence| {
        node_ids.contains(fence.node_id.as_str()) && fence_kind_is_valid(&fence.kind)
    }))?;
    let expected_fences = plan
        .nodes
        .iter()
        .flat_map(|node| {
            node.fences
                .iter()
                .map(move |kind| (node.node_id.as_str(), kind.as_str()))
        })
        .collect::<BTreeSet<_>>();
    require_integrity(expected_fences == fence_keys)?;

    let valid_events = plan
        .nodes
        .iter()
        .map(|node| format!("work:{}", node.node_id))
        .chain(
            plan.fences
                .iter()
                .map(|fence| format!("fence:{}:{}", fence.node_id, fence.kind)),
        )
        .collect::<BTreeSet<_>>();
    let allocation_ids = plan
        .allocation_generations
        .iter()
        .map(|allocation| allocation.generation_identity.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(allocation_ids.len() == plan.allocation_generations.len())?;
    let slot_ids = plan
        .physical_slots
        .iter()
        .map(|slot| slot.slot_identity.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(slot_ids.len() == plan.physical_slots.len())?;

    let mut used_allocations = BTreeSet::new();
    for node in &plan.nodes {
        let node_allocations = node
            .allocation_uses
            .iter()
            .map(|usage| usage.generation_identity.as_str())
            .collect::<BTreeSet<_>>();
        used_allocations.extend(node_allocations.iter().copied());
        require_integrity(
            work_kind_is_valid(&node.kind)
                && work_domain_is_valid(&node.domain)
                && node
                    .dependencies
                    .iter()
                    .all(|event| valid_events.contains(event))
                && node.allocation_uses.iter().all(|usage| {
                    allocation_ids.contains(usage.generation_identity.as_str())
                        && claim_lifetime_is_valid(&usage.lifetime)
                })
                && node_allocations.len() == node.allocation_uses.len()
                && node.fences.iter().all(|kind| fence_kind_is_valid(kind))
                && node
                    .quiescence_after
                    .iter()
                    .all(|point| quiescence_is_valid(point))
                && node.claims.iter().all(|claim| {
                    is_redacted_text(&claim.resource)
                        && claim_lifetime_is_valid(&claim.lifetime)
                        && claim.actual_peak.is_none_or(|peak| {
                            peak <= claim.amount || node.status == ReceiptStatus::Failed
                        })
                })
                && node.io.iter().all(|io| {
                    io_buffer_is_valid(&io.kind)
                        && io.actual_bytes.is_some() == io.actual_operations.is_some()
                }),
        )?;
    }
    require_integrity(used_allocations == allocation_ids)?;
    let mut used_slots = BTreeSet::new();
    for allocation in &plan.allocation_generations {
        used_slots.insert(allocation.physical_slot.as_str());
        require_integrity(
            is_redacted_text(&allocation.generation_identity)
                && allocation.bytes > 0
                && allocation_purpose_is_valid(&allocation.purpose)
                && slot_ids.contains(allocation.physical_slot.as_str())
                && node_ids.contains(allocation.acquire_at.as_str())
                && !allocation.release_after.is_empty()
                && allocation
                    .release_after
                    .iter()
                    .all(|event| valid_events.contains(event)),
        )?;
        validate_compatibility_projection(&allocation.compatibility)?;
    }
    require_integrity(used_slots == slot_ids)?;
    for slot in &plan.physical_slots {
        require_integrity(
            is_redacted_text(&slot.slot_identity)
                && is_redacted_text(&slot.lease_resource)
                && slot.capacity_bytes > 0,
        )?;
        validate_compatibility_projection(&slot.compatibility)?;
    }

    let artifact_ids = plan
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_identity.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(artifact_ids.len() == plan.artifacts.len())?;
    for artifact in &plan.artifacts {
        require_integrity(
            is_digest(&artifact.artifact_identity)
                && node_ids.contains(artifact.node_id.as_str())
                && artifact_role_is_valid(&artifact.role)
                && artifact.cache_identity.as_deref().is_none_or(is_digest)
                && artifact.observed_identity.as_deref().is_none_or(is_digest)
                && artifact.path_identity.as_deref().is_none_or(is_digest)
                && (artifact.disposition.is_none() || artifact.actual_bytes.is_some())
                && (artifact.actual_bytes.is_none()
                    || artifact.disposition.is_some()
                    || artifact.role == "output")
                && (artifact.path_identity.is_none() || artifact.actual_bytes.is_some()),
        )?;
    }
    let output_artifacts = plan
        .artifacts
        .iter()
        .filter(|artifact| artifact.role == "output")
        .map(|artifact| artifact.artifact_identity.as_str())
        .collect::<BTreeSet<_>>();
    let layout_artifacts = plan
        .publication_layouts
        .iter()
        .map(|layout| layout.artifact_identity.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(
        layout_artifacts.len() == plan.publication_layouts.len()
            && layout_artifacts == output_artifacts,
    )?;
    let participants = plan
        .publication_layouts
        .iter()
        .map(|layout| match &layout.participant {
            PublicationParticipantProjection::Product {
                graph_identity,
                node_ordinal,
            } => {
                format!("product:{graph_identity}:{node_ordinal}")
            }
            PublicationParticipantProjection::ModelData {
                measurement_set_identity,
            } => format!("model_data:{measurement_set_identity}"),
        })
        .collect::<BTreeSet<_>>();
    require_integrity(participants.len() == plan.publication_layouts.len())?;
    let product_participants = plan
        .publication_layouts
        .iter()
        .filter_map(|layout| match &layout.participant {
            PublicationParticipantProjection::Product { node_ordinal, .. } => Some(*node_ordinal),
            PublicationParticipantProjection::ModelData { .. } => None,
        })
        .collect::<Vec<_>>();
    require_integrity(product_participants.as_slice() == publication_members)?;
    for layout in &plan.publication_layouts {
        let bounds = &layout.resource_bounds;
        let participant_is_valid = match &layout.participant {
            PublicationParticipantProjection::Product {
                graph_identity,
                node_ordinal,
            } => {
                is_digest(graph_identity)
                    && graph_identity == &plan.product_graph_identity
                    && publication_members.binary_search(node_ordinal).is_ok()
            }
            PublicationParticipantProjection::ModelData {
                measurement_set_identity,
            } => is_digest(measurement_set_identity),
        };
        require_integrity(
            participant_is_valid
                && is_digest(&layout.artifact_identity)
                && is_digest(&layout.layout_identity)
                && node_ids.contains(layout.producer.as_str())
                && valid_events.contains(&layout.terminal)
                && allocation_ids.contains(layout.writer_allocation.as_str())
                && io_buffer_is_valid(&layout.writer_buffer_kind)
                && bounds.staged_storage_bytes > 0
                && bounds.final_storage_bytes > 0
                && bounds.writer_buffer_bytes > 0
                && (bounds.mapped_page_cache_bytes > 0) == layout.mapped_page_cache.is_some(),
        )?;
        if let Some(mapped) = &layout.mapped_page_cache {
            require_integrity(
                node_ids.contains(mapped.producer.as_str())
                    && valid_events.contains(&mapped.terminal)
                    && allocation_ids.contains(mapped.allocation.as_str()),
            )?;
        }
    }

    let adaptation_ids = plan
        .adaptations
        .iter()
        .map(|adaptation| adaptation.adaptation_identity.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(adaptation_ids.len() == plan.adaptations.len())?;
    require_integrity(execution_knobs_are_valid(&plan.initial_execution_knobs))?;
    let mut applied_revisions = BTreeSet::new();
    for adaptation in &plan.adaptations {
        require_integrity(
            is_redacted_text(&adaptation.adaptation_identity)
                && quiescence_is_valid(&adaptation.quiescence)
                && plan
                    .selected_alternative
                    .quiescence_points
                    .contains(&adaptation.quiescence)
                && execution_knobs_are_valid(&adaptation.from)
                && execution_knobs_are_valid(&adaptation.to),
        )?;
        if let Some(applied_revision) = adaptation.applied_revision {
            require_integrity(
                applied_revision > 0
                    && applied_revision <= revision
                    && applied_revisions.insert(applied_revision),
            )?;
        }
    }
    require_integrity(plan.prediction.confidence_ppm <= 1_000_000)?;
    Ok(())
}

fn validate_resource_policy_projection(
    policy: &ResourcePolicyProjection,
) -> Result<(), ReceiptError> {
    let ResourcePolicyProjection::Explicit { ceilings } = policy else {
        return Ok(());
    };
    require_integrity(
        map_keys_are_redacted(&ceilings.memory_bytes)
            && map_keys_are_redacted(&ceilings.storage_bytes)
            && map_keys_are_redacted(&ceilings.rates_per_second)
            && map_keys_are_redacted(&ceilings.queue_slots)
            && map_keys_are_redacted(&ceilings.accelerator_slots),
    )
}

fn validate_alternative_projection(
    alternative: &DemandAlternativeProjection,
) -> Result<(), ReceiptError> {
    require_integrity(is_redacted_text(&alternative.alternative_identity))?;
    let capabilities = alternative
        .supported_capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    require_integrity(
        capabilities.len() == alternative.supported_capabilities.len()
            && capabilities
                .iter()
                .all(|identity| is_redacted_text(identity)),
    )?;
    let quiescence_points = alternative
        .quiescence_points
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    require_integrity(
        quiescence_points.len() == alternative.quiescence_points.len()
            && quiescence_points
                .iter()
                .all(|point| quiescence_is_valid(point)),
    )?;

    let demand = &alternative.demand;
    require_integrity(
        is_redacted_text(&demand.host_memory_view)
            && valid_count(demand.workers)
            && valid_count(demand.locks)
            && valid_count(demand.file_descriptors)
            && demand.caches.preferred_resident_bytes <= demand.caches.hard_resident_bytes
            && demand.rates.iter().all(|item| valid_count(item.amount))
            && demand.queues.iter().all(|item| valid_count(item.slots))
            && demand
                .transfers
                .iter()
                .all(|item| valid_count(item.rate) && valid_count(item.queue_slots))
            && demand
                .accelerators
                .iter()
                .all(|item| valid_count(item.slots) && valid_count(item.command_queue_slots)),
    )?;
    let memory_ids = demand
        .memory
        .iter()
        .map(|item| item.allocation_identity.as_str())
        .collect::<BTreeSet<_>>();
    require_integrity(memory_ids.len() == demand.memory.len())?;
    for memory in &demand.memory {
        let views = memory
            .views
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        require_integrity(
            is_redacted_text(&memory.allocation_identity)
                && memory.hard_bytes > 0
                && memory.preferred_bytes <= memory.hard_bytes
                && !views.is_empty()
                && views.len() == memory.views.len()
                && views.iter().all(|view| is_redacted_text(view)),
        )?;
    }
    let mut demand_ids = BTreeSet::new();
    for storage in &demand.storage {
        require_integrity(
            demand_ids.insert(storage.demand_identity.as_str())
                && is_redacted_text(&storage.demand_identity)
                && is_redacted_text(&storage.domain_identity)
                && valid_count(storage.read_rate)
                && valid_count(storage.write_rate)
                && valid_count(storage.operations_rate)
                && valid_count(storage.queue_slots),
        )?;
    }
    for rate in &demand.rates {
        require_integrity(
            demand_ids.insert(rate.demand_identity.as_str())
                && is_redacted_text(&rate.demand_identity)
                && is_redacted_text(&rate.resource_identity),
        )?;
    }
    for queue in &demand.queues {
        require_integrity(
            demand_ids.insert(queue.demand_identity.as_str())
                && is_redacted_text(&queue.demand_identity)
                && is_redacted_text(&queue.resource_identity),
        )?;
    }
    for transfer in &demand.transfers {
        require_integrity(
            demand_ids.insert(transfer.demand_identity.as_str())
                && is_redacted_text(&transfer.demand_identity)
                && is_redacted_text(&transfer.link_identity),
        )?;
    }
    for accelerator in &demand.accelerators {
        require_integrity(
            demand_ids.insert(accelerator.demand_identity.as_str())
                && is_redacted_text(&accelerator.demand_identity)
                && is_redacted_text(&accelerator.accelerator_identity),
        )?;
    }
    require_integrity(
        map_keys_are_redacted(&alternative.headroom.memory_bytes)
            && map_keys_are_redacted(&alternative.headroom.storage_bytes)
            && map_keys_are_redacted(&alternative.headroom.rates_per_second)
            && map_keys_are_redacted(&alternative.headroom.queue_slots)
            && map_keys_are_redacted(&alternative.headroom.accelerator_slots)
            && map_keys_are_redacted(&alternative.scaling.memory_bytes_per_worker)
            && alternative.scaling.minimum_workers > 0
            && alternative.scaling.minimum_workers <= alternative.scaling.maximum_workers
            && alternative.scaling.maximum_batch_size > 0
            && alternative.scaling.maximum_tile_width > 0
            && alternative.scaling.maximum_tile_height > 0
            && alternative.scaling.maximum_slab_depth > 0,
    )
}

fn validate_compatibility_projection(
    compatibility: &CompatibilityProjection,
) -> Result<(), ReceiptError> {
    let views = compatibility
        .views
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    require_integrity(
        is_redacted_text(&compatibility.memory_domain)
            && !views.is_empty()
            && views.len() == compatibility.views.len()
            && views.iter().all(|view| is_redacted_text(view))
            && compatibility.alignment_bytes.is_power_of_two()
            && matches!(compatibility.storage_mode.as_str(), "host" | "metal_shared")
            && is_redacted_text(&compatibility.layout)
            && matches!(
                compatibility.initialization.as_str(),
                "preserve" | "zero_before_read" | "overwrite_before_read"
            )
            && matches!(
                compatibility.access.as_str(),
                "read_only" | "read_write" | "write_only"
            ),
    )
}

fn require_integrity(condition: bool) -> Result<(), ReceiptError> {
    if condition {
        Ok(())
    } else {
        Err(ReceiptError::IntegrityMismatch)
    }
}

fn valid_count(demand: CountDemandProjection) -> bool {
    demand.preferred <= demand.hard
}

fn execution_knobs_are_valid(knobs: &ExecutionKnobsProjection) -> bool {
    knobs.workers > 0
        && knobs.batch_size > 0
        && knobs.tile_width > 0
        && knobs.tile_height > 0
        && knobs.slab_depth > 0
        && knobs.io_depth > 0
}

fn map_keys_are_redacted(values: &BTreeMap<String, u64>) -> bool {
    values.keys().all(|identity| is_redacted_text(identity))
}

fn is_redacted_text(value: &str) -> bool {
    !value.is_empty() && !value.contains('/') && !value.contains('\\') && !value.starts_with('~')
}

fn reference_kind_is_valid(value: &str) -> bool {
    matches!(
        value,
        "measures" | "ephemeris" | "observatory" | "spectral_lines" | "instrument"
    )
}

fn work_kind_is_valid(value: &str) -> bool {
    matches!(
        value,
        "data_census"
            | "preparation"
            | "cache"
            | "convolution_function"
            | "fft_planning"
            | "jit"
            | "compute"
            | "transfer"
            | "spill"
            | "prefetch"
            | "io"
            | "observation_read"
            | "serialization"
            | "writeback"
            | "publication"
            | "release"
            | "synchronization"
    )
}

fn work_domain_is_valid(value: &str) -> bool {
    matches!(value, "cpu" | "io" | "control")
        || value.strip_prefix("metal:").is_some_and(is_redacted_text)
}

fn fence_kind_is_valid(value: &str) -> bool {
    matches!(value, "device" | "io" | "writeback" | "publication")
}

fn quiescence_is_valid(value: &str) -> bool {
    matches!(
        value,
        "run_boundary" | "stage" | "major_cycle" | "tile_batch" | "slab"
    )
}

fn io_buffer_is_valid(value: &str) -> bool {
    matches!(
        value,
        "source_read_ahead"
            | "decode"
            | "preparation"
            | "host_to_device_transfer"
            | "device_to_host_transfer"
            | "spill_read"
            | "spill_write"
            | "serialization"
            | "storage_manager"
            | "tiled_column_writer"
            | "scalar_column_writer"
            | "writeback"
            | "publication"
            | "mapped_page_cache"
    )
}

fn claim_lifetime_is_valid(value: &str) -> bool {
    if value == "work" {
        return true;
    }
    let Some(fences) = value.strip_prefix("fences:") else {
        return false;
    };
    let kinds = fences.split(',').collect::<BTreeSet<_>>();
    !kinds.is_empty() && !kinds.contains("") && kinds.iter().all(|kind| fence_kind_is_valid(kind))
}

fn allocation_purpose_is_valid(value: &str) -> bool {
    value == "data"
        || value
            .strip_prefix("io_buffer:")
            .is_some_and(io_buffer_is_valid)
}

fn artifact_role_is_valid(value: &str) -> bool {
    matches!(value, "input" | "prepared" | "cache" | "output")
}

fn decode_document(bytes: &[u8]) -> Result<ReceiptDocument, ReceiptError> {
    let document: ReceiptDocument =
        serde_json::from_slice(bytes).map_err(|source| ReceiptError::Json { source })?;
    if document.schema.name != RECEIPT_SCHEMA || document.schema.version != RECEIPT_SCHEMA_VERSION {
        return Err(ReceiptError::UnsupportedSchema {
            name: document.schema.name,
            version: document.schema.version,
        });
    }
    let payload =
        serde_json::to_vec(&document.receipt).map_err(|source| ReceiptError::Json { source })?;
    if sha256(&payload) != document.payload_sha256 {
        return Err(ReceiptError::IntegrityMismatch);
    }
    validate_body(&document.receipt)?;
    Ok(document)
}

fn encode_document(body: &ReceiptBody) -> Result<Vec<u8>, ReceiptError> {
    let payload = serde_json::to_vec(body).map_err(|source| ReceiptError::Json { source })?;
    let document = ReceiptDocument {
        schema: ReceiptSchema {
            name: RECEIPT_SCHEMA.to_string(),
            version: RECEIPT_SCHEMA_VERSION,
        },
        payload_sha256: sha256(&payload),
        receipt: body.clone(),
    };
    serde_json::to_vec_pretty(&document).map_err(|source| ReceiptError::Json { source })
}

fn worst_case_receipt_bytes(body: &ReceiptBody) -> Result<u64, ReceiptError> {
    let bytes = encode_document(&body.worst_case_terminal())?;
    Ok(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
}

fn prepared_publication_bytes(
    prepared_bytes: usize,
    completed_bytes: usize,
    worst_case_prepared_bytes: u64,
) -> Result<u64, ReceiptError> {
    u64::try_from(prepared_bytes)
        .unwrap_or(u64::MAX)
        .max(worst_case_prepared_bytes)
        .checked_add(u64::try_from(completed_bytes).unwrap_or(u64::MAX))
        .ok_or(ReceiptError::RetentionExceeded)
}

fn read_receipt_body(path: &Path) -> Result<ReceiptBody, ReceiptError> {
    let bytes = fs::read(path).map_err(|source| ReceiptError::Io {
        action: "read retained execution receipt",
        source,
    })?;
    Ok(decode_document(&bytes)?.receipt)
}

fn is_receipt_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(RECEIPT_SUFFIX))
}

fn is_receipt_staging_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.starts_with(RECEIPT_STAGING_PREFIX) && name.ends_with(RECEIPT_STAGING_SUFFIX)
        })
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ReceiptError> {
    let (parent, temporary) = staged_receipt(path, bytes)?;
    temporary.persist(path).map_err(|error| ReceiptError::Io {
        action: "publish execution receipt",
        source: error.error,
    })?;
    sync_directory(parent)
}

fn atomic_create(path: &Path, bytes: &[u8]) -> Result<(), ReceiptError> {
    let (parent, temporary) = staged_receipt(path, bytes)?;
    match temporary.persist_noclobber(path) {
        Ok(_) => sync_directory(parent),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(ReceiptError::AttemptAlreadyExists)
        }
        Err(error) => Err(ReceiptError::Io {
            action: "publish new execution receipt",
            source: error.error,
        }),
    }
}

fn staged_receipt<'path>(
    path: &'path Path,
    bytes: &[u8],
) -> Result<(&'path Path, NamedTempFile), ReceiptError> {
    let parent = path.parent().ok_or(ReceiptError::InvalidStore)?;
    let mut temporary = tempfile::Builder::new()
        .prefix(RECEIPT_STAGING_PREFIX)
        .suffix(RECEIPT_STAGING_SUFFIX)
        .tempfile_in(parent)
        .map_err(|source| ReceiptError::Io {
            action: "create receipt staging file",
            source,
        })?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| ReceiptError::Io {
            action: "write and sync receipt staging file",
            source,
        })?;
    Ok((parent, temporary))
}

fn sync_directory(path: &Path) -> Result<(), ReceiptError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ReceiptError::Io {
            action: "sync receipt directory",
            source,
        })
}

fn project_problem_fields(problem: &CompiledProblem) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    evidence_field(
        &mut fields,
        "problem.identity",
        hex(&problem.problem_id().as_bytes()),
    );
    evidence_field(
        &mut fields,
        "problem.identity_basis",
        hex(&problem.problem_identity_basis().as_bytes()),
    );
    evidence_field(
        &mut fields,
        "problem.numerics_identity",
        hex(&problem.numerics_id().as_bytes()),
    );
    project_science(&mut fields, problem);
    project_reconstruction(&mut fields, problem);
    project_weighting(&mut fields, problem);
    project_products(&mut fields, problem);
    project_numerics(&mut fields, problem);
    project_model_lifecycle(&mut fields, problem);
    for (index, capability) in problem.required_capabilities().iter().enumerate() {
        evidence_field(
            &mut fields,
            format!("required_capabilities.{index}"),
            required_capability(*capability),
        );
    }
    project_geometry(&mut fields, problem.geometry());
    project_observation(&mut fields, problem.inputs());
    fields
}

fn project_science(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let science = problem.science();
    evidence_field(
        fields,
        "science.spectral.sampling",
        spectral_sampling(science.spectral().sampling()),
    );
    if let SpectralSampling::ChannelAverage { channels_per_bin } = science.spectral().sampling() {
        evidence_field(
            fields,
            "science.spectral.channels_per_bin",
            channels_per_bin,
        );
    }
    evidence_field(
        fields,
        "science.spectral.coupling",
        spectral_coupling(science.spectral().coupling()),
    );
    let measurement = science.measurement_equation();
    evidence_field(
        fields,
        "science.measurement_equation.instrument_response",
        instrument_response(measurement.instrument_response()),
    );
    evidence_field(
        fields,
        "science.measurement_equation.inner_products.model",
        model_inner_product(measurement.inner_products().model()),
    );
    evidence_field(
        fields,
        "science.measurement_equation.inner_products.visibility",
        visibility_inner_product(measurement.inner_products().visibility()),
    );
    let normal = problem.normal_equation();
    let operator = normal.measurement_operator();
    evidence_field(
        fields,
        "science.measurement_equation.operator.domain.geometry_identity",
        hex(&operator.domain().geometry().as_bytes()),
    );
    evidence_field(
        fields,
        "science.measurement_equation.operator.domain.basis",
        reconstruction_basis(operator.domain().basis()),
    );
    for (index, coordinate) in operator
        .domain()
        .polarization()
        .coordinates()
        .iter()
        .enumerate()
    {
        evidence_field(
            fields,
            format!("science.measurement_equation.operator.domain.polarization.{index}"),
            polarization_coordinate(*coordinate),
        );
    }
    evidence_field(
        fields,
        "science.measurement_equation.operator.codomain.observation_identity",
        hex(&operator.codomain().observation().as_bytes()),
    );
    for (index, transform) in operator.transforms().iter().enumerate() {
        let prefix = format!("science.measurement_equation.operator.transforms.{index}");
        evidence_field(
            fields,
            format!("{prefix}.kind"),
            paired_transform_kind(transform.kind()),
        );
        match transform {
            PairedMeasurementTransform::SpectralBasis { basis } => {
                evidence_field(
                    fields,
                    format!("{prefix}.basis"),
                    reconstruction_basis(*basis),
                );
            }
            PairedMeasurementTransform::DirectionDependentResponse { response } => {
                evidence_field(
                    fields,
                    format!("{prefix}.response"),
                    instrument_response(*response),
                );
            }
            PairedMeasurementTransform::PhaseRotation { convention } => {
                evidence_field(
                    fields,
                    format!("{prefix}.convention"),
                    visibility_phase(*convention),
                );
            }
            PairedMeasurementTransform::SpectralResampling { sampling } => {
                evidence_field(
                    fields,
                    format!("{prefix}.sampling"),
                    spectral_sampling(*sampling),
                );
            }
            PairedMeasurementTransform::ChannelIntegration { channels_per_bin } => {
                evidence_field(
                    fields,
                    format!("{prefix}.channels_per_bin"),
                    *channels_per_bin,
                );
            }
            PairedMeasurementTransform::PolarizationMapping => {}
        }
    }
    evidence_field(
        fields,
        "science.normal_equation.output.normalization",
        normal_state_normalization(normal.output().normalization()),
    );
    for (index, form) in normal.forms().into_iter().enumerate() {
        evidence_field(
            fields,
            format!("science.normal_equation.forms.{index}"),
            normal_equation_form(form),
        );
    }
}

fn project_reconstruction(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let reconstruction = problem.reconstruction();
    let basis = reconstruction.basis();
    evidence_field(
        fields,
        "reconstruction.basis.kind",
        reconstruction_basis(basis),
    );
    match basis {
        ReconstructionBasis::Taylor { terms } => {
            evidence_field(fields, "reconstruction.basis.terms", terms);
        }
        ReconstructionBasis::ChannelLocal { channels } => {
            evidence_field(fields, "reconstruction.basis.channels", channels);
        }
        ReconstructionBasis::Constant => {}
    }
    let algorithm = reconstruction.algorithm();
    evidence_field(
        fields,
        "reconstruction.algorithm.kind",
        reconstruction_algorithm(algorithm),
    );
    if let ReconstructionAlgorithm::Multiscale { scales_px } = algorithm {
        for (index, scale) in scales_px.iter().enumerate() {
            evidence_field(
                fields,
                format!("reconstruction.algorithm.scales_px.{index}"),
                stable_float(*scale),
            );
        }
    }
    let controls = reconstruction.controls();
    evidence_field(
        fields,
        "reconstruction.controls.max_minor_iterations",
        controls.max_minor_iterations(),
    );
    evidence_field(
        fields,
        "reconstruction.controls.gain",
        stable_float(controls.gain()),
    );
    evidence_field(
        fields,
        "reconstruction.controls.threshold_jy_per_beam",
        stable_float(controls.threshold_jy_per_beam()),
    );
    for (index, coordinate) in reconstruction
        .polarization()
        .coordinates()
        .iter()
        .enumerate()
    {
        evidence_field(
            fields,
            format!("reconstruction.polarization.{index}"),
            polarization_coordinate(*coordinate),
        );
    }
}

fn project_weighting(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let weighting = problem.weighting();
    let scheme = weighting.scheme();
    evidence_field(fields, "weighting.scheme.kind", weighting_scheme(scheme));
    if let WeightingScheme::Briggs { robust } | WeightingScheme::BriggsBandwidthTaper { robust } =
        scheme
    {
        evidence_field(fields, "weighting.scheme.robust", stable_float(robust));
    }
    evidence_field(
        fields,
        "weighting.density_scope",
        weight_density_scope(weighting.density_scope()),
    );
    match weighting.uv_taper() {
        None => evidence_field(fields, "weighting.uv_taper.kind", "none"),
        Some(taper) => {
            evidence_field(fields, "weighting.uv_taper.kind", "gaussian");
            evidence_field(
                fields,
                "weighting.uv_taper.major_lambda",
                stable_float(taper.major_lambda()),
            );
            evidence_field(
                fields,
                "weighting.uv_taper.minor_lambda",
                stable_float(taper.minor_lambda()),
            );
            evidence_field(
                fields,
                "weighting.uv_taper.position_angle_rad",
                stable_float(taper.position_angle_rad()),
            );
        }
    }
    evidence_field(
        fields,
        "weighting.generation.identity",
        hex(&weighting.generation_id().as_bytes()),
    );
    evidence_field(
        fields,
        "weighting.generation.snapshot_identity",
        hex(&weighting.snapshot().as_bytes()),
    );
    for (index, source) in weighting.sources().iter().enumerate() {
        let prefix = format!("weighting.sources.{index}");
        evidence_field(
            fields,
            format!("{prefix}.measurement_set_identity"),
            hex(&source.source().identity().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.flag_policy"),
            flag_policy(source.flags()),
        );
        evidence_field(
            fields,
            format!("{prefix}.input_weight_column"),
            weight_column(source.input_weights()),
        );
        evidence_field(
            fields,
            format!("{prefix}.flag_generation"),
            hex(&source.flag_generation().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.flag_row_generation"),
            hex(&source.flag_row_generation().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.input_weight_generation"),
            hex(&source.input_weight_generation().as_bytes()),
        );
    }
}

fn project_products(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let products = problem.products();
    for (index, product) in products.products().iter().enumerate() {
        evidence_field(
            fields,
            format!("products.requested.{index}"),
            product_kind(*product),
        );
    }
    evidence_field(
        fields,
        "products.normalization",
        product_normalization(products.normalization()),
    );
    evidence_field(
        fields,
        "products.restoring_beam",
        restoring_beam(products.restoring_beam()),
    );
    let boundary = products.normalization_boundary();
    evidence_field(
        fields,
        "products.normalization_boundary.input",
        normal_state_normalization(boundary.input()),
    );
    for (index, operation) in boundary.operations().iter().enumerate() {
        let prefix = format!("products.normalization_boundary.operations.{index}");
        evidence_field(
            fields,
            format!("{prefix}.kind"),
            product_boundary_operation(*operation),
        );
        match operation {
            ProductBoundaryOperation::Normalize(normalization) => evidence_field(
                fields,
                format!("{prefix}.normalization"),
                product_normalization(*normalization),
            ),
            ProductBoundaryOperation::Restore(policy) => evidence_field(
                fields,
                format!("{prefix}.restoring_beam"),
                restoring_beam(*policy),
            ),
            ProductBoundaryOperation::ScaleResidual
            | ProductBoundaryOperation::CorrectPrimaryBeam
            | ProductBoundaryOperation::BlankInvalid
            | ProductBoundaryOperation::ConvertUnits => {}
        }
    }
    project_product_graph(fields, problem);
}

fn project_product_graph(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let graph = problem.product_graph();
    evidence_field(
        fields,
        "products.graph.identity",
        hex(&graph.graph_id().as_bytes()),
    );
    evidence_field(
        fields,
        "products.graph.schema_version",
        graph.schema_version(),
    );
    for node in graph.nodes() {
        let prefix = format!("products.graph.nodes.{}", node.node_id().ordinal());
        evidence_field(
            fields,
            format!("{prefix}.ordinal"),
            node.node_id().ordinal(),
        );
        evidence_field(fields, format!("{prefix}.role"), product_role(node.role()));
        match node.name() {
            Some(name) => {
                evidence_field(fields, format!("{prefix}.name.kind"), "suffix");
                evidence_field(fields, format!("{prefix}.name.value"), stable_text(name));
            }
            None => evidence_field(fields, format!("{prefix}.name.kind"), "none"),
        }
        evidence_field(
            fields,
            format!("{prefix}.axes.kind"),
            product_axis_kind(node.axes().kind()),
        );
        evidence_field(
            fields,
            format!("{prefix}.axes.geometry_identity"),
            hex(&node.axes().geometry_id().as_bytes()),
        );
        match node.axes().domain() {
            ImageDomainRole::Main => {
                evidence_field(fields, format!("{prefix}.axes.domain.kind"), "main");
            }
            ImageDomainRole::Outlier(name) => {
                evidence_field(fields, format!("{prefix}.axes.domain.kind"), "outlier");
                evidence_field(
                    fields,
                    format!("{prefix}.axes.domain.name"),
                    stable_text(name),
                );
            }
        }
        for (index, axis) in node.axes().order().positions().iter().enumerate() {
            evidence_field(
                fields,
                format!("{prefix}.axes.order.{index}"),
                image_axis(*axis),
            );
        }
        for (index, extent) in node.axes().shape().iter().enumerate() {
            evidence_field(fields, format!("{prefix}.axes.shape.{index}"), extent);
        }
        for (index, coordinate) in node.axes().polarization().iter().enumerate() {
            evidence_field(
                fields,
                format!("{prefix}.axes.polarization.{index}"),
                polarization_coordinate(*coordinate),
            );
        }
        evidence_field(fields, format!("{prefix}.unit"), product_unit(node.unit()));
        match node.normalization() {
            Some(normalization) => {
                evidence_field(
                    fields,
                    format!("{prefix}.normalization.kind"),
                    "normalization",
                );
                evidence_field(
                    fields,
                    format!("{prefix}.normalization.value"),
                    product_normalization(normalization),
                );
            }
            None => evidence_field(fields, format!("{prefix}.normalization.kind"), "none"),
        }
        project_product_beam(fields, &prefix, node.beam());
        project_product_validity(fields, &prefix, node.validity());
        evidence_field(
            fields,
            format!("{prefix}.schema"),
            product_schema(node.schema()),
        );
        for (index, dependency) in node.dependencies().iter().enumerate() {
            evidence_field(
                fields,
                format!("{prefix}.dependencies.{index}"),
                dependency.ordinal(),
            );
        }
    }
    let publication = graph.publication();
    evidence_field(
        fields,
        "products.graph.publication.protocol.requires_durable_prepare",
        publication.protocol().requires_durable_prepare(),
    );
    evidence_field(
        fields,
        "products.graph.publication.protocol.has_one_visibility_operation",
        publication.protocol().has_one_visibility_operation(),
    );
    evidence_field(
        fields,
        "products.graph.publication.protocol.has_infallible_terminal_promotion",
        publication.protocol().has_infallible_terminal_promotion(),
    );
    for (index, member) in publication.members().iter().enumerate() {
        evidence_field(
            fields,
            format!("products.graph.publication.members.{index}"),
            member.ordinal(),
        );
    }
}

fn project_product_beam(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    beam: ProductBeamRule,
) {
    let prefix = format!("{prefix}.beam");
    match beam {
        ProductBeamRule::None => evidence_field(fields, format!("{prefix}.kind"), "none"),
        ProductBeamRule::Fitted => evidence_field(fields, format!("{prefix}.kind"), "fitted"),
        ProductBeamRule::Restoring(policy) => {
            evidence_field(fields, format!("{prefix}.kind"), "restoring");
            evidence_field(fields, format!("{prefix}.policy"), restoring_beam(policy));
        }
        ProductBeamRule::Inherit(node) => {
            evidence_field(fields, format!("{prefix}.kind"), "inherit");
            evidence_field(fields, format!("{prefix}.node_ordinal"), node.ordinal());
        }
        ProductBeamRule::Metadata(policy) => {
            evidence_field(fields, format!("{prefix}.kind"), "metadata");
            evidence_field(fields, format!("{prefix}.policy"), restoring_beam(policy));
        }
    }
}

fn project_product_validity(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    validity: ProductValidityRule,
) {
    let prefix = format!("{prefix}.validity");
    match validity {
        ProductValidityRule::All => evidence_field(fields, format!("{prefix}.kind"), "all"),
        ProductValidityRule::FinalNormalState => {
            evidence_field(fields, format!("{prefix}.kind"), "final_normal_state");
        }
        ProductValidityRule::PrimaryBeam(policy) => {
            evidence_field(fields, format!("{prefix}.kind"), "primary_beam");
            project_primary_beam_validity(fields, &prefix, policy);
        }
        ProductValidityRule::Taylor(policy) => {
            evidence_field(fields, format!("{prefix}.kind"), "taylor");
            project_taylor_validity(fields, &prefix, policy);
        }
        ProductValidityRule::TaylorAndPrimaryBeam {
            taylor,
            primary_beam,
        } => {
            evidence_field(fields, format!("{prefix}.kind"), "taylor_and_primary_beam");
            project_taylor_validity(fields, &format!("{prefix}.taylor"), taylor);
            project_primary_beam_validity(fields, &format!("{prefix}.primary_beam"), primary_beam);
        }
    }
}

fn project_primary_beam_validity(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    policy: casa_imaging_model::PrimaryBeamValidityPolicy,
) {
    evidence_field(
        fields,
        format!("{prefix}.cutoff"),
        stable_float32(policy.cutoff()),
    );
    evidence_field(
        fields,
        format!("{prefix}.comparison"),
        product_support_comparison(policy.comparison()),
    );
    evidence_field(
        fields,
        format!("{prefix}.blanking"),
        product_blanking(policy.blanking()),
    );
}

fn project_taylor_validity(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    policy: casa_imaging_model::TaylorValidityPolicy,
) {
    evidence_field(
        fields,
        format!("{prefix}.reference"),
        taylor_support_reference(policy.reference()),
    );
    evidence_field(
        fields,
        format!("{prefix}.peak_fraction"),
        stable_float32(policy.peak_fraction()),
    );
    evidence_field(
        fields,
        format!("{prefix}.comparison"),
        product_support_comparison(policy.comparison()),
    );
    evidence_field(
        fields,
        format!("{prefix}.blanking"),
        product_blanking(policy.blanking()),
    );
}

fn project_numerics(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let numerics = problem.numerics();
    for (index, precision) in numerics.permitted_precisions().iter().enumerate() {
        evidence_field(
            fields,
            format!("numerics.permitted_precisions.{index}"),
            numeric_precision(*precision),
        );
    }
    evidence_field(
        fields,
        "numerics.reduction",
        reduction_policy(numerics.reduction()),
    );
    evidence_field(
        fields,
        "numerics.finite_values",
        finite_value_policy(numerics.finite_values()),
    );
    for (stage, budget) in numerics.stage_error_budgets() {
        let prefix = format!("numerics.stage_error_budgets.{}", numerical_stage(*stage));
        evidence_field(
            fields,
            format!("{prefix}.absolute"),
            stable_float(budget.absolute()),
        );
        evidence_field(
            fields,
            format!("{prefix}.relative"),
            stable_float(budget.relative()),
        );
    }
}

fn project_model_lifecycle(fields: &mut BTreeMap<String, String>, problem: &CompiledProblem) {
    let contract = problem.model_lifecycle();
    let reprojection_policy = contract.reprojection_policy();
    let bounds = contract.bounds();
    evidence_field(
        fields,
        "model_lifecycle.identity",
        hex(&contract.contract_id().as_bytes()),
    );
    evidence_field(
        fields,
        "model_lifecycle.numerics_identity",
        hex(&contract.numerics().as_bytes()),
    );
    evidence_field(
        fields,
        "model_lifecycle.target_shape_identity",
        hex(&contract.target().identity().as_bytes()),
    );
    evidence_field(
        fields,
        "model_lifecycle.bounds.max_model_samples",
        bounds.max_model_samples(),
    );
    evidence_field(
        fields,
        "model_lifecycle.bounds.max_source_samples",
        bounds.max_source_samples(),
    );
    evidence_field(
        fields,
        "model_lifecycle.bounds.max_reprojection_terms",
        bounds.max_reprojection_terms(),
    );
    evidence_field(
        fields,
        "model_lifecycle.bounds.max_delta_terms",
        bounds.max_delta_terms(),
    );
    evidence_field(
        fields,
        "model_lifecycle.bounds.max_absolute_model_value",
        stable_float(bounds.max_absolute_model_value()),
    );
    evidence_field(
        fields,
        "model_lifecycle.bounds.max_absolute_delta_value",
        stable_float(bounds.max_absolute_delta_value()),
    );
    evidence_field(
        fields,
        "model_lifecycle.arithmetic_precision",
        numeric_precision(contract.arithmetic_precision()),
    );
    evidence_field(
        fields,
        "model_lifecycle.state_encoding",
        model_state_encoding(contract.state_encoding()),
    );
    evidence_field(
        fields,
        "model_lifecycle.support_semantics",
        model_support_semantics(contract.support_semantics()),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.identity",
        hex(&contract.reprojection_contract_identity().as_bytes()),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.product_graph_identity",
        hex(&problem.product_graph().graph_id().as_bytes()),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.numerics_identity",
        hex(&contract.numerics().as_bytes()),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.conversion_precision",
        numeric_precision(contract.arithmetic_precision()),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.direction_registry",
        reprojection_policy.direction_registry().as_str(),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.basis_registry",
        reprojection_policy.basis_registry().as_str(),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.polarization_registry",
        reprojection_policy.polarization_registry().as_str(),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.invalid_contributor_policy",
        reprojection_policy.invalid_contributor().as_str(),
    );
    evidence_field(
        fields,
        "model_lifecycle.reprojection.uncovered_target_policy",
        reprojection_policy.uncovered_target().as_str(),
    );
    match contract.input() {
        ModelInputCommitment::Empty => {
            evidence_field(fields, "model_lifecycle.input.kind", "empty");
        }
        ModelInputCommitment::AlignedSeed { source, support } => {
            evidence_field(fields, "model_lifecycle.input.kind", "aligned_seed");
            evidence_field(
                fields,
                "model_lifecycle.input.source_identity",
                hex(&source.as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.support_identity",
                hex(&support.as_bytes()),
            );
        }
        ModelInputCommitment::ReprojectedSeed(commitment) => {
            evidence_field(fields, "model_lifecycle.input.kind", "reprojected_seed");
            evidence_field(
                fields,
                "model_lifecycle.input.source_identity",
                hex(&commitment.source().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.source_shape_identity",
                hex(&commitment.source_shape().identity().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.preparation_contract_identity",
                hex(&commitment.preparation_contract().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.reprojection_identity",
                hex(&commitment.reprojection().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.support_identity",
                hex(&commitment.support().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.sample_identity",
                hex(&commitment.samples().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.stencil_identity",
                hex(&commitment.stencil().as_bytes()),
            );
            evidence_field(
                fields,
                "model_lifecycle.input.proof_identity",
                hex(&commitment.proof().as_bytes()),
            );
        }
        ModelInputCommitment::Generation(generation) => {
            evidence_field(fields, "model_lifecycle.input.kind", "generation");
            evidence_field(
                fields,
                "model_lifecycle.input.generation_identity",
                hex(&generation.as_bytes()),
            );
        }
    }
}

fn project_geometry(fields: &mut BTreeMap<String, String>, geometry: &CompiledGeometry) {
    evidence_field(
        fields,
        "geometry.identity",
        hex(&geometry.geometry_id().as_bytes()),
    );
    for (index, domain) in geometry.domains().iter().enumerate() {
        let prefix = format!("geometry.domains.{index}");
        match domain.role() {
            ImageDomainRole::Main => evidence_field(fields, format!("{prefix}.role"), "main"),
            ImageDomainRole::Outlier(name) => {
                evidence_field(fields, format!("{prefix}.role"), "outlier");
                evidence_field(fields, format!("{prefix}.role_name"), stable_text(name));
            }
        }
        let [width, height] = domain.shape().pixels();
        evidence_field(fields, format!("{prefix}.shape.width"), width);
        evidence_field(fields, format!("{prefix}.shape.height"), height);
        project_direction(fields, &format!("{prefix}.direction"), domain.direction());
        for (facet_index, facet) in domain.facets().iter().enumerate() {
            let facet_prefix = format!("{prefix}.facets.{facet_index}");
            let origin = facet.origin();
            let end = facet.end_exclusive();
            evidence_field(fields, format!("{facet_prefix}.origin_x"), origin[0]);
            evidence_field(fields, format!("{facet_prefix}.origin_y"), origin[1]);
            evidence_field(fields, format!("{facet_prefix}.end_x"), end[0]);
            evidence_field(fields, format!("{facet_prefix}.end_y"), end[1]);
        }
        for (axis_index, axis) in domain.axes().positions().iter().enumerate() {
            evidence_field(
                fields,
                format!("{prefix}.axes.{axis_index}"),
                image_axis(*axis),
            );
        }
    }
    project_centres(fields, geometry);
    let uvw = geometry.uvw();
    evidence_field(fields, "geometry.uvw.unit", uvw_unit(uvw.unit()));
    evidence_field(fields, "geometry.uvw.axes", uvw_axes(uvw.axes()));
    evidence_field(
        fields,
        "geometry.uvw.prediction_phase",
        visibility_phase(uvw.prediction_phase()),
    );
    project_spectral_geometry(fields, geometry);
    project_optional_identity(
        fields,
        "geometry.measures_reference",
        geometry.measures_reference(),
    );
    project_optional_identity(
        fields,
        "geometry.ephemeris_reference",
        geometry.ephemeris_reference(),
    );
}

fn project_direction(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    direction: casa_imaging_model::DirectionCoordinateSpec,
) {
    evidence_field(
        fields,
        format!("{prefix}.projection"),
        projection(direction.projection()),
    );
    project_sky_direction(
        fields,
        &format!("{prefix}.reference_direction"),
        direction.reference_direction(),
    );
    for (index, value) in direction.reference_pixel().iter().enumerate() {
        evidence_field(
            fields,
            format!("{prefix}.reference_pixel.{index}"),
            stable_float(*value),
        );
    }
    for (index, value) in direction.increment_rad().iter().enumerate() {
        evidence_field(
            fields,
            format!("{prefix}.increment_rad.{index}"),
            stable_float(*value),
        );
    }
    for (row, values) in direction.pc().iter().enumerate() {
        for (column, value) in values.iter().enumerate() {
            evidence_field(
                fields,
                format!("{prefix}.pc.{row}.{column}"),
                stable_float(*value),
            );
        }
    }
    for (index, value) in direction.pole_deg().iter().enumerate() {
        evidence_field(
            fields,
            format!("{prefix}.pole_deg.{index}"),
            stable_float(*value),
        );
    }
}

fn project_sky_direction(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    direction: casa_imaging_model::SkyDirection,
) {
    evidence_field(
        fields,
        format!("{prefix}.frame"),
        direction_frame(direction.frame()),
    );
    evidence_field(
        fields,
        format!("{prefix}.longitude_rad"),
        stable_float(direction.longitude_rad()),
    );
    evidence_field(
        fields,
        format!("{prefix}.latitude_rad"),
        stable_float(direction.latitude_rad()),
    );
}

fn project_centres(fields: &mut BTreeMap<String, String>, geometry: &CompiledGeometry) {
    let centres = geometry.centres();
    match centres.phase_tracking() {
        PhaseCentreLaw::Observation => {
            evidence_field(fields, "geometry.centres.phase.kind", "observation");
        }
        PhaseCentreLaw::Fixed(direction) => {
            evidence_field(fields, "geometry.centres.phase.kind", "fixed");
            project_sky_direction(fields, "geometry.centres.phase.direction", *direction);
        }
        PhaseCentreLaw::Ephemeris(name) => {
            evidence_field(fields, "geometry.centres.phase.kind", "ephemeris");
            evidence_field(fields, "geometry.centres.phase.name", stable_text(name));
        }
    }
    match centres.delay() {
        DelayCentreLaw::PhaseTrackingCentre => {
            evidence_field(
                fields,
                "geometry.centres.delay.kind",
                "phase_tracking_centre",
            );
        }
        DelayCentreLaw::Observation => {
            evidence_field(fields, "geometry.centres.delay.kind", "observation");
        }
        DelayCentreLaw::Fixed(direction) => {
            evidence_field(fields, "geometry.centres.delay.kind", "fixed");
            project_sky_direction(fields, "geometry.centres.delay.direction", *direction);
        }
    }
    match centres.pointing() {
        PointingCentreLaw::PhaseTrackingCentre => {
            evidence_field(
                fields,
                "geometry.centres.pointing.kind",
                "phase_tracking_centre",
            );
        }
        PointingCentreLaw::Fixed(direction) => {
            evidence_field(fields, "geometry.centres.pointing.kind", "fixed");
            project_sky_direction(fields, "geometry.centres.pointing.direction", *direction);
        }
        PointingCentreLaw::Observation(law) => {
            evidence_field(fields, "geometry.centres.pointing.kind", "observation");
            evidence_field(
                fields,
                "geometry.centres.pointing.direction_column",
                pointing_direction_column(law.direction_column()),
            );
            evidence_field(
                fields,
                "geometry.centres.pointing.direction_semantic",
                pointing_direction_semantic(law.direction_semantic()),
            );
            evidence_field(
                fields,
                "geometry.centres.pointing.time_sampling",
                pointing_time_sampling(law.time_sampling()),
            );
            evidence_field(
                fields,
                "geometry.centres.pointing.interpolation",
                pointing_interpolation(law.interpolation()),
            );
            evidence_field(
                fields,
                "geometry.centres.pointing.extrapolation",
                pointing_extrapolation(law.extrapolation()),
            );
            evidence_field(
                fields,
                "geometry.centres.pointing.missing",
                missing_pointing_policy(law.missing()),
            );
        }
    }
}

fn project_spectral_geometry(fields: &mut BTreeMap<String, String>, geometry: &CompiledGeometry) {
    let spectral = geometry.spectral();
    evidence_field(
        fields,
        "geometry.spectral.source_frame",
        frequency_frame(spectral.source_frame()),
    );
    evidence_field(
        fields,
        "geometry.spectral.output_frame",
        frequency_frame(spectral.output_frame()),
    );
    match spectral.anchor() {
        SpectralFrameAnchor::NotApplicable => {
            evidence_field(fields, "geometry.spectral.anchor.kind", "not_applicable");
        }
        SpectralFrameAnchor::Conversion {
            epoch,
            direction,
            observatory_position,
        } => {
            evidence_field(fields, "geometry.spectral.anchor.kind", "conversion");
            evidence_field(
                fields,
                "geometry.spectral.anchor.epoch_mjd_days",
                stable_float(epoch.mjd_days()),
            );
            evidence_field(
                fields,
                "geometry.spectral.anchor.epoch_scale",
                time_scale(epoch.scale()),
            );
            project_sky_direction(fields, "geometry.spectral.anchor.direction", direction);
            for (index, value) in observatory_position.metres().iter().enumerate() {
                evidence_field(
                    fields,
                    format!("geometry.spectral.anchor.observatory_position_metres.{index}"),
                    stable_float(*value),
                );
            }
        }
    }
    match spectral.wcs() {
        SpectralWcs::Linear {
            channels,
            reference_pixel,
            reference_frequency_hz,
            increment_hz,
        } => {
            evidence_field(fields, "geometry.spectral.wcs.kind", "linear");
            evidence_field(fields, "geometry.spectral.wcs.channels", *channels);
            evidence_field(
                fields,
                "geometry.spectral.wcs.reference_pixel",
                stable_float(*reference_pixel),
            );
            evidence_field(
                fields,
                "geometry.spectral.wcs.reference_frequency_hz",
                stable_float(*reference_frequency_hz),
            );
            evidence_field(
                fields,
                "geometry.spectral.wcs.increment_hz",
                stable_float(*increment_hz),
            );
        }
        SpectralWcs::Tabular {
            channel_centres_hz,
            channel_boundaries_hz,
        } => {
            evidence_field(fields, "geometry.spectral.wcs.kind", "tabular");
            for (index, value) in channel_centres_hz.iter().enumerate() {
                evidence_field(
                    fields,
                    format!("geometry.spectral.wcs.channel_centres_hz.{index}"),
                    stable_float(*value),
                );
            }
            for (index, value) in channel_boundaries_hz.iter().enumerate() {
                evidence_field(
                    fields,
                    format!("geometry.spectral.wcs.channel_boundaries_hz.{index}"),
                    stable_float(*value),
                );
            }
        }
    }
    match spectral.rest_frequency() {
        RestFrequency::NotApplicable => {
            evidence_field(
                fields,
                "geometry.spectral.rest_frequency.kind",
                "not_applicable",
            );
        }
        RestFrequency::Line { hertz } => {
            evidence_field(fields, "geometry.spectral.rest_frequency.kind", "line");
            evidence_field(
                fields,
                "geometry.spectral.rest_frequency.hertz",
                stable_float(hertz),
            );
        }
    }
    evidence_field(
        fields,
        "geometry.spectral.doppler_convention",
        doppler_convention(spectral.doppler_convention()),
    );
}

fn project_optional_identity(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    identity: Option<LogicalIdentity>,
) {
    match identity {
        Some(identity) => {
            evidence_field(fields, format!("{prefix}.kind"), "identity");
            evidence_field(
                fields,
                format!("{prefix}.identity"),
                hex(&identity.as_bytes()),
            );
        }
        None => evidence_field(fields, format!("{prefix}.kind"), "none"),
    }
}

fn project_observation(fields: &mut BTreeMap<String, String>, inputs: &ProblemInputIdentities) {
    let snapshot = inputs.observation_snapshot();
    evidence_field(
        fields,
        "observation.snapshot.identity",
        hex(&snapshot.snapshot_id().as_bytes()),
    );
    evidence_field(
        fields,
        "observation.snapshot.provenance_identity",
        hex(&snapshot.provenance_id().as_bytes()),
    );
    for (source_index, source) in snapshot.sources().iter().enumerate() {
        let prefix = format!("observation.sources.{source_index}");
        evidence_field(
            fields,
            format!("{prefix}.measurement_set_identity"),
            hex(&source.identity().identity().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.input_ordinal"),
            source.input_ordinal(),
        );
        evidence_field(
            fields,
            format!("{prefix}.provenance.locator_identity"),
            sha256(source.provenance().locator().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.provenance.selection_request_identity"),
            hex(&source.provenance().selection_request_identity().as_bytes()),
        );
        project_observation_selection(fields, &prefix, source.selection());
        project_source_generations(fields, &prefix, source.generations());
    }
    for (kind, identity) in snapshot.reference_data() {
        evidence_field(
            fields,
            format!("observation.reference_data.{}", reference_kind(*kind)),
            hex(&identity.as_bytes()),
        );
    }
    match snapshot.model() {
        ModelStateIdentity::Empty => {
            evidence_field(fields, "observation.model.kind", "empty");
        }
        ModelStateIdentity::Seed(identity) => {
            evidence_field(fields, "observation.model.kind", "seed");
            evidence_field(
                fields,
                "observation.model.identity",
                hex(&identity.as_bytes()),
            );
        }
        ModelStateIdentity::Generation(identity) => {
            evidence_field(fields, "observation.model.kind", "generation");
            evidence_field(
                fields,
                "observation.model.identity",
                hex(&identity.as_bytes()),
            );
        }
    }
}

fn project_observation_selection(
    fields: &mut BTreeMap<String, String>,
    source_prefix: &str,
    selection: &casa_imaging_model::ObservationSelection,
) {
    let prefix = format!("{source_prefix}.selection");
    let rows = selection.rows();
    evidence_field(
        fields,
        format!("{prefix}.rows.source_count"),
        rows.source_row_count(),
    );
    evidence_field(
        fields,
        format!("{prefix}.rows.selected_count"),
        rows.selected_row_count(),
    );
    evidence_field(
        fields,
        format!("{prefix}.data_descriptions.count"),
        selection.data_descriptions().len(),
    );
    evidence_field(
        fields,
        format!("{prefix}.rows.sequence_identity"),
        hex(&rows.sequence_id().as_bytes()),
    );
    let row_filter = selection.rows_filter();
    project_id_selection(
        fields,
        &format!("{prefix}.row_filter.fields"),
        row_filter.fields(),
    );
    project_time_selection(
        fields,
        &format!("{prefix}.row_filter.times"),
        row_filter.times(),
    );
    project_uv_selection(
        fields,
        &format!("{prefix}.row_filter.uv_distances"),
        row_filter.uv_distances(),
    );
    project_antenna_selection(
        fields,
        &format!("{prefix}.row_filter.antennas"),
        row_filter.antennas(),
    );
    project_id_selection(
        fields,
        &format!("{prefix}.row_filter.scans"),
        row_filter.scans(),
    );
    project_id_selection(
        fields,
        &format!("{prefix}.row_filter.observations"),
        row_filter.observations(),
    );
    project_intent_selection(
        fields,
        &format!("{prefix}.row_filter.intents"),
        row_filter.intents(),
    );
    project_id_selection(
        fields,
        &format!("{prefix}.row_filter.arrays"),
        row_filter.arrays(),
    );
    for (index, data_description) in selection.data_descriptions().iter().enumerate() {
        let data_description_prefix = format!("{prefix}.data_descriptions.{index}");
        evidence_field(
            fields,
            format!("{data_description_prefix}.data_description_id"),
            data_description.data_description_id(),
        );
        evidence_field(
            fields,
            format!("{data_description_prefix}.spectral_window_id"),
            data_description.spectral_window_id(),
        );
        evidence_field(
            fields,
            format!("{data_description_prefix}.polarization_id"),
            data_description.polarization_id(),
        );
    }
    for (index, window) in selection.spectral_windows().iter().enumerate() {
        let window_prefix = format!("{prefix}.spectral_windows.{index}");
        evidence_field(
            fields,
            format!("{window_prefix}.spectral_window_id"),
            window.spectral_window_id(),
        );
        for (item, channel) in window.channel_indices().iter().enumerate() {
            evidence_field(
                fields,
                format!("{window_prefix}.channel_indices.{item}"),
                *channel,
            );
        }
    }
    for (index, correlation) in selection.correlations().iter().enumerate() {
        let correlation_prefix = format!("{prefix}.correlations.{index}");
        evidence_field(
            fields,
            format!("{correlation_prefix}.polarization_id"),
            correlation.polarization_id(),
        );
        for (item, product) in correlation.products().iter().enumerate() {
            evidence_field(
                fields,
                format!("{correlation_prefix}.products.{item}.index"),
                product.correlation_index(),
            );
            evidence_field(
                fields,
                format!("{correlation_prefix}.products.{item}.kind"),
                correlation_type(product.correlation_type()),
            );
        }
    }
}

fn project_source_generations(
    fields: &mut BTreeMap<String, String>,
    source_prefix: &str,
    generations: &casa_imaging_model::SourceGenerations,
) {
    let prefix = format!("{source_prefix}.generations");
    evidence_field(
        fields,
        format!("{prefix}.consistency_token"),
        hex(&generations.consistency_token().identity().as_bytes()),
    );
    let columns = generations.columns();
    evidence_field(
        fields,
        format!("{prefix}.visibility_column"),
        visibility_column(columns.visibility()),
    );
    evidence_field(
        fields,
        format!("{prefix}.flag_policy"),
        flag_policy(columns.flags()),
    );
    evidence_field(
        fields,
        format!("{prefix}.weight_column"),
        weight_column(columns.weights()),
    );
    for generation in columns.generations() {
        evidence_field(
            fields,
            format!("{prefix}.columns.{}", ms_column_kind(generation.kind())),
            hex(&generation.identity().as_bytes()),
        );
    }
    for generation in generations.metadata_generations() {
        evidence_field(
            fields,
            format!(
                "{prefix}.metadata.{}",
                metadata_table_kind(generation.kind())
            ),
            hex(&generation.identity().as_bytes()),
        );
    }
}

fn project_id_selection(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    selection: &IdSelection,
) {
    match selection {
        IdSelection::All => evidence_field(fields, format!("{prefix}.kind"), "all"),
        IdSelection::Only(ids) => {
            evidence_field(fields, format!("{prefix}.kind"), "only");
            for (index, id) in ids.iter().enumerate() {
                evidence_field(fields, format!("{prefix}.ids.{index}"), *id);
            }
        }
    }
}

fn project_time_selection(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    selection: &TimeSelection,
) {
    match selection {
        TimeSelection::All => evidence_field(fields, format!("{prefix}.kind"), "all"),
        TimeSelection::Ranges(ranges) => {
            evidence_field(fields, format!("{prefix}.kind"), "ranges");
            for (index, range) in ranges.iter().enumerate() {
                project_selection_bound(fields, &format!("{prefix}.{index}.lower"), range.lower());
                project_selection_bound(fields, &format!("{prefix}.{index}.upper"), range.upper());
            }
        }
    }
}

fn project_uv_selection(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    selection: &UvSelection,
) {
    match selection {
        UvSelection::All => evidence_field(fields, format!("{prefix}.kind"), "all"),
        UvSelection::Ranges(ranges) => {
            evidence_field(fields, format!("{prefix}.kind"), "ranges");
            for (index, range) in ranges.iter().enumerate() {
                let range_prefix = format!("{prefix}.{index}");
                evidence_field(
                    fields,
                    format!("{range_prefix}.unit"),
                    uv_distance_unit(range.unit()),
                );
                project_selection_bound(fields, &format!("{range_prefix}.lower"), range.lower());
                project_selection_bound(fields, &format!("{range_prefix}.upper"), range.upper());
            }
        }
    }
}

fn project_selection_bound(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    bound: Option<casa_imaging_model::SelectionBound>,
) {
    match bound {
        None => evidence_field(fields, format!("{prefix}.kind"), "unbounded"),
        Some(bound) => {
            evidence_field(
                fields,
                format!("{prefix}.kind"),
                if bound.is_inclusive() {
                    "inclusive"
                } else {
                    "exclusive"
                },
            );
            evidence_field(
                fields,
                format!("{prefix}.value"),
                stable_float(bound.value()),
            );
        }
    }
}

fn project_antenna_selection(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    selection: &AntennaSelection,
) {
    match selection {
        AntennaSelection::All => evidence_field(fields, format!("{prefix}.kind"), "all"),
        AntennaSelection::Only(baselines) => {
            evidence_field(fields, format!("{prefix}.kind"), "only");
            for (index, baseline) in baselines.iter().enumerate() {
                let [first, second] = baseline.antennas();
                evidence_field(fields, format!("{prefix}.{index}.first"), first);
                evidence_field(fields, format!("{prefix}.{index}.second"), second);
            }
        }
    }
}

fn project_intent_selection(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    selection: &IntentSelection,
) {
    match selection {
        IntentSelection::All => evidence_field(fields, format!("{prefix}.kind"), "all"),
        IntentSelection::Only(intents) => {
            evidence_field(fields, format!("{prefix}.kind"), "only");
            for (index, intent) in intents.iter().enumerate() {
                evidence_field(
                    fields,
                    format!("{prefix}.{index}.state_id"),
                    intent.state_id(),
                );
                evidence_field(
                    fields,
                    format!("{prefix}.{index}.observation_mode"),
                    stable_text(intent.observation_mode()),
                );
            }
        }
    }
}

fn evidence_field(
    fields: &mut BTreeMap<String, String>,
    path: impl Into<String>,
    value: impl ToString,
) {
    let replaced = fields.insert(path.into(), value.to_string());
    debug_assert!(
        replaced.is_none(),
        "problem evidence field paths are unique"
    );
}

fn stable_float(value: f64) -> String {
    let bits = if value == 0.0 { 0 } else { value.to_bits() };
    format!("f64:{bits:016x}")
}

fn stable_float32(value: f32) -> String {
    let bits = if value == 0.0 { 0 } else { value.to_bits() };
    format!("f32:{bits:08x}")
}

fn spectral_sampling(value: SpectralSampling) -> &'static str {
    match value {
        SpectralSampling::Identity => "identity",
        SpectralSampling::Nearest => "nearest",
        SpectralSampling::Linear => "linear",
        SpectralSampling::ChannelAverage { .. } => "channel_average",
    }
}

fn spectral_coupling(value: SpectralCoupling) -> &'static str {
    match value {
        SpectralCoupling::Independent => "independent",
        SpectralCoupling::CommonRestoringBeam => "common_restoring_beam",
    }
}

fn instrument_response(value: InstrumentResponse) -> &'static str {
    match value {
        InstrumentResponse::Scalar => "scalar",
        InstrumentResponse::PrimaryBeam => "primary_beam",
        InstrumentResponse::FullMueller => "full_mueller",
    }
}

fn model_inner_product(value: ModelInnerProduct) -> &'static str {
    match value {
        ModelInnerProduct::HermitianEuclidean => "hermitian_euclidean",
    }
}

fn visibility_inner_product(value: VisibilityInnerProduct) -> &'static str {
    match value {
        VisibilityInnerProduct::HermitianEuclidean => "hermitian_euclidean",
    }
}

fn paired_transform_kind(value: PairedTransformKind) -> &'static str {
    match value {
        PairedTransformKind::SpectralBasis => "spectral_basis",
        PairedTransformKind::Polarization => "polarization",
        PairedTransformKind::DirectionDependentResponse => "direction_dependent_response",
        PairedTransformKind::Phase => "phase",
        PairedTransformKind::SpectralResampling => "spectral_resampling",
        PairedTransformKind::ChannelIntegration => "channel_integration",
    }
}

fn normal_state_normalization(value: NormalStateNormalization) -> &'static str {
    match value {
        NormalStateNormalization::Unnormalized => "unnormalized",
    }
}

fn normal_equation_form(value: NormalEquationForm) -> &'static str {
    match value {
        NormalEquationForm::RightHandSide => "right_hand_side_a_star_w_d",
        NormalEquationForm::Residual => "residual_a_star_w_d_minus_a_x",
        NormalEquationForm::NormalOperator => "normal_operator_a_star_w_a",
    }
}

fn reconstruction_basis(value: ReconstructionBasis) -> &'static str {
    match value {
        ReconstructionBasis::Constant => "constant",
        ReconstructionBasis::Taylor { .. } => "taylor",
        ReconstructionBasis::ChannelLocal { .. } => "channel_local",
    }
}

fn reconstruction_algorithm(value: &ReconstructionAlgorithm) -> &'static str {
    match value {
        ReconstructionAlgorithm::Dirty => "dirty",
        ReconstructionAlgorithm::Hogbom => "hogbom",
        ReconstructionAlgorithm::Clark => "clark",
        ReconstructionAlgorithm::Multiscale { .. } => "multiscale",
        ReconstructionAlgorithm::Mtmfs => "mtmfs",
    }
}

fn polarization_coordinate(value: PolarizationCoordinate) -> &'static str {
    match value {
        PolarizationCoordinate::StokesI => "stokes_i",
        PolarizationCoordinate::StokesQ => "stokes_q",
        PolarizationCoordinate::StokesU => "stokes_u",
        PolarizationCoordinate::StokesV => "stokes_v",
        PolarizationCoordinate::LinearXx => "linear_xx",
        PolarizationCoordinate::LinearXy => "linear_xy",
        PolarizationCoordinate::LinearYx => "linear_yx",
        PolarizationCoordinate::LinearYy => "linear_yy",
        PolarizationCoordinate::CircularRr => "circular_rr",
        PolarizationCoordinate::CircularRl => "circular_rl",
        PolarizationCoordinate::CircularLr => "circular_lr",
        PolarizationCoordinate::CircularLl => "circular_ll",
    }
}

fn weighting_scheme(value: WeightingScheme) -> &'static str {
    match value {
        WeightingScheme::Natural => "natural",
        WeightingScheme::Uniform => "uniform",
        WeightingScheme::Briggs { .. } => "briggs",
        WeightingScheme::BriggsBandwidthTaper { .. } => "briggs_bandwidth_taper",
    }
}

fn weight_density_scope(value: WeightDensityScope) -> &'static str {
    match value {
        WeightDensityScope::NotApplicable => "not_applicable",
        WeightDensityScope::GlobalSelection => "global_selection",
        WeightDensityScope::PerOutputChannel => "per_output_channel",
    }
}

fn product_kind(value: ProductKind) -> &'static str {
    match value {
        ProductKind::Psf => "psf",
        ProductKind::Residual => "residual",
        ProductKind::Model => "model",
        ProductKind::RestoredImage => "restored_image",
        ProductKind::SumWeights => "sum_weights",
        ProductKind::Mask => "mask",
        ProductKind::Weight => "weight",
        ProductKind::PrimaryBeam => "primary_beam",
        ProductKind::Sensitivity => "sensitivity",
        ProductKind::PbCorrectedImage => "pb_corrected_image",
        ProductKind::TaylorTerms => "taylor_terms",
        ProductKind::SpectralIndex => "spectral_index",
        ProductKind::SpectralIndexError => "spectral_index_error",
        ProductKind::PbCorrectedSpectralIndex => "pb_corrected_spectral_index",
        ProductKind::Beam => "beam",
    }
}

fn product_role(value: ProductRole) -> String {
    match value {
        ProductRole::Psf(term) => format!("psf:{}", product_term(term)),
        ProductRole::Residual(term) => format!("residual:{}", product_term(term)),
        ProductRole::Model(term) => format!("model:{}", product_term(term)),
        ProductRole::RestoredImage(term) => {
            format!("restored_image:{}", product_term(term))
        }
        ProductRole::SumWeights(term) => format!("sum_weights:{}", product_term(term)),
        ProductRole::CleanMask => "clean_mask".to_string(),
        ProductRole::Weight(term) => format!("weight:{}", product_term(term)),
        ProductRole::PrimaryBeam(term) => format!("primary_beam:{}", product_term(term)),
        ProductRole::PrimaryBeamSpectralIndex => "primary_beam_spectral_index".to_string(),
        ProductRole::Sensitivity => "sensitivity".to_string(),
        ProductRole::PbCorrectedImage(term) => {
            format!("pb_corrected_image:{}", product_term(term))
        }
        ProductRole::TaylorCoefficientSet => "taylor_coefficient_set".to_string(),
        ProductRole::SpectralIndex => "spectral_index".to_string(),
        ProductRole::SpectralIndexError => "spectral_index_error".to_string(),
        ProductRole::PbCorrectedSpectralIndex => "pb_corrected_spectral_index".to_string(),
        ProductRole::BeamMetadata => "beam_metadata".to_string(),
    }
}

fn product_term(value: ProductTerm) -> String {
    match value {
        ProductTerm::Single => "single".to_string(),
        ProductTerm::Taylor(term) => format!("taylor_{term}"),
    }
}

fn product_axis_kind(value: ProductAxisKind) -> &'static str {
    match value {
        ProductAxisKind::SkyImage => "sky_image",
        ProductAxisKind::PlaneState => "plane_state",
        ProductAxisKind::Metadata => "metadata",
    }
}

fn product_unit(value: ProductUnit) -> &'static str {
    match value {
        ProductUnit::NotApplicable => "not_applicable",
        ProductUnit::JyPerBeam => "jy_per_beam",
        ProductUnit::JyPerPixel => "jy_per_pixel",
        ProductUnit::Dimensionless => "dimensionless",
        ProductUnit::VisibilityWeight => "visibility_weight",
    }
}

fn product_schema(value: ProductSchema) -> &'static str {
    match value {
        ProductSchema::ImageF32V1 => "image_f32_v1",
        ProductSchema::LogicalCollectionV1 => "logical_collection_v1",
        ProductSchema::EmbeddedImageMetadataV1 => "embedded_image_metadata_v1",
        ProductSchema::InternalImageF32V1 => "internal_image_f32_v1",
    }
}

fn product_support_comparison(value: ProductSupportComparison) -> &'static str {
    match value {
        ProductSupportComparison::StrictlyGreater => "strictly_greater",
    }
}

fn product_blanking(value: ProductBlankingPolicy) -> &'static str {
    match value {
        ProductBlankingPolicy::ZeroAndFalseMask => "zero_and_false_mask",
    }
}

fn taylor_support_reference(value: TaylorSupportReference) -> &'static str {
    match value {
        TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum => {
            "principal_residual_taylor_0_positive_maximum"
        }
    }
}

fn product_normalization(value: ProductNormalization) -> &'static str {
    match value {
        ProductNormalization::UnitResponse => "unit_response",
        ProductNormalization::FlatNoise => "flat_noise",
        ProductNormalization::FlatSky => "flat_sky",
    }
}

fn product_boundary_operation(value: ProductBoundaryOperation) -> &'static str {
    match value {
        ProductBoundaryOperation::Normalize(_) => "normalize",
        ProductBoundaryOperation::ScaleResidual => "scale_residual",
        ProductBoundaryOperation::Restore(_) => "restore",
        ProductBoundaryOperation::CorrectPrimaryBeam => "correct_primary_beam",
        ProductBoundaryOperation::BlankInvalid => "blank_invalid",
        ProductBoundaryOperation::ConvertUnits => "convert_units",
    }
}

fn restoring_beam(value: RestoringBeamPolicy) -> &'static str {
    match value {
        RestoringBeamPolicy::None => "none",
        RestoringBeamPolicy::PerPlane => "per_plane",
        RestoringBeamPolicy::Common => "common",
    }
}

fn numeric_precision(value: NumericPrecision) -> &'static str {
    match value {
        NumericPrecision::F32 => "f32",
        NumericPrecision::F64 => "f64",
    }
}

fn parse_numeric_precision(value: &str) -> Option<NumericPrecision> {
    match value {
        "f32" => Some(NumericPrecision::F32),
        "f64" => Some(NumericPrecision::F64),
        _ => None,
    }
}

const fn model_state_encoding(value: ModelStateEncoding) -> &'static str {
    match value {
        ModelStateEncoding::CanonicalF64 => "canonical_f64",
    }
}

fn parse_model_state_encoding(value: &str) -> Option<ModelStateEncoding> {
    match value {
        "canonical_f64" => Some(ModelStateEncoding::CanonicalF64),
        _ => None,
    }
}

const fn model_support_semantics(value: ModelSupportSemantics) -> &'static str {
    match value {
        ModelSupportSemantics::ExplicitValidity => "explicit_validity",
    }
}

fn parse_model_support_semantics(value: &str) -> Option<ModelSupportSemantics> {
    match value {
        "explicit_validity" => Some(ModelSupportSemantics::ExplicitValidity),
        _ => None,
    }
}

fn reduction_policy(value: ReductionPolicy) -> &'static str {
    match value {
        ReductionPolicy::DeterministicPairwise => "deterministic_pairwise",
        ReductionPolicy::Compensated => "compensated",
        ReductionPolicy::UnorderedWithinBudget => "unordered_within_budget",
    }
}

fn finite_value_policy(value: FiniteValuePolicy) -> &'static str {
    match value {
        FiniteValuePolicy::RejectAll => "reject_all",
        FiniteValuePolicy::FlagInputRejectGenerated => "flag_input_reject_generated",
    }
}

fn numerical_stage(value: NumericalStage) -> &'static str {
    match value {
        NumericalStage::CoordinateTransforms => "coordinate_transforms",
        NumericalStage::SpectralTransforms => "spectral_transforms",
        NumericalStage::Weighting => "weighting",
        NumericalStage::ForwardOperator => "forward_operator",
        NumericalStage::AdjointOperator => "adjoint_operator",
        NumericalStage::Reductions => "reductions",
        NumericalStage::Reconstruction => "reconstruction",
        NumericalStage::Restoration => "restoration",
        NumericalStage::ProductFormation => "product_formation",
    }
}

fn required_capability(value: RequiredCapability) -> String {
    match value {
        RequiredCapability::FacetedGeometry => "faceted_geometry".to_string(),
        RequiredCapability::SpectralFrameTransform => "spectral_frame_transform".to_string(),
        RequiredCapability::SpectralResampling => "spectral_resampling".to_string(),
        RequiredCapability::CommonBeamSpectralCoupling => {
            "common_beam_spectral_coupling".to_string()
        }
        RequiredCapability::Polarization(coordinate) => {
            format!("polarization:{}", polarization_coordinate(coordinate))
        }
        RequiredCapability::PrimaryBeamResponse => "primary_beam_response".to_string(),
        RequiredCapability::FullMuellerResponse => "full_mueller_response".to_string(),
        RequiredCapability::UvTaper => "uv_taper".to_string(),
        RequiredCapability::ConstantBasis => "constant_basis".to_string(),
        RequiredCapability::TaylorBasis => "taylor_basis".to_string(),
        RequiredCapability::ChannelLocalBasis => "channel_local_basis".to_string(),
        RequiredCapability::DirtyReconstruction => "dirty_reconstruction".to_string(),
        RequiredCapability::HogbomReconstruction => "hogbom_reconstruction".to_string(),
        RequiredCapability::ClarkReconstruction => "clark_reconstruction".to_string(),
        RequiredCapability::MultiscaleReconstruction => "multiscale_reconstruction".to_string(),
        RequiredCapability::MtmfsReconstruction => "mtmfs_reconstruction".to_string(),
        RequiredCapability::NaturalWeighting => "natural_weighting".to_string(),
        RequiredCapability::UniformWeighting => "uniform_weighting".to_string(),
        RequiredCapability::BriggsWeighting => "briggs_weighting".to_string(),
        RequiredCapability::BriggsBandwidthTaperWeighting => {
            "briggs_bandwidth_taper_weighting".to_string()
        }
        RequiredCapability::UnitResponseNormalization => "unit_response_normalization".to_string(),
        RequiredCapability::FlatNoiseNormalization => "flat_noise_normalization".to_string(),
        RequiredCapability::FlatSkyNormalization => "flat_sky_normalization".to_string(),
        RequiredCapability::Product(product) => format!("product:{}", product_kind(product)),
    }
}

fn projection(value: Projection) -> &'static str {
    match value {
        Projection::Sin => "sin",
    }
}

fn direction_frame(value: DirectionFrame) -> &'static str {
    match value {
        DirectionFrame::Icrs => "icrs",
        DirectionFrame::J2000 => "j2000",
        DirectionFrame::B1950 => "b1950",
        DirectionFrame::Galactic => "galactic",
    }
}

fn image_axis(value: ImageAxis) -> &'static str {
    match value {
        ImageAxis::DirectionLongitude => "direction_longitude",
        ImageAxis::DirectionLatitude => "direction_latitude",
        ImageAxis::Polarization => "polarization",
        ImageAxis::Spectral => "spectral",
    }
}

fn pointing_direction_column(value: PointingDirectionColumn) -> &'static str {
    match value {
        PointingDirectionColumn::Direction => "direction",
        PointingDirectionColumn::Target => "target",
    }
}

fn pointing_direction_semantic(value: PointingDirectionSemantic) -> &'static str {
    match value {
        PointingDirectionSemantic::AntennaBoresight => "antenna_boresight",
        PointingDirectionSemantic::TrackingTarget => "tracking_target",
    }
}

fn pointing_time_sampling(value: PointingTimeSampling) -> &'static str {
    match value {
        PointingTimeSampling::VisibilityTime => "visibility_time",
        PointingTimeSampling::VisibilityTimeCentroid => "visibility_time_centroid",
    }
}

fn pointing_interpolation(value: PointingInterpolation) -> &'static str {
    match value {
        PointingInterpolation::Nearest => "nearest",
        PointingInterpolation::GreatCircleShortestArc => "great_circle_shortest_arc",
    }
}

fn pointing_extrapolation(value: PointingExtrapolation) -> &'static str {
    match value {
        PointingExtrapolation::Reject => "reject",
        PointingExtrapolation::HoldNearest => "hold_nearest",
    }
}

fn missing_pointing_policy(value: MissingPointingPolicy) -> &'static str {
    match value {
        MissingPointingPolicy::Reject => "reject",
        MissingPointingPolicy::UsePhaseTrackingCentre => "use_phase_tracking_centre",
    }
}

fn uvw_unit(value: UvwUnit) -> &'static str {
    match value {
        UvwUnit::Metres => "metres",
    }
}

fn uvw_axes(value: UvwAxes) -> &'static str {
    match value {
        UvwAxes::EastNorthPhaseTrackingCentre => "east_north_phase_tracking_centre",
    }
}

fn visibility_phase(value: VisibilityPhaseConvention) -> &'static str {
    match value {
        VisibilityPhaseConvention::NegativeTwoPiFrequencyDelay => "negative_two_pi_frequency_delay",
    }
}

fn frequency_frame(value: FrequencyFrame) -> &'static str {
    match value {
        FrequencyFrame::Topocentric => "topocentric",
        FrequencyFrame::Barycentric => "barycentric",
        FrequencyFrame::Lsrk => "lsrk",
    }
}

fn time_scale(value: TimeScale) -> &'static str {
    match value {
        TimeScale::Utc => "utc",
        TimeScale::Tai => "tai",
        TimeScale::Tt => "tt",
        TimeScale::Tdb => "tdb",
    }
}

fn doppler_convention(value: DopplerConvention) -> &'static str {
    match value {
        DopplerConvention::NotApplicable => "not_applicable",
        DopplerConvention::Radio => "radio",
        DopplerConvention::Optical => "optical",
        DopplerConvention::Relativistic => "relativistic",
    }
}

fn uv_distance_unit(value: UvDistanceUnit) -> &'static str {
    match value {
        UvDistanceUnit::Meters => "meters",
        UvDistanceUnit::Wavelengths => "wavelengths",
    }
}

fn correlation_type(value: CorrelationType) -> &'static str {
    match value {
        CorrelationType::StokesI => "stokes_i",
        CorrelationType::StokesQ => "stokes_q",
        CorrelationType::StokesU => "stokes_u",
        CorrelationType::StokesV => "stokes_v",
        CorrelationType::CircularRr => "circular_rr",
        CorrelationType::CircularRl => "circular_rl",
        CorrelationType::CircularLr => "circular_lr",
        CorrelationType::CircularLl => "circular_ll",
        CorrelationType::LinearXx => "linear_xx",
        CorrelationType::LinearXy => "linear_xy",
        CorrelationType::LinearYx => "linear_yx",
        CorrelationType::LinearYy => "linear_yy",
        CorrelationType::MixedRx => "mixed_rx",
        CorrelationType::MixedRy => "mixed_ry",
        CorrelationType::MixedLx => "mixed_lx",
        CorrelationType::MixedLy => "mixed_ly",
        CorrelationType::MixedXr => "mixed_xr",
        CorrelationType::MixedXl => "mixed_xl",
        CorrelationType::MixedYr => "mixed_yr",
        CorrelationType::MixedYl => "mixed_yl",
        CorrelationType::QuasiOrthogonalPp => "quasi_orthogonal_pp",
        CorrelationType::QuasiOrthogonalPq => "quasi_orthogonal_pq",
        CorrelationType::QuasiOrthogonalQp => "quasi_orthogonal_qp",
        CorrelationType::QuasiOrthogonalQq => "quasi_orthogonal_qq",
        CorrelationType::RightCircular => "right_circular",
        CorrelationType::LeftCircular => "left_circular",
        CorrelationType::Linear => "linear",
        CorrelationType::PolarizedIntensity => "polarized_intensity",
        CorrelationType::LinearPolarizedIntensity => "linear_polarized_intensity",
        CorrelationType::FractionalPolarizedIntensity => "fractional_polarized_intensity",
        CorrelationType::FractionalLinearPolarizedIntensity => {
            "fractional_linear_polarized_intensity"
        }
        CorrelationType::PolarizationAngle => "polarization_angle",
    }
}

fn visibility_column(value: VisibilityColumn) -> &'static str {
    match value {
        VisibilityColumn::Data => "data",
        VisibilityColumn::CorrectedData => "corrected_data",
        VisibilityColumn::FloatData => "float_data",
    }
}

fn flag_policy(value: FlagPolicy) -> &'static str {
    match value {
        FlagPolicy::FlagOrFlagRow => "flag_or_flag_row",
    }
}

fn weight_column(value: WeightColumn) -> &'static str {
    match value {
        WeightColumn::Weight => "weight",
        WeightColumn::WeightSpectrum => "weight_spectrum",
    }
}

fn ms_column_kind(value: MsColumnKind) -> &'static str {
    match value {
        MsColumnKind::Data => "data",
        MsColumnKind::CorrectedData => "corrected_data",
        MsColumnKind::FloatData => "float_data",
        MsColumnKind::Flag => "flag",
        MsColumnKind::FlagRow => "flag_row",
        MsColumnKind::Weight => "weight",
        MsColumnKind::WeightSpectrum => "weight_spectrum",
        MsColumnKind::Uvw => "uvw",
        MsColumnKind::Time => "time",
        MsColumnKind::TimeCentroid => "time_centroid",
        MsColumnKind::Interval => "interval",
        MsColumnKind::Exposure => "exposure",
        MsColumnKind::FieldId => "field_id",
        MsColumnKind::DataDescriptionId => "data_description_id",
        MsColumnKind::Antenna1 => "antenna_1",
        MsColumnKind::Antenna2 => "antenna_2",
        MsColumnKind::Feed1 => "feed_1",
        MsColumnKind::Feed2 => "feed_2",
        MsColumnKind::ScanNumber => "scan_number",
        MsColumnKind::StateId => "state_id",
        MsColumnKind::ObservationId => "observation_id",
        MsColumnKind::ArrayId => "array_id",
        MsColumnKind::ModelData => "model_data",
    }
}

fn metadata_table_kind(value: MetadataTableKind) -> &'static str {
    match value {
        MetadataTableKind::Antenna => "antenna",
        MetadataTableKind::DataDescription => "data_description",
        MetadataTableKind::Doppler => "doppler",
        MetadataTableKind::Feed => "feed",
        MetadataTableKind::Field => "field",
        MetadataTableKind::FrequencyOffset => "frequency_offset",
        MetadataTableKind::Observation => "observation",
        MetadataTableKind::Pointing => "pointing",
        MetadataTableKind::Polarization => "polarization",
        MetadataTableKind::Source => "source",
        MetadataTableKind::SpectralWindow => "spectral_window",
        MetadataTableKind::State => "state",
        MetadataTableKind::SysCal => "sys_cal",
        MetadataTableKind::Weather => "weather",
    }
}

fn model_projection(inputs: &ProblemInputIdentities) -> ModelIdentityProjection {
    match inputs.model() {
        ModelStateIdentity::Empty => ModelIdentityProjection::Empty,
        ModelStateIdentity::Seed(identity) => {
            ModelIdentityProjection::Seed(hex(&identity.as_bytes()))
        }
        ModelStateIdentity::Generation(identity) => {
            ModelIdentityProjection::Generation(hex(&identity.as_bytes()))
        }
    }
}

fn reference_kind(kind: ReferenceDataKind) -> &'static str {
    match kind {
        ReferenceDataKind::Measures => "measures",
        ReferenceDataKind::Ephemeris => "ephemeris",
        ReferenceDataKind::Observatory => "observatory",
        ReferenceDataKind::SpectralLines => "spectral_lines",
        ReferenceDataKind::Instrument => "instrument",
    }
}

const fn route_disposition(disposition: ExecutionRouteDisposition) -> &'static str {
    match disposition {
        ExecutionRouteDisposition::Native => "native",
        ExecutionRouteDisposition::LegacyWholeRun => "legacy_whole_run",
        ExecutionRouteDisposition::TemporarilyUnavailable => "temporarily_unavailable",
    }
}

const fn route_requirement_kind(kind: ExecutionRouteRequirementKind) -> &'static str {
    match kind {
        ExecutionRouteRequirementKind::Capability => "capability",
        ExecutionRouteRequirementKind::Product => "product",
        ExecutionRouteRequirementKind::Solver => "solver",
        ExecutionRouteRequirementKind::Frontend => "frontend",
        ExecutionRouteRequirementKind::Backend => "backend",
    }
}

fn execution_route_disposition(value: &str) -> Option<ExecutionRouteDisposition> {
    match value {
        "native" => Some(ExecutionRouteDisposition::Native),
        "legacy_whole_run" => Some(ExecutionRouteDisposition::LegacyWholeRun),
        "temporarily_unavailable" => Some(ExecutionRouteDisposition::TemporarilyUnavailable),
        _ => None,
    }
}

fn execution_route_requirement_kind(value: &str) -> Option<ExecutionRouteRequirementKind> {
    match value {
        "capability" => Some(ExecutionRouteRequirementKind::Capability),
        "product" => Some(ExecutionRouteRequirementKind::Product),
        "solver" => Some(ExecutionRouteRequirementKind::Solver),
        "frontend" => Some(ExecutionRouteRequirementKind::Frontend),
        "backend" => Some(ExecutionRouteRequirementKind::Backend),
        _ => None,
    }
}

fn route_disposition_is_valid(value: &str) -> bool {
    matches!(
        value,
        "native" | "legacy_whole_run" | "temporarily_unavailable"
    )
}

fn route_requirement_kind_is_valid(value: &str) -> bool {
    matches!(
        value,
        "capability" | "product" | "solver" | "frontend" | "backend"
    )
}

fn work_kind(kind: WorkKind) -> &'static str {
    match kind {
        WorkKind::DataCensus => "data_census",
        WorkKind::Preparation => "preparation",
        WorkKind::Cache => "cache",
        WorkKind::ConvolutionFunction => "convolution_function",
        WorkKind::FftPlanning => "fft_planning",
        WorkKind::Jit => "jit",
        WorkKind::Compute => "compute",
        WorkKind::Transfer => "transfer",
        WorkKind::Spill => "spill",
        WorkKind::Prefetch => "prefetch",
        WorkKind::Io => "io",
        WorkKind::ObservationRead => "observation_read",
        WorkKind::Serialization => "serialization",
        WorkKind::Writeback => "writeback",
        WorkKind::Publication => "publication",
        WorkKind::Release => "release",
        WorkKind::Synchronization => "synchronization",
    }
}

fn work_domain(domain: &WorkDomain) -> String {
    match domain {
        WorkDomain::Cpu => "cpu".to_string(),
        WorkDomain::Metal { demand_id } => format!("metal:{}", stable_text(demand_id)),
        WorkDomain::Io => "io".to_string(),
        WorkDomain::Control => "control".to_string(),
    }
}

fn dependency(dependency: &WorkDependency) -> String {
    match dependency {
        WorkDependency::Work(node) => format!("work:{}", stable_text(node.as_str())),
        WorkDependency::Fence(fence) => format!(
            "fence:{}:{}",
            stable_text(fence.node().as_str()),
            fence_kind(fence.kind())
        ),
    }
}

fn parse_dependency(value: &str) -> WorkDependency {
    if let Some(node) = value.strip_prefix("work:") {
        return WorkDependency::Work(WorkNodeId::new(node.to_string()));
    }
    let value = value
        .strip_prefix("fence:")
        .expect("validated publication terminal");
    let (node, kind) = value
        .rsplit_once(':')
        .expect("validated publication fence terminal");
    WorkDependency::Fence(FenceId::new(
        WorkNodeId::new(node.to_string()),
        parse_fence_kind(kind),
    ))
}

fn claim_lifetime(lifetime: &ClaimLifetime) -> String {
    match lifetime {
        ClaimLifetime::Work => "work".to_string(),
        ClaimLifetime::Fences(kinds) => format!(
            "fences:{}",
            kinds
                .iter()
                .map(|kind| fence_kind(*kind))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

fn parse_claim_lifetime(value: &str) -> ClaimLifetime {
    if value == "work" {
        return ClaimLifetime::Work;
    }
    ClaimLifetime::through_fences(
        value
            .strip_prefix("fences:")
            .expect("validated claim lifetime")
            .split(',')
            .map(parse_fence_kind),
    )
}

fn lease_resource(resource: &LeaseResource) -> String {
    match resource {
        LeaseResource::Memory { allocation_id } => format!("memory:{}", stable_text(allocation_id)),
        LeaseResource::Workers => "workers".to_string(),
        LeaseResource::RuntimeOverhead(kind) => {
            format!("runtime_overhead:{}", runtime_overhead(*kind))
        }
        LeaseResource::IoBuffer(kind) => format!("io_buffer:{}", io_buffer(*kind)),
        LeaseResource::Storage {
            demand_id,
            use_kind,
        } => {
            format!(
                "storage:{}:{}",
                stable_text(demand_id),
                storage_use(*use_kind)
            )
        }
        LeaseResource::StorageReadRate { demand_id } => {
            format!("storage_read_rate:{}", stable_text(demand_id))
        }
        LeaseResource::StorageWriteRate { demand_id } => {
            format!("storage_write_rate:{}", stable_text(demand_id))
        }
        LeaseResource::StorageOperationsRate { demand_id } => {
            format!("storage_operations_rate:{}", stable_text(demand_id))
        }
        LeaseResource::StorageQueue { demand_id } => {
            format!("storage_queue:{}", stable_text(demand_id))
        }
        LeaseResource::Rate { demand_id } => format!("rate:{}", stable_text(demand_id)),
        LeaseResource::Queue { demand_id } => format!("queue:{}", stable_text(demand_id)),
        LeaseResource::TransferRate { demand_id } => {
            format!("transfer_rate:{}", stable_text(demand_id))
        }
        LeaseResource::TransferQueue { demand_id } => {
            format!("transfer_queue:{}", stable_text(demand_id))
        }
        LeaseResource::Accelerator { demand_id } => {
            format!("accelerator:{}", stable_text(demand_id))
        }
        LeaseResource::AcceleratorCommandQueue { demand_id } => {
            format!("accelerator_queue:{}", stable_text(demand_id))
        }
        LeaseResource::ResidentCache => "resident_cache".to_string(),
        LeaseResource::MeasurementSetLock { measurement_set } => {
            format!("measurement_set_lock:{}", measurement_set)
        }
        LeaseResource::Locks => "locks".to_string(),
        LeaseResource::FileDescriptors => "file_descriptors".to_string(),
    }
}

fn runtime_overhead(kind: crate::RuntimeOverheadKind) -> &'static str {
    match kind {
        crate::RuntimeOverheadKind::ThreadStack => "thread_stack",
        crate::RuntimeOverheadKind::AllocatorFragmentation => "allocator_fragmentation",
        crate::RuntimeOverheadKind::ExternalLibrary => "external_library",
        crate::RuntimeOverheadKind::FftWorkspace => "fft_workspace",
        crate::RuntimeOverheadKind::Driver => "driver",
        crate::RuntimeOverheadKind::Jit => "jit",
        crate::RuntimeOverheadKind::CommandBuffer => "command_buffer",
    }
}

fn storage_use(kind: crate::StorageUseKind) -> &'static str {
    match kind {
        crate::StorageUseKind::Temporary => "temporary",
        crate::StorageUseKind::StagedOutput => "staged_output",
        crate::StorageUseKind::FinalOutput => "final_output",
        crate::StorageUseKind::PersistentCache => "persistent_cache",
    }
}

fn io_buffer(kind: crate::IoBufferKind) -> &'static str {
    match kind {
        crate::IoBufferKind::SourceReadAhead => "source_read_ahead",
        crate::IoBufferKind::Decode => "decode",
        crate::IoBufferKind::Preparation => "preparation",
        crate::IoBufferKind::HostToDeviceTransfer => "host_to_device_transfer",
        crate::IoBufferKind::DeviceToHostTransfer => "device_to_host_transfer",
        crate::IoBufferKind::SpillRead => "spill_read",
        crate::IoBufferKind::SpillWrite => "spill_write",
        crate::IoBufferKind::Serialization => "serialization",
        crate::IoBufferKind::StorageManager => "storage_manager",
        crate::IoBufferKind::TiledColumnWriter => "tiled_column_writer",
        crate::IoBufferKind::ScalarColumnWriter => "scalar_column_writer",
        crate::IoBufferKind::Writeback => "writeback",
        crate::IoBufferKind::Publication => "publication",
        crate::IoBufferKind::MappedPageCache => "mapped_page_cache",
    }
}

fn parse_io_buffer(value: &str) -> IoBufferKind {
    match value {
        "source_read_ahead" => IoBufferKind::SourceReadAhead,
        "decode" => IoBufferKind::Decode,
        "preparation" => IoBufferKind::Preparation,
        "host_to_device_transfer" => IoBufferKind::HostToDeviceTransfer,
        "device_to_host_transfer" => IoBufferKind::DeviceToHostTransfer,
        "spill_read" => IoBufferKind::SpillRead,
        "spill_write" => IoBufferKind::SpillWrite,
        "serialization" => IoBufferKind::Serialization,
        "storage_manager" => IoBufferKind::StorageManager,
        "tiled_column_writer" => IoBufferKind::TiledColumnWriter,
        "scalar_column_writer" => IoBufferKind::ScalarColumnWriter,
        "writeback" => IoBufferKind::Writeback,
        "publication" => IoBufferKind::Publication,
        "mapped_page_cache" => IoBufferKind::MappedPageCache,
        _ => unreachable!("validated receipt I/O buffer projection"),
    }
}

fn fence_kind(kind: FenceKind) -> &'static str {
    match kind {
        FenceKind::Device => "device",
        FenceKind::Io => "io",
        FenceKind::Writeback => "writeback",
        FenceKind::Publication => "publication",
    }
}

fn quiescence(point: QuiescencePoint) -> &'static str {
    match point {
        QuiescencePoint::RunBoundary => "run_boundary",
        QuiescencePoint::Stage => "stage",
        QuiescencePoint::MajorCycle => "major_cycle",
        QuiescencePoint::TileBatch => "tile_batch",
        QuiescencePoint::Slab => "slab",
    }
}

fn parse_quiescence(value: &str) -> QuiescencePoint {
    match value {
        "run_boundary" => QuiescencePoint::RunBoundary,
        "stage" => QuiescencePoint::Stage,
        "major_cycle" => QuiescencePoint::MajorCycle,
        "tile_batch" => QuiescencePoint::TileBatch,
        "slab" => QuiescencePoint::Slab,
        _ => unreachable!("validated receipt quiescence projection"),
    }
}

fn parse_fence_kind(value: &str) -> FenceKind {
    match value {
        "device" => FenceKind::Device,
        "io" => FenceKind::Io,
        "writeback" => FenceKind::Writeback,
        "publication" => FenceKind::Publication,
        _ => unreachable!("validated receipt fence-kind projection"),
    }
}

fn stable_text(value: &str) -> String {
    if value.contains('/') || value.contains('\\') || value.starts_with('~') {
        format!("redacted:{}", sha256(value.as_bytes()))
    } else {
        value.to_string()
    }
}

fn bounded_evidence_text(value: &str) -> String {
    let stable = stable_text(value);
    if stable.len() <= MAX_FAILURE_SUBJECT_BYTES {
        stable
    } else {
        format!("redacted:{}", sha256(value.as_bytes()))
    }
}

fn maximum_json_escaped_evidence() -> String {
    "\0".repeat(MAX_FAILURE_SUBJECT_BYTES)
}

fn maximum_json_serialized_text<'text>(
    values: impl IntoIterator<Item = &'text str>,
) -> Option<&'text str> {
    values.into_iter().max_by_key(|value| {
        serde_json::to_vec(value)
            .expect("serializing receipt identity text cannot fail")
            .len()
    })
}

fn now_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn elapsed_nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_nonzero_digest(value: &str) -> bool {
    is_digest(value) && value.bytes().any(|byte| byte != b'0')
}

fn parse_digest(value: &str) -> [u8; 32] {
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("validated receipt digest");
    }
    digest
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        ExecutionReceiptStore, ExecutionRouteDisposition, ExecutionRouteEvidence,
        ExecutionRouteRequirement, ExecutionRouteRequirementEvidence,
        ExecutionRouteRequirementKind, ReceiptError, ReceiptRetention,
        maximum_json_serialized_text, prepared_publication_bytes, stable_float, staged_receipt,
    };

    #[test]
    fn stable_float_canonicalizes_only_signed_zero() {
        assert_eq!(stable_float(0.0), stable_float(-0.0));
        assert_ne!(stable_float(1.0), stable_float(-1.0));
        assert_ne!(
            stable_float(f64::from_bits(0x7ff8_0000_0000_0001)),
            stable_float(f64::from_bits(0x7ff8_0000_0000_0002))
        );
    }

    #[test]
    fn worst_case_failure_node_prefers_json_escaped_size_over_raw_length() {
        let longer_plain = "x".repeat(12);
        let shorter_control_heavy = "\0".repeat(3);
        assert!(longer_plain.len() > shorter_control_heavy.len());

        assert_eq!(
            maximum_json_serialized_text([longer_plain.as_str(), shorter_control_heavy.as_str()]),
            Some(shorter_control_heavy.as_str())
        );
    }

    #[test]
    fn stores_for_the_same_canonical_root_share_one_process_lock() {
        let directory = tempfile::tempdir().expect("receipt directory");
        let retention = ReceiptRetention::new(1, 1_048_576).expect("retention");
        let first =
            ExecutionReceiptStore::new(directory.path(), retention).expect("first receipt store");
        let second = ExecutionReceiptStore::new(directory.path().join("."), retention)
            .expect("second receipt store");

        assert!(Arc::ptr_eq(&first.state, &second.state));
    }

    #[test]
    fn stores_for_the_same_canonical_root_reject_conflicting_retention() {
        let directory = tempfile::tempdir().expect("receipt directory");
        let first_retention = ReceiptRetention::new(1, 1_048_576).expect("retention");
        let second_retention = ReceiptRetention::new(2, 1_048_576).expect("retention");
        let _first = ExecutionReceiptStore::new(directory.path(), first_retention)
            .expect("first receipt store");

        assert!(matches!(
            ExecutionReceiptStore::new(directory.path().join("."), second_retention),
            Err(ReceiptError::ConflictingRetention)
        ));
    }

    #[test]
    fn opening_a_store_removes_and_syncs_orphaned_receipt_staging_files() {
        let directory = tempfile::tempdir().expect("receipt directory");
        let target = directory.path().join("orphan.receipt.json");
        let (_, staging) = staged_receipt(&target, b"pre-synced terminal candidate")
            .expect("stage terminal receipt candidate");
        let (staging_file, staging_path) = staging.keep().expect("persist orphaned staging file");
        drop(staging_file);
        assert!(staging_path.exists());

        let _store = ExecutionReceiptStore::new(
            directory.path(),
            ReceiptRetention::new(1, 1_048_576).expect("retention"),
        )
        .expect("receipt store removes orphaned staging");

        assert!(!staging_path.exists());
    }

    #[test]
    fn prepared_and_terminal_bytes_cross_their_joint_retention_boundary() {
        let prepared_bytes = 11;
        let terminal_bytes = 13;
        let worst_case_prepared_bytes = 12;
        let ceiling_between_max_and_sum = 20_u64;

        assert!(
            u64::try_from(prepared_bytes.max(terminal_bytes)).expect("test byte count")
                <= ceiling_between_max_and_sum
        );
        assert!(
            prepared_publication_bytes(prepared_bytes, terminal_bytes, worst_case_prepared_bytes,)
                .expect("checked joint reservation")
                > ceiling_between_max_and_sum
        );
    }

    #[test]
    fn route_evidence_rejects_a_disposition_that_disagrees_with_its_rows() {
        let row = ExecutionRouteRequirement::new(
            "capability.compiled-problem",
            ExecutionRouteRequirementKind::Capability,
            ExecutionRouteDisposition::LegacyWholeRun,
            ExecutionRouteRequirementEvidence {
                current_owner: "legacy-imaging".to_string(),
                destination_tickets: vec!["T06".to_string()],
                evidence_issues: vec![492],
                baseline_manifests: vec!["tests/fixtures/imaging/manifest.json".to_string()],
                acceptance_contract: "compiled-problem-v1".to_string(),
                transfer_point: "T06 acceptance".to_string(),
                deletion_condition: "legacy compiler removed".to_string(),
                source_evidence: vec!["crates/casa-imaging/src/lib.rs".to_string()],
                obligation_ticket: Some("T06".to_string()),
                obligation_reason: Some("transfer remains incomplete".to_string()),
            },
        )
        .expect("canonical route row");

        assert!(matches!(
            ExecutionRouteEvidence::new(1, 5, ExecutionRouteDisposition::Native, vec![row]),
            Err(ReceiptError::InvalidRouteEvidence)
        ));
    }
}
