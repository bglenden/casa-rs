// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use casa_imaging_model::{
    AntennaSelection, CompiledGeometry, CompiledProblem, CorrelationType, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, Epoch, FiniteValuePolicy,
    FlagPolicy, FrequencyFrame, IdSelection, ImageAxis, ImageDomainRole, InstrumentResponse,
    IntentSelection, ItrfPosition, LogicalIdentity, MetadataTableKind, MissingPointingPolicy,
    ModelColumnInitialization, ModelColumnPrecondition, ModelColumnWriteDisposition,
    ModelInnerProduct, ModelStateIdentity, MsColumnKind, NormalEquationForm,
    NormalStateNormalization, NumericPrecision, NumericalStage, PairedMeasurementTransform,
    PairedTransformKind, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInputIdentities, ProductAxisKind,
    ProductBeamRule, ProductBlankingPolicy, ProductBoundaryOperation, ProductElementRepresentation,
    ProductGenerationId, ProductGraphId, ProductKind, ProductNormalization, ProductRole,
    ProductSchema, ProductSourceGenerationId, ProductSourceRole, ProductSupportComparison,
    ProductTerm, ProductUnit, ProductValidityRule, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReductionPolicy, ReferenceDataKind, RequiredCapability, RestFrequency,
    RestoringBeamPolicy, SkyDirection, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSampling, SpectralWcs, TaylorSupportReference,
    TaylorValidityPolicy, TimeScale, TimeSelection, UvDistanceUnit, UvSelection, UvwAxes, UvwUnit,
    VisibilityColumn, VisibilityInnerProduct, VisibilityPhaseConvention, WeightColumn,
    WeightDensityScope, WeightingScheme,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::{
    AdaptationId, AdaptationTransition, AllocationId, AllocationPurpose, ArtifactDisposition,
    ArtifactIdentity, ArtifactMeasurement, ArtifactRole, CacheIdentity, CapabilityId,
    ClaimLifetime, DemandAlternative, ExecutionKnobs, ExecutionPlan, FenceId, FenceKind,
    InitializationPolicy, IoBufferKind, LeaseResource, PhysicalLayoutId, PhysicalSlotId,
    PublicationParticipant, QuiescencePoint, ResourcePolicy, StorageMode, WorkDependency,
    WorkDomain, WorkImplementationId, WorkKind, WorkMeasurements, WorkNodeId,
};

const RECEIPT_SCHEMA: &str = "casa-rs-imaging-execution-receipt";
const RECEIPT_SCHEMA_VERSION: u32 = 3;
const COMPILED_PROBLEM_EVIDENCE_VERSION: u32 = 5;
const RECEIPT_SUFFIX: &str = ".receipt.json";
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

/// Provenance supplied at the sole execution seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionProvenance {
    attempt: ExecutionAttemptId,
    build: BuildIdentity,
}

impl ExecutionProvenance {
    /// Bind one unique attempt to the exact executable build.
    #[must_use]
    pub const fn new(attempt: ExecutionAttemptId, build: BuildIdentity) -> Self {
        Self { attempt, build }
    }

    /// Return the attempt identity used to reopen its receipt.
    #[must_use]
    pub const fn attempt_id(self) -> ExecutionAttemptId {
        self.attempt
    }

    /// Return the exact executable build identity.
    #[must_use]
    pub const fn build_identity(self) -> BuildIdentity {
        self.build
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
        !matches!(self, Self::Planned | Self::Running)
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
        /// Mandatory requested amount.
        required: u64,
        /// Available amount after policy, pressure, and active reservations.
        available: u64,
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

/// Stable, versioned field projection of one effective Compiled Problem.
///
/// This is audit evidence only. It deliberately cannot be converted back into
/// a [`CompiledProblem`] and must not be used as planning or science authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledProblemEvidence {
    schema_version: u32,
    fields: BTreeMap<String, String>,
    product_graph: ProductGraphEvidence,
}

impl CompiledProblemEvidence {
    /// Project every stable field of an immutable Compiled Problem for audit comparison.
    #[must_use]
    pub fn project(problem: &CompiledProblem) -> Self {
        Self {
            schema_version: COMPILED_PROBLEM_EVIDENCE_VERSION,
            fields: project_problem_fields(problem),
            product_graph: ProductGraphEvidence::new(problem),
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

    /// Return the compiler-owned product graph audit projection.
    #[must_use]
    pub const fn product_graph(&self) -> &ProductGraphEvidence {
        &self.product_graph
    }
}

/// Immutable receipt projection of the compiler-owned Product Graph.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductGraphEvidence {
    schema_version: u32,
    graph_identity: String,
    sources: Vec<ProductSourceEvidence>,
    nodes: Vec<ProductGraphNodeEvidence>,
    publication_members: Vec<usize>,
}

impl ProductGraphEvidence {
    fn new(problem: &CompiledProblem) -> Self {
        let graph = problem.product_graph();
        Self {
            schema_version: graph.schema_version(),
            graph_identity: hex(&graph.graph_id().as_bytes()),
            sources: graph
                .sources()
                .iter()
                .map(ProductSourceEvidence::new)
                .collect(),
            nodes: graph
                .nodes()
                .iter()
                .map(ProductGraphNodeEvidence::new)
                .collect(),
            publication_members: graph
                .publication()
                .members()
                .iter()
                .map(|member| member.ordinal())
                .collect(),
        }
    }

    /// Return the embedded Product Graph schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Return the exact compiler-owned Product Graph identity.
    #[must_use]
    pub fn graph_id(&self) -> [u8; 32] {
        parse_digest(&self.graph_identity)
    }

    /// Return every compiler-owned source slot in canonical order.
    #[must_use]
    pub fn sources(&self) -> &[ProductSourceEvidence] {
        &self.sources
    }

    /// Return every projected graph node in canonical order.
    #[must_use]
    pub fn nodes(&self) -> &[ProductGraphNodeEvidence] {
        &self.nodes
    }

    /// Return the exact physical publication members in canonical order.
    #[must_use]
    pub fn publication_members(&self) -> &[usize] {
        &self.publication_members
    }
}

/// One compiler-owned source slot preserved as immutable receipt evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductSourceEvidence {
    source_ordinal: usize,
    role: ProductSourceRoleProjection,
    domain: ImageDomainProjection,
    term: ProductTermProjection,
}

impl ProductSourceEvidence {
    fn new(source: &casa_imaging_model::ProductSource) -> Self {
        Self {
            source_ordinal: source.source_id().ordinal(),
            role: ProductSourceRoleProjection::new(source.role()),
            domain: ImageDomainProjection::new(source.domain()),
            term: ProductTermProjection::new(source.term()),
        }
    }

    /// Return the graph-local source ordinal.
    #[must_use]
    pub const fn source_ordinal(&self) -> usize {
        self.source_ordinal
    }

    /// Return the exact upstream generation role.
    #[must_use]
    pub const fn role(&self) -> ProductSourceRole {
        self.role.to_runtime()
    }

    /// Return the image domain supplied by this source.
    #[must_use]
    pub fn domain(&self) -> ImageDomainRole {
        self.domain.to_runtime()
    }

    /// Return the coefficient placement supplied by this source.
    #[must_use]
    pub const fn term(&self) -> ProductTerm {
        self.term.to_runtime()
    }
}

/// One immutable Product Graph node projection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductGraphNodeEvidence {
    node_id: usize,
    role: ProductRoleProjection,
    name: Option<String>,
    axes: ProductAxesEvidence,
    unit: ProductUnitProjection,
    normalization: Option<ProductNormalizationProjection>,
    beam: ProductBeamEvidence,
    validity: ProductValidityProjection,
    schema: ProductSchemaProjection,
    payload: ProductPayloadEvidence,
    source_dependencies: Vec<usize>,
    dependencies: Vec<usize>,
}

impl ProductGraphNodeEvidence {
    fn new(node: &casa_imaging_model::ProductNode) -> Self {
        Self {
            node_id: node.node_id().ordinal(),
            role: ProductRoleProjection::new(node.role()),
            name: node.name().map(stable_text),
            axes: ProductAxesEvidence::new(node.axes()),
            unit: ProductUnitProjection::new(node.unit()),
            normalization: node
                .normalization()
                .map(ProductNormalizationProjection::new),
            beam: ProductBeamEvidence::new(node.beam()),
            validity: ProductValidityProjection::new(node.validity()),
            schema: ProductSchemaProjection::new(node.schema()),
            payload: ProductPayloadEvidence::new(node.payload()),
            source_dependencies: node
                .source_dependencies()
                .iter()
                .map(|source| source.ordinal())
                .collect(),
            dependencies: node
                .dependencies()
                .iter()
                .map(|dependency| dependency.ordinal())
                .collect(),
        }
    }

    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node_ordinal(&self) -> usize {
        self.node_id
    }

    /// Return the exact logical product role.
    #[must_use]
    pub const fn role(&self) -> ProductRole {
        self.role.to_runtime()
    }

    /// Return the CASA-compatible suffix, when this node owns a persisted image.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Return the exact coordinate and storage-axis projection.
    #[must_use]
    pub const fn axes(&self) -> &ProductAxesEvidence {
        &self.axes
    }

    /// Return the required physical unit.
    #[must_use]
    pub const fn unit(&self) -> ProductUnit {
        self.unit.to_runtime()
    }

    /// Return normalization semantics, when numeric normalization applies.
    #[must_use]
    pub const fn normalization(&self) -> Option<ProductNormalization> {
        match self.normalization {
            Some(normalization) => Some(normalization.to_runtime()),
            None => None,
        }
    }

    /// Return fitted, restoring, inherited, or absent beam semantics.
    #[must_use]
    pub const fn beam(&self) -> ProductBeamEvidence {
        self.beam
    }

    /// Return the exact output-validity rule.
    #[must_use]
    pub fn validity(&self) -> ProductValidityRule {
        self.validity.to_runtime()
    }

    /// Return the declared persistent representation.
    #[must_use]
    pub const fn schema(&self) -> ProductSchema {
        self.schema.to_runtime()
    }

    /// Return the compiler-owned logical payload projection.
    #[must_use]
    pub const fn payload(&self) -> &ProductPayloadEvidence {
        &self.payload
    }

    /// Return exact upstream source-slot dependencies.
    #[must_use]
    pub fn source_dependencies(&self) -> &[usize] {
        &self.source_dependencies
    }

    /// Return graph-node dependencies, all of which precede this node.
    #[must_use]
    pub fn dependencies(&self) -> &[usize] {
        &self.dependencies
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductSourceRoleProjection {
    FinalNormalState,
    FinalModel,
    WeightingGeneration,
    CleanMaskGeneration,
    PrimaryBeamGeneration,
    PrimaryBeamSpectralIndexGeneration,
    SensitivityGeneration,
    RestoringBeamGeneration,
}

impl ProductSourceRoleProjection {
    const fn new(role: ProductSourceRole) -> Self {
        match role {
            ProductSourceRole::FinalNormalState => Self::FinalNormalState,
            ProductSourceRole::FinalModel => Self::FinalModel,
            ProductSourceRole::WeightingGeneration => Self::WeightingGeneration,
            ProductSourceRole::CleanMaskGeneration => Self::CleanMaskGeneration,
            ProductSourceRole::PrimaryBeamGeneration => Self::PrimaryBeamGeneration,
            ProductSourceRole::PrimaryBeamSpectralIndexGeneration => {
                Self::PrimaryBeamSpectralIndexGeneration
            }
            ProductSourceRole::SensitivityGeneration => Self::SensitivityGeneration,
            ProductSourceRole::RestoringBeamGeneration => Self::RestoringBeamGeneration,
        }
    }

    const fn to_runtime(self) -> ProductSourceRole {
        match self {
            Self::FinalNormalState => ProductSourceRole::FinalNormalState,
            Self::FinalModel => ProductSourceRole::FinalModel,
            Self::WeightingGeneration => ProductSourceRole::WeightingGeneration,
            Self::CleanMaskGeneration => ProductSourceRole::CleanMaskGeneration,
            Self::PrimaryBeamGeneration => ProductSourceRole::PrimaryBeamGeneration,
            Self::PrimaryBeamSpectralIndexGeneration => {
                ProductSourceRole::PrimaryBeamSpectralIndexGeneration
            }
            Self::SensitivityGeneration => ProductSourceRole::SensitivityGeneration,
            Self::RestoringBeamGeneration => ProductSourceRole::RestoringBeamGeneration,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
enum ImageDomainProjection {
    Main,
    Outlier(String),
}

impl ImageDomainProjection {
    fn new(domain: &ImageDomainRole) -> Self {
        match domain {
            ImageDomainRole::Main => Self::Main,
            ImageDomainRole::Outlier(name) => Self::Outlier(stable_text(name)),
        }
    }

    fn to_runtime(&self) -> ImageDomainRole {
        match self {
            Self::Main => ImageDomainRole::Main,
            Self::Outlier(name) => ImageDomainRole::Outlier(name.clone()),
        }
    }
}

/// Exact coordinate and storage-axis facts preserved as receipt evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductAxesEvidence {
    kind: ProductAxisKindProjection,
    geometry_identity: String,
    domain: ImageDomainProjection,
    order: [ImageAxisProjection; 4],
    shape: [usize; 4],
    direction: DirectionCoordinateProjection,
    spectral: SpectralCoordinateProjection,
    polarization: Vec<PolarizationProjection>,
}

impl ProductAxesEvidence {
    fn new(axes: &casa_imaging_model::ProductAxes) -> Self {
        Self {
            kind: ProductAxisKindProjection::new(axes.kind()),
            geometry_identity: hex(&axes.geometry_id().as_bytes()),
            domain: ImageDomainProjection::new(axes.domain()),
            order: axes.order().positions().map(ImageAxisProjection::new),
            shape: axes.shape(),
            direction: DirectionCoordinateProjection::new(axes.direction()),
            spectral: SpectralCoordinateProjection::new(axes.spectral()),
            polarization: axes
                .polarization()
                .iter()
                .copied()
                .map(PolarizationProjection::new)
                .collect(),
        }
    }

    /// Return the logical axis role.
    #[must_use]
    pub const fn kind(&self) -> ProductAxisKind {
        self.kind.to_runtime()
    }

    /// Return the exact compiler-owned geometry identity.
    #[must_use]
    pub fn geometry_identity(&self) -> [u8; 32] {
        parse_digest(&self.geometry_identity)
    }

    /// Return the user-visible image domain.
    #[must_use]
    pub fn domain(&self) -> ImageDomainRole {
        self.domain.to_runtime()
    }

    /// Return axes in CASA storage order.
    #[must_use]
    pub fn order(&self) -> [ImageAxis; 4] {
        self.order.map(ImageAxisProjection::to_runtime)
    }

    /// Return exact extents in storage-axis order.
    #[must_use]
    pub const fn shape(&self) -> [usize; 4] {
        self.shape
    }

    /// Return the exact direction-coordinate law.
    #[must_use]
    pub fn direction(&self) -> DirectionCoordinateSpec {
        self.direction.to_runtime()
    }

    /// Return the exact spectral-coordinate law.
    #[must_use]
    pub fn spectral(&self) -> SpectralCoordinateSpec {
        self.spectral.to_runtime()
    }

    /// Return reconstruction-owned polarization coordinates.
    #[must_use]
    pub fn polarization(&self) -> Vec<PolarizationCoordinate> {
        self.polarization
            .iter()
            .copied()
            .map(PolarizationProjection::to_runtime)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductAxisKindProjection {
    SkyImage,
    PlaneState,
    Metadata,
}

impl ProductAxisKindProjection {
    const fn new(kind: ProductAxisKind) -> Self {
        match kind {
            ProductAxisKind::SkyImage => Self::SkyImage,
            ProductAxisKind::PlaneState => Self::PlaneState,
            ProductAxisKind::Metadata => Self::Metadata,
        }
    }

    const fn to_runtime(self) -> ProductAxisKind {
        match self {
            Self::SkyImage => ProductAxisKind::SkyImage,
            Self::PlaneState => ProductAxisKind::PlaneState,
            Self::Metadata => ProductAxisKind::Metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ImageAxisProjection {
    DirectionLongitude,
    DirectionLatitude,
    Polarization,
    Spectral,
}

impl ImageAxisProjection {
    const fn new(axis: ImageAxis) -> Self {
        match axis {
            ImageAxis::DirectionLongitude => Self::DirectionLongitude,
            ImageAxis::DirectionLatitude => Self::DirectionLatitude,
            ImageAxis::Polarization => Self::Polarization,
            ImageAxis::Spectral => Self::Spectral,
        }
    }

    const fn to_runtime(self) -> ImageAxis {
        match self {
            Self::DirectionLongitude => ImageAxis::DirectionLongitude,
            Self::DirectionLatitude => ImageAxis::DirectionLatitude,
            Self::Polarization => ImageAxis::Polarization,
            Self::Spectral => ImageAxis::Spectral,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DirectionCoordinateProjection {
    projection: ProjectionProjection,
    frame: DirectionFrameProjection,
    longitude_bits: u64,
    latitude_bits: u64,
    reference_pixel_bits: [u64; 2],
    increment_rad_bits: [u64; 2],
    pc_bits: [[u64; 2]; 2],
    pole_deg_bits: [u64; 2],
}

impl DirectionCoordinateProjection {
    fn new(direction: DirectionCoordinateSpec) -> Self {
        let reference = direction.reference_direction();
        Self {
            projection: ProjectionProjection::new(direction.projection()),
            frame: DirectionFrameProjection::new(reference.frame()),
            longitude_bits: reference.longitude_rad().to_bits(),
            latitude_bits: reference.latitude_rad().to_bits(),
            reference_pixel_bits: direction.reference_pixel().map(f64::to_bits),
            increment_rad_bits: direction.increment_rad().map(f64::to_bits),
            pc_bits: direction.pc().map(|row| row.map(f64::to_bits)),
            pole_deg_bits: direction.pole_deg().map(f64::to_bits),
        }
    }

    fn to_runtime(self) -> DirectionCoordinateSpec {
        DirectionCoordinateSpec::new(
            self.projection.to_runtime(),
            SkyDirection::new(
                self.frame.to_runtime(),
                f64::from_bits(self.longitude_bits),
                f64::from_bits(self.latitude_bits),
            ),
            self.reference_pixel_bits.map(f64::from_bits),
            self.increment_rad_bits.map(f64::from_bits),
            self.pc_bits.map(|row| row.map(f64::from_bits)),
            self.pole_deg_bits.map(f64::from_bits),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProjectionProjection {
    Sin,
}

impl ProjectionProjection {
    const fn new(projection: Projection) -> Self {
        match projection {
            Projection::Sin => Self::Sin,
        }
    }

    const fn to_runtime(self) -> Projection {
        match self {
            Self::Sin => Projection::Sin,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DirectionFrameProjection {
    Icrs,
    J2000,
    B1950,
    Galactic,
}

impl DirectionFrameProjection {
    const fn new(frame: DirectionFrame) -> Self {
        match frame {
            DirectionFrame::Icrs => Self::Icrs,
            DirectionFrame::J2000 => Self::J2000,
            DirectionFrame::B1950 => Self::B1950,
            DirectionFrame::Galactic => Self::Galactic,
        }
    }

    const fn to_runtime(self) -> DirectionFrame {
        match self {
            Self::Icrs => DirectionFrame::Icrs,
            Self::J2000 => DirectionFrame::J2000,
            Self::B1950 => DirectionFrame::B1950,
            Self::Galactic => DirectionFrame::Galactic,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SpectralCoordinateProjection {
    source_frame: FrequencyFrameProjection,
    output_frame: FrequencyFrameProjection,
    anchor: SpectralAnchorProjection,
    wcs: SpectralWcsProjection,
    rest_frequency: RestFrequencyProjection,
    doppler: DopplerProjection,
}

impl SpectralCoordinateProjection {
    fn new(spectral: &SpectralCoordinateSpec) -> Self {
        Self {
            source_frame: FrequencyFrameProjection::new(spectral.source_frame()),
            output_frame: FrequencyFrameProjection::new(spectral.output_frame()),
            anchor: SpectralAnchorProjection::new(spectral.anchor()),
            wcs: SpectralWcsProjection::new(spectral.wcs()),
            rest_frequency: RestFrequencyProjection::new(spectral.rest_frequency()),
            doppler: DopplerProjection::new(spectral.doppler_convention()),
        }
    }

    fn to_runtime(&self) -> SpectralCoordinateSpec {
        SpectralCoordinateSpec::new(
            self.source_frame.to_runtime(),
            self.output_frame.to_runtime(),
            self.anchor.to_runtime(),
            self.wcs.to_runtime(),
            self.rest_frequency.to_runtime(),
            self.doppler.to_runtime(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FrequencyFrameProjection {
    Topocentric,
    Barycentric,
    Lsrk,
}

impl FrequencyFrameProjection {
    const fn new(frame: FrequencyFrame) -> Self {
        match frame {
            FrequencyFrame::Topocentric => Self::Topocentric,
            FrequencyFrame::Barycentric => Self::Barycentric,
            FrequencyFrame::Lsrk => Self::Lsrk,
        }
    }

    const fn to_runtime(self) -> FrequencyFrame {
        match self {
            Self::Topocentric => FrequencyFrame::Topocentric,
            Self::Barycentric => FrequencyFrame::Barycentric,
            Self::Lsrk => FrequencyFrame::Lsrk,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SpectralAnchorProjection {
    NotApplicable,
    Conversion {
        epoch_mjd_bits: u64,
        time_scale: TimeScaleProjection,
        direction: DirectionValueProjection,
        observatory_metres_bits: [u64; 3],
    },
}

impl SpectralAnchorProjection {
    fn new(anchor: SpectralFrameAnchor) -> Self {
        match anchor {
            SpectralFrameAnchor::NotApplicable => Self::NotApplicable,
            SpectralFrameAnchor::Conversion {
                epoch,
                direction,
                observatory_position,
            } => Self::Conversion {
                epoch_mjd_bits: epoch.mjd_days().to_bits(),
                time_scale: TimeScaleProjection::new(epoch.scale()),
                direction: DirectionValueProjection::new(direction),
                observatory_metres_bits: observatory_position.metres().map(f64::to_bits),
            },
        }
    }

    fn to_runtime(self) -> SpectralFrameAnchor {
        match self {
            Self::NotApplicable => SpectralFrameAnchor::NotApplicable,
            Self::Conversion {
                epoch_mjd_bits,
                time_scale,
                direction,
                observatory_metres_bits,
            } => {
                let metres = observatory_metres_bits.map(f64::from_bits);
                SpectralFrameAnchor::Conversion {
                    epoch: Epoch::new(f64::from_bits(epoch_mjd_bits), time_scale.to_runtime()),
                    direction: direction.to_runtime(),
                    observatory_position: ItrfPosition::new(metres[0], metres[1], metres[2]),
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DirectionValueProjection {
    frame: DirectionFrameProjection,
    longitude_bits: u64,
    latitude_bits: u64,
}

impl DirectionValueProjection {
    fn new(direction: SkyDirection) -> Self {
        Self {
            frame: DirectionFrameProjection::new(direction.frame()),
            longitude_bits: direction.longitude_rad().to_bits(),
            latitude_bits: direction.latitude_rad().to_bits(),
        }
    }

    fn to_runtime(self) -> SkyDirection {
        SkyDirection::new(
            self.frame.to_runtime(),
            f64::from_bits(self.longitude_bits),
            f64::from_bits(self.latitude_bits),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TimeScaleProjection {
    Utc,
    Tai,
    Tt,
    Tdb,
}

impl TimeScaleProjection {
    const fn new(scale: TimeScale) -> Self {
        match scale {
            TimeScale::Utc => Self::Utc,
            TimeScale::Tai => Self::Tai,
            TimeScale::Tt => Self::Tt,
            TimeScale::Tdb => Self::Tdb,
        }
    }

    const fn to_runtime(self) -> TimeScale {
        match self {
            Self::Utc => TimeScale::Utc,
            Self::Tai => TimeScale::Tai,
            Self::Tt => TimeScale::Tt,
            Self::Tdb => TimeScale::Tdb,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SpectralWcsProjection {
    Linear {
        channels: usize,
        reference_pixel_bits: u64,
        reference_frequency_hz_bits: u64,
        increment_hz_bits: u64,
    },
    Tabular {
        channel_centres_hz_bits: Vec<u64>,
        channel_boundaries_hz_bits: Vec<u64>,
    },
}

impl SpectralWcsProjection {
    fn new(wcs: &SpectralWcs) -> Self {
        match wcs {
            SpectralWcs::Linear {
                channels,
                reference_pixel,
                reference_frequency_hz,
                increment_hz,
            } => Self::Linear {
                channels: *channels,
                reference_pixel_bits: reference_pixel.to_bits(),
                reference_frequency_hz_bits: reference_frequency_hz.to_bits(),
                increment_hz_bits: increment_hz.to_bits(),
            },
            SpectralWcs::Tabular {
                channel_centres_hz,
                channel_boundaries_hz,
            } => Self::Tabular {
                channel_centres_hz_bits: channel_centres_hz
                    .iter()
                    .map(|value| value.to_bits())
                    .collect(),
                channel_boundaries_hz_bits: channel_boundaries_hz
                    .iter()
                    .map(|value| value.to_bits())
                    .collect(),
            },
        }
    }

    fn to_runtime(&self) -> SpectralWcs {
        match self {
            Self::Linear {
                channels,
                reference_pixel_bits,
                reference_frequency_hz_bits,
                increment_hz_bits,
            } => SpectralWcs::Linear {
                channels: *channels,
                reference_pixel: f64::from_bits(*reference_pixel_bits),
                reference_frequency_hz: f64::from_bits(*reference_frequency_hz_bits),
                increment_hz: f64::from_bits(*increment_hz_bits),
            },
            Self::Tabular {
                channel_centres_hz_bits,
                channel_boundaries_hz_bits,
            } => SpectralWcs::Tabular {
                channel_centres_hz: channel_centres_hz_bits
                    .iter()
                    .copied()
                    .map(f64::from_bits)
                    .collect(),
                channel_boundaries_hz: channel_boundaries_hz_bits
                    .iter()
                    .copied()
                    .map(f64::from_bits)
                    .collect(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "hertz_bits", rename_all = "snake_case")]
enum RestFrequencyProjection {
    NotApplicable,
    Line(u64),
}

impl RestFrequencyProjection {
    fn new(frequency: RestFrequency) -> Self {
        match frequency {
            RestFrequency::NotApplicable => Self::NotApplicable,
            RestFrequency::Line { hertz } => Self::Line(hertz.to_bits()),
        }
    }

    fn to_runtime(self) -> RestFrequency {
        match self {
            Self::NotApplicable => RestFrequency::NotApplicable,
            Self::Line(bits) => RestFrequency::Line {
                hertz: f64::from_bits(bits),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DopplerProjection {
    NotApplicable,
    Radio,
    Optical,
    Relativistic,
}

impl DopplerProjection {
    const fn new(value: DopplerConvention) -> Self {
        match value {
            DopplerConvention::NotApplicable => Self::NotApplicable,
            DopplerConvention::Radio => Self::Radio,
            DopplerConvention::Optical => Self::Optical,
            DopplerConvention::Relativistic => Self::Relativistic,
        }
    }

    const fn to_runtime(self) -> DopplerConvention {
        match self {
            Self::NotApplicable => DopplerConvention::NotApplicable,
            Self::Radio => DopplerConvention::Radio,
            Self::Optical => DopplerConvention::Optical,
            Self::Relativistic => DopplerConvention::Relativistic,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PolarizationProjection {
    StokesI,
    StokesQ,
    StokesU,
    StokesV,
    LinearXx,
    LinearXy,
    LinearYx,
    LinearYy,
    CircularRr,
    CircularRl,
    CircularLr,
    CircularLl,
}

impl PolarizationProjection {
    const fn new(value: PolarizationCoordinate) -> Self {
        match value {
            PolarizationCoordinate::StokesI => Self::StokesI,
            PolarizationCoordinate::StokesQ => Self::StokesQ,
            PolarizationCoordinate::StokesU => Self::StokesU,
            PolarizationCoordinate::StokesV => Self::StokesV,
            PolarizationCoordinate::LinearXx => Self::LinearXx,
            PolarizationCoordinate::LinearXy => Self::LinearXy,
            PolarizationCoordinate::LinearYx => Self::LinearYx,
            PolarizationCoordinate::LinearYy => Self::LinearYy,
            PolarizationCoordinate::CircularRr => Self::CircularRr,
            PolarizationCoordinate::CircularRl => Self::CircularRl,
            PolarizationCoordinate::CircularLr => Self::CircularLr,
            PolarizationCoordinate::CircularLl => Self::CircularLl,
        }
    }

    const fn to_runtime(self) -> PolarizationCoordinate {
        match self {
            Self::StokesI => PolarizationCoordinate::StokesI,
            Self::StokesQ => PolarizationCoordinate::StokesQ,
            Self::StokesU => PolarizationCoordinate::StokesU,
            Self::StokesV => PolarizationCoordinate::StokesV,
            Self::LinearXx => PolarizationCoordinate::LinearXx,
            Self::LinearXy => PolarizationCoordinate::LinearXy,
            Self::LinearYx => PolarizationCoordinate::LinearYx,
            Self::LinearYy => PolarizationCoordinate::LinearYy,
            Self::CircularRr => PolarizationCoordinate::CircularRr,
            Self::CircularRl => PolarizationCoordinate::CircularRl,
            Self::CircularLr => PolarizationCoordinate::CircularLr,
            Self::CircularLl => PolarizationCoordinate::CircularLl,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductUnitProjection {
    NotApplicable,
    JyPerBeam,
    JyPerPixel,
    Dimensionless,
    VisibilityWeight,
}

impl ProductUnitProjection {
    const fn new(unit: ProductUnit) -> Self {
        match unit {
            ProductUnit::NotApplicable => Self::NotApplicable,
            ProductUnit::JyPerBeam => Self::JyPerBeam,
            ProductUnit::JyPerPixel => Self::JyPerPixel,
            ProductUnit::Dimensionless => Self::Dimensionless,
            ProductUnit::VisibilityWeight => Self::VisibilityWeight,
        }
    }

    const fn to_runtime(self) -> ProductUnit {
        match self {
            Self::NotApplicable => ProductUnit::NotApplicable,
            Self::JyPerBeam => ProductUnit::JyPerBeam,
            Self::JyPerPixel => ProductUnit::JyPerPixel,
            Self::Dimensionless => ProductUnit::Dimensionless,
            Self::VisibilityWeight => ProductUnit::VisibilityWeight,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductNormalizationProjection {
    UnitResponse,
    FlatNoise,
    FlatSky,
}

impl ProductNormalizationProjection {
    const fn new(normalization: ProductNormalization) -> Self {
        match normalization {
            ProductNormalization::UnitResponse => Self::UnitResponse,
            ProductNormalization::FlatNoise => Self::FlatNoise,
            ProductNormalization::FlatSky => Self::FlatSky,
        }
    }

    const fn to_runtime(self) -> ProductNormalization {
        match self {
            Self::UnitResponse => ProductNormalization::UnitResponse,
            Self::FlatNoise => ProductNormalization::FlatNoise,
            Self::FlatSky => ProductNormalization::FlatSky,
        }
    }
}

/// Beam semantics preserved without constructing graph authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ProductBeamEvidence {
    /// No beam metadata applies.
    None,
    /// Attach the fitted PSF beam set.
    Fitted,
    /// Restore using the exact policy.
    Restoring(RestoringBeamEvidence),
    /// Inherit from one graph-local node ordinal.
    Inherit(usize),
    /// Persist fitted and selected beam metadata.
    Metadata(RestoringBeamEvidence),
}

impl ProductBeamEvidence {
    const fn new(beam: ProductBeamRule) -> Self {
        match beam {
            ProductBeamRule::None => Self::None,
            ProductBeamRule::Fitted => Self::Fitted,
            ProductBeamRule::Restoring(policy) => {
                Self::Restoring(RestoringBeamEvidence::new(policy))
            }
            ProductBeamRule::Inherit(node) => Self::Inherit(node.ordinal()),
            ProductBeamRule::Metadata(policy) => Self::Metadata(RestoringBeamEvidence::new(policy)),
        }
    }
}

impl PartialEq<ProductBeamRule> for ProductBeamEvidence {
    fn eq(&self, other: &ProductBeamRule) -> bool {
        match (self, other) {
            (Self::None, ProductBeamRule::None) | (Self::Fitted, ProductBeamRule::Fitted) => true,
            (Self::Restoring(left), ProductBeamRule::Restoring(right))
            | (Self::Metadata(left), ProductBeamRule::Metadata(right)) => {
                left.to_runtime() == *right
            }
            (Self::Inherit(left), ProductBeamRule::Inherit(right)) => *left == right.ordinal(),
            _ => false,
        }
    }
}

/// Receipt-local restoring-beam policy projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoringBeamEvidence {
    /// No restoring beam applies.
    None,
    /// Use one beam per plane.
    PerPlane,
    /// Use one common enclosing beam.
    Common,
}

impl RestoringBeamEvidence {
    const fn new(policy: RestoringBeamPolicy) -> Self {
        match policy {
            RestoringBeamPolicy::None => Self::None,
            RestoringBeamPolicy::PerPlane => Self::PerPlane,
            RestoringBeamPolicy::Common => Self::Common,
        }
    }

    const fn to_runtime(self) -> RestoringBeamPolicy {
        match self {
            Self::None => RestoringBeamPolicy::None,
            Self::PerPlane => RestoringBeamPolicy::PerPlane,
            Self::Common => RestoringBeamPolicy::Common,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProductValidityProjection {
    All,
    FinalNormalState,
    PrimaryBeam {
        policy: PrimaryBeamPolicyProjection,
    },
    Taylor {
        policy: TaylorPolicyProjection,
    },
    TaylorAndPrimaryBeam {
        taylor: TaylorPolicyProjection,
        primary_beam: PrimaryBeamPolicyProjection,
    },
}

impl ProductValidityProjection {
    fn new(validity: ProductValidityRule) -> Self {
        match validity {
            ProductValidityRule::All => Self::All,
            ProductValidityRule::FinalNormalState => Self::FinalNormalState,
            ProductValidityRule::PrimaryBeam(policy) => Self::PrimaryBeam {
                policy: PrimaryBeamPolicyProjection::new(policy),
            },
            ProductValidityRule::Taylor(policy) => Self::Taylor {
                policy: TaylorPolicyProjection::new(policy),
            },
            ProductValidityRule::TaylorAndPrimaryBeam {
                taylor,
                primary_beam,
            } => Self::TaylorAndPrimaryBeam {
                taylor: TaylorPolicyProjection::new(taylor),
                primary_beam: PrimaryBeamPolicyProjection::new(primary_beam),
            },
        }
    }

    fn to_runtime(self) -> ProductValidityRule {
        match self {
            Self::All => ProductValidityRule::All,
            Self::FinalNormalState => ProductValidityRule::FinalNormalState,
            Self::PrimaryBeam { policy } => ProductValidityRule::PrimaryBeam(policy.to_runtime()),
            Self::Taylor { policy } => ProductValidityRule::Taylor(policy.to_runtime()),
            Self::TaylorAndPrimaryBeam {
                taylor,
                primary_beam,
            } => ProductValidityRule::TaylorAndPrimaryBeam {
                taylor: taylor.to_runtime(),
                primary_beam: primary_beam.to_runtime(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PrimaryBeamPolicyProjection {
    cutoff_bits: u32,
    comparison: SupportComparisonProjection,
    blanking: BlankingProjection,
}

impl PrimaryBeamPolicyProjection {
    fn new(policy: PrimaryBeamValidityPolicy) -> Self {
        Self {
            cutoff_bits: policy.cutoff().to_bits(),
            comparison: SupportComparisonProjection::new(policy.comparison()),
            blanking: BlankingProjection::new(policy.blanking()),
        }
    }

    fn to_runtime(self) -> PrimaryBeamValidityPolicy {
        PrimaryBeamValidityPolicy::new(
            f32::from_bits(self.cutoff_bits),
            self.comparison.to_runtime(),
            self.blanking.to_runtime(),
        )
        .expect("validated receipt primary-beam policy")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TaylorPolicyProjection {
    reference: TaylorReferenceProjection,
    peak_fraction_bits: u32,
    comparison: SupportComparisonProjection,
    blanking: BlankingProjection,
}

impl TaylorPolicyProjection {
    fn new(policy: TaylorValidityPolicy) -> Self {
        Self {
            reference: TaylorReferenceProjection::new(policy.reference()),
            peak_fraction_bits: policy.peak_fraction().to_bits(),
            comparison: SupportComparisonProjection::new(policy.comparison()),
            blanking: BlankingProjection::new(policy.blanking()),
        }
    }

    fn to_runtime(self) -> TaylorValidityPolicy {
        TaylorValidityPolicy::new(
            self.reference.to_runtime(),
            f32::from_bits(self.peak_fraction_bits),
            self.comparison.to_runtime(),
            self.blanking.to_runtime(),
        )
        .expect("validated receipt Taylor policy")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TaylorReferenceProjection {
    PrincipalResidualTaylor0PositiveMaximum,
}

impl TaylorReferenceProjection {
    const fn new(reference: TaylorSupportReference) -> Self {
        match reference {
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum => {
                Self::PrincipalResidualTaylor0PositiveMaximum
            }
        }
    }

    const fn to_runtime(self) -> TaylorSupportReference {
        match self {
            Self::PrincipalResidualTaylor0PositiveMaximum => {
                TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SupportComparisonProjection {
    StrictlyGreater,
}

impl SupportComparisonProjection {
    const fn new(comparison: ProductSupportComparison) -> Self {
        match comparison {
            ProductSupportComparison::StrictlyGreater => Self::StrictlyGreater,
        }
    }

    const fn to_runtime(self) -> ProductSupportComparison {
        match self {
            Self::StrictlyGreater => ProductSupportComparison::StrictlyGreater,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BlankingProjection {
    ZeroAndFalseMask,
}

impl BlankingProjection {
    const fn new(blanking: ProductBlankingPolicy) -> Self {
        match blanking {
            ProductBlankingPolicy::ZeroAndFalseMask => Self::ZeroAndFalseMask,
        }
    }

    const fn to_runtime(self) -> ProductBlankingPolicy {
        match self {
            Self::ZeroAndFalseMask => ProductBlankingPolicy::ZeroAndFalseMask,
        }
    }
}

/// Logical payload and identity envelope preserved as receipt evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductPayloadEvidence {
    element_representation: ProductElementProjection,
    logical_elements: u64,
    logical_pixel_bytes: u64,
    identity_metadata_bytes: u64,
    identity_envelope_bytes: u64,
}

impl ProductPayloadEvidence {
    fn new(payload: casa_imaging_model::ProductPayloadEnvelope) -> Self {
        Self {
            element_representation: ProductElementProjection::new(payload.element_representation()),
            logical_elements: payload.logical_elements(),
            logical_pixel_bytes: payload.logical_pixel_bytes(),
            identity_metadata_bytes: payload.identity_metadata_bytes(),
            identity_envelope_bytes: payload.identity_envelope_bytes(),
        }
    }

    /// Return the exact logical element representation.
    #[must_use]
    pub const fn element_representation(&self) -> ProductElementRepresentation {
        self.element_representation.to_runtime()
    }

    /// Return logical element count.
    #[must_use]
    pub const fn logical_elements(&self) -> u64 {
        self.logical_elements
    }

    /// Return logical pixel bytes.
    #[must_use]
    pub const fn logical_pixel_bytes(&self) -> u64 {
        self.logical_pixel_bytes
    }

    /// Return canonical identity-metadata bytes.
    #[must_use]
    pub const fn identity_metadata_bytes(&self) -> u64 {
        self.identity_metadata_bytes
    }

    /// Return the complete logical identity envelope bytes.
    #[must_use]
    pub const fn identity_envelope_bytes(&self) -> u64 {
        self.identity_envelope_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductElementProjection {
    NotApplicable,
    Float32,
}

impl ProductElementProjection {
    const fn new(element: ProductElementRepresentation) -> Self {
        match element {
            ProductElementRepresentation::NotApplicable => Self::NotApplicable,
            ProductElementRepresentation::Float32 => Self::Float32,
        }
    }

    const fn to_runtime(self) -> ProductElementRepresentation {
        match self {
            Self::NotApplicable => ProductElementRepresentation::NotApplicable,
            Self::Float32 => ProductElementRepresentation::Float32,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "term", rename_all = "snake_case")]
enum ProductRoleProjection {
    Psf(ProductTermProjection),
    Residual(ProductTermProjection),
    Model(ProductTermProjection),
    RestoredImage(ProductTermProjection),
    SumWeights(ProductTermProjection),
    CleanMask,
    Weight(ProductTermProjection),
    PrimaryBeam(ProductTermProjection),
    PrimaryBeamSpectralIndex,
    Sensitivity,
    PbCorrectedImage(ProductTermProjection),
    TaylorCoefficientSet,
    SpectralIndex,
    SpectralIndexError,
    PbCorrectedSpectralIndex,
    BeamMetadata,
}

impl ProductRoleProjection {
    const fn new(role: ProductRole) -> Self {
        match role {
            ProductRole::Psf(term) => Self::Psf(ProductTermProjection::new(term)),
            ProductRole::Residual(term) => Self::Residual(ProductTermProjection::new(term)),
            ProductRole::Model(term) => Self::Model(ProductTermProjection::new(term)),
            ProductRole::RestoredImage(term) => {
                Self::RestoredImage(ProductTermProjection::new(term))
            }
            ProductRole::SumWeights(term) => Self::SumWeights(ProductTermProjection::new(term)),
            ProductRole::CleanMask => Self::CleanMask,
            ProductRole::Weight(term) => Self::Weight(ProductTermProjection::new(term)),
            ProductRole::PrimaryBeam(term) => Self::PrimaryBeam(ProductTermProjection::new(term)),
            ProductRole::PrimaryBeamSpectralIndex => Self::PrimaryBeamSpectralIndex,
            ProductRole::Sensitivity => Self::Sensitivity,
            ProductRole::PbCorrectedImage(term) => {
                Self::PbCorrectedImage(ProductTermProjection::new(term))
            }
            ProductRole::TaylorCoefficientSet => Self::TaylorCoefficientSet,
            ProductRole::SpectralIndex => Self::SpectralIndex,
            ProductRole::SpectralIndexError => Self::SpectralIndexError,
            ProductRole::PbCorrectedSpectralIndex => Self::PbCorrectedSpectralIndex,
            ProductRole::BeamMetadata => Self::BeamMetadata,
        }
    }

    const fn to_runtime(self) -> ProductRole {
        match self {
            Self::Psf(term) => ProductRole::Psf(term.to_runtime()),
            Self::Residual(term) => ProductRole::Residual(term.to_runtime()),
            Self::Model(term) => ProductRole::Model(term.to_runtime()),
            Self::RestoredImage(term) => ProductRole::RestoredImage(term.to_runtime()),
            Self::SumWeights(term) => ProductRole::SumWeights(term.to_runtime()),
            Self::CleanMask => ProductRole::CleanMask,
            Self::Weight(term) => ProductRole::Weight(term.to_runtime()),
            Self::PrimaryBeam(term) => ProductRole::PrimaryBeam(term.to_runtime()),
            Self::PrimaryBeamSpectralIndex => ProductRole::PrimaryBeamSpectralIndex,
            Self::Sensitivity => ProductRole::Sensitivity,
            Self::PbCorrectedImage(term) => ProductRole::PbCorrectedImage(term.to_runtime()),
            Self::TaylorCoefficientSet => ProductRole::TaylorCoefficientSet,
            Self::SpectralIndex => ProductRole::SpectralIndex,
            Self::SpectralIndexError => ProductRole::SpectralIndexError,
            Self::PbCorrectedSpectralIndex => ProductRole::PbCorrectedSpectralIndex,
            Self::BeamMetadata => ProductRole::BeamMetadata,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "ordinal", rename_all = "snake_case")]
enum ProductTermProjection {
    Single,
    Taylor(usize),
}

impl ProductTermProjection {
    const fn new(term: ProductTerm) -> Self {
        match term {
            ProductTerm::Single => Self::Single,
            ProductTerm::Taylor(ordinal) => Self::Taylor(ordinal),
        }
    }

    const fn to_runtime(self) -> ProductTerm {
        match self {
            Self::Single => ProductTerm::Single,
            Self::Taylor(ordinal) => ProductTerm::Taylor(ordinal),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductSchemaProjection {
    CasaPagedImageF32,
    LogicalCollection,
    CasaImageMetadata,
}

impl ProductSchemaProjection {
    const fn new(schema: ProductSchema) -> Self {
        match schema {
            ProductSchema::CasaPagedImageF32 => Self::CasaPagedImageF32,
            ProductSchema::LogicalCollection => Self::LogicalCollection,
            ProductSchema::CasaImageMetadata => Self::CasaImageMetadata,
        }
    }

    const fn to_runtime(self) -> ProductSchema {
        match self {
            Self::CasaPagedImageF32 => ProductSchema::CasaPagedImageF32,
            Self::LogicalCollection => ProductSchema::LogicalCollection,
            Self::CasaImageMetadata => ProductSchema::CasaImageMetadata,
        }
    }
}

/// Immutable receipt projection of one product generation and its publication seal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductExecutionEvidence {
    generation_identity: String,
    graph_identity: String,
    source_bindings: Vec<ProductSourceBindingEvidence>,
    publication_node: String,
    members: Vec<ProductPublicationMemberEvidence>,
}

impl ProductExecutionEvidence {
    fn new(plan: &ExecutionPlan) -> Self {
        let generation = plan.product_generation();
        Self {
            generation_identity: hex(&generation.generation_id().as_bytes()),
            graph_identity: hex(&generation.graph_id().as_bytes()),
            source_bindings: generation
                .source_bindings()
                .iter()
                .map(ProductSourceBindingEvidence::new)
                .collect(),
            publication_node: stable_text(plan.product_publication().publication_node().as_str()),
            members: plan
                .product_publication()
                .artifacts()
                .iter()
                .map(|artifact| {
                    let layout = plan
                        .publication_layouts()
                        .entries()
                        .iter()
                        .find(|layout| layout.artifact() == artifact.artifact())
                        .expect("sealed product publication has one exact physical layout");
                    ProductPublicationMemberEvidence::new(layout, plan.execution_dag())
                })
                .collect(),
        }
    }

    /// Return the exact product generation sealed into the plan.
    #[must_use]
    pub fn generation_id(&self) -> ProductGenerationId {
        ProductGenerationId::from_sha256(parse_digest(&self.generation_identity))
    }

    /// Return the compiler-owned graph that created the generation.
    #[must_use]
    pub fn graph_id(&self) -> [u8; 32] {
        parse_digest(&self.graph_identity)
    }

    /// Return every exact upstream source-generation binding in canonical order.
    #[must_use]
    pub fn source_bindings(&self) -> &[ProductSourceBindingEvidence] {
        &self.source_bindings
    }

    /// Return the sole plan node that owns publication.
    #[must_use]
    pub fn publication_node(&self) -> WorkNodeId {
        WorkNodeId::new(self.publication_node.clone())
    }

    /// Return every exact physical publication member in canonical order.
    #[must_use]
    pub fn members(&self) -> &[ProductPublicationMemberEvidence] {
        &self.members
    }
}

/// One canonical upstream source-generation binding in receipt evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductSourceBindingEvidence {
    source_ordinal: usize,
    generation_identity: String,
}

impl ProductSourceBindingEvidence {
    fn new(binding: &casa_imaging_model::ProductSourceBinding) -> Self {
        Self {
            source_ordinal: binding.source_id().ordinal(),
            generation_identity: hex(&binding.generation_id().as_bytes()),
        }
    }

    /// Return the graph-local source slot ordinal.
    #[must_use]
    pub const fn source_ordinal(&self) -> usize {
        self.source_ordinal
    }

    /// Return the exact upstream generation identity.
    #[must_use]
    pub fn generation_id(&self) -> ProductSourceGenerationId {
        ProductSourceGenerationId::from_sha256(parse_digest(&self.generation_identity))
    }
}

/// Receipt-local semantic owner of one publication member.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
pub enum ProductParticipantEvidence {
    /// One graph-local physical product node ordinal.
    Product(usize),
    /// One MeasurementSet identity participating through MODEL_DATA.
    ModelData(String),
}

impl ProductParticipantEvidence {
    fn new(participant: PublicationParticipant) -> Self {
        match participant {
            PublicationParticipant::Product(node) => Self::Product(node.ordinal()),
            PublicationParticipant::ModelData(measurement_set) => {
                Self::ModelData(hex(&measurement_set.identity().as_bytes()))
            }
        }
    }
}

/// One canonical publication member, planned artifact, and physical layout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductPublicationMemberEvidence {
    participant: ProductParticipantEvidence,
    planned_artifact: String,
    layout: PublicationPhysicalLayoutEvidence,
}

impl ProductPublicationMemberEvidence {
    fn new(layout: &crate::PublicationPhysicalLayout, execution_dag: &crate::ExecutionDag) -> Self {
        Self {
            participant: ProductParticipantEvidence::new(layout.participant()),
            planned_artifact: hex(&layout.artifact().as_bytes()),
            layout: PublicationPhysicalLayoutEvidence::new(layout, execution_dag),
        }
    }

    /// Return the receipt-local semantic participant.
    #[must_use]
    pub const fn participant(&self) -> &ProductParticipantEvidence {
        &self.participant
    }

    /// Return the exact planned output artifact.
    #[must_use]
    pub fn planned_artifact(&self) -> ArtifactIdentity {
        ArtifactIdentity::from_sha256(parse_digest(&self.planned_artifact))
    }

    /// Return the adapter-derived physical layout projection.
    #[must_use]
    pub const fn layout(&self) -> &PublicationPhysicalLayoutEvidence {
        &self.layout
    }
}

/// Adapter-derived physical layout preserved only as immutable receipt evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationPhysicalLayoutEvidence {
    layout_identity: String,
    staging: PublicationStagingEvidence,
    staged_storage_bytes: u64,
    final_storage_bytes: u64,
    writer_buffer_bytes: u64,
    mapped_page_cache_bytes: u64,
}

impl PublicationPhysicalLayoutEvidence {
    fn new(layout: &crate::PublicationPhysicalLayout, execution_dag: &crate::ExecutionDag) -> Self {
        Self {
            layout_identity: hex(&layout.layout_id().as_bytes()),
            staging: PublicationStagingEvidence::new(layout.staging(), execution_dag),
            staged_storage_bytes: layout.staged_storage_bytes(),
            final_storage_bytes: layout.final_storage_bytes(),
            writer_buffer_bytes: layout.writer_buffer_bytes(),
            mapped_page_cache_bytes: layout.mapped_page_cache_bytes(),
        }
    }

    /// Return the selected adapter writer/layout identity.
    #[must_use]
    pub fn layout_id(&self) -> PhysicalLayoutId {
        PhysicalLayoutId::from_sha256(parse_digest(&self.layout_identity))
    }

    /// Return the exact producer-owned staging projection.
    #[must_use]
    pub const fn staging(&self) -> &PublicationStagingEvidence {
        &self.staging
    }

    /// Return private staged-storage bytes.
    #[must_use]
    pub const fn staged_storage_bytes(&self) -> u64 {
        self.staged_storage_bytes
    }

    /// Return activated final-storage bytes.
    #[must_use]
    pub const fn final_storage_bytes(&self) -> u64 {
        self.final_storage_bytes
    }

    /// Return the maximum writer-owned buffer bytes.
    #[must_use]
    pub const fn writer_buffer_bytes(&self) -> u64 {
        self.writer_buffer_bytes
    }

    /// Return mapped/page-cache exposure bytes.
    #[must_use]
    pub const fn mapped_page_cache_bytes(&self) -> u64 {
        self.mapped_page_cache_bytes
    }
}

/// Producer-owned staging facts preserved as immutable receipt evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationStagingEvidence {
    producer: String,
    terminal: String,
    writer_buffer_kind: String,
    writer_allocation: String,
    writer_physical_slot: String,
    writer_lease_resource: String,
    mapped_page_cache: Option<PublicationMappedStagingEvidence>,
}

impl PublicationStagingEvidence {
    fn new(staging: &crate::PublicationStaging, execution_dag: &crate::ExecutionDag) -> Self {
        let (writer_physical_slot, writer_lease_resource) =
            publication_allocation_projection(execution_dag, staging.writer_allocation());
        Self {
            producer: stable_text(staging.producer().as_str()),
            terminal: dependency(staging.terminal()),
            writer_buffer_kind: io_buffer(staging.writer_buffer_kind()).to_string(),
            writer_allocation: stable_text(staging.writer_allocation().as_str()),
            writer_physical_slot,
            writer_lease_resource,
            mapped_page_cache: staging
                .mapped_page_cache()
                .map(|mapped| PublicationMappedStagingEvidence::new(mapped, execution_dag)),
        }
    }

    /// Return the exact staging producer.
    #[must_use]
    pub fn producer(&self) -> WorkNodeId {
        WorkNodeId::new(self.producer.clone())
    }

    /// Return the producer terminal event required before publication.
    #[must_use]
    pub fn terminal(&self) -> WorkDependency {
        parse_dependency(&self.terminal)
    }

    /// Return the adapter-selected writer buffer category.
    #[must_use]
    pub fn writer_buffer_kind(&self) -> IoBufferKind {
        parse_io_buffer(&self.writer_buffer_kind)
    }

    /// Return the producer-owned writer allocation.
    #[must_use]
    pub fn writer_allocation(&self) -> AllocationId {
        AllocationId::new(self.writer_allocation.clone())
    }

    /// Return the exact physical slot backing the writer allocation.
    #[must_use]
    pub fn writer_physical_slot(&self) -> PhysicalSlotId {
        PhysicalSlotId::new(self.writer_physical_slot.clone())
    }

    /// Return the resource leased by the writer allocation's physical slot.
    #[must_use]
    pub fn writer_lease_resource(&self) -> LeaseResource {
        parse_lease_resource(&self.writer_lease_resource)
    }

    /// Return mapped/page-cache staging, when the adapter requires it.
    #[must_use]
    pub const fn mapped_page_cache(&self) -> Option<&PublicationMappedStagingEvidence> {
        self.mapped_page_cache.as_ref()
    }
}

/// Mapped/page-cache staging facts preserved as immutable receipt evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationMappedStagingEvidence {
    producer: String,
    terminal: String,
    allocation: String,
    physical_slot: String,
    lease_resource: String,
}

impl PublicationMappedStagingEvidence {
    fn new(staging: &crate::PublicationMappedStaging, execution_dag: &crate::ExecutionDag) -> Self {
        let (physical_slot, lease_resource) =
            publication_allocation_projection(execution_dag, staging.allocation());
        Self {
            producer: stable_text(staging.producer().as_str()),
            terminal: dependency(staging.terminal()),
            allocation: stable_text(staging.allocation().as_str()),
            physical_slot,
            lease_resource,
        }
    }

    /// Return the mapped-exposure producer.
    #[must_use]
    pub fn producer(&self) -> WorkNodeId {
        WorkNodeId::new(self.producer.clone())
    }

    /// Return the distinct mapped release event.
    #[must_use]
    pub fn terminal(&self) -> WorkDependency {
        parse_dependency(&self.terminal)
    }

    /// Return the mapped/page-cache allocation.
    #[must_use]
    pub fn allocation(&self) -> AllocationId {
        AllocationId::new(self.allocation.clone())
    }

    /// Return the exact physical slot backing mapped/page-cache exposure.
    #[must_use]
    pub fn physical_slot(&self) -> PhysicalSlotId {
        PhysicalSlotId::new(self.physical_slot.clone())
    }

    /// Return the resource leased by the mapped allocation's physical slot.
    #[must_use]
    pub fn lease_resource(&self) -> LeaseResource {
        parse_lease_resource(&self.lease_resource)
    }
}

fn publication_allocation_projection(
    execution_dag: &crate::ExecutionDag,
    allocation_id: &AllocationId,
) -> (String, String) {
    let allocation = execution_dag
        .logical_allocations()
        .get(allocation_id)
        .expect("sealed publication staging owns one exact logical allocation");
    let slot = execution_dag
        .physical_slots()
        .get(&allocation.physical_slot)
        .expect("sealed publication staging allocation owns one exact physical slot");
    (
        stable_text(slot.id.as_str()),
        lease_resource(&slot.lease_resource),
    )
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

    /// Return the exact product generation, publication binding, and physical layouts.
    #[must_use]
    pub const fn product_execution_evidence(&self) -> &ProductExecutionEvidence {
        &self.body.plan.product_execution
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

    /// Return the actual observed content identity for one artifact.
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
}

/// Local, bounded owner of atomic imaging Execution Receipts.
#[derive(Debug)]
pub struct ExecutionReceiptStore {
    root: PathBuf,
    retention: ReceiptRetention,
    mutation: Mutex<()>,
}

/// One attempt-scoped durable-evidence binding consumed by the run seam.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionReceiptBinding<'store> {
    store: &'store ExecutionReceiptStore,
    provenance: ExecutionProvenance,
}

impl<'store> ExecutionReceiptBinding<'store> {
    pub(crate) fn begin(
        self,
        problem: &CompiledProblem,
        plan: &ExecutionPlan,
    ) -> Result<ReceiptRecorder<'store>, ReceiptError> {
        self.store.begin(self.provenance, problem, plan)
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
        Ok(Self {
            root,
            retention,
            mutation: Mutex::new(()),
        })
    }

    /// Bind one caller-owned attempt and build identity to this local store.
    #[must_use]
    pub const fn bind(&self, provenance: ExecutionProvenance) -> ExecutionReceiptBinding<'_> {
        ExecutionReceiptBinding {
            store: self,
            provenance,
        }
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

    fn persist(&self, body: &ReceiptBody, is_new: bool) -> Result<(), ReceiptError> {
        let _mutation = self
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

    fn make_room(&self, body: &ReceiptBody, incoming_bytes: u64) -> Result<(), ReceiptError> {
        if incoming_bytes > self.retention.max_bytes {
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
            if count <= self.retention.max_receipts && bytes <= self.retention.max_bytes {
                break;
            }
            if !terminal {
                continue;
            }
            prune.push(path);
            count -= 1;
            bytes = bytes.saturating_sub(file_bytes);
        }
        if count > self.retention.max_receipts || bytes > self.retention.max_bytes {
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
        Self {
            attempt_identity: provenance.attempt.to_string(),
            build_identity: provenance.build.to_string(),
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
        required: u64,
        available: u64,
    },
}

impl InfeasibilityProjection {
    fn to_runtime(&self) -> ReceiptInfeasibilityCertificate {
        match self {
            Self::NoCapableAlternative => ReceiptInfeasibilityCertificate::NoCapableAlternative,
            Self::Infeasible {
                resource,
                required,
                available,
            } => ReceiptInfeasibilityCertificate::Infeasible {
                resource: resource.clone(),
                required: *required,
                available: *available,
            },
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
            crate::ResourceError::Infeasible {
                resource,
                required,
                available,
            } => InfeasibilityProjection::Infeasible {
                resource: bounded_evidence_text(resource),
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
    geometry_identity: String,
    observation_identity: String,
    reference_identities: Vec<ReferenceIdentityProjection>,
    model_identity: ModelIdentityProjection,
    numerics_identity: String,
    effective: CompiledProblemEvidence,
}

impl ProblemProjection {
    fn new(problem: &CompiledProblem) -> Self {
        let inputs = problem.inputs();
        Self {
            problem_identity: hex(&problem.problem_id().as_bytes()),
            geometry_identity: hex(&problem.geometry().geometry_id().as_bytes()),
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
            effective: CompiledProblemEvidence::project(problem),
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PlanProjection {
    plan_identity: String,
    product_execution: ProductExecutionEvidence,
    dag_identity: String,
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
            product_execution: ProductExecutionEvidence::new(plan),
            dag_identity: hex(&plan.physical_work_id().as_bytes()),
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
    Published,
}

impl From<ArtifactDisposition> for ArtifactDispositionProjection {
    fn from(value: ArtifactDisposition) -> Self {
        match value {
            ArtifactDisposition::Built => Self::Built,
            ArtifactDisposition::Loaded => Self::Loaded,
            ArtifactDisposition::Reused => Self::Reused,
            ArtifactDisposition::RejectedStale => Self::RejectedStale,
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

impl ReceiptRecorder<'_> {
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

    pub(crate) fn work_launched(
        &mut self,
        node: &WorkNodeId,
        measurements: &WorkMeasurements,
    ) -> Result<(), ReceiptError> {
        self.record_measurements(node, measurements)?;
        self.checkpoint()
    }

    pub(crate) fn work_completed(&mut self, node: &WorkNodeId) -> Result<(), ReceiptError> {
        self.finish_node(node, ReceiptStatus::Completed)?;
        self.checkpoint()
    }

    pub(crate) fn work_failed(&mut self, node: &WorkNodeId) -> Result<(), ReceiptError> {
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

    pub(crate) fn publication_succeeded(&mut self, node: &WorkNodeId) -> Result<(), ReceiptError> {
        let publications = self.settled_publications(node);
        for identity in &publications {
            self.artifact_by_identity_mut(identity)?.disposition =
                Some(ArtifactDispositionProjection::Published);
        }
        match self.checkpoint() {
            Ok(()) => {
                for identity in publications {
                    self.pending_publications.remove(&identity);
                }
                Ok(())
            }
            Err(error) => {
                for identity in publications {
                    self.artifact_by_identity_mut(&identity)?.disposition = None;
                }
                Err(error)
            }
        }
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
        item.actual_elapsed_nanos = elapsed;
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
        if measurement.disposition() == ArtifactDisposition::Published {
            self.pending_publications.insert(planned);
        } else {
            artifact.disposition = Some(measurement.disposition().into());
        }
        artifact.actual_bytes = Some(measurement.bytes());
        artifact.path_identity = measurement.path().map(|path| hex(&path.as_bytes()));
        Ok(())
    }

    fn settled_publications(&self, node: &WorkNodeId) -> Vec<String> {
        let node_id = stable_text(node.as_str());
        let fences = self
            .body
            .plan
            .fences
            .iter()
            .filter(|fence| fence.node_id == node_id)
            .collect::<Vec<_>>();
        if !fences
            .iter()
            .any(|fence| fence.kind == fence_kind(FenceKind::Publication))
            || !fences
                .iter()
                .all(|fence| fence.status == ReceiptStatus::Completed)
        {
            return Vec::new();
        }
        self.body
            .plan
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.node_id == node_id
                    && self
                        .pending_publications
                        .contains(&artifact.artifact_identity)
            })
            .map(|artifact| artifact.artifact_identity.clone())
            .collect()
    }

    fn artifact_by_identity_mut(
        &mut self,
        identity: &str,
    ) -> Result<&mut ArtifactProjection, ReceiptError> {
        self.body
            .plan
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.artifact_identity == identity)
            .ok_or(ReceiptError::UnlistedPlanEvidence { kind: "artifact" })
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
    /// The configured root is not a directory.
    InvalidStore,
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
            Self::InvalidStore => formatter.write_str("receipt store root is not a directory"),
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
        &body.problem.geometry_identity,
        &body.problem.observation_identity,
        &body.problem.numerics_identity,
        &body.plan.plan_identity,
        &body.plan.product_execution.generation_identity,
        &body.plan.product_execution.graph_identity,
        &body.plan.dag_identity,
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
    match &body.problem.model_identity {
        ModelIdentityProjection::Empty => {}
        ModelIdentityProjection::Seed(identity) | ModelIdentityProjection::Generation(identity) => {
            require_integrity(is_digest(identity))?;
        }
    }
    validate_problem_evidence(&body.problem)?;

    require_integrity(body.revision > 0)?;
    require_integrity(!matches!(
        body.status,
        ReceiptStatus::Planned | ReceiptStatus::NotStarted
    ))?;
    require_integrity(body.status.is_terminal() == body.finished_unix_millis.is_some())?;
    match body.status {
        ReceiptStatus::Completed => {
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

    validate_plan_projection(&body.plan, body.revision)?;
    validate_product_execution(&body.problem.effective.product_graph, &body.plan)?;
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
                    required,
                    available,
                }),
            ) => require_integrity(
                is_redacted_text(resource)
                    && resource.len() <= MAX_FAILURE_SUBJECT_BYTES
                    && required > available,
            )?,
            (FailureKindProjection::ResourceInfeasible, None) | (_, Some(_)) => {
                return Err(ReceiptError::IntegrityMismatch);
            }
            (_, None) => {}
        }
    }
    require_integrity(
        (body.status == ReceiptStatus::Infeasible)
            == body
                .failure
                .as_ref()
                .is_some_and(|failure| failure.infeasibility.is_some()),
    )?;
    Ok(())
}

fn validate_problem_evidence(problem: &ProblemProjection) -> Result<(), ReceiptError> {
    let evidence = &problem.effective;
    require_integrity(evidence.schema_version == COMPILED_PROBLEM_EVIDENCE_VERSION)?;
    require_integrity(
        evidence.field("problem.identity") == Some(problem.problem_identity.as_str())
            && evidence.field("problem.numerics_identity")
                == Some(problem.numerics_identity.as_str())
            && evidence.field("geometry.identity") == Some(problem.geometry_identity.as_str())
            && evidence.field("observation.snapshot.identity")
                == Some(problem.observation_identity.as_str())
            && evidence.field("observation_transaction.snapshot_identity")
                == Some(problem.observation_identity.as_str())
            && evidence
                .field("observation_transaction.identity")
                .is_some_and(is_digest),
    )?;
    require_integrity(evidence.fields.iter().all(|(path, value)| {
        !path.is_empty()
            && path.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_')
            })
            && is_redacted_text(value)
    }))?;
    validate_data_description_evidence(&evidence.fields)?;
    for prefix in [
        "science.",
        "reconstruction.",
        "weighting.",
        "products.",
        "observation_transaction.",
        "numerics.",
        "required_capabilities.",
        "geometry.",
        "observation.",
    ] {
        require_integrity(evidence.fields.keys().any(|path| path.starts_with(prefix)))?;
    }
    let graph = &evidence.product_graph;
    require_integrity(
        graph.schema_version == ProductGraphId::SCHEMA_VERSION
            && is_digest(&graph.graph_identity)
            && graph
                .sources
                .iter()
                .map(|source| source.source_ordinal)
                .eq(0..graph.sources.len())
            && graph
                .nodes
                .iter()
                .enumerate()
                .all(|(ordinal, node)| node.node_id == ordinal)
            && !graph.publication_members.is_empty(),
    )?;
    for source in &graph.sources {
        require_integrity(match &source.domain {
            ImageDomainProjection::Main => true,
            ImageDomainProjection::Outlier(name) => is_redacted_text(name),
        })?;
    }
    for node in &graph.nodes {
        validate_product_graph_node(node, graph.sources.len())?;
    }
    let publication_members = graph
        .publication_members
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let physical_members = graph
        .nodes
        .iter()
        .filter(|node| node.schema == ProductSchemaProjection::CasaPagedImageF32)
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    require_integrity(
        publication_members.len() == graph.publication_members.len()
            && graph.publication_members == physical_members
            && graph.publication_members.iter().all(|member| {
                graph
                    .nodes
                    .get(*member)
                    .is_some_and(|node| node.schema == ProductSchemaProjection::CasaPagedImageF32)
            }),
    )?;
    Ok(())
}

fn validate_data_description_evidence(
    fields: &BTreeMap<String, String>,
) -> Result<(), ReceiptError> {
    let source_count = fields
        .get("observation.sources.count")
        .map(String::as_str)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0 && *count <= fields.len())
        .ok_or(ReceiptError::IntegrityMismatch)?;
    let read_count = fields
        .get("observation_transaction.read_set.count")
        .map(String::as_str)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count == source_count)
        .ok_or(ReceiptError::IntegrityMismatch)?;
    require_integrity(read_count == source_count)?;
    let mut expected_paths = BTreeSet::new();
    for source_index in 0..source_count {
        let source_prefix = format!("observation.sources.{source_index}");
        let source_identity = fields
            .get(&format!("{source_prefix}.measurement_set_identity"))
            .filter(|identity| is_digest(identity))
            .ok_or(ReceiptError::IntegrityMismatch)?;
        let transaction_identity = fields
            .get(&format!(
                "observation_transaction.read_set.{source_index}.measurement_set_identity"
            ))
            .filter(|identity| is_digest(identity))
            .ok_or(ReceiptError::IntegrityMismatch)?;
        require_integrity(source_identity == transaction_identity)?;
        let catalog_prefix = format!("{source_prefix}.selection.data_descriptions");
        let count_path = format!("{catalog_prefix}.count");
        let count = fields
            .get(&count_path)
            .map(String::as_str)
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|count| *count <= fields.len() / 3)
            .ok_or(ReceiptError::IntegrityMismatch)?;
        let selected_rows = fields
            .get(&format!("{source_prefix}.selection.rows.selected_count"))
            .map(String::as_str)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(ReceiptError::IntegrityMismatch)?;
        require_integrity(selected_rows == 0 || count > 0)?;
        expected_paths.insert(count_path);
        let mut previous_data_description = None;
        for index in 0..count {
            let member_prefix = format!("{catalog_prefix}.{index}");
            let data_description_path = format!("{member_prefix}.data_description_id");
            let spectral_window_path = format!("{member_prefix}.spectral_window_id");
            let polarization_path = format!("{member_prefix}.polarization_id");
            let data_description_id = fields
                .get(&data_description_path)
                .map(String::as_str)
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|value| i32::try_from(*value).is_ok())
                .ok_or(ReceiptError::IntegrityMismatch)?;
            let spectral_window_id = fields
                .get(&spectral_window_path)
                .map(String::as_str)
                .and_then(|value| value.parse::<u32>().ok());
            let polarization_id = fields
                .get(&polarization_path)
                .map(String::as_str)
                .and_then(|value| value.parse::<u32>().ok());
            require_integrity(spectral_window_id.is_some() && polarization_id.is_some())?;
            require_integrity(
                previous_data_description.is_none_or(|previous| previous < data_description_id),
            )?;
            previous_data_description = Some(data_description_id);
            expected_paths.extend([
                data_description_path,
                spectral_window_path,
                polarization_path,
            ]);
        }
    }
    require_integrity(fields.keys().all(|path| {
        source_member_is_within(path, "observation.sources.", source_count)
            && source_member_is_within(path, "observation_transaction.read_set.", source_count)
    }))?;
    require_integrity(fields.keys().all(|path| {
        !path.contains(".selection.data_descriptions.") || expected_paths.contains(path)
    }))
}

fn source_member_is_within(path: &str, prefix: &str, count: usize) -> bool {
    let Some(member) = path.strip_prefix(prefix) else {
        return true;
    };
    if member == "count" {
        return true;
    }
    member.split_once('.').is_some_and(|(index_text, tail)| {
        !tail.is_empty()
            && index_text
                .parse::<usize>()
                .is_ok_and(|index| index < count && index_text == index.to_string())
    })
}

fn validate_product_graph_node(
    node: &ProductGraphNodeEvidence,
    source_count: usize,
) -> Result<(), ReceiptError> {
    let physical = node.schema == ProductSchemaProjection::CasaPagedImageF32;
    require_integrity(
        node.name.as_deref().is_some_and(is_redacted_text) == physical
            && is_digest(&node.axes.geometry_identity)
            && validate_product_axes(&node.axes)
            && validate_product_validity(node.validity)
            && node
                .source_dependencies
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && node
                .source_dependencies
                .iter()
                .all(|source| *source < source_count)
            && node.dependencies.windows(2).all(|pair| pair[0] < pair[1])
            && node
                .dependencies
                .iter()
                .all(|dependency| *dependency < node.node_id)
            && match node.beam {
                ProductBeamEvidence::Inherit(dependency) => dependency < node.node_id,
                _ => true,
            },
    )?;
    validate_product_payload(&node.payload, node.schema, node.axes.shape)
}

fn validate_product_axes(axes: &ProductAxesEvidence) -> bool {
    let distinct_axes = axes.order.iter().copied().collect::<BTreeSet<_>>();
    let shape_valid = match axes.kind {
        ProductAxisKindProjection::SkyImage | ProductAxisKindProjection::PlaneState => {
            axes.shape.iter().all(|extent| *extent > 0)
        }
        ProductAxisKindProjection::Metadata => axes.shape.iter().all(|extent| *extent == 0),
    };
    shape_valid
        && distinct_axes.len() == axes.order.len()
        && !axes.polarization.is_empty()
        && match &axes.domain {
            ImageDomainProjection::Main => true,
            ImageDomainProjection::Outlier(name) => is_redacted_text(name),
        }
        && validate_direction_projection(axes.direction)
        && validate_spectral_projection(&axes.spectral)
}

fn validate_direction_projection(direction: DirectionCoordinateProjection) -> bool {
    let finite = |bits| f64::from_bits(bits).is_finite();
    finite(direction.longitude_bits)
        && finite(direction.latitude_bits)
        && direction.reference_pixel_bits.into_iter().all(finite)
        && direction
            .increment_rad_bits
            .into_iter()
            .all(|bits| finite(bits) && f64::from_bits(bits) != 0.0)
        && direction.pc_bits.into_iter().flatten().all(finite)
        && direction.pole_deg_bits.into_iter().all(finite)
}

fn validate_spectral_projection(spectral: &SpectralCoordinateProjection) -> bool {
    let finite = |bits| f64::from_bits(bits).is_finite();
    let anchor_valid = match spectral.anchor {
        SpectralAnchorProjection::NotApplicable => true,
        SpectralAnchorProjection::Conversion {
            epoch_mjd_bits,
            direction,
            observatory_metres_bits,
            ..
        } => {
            finite(epoch_mjd_bits)
                && finite(direction.longitude_bits)
                && finite(direction.latitude_bits)
                && observatory_metres_bits.into_iter().all(finite)
        }
    };
    let wcs_valid = match &spectral.wcs {
        SpectralWcsProjection::Linear {
            channels,
            reference_pixel_bits,
            reference_frequency_hz_bits,
            increment_hz_bits,
        } => {
            *channels > 0
                && finite(*reference_pixel_bits)
                && f64::from_bits(*reference_frequency_hz_bits).is_finite()
                && f64::from_bits(*reference_frequency_hz_bits) > 0.0
                && finite(*increment_hz_bits)
                && f64::from_bits(*increment_hz_bits) != 0.0
        }
        SpectralWcsProjection::Tabular {
            channel_centres_hz_bits,
            channel_boundaries_hz_bits,
        } => {
            !channel_centres_hz_bits.is_empty()
                && channel_boundaries_hz_bits.len() == channel_centres_hz_bits.len() + 1
                && channel_centres_hz_bits
                    .iter()
                    .chain(channel_boundaries_hz_bits)
                    .all(|bits| f64::from_bits(*bits).is_finite() && f64::from_bits(*bits) > 0.0)
        }
    };
    let rest_frequency_valid = match spectral.rest_frequency {
        RestFrequencyProjection::NotApplicable => true,
        RestFrequencyProjection::Line(bits) => {
            f64::from_bits(bits).is_finite() && f64::from_bits(bits) > 0.0
        }
    };
    anchor_valid && wcs_valid && rest_frequency_valid
}

fn validate_product_validity(validity: ProductValidityProjection) -> bool {
    fn primary_beam(policy: PrimaryBeamPolicyProjection) -> bool {
        let cutoff = f32::from_bits(policy.cutoff_bits);
        cutoff.is_finite() && cutoff > 0.0
    }
    fn taylor(policy: TaylorPolicyProjection) -> bool {
        let fraction = f32::from_bits(policy.peak_fraction_bits);
        fraction.is_finite() && fraction > 0.0 && fraction <= 1.0
    }
    match validity {
        ProductValidityProjection::All | ProductValidityProjection::FinalNormalState => true,
        ProductValidityProjection::PrimaryBeam { policy } => primary_beam(policy),
        ProductValidityProjection::Taylor { policy } => taylor(policy),
        ProductValidityProjection::TaylorAndPrimaryBeam {
            taylor: taylor_policy,
            primary_beam: primary_beam_policy,
        } => taylor(taylor_policy) && primary_beam(primary_beam_policy),
    }
}

fn validate_product_payload(
    payload: &ProductPayloadEvidence,
    schema: ProductSchemaProjection,
    shape: [usize; 4],
) -> Result<(), ReceiptError> {
    let shape_elements = shape.into_iter().try_fold(1_u64, |elements, extent| {
        elements.checked_mul(u64::try_from(extent).ok()?)
    });
    require_integrity(
        payload
            .logical_pixel_bytes
            .checked_add(payload.identity_metadata_bytes)
            == Some(payload.identity_envelope_bytes)
            && payload.identity_metadata_bytes > 0
            && match (payload.element_representation, schema) {
                (
                    ProductElementProjection::NotApplicable,
                    ProductSchemaProjection::LogicalCollection
                    | ProductSchemaProjection::CasaImageMetadata,
                ) => payload.logical_elements == 0 && payload.logical_pixel_bytes == 0,
                (ProductElementProjection::Float32, ProductSchemaProjection::CasaPagedImageF32) => {
                    Some(payload.logical_elements) == shape_elements
                        && payload.logical_elements.checked_mul(4)
                            == Some(payload.logical_pixel_bytes)
                }
                _ => false,
            },
    )
}

fn validate_plan_projection(plan: &PlanProjection, revision: u64) -> Result<(), ReceiptError> {
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
                        && claim.actual_peak.is_none_or(|peak| peak <= claim.amount)
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
                && lease_resource_is_valid(&slot.lease_resource)
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

fn validate_product_execution(
    graph: &ProductGraphEvidence,
    plan: &PlanProjection,
) -> Result<(), ReceiptError> {
    let product = &plan.product_execution;
    require_integrity(
        product.graph_identity == graph.graph_identity
            && is_digest(&product.generation_identity)
            && product
                .source_bindings
                .iter()
                .map(|binding| binding.source_ordinal)
                .eq(graph.sources.iter().map(|source| source.source_ordinal))
            && product
                .source_bindings
                .iter()
                .all(|binding| is_digest(&binding.generation_identity)),
    )?;
    require_integrity(
        parse_digest(&product.generation_identity) == projected_product_generation_id(product),
    )?;
    let publication_nodes = plan
        .nodes
        .iter()
        .filter(|node| node.kind == "publication")
        .collect::<Vec<_>>();
    require_integrity(
        publication_nodes.len() == 1
            && publication_nodes[0].node_id == product.publication_node
            && product.members.len() == graph.publication_members.len(),
    )?;
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
    for (member, expected_node) in product
        .members
        .iter()
        .zip(graph.publication_members.iter().copied())
    {
        require_integrity(matches!(
            &member.participant,
            ProductParticipantEvidence::Product(node) if *node == expected_node
        ))?;
        require_integrity(is_digest(&member.planned_artifact))?;
        require_integrity(
            parse_digest(&member.planned_artifact)
                == projected_product_artifact_id(&product.generation_identity, expected_node),
        )?;
        let planned = plan
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_identity == member.planned_artifact);
        require_integrity(planned.is_some_and(|artifact| {
            artifact.role == "output" && artifact.node_id == product.publication_node
        }))?;
        let layout = &member.layout;
        let staging = &layout.staging;
        require_integrity(
            is_digest(&layout.layout_identity)
                && layout.staged_storage_bytes > 0
                && layout.final_storage_bytes > 0
                && layout.writer_buffer_bytes > 0
                && plan
                    .nodes
                    .iter()
                    .any(|node| node.node_id == staging.producer)
                && dependency_owner(&staging.terminal) == Some(staging.producer.as_str())
                && valid_events.contains(&staging.terminal)
                && projection_depends_on_event(plan, &product.publication_node, &staging.terminal)
                && io_buffer_is_valid(&staging.writer_buffer_kind)
                && crate::publication_layout::is_writer_buffer(parse_io_buffer(
                    &staging.writer_buffer_kind,
                ))
                && plan
                    .allocation_generations
                    .iter()
                    .any(|allocation| allocation.generation_identity == staging.writer_allocation)
                && publication_allocation_matches(
                    plan,
                    &staging.writer_allocation,
                    &staging.writer_physical_slot,
                    &staging.writer_lease_resource,
                )
                && plan.nodes.iter().any(|node| {
                    node.node_id == staging.producer
                        && node
                            .allocation_uses
                            .iter()
                            .any(|usage| usage.generation_identity == staging.writer_allocation)
                }),
        )?;
        match &staging.mapped_page_cache {
            Some(mapped) => require_integrity(
                layout.mapped_page_cache_bytes > 0
                    && plan
                        .nodes
                        .iter()
                        .any(|node| node.node_id == mapped.producer)
                    && dependency_owner(&mapped.terminal) != Some(mapped.producer.as_str())
                    && valid_events.contains(&mapped.terminal)
                    && projection_depends_on_event(
                        plan,
                        &product.publication_node,
                        &mapped.terminal,
                    )
                    && plan
                        .allocation_generations
                        .iter()
                        .any(|allocation| allocation.generation_identity == mapped.allocation)
                    && publication_allocation_matches(
                        plan,
                        &mapped.allocation,
                        &mapped.physical_slot,
                        &mapped.lease_resource,
                    )
                    && plan.nodes.iter().any(|node| {
                        node.node_id == mapped.producer
                            && node
                                .allocation_uses
                                .iter()
                                .any(|usage| usage.generation_identity == mapped.allocation)
                    }),
            )?,
            None => require_integrity(layout.mapped_page_cache_bytes == 0)?,
        }
    }
    Ok(())
}

fn publication_allocation_matches(
    plan: &PlanProjection,
    allocation_identity: &str,
    physical_slot: &str,
    lease_resource: &str,
) -> bool {
    let Some(allocation) = plan
        .allocation_generations
        .iter()
        .find(|allocation| allocation.generation_identity == allocation_identity)
    else {
        return false;
    };
    allocation.physical_slot == physical_slot
        && plan.physical_slots.iter().any(|slot| {
            slot.slot_identity == physical_slot && slot.lease_resource == lease_resource
        })
}

fn projected_product_generation_id(product: &ProductExecutionEvidence) -> [u8; 32] {
    let mut digest = Sha256::new();
    canonical_bytes(&mut digest, b"casa-rs-product-generation");
    digest.update(1_u32.to_le_bytes());
    digest.update(parse_digest(&product.graph_identity));
    digest.update((product.source_bindings.len() as u128).to_le_bytes());
    for binding in &product.source_bindings {
        digest.update((binding.source_ordinal as u128).to_le_bytes());
        digest.update(parse_digest(&binding.generation_identity));
    }
    digest.finalize().into()
}

fn projected_product_artifact_id(generation: &str, node: usize) -> [u8; 32] {
    let mut digest = Sha256::new();
    canonical_bytes(&mut digest, b"casa-rs-product-artifact");
    digest.update(1_u32.to_le_bytes());
    digest.update(parse_digest(generation));
    digest.update((node as u128).to_le_bytes());
    digest.finalize().into()
}

fn canonical_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u128).to_le_bytes());
    digest.update(value);
}

fn dependency_owner(value: &str) -> Option<&str> {
    if let Some(node) = value.strip_prefix("work:") {
        return Some(node);
    }
    value
        .strip_prefix("fence:")?
        .rsplit_once(':')
        .map(|(node, _)| node)
}

fn projection_depends_on_event(plan: &PlanProjection, node: &str, event: &str) -> bool {
    fn visit(
        plan: &PlanProjection,
        node: &str,
        event: &str,
        visited: &mut BTreeSet<String>,
    ) -> bool {
        if !visited.insert(node.to_string()) {
            return false;
        }
        let Some(node) = plan
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == node)
        else {
            return false;
        };
        node.dependencies.iter().any(|dependency| {
            dependency == event
                || dependency_owner(dependency)
                    .is_some_and(|owner| visit(plan, owner, event, visited))
        })
    }
    visit(plan, node, event, &mut BTreeSet::new())
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
            | "observation_read"
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
    let mut temporary = NamedTempFile::new_in(parent).map_err(|source| ReceiptError::Io {
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
        "problem.numerics_identity",
        hex(&problem.numerics_id().as_bytes()),
    );
    project_science(&mut fields, problem);
    project_reconstruction(&mut fields, problem);
    project_weighting(&mut fields, problem);
    project_products(&mut fields, problem);
    project_observation_transaction(&mut fields, problem);
    project_numerics(&mut fields, problem);
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

fn project_observation_transaction(
    fields: &mut BTreeMap<String, String>,
    problem: &CompiledProblem,
) {
    let transaction = problem.observation_transaction();
    evidence_field(
        fields,
        "observation_transaction.identity",
        hex(&transaction.transaction_id().as_bytes()),
    );
    evidence_field(
        fields,
        "observation_transaction.snapshot_identity",
        hex(&transaction.observation_snapshot_id().as_bytes()),
    );
    evidence_field(
        fields,
        "observation_transaction.read_set.count",
        transaction.read_set().sources().len(),
    );
    for (index, source) in transaction.read_set().sources().iter().enumerate() {
        let prefix = format!("observation_transaction.read_set.{index}");
        evidence_field(
            fields,
            format!("{prefix}.measurement_set_identity"),
            hex(&source.measurement_set().identity().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.consistency_token"),
            hex(&source.consistency_token().identity().as_bytes()),
        );
    }
    for (index, write) in transaction.write_set().model_columns().iter().enumerate() {
        let prefix = format!("observation_transaction.write_set.model_columns.{index}");
        evidence_field(
            fields,
            format!("{prefix}.measurement_set_identity"),
            hex(&write.measurement_set().identity().as_bytes()),
        );
        evidence_field(
            fields,
            format!("{prefix}.expected_consistency_token"),
            hex(&write.expected_consistency_token().identity().as_bytes()),
        );
        match write.precondition() {
            ModelColumnPrecondition::Absent => {
                evidence_field(fields, format!("{prefix}.precondition"), "absent");
            }
            ModelColumnPrecondition::Generation(identity) => {
                evidence_field(fields, format!("{prefix}.precondition"), "generation");
                evidence_field(
                    fields,
                    format!("{prefix}.precondition_generation"),
                    hex(&identity.as_bytes()),
                );
            }
        }
        match write.disposition() {
            ModelColumnWriteDisposition::ReplaceSelectedCells => {
                evidence_field(
                    fields,
                    format!("{prefix}.disposition"),
                    "replace_selected_cells",
                );
            }
            ModelColumnWriteDisposition::CreateAndInitializeAllRows {
                row_count,
                initialization: ModelColumnInitialization::Zero,
            } => {
                evidence_field(
                    fields,
                    format!("{prefix}.disposition"),
                    "create_and_initialize_all_rows",
                );
                evidence_field(fields, format!("{prefix}.row_count"), row_count);
                evidence_field(fields, format!("{prefix}.initialization"), "zero");
            }
        }
    }
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
    evidence_field(
        fields,
        "products.validity.primary_beam.cutoff",
        format!(
            "f32:{:08x}",
            products.validity().primary_beam().cutoff().to_bits()
        ),
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
    evidence_field(
        fields,
        "observation.sources.count",
        snapshot.sources().len(),
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

fn work_kind(kind: WorkKind) -> &'static str {
    match kind {
        WorkKind::DataCensus => "data_census",
        WorkKind::ObservationRead => "observation_read",
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
        .expect("validated receipt dependency projection");
    let (node, kind) = value
        .rsplit_once(':')
        .expect("validated receipt fence dependency projection");
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
        LeaseResource::Locks => "locks".to_string(),
        LeaseResource::MeasurementSetLock { measurement_set } => {
            format!(
                "measurement_set_lock:{}",
                hex(&measurement_set.identity().as_bytes())
            )
        }
        LeaseResource::FileDescriptors => "file_descriptors".to_string(),
    }
}

fn parse_lease_resource(value: &str) -> LeaseResource {
    if let Some(allocation_id) = value.strip_prefix("memory:") {
        return LeaseResource::Memory {
            allocation_id: allocation_id.to_string(),
        };
    }
    if let Some(kind) = value.strip_prefix("runtime_overhead:") {
        return LeaseResource::RuntimeOverhead(parse_runtime_overhead(kind));
    }
    if let Some(kind) = value.strip_prefix("io_buffer:") {
        return LeaseResource::IoBuffer(parse_io_buffer(kind));
    }
    if let Some(storage) = value.strip_prefix("storage:") {
        let (demand_id, use_kind) = storage
            .rsplit_once(':')
            .expect("validated receipt storage lease resource");
        return LeaseResource::Storage {
            demand_id: demand_id.to_string(),
            use_kind: parse_storage_use(use_kind),
        };
    }
    if let Some(demand_id) = value.strip_prefix("storage_read_rate:") {
        return LeaseResource::StorageReadRate {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("storage_write_rate:") {
        return LeaseResource::StorageWriteRate {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("storage_operations_rate:") {
        return LeaseResource::StorageOperationsRate {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("storage_queue:") {
        return LeaseResource::StorageQueue {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("rate:") {
        return LeaseResource::Rate {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("queue:") {
        return LeaseResource::Queue {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("transfer_rate:") {
        return LeaseResource::TransferRate {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("transfer_queue:") {
        return LeaseResource::TransferQueue {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("accelerator:") {
        return LeaseResource::Accelerator {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(demand_id) = value.strip_prefix("accelerator_queue:") {
        return LeaseResource::AcceleratorCommandQueue {
            demand_id: demand_id.to_string(),
        };
    }
    if let Some(measurement_set) = value.strip_prefix("measurement_set_lock:") {
        return LeaseResource::MeasurementSetLock {
            measurement_set: casa_imaging_model::MeasurementSetIdentity::new(
                LogicalIdentity::from_sha256(parse_digest(measurement_set)),
            ),
        };
    }
    match value {
        "workers" => LeaseResource::Workers,
        "resident_cache" => LeaseResource::ResidentCache,
        "locks" => LeaseResource::Locks,
        "file_descriptors" => LeaseResource::FileDescriptors,
        _ => unreachable!("validated receipt lease-resource projection"),
    }
}

fn lease_resource_is_valid(value: &str) -> bool {
    if let Some(allocation_id) = value.strip_prefix("memory:") {
        return is_redacted_text(allocation_id);
    }
    if let Some(kind) = value.strip_prefix("runtime_overhead:") {
        return matches!(
            kind,
            "thread_stack"
                | "allocator_fragmentation"
                | "external_library"
                | "fft_workspace"
                | "driver"
                | "jit"
                | "command_buffer"
        );
    }
    if let Some(kind) = value.strip_prefix("io_buffer:") {
        return io_buffer_is_valid(kind);
    }
    if let Some(storage) = value.strip_prefix("storage:") {
        return storage
            .rsplit_once(':')
            .is_some_and(|(demand_id, use_kind)| {
                is_redacted_text(demand_id)
                    && matches!(
                        use_kind,
                        "temporary" | "staged_output" | "final_output" | "persistent_cache"
                    )
            });
    }
    for prefix in [
        "storage_read_rate:",
        "storage_write_rate:",
        "storage_operations_rate:",
        "storage_queue:",
        "rate:",
        "queue:",
        "transfer_rate:",
        "transfer_queue:",
        "accelerator:",
        "accelerator_queue:",
    ] {
        if let Some(demand_id) = value.strip_prefix(prefix) {
            return is_redacted_text(demand_id);
        }
    }
    if let Some(measurement_set) = value.strip_prefix("measurement_set_lock:") {
        return is_digest(measurement_set);
    }
    matches!(
        value,
        "workers" | "resident_cache" | "locks" | "file_descriptors"
    )
}

fn parse_runtime_overhead(value: &str) -> crate::RuntimeOverheadKind {
    match value {
        "thread_stack" => crate::RuntimeOverheadKind::ThreadStack,
        "allocator_fragmentation" => crate::RuntimeOverheadKind::AllocatorFragmentation,
        "external_library" => crate::RuntimeOverheadKind::ExternalLibrary,
        "fft_workspace" => crate::RuntimeOverheadKind::FftWorkspace,
        "driver" => crate::RuntimeOverheadKind::Driver,
        "jit" => crate::RuntimeOverheadKind::Jit,
        "command_buffer" => crate::RuntimeOverheadKind::CommandBuffer,
        _ => unreachable!("validated receipt runtime-overhead projection"),
    }
}

fn parse_storage_use(value: &str) -> crate::StorageUseKind {
    match value {
        "temporary" => crate::StorageUseKind::Temporary,
        "staged_output" => crate::StorageUseKind::StagedOutput,
        "final_output" => crate::StorageUseKind::FinalOutput,
        "persistent_cache" => crate::StorageUseKind::PersistentCache,
        _ => unreachable!("validated receipt storage-use projection"),
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
        _ => unreachable!("validated receipt I/O-buffer projection"),
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
    use std::collections::BTreeMap;

    use super::{
        ReceiptError, maximum_json_serialized_text, stable_float,
        validate_data_description_evidence,
    };

    #[test]
    fn compiled_problem_evidence_requires_complete_data_description_tuples() {
        let mut fields = BTreeMap::from([
            ("observation.sources.count".to_string(), "1".to_string()),
            (
                "observation_transaction.read_set.count".to_string(),
                "1".to_string(),
            ),
            (
                "observation.sources.0.measurement_set_identity".to_string(),
                "00".repeat(32),
            ),
            (
                "observation_transaction.read_set.0.measurement_set_identity".to_string(),
                "00".repeat(32),
            ),
            (
                "observation.sources.0.selection.rows.selected_count".to_string(),
                "1".to_string(),
            ),
            (
                "observation.sources.0.selection.data_descriptions.count".to_string(),
                "1".to_string(),
            ),
            (
                "observation.sources.0.selection.data_descriptions.0.data_description_id"
                    .to_string(),
                "0".to_string(),
            ),
            (
                "observation.sources.0.selection.data_descriptions.0.spectral_window_id"
                    .to_string(),
                "2".to_string(),
            ),
            (
                "observation.sources.0.selection.data_descriptions.0.polarization_id".to_string(),
                "3".to_string(),
            ),
        ]);
        assert!(validate_data_description_evidence(&fields).is_ok());

        let mut empty_catalog = fields.clone();
        empty_catalog.insert(
            "observation.sources.0.selection.data_descriptions.count".to_string(),
            "0".to_string(),
        );
        empty_catalog.retain(|path, _| {
            !path.starts_with("observation.sources.0.selection.data_descriptions.0.")
        });
        assert!(matches!(
            validate_data_description_evidence(&empty_catalog),
            Err(ReceiptError::IntegrityMismatch)
        ));

        let polarization = fields
            .remove("observation.sources.0.selection.data_descriptions.0.polarization_id")
            .expect("polarization fixture");
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
        fields.insert(
            "observation.sources.0.selection.data_descriptions.0.polarization_id".to_string(),
            polarization,
        );
        fields.insert(
            "observation.sources.0.selection.data_descriptions.1.data_description_id".to_string(),
            "1".to_string(),
        );
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
    }

    #[test]
    fn compiled_problem_evidence_requires_exact_contiguous_observation_sources() {
        let mut fields = BTreeMap::from([
            ("observation.sources.count".to_string(), "2".to_string()),
            (
                "observation_transaction.read_set.count".to_string(),
                "2".to_string(),
            ),
        ]);
        for index in 0..2 {
            let source = format!("observation.sources.{index}");
            let read = format!("observation_transaction.read_set.{index}");
            fields.insert(
                format!("{source}.measurement_set_identity"),
                format!("{index:064x}"),
            );
            fields.insert(
                format!("{read}.measurement_set_identity"),
                format!("{index:064x}"),
            );
            fields.insert(
                format!("{source}.selection.rows.selected_count"),
                "1".to_string(),
            );
            fields.insert(
                format!("{source}.selection.data_descriptions.count"),
                "1".to_string(),
            );
            fields.insert(
                format!("{source}.selection.data_descriptions.0.data_description_id"),
                index.to_string(),
            );
            fields.insert(
                format!("{source}.selection.data_descriptions.0.spectral_window_id"),
                index.to_string(),
            );
            fields.insert(
                format!("{source}.selection.data_descriptions.0.polarization_id"),
                index.to_string(),
            );
        }
        assert!(validate_data_description_evidence(&fields).is_ok());

        let source_one = fields
            .iter()
            .filter(|(path, _)| path.starts_with("observation.sources.1."))
            .map(|(path, value)| (path.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (path, _) in &source_one {
            fields.remove(path);
        }
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
        for (path, value) in source_one {
            fields.insert(path, value);
        }

        fields.insert(
            "observation.sources.2.measurement_set_identity".to_string(),
            "02".repeat(32),
        );
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
        fields.remove("observation.sources.2.measurement_set_identity");

        fields.insert(
            "observation.sources.01.measurement_set_identity".to_string(),
            "01".repeat(32),
        );
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
        fields.remove("observation.sources.01.measurement_set_identity");
        fields.insert(
            "observation.sources.0.".to_string(),
            "malformed".to_string(),
        );
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
        fields.remove("observation.sources.0.");

        let source_zero_catalog = "observation.sources.0.selection.data_descriptions";
        fields.insert(format!("{source_zero_catalog}.count"), "2".to_string());
        for (field, value) in [
            ("data_description_id", "1"),
            ("spectral_window_id", "1"),
            ("polarization_id", "1"),
        ] {
            fields.insert(
                format!("{source_zero_catalog}.1.{field}"),
                value.to_string(),
            );
        }
        assert!(validate_data_description_evidence(&fields).is_ok());
        fields.insert(
            format!("{source_zero_catalog}.1.data_description_id"),
            "0".to_string(),
        );
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
        fields.insert(
            format!("{source_zero_catalog}.1.data_description_id"),
            "1".to_string(),
        );

        fields.insert(
            "observation.sources.count".to_string(),
            usize::MAX.to_string(),
        );
        assert!(matches!(
            validate_data_description_evidence(&fields),
            Err(ReceiptError::IntegrityMismatch)
        ));
    }

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
}
