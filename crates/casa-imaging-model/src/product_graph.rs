// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-owned product topology and atomic publication contract.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use thiserror::Error;

use crate::{
    AxisOrder, CompiledGeometry, CompiledGeometryId, CompiledImageDomain, CompiledProblemId,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FrequencyFrame, ImageAxis,
    ImageDomainRole, PolarizationCoordinate, PrimaryBeamValidityPolicy, ProductBlankingPolicy,
    ProductBoundaryOperation, ProductNormalization, ProductNormalizationBoundary,
    ProductRequirements, ProductSupportComparison, Projection, ReconstructionBasis,
    ReconstructionContract, RestFrequency, RestoringBeamPolicy, SpectralCoordinateSpec,
    SpectralFrameAnchor, SpectralWcs, TaylorSupportReference, TaylorValidityPolicy, TimeScale,
    WeightingGenerationCompletionEvidence, WeightingGenerationId, WeightingOperatorContract,
    compiled_problem::{CanonicalEncoder, LogicalIdentity, ProductKind},
};

const PRODUCT_GRAPH_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-graph";
const PRODUCT_GRAPH_IDENTITY_VERSION: u32 = 3;
const PRODUCT_GENERATION_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-generation";
const PRODUCT_GENERATION_IDENTITY_VERSION: u32 = 1;
const PRODUCT_ARTIFACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-artifact";
const PRODUCT_ARTIFACT_IDENTITY_VERSION: u32 = 1;

/// Stable compiler-derived identity of one complete product topology.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProductGraphId(LogicalIdentity);

impl ProductGraphId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = PRODUCT_GRAPH_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ProductGraphId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductGraphId(")?;
        write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ProductGraphId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.as_bytes())
    }
}

/// Stable local identity of one source-generation slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProductSourceId(usize);

impl ProductSourceId {
    /// Construct an ordinal for decoding and checked graph binding.
    #[must_use]
    pub const fn from_ordinal(ordinal: usize) -> Self {
        Self(ordinal)
    }

    /// Return the zero-based canonical source ordinal.
    #[must_use]
    pub const fn ordinal(self) -> usize {
        self.0
    }
}

/// Stable local identity of one logical product node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProductNodeId(usize);

impl ProductNodeId {
    /// Return the zero-based canonical node ordinal.
    #[must_use]
    pub const fn ordinal(self) -> usize {
        self.0
    }
}

/// Coefficient placement of a product or source generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductTerm {
    /// A non-Taylor product.
    Single,
    /// One zero-based Taylor coefficient or convolution order.
    Taylor(usize),
}

/// Exact logical role of one graph node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductRole {
    /// Point-spread function or Taylor convolution term.
    Psf(ProductTerm),
    /// Authoritative final residual.
    Residual(ProductTerm),
    /// Reconstructed coefficient model.
    Model(ProductTerm),
    /// Restored image.
    RestoredImage(ProductTerm),
    /// Sum-of-weights plane state.
    SumWeights(ProductTerm),
    /// CLEAN constraint mask, distinct from output validity.
    CleanMask,
    /// Imaging weight image or Taylor convolution term.
    Weight(ProductTerm),
    /// Primary-beam response.
    PrimaryBeam(ProductTerm),
    /// Primary-beam spectral index required to correct the sky spectral index.
    PrimaryBeamSpectralIndex,
    /// Sensitivity response, distinct from weight and primary beam.
    Sensitivity,
    /// Primary-beam-corrected restored image.
    PbCorrectedImage(ProductTerm),
    /// Authoritative collection of Taylor coefficient products.
    TaylorCoefficientSet,
    /// Spectral-index product.
    SpectralIndex,
    /// Spectral-index uncertainty.
    SpectralIndexError,
    /// Primary-beam-corrected spectral index.
    PbCorrectedSpectralIndex,
    /// Fitted and selected beam metadata embedded in image products.
    BeamMetadata,
}

/// Upstream generation authority required by product formation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductSourceRole {
    /// Final normal state after the mandatory final Major Cycle.
    FinalNormalState,
    /// Final reconstruction model generation.
    FinalModel,
    /// Global weighting and sum-weight generation.
    WeightingGeneration,
    /// CLEAN-mask generation supplied by the minor cycle.
    CleanMaskGeneration,
    /// Primary-beam generation supplied by the response or mosaic layer.
    PrimaryBeamGeneration,
    /// Primary-beam spectral-index generation supplied by the response or mosaic layer.
    PrimaryBeamSpectralIndexGeneration,
    /// Sensitivity generation supplied by the response or mosaic layer.
    SensitivityGeneration,
    /// Fitted or selected restoring-beam generation.
    RestoringBeamGeneration,
}

/// One typed slot bound to an exact runtime generation before staging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductSource {
    source_id: ProductSourceId,
    role: ProductSourceRole,
    domain: ImageDomainRole,
    term: ProductTerm,
}

impl ProductSource {
    /// Return the graph-local source identity.
    #[must_use]
    pub const fn source_id(&self) -> ProductSourceId {
        self.source_id
    }

    /// Return the upstream generation role.
    #[must_use]
    pub const fn role(&self) -> ProductSourceRole {
        self.role
    }

    /// Return the image domain supplied by this generation.
    #[must_use]
    pub const fn domain(&self) -> &ImageDomainRole {
        &self.domain
    }

    /// Return the coefficient placement supplied by this generation.
    #[must_use]
    pub const fn term(&self) -> ProductTerm {
        self.term
    }
}

/// Pixel-axis role of a product node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductAxisKind {
    /// Full sky-image axes.
    SkyImage,
    /// Per-polarization/per-spectral-plane state with unit direction extents.
    PlaneState,
    /// Logical collection or metadata embedded in image products.
    Metadata,
}

/// Exact coordinate and storage-axis binding of one product node.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductAxes {
    kind: ProductAxisKind,
    geometry_id: CompiledGeometryId,
    domain: ImageDomainRole,
    order: AxisOrder,
    shape: [usize; 4],
    direction: DirectionCoordinateSpec,
    spectral: SpectralCoordinateSpec,
    polarization: Box<[PolarizationCoordinate]>,
}

impl ProductAxes {
    /// Return the logical pixel-axis role.
    #[must_use]
    pub const fn kind(&self) -> ProductAxisKind {
        self.kind
    }

    /// Return the immutable geometry identity supplying WCS semantics.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry_id
    }

    /// Return the user-visible image domain.
    #[must_use]
    pub const fn domain(&self) -> &ImageDomainRole {
        &self.domain
    }

    /// Return axes in CASA image storage order.
    #[must_use]
    pub const fn order(&self) -> &AxisOrder {
        &self.order
    }

    /// Return exact extents in storage-axis order.
    #[must_use]
    pub const fn shape(&self) -> [usize; 4] {
        self.shape
    }

    /// Return the exact direction-coordinate law.
    #[must_use]
    pub const fn direction(&self) -> DirectionCoordinateSpec {
        self.direction
    }

    /// Return the exact spectral-coordinate law.
    #[must_use]
    pub const fn spectral(&self) -> &SpectralCoordinateSpec {
        &self.spectral
    }

    /// Return reconstruction-owned polarization coordinates.
    #[must_use]
    pub const fn polarization(&self) -> &[PolarizationCoordinate] {
        &self.polarization
    }
}

/// Physical unit required in a product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductUnit {
    /// No numeric payload unit applies.
    NotApplicable,
    /// Jansky per fitted or restoring beam.
    JyPerBeam,
    /// Jansky per image pixel.
    JyPerPixel,
    /// Dimensionless response, mask, or spectral index.
    Dimensionless,
    /// Visibility-weight sum in the weighting contract's native measure.
    VisibilityWeight,
}

/// Beam and restoration metadata required by one product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductBeamRule {
    /// The product has no beam metadata.
    None,
    /// Attach the fitted point-spread-function beam set.
    Fitted,
    /// Restore with the exact compiled restoring-beam policy.
    Restoring(RestoringBeamPolicy),
    /// Inherit beam metadata from another product.
    Inherit(ProductNodeId),
    /// Publish fitted and selected beam metadata for this policy.
    Metadata(RestoringBeamPolicy),
}

/// Valid support persisted with one product, distinct from a CLEAN mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductValidityRule {
    /// Every pixel represented by the coordinate domain is valid.
    All,
    /// Validity and blank channels come from the final normal state.
    FinalNormalState,
    /// Validity requires the exact request-carried primary-beam policy.
    PrimaryBeam(PrimaryBeamValidityPolicy),
    /// Validity requires the exact request-carried Taylor-coefficient policy.
    Taylor(TaylorValidityPolicy),
    /// Validity requires both exact request-carried policies.
    TaylorAndPrimaryBeam {
        /// Taylor-coefficient support policy.
        taylor: TaylorValidityPolicy,
        /// Primary-beam support policy.
        primary_beam: PrimaryBeamValidityPolicy,
    },
}

/// Logical element representation required by one product contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductElementRepresentation {
    /// The product has no numeric pixel payload.
    NotApplicable,
    /// IEEE-754 binary32 pixels.
    Float32,
}

/// Compiler-owned logical payload and identity projection for one product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductPayloadEnvelope {
    element_representation: ProductElementRepresentation,
    logical_elements: u64,
    logical_pixel_bytes: u64,
    identity_metadata_bytes: u64,
    identity_envelope_bytes: u64,
}

impl ProductPayloadEnvelope {
    /// Return the typed logical pixel representation.
    #[must_use]
    pub const fn element_representation(self) -> ProductElementRepresentation {
        self.element_representation
    }

    /// Return the number of logical pixel elements.
    #[must_use]
    pub const fn logical_elements(self) -> u64 {
        self.logical_elements
    }

    /// Return logical pixel payload bytes, independent of physical layout.
    #[must_use]
    pub const fn logical_pixel_bytes(self) -> u64 {
        self.logical_pixel_bytes
    }

    /// Return bytes in the canonical compiler-owned metadata identity projection.
    #[must_use]
    pub const fn identity_metadata_bytes(self) -> u64 {
        self.identity_metadata_bytes
    }

    /// Return logical pixels plus canonical metadata identity-projection bytes.
    #[must_use]
    pub const fn identity_envelope_bytes(self) -> u64 {
        self.identity_envelope_bytes
    }
}

/// Existing interoperable representation required for a logical product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductSchema {
    /// A CASA/casacore paged image with binary32 pixels and attached metadata.
    CasaPagedImageF32,
    /// A logical collection of named CASA Taylor products in this graph.
    LogicalCollection,
    /// Beam metadata embedded in the graph's CASA image products.
    CasaImageMetadata,
}

/// One immutable product node in topological and publication order.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductNode {
    node_id: ProductNodeId,
    role: ProductRole,
    name: Option<String>,
    axes: ProductAxes,
    unit: ProductUnit,
    normalization: Option<ProductNormalization>,
    beam: ProductBeamRule,
    validity: ProductValidityRule,
    schema: ProductSchema,
    payload: ProductPayloadEnvelope,
    source_dependencies: Box<[ProductSourceId]>,
    dependencies: Box<[ProductNodeId]>,
}

impl ProductNode {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node_id(&self) -> ProductNodeId {
        self.node_id
    }

    /// Return the exact logical product role.
    #[must_use]
    pub const fn role(&self) -> ProductRole {
        self.role
    }

    /// Return the CASA-compatible suffix, or `None` for metadata and collections.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Return the exact WCS and storage-axis binding.
    #[must_use]
    pub const fn axes(&self) -> &ProductAxes {
        &self.axes
    }

    /// Return the required physical unit.
    #[must_use]
    pub const fn unit(&self) -> ProductUnit {
        self.unit
    }

    /// Return normalization semantics, if numeric normalization applies.
    #[must_use]
    pub const fn normalization(&self) -> Option<ProductNormalization> {
        self.normalization
    }

    /// Return fitted, restoring, inherited, or absent beam semantics.
    #[must_use]
    pub const fn beam(&self) -> ProductBeamRule {
        self.beam
    }

    /// Return the output-validity rule.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityRule {
        self.validity
    }

    /// Return the existing interoperable output schema.
    #[must_use]
    pub const fn schema(&self) -> ProductSchema {
        self.schema
    }

    /// Return the logical payload envelope without physical writer-layout claims.
    #[must_use]
    pub const fn payload(&self) -> ProductPayloadEnvelope {
        self.payload
    }

    /// Return exact upstream generation dependencies.
    #[must_use]
    pub const fn source_dependencies(&self) -> &[ProductSourceId] {
        &self.source_dependencies
    }

    /// Return graph-node dependencies, all of which precede this node.
    #[must_use]
    pub const fn dependencies(&self) -> &[ProductNodeId] {
        &self.dependencies
    }
}

/// Transaction boundary that activates a complete product generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductPublicationJoin {
    /// Join product activation with the observation transaction's optional model-column commit.
    ObservationTransaction,
}

/// One atomic publication set for all logical products in a graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublication {
    join: ProductPublicationJoin,
    members: Box<[ProductNodeId]>,
}

impl ProductPublication {
    /// Return the cross-artifact commit boundary.
    #[must_use]
    pub const fn join(&self) -> ProductPublicationJoin {
        self.join
    }

    /// Return every product that must activate together.
    #[must_use]
    pub const fn members(&self) -> &[ProductNodeId] {
        &self.members
    }
}

/// Complete compiler-owned product DAG for one immutable imaging problem.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductGraph {
    graph_id: ProductGraphId,
    expected_weighting_generation: WeightingGenerationId,
    normalization_boundary: ProductNormalizationBoundary,
    sources: Box<[ProductSource]>,
    nodes: Box<[ProductNode]>,
    publication: ProductPublication,
}

impl ProductGraph {
    /// Return the stable product-topology identity.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the product-graph schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        ProductGraphId::SCHEMA_VERSION
    }

    /// Return T14's typed handoff from unnormalized normal state.
    #[must_use]
    pub const fn normalization_boundary(&self) -> &ProductNormalizationBoundary {
        &self.normalization_boundary
    }

    /// Return every exact source-generation slot in canonical order.
    #[must_use]
    pub const fn sources(&self) -> &[ProductSource] {
        &self.sources
    }

    /// Return every node in topological and publication order.
    #[must_use]
    pub const fn nodes(&self) -> &[ProductNode] {
        &self.nodes
    }

    /// Return the sole atomic publication set.
    #[must_use]
    pub const fn publication(&self) -> &ProductPublication {
        &self.publication
    }

    /// Bind one graph-local source slot to an exact upstream generation.
    pub fn bind_source_generation(
        &self,
        source: ProductSourceId,
        generation: ProductSourceGenerationId,
    ) -> Result<ProductSourceBinding, ProductGenerationError> {
        if self
            .sources
            .get(source.ordinal())
            .map(ProductSource::source_id)
            != Some(source)
        {
            return Err(ProductGenerationError::UnexpectedSource { source_id: source });
        }
        Ok(ProductSourceBinding {
            graph_id: self.graph_id,
            source,
            generation,
        })
    }

    /// Bind every required source exactly once into one immutable product generation.
    pub fn bind_generation(
        &self,
        mut bindings: Vec<ProductSourceBinding>,
    ) -> Result<ProductGeneration, ProductGenerationError> {
        for binding in &bindings {
            if binding.graph_id != self.graph_id {
                return Err(ProductGenerationError::StaleSourceBinding {
                    source_id: binding.source,
                    expected: self.graph_id,
                    actual: binding.graph_id,
                });
            }
            if self
                .sources
                .get(binding.source.ordinal())
                .map(ProductSource::source_id)
                != Some(binding.source)
            {
                return Err(ProductGenerationError::UnexpectedSource {
                    source_id: binding.source,
                });
            }
        }
        bindings.sort_unstable_by_key(ProductSourceBinding::source_id);
        if let Some(duplicate) = bindings
            .windows(2)
            .find(|pair| pair[0].source == pair[1].source)
        {
            return Err(ProductGenerationError::DuplicateSource {
                source_id: duplicate[0].source,
            });
        }
        for source in &self.sources {
            if bindings
                .binary_search_by_key(&source.source_id, ProductSourceBinding::source_id)
                .is_err()
            {
                return Err(ProductGenerationError::MissingSource {
                    source_id: source.source_id,
                });
            }
        }
        let generation_id = generation_id(self.graph_id, &bindings);
        Ok(self.generation_from_canonical_bindings(bindings, generation_id))
    }

    fn generation_from_canonical_bindings(
        &self,
        bindings: Vec<ProductSourceBinding>,
        generation_id: ProductGenerationId,
    ) -> ProductGeneration {
        let publication_members = self
            .publication
            .members
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let artifacts = self
            .nodes
            .iter()
            .map(|node| {
                publication_members
                    .contains(&node.node_id)
                    .then(|| artifact_id(generation_id, node.node_id))
            })
            .collect();
        ProductGeneration {
            generation_id,
            graph_id: self.graph_id,
            bindings: bindings.into_boxed_slice(),
            artifacts,
        }
    }
}

macro_rules! digest_identity {
    ($name:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(LogicalIdentity);

        impl $name {
            /// Construct an identity from an already computed SHA-256 digest.
            #[must_use]
            pub const fn from_sha256(digest: [u8; 32]) -> Self {
                Self(LogicalIdentity::from_sha256(digest))
            }

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0.as_bytes()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.as_bytes())?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.as_bytes())
            }
        }
    };
}

digest_identity!(
    ProductSourceGenerationId,
    "Stable identity supplied by the owner of one exact product source generation."
);
digest_identity!(
    ProductGenerationId,
    "Stable identity of a product graph bound to all exact source generations."
);
digest_identity!(
    ProductArtifactId,
    "Stable identity of one publishable product artifact in an exact product generation."
);

/// One graph-stamped source-generation binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductSourceBinding {
    graph_id: ProductGraphId,
    source: ProductSourceId,
    generation: ProductSourceGenerationId,
}

impl ProductSourceBinding {
    /// Return the product graph that created this binding.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the graph-local source slot.
    #[must_use]
    pub const fn source_id(&self) -> ProductSourceId {
        self.source
    }

    /// Return the exact upstream generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> ProductSourceGenerationId {
        self.generation
    }
}

/// One product graph bound to every authoritative source generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductGeneration {
    generation_id: ProductGenerationId,
    graph_id: ProductGraphId,
    bindings: Box<[ProductSourceBinding]>,
    artifacts: Box<[Option<ProductArtifactId>]>,
}

impl ProductGeneration {
    /// Return the stable identity of this exact generation.
    #[must_use]
    pub const fn generation_id(&self) -> ProductGenerationId {
        self.generation_id
    }

    /// Return the product graph compiled for this generation.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return canonical exact source bindings.
    #[must_use]
    pub const fn source_bindings(&self) -> &[ProductSourceBinding] {
        &self.bindings
    }

    /// Return the stable artifact identity for one publishable graph node.
    #[must_use]
    pub fn artifact_id(&self, node: ProductNodeId) -> Option<ProductArtifactId> {
        self.artifacts.get(node.ordinal()).copied().flatten()
    }
}

/// One typed owner commitment accepted by product-generation planning.
#[derive(Debug, Clone, PartialEq)]
pub enum ProductSourceCommitment {
    /// The real complete-selection weighting owner contract.
    Weighting(WeightingOperatorContract),
}

/// One typed owner completion accepted by final product-generation authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductSourceCompletionEvidence {
    /// Snapshot-bound actual completion facts for the weighting generation.
    Weighting(WeightingGenerationCompletionEvidence),
}

/// An immutable product generation planned from exact owner commitments.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedProductGeneration {
    generation: ProductGeneration,
    expected_weighting_generation: WeightingGenerationId,
    commitments: Box<[ProductSourceCommitment]>,
}

impl PlannedProductGeneration {
    /// Return the identity derived from the graph and every committed source generation.
    #[must_use]
    pub const fn generation_id(&self) -> ProductGenerationId {
        self.generation.generation_id
    }

    /// Return the product graph whose source commitments were planned.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.generation.graph_id
    }

    /// Return canonical immutable source bindings established during planning.
    #[must_use]
    pub const fn source_bindings(&self) -> &[ProductSourceBinding] {
        &self.generation.bindings
    }

    /// Return owner commitments in canonical product-source-role order.
    #[must_use]
    pub const fn commitments(&self) -> &[ProductSourceCommitment] {
        &self.commitments
    }

    /// Return the planned artifact identity for one physical publication member.
    #[must_use]
    pub fn artifact_id(&self, node: ProductNodeId) -> Option<ProductArtifactId> {
        self.generation.artifact_id(node)
    }
}

/// Final authorization that every planned source generation supplied matching completion evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductGenerationSeal {
    generation: ProductGeneration,
    commitments: Box<[ProductSourceCommitment]>,
    completions: Box<[ProductSourceCompletionEvidence]>,
}

impl ProductGenerationSeal {
    /// Return the exact planned generation authorized by this seal.
    #[must_use]
    pub const fn generation_id(&self) -> ProductGenerationId {
        self.generation.generation_id
    }

    /// Return the product graph whose exact generation was authorized.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.generation.graph_id
    }

    /// Return canonical immutable source bindings authorized by this seal.
    #[must_use]
    pub const fn source_bindings(&self) -> &[ProductSourceBinding] {
        &self.generation.bindings
    }

    /// Return the exact owner commitments authorized by this seal.
    #[must_use]
    pub const fn commitments(&self) -> &[ProductSourceCommitment] {
        &self.commitments
    }

    /// Return canonical owner completion evidence authorized by this seal.
    #[must_use]
    pub const fn completions(&self) -> &[ProductSourceCompletionEvidence] {
        &self.completions
    }

    /// Return the authorized artifact identity for one physical publication member.
    #[must_use]
    pub fn artifact_id(&self, node: ProductNodeId) -> Option<ProductArtifactId> {
        self.generation.artifact_id(node)
    }
}

/// Versioned two-phase authority that plans commitments and authorizes completion evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductGenerationAuthority;

impl ProductGenerationAuthority {
    /// Schema version of the closed commitment and completion authority catalog.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Plan one immutable product generation from exact owner commitments.
    pub fn plan(
        graph: &ProductGraph,
        commitments: Vec<ProductSourceCommitment>,
    ) -> Result<PlannedProductGeneration, ProductGenerationAuthorityError> {
        let weighting = match commitments.as_slice() {
            [] => {
                return Err(ProductGenerationAuthorityError::MissingCommitment {
                    role: ProductSourceRole::WeightingGeneration,
                });
            }
            [ProductSourceCommitment::Weighting(weighting)] => weighting,
            [_, _, ..] => {
                return Err(ProductGenerationAuthorityError::DuplicateCommitment {
                    role: ProductSourceRole::WeightingGeneration,
                });
            }
        };
        if weighting.generation_id() != graph.expected_weighting_generation {
            return Err(ProductGenerationAuthorityError::StaleCommitment {
                role: ProductSourceRole::WeightingGeneration,
                expected: graph.expected_weighting_generation,
                actual: weighting.generation_id(),
            });
        }
        if let Some(source) = graph
            .sources
            .iter()
            .find(|source| source.role != ProductSourceRole::WeightingGeneration)
        {
            return Err(ProductGenerationAuthorityError::MissingCommitment { role: source.role });
        }
        let generation = ProductSourceGenerationId(LogicalIdentity::from_sha256(
            weighting.generation_id().as_bytes(),
        ));
        let bindings = graph
            .sources
            .iter()
            .map(|source| ProductSourceBinding {
                graph_id: graph.graph_id,
                source: source.source_id,
                generation,
            })
            .collect::<Vec<_>>();
        let generation_id = generation_id(graph.graph_id, &bindings);
        Ok(PlannedProductGeneration {
            generation: graph.generation_from_canonical_bindings(bindings, generation_id),
            expected_weighting_generation: graph.expected_weighting_generation,
            commitments: commitments.into_boxed_slice(),
        })
    }

    /// Authorize one planned generation from exact owner completion evidence.
    pub fn authorize(
        planned: &PlannedProductGeneration,
        completions: Vec<ProductSourceCompletionEvidence>,
    ) -> Result<ProductGenerationSeal, ProductGenerationAuthorityError> {
        let completion = match completions.as_slice() {
            [] => {
                return Err(ProductGenerationAuthorityError::MissingCompletion {
                    role: ProductSourceRole::WeightingGeneration,
                });
            }
            [ProductSourceCompletionEvidence::Weighting(completion)] => completion,
            [_, _, ..] => {
                return Err(ProductGenerationAuthorityError::DuplicateCompletion {
                    role: ProductSourceRole::WeightingGeneration,
                });
            }
        };
        if completion.generation_id() != planned.expected_weighting_generation {
            return Err(ProductGenerationAuthorityError::StaleCompletion {
                role: ProductSourceRole::WeightingGeneration,
                expected: planned.expected_weighting_generation,
                actual: completion.generation_id(),
            });
        }
        Ok(ProductGenerationSeal {
            generation: planned.generation.clone(),
            commitments: planned.commitments.clone(),
            completions: completions.into_boxed_slice(),
        })
    }
}

/// Exact reason product-generation planning or completion authorization failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProductGenerationAuthorityError {
    /// No owner commitment was supplied for a required source role.
    #[error("missing product source commitment for {role:?}")]
    MissingCommitment {
        /// Required generation role.
        role: ProductSourceRole,
    },
    /// More than one owner commitment was supplied for a single source role.
    #[error("duplicate product source commitment for {role:?}")]
    DuplicateCommitment {
        /// Duplicated generation role.
        role: ProductSourceRole,
    },
    /// A real owner commitment does not match the generation compiled into the graph.
    #[error("stale product source commitment for {role:?}: expected {expected}, actual {actual}")]
    StaleCommitment {
        /// Stale generation role.
        role: ProductSourceRole,
        /// Generation frozen into the compiled product graph.
        expected: WeightingGenerationId,
        /// Generation supplied by the owner commitment.
        actual: WeightingGenerationId,
    },
    /// No completion evidence was supplied for a planned source role.
    #[error("missing product source completion for {role:?}")]
    MissingCompletion {
        /// Required generation role.
        role: ProductSourceRole,
    },
    /// More than one completion record was supplied for a single source role.
    #[error("duplicate product source completion for {role:?}")]
    DuplicateCompletion {
        /// Duplicated generation role.
        role: ProductSourceRole,
    },
    /// Completion evidence belongs to a generation other than the planned commitment.
    #[error("stale product source completion for {role:?}: expected {expected}, actual {actual}")]
    StaleCompletion {
        /// Stale generation role.
        role: ProductSourceRole,
        /// Generation frozen into the planned product generation.
        expected: WeightingGenerationId,
        /// Generation carried by completion evidence.
        actual: WeightingGenerationId,
    },
}

/// Exact reason source generations could not bind to a product graph.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProductGenerationError {
    /// A binding names no source slot in this graph.
    #[error("unexpected product source {source_id:?}")]
    UnexpectedSource {
        /// Unknown graph-local source identity.
        source_id: ProductSourceId,
    },
    /// One required source slot has no exact generation binding.
    #[error("missing product source {source_id:?}")]
    MissingSource {
        /// Missing graph-local source identity.
        source_id: ProductSourceId,
    },
    /// One source slot was bound more than once.
    #[error("duplicate product source {source_id:?}")]
    DuplicateSource {
        /// Duplicate graph-local source identity.
        source_id: ProductSourceId,
    },
    /// A source binding was created for a different product graph.
    #[error(
        "source {source_id:?} belongs to product graph {actual}, expected product graph {expected}"
    )]
    StaleSourceBinding {
        /// Graph-local source identity carried by the stale binding.
        source_id: ProductSourceId,
        /// Product graph being bound.
        expected: ProductGraphId,
        /// Product graph that created the binding.
        actual: ProductGraphId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SourceKey {
    role: ProductSourceRole,
    domain: usize,
    term: ProductTerm,
}

struct ImageProjection {
    role: ProductRole,
    name: String,
    axis_kind: ProductAxisKind,
    unit: ProductUnit,
    normalization: Option<ProductNormalization>,
    beam: ProductBeamRule,
    validity: ProductValidityRule,
}

struct MetadataProjection {
    role: ProductRole,
    schema: ProductSchema,
    beam: ProductBeamRule,
}

struct GraphBuilder<'a> {
    problem_id: CompiledProblemId,
    expected_weighting_generation: WeightingGenerationId,
    geometry: &'a CompiledGeometry,
    reconstruction: &'a ReconstructionContract,
    products: &'a ProductRequirements,
    sources: Vec<ProductSource>,
    source_ids: BTreeMap<SourceKey, ProductSourceId>,
    nodes: Vec<ProductNode>,
    node_ids: BTreeMap<(usize, ProductRole), ProductNodeId>,
}

impl<'a> GraphBuilder<'a> {
    fn new(
        problem_id: CompiledProblemId,
        expected_weighting_generation: WeightingGenerationId,
        geometry: &'a CompiledGeometry,
        reconstruction: &'a ReconstructionContract,
        products: &'a ProductRequirements,
    ) -> Self {
        Self {
            problem_id,
            expected_weighting_generation,
            geometry,
            reconstruction,
            products,
            sources: Vec::new(),
            source_ids: BTreeMap::new(),
            nodes: Vec::new(),
            node_ids: BTreeMap::new(),
        }
    }

    fn compile(mut self) -> Result<ProductGraph, &'static str> {
        let domains = self.geometry.domains().to_vec();
        let products = self.products.products().to_vec();
        for (domain_index, domain) in domains.iter().enumerate() {
            for product in &products {
                self.compile_product(domain_index, domain, *product);
            }
        }
        for node in &mut self.nodes {
            node.payload = product_payload_envelope(node)
                .ok_or("product logical payload exceeds the supported 64-bit identity domain")?;
        }
        let publication = ProductPublication {
            join: ProductPublicationJoin::ObservationTransaction,
            members: self
                .nodes
                .iter()
                .filter(|node| matches!(node.schema, ProductSchema::CasaPagedImageF32))
                .map(|node| node.node_id)
                .collect(),
        };
        let normalization_boundary = self.products.normalization_boundary().clone();
        let graph_id = canonical_graph_id(
            self.problem_id,
            &normalization_boundary,
            &self.sources,
            &self.nodes,
            &publication,
        );
        Ok(ProductGraph {
            graph_id,
            expected_weighting_generation: self.expected_weighting_generation,
            normalization_boundary,
            sources: self.sources.into_boxed_slice(),
            nodes: self.nodes.into_boxed_slice(),
            publication,
        })
    }

    fn compile_product(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        product: ProductKind,
    ) {
        match product {
            ProductKind::Psf => self.compile_psf(domain_index, domain),
            ProductKind::Residual => self.compile_residual(domain_index, domain),
            ProductKind::Model => self.compile_model(domain_index, domain),
            ProductKind::RestoredImage => self.compile_restored(domain_index, domain),
            ProductKind::SumWeights => self.compile_sum_weights(domain_index, domain),
            ProductKind::Mask => self.compile_mask(domain_index, domain),
            ProductKind::Weight => self.compile_weight(domain_index, domain),
            ProductKind::PrimaryBeam => self.compile_primary_beam(domain_index, domain),
            ProductKind::Sensitivity => self.compile_sensitivity(domain_index, domain),
            ProductKind::PbCorrectedImage => self.compile_pb_corrected(domain_index, domain),
            ProductKind::TaylorTerms => self.compile_taylor_set(domain_index, domain),
            ProductKind::SpectralIndex => self.compile_spectral_index(domain_index, domain),
            ProductKind::SpectralIndexError => {
                self.compile_spectral_index_error(domain_index, domain)
            }
            ProductKind::PbCorrectedSpectralIndex => {
                self.compile_pb_corrected_spectral_index(domain_index, domain)
            }
            ProductKind::Beam => self.compile_beam_metadata(domain_index, domain),
        }
    }

    fn compile_psf(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        for term in self.convolution_terms() {
            let source = self.source(
                domain_index,
                domain,
                ProductSourceRole::FinalNormalState,
                term,
            );
            let beam = if is_zeroth(term) {
                ProductBeamRule::Fitted
            } else {
                ProductBeamRule::None
            };
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::Psf(term),
                    name: product_name("psf", term, false),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::JyPerBeam,
                    normalization: Some(ProductNormalization::UnitResponse),
                    beam,
                    validity: ProductValidityRule::All,
                },
                [source],
                [],
            );
        }
    }

    fn compile_residual(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        for term in self.image_terms() {
            let source = self.source(
                domain_index,
                domain,
                ProductSourceRole::FinalNormalState,
                term,
            );
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::Residual(term),
                    name: product_name("residual", term, false),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::JyPerBeam,
                    normalization: Some(self.products.normalization()),
                    beam: ProductBeamRule::Fitted,
                    validity: ProductValidityRule::FinalNormalState,
                },
                [source],
                [],
            );
        }
    }

    fn compile_model(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        for term in self.image_terms() {
            let source = self.source(domain_index, domain, ProductSourceRole::FinalModel, term);
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::Model(term),
                    name: product_name("model", term, false),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::JyPerPixel,
                    normalization: None,
                    beam: ProductBeamRule::None,
                    validity: ProductValidityRule::All,
                },
                [source],
                [],
            );
        }
    }

    fn compile_restored(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        for term in self.image_terms() {
            let normal = self.source(
                domain_index,
                domain,
                ProductSourceRole::FinalNormalState,
                term,
            );
            let model = self.source(domain_index, domain, ProductSourceRole::FinalModel, term);
            let beam = self.source(
                domain_index,
                domain,
                ProductSourceRole::RestoringBeamGeneration,
                ProductTerm::Single,
            );
            let dependencies = self.nodes_for(
                domain_index,
                [ProductRole::Residual(term), ProductRole::Model(term)],
            );
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::RestoredImage(term),
                    name: product_name("image", term, false),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::JyPerBeam,
                    normalization: Some(self.products.normalization()),
                    beam: ProductBeamRule::Restoring(self.products.restoring_beam()),
                    validity: ProductValidityRule::FinalNormalState,
                },
                [normal, model, beam],
                dependencies,
            );
        }
    }

    fn compile_sum_weights(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        for term in self.convolution_terms() {
            let source = self.source(
                domain_index,
                domain,
                ProductSourceRole::WeightingGeneration,
                term,
            );
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::SumWeights(term),
                    name: product_name("sumwt", term, false),
                    axis_kind: ProductAxisKind::PlaneState,
                    unit: ProductUnit::VisibilityWeight,
                    normalization: None,
                    beam: ProductBeamRule::None,
                    validity: ProductValidityRule::All,
                },
                [source],
                [],
            );
        }
    }

    fn compile_mask(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let source = self.source(
            domain_index,
            domain,
            ProductSourceRole::CleanMaskGeneration,
            ProductTerm::Single,
        );
        self.add_image(
            domain_index,
            domain,
            ImageProjection {
                role: ProductRole::CleanMask,
                name: ".mask".to_string(),
                axis_kind: ProductAxisKind::SkyImage,
                unit: ProductUnit::Dimensionless,
                normalization: None,
                beam: ProductBeamRule::None,
                validity: ProductValidityRule::All,
            },
            [source],
            [],
        );
    }

    fn compile_weight(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        for term in self.convolution_terms() {
            let source = self.source(
                domain_index,
                domain,
                ProductSourceRole::WeightingGeneration,
                term,
            );
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::Weight(term),
                    name: product_name("weight", term, false),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::Dimensionless,
                    normalization: None,
                    beam: ProductBeamRule::None,
                    validity: ProductValidityRule::All,
                },
                [source],
                [],
            );
        }
    }

    fn compile_primary_beam(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let term = match self.reconstruction.basis() {
            ReconstructionBasis::Taylor { .. } => ProductTerm::Taylor(0),
            ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => {
                ProductTerm::Single
            }
        };
        let source = self.source(
            domain_index,
            domain,
            ProductSourceRole::PrimaryBeamGeneration,
            term,
        );
        let primary_beam = self.add_image(
            domain_index,
            domain,
            ImageProjection {
                role: ProductRole::PrimaryBeam(term),
                name: product_name("pb", term, false),
                axis_kind: ProductAxisKind::SkyImage,
                unit: ProductUnit::Dimensionless,
                normalization: None,
                beam: ProductBeamRule::None,
                validity: ProductValidityRule::All,
            },
            [source],
            [],
        );
        if matches!(
            self.reconstruction.basis(),
            ReconstructionBasis::Taylor { .. }
        ) && self
            .products
            .contains(ProductKind::PbCorrectedSpectralIndex)
        {
            let alpha_source = self.source(
                domain_index,
                domain,
                ProductSourceRole::PrimaryBeamSpectralIndexGeneration,
                ProductTerm::Single,
            );
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::PrimaryBeamSpectralIndex,
                    name: ".pb.alpha".to_string(),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::Dimensionless,
                    normalization: None,
                    beam: ProductBeamRule::None,
                    validity: ProductValidityRule::PrimaryBeam(
                        self.products.validity().primary_beam(),
                    ),
                },
                [alpha_source],
                [primary_beam],
            );
        }
    }

    fn compile_sensitivity(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let source = self.source(
            domain_index,
            domain,
            ProductSourceRole::SensitivityGeneration,
            ProductTerm::Single,
        );
        self.add_image(
            domain_index,
            domain,
            ImageProjection {
                role: ProductRole::Sensitivity,
                name: ".sensitivity".to_string(),
                axis_kind: ProductAxisKind::SkyImage,
                unit: ProductUnit::Dimensionless,
                normalization: None,
                beam: ProductBeamRule::None,
                validity: ProductValidityRule::All,
            },
            [source],
            [],
        );
    }

    fn compile_pb_corrected(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let primary_beam = self.node_id(
            domain_index,
            ProductRole::PrimaryBeam(match self.reconstruction.basis() {
                ReconstructionBasis::Taylor { .. } => ProductTerm::Taylor(0),
                ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => {
                    ProductTerm::Single
                }
            }),
        );
        for term in self.image_terms() {
            let restored = self.node_id(domain_index, ProductRole::RestoredImage(term));
            self.add_image(
                domain_index,
                domain,
                ImageProjection {
                    role: ProductRole::PbCorrectedImage(term),
                    name: product_name("image", term, true),
                    axis_kind: ProductAxisKind::SkyImage,
                    unit: ProductUnit::JyPerBeam,
                    normalization: Some(self.products.normalization()),
                    beam: ProductBeamRule::Inherit(restored),
                    validity: ProductValidityRule::PrimaryBeam(
                        self.products.validity().primary_beam(),
                    ),
                },
                [],
                [restored, primary_beam],
            );
        }
    }

    fn compile_taylor_set(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let dependencies = self
            .nodes
            .iter()
            .filter(|node| {
                node.axes.domain == *domain.role()
                    && matches!(
                        node.role,
                        ProductRole::Psf(ProductTerm::Taylor(_))
                            | ProductRole::Residual(ProductTerm::Taylor(_))
                            | ProductRole::Model(ProductTerm::Taylor(_))
                            | ProductRole::RestoredImage(ProductTerm::Taylor(_))
                            | ProductRole::SumWeights(ProductTerm::Taylor(_))
                            | ProductRole::Weight(ProductTerm::Taylor(_))
                            | ProductRole::PrimaryBeam(ProductTerm::Taylor(_))
                            | ProductRole::PrimaryBeamSpectralIndex
                            | ProductRole::PbCorrectedImage(ProductTerm::Taylor(_))
                    )
            })
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        self.add_metadata(
            domain_index,
            domain,
            MetadataProjection {
                role: ProductRole::TaylorCoefficientSet,
                schema: ProductSchema::LogicalCollection,
                beam: ProductBeamRule::None,
            },
            [],
            dependencies,
        );
    }

    fn compile_spectral_index(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let terms = [ProductTerm::Taylor(0), ProductTerm::Taylor(1)];
        let sources = terms
            .map(|term| self.source(domain_index, domain, ProductSourceRole::FinalModel, term));
        let dependencies = self.preferred_taylor_dependencies(domain_index, terms);
        let beam = self.derived_beam(domain_index);
        self.add_image(
            domain_index,
            domain,
            ImageProjection {
                role: ProductRole::SpectralIndex,
                name: ".alpha".to_string(),
                axis_kind: ProductAxisKind::SkyImage,
                unit: ProductUnit::Dimensionless,
                normalization: None,
                beam,
                validity: ProductValidityRule::Taylor(self.products.validity().taylor()),
            },
            sources,
            dependencies,
        );
    }

    fn compile_spectral_index_error(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let alpha = self.node_id(domain_index, ProductRole::SpectralIndex);
        let normal = self.source(
            domain_index,
            domain,
            ProductSourceRole::FinalNormalState,
            ProductTerm::Taylor(0),
        );
        let beam = self.derived_beam(domain_index);
        self.add_image(
            domain_index,
            domain,
            ImageProjection {
                role: ProductRole::SpectralIndexError,
                name: ".alpha.error".to_string(),
                axis_kind: ProductAxisKind::SkyImage,
                unit: ProductUnit::Dimensionless,
                normalization: None,
                beam,
                validity: ProductValidityRule::Taylor(self.products.validity().taylor()),
            },
            [normal],
            [alpha],
        );
    }

    fn compile_pb_corrected_spectral_index(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
    ) {
        let alpha = self.node_id(domain_index, ProductRole::SpectralIndex);
        let primary_beam = self.node_id(
            domain_index,
            ProductRole::PrimaryBeam(ProductTerm::Taylor(0)),
        );
        let primary_beam_alpha = self.node_id(domain_index, ProductRole::PrimaryBeamSpectralIndex);
        self.add_image(
            domain_index,
            domain,
            ImageProjection {
                role: ProductRole::PbCorrectedSpectralIndex,
                name: ".alpha.pbcor".to_string(),
                axis_kind: ProductAxisKind::SkyImage,
                unit: ProductUnit::Dimensionless,
                normalization: None,
                beam: ProductBeamRule::Inherit(alpha),
                validity: ProductValidityRule::TaylorAndPrimaryBeam {
                    taylor: self.products.validity().taylor(),
                    primary_beam: self.products.validity().primary_beam(),
                },
            },
            [],
            [alpha, primary_beam, primary_beam_alpha],
        );
    }

    fn compile_beam_metadata(&mut self, domain_index: usize, domain: &CompiledImageDomain) {
        let source = self.source(
            domain_index,
            domain,
            ProductSourceRole::RestoringBeamGeneration,
            ProductTerm::Single,
        );
        let dependencies = self
            .nodes
            .iter()
            .filter(|node| {
                node.axes.domain == *domain.role() && !matches!(node.beam, ProductBeamRule::None)
            })
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        self.add_metadata(
            domain_index,
            domain,
            MetadataProjection {
                role: ProductRole::BeamMetadata,
                schema: ProductSchema::CasaImageMetadata,
                beam: ProductBeamRule::Metadata(self.products.restoring_beam()),
            },
            [source],
            dependencies,
        );
    }

    fn add_image(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        projection: ImageProjection,
        sources: impl IntoIterator<Item = ProductSourceId>,
        dependencies: impl IntoIterator<Item = ProductNodeId>,
    ) -> ProductNodeId {
        self.add_node(
            domain_index,
            ProductNode {
                node_id: ProductNodeId(0),
                role: projection.role,
                name: Some(projection.name),
                axes: product_axes(
                    self.geometry,
                    domain,
                    self.reconstruction,
                    projection.axis_kind,
                ),
                unit: projection.unit,
                normalization: projection.normalization,
                beam: projection.beam,
                validity: projection.validity,
                schema: ProductSchema::CasaPagedImageF32,
                payload: empty_payload_envelope(),
                source_dependencies: canonical_ids(sources),
                dependencies: canonical_ids(dependencies),
            },
        )
    }

    fn add_metadata(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        projection: MetadataProjection,
        sources: impl IntoIterator<Item = ProductSourceId>,
        dependencies: impl IntoIterator<Item = ProductNodeId>,
    ) -> ProductNodeId {
        self.add_node(
            domain_index,
            ProductNode {
                node_id: ProductNodeId(0),
                role: projection.role,
                name: None,
                axes: product_axes(
                    self.geometry,
                    domain,
                    self.reconstruction,
                    ProductAxisKind::Metadata,
                ),
                unit: ProductUnit::NotApplicable,
                normalization: None,
                beam: projection.beam,
                validity: ProductValidityRule::All,
                schema: projection.schema,
                payload: empty_payload_envelope(),
                source_dependencies: canonical_ids(sources),
                dependencies: canonical_ids(dependencies),
            },
        )
    }

    fn add_node(&mut self, domain_index: usize, mut node: ProductNode) -> ProductNodeId {
        let node_id = ProductNodeId(self.nodes.len());
        node.node_id = node_id;
        debug_assert!(
            node.dependencies
                .iter()
                .all(|dependency| dependency.0 < node_id.0)
        );
        let previous = self.node_ids.insert((domain_index, node.role), node_id);
        debug_assert!(previous.is_none());
        self.nodes.push(node);
        node_id
    }

    fn source(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        role: ProductSourceRole,
        term: ProductTerm,
    ) -> ProductSourceId {
        let key = SourceKey {
            role,
            domain: domain_index,
            term,
        };
        if let Some(source_id) = self.source_ids.get(&key) {
            return *source_id;
        }
        let source_id = ProductSourceId(self.sources.len());
        self.sources.push(ProductSource {
            source_id,
            role,
            domain: domain.role().clone(),
            term,
        });
        self.source_ids.insert(key, source_id);
        source_id
    }

    fn node_id(&self, domain: usize, role: ProductRole) -> ProductNodeId {
        self.node_ids[&(domain, role)]
    }

    fn nodes_for<const N: usize>(
        &self,
        domain: usize,
        roles: [ProductRole; N],
    ) -> Vec<ProductNodeId> {
        roles
            .into_iter()
            .filter_map(|role| self.node_ids.get(&(domain, role)).copied())
            .collect()
    }

    fn preferred_taylor_dependencies(
        &self,
        domain: usize,
        terms: [ProductTerm; 2],
    ) -> Vec<ProductNodeId> {
        let restored = self.nodes_for(domain, terms.map(ProductRole::RestoredImage));
        if restored.len() == terms.len() {
            restored
        } else {
            self.nodes_for(domain, terms.map(ProductRole::Model))
        }
    }

    fn derived_beam(&self, domain: usize) -> ProductBeamRule {
        self.node_ids
            .get(&(domain, ProductRole::RestoredImage(ProductTerm::Taylor(0))))
            .copied()
            .map_or(
                ProductBeamRule::Restoring(self.products.restoring_beam()),
                ProductBeamRule::Inherit,
            )
    }

    fn image_terms(&self) -> Vec<ProductTerm> {
        match self.reconstruction.basis() {
            ReconstructionBasis::Taylor { terms } => (0..terms).map(ProductTerm::Taylor).collect(),
            ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => {
                vec![ProductTerm::Single]
            }
        }
    }

    fn convolution_terms(&self) -> Vec<ProductTerm> {
        match self.reconstruction.basis() {
            ReconstructionBasis::Taylor { terms } => (0..terms.saturating_mul(2).saturating_sub(1))
                .map(ProductTerm::Taylor)
                .collect(),
            ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => {
                vec![ProductTerm::Single]
            }
        }
    }
}

pub(crate) fn compile_product_graph(
    problem_id: CompiledProblemId,
    expected_weighting_generation: WeightingGenerationId,
    geometry: &CompiledGeometry,
    reconstruction: &ReconstructionContract,
    products: &ProductRequirements,
) -> Result<ProductGraph, &'static str> {
    GraphBuilder::new(
        problem_id,
        expected_weighting_generation,
        geometry,
        reconstruction,
        products,
    )
    .compile()
}

fn canonical_graph_id(
    problem_id: CompiledProblemId,
    normalization_boundary: &ProductNormalizationBoundary,
    sources: &[ProductSource],
    nodes: &[ProductNode],
    publication: &ProductPublication,
) -> ProductGraphId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(PRODUCT_GRAPH_IDENTITY_DOMAIN);
    encoder.u32(PRODUCT_GRAPH_IDENTITY_VERSION);
    encoder.digest(problem_id.as_bytes());
    encoder.u8(match normalization_boundary.input() {
        crate::NormalStateNormalization::Unnormalized => 0,
    });
    encoder.usize(normalization_boundary.operations().len());
    for operation in normalization_boundary.operations() {
        match operation {
            ProductBoundaryOperation::Normalize(normalization) => {
                encoder.u8(0);
                encoder.u8(normalization_tag(*normalization));
            }
            ProductBoundaryOperation::ScaleResidual => encoder.u8(1),
            ProductBoundaryOperation::Restore(policy) => {
                encoder.u8(2);
                encoder.u8(beam_policy_tag(*policy));
            }
            ProductBoundaryOperation::CorrectPrimaryBeam => encoder.u8(3),
            ProductBoundaryOperation::BlankInvalid => encoder.u8(4),
            ProductBoundaryOperation::ConvertUnits => encoder.u8(5),
        }
    }
    encoder.usize(sources.len());
    for source in sources {
        encoder.usize(source.source_id.ordinal());
        encoder.u8(source_role_tag(source.role));
        encode_domain(&mut encoder, &source.domain);
        encode_term(&mut encoder, source.term);
    }
    encoder.usize(nodes.len());
    for node in nodes {
        encode_node_metadata(&mut encoder, node);
        encoder.u8(match node.payload.element_representation {
            ProductElementRepresentation::NotApplicable => 0,
            ProductElementRepresentation::Float32 => 1,
        });
        encoder.u64(node.payload.logical_elements);
        encoder.u64(node.payload.logical_pixel_bytes);
        encoder.u64(node.payload.identity_metadata_bytes);
        encoder.u64(node.payload.identity_envelope_bytes);
    }
    encoder.u8(match publication.join {
        ProductPublicationJoin::ObservationTransaction => 0,
    });
    encoder.usize(publication.members.len());
    for member in &publication.members {
        encoder.usize(member.ordinal());
    }
    ProductGraphId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn generation_id(
    graph_id: ProductGraphId,
    bindings: &[ProductSourceBinding],
) -> ProductGenerationId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(PRODUCT_GENERATION_IDENTITY_DOMAIN);
    encoder.u32(PRODUCT_GENERATION_IDENTITY_VERSION);
    encoder.digest(graph_id.as_bytes());
    encoder.usize(bindings.len());
    for binding in bindings {
        encoder.usize(binding.source.ordinal());
        encoder.digest(binding.generation.as_bytes());
    }
    ProductGenerationId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn artifact_id(generation: ProductGenerationId, node: ProductNodeId) -> ProductArtifactId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(PRODUCT_ARTIFACT_IDENTITY_DOMAIN);
    encoder.u32(PRODUCT_ARTIFACT_IDENTITY_VERSION);
    encoder.digest(generation.as_bytes());
    encoder.usize(node.ordinal());
    ProductArtifactId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn empty_payload_envelope() -> ProductPayloadEnvelope {
    ProductPayloadEnvelope {
        element_representation: ProductElementRepresentation::NotApplicable,
        logical_elements: 0,
        logical_pixel_bytes: 0,
        identity_metadata_bytes: 0,
        identity_envelope_bytes: 0,
    }
}

fn product_payload_envelope(node: &ProductNode) -> Option<ProductPayloadEnvelope> {
    let (element_representation, logical_elements, logical_pixel_bytes) = match node.schema {
        ProductSchema::CasaPagedImageF32 => {
            let logical_elements = node
                .axes
                .shape
                .into_iter()
                .try_fold(1_u64, |elements, extent| {
                    elements.checked_mul(u64::try_from(extent).ok()?)
                })?;
            (
                ProductElementRepresentation::Float32,
                logical_elements,
                logical_elements.checked_mul(4)?,
            )
        }
        ProductSchema::LogicalCollection | ProductSchema::CasaImageMetadata => {
            (ProductElementRepresentation::NotApplicable, 0, 0)
        }
    };
    let mut counter = CanonicalByteCounter::default();
    encode_node_metadata(&mut counter, node);
    let identity_metadata_bytes = counter.finish()?;
    let identity_envelope_bytes = logical_pixel_bytes.checked_add(identity_metadata_bytes)?;
    Some(ProductPayloadEnvelope {
        element_representation,
        logical_elements,
        logical_pixel_bytes,
        identity_metadata_bytes,
        identity_envelope_bytes,
    })
}

trait ProductMetadataEncoder {
    fn u8(&mut self, value: u8);
    fn u32(&mut self, value: u32);
    fn usize(&mut self, value: usize);
    fn bytes(&mut self, value: &[u8]);
    fn digest(&mut self, value: [u8; 32]);
    fn f64(&mut self, value: f64);
}

impl ProductMetadataEncoder for CanonicalEncoder {
    fn u8(&mut self, value: u8) {
        CanonicalEncoder::u8(self, value);
    }

    fn u32(&mut self, value: u32) {
        CanonicalEncoder::u32(self, value);
    }

    fn usize(&mut self, value: usize) {
        CanonicalEncoder::usize(self, value);
    }

    fn bytes(&mut self, value: &[u8]) {
        CanonicalEncoder::bytes(self, value);
    }

    fn digest(&mut self, value: [u8; 32]) {
        CanonicalEncoder::digest(self, value);
    }

    fn f64(&mut self, value: f64) {
        CanonicalEncoder::f64(self, value);
    }
}

#[derive(Default)]
struct CanonicalByteCounter {
    bytes: u64,
    overflow: bool,
}

impl CanonicalByteCounter {
    fn add(&mut self, bytes: usize) {
        if self.overflow {
            return;
        }
        let Some(total) = u64::try_from(bytes)
            .ok()
            .and_then(|bytes| self.bytes.checked_add(bytes))
        else {
            self.overflow = true;
            return;
        };
        self.bytes = total;
    }

    fn finish(self) -> Option<u64> {
        (!self.overflow).then_some(self.bytes)
    }
}

impl ProductMetadataEncoder for CanonicalByteCounter {
    fn u8(&mut self, _value: u8) {
        self.add(std::mem::size_of::<u8>());
    }

    fn u32(&mut self, _value: u32) {
        self.add(std::mem::size_of::<u32>());
    }

    fn usize(&mut self, _value: usize) {
        self.add(std::mem::size_of::<u128>());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.add(value.len());
    }

    fn digest(&mut self, _value: [u8; 32]) {
        self.add(32);
    }

    fn f64(&mut self, _value: f64) {
        self.add(std::mem::size_of::<f64>());
    }
}

fn encode_node_metadata(encoder: &mut impl ProductMetadataEncoder, node: &ProductNode) {
    encoder.usize(node.node_id.ordinal());
    encode_role(encoder, node.role);
    match &node.name {
        Some(name) => {
            encoder.u8(1);
            encoder.bytes(name.as_bytes());
        }
        None => encoder.u8(0),
    }
    encode_axes(encoder, &node.axes);
    encoder.u8(unit_tag(node.unit));
    match node.normalization {
        Some(normalization) => {
            encoder.u8(1);
            encoder.u8(normalization_tag(normalization));
        }
        None => encoder.u8(0),
    }
    encode_beam(encoder, node.beam);
    encode_validity(encoder, node.validity);
    encoder.u8(schema_tag(node.schema));
    encoder.usize(node.source_dependencies.len());
    for source in &node.source_dependencies {
        encoder.usize(source.ordinal());
    }
    encoder.usize(node.dependencies.len());
    for dependency in &node.dependencies {
        encoder.usize(dependency.ordinal());
    }
}

fn encode_domain(encoder: &mut impl ProductMetadataEncoder, domain: &ImageDomainRole) {
    match domain {
        ImageDomainRole::Main => encoder.u8(0),
        ImageDomainRole::Outlier(name) => {
            encoder.u8(1);
            encoder.bytes(name.as_bytes());
        }
    }
}

fn encode_term(encoder: &mut impl ProductMetadataEncoder, term: ProductTerm) {
    match term {
        ProductTerm::Single => encoder.u8(0),
        ProductTerm::Taylor(index) => {
            encoder.u8(1);
            encoder.usize(index);
        }
    }
}

fn encode_role(encoder: &mut impl ProductMetadataEncoder, role: ProductRole) {
    let (tag, term) = match role {
        ProductRole::Psf(term) => (0, Some(term)),
        ProductRole::Residual(term) => (1, Some(term)),
        ProductRole::Model(term) => (2, Some(term)),
        ProductRole::RestoredImage(term) => (3, Some(term)),
        ProductRole::SumWeights(term) => (4, Some(term)),
        ProductRole::CleanMask => (5, None),
        ProductRole::Weight(term) => (6, Some(term)),
        ProductRole::PrimaryBeam(term) => (7, Some(term)),
        ProductRole::PrimaryBeamSpectralIndex => (8, None),
        ProductRole::Sensitivity => (9, None),
        ProductRole::PbCorrectedImage(term) => (10, Some(term)),
        ProductRole::TaylorCoefficientSet => (11, None),
        ProductRole::SpectralIndex => (12, None),
        ProductRole::SpectralIndexError => (13, None),
        ProductRole::PbCorrectedSpectralIndex => (14, None),
        ProductRole::BeamMetadata => (15, None),
    };
    encoder.u8(tag);
    if let Some(term) = term {
        encode_term(encoder, term);
    }
}

fn encode_direction(encoder: &mut impl ProductMetadataEncoder, direction: DirectionCoordinateSpec) {
    encoder.u8(match direction.projection() {
        Projection::Sin => 0,
    });
    encode_sky_direction(encoder, direction.reference_direction());
    for value in direction.reference_pixel() {
        encoder.f64(value);
    }
    for value in direction.increment_rad() {
        encoder.f64(value);
    }
    for value in direction.pc().into_iter().flatten() {
        encoder.f64(value);
    }
    for value in direction.pole_deg() {
        encoder.f64(value);
    }
}

fn encode_sky_direction(encoder: &mut impl ProductMetadataEncoder, direction: crate::SkyDirection) {
    encoder.u8(match direction.frame() {
        DirectionFrame::Icrs => 0,
        DirectionFrame::J2000 => 1,
        DirectionFrame::B1950 => 2,
        DirectionFrame::Galactic => 3,
    });
    encoder.f64(direction.longitude_rad());
    encoder.f64(direction.latitude_rad());
}

fn encode_spectral(encoder: &mut impl ProductMetadataEncoder, spectral: &SpectralCoordinateSpec) {
    encoder.u8(frequency_frame_tag(spectral.source_frame()));
    encoder.u8(frequency_frame_tag(spectral.output_frame()));
    match spectral.anchor() {
        SpectralFrameAnchor::NotApplicable => encoder.u8(0),
        SpectralFrameAnchor::Conversion {
            epoch,
            direction,
            observatory_position,
        } => {
            encoder.u8(1);
            encoder.f64(epoch.mjd_days());
            encoder.u8(match epoch.scale() {
                TimeScale::Utc => 0,
                TimeScale::Tai => 1,
                TimeScale::Tt => 2,
                TimeScale::Tdb => 3,
            });
            encode_sky_direction(encoder, direction);
            for metres in observatory_position.metres() {
                encoder.f64(metres);
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
            encoder.u8(0);
            encoder.usize(*channels);
            encoder.f64(*reference_pixel);
            encoder.f64(*reference_frequency_hz);
            encoder.f64(*increment_hz);
        }
        SpectralWcs::Tabular {
            channel_centres_hz,
            channel_boundaries_hz,
        } => {
            encoder.u8(1);
            encoder.usize(channel_centres_hz.len());
            for frequency in channel_centres_hz {
                encoder.f64(*frequency);
            }
            encoder.usize(channel_boundaries_hz.len());
            for frequency in channel_boundaries_hz {
                encoder.f64(*frequency);
            }
        }
    }
    match spectral.rest_frequency() {
        RestFrequency::NotApplicable => encoder.u8(0),
        RestFrequency::Line { hertz } => {
            encoder.u8(1);
            encoder.f64(hertz);
        }
    }
    encoder.u8(match spectral.doppler_convention() {
        DopplerConvention::NotApplicable => 0,
        DopplerConvention::Radio => 1,
        DopplerConvention::Optical => 2,
        DopplerConvention::Relativistic => 3,
    });
}

fn frequency_frame_tag(frame: FrequencyFrame) -> u8 {
    match frame {
        FrequencyFrame::Topocentric => 0,
        FrequencyFrame::Barycentric => 1,
        FrequencyFrame::Lsrk => 2,
    }
}

fn encode_axes(encoder: &mut impl ProductMetadataEncoder, axes: &ProductAxes) {
    encoder.u8(match axes.kind {
        ProductAxisKind::SkyImage => 0,
        ProductAxisKind::PlaneState => 1,
        ProductAxisKind::Metadata => 2,
    });
    // Retain both the owning geometry identity and the exact node-local WCS projection.
    encoder.digest(axes.geometry_id.as_bytes());
    encode_direction(encoder, axes.direction);
    encode_spectral(encoder, &axes.spectral);
    encode_domain(encoder, &axes.domain);
    for axis in axes.order.positions() {
        encoder.u8(match axis {
            ImageAxis::DirectionLongitude => 0,
            ImageAxis::DirectionLatitude => 1,
            ImageAxis::Polarization => 2,
            ImageAxis::Spectral => 3,
        });
    }
    for extent in axes.shape {
        encoder.usize(extent);
    }
    encoder.usize(axes.polarization.len());
    for coordinate in &axes.polarization {
        encoder.u8(polarization_tag(*coordinate));
    }
}

fn encode_beam(encoder: &mut impl ProductMetadataEncoder, beam: ProductBeamRule) {
    match beam {
        ProductBeamRule::None => encoder.u8(0),
        ProductBeamRule::Fitted => encoder.u8(1),
        ProductBeamRule::Restoring(policy) => {
            encoder.u8(2);
            encoder.u8(beam_policy_tag(policy));
        }
        ProductBeamRule::Inherit(node) => {
            encoder.u8(3);
            encoder.usize(node.ordinal());
        }
        ProductBeamRule::Metadata(policy) => {
            encoder.u8(4);
            encoder.u8(beam_policy_tag(policy));
        }
    }
}

fn source_role_tag(role: ProductSourceRole) -> u8 {
    match role {
        ProductSourceRole::FinalNormalState => 0,
        ProductSourceRole::FinalModel => 1,
        ProductSourceRole::WeightingGeneration => 2,
        ProductSourceRole::CleanMaskGeneration => 3,
        ProductSourceRole::PrimaryBeamGeneration => 4,
        ProductSourceRole::PrimaryBeamSpectralIndexGeneration => 5,
        ProductSourceRole::SensitivityGeneration => 6,
        ProductSourceRole::RestoringBeamGeneration => 7,
    }
}

fn unit_tag(unit: ProductUnit) -> u8 {
    match unit {
        ProductUnit::NotApplicable => 0,
        ProductUnit::JyPerBeam => 1,
        ProductUnit::JyPerPixel => 2,
        ProductUnit::Dimensionless => 3,
        ProductUnit::VisibilityWeight => 4,
    }
}

fn normalization_tag(normalization: ProductNormalization) -> u8 {
    match normalization {
        ProductNormalization::UnitResponse => 0,
        ProductNormalization::FlatNoise => 1,
        ProductNormalization::FlatSky => 2,
    }
}

fn beam_policy_tag(policy: RestoringBeamPolicy) -> u8 {
    match policy {
        RestoringBeamPolicy::None => 0,
        RestoringBeamPolicy::PerPlane => 1,
        RestoringBeamPolicy::Common => 2,
    }
}

fn encode_validity(encoder: &mut impl ProductMetadataEncoder, validity: ProductValidityRule) {
    match validity {
        ProductValidityRule::All => encoder.u8(0),
        ProductValidityRule::FinalNormalState => encoder.u8(1),
        ProductValidityRule::PrimaryBeam(policy) => {
            encoder.u8(2);
            encode_primary_beam_validity(encoder, policy);
        }
        ProductValidityRule::Taylor(policy) => {
            encoder.u8(3);
            encode_taylor_validity(encoder, policy);
        }
        ProductValidityRule::TaylorAndPrimaryBeam {
            taylor,
            primary_beam,
        } => {
            encoder.u8(4);
            encode_taylor_validity(encoder, taylor);
            encode_primary_beam_validity(encoder, primary_beam);
        }
    }
}

fn encode_primary_beam_validity(
    encoder: &mut impl ProductMetadataEncoder,
    policy: PrimaryBeamValidityPolicy,
) {
    encoder.u32(policy.cutoff().to_bits());
    encoder.u8(support_comparison_tag(policy.comparison()));
    encoder.u8(blanking_policy_tag(policy.blanking()));
}

fn encode_taylor_validity(encoder: &mut impl ProductMetadataEncoder, policy: TaylorValidityPolicy) {
    encoder.u8(match policy.reference() {
        TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum => 0,
    });
    encoder.u32(policy.peak_fraction().to_bits());
    encoder.u8(support_comparison_tag(policy.comparison()));
    encoder.u8(blanking_policy_tag(policy.blanking()));
}

fn support_comparison_tag(comparison: ProductSupportComparison) -> u8 {
    match comparison {
        ProductSupportComparison::StrictlyGreater => 0,
    }
}

fn blanking_policy_tag(blanking: ProductBlankingPolicy) -> u8 {
    match blanking {
        ProductBlankingPolicy::ZeroAndFalseMask => 0,
    }
}

fn schema_tag(schema: ProductSchema) -> u8 {
    match schema {
        ProductSchema::CasaPagedImageF32 => 0,
        ProductSchema::LogicalCollection => 1,
        ProductSchema::CasaImageMetadata => 2,
    }
}

fn polarization_tag(coordinate: PolarizationCoordinate) -> u8 {
    match coordinate {
        PolarizationCoordinate::StokesI => 0,
        PolarizationCoordinate::StokesQ => 1,
        PolarizationCoordinate::StokesU => 2,
        PolarizationCoordinate::StokesV => 3,
        PolarizationCoordinate::LinearXx => 4,
        PolarizationCoordinate::LinearXy => 5,
        PolarizationCoordinate::LinearYx => 6,
        PolarizationCoordinate::LinearYy => 7,
        PolarizationCoordinate::CircularRr => 8,
        PolarizationCoordinate::CircularRl => 9,
        PolarizationCoordinate::CircularLr => 10,
        PolarizationCoordinate::CircularLl => 11,
    }
}

fn product_axes(
    geometry: &CompiledGeometry,
    domain: &CompiledImageDomain,
    reconstruction: &ReconstructionContract,
    kind: ProductAxisKind,
) -> ProductAxes {
    let direction_pixels = match kind {
        ProductAxisKind::SkyImage => domain.shape().pixels(),
        ProductAxisKind::PlaneState => [1, 1],
        ProductAxisKind::Metadata => [0, 0],
    };
    let polarization = reconstruction.polarization().coordinates();
    let spectral = if kind == ProductAxisKind::Metadata {
        0
    } else {
        geometry.spectral().output_channels()
    };
    let mut shape = [0; 4];
    for (position, axis) in domain.axes().positions().iter().enumerate() {
        shape[position] = match axis {
            ImageAxis::DirectionLongitude => direction_pixels[0],
            ImageAxis::DirectionLatitude => direction_pixels[1],
            ImageAxis::Polarization if kind != ProductAxisKind::Metadata => polarization.len(),
            ImageAxis::Spectral if kind != ProductAxisKind::Metadata => spectral,
            ImageAxis::Polarization | ImageAxis::Spectral => 0,
        };
    }
    ProductAxes {
        kind,
        geometry_id: geometry.geometry_id(),
        domain: domain.role().clone(),
        order: domain.axes().clone(),
        shape,
        direction: domain.direction(),
        spectral: geometry.spectral().clone(),
        polarization: polarization.into(),
    }
}

fn product_name(stem: &str, term: ProductTerm, pb_corrected: bool) -> String {
    match (term, pb_corrected) {
        (ProductTerm::Single, false) => format!(".{stem}"),
        (ProductTerm::Single, true) => format!(".{stem}.pbcor"),
        (ProductTerm::Taylor(term), false) => format!(".{stem}.tt{term}"),
        (ProductTerm::Taylor(term), true) => format!(".{stem}.tt{term}.pbcor"),
    }
}

fn is_zeroth(term: ProductTerm) -> bool {
    matches!(term, ProductTerm::Single | ProductTerm::Taylor(0))
}

fn canonical_ids<T: Ord>(values: impl IntoIterator<Item = T>) -> Box<[T]> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values.into_boxed_slice()
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AxisOrder, DirectionFrame, DopplerConvention, FrequencyFrame, Projection, RestFrequency,
        SkyDirection, SpectralFrameAnchor, SpectralWcs,
        measurement_equation::compile_product_boundary,
    };

    #[derive(Clone)]
    struct IdentityFixture {
        problem_id: CompiledProblemId,
        normalization_boundary: ProductNormalizationBoundary,
        sources: Vec<ProductSource>,
        nodes: Vec<ProductNode>,
        publication: ProductPublication,
    }

    impl IdentityFixture {
        fn graph_id(&self) -> ProductGraphId {
            canonical_graph_id(
                self.problem_id,
                &self.normalization_boundary,
                &self.sources,
                &self.nodes,
                &self.publication,
            )
        }
    }

    fn direction(reference_pixel: f64) -> DirectionCoordinateSpec {
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [reference_pixel, 23.0],
            [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        )
    }

    fn spectral(reference_frequency_hz: f64) -> SpectralCoordinateSpec {
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        )
    }

    fn primary_beam_policy(cutoff: f32) -> PrimaryBeamValidityPolicy {
        PrimaryBeamValidityPolicy::new(
            cutoff,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB identity fixture")
    }

    fn fixture() -> IdentityFixture {
        let source = ProductSource {
            source_id: ProductSourceId(0),
            role: ProductSourceRole::FinalNormalState,
            domain: ImageDomainRole::Main,
            term: ProductTerm::Taylor(0),
        };
        let mut node = ProductNode {
            node_id: ProductNodeId(0),
            role: ProductRole::Psf(ProductTerm::Taylor(0)),
            name: Some(".psf.tt0".to_string()),
            axes: ProductAxes {
                kind: ProductAxisKind::SkyImage,
                geometry_id: CompiledGeometryId::from_sha256_for_test([2; 32]),
                domain: ImageDomainRole::Main,
                order: AxisOrder::new([
                    ImageAxis::DirectionLongitude,
                    ImageAxis::DirectionLatitude,
                    ImageAxis::Polarization,
                    ImageAxis::Spectral,
                ]),
                shape: [64, 48, 1, 1],
                direction: direction(31.0),
                spectral: spectral(1.4e9),
                polarization: vec![PolarizationCoordinate::StokesI].into_boxed_slice(),
            },
            unit: ProductUnit::JyPerBeam,
            normalization: Some(ProductNormalization::UnitResponse),
            beam: ProductBeamRule::Fitted,
            validity: ProductValidityRule::PrimaryBeam(primary_beam_policy(0.2)),
            schema: ProductSchema::CasaPagedImageF32,
            payload: empty_payload_envelope(),
            source_dependencies: vec![ProductSourceId(0)].into_boxed_slice(),
            dependencies: Box::new([]),
        };
        node.payload = product_payload_envelope(&node).expect("bounded identity fixture");
        let publication = ProductPublication {
            join: ProductPublicationJoin::ObservationTransaction,
            members: vec![node.node_id].into_boxed_slice(),
        };
        IdentityFixture {
            problem_id: CompiledProblemId::from_sha256_for_test([1; 32]),
            normalization_boundary: compile_product_boundary(
                &[ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
            ),
            sources: vec![source],
            nodes: vec![node],
            publication,
        }
    }

    #[test]
    fn product_graph_identity_encodes_every_semantic_projection() {
        type Mutation = Box<dyn Fn(&mut IdentityFixture)>;
        let baseline = fixture();
        let baseline_id = baseline.graph_id();
        let mutations: Vec<(&str, Mutation)> = vec![
            (
                "problem",
                Box::new(|value| {
                    value.problem_id = CompiledProblemId::from_sha256_for_test([9; 32])
                }),
            ),
            (
                "normalization boundary",
                Box::new(|value| {
                    value.normalization_boundary = compile_product_boundary(
                        &[ProductKind::Psf],
                        ProductNormalization::FlatNoise,
                        RestoringBeamPolicy::None,
                    )
                }),
            ),
            (
                "source identity",
                Box::new(|value| value.sources[0].source_id = ProductSourceId(1)),
            ),
            (
                "source role",
                Box::new(|value| value.sources[0].role = ProductSourceRole::FinalModel),
            ),
            (
                "source domain",
                Box::new(|value| {
                    value.sources[0].domain = ImageDomainRole::Outlier("field".to_string())
                }),
            ),
            (
                "source term",
                Box::new(|value| value.sources[0].term = ProductTerm::Taylor(1)),
            ),
            (
                "node identity",
                Box::new(|value| value.nodes[0].node_id = ProductNodeId(1)),
            ),
            (
                "role payload",
                Box::new(|value| value.nodes[0].role = ProductRole::Psf(ProductTerm::Taylor(1))),
            ),
            (
                "name",
                Box::new(|value| value.nodes[0].name = Some(".other".to_string())),
            ),
            (
                "axis kind",
                Box::new(|value| value.nodes[0].axes.kind = ProductAxisKind::PlaneState),
            ),
            (
                "geometry WCS identity",
                Box::new(|value| {
                    value.nodes[0].axes.geometry_id =
                        CompiledGeometryId::from_sha256_for_test([7; 32])
                }),
            ),
            (
                "direction WCS",
                Box::new(|value| value.nodes[0].axes.direction = direction(30.0)),
            ),
            (
                "spectral WCS",
                Box::new(|value| value.nodes[0].axes.spectral = spectral(1.5e9)),
            ),
            (
                "axis domain",
                Box::new(|value| {
                    value.nodes[0].axes.domain = ImageDomainRole::Outlier("field".to_string())
                }),
            ),
            (
                "axis order",
                Box::new(|value| {
                    value.nodes[0].axes.order = AxisOrder::new([
                        ImageAxis::DirectionLatitude,
                        ImageAxis::DirectionLongitude,
                        ImageAxis::Polarization,
                        ImageAxis::Spectral,
                    ])
                }),
            ),
            ("shape", Box::new(|value| value.nodes[0].axes.shape[0] = 65)),
            (
                "polarization",
                Box::new(|value| {
                    value.nodes[0].axes.polarization =
                        vec![PolarizationCoordinate::StokesQ].into_boxed_slice()
                }),
            ),
            (
                "unit",
                Box::new(|value| value.nodes[0].unit = ProductUnit::JyPerPixel),
            ),
            (
                "normalization",
                Box::new(|value| {
                    value.nodes[0].normalization = Some(ProductNormalization::FlatSky)
                }),
            ),
            (
                "beam",
                Box::new(|value| value.nodes[0].beam = ProductBeamRule::None),
            ),
            (
                "validity",
                Box::new(|value| {
                    value.nodes[0].validity =
                        ProductValidityRule::PrimaryBeam(primary_beam_policy(0.25))
                }),
            ),
            (
                "schema",
                Box::new(|value| value.nodes[0].schema = ProductSchema::LogicalCollection),
            ),
            (
                "source dependencies",
                Box::new(|value| {
                    value.nodes[0].source_dependencies = vec![ProductSourceId(1)].into_boxed_slice()
                }),
            ),
            (
                "node dependencies",
                Box::new(|value| {
                    value.nodes[0].dependencies = vec![ProductNodeId(1)].into_boxed_slice()
                }),
            ),
            (
                "element representation",
                Box::new(|value| {
                    value.nodes[0].payload.element_representation =
                        ProductElementRepresentation::NotApplicable
                }),
            ),
            (
                "logical elements",
                Box::new(|value| value.nodes[0].payload.logical_elements += 1),
            ),
            (
                "logical pixel bytes",
                Box::new(|value| value.nodes[0].payload.logical_pixel_bytes += 1),
            ),
            (
                "identity metadata bytes",
                Box::new(|value| value.nodes[0].payload.identity_metadata_bytes += 1),
            ),
            (
                "identity envelope bytes",
                Box::new(|value| value.nodes[0].payload.identity_envelope_bytes += 1),
            ),
            (
                "publication membership",
                Box::new(|value| value.publication.members = Box::new([])),
            ),
        ];

        for (label, mutate) in mutations {
            let mut changed = baseline.clone();
            mutate(&mut changed);
            assert_ne!(
                baseline_id,
                changed.graph_id(),
                "missing {label} identity field"
            );
        }
        assert_eq!(
            baseline_id.as_bytes(),
            [
                76, 224, 66, 0, 244, 143, 4, 104, 78, 226, 238, 41, 35, 96, 211, 75, 172, 101, 70,
                135, 193, 150, 37, 3, 251, 95, 174, 132, 232, 217, 42, 140,
            ],
            "pin ProductGraphId v3"
        );
    }
}
