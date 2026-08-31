// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-owned product meaning, topology, and independently atomic publication contract.

use std::{collections::BTreeMap, fmt};

use crate::{
    AxisOrder, CompiledGeometry, CompiledGeometryId, CompiledImageDomain, DirectionCoordinateSpec,
    ImageAxis, ImageDomainRole, LogicalIdentity, NormalStateNormalization, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProductBlankingPolicy, ProductBoundaryOperation, ProductKind,
    ProductNormalization, ProductNormalizationBoundary, ProductRequirements,
    ProductSupportComparison, ReconstructionBasis, ReconstructionContract, RestoringBeamPolicy,
    SpectralCoordinateSpec, TaylorSupportReference, TaylorValidityPolicy,
    compiled_problem::{CanonicalEncoder, polarization_tag},
};

const PRODUCT_GRAPH_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-graph";
const PRODUCT_GRAPH_IDENTITY_VERSION: u32 = 2;

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

/// Stable graph-local identity of one logical product node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProductNodeId(usize);

impl ProductNodeId {
    /// Return the zero-based canonical node ordinal.
    #[must_use]
    pub const fn ordinal(self) -> usize {
        self.0
    }
}

/// Coefficient placement of a product.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductTerm {
    /// A non-Taylor product.
    Single,
    /// One zero-based Taylor coefficient or convolution order.
    Taylor(usize),
}

/// Exact logical meaning of one product node.
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
    /// Reconstruction mask, distinct from output validity.
    CleanMask,
    /// Imaging weight image or Taylor convolution term.
    Weight(ProductTerm),
    /// Primary-beam response.
    PrimaryBeam(ProductTerm),
    /// Primary-beam spectral index used for spectral-index correction.
    PrimaryBeamSpectralIndex,
    /// Sensitivity response.
    Sensitivity,
    /// Primary-beam-corrected restored image.
    PbCorrectedImage(ProductTerm),
    /// Logical collection of Taylor coefficient products.
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

    /// Return axes in requested storage order.
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

/// Physical unit required by a product.
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
    /// Embed fitted and selected beam metadata for this policy.
    Metadata(RestoringBeamPolicy),
}

/// Valid support carried by one product, distinct from a reconstruction mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductValidityRule {
    /// Every represented pixel is valid.
    All,
    /// Validity and blank channels come from the final normal state.
    FinalNormalState,
    /// Validity requires the Product Contract's primary-beam support rule.
    PrimaryBeam(PrimaryBeamValidityPolicy),
    /// Validity requires the Product Contract's Taylor-coefficient support rule.
    Taylor(TaylorValidityPolicy),
    /// Both primary-beam and Taylor support rules apply.
    TaylorAndPrimaryBeam {
        /// Exact Taylor-coefficient support policy.
        taylor: TaylorValidityPolicy,
        /// Exact primary-beam support policy.
        primary_beam: PrimaryBeamValidityPolicy,
    },
}

/// Backend-independent logical schema of a product payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductSchema {
    /// Version-one four-axis image carrying binary32 pixels and typed metadata.
    ImageF32V1,
    /// Version-one logical collection of nodes already named in this graph.
    LogicalCollectionV1,
    /// Version-one metadata embedded in image members rather than separately published.
    EmbeddedImageMetadataV1,
    /// Version-one internal image input that participates in topology but is not published.
    InternalImageF32V1,
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
    dependencies: Box<[ProductNodeId]>,
}

impl ProductNode {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node_id(&self) -> ProductNodeId {
        self.node_id
    }

    /// Return the exact logical product meaning.
    #[must_use]
    pub const fn role(&self) -> ProductRole {
        self.role
    }

    /// Return the compiler-owned output suffix, if this node is independently materialized.
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

    /// Return the backend-independent logical payload schema.
    #[must_use]
    pub const fn schema(&self) -> ProductSchema {
        self.schema
    }

    /// Return graph-node dependencies, all of which precede this node.
    #[must_use]
    pub const fn dependencies(&self) -> &[ProductNodeId] {
        &self.dependencies
    }
}

/// The fixed independently atomic product-store protocol.
///
/// CASA image products have conventional sibling names and independent
/// lifetimes: users may retain or delete one product without the others.  A
/// generation therefore authorizes one private prepare and one atomic
/// replacement per member, rather than claiming one atomic visibility change
/// for the whole product set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndependentProductStoreProtocol;

impl IndependentProductStoreProtocol {
    /// Exact staged evidence is durable before any member replacement.
    #[must_use]
    pub const fn requires_durable_prepare(self) -> bool {
        true
    }

    /// Every member has exactly one independently atomic visibility operation.
    #[must_use]
    pub const fn has_one_visibility_operation_per_member(self) -> bool {
        true
    }

    /// A promoted member remains valid even if a later member fails.
    #[must_use]
    pub const fn preserves_promoted_members_on_later_failure(self) -> bool {
        true
    }
}

/// One independently atomic publication sequence for all materialized members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublication {
    protocol: IndependentProductStoreProtocol,
    members: Box<[ProductNodeId]>,
}

impl ProductPublication {
    /// Return the fixed atomic-store choreography.
    #[must_use]
    pub const fn protocol(&self) -> IndependentProductStoreProtocol {
        self.protocol
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
    normalization_boundary: ProductNormalizationBoundary,
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

    /// Return the typed handoff from unnormalized normal state.
    #[must_use]
    pub const fn normalization_boundary(&self) -> &ProductNormalizationBoundary {
        &self.normalization_boundary
    }

    /// Return every node in topological and publication order.
    #[must_use]
    pub const fn nodes(&self) -> &[ProductNode] {
        &self.nodes
    }

    /// Find a uniquely named role across all image domains.
    #[must_use]
    pub fn node(&self, role: ProductRole) -> Option<&ProductNode> {
        let mut matching = self.nodes.iter().filter(|node| node.role == role);
        let node = matching.next()?;
        matching.next().is_none().then_some(node)
    }

    /// Return the canonical independently published member sequence.
    #[must_use]
    pub const fn publication(&self) -> &ProductPublication {
        &self.publication
    }
}

struct NodeProjection {
    role: ProductRole,
    name: Option<String>,
    axis_kind: ProductAxisKind,
    unit: ProductUnit,
    normalization: Option<ProductNormalization>,
    beam: ProductBeamRule,
    validity: ProductValidityRule,
    schema: ProductSchema,
}

struct GraphBuilder<'a> {
    geometry: &'a CompiledGeometry,
    reconstruction: &'a ReconstructionContract,
    products: &'a ProductRequirements,
    nodes: Vec<ProductNode>,
    node_ids: BTreeMap<(usize, ProductRole), ProductNodeId>,
}

impl<'a> GraphBuilder<'a> {
    fn compile(mut self) -> ProductGraph {
        for (domain_index, domain) in self.geometry.domains().iter().enumerate() {
            for product in self.products.products() {
                self.compile_product(domain_index, domain, *product);
            }
        }
        let publication_members = self
            .nodes
            .iter()
            .filter(|node| node.schema == ProductSchema::ImageF32V1)
            .map(|node| node.node_id)
            .collect::<Box<[_]>>();
        let graph_id = graph_id(
            self.products.normalization_boundary(),
            &self.nodes,
            &publication_members,
        );
        let publication = ProductPublication {
            protocol: IndependentProductStoreProtocol,
            members: publication_members,
        };
        ProductGraph {
            graph_id,
            normalization_boundary: self.products.normalization_boundary().clone(),
            nodes: self.nodes.into_boxed_slice(),
            publication,
        }
    }

    fn compile_product(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        product: ProductKind,
    ) {
        match product {
            ProductKind::Psf => {
                for term in self.convolution_terms() {
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::Psf(term),
                        product_name("psf", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::JyPerBeam,
                        Some(ProductNormalization::UnitResponse),
                        if is_zeroth(term) {
                            ProductBeamRule::Fitted
                        } else {
                            ProductBeamRule::None
                        },
                        ProductValidityRule::All,
                        [],
                    );
                }
            }
            ProductKind::Residual => {
                for term in self.image_terms() {
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::Residual(term),
                        product_name("residual", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::JyPerBeam,
                        Some(self.products.normalization()),
                        ProductBeamRule::Fitted,
                        ProductValidityRule::FinalNormalState,
                        [],
                    );
                }
            }
            ProductKind::Model => {
                for term in self.image_terms() {
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::Model(term),
                        product_name("model", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::JyPerPixel,
                        None,
                        ProductBeamRule::None,
                        ProductValidityRule::All,
                        [],
                    );
                }
            }
            ProductKind::RestoredImage => {
                for term in self.image_terms() {
                    let dependencies = self.required_nodes_for(
                        domain_index,
                        [ProductRole::Residual(term), ProductRole::Model(term)],
                    );
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::RestoredImage(term),
                        product_name("image", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::JyPerBeam,
                        Some(self.products.normalization()),
                        ProductBeamRule::Restoring(self.products.restoring_beam()),
                        ProductValidityRule::FinalNormalState,
                        dependencies,
                    );
                }
            }
            ProductKind::SumWeights => {
                for term in self.convolution_terms() {
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::SumWeights(term),
                        product_name("sumwt", term, false),
                        ProductAxisKind::PlaneState,
                        ProductUnit::VisibilityWeight,
                        None,
                        ProductBeamRule::None,
                        ProductValidityRule::All,
                        [],
                    );
                }
            }
            ProductKind::Mask => {
                self.add_image(
                    domain_index,
                    domain,
                    ProductRole::CleanMask,
                    ".mask".to_string(),
                    ProductAxisKind::SkyImage,
                    ProductUnit::Dimensionless,
                    None,
                    ProductBeamRule::None,
                    ProductValidityRule::All,
                    [],
                );
            }
            ProductKind::Weight => {
                for term in self.convolution_terms() {
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::Weight(term),
                        product_name("weight", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::Dimensionless,
                        None,
                        ProductBeamRule::None,
                        ProductValidityRule::All,
                        [],
                    );
                }
            }
            ProductKind::PrimaryBeam => {
                let mut primary_beam = None;
                for term in self.image_terms() {
                    let node = self.add_image(
                        domain_index,
                        domain,
                        ProductRole::PrimaryBeam(term),
                        product_name("pb", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::Dimensionless,
                        None,
                        ProductBeamRule::None,
                        ProductValidityRule::All,
                        [],
                    );
                    if term == self.primary_beam_term() {
                        primary_beam = Some(node);
                    }
                }
                if matches!(
                    self.reconstruction.basis(),
                    ReconstructionBasis::Taylor { .. }
                ) && self
                    .products
                    .contains(ProductKind::PbCorrectedSpectralIndex)
                {
                    self.add_internal_image(
                        domain_index,
                        domain,
                        ProductRole::PrimaryBeamSpectralIndex,
                        ProductUnit::Dimensionless,
                        ProductBeamRule::None,
                        ProductValidityRule::PrimaryBeam(self.products.validity().primary_beam()),
                        [primary_beam.expect("primary-beam Taylor zero is compiled")],
                    );
                }
            }
            ProductKind::Sensitivity => {
                self.add_image(
                    domain_index,
                    domain,
                    ProductRole::Sensitivity,
                    ".sensitivity".to_string(),
                    ProductAxisKind::SkyImage,
                    ProductUnit::Dimensionless,
                    None,
                    ProductBeamRule::None,
                    ProductValidityRule::All,
                    [],
                );
            }
            ProductKind::PbCorrectedImage => {
                let primary_beam = self.node_id(
                    domain_index,
                    ProductRole::PrimaryBeam(self.primary_beam_term()),
                );
                for term in self.image_terms() {
                    let restored = self.node_id(domain_index, ProductRole::RestoredImage(term));
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::PbCorrectedImage(term),
                        product_name("image", term, true),
                        ProductAxisKind::SkyImage,
                        ProductUnit::JyPerBeam,
                        Some(self.products.normalization()),
                        ProductBeamRule::Inherit(restored),
                        ProductValidityRule::PrimaryBeam(self.products.validity().primary_beam()),
                        [restored, primary_beam],
                    );
                }
            }
            ProductKind::TaylorTerms => {
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
                    ProductRole::TaylorCoefficientSet,
                    ProductSchema::LogicalCollectionV1,
                    ProductBeamRule::None,
                    dependencies,
                );
            }
            ProductKind::SpectralIndex => {
                let terms = [ProductTerm::Taylor(0), ProductTerm::Taylor(1)];
                let dependencies = terms
                    .into_iter()
                    .flat_map(|term| {
                        [
                            self.node_id(domain_index, ProductRole::Residual(term)),
                            self.node_id(domain_index, ProductRole::RestoredImage(term)),
                        ]
                    })
                    .collect::<Vec<_>>();
                self.add_image(
                    domain_index,
                    domain,
                    ProductRole::SpectralIndex,
                    ".alpha".to_string(),
                    ProductAxisKind::SkyImage,
                    ProductUnit::Dimensionless,
                    None,
                    self.derived_beam(domain_index),
                    ProductValidityRule::Taylor(self.products.validity().taylor()),
                    dependencies,
                );
            }
            ProductKind::SpectralIndexError => {
                let alpha = self.node_id(domain_index, ProductRole::SpectralIndex);
                let terms = [ProductTerm::Taylor(0), ProductTerm::Taylor(1)];
                let dependencies = std::iter::once(alpha)
                    .chain(terms.into_iter().flat_map(|term| {
                        [
                            self.node_id(domain_index, ProductRole::Residual(term)),
                            self.node_id(domain_index, ProductRole::RestoredImage(term)),
                        ]
                    }))
                    .collect::<Vec<_>>();
                self.add_image(
                    domain_index,
                    domain,
                    ProductRole::SpectralIndexError,
                    ".alpha.error".to_string(),
                    ProductAxisKind::SkyImage,
                    ProductUnit::Dimensionless,
                    None,
                    self.derived_beam(domain_index),
                    ProductValidityRule::Taylor(self.products.validity().taylor()),
                    dependencies,
                );
            }
            ProductKind::PbCorrectedSpectralIndex => {
                let alpha = self.node_id(domain_index, ProductRole::SpectralIndex);
                let primary_beam = self.node_id(
                    domain_index,
                    ProductRole::PrimaryBeam(self.primary_beam_term()),
                );
                let primary_beam_alpha =
                    self.node_id(domain_index, ProductRole::PrimaryBeamSpectralIndex);
                self.add_image(
                    domain_index,
                    domain,
                    ProductRole::PbCorrectedSpectralIndex,
                    ".alpha.pbcor".to_string(),
                    ProductAxisKind::SkyImage,
                    ProductUnit::Dimensionless,
                    None,
                    ProductBeamRule::Inherit(alpha),
                    ProductValidityRule::TaylorAndPrimaryBeam {
                        taylor: self.products.validity().taylor(),
                        primary_beam: self.products.validity().primary_beam(),
                    },
                    [alpha, primary_beam, primary_beam_alpha],
                );
            }
            ProductKind::Beam => {
                let dependencies = self
                    .nodes
                    .iter()
                    .filter(|node| {
                        node.axes.domain == *domain.role()
                            && !matches!(node.beam, ProductBeamRule::None)
                    })
                    .map(|node| node.node_id)
                    .collect::<Vec<_>>();
                self.add_metadata(
                    domain_index,
                    domain,
                    ProductRole::BeamMetadata,
                    ProductSchema::EmbeddedImageMetadataV1,
                    ProductBeamRule::Metadata(self.products.restoring_beam()),
                    dependencies,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_image(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        role: ProductRole,
        name: String,
        axis_kind: ProductAxisKind,
        unit: ProductUnit,
        normalization: Option<ProductNormalization>,
        beam: ProductBeamRule,
        validity: ProductValidityRule,
        dependencies: impl IntoIterator<Item = ProductNodeId>,
    ) -> ProductNodeId {
        self.add_node(
            domain_index,
            domain,
            NodeProjection {
                role,
                name: Some(name),
                axis_kind,
                unit,
                normalization,
                beam,
                validity,
                schema: ProductSchema::ImageF32V1,
            },
            dependencies,
        )
    }

    fn add_metadata(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        role: ProductRole,
        schema: ProductSchema,
        beam: ProductBeamRule,
        dependencies: impl IntoIterator<Item = ProductNodeId>,
    ) -> ProductNodeId {
        self.add_node(
            domain_index,
            domain,
            NodeProjection {
                role,
                name: None,
                axis_kind: ProductAxisKind::Metadata,
                unit: ProductUnit::NotApplicable,
                normalization: None,
                beam,
                validity: ProductValidityRule::All,
                schema,
            },
            dependencies,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_internal_image(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        role: ProductRole,
        unit: ProductUnit,
        beam: ProductBeamRule,
        validity: ProductValidityRule,
        dependencies: impl IntoIterator<Item = ProductNodeId>,
    ) -> ProductNodeId {
        self.add_node(
            domain_index,
            domain,
            NodeProjection {
                role,
                name: None,
                axis_kind: ProductAxisKind::SkyImage,
                unit,
                normalization: None,
                beam,
                validity,
                schema: ProductSchema::InternalImageF32V1,
            },
            dependencies,
        )
    }

    fn add_node(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        projection: NodeProjection,
        dependencies: impl IntoIterator<Item = ProductNodeId>,
    ) -> ProductNodeId {
        let node_id = ProductNodeId(self.nodes.len());
        let dependencies = canonical_ids(dependencies);
        debug_assert!(
            dependencies
                .iter()
                .all(|dependency| dependency.0 < node_id.0)
        );
        let previous = self
            .node_ids
            .insert((domain_index, projection.role), node_id);
        debug_assert!(previous.is_none());
        self.nodes.push(ProductNode {
            node_id,
            role: projection.role,
            name: projection.name,
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
            schema: projection.schema,
            dependencies,
        });
        node_id
    }

    fn node_id(&self, domain: usize, role: ProductRole) -> ProductNodeId {
        self.node_ids[&(domain, role)]
    }

    fn required_nodes_for<const N: usize>(
        &self,
        domain: usize,
        roles: [ProductRole; N],
    ) -> Vec<ProductNodeId> {
        roles
            .into_iter()
            .map(|role| self.node_id(domain, role))
            .collect()
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

    fn primary_beam_term(&self) -> ProductTerm {
        match self.reconstruction.basis() {
            ReconstructionBasis::Taylor { .. } => ProductTerm::Taylor(0),
            ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => {
                ProductTerm::Single
            }
        }
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
    geometry: &CompiledGeometry,
    reconstruction: &ReconstructionContract,
    products: &ProductRequirements,
) -> ProductGraph {
    GraphBuilder {
        geometry,
        reconstruction,
        products,
        nodes: Vec::new(),
        node_ids: BTreeMap::new(),
    }
    .compile()
}

fn graph_id(
    normalization_boundary: &ProductNormalizationBoundary,
    nodes: &[ProductNode],
    publication_members: &[ProductNodeId],
) -> ProductGraphId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(PRODUCT_GRAPH_IDENTITY_DOMAIN);
    encoder.u32(PRODUCT_GRAPH_IDENTITY_VERSION);
    encode_normalization_boundary(&mut encoder, normalization_boundary);
    encoder.usize(nodes.len());
    for node in nodes {
        encode_node(&mut encoder, node);
    }
    encoder.u8(0);
    encoder.usize(publication_members.len());
    for member in publication_members {
        encoder.usize(member.ordinal());
    }
    ProductGraphId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn encode_normalization_boundary(
    encoder: &mut CanonicalEncoder,
    boundary: &ProductNormalizationBoundary,
) {
    encoder.u8(match boundary.input() {
        NormalStateNormalization::Unnormalized => 0,
    });
    encoder.usize(boundary.operations().len());
    for operation in boundary.operations() {
        match operation {
            ProductBoundaryOperation::Normalize(normalization) => {
                encoder.u8(0);
                encode_normalization(encoder, *normalization);
            }
            ProductBoundaryOperation::ScaleResidual => encoder.u8(1),
            ProductBoundaryOperation::Restore(policy) => {
                encoder.u8(2);
                encode_restoring_beam(encoder, *policy);
            }
            ProductBoundaryOperation::CorrectPrimaryBeam => encoder.u8(3),
            ProductBoundaryOperation::BlankInvalid => encoder.u8(4),
            ProductBoundaryOperation::ConvertUnits => encoder.u8(5),
        }
    }
}

fn encode_node(encoder: &mut CanonicalEncoder, node: &ProductNode) {
    encoder.usize(node.node_id.ordinal());
    encode_role(encoder, node.role);
    match &node.name {
        Some(name) => {
            encoder.u8(1);
            encoder.bytes(name.as_bytes());
        }
        None => encoder.u8(0),
    }
    encoder.u8(match node.axes.kind {
        ProductAxisKind::SkyImage => 0,
        ProductAxisKind::PlaneState => 1,
        ProductAxisKind::Metadata => 2,
    });
    encoder.digest(node.axes.geometry_id.as_bytes());
    match &node.axes.domain {
        ImageDomainRole::Main => encoder.u8(0),
        ImageDomainRole::Outlier(name) => {
            encoder.u8(1);
            encoder.bytes(name.as_bytes());
        }
    }
    for axis in node.axes.order.positions() {
        encoder.u8(match axis {
            ImageAxis::DirectionLongitude => 0,
            ImageAxis::DirectionLatitude => 1,
            ImageAxis::Polarization => 2,
            ImageAxis::Spectral => 3,
        });
    }
    for extent in node.axes.shape {
        encoder.usize(extent);
    }
    encoder.usize(node.axes.polarization.len());
    for coordinate in &node.axes.polarization {
        encoder.u8(polarization_tag(*coordinate));
    }
    encoder.u8(match node.unit {
        ProductUnit::NotApplicable => 0,
        ProductUnit::JyPerBeam => 1,
        ProductUnit::JyPerPixel => 2,
        ProductUnit::Dimensionless => 3,
        ProductUnit::VisibilityWeight => 4,
    });
    match node.normalization {
        Some(normalization) => {
            encoder.u8(1);
            encode_normalization(encoder, normalization);
        }
        None => encoder.u8(0),
    }
    encode_beam_rule(encoder, node.beam);
    encode_validity_rule(encoder, node.validity);
    encoder.u8(match node.schema {
        ProductSchema::ImageF32V1 => 0,
        ProductSchema::LogicalCollectionV1 => 1,
        ProductSchema::EmbeddedImageMetadataV1 => 2,
        ProductSchema::InternalImageF32V1 => 3,
    });
    encoder.usize(node.dependencies.len());
    for dependency in &node.dependencies {
        encoder.usize(dependency.ordinal());
    }
}

fn encode_role(encoder: &mut CanonicalEncoder, role: ProductRole) {
    match role {
        ProductRole::Psf(term) => encode_term_role(encoder, 0, term),
        ProductRole::Residual(term) => encode_term_role(encoder, 1, term),
        ProductRole::Model(term) => encode_term_role(encoder, 2, term),
        ProductRole::RestoredImage(term) => encode_term_role(encoder, 3, term),
        ProductRole::SumWeights(term) => encode_term_role(encoder, 4, term),
        ProductRole::CleanMask => encoder.u8(5),
        ProductRole::Weight(term) => encode_term_role(encoder, 6, term),
        ProductRole::PrimaryBeam(term) => encode_term_role(encoder, 7, term),
        ProductRole::PrimaryBeamSpectralIndex => encoder.u8(8),
        ProductRole::Sensitivity => encoder.u8(9),
        ProductRole::PbCorrectedImage(term) => encode_term_role(encoder, 10, term),
        ProductRole::TaylorCoefficientSet => encoder.u8(11),
        ProductRole::SpectralIndex => encoder.u8(12),
        ProductRole::SpectralIndexError => encoder.u8(13),
        ProductRole::PbCorrectedSpectralIndex => encoder.u8(14),
        ProductRole::BeamMetadata => encoder.u8(15),
    }
}

fn encode_term_role(encoder: &mut CanonicalEncoder, tag: u8, term: ProductTerm) {
    encoder.u8(tag);
    match term {
        ProductTerm::Single => encoder.u8(0),
        ProductTerm::Taylor(term) => {
            encoder.u8(1);
            encoder.usize(term);
        }
    }
}

fn encode_beam_rule(encoder: &mut CanonicalEncoder, beam: ProductBeamRule) {
    match beam {
        ProductBeamRule::None => encoder.u8(0),
        ProductBeamRule::Fitted => encoder.u8(1),
        ProductBeamRule::Restoring(policy) => {
            encoder.u8(2);
            encode_restoring_beam(encoder, policy);
        }
        ProductBeamRule::Inherit(node) => {
            encoder.u8(3);
            encoder.usize(node.ordinal());
        }
        ProductBeamRule::Metadata(policy) => {
            encoder.u8(4);
            encode_restoring_beam(encoder, policy);
        }
    }
}

fn encode_validity_rule(encoder: &mut CanonicalEncoder, validity: ProductValidityRule) {
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

fn encode_primary_beam_validity(encoder: &mut CanonicalEncoder, policy: PrimaryBeamValidityPolicy) {
    encoder.u32(policy.cutoff().to_bits());
    encode_support_comparison(encoder, policy.comparison());
    encode_blanking(encoder, policy.blanking());
}

fn encode_taylor_validity(encoder: &mut CanonicalEncoder, policy: TaylorValidityPolicy) {
    encoder.u8(match policy.reference() {
        TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum => 0,
    });
    encoder.u32(policy.peak_fraction().to_bits());
    encode_support_comparison(encoder, policy.comparison());
    encode_blanking(encoder, policy.blanking());
}

fn encode_support_comparison(encoder: &mut CanonicalEncoder, comparison: ProductSupportComparison) {
    encoder.u8(match comparison {
        ProductSupportComparison::StrictlyGreater => 0,
    });
}

fn encode_blanking(encoder: &mut CanonicalEncoder, blanking: ProductBlankingPolicy) {
    encoder.u8(match blanking {
        ProductBlankingPolicy::ZeroAndFalseMask => 0,
    });
}

fn encode_normalization(encoder: &mut CanonicalEncoder, normalization: ProductNormalization) {
    encoder.u8(match normalization {
        ProductNormalization::UnitResponse => 0,
        ProductNormalization::FlatNoise => 1,
        ProductNormalization::FlatSky => 2,
    });
}

fn encode_restoring_beam(encoder: &mut CanonicalEncoder, policy: RestoringBeamPolicy) {
    encoder.u8(match policy {
        RestoringBeamPolicy::None => 0,
        RestoringBeamPolicy::PerPlane => 1,
        RestoringBeamPolicy::Common => 2,
    });
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
