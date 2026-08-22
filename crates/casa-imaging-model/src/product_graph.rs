// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-owned product meaning, topology, and atomic publication contract.

use std::{collections::BTreeMap, fmt};

use crate::{
    AxisOrder, CompiledGeometry, CompiledGeometryId, CompiledImageDomain, CompiledProblemId,
    DirectionCoordinateSpec, ImageAxis, ImageDomainRole, LogicalIdentity, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProductKind, ProductNormalization, ProductNormalizationBoundary,
    ProductRequirements, ReconstructionBasis, ReconstructionContract, RestoringBeamPolicy,
    SpectralCoordinateSpec, TaylorValidityPolicy, compiled_problem::CanonicalEncoder,
};

const PRODUCT_GRAPH_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-graph";
const PRODUCT_GRAPH_IDENTITY_VERSION: u32 = 1;

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

/// The fixed atomic-store protocol shared with runtime transaction publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomicStoreProtocol;

impl AtomicStoreProtocol {
    /// Exact staged evidence and terminal receipt candidate are durable before visibility.
    #[must_use]
    pub const fn requires_durable_prepare(self) -> bool {
        true
    }

    /// One observation-transaction operation is the sole visibility change.
    #[must_use]
    pub const fn has_one_visibility_operation(self) -> bool {
        true
    }

    /// Receipt terminal promotion after visibility has no fallible result path.
    #[must_use]
    pub const fn has_infallible_terminal_promotion(self) -> bool {
        true
    }
}

/// One atomic publication set for all materialized graph members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublication {
    protocol: AtomicStoreProtocol,
    members: Box<[ProductNodeId]>,
}

impl ProductPublication {
    /// Return the fixed atomic-store choreography.
    #[must_use]
    pub const fn protocol(&self) -> AtomicStoreProtocol {
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

    /// Return the sole atomic publication set.
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
    fn compile(mut self, problem_id: CompiledProblemId) -> ProductGraph {
        for (domain_index, domain) in self.geometry.domains().iter().enumerate() {
            for product in self.products.products() {
                self.compile_product(domain_index, domain, *product);
            }
        }
        let publication = ProductPublication {
            protocol: AtomicStoreProtocol,
            members: self
                .nodes
                .iter()
                .filter(|node| node.schema == ProductSchema::ImageF32V1)
                .map(|node| node.node_id)
                .collect(),
        };
        ProductGraph {
            graph_id: graph_id(problem_id),
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
                    let dependencies = self.nodes_for(
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
                let term = self.primary_beam_term();
                let primary_beam = self.add_image(
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
                if matches!(
                    self.reconstruction.basis(),
                    ReconstructionBasis::Taylor { .. }
                ) && self
                    .products
                    .contains(ProductKind::PbCorrectedSpectralIndex)
                {
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::PrimaryBeamSpectralIndex,
                        ".pb.alpha".to_string(),
                        ProductAxisKind::SkyImage,
                        ProductUnit::Dimensionless,
                        None,
                        ProductBeamRule::None,
                        ProductValidityRule::PrimaryBeam(self.products.validity().primary_beam()),
                        [primary_beam],
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
                let dependencies = self.nodes_for(domain_index, terms.map(ProductRole::Model));
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
                    [alpha],
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
    problem_id: CompiledProblemId,
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
    .compile(problem_id)
}

fn graph_id(problem_id: CompiledProblemId) -> ProductGraphId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(PRODUCT_GRAPH_IDENTITY_DOMAIN);
    encoder.u32(PRODUCT_GRAPH_IDENTITY_VERSION);
    encoder.digest(problem_id.as_bytes());
    ProductGraphId(LogicalIdentity::from_sha256(encoder.finish()))
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
