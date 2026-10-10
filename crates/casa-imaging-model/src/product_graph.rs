// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-owned product meaning, topology, and independently atomic publication contract.

use std::collections::BTreeMap;

use crate::{
    AxisOrder, CompiledGeometry, CompiledImageDomain, DirectionCoordinateSpec, ImageAxis,
    ImageDomainRole, PolarizationCoordinate, PrimaryBeamValidityPolicy, ProductKind,
    ProductNormalization, ProductNormalizationBoundary, ProductRequirements, ReconstructionBasis,
    ReconstructionContract, RestoringBeamPolicy, SpectralCoordinateSpec, TaylorValidityPolicy,
};

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

/// Support predicate reused by numerical blanking and stored pixel masks.
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

/// Exact presence and support of a stored pixel mask.
///
/// An explicit all-true mask is distinct from an absent mask. Neither choice
/// changes numerical normalization or the reconstruction owner's search mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductPixelMask {
    /// No named/default pixel mask is stored.
    Absent,
    /// Store the support predicate as an explicit default pixel mask.
    Explicit(ProductValidityRule),
}

/// Compiler-owned metadata to serialize alongside a product's numeric payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStorageContract {
    pixel_mask: ProductPixelMask,
    unit: Option<ProductUnit>,
    attach_beam: bool,
}

impl ProductStorageContract {
    /// Return the required mask presence and support, including all-true masks.
    #[must_use]
    pub const fn pixel_mask(self) -> ProductPixelMask {
        self.pixel_mask
    }

    /// Return the serialized unit, or an explicitly empty unit label.
    ///
    /// The node's scientific unit remains available independently.
    #[must_use]
    pub const fn unit(self) -> Option<ProductUnit> {
        self.unit
    }

    /// Whether the product's resolved scientific beam metadata is attached.
    #[must_use]
    pub const fn attach_beam(self) -> bool {
        self.attach_beam
    }
}

/// One immutable product node in publication order.
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
    storage: ProductStorageContract,
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

    /// Return the numerical-support rule, independently of the stored mask.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityRule {
        self.validity
    }

    /// Return the exact stored mask, unit label, and beam-attachment contract.
    #[must_use]
    pub const fn storage(&self) -> ProductStorageContract {
        self.storage
    }
}

/// The published image members, in publication order.
///
/// CASA image products have conventional sibling names and independent
/// lifetimes: each member is staged and replaced on its own, and a member
/// already replaced stays valid if a later one fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublication {
    members: Box<[ProductNodeId]>,
}

impl ProductPublication {
    /// Return every published member.
    #[must_use]
    pub const fn members(&self) -> &[ProductNodeId] {
        &self.members
    }
}

/// Complete compiler-owned product DAG for one immutable imaging problem.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductGraph {
    normalization_boundary: ProductNormalizationBoundary,
    nodes: Box<[ProductNode]>,
    publication: ProductPublication,
}

impl ProductGraph {
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
    /// Whether the node is a published image member rather than an internal
    /// image or metadata.
    published: bool,
}

struct GraphBuilder<'a> {
    geometry: &'a CompiledGeometry,
    reconstruction: &'a ReconstructionContract,
    products: &'a ProductRequirements,
    nodes: Vec<ProductNode>,
    node_ids: BTreeMap<(usize, ProductRole), ProductNodeId>,
    members: Vec<ProductNodeId>,
}

impl<'a> GraphBuilder<'a> {
    fn compile(mut self) -> ProductGraph {
        for (domain_index, domain) in self.geometry.domains().iter().enumerate() {
            for product in self.products.products() {
                self.compile_product(domain_index, domain, *product);
            }
        }
        ProductGraph {
            normalization_boundary: self.products.normalization_boundary().clone(),
            nodes: self.nodes.into_boxed_slice(),
            publication: ProductPublication {
                members: self.members.into_boxed_slice(),
            },
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
                    );
                }
            }
            ProductKind::RestoredImage => {
                for term in self.image_terms() {
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
                    );
                }
            }
            ProductKind::PrimaryBeam => {
                for term in self.primary_beam_terms() {
                    let validity = if term == self.primary_beam_term() {
                        ProductValidityRule::PrimaryBeam(self.products.validity().primary_beam())
                    } else {
                        ProductValidityRule::All
                    };
                    self.add_image(
                        domain_index,
                        domain,
                        ProductRole::PrimaryBeam(term),
                        product_name("pb", term, false),
                        ProductAxisKind::SkyImage,
                        ProductUnit::Dimensionless,
                        None,
                        ProductBeamRule::None,
                        validity,
                    );
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
                );
            }
            ProductKind::PbCorrectedImage => {
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
                    );
                }
            }
            ProductKind::TaylorTerms => {
                self.add_metadata(
                    domain_index,
                    domain,
                    ProductRole::TaylorCoefficientSet,
                    ProductBeamRule::None,
                );
            }
            ProductKind::SpectralIndex => {
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
                );
            }
            ProductKind::SpectralIndexError => {
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
                );
            }
            ProductKind::PbCorrectedSpectralIndex => {
                let alpha = self.node_id(domain_index, ProductRole::SpectralIndex);
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
                );
            }
            ProductKind::Beam => {
                self.add_metadata(
                    domain_index,
                    domain,
                    ProductRole::BeamMetadata,
                    ProductBeamRule::Metadata(self.products.restoring_beam()),
                );
            }
        }
    }

    fn storage_contract(&self, projection: &NodeProjection) -> ProductStorageContract {
        let primary_beam =
            ProductValidityRule::PrimaryBeam(self.products.validity().primary_beam());
        let pixel_mask = match projection.role {
            ProductRole::Residual(_) | ProductRole::RestoredImage(_) => {
                match self.products.validity().uncorrected_mask() {
                    crate::UncorrectedImageMaskPolicy::None => ProductPixelMask::Absent,
                    crate::UncorrectedImageMaskPolicy::PrimaryBeam => {
                        ProductPixelMask::Explicit(primary_beam)
                    }
                }
            }
            ProductRole::PrimaryBeam(term) if term == self.primary_beam_term() => {
                ProductPixelMask::Explicit(primary_beam)
            }
            ProductRole::PbCorrectedImage(_)
            | ProductRole::SpectralIndex
            | ProductRole::SpectralIndexError
            | ProductRole::PbCorrectedSpectralIndex => {
                ProductPixelMask::Explicit(projection.validity)
            }
            _ => ProductPixelMask::Absent,
        };
        ProductStorageContract {
            pixel_mask,
            unit: match projection.role {
                ProductRole::Psf(_) | ProductRole::Residual(_) => None,
                _ => Some(projection.unit),
            },
            attach_beam: projection.beam != ProductBeamRule::None
                && !matches!(projection.role, ProductRole::Residual(_)),
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
    ) {
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
                published: true,
            },
        );
    }

    fn add_metadata(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        role: ProductRole,
        beam: ProductBeamRule,
    ) {
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
                published: false,
            },
        );
    }

    fn add_internal_image(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        role: ProductRole,
        unit: ProductUnit,
        beam: ProductBeamRule,
        validity: ProductValidityRule,
    ) {
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
                published: false,
            },
        );
    }

    fn add_node(
        &mut self,
        domain_index: usize,
        domain: &CompiledImageDomain,
        projection: NodeProjection,
    ) {
        let node_id = ProductNodeId(self.nodes.len());
        let previous = self
            .node_ids
            .insert((domain_index, projection.role), node_id);
        debug_assert!(previous.is_none());
        if projection.published {
            self.members.push(node_id);
        }
        let storage = self.storage_contract(&projection);
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
            storage,
        });
    }

    fn node_id(&self, domain: usize, role: ProductRole) -> ProductNodeId {
        self.node_ids[&(domain, role)]
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

    fn primary_beam_terms(&self) -> Vec<ProductTerm> {
        if self.products.contains(ProductKind::Weight) {
            vec![self.primary_beam_term()]
        } else {
            self.image_terms()
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
        members: Vec::new(),
    }
    .compile()
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
    let spectral = match kind {
        ProductAxisKind::Metadata => 0,
        ProductAxisKind::SkyImage | ProductAxisKind::PlaneState => {
            geometry.spectral().output_channels()
        }
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
