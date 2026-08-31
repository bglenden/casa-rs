// SPDX-License-Identifier: LGPL-3.0-or-later

//! The two-phase Product Generation Authority for complete continuum products.
//!
//! Phase one ([`ProductGenerationAuthority::plan`]) binds the exact
//! compiler-owned Product Graph and the closed typed source commitments into
//! one schema-versioned planned generation whose members carry derived
//! artifact identities. Phase two
//! ([`ProductGenerationAuthority::authorize`]) seals the produced artifacts
//! only against the matching closed typed completions: identical lineage,
//! exact member set, exact content identities. Neither phase exposes any
//! construction path for the records they mint.

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, ImageAxis, ModelCell, ProductAxes, ProductBeamRule,
    ProductGraphId, ProductNodeId, ProductNormalization, ProductRole, ProductSchema, ProductTerm,
    ProductUnit, ProductValidityRule, ReconstructionBasis, RestoringBeamPolicy,
};
use casa_imaging_reconstruction::{
    FinalNormalStatePlane, ModelGeneration, NormalStateCatalog, SpectralChannelValidity,
};

use crate::beam::{RestoringBeam, fit_restoring_beam};
use crate::digest::{
    ARTIFACT_IDENTITY_DOMAIN, ARTIFACT_IDENTITY_VERSION, COMPLETIONS_DOMAIN, COMPLETIONS_VERSION,
    Encoder, PLANNED_GENERATION_DOMAIN, PLANNED_GENERATION_VERSION, SEAL_DOMAIN, SEAL_VERSION,
    member_content_digest,
};
use crate::error::ProductsError;
use crate::restore::{
    fft_convolve, gaussian_beam_image, normalize_plane, rescale_residual_to_beam,
};
use crate::source::{ContinuumProductInputs, ContinuumSourceCatalog};
use crate::taylor::TaylorProducts;

/// Version of the native continuum product-algorithm catalog.
///
/// The identity binds every product algorithm's semantics; changing any
/// algorithm changes every derived artifact identity and seal.
pub const CONTINUUM_ALGORITHM_CATALOG_VERSION: u32 = 6;

/// Default main-lobe cutoff fraction for restoring-beam fitting.
pub const DEFAULT_PSF_CUTOFF: f32 = casa_imaging_reconstruction::DEFAULT_PSF_FIT_CUTOFF;

/// Explicit analytic primary-beam law available to product construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyticPrimaryBeamModel {
    /// CASA's common EVLA primary-beam power polynomial with sampled radial lookup.
    CasaEvlaCommon,
}

/// Explicit continuum production controls.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContinuumProductControls {
    psf_cutoff: f32,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
}

impl ContinuumProductControls {
    /// Construct validated controls.
    ///
    /// # Errors
    ///
    /// Rejects a cutoff outside `(0, 1)` or non-finite values.
    pub fn new(psf_cutoff: f32) -> Result<Self, ProductsError> {
        if !psf_cutoff.is_finite() || psf_cutoff <= 0.0 || psf_cutoff >= 1.0 {
            return Err(ProductsError::InvalidControls);
        }
        Ok(Self {
            psf_cutoff,
            primary_beam_model: None,
        })
    }

    /// Return the main-lobe cutoff fraction used for beam fitting.
    #[must_use]
    pub const fn psf_cutoff(self) -> f32 {
        self.psf_cutoff
    }

    /// Select the exact analytic primary-beam law used by requested PB products.
    #[must_use]
    pub const fn with_primary_beam_model(mut self, model: AnalyticPrimaryBeamModel) -> Self {
        self.primary_beam_model = Some(model);
        self
    }

    /// Return the explicitly selected analytic primary-beam law, if any.
    #[must_use]
    pub const fn primary_beam_model(self) -> Option<AnalyticPrimaryBeamModel> {
        self.primary_beam_model
    }
}

impl Default for ContinuumProductControls {
    fn default() -> Self {
        Self {
            psf_cutoff: DEFAULT_PSF_CUTOFF,
            primary_beam_model: None,
        }
    }
}

/// Stable identity of one source-role commitment set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumCommitmentId([u8; 32]);

impl ContinuumCommitmentId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Stable identity of one planned continuum product generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedGenerationId([u8; 32]);

impl PlannedGenerationId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Stable content-addressed identity of one planned member artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemberArtifactId([u8; 32]);

impl MemberArtifactId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Stable identity of one closed typed completions record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumCompletionsId([u8; 32]);

impl ContinuumCompletionsId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Stable identity of one authorized Product Generation seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumSealId([u8; 32]);

impl ContinuumSealId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// One two-phase Product Generation Authority bound to one compiled graph.
#[derive(Debug)]
pub struct ProductGenerationAuthority {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    identity: [u8; 32],
}

impl ProductGenerationAuthority {
    /// Bind one affine authority to one compiled continuum problem.
    #[must_use]
    pub fn bind(problem: &CompiledProblem) -> Self {
        let mut encoder = Encoder::new(b"casa-rs-product-generation-authority", 1);
        encoder.identity(problem.problem_id().as_bytes());
        encoder.identity(problem.product_graph().graph_id().as_bytes());
        Self {
            problem_id: problem.problem_id(),
            graph_id: problem.product_graph().graph_id(),
            identity: encoder.finish(),
        }
    }

    /// Return the exact compiled problem this authority plans against.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Plan the exact Product Graph over closed typed source commitments.
    ///
    /// Every publication member of the compiler-owned graph receives one
    /// derived artifact identity binding its role, name, schema, shape,
    /// physical unit, beam rule, validity rule, dependencies, and exact
    /// WCS/axes law under this algorithm catalog version.
    ///
    /// # Errors
    ///
    /// Rejects catalogs minted against another problem or graph.
    pub fn plan(
        &self,
        sources: &ContinuumSourceCatalog,
        controls: &ContinuumProductControls,
    ) -> Result<PlannedContinuumGeneration, ProductsError> {
        if sources.graph_id() != self.graph_id || sources.problem().problem_id() != self.problem_id
        {
            return Err(ProductsError::ForeignPlannedGeneration);
        }
        let graph = sources.problem().product_graph();
        let commitment_id = ContinuumCommitmentId(sources.commitment_id());
        let mut members = Vec::with_capacity(graph.publication().members().len());
        for node_ordinal in graph.publication().members() {
            let node = graph
                .nodes()
                .get(node_ordinal.ordinal())
                .ok_or(ProductsError::UnsupportedProblem)?;
            ensure_producible(node.role())?;
            let axes = node.axes();
            let shape = axes.shape();
            let payload_values = shape
                .iter()
                .copied()
                .fold(1usize, |total, extent| total.saturating_mul(extent));
            let mut encoder = Encoder::new(ARTIFACT_IDENTITY_DOMAIN, ARTIFACT_IDENTITY_VERSION);
            encoder.identity(self.identity);
            encoder.identity(commitment_id.as_bytes());
            encoder.u32(CONTINUUM_ALGORITHM_CATALOG_VERSION);
            encoder.usize(node.node_id().ordinal());
            encoder.bytes(node.name().unwrap_or_default().as_bytes());
            encode_contract(&mut encoder, node);
            let artifact_id = MemberArtifactId(encoder.finish());
            members.push(PlannedMember {
                node: node.node_id(),
                role: node.role(),
                name: node.name().unwrap_or_default().to_string(),
                shape,
                payload_values,
                unit: node.unit(),
                schema: node.schema(),
                axes: axes.clone(),
                normalization: node.normalization(),
                beam_rule: node.beam(),
                validity: node.validity(),
                dependencies: node.dependencies().to_vec().into_boxed_slice(),
                artifact_id,
            });
        }

        let mut encoder = Encoder::new(PLANNED_GENERATION_DOMAIN, PLANNED_GENERATION_VERSION);
        encoder.identity(self.identity);
        encoder.identity(commitment_id.as_bytes());
        encoder.u32(CONTINUUM_ALGORITHM_CATALOG_VERSION);
        encoder.u32(controls.psf_cutoff().to_bits());
        match controls.primary_beam_model() {
            Some(AnalyticPrimaryBeamModel::CasaEvlaCommon) => encoder.u8(1),
            None => encoder.u8(0),
        }
        encoder.usize(members.len());
        for member in &members {
            encoder.identity(member.artifact_id.as_bytes());
        }
        Ok(PlannedContinuumGeneration {
            authority: self.identity,
            problem_id: self.problem_id,
            graph_id: self.graph_id,
            generation_id: PlannedGenerationId(encoder.finish()),
            commitment_id,
            psf_cutoff: controls.psf_cutoff(),
            primary_beam_model: controls.primary_beam_model(),
            members: members.into_boxed_slice(),
            final_model_generation: sources.final_model_generation(),
            reconstruction_mask_generation: sources.reconstruction_mask_generation(),
            line_reconstruction_mask_generation: sources.line_reconstruction_mask_generation(),
        })
    }

    /// Authorize one sealed generation over matching typed completions.
    ///
    /// The completions must name exactly the planned generation and its
    /// committed lineage, and every produced payload must re-digest to its
    /// claimed and planned artifact identity in the planned order with no
    /// missing, additional, reordered, or substituted members.
    ///
    /// # Errors
    ///
    /// Rejects foreign generations, mismatched commitments or member sets,
    /// and any content-identity mismatch.
    pub fn authorize(
        &self,
        planned: &PlannedContinuumGeneration,
        completions: &ContinuumProducedMembers,
    ) -> Result<SealedContinuumGeneration, ProductsError> {
        if planned.authority != self.identity
            || completions.planned_generation != planned.generation_id
        {
            return Err(ProductsError::ForeignPlannedGeneration);
        }
        if completions.commitment_id != planned.commitment_id {
            return Err(ProductsError::CommitmentMismatch);
        }
        if completions.members.len() != planned.members.len() {
            return Err(ProductsError::MemberSetMismatch {
                expected: planned.members.len(),
                actual: completions.members.len(),
            });
        }
        for (member, produced) in planned.members.iter().zip(&completions.members) {
            // The produced member must claim exactly this planned slot: same
            // graph node and same planned artifact identity. The recomputed
            // content identity must then match the produced claim, so any
            // substitution or tampering fails closed here rather than at
            // publication time.
            if member.node != produced.node || member.artifact_id != produced.artifact_id {
                return Err(ProductsError::MemberSetMismatch {
                    expected: planned.members.len(),
                    actual: completions.members.len(),
                });
            }
            if produced.payload.len() != member.payload_values {
                return Err(ProductsError::PayloadLengthMismatch {
                    expected: member.payload_values,
                    actual: produced.payload.len(),
                });
            }
            if produced.validity.len() != member.payload_values {
                return Err(ProductsError::PayloadLengthMismatch {
                    expected: member.payload_values,
                    actual: produced.validity.len(),
                });
            }
            let digest = member_content_digest(&produced.payload, &produced.validity);
            if digest != produced.digest.as_bytes() {
                return Err(ProductsError::MemberContentMismatch);
            }
        }

        let mut encoder = Encoder::new(COMPLETIONS_DOMAIN, COMPLETIONS_VERSION);
        encoder.identity(planned.generation_id.as_bytes());
        encoder.identity(planned.commitment_id.as_bytes());
        encoder.u32(CONTINUUM_ALGORITHM_CATALOG_VERSION);
        encode_beams(&mut encoder, &completions.fitted_beams);
        encode_beams(&mut encoder, &completions.restoring_beams);
        for pair in planned.members.iter().zip(&completions.members) {
            let (member, produced) = pair;
            // Bind the planned artifact identity and the exact produced
            // content together: a payload digest matching itself is not
            // authorization without this pairing.
            encoder.identity(member.artifact_id.as_bytes());
            encoder.identity(produced.digest.as_bytes());
        }
        let completions_id = ContinuumCompletionsId(encoder.finish());

        let mut encoder = Encoder::new(SEAL_DOMAIN, SEAL_VERSION);
        encoder.identity(planned.generation_id.as_bytes());
        encoder.identity(completions_id.as_bytes());
        encoder.identity(self.identity);
        let seal_id = ContinuumSealId(encoder.finish());

        let members = planned
            .members
            .iter()
            .zip(&completions.members)
            .map(|(member, produced)| SealedMember {
                node: member.node,
                name: member.name.clone(),
                artifact_id: member.artifact_id,
                content_identity: produced.digest,
                contract: SealedMemberContract {
                    role: member.role,
                    unit: member.unit,
                    schema: member.schema,
                    axes: member.axes.clone(),
                    beam_rule: member.beam_rule,
                    validity: member.validity,
                    dependencies: member.dependencies.clone(),
                },
                resolved_beams: sealed_beams(
                    member.beam_rule,
                    &completions.fitted_beams,
                    &completions.restoring_beams,
                ),
                payload: produced.payload.clone(),
                validity: produced.validity.clone().into_boxed_slice(),
            })
            .collect::<Box<[_]>>();
        Ok(SealedContinuumGeneration {
            problem_id: self.problem_id,
            graph_id: self.graph_id,
            seal_id,
            generation_id: planned.generation_id,
            completions_id,
            fitted_beams: completions.fitted_beams.clone(),
            restoring_beams: completions.restoring_beams.clone(),
            members,
        })
    }
}

fn encode_beams(encoder: &mut Encoder, beams: &[Option<RestoringBeam>]) {
    encoder.usize(beams.len());
    for beam in beams {
        match beam {
            Some(beam) => {
                encoder.u8(1);
                encoder.u64(beam.major_fwhm_rad().to_bits());
                encoder.u64(beam.minor_fwhm_rad().to_bits());
                encoder.u64(beam.position_angle_rad().to_bits());
            }
            None => encoder.u8(0),
        }
    }
}

fn ensure_producible(role: ProductRole) -> Result<(), ProductsError> {
    match role {
        ProductRole::Psf(_)
        | ProductRole::Residual(_)
        | ProductRole::Model(_)
        | ProductRole::Weight(_)
        | ProductRole::RestoredImage(_)
        | ProductRole::SumWeights(_)
        | ProductRole::PrimaryBeam(_)
        | ProductRole::PbCorrectedImage(_)
        | ProductRole::SpectralIndex
        | ProductRole::SpectralIndexError
        | ProductRole::Sensitivity
        | ProductRole::CleanMask
        | ProductRole::ContinuumCleanMask
        | ProductRole::LineCleanMask => Ok(()),
        role => Err(ProductsError::UnsupportedProductRole {
            role,
            catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
        }),
    }
}

/// Bind every compiled contract dimension of one node into an encoder.
fn encode_contract(encoder: &mut Encoder, node: &casa_imaging_model::ProductNode) {
    encoder.u8(match node.schema() {
        ProductSchema::ImageF32V1 => 0,
        ProductSchema::LogicalCollectionV1 => 1,
        ProductSchema::EmbeddedImageMetadataV1 => 2,
        ProductSchema::InternalImageF32V1 => 3,
    });
    encoder.u8(match node.unit() {
        ProductUnit::NotApplicable => 0,
        ProductUnit::JyPerBeam => 1,
        ProductUnit::JyPerPixel => 2,
        ProductUnit::Dimensionless => 3,
        ProductUnit::VisibilityWeight => 4,
    });
    encoder.u8(match node.beam() {
        ProductBeamRule::None => 0,
        ProductBeamRule::Fitted => 1,
        ProductBeamRule::Restoring(_) => 2,
        ProductBeamRule::Inherit(_) => 3,
        ProductBeamRule::Metadata(_) => 4,
    });
    encoder.u8(match node.validity() {
        ProductValidityRule::All => 0,
        ProductValidityRule::FinalNormalState => 1,
        ProductValidityRule::PrimaryBeam(_) => 2,
        ProductValidityRule::Taylor(_) => 3,
        ProductValidityRule::TaylorAndPrimaryBeam { .. } => 4,
    });
    let axes = node.axes();
    for extent in axes.shape() {
        encoder.usize(extent);
    }
    for dependency in node.dependencies() {
        encoder.usize(dependency.ordinal());
    }
}

/// Schema-versioned planned continuum generation.
///
/// Minted only by [`ProductGenerationAuthority::plan`]:
///
/// ```compile_fail
/// use casa_imaging_products::PlannedContinuumGeneration;
///
/// let _ = PlannedContinuumGeneration {};
/// ```
#[derive(Debug)]
pub struct PlannedContinuumGeneration {
    authority: [u8; 32],
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    generation_id: PlannedGenerationId,
    commitment_id: ContinuumCommitmentId,
    psf_cutoff: f32,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    members: Box<[PlannedMember]>,
    final_model_generation: casa_imaging_reconstruction::ModelGenerationId,
    reconstruction_mask_generation:
        Option<casa_imaging_reconstruction::ReconstructionMaskGenerationId>,
    line_reconstruction_mask_generation:
        Option<casa_imaging_reconstruction::ReconstructionMaskGenerationId>,
}

impl PlannedContinuumGeneration {
    /// Return the exact compiled problem this generation was planned for.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiler-owned Product Graph this generation realizes.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the stable planned-generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return the committed source-lineage digest behind the plan.
    #[must_use]
    pub const fn commitment_id(&self) -> ContinuumCommitmentId {
        self.commitment_id
    }

    /// Return planned members in exact publication order.
    #[must_use]
    pub const fn members(&self) -> &[PlannedMember] {
        &self.members
    }

    /// Return the named final model generation this plan restores from.
    #[must_use]
    pub const fn final_model_generation(&self) -> casa_imaging_reconstruction::ModelGenerationId {
        self.final_model_generation
    }

    /// Return the beam-fitting cutoff bound into this plan.
    #[must_use]
    pub const fn psf_cutoff(&self) -> f32 {
        self.psf_cutoff
    }

    /// Return the analytic primary-beam law bound into this plan.
    #[must_use]
    pub const fn primary_beam_model(&self) -> Option<AnalyticPrimaryBeamModel> {
        self.primary_beam_model
    }

    pub(crate) const fn reconstruction_mask_generation(
        &self,
    ) -> Option<casa_imaging_reconstruction::ReconstructionMaskGenerationId> {
        self.reconstruction_mask_generation
    }

    pub(crate) const fn line_reconstruction_mask_generation(
        &self,
    ) -> Option<casa_imaging_reconstruction::ReconstructionMaskGenerationId> {
        self.line_reconstruction_mask_generation
    }
}

/// One planned publication member in exact graph order.
#[derive(Debug, Clone)]
pub struct PlannedMember {
    node: ProductNodeId,
    role: ProductRole,
    name: String,
    shape: [usize; 4],
    payload_values: usize,
    unit: ProductUnit,
    schema: ProductSchema,
    axes: ProductAxes,
    normalization: Option<ProductNormalization>,
    beam_rule: ProductBeamRule,
    validity: ProductValidityRule,
    dependencies: Box<[ProductNodeId]>,
    artifact_id: MemberArtifactId,
}

impl PlannedMember {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }

    /// Return the logical product role.
    #[must_use]
    pub const fn role(&self) -> ProductRole {
        self.role
    }

    /// Return the compiled product name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the declared four-axis shape.
    #[must_use]
    pub const fn shape(&self) -> [usize; 4] {
        self.shape
    }

    /// Return the required physical unit of this member.
    #[must_use]
    pub const fn unit(&self) -> ProductUnit {
        self.unit
    }

    /// Return the backend-independent logical payload schema.
    #[must_use]
    pub const fn schema(&self) -> ProductSchema {
        self.schema
    }

    /// Return the exact WCS and storage-axis binding.
    #[must_use]
    pub const fn axes(&self) -> &ProductAxes {
        &self.axes
    }

    /// Return fitted, restoring, inherited, or absent beam semantics.
    #[must_use]
    pub const fn beam_rule(&self) -> ProductBeamRule {
        self.beam_rule
    }

    /// Return the output-validity rule of this member.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityRule {
        self.validity
    }

    /// Return graph-node dependencies, all of which precede this node.
    #[must_use]
    pub const fn dependencies(&self) -> &[ProductNodeId] {
        &self.dependencies
    }

    /// Return the planned payload value count.
    #[must_use]
    pub const fn payload_values(&self) -> usize {
        self.payload_values
    }

    /// Return the compiled normalization of this member, when one applies.
    #[must_use]
    pub const fn normalization(&self) -> Option<ProductNormalization> {
        self.normalization
    }

    /// Return the derived artifact identity of this member.
    #[must_use]
    pub const fn artifact_id(&self) -> MemberArtifactId {
        self.artifact_id
    }
}

/// Produced artifacts awaiting authorization.
///
/// Minted only by [`produce_continuum_members`].
#[derive(Debug)]
pub struct ContinuumProducedMembers {
    planned_generation: PlannedGenerationId,
    commitment_id: ContinuumCommitmentId,
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[ProducedMember]>,
}

/// One produced member payload with its computed content identity.
#[derive(Debug)]
struct ProducedMember {
    node: ProductNodeId,
    artifact_id: MemberArtifactId,
    digest: MemberArtifactId,
    payload: Vec<f32>,
    validity: Vec<bool>,
}

/// Produce every planned member through the continuum algorithm catalog.
///
/// Runs restoring-beam fitting, restoration, residual scaling, normalization,
/// validity, and metadata exactly once per member, in the planned publication
/// order.
///
/// # Errors
///
/// Rejects plans minted from other lineages, unsupported roles, failed beam
/// fits, and generated non-finite payloads.
pub fn produce_continuum_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<ContinuumProducedMembers, ProductsError> {
    if inputs.final_model().generation_id() != planned.final_model_generation {
        return Err(ProductsError::CommitmentMismatch);
    }
    if inputs
        .reconstruction_mask()
        .map(casa_imaging_reconstruction::ReconstructionMask::generation_id)
        != planned.reconstruction_mask_generation
    {
        return Err(ProductsError::CommitmentMismatch);
    }
    if inputs
        .coupled_reconstruction_masks()
        .map(|masks| masks.line().generation_id())
        != planned.line_reconstruction_mask_generation
    {
        return Err(ProductsError::CommitmentMismatch);
    }
    if inputs.normal_state().catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1 {
        return produce_taylor_members(planned, inputs);
    }
    if inputs.normal_state().catalog() == NormalStateCatalog::UnnormalizedJointBlockV1 {
        return produce_joint_members(planned, inputs);
    }
    let normal_state = inputs.normal_state();
    let plane_shape = normal_state.shape();
    let channel_count = normal_state.channel_count();
    if normal_state.slab().core_range() != (0..normal_state.slab().total_channels())
        || channel_count != normal_state.slab().total_channels()
        || inputs.final_model().shape().coefficients() != channel_count
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let requires_beam = planned
        .members
        .iter()
        .any(|member| member.beam_rule != ProductBeamRule::None);
    let fitted_beams = if requires_beam {
        (0..channel_count)
            .map(|local_channel| {
                let plane = normal_state
                    .plane(local_channel)
                    .ok_or(ProductsError::SourceLineageMismatch)?;
                if plane.validity() == SpectralChannelValidity::Valid {
                    fit_restoring_beam(
                        &psf_real_plane(plane),
                        plane_shape,
                        inputs.cell_size_rad(),
                        planned.psf_cutoff(),
                    )
                    .map(Some)
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Box<[_]>, _>>()?
    } else {
        Box::new([])
    };
    let restoring_beams = match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane => fitted_beams.clone(),
        RestoringBeamPolicy::Common => {
            let valid = fitted_beams.iter().flatten().copied().collect::<Vec<_>>();
            if valid.is_empty() {
                fitted_beams.clone()
            } else {
                let common = RestoringBeam::common_enclosing(&valid)
                    .map_err(|error| ProductsError::BeamFitFailed(error.to_string()))?;
                fitted_beams
                    .iter()
                    .map(|fitted| fitted.map(|_| common))
                    .collect()
            }
        }
    };

    let mut members = Vec::with_capacity(planned.members.len());
    for member in &planned.members {
        let mut payload = vec![0.0_f32; member.payload_values];
        let mut validity = vec![true; member.payload_values];
        for local_channel in 0..channel_count {
            let plane = normal_state
                .plane(local_channel)
                .ok_or(ProductsError::SourceLineageMismatch)?;
            let output_channel = plane.output_channel();
            if member.validity != ProductValidityRule::All {
                let plane_validity = product_plane_validity(member.validity, plane, plane_shape)?;
                scatter_image_plane(
                    &mut validity,
                    member.axes(),
                    output_channel,
                    plane_shape,
                    &plane_validity,
                )?;
            }
            if matches!(member.role, ProductRole::SumWeights(_)) {
                scatter_plane_state(
                    &mut payload,
                    member.axes(),
                    output_channel,
                    plane.sum_weight() as f32,
                )?;
                continue;
            }
            let plane_payload = produce_plane_member(
                member,
                inputs,
                plane,
                fitted_beams.get(local_channel).copied().flatten(),
                restoring_beams.get(local_channel).copied().flatten(),
            )?;
            scatter_image_plane(
                &mut payload,
                member.axes(),
                output_channel,
                plane_shape,
                &plane_payload,
            )?;
        }
        if payload.iter().any(|value| value.is_infinite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        if payload.len() != member.payload_values {
            return Err(ProductsError::PayloadLengthMismatch {
                expected: member.payload_values,
                actual: payload.len(),
            });
        }
        let digest = MemberArtifactId(member_content_digest(&payload, &validity));
        members.push(ProducedMember {
            node: member.node,
            artifact_id: member.artifact_id,
            digest,
            payload,
            validity,
        });
    }

    Ok(ContinuumProducedMembers {
        planned_generation: planned.generation_id,
        commitment_id: planned.commitment_id,
        fitted_beams,
        restoring_beams,
        members: members.into_boxed_slice(),
    })
}

fn produce_taylor_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<ContinuumProducedMembers, ProductsError> {
    let products = TaylorProducts::build(inputs, planned.psf_cutoff, planned.primary_beam_model)?;
    let requires_beam = planned
        .members
        .iter()
        .any(|member| member.beam_rule != ProductBeamRule::None);
    let fitted_beams = if requires_beam {
        vec![products.fitted_beam()].into_boxed_slice()
    } else {
        Box::new([])
    };
    let restoring_beams = match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane | RestoringBeamPolicy::Common => {
            vec![products.restoring_beam()].into_boxed_slice()
        }
    };
    let mut members = Vec::with_capacity(planned.members.len());
    for member in &planned.members {
        let payload = products.payload(member.role)?;
        let validity = if matches!(
            member.validity,
            ProductValidityRule::All | ProductValidityRule::FinalNormalState
        ) {
            vec![true; member.payload_values]
        } else {
            products.validity(member.validity)?
        };
        if payload.len() != member.payload_values || validity.len() != member.payload_values {
            return Err(ProductsError::PayloadLengthMismatch {
                expected: member.payload_values,
                actual: payload.len().max(validity.len()),
            });
        }
        if payload.iter().any(|value| !value.is_finite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        let digest = MemberArtifactId(member_content_digest(&payload, &validity));
        members.push(ProducedMember {
            node: member.node,
            artifact_id: member.artifact_id,
            digest,
            payload,
            validity,
        });
    }
    Ok(ContinuumProducedMembers {
        planned_generation: planned.generation_id,
        commitment_id: planned.commitment_id,
        fitted_beams,
        restoring_beams,
        members: members.into_boxed_slice(),
    })
}

fn produce_joint_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<ContinuumProducedMembers, ProductsError> {
    let normal = inputs.normal_state();
    let shape = normal.shape();
    let channels = normal.slab().total_channels();
    if normal.slab().core_range() != (0..channels)
        || inputs.coupled_reconstruction_masks().is_none()
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let ReconstructionBasis::JointContinuumLine {
        continuum_terms,
        line_terms,
    } = inputs.problem().reconstruction().basis()
    else {
        return Err(ProductsError::SourceLineageMismatch);
    };
    if normal.coefficient_term_count() != continuum_terms + line_terms
        || normal.normal_moment_count() != (continuum_terms + line_terms).pow(2)
        || inputs.final_model().shape().coefficients() != continuum_terms + line_terms
        || normal.channel_sum_weights().len() != channels
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let h00 = normal
        .normal_block(0, 0)
        .ok_or(ProductsError::SourceLineageMismatch)?;
    let normalization_weight = h00.sum_weight();
    if !normalization_weight.is_finite() || normalization_weight <= 0.0 {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let requires_beam = planned
        .members
        .iter()
        .any(|member| member.beam_rule != ProductBeamRule::None);
    let fitted_beam = requires_beam
        .then(|| {
            fit_restoring_beam(
                &h00.normal_approximation()
                    .iter()
                    .map(|value| value.re as f32)
                    .collect::<Vec<_>>(),
                shape,
                inputs.cell_size_rad(),
                planned.psf_cutoff(),
            )
        })
        .transpose()?;
    let restoring_beam = match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => None,
        RestoringBeamPolicy::PerPlane | RestoringBeamPolicy::Common => fitted_beam,
    };
    let fitted_beams = fitted_beam.map_or_else(Box::default, |beam| {
        vec![Some(beam); channels].into_boxed_slice()
    });
    let restoring_beams = restoring_beam.map_or_else(Box::default, |beam| {
        vec![Some(beam); channels].into_boxed_slice()
    });
    let mut members = Vec::with_capacity(planned.members.len());
    for member in &planned.members {
        let (payload, validity) = produce_joint_member(
            member,
            inputs,
            shape,
            channels,
            continuum_terms,
            normalization_weight,
            fitted_beam,
            restoring_beam,
        )?;
        if payload.len() != member.payload_values || validity.len() != member.payload_values {
            return Err(ProductsError::PayloadLengthMismatch {
                expected: member.payload_values,
                actual: payload.len(),
            });
        }
        if payload.iter().any(|value| !value.is_finite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        let digest = MemberArtifactId(member_content_digest(&payload, &validity));
        members.push(ProducedMember {
            node: member.node,
            artifact_id: member.artifact_id,
            digest,
            payload,
            validity,
        });
    }
    Ok(ContinuumProducedMembers {
        planned_generation: planned.generation_id,
        commitment_id: planned.commitment_id,
        fitted_beams,
        restoring_beams,
        members: members.into_boxed_slice(),
    })
}

#[allow(clippy::too_many_arguments)]
fn produce_joint_member(
    member: &PlannedMember,
    inputs: &ContinuumProductInputs<'_>,
    shape: [usize; 2],
    channels: usize,
    continuum_terms: usize,
    normalization_weight: f64,
    fitted_beam: Option<RestoringBeam>,
    restoring_beam: Option<RestoringBeam>,
) -> Result<(Vec<f32>, Vec<bool>), ProductsError> {
    let mut payload = vec![0.0_f32; member.payload_values];
    let mut validity = vec![true; member.payload_values];
    match member.role {
        ProductRole::Psf(ProductTerm::JointNormal { row, column }) => {
            let block = inputs
                .normal_state()
                .normal_block(row, column)
                .ok_or(ProductsError::SourceLineageMismatch)?;
            payload = normalize_plane(
                &block
                    .normal_approximation()
                    .iter()
                    .map(|value| value.re as f32)
                    .collect::<Vec<_>>(),
                member
                    .normalization
                    .unwrap_or(ProductNormalization::UnitResponse),
                normalization_weight,
            )?;
        }
        ProductRole::SumWeights(ProductTerm::JointNormal { row, column }) => {
            let block = inputs
                .normal_state()
                .normal_block(row, column)
                .ok_or(ProductsError::SourceLineageMismatch)?;
            scatter_plane_state(&mut payload, member.axes(), 0, block.sum_weight() as f32)?;
        }
        ProductRole::Weight(ProductTerm::JointNormal { row, column }) => {
            let block = inputs
                .normal_state()
                .normal_block(row, column)
                .ok_or(ProductsError::SourceLineageMismatch)?;
            payload = block
                .sensitivity()
                .iter()
                .map(|value| *value as f32)
                .collect();
        }
        ProductRole::Model(ProductTerm::Continuum(term)) => {
            payload = model_real_plane(inputs.final_model(), term, shape)?;
        }
        ProductRole::Model(ProductTerm::Line) | ProductRole::Model(ProductTerm::Total) => {
            for channel in 0..channels {
                let plane = evaluate_joint_model_plane(
                    inputs,
                    shape,
                    channel,
                    continuum_terms,
                    matches!(member.role, ProductRole::Model(ProductTerm::Line)),
                )?;
                scatter_image_plane(&mut payload, member.axes(), channel, shape, &plane)?;
            }
        }
        ProductRole::Residual(ProductTerm::Total) => {
            for channel in 0..channels {
                let plane = evaluate_joint_residual_plane(
                    inputs,
                    shape,
                    channel,
                    required_normalization(member)?,
                )?;
                scatter_image_plane(&mut payload, member.axes(), channel, shape, &plane)?;
                if inputs.normal_state().channel_validity()[channel]
                    != SpectralChannelValidity::Valid
                {
                    let blank = vec![false; shape[0] * shape[1]];
                    scatter_image_plane(&mut validity, member.axes(), channel, shape, &blank)?;
                }
            }
        }
        ProductRole::RestoredImage(ProductTerm::Line | ProductTerm::Total) => {
            let fitted_beam = fitted_beam.ok_or_else(|| {
                ProductsError::BeamFitFailed("joint restoration requires a fitted beam".to_string())
            })?;
            let restoring_beam = restoring_beam.ok_or_else(|| {
                ProductsError::BeamFitFailed(
                    "joint restoration requires a restoring beam".to_string(),
                )
            })?;
            let line_only = member.role == ProductRole::RestoredImage(ProductTerm::Line);
            let kernel = gaussian_beam_image(shape, &restoring_beam, inputs.cell_size_rad());
            for channel in 0..channels {
                let model =
                    evaluate_joint_model_plane(inputs, shape, channel, continuum_terms, line_only)?;
                let mut restored =
                    fft_convolve(&model, kernel.as_slice().expect("contiguous"), shape);
                let residual = evaluate_joint_residual_plane(
                    inputs,
                    shape,
                    channel,
                    required_normalization(member)?,
                )?;
                let residual = rescale_residual_to_beam(
                    &residual,
                    shape,
                    inputs.cell_size_rad(),
                    fitted_beam,
                    restoring_beam,
                )?
                .into_values();
                for (restored, residual) in restored.iter_mut().zip(residual) {
                    *restored += residual;
                }
                scatter_image_plane(&mut payload, member.axes(), channel, shape, &restored)?;
                if inputs.normal_state().channel_validity()[channel]
                    != SpectralChannelValidity::Valid
                {
                    let blank = vec![false; shape[0] * shape[1]];
                    scatter_image_plane(&mut validity, member.axes(), channel, shape, &blank)?;
                }
            }
        }
        ProductRole::ContinuumCleanMask | ProductRole::LineCleanMask => {
            let masks = inputs
                .coupled_reconstruction_masks()
                .ok_or(ProductsError::SourceLineageMismatch)?;
            let mask = if member.role == ProductRole::ContinuumCleanMask {
                masks.continuum()
            } else {
                masks.line()
            };
            for channel in 0..channels {
                let plane = mask
                    .support()
                    .iter()
                    .map(|selected| if *selected { 1.0 } else { 0.0 })
                    .collect::<Vec<_>>();
                scatter_image_plane(&mut payload, member.axes(), channel, shape, &plane)?;
            }
        }
        ProductRole::Sensitivity => {
            for channel in 0..channels {
                let plane = h00_sensitivity(inputs)?;
                scatter_image_plane(&mut payload, member.axes(), channel, shape, &plane)?;
            }
        }
        role => {
            return Err(ProductsError::UnsupportedProductRole {
                role,
                catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
            });
        }
    }
    Ok((payload, validity))
}

fn evaluate_joint_model_plane(
    inputs: &ContinuumProductInputs<'_>,
    shape: [usize; 2],
    channel: usize,
    continuum_terms: usize,
    line_only: bool,
) -> Result<Vec<f32>, ProductsError> {
    let mut output = vec![0.0_f32; shape[0] * shape[1]];
    if !line_only {
        let reference = inputs
            .normal_state()
            .reference_frequency_hz()
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let frequency = inputs
            .problem()
            .geometry()
            .spectral()
            .channel_centre_hz(channel)
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let x = (frequency - reference) / reference;
        for term in 0..continuum_terms {
            let plane = model_real_plane(inputs.final_model(), term, shape)?;
            let factor =
                x.powi(i32::try_from(term).map_err(|_| ProductsError::UnsupportedProblem)?);
            for (output, value) in output.iter_mut().zip(plane) {
                *output += (factor * f64::from(value)) as f32;
            }
        }
    }
    if let Some(line) = joint_line_term(inputs.problem(), channel)? {
        let plane = model_real_plane(inputs.final_model(), continuum_terms + line, shape)?;
        for (output, value) in output.iter_mut().zip(plane) {
            *output += value;
        }
    }
    Ok(output)
}

fn evaluate_joint_residual_plane(
    inputs: &ContinuumProductInputs<'_>,
    shape: [usize; 2],
    channel: usize,
    normalization: ProductNormalization,
) -> Result<Vec<f32>, ProductsError> {
    let residual = inputs
        .normal_state()
        .joint_common_residual(channel)
        .ok_or(ProductsError::SourceLineageMismatch)?;
    if residual.len() != shape[0] * shape[1] {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let output = residual
        .iter()
        .map(|value| value.re as f32)
        .collect::<Vec<_>>();
    let normalization_weight = *inputs
        .normal_state()
        .channel_sum_weights()
        .get(channel)
        .ok_or(ProductsError::SourceLineageMismatch)?;
    normalize_plane(&output, normalization, normalization_weight)
}

fn joint_line_term(
    problem: &CompiledProblem,
    channel: usize,
) -> Result<Option<usize>, ProductsError> {
    let contract = problem
        .reconstruction()
        .joint_continuum_line()
        .ok_or(ProductsError::SourceLineageMismatch)?;
    Ok(contract.line_channels().binary_search(&channel).ok())
}

fn h00_sensitivity(inputs: &ContinuumProductInputs<'_>) -> Result<Vec<f32>, ProductsError> {
    Ok(inputs
        .normal_state()
        .normal_block(0, 0)
        .ok_or(ProductsError::SourceLineageMismatch)?
        .sensitivity()
        .iter()
        .map(|value| *value as f32)
        .collect())
}

fn produce_plane_member(
    member: &PlannedMember,
    inputs: &ContinuumProductInputs<'_>,
    plane: FinalNormalStatePlane<'_>,
    fitted_beam: Option<RestoringBeam>,
    restoring_beam: Option<RestoringBeam>,
) -> Result<Vec<f32>, ProductsError> {
    let sensitivity = plane.sum_weight();
    let valid = plane.validity() == SpectralChannelValidity::Valid
        && sensitivity.is_finite()
        && sensitivity > 0.0;
    let shape = plane.shape();
    let cells = shape[0] * shape[1];
    let invalid_residual = || vec![0.0; cells];
    match member.role {
        ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
        | ProductRole::Psf(casa_imaging_model::ProductTerm::Taylor(0)) => {
            if valid {
                normalize_plane(
                    &psf_real_plane(plane),
                    member
                        .normalization
                        .unwrap_or(ProductNormalization::UnitResponse),
                    sensitivity,
                )
            } else {
                Ok(vec![0.0; cells])
            }
        }
        ProductRole::Residual(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if valid {
                normalize_plane(
                    &residual_real_plane(plane),
                    required_normalization(member)?,
                    sensitivity,
                )
            } else {
                Ok(invalid_residual())
            }
        }
        ProductRole::Model(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => model_real_plane(inputs.final_model(), plane.output_channel(), shape),
        ProductRole::Weight(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        )
        | ProductRole::Sensitivity => Ok(plane
            .sensitivity()
            .iter()
            .map(|value| *value as f32)
            .collect()),
        ProductRole::RestoredImage(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if !valid {
                return Ok(invalid_residual());
            }
            let Some(beam) = restoring_beam.as_ref() else {
                return Err(ProductsError::BeamFitFailed(
                    "restoration requires a fitted beam for every valid plane".to_string(),
                ));
            };
            let model = model_real_plane(inputs.final_model(), plane.output_channel(), shape)?;
            let kernel = gaussian_beam_image(shape, beam, inputs.cell_size_rad());
            let mut restored = fft_convolve(&model, kernel.as_slice().expect("contiguous"), shape);
            let residual = normalize_plane(
                &residual_real_plane(plane),
                required_normalization(member)?,
                sensitivity,
            )?;
            let fitted_beam = fitted_beam.ok_or_else(|| {
                ProductsError::BeamFitFailed(
                    "restoration requires a fitted beam for every valid plane".to_string(),
                )
            })?;
            let residual = rescale_residual_to_beam(
                &residual,
                shape,
                inputs.cell_size_rad(),
                fitted_beam,
                *beam,
            )?
            .into_values();
            for (restored, residual) in restored.iter_mut().zip(residual) {
                *restored += residual;
            }
            Ok(restored)
        }
        ProductRole::CleanMask => Ok(plane
            .sensitivity()
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let selected = inputs
                    .reconstruction_mask()
                    .is_none_or(|mask| mask.support()[index]);
                if valid && selected && *value > 0.0 && value.is_finite() {
                    1.0
                } else {
                    0.0
                }
            })
            .collect()),
        role => Err(ProductsError::UnsupportedProductRole {
            role,
            catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
        }),
    }
}

fn required_normalization(member: &PlannedMember) -> Result<ProductNormalization, ProductsError> {
    member
        .normalization
        .ok_or(ProductsError::UnsupportedProblem)
}

fn psf_real_plane(plane: FinalNormalStatePlane<'_>) -> Vec<f32> {
    plane
        .normal_approximation()
        .iter()
        .map(|value| value.re as f32)
        .collect()
}

fn residual_real_plane(plane: FinalNormalStatePlane<'_>) -> Vec<f32> {
    plane
        .residual()
        .iter()
        .map(|value| value.re as f32)
        .collect()
}

fn product_plane_validity(
    rule: ProductValidityRule,
    plane: FinalNormalStatePlane<'_>,
    shape: [usize; 2],
) -> Result<Vec<bool>, ProductsError> {
    match rule {
        ProductValidityRule::All => Ok(vec![true; shape[0] * shape[1]]),
        ProductValidityRule::FinalNormalState => {
            let valid = plane.validity() == SpectralChannelValidity::Valid
                && plane.sum_weight().is_finite()
                && plane.sum_weight() > 0.0;
            Ok(vec![valid; shape[0] * shape[1]])
        }
        ProductValidityRule::PrimaryBeam(_)
        | ProductValidityRule::Taylor(_)
        | ProductValidityRule::TaylorAndPrimaryBeam { .. } => {
            Err(ProductsError::UnsupportedProblem)
        }
    }
}

fn model_real_plane(
    model: &ModelGeneration,
    output_channel: usize,
    plane_shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    let [width, height] = plane_shape;
    if model.shape().domains().len() != 1
        || output_channel >= model.shape().coefficients()
        || model.shape().polarizations() != 1
        || model.shape().domains()[0].pixels() != plane_shape
        || model.samples().len() != model.shape().sample_count()
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    // Canonical model order is y-major (`flat = y * W + x`); product planes
    // are stored x-major like every normal-state primitive.
    let mut plane = vec![0.0_f32; width * height];
    for y in 0..height {
        for x in 0..width {
            let index = model
                .shape()
                .flat_index(ModelCell::new(0, output_channel, 0, [x, y]))
                .ok_or(ProductsError::SourceLineageMismatch)?;
            plane[x * height + y] = model.samples()[index].value().value() as f32;
        }
    }
    Ok(plane)
}

fn scatter_image_plane<T: Copy>(
    payload: &mut [T],
    axes: &ProductAxes,
    output_channel: usize,
    plane_shape: [usize; 2],
    plane: &[T],
) -> Result<(), ProductsError> {
    let [width, height] = plane_shape;
    if plane.len() != width * height {
        return Err(ProductsError::SourceLineageMismatch);
    }
    for x in 0..width {
        for y in 0..height {
            let offset = product_offset(axes, x, y, 0, output_channel)?;
            payload[offset] = plane[x * height + y];
        }
    }
    Ok(())
}

fn scatter_plane_state(
    payload: &mut [f32],
    axes: &ProductAxes,
    output_channel: usize,
    value: f32,
) -> Result<(), ProductsError> {
    let offset = product_offset(axes, 0, 0, 0, output_channel)?;
    payload[offset] = value;
    Ok(())
}

fn product_offset(
    axes: &ProductAxes,
    longitude: usize,
    latitude: usize,
    polarization: usize,
    spectral: usize,
) -> Result<usize, ProductsError> {
    let mut offset = 0usize;
    for (position, axis) in axes.order().positions().iter().enumerate() {
        let coordinate = match axis {
            ImageAxis::DirectionLongitude => longitude,
            ImageAxis::DirectionLatitude => latitude,
            ImageAxis::Polarization => polarization,
            ImageAxis::Spectral => spectral,
        };
        let extent = axes.shape()[position];
        if coordinate >= extent {
            return Err(ProductsError::SourceLineageMismatch);
        }
        offset = offset
            .checked_mul(extent)
            .and_then(|offset| offset.checked_add(coordinate))
            .ok_or(ProductsError::SourceLineageMismatch)?;
    }
    Ok(offset)
}

/// The complete compiled contract carried by one sealed member.
#[derive(Debug, Clone)]
pub struct SealedMemberContract {
    role: ProductRole,
    unit: ProductUnit,
    schema: ProductSchema,
    axes: ProductAxes,
    beam_rule: ProductBeamRule,
    validity: ProductValidityRule,
    dependencies: Box<[ProductNodeId]>,
}

impl SealedMemberContract {
    /// Return the exact logical product meaning.
    #[must_use]
    pub const fn role(&self) -> ProductRole {
        self.role
    }

    /// Return the required physical unit.
    #[must_use]
    pub const fn unit(&self) -> ProductUnit {
        self.unit
    }

    /// Return the backend-independent logical payload schema.
    #[must_use]
    pub const fn schema(&self) -> ProductSchema {
        self.schema
    }

    /// Return the exact WCS and storage-axis binding.
    #[must_use]
    pub const fn axes(&self) -> &ProductAxes {
        &self.axes
    }

    /// Return fitted, restoring, inherited, or absent beam semantics.
    #[must_use]
    pub const fn beam_rule(&self) -> ProductBeamRule {
        self.beam_rule
    }

    /// Return the output-validity rule.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityRule {
        self.validity
    }

    /// Return graph-node dependencies, all of which precede this node.
    #[must_use]
    pub const fn dependencies(&self) -> &[ProductNodeId] {
        &self.dependencies
    }
}

/// One authorized member of an exactly-once sealed member set.
#[derive(Debug, Clone)]
pub struct SealedMember {
    node: ProductNodeId,
    name: String,
    artifact_id: MemberArtifactId,
    content_identity: MemberArtifactId,
    contract: SealedMemberContract,
    resolved_beams: Box<[Option<RestoringBeam>]>,
    payload: Vec<f32>,
    validity: Box<[bool]>,
}

impl SealedMember {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }

    /// Return the compiled product name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the planned-and-sealed artifact identity.
    #[must_use]
    pub const fn artifact_id(&self) -> MemberArtifactId {
        self.artifact_id
    }

    /// Return the content identity bound to this member's artifact identity
    /// by the authorization seal.
    #[must_use]
    pub const fn content_identity(&self) -> MemberArtifactId {
        self.content_identity
    }

    /// Return the complete compiled contract of this member.
    #[must_use]
    pub const fn contract(&self) -> &SealedMemberContract {
        &self.contract
    }

    /// Return the single resolved beam when exactly one valid plane exists.
    #[must_use]
    pub fn resolved_beam(&self) -> Option<&RestoringBeam> {
        match self.resolved_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }

    /// Return resolved beam metadata in output-channel order.
    ///
    /// Blank and unmapped channel planes carry `None` rather than fabricated
    /// beam metadata.
    #[must_use]
    pub const fn resolved_beams(&self) -> &[Option<RestoringBeam>] {
        &self.resolved_beams
    }

    /// Borrow the sealed binary32 payload.
    #[must_use]
    pub fn payload(&self) -> &[f32] {
        &self.payload
    }

    /// Borrow the sealed product-validity mask in the same storage order as
    /// the numeric payload.
    ///
    /// This is independent of the numeric CLEAN-mask product. Invalid or
    /// unmapped spectral planes are false here even when a CASA-compatible
    /// persistence beam placeholder is later required.
    #[must_use]
    pub fn validity(&self) -> &[bool] {
        &self.validity
    }
}

/// Resolve one member's compiled beam rule against the fitted generation beam.
fn sealed_beams(
    rule: ProductBeamRule,
    fitted: &[Option<RestoringBeam>],
    restoring: &[Option<RestoringBeam>],
) -> Box<[Option<RestoringBeam>]> {
    match rule {
        ProductBeamRule::None => Box::new([]),
        ProductBeamRule::Restoring(RestoringBeamPolicy::None)
        | ProductBeamRule::Metadata(RestoringBeamPolicy::None) => Box::new([]),
        ProductBeamRule::Fitted => fitted.into(),
        ProductBeamRule::Restoring(RestoringBeamPolicy::PerPlane)
        | ProductBeamRule::Metadata(RestoringBeamPolicy::PerPlane) => restoring.into(),
        ProductBeamRule::Restoring(RestoringBeamPolicy::Common)
        | ProductBeamRule::Metadata(RestoringBeamPolicy::Common) => {
            vec![restoring.iter().flatten().next().copied()].into_boxed_slice()
        }
        // Inherit rules resolve through their referenced member; the
        // embedded metadata is identical because the generation carries one
        // fitted beam set, so resolving to it preserves the contract.
        ProductBeamRule::Inherit(_) => restoring.into(),
    }
}

/// One authorized continuum generation carrying its exact sealed member set.
///
/// Minted only by [`ProductGenerationAuthority::authorize`]:
///
/// ```compile_fail
/// use casa_imaging_products::SealedContinuumGeneration;
///
/// let _ = SealedContinuumGeneration {};
/// ```
#[derive(Debug)]
pub struct SealedContinuumGeneration {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    seal_id: ContinuumSealId,
    generation_id: PlannedGenerationId,
    completions_id: ContinuumCompletionsId,
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[SealedMember]>,
}

impl SealedContinuumGeneration {
    /// Return the exact compiled problem authorized by this seal.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiled Product Graph authorized by this seal.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the Product Generation seal identity.
    #[must_use]
    pub const fn seal_id(&self) -> ContinuumSealId {
        self.seal_id
    }

    /// Return the planned generation this seal authorizes.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return the typed completions record behind this seal.
    #[must_use]
    pub const fn completions_id(&self) -> ContinuumCompletionsId {
        self.completions_id
    }

    /// Return the fitted restoring beam, when the graph required one.
    #[must_use]
    pub fn restoring_beam(&self) -> Option<&RestoringBeam> {
        match self.restoring_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }

    /// Return all selected restoring beams in output-channel order.
    #[must_use]
    pub const fn restoring_beams(&self) -> &[Option<RestoringBeam>] {
        &self.restoring_beams
    }

    /// Return all independently fitted PSF beams in output-channel order.
    ///
    /// Common restoration may select a different beam while PSF and residual
    /// products retain this fitted topology.
    #[must_use]
    pub const fn fitted_beams(&self) -> &[Option<RestoringBeam>] {
        &self.fitted_beams
    }

    /// Return the exact sealed member set in canonical publication order.
    #[must_use]
    pub const fn members(&self) -> &[SealedMember] {
        &self.members
    }

    /// Consume staged sealed payloads into their payload-free terminal record.
    ///
    /// The publication runtime calls this only after all member arrays have
    /// been staged. Consuming the seal releases those arrays while preserving
    /// every identity, contract, name, and beam needed for terminal receipts.
    #[must_use]
    pub fn into_published_summary(self) -> PublishedContinuumGeneration {
        PublishedContinuumGeneration {
            problem_id: self.problem_id,
            graph_id: self.graph_id,
            seal_id: self.seal_id,
            generation_id: self.generation_id,
            completions_id: self.completions_id,
            fitted_beams: self.fitted_beams,
            restoring_beams: self.restoring_beams,
            members: self
                .members
                .into_vec()
                .into_iter()
                .map(|member| PublishedMember {
                    node: member.node,
                    name: member.name,
                    artifact_id: member.artifact_id,
                    content_identity: member.content_identity,
                    contract: member.contract,
                    resolved_beams: member.resolved_beams,
                })
                .collect(),
        }
    }
}

/// Payload-free terminal record for one published member.
///
/// This type has no public constructor and is minted only when a sealed
/// generation relinquishes its staged numeric and validity arrays.
#[derive(Debug, Clone)]
pub struct PublishedMember {
    node: ProductNodeId,
    name: String,
    artifact_id: MemberArtifactId,
    content_identity: MemberArtifactId,
    contract: SealedMemberContract,
    resolved_beams: Box<[Option<RestoringBeam>]>,
}

impl PublishedMember {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }

    /// Return the compiled product name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the planned-and-published artifact identity.
    #[must_use]
    pub const fn artifact_id(&self) -> MemberArtifactId {
        self.artifact_id
    }

    /// Return the exact published content identity.
    #[must_use]
    pub const fn content_identity(&self) -> MemberArtifactId {
        self.content_identity
    }

    /// Return the complete compiled member contract.
    #[must_use]
    pub const fn contract(&self) -> &SealedMemberContract {
        &self.contract
    }

    /// Return resolved beam metadata in output-channel order.
    #[must_use]
    pub const fn resolved_beams(&self) -> &[Option<RestoringBeam>] {
        &self.resolved_beams
    }
}

/// Payload-free terminal record for one published continuum generation.
///
/// This type has no public constructor and can only be obtained by consuming
/// an authorized sealed generation after its member arrays have been staged.
#[derive(Debug, Clone)]
pub struct PublishedContinuumGeneration {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    seal_id: ContinuumSealId,
    generation_id: PlannedGenerationId,
    completions_id: ContinuumCompletionsId,
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[PublishedMember]>,
}

impl PublishedContinuumGeneration {
    /// Return numeric and validity array residency retained after staging.
    #[must_use]
    pub const fn payload_residency_bytes(&self) -> u64 {
        0
    }

    /// Return the exact compiled problem authorized by the publication seal.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiled Product Graph authorized by the seal.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the terminal Product Generation seal identity.
    #[must_use]
    pub const fn seal_id(&self) -> ContinuumSealId {
        self.seal_id
    }

    /// Return the planned generation authorized by the seal.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return the typed completions identity behind this publication.
    #[must_use]
    pub const fn completions_id(&self) -> ContinuumCompletionsId {
        self.completions_id
    }

    /// Return the fitted restoring beams retained as terminal metadata.
    #[must_use]
    pub const fn fitted_beams(&self) -> &[Option<RestoringBeam>] {
        &self.fitted_beams
    }

    /// Return selected restoring beams retained as terminal metadata.
    #[must_use]
    pub const fn restoring_beams(&self) -> &[Option<RestoringBeam>] {
        &self.restoring_beams
    }

    /// Return published members in exact graph order.
    #[must_use]
    pub const fn members(&self) -> &[PublishedMember] {
        &self.members
    }
}
