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
    CompiledProblem, CompiledProblemId, ProductAxes, ProductBeamRule, ProductGraphId,
    ProductNodeId, ProductNormalization, ProductRole, ProductSchema, ProductUnit,
    ProductValidityRule,
};

use crate::beam::{RestoringBeam, fit_restoring_beam};
use crate::digest::{
    ARTIFACT_IDENTITY_DOMAIN, ARTIFACT_IDENTITY_VERSION, COMPLETIONS_DOMAIN, COMPLETIONS_VERSION,
    Encoder, PLANNED_GENERATION_DOMAIN, PLANNED_GENERATION_VERSION, SEAL_DOMAIN, SEAL_VERSION,
    plane_digest,
};
use crate::error::ProductsError;
use crate::restore::{fft_convolve, gaussian_beam_image, normalize_plane};
use crate::source::{ContinuumProductInputs, ContinuumSourceCatalog};

/// Version of the native continuum product-algorithm catalog.
///
/// The identity binds every product algorithm's semantics; changing any
/// algorithm changes every derived artifact identity and seal.
pub const CONTINUUM_ALGORITHM_CATALOG_VERSION: u32 = 2;

/// Default main-lobe cutoff fraction for restoring-beam fitting.
pub const DEFAULT_PSF_CUTOFF: f32 = casa_imaging_reconstruction::DEFAULT_PSF_FIT_CUTOFF;

/// Explicit continuum production controls.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContinuumProductControls {
    psf_cutoff: f32,
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
        Ok(Self { psf_cutoff })
    }

    /// Return the main-lobe cutoff fraction used for beam fitting.
    #[must_use]
    pub const fn psf_cutoff(self) -> f32 {
        self.psf_cutoff
    }
}

impl Default for ContinuumProductControls {
    fn default() -> Self {
        Self {
            psf_cutoff: DEFAULT_PSF_CUTOFF,
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
            members: members.into_boxed_slice(),
            final_model_generation: sources.final_model_generation(),
            reconstruction_mask_generation: sources.reconstruction_mask_generation(),
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
            let digest = plane_digest(&produced.payload);
            if digest != produced.digest.as_bytes() {
                return Err(ProductsError::MemberContentMismatch);
            }
        }

        let mut encoder = Encoder::new(COMPLETIONS_DOMAIN, COMPLETIONS_VERSION);
        encoder.identity(planned.generation_id.as_bytes());
        encoder.identity(planned.commitment_id.as_bytes());
        encoder.u32(CONTINUUM_ALGORITHM_CATALOG_VERSION);
        match completions.restoring_beam {
            Some(beam) => {
                encoder.u8(1);
                encoder.u64(beam.major_fwhm_rad().to_bits());
                encoder.u64(beam.minor_fwhm_rad().to_bits());
                encoder.u64(beam.position_angle_rad().to_bits());
            }
            None => encoder.u8(0),
        }
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
                resolved_beam: sealed_beam(member.beam_rule, completions.restoring_beam),
                payload: produced.payload.clone(),
            })
            .collect::<Box<[_]>>();
        Ok(SealedContinuumGeneration {
            problem_id: self.problem_id,
            graph_id: self.graph_id,
            seal_id,
            generation_id: planned.generation_id,
            completions_id,
            restoring_beam: completions.restoring_beam,
            members,
        })
    }
}

fn ensure_producible(role: ProductRole) -> Result<(), ProductsError> {
    match role {
        ProductRole::Psf(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        )
        | ProductRole::Residual(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        )
        | ProductRole::Model(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        )
        | ProductRole::Weight(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        )
        | ProductRole::RestoredImage(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        )
        | ProductRole::SumWeights(_)
        | ProductRole::Sensitivity
        | ProductRole::CleanMask => Ok(()),
        ProductRole::Psf(casa_imaging_model::ProductTerm::Taylor(_))
        | ProductRole::Residual(casa_imaging_model::ProductTerm::Taylor(_))
        | ProductRole::Model(casa_imaging_model::ProductTerm::Taylor(_))
        | ProductRole::Weight(casa_imaging_model::ProductTerm::Taylor(_))
        | ProductRole::RestoredImage(casa_imaging_model::ProductTerm::Taylor(_)) => {
            Err(ProductsError::UnsupportedProductRole {
                role,
                catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
            })
        }
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
    members: Box<[PlannedMember]>,
    final_model_generation: casa_imaging_reconstruction::ModelGenerationId,
    reconstruction_mask_generation:
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
    restoring_beam: Option<RestoringBeam>,
    members: Box<[ProducedMember]>,
}

/// One produced member payload with its computed content identity.
#[derive(Debug)]
struct ProducedMember {
    node: ProductNodeId,
    artifact_id: MemberArtifactId,
    digest: MemberArtifactId,
    payload: Vec<f32>,
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
    let normal_state = inputs.normal_state();
    let sensitivity = normal_state.sum_weight();
    let plane_shape = normal_state.shape();

    // Fit the restoring beam once: the supported graphs demand fitted beam
    // metadata on their PSF/residual/restored members whenever any such
    // member exists.
    let requires_beam = planned.members.iter().any(|member| {
        matches!(
            member.role(),
            ProductRole::Psf(_) | ProductRole::Residual(_) | ProductRole::RestoredImage(_)
        )
    });
    let restoring_beam = if requires_beam {
        Some(fit_restoring_beam(
            &psf_real_plane(normal_state),
            plane_shape,
            inputs.cell_size_rad(),
            planned.psf_cutoff(),
        )?)
    } else {
        None
    };

    let model_plane = model_real_plane(inputs.final_model(), plane_shape)?;
    let residual_unnormalized = residual_real_plane(normal_state);

    let mut members = Vec::with_capacity(planned.members.len());
    for member in &planned.members {
        let normalization = member.normalization;
        let mut payload = match member.role {
            ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
            | ProductRole::Psf(casa_imaging_model::ProductTerm::Taylor(0)) => normalize_plane(
                &psf_real_plane(normal_state),
                normalization.unwrap_or(ProductNormalization::UnitResponse),
                sensitivity,
            )?,
            ProductRole::Residual(
                casa_imaging_model::ProductTerm::Single
                | casa_imaging_model::ProductTerm::Taylor(0),
            ) => normalize_plane(
                &residual_unnormalized,
                required_normalization(member)?,
                sensitivity,
            )?,
            ProductRole::Model(
                casa_imaging_model::ProductTerm::Single
                | casa_imaging_model::ProductTerm::Taylor(0),
            ) => model_plane.clone(),
            ProductRole::Weight(
                casa_imaging_model::ProductTerm::Single
                | casa_imaging_model::ProductTerm::Taylor(0),
            ) => normal_state
                .sensitivity()
                .iter()
                .map(|value| *value as f32)
                .collect(),
            ProductRole::RestoredImage(
                casa_imaging_model::ProductTerm::Single
                | casa_imaging_model::ProductTerm::Taylor(0),
            ) => {
                let Some(beam) = restoring_beam.as_ref() else {
                    return Err(ProductsError::BeamFitFailed(
                        "restoration requires a fitted restoring beam".to_string(),
                    ));
                };
                let kernel = gaussian_beam_image(plane_shape, beam, inputs.cell_size_rad());
                let mut restored = fft_convolve(
                    &model_plane,
                    kernel.as_slice().expect("contiguous"),
                    plane_shape,
                );
                // CASA restoration equation (`SIImageStore::restore`):
                // restored = conv(model, beam) + residual, where the residual
                // enters exactly as it is published. The convolved sky model
                // is never divided by the residual sensitivity.
                let residual_component = normalize_plane(
                    &residual_unnormalized,
                    required_normalization(member)?,
                    sensitivity,
                )?;
                for (restored, residual) in restored.iter_mut().zip(&residual_component) {
                    *restored += residual;
                }
                restored
            }
            ProductRole::SumWeights(_) | ProductRole::Sensitivity => {
                vec![sensitivity as f32; member.payload_values]
            }
            ProductRole::CleanMask => normal_state
                .sensitivity()
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let selected = inputs
                        .reconstruction_mask()
                        .is_none_or(|mask| mask.support()[index]);
                    if selected && *value > 0.0 && value.is_finite() {
                        1.0_f32
                    } else {
                        0.0
                    }
                })
                .collect(),
            role => {
                return Err(ProductsError::UnsupportedProductRole {
                    role,
                    catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
                });
            }
        };
        // FinalNormalState validity blanks pixels without usable sensitivity.
        if matches!(
            member.role,
            ProductRole::Residual(_) | ProductRole::RestoredImage(_)
        ) && !(sensitivity.is_finite() && sensitivity > 0.0)
        {
            payload.fill(f32::NAN);
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
        let digest = MemberArtifactId(plane_digest(&payload));
        members.push(ProducedMember {
            node: member.node,
            artifact_id: member.artifact_id,
            digest,
            payload,
        });
    }

    Ok(ContinuumProducedMembers {
        planned_generation: planned.generation_id,
        commitment_id: planned.commitment_id,
        restoring_beam,
        members: members.into_boxed_slice(),
    })
}

fn required_normalization(member: &PlannedMember) -> Result<ProductNormalization, ProductsError> {
    member
        .normalization
        .ok_or(ProductsError::UnsupportedProblem)
}

fn psf_real_plane(normal_state: &casa_imaging_reconstruction::FinalNormalState) -> Vec<f32> {
    normal_state
        .normal_approximation()
        .iter()
        .map(|value| value.re as f32)
        .collect()
}

fn residual_real_plane(normal_state: &casa_imaging_reconstruction::FinalNormalState) -> Vec<f32> {
    normal_state
        .residual()
        .iter()
        .map(|value| value.re as f32)
        .collect()
}

fn model_real_plane(
    model: &casa_imaging_reconstruction::ModelGeneration,
    plane_shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    let [width, height] = plane_shape;
    if model.shape().domains().len() != 1
        || model.shape().coefficients() != 1
        || model.shape().polarizations() != 1
        || model.shape().domains()[0].pixels() != plane_shape
        || model.samples().len() != width * height
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    // Canonical model order is y-major (`flat = y * W + x`); product planes
    // are stored x-major like every normal-state primitive.
    let mut plane = vec![0.0_f32; width * height];
    for y in 0..height {
        for x in 0..width {
            plane[x * height + y] = model.samples()[y * width + x].value().value() as f32;
        }
    }
    Ok(plane)
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
    resolved_beam: Option<RestoringBeam>,
    payload: Vec<f32>,
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

    /// Return the resolved beam metadata, when this member's beam rule
    /// resolves to the generation's fitted beam.
    #[must_use]
    pub const fn resolved_beam(&self) -> Option<&RestoringBeam> {
        self.resolved_beam.as_ref()
    }

    /// Borrow the sealed binary32 payload.
    #[must_use]
    pub fn payload(&self) -> &[f32] {
        &self.payload
    }
}

/// Resolve one member's compiled beam rule against the fitted generation beam.
fn sealed_beam(rule: ProductBeamRule, fitted: Option<RestoringBeam>) -> Option<RestoringBeam> {
    match rule {
        ProductBeamRule::None => None,
        ProductBeamRule::Fitted | ProductBeamRule::Metadata(_) | ProductBeamRule::Restoring(_) => {
            fitted
        }
        // Inherit rules resolve through their referenced member; the
        // embedded metadata is identical because the generation carries one
        // fitted beam set, so resolving to it preserves the contract.
        ProductBeamRule::Inherit(_) => fitted,
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
    restoring_beam: Option<RestoringBeam>,
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
    pub const fn restoring_beam(&self) -> Option<&RestoringBeam> {
        self.restoring_beam.as_ref()
    }

    /// Return the exact sealed member set in canonical publication order.
    #[must_use]
    pub const fn members(&self) -> &[SealedMember] {
        &self.members
    }
}
