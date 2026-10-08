// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct, bounded continuum product generation.
//!
//! This owner compiles the exact Product Graph inventory, validates its
//! scientific source association, and transfers every generated window to a
//! write-only output owner. It retains useful contracts, beams, and run
//! association for publication without hashing or rereading product content.

use casa_imaging_model::{
    AxisOrder, CompiledProblem, CompiledProblemId, ImageAxis, ImageDomainRole, ProductAxes,
    ProductBeamRule, ProductGraphId, ProductNodeId, ProductNormalization, ProductPixelMask,
    ProductRole, ProductSchema, ProductStorageContract, ProductSupportComparison, ProductUnit,
    ProductValidityRule, ReconstructionBasis, RestoringBeamPolicy,
};
use casa_imaging_reconstruction::{
    FinalNormalPlaneReader, ModelGeneration, NormalStateCatalog, SpectralChannelValidity,
};

use crate::ProductStoragePlan;
use crate::beam::{RestoringBeam, fit_restoring_beam};
use crate::error::ProductsError;
use crate::restore::{
    MosaicSensitivity, normalize_plane, normalized_psf_value, psf_peak, rescale_residual_to_beam,
    restore_model_plane,
};
use crate::source::ContinuumProductInputs;
use crate::storage::{ProductMemberWriter, ProductOutput};
use crate::taylor::{
    TaylorProducts, analytic_alma_airy_primary_beam, analytic_evla_primary_beam,
    analytic_vla_primary_beam, primary_beam_frequency_supported,
};

/// Version of the native continuum product-algorithm catalog.
pub const CONTINUUM_ALGORITHM_CATALOG_VERSION: u32 = 10;

/// Default main-lobe cutoff fraction for restoring-beam fitting.
pub const DEFAULT_PSF_CUTOFF: f32 = casa_imaging_reconstruction::DEFAULT_PSF_FIT_CUTOFF;

/// Explicit analytic primary-beam law available to product construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyticPrimaryBeamModel {
    /// CASA's common EVLA primary-beam power polynomial with sampled radial lookup.
    CasaEvlaCommon,
    /// CASA's frequency-selected legacy-VLA L- and Q-band primary beams.
    CasaVlaBand,
    /// CASA's 10.7 m effective Airy aperture for homogeneous ALMA 12 m data.
    CasaAlma12mAiry,
    /// CASA's 6.25 m effective Airy aperture for homogeneous ACA 7 m data.
    CasaAca7mAiry,
    /// CASA mosaic PB formed from the reconstruction-owned sensitivity image.
    MosaicSensitivity,
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

    /// Check beam-frequency coverage before visibility processing or production.
    ///
    /// Uses compiled output frequencies, including frame conversion and channel
    /// ordering. Taylor products evaluate the beam at output channel zero;
    /// channel-local products require coverage of every output channel. Unused
    /// models impose no additional restriction on non-Taylor products.
    ///
    /// # Errors
    ///
    /// Returns the first unsupported output channel and its frequency without
    /// constructing any beam planes or reading visibility payloads.
    pub fn validate_for_problem(&self, problem: &CompiledProblem) -> Result<(), ProductsError> {
        let Some(model) = self.primary_beam_model else {
            return Ok(());
        };
        let taylor = matches!(
            problem.reconstruction().basis(),
            ReconstructionBasis::Taylor { .. }
        );
        let graph = problem.product_graph();
        let needs_beam = taylor || graph.publication().members().iter().any(|ordinal| {
            let node = &graph.nodes()[ordinal.ordinal()];
            let needs_validity = |rule| matches!(rule,
                ProductValidityRule::PrimaryBeam(_) | ProductValidityRule::TaylorAndPrimaryBeam { .. });
            matches!(node.role(), ProductRole::PrimaryBeam(_) | ProductRole::PbCorrectedImage(_))
                || needs_validity(node.validity())
                || matches!(node.storage().pixel_mask(), ProductPixelMask::Explicit(rule) if needs_validity(rule))
        });
        if !needs_beam {
            return Ok(());
        }
        let spectral = problem.geometry().spectral();
        let channels = if taylor {
            1
        } else {
            spectral.output_channels()
        };
        for output_channel in 0..channels {
            let frequency_hz = spectral
                .channel_centre_hz(output_channel)
                .ok_or(ProductsError::UnsupportedProblem)?;
            if !primary_beam_frequency_supported(model, frequency_hz) {
                return Err(ProductsError::UnsupportedPrimaryBeamFrequency {
                    model,
                    output_channel,
                    frequency_hz,
                });
            }
        }
        Ok(())
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
        | ProductRole::PbCorrectedSpectralIndex
        | ProductRole::Sensitivity
        | ProductRole::CleanMask => Ok(()),
        role => Err(ProductsError::UnsupportedProductRole {
            role,
            catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
        }),
    }
}

/// Exact compiler-owned inventory and source/run association for one
/// continuum generation.
#[derive(Debug)]
pub struct PlannedContinuumGeneration {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    major_cycle_completion: casa_imaging_reconstruction::MajorCycleCompletionId,
    normal_state_completion: casa_imaging_reconstruction::FinalNormalStateCompletionId,
    psf_cutoff: f32,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    members: Box<[PlannedMember]>,
    final_model_generation: casa_imaging_reconstruction::ModelGenerationId,
    reconstruction_mask_generation:
        Option<casa_imaging_reconstruction::ReconstructionMaskGenerationId>,
}

impl PlannedContinuumGeneration {
    /// Compile the exact publication inventory for these scientific inputs.
    pub fn new(
        inputs: &ContinuumProductInputs<'_>,
        controls: &ContinuumProductControls,
    ) -> Result<Self, ProductsError> {
        controls.validate_for_problem(inputs.problem())?;
        let graph = inputs.problem().product_graph();
        let mut members = Vec::with_capacity(graph.publication().members().len());
        for node_ordinal in graph.publication().members() {
            let node = graph
                .nodes()
                .get(node_ordinal.ordinal())
                .ok_or(ProductsError::UnsupportedProblem)?;
            ensure_producible(node.role())?;
            let needs_primary_beam = |rule| {
                matches!(
                    rule,
                    ProductValidityRule::PrimaryBeam(_)
                        | ProductValidityRule::TaylorAndPrimaryBeam { .. }
                )
            };
            let requires_primary_beam = needs_primary_beam(node.validity())
                || matches!(node.storage().pixel_mask(), ProductPixelMask::Explicit(rule)
                    if needs_primary_beam(rule));
            if requires_primary_beam && controls.primary_beam_model.is_none() {
                return Err(ProductsError::UnsupportedProblem);
            }
            let axes = node.axes();
            let shape = axes.shape();
            let payload_values = shape
                .iter()
                .copied()
                .try_fold(1usize, |total, extent| total.checked_mul(extent))
                .ok_or(ProductsError::ResourceDemandOverflow("product shape"))?;
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
                storage: node.storage(),
                dependencies: node.dependencies().to_vec().into_boxed_slice(),
            });
        }
        Ok(Self {
            problem_id: inputs.problem().problem_id(),
            graph_id: graph.graph_id(),
            major_cycle_completion: inputs.major_cycle_completion(),
            normal_state_completion: inputs.normal_state_completion(),
            psf_cutoff: controls.psf_cutoff(),
            primary_beam_model: controls.primary_beam_model(),
            members: members.into_boxed_slice(),
            final_model_generation: inputs.final_model().generation_id(),
            reconstruction_mask_generation: inputs.reconstruction_mask_generation(),
        })
    }

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

    /// Return the released Major-Cycle run association.
    #[must_use]
    pub const fn major_cycle_completion(
        &self,
    ) -> casa_imaging_reconstruction::MajorCycleCompletionId {
        self.major_cycle_completion
    }

    /// Return the released Normal-State completion association.
    #[must_use]
    pub const fn normal_state_completion(
        &self,
    ) -> casa_imaging_reconstruction::FinalNormalStateCompletionId {
        self.normal_state_completion
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
    storage: ProductStorageContract,
    dependencies: Box<[ProductNodeId]>,
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

    /// Return this member's numerical-support rule, independently of its stored mask.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityRule {
        self.validity
    }

    /// Return the exact stored-mask and metadata contract.
    #[must_use]
    pub const fn storage(&self) -> ProductStorageContract {
        self.storage
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
}

/// Complete compiled contract carried by one generated member.
#[derive(Debug, Clone)]
pub struct ProductMemberContract {
    role: ProductRole,
    unit: ProductUnit,
    schema: ProductSchema,
    axes: ProductAxes,
    beam_rule: ProductBeamRule,
    validity: ProductValidityRule,
    storage: ProductStorageContract,
    dependencies: Box<[ProductNodeId]>,
}

impl ProductMemberContract {
    fn from_planned(member: &PlannedMember) -> Self {
        Self {
            role: member.role,
            unit: member.unit,
            schema: member.schema,
            axes: member.axes.clone(),
            beam_rule: member.beam_rule,
            validity: member.validity,
            storage: member.storage,
            dependencies: member.dependencies.clone(),
        }
    }

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

    /// Return the numerical-support rule, independently of the stored mask.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityRule {
        self.validity
    }

    /// Return the exact stored-mask and metadata contract.
    #[must_use]
    pub const fn storage(&self) -> ProductStorageContract {
        self.storage
    }

    /// Return graph-node dependencies, all of which precede this node.
    #[must_use]
    pub const fn dependencies(&self) -> &[ProductNodeId] {
        &self.dependencies
    }
}

/// Produce every planned member through the continuum algorithm catalog.
///
/// Runs restoring-beam fitting, restoration, residual scaling, normalization,
/// validity, and metadata exactly once per member, in the planned publication
/// order.
///
/// # Errors
///
/// Rejects source/run mismatches, unsupported roles, failed beam fits, and
/// generated non-finite payloads.
pub fn produce_continuum_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    execution: &impl crate::ProductWindowExecutor,
    output: &dyn ProductOutput,
) -> Result<PublishedContinuumGeneration, ProductsError> {
    if inputs.problem().problem_id() != planned.problem_id
        || inputs.problem().product_graph().graph_id() != planned.graph_id
        || inputs.major_cycle_completion() != planned.major_cycle_completion
        || inputs.normal_state_completion() != planned.normal_state_completion
        || inputs.final_model().generation_id() != planned.final_model_generation
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    if inputs.reconstruction_mask_generation() != planned.reconstruction_mask_generation {
        return Err(ProductsError::SourceLineageMismatch);
    }
    if inputs.normal_state().catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1 {
        return produce_taylor_members(planned, inputs, storage_plan, output);
    }
    let normal_state = inputs.normal_state();
    let channel_count = normal_state.channel_count();
    if normal_state.slab().core_range() != (0..normal_state.slab().total_channels())
        || channel_count != normal_state.slab().total_channels()
        || inputs.final_model().shape().coefficients() != channel_count
        || inputs.final_model().shape().polarizations() != normal_state.polarization_count()
        || normal_state.domain_count() != inputs.problem().geometry().domains().len()
        || inputs.final_model().shape().domains().len()
            != inputs.problem().geometry().domains().len()
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let requires_beam = planned
        .members
        .iter()
        .any(|member| member.beam_rule != ProductBeamRule::None);
    let fitted_beams = if requires_beam {
        let beam_count = normal_state
            .domain_count()
            .checked_mul(channel_count)
            .and_then(|count| count.checked_mul(normal_state.polarization_count()))
            .ok_or(ProductsError::ResourceDemandOverflow(
                "generation beam count",
            ))?;
        let mut fitted = Vec::with_capacity(beam_count);
        for start in (0..beam_count).step_by(storage_plan.maximum_workers()) {
            let count = storage_plan.maximum_workers().min(beam_count - start);
            let mut slots = vec![None; count];
            execution.prepare(&mut slots, &|index| {
                let ordinal = start + index;
                let polarization = ordinal % normal_state.polarization_count();
                let local_channel = (ordinal / normal_state.polarization_count()) % channel_count;
                let domain = &inputs.problem().geometry().domains()
                    [ordinal / (channel_count * normal_state.polarization_count())];
                let channel = normal_state.slab().core_range().start + local_channel;
                let plane = normal_state.read_plane(
                    inputs.model_domain_ordinal(domain.role())?,
                    channel,
                    polarization,
                )?;
                if plane.validity() == SpectralChannelValidity::Valid {
                    fit_restoring_beam(
                        &psf_real_plane(&plane)?,
                        plane.shape(),
                        inputs.cell_size_rad_for_domain(domain.role())?,
                        planned.psf_cutoff(),
                    )
                    .map(Some)
                } else {
                    Ok(None)
                }
            })?;
            for slot in slots {
                fitted.push(slot.ok_or(ProductsError::SourceLineageMismatch)?);
            }
        }
        fitted.into_boxed_slice()
    } else {
        Box::new([])
    };
    let restoring_beams = match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane => fitted_beams.clone(),
        RestoringBeamPolicy::Common => {
            let mut selected = Vec::with_capacity(fitted_beams.len());
            for domain_beams in
                fitted_beams.chunks(channel_count * normal_state.polarization_count())
            {
                let mut valid = Vec::with_capacity(domain_beams.len());
                valid.extend(domain_beams.iter().flatten().copied());
                if valid.is_empty() {
                    selected.extend_from_slice(domain_beams);
                } else {
                    let common = RestoringBeam::common_enclosing(&valid)
                        .map_err(|error| ProductsError::BeamFitFailed(error.to_string()))?;
                    selected.extend(domain_beams.iter().map(|fitted| fitted.map(|_| common)));
                }
            }
            selected.into_boxed_slice()
        }
    };

    for member in &planned.members {
        let domain_ordinal = inputs.model_domain_ordinal(member.axes().domain())?;
        let plane_shape = inputs
            .final_model()
            .shape()
            .domains()
            .get(domain_ordinal)
            .ok_or(ProductsError::SourceLineageMismatch)?
            .pixels();
        let reconstruction_only = matches!(
            member.role,
            ProductRole::Model(
                casa_imaging_model::ProductTerm::Single
                    | casa_imaging_model::ProductTerm::Taylor(0)
            ) | ProductRole::CleanMask
        );
        if normal_state.domain_shape(domain_ordinal) != Some(plane_shape)
            || (reconstruction_only
                && (member.validity != ProductValidityRule::All
                    || member.storage.pixel_mask() != ProductPixelMask::Absent))
        {
            return Err(ProductsError::SourceLineageMismatch);
        }
        let beam_offset = domain_ordinal
            .checked_mul(channel_count)
            .and_then(|offset| offset.checked_mul(normal_state.polarization_count()))
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let layout = storage_plan.layout(member.axes())?;
        let member_beams = beams_for_member(
            member,
            &planned.members,
            member.beam_rule,
            &fitted_beams,
            &restoring_beams,
        )?;
        let writer = output.begin_member(member, layout, &member_beams)?;
        let mut writer = ProductMemberWriter::new(layout, writer)?;
        let window_count = channel_count.div_ceil(layout.maximum_channels());
        let workers = storage_plan.maximum_workers().min(window_count);
        let fft_threads = storage_plan.fft_threads(window_count);
        for wave_start in (0..window_count).step_by(workers) {
            let mut slots: Vec<Option<crate::ProductWindow>> = (0..workers
                .min(window_count - wave_start))
                .map(|_| None)
                .collect();
            execution.prepare(&mut slots, &|index| {
                let window_start = (wave_start + index) * layout.maximum_channels();
                let window_end = (window_start + layout.maximum_channels()).min(channel_count);
                let mut output = layout.window(window_start..window_end)?;
                for local_channel in window_start..window_end {
                    let channel = normal_state.slab().core_range().start + local_channel;
                    if reconstruction_only {
                        for polarization in 0..normal_state.polarization_count() {
                            let payload = if member.role == ProductRole::CleanMask {
                                reconstruction_support_plane(
                                    inputs,
                                    member.axes().domain(),
                                    plane_shape[0] * plane_shape[1],
                                )?
                            } else {
                                let model = model_real_plane(
                                    inputs.final_model(),
                                    domain_ordinal,
                                    channel,
                                    polarization,
                                    plane_shape,
                                )?;
                                if planned.primary_beam_model
                                    == Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
                                {
                                    let plane = normal_state.read_plane(
                                        domain_ordinal,
                                        channel,
                                        polarization,
                                    )?;
                                    apparent_model(
                                        model,
                                        &plane,
                                        inputs.problem().products().normalization(),
                                        inputs.problem().products().validity().primary_beam(),
                                        planned.primary_beam_model,
                                    )?
                                } else {
                                    model
                                }
                            };
                            scatter_image_polarization_plane(
                                &mut output.payload,
                                member.axes().order(),
                                output.shape,
                                polarization,
                                channel - window_start,
                                plane_shape,
                                &payload,
                            )?;
                        }
                        continue;
                    }
                    for polarization in 0..normal_state.polarization_count() {
                        let plane =
                            normal_state.read_plane(domain_ordinal, channel, polarization)?;
                        if plane.shape() != plane_shape {
                            return Err(ProductsError::SourceLineageMismatch);
                        }
                        let output_channel = plane.output_channel() - window_start;
                        if matches!(member.role, ProductRole::SumWeights(_)) {
                            // CASA's `.sumwt` is the PSF gridding's sum:
                            // `FTMachine::finalizeToSkyNew` seeds the image
                            // store's sumwt once, from `makePSF`
                            // (`refim_mawproject`: 8987.97 is the weight
                            // cell's norms, 0.24 % above the data
                            // gridding's). The flat-noise products divide
                            // by the raw sensitivity
                            // (`normalize_domain_plane`), where every such
                            // sum cancels, as CASA's
                            // `divideResidualByWeight` reduces to.
                            scatter_polarization_plane_state(
                                &mut output.payload,
                                member.axes(),
                                output.shape,
                                polarization,
                                output_channel,
                                plane.sum_weight() as f32,
                            )?;
                            continue;
                        }
                        let beam_index = beam_offset
                            + local_channel * normal_state.polarization_count()
                            + polarization;
                        let mut plane_payload = produce_plane_member(PlaneMemberRequest {
                            member,
                            inputs,
                            plane: &plane,
                            domain_ordinal,
                            polarization,
                            fitted_beam: fitted_beams.get(beam_index).copied().flatten(),
                            restoring_beam: restoring_beams.get(beam_index).copied().flatten(),
                            primary_beam_model: planned.primary_beam_model,
                            fft_threads,
                        })?;
                        if member.validity != ProductValidityRule::All {
                            let support = product_plane_validity(
                                member.validity,
                                &plane,
                                planned.primary_beam_model,
                                inputs,
                                member.axes().domain(),
                            )?;
                            zero_invalid_plane_values(&mut plane_payload, &support)?;
                        }
                        if let ProductPixelMask::Explicit(rule) = member.storage.pixel_mask() {
                            let support = product_plane_validity(
                                rule,
                                &plane,
                                planned.primary_beam_model,
                                inputs,
                                member.axes().domain(),
                            )?;
                            scatter_image_polarization_plane(
                                &mut output.validity,
                                member.axes().order(),
                                output.shape,
                                polarization,
                                output_channel,
                                plane_shape,
                                &support,
                            )?;
                        }
                        scatter_image_polarization_plane(
                            &mut output.payload,
                            member.axes().order(),
                            output.shape,
                            polarization,
                            output_channel,
                            plane_shape,
                            &plane_payload,
                        )?;
                    }
                }
                Ok(output)
            })?;
            for slot in slots {
                writer.write(slot.ok_or(ProductsError::InvalidWindow)?)?;
            }
        }
        writer.finish()?;
    }

    published_generation(planned, fitted_beams, restoring_beams)
}

fn produce_taylor_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    output: &dyn ProductOutput,
) -> Result<PublishedContinuumGeneration, ProductsError> {
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
    for member in &planned.members {
        let payload = products.payload(member.role)?;
        let validity = match member.storage.pixel_mask() {
            ProductPixelMask::Absent
            | ProductPixelMask::Explicit(
                ProductValidityRule::All | ProductValidityRule::FinalNormalState,
            ) => {
                vec![true; member.payload_values]
            }
            ProductPixelMask::Explicit(rule) => products.validity(rule)?,
        };
        if payload.len() != member.payload_values || validity.len() != member.payload_values {
            return Err(ProductsError::PayloadLengthMismatch {
                expected: member.payload_values,
                actual: payload.len().max(validity.len()),
            });
        }
        let layout = storage_plan.layout(member.axes())?;
        let member_beams = beams_for_member(
            member,
            &planned.members,
            member.beam_rule,
            &fitted_beams,
            &restoring_beams,
        )?;
        let writer = output.begin_member(member, layout, &member_beams)?;
        let mut writer = ProductMemberWriter::new(layout, writer)?;
        writer.write_coupled(&payload, &validity)?;
        writer.finish()?;
    }
    published_generation(planned, fitted_beams, restoring_beams)
}

struct PlaneMemberRequest<'request, 'inputs, 'plane> {
    member: &'request PlannedMember,
    inputs: &'request ContinuumProductInputs<'inputs>,
    plane: &'request FinalNormalPlaneReader<'plane>,
    domain_ordinal: usize,
    polarization: usize,
    fitted_beam: Option<RestoringBeam>,
    restoring_beam: Option<RestoringBeam>,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    fft_threads: usize,
}

fn produce_plane_member(
    request: PlaneMemberRequest<'_, '_, '_>,
) -> Result<Vec<f32>, ProductsError> {
    let PlaneMemberRequest {
        member,
        inputs,
        plane,
        domain_ordinal,
        polarization,
        fitted_beam,
        restoring_beam,
        primary_beam_model,
        fft_threads,
    } = request;
    let scalar_sensitivity = plane.sum_weight();
    let valid = plane.validity() == SpectralChannelValidity::Valid
        && scalar_sensitivity.is_finite()
        && scalar_sensitivity > 0.0;
    let shape = plane.shape();
    let cells = shape[0] * shape[1];
    let invalid_residual = || vec![0.0; cells];
    match member.role {
        ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
        | ProductRole::Psf(casa_imaging_model::ProductTerm::Taylor(0)) => {
            if valid {
                let mut values = psf_real_plane(plane)?;
                let peak = psf_peak(values.iter().copied())?;
                for value in &mut values {
                    *value = normalized_psf_value(*value, peak);
                }
                Ok(values)
            } else {
                Ok(vec![0.0; cells])
            }
        }
        ProductRole::Residual(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if valid {
                normalize_domain_plane(
                    &residual_real_plane(plane)?,
                    required_normalization(member)?,
                    plane,
                )
            } else {
                Ok(invalid_residual())
            }
        }
        ProductRole::Weight(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            let scale = if primary_beam_model == Some(AnalyticPrimaryBeamModel::MosaicSensitivity) {
                if !plane.sum_weight().is_finite() || plane.sum_weight() <= 0.0 {
                    return Err(ProductsError::GeneratedNonfinite);
                }
                plane.sum_weight()
            } else {
                1.0
            };
            Ok(plane
                .read_sensitivity()?
                .iter()
                .map(|value| (*value / scale) as f32)
                .collect())
        }
        ProductRole::Sensitivity => Ok(plane
            .read_sensitivity()?
            .iter()
            .map(|value| *value as f32)
            .collect()),
        ProductRole::RestoredImage(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if !valid {
                return Ok(invalid_residual());
            }
            restored_plane(
                member,
                inputs,
                plane,
                domain_ordinal,
                polarization,
                fitted_beam,
                restoring_beam,
                primary_beam_model,
                fft_threads,
            )
        }
        ProductRole::PrimaryBeam(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => primary_beam_plane(primary_beam_model, inputs, member.axes().domain(), plane),
        ProductRole::PbCorrectedImage(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if !valid {
                return Ok(invalid_residual());
            }
            let restored = restored_plane(
                member,
                inputs,
                plane,
                domain_ordinal,
                polarization,
                fitted_beam,
                restoring_beam,
                primary_beam_model,
                fft_threads,
            )?;
            let primary_beam =
                primary_beam_plane(primary_beam_model, inputs, member.axes().domain(), plane)?;
            correct_primary_beam(
                &restored,
                &primary_beam,
                inputs.problem().products().validity().primary_beam(),
            )
        }
        role => Err(ProductsError::UnsupportedProductRole {
            role,
            catalog: CONTINUUM_ALGORITHM_CATALOG_VERSION,
        }),
    }
}

fn normalize_domain_plane(
    values: &[f32],
    normalization: ProductNormalization,
    plane: &FinalNormalPlaneReader<'_>,
) -> Result<Vec<f32>, ProductsError> {
    match normalization {
        ProductNormalization::UnitResponse => {
            normalize_plane(values, normalization, plane.sum_weight())
        }
        ProductNormalization::FlatNoise | ProductNormalization::FlatSky => {
            Ok(MosaicSensitivity::new(&plane.read_sensitivity()?)?
                .normalize(values, normalization)?)
        }
    }
}

/// The model in the units CASA restores and publishes: under flat-noise
/// with a direction-dependent sensitivity the apparent model, the physical
/// model the lifecycle holds times the unit-peak beam and zero outside its
/// support (`SynthesisNormalizer::multiplyModelByWeight` after every major
/// cycle); the physical model otherwise (flat-sky, or a scalar
/// sensitivity).
fn apparent_model(
    model: Vec<f32>,
    plane: &FinalNormalPlaneReader<'_>,
    normalization: ProductNormalization,
    policy: casa_imaging_model::PrimaryBeamValidityPolicy,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
) -> Result<Vec<f32>, ProductsError> {
    if primary_beam_model != Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
        || !matches!(
            normalization,
            ProductNormalization::FlatNoise | ProductNormalization::FlatSky
        )
    {
        return Ok(model);
    }
    let sensitivity = plane.read_sensitivity()?;
    let response =
        MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(plane.sum_weight())?;
    model
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            Ok(
                response.physical_to_apparent(f64::from(value), index, normalization, policy)?
                    as f32,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn restored_plane(
    member: &PlannedMember,
    inputs: &ContinuumProductInputs<'_>,
    plane: &FinalNormalPlaneReader<'_>,
    domain_ordinal: usize,
    polarization: usize,
    fitted_beam: Option<RestoringBeam>,
    restoring_beam: Option<RestoringBeam>,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    fft_threads: usize,
) -> Result<Vec<f32>, ProductsError> {
    let beam = restoring_beam.ok_or_else(|| {
        ProductsError::BeamFitFailed(
            "restoration requires a fitted beam for every valid plane".to_string(),
        )
    })?;
    let model = apparent_model(
        model_real_plane(
            inputs.final_model(),
            domain_ordinal,
            plane.output_channel(),
            polarization,
            plane.shape(),
        )?,
        plane,
        required_normalization(member)?,
        inputs.problem().products().validity().primary_beam(),
        primary_beam_model,
    )?;
    let cell_size = inputs.cell_size_rad_for_domain(member.axes().domain())?;
    let residual = normalize_domain_plane(
        &residual_real_plane(plane)?,
        required_normalization(member)?,
        plane,
    )?;
    let fitted_beam = fitted_beam.ok_or_else(|| {
        ProductsError::BeamFitFailed(
            "restoration requires a fitted beam for every valid plane".to_string(),
        )
    })?;
    let residual =
        rescale_residual_to_beam(&residual, plane.shape(), cell_size, fitted_beam, beam)?
            .into_values();
    Ok(restore_model_plane(
        &model,
        residual,
        plane.shape(),
        &beam,
        cell_size,
        inputs.normal_state().slab().total_channels(),
        fft_threads,
    ))
}

pub(crate) fn reconstruction_support_plane(
    inputs: &ContinuumProductInputs<'_>,
    role: &ImageDomainRole,
    cells: usize,
) -> Result<Vec<f32>, ProductsError> {
    let mask = reconstruction_mask_for_domain(inputs, role)?;
    Ok((0..cells)
        .map(|index| {
            if mask.is_none_or(|mask| mask.support()[index]) {
                1.0
            } else {
                0.0
            }
        })
        .collect())
}

fn reconstruction_mask_for_domain<'a>(
    inputs: &'a ContinuumProductInputs<'_>,
    role: &ImageDomainRole,
) -> Result<Option<&'a casa_imaging_reconstruction::ReconstructionMask>, ProductsError> {
    let Some(mask) = inputs.reconstruction_mask() else {
        let Some(masks) = inputs.domain_reconstruction_masks() else {
            return Ok(None);
        };
        let ordinal = inputs.model_domain_ordinal(role)?;
        return masks
            .get(ordinal)
            .ok_or(ProductsError::SourceLineageMismatch)
            .map(Some);
    };
    let domain = inputs
        .problem()
        .geometry()
        .domains()
        .iter()
        .find(|domain| domain.role() == role)
        .ok_or(ProductsError::SourceLineageMismatch)?;
    if mask.shape() == domain.shape().pixels() && mask.coordinate() == domain.direction() {
        Ok(Some(mask))
    } else {
        Err(ProductsError::SourceLineageMismatch)
    }
}

fn required_normalization(member: &PlannedMember) -> Result<ProductNormalization, ProductsError> {
    member
        .normalization
        .ok_or(ProductsError::UnsupportedProblem)
}

fn psf_real_plane(plane: &FinalNormalPlaneReader<'_>) -> Result<Vec<f32>, ProductsError> {
    Ok(plane
        .read_psf()?
        .iter()
        .map(|value| value.re as f32)
        .collect())
}

fn residual_real_plane(plane: &FinalNormalPlaneReader<'_>) -> Result<Vec<f32>, ProductsError> {
    Ok(plane
        .read_residual()?
        .iter()
        .map(|value| value.re as f32)
        .collect())
}

fn product_plane_validity(
    rule: ProductValidityRule,
    plane: &FinalNormalPlaneReader<'_>,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    inputs: &ContinuumProductInputs<'_>,
    domain_role: &ImageDomainRole,
) -> Result<Vec<bool>, ProductsError> {
    let shape = plane.shape();
    match rule {
        ProductValidityRule::All => Ok(vec![true; shape[0] * shape[1]]),
        ProductValidityRule::FinalNormalState => {
            let valid = plane.validity() == SpectralChannelValidity::Valid
                && plane.sum_weight().is_finite()
                && plane.sum_weight() > 0.0;
            Ok(vec![valid; shape[0] * shape[1]])
        }
        ProductValidityRule::PrimaryBeam(policy) => {
            Ok(
                primary_beam_plane(primary_beam_model, inputs, domain_role, plane)?
                    .into_iter()
                    .map(|value| match policy.comparison() {
                        ProductSupportComparison::StrictlyGreater => value > policy.cutoff(),
                    })
                    .collect(),
            )
        }
        ProductValidityRule::Taylor(_) | ProductValidityRule::TaylorAndPrimaryBeam { .. } => {
            Err(ProductsError::UnsupportedProblem)
        }
    }
}

fn primary_beam_plane(
    model: Option<AnalyticPrimaryBeamModel>,
    inputs: &ContinuumProductInputs<'_>,
    domain_role: &ImageDomainRole,
    plane: &FinalNormalPlaneReader<'_>,
) -> Result<Vec<f32>, ProductsError> {
    match model {
        Some(AnalyticPrimaryBeamModel::CasaEvlaCommon) => {
            analytic_evla_primary_beam(inputs, domain_role, plane.shape(), plane.output_channel())
        }
        Some(AnalyticPrimaryBeamModel::CasaVlaBand) => {
            analytic_vla_primary_beam(inputs, domain_role, plane.shape(), plane.output_channel())
        }
        Some(AnalyticPrimaryBeamModel::CasaAlma12mAiry) => analytic_alma_airy_primary_beam(
            inputs,
            domain_role,
            plane.shape(),
            plane.output_channel(),
            10.7,
        ),
        Some(AnalyticPrimaryBeamModel::CasaAca7mAiry) => analytic_alma_airy_primary_beam(
            inputs,
            domain_role,
            plane.shape(),
            plane.output_channel(),
            6.25,
        ),
        Some(AnalyticPrimaryBeamModel::MosaicSensitivity) => {
            Ok(MosaicSensitivity::new(&plane.read_sensitivity()?)?.primary_beam())
        }
        None => Err(ProductsError::UnsupportedProblem),
    }
}

fn correct_primary_beam(
    values: &[f32],
    primary_beam: &[f32],
    policy: casa_imaging_model::PrimaryBeamValidityPolicy,
) -> Result<Vec<f32>, ProductsError> {
    if values.len() != primary_beam.len() {
        return Err(ProductsError::SourceLineageMismatch);
    }
    values
        .iter()
        .zip(primary_beam)
        .map(|(value, beam)| {
            let valid = match policy.comparison() {
                ProductSupportComparison::StrictlyGreater => *beam > policy.cutoff(),
            };
            if !valid {
                return Ok(0.0);
            }
            let corrected = *value / *beam;
            corrected
                .is_finite()
                .then_some(corrected)
                .ok_or(ProductsError::GeneratedNonfinite)
        })
        .collect()
}

fn zero_invalid_plane_values(payload: &mut [f32], validity: &[bool]) -> Result<(), ProductsError> {
    if payload.len() != validity.len() {
        return Err(ProductsError::SourceLineageMismatch);
    }
    for (value, valid) in payload.iter_mut().zip(validity) {
        if !*valid {
            *value = 0.0;
        }
    }
    Ok(())
}

fn model_real_plane(
    model: &ModelGeneration,
    domain_ordinal: usize,
    output_channel: usize,
    polarization: usize,
    plane_shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    let [width, height] = plane_shape;
    if output_channel >= model.shape().coefficients()
        || polarization >= model.shape().polarizations()
        || model
            .shape()
            .domains()
            .get(domain_ordinal)
            .map(|shape| shape.pixels())
            != Some(plane_shape)
        || model.sample_count() != model.shape().sample_count()
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    // Canonical model order is y-major (`flat = y * W + x`); product planes
    // are stored x-major like every normal-state primitive.
    let mut plane = vec![0.0_f32; width * height];
    let samples = model.read_plane(domain_ordinal, output_channel, polarization)?;
    for y in 0..height {
        for x in 0..width {
            plane[x * height + y] = samples[y * width + x].value().value() as f32;
        }
    }
    Ok(plane)
}

fn scatter_image_polarization_plane<T: Copy>(
    payload: &mut [T],
    order: &AxisOrder,
    storage_shape: [usize; 4],
    polarization: usize,
    output_channel: usize,
    plane_shape: [usize; 2],
    plane: &[T],
) -> Result<(), ProductsError> {
    let [width, height] = plane_shape;
    if width == 0 || height == 0 || width.checked_mul(height) != Some(plane.len()) {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let last = product_offset(
        order,
        storage_shape,
        width - 1,
        height - 1,
        polarization,
        output_channel,
    )?;
    if last >= payload.len() {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let base = product_offset(order, storage_shape, 0, 0, polarization, output_channel)?;
    let (mut longitude_stride, mut latitude_stride) = (0, 0);
    let mut stride = 1usize;
    for (axis, extent) in order.positions().iter().zip(storage_shape).rev() {
        match axis {
            ImageAxis::DirectionLongitude => longitude_stride = stride,
            ImageAxis::DirectionLatitude => latitude_stride = stride,
            _ => {}
        }
        stride = stride
            .checked_mul(extent)
            .ok_or(ProductsError::SourceLineageMismatch)?;
    }
    for (x, column) in plane.chunks_exact(height).enumerate() {
        let start = base + x * longitude_stride;
        for (y, value) in column.iter().enumerate() {
            payload[start + y * latitude_stride] = *value;
        }
    }
    Ok(())
}

fn scatter_polarization_plane_state(
    payload: &mut [f32],
    axes: &ProductAxes,
    storage_shape: [usize; 4],
    polarization: usize,
    output_channel: usize,
    value: f32,
) -> Result<(), ProductsError> {
    let offset = product_offset(
        axes.order(),
        storage_shape,
        0,
        0,
        polarization,
        output_channel,
    )?;
    payload[offset] = value;
    Ok(())
}

fn product_offset(
    order: &AxisOrder,
    storage_shape: [usize; 4],
    longitude: usize,
    latitude: usize,
    polarization: usize,
    spectral: usize,
) -> Result<usize, ProductsError> {
    let mut offset = 0usize;
    for (position, axis) in order.positions().iter().enumerate() {
        let coordinate = match axis {
            ImageAxis::DirectionLongitude => longitude,
            ImageAxis::DirectionLatitude => latitude,
            ImageAxis::Polarization => polarization,
            ImageAxis::Spectral => spectral,
        };
        let extent = storage_shape[position];
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

/// Resolve one member's compiled beam rule against the fitted generation beam.
fn beams_for_member(
    member: &PlannedMember,
    members: &[PlannedMember],
    rule: ProductBeamRule,
    fitted: &[Option<RestoringBeam>],
    restoring: &[Option<RestoringBeam>],
) -> Result<Box<[Option<RestoringBeam>]>, ProductsError> {
    let mut domain_count = 0;
    let mut domain_ordinal = None;
    for (index, planned) in members.iter().enumerate() {
        let role = planned.axes().domain();
        if members[..index]
            .iter()
            .any(|prior| prior.axes().domain() == role)
        {
            continue;
        }
        if role == member.axes().domain() {
            domain_ordinal = Some(domain_count);
        }
        domain_count += 1;
    }
    let domain_ordinal = domain_ordinal.ok_or(ProductsError::SourceLineageMismatch)?;
    Ok(resolve_beams(
        rule,
        domain_beam_slice(fitted, domain_ordinal, domain_count)?,
        domain_beam_slice(restoring, domain_ordinal, domain_count)?,
    ))
}

fn domain_beam_slice(
    beams: &[Option<RestoringBeam>],
    domain_ordinal: usize,
    domain_count: usize,
) -> Result<&[Option<RestoringBeam>], ProductsError> {
    if beams.is_empty() {
        return Ok(beams);
    }
    if domain_count == 0 || !beams.len().is_multiple_of(domain_count) {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let channels = beams.len() / domain_count;
    let start = domain_ordinal
        .checked_mul(channels)
        .ok_or(ProductsError::SourceLineageMismatch)?;
    beams
        .get(start..start + channels)
        .ok_or(ProductsError::SourceLineageMismatch)
}

fn resolve_beams(
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
        ProductBeamRule::Inherit(_) => restoring.into(),
    }
}

fn published_generation(
    planned: &PlannedContinuumGeneration,
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
) -> Result<PublishedContinuumGeneration, ProductsError> {
    let mut members = Vec::with_capacity(planned.members.len());
    for member in &planned.members {
        members.push(PublishedMember {
            node: member.node,
            name: member.name.clone(),
            contract: ProductMemberContract::from_planned(member),
            resolved_beams: beams_for_member(
                member,
                &planned.members,
                member.beam_rule,
                &fitted_beams,
                &restoring_beams,
            )?,
        });
    }
    Ok(PublishedContinuumGeneration {
        problem_id: planned.problem_id,
        graph_id: planned.graph_id,
        major_cycle_completion: planned.major_cycle_completion,
        normal_state_completion: planned.normal_state_completion,
        fitted_beams,
        restoring_beams,
        members: members.into_boxed_slice(),
    })
}

/// Payload-free metadata for one generated member.
#[derive(Debug, Clone)]
pub struct PublishedMember {
    node: ProductNodeId,
    name: String,
    contract: ProductMemberContract,
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

    /// Return the complete compiled member contract.
    #[must_use]
    pub const fn contract(&self) -> &ProductMemberContract {
        &self.contract
    }

    /// Return resolved beam metadata in output-channel order.
    #[must_use]
    pub const fn resolved_beams(&self) -> &[Option<RestoringBeam>] {
        &self.resolved_beams
    }

    /// Return the single resolved beam when exactly one valid plane exists.
    #[must_use]
    pub fn resolved_beam(&self) -> Option<&RestoringBeam> {
        match self.resolved_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }
}

/// Payload-free metadata for one generated continuum run.
#[derive(Debug, Clone)]
pub struct PublishedContinuumGeneration {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    major_cycle_completion: casa_imaging_reconstruction::MajorCycleCompletionId,
    normal_state_completion: casa_imaging_reconstruction::FinalNormalStateCompletionId,
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[PublishedMember]>,
}

impl PublishedContinuumGeneration {
    /// Return the exact compiled problem for this generated run.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiler-owned Product Graph realized by this run.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the released Major-Cycle run association.
    #[must_use]
    pub const fn major_cycle_completion(
        &self,
    ) -> casa_imaging_reconstruction::MajorCycleCompletionId {
        self.major_cycle_completion
    }

    /// Return the released Normal-State completion association.
    #[must_use]
    pub const fn normal_state_completion(
        &self,
    ) -> casa_imaging_reconstruction::FinalNormalStateCompletionId {
        self.normal_state_completion
    }

    /// Return the fitted restoring beams retained as metadata.
    #[must_use]
    pub const fn fitted_beams(&self) -> &[Option<RestoringBeam>] {
        &self.fitted_beams
    }

    /// Return selected restoring beams retained as metadata.
    #[must_use]
    pub const fn restoring_beams(&self) -> &[Option<RestoringBeam>] {
        &self.restoring_beams
    }

    /// Return generated members in exact graph order.
    #[must_use]
    pub const fn members(&self) -> &[PublishedMember] {
        &self.members
    }
}

#[cfg(test)]
mod scatter_tests {
    use super::*;

    #[test]
    fn scatter_matches_scalar_offsets_for_all_axis_orders() {
        let axes = [
            ImageAxis::DirectionLongitude,
            ImageAxis::DirectionLatitude,
            ImageAxis::Polarization,
            ImageAxis::Spectral,
        ];
        for a in 0..4 {
            for b in (0..4).filter(|b| *b != a) {
                for c in (0..4).filter(|c| *c != a && *c != b) {
                    let d = (0..4).find(|d| *d != a && *d != b && *d != c).unwrap();
                    let order = AxisOrder::new([axes[a], axes[b], axes[c], axes[d]]);
                    let shape = [a, b, c, d].map(|axis| [3, 5, 2, 4][axis]);
                    let mut actual = vec![usize::MAX; 120];
                    let mut expected = actual.clone();
                    for polarization in 0..2 {
                        for channel in 0..4 {
                            let plane = (0..15)
                                .map(|value| value + 15 * (channel + 4 * polarization))
                                .collect::<Vec<_>>();
                            scatter_image_polarization_plane(
                                &mut actual,
                                &order,
                                shape,
                                polarization,
                                channel,
                                [3, 5],
                                &plane,
                            )
                            .unwrap();
                            for x in 0..3 {
                                for y in 0..5 {
                                    let offset =
                                        product_offset(&order, shape, x, y, polarization, channel)
                                            .unwrap();
                                    expected[offset] = plane[x * 5 + y];
                                }
                            }
                        }
                    }
                    assert_eq!(actual, expected, "{:?}", order.positions());
                }
            }
        }
    }

    #[test]
    fn scatter_rejects_invalid_bounds_before_writing() {
        let order = AxisOrder::new([
            ImageAxis::DirectionLongitude,
            ImageAxis::DirectionLatitude,
            ImageAxis::Polarization,
            ImageAxis::Spectral,
        ]);
        for (shape, polarization, channel, plane_shape) in [
            ([3, 5, 2, 4], 2, 0, [3, 5]),
            ([3, 5, 2, 4], 0, 4, [3, 5]),
            ([2, 5, 2, 4], 0, 0, [3, 5]),
            ([3, 4, 2, 4], 0, 0, [3, 5]),
            ([3, 5, 2, 4], 0, 0, [5, 3]),
            ([usize::MAX, 5, 2, 4], 0, 0, [3, 5]),
        ] {
            let mut output = vec![usize::MAX; 120];
            assert!(
                scatter_image_polarization_plane(
                    &mut output,
                    &order,
                    shape,
                    polarization,
                    channel,
                    plane_shape,
                    &[1; 15]
                )
                .is_err()
            );
            assert!(output.iter().all(|value| *value == usize::MAX));
        }
        let mut short = vec![usize::MAX; 119];
        assert!(
            scatter_image_polarization_plane(
                &mut short,
                &order,
                [3, 5, 2, 4],
                1,
                3,
                [3, 5],
                &[1; 15]
            )
            .is_err()
        );
        assert!(short.iter().all(|value| *value == usize::MAX));
    }
}
