// SPDX-License-Identifier: LGPL-3.0-or-later

//! The product plan: the controls a run's products are made with, the
//! members planned from the problem's product list, and the metadata a
//! generation leaves.

use casa_imaging_model::{
    CompiledProblem, ProductAxes, ProductBeamRule, ProductId, ProductNormalization,
    ProductPixelMask, ProductRole, ProductStorageContract, ProductTerm, ProductUnit,
    ProductValidityRule, ReconstructionBasis, RestoringBeamPolicy,
};

use crate::analytic_beam::check_frequencies;
use crate::beam::{RestoringBeam, fixed_restoring_beam};
use crate::error::ProductsError;
use crate::source::ContinuumProductInputs;

/// Default main-lobe cutoff fraction for restoring-beam fitting.
pub const DEFAULT_PSF_CUTOFF: f32 = casa_imaging_deconvolution::DEFAULT_PSF_FIT_CUTOFF;

/// Explicit analytic primary-beam law available to product construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyticPrimaryBeamModel {
    /// CASA's common EVLA primary-beam power polynomial with sampled radial lookup.
    CasaEvlaCommon,
    /// CASA's frequency-selected legacy-VLA primary beams: the named bands'
    /// annular aperture and the NVSS Airy aperture at other positive frequencies.
    CasaVlaBand,
    /// CASA's 10.7 m effective Airy aperture for homogeneous ALMA 12 m data.
    CasaAlma12mAiry,
    /// CASA's 6.25 m effective Airy aperture for homogeneous ACA 7 m data.
    CasaAca7mAiry,
    /// The caller's circular radial voltage pattern, bound once per run.
    SavedRadial,
    /// CASA mosaic PB formed from the reconstruction-owned sensitivity image.
    MosaicSensitivity,
}

/// The mask CASA leaves on a scalar residual during a field restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldResidualMask {
    Saved,
    SavedOrPrimaryBeam,
    CustomOnly,
}

/// CASA seeds a corrected image's mask only when it has no default mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SavedPbcorMask {
    Existing(u64),
    PrimaryBeam,
}

/// Explicit continuum production controls.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContinuumProductControls {
    psf_cutoff: f32,
    reuses_single_psf: bool,
    preserves_saved_psfs: bool,
    refits_saved_psf_beams: bool,
    refills_saved_sums: bool,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    saved_mosaic_beam_read_bytes: Option<u64>,
    saved_higher_primary_beam: bool,
    saved_residual_mask_read_bytes: Option<u64>,
    saved_cube_model_read_bytes: u64,
    saved_cube_pbcor_mask: Option<SavedPbcorMask>,
    preserves_saved_residual: bool,
    saved_weighted_residual: bool,
    field_residual_mask: Option<FieldResidualMask>,
    rebuilds_field_pb: bool,
    rebuilds_field_psfs: bool,
}

impl ContinuumProductControls {
    /// Construct validated controls.
    ///
    /// # Errors
    ///
    /// Rejects a cutoff outside `(0, 1)` or non-finite values.
    pub fn new(psf_cutoff: f32) -> Result<Self, ProductsError> {
        Self::default().with_psf_cutoff(psf_cutoff)
    }

    /// Set the fit cutoff while keeping the other production controls.
    ///
    /// # Errors
    /// Rejects a cutoff outside `(0, 1)` or non-finite values.
    pub fn with_psf_cutoff(mut self, psf_cutoff: f32) -> Result<Self, ProductsError> {
        if !psf_cutoff.is_finite() || psf_cutoff <= 0.0 || psf_cutoff >= 1.0 {
            return Err(ProductsError::InvalidControls);
        }
        self.psf_cutoff = psf_cutoff;
        Ok(self)
    }

    /// Keep the PSF, weight, sum of weights and primary beam already on
    /// disk when a mosaic continuation reads them instead of forming them.
    /// The scientific product dependencies still include their response.
    /// `read_bytes` bounds one worker's PB or pixel mask read. A two-term
    /// PB correction also keeps its auxiliary beam when it already exists.
    #[must_use]
    pub const fn preserving_mosaic_products(
        mut self,
        read_bytes: u64,
        saved_higher_primary_beam: bool,
    ) -> Self {
        self.saved_mosaic_beam_read_bytes = Some(read_bytes);
        self.saved_higher_primary_beam = saved_higher_primary_beam;
        self
    }

    /// Restore from weighted residuals read in their published units.
    /// Leave the saved residuals in place. Charge `mask_read_bytes` for
    /// copying a residual's pixel mask to its restored image.
    #[must_use]
    pub const fn preserving_weighted_residuals(mut self, mask_read_bytes: u64) -> Self {
        self.preserves_saved_residual = true;
        self.saved_weighted_residual = true;
        self.with_saved_residual_mask(mask_read_bytes)
    }

    /// Preserve scalar saved residuals through a zero-major restart.
    /// Leave saved residual tables in place while publishing restoration.
    #[must_use]
    pub const fn preserving_saved_residuals(mut self) -> Self {
        self.preserves_saved_residual = true;
        self
    }

    /// Keep a field's user-named residual mask. On refresh, a positive
    /// cutoff keeps or copies the generated PB support, and a negative
    /// cutoff removes the generated mask. The field keeps its own sum of weights.
    #[must_use]
    pub const fn with_saved_field_residuals(
        mut self,
        mask_read_bytes: u64,
        refreshed: bool,
        keeps_pb_mask: bool,
        (rebuilds_psfs, rebuilds_pb): (bool, bool),
    ) -> Self {
        self.preserves_saved_residual = !refreshed;
        self.rebuilds_field_psfs = rebuilds_psfs;
        self.rebuilds_field_pb = rebuilds_pb;
        self.field_residual_mask = Some(match (refreshed, keeps_pb_mask) {
            (false, _) => FieldResidualMask::Saved,
            (true, true) => FieldResidualMask::SavedOrPrimaryBeam,
            (true, false) => FieldResidualMask::CustomOnly,
        });
        self.with_saved_residual_mask(mask_read_bytes)
    }

    /// Use the existing single-term residual pixel mask when publishing a
    /// refreshed residual and its restored image.
    #[must_use]
    pub const fn with_saved_residual_mask(mut self, mask_read_bytes: u64) -> Self {
        self.saved_residual_mask_read_bytes = Some(mask_read_bytes);
        self
    }

    /// Keep saved apparent model pixels that a cube's temporary prediction
    /// excludes, admitting one saved model plane per publication lane.
    #[must_use]
    pub const fn with_saved_cube_model(mut self, read_bytes: u64) -> Self {
        self.saved_cube_model_read_bytes = read_bytes;
        self
    }

    /// Retain the existing corrected cube's mask, or take the saved beam's
    /// generated mask when there is no corrected-image mask to retain.
    #[must_use]
    pub const fn with_saved_cube_pbcor_mask(mut self, mask_read_bytes: Option<u64>) -> Self {
        self.saved_cube_pbcor_mask = Some(match mask_read_bytes {
            Some(bytes) => SavedPbcorMask::Existing(bytes),
            None => SavedPbcorMask::PrimaryBeam,
        });
        self
    }

    /// Return the main-lobe cutoff fraction used for beam fitting.
    #[must_use]
    pub const fn psf_cutoff(self) -> f32 {
        self.psf_cutoff
    }

    /// Keep the pixels of a single-term PSF read from an earlier image.
    /// The normal state then holds those pixels in the saved image's units.
    #[must_use]
    pub const fn with_reused_single_psf(mut self) -> Self {
        self.reuses_single_psf = true;
        self
    }

    /// Keep the PSF, sums of weights and primary-beam images already
    /// present on the main field and its outliers. If a saved PSF has
    /// no beam, allow generation to attach the fitted beam to that image.
    /// A residual gridding pass can also publish only the sums it refilled.
    #[must_use]
    pub const fn preserving_saved_psfs(mut self, refits_beams: bool, refills_sums: bool) -> Self {
        self.preserves_saved_psfs = true;
        self.refits_saved_psf_beams = refits_beams;
        self.refills_saved_sums = refills_sums;
        self
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
        let list = problem.product_list();
        let needs_beam = taylor || list.publication().members().iter().any(|id| {
            let entry = &list.entries()[id.ordinal()];
            let needs_validity = |rule| matches!(rule,
                ProductValidityRule::PrimaryBeam(_) | ProductValidityRule::TaylorAndPrimaryBeam { .. });
            matches!(entry.role(), ProductRole::PrimaryBeam(_) | ProductRole::PbCorrectedImage(_))
                || needs_validity(entry.validity())
                || matches!(entry.storage().pixel_mask(), ProductPixelMask::Explicit(rule) if needs_validity(rule))
        });
        if !needs_beam {
            return Ok(());
        }
        check_frequencies(problem, model)
    }
}

impl Default for ContinuumProductControls {
    fn default() -> Self {
        Self {
            psf_cutoff: DEFAULT_PSF_CUTOFF,
            reuses_single_psf: false,
            preserves_saved_psfs: false,
            refits_saved_psf_beams: false,
            refills_saved_sums: false,
            primary_beam_model: None,
            saved_mosaic_beam_read_bytes: None,
            saved_higher_primary_beam: false,
            saved_residual_mask_read_bytes: None,
            saved_cube_model_read_bytes: 0,
            saved_cube_pbcor_mask: None,
            preserves_saved_residual: false,
            saved_weighted_residual: false,
            field_residual_mask: None,
            rebuilds_field_pb: false,
            rebuilds_field_psfs: false,
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
        | ProductRole::SpectralCurvature
        | ProductRole::PbCorrectedSpectralIndex
        | ProductRole::Sensitivity
        | ProductRole::CleanMask => Ok(()),
        role => Err(ProductsError::UnsupportedProductRole { role }),
    }
}

/// Exact compiler-owned inventory and source/run association for one
/// continuum generation.
#[derive(Debug)]
pub struct PlannedContinuumGeneration {
    pub(crate) psf_cutoff: f32,
    pub(crate) reuses_single_psf: bool,
    pub(crate) primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    pub(crate) uses_weight_image: bool,
    pub(crate) saved_mosaic_beam_read_bytes: u64,
    pub(crate) saved_residual_mask_read_bytes: Option<u64>,
    pub(crate) saved_cube_model_read_bytes: u64,
    pub(crate) saved_cube_pbcor_mask: Option<SavedPbcorMask>,
    pub(crate) saved_weighted_residual: bool,
    pub(crate) field_residual_mask: Option<FieldResidualMask>,
    pub(crate) rebuilds_field_pb: bool,
    pub(crate) rebuilds_field_psfs: bool,
    pub(crate) refits_saved_psf_beams: bool,
    pub(crate) members: Box<[PlannedMember]>,
}

impl PlannedContinuumGeneration {
    /// Compile the exact publication inventory for these scientific inputs.
    ///
    /// # Errors
    ///
    /// The PB law does not cover an output channel, the product list
    /// publishes a role the products cannot produce, or a member's size
    /// overflows.
    pub fn new(
        inputs: &ContinuumProductInputs<'_>,
        controls: &ContinuumProductControls,
    ) -> Result<Self, ProductsError> {
        if controls.primary_beam_model() == Some(AnalyticPrimaryBeamModel::SavedRadial)
            && !inputs.has_saved_radial_primary_beam()
        {
            return Err(ProductsError::InvalidControls);
        }
        let mut planned = Self::for_problem(inputs.problem(), controls)?;
        if controls.refits_saved_psf_beams || controls.refills_saved_sums {
            let mut members = planned.members.into_vec();
            members.retain(|member| {
                let domain = inputs.model_domain_ordinal(member.axes().domain());
                match member.role {
                    ProductRole::Psf(_) if controls.refits_saved_psf_beams => {
                        inputs.saved_psf_beams(domain).is_none()
                    }
                    ProductRole::SumWeights(_) if controls.refills_saved_sums => {
                        inputs.normal_state().refilled_sum_weight(domain)
                    }
                    _ => true,
                }
            });
            planned.members = members.into_boxed_slice();
        }
        Ok(planned)
    }

    /// The publication inventory of a run of `problem`, before it runs:
    /// what [`Self::new`] compiles from the run's completion, which the
    /// inventory depends on only through its problem.
    ///
    /// # Errors
    ///
    /// As [`Self::new`].
    pub fn for_problem(
        problem: &CompiledProblem,
        controls: &ContinuumProductControls,
    ) -> Result<Self, ProductsError> {
        controls.validate_for_problem(problem)?;
        let list = problem.product_list();
        let mut members = Vec::with_capacity(list.publication().members().len());
        for id in list.publication().members() {
            let entry = &list.entries()[id.ordinal()];
            let keep_psf =
                controls.preserves_saved_psfs || controls.saved_mosaic_beam_read_bytes.is_some();
            if keep_psf
                && match entry.role() {
                    ProductRole::Psf(_) => {
                        !controls.refits_saved_psf_beams || entry.beam() != ProductBeamRule::Fitted
                    }
                    ProductRole::SumWeights(_) => !controls.refills_saved_sums,
                    ProductRole::Weight(_)
                    | ProductRole::PrimaryBeam(ProductTerm::Single | ProductTerm::Taylor(0)) => {
                        true
                    }
                    ProductRole::PrimaryBeam(ProductTerm::Taylor(1)) => {
                        controls.saved_higher_primary_beam
                    }
                    _ => false,
                }
            {
                continue;
            }
            if controls.preserves_saved_residual && matches!(entry.role(), ProductRole::Residual(_))
            {
                continue;
            }
            ensure_producible(entry.role())?;
            let needs_primary_beam = |rule| {
                matches!(
                    rule,
                    ProductValidityRule::PrimaryBeam(_)
                        | ProductValidityRule::TaylorAndPrimaryBeam { .. }
                )
            };
            let requires_primary_beam = needs_primary_beam(entry.validity())
                || matches!(entry.storage().pixel_mask(), ProductPixelMask::Explicit(rule)
                    if needs_primary_beam(rule));
            if requires_primary_beam && controls.primary_beam_model.is_none() {
                return Err(ProductsError::MissingPrimaryBeamModel);
            }
            let axes = entry.axes();
            let shape = axes.shape();
            let payload_values = shape
                .iter()
                .copied()
                .try_fold(1usize, |total, extent| total.checked_mul(extent))
                .ok_or(ProductsError::ResourceDemandOverflow("product shape"))?;
            members.push(PlannedMember {
                id: entry.id(),
                role: entry.role(),
                name: entry.name().unwrap_or_default().to_string(),
                shape,
                payload_values,
                unit: entry.unit(),
                axes: axes.clone(),
                normalization: entry.normalization(),
                beam_rule: entry.beam(),
                validity: entry.validity(),
                storage: entry.storage(),
            });
        }
        Ok(Self {
            psf_cutoff: controls.psf_cutoff(),
            reuses_single_psf: controls.reuses_single_psf
                && !matches!(
                    problem.reconstruction().basis(),
                    ReconstructionBasis::Taylor { .. }
                ),
            primary_beam_model: controls.primary_beam_model(),
            saved_mosaic_beam_read_bytes: controls.saved_mosaic_beam_read_bytes.unwrap_or(0),
            saved_residual_mask_read_bytes: controls.saved_residual_mask_read_bytes,
            saved_cube_model_read_bytes: controls.saved_cube_model_read_bytes,
            saved_cube_pbcor_mask: controls.saved_cube_pbcor_mask,
            saved_weighted_residual: controls.saved_weighted_residual,
            field_residual_mask: controls.field_residual_mask,
            rebuilds_field_pb: controls.rebuilds_field_pb,
            rebuilds_field_psfs: controls.rebuilds_field_psfs,
            refits_saved_psf_beams: controls.refits_saved_psf_beams,
            uses_weight_image: list
                .entries()
                .iter()
                .any(|entry| matches!(entry.role(), ProductRole::Weight(_))),
            members: members.into_boxed_slice(),
        })
    }

    /// Leave a single-term model unpublished. MVC may stop before it creates
    /// a cube model, and a MEM continuation may leave its saved model
    /// unchanged. Restoration still uses the model in the run's state.
    #[must_use]
    pub fn without_single_term_model(mut self) -> Self {
        let mut members = self.members.into_vec();
        members.retain(|member| {
            !matches!(
                member.role,
                ProductRole::Model(casa_imaging_model::ProductTerm::Single)
            )
        });
        self.members = members.into_boxed_slice();
        self
    }

    /// Keep the cube residual on disk when an MVC restart stopped before
    /// the major cycle that would have replaced it.
    #[must_use]
    pub fn without_mvc_cube_residual(mut self) -> Self {
        let mut members = self.members.into_vec();
        members.retain(|member| !matches!(member.role, ProductRole::Residual(ProductTerm::Single)));
        self.members = members.into_boxed_slice();
        self
    }

    /// Return planned members in exact publication order.
    #[must_use]
    pub const fn members(&self) -> &[PlannedMember] {
        &self.members
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

    /// Whether any member carries beam metadata.
    pub(crate) fn requires_beam(&self) -> bool {
        self.members
            .iter()
            .any(|member| member.beam_rule != ProductBeamRule::None)
    }

    /// The beams `member` carries: the generation's fitted or restoring
    /// beams for its domain, by its beam rule. A member that inherits its
    /// beams carries exactly its source's: CASA's `.image.pbcor` keeps the
    /// restored image's one common beam under `restoringbeam='common'`.
    pub(crate) fn beams_for_member(
        &self,
        member: &PlannedMember,
        fitted: &[Option<RestoringBeam>],
        restoring: &[Option<RestoringBeam>],
    ) -> Box<[Option<RestoringBeam>]> {
        if let ProductBeamRule::Inherit(source) = member.beam_rule
            && let Some(source) = self.members.iter().find(|planned| planned.id == source)
        {
            return self.beams_for_member(source, fitted, restoring);
        }
        // Members are listed domain by domain, in the problem's domain
        // order, which is the order of the generation's beams.
        let mut domains = Vec::new();
        for planned in &self.members {
            if !domains.contains(&planned.axes().domain()) {
                domains.push(planned.axes().domain());
            }
        }
        let domain = domains
            .iter()
            .position(|domain| *domain == member.axes().domain())
            .expect("a member's domain is one of the plan's domains");
        let slice = |beams: &'_ [Option<RestoringBeam>]| -> Box<[Option<RestoringBeam>]> {
            if beams.is_empty() {
                return Box::new([]);
            }
            let per_domain = beams.len() / domains.len();
            beams[domain * per_domain..(domain + 1) * per_domain].into()
        };
        let (fitted, restoring) = (slice(fitted), slice(restoring));
        match member.beam_rule {
            ProductBeamRule::None
            | ProductBeamRule::Restoring(RestoringBeamPolicy::None)
            | ProductBeamRule::Metadata(RestoringBeamPolicy::None) => Box::new([]),
            ProductBeamRule::Fitted => fitted,
            ProductBeamRule::Restoring(RestoringBeamPolicy::PerPlane)
            | ProductBeamRule::Metadata(RestoringBeamPolicy::PerPlane)
            | ProductBeamRule::Inherit(_) => restoring,
            ProductBeamRule::Restoring(RestoringBeamPolicy::Common)
            | ProductBeamRule::Metadata(RestoringBeamPolicy::Common) => {
                vec![restoring.iter().flatten().next().copied()].into_boxed_slice()
            }
            // CASA records the given beam as the image's one beam
            // (`SIImageStore::restore`), whatever its planes hold.
            ProductBeamRule::Restoring(RestoringBeamPolicy::Fixed(beam))
            | ProductBeamRule::Metadata(RestoringBeamPolicy::Fixed(beam)) => {
                vec![fixed_restoring_beam(beam).ok()].into_boxed_slice()
            }
        }
    }

    /// The metadata a generation of this plan leaves.
    pub(crate) fn published(
        &self,
        fitted_beams: Box<[Option<RestoringBeam>]>,
        restoring_beams: Box<[Option<RestoringBeam>]>,
    ) -> PublishedContinuumGeneration {
        let members = self
            .members
            .iter()
            .map(|member| PublishedMember {
                id: member.id,
                name: member.name.clone(),
                resolved_beams: self.beams_for_member(member, &fitted_beams, &restoring_beams),
            })
            .collect();
        PublishedContinuumGeneration {
            fitted_beams,
            restoring_beams,
            members,
        }
    }
}

/// One planned publication member, in product-list order.
#[derive(Debug, Clone)]
pub struct PlannedMember {
    pub(crate) id: ProductId,
    pub(crate) role: ProductRole,
    pub(crate) name: String,
    pub(crate) shape: [usize; 4],
    pub(crate) payload_values: usize,
    pub(crate) unit: ProductUnit,
    pub(crate) axes: ProductAxes,
    pub(crate) normalization: Option<ProductNormalization>,
    pub(crate) beam_rule: ProductBeamRule,
    pub(crate) validity: ProductValidityRule,
    pub(crate) storage: ProductStorageContract,
}

impl PlannedMember {
    /// Return the member's position in the product list.
    #[must_use]
    pub const fn id(&self) -> ProductId {
        self.id
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

/// Payload-free metadata for one generated member.
#[derive(Debug, Clone)]
pub struct PublishedMember {
    id: ProductId,
    name: String,
    resolved_beams: Box<[Option<RestoringBeam>]>,
}

impl PublishedMember {
    /// Return the member's position in the product list.
    #[must_use]
    pub const fn id(&self) -> ProductId {
        self.id
    }

    /// Return the compiled product name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
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
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[PublishedMember]>,
}

impl PublishedContinuumGeneration {
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

    /// Return generated members in product-list order.
    #[must_use]
    pub const fn members(&self) -> &[PublishedMember] {
        &self.members
    }
}
