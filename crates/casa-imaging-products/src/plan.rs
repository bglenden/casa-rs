// SPDX-License-Identifier: LGPL-3.0-or-later

//! The product plan: the controls a run's products are made with, the
//! members planned from the problem's product list, and the metadata a
//! generation leaves.

use casa_imaging_model::{
    CompiledProblem, ProductAxes, ProductBeamRule, ProductId, ProductNormalization,
    ProductPixelMask, ProductRole, ProductStorageContract, ProductUnit, ProductValidityRule,
    ReconstructionBasis, RestoringBeamPolicy,
};

use crate::beam::RestoringBeam;
use crate::error::ProductsError;
use crate::source::ContinuumProductInputs;
use crate::taylor::primary_beam_frequency_supported;

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
        let spectral = problem.geometry().spectral();
        let channels = if taylor {
            1
        } else {
            spectral.output_channels()
        };
        for output_channel in 0..channels {
            let frequency_hz = spectral
                .channel_centre_hz(output_channel)
                .expect("an output channel has a centre frequency");
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
        role => Err(ProductsError::UnsupportedProductRole { role }),
    }
}

/// Exact compiler-owned inventory and source/run association for one
/// continuum generation.
#[derive(Debug)]
pub struct PlannedContinuumGeneration {
    pub(crate) psf_cutoff: f32,
    pub(crate) primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    pub(crate) uses_weight_image: bool,
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
        controls.validate_for_problem(inputs.problem())?;
        let list = inputs.problem().product_list();
        let mut members = Vec::with_capacity(list.publication().members().len());
        for id in list.publication().members() {
            let entry = &list.entries()[id.ordinal()];
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
            primary_beam_model: controls.primary_beam_model(),
            uses_weight_image: list
                .entries()
                .iter()
                .any(|entry| matches!(entry.role(), ProductRole::Weight(_))),
            members: members.into_boxed_slice(),
        })
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
    /// beams for its domain, by its beam rule.
    pub(crate) fn beams_for_member(
        &self,
        member: &PlannedMember,
        fitted: &[Option<RestoringBeam>],
        restoring: &[Option<RestoringBeam>],
    ) -> Box<[Option<RestoringBeam>]> {
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
