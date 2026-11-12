// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct scientific inputs for continuum products.
//!
//! The inputs borrow one whole Major-Cycle completion, so the final normal
//! state and model they carry come from the same reconciliation.

use std::collections::BTreeMap;

use casa_imaging_deconvolution::{
    FinalNormalState, ImageDomainReconstructionMasks, MajorCycleCompletion, ModelGeneration,
    NormalBasis, ReconstructionMask, RestoringBeam,
};
use casa_imaging_model::{
    CompiledImageDomain, CompiledProblem, ImageDomainRole, ReconstructionBasis,
};

use crate::AnalyticPrimaryBeamModel;
use crate::analytic_beam::AnalyticPrimaryBeam;

/// Where an analytic primary beam is centred on each image domain: the
/// pixel position `[x, y]` of every selected field's phase direction.
///
/// CASA centres one beam on each field's `PHASE_DIR` and sums them on every
/// image, outliers included (`SynthesisImagerVi2::makePrimaryBeam`,
/// `SIMapper::addPB`).
pub type PrimaryBeamCentres = BTreeMap<ImageDomainRole, Vec<[f64; 2]>>;

/// Borrowed scientific payloads for producing planned members.
///
/// This value borrows one whole completion and its compiled problem, so the
/// final normal state and model it carries come from the same reconciliation.
/// Binding them checks once that they have the problem's shape; product
/// generation then indexes them without checking again.
#[derive(Clone, Debug)]
pub struct ContinuumProductInputs<'a> {
    problem: &'a CompiledProblem,
    normal_state: &'a FinalNormalState,
    final_model: &'a ModelGeneration,
    reconstruction_mask: Option<&'a ReconstructionMask>,
    domain_reconstruction_masks: Option<&'a ImageDomainReconstructionMasks>,
    primary_beam_centres: Option<&'a PrimaryBeamCentres>,
    taylor_primary_beam: Option<&'a [f32]>,
    taylor_residual_sum_weight: Option<f64>,
    cube_taylor_primary_beam: Option<&'a [f32]>,
    vla_selection_hz: Option<f64>,
    facet_sum_weights: Option<&'a FacetSumWeights>,
    saved_psf_beams: Option<&'a [RestoringBeam]>,
    saved_mosaic_beam: Option<&'a crate::SavedMosaicBeam>,
}

/// Each faceted image domain's facets' own sums of weights, by domain
/// ordinal: per facet in CASA's order (facet `row·N + column`, facet 0 at
/// the bottom left), per polarization. CASA's `.sumwt` of a faceted image
/// holds each facet store's own sum (`SIImageStore::getSubImageStore`).
pub type FacetSumWeights = std::collections::BTreeMap<usize, Vec<Vec<f64>>>;

impl<'a> ContinuumProductInputs<'a> {
    /// Bind the completion of a run of `problem`.
    ///
    /// # Panics
    ///
    /// If the completion does not have `problem`'s image domains, their
    /// shapes, planes and Taylor terms: it came from another problem's run.
    pub fn from_major_cycle(problem: &'a CompiledProblem, join: &'a MajorCycleCompletion) -> Self {
        let normal = join.normal_state();
        let model = join.final_model().shape();
        let domains = problem.geometry().domains();
        assert!(
            normal.domain_count() == domains.len()
                && model.domains().len() == domains.len()
                && domains.iter().all(|domain| {
                    let pixels = domain.shape().pixels();
                    model
                        .domain_roles()
                        .iter()
                        .position(|role| role == domain.role())
                        .is_some_and(|ordinal| {
                            model.domains()[ordinal].pixels() == pixels
                                && normal.domain_shape(ordinal) == Some(pixels)
                        })
                }),
            "a run's completion has its problem's image domains and their shapes"
        );
        match normal.basis() {
            NormalBasis::Taylor => {
                let ReconstructionBasis::Taylor { terms } = problem.reconstruction().basis() else {
                    panic!("a Taylor normal state comes from a Taylor-basis problem");
                };
                assert!(
                    terms >= 1
                        && normal.coefficient_term_count() == terms
                        && normal.normal_moment_count() == 2 * terms - 1
                        && model.coefficients() == terms
                        && model.polarizations() == normal.polarization_count()
                        && domains.len() == 1,
                    "a Taylor completion has its problem's terms, 2·terms − 1 moments, \
                     polarizations and one domain"
                );
            }
            NormalBasis::Constant | NormalBasis::ChannelLocal => {
                let slab = normal.slab();
                assert!(
                    slab.core_range() == (0..slab.total_channels())
                        && normal.channel_count() == slab.total_channels()
                        && model.coefficients() == normal.channel_count()
                        && model.polarizations() == normal.polarization_count(),
                    "a run's final completion covers every output channel and polarization"
                );
            }
        }
        Self {
            problem,
            normal_state: normal,
            final_model: join.final_model(),
            reconstruction_mask: None,
            domain_reconstruction_masks: None,
            primary_beam_centres: None,
            taylor_primary_beam: None,
            taylor_residual_sum_weight: None,
            cube_taylor_primary_beam: None,
            vla_selection_hz: None,
            facet_sum_weights: None,
            saved_psf_beams: None,
            saved_mosaic_beam: None,
        }
    }

    /// Bind a precomputed primary beam for a Taylor family, in x-major order.
    ///
    /// This is the already-normalized beam for the one Taylor image domain,
    /// such as a sumweight-weighted mean of the published cube-channel PB
    /// planes. CASA MVC retains its values below the PB cutoff while masking
    /// those pixels. The caller retains and accounts for the borrowed plane
    /// until generation ends.
    ///
    /// # Errors
    ///
    /// The completion is not a Taylor family, the plane has the wrong size,
    /// or it contains a negative or non-finite value.
    pub fn with_taylor_primary_beam(
        mut self,
        beam: &'a [f32],
    ) -> Result<Self, crate::ProductsError> {
        if self.normal_state.basis() != NormalBasis::Taylor {
            return Err(crate::ProductsError::InvalidTaylorPrimaryBeam);
        }
        let [width, height] = self.normal_state.shape();
        let expected =
            width
                .checked_mul(height)
                .ok_or(crate::ProductsError::ResourceDemandOverflow(
                    "Taylor primary-beam plane",
                ))?;
        if beam.len() != expected {
            return Err(crate::ProductsError::PayloadLengthMismatch {
                expected,
                actual: beam.len(),
            });
        }
        if beam.iter().any(|value| !value.is_finite() || *value < 0.0) {
            return Err(crate::ProductsError::InvalidTaylorPrimaryBeam);
        }
        self.taylor_primary_beam = Some(beam);
        Ok(self)
    }

    /// Bind MVC's weighted Taylor beam for adjusting a cube's residual.
    ///
    /// The caller owns and admits the x-major plane until generation ends.
    /// Its channel PB and the PB cutoff adjust only the cube residual, as
    /// `SynthesisUtilMethods::removeFreqDepPB` does before Taylor reduction.
    ///
    /// # Errors
    ///
    /// The completion is not a single-domain, single-polarization cube, or
    /// the beam's size or values are invalid.
    pub fn with_cube_taylor_primary_beam(
        mut self,
        beam: &'a [f32],
    ) -> Result<Self, crate::ProductsError> {
        if !supports_cube_taylor_beam(
            self.normal_state.basis(),
            self.normal_state.domain_count(),
            self.normal_state.polarization_count(),
        ) {
            return Err(crate::ProductsError::InvalidCubeTaylorPrimaryBeam);
        }
        let [width, height] = self.normal_state.shape();
        let expected =
            width
                .checked_mul(height)
                .ok_or(crate::ProductsError::ResourceDemandOverflow(
                    "cube Taylor primary-beam plane",
                ))?;
        if beam.len() != expected {
            return Err(crate::ProductsError::PayloadLengthMismatch {
                expected,
                actual: beam.len(),
            });
        }
        if beam.iter().any(|value| !value.is_finite() || *value < 0.0) {
            return Err(crate::ProductsError::InvalidCubeTaylorPrimaryBeam);
        }
        self.cube_taylor_primary_beam = Some(beam);
        Ok(self)
    }

    pub(crate) fn cube_taylor_primary_beam(&self) -> Option<&'a [f32]> {
        self.cube_taylor_primary_beam
    }

    /// Bind the divisor for an already-normalized Taylor residual family.
    ///
    /// CASA MVC's cube-to-Taylor conversion produces normalized residual
    /// planes while retaining its distinct published sum-weight moments.
    /// Supplying `1.0` preserves those Float pixels without multiplying and
    /// dividing them; published sum weights remain unchanged.
    ///
    /// # Errors
    ///
    /// The completion is not Taylor-based, or the divisor is nonpositive or
    /// nonfinite.
    pub fn with_taylor_residual_sum_weight(
        mut self,
        divisor: f64,
    ) -> Result<Self, crate::ProductsError> {
        if self.normal_state.basis() != NormalBasis::Taylor
            || !divisor.is_finite()
            || divisor <= 0.0
        {
            return Err(crate::ProductsError::InvalidTaylorResidualSumWeight);
        }
        self.taylor_residual_sum_weight = Some(divisor);
        Ok(self)
    }

    pub(crate) fn taylor_residual_sum_weight(&self) -> Option<f64> {
        self.taylor_residual_sum_weight
    }

    /// The supplied, already-normalized beam of the Taylor domain.
    pub(crate) fn taylor_primary_beam(&self) -> Option<&'a [f32]> {
        self.taylor_primary_beam
    }

    /// Bind the complete PSF beam grid retained from an earlier run.
    ///
    /// # Errors
    /// The grid does not cover the main image's channels and polarizations.
    pub fn with_saved_psf_beams(
        mut self,
        beams: &'a [RestoringBeam],
    ) -> Result<Self, crate::ProductsError> {
        let channels = self.normal_state.channel_count();
        let expected = channels * self.normal_state.polarization_count();
        if beams.len() != expected {
            return Err(crate::ProductsError::BeamGrid {
                beams: beams.len(),
                channels,
            });
        }
        self.saved_psf_beams = Some(beams);
        Ok(self)
    }

    /// Bind an earlier MFS mosaic's PB and pixel mask for the products of a
    /// continuation, polarization-major with x-major image planes.
    ///
    /// # Errors
    /// The completion is outside the single-term mosaic layout, or the
    /// saved PB does not match its grid.
    pub fn with_saved_mosaic_beam(
        mut self,
        beam: &'a crate::SavedMosaicBeam,
    ) -> Result<Self, crate::ProductsError> {
        let normal = self.normal_state;
        let [width, height] = normal.shape();
        let expected = [
            width,
            height,
            normal.polarization_count(),
            normal.channel_count(),
        ];
        if normal.basis() == NormalBasis::Taylor || normal.domain_count() != 1 {
            return Err(crate::ProductsError::UnsupportedProblem);
        }
        if beam.shape() != expected {
            return Err(crate::ProductsError::SavedMosaicBeamShape {
                saved: beam.shape(),
                expected,
            });
        }
        self.saved_mosaic_beam = Some(beam);
        Ok(self)
    }

    pub(crate) fn saved_mosaic_beam(&self) -> Option<&'a crate::SavedMosaicBeam> {
        self.saved_mosaic_beam
    }

    /// The main image's saved PSF beams, channel-major.
    pub(crate) fn saved_psf_beams(&self) -> Option<&'a [RestoringBeam]> {
        self.saved_psf_beams
    }

    /// Bind each faceted domain's facets' own sums of weights, which its
    /// `.sumwt` publishes one per facet pixel; an unfaceted domain publishes
    /// its normal state's sum.
    ///
    /// # Panics
    ///
    /// If an entry is not a faceted domain of the problem with one sum per
    /// facet and polarization.
    #[must_use]
    pub fn with_facet_sum_weights(mut self, sums: &'a FacetSumWeights) -> Self {
        let domains = self.problem.geometry().domains();
        let polarizations = self.normal_state.polarization_count();
        assert!(
            sums.iter().all(|(ordinal, facets)| {
                domains.get(*ordinal).is_some_and(|domain| {
                    domain.facets().len() > 1
                        && facets.len() == domain.facets().len()
                        && facets.iter().all(|facet| facet.len() == polarizations)
                })
            }),
            "facet sums of weights cover faceted domains, one per facet and polarization"
        );
        self.facet_sum_weights = Some(sums);
        self
    }

    /// Domain `ordinal`'s facets' own sums of weights when it is faceted.
    #[must_use]
    pub fn facet_sum_weights(&self, ordinal: usize) -> Option<&'a [Vec<f64>]> {
        self.facet_sum_weights
            .and_then(|sums| sums.get(&ordinal))
            .map(Vec::as_slice)
    }

    /// Whether `mask` covers every plane of `domain`: its grid, and each of
    /// the state's channels and polarizations (a Taylor family is one
    /// channel).
    fn covers(&self, mask: &ReconstructionMask, domain: &CompiledImageDomain) -> bool {
        let normal = self.normal_state;
        let channels = if normal.basis() == NormalBasis::Taylor {
            1
        } else {
            normal.channel_count()
        };
        mask.shape() == domain.shape().pixels()
            && mask.coordinate() == domain.direction()
            && mask.planes().covers(channels, normal.polarization_count())
    }

    /// Bind the exact reconstruction mask used by the final bounded solve.
    ///
    /// # Panics
    ///
    /// If the mask does not cover every plane of the main domain.
    #[must_use]
    pub fn with_reconstruction_mask(mut self, mask: &'a ReconstructionMask) -> Self {
        let main = self.domain(&ImageDomainRole::Main);
        assert!(
            self.covers(mask, main),
            "the run's reconstruction mask covers its main domain"
        );
        self.reconstruction_mask = Some(mask);
        self.domain_reconstruction_masks = None;
        self
    }

    /// Bind the exact support of every plane of every canonical image
    /// domain.
    ///
    /// # Panics
    ///
    /// If the masks are not one per domain, each covering its domain's
    /// planes.
    #[must_use]
    pub fn with_domain_reconstruction_masks(
        mut self,
        masks: &'a ImageDomainReconstructionMasks,
    ) -> Self {
        let domains = self.problem.geometry().domains();
        assert!(
            masks.len() == domains.len()
                && masks
                    .iter()
                    .zip(domains)
                    .all(|(mask, domain)| self.covers(mask, domain)),
            "the run's reconstruction masks cover its domains, one each"
        );
        self.reconstruction_mask = None;
        self.domain_reconstruction_masks = Some(masks);
        self
    }

    /// Centre an analytic primary beam on the selected fields' pointings.
    /// Without them it is centred on each domain's reference pixel. A
    /// domain with no pointing has a zero PB.
    ///
    /// # Panics
    ///
    /// If a domain of the problem has no entry.
    #[must_use]
    pub fn with_primary_beam_centres(mut self, centres: &'a PrimaryBeamCentres) -> Self {
        assert!(
            self.problem
                .geometry()
                .domains()
                .iter()
                .all(|domain| centres.contains_key(domain.role())),
            "the pointings cover every image domain"
        );
        self.primary_beam_centres = Some(centres);
        self
    }

    /// Select the legacy VLA aperture at `frequency_hz`, while evaluating
    /// it at each output plane's own frequency.
    ///
    /// # Panics
    ///
    /// If the selection frequency is non-finite or not positive.
    #[must_use]
    pub fn with_vla_selection_frequency(mut self, frequency_hz: f64) -> Self {
        assert!(frequency_hz.is_finite() && frequency_hz > 0.0);
        self.vla_selection_hz = Some(frequency_hz);
        self
    }

    /// The run's analytic primary beam of `model`, centred on the bound
    /// pointings; `None` for the mosaic's, which is each plane's weight
    /// image. Planning checked its frequencies
    /// ([`crate::ContinuumProductControls::validate_for_problem`]).
    pub(crate) fn analytic_primary_beam(
        &self,
        model: AnalyticPrimaryBeamModel,
    ) -> Option<AnalyticPrimaryBeam<'a>> {
        AnalyticPrimaryBeam::bind(self.problem, model, self.primary_beam_centres).map(|beam| {
            self.vla_selection_hz.map_or(beam, |frequency| {
                beam.with_vla_selection_frequency(frequency)
            })
        })
    }

    /// Borrow the exact compiled problem behind these payloads.
    #[must_use]
    pub const fn problem(&self) -> &CompiledProblem {
        self.problem
    }

    fn domain(&self, role: &ImageDomainRole) -> &casa_imaging_model::CompiledImageDomain {
        self.problem
            .geometry()
            .domains()
            .iter()
            .find(|domain| domain.role() == role)
            .expect("a product's domain is one of its problem's domains")
    }

    /// Return radians-per-pixel on each direction axis of the main domain.
    #[must_use]
    pub fn cell_size_rad(&self) -> [f64; 2] {
        self.cell_size_rad_for_domain(&ImageDomainRole::Main)
    }

    /// Return radians-per-pixel on each direction axis of one image domain.
    pub(crate) fn cell_size_rad_for_domain(&self, role: &ImageDomainRole) -> [f64; 2] {
        let increment = self.domain(role).direction().increment_rad();
        [increment[0].abs(), increment[1].abs()]
    }

    /// The ordinal of a domain in the normal state and the model, which
    /// share the problem's domain order.
    pub(crate) fn model_domain_ordinal(&self, role: &ImageDomainRole) -> usize {
        self.final_model
            .shape()
            .domain_roles()
            .iter()
            .position(|candidate| candidate == role)
            .expect("a product's domain is one of its problem's domains")
    }

    /// Borrow the authoritative normal state (residual, PSF, sensitivity).
    #[must_use]
    pub const fn normal_state(&self) -> &FinalNormalState {
        self.normal_state
    }

    /// Borrow the authoritative final model generation.
    #[must_use]
    pub const fn final_model(&self) -> &ModelGeneration {
        self.final_model
    }

    /// Borrow the exact CLEAN mask, when a bounded solve supplied one.
    #[must_use]
    pub const fn reconstruction_mask(&self) -> Option<&ReconstructionMask> {
        self.reconstruction_mask
    }

    /// Borrow canonical per-domain CLEAN masks, when supplied.
    #[must_use]
    pub const fn domain_reconstruction_masks(&self) -> Option<&ImageDomainReconstructionMasks> {
        self.domain_reconstruction_masks
    }
}

/// The current MVC conversion owns one common beam for one domain and Stokes
/// plane; accepting other shapes would silently reuse it for unrelated planes.
pub(crate) fn supports_cube_taylor_beam(
    basis: NormalBasis,
    domains: usize,
    polarizations: usize,
) -> bool {
    basis == NormalBasis::ChannelLocal && domains == 1 && polarizations == 1
}

#[cfg(test)]
mod mvc_beam_shape_tests {
    use super::{NormalBasis, supports_cube_taylor_beam};

    #[test]
    fn cube_taylor_beam_is_bound_only_to_its_one_domain_and_stokes_plane() {
        assert!(supports_cube_taylor_beam(NormalBasis::ChannelLocal, 1, 1));
        assert!(!supports_cube_taylor_beam(NormalBasis::Constant, 1, 1));
        assert!(!supports_cube_taylor_beam(NormalBasis::Taylor, 1, 1));
        assert!(!supports_cube_taylor_beam(NormalBasis::ChannelLocal, 2, 1));
        assert!(!supports_cube_taylor_beam(NormalBasis::ChannelLocal, 1, 2));
    }
}
