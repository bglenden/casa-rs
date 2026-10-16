// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct scientific inputs for continuum products.
//!
//! The inputs borrow one whole Major-Cycle completion, so the final normal
//! state and model they carry come from the same reconciliation.

use std::borrow::Cow;
use std::collections::BTreeMap;

use casa_imaging_deconvolution::{
    FinalNormalState, ImageDomainReconstructionMasks, MajorCycleCompletion, ModelGeneration,
    NormalStateCatalog, ReconstructionMask,
};
use casa_imaging_model::{CompiledProblem, ImageDomainRole, ReconstructionBasis};

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
#[derive(Debug)]
pub struct ContinuumProductInputs<'a> {
    problem: &'a CompiledProblem,
    normal_state: &'a FinalNormalState,
    final_model: &'a ModelGeneration,
    reconstruction_mask: Option<&'a ReconstructionMask>,
    domain_reconstruction_masks: Option<&'a ImageDomainReconstructionMasks>,
    primary_beam_centres: Option<&'a PrimaryBeamCentres>,
    facet_sum_weights: Option<&'a FacetSumWeights>,
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
        match normal.catalog() {
            NormalStateCatalog::UnnormalizedTaylorBlockV1 => {
                let ReconstructionBasis::Taylor { terms } = problem.reconstruction().basis() else {
                    panic!("a Taylor normal state comes from a Taylor-basis problem");
                };
                assert!(
                    terms >= 2
                        && normal.coefficient_term_count() == terms
                        && normal.normal_moment_count() == 2 * terms - 1
                        && model.coefficients() == terms
                        && domains.len() == 1,
                    "a Taylor completion has its problem's terms, 2·terms − 1 moments and one domain"
                );
            }
            NormalStateCatalog::UnnormalizedPlaneV1
            | NormalStateCatalog::UnnormalizedChannelSlabV1 => {
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
            facet_sum_weights: None,
        }
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

    /// Bind the exact reconstruction mask used by the final bounded solve.
    ///
    /// # Panics
    ///
    /// If the mask does not cover the main domain's grid.
    #[must_use]
    pub fn with_reconstruction_mask(mut self, mask: &'a ReconstructionMask) -> Self {
        let main = self.domain(&ImageDomainRole::Main);
        assert!(
            mask.shape() == main.shape().pixels() && mask.coordinate() == main.direction(),
            "the run's reconstruction mask covers its main domain"
        );
        self.reconstruction_mask = Some(mask);
        self.domain_reconstruction_masks = None;
        self
    }

    /// Bind the exact spatial support for every canonical image domain.
    ///
    /// # Panics
    ///
    /// If the masks are not one per domain, each covering its domain.
    #[must_use]
    pub fn with_domain_reconstruction_masks(
        mut self,
        masks: &'a ImageDomainReconstructionMasks,
    ) -> Self {
        let domains = self.problem.geometry().domains();
        assert!(
            masks.len() == domains.len()
                && masks.iter().zip(domains).all(|(mask, domain)| {
                    mask.shape() == domain.shape().pixels()
                        && mask.coordinate() == domain.direction()
                }),
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

    /// The pixel positions an analytic primary beam is centred on in the
    /// domain `role`, whose reference pixel is `reference_pixel`: borrowed
    /// from the bound pointings, so a plane holds no copy of them.
    pub(crate) fn primary_beam_centres(
        &self,
        role: &ImageDomainRole,
        reference_pixel: [f64; 2],
    ) -> Cow<'a, [[f64; 2]]> {
        self.primary_beam_centres.map_or_else(
            || Cow::Owned(vec![reference_pixel]),
            |centres| Cow::Borrowed(centres[role].as_slice()),
        )
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
