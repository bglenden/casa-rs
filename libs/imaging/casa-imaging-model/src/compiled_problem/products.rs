// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Logical image product requested from the problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductKind {
    /// Point-spread function.
    Psf,
    /// Authoritative final residual.
    Residual,
    /// Reconstructed coefficient model.
    Model,
    /// Restored image.
    RestoredImage,
    /// Sum-of-weights state.
    SumWeights,
    /// Reconstruction mask.
    Mask,
    /// Imaging weight image.
    Weight,
    /// Primary-beam response.
    PrimaryBeam,
    /// Sensitivity response.
    Sensitivity,
    /// Primary-beam-corrected restored image.
    PbCorrectedImage,
    /// Taylor coefficient products.
    TaylorTerms,
    /// Spectral-index product.
    SpectralIndex,
    /// Spectral-index uncertainty.
    SpectralIndexError,
    /// Spectral curvature from the first three restored Taylor terms.
    SpectralCurvature,
    /// Primary-beam-corrected spectral index.
    PbCorrectedSpectralIndex,
    /// Restoring and fitted beam metadata.
    Beam,
}

impl ProductKind {
    /// Complete stable catalog of logical products understood by the current
    /// request compiler.
    pub const ALL: [Self; 16] = [
        Self::Psf,
        Self::Residual,
        Self::Model,
        Self::RestoredImage,
        Self::SumWeights,
        Self::Mask,
        Self::Weight,
        Self::PrimaryBeam,
        Self::Sensitivity,
        Self::PbCorrectedImage,
        Self::TaylorTerms,
        Self::SpectralIndex,
        Self::SpectralIndexError,
        Self::SpectralCurvature,
        Self::PbCorrectedSpectralIndex,
        Self::Beam,
    ];

    /// Return the stable request-catalog identity.
    #[must_use]
    pub const fn catalog_id(self) -> &'static str {
        match self {
            Self::Psf => "psf",
            Self::Residual => "residual",
            Self::Model => "model",
            Self::RestoredImage => "restored_image",
            Self::SumWeights => "sum_weights",
            Self::Mask => "mask",
            Self::Weight => "weight",
            Self::PrimaryBeam => "primary_beam",
            Self::Sensitivity => "sensitivity",
            Self::PbCorrectedImage => "pb_corrected_image",
            Self::TaylorTerms => "taylor_terms",
            Self::SpectralIndex => "spectral_index",
            Self::SpectralIndexError => "spectral_index_error",
            Self::SpectralCurvature => "spectral_curvature",
            Self::PbCorrectedSpectralIndex => "pb_corrected_spectral_index",
            Self::Beam => "beam",
        }
    }
}

/// Published image normalization semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductNormalization {
    /// Unit-response normalization without direction-dependent sensitivity division.
    UnitResponse,
    /// Flat-noise normalization using sensitivity state.
    FlatNoise,
    /// Flat-sky normalization using sensitivity state.
    FlatSky,
    /// CASA's `pbsquare` normalization using sensitivity state: the
    /// residual divided by the sensitivity's peak, an image of optimal
    /// signal to noise whose sky is weighted by the beam squared, and the
    /// model taken to and from it through the beam squared.
    PbSquare,
}

/// Comparison used to decide whether a product pixel has valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductSupportComparison {
    /// The measured support must be strictly greater than the configured threshold.
    StrictlyGreater,
}

/// Numerical treatment of pixels outside a product's valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductBlankingPolicy {
    /// Store numeric zero, independently of any attached pixel mask.
    Zero,
}

/// Stored pixel-mask policy for uncorrected residual and restored images.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UncorrectedImageMaskPolicy {
    /// Do not attach a pixel mask; internal numerical blanking still applies.
    None,
    /// Attach the configured primary-beam support without changing pixel values.
    PrimaryBeam,
}

/// Reference statistic used by the MT-MFS Taylor-support threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaylorSupportReference {
    /// Positive maximum of the temporary principal-solution Taylor-zero residual.
    PrincipalResidualTaylor0PositiveMaximum,
}

/// Failure to construct a finite product-validity policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProductValidityPolicyError {
    /// Primary-beam support requires a finite positive cutoff.
    #[error("primary-beam support cutoff must be finite and positive")]
    InvalidPrimaryBeamCutoff,
    /// Taylor support requires a finite fraction in `(0, 1]`.
    #[error("Taylor support peak fraction must be finite, positive, and at most one")]
    InvalidTaylorPeakFraction,
}

/// Exact primary-beam support and blanking policy carried by a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryBeamValidityPolicy {
    pub(super) cutoff_bits: u32,
    pub(super) comparison: ProductSupportComparison,
    pub(super) blanking: ProductBlankingPolicy,
}

impl PrimaryBeamValidityPolicy {
    /// Construct an exact finite primary-beam support policy.
    pub fn new(
        cutoff: f32,
        comparison: ProductSupportComparison,
        blanking: ProductBlankingPolicy,
    ) -> Result<Self, ProductValidityPolicyError> {
        if !(cutoff.is_finite() && cutoff > 0.0) {
            return Err(ProductValidityPolicyError::InvalidPrimaryBeamCutoff);
        }
        Ok(Self {
            cutoff_bits: cutoff.to_bits(),
            comparison,
            blanking,
        })
    }

    /// Return the exact primary-beam cutoff.
    #[must_use]
    pub fn cutoff(self) -> f32 {
        f32::from_bits(self.cutoff_bits)
    }

    /// Return the exact support comparison.
    #[must_use]
    pub const fn comparison(self) -> ProductSupportComparison {
        self.comparison
    }

    /// Return the exact persisted treatment outside support.
    #[must_use]
    pub const fn blanking(self) -> ProductBlankingPolicy {
        self.blanking
    }
}

/// Exact Taylor-coefficient support and blanking policy carried by a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaylorValidityPolicy {
    pub(super) reference: TaylorSupportReference,
    pub(super) peak_fraction_bits: u32,
    pub(super) comparison: ProductSupportComparison,
    pub(super) blanking: ProductBlankingPolicy,
}

impl TaylorValidityPolicy {
    /// Construct an exact finite Taylor-support policy.
    pub fn new(
        reference: TaylorSupportReference,
        peak_fraction: f32,
        comparison: ProductSupportComparison,
        blanking: ProductBlankingPolicy,
    ) -> Result<Self, ProductValidityPolicyError> {
        if !(peak_fraction.is_finite() && peak_fraction > 0.0 && peak_fraction <= 1.0) {
            return Err(ProductValidityPolicyError::InvalidTaylorPeakFraction);
        }
        Ok(Self {
            reference,
            peak_fraction_bits: peak_fraction.to_bits(),
            comparison,
            blanking,
        })
    }

    /// Return the reference statistic for the Taylor threshold.
    #[must_use]
    pub const fn reference(self) -> TaylorSupportReference {
        self.reference
    }

    /// Return the fraction applied to the reference statistic.
    #[must_use]
    pub fn peak_fraction(self) -> f32 {
        f32::from_bits(self.peak_fraction_bits)
    }

    /// Return the exact support comparison.
    #[must_use]
    pub const fn comparison(self) -> ProductSupportComparison {
        self.comparison
    }

    /// Return the exact persisted treatment outside support.
    #[must_use]
    pub const fn blanking(self) -> ProductBlankingPolicy {
        self.blanking
    }
}

/// Exact compiler-owned validity policies for all requested products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductValidityPolicies {
    pub(super) primary_beam: PrimaryBeamValidityPolicy,
    pub(super) taylor: TaylorValidityPolicy,
    pub(super) uncorrected_mask: UncorrectedImageMaskPolicy,
}

impl ProductValidityPolicies {
    /// Bind the exact primary-beam and Taylor validity policies into a request.
    #[must_use]
    pub const fn new(
        primary_beam: PrimaryBeamValidityPolicy,
        taylor: TaylorValidityPolicy,
    ) -> Self {
        Self {
            primary_beam,
            taylor,
            uncorrected_mask: UncorrectedImageMaskPolicy::None,
        }
    }

    /// Select stored-mask attachment for uncorrected products, separately from blanking.
    #[must_use]
    pub const fn with_uncorrected_mask(mut self, policy: UncorrectedImageMaskPolicy) -> Self {
        self.uncorrected_mask = policy;
        self
    }

    /// Return the primary-beam support policy.
    #[must_use]
    pub const fn primary_beam(self) -> PrimaryBeamValidityPolicy {
        self.primary_beam
    }

    /// Return the Taylor-coefficient support policy.
    #[must_use]
    pub const fn taylor(self) -> TaylorValidityPolicy {
        self.taylor
    }

    /// Return stored-mask attachment for uncorrected products.
    #[must_use]
    pub const fn uncorrected_mask(self) -> UncorrectedImageMaskPolicy {
        self.uncorrected_mask
    }
}

/// Requested product set and publication semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductRequirements {
    pub(super) products: Vec<ProductKind>,
    pub(super) normalization: ProductNormalization,
    pub(super) restoring_beam: RestoringBeamPolicy,
    pub(super) validity: ProductValidityPolicies,
    pub(super) normalization_boundary: ProductNormalizationBoundary,
}

impl ProductRequirements {
    /// Construct product requirements. Compilation canonicalizes product ordering.
    #[must_use]
    pub fn new(
        products: Vec<ProductKind>,
        normalization: ProductNormalization,
        restoring_beam: RestoringBeamPolicy,
        validity: ProductValidityPolicies,
    ) -> Self {
        let normalization_boundary =
            compile_product_boundary(&products, normalization, restoring_beam);
        Self {
            products,
            normalization,
            restoring_beam,
            validity,
            normalization_boundary,
        }
    }

    /// Return requested products in canonical order after compilation.
    #[must_use]
    pub fn products(&self) -> &[ProductKind] {
        &self.products
    }

    /// Return product normalization semantics.
    #[must_use]
    pub const fn normalization(&self) -> ProductNormalization {
        self.normalization
    }

    /// Return restoring-beam semantics.
    #[must_use]
    pub const fn restoring_beam(&self) -> RestoringBeamPolicy {
        self.restoring_beam
    }

    /// Return the exact product-validity policies supplied by the request.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityPolicies {
        self.validity
    }

    /// Return the downstream handoff that keeps product operations outside A*.
    #[must_use]
    pub const fn normalization_boundary(&self) -> &ProductNormalizationBoundary {
        &self.normalization_boundary
    }

    pub(super) fn canonicalize(mut self) -> Self {
        self.products.sort_unstable();
        self.products.dedup();
        self.normalization_boundary =
            compile_product_boundary(&self.products, self.normalization, self.restoring_beam);
        self
    }

    pub(crate) fn contains(&self, product: ProductKind) -> bool {
        self.products.binary_search(&product).is_ok()
    }
}
