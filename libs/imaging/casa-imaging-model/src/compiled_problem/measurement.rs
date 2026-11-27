// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Spectral coefficient kernel shared exactly by prediction and adjoint imaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralKernel {
    /// Preserve channel-local samples exactly.
    Identity,
    /// Select the nearest covered output-channel centre.
    Nearest,
    /// Interpolate between the two bracketing output-channel centres.
    Linear,
    /// Fit the four-point Lagrange polynomial used by casacore cubic interpolation.
    Cubic,
    /// Integrate source-channel intervals into output-channel intervals.
    ChannelIntegration {
        /// Planner-proved upper bound on non-zero output terms for one source channel.
        maximum_terms: usize,
    },
}

/// Treatment of a source channel whose support touches or crosses output-axis edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralEdgePolicy {
    /// Reject samples without the complete requested interpolation support.
    CompleteSupport,
    /// Retain the covered fraction of interval-integration samples.
    PartialOverlap,
}

/// Covariance law declared for a compiled spectral stencil.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralCovariance {
    /// Source-sample noise is independent and shared-source output covariance is `A C A^H`.
    PropagateIndependentSourceNoise,
}

/// One coherent paired spectral sampling law.
///
/// Coefficients are compiled once from this law and reused byte-for-byte by
/// prediction, weighting, PSF, dirty/adjoint, and sum-weight consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpectralSamplingLaw {
    pub(super) kernel: SpectralKernel,
    pub(super) edge_policy: SpectralEdgePolicy,
    pub(super) covariance: SpectralCovariance,
}

impl SpectralSamplingLaw {
    /// Channel-local identity law.
    pub const IDENTITY: Self = Self::new(
        SpectralKernel::Identity,
        SpectralEdgePolicy::CompleteSupport,
        SpectralCovariance::PropagateIndependentSourceNoise,
    );

    /// Nearest-centre interpolation law.
    pub const NEAREST: Self = Self::new(
        SpectralKernel::Nearest,
        SpectralEdgePolicy::CompleteSupport,
        SpectralCovariance::PropagateIndependentSourceNoise,
    );

    /// Linear interpolation law.
    pub const LINEAR: Self = Self::new(
        SpectralKernel::Linear,
        SpectralEdgePolicy::CompleteSupport,
        SpectralCovariance::PropagateIndependentSourceNoise,
    );

    /// Four-point cubic interpolation law.
    pub const CUBIC: Self = Self::new(
        SpectralKernel::Cubic,
        SpectralEdgePolicy::CompleteSupport,
        SpectralCovariance::PropagateIndependentSourceNoise,
    );

    /// Construct an explicit paired spectral law.
    #[must_use]
    pub const fn new(
        kernel: SpectralKernel,
        edge_policy: SpectralEdgePolicy,
        covariance: SpectralCovariance,
    ) -> Self {
        Self {
            kernel,
            edge_policy,
            covariance,
        }
    }

    /// Construct partial-overlap channel integration with a planner term bound.
    #[must_use]
    pub const fn channel_integration(maximum_terms: usize) -> Self {
        Self::new(
            SpectralKernel::ChannelIntegration { maximum_terms },
            SpectralEdgePolicy::PartialOverlap,
            SpectralCovariance::PropagateIndependentSourceNoise,
        )
    }

    /// Return the coefficient kernel.
    #[must_use]
    pub const fn kernel(self) -> SpectralKernel {
        self.kernel
    }

    /// Return the edge-coverage policy.
    #[must_use]
    pub const fn edge_policy(self) -> SpectralEdgePolicy {
        self.edge_policy
    }

    /// Return the covariance declaration.
    #[must_use]
    pub const fn covariance(self) -> SpectralCovariance {
        self.covariance
    }
}

/// Scientific coupling between reconstructed spectral planes or coefficients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralCoupling {
    /// Planes or coefficients have no shared product constraint.
    Independent,
    /// Published planes share one common restoring beam.
    CommonRestoringBeam,
}

/// Spectral coordinate, sampling, and cross-plane requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpectralContract {
    pub(super) sampling: SpectralSamplingLaw,
    pub(super) coupling: SpectralCoupling,
}

impl SpectralContract {
    /// Construct spectral requirements.
    #[must_use]
    pub const fn new(sampling: SpectralSamplingLaw, coupling: SpectralCoupling) -> Self {
        Self { sampling, coupling }
    }

    /// Return paired spectral sampling semantics.
    #[must_use]
    pub const fn sampling(self) -> SpectralSamplingLaw {
        self.sampling
    }

    /// Return spectral coupling semantics.
    #[must_use]
    pub const fn coupling(self) -> SpectralCoupling {
        self.coupling
    }
}

/// Requested reconstruction coordinate in Stokes or correlation space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolarizationCoordinate {
    /// Stokes I.
    StokesI,
    /// Stokes Q.
    StokesQ,
    /// Stokes U.
    StokesU,
    /// Stokes V.
    StokesV,
    /// Linear-feed XX correlation.
    LinearXx,
    /// Linear-feed XY correlation.
    LinearXy,
    /// Linear-feed YX correlation.
    LinearYx,
    /// Linear-feed YY correlation.
    LinearYy,
    /// Circular-feed RR correlation.
    CircularRr,
    /// Circular-feed RL correlation.
    CircularRl,
    /// Circular-feed LR correlation.
    CircularLr,
    /// Circular-feed LL correlation.
    CircularLl,
}

impl PolarizationCoordinate {
    /// Complete stable catalog of polarization coordinates understood by the
    /// current request compiler.
    pub const ALL: [Self; 12] = [
        Self::StokesI,
        Self::StokesQ,
        Self::StokesU,
        Self::StokesV,
        Self::LinearXx,
        Self::LinearXy,
        Self::LinearYx,
        Self::LinearYy,
        Self::CircularRr,
        Self::CircularRl,
        Self::CircularLr,
        Self::CircularLl,
    ];

    /// Return the stable request-catalog identity.
    #[must_use]
    pub const fn catalog_id(self) -> &'static str {
        match self {
            Self::StokesI => "stokes_i",
            Self::StokesQ => "stokes_q",
            Self::StokesU => "stokes_u",
            Self::StokesV => "stokes_v",
            Self::LinearXx => "linear_xx",
            Self::LinearXy => "linear_xy",
            Self::LinearYx => "linear_yx",
            Self::LinearYy => "linear_yy",
            Self::CircularRr => "circular_rr",
            Self::CircularRl => "circular_rl",
            Self::CircularLr => "circular_lr",
            Self::CircularLl => "circular_ll",
        }
    }
}

/// Requested polarization reconstruction coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolarizationContract {
    pub(super) coordinates: Vec<PolarizationCoordinate>,
}

impl PolarizationContract {
    /// Construct requested coordinates. Compilation canonicalizes ordering.
    #[must_use]
    pub const fn new(coordinates: Vec<PolarizationCoordinate>) -> Self {
        Self { coordinates }
    }

    /// Return canonical requested coordinates after compilation.
    #[must_use]
    pub fn coordinates(&self) -> &[PolarizationCoordinate] {
        &self.coordinates
    }

    pub(super) fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.coordinates.sort_unstable();
        self.coordinates.dedup();
        if self.coordinates.is_empty() {
            return Err(CompileProblemError::InvalidReconstructionContract {
                reason: "at least one polarization coordinate must be requested",
            });
        }
        let categories = self
            .coordinates
            .iter()
            .fold([false; 3], |mut present, coordinate| {
                match coordinate {
                    PolarizationCoordinate::StokesI
                    | PolarizationCoordinate::StokesQ
                    | PolarizationCoordinate::StokesU
                    | PolarizationCoordinate::StokesV => present[0] = true,
                    PolarizationCoordinate::LinearXx
                    | PolarizationCoordinate::LinearXy
                    | PolarizationCoordinate::LinearYx
                    | PolarizationCoordinate::LinearYy => present[1] = true,
                    PolarizationCoordinate::CircularRr
                    | PolarizationCoordinate::CircularRl
                    | PolarizationCoordinate::CircularLr
                    | PolarizationCoordinate::CircularLl => present[2] = true,
                }
                present
            });
        if categories.into_iter().filter(|present| *present).count() > 1 {
            return Err(CompileProblemError::InvalidReconstructionContract {
                reason: "one reconstruction cannot mix Stokes, linear, and circular coordinates",
            });
        }
        Ok(self)
    }
}

/// Direction-dependent instrument response included in the measurement equation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentResponse {
    /// Direction-independent scalar response.
    Scalar,
    /// Scalar primary-beam response.
    PrimaryBeam,
    /// Full polarization Mueller response.
    FullMueller,
}

/// Closed identity of an instrument power-response law compiled into the
/// paired measurement operator.
///
/// Each variant names one law, not a runtime backend. A changed law is a
/// new variant, so a variant's meaning never changes under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InstrumentModel {
    /// CASA-compatible direct scalar power response for a homogeneous ACA
    /// 7 m interferometric array, version 1.
    CasaAca7mInterferometricDirectPbV1,
    /// CASA-compatible paired voltage response for heterogeneous ALMA 12 m
    /// and ACA 7 m interferometric baselines, version 1.
    CasaAlmaAcaHeterogeneousInterferometricResponseV1,
    /// CASA-compatible paired voltage response of CASA's common EVLA beam
    /// (`PBMath1DEVLA`), which every EVLA interferometric baseline shares,
    /// version 1: the mosaic gridder's `HetArrayConvFunc` of
    /// `PBMathInterface::COMMONPB`.
    CasaEvlaCommonInterferometricResponseV1,
    /// CASA-compatible paired voltage response of Airy dishes, one class
    /// per dish diameter, for a telescope CASA has no default voltage
    /// pattern for, version 1: the mosaic gridder's `HetArrayConvFunc` of
    /// `PBMathInterface::AIRY` (each dish its own diameter, a blockage of
    /// `d/12 · 0.75` and a 150″ radius at 100 GHz).
    CasaAiryHeterogeneousInterferometricResponseV1,
    /// CASA-compatible EVLA aperture, pointing, and beam response with the
    /// terms its [`AwProjectionContract`] states (wideband and conjugate
    /// beams among them), consumed through validated paired AW convolution
    /// functions.
    CasaEvlaWidebandAwV1,
    /// VLA or EVLA L-band AWPLPG with a beam shared by all correlations.
    CasaVlaAwplpg,
    /// VLA or EVLA L-band AWPLPG with independent RR and LL aperture
    /// beams keyed by the visibility buffer's feed parallactic angle.
    CasaVlaAwplpgSquint,
}

/// Science-owned A/W-projection controls compiled into one paired operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AwProjectionContract {
    pub(super) maximum_abs_w_lambda_bits: u64,
    pub(super) planes: std::num::NonZeroUsize,
    pub(super) a_term: bool,
    pub(super) ps_term: bool,
    pub(super) wideband: bool,
    pub(super) conjugate_beams: bool,
    pub(super) use_pointing: bool,
    pub(super) pointing_group_threshold_arcsec_bits: u64,
    pub(super) pointing_refresh_threshold_arcsec_bits: u64,
    pub(super) compute_pa_step_deg_bits: u64,
    pub(super) rotate_pa_step_deg_bits: u64,
}

impl AwProjectionContract {
    /// Construct one complete paired A/W request independent of cache state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        maximum_abs_w_lambda: f64,
        planes: std::num::NonZeroUsize,
        a_term: bool,
        ps_term: bool,
        wideband: bool,
        conjugate_beams: bool,
        use_pointing: bool,
        pointing_offset_sigdev_arcsec: [f64; 2],
        compute_pa_step_deg: f64,
        rotate_pa_step_deg: f64,
    ) -> Result<Self, AwProjectionContractError> {
        if !maximum_abs_w_lambda.is_finite() || maximum_abs_w_lambda < 0.0 {
            return Err(AwProjectionContractError::InvalidMaximumAbsWLambda);
        }
        if !compute_pa_step_deg.is_finite() || compute_pa_step_deg <= 0.0 {
            return Err(AwProjectionContractError::InvalidComputePaStep);
        }
        if !rotate_pa_step_deg.is_finite() || rotate_pa_step_deg <= 0.0 {
            return Err(AwProjectionContractError::InvalidRotatePaStep);
        }
        if pointing_offset_sigdev_arcsec
            .into_iter()
            .any(|threshold| !threshold.is_finite() || threshold < 0.0)
        {
            return Err(AwProjectionContractError::InvalidPointingOffsetSigdev);
        }
        let [
            pointing_group_threshold_arcsec,
            pointing_refresh_threshold_arcsec,
        ] = pointing_offset_sigdev_arcsec;
        Ok(Self {
            maximum_abs_w_lambda_bits: maximum_abs_w_lambda.to_bits(),
            planes,
            a_term,
            ps_term,
            wideband,
            conjugate_beams,
            use_pointing,
            pointing_group_threshold_arcsec_bits: pointing_group_threshold_arcsec.to_bits(),
            pointing_refresh_threshold_arcsec_bits: pointing_refresh_threshold_arcsec.to_bits(),
            compute_pa_step_deg_bits: compute_pa_step_deg.to_bits(),
            rotate_pa_step_deg_bits: rotate_pa_step_deg.to_bits(),
        })
    }

    /// Return the selected-observation W envelope in wavelengths.
    #[must_use]
    pub fn maximum_abs_w_lambda(self) -> f64 {
        f64::from_bits(self.maximum_abs_w_lambda_bits)
    }

    /// Return the exact requested W-plane count.
    #[must_use]
    pub const fn planes(self) -> std::num::NonZeroUsize {
        self.planes
    }

    /// Return whether the aperture term is required.
    #[must_use]
    pub const fn a_term(self) -> bool {
        self.a_term
    }

    /// Return whether the prolate-spheroidal term is required.
    #[must_use]
    pub const fn ps_term(self) -> bool {
        self.ps_term
    }

    /// Return whether frequency-dependent aperture cells are required.
    #[must_use]
    pub const fn wideband(self) -> bool {
        self.wideband
    }

    /// Return whether conjugate-frequency beam cells are required.
    #[must_use]
    pub const fn conjugate_beams(self) -> bool {
        self.conjugate_beams
    }

    /// Return whether row-local POINTING directions are required.
    #[must_use]
    pub const fn use_pointing(self) -> bool {
        self.use_pointing
    }

    /// Return CASA's two effective pointing thresholds in arcseconds.
    ///
    /// The first threshold groups antenna pointing offsets that may share a
    /// correction. The second is the time-dependent mean-shift threshold after
    /// which those antenna groups must be refreshed.
    #[must_use]
    pub fn pointing_offset_sigdev_arcsec(self) -> [f64; 2] {
        [
            f64::from_bits(self.pointing_group_threshold_arcsec_bits),
            f64::from_bits(self.pointing_refresh_threshold_arcsec_bits),
        ]
    }

    /// Return the antenna pointing-offset grouping threshold in arcseconds.
    #[must_use]
    pub fn pointing_group_threshold_arcsec(self) -> f64 {
        f64::from_bits(self.pointing_group_threshold_arcsec_bits)
    }

    /// Return the time-dependent antenna-group refresh threshold in arcseconds.
    #[must_use]
    pub fn pointing_refresh_threshold_arcsec(self) -> f64 {
        f64::from_bits(self.pointing_refresh_threshold_arcsec_bits)
    }

    /// Return the CF computation parallactic-angle step in degrees.
    #[must_use]
    pub fn compute_pa_step_deg(self) -> f64 {
        f64::from_bits(self.compute_pa_step_deg_bits)
    }

    /// Return the CF rotation parallactic-angle step in degrees.
    #[must_use]
    pub fn rotate_pa_step_deg(self) -> f64 {
        f64::from_bits(self.rotate_pa_step_deg_bits)
    }
}

/// Invalid paired A/W-projection science contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AwProjectionContractError {
    /// The selected-observation W envelope must be finite and non-negative.
    #[error("maximum absolute W wavelength must be finite and non-negative")]
    InvalidMaximumAbsWLambda,
    /// The CF computation PA step must be finite and positive.
    #[error("AW computation parallactic-angle step must be finite and positive")]
    InvalidComputePaStep,
    /// The CF rotation PA step must be finite and positive.
    #[error("AW rotation parallactic-angle step must be finite and positive")]
    InvalidRotatePaStep,
    /// Both pointing grouping and refresh thresholds must be finite and non-negative.
    #[error("AW pointing-offset thresholds must be finite and non-negative")]
    InvalidPointingOffsetSigdev,
}

/// Science-owned W-projection envelope compiled into the paired measurement operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WProjectionContract {
    pub(super) maximum_abs_w_lambda_bits: u64,
    pub(super) planes: Option<std::num::NonZeroUsize>,
    pub(super) statistics: Option<WStatistics>,
}

/// The selected observation's `|w|` statistics CASA's `WProjectFT` sizes an
/// automatic plane count from (`wprojplanes = -1`): the smallest and the
/// root-mean-square `|w|` in wavelengths at the selection's top frequency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WStatistics {
    pub(super) minimum_abs_w_lambda_bits: u64,
    pub(super) rms_w_lambda_bits: u64,
}

impl WStatistics {
    /// Statistics of a selection; both values must be finite and non-negative.
    pub fn new(
        minimum_abs_w_lambda: f64,
        rms_w_lambda: f64,
    ) -> Result<Self, WProjectionContractError> {
        if !minimum_abs_w_lambda.is_finite()
            || minimum_abs_w_lambda < 0.0
            || !rms_w_lambda.is_finite()
            || rms_w_lambda < 0.0
        {
            return Err(WProjectionContractError::InvalidStatistics);
        }
        Ok(Self {
            minimum_abs_w_lambda_bits: minimum_abs_w_lambda.to_bits(),
            rms_w_lambda_bits: rms_w_lambda.to_bits(),
        })
    }

    /// The smallest `|w|` in wavelengths.
    #[must_use]
    pub fn minimum_abs_w_lambda(self) -> f64 {
        f64::from_bits(self.minimum_abs_w_lambda_bits)
    }

    /// The root-mean-square `w` in wavelengths.
    #[must_use]
    pub fn rms_w_lambda(self) -> f64 {
        f64::from_bits(self.rms_w_lambda_bits)
    }
}

impl WProjectionContract {
    /// Construct a bounded W-projection contract.
    pub fn new(
        maximum_abs_w_lambda: f64,
        planes: Option<std::num::NonZeroUsize>,
    ) -> Result<Self, WProjectionContractError> {
        if !maximum_abs_w_lambda.is_finite() || maximum_abs_w_lambda < 0.0 {
            return Err(WProjectionContractError::InvalidMaximumAbsWLambda);
        }
        Ok(Self {
            maximum_abs_w_lambda_bits: maximum_abs_w_lambda.to_bits(),
            planes,
            statistics: None,
        })
    }

    /// Carry the selection's `|w|` statistics for an automatic plane count.
    #[must_use]
    pub const fn with_statistics(mut self, statistics: WStatistics) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Return the selected-observation W envelope in wavelengths.
    #[must_use]
    pub fn maximum_abs_w_lambda(self) -> f64 {
        f64::from_bits(self.maximum_abs_w_lambda_bits)
    }

    /// Return the requested convolution-plane count, when fixed by the caller.
    #[must_use]
    pub const fn planes(self) -> Option<std::num::NonZeroUsize> {
        self.planes
    }

    /// Return the selection's `|w|` statistics, when the request carries them.
    #[must_use]
    pub const fn statistics(self) -> Option<WStatistics> {
        self.statistics
    }
}

/// Invalid W-projection science contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WProjectionContractError {
    /// The selected-observation W envelope must be finite and non-negative.
    #[error("maximum absolute W wavelength must be finite and non-negative")]
    InvalidMaximumAbsWLambda,
    /// The `|w|` statistics must be finite and non-negative.
    #[error("W statistics must be finite and non-negative")]
    InvalidStatistics,
}

/// Logical measurement-equation terms independent of an implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurementEquationContract {
    pub(super) instrument_response: InstrumentResponse,
    pub(super) inner_products: DeclaredInnerProducts,
    pub(super) w_projection: Option<WProjectionContract>,
    pub(super) aw_projection: Option<AwProjectionContract>,
    pub(super) mosaic_conjugate_beams: bool,
}

impl MeasurementEquationContract {
    /// Construct measurement-equation requirements.
    #[must_use]
    pub const fn new(
        instrument_response: InstrumentResponse,
        inner_products: DeclaredInnerProducts,
    ) -> Self {
        Self {
            instrument_response,
            inner_products,
            w_projection: None,
            aw_projection: None,
            mosaic_conjugate_beams: false,
        }
    }

    /// Grid a mosaic's samples with each beam's response at its conjugate
    /// frequency `√(2ν₀² − ν²)` about the image frequency `ν₀`, and predict
    /// with the response at `ν`: tclean's `conjbeams` on a mosaic of one
    /// image plane (`HetArrayConvFunc::fillConjConvFunc`). An A/W
    /// projection states its own ([`AwProjectionContract`]).
    #[must_use]
    pub const fn with_mosaic_conjugate_beams(mut self) -> Self {
        self.mosaic_conjugate_beams = true;
        self
    }

    /// Whether a mosaic grids with its conjugate beams
    /// ([`Self::with_mosaic_conjugate_beams`]).
    #[must_use]
    pub const fn mosaic_conjugate_beams(self) -> bool {
        self.mosaic_conjugate_beams
    }

    /// Include one explicit paired W-projection transform.
    #[must_use]
    pub const fn with_w_projection(mut self, contract: WProjectionContract) -> Self {
        self.w_projection = Some(contract);
        self
    }

    /// Include one explicit paired A/W-projection transform.
    #[must_use]
    pub const fn with_aw_projection(mut self, contract: AwProjectionContract) -> Self {
        self.aw_projection = Some(contract);
        self
    }

    /// Return the required instrument response.
    #[must_use]
    pub const fn instrument_response(self) -> InstrumentResponse {
        self.instrument_response
    }

    /// Return the model and visibility inner products defining the adjoint.
    #[must_use]
    pub const fn inner_products(self) -> DeclaredInnerProducts {
        self.inner_products
    }

    /// Return the W-projection envelope, when the measurement operator includes it.
    #[must_use]
    pub const fn w_projection(self) -> Option<WProjectionContract> {
        self.w_projection
    }

    /// Return the paired A/W-projection contract, when requested.
    #[must_use]
    pub const fn aw_projection(self) -> Option<AwProjectionContract> {
        self.aw_projection
    }
}

/// Complete science-owned contract outside reconstruction, weighting, and products.
#[derive(Debug, Clone, PartialEq)]
pub struct ScientificContract {
    pub(super) spectral: SpectralContract,
    pub(super) measurement_equation: MeasurementEquationContract,
    pub(super) instrument_model: Option<InstrumentModel>,
}

impl ScientificContract {
    /// Construct a complete logical scientific contract.
    #[must_use]
    pub const fn new(
        spectral: SpectralContract,
        measurement_equation: MeasurementEquationContract,
    ) -> Self {
        Self {
            spectral,
            measurement_equation,
            instrument_model: None,
        }
    }

    /// Bind the exact instrument power-response law used by the measurement
    /// equation.
    #[must_use]
    pub const fn with_instrument_model(mut self, instrument_model: InstrumentModel) -> Self {
        self.instrument_model = Some(instrument_model);
        self
    }

    /// Return spectral requirements.
    #[must_use]
    pub const fn spectral(&self) -> SpectralContract {
        self.spectral
    }

    /// Return measurement-equation requirements.
    #[must_use]
    pub const fn measurement_equation(&self) -> MeasurementEquationContract {
        self.measurement_equation
    }

    /// Return the exact instrument power-response law, when one is declared.
    #[must_use]
    pub const fn instrument_model(&self) -> Option<InstrumentModel> {
        self.instrument_model
    }
}
