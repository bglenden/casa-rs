// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeSet, fmt};

use thiserror::Error;

use crate::geometry::{CompileGeometryError, CompiledGeometry, GeometryInput, compile_geometry};
use crate::measurement_equation::{
    DeclaredInnerProducts, NormalEquationContract, ProductNormalizationBoundary,
    WeightingOperatorContract, compile_normal_equation, compile_product_boundary,
};
use crate::model_state::{
    ModelContractError, ModelLifecycleContract, ModelLifecycleRequirements,
    compile_model_lifecycle_contract,
};
use crate::observation::ObservationSnapshot;
use crate::product_graph::{ProductGraph, compile_product_graph};
use crate::transaction::{
    ObservationTransactionCompileError, ObservationTransactionContract,
    ObservationTransactionRequirements, compile_observation_transaction,
};

/// An identity supplied by an owner outside the problem compiler.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalIdentity([u8; 32]);

impl LogicalIdentity {
    /// An identity of 32 opaque bytes. Only equality is meaningful: the
    /// owner chooses the bytes (a digest, a counter or a tag), and nothing
    /// reads structure from them.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The identity's 32 bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for LogicalIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LogicalIdentity(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for LogicalIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

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
    kernel: SpectralKernel,
    edge_policy: SpectralEdgePolicy,
    covariance: SpectralCovariance,
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
    sampling: SpectralSamplingLaw,
    coupling: SpectralCoupling,
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
    coordinates: Vec<PolarizationCoordinate>,
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

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
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
/// Each variant is a versioned scientific identity, not a runtime backend
/// selector. Changing the law requires a new variant so compiled-problem
/// identities cannot silently change meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InstrumentModel {
    /// CASA-compatible direct scalar power response for a homogeneous ACA
    /// 7 m interferometric array, version 1.
    CasaAca7mInterferometricDirectPbV1,
    /// CASA-compatible paired voltage response for heterogeneous ALMA 12 m
    /// and ACA 7 m interferometric baselines, version 1.
    CasaAlmaAcaHeterogeneousInterferometricResponseV1,
    /// CASA-compatible EVLA wideband aperture, pointing, and conjugate-beam
    /// response consumed through validated paired AW convolution functions.
    CasaEvlaWidebandAwV1,
}

/// Science-owned A/W-projection controls compiled into one paired operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AwProjectionContract {
    maximum_abs_w_lambda_bits: u64,
    planes: std::num::NonZeroUsize,
    a_term: bool,
    ps_term: bool,
    wideband: bool,
    conjugate_beams: bool,
    use_pointing: bool,
    pointing_group_threshold_arcsec_bits: u64,
    pointing_refresh_threshold_arcsec_bits: u64,
    compute_pa_step_deg_bits: u64,
    rotate_pa_step_deg_bits: u64,
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
    maximum_abs_w_lambda_bits: u64,
    planes: Option<std::num::NonZeroUsize>,
    statistics: Option<WStatistics>,
}

/// The selected observation's `|w|` statistics CASA's `WProjectFT` sizes an
/// automatic plane count from (`wprojplanes = -1`): the smallest and the
/// root-mean-square `|w|` in wavelengths at the selection's top frequency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WStatistics {
    minimum_abs_w_lambda_bits: u64,
    rms_w_lambda_bits: u64,
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
    instrument_response: InstrumentResponse,
    inner_products: DeclaredInnerProducts,
    w_projection: Option<WProjectionContract>,
    aw_projection: Option<AwProjectionContract>,
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
        }
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
    spectral: SpectralContract,
    measurement_equation: MeasurementEquationContract,
    instrument_model: Option<InstrumentModel>,
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

/// Frequency-domain model coefficient basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconstructionBasis {
    /// One coefficient shared across the selected frequency domain.
    Constant,
    /// Taylor polynomial coefficients across frequency.
    Taylor {
        /// Number of Taylor coefficients.
        terms: usize,
    },
    /// Independent coefficient state for each output channel.
    ChannelLocal {
        /// Number of output channels.
        channels: usize,
    },
}

/// Logical reconstruction algorithm, independent of its implementation backend.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconstructionAlgorithm {
    /// Produce normal-state and dirty products without a minor cycle.
    Dirty,
    /// Högbom point-component minor cycle.
    Hogbom,
    /// Clark point-component minor cycle.
    Clark,
    /// Multiscale minor cycle with explicit scale sizes in pixels.
    Multiscale {
        /// Canonical requested scale sizes.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
    },
    /// Multi-term multi-frequency synthesis minor cycle.
    Mtmfs {
        /// Canonical requested scale sizes shared with multiscale cleaning.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
    },
}

/// Accounting policy for Högbom's historical inclusive iteration loop.
///
/// Iteration limits remain task/controller budgets under both policies. CASA's
/// Högbom implementation may apply one additional component when a cycle runs
/// all the way to that budget; early scientific stops report every component
/// actually applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HogbomIterationAccounting {
    /// Apply and report no more components than the requested budget.
    #[default]
    Strict,
    /// Admit CASA's inclusive terminal component while preserving the reported budget.
    CasaInclusive,
}

/// Scientific stopping and update controls for reconstruction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReconstructionControls {
    max_minor_iterations: usize,
    gain: f64,
    threshold_jy_per_beam: f64,
    cycle_iteration_limit: Option<usize>,
    maximum_major_cycles: Option<usize>,
    noise_sigma: Option<f64>,
    cycle_factor: Option<f64>,
    minimum_psf_fraction: Option<f64>,
    maximum_psf_fraction: Option<f64>,
    hogbom_iteration_accounting: HogbomIterationAccounting,
}

impl ReconstructionControls {
    /// Construct reconstruction controls.
    #[must_use]
    pub const fn new(max_minor_iterations: usize, gain: f64, threshold_jy_per_beam: f64) -> Self {
        Self {
            max_minor_iterations,
            gain,
            threshold_jy_per_beam,
            cycle_iteration_limit: None,
            maximum_major_cycles: None,
            noise_sigma: None,
            cycle_factor: None,
            minimum_psf_fraction: None,
            maximum_psf_fraction: None,
            hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        }
    }

    /// Bind the reported per-cycle iteration budget and optional total major-cycle bound.
    #[must_use]
    pub const fn with_cycle_limits(
        mut self,
        cycle_iteration_limit: usize,
        maximum_major_cycles: Option<usize>,
    ) -> Self {
        self.cycle_iteration_limit = Some(cycle_iteration_limit);
        self.maximum_major_cycles = maximum_major_cycles;
        self
    }

    /// Bind an RMS-multiple stopping threshold in addition to the absolute threshold.
    #[must_use]
    pub const fn with_noise_sigma(mut self, noise_sigma: f64) -> Self {
        self.noise_sigma = Some(noise_sigma);
        self
    }

    /// Bind CASA's PSF-sidelobe-based per-cycle stopping threshold law.
    #[must_use]
    pub const fn with_cycle_threshold(
        mut self,
        cycle_factor: f64,
        minimum_psf_fraction: f64,
        maximum_psf_fraction: f64,
    ) -> Self {
        self.cycle_factor = Some(cycle_factor);
        self.minimum_psf_fraction = Some(minimum_psf_fraction);
        self.maximum_psf_fraction = Some(maximum_psf_fraction);
        self
    }

    /// Select Högbom's strict or CASA-inclusive iteration accounting.
    #[must_use]
    pub const fn with_hogbom_iteration_accounting(
        mut self,
        accounting: HogbomIterationAccounting,
    ) -> Self {
        self.hogbom_iteration_accounting = accounting;
        self
    }

    /// Return the reported total minor-iteration budget.
    #[must_use]
    pub const fn max_minor_iterations(self) -> usize {
        self.max_minor_iterations
    }

    /// Return the loop gain.
    #[must_use]
    pub const fn gain(self) -> f64 {
        self.gain
    }

    /// Return the absolute stopping threshold in Jy/beam.
    #[must_use]
    pub const fn threshold_jy_per_beam(self) -> f64 {
        self.threshold_jy_per_beam
    }

    /// Return the explicit reported per-cycle iteration budget.
    #[must_use]
    pub const fn cycle_iteration_limit(self) -> Option<usize> {
        self.cycle_iteration_limit
    }

    /// Return the explicit total major-cycle bound.
    #[must_use]
    pub const fn maximum_major_cycles(self) -> Option<usize> {
        self.maximum_major_cycles
    }

    /// Return the optional robust-RMS threshold multiplier.
    #[must_use]
    pub const fn noise_sigma(self) -> Option<f64> {
        self.noise_sigma
    }

    /// Return the optional CASA cycle-factor multiplier.
    #[must_use]
    pub const fn cycle_factor(self) -> Option<f64> {
        self.cycle_factor
    }

    /// Return the optional lower PSF-fraction clamp.
    #[must_use]
    pub const fn minimum_psf_fraction(self) -> Option<f64> {
        self.minimum_psf_fraction
    }

    /// Return the optional upper PSF-fraction clamp.
    #[must_use]
    pub const fn maximum_psf_fraction(self) -> Option<f64> {
        self.maximum_psf_fraction
    }

    /// Return Högbom's iteration-accounting policy.
    #[must_use]
    pub const fn hogbom_iteration_accounting(self) -> HogbomIterationAccounting {
        self.hogbom_iteration_accounting
    }
}

/// Logical reconstruction requirements for one imaging problem.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconstructionContract {
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
    polarization: PolarizationContract,
}

impl ReconstructionContract {
    /// Construct reconstruction requirements.
    #[must_use]
    pub const fn new(
        basis: ReconstructionBasis,
        algorithm: ReconstructionAlgorithm,
        controls: ReconstructionControls,
        polarization: PolarizationContract,
    ) -> Self {
        Self {
            basis,
            algorithm,
            controls,
            polarization,
        }
    }

    /// Return the reconstruction basis.
    #[must_use]
    pub const fn basis(&self) -> ReconstructionBasis {
        self.basis
    }

    /// Return the requested algorithm.
    #[must_use]
    pub const fn algorithm(&self) -> &ReconstructionAlgorithm {
        &self.algorithm
    }

    /// Return reconstruction controls.
    #[must_use]
    pub const fn controls(&self) -> ReconstructionControls {
        self.controls
    }

    /// Return reconstruction-owned polarization coordinates.
    #[must_use]
    pub const fn polarization(&self) -> &PolarizationContract {
        &self.polarization
    }

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.polarization = self.polarization.canonicalize()?;
        if let ReconstructionAlgorithm::Multiscale { scales_px, .. }
        | ReconstructionAlgorithm::Mtmfs { scales_px, .. } = &mut self.algorithm
        {
            for scale in scales_px.iter_mut() {
                if *scale == 0.0 {
                    *scale = 0.0;
                }
            }
            scales_px.sort_unstable_by(|left, right| left.total_cmp(right));
            scales_px.dedup();
        }
        Ok(self)
    }
}

/// Visibility-weighting formula.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WeightingScheme {
    /// Natural weighting.
    Natural,
    /// Uniform density weighting.
    Uniform,
    /// Briggs robust weighting.
    Briggs {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
    },
    /// Briggs bandwidth-taper weighting.
    BriggsBandwidthTaper {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
    },
}

/// Domain over which visibility-density weights are derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightDensityScope {
    /// The weighting formula does not use a density generation.
    NotApplicable,
    /// All selected data contributing to the logical product.
    GlobalSelection,
    /// An explicit global density generation per output channel.
    PerOutputChannel,
}

/// Gaussian taper in the UV plane, expressed as baseline HWHM wavelengths.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvTaper {
    major_lambda: f64,
    minor_lambda: f64,
    position_angle_rad: f64,
}

impl UvTaper {
    /// Construct a Gaussian UV taper from major/minor baseline HWHM and angle.
    #[must_use]
    pub const fn new(major_lambda: f64, minor_lambda: f64, position_angle_rad: f64) -> Self {
        Self {
            major_lambda,
            minor_lambda,
            position_angle_rad,
        }
    }

    /// Return the major-axis baseline HWHM in wavelengths.
    #[must_use]
    pub const fn major_lambda(self) -> f64 {
        self.major_lambda
    }

    /// Return the minor-axis baseline HWHM in wavelengths.
    #[must_use]
    pub const fn minor_lambda(self) -> f64 {
        self.minor_lambda
    }

    /// Return the position angle in radians.
    #[must_use]
    pub const fn position_angle_rad(self) -> f64 {
        self.position_angle_rad
    }
}

/// Complete logical visibility-weighting requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightingContract {
    scheme: WeightingScheme,
    density_scope: WeightDensityScope,
    uv_taper: Option<UvTaper>,
    casa_cube_density_padding: Option<usize>,
}

impl WeightingContract {
    /// Construct weighting requirements.
    #[must_use]
    pub const fn new(scheme: WeightingScheme, density_scope: WeightDensityScope) -> Self {
        Self {
            scheme,
            density_scope,
            uv_taper: None,
            casa_cube_density_padding: None,
        }
    }

    /// Add a Gaussian UV taper to the weighting metric.
    #[must_use]
    pub const fn with_uv_taper(mut self, uv_taper: UvTaper) -> Self {
        self.uv_taper = Some(uv_taper);
        self
    }

    /// Bind CASA's cube density and native-weight transfer law, with a
    /// metadata-derived number of density channels on each side of the image.
    /// Zero padding still selects the cube law. The published axis is unchanged.
    #[must_use]
    pub const fn with_casa_cube_density_padding(mut self, channels_per_side: usize) -> Self {
        self.casa_cube_density_padding = Some(channels_per_side);
        self
    }

    /// Return the bound CASA cube law's symmetric density-axis padding.
    #[must_use]
    pub const fn casa_cube_density_padding(self) -> Option<usize> {
        self.casa_cube_density_padding
    }

    /// Return the weighting formula.
    #[must_use]
    pub const fn scheme(self) -> WeightingScheme {
        self.scheme
    }

    /// Return the density-generation scope.
    #[must_use]
    pub const fn density_scope(self) -> WeightDensityScope {
        self.density_scope
    }

    /// Return the optional Gaussian UV taper.
    #[must_use]
    pub const fn uv_taper(self) -> Option<UvTaper> {
        self.uv_taper
    }
}

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
    /// Primary-beam-corrected spectral index.
    PbCorrectedSpectralIndex,
    /// Restoring and fitted beam metadata.
    Beam,
}

impl ProductKind {
    /// Complete stable catalog of logical products understood by the current
    /// request compiler.
    pub const ALL: [Self; 15] = [
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
}

/// Restoring-beam requirement for published products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoringBeamPolicy {
    /// Do not create a restored product.
    None,
    /// Fit and use an independent beam for each plane.
    PerPlane,
    /// Use one common enclosing beam across all spectral planes.
    Common,
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
    cutoff_bits: u32,
    comparison: ProductSupportComparison,
    blanking: ProductBlankingPolicy,
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
    reference: TaylorSupportReference,
    peak_fraction_bits: u32,
    comparison: ProductSupportComparison,
    blanking: ProductBlankingPolicy,
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
    primary_beam: PrimaryBeamValidityPolicy,
    taylor: TaylorValidityPolicy,
    uncorrected_mask: UncorrectedImageMaskPolicy,
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
    products: Vec<ProductKind>,
    normalization: ProductNormalization,
    restoring_beam: RestoringBeamPolicy,
    validity: ProductValidityPolicies,
    normalization_boundary: ProductNormalizationBoundary,
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

    fn canonicalize(mut self) -> Self {
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

/// Arithmetic precision permitted by a problem's numerical contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NumericPrecision {
    /// IEEE-754 binary32 arithmetic.
    F32,
    /// IEEE-754 binary64 arithmetic.
    F64,
}

/// Reduction semantics permitted by a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReductionPolicy {
    /// A fixed pairwise reduction tree.
    DeterministicPairwise,
    /// Compensated accumulation with implementation-independent error bounds.
    Compensated,
    /// An unordered reduction accepted only within declared stage budgets.
    UnorderedWithinBudget,
}

/// Treatment of non-finite input and generated values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiniteValuePolicy {
    /// Reject every non-finite value.
    RejectAll,
    /// Treat declared non-finite inputs as flagged and reject generated non-finite values.
    FlagInputRejectGenerated,
}

/// Logical numerical stage requiring an explicit error budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NumericalStage {
    /// Coordinate transformations.
    CoordinateTransforms,
    /// Spectral transformations and sampling.
    SpectralTransforms,
    /// Visibility weighting.
    Weighting,
    /// Forward measurement operation.
    ForwardOperator,
    /// Adjoint measurement operation.
    AdjointOperator,
    /// Global reductions.
    Reductions,
    /// Reconstruction and minor-cycle updates.
    Reconstruction,
    /// Restoration.
    Restoration,
    /// Product formation and normalization.
    ProductFormation,
}

impl NumericalStage {
    /// Every stage that must have an explicit budget.
    pub const ALL: [Self; 9] = [
        Self::CoordinateTransforms,
        Self::SpectralTransforms,
        Self::Weighting,
        Self::ForwardOperator,
        Self::AdjointOperator,
        Self::Reductions,
        Self::Reconstruction,
        Self::Restoration,
        Self::ProductFormation,
    ];
}

/// Absolute and relative error allowance for one numerical stage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StageErrorBudget {
    absolute: f64,
    relative: f64,
}

impl StageErrorBudget {
    /// Construct a stage error budget.
    #[must_use]
    pub const fn new(absolute: f64, relative: f64) -> Self {
        Self { absolute, relative }
    }

    /// Return the absolute error allowance.
    #[must_use]
    pub const fn absolute(self) -> f64 {
        self.absolute
    }

    /// Return the relative error allowance.
    #[must_use]
    pub const fn relative(self) -> f64 {
        self.relative
    }
}

/// Complete numerical behavior permitted for one problem.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericsContract {
    permitted_precisions: Vec<NumericPrecision>,
    reduction: ReductionPolicy,
    finite_values: FiniteValuePolicy,
    stage_error_budgets: Vec<(NumericalStage, StageErrorBudget)>,
}

impl NumericsContract {
    /// Construct numerical requirements. Compilation canonicalizes ordering.
    #[must_use]
    pub fn new(
        permitted_precisions: Vec<NumericPrecision>,
        reduction: ReductionPolicy,
        finite_values: FiniteValuePolicy,
        stage_error_budgets: Vec<(NumericalStage, StageErrorBudget)>,
    ) -> Self {
        Self {
            permitted_precisions,
            reduction,
            finite_values,
            stage_error_budgets,
        }
    }

    /// Return permitted arithmetic precisions in canonical order after compilation.
    #[must_use]
    pub fn permitted_precisions(&self) -> &[NumericPrecision] {
        &self.permitted_precisions
    }

    /// Return permitted reduction semantics.
    #[must_use]
    pub const fn reduction(&self) -> ReductionPolicy {
        self.reduction
    }

    /// Return finite-value behavior.
    #[must_use]
    pub const fn finite_values(&self) -> FiniteValuePolicy {
        self.finite_values
    }

    /// Return complete stage budgets in canonical stage order after compilation.
    #[must_use]
    pub fn stage_error_budgets(&self) -> &[(NumericalStage, StageErrorBudget)] {
        &self.stage_error_budgets
    }

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.permitted_precisions.sort_unstable();
        self.permitted_precisions.dedup();
        if self.permitted_precisions.is_empty() {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "at least one arithmetic precision must be permitted",
            });
        }
        self.stage_error_budgets
            .sort_unstable_by_key(|(stage, _)| *stage);
        if self
            .stage_error_budgets
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "a numerical stage has more than one error budget",
            });
        }
        if self.stage_error_budgets.len() != NumericalStage::ALL.len()
            || NumericalStage::ALL
                .iter()
                .zip(&self.stage_error_budgets)
                .any(|(required, (actual, _))| required != actual)
        {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "every numerical stage must have exactly one error budget",
            });
        }
        if self.stage_error_budgets.iter().any(|(_, budget)| {
            !(budget.absolute.is_finite()
                && budget.absolute >= 0.0
                && budget.relative.is_finite()
                && budget.relative >= 0.0)
        }) {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "stage error budgets must be finite and non-negative",
            });
        }
        Ok(self)
    }
}

/// Complete uncompiled logical problem specification.
#[derive(Debug, Clone, PartialEq)]
pub struct ProblemSpecification {
    science: ScientificContract,
    reconstruction: ReconstructionContract,
    weighting: WeightingContract,
    products: ProductRequirements,
    observation_transaction: ObservationTransactionRequirements,
    numerics: NumericsContract,
    visibility_transform: Option<crate::SequentialContinuumTransform>,
}

/// Everything [`compile`] needs: the logical specification, the geometry,
/// the selected observation and the model lifecycle.
#[derive(Debug, Clone, PartialEq)]
pub struct ProblemInput {
    specification: ProblemSpecification,
    geometry: GeometryInput,
    observation: ObservationSnapshot,
    model_lifecycle: ModelLifecycleRequirements,
}

impl ProblemInput {
    /// Gather the inputs of one compile.
    #[must_use]
    pub const fn new(
        specification: ProblemSpecification,
        geometry: GeometryInput,
        observation: ObservationSnapshot,
        model_lifecycle: ModelLifecycleRequirements,
    ) -> Self {
        Self {
            specification,
            geometry,
            observation,
            model_lifecycle,
        }
    }
}

impl ProblemSpecification {
    /// Construct a logical problem specification.
    #[must_use]
    pub const fn new(
        science: ScientificContract,
        reconstruction: ReconstructionContract,
        weighting: WeightingContract,
        products: ProductRequirements,
        observation_transaction: ObservationTransactionRequirements,
        numerics: NumericsContract,
    ) -> Self {
        Self {
            science,
            reconstruction,
            weighting,
            products,
            observation_transaction,
            numerics,
            visibility_transform: None,
        }
    }

    /// Compose one sequential visibility transform into the logical problem.
    #[must_use]
    pub fn with_visibility_transform(
        mut self,
        transform: crate::SequentialContinuumTransform,
    ) -> Self {
        self.visibility_transform = Some(transform);
        self
    }
}

/// Backend-independent capability required to plan and execute a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequiredCapability {
    /// More than one user-visible image-domain chart.
    MultiDomainGeometry,
    /// Multiple image-domain facets.
    FacetedGeometry,
    /// Paired W-projection convolution.
    WProjection,
    /// Paired prepared-convolution-function A/W projection.
    AwProjection,
    /// Spectral reference-frame transformation.
    SpectralFrameTransform,
    /// Non-identity paired spectral sampling.
    SpectralResampling,
    /// Sequential visibility-domain continuum subtraction.
    SequentialContinuumTransform,
    /// Common restoring-beam coupling across spectral planes.
    CommonBeamSpectralCoupling,
    /// Reconstruction of one polarization coordinate.
    Polarization(PolarizationCoordinate),
    /// Scalar primary-beam response.
    PrimaryBeamResponse,
    /// Full Mueller instrument response.
    FullMuellerResponse,
    /// Gaussian UV tapering in the data metric.
    UvTaper,
    /// Constant reconstruction basis.
    ConstantBasis,
    /// Taylor reconstruction basis.
    TaylorBasis,
    /// Channel-local reconstruction basis.
    ChannelLocalBasis,
    /// Dirty-only reconstruction.
    DirtyReconstruction,
    /// Högbom reconstruction.
    HogbomReconstruction,
    /// Clark reconstruction.
    ClarkReconstruction,
    /// Multiscale reconstruction.
    MultiscaleReconstruction,
    /// MT-MFS reconstruction.
    MtmfsReconstruction,
    /// Natural weighting.
    NaturalWeighting,
    /// Uniform density weighting.
    UniformWeighting,
    /// Briggs density weighting.
    BriggsWeighting,
    /// Briggs bandwidth-taper weighting.
    BriggsBandwidthTaperWeighting,
    /// Unit-response product normalization.
    UnitResponseNormalization,
    /// Flat-noise product normalization.
    FlatNoiseNormalization,
    /// Flat-sky product normalization.
    FlatSkyNormalization,
    /// Formation of a particular logical product.
    Product(ProductKind),
}

impl RequiredCapability {
    /// Return every stable compiler-derived capability in the current request
    /// catalog, including every polarization coordinate and logical product.
    #[must_use]
    pub fn catalog() -> Vec<Self> {
        let mut capabilities = vec![
            Self::MultiDomainGeometry,
            Self::FacetedGeometry,
            Self::WProjection,
            Self::AwProjection,
            Self::SpectralFrameTransform,
            Self::SpectralResampling,
            Self::SequentialContinuumTransform,
            Self::CommonBeamSpectralCoupling,
            Self::PrimaryBeamResponse,
            Self::FullMuellerResponse,
            Self::UvTaper,
            Self::ConstantBasis,
            Self::TaylorBasis,
            Self::ChannelLocalBasis,
            Self::DirtyReconstruction,
            Self::HogbomReconstruction,
            Self::ClarkReconstruction,
            Self::MultiscaleReconstruction,
            Self::MtmfsReconstruction,
            Self::NaturalWeighting,
            Self::UniformWeighting,
            Self::BriggsWeighting,
            Self::BriggsBandwidthTaperWeighting,
            Self::UnitResponseNormalization,
            Self::FlatNoiseNormalization,
            Self::FlatSkyNormalization,
        ];
        capabilities.extend(Self::polarization_catalog());
        capabilities.extend(ProductKind::ALL.into_iter().map(Self::Product));
        capabilities
    }

    fn polarization_catalog() -> impl Iterator<Item = Self> {
        PolarizationCoordinate::ALL
            .into_iter()
            .map(Self::Polarization)
    }

    /// Return the stable request-catalog identity used by boundary projections.
    #[must_use]
    pub fn catalog_id(self) -> String {
        match self {
            Self::MultiDomainGeometry => "multi_domain_geometry".to_string(),
            Self::FacetedGeometry => "faceted_geometry".to_string(),
            Self::WProjection => "w_projection".to_string(),
            Self::AwProjection => "aw_projection".to_string(),
            Self::SpectralFrameTransform => "spectral_frame_transform".to_string(),
            Self::SpectralResampling => "spectral_resampling".to_string(),
            Self::SequentialContinuumTransform => "sequential_continuum_transform".to_string(),
            Self::CommonBeamSpectralCoupling => "common_beam_spectral_coupling".to_string(),
            Self::Polarization(coordinate) => {
                format!("polarization.{}", coordinate.catalog_id())
            }
            Self::PrimaryBeamResponse => "primary_beam_response".to_string(),
            Self::FullMuellerResponse => "full_mueller_response".to_string(),
            Self::UvTaper => "uv_taper".to_string(),
            Self::ConstantBasis => "constant_basis".to_string(),
            Self::TaylorBasis => "taylor_basis".to_string(),
            Self::ChannelLocalBasis => "channel_local_basis".to_string(),
            Self::DirtyReconstruction => "dirty_reconstruction".to_string(),
            Self::HogbomReconstruction => "hogbom_reconstruction".to_string(),
            Self::ClarkReconstruction => "clark_reconstruction".to_string(),
            Self::MultiscaleReconstruction => "multiscale_reconstruction".to_string(),
            Self::MtmfsReconstruction => "mtmfs_reconstruction".to_string(),
            Self::NaturalWeighting => "natural_weighting".to_string(),
            Self::UniformWeighting => "uniform_weighting".to_string(),
            Self::BriggsWeighting => "briggs_weighting".to_string(),
            Self::BriggsBandwidthTaperWeighting => "briggs_bandwidth_taper_weighting".to_string(),
            Self::UnitResponseNormalization => "unit_response_normalization".to_string(),
            Self::FlatNoiseNormalization => "flat_noise_normalization".to_string(),
            Self::FlatSkyNormalization => "flat_sky_normalization".to_string(),
            Self::Product(product) => format!("product.{}", product.catalog_id()),
        }
    }
}

/// Immutable logical problem accepted by downstream planning.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledProblem {
    model_lifecycle: ModelLifecycleContract,
    observation: ObservationSnapshot,
    geometry: CompiledGeometry,
    science: ScientificContract,
    reconstruction: ReconstructionContract,
    normal_equation: NormalEquationContract,
    products: ProductRequirements,
    product_graph: ProductGraph,
    observation_transaction: ObservationTransactionContract,
    numerics: NumericsContract,
    required_capabilities: BTreeSet<RequiredCapability>,
    visibility_transform: Option<crate::SequentialContinuumTransform>,
}

impl CompiledProblem {
    /// Return the compiler-owned model-lifecycle commitment.
    #[must_use]
    pub const fn model_lifecycle(&self) -> &ModelLifecycleContract {
        &self.model_lifecycle
    }

    /// Return the selected observation the problem was compiled from.
    #[must_use]
    pub const fn observation(&self) -> &ObservationSnapshot {
        &self.observation
    }

    /// Return immutable compiler-owned geometry.
    #[must_use]
    pub const fn geometry(&self) -> &CompiledGeometry {
        &self.geometry
    }

    /// Return the complete science-owned logical contract.
    #[must_use]
    pub const fn science(&self) -> &ScientificContract {
        &self.science
    }

    /// Return reconstruction requirements.
    #[must_use]
    pub const fn reconstruction(&self) -> &ReconstructionContract {
        &self.reconstruction
    }

    /// Whether selected rows must evaluate physical feed-rotation coordinates.
    ///
    /// The ordinary Stokes-I operator has no parallactic-angle input. Polarized
    /// reconstruction and AW responses retain their physical angles;
    /// phase-centre and spectral-frame conversions have independent requirements.
    #[must_use]
    pub fn requires_parallactic_angles(&self) -> bool {
        self.reconstruction.polarization().coordinates() != [PolarizationCoordinate::StokesI]
            || self
                .science
                .measurement_equation()
                .aw_projection()
                .is_some()
    }

    /// Return the compiled positive-semidefinite data metric W.
    #[must_use]
    pub const fn weighting(&self) -> &WeightingOperatorContract {
        self.normal_equation.weighting()
    }

    /// Return the typed A/A*, W, b, g(x), and H contract.
    #[must_use]
    pub const fn normal_equation(&self) -> &NormalEquationContract {
        &self.normal_equation
    }

    /// Return product requirements.
    #[must_use]
    pub const fn products(&self) -> &ProductRequirements {
        &self.products
    }

    /// Return the mandatory compiler-owned product topology and publication contract.
    #[must_use]
    pub const fn product_graph(&self) -> &ProductGraph {
        &self.product_graph
    }

    /// Return exact snapshot-bound MeasurementSet read and write sets.
    #[must_use]
    pub const fn observation_transaction(&self) -> &ObservationTransactionContract {
        &self.observation_transaction
    }

    /// Return numerical requirements.
    #[must_use]
    pub const fn numerics(&self) -> &NumericsContract {
        &self.numerics
    }

    /// Return the complete sorted capability set.
    #[must_use]
    pub const fn required_capabilities(&self) -> &BTreeSet<RequiredCapability> {
        &self.required_capabilities
    }

    /// Return the compiled sequential visibility transform, when present.
    #[must_use]
    pub const fn visibility_transform(&self) -> Option<&crate::SequentialContinuumTransform> {
        self.visibility_transform.as_ref()
    }
}

/// Failure to compile a logical imaging problem.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompileProblemError {
    /// Coordinate or image-domain geometry is invalid or incomplete.
    #[error(transparent)]
    Geometry(#[from] CompileGeometryError),
    /// The requested model lifecycle is incomplete or conflicts with the problem.
    #[error(transparent)]
    ModelLifecycle(#[from] ModelContractError),
    /// The requested MeasurementSet write contract is invalid for this snapshot.
    #[error(transparent)]
    ObservationTransaction(#[from] ObservationTransactionCompileError),
    /// Reconstruction and capability requirements contradict each other.
    #[error("invalid capability combination: {reason}")]
    InvalidCapabilityCombination {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Reconstruction-owned output coordinates are invalid.
    #[error("invalid reconstruction contract: {reason}")]
    InvalidReconstructionContract {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Channel-local reconstruction disagrees with the compiled output axis.
    #[error(
        "channel-local reconstruction requested {reconstruction_channels} channels but geometry compiles {geometry_channels}"
    )]
    SpectralChannelCountMismatch {
        /// Exact channel count compiled from spectral geometry.
        geometry_channels: usize,
        /// Exact channel count requested by reconstruction.
        reconstruction_channels: usize,
    },
    /// Spectral-sampling or measurement-equation requirements conflict.
    #[error("invalid scientific contract: {reason}")]
    InvalidScientificContract {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Weighting requirements are outside the logical domain.
    #[error("invalid weighting contract: {reason}")]
    InvalidWeighting {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Numerical requirements are incomplete or invalid.
    #[error("invalid numerics contract: {reason}")]
    InvalidNumerics {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Requested products contradict each other or reconstruction semantics.
    #[error("invalid product combination: {reason}")]
    InvalidProductCombination {
        /// Stable human-readable reason.
        reason: &'static str,
    },
}

/// Compile and validate one immutable backend-independent problem.
pub fn compile(input: ProblemInput) -> Result<CompiledProblem, CompileProblemError> {
    let ProblemInput {
        specification,
        geometry,
        observation,
        model_lifecycle,
    } = input;
    let geometry = compile_geometry(geometry)?;
    let visibility_transform = specification.visibility_transform;
    let science = specification.science;
    let products = specification.products.canonicalize();
    let observation_transaction = compile_observation_transaction(
        &observation,
        specification.observation_transaction,
        visibility_transform.as_ref(),
    )?;
    let numerics = specification.numerics.canonicalize()?;
    validate_science(&science)?;
    validate_reconstruction(&specification.reconstruction, &geometry)?;
    if visibility_transform.is_some()
        && !matches!(
            specification.reconstruction.basis(),
            ReconstructionBasis::ChannelLocal { .. }
        )
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "sequential visibility transforms require channel-local reconstruction",
        });
    }
    let reconstruction = specification.reconstruction.canonicalize()?;
    validate_weighting(specification.weighting)?;
    validate_products(&science, &reconstruction, &products)?;
    let normal_equation = compile_normal_equation(
        &geometry,
        &observation,
        &science,
        &reconstruction,
        specification.weighting,
    );
    let mut required_capabilities = derive_capabilities(
        &geometry,
        &science,
        &reconstruction,
        normal_equation.weighting(),
        &products,
    );
    if visibility_transform.is_some() {
        required_capabilities.insert(RequiredCapability::SequentialContinuumTransform);
    }
    let product_graph = compile_product_graph(&geometry, &reconstruction, &products);
    let model_lifecycle = compile_model_lifecycle_contract(
        &geometry,
        normal_equation.measurement_operator().domain(),
        &numerics,
        model_lifecycle,
    )?;
    Ok(CompiledProblem {
        model_lifecycle,
        observation,
        geometry,
        science,
        reconstruction,
        normal_equation,
        products,
        product_graph,
        observation_transaction,
        numerics,
        required_capabilities,
        visibility_transform,
    })
}

fn validate_science(science: &ScientificContract) -> Result<(), CompileProblemError> {
    if let SpectralKernel::ChannelIntegration { maximum_terms: 0 } =
        science.spectral.sampling.kernel()
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "spectral channel averaging requires a positive bin width",
        });
    }
    if !matches!(
        (
            science.measurement_equation.instrument_response,
            science.instrument_model,
        ),
        (InstrumentResponse::Scalar, None)
            | (
                InstrumentResponse::PrimaryBeam,
                Some(
                    InstrumentModel::CasaAca7mInterferometricDirectPbV1
                        | InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1
                        | InstrumentModel::CasaEvlaWidebandAwV1
                )
            )
    ) {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "instrument response and instrument model must form one supported exact pair",
        });
    }
    if science.measurement_equation.w_projection.is_some()
        && science.measurement_equation.aw_projection.is_some()
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "W projection and AW projection are distinct paired operators",
        });
    }
    if science.measurement_equation.aw_projection.is_some()
        && science.instrument_model != Some(InstrumentModel::CasaEvlaWidebandAwV1)
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "the paired AW contract requires the exact EVLA wideband instrument model",
        });
    }
    Ok(())
}

fn validate_reconstruction(
    contract: &ReconstructionContract,
    geometry: &CompiledGeometry,
) -> Result<(), CompileProblemError> {
    match contract.basis {
        ReconstructionBasis::Taylor { terms: 0 | 1 } => {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "a Taylor basis requires at least two terms; single-term MFS uses the constant basis",
            });
        }
        ReconstructionBasis::ChannelLocal { channels: 0 } => {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "a channel-local basis requires at least one channel",
            });
        }
        ReconstructionBasis::Constant
        | ReconstructionBasis::Taylor { .. }
        | ReconstructionBasis::ChannelLocal { .. } => {}
    }
    if let ReconstructionBasis::ChannelLocal { channels } = contract.basis
        && channels != geometry.spectral().output_channels()
    {
        return Err(CompileProblemError::SpectralChannelCountMismatch {
            geometry_channels: geometry.spectral().output_channels(),
            reconstruction_channels: channels,
        });
    }
    if matches!(contract.algorithm, ReconstructionAlgorithm::Mtmfs { .. })
        != matches!(contract.basis, ReconstructionBasis::Taylor { .. })
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "MT-MFS and Taylor-basis reconstruction must be requested together",
        });
    }
    if matches!(contract.algorithm, ReconstructionAlgorithm::Dirty)
        && contract.controls.max_minor_iterations != 0
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "dirty reconstruction cannot request minor-cycle iterations",
        });
    }
    if matches!(contract.algorithm, ReconstructionAlgorithm::Dirty)
        && (contract.controls.gain != 1.0 || contract.controls.threshold_jy_per_beam != 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "dirty reconstruction requires canonical inactive controls: gain 1 and threshold 0",
        });
    }
    if !matches!(contract.algorithm, ReconstructionAlgorithm::Hogbom)
        && contract.controls.hogbom_iteration_accounting != HogbomIterationAccounting::Strict
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "CASA-inclusive iteration accounting is specific to Högbom reconstruction",
        });
    }
    if !(contract.controls.gain.is_finite()
        && contract.controls.gain > 0.0
        && contract.controls.gain <= 1.0
        && contract.controls.threshold_jy_per_beam.is_finite()
        && contract.controls.threshold_jy_per_beam >= 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "reconstruction gain and threshold must be finite and in their valid domains",
        });
    }
    if contract.controls.cycle_iteration_limit == Some(0)
        || contract.controls.maximum_major_cycles == Some(0)
        || contract
            .controls
            .noise_sigma
            .is_some_and(|sigma| !sigma.is_finite() || sigma < 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "cycle limits must be positive and nsigma must be finite and non-negative",
        });
    }
    let cycle_threshold_values = [
        contract.controls.cycle_factor,
        contract.controls.minimum_psf_fraction,
        contract.controls.maximum_psf_fraction,
    ];
    if cycle_threshold_values.iter().any(Option::is_some)
        && (cycle_threshold_values.iter().any(Option::is_none)
            || contract
                .controls
                .cycle_factor
                .is_some_and(|value| !value.is_finite() || value <= 0.0)
            || contract
                .controls
                .minimum_psf_fraction
                .is_some_and(|value| !value.is_finite() || value < 0.0)
            || contract
                .controls
                .maximum_psf_fraction
                .is_some_and(|value| !value.is_finite() || value <= 0.0)
            || contract.controls.minimum_psf_fraction > contract.controls.maximum_psf_fraction)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "cycle threshold requires a positive factor and ordered finite PSF fractions",
        });
    }
    if let ReconstructionAlgorithm::Multiscale {
        scales_px,
        small_scale_bias,
    }
    | ReconstructionAlgorithm::Mtmfs {
        scales_px,
        small_scale_bias,
    } = &contract.algorithm
    {
        if scales_px.is_empty()
            || scales_px
                .iter()
                .any(|scale| !(scale.is_finite() && *scale >= 0.0))
        {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "scale-aware reconstruction requires finite non-negative explicit scales",
            });
        }
        if !small_scale_bias.is_finite() || !(0.0..=1.0).contains(small_scale_bias) {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "scale-aware small-scale bias must be finite and in [0, 1]",
            });
        }
    }
    Ok(())
}

fn validate_weighting(contract: WeightingContract) -> Result<(), CompileProblemError> {
    if contract.casa_cube_density_padding.is_some()
        && contract.density_scope != WeightDensityScope::PerOutputChannel
    {
        return Err(CompileProblemError::InvalidWeighting {
            reason: "CASA cube density requires per-output-channel weighting",
        });
    }
    match contract.scheme {
        WeightingScheme::Natural if contract.density_scope != WeightDensityScope::NotApplicable => {
            return Err(CompileProblemError::InvalidWeighting {
                reason: "natural weighting has no density generation",
            });
        }
        WeightingScheme::Uniform
        | WeightingScheme::Briggs { .. }
        | WeightingScheme::BriggsBandwidthTaper { .. }
            if contract.density_scope == WeightDensityScope::NotApplicable =>
        {
            return Err(CompileProblemError::InvalidWeighting {
                reason: "density weighting requires an explicit global density scope",
            });
        }
        WeightingScheme::Natural
        | WeightingScheme::Uniform
        | WeightingScheme::Briggs { .. }
        | WeightingScheme::BriggsBandwidthTaper { .. } => {}
    }
    let robust = match contract.scheme {
        WeightingScheme::Briggs { robust } | WeightingScheme::BriggsBandwidthTaper { robust } => {
            Some(robust)
        }
        WeightingScheme::Natural | WeightingScheme::Uniform => None,
    };
    if robust.is_some_and(|value| !(value.is_finite() && (-2.0..=2.0).contains(&value))) {
        return Err(CompileProblemError::InvalidWeighting {
            reason: "Briggs robustness must be finite and in [-2, 2]",
        });
    }
    if contract.uv_taper.is_some_and(|taper| {
        !(taper.major_lambda.is_finite()
            && taper.major_lambda > 0.0
            && taper.minor_lambda.is_finite()
            && taper.minor_lambda > 0.0
            && taper.position_angle_rad.is_finite())
    }) {
        return Err(CompileProblemError::InvalidWeighting {
            reason: "UV taper axes must be finite and positive and its angle must be finite",
        });
    }
    Ok(())
}

fn validate_products(
    science: &ScientificContract,
    reconstruction: &ReconstructionContract,
    products: &ProductRequirements,
) -> Result<(), CompileProblemError> {
    if products.products.is_empty() {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "at least one product must be requested",
        });
    }
    let restored_image_requested = products.contains(ProductKind::RestoredImage);
    let restoring_beam_requested = !matches!(products.restoring_beam, RestoringBeamPolicy::None);
    if restored_image_requested != restoring_beam_requested {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "restored-image and restoring-beam requirements must be requested together",
        });
    }
    if restored_image_requested
        && !(products.contains(ProductKind::Residual) && products.contains(ProductKind::Model))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a restored image requires residual and model products",
        });
    }
    let common_spectral_beam = science.spectral.coupling == SpectralCoupling::CommonRestoringBeam;
    let common_product_beam = products.restoring_beam == RestoringBeamPolicy::Common;
    if common_spectral_beam != common_product_beam {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "common spectral coupling and common restoring-beam publication must be requested together",
        });
    }
    if products.contains(ProductKind::PbCorrectedImage)
        && !(products.contains(ProductKind::RestoredImage)
            && products.contains(ProductKind::PrimaryBeam))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a PB-corrected image requires restored-image and primary-beam products",
        });
    }
    let taylor_terms = match reconstruction.basis {
        ReconstructionBasis::Taylor { terms } => terms,
        ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => 0,
    };
    if products.contains(ProductKind::TaylorTerms) && taylor_terms == 0 {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "Taylor products require a Taylor reconstruction basis",
        });
    }
    if products.contains(ProductKind::TaylorTerms)
        && ![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::Weight,
            ProductKind::PrimaryBeam,
            ProductKind::PbCorrectedImage,
        ]
        .into_iter()
        .any(|product| products.contains(product))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a Taylor coefficient set requires at least one Taylor image product",
        });
    }
    if products.contains(ProductKind::SpectralIndex)
        && !(taylor_terms >= 2
            && products.contains(ProductKind::TaylorTerms)
            && products.contains(ProductKind::Residual)
            && products.contains(ProductKind::RestoredImage))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral index requires at least two Taylor terms plus Taylor, residual, and restored-image products",
        });
    }
    if products.contains(ProductKind::SpectralIndexError)
        && !products.contains(ProductKind::SpectralIndex)
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral-index uncertainty requires a spectral-index product",
        });
    }
    if products.contains(ProductKind::PbCorrectedSpectralIndex)
        && !(products.contains(ProductKind::SpectralIndex)
            && products.contains(ProductKind::PrimaryBeam))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "PB-corrected spectral index requires spectral-index and primary-beam products",
        });
    }
    if products.contains(ProductKind::Beam)
        && ![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::RestoredImage,
        ]
        .into_iter()
        .any(|product| products.contains(product))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "beam metadata requires a PSF, residual, or restored-image product",
        });
    }
    Ok(())
}

fn derive_capabilities(
    geometry: &CompiledGeometry,
    science: &ScientificContract,
    reconstruction: &ReconstructionContract,
    weighting: &WeightingOperatorContract,
    products: &ProductRequirements,
) -> BTreeSet<RequiredCapability> {
    let mut capabilities = BTreeSet::new();
    if geometry.domains().len() > 1 {
        capabilities.insert(RequiredCapability::MultiDomainGeometry);
    }
    if geometry
        .domains()
        .iter()
        .any(|domain| domain.facets().len() > 1)
    {
        capabilities.insert(RequiredCapability::FacetedGeometry);
    }
    if geometry.spectral().source_frame() != geometry.spectral().output_frame() {
        capabilities.insert(RequiredCapability::SpectralFrameTransform);
    }
    if science.spectral.sampling != SpectralSamplingLaw::IDENTITY {
        capabilities.insert(RequiredCapability::SpectralResampling);
    }
    if science.measurement_equation.w_projection.is_some() {
        capabilities.insert(RequiredCapability::WProjection);
    }
    if science.measurement_equation.aw_projection.is_some() {
        capabilities.insert(RequiredCapability::AwProjection);
    }
    if science.spectral.coupling == SpectralCoupling::CommonRestoringBeam {
        capabilities.insert(RequiredCapability::CommonBeamSpectralCoupling);
    }
    capabilities.extend(
        reconstruction
            .polarization
            .coordinates
            .iter()
            .copied()
            .map(RequiredCapability::Polarization),
    );
    match science.measurement_equation.instrument_response {
        InstrumentResponse::Scalar => {}
        InstrumentResponse::PrimaryBeam => {
            capabilities.insert(RequiredCapability::PrimaryBeamResponse);
        }
        InstrumentResponse::FullMueller => {
            capabilities.insert(RequiredCapability::FullMuellerResponse);
        }
    }
    if weighting.uv_taper().is_some() {
        capabilities.insert(RequiredCapability::UvTaper);
    }
    capabilities.insert(match reconstruction.basis {
        ReconstructionBasis::Constant => RequiredCapability::ConstantBasis,
        ReconstructionBasis::Taylor { .. } => RequiredCapability::TaylorBasis,
        ReconstructionBasis::ChannelLocal { .. } => RequiredCapability::ChannelLocalBasis,
    });
    capabilities.insert(match reconstruction.algorithm {
        ReconstructionAlgorithm::Dirty => RequiredCapability::DirtyReconstruction,
        ReconstructionAlgorithm::Hogbom => RequiredCapability::HogbomReconstruction,
        ReconstructionAlgorithm::Clark => RequiredCapability::ClarkReconstruction,
        ReconstructionAlgorithm::Multiscale { .. } => RequiredCapability::MultiscaleReconstruction,
        ReconstructionAlgorithm::Mtmfs { .. } => RequiredCapability::MtmfsReconstruction,
    });
    capabilities.insert(match weighting.scheme() {
        WeightingScheme::Natural => RequiredCapability::NaturalWeighting,
        WeightingScheme::Uniform => RequiredCapability::UniformWeighting,
        WeightingScheme::Briggs { .. } => RequiredCapability::BriggsWeighting,
        WeightingScheme::BriggsBandwidthTaper { .. } => {
            RequiredCapability::BriggsBandwidthTaperWeighting
        }
    });
    capabilities.insert(match products.normalization {
        ProductNormalization::UnitResponse => RequiredCapability::UnitResponseNormalization,
        ProductNormalization::FlatNoise => RequiredCapability::FlatNoiseNormalization,
        ProductNormalization::FlatSky => RequiredCapability::FlatSkyNormalization,
    });
    capabilities.extend(
        products
            .products
            .iter()
            .copied()
            .map(RequiredCapability::Product),
    );
    capabilities
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
