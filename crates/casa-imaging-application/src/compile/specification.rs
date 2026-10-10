// SPDX-License-Identifier: LGPL-3.0-or-later

//! The logical problem a request specifies: the solver and its controls,
//! the weighting, the measurement equation with its W or AW projection, the
//! products and their validity, and a sequential continuum subtraction.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use casa_imaging_model::{
    AwProjectionContract, ContinuumChannelRole, ContinuumChannelUse, ContinuumFitRule,
    CorrectedDataWrite, DeclaredInnerProducts, FiniteValuePolicy, HogbomIterationAccounting,
    InstrumentModel, InstrumentResponse, MeasurementEquationContract, ModelColumnWrite,
    ModelInnerProduct, NumericPrecision, NumericalStage, NumericsContract,
    ObservationTransactionRequirements, PolarizationContract, PrimaryBeamValidityPolicy,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestoringBeamPolicy, ScientificContract, SequentialContinuumTransform,
    SpectralContract, SpectralCoupling, SpectralSamplingLaw, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, UncorrectedImageMaskPolicy,
    VisibilityInnerProduct, WProjectionContract, WStatistics, WeightDensityScope,
    WeightingContract, WeightingScheme,
};
use casa_ms::{parse_spw_selector, resolve_channel_selector_selection};

use super::PrepareError;
use super::selection::{SourceSpectralWindow, WRange};
use super::spectral::PreparedSpectralAxis;
use crate::{AwProjection, Deconvolver, Gridder, ImagingRequest, SpecMode, Weighting};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// What the specification needs besides the request and the spectral axis.
pub(super) struct SpecificationInputs {
    pub(super) instrument: Option<InstrumentModel>,
    pub(super) uncorrected_mask: UncorrectedImageMaskPolicy,
    pub(super) w_projection: Option<WProjectionContract>,
    pub(super) aw_projection: Option<AwProjectionContract>,
    pub(super) cube_density_padding: Option<usize>,
    pub(super) continuum_transform: Option<SequentialContinuumTransform>,
}

/// The reconstruction algorithm: `mtmfs` whether or not it cleans; a dirty
/// run for the other solvers without iterations.
pub(super) fn reconstruction_algorithm(request: &ImagingRequest) -> ReconstructionAlgorithm {
    match request.deconvolver {
        Deconvolver::Mtmfs => ReconstructionAlgorithm::Mtmfs {
            scales_px: if request.scales.is_empty() {
                vec![0.0]
            } else {
                request.scales.clone()
            },
            small_scale_bias: request.smallscalebias,
        },
        _ if !request.solves() => ReconstructionAlgorithm::Dirty,
        Deconvolver::Hogbom => ReconstructionAlgorithm::Hogbom,
        Deconvolver::Clark => ReconstructionAlgorithm::Clark,
        Deconvolver::Multiscale => ReconstructionAlgorithm::Multiscale {
            scales_px: request.scales.clone(),
            small_scale_bias: request.smallscalebias,
        },
    }
}

/// The product normalization: the request's for a direction-dependent
/// gridder, unit response otherwise.
pub(super) fn normalization(request: &ImagingRequest) -> ProductNormalization {
    match &request.gridder {
        Gridder::Mosaic { normtype, .. } | Gridder::Awproject(AwProjection { normtype, .. }) => {
            *normtype
        }
        Gridder::Standard | Gridder::Wproject { .. } => ProductNormalization::UnitResponse,
    }
}

/// The primary-beam support of products and residual units.
pub(super) fn primary_beam_validity(
    request: &ImagingRequest,
) -> Result<PrimaryBeamValidityPolicy, PrepareError> {
    Ok(PrimaryBeamValidityPolicy::new(
        request.pblimit.abs() as f32,
        ProductSupportComparison::StrictlyGreater,
        ProductBlankingPolicy::Zero,
    )?)
}

/// The problem specification of `request` over `spectral`.
pub(super) fn specification(
    request: &ImagingRequest,
    spectral: &PreparedSpectralAxis,
    inputs: SpecificationInputs,
) -> Result<ProblemSpecification, PrepareError> {
    let direction_dependent = !matches!(
        request.gridder,
        Gridder::Standard | Gridder::Wproject { .. }
    );
    let algorithm = reconstruction_algorithm(request);
    let basis = match request.deconvolver {
        Deconvolver::Mtmfs => ReconstructionBasis::Taylor {
            terms: request.nterms,
        },
        _ => spectral.basis,
    };
    let reconstruction = ReconstructionContract::new(
        basis,
        algorithm.clone(),
        reconstruction_controls(request, &algorithm),
        PolarizationContract::new(request.stokes.clone()),
    );
    let equation = measurement_equation(&inputs);
    let common_beam = request.deconvolver == Deconvolver::Mtmfs
        || request.restoringbeam == RestoringBeamPolicy::Common;
    let mut science = ScientificContract::new(
        SpectralContract::new(
            spectral.sampling,
            if common_beam {
                SpectralCoupling::CommonRestoringBeam
            } else {
                SpectralCoupling::Independent
            },
        ),
        equation,
    );
    if let Some(model) = inputs.instrument {
        science = science.with_instrument_model(model);
    }
    let mut weighting = weighting(request);
    if let Some(padding) = inputs.cube_density_padding {
        weighting = weighting.with_casa_cube_density_padding(padding);
    }
    let minor_cycle = algorithm != ReconstructionAlgorithm::Dirty && request.iterations() > 0;
    let specification = ProblemSpecification::new(
        science,
        reconstruction,
        weighting,
        ProductRequirements::new(
            requested_products(request, minor_cycle, direction_dependent),
            normalization(request),
            if common_beam {
                RestoringBeamPolicy::Common
            } else {
                RestoringBeamPolicy::PerPlane
            },
            ProductValidityPolicies::new(
                primary_beam_validity(request)?,
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::Zero,
                )?,
            )
            .with_uncorrected_mask(inputs.uncorrected_mask),
        ),
        transaction(request),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::UnorderedWithinBudget,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    );
    Ok(match inputs.continuum_transform {
        Some(transform) => specification.with_visibility_transform(transform),
        None => specification,
    })
}

/// The measurement equation: the primary beam with an instrument model,
/// else scalar, with the request's W or AW projection.
fn measurement_equation(inputs: &SpecificationInputs) -> MeasurementEquationContract {
    let mut equation = MeasurementEquationContract::new(
        if inputs.instrument.is_some() {
            InstrumentResponse::PrimaryBeam
        } else {
            InstrumentResponse::Scalar
        },
        DeclaredInnerProducts::new(
            ModelInnerProduct::HermitianEuclidean,
            VisibilityInnerProduct::HermitianEuclidean,
        ),
    );
    if let Some(contract) = inputs.w_projection {
        equation = equation.with_w_projection(contract);
    }
    if let Some(contract) = inputs.aw_projection {
        equation = equation.with_aw_projection(contract);
    }
    equation
}

/// The writes to the observation: the model column and continuum-subtracted
/// corrected data, when asked for.
fn transaction(request: &ImagingRequest) -> ObservationTransactionRequirements {
    ObservationTransactionRequirements::new(if request.savemodel {
        ModelColumnWrite::SelectedRows
    } else {
        ModelColumnWrite::Disabled
    })
    .with_corrected_data_write(if request.save_continuum_residual {
        CorrectedDataWrite::SelectedOutputRows
    } else {
        CorrectedDataWrite::Disabled
    })
}

fn reconstruction_controls(
    request: &ImagingRequest,
    algorithm: &ReconstructionAlgorithm,
) -> ReconstructionControls {
    if *algorithm == ReconstructionAlgorithm::Dirty {
        return ReconstructionControls::new(0, 1.0, 0.0);
    }
    let iterations = request.iterations();
    let controls = ReconstructionControls::new(iterations, request.gain, request.threshold)
        .with_cycle_limits(
            request.minor_cycle_length.min(iterations.max(1)),
            request.nmajor,
        )
        .with_hogbom_iteration_accounting(if *algorithm == ReconstructionAlgorithm::Hogbom {
            request.hogbom_iteration_mode
        } else {
            HogbomIterationAccounting::Strict
        })
        .with_cycle_threshold(
            request.cyclefactor,
            request.minpsffraction,
            request.maxpsffraction,
        );
    if request.nsigma > 0.0 {
        controls.with_noise_sigma(request.nsigma)
    } else {
        controls
    }
}

/// The weighting: a cube's density per output channel under
/// `perchanweightdensity`, the whole selection's otherwise (CASA disables
/// cube density for continuum requests).
fn weighting(request: &ImagingRequest) -> WeightingContract {
    let density = if request.specmode != SpecMode::Mfs && request.perchanweightdensity {
        WeightDensityScope::PerOutputChannel
    } else {
        WeightDensityScope::GlobalSelection
    };
    match request.weighting {
        Weighting::Natural => {
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable)
        }
        Weighting::Uniform => WeightingContract::new(WeightingScheme::Uniform, density),
        Weighting::Briggs => WeightingContract::new(
            WeightingScheme::Briggs {
                robust: request.robust,
            },
            density,
        ),
        Weighting::Briggsbwtaper => WeightingContract::new(
            WeightingScheme::BriggsBandwidthTaper {
                robust: request.robust,
            },
            density,
        ),
    }
}

/// The products of a run: the standard set, the mask when a minor cycle
/// runs, the Taylor family for `mtmfs`, the weight image for a
/// direction-dependent gridder, and the primary-beam products asked for.
fn requested_products(
    request: &ImagingRequest,
    minor_cycle: bool,
    weight_image: bool,
) -> Vec<ProductKind> {
    let mtmfs = request.deconvolver == Deconvolver::Mtmfs;
    let mut products = vec![
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
    ];
    if minor_cycle {
        products.push(ProductKind::Mask);
    }
    products.push(ProductKind::Beam);
    if mtmfs {
        products.extend([
            ProductKind::TaylorTerms,
            ProductKind::SpectralIndex,
            ProductKind::SpectralIndexError,
        ]);
    }
    if weight_image {
        products.push(ProductKind::Weight);
    }
    if request.write_pb || request.pbcor {
        products.push(ProductKind::PrimaryBeam);
    }
    if request.pbcor {
        products.push(ProductKind::PbCorrectedImage);
        if mtmfs && matches!(request.gridder, Gridder::Mosaic { .. }) {
            products.push(ProductKind::PbCorrectedSpectralIndex);
        }
    }
    products
}

/// The highest and lowest selected frequencies.
fn frequency_range_hz(windows: &[SourceSpectralWindow]) -> [f64; 2] {
    let frequencies = || {
        windows
            .iter()
            .flat_map(|window| window.frequencies_hz.iter().copied())
    };
    [
        frequencies().fold(f64::INFINITY, f64::min),
        frequencies().fold(0.0_f64, f64::max),
    ]
}

/// W-projection over the selected `|w|` range: the requested planes, else
/// CASA `wStat` of the selection (the smallest `|w|` at the lowest
/// frequency, the rms at the highest).
pub(super) fn w_projection(
    planes: Option<NonZeroUsize>,
    windows: &[SourceSpectralWindow],
    w: WRange,
) -> Result<WProjectionContract, PrepareError> {
    let [lowest_hz, highest_hz] = frequency_range_hz(windows);
    let contract = WProjectionContract::new(
        w.maximum_abs_m * highest_hz / SPEED_OF_LIGHT_M_PER_S,
        planes,
    )?;
    if w.rows == 0 {
        return Ok(contract);
    }
    let rms_w_m = (w.sum_squares_m2 / w.rows as f64).sqrt();
    Ok(contract.with_statistics(WStatistics::new(
        w.minimum_abs_m * lowest_hz / SPEED_OF_LIGHT_M_PER_S,
        rms_w_m * highest_hz / SPEED_OF_LIGHT_M_PER_S,
    )?))
}

/// CASA's default AW pointing thresholds under `usepointing`.
const CASA_DEFAULT_AW_POINTING_OFFSET_SIGDEV_ARCSEC: [f64; 2] = [600.0, 600.0];

/// The installed A-projection: CASA's EVLA wideband aperture term with
/// conjugate beams, no separate prolate spheroidal, and 360-degree
/// parallactic-angle steps, over the selected `|w|` range.
pub(super) fn aw_projection(
    aw: &AwProjection,
    windows: &[SourceSpectralWindow],
    w: WRange,
) -> Result<AwProjectionContract, PrepareError> {
    let [_, highest_hz] = frequency_range_hz(windows);
    let planes = aw
        .wprojplanes
        .expect("a validated AW request names its W planes");
    Ok(AwProjectionContract::new(
        w.maximum_abs_m * highest_hz / SPEED_OF_LIGHT_M_PER_S,
        planes,
        true,
        false,
        true,
        true,
        aw.usepointing,
        effective_aw_pointing_offset_sigdev_arcsec(aw.usepointing, &aw.pointingoffsetsigdev),
        360.0,
        360.0,
    )?)
}

/// CASA's pointing-offset thresholds: any count but two takes `[600,
/// 600]` under `usepointing` and none without it.
fn effective_aw_pointing_offset_sigdev_arcsec(use_pointing: bool, requested: &[f64]) -> [f64; 2] {
    match requested {
        [group_threshold, refresh_threshold] => [*group_threshold, *refresh_threshold],
        _ if use_pointing => CASA_DEFAULT_AW_POINTING_OFFSET_SIGDEV_ARCSEC,
        _ => [0.0, 0.0],
    }
}

/// The sequential continuum subtraction of `fitspw` on the one selected
/// window: fit-only, apply-only and fit-and-apply roles over the union of
/// the fit and output channels, which it returns.
pub(super) fn continuum_transform(
    fitspw: &str,
    fitorder: usize,
    field_id: i32,
    window: &SourceSpectralWindow,
    output_channels: &[usize],
) -> Result<(SequentialContinuumTransform, Vec<usize>), PrepareError> {
    let order =
        u8::try_from(fitorder).map_err(|_| PrepareError::ContinuumFitOrder { order: fitorder })?;
    let selectors = parse_spw_selector(fitspw)?;
    if selectors.len() != 1 || usize::try_from(selectors[0].spw_id).ok() != Some(window.spw_id) {
        return Err(PrepareError::ContinuumFitWindow);
    }
    let fit_channels = match selectors.into_iter().next().expect("one selector").channels {
        Some(selector) => {
            resolve_channel_selector_selection(&window.frequencies_hz, &selector)?.indices
        }
        None => (0..window.frequencies_hz.len()).collect(),
    };
    let fit = fit_channels.into_iter().collect::<BTreeSet<_>>();
    let output = output_channels.iter().copied().collect::<BTreeSet<_>>();
    let selected = fit.union(&output).copied().collect::<Vec<_>>();
    let roles = selected
        .iter()
        .map(|&channel| {
            let use_role = match (fit.contains(&channel), output.contains(&channel)) {
                (true, false) => ContinuumChannelUse::FitOnly,
                (false, true) => ContinuumChannelUse::ApplyOnly,
                (true, true) => ContinuumChannelUse::FitAndApply,
                (false, false) => unreachable!("a member of the union is in fit or output"),
            };
            ContinuumChannelRole::new(
                u32::try_from(channel).expect("a CHAN_FREQ index fits u32"),
                use_role,
            )
        })
        .collect::<Vec<_>>();
    let rule = ContinuumFitRule::new(
        field_id,
        u32::try_from(window.spw_id).expect("SPW ids are nonnegative stored i32 values"),
        order,
        roles,
    )?;
    Ok((SequentialContinuumTransform::new(vec![rule])?, selected))
}

/// Whether the cube's density weighting follows CASA's padded
/// per-channel grid: a linearly interpolated multi-channel cube with
/// non-natural per-channel density.
pub(super) fn cube_density_padded(
    request: &ImagingRequest,
    spectral: &PreparedSpectralAxis,
) -> bool {
    matches!(request.specmode, SpecMode::Cube | SpecMode::Cubedata)
        && spectral.sampling == SpectralSamplingLaw::LINEAR
        && spectral.output_channels > 1
        && request.weighting != Weighting::Natural
        && request.perchanweightdensity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aw_pointing_thresholds_follow_casa_cardinality_default() {
        assert_eq!(
            effective_aw_pointing_offset_sigdev_arcsec(true, &[]),
            [600.0, 600.0]
        );
        assert_eq!(
            effective_aw_pointing_offset_sigdev_arcsec(true, &[30.0]),
            [600.0, 600.0]
        );
        assert_eq!(
            effective_aw_pointing_offset_sigdev_arcsec(true, &[300.0, 30.0, 10.0]),
            [600.0, 600.0]
        );
        assert_eq!(
            effective_aw_pointing_offset_sigdev_arcsec(true, &[300.0, 30.0]),
            [300.0, 30.0]
        );
        assert_eq!(
            effective_aw_pointing_offset_sigdev_arcsec(false, &[]),
            [0.0, 0.0]
        );
    }

    #[test]
    fn fit_and_output_channels_compile_to_one_union_with_exact_roles() {
        let window = SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: casa_types::measures::frequency::FrequencyRef::LSRK,
            frequencies_hz: vec![100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
            channel_widths_hz: vec![1.0; 8],
        };
        let (transform, selected) = continuum_transform("0:0~1;6~7", 1, 5, &window, &[1, 3, 4])
            .expect("compile transform roles");
        assert_eq!(selected, [0, 1, 3, 4, 6, 7]);
        let rule = transform.rule(5, 0).expect("field/SPW rule");
        assert_eq!(rule.channel_use(0), Some(ContinuumChannelUse::FitOnly));
        assert_eq!(rule.channel_use(1), Some(ContinuumChannelUse::FitAndApply));
        assert_eq!(rule.channel_use(3), Some(ContinuumChannelUse::ApplyOnly));
        assert_eq!(rule.channel_use(4), Some(ContinuumChannelUse::ApplyOnly));
        assert_eq!(rule.channel_use(6), Some(ContinuumChannelUse::FitOnly));
        assert_eq!(rule.channel_use(7), Some(ContinuumChannelUse::FitOnly));
    }

    fn products(overrides: serde_json::Value) -> BTreeSet<ProductKind> {
        let request = crate::request::tests::request(overrides);
        let minor_cycle = reconstruction_algorithm(&request) != ReconstructionAlgorithm::Dirty
            && request.iterations() > 0;
        let weight_image = !matches!(
            request.gridder,
            Gridder::Standard | Gridder::Wproject { .. }
        );
        requested_products(&request, minor_cycle, weight_image)
            .into_iter()
            .collect()
    }

    #[test]
    fn a_dirty_run_writes_no_clean_mask() {
        let dirty = products(serde_json::json!({ "write_pb": true }));
        assert!(!dirty.contains(&ProductKind::Mask));
        assert!(dirty.contains(&ProductKind::PrimaryBeam));
        let clean = products(serde_json::json!({ "niter": 10 }));
        assert!(clean.contains(&ProductKind::Mask));
    }

    #[test]
    fn clark_publishes_no_internal_sensitivity_or_weight() {
        let products = products(serde_json::json!({ "deconvolver": "clark", "niter": 10 }));
        assert!(products.contains(&ProductKind::RestoredImage));
        assert!(!products.contains(&ProductKind::Sensitivity));
        assert!(!products.contains(&ProductKind::Weight));
    }

    #[test]
    fn a_dirty_mosaic_mtmfs_writes_the_taylor_family_and_weight_without_a_mask() {
        assert_eq!(
            products(serde_json::json!({
                "deconvolver": "mtmfs",
                "nterms": 2,
                "gridder": "mosaic",
                "write_pb": true,
            })),
            BTreeSet::from([
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::Beam,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndex,
                ProductKind::SpectralIndexError,
                ProductKind::Weight,
                ProductKind::PrimaryBeam,
            ])
        );
    }

    #[test]
    fn a_corrected_mosaic_mtmfs_writes_the_corrected_spectral_index() {
        let products = products(serde_json::json!({
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 10,
            "gridder": "mosaic",
            "pbcor": true,
        }));
        for product in [ProductKind::Weight, ProductKind::PbCorrectedSpectralIndex] {
            assert!(products.contains(&product));
        }
        assert!(!products.contains(&ProductKind::Sensitivity));
    }
}
