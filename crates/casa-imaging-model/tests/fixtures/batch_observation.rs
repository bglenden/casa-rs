// SPDX-License-Identifier: LGPL-3.0-or-later
//! A small hand-made observation and the problems batch geometry is tested
//! on: three VLA-like antennas and one field, in partitions of one window.
//! A test binary that includes it declares `common` and
//! `model_lifecycle_fixture` beside it.

// Each test binary uses part of the fixture.
#![allow(dead_code)]

use std::sync::Arc;

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompiledProblem, DeclaredInnerProducts, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, InstrumentResponse,
    ItrfPosition, MeasurementEquationContract, ModelColumnWrite, ModelInnerProduct,
    NumericPrecision, NumericalStage, NumericsContract, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemInput, ProblemSpecification, ProductBlankingPolicy,
    ProductKind, ProductNormalization, ProductRequirements, ProductSupportComparison,
    ProductValidityPolicies, Projection, PsfPhaseCentreLaw, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, TimeScale,
    UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope, WeightingContract,
    WeightingScheme, compile,
};
use casa_observation::{
    AntennaEntry, AntennaId, BatchBuffers, BatchHeader, Catalogue, CorrelationAxis, DataGroupEntry,
    FieldEntry, FieldId, ObservationEntry, ObservationId, OrderedBaseline, PartitionEntry,
    PartitionId, Roles, ScanEntry, ScanId, Sharing, UvwFrame, VisibilityUnits, WeightRule,
};
use casa_types::Correlation;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::EpochRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EopValues, MPosition, MeasuresProvider, MjdHighPrec};
use casa_types::quanta::Quantity;

/// The image's reference direction (J2000).
pub const REFERENCE: [f64; 2] = [1.0, -0.5];
/// The observation's first time: MJD 60000 UTC.
pub const MJD_DAYS: f64 = 60_000.0;
/// Each baseline's stored UVW, at every time.
pub const UVW_M: [[f64; 3]; 3] = [
    [100.0, 200.0, 5.0],
    [-300.0, 50.0, 2.0],
    [400.0, -150.0, -3.0],
];

/// Earth orientation without corrections and a fixed TAI−UTC.
#[derive(Debug)]
pub struct Measures;

impl MeasuresProvider for Measures {
    fn eop_values(&self, _utc_mjd: f64) -> Result<Option<EopValues>, String> {
        Ok(Some(EopValues {
            dut1_seconds: 0.0,
            x_arcsec: 0.0,
            y_arcsec: 0.0,
            dx_mas: 0.0,
            dy_mas: 0.0,
            is_predicted: false,
        }))
    }

    fn tai_minus_utc_seconds(&self, _utc_mjd: f64) -> Result<f64, String> {
        Ok(37.0)
    }

    fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, String> {
        Ok(tai_mjd - 37.0 / 86_400.0)
    }
}

pub fn measures() -> Arc<dyn MeasuresProvider> {
    Arc::new(Measures)
}

/// Three VLA-like antennas, one field at `field`, and `partitions`
/// partitions of the same content, each of their three baselines in one
/// topocentric window.
pub fn catalogue(field: MDirection, partitions: usize) -> Arc<Catalogue> {
    catalogue_in(field, partitions, FrequencyRef::TOPO)
}

/// [`catalogue`] with its window in `frame`.
pub fn catalogue_in(field: MDirection, partitions: usize, frame: FrequencyRef) -> Arc<Catalogue> {
    let antenna = |name: &str, offset_m: [f64; 3]| AntennaEntry {
        name: name.to_string(),
        station: name.to_string(),
        position: MPosition::new_itrf(
            -1_601_185.4 + offset_m[0],
            -5_041_977.5 + offset_m[1],
            3_554_875.9 + offset_m[2],
        ),
        mount: "ALT-AZ".to_string(),
        dish_diameter: Quantity::new(25.0, "m").expect("metres"),
    };
    let baseline = |a, b| OrderedBaseline {
        antenna1: AntennaId(a),
        antenna2: AntennaId(b),
    };
    let partition = PartitionEntry {
        spectral_window: "spw".to_string(),
        frequencies_hz: vec![1.4e9],
        frequency_frame: frame,
        channel_widths_hz: vec![1.0e6],
        correlations: vec![Correlation::XX],
        intents: Vec::new(),
        observation: ObservationId(0),
        feeds: None,
        fields: vec![FieldId(0)],
        scans: vec![ScanId(0)],
        baselines: vec![baseline(0, 1), baseline(0, 2), baseline(1, 2)],
        time_scale: EpochRef::UTC,
        time_origin: MjdHighPrec::new(MJD_DAYS, 0.0),
        data_groups: vec![DataGroupEntry {
            name: "base".to_string(),
            visibilities: "VISIBILITY".to_string(),
            flags: "FLAG".to_string(),
            weights: "WEIGHT".to_string(),
            uvw: "UVW".to_string(),
            units: VisibilityUnits::Undeclared,
            uvw_frame: UvwFrame {
                effective: DirectionRef::J2000,
                stored_label: None,
            },
        }],
        receptors: std::collections::BTreeMap::new(),
    };
    Arc::new(Catalogue {
        antennas: vec![
            antenna("A", [0.0, 0.0, 0.0]),
            antenna("B", [300.0, -100.0, 50.0]),
            antenna("C", [-200.0, 400.0, -150.0]),
        ],
        fields: vec![FieldEntry {
            name: "f".to_string(),
            phase_centre: field,
            reference_epoch: None,
            source_name: None,
            ephemeris: None,
        }],
        scans: vec![ScanEntry {
            name: "1".to_string(),
            number: Some(1),
            intents: Vec::new(),
        }],
        observations: vec![ObservationEntry::default()],
        partitions: vec![partition; partitions],
    })
}

/// `times` times of UVW, 10 s apart from the first, as a batch of
/// `partition` of `catalogue`.
pub fn fill(buffers: &mut BatchBuffers, catalogue: &Arc<Catalogue>, partition: u32, times: usize) {
    let header = BatchHeader {
        partition: PartitionId(partition),
        data_group: 0,
        times,
        first_channel: 0,
        channels: 1,
        correlations: CorrelationAxis::All,
        roles: Roles {
            values: false,
            flags: false,
            weights: false,
            uvw: true,
            time_centroid: false,
            interval: false,
            exposure: false,
            scan: false,
        },
        weights_per_channel: false,
        weight_rule: WeightRule::Stored,
        interval: Sharing::Constant(10.0),
        exposure: Sharing::Constant(10.0),
    };
    let mut fill = buffers.begin(header, catalogue).expect("a batch header");
    let arrays = fill.arrays();
    for time in 0..times {
        arrays.time_offsets_s.push(10.0 * time as f64);
        arrays.field.push(FieldId(0));
        arrays.uvw.extend(UVW_M.iter().flatten());
    }
    fill.finish().expect("a whole batch");
}

/// The problem of one Stokes I image, centred on [`REFERENCE`].
pub fn problem(
    phase: PhaseCentreLaw,
    facets: FacetLayout,
    psf: PsfPhaseCentreLaw,
) -> CompiledProblem {
    problem_in(phase, facets, psf, vec![PolarizationCoordinate::StokesI])
}

/// The problem of an image of `polarization`, centred on [`REFERENCE`].
pub fn problem_in(
    phase: PhaseCentreLaw,
    facets: FacetLayout,
    psf: PsfPhaseCentreLaw,
    polarization: Vec<PolarizationCoordinate>,
) -> CompiledProblem {
    let frames = [FrequencyFrame::Topocentric; 2];
    compile(ProblemInput::new(
        specification(polarization),
        geometry(phase, facets, psf, frames),
        super::common::observation_snapshot(0),
        super::model_lifecycle_fixture::model_lifecycle(),
    ))
    .expect("a compiled problem")
}

/// The Stokes I problem of `phase`, its frequencies taken from `source` to
/// `output`.
pub fn problem_framed(
    phase: PhaseCentreLaw,
    source: FrequencyFrame,
    output: FrequencyFrame,
) -> CompiledProblem {
    compile(ProblemInput::new(
        specification(vec![PolarizationCoordinate::StokesI]),
        geometry(
            phase,
            FacetLayout::Single,
            PsfPhaseCentreLaw::ImageDomainReference,
            [source, output],
        ),
        super::common::observation_snapshot(0),
        super::model_lifecycle_fixture::model_lifecycle(),
    ))
    .expect("a compiled problem")
}

fn geometry(
    phase: PhaseCentreLaw,
    facets: FacetLayout,
    psf: PsfPhaseCentreLaw,
    [source, output]: [FrequencyFrame; 2],
) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, REFERENCE[0], REFERENCE[1]),
        [255.0, 255.0],
        [-4.848_136_811_095_36e-5, 4.848_136_811_095_36e-5],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![
            ImageDomainSpec::new(
                ImageDomainRole::Main,
                ImageShape::new(512, 512),
                direction,
                facets,
                AxisOrder::new([
                    ImageAxis::DirectionLongitude,
                    ImageAxis::DirectionLatitude,
                    ImageAxis::Polarization,
                    ImageAxis::Spectral,
                ]),
            )
            .with_psf_phase_centre(psf),
        ],
        CentreLaws::new(phase, PointingCentreLaw::PhaseTrackingCentre),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            source,
            output,
            if source == output {
                SpectralFrameAnchor::NotApplicable
            } else {
                SpectralFrameAnchor::Conversion {
                    epoch: Epoch::new(MJD_DAYS, TimeScale::Utc),
                    direction: SkyDirection::new(DirectionFrame::J2000, REFERENCE[0], REFERENCE[1]),
                    observatory_position: ItrfPosition::new(
                        -1_601_185.4,
                        -5_041_977.5,
                        3_554_875.9,
                    ),
                }
            },
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn specification(polarization: Vec<PolarizationCoordinate>) -> ProblemSpecification {
    let budgets = NumericalStage::ALL
        .into_iter()
        .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
        .collect();
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(polarization),
        ),
        WeightingContract::new(
            WeightingScheme::Briggs { robust: 0.5 },
            WeightDensityScope::GlobalSelection,
        ),
        ProductRequirements::new(
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::Sensitivity,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndex,
            ],
            ProductNormalization::FlatNoise,
            RestoringBeamPolicy::PerPlane,
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    0.2,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::Zero,
                )
                .expect("a primary-beam support policy"),
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::Zero,
                )
                .expect("a Taylor support policy"),
            ),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F32, NumericPrecision::F64],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::RejectUnflaggedInput,
            budgets,
        ),
    )
}

pub fn j2000(longitude: f64, latitude: f64) -> MDirection {
    MDirection::from_angles(longitude, latitude, DirectionRef::J2000)
}
