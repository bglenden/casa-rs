// SPDX-License-Identifier: LGPL-3.0-or-later
//! Batch geometry on a small hand-made observation: the cases the
//! selected-observation route's equality law (T1, T1.5) does not reach.

use std::sync::Arc;

use casa_imaging_model::{
    AxisOrder, BatchGeometry, BatchGeometryError, CentreLaws, CompiledProblem,
    DeclaredInnerProducts, DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, GeometryInput, GeometryPlan, GeometryTraversal,
    GridFtProjection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, InstrumentResponse,
    MeasurementEquationContract, ModelColumnWrite, ModelInnerProduct, NumericPrecision,
    NumericalStage, NumericsContract, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInput, ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    PsfPhaseCentreLaw, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy,
    ScientificContract, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
};
use casa_observation::{
    AntennaEntry, AntennaId, BatchBuffers, BatchHeader, Catalogue, CorrelationAxis, DataGroupEntry,
    FieldEntry, FieldId, ObservationEntry, ObservationId, OrderedBaseline, PartitionEntry,
    PartitionId, Roles, ScanEntry, ScanId, Sharing, UvwFrame, VisibilityUnits, WeightRule,
};
use casa_types::Correlation;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EopValues, MPosition, MeasFrame, MeasuresProvider, MjdHighPrec};
use casa_types::quanta::Quantity;

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

/// The image's reference direction (J2000).
const REFERENCE: [f64; 2] = [1.0, -0.5];
/// The observation's one time: MJD 60000 UTC.
const MJD_DAYS: f64 = 60_000.0;
/// Each baseline's stored UVW.
const UVW_M: [[f64; 3]; 3] = [
    [100.0, 200.0, 5.0],
    [-300.0, 50.0, 2.0],
    [400.0, -150.0, -3.0],
];

/// Earth orientation without corrections and a fixed TAI−UTC.
#[derive(Debug)]
struct Measures;

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

fn measures() -> Arc<dyn MeasuresProvider> {
    Arc::new(Measures)
}

/// Three VLA-like antennas, one field at `field` and one partition of
/// their three baselines.
fn catalogue(field: MDirection) -> Arc<Catalogue> {
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
        }],
        scans: vec![ScanEntry {
            name: "1".to_string(),
            number: Some(1),
            intents: Vec::new(),
        }],
        observations: vec![ObservationEntry::default()],
        partitions: vec![PartitionEntry {
            spectral_window: "spw".to_string(),
            frequencies_hz: vec![1.4e9],
            frequency_frame: FrequencyRef::TOPO,
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
        }],
    })
}

/// The partition's one time of UVW, as a batch of `catalogue`.
fn fill(buffers: &mut BatchBuffers, catalogue: &Arc<Catalogue>) {
    let header = BatchHeader {
        partition: PartitionId(0),
        data_group: 0,
        times: 1,
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
    arrays.time_offsets_s.push(0.0);
    arrays.field.push(FieldId(0));
    arrays.uvw.extend(UVW_M.iter().flatten());
    fill.finish().expect("a whole batch");
}

/// The geometry of every sample of `catalogue`'s one batch under `problem`.
fn apply(problem: &CompiledProblem, catalogue: &Arc<Catalogue>) -> (GeometryPlan, BatchGeometry) {
    let plan = GeometryPlan::compile(problem, catalogue, measures()).expect("a geometry plan");
    let mut buffers = BatchBuffers::default();
    fill(&mut buffers, catalogue);
    let batch = buffers.batch().expect("a batch");
    let mut geometry = BatchGeometry::default();
    plan.apply(
        &batch,
        &[true; 3],
        None,
        &mut GeometryTraversal::default(),
        &mut geometry,
    )
    .expect("batch geometry");
    (plan, geometry)
}

fn problem(phase: PhaseCentreLaw, facets: FacetLayout, psf: PsfPhaseCentreLaw) -> CompiledProblem {
    compile(ProblemInput::new(
        specification(),
        geometry(phase, facets, psf),
        common::observation_snapshot(0),
        model_lifecycle_fixture::model_lifecycle(),
    ))
    .expect("a compiled problem")
}

fn geometry(phase: PhaseCentreLaw, facets: FacetLayout, psf: PsfPhaseCentreLaw) -> GeometryInput {
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
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
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

fn specification() -> ProblemSpecification {
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
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
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

fn j2000(longitude: f64, latitude: f64) -> MDirection {
    MDirection::from_angles(longitude, latitude, DirectionRef::J2000)
}

/// Whether two reprojections of one sample agree to rounding.
fn assert_close(actual: ([f64; 3], f64), expected: ([f64; 3], f64), what: &str) {
    let difference = (0..3)
        .map(|axis| (actual.0[axis] - expected.0[axis]).abs())
        .fold((actual.1 - expected.1).abs(), f64::max);
    assert!(
        difference <= 1.0e-9,
        "{what}: {actual:?} against {expected:?}"
    );
}

/// A moving phase centre moves every facet with it: each facet's target is
/// its own centre offset by the body's departure from the image reference
/// direction, so the facets keep their layout on the sky.
#[test]
fn facets_follow_a_moving_phase_centre_from_the_image_reference() {
    let field = j2000(0.9, -0.45);
    let catalogue = catalogue(field.clone());
    let problem = problem(
        PhaseCentreLaw::Ephemeris("SUN".to_string()),
        FacetLayout::Regular {
            columns: 2,
            rows: 2,
        },
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let (plan, geometry) = apply(&problem, &catalogue);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd_seconds(MJD_DAYS * 86_400.0, EpochRef::UTC))
        .with_position(catalogue.antennas[0].position.clone())
        .with_measures(measures());
    let (sun_longitude, sun_latitude) = MDirection::from_source_name("SUN", &frame)
        .and_then(|sun| sun.convert_to(DirectionRef::J2000, &frame))
        .expect("the Sun's J2000 direction")
        .as_angles();
    let shift = [sun_longitude - REFERENCE[0], sun_latitude - REFERENCE[1]];
    let facets = problem.geometry().domains()[0].facets();
    assert_eq!(plan.charts(), 4);
    assert_eq!(facets.len(), 4);
    for (chart, facet) in facets.iter().enumerate() {
        let centre = facet.phase_centre();
        let machine = GridFtProjection::faceted_between_directions(
            &field,
            &j2000(
                centre.longitude_rad() + shift[0],
                centre.latitude_rad() + shift[1],
            ),
        );
        for (sample, uvw) in UVW_M.iter().enumerate() {
            let projection = geometry.projection(sample, chart);
            assert_close(
                (projection.uvw_m, projection.phase_shift_m),
                machine.apply(*uvw),
                &format!("facet {chart}, sample {sample}"),
            );
        }
    }
    let first = geometry.projection(0, 0);
    let last = geometry.projection(0, 3);
    assert!(
        (first.phase_shift_m - last.phase_shift_m).abs() > 1.0e-3,
        "facets keep their layout: {first:?} against {last:?}"
    );
}

/// A field in ICRS with no reference epoch (an MSv4 field) is converted to
/// J2000 by the frame tie alone.
#[test]
fn an_icrs_field_without_an_epoch_is_tied_to_j2000() {
    let icrs = MDirection::from_angles(0.9, -0.45, DirectionRef::ICRS);
    let catalogue = catalogue(icrs.clone());
    let problem = problem(
        PhaseCentreLaw::Observation,
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let (_, geometry) = apply(&problem, &catalogue);
    let frame = MeasFrame::new()
        .with_position(catalogue.antennas[0].position.clone())
        .with_measures(measures());
    let field = icrs
        .convert_to(DirectionRef::J2000, &frame)
        .expect("ICRS to J2000 without an epoch");
    let (longitude, latitude) = field.as_angles();
    let machine = GridFtProjection::between_directions(
        &j2000(longitude, latitude),
        &j2000(REFERENCE[0], REFERENCE[1]),
    );
    let untied = GridFtProjection::between_directions(
        &j2000(0.9, -0.45),
        &j2000(REFERENCE[0], REFERENCE[1]),
    );
    for (sample, uvw) in UVW_M.iter().enumerate() {
        let projection = geometry.projection(sample, 0);
        let actual = (projection.uvw_m, projection.phase_shift_m);
        assert_close(actual, machine.apply(*uvw), &format!("sample {sample}"));
        assert_ne!(actual, untied.apply(*uvw), "the frame tie moves the field");
    }
}

/// No consumer reads a PSF centred elsewhere than its model, so geometry
/// refuses one rather than project to the model centre in silence.
#[test]
fn a_distinct_psf_phase_centre_is_refused() {
    let problem = problem(
        PhaseCentreLaw::Observation,
        FacetLayout::Single,
        PsfPhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.01, -0.5)),
    );
    let refused = GeometryPlan::compile(&problem, &catalogue(j2000(0.9, -0.45)), measures());
    assert!(
        matches!(refused, Err(BatchGeometryError::DistinctPsfCentre)),
        "{refused:?}"
    );
}

/// A plan applies only to batches of the catalogue it was compiled on.
#[test]
fn a_batch_of_another_catalogue_is_refused() {
    let problem = problem(
        PhaseCentreLaw::Observation,
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let compiled = catalogue(j2000(0.9, -0.45));
    let plan = GeometryPlan::compile(&problem, &compiled, measures()).expect("a geometry plan");
    let other = catalogue(j2000(0.9, -0.45));
    let mut buffers = BatchBuffers::default();
    fill(&mut buffers, &other);
    let batch = buffers.batch().expect("a batch");
    let refused = plan.apply(
        &batch,
        &[true; 3],
        None,
        &mut GeometryTraversal::default(),
        &mut BatchGeometry::default(),
    );
    assert!(
        matches!(refused, Err(BatchGeometryError::Catalogue(_))),
        "{refused:?}"
    );
}
