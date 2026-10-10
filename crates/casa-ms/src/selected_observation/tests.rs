// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{
    BoundObservationSource, BoundSelectedObservation, ObservationSourceBinding,
    SelectedObservationContentBudget, SelectedObservationRow,
    access::{BoundObservationReferenceData, validate_input_weight_group},
};
use crate::derived::engine::MsCalEngine;
use crate::subtables::SubTable;
use crate::{
    MeasurementSet, MsSelectionIoBudget, ResolvedSelectedObservationAccess,
    SelectedObservationResolutionRequest, SyntheticObservationRequest, SyntheticSpectralSetup,
    SyntheticWorkerPolicy, generate_synthetic_observation_ms, resolve_selected_observation,
    tutorial_vla_a_antennas,
};
use casa_imaging_model::{
    AntennaResponseClass, AxisOrder, CentreLaws, CorrelationProduct, CorrelationSelection,
    CorrelationType, DataDescriptionSelection, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy,
    FrequencyFrame, GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, InstrumentModel, InstrumentResponse, IntentSelection, ItrfPosition,
    MeasurementEquationContract, MissingPointingPolicy, ModelBounds, ModelColumnWrite,
    ModelInnerProduct, ModelLifecycleRequirements, NumericPrecision, NumericalStage,
    NumericsContract, ObservationPointingLaw, ObservationSelection, ObservationSnapshotInput,
    ObservationSource, ObservationSourceInput, ObservationSourceProvenance,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInput,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    PsfPhaseCentreLaw, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedObservationRunChannel,
    SelectedObservationRunRow, SelectedRows, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, SpectralWindowSelection, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, TimeScale, UvSelection, UvwCoordinateLaw, VisibilityColumn,
    VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
    compile, compile_observation,
};
use casa_tables::{ColumnSchema, Table, TableOptions};
use casa_types::measures::{
    EopValues, MeasuresProvider, MeasuresProviderState,
    direction::{DirectionRef, MDirection},
    epoch::{EpochRef, MEpoch},
    frame::MeasFrame,
    frequency::{FrequencyRef, MFrequency},
    position::MPosition,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
mod content_requirements;
mod t41_ephemeris_oracle;

/// Canonical model-lifecycle requirements for the fixture problems.
fn model_lifecycle() -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(
        ModelBounds::new(10_000_000, 10_000_000, 1.0e30, 1.0e30)
            .expect("valid model lifecycle bounds"),
        NumericPrecision::F32,
    )
}

#[derive(Debug)]
struct AccountedTestMeasures {
    state: Mutex<AccountedTestMeasuresState>,
}

#[derive(Debug)]
struct AccountedTestMeasuresState {
    identity_sha256: [u8; 32],
    retained: Vec<u8>,
}

impl AccountedTestMeasures {
    fn with_heap_bytes(bytes: usize) -> Self {
        Self {
            state: Mutex::new(AccountedTestMeasuresState {
                identity_sha256: [90; 32],
                retained: vec![0; bytes],
            }),
        }
    }
}

impl MeasuresProvider for AccountedTestMeasures {
    fn prepare_bounded_state(&self) -> Result<Option<MeasuresProviderState>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "test Measures state lock poisoned".to_string())?;
        Ok(Some(MeasuresProviderState::new(
            state.identity_sha256,
            state.retained.capacity(),
        )))
    }

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
        Ok(32.0)
    }

    fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, String> {
        Ok(tai_mjd - 32.0 / 86_400.0)
    }
}

#[derive(Debug)]
struct OpaqueTestMeasures;

impl MeasuresProvider for OpaqueTestMeasures {}

#[test]
fn retained_selected_samples_are_bounded_and_block_partition_invariant() {
    let directory = tempfile::tempdir().expect("temporary selected-observation fixture");
    let path = directory.path().join("selected.ms");
    generate_fixture(&path);

    let problem = compiled_problem(&path, 2);
    let source = &problem.observation().sources()[0];
    let one_row_budget = content_budget_for_rows(&problem, source, 1, 1);
    let two_row_budget = content_budget_for_rows(&problem, source, 2, 1);
    let one_row = BoundObservationSource::open(&problem, source, one_row_budget)
        .expect("bind one-row physical blocks");
    let two_rows = BoundObservationSource::open(&problem, source, two_row_budget)
        .expect("bind two-row physical blocks");
    assert_eq!(
        one_row.content_plan().rows_per_block(),
        1,
        "{:?}",
        one_row.content_plan()
    );
    assert_eq!(
        two_rows.content_plan().rows_per_block(),
        2,
        "{:?}",
        two_rows.content_plan()
    );
    assert_eq!(
        one_row.content_plan().bytes_per_row(),
        two_rows.content_plan().bytes_per_row()
    );
    assert!(
        one_row.content_plan().preparation_bytes_per_row() > one_row.content_plan().bytes_per_row()
    );
    assert!(one_row.content_plan().retained_bytes() > 0);
    assert!(one_row.content_plan().initialization_scratch_bytes() > 0);
    assert!(one_row.content_plan().maximum_resident_bytes() <= one_row_budget.available_bytes());
    assert!(two_rows.content_plan().maximum_resident_bytes() <= two_row_budget.available_bytes());
    assert!(
        one_row.content_plan().preparation_bytes_per_block()
            > one_row.content_plan().bytes_per_block()
    );

    let one_row_samples = stream_rows(&problem, 1);
    let two_row_samples = stream_rows(&problem, 2);

    assert_eq!(one_row_samples.len(), 8);
    assert_eq!(one_row_samples, two_row_samples);
    assert_eq!(
        one_row_samples
            .iter()
            .map(|sample| {
                (
                    sample.row.physical_row,
                    sample.channel.channel_index,
                    sample.correlation.correlation_index(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (0, 0, 0),
            (0, 0, 1),
            (0, 2, 0),
            (0, 2, 1),
            (1, 0, 0),
            (1, 0, 1),
            (1, 2, 0),
            (1, 2, 1),
        ]
    );
    let first = &one_row_samples[0];
    assert_eq!(first.channel.frequency_centre_hz, 1.4e9);
    assert_eq!(first.frequency_hz, 1.4e9);
    assert_eq!(
        first.correlation.correlation_type(),
        CorrelationType::CircularRr
    );
    assert_eq!(first.visibility, Visibility::Complex32([0.0, 0.0]));
    let coordinates = &first.row.coordinates;
    assert_eq!(
        coordinates.pointing_directions.antenna1.frame(),
        DirectionFrame::J2000
    );
    assert_eq!(first.row.metadata.antenna1, 0);
    assert_eq!(first.row.metadata.antenna2, 1);
}

#[test]
fn t33_non_toy_vla_traversal_reports_row_shared_parallactic_angles() {
    let directory = tempfile::tempdir().expect("temporary T33 VLA fixture");
    let path = directory.path().join("t33-vla-polarization.ms");
    let mut request =
        SyntheticObservationRequest::vla_ppdisk("unused.fits", &path, tutorial_vla_a_antennas());
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = 3.0;
    request.integration_seconds = 1.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "t33-three-channel".to_string(),
        start_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        channel_count: 3,
    }];
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    let report =
        generate_synthetic_observation_ms(&request).expect("generate non-toy T33 VLA fixture");
    assert_eq!(report.antenna_count, 27);
    assert_eq!(report.baseline_count, 351);
    assert_eq!(report.time_sample_count, 3);
    assert_eq!(report.main_row_count, 1_053);

    let ordinary_problem = compiled_problem_with_polarization(
        &path,
        report.main_row_count,
        vec![PolarizationCoordinate::StokesI],
    );
    assert!(!ordinary_problem.requires_parallactic_angles());
    let ordinary_source = &ordinary_problem.observation().sources()[0];
    let ordinary = open_observation(
        &ordinary_problem,
        ordinary_source,
        content_budget_for_rows(&ordinary_problem, ordinary_source, 37, 1),
    )
    .unwrap();
    assert_eq!(
        ordinary
            .source(0)
            .geometry_engine()
            .parallactic_angle_cache_entries(),
        0
    );
    let (ordinary, ordinary_samples) = stream(&ordinary_problem, ordinary).unwrap();
    assert!(
        ordinary_samples.iter().all(|sample| sample
            .row
            .coordinates
            .parallactic_angles_rad
            .is_none())
    );
    // This fresh engine inserts an entry on every PA evaluation, even for a
    // non-alt-az mount. No entries means the unused PA/AZEL chain was not run.
    assert_eq!(
        ordinary
            .source(0)
            .geometry_engine()
            .parallactic_angle_cache_entries(),
        0
    );

    let problem = compiled_problem_with_polarization(
        &path,
        report.main_row_count,
        vec![
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesU,
            PolarizationCoordinate::StokesV,
        ],
    );
    let source = &problem.observation().sources()[0];
    assert!(problem.requires_parallactic_angles());
    let bound = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 37, 1),
    )
    .expect("bind non-toy T33 traversal");
    let (bound, samples) = stream(&problem, bound).expect("read non-toy T33 stream");
    assert_eq!(samples.len(), report.main_row_count * 4);

    let measurement_set = MeasurementSet::open(&path).expect("open T33 VLA fixture");
    let geometry = MsCalEngine::new(&measurement_set).expect("bind physical geometry");
    let first = &samples[0];
    let time_mjd_seconds = first.row.coordinates.time.mjd_days() * 86_400.0;
    let field_id = usize::try_from(first.row.metadata.field_id).expect("field id");
    let antenna1 = usize::try_from(first.row.metadata.antenna1).expect("antenna 1");
    let antenna2 = usize::try_from(first.row.metadata.antenna2).expect("antenna 2");
    let physical = [
        geometry
            .parallactic_angle(time_mjd_seconds, field_id, antenna1)
            .expect("antenna 1 physical parallactic angle"),
        geometry
            .parallactic_angle(time_mjd_seconds, field_id, antenna2)
            .expect("antenna 2 physical parallactic angle"),
    ];
    for (operator, physical) in first
        .row
        .coordinates
        .parallactic_angles_rad
        .expect("polarized reconstruction requires physical angles")
        .iter()
        .zip(physical)
    {
        assert!(
            (operator + physical).abs() < 1.0e-12,
            "CASA's polarization operator uses the negative physical parallactic angle"
        );
    }

    let mut minimum = [f64::INFINITY; 2];
    let mut maximum = [f64::NEG_INFINITY; 2];
    for row_samples in samples.as_chunks::<4>().0 {
        let expected = row_samples[0]
            .row
            .coordinates
            .parallactic_angles_rad
            .unwrap();
        assert!(expected.iter().all(|angle| angle.is_finite()));
        assert!(
            row_samples
                .iter()
                .all(|sample| sample.row.coordinates.parallactic_angles_rad == Some(expected))
        );
        for antenna in 0..2 {
            minimum[antenna] = minimum[antenna].min(expected[antenna]);
            maximum[antenna] = maximum[antenna].max(expected[antenna]);
        }
    }
    assert!(
        (maximum[0] - minimum[0]).abs() > 1.0e-4 || (maximum[1] - minimum[1]).abs() > 1.0e-4,
        "realistic VLA rows must not collapse to a constant feed rotation"
    );
    assert!(
        bound
            .source(0)
            .geometry_engine()
            .parallactic_angle_cache_entries()
            > 0
    );
}

#[test]
fn facet_chart_projections_are_domain_major_and_block_partition_invariant() {
    let directory = tempfile::tempdir().expect("temporary multidomain projection fixture");
    let path = directory.path().join("multidomain.ms");
    generate_fixture_with_phase_center(&path, 32, [1.0, -0.5]);

    let problem = compiled_problem_with_geometry(&path, 480, multidomain_geometry());
    assert!(matches!(
        problem.geometry().domains()[0].role(),
        ImageDomainRole::Main
    ));
    assert!(matches!(
        problem.geometry().domains()[1].role(),
        ImageDomainRole::Outlier(name) if name == "alpha"
    ));
    assert!(matches!(
        problem.geometry().domains()[2].role(),
        ImageDomainRole::Outlier(name) if name == "zeta"
    ));
    let source = &problem.observation().sources()[0];

    let traverse = |rows_per_block| {
        let observation = open_observation(
            &problem,
            source,
            content_budget_for_rows(&problem, source, rows_per_block, 1),
        )
        .expect("bind multidomain traversal");
        let (_, samples) = stream(&problem, observation).expect("stream multidomain blocks");
        let projected_rows = samples
            .iter()
            .filter(|sample| {
                sample.channel.channel_index == 0 && sample.correlation.correlation_index() == 0
            })
            .map(|sample| {
                (
                    sample.row.physical_row,
                    sample.row.coordinates.raw_uvw_m,
                    sample.row.domain_projections().iter().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        (projected_rows, samples.len())
    };

    let (one_row, one_row_samples) = traverse(7);
    let (two_rows, two_row_samples) = traverse(19);
    assert_eq!(
        one_row, two_rows,
        "physical block boundaries are not geometry"
    );
    assert_eq!(
        one_row_samples, two_row_samples,
        "sample count is invariant to block partitioning"
    );
    assert_eq!(one_row.len(), 480);
    for (_, raw_uvw_m, projections) in one_row {
        assert_eq!(
            projections
                .iter()
                .map(|projection| (projection.domain_ordinal(), projection.facet_ordinal()))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (2, 0)]
        );
        assert_ne!(projections[0].model().transformed_uvw_m(), raw_uvw_m);
        assert_ne!(projections[0].model(), projections[1].model());
        assert!(!projections[0].psf_shares_model());
        assert_eq!(projections[0].psf(), projections[3].psf());
        assert!(projections[4].psf_shares_model());
        assert!(!projections[5].psf_shares_model());
        assert_ne!(
            projections[4].model().transformed_uvw_m(),
            projections[5].model().transformed_uvw_m()
        );
    }
}

#[test]
fn selected_projection_preserves_cell_flags() {
    let directory = tempfile::tempdir().expect("temporary paired-flag fixture");
    let path = directory.path().join("paired-flag.ms");
    generate_fixture(&path);

    let mut measurement_set = MeasurementSet::open(&path).expect("open paired-flag fixture");
    let mut flags = match measurement_set
        .main_table()
        .cell_accessor(0, "FLAG")
        .and_then(|cell| cell.array())
        .expect("read FLAG cell")
        .clone()
    {
        ArrayValue::Bool(flags) => flags,
        other => panic!("FLAG must be Bool, found {:?}", other.primitive_type()),
    };
    *flags.iter_mut().next().expect("nonempty FLAG cell") = true;
    measurement_set
        .main_table_mut()
        .cell_accessor_mut(0, "FLAG")
        .expect("open FLAG cell for mutation")
        .set(Value::Array(ArrayValue::Bool(flags)))
        .expect("flag only the first parallel hand");
    measurement_set.save().expect("persist paired-flag fixture");
    drop(measurement_set);

    let problem = compiled_problem(&path, 2);
    let samples = stream_rows(&problem, 1);

    assert!(samples[0].flag, "the stored RR flag remains exact");
    assert!(!samples[1].flag, "the stored LL flag remains exact");
    assert!(
        samples[2..].iter().all(|sample| !sample.flag),
        "other cells remain unflagged"
    );
}

#[test]
fn block_traversal_reports_unequal_parallel_hand_weights() {
    let directory = tempfile::tempdir().expect("temporary paired-weight fixture");
    let path = directory.path().join("paired-weight.ms");
    generate_fixture(&path);

    let mut measurement_set = MeasurementSet::open(&path).expect("open paired-weight fixture");
    let mut weights = match measurement_set
        .main_table()
        .cell_accessor(0, "WEIGHT")
        .and_then(|cell| cell.array())
        .expect("read WEIGHT cell")
        .clone()
    {
        ArrayValue::Float32(weights) => weights,
        other => panic!("WEIGHT must be Float32, found {:?}", other.primitive_type()),
    };
    {
        let mut elements = weights.iter_mut();
        *elements.next().expect("first parallel-hand weight") = 3.0;
        *elements.next().expect("last parallel-hand weight") = 7.0;
        assert!(elements.next().is_none(), "fixture has exactly two hands");
    }
    measurement_set
        .main_table_mut()
        .cell_accessor_mut(0, "WEIGHT")
        .expect("open WEIGHT cell for mutation")
        .set(Value::Array(ArrayValue::Float32(weights)))
        .expect("persist unequal parallel-hand weights");
    measurement_set.save().expect("save paired-weight fixture");
    drop(measurement_set);

    let problem = compiled_problem(&path, 2);
    let reported = stream_rows(&problem, 2)
        .into_iter()
        .map(|sample| {
            (
                sample.row.physical_row,
                sample.channel.channel_index,
                sample.correlation.correlation_index(),
                sample.weight,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(reported[0].0, 0);
    assert_eq!(reported[0].1, reported[1].1);
    assert_eq!(reported[0].2, 0);
    assert_eq!(reported[1].2, 1);
    assert_eq!(reported[0].3, 3.0);
    assert_eq!(reported[1].3, 7.0);
}

#[test]
fn imaging_weight_groups_reject_ambiguous_or_mixed_multi_correlation_layouts() {
    let products = |types: &[CorrelationType]| {
        types
            .iter()
            .copied()
            .enumerate()
            .map(|(index, correlation_type)| {
                CorrelationProduct::new(index as u32, correlation_type)
            })
            .collect::<Vec<_>>()
    };
    for valid in [
        products(&[CorrelationType::CircularRl]),
        products(&[CorrelationType::CircularRr, CorrelationType::CircularLl]),
        products(&[
            CorrelationType::CircularRr,
            CorrelationType::CircularRl,
            CorrelationType::CircularLl,
        ]),
        products(&[
            CorrelationType::CircularRr,
            CorrelationType::CircularRl,
            CorrelationType::CircularLr,
            CorrelationType::CircularLl,
        ]),
        products(&[
            CorrelationType::LinearXx,
            CorrelationType::LinearXy,
            CorrelationType::LinearYx,
            CorrelationType::LinearYy,
        ]),
    ] {
        validate_input_weight_group(&valid, 0).expect("canonical imaging-weight group");
    }
    for invalid in [
        products(&[]),
        products(&[CorrelationType::CircularRr, CorrelationType::CircularRl]),
        products(&[CorrelationType::CircularRl, CorrelationType::CircularLr]),
        products(&[
            CorrelationType::CircularRr,
            CorrelationType::LinearXy,
            CorrelationType::CircularLl,
        ]),
        products(&[
            CorrelationType::CircularRr,
            CorrelationType::CircularLr,
            CorrelationType::CircularRl,
            CorrelationType::CircularLl,
        ]),
        products(&[
            CorrelationType::CircularRr,
            CorrelationType::CircularRl,
            CorrelationType::CircularRl,
            CorrelationType::CircularLl,
        ]),
        products(&[CorrelationType::CircularRr, CorrelationType::LinearYy]),
    ] {
        assert!(matches!(
            validate_input_weight_group(&invalid, 7),
            Err(
                super::BoundObservationSourceError::UnsupportedImagingWeightCorrelationGroup {
                    polarization_id: 7
                }
            )
        ));
    }
}

#[test]
fn retained_metadata_is_rejected_before_content_blocks_are_planned() {
    let directory = tempfile::tempdir().expect("temporary metadata-budget fixture");
    let path = directory.path().join("metadata-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.observation().sources()[0];
    let error = match BoundObservationSource::open(
        &problem,
        source,
        SelectedObservationContentBudget::new(1, 1, 4),
    ) {
        Ok(_) => {
            panic!("retained geometry and coordinate catalogs must fit before engine construction")
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
        )
    ));
}

#[test]
fn selected_observation_rejects_opaque_measures_providers() {
    assert!(matches!(
        super::SelectedObservationMeasures::new(Arc::new(OpaqueTestMeasures)),
        Err(super::SelectedObservationMeasuresError::UnaccountedProvider)
    ));
}

#[test]
fn selected_observation_accepts_exact_ephemeris_within_its_budget() {
    let directory = tempfile::tempdir().expect("temporary exact ephemeris fixture");
    let path = directory.path().join("exact-ephemeris.ms");
    generate_fixture(&path);
    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Ephemeris("Mars".to_string()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
    );
    let source = &problem.observation().sources()[0];
    let budget = SelectedObservationContentBudget::new(64 << 20, 1, 4);
    let ephemeris =
        crate::SelectedObservationEphemeris::named("Mars", budget.reference_data_budget())
            .expect("admit exact ephemeris fixture");
    let binding = ObservationSourceBinding::new(source_ordinal(source), budget)
        .with_ephemeris(Some(ephemeris.clone()));
    let reference_data_bytes = binding.reference_data_bytes();
    assert!(reference_data_bytes > 0);
    BoundSelectedObservation::open(&problem, test_measures(), vec![binding])
        .expect("open exact ephemeris binding");

    let tight = ObservationSourceBinding::new(
        source_ordinal(source),
        SelectedObservationContentBudget::new(reference_data_bytes - 1, 1, 4),
    )
    .with_ephemeris(Some(ephemeris));
    assert!(matches!(
        BoundSelectedObservation::open(&problem, test_measures(), vec![tight]),
        Err(super::BoundSelectedObservationError::ReferenceDataBudgetExceeded {
            required_bytes,
            ..
        }) if required_bytes == reference_data_bytes
    ));
}

#[test]
fn cube_traversals_report_native_channels_and_their_output_frame_centres() {
    let directory = tempfile::tempdir().expect("temporary cube-frequency fixture");
    let path = directory.path().join("cube-frequencies.ms");
    generate_fixture(&path);
    let identity = compiled_problem_with_sampling(
        &path,
        2,
        SpectralSamplingLaw::IDENTITY,
        SpectralWcs::Tabular {
            channel_centres_hz: vec![1.4e9, 1.402e9],
            channel_boundaries_hz: vec![1.3995e9, 1.401e9, 1.4025e9],
        },
    );
    let cubic = compiled_problem_with_sampling(
        &path,
        2,
        SpectralSamplingLaw::CUBIC,
        SpectralWcs::Tabular {
            channel_centres_hz: vec![1.3995e9, 1.4005e9, 1.4015e9, 1.4025e9],
            channel_boundaries_hz: vec![1.399e9, 1.4e9, 1.401e9, 1.402e9, 1.403e9],
        },
    );

    for problem in [&identity, &cubic] {
        let samples = stream_rows(problem, 1);
        assert_eq!(samples.len(), 8);
        for sample in &samples {
            // The fixture is TOPO and so is the output frame: the centre
            // passes through unchanged.
            assert_eq!(
                sample.frequency_hz.to_bits(),
                sample.channel.frequency_centre_hz.to_bits()
            );
            assert_eq!(sample.weight, 1.0);
            assert!(!sample.flag);
        }
        let channel = |sample: &Sample| {
            (
                sample.channel.channel_index,
                sample.channel.frequency_centre_hz,
            )
        };
        assert_eq!(channel(&samples[0]), (0, 1.4e9));
        assert_eq!(channel(&samples[1]), channel(&samples[0]));
        assert_eq!(channel(&samples[2]), (2, 1.402e9));
        assert_eq!(channel(&samples[3]), channel(&samples[2]));
    }
}

#[test]
fn selected_channels_report_exact_centres_including_flagged_channels() {
    for (frequencies, selected) in [
        (vec![1.4e9, 1.401e9, 1.402e9, 1.407e9, 1.408e9], vec![1, 3]),
        (
            vec![1.4e9, 1.401e9, 1.402e9, 1.407e9, 1.408e9],
            vec![1, 3, 4],
        ),
        (vec![1.408e9, 1.407e9, 1.402e9, 1.401e9, 1.4e9], vec![1, 3]),
        (vec![1.4e9, 1.401e9, 1.402e9, 1.407e9, 1.408e9], vec![2]),
    ] {
        let directory = tempfile::tempdir().expect("selected geometry fixture");
        let path = directory.path().join("selected-geometry.ms");
        generate_fixture_with_channel_count(&path, 2, frequencies.len());
        let mut ms = MeasurementSet::open(&path).expect("open geometry fixture");
        ms.spectral_window_mut()
            .expect("SPW")
            .table_mut()
            .cell_accessor_mut(0, "CHAN_FREQ")
            .expect("frequency cell")
            .set(Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![frequencies.len()], frequencies.clone())
                    .expect("frequency array"),
            )))
            .expect("set unequal centre spacing");
        let mut flags = ArrayD::from_elem(vec![2, frequencies.len()], false);
        flags[[0, selected[0] as usize]] = true;
        flags[[1, selected[0] as usize]] = true;
        ms.main_table_mut()
            .cell_accessor_mut(0, "FLAG")
            .expect("flag cell")
            .set(Value::Array(ArrayValue::Bool(flags)))
            .expect("flag only the first selected channel in the first row");
        ms.save().expect("save geometry fixture");
        drop(ms);
        let base = compiled_problem(&path, 2);
        let source = &base.observation().sources()[0];
        let selection = source.selection();
        let snapshot = compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                source.provenance().clone(),
                ObservationSelection::new(
                    selection.rows().clone(),
                    selection.rows_filter().clone(),
                    selection.data_descriptions().to_vec(),
                    vec![SpectralWindowSelection::new(0, selected.clone())],
                    selection.correlations().to_vec(),
                ),
                source.columns(),
                source.corrected_data_present(),
            ),
        ]))
        .expect("compile exact selected channels");
        let problem = compile(ProblemInput::new(
            specification(),
            geometry(),
            snapshot,
            model_lifecycle(),
        ))
        .expect("compile geometry problem");
        let samples = stream_rows(&problem, 1);
        assert_eq!(samples.len(), 2 * 2 * selected.len());
        for (index, sample) in samples.iter().enumerate() {
            let channel = selected[index / 2 % selected.len()];
            assert_eq!(sample.channel.channel_index, channel);
            assert_eq!(
                sample.channel.frequency_centre_hz,
                frequencies[channel as usize]
            );
            assert_eq!(sample.frequency_hz, frequencies[channel as usize]);
            assert_eq!(
                sample.flag,
                sample.row.physical_row == 0 && channel == selected[0]
            );
        }
        assert_eq!(stream_rows(&problem, 2), samples);
    }
}

#[test]
fn real_ms_cube_traversal_uses_the_native_field_frame_for_output_conversion() {
    let directory = tempfile::tempdir().expect("temporary transformed-frame fixture");
    let path = directory.path().join("transformed-contributions.ms");
    generate_fixture(&path);
    let problem = compiled_problem_with_transformed_sampling(
        &path,
        2,
        SpectralSamplingLaw::LINEAR,
        SpectralWcs::Tabular {
            channel_centres_hz: vec![1.4e9, 1.4001e9],
            channel_boundaries_hz: vec![1.39995e9, 1.40005e9, 1.40015e9],
        },
    );
    let source = &problem.observation().sources()[0];
    let expected_measures = test_measures();
    let expected_output_frame = MeasFrame::new()
        .with_measures(expected_measures.provider())
        .with_epoch(MEpoch::from_mjd(59_000.25, EpochRef::UTC))
        .with_position(MPosition::new_itrf(-1_601_188.0, -5_041_977.0, 3_554_875.0))
        .with_direction(MDirection::from_angles(1.0, -0.5, DirectionRef::J2000));
    let observation = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind transformed-frame traversal");
    let (observation, samples) = stream(&problem, observation).expect("stream transformed frames");
    let mut values = Vec::new();

    for sample in &samples {
        let frame = observation
            .source(0)
            .geometry_engine()
            .spectral_frame_observatory(
                sample.row.coordinates.time.mjd_days() * 86_400.0,
                usize::try_from(sample.row.metadata.field_id).expect("non-negative FIELD_ID"),
            )
            .expect("independent row frame");
        let transform_to_output = |frequency_hz| {
            MFrequency::new(frequency_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::GEO, &frame)
                .expect("TOPO to GEO")
                .convert_to(FrequencyRef::BARY, &frame)
                .expect("GEO to BARY")
                .convert_to(FrequencyRef::LSRK, &expected_output_frame)
                .expect("BARY to LSRK")
                .hz()
        };
        let image_anchor_hz = transform_to_output(sample.channel.frequency_centre_hz);
        let transform_in_native_field_frame = |frequency_hz| {
            MFrequency::new(frequency_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::GEO, &frame)
                .expect("source-only TOPO to GEO")
                .convert_to(FrequencyRef::BARY, &frame)
                .expect("source-only GEO to BARY")
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("source-only BARY to LSRK")
                .hz()
        };
        let native_field_hz = transform_in_native_field_frame(sample.channel.frequency_centre_hz);
        assert!((image_anchor_hz - native_field_hz).abs() > 1.0);
        assert_eq!(sample.frequency_hz.to_bits(), native_field_hz.to_bits());
        assert_eq!(sample.weight, 1.0);
        assert!(!sample.flag);
        values.push((
            sample.channel.channel_index,
            image_anchor_hz,
            native_field_hz,
        ));
    }

    assert_eq!(
        problem.geometry().spectral().output_frame(),
        FrequencyFrame::Lsrk
    );
    assert_eq!(values.len(), 8);
    assert_eq!(values[0].0, 0);
    assert!(
        values[0].1 != values[0].2,
        "the image anchor must remain observably distinct from the native FIELD frame"
    );
    assert_eq!(values[1].2, values[0].2);
    assert_eq!(values[2].0, 2);
    assert_eq!(values[3].2, values[2].2);
}

#[test]
fn measures_provider_residency_is_charged_once_and_rejected_under_a_tight_budget() {
    let directory = tempfile::tempdir().expect("temporary Measures-budget fixture");
    let path = directory.path().join("measures-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.observation().sources()[0];

    let baseline_measures = test_measures();
    let baseline_shared_bytes = selected_observation_shared_bytes(&baseline_measures);
    let baseline_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, baseline_shared_bytes, 1, 1);
    let baseline = BoundObservationSource::open_with_measures(
        &problem,
        source,
        &baseline_measures,
        baseline_shared_bytes,
        baseline_budget,
        BoundObservationReferenceData::new(None, None),
    )
    .expect("bind baseline provider residency");

    let large_provider = Arc::new(AccountedTestMeasures::with_heap_bytes(128 * 1_024));
    let erased_provider: Arc<dyn MeasuresProvider> = large_provider;
    let large_measures = super::SelectedObservationMeasures::new(erased_provider)
        .expect("account large provider residency");
    let large_shared_bytes = selected_observation_shared_bytes(&large_measures);
    assert!(matches!(
        BoundObservationSource::open_with_measures(
            &problem,
            source,
            &large_measures,
            large_shared_bytes,
            baseline_budget,
            BoundObservationReferenceData::new(None, None),
        ),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));

    let large_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, large_shared_bytes, 1, 1);
    let large = BoundObservationSource::open_with_measures(
        &problem,
        source,
        &large_measures,
        large_shared_bytes,
        large_budget,
        BoundObservationReferenceData::new(None, None),
    )
    .expect("bind admitted large provider residency");
    assert_eq!(
        large.content_plan().retained_bytes() - baseline.content_plan().retained_bytes(),
        large_measures.retained_bytes() - baseline_measures.retained_bytes(),
        "the shared provider allocation must have one exact retained owner"
    );
    assert!(large.content_plan().maximum_resident_bytes() <= large_budget.available_bytes());
}

#[test]
fn retained_opened_table_metadata_is_charged_once_for_oversized_variable_references() {
    let directory = tempfile::tempdir().expect("temporary oversized-MEASINFO fixture");
    let path = directory.path().join("oversized-measinfo.ms");
    generate_fixture(&path);
    let centres = CentreLaws::new(
        PhaseCentreLaw::Observation,
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::Observation(ObservationPointingLaw::new(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::AntennaBoresight,
            PointingTimeSampling::VisibilityTime,
            PointingInterpolation::Nearest,
            PointingExtrapolation::HoldNearest,
            MissingPointingPolicy::Reject,
        )),
    );
    let baseline_problem = compiled_problem_with_centres(&path, 2, centres.clone());
    let baseline_source = &baseline_problem.observation().sources()[0];
    let baseline_budget = content_budget_for_rows(&baseline_problem, baseline_source, 1, 1);
    let baseline =
        BoundObservationSource::open(&baseline_problem, baseline_source, baseline_budget)
            .expect("bind baseline POINTING source");
    let baseline_retained_bytes = baseline.content_plan().retained_bytes();
    drop(baseline);
    let baseline_storage_bytes = MeasurementSet::open_retained_read(&path)
        .expect("open baseline retained MeasurementSet")
        .retained_read_metadata_bytes()
        .expect("project baseline retained MeasurementSet");

    const REFERENCE_COUNT: usize = 2_048;
    let mut measurement_set = MeasurementSet::open(&path).expect("open metadata fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        let table = pointing.table_mut();
        table
            .add_column(
                ColumnSchema::scalar("DIRECTION_REF", PrimitiveType::Int32),
                Some(Value::Scalar(ScalarValue::Int32(0))),
            )
            .expect("add variable reference column");
        let mut keywords = table
            .column_keywords("DIRECTION")
            .cloned()
            .expect("DIRECTION keywords");
        keywords.upsert(
            "MEASINFO",
            Value::Record(RecordValue::new(vec![
                RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String("direction".to_string())),
                ),
                RecordField::new(
                    "VarRefCol",
                    Value::Scalar(ScalarValue::String("DIRECTION_REF".to_string())),
                ),
                RecordField::new(
                    "TabRefTypes",
                    Value::Array(ArrayValue::from_string_vec(vec![
                        "J2000".to_string();
                        REFERENCE_COUNT
                    ])),
                ),
                RecordField::new(
                    "TabRefCodes",
                    Value::Array(ArrayValue::from_i32_vec(
                        (0..REFERENCE_COUNT)
                            .map(|code| i32::try_from(code).expect("reference code fits i32"))
                            .collect(),
                    )),
                ),
            ])),
        );
        table.set_column_keywords("DIRECTION", keywords);
    }
    measurement_set.save().expect("save oversized MEASINFO");

    let problem = compiled_problem_with_centres(&path, 2, centres);
    let source = &problem.observation().sources()[0];
    assert!(matches!(
        BoundObservationSource::open(&problem, source, baseline_budget,),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));

    let inflated_budget = content_budget_for_rows(&problem, source, 1, 1);
    let inflated = BoundObservationSource::open(&problem, source, inflated_budget)
        .expect("bind oversized variable-reference source");
    let inflated_storage_bytes = MeasurementSet::open_retained_read(&path)
        .expect("open inflated retained MeasurementSet")
        .retained_read_metadata_bytes()
        .expect("project inflated retained MeasurementSet");
    assert_eq!(
        inflated.content_plan().retained_bytes() - baseline_retained_bytes,
        inflated_storage_bytes - baseline_storage_bytes,
        "the opened MeasurementSet object graph must be the sole retained owner of persisted MEASINFO"
    );
    assert!(inflated.content_plan().maximum_resident_bytes() <= inflated_budget.available_bytes());
    drop(inflated);
    let inflated = open_observation(&problem, source, inflated_budget)
        .expect("open oversized variable-reference observation");
    let (inflated, samples) =
        stream(&problem, inflated).expect("evaluate borrowed TabRefTypes and TabRefCodes");
    assert_eq!(samples.len(), 8);
    assert_eq!(
        inflated.source(0).retained_storage_metadata_bytes(),
        Some(inflated_storage_bytes),
        "bounded traversal must not populate an uncharged retained table cache"
    );
}

#[test]
fn variable_pointing_string_references_are_read_without_retaining_table_state() {
    let directory = tempfile::tempdir().expect("temporary variable-reference fixture");
    let path = directory.path().join("variable-reference.ms");
    generate_fixture(&path);
    let reference_column = format!("DIRECTION_REF_{}", "X".repeat(8_192));
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        let table = pointing.table_mut();
        table
            .add_column(
                ColumnSchema::scalar(&reference_column, PrimitiveType::String),
                Some(Value::Scalar(ScalarValue::String("J2000".to_string()))),
            )
            .expect("add string reference column");
        let mut keywords = table
            .column_keywords("DIRECTION")
            .cloned()
            .expect("DIRECTION keywords");
        keywords.upsert(
            "MEASINFO",
            Value::Record(RecordValue::new(vec![
                RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String("direction".to_string())),
                ),
                RecordField::new(
                    "VarRefCol",
                    Value::Scalar(ScalarValue::String(reference_column.clone())),
                ),
            ])),
        );
        table.set_column_keywords("DIRECTION", keywords);
    }
    measurement_set
        .save()
        .expect("save string variable-reference POINTING metadata");

    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::Nearest,
                PointingExtrapolation::HoldNearest,
                MissingPointingPolicy::Reject,
            )),
        ),
    );
    let source = &problem.observation().sources()[0];
    let two_row_budget = content_budget_for_rows(&problem, source, 2, 1);
    let two_rows = open_observation(&problem, source, two_row_budget)
        .expect("open variable-string reference observation");
    let retained_storage_bytes = two_rows
        .source(0)
        .retained_storage_metadata_bytes()
        .expect("project retained string-reference storage");
    let (two_rows, samples) =
        stream(&problem, two_rows).expect("evaluate bounded variable-string references");
    assert_eq!(samples.len(), 8);
    assert_eq!(
        two_rows.source(0).retained_storage_metadata_bytes(),
        Some(retained_storage_bytes),
        "variable reference reads must not create hidden retained table state"
    );
}

#[test]
fn retained_predicate_catalog_is_charged_before_construction() {
    let directory = tempfile::tempdir().expect("temporary predicate-budget fixture");
    let path = directory.path().join("predicate-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.observation().sources()[0];
    let admitted = BoundObservationSource::open(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind source with predicate allowance");
    let predicate_bytes =
        super::row_selection::CompiledRowPredicate::shared_retained_heap_bytes(source)
            .expect("finite predicate projection");
    let old_unaccounted_budget = admitted
        .content_plan()
        .maximum_resident_bytes()
        .checked_sub(predicate_bytes)
        .expect("predicate contributes retained bytes");

    assert!(matches!(
        BoundObservationSource::open(
            &problem,
            source,
            SelectedObservationContentBudget::new(old_unaccounted_budget, 1, 4),
        ),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));
}

#[test]
fn frontend_row_projection_uses_the_canonical_bounded_observation_evaluator() {
    assert_eq!(SelectedObservationRow::STORAGE_BYTES_PER_ROW, 65);
    let directory = tempfile::tempdir().expect("temporary row-projection fixture");
    let path = directory.path().join("row-projection.ms");
    generate_fixture(&path);
    let measurement_set = MeasurementSet::open(&path).expect("open row-projection fixture");
    let selection = measurement_set
        .selected_observation_row_selection(&[0], None, None, None)
        .expect("resolve frontend selectors to the native row contract");
    let mut rows = Vec::new();

    measurement_set
        .visit_selected_observation_rows(
            &selection,
            MsSelectionIoBudget {
                available_bytes: 2 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                maximum_live_blocks: 2,
                requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            },
            |row| rows.push(row),
        )
        .expect("visit canonical selected rows");
    let expected_time_centroids = (0..2)
        .map(|row| {
            match measurement_set
                .main_table()
                .cell_accessor(row, "TIME_CENTROID")
                .and_then(|cell| cell.scalar())
                .expect("stored MAIN.TIME_CENTROID")
            {
                ScalarValue::Float64(value) => *value,
                other => panic!(
                    "MAIN.TIME_CENTROID must be Float64, found {:?}",
                    other.primitive_type()
                ),
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| (row.physical_row(), row.data_description_id()))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)]
    );
    assert!(rows.iter().all(|row| !row.flag_row()));
    assert!(rows.iter().all(|row| row.observation_id() == 0));
    assert_eq!(
        rows.iter()
            .map(|row| row.time_centroid_mjd_seconds())
            .collect::<Vec<_>>(),
        expected_time_centroids
    );
}

#[test]
fn selected_observation_residency_is_cardinality_independent() {
    let directory = tempfile::tempdir().expect("temporary residency fixtures");
    let small_path = directory.path().join("small.ms");
    let large_path = directory.path().join("large.ms");
    generate_fixture_with_rows(&small_path, 4);
    generate_fixture_with_rows(&large_path, 64);

    let small_problem = compiled_problem(&small_path, 4);
    let small_source = &small_problem.observation().sources()[0];
    let synchronous_budget = content_budget_for_rows(&small_problem, small_source, 1, 1);
    let synchronous =
        BoundObservationSource::open(&small_problem, small_source, synchronous_budget)
            .expect("bind synchronous selected observation");
    assert_eq!(synchronous.content_plan().rows_per_block(), 1);
    assert!(
        synchronous.content_plan().maximum_resident_bytes() <= synchronous_budget.available_bytes()
    );
    assert_eq!(
        content_budget_for_rows(&small_problem, small_source, 1, 2).available_bytes(),
        synchronous_budget.available_bytes(),
        "the stream holds one block whatever the live-block allowance"
    );

    let large_problem = compiled_problem(&large_path, 64);
    let large_source = &large_problem.observation().sources()[0];
    let large_budget = content_budget_for_rows(&large_problem, large_source, 1, 1);
    let large = BoundObservationSource::open(&large_problem, large_source, large_budget)
        .expect("bind large selected observation");
    assert_eq!(
        large.content_plan().bytes_per_row(),
        synchronous.content_plan().bytes_per_row(),
        "MAIN and POINTING table cardinality must not enter simultaneous residency"
    );
    assert_eq!(
        large.content_plan().bytes_per_block(),
        synchronous.content_plan().bytes_per_block()
    );
    assert_eq!(
        large_source
            .selection()
            .rows()
            .retained_manifest_bytes()
            .expect("large retained row manifest byte count"),
        small_source
            .selection()
            .rows()
            .retained_manifest_bytes()
            .expect("small retained row manifest byte count"),
        "selected-row cardinality is encoded without retaining a row-sized corpus"
    );
    assert_eq!(
        large.content_plan().retained_bytes(),
        synchronous.content_plan().retained_bytes(),
        "source cardinality does not increase retained selection state"
    );
    assert_eq!(
        large.content_plan().initialization_scratch_bytes(),
        synchronous.content_plan().initialization_scratch_bytes(),
        "shared selected-row allocations are retained once, not recharged as validation scratch"
    );
    assert_eq!(
        large_budget.available_bytes(),
        synchronous_budget.available_bytes()
    );
    assert!(large.content_plan().maximum_resident_bytes() <= large_budget.available_bytes());
    let large_with_small_budget =
        BoundObservationSource::open(&large_problem, large_source, synchronous_budget)
            .expect("compact selection admits the larger source under the same one-row budget");
    assert_eq!(
        large_with_small_budget.content_plan().rows_per_block(),
        synchronous.content_plan().rows_per_block()
    );
    assert_eq!(stream_rows(&large_problem, 1).len(), 64 * 2 * 2);
}

#[test]
fn numeric_blocks_skip_unused_parallactic_angles_and_reject_stale_geometry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("numeric-stream.ms");
    generate_fixture_with_rows(&path, 4);
    let problem = compiled_problem(&path, 4);
    let source = &problem.observation().sources()[0];
    let observation = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .unwrap();
    let mut source = observation.into_block_stream(&problem);
    let mut storage = source.create_storage();
    let mut geometry = super::SelectedObservationNumericGeometry::new(1, 2).unwrap();
    let mut blocks = 0;
    while source.fill_next(&mut storage).unwrap() {
        storage
            .project_numeric_geometry(&problem, &mut geometry)
            .unwrap();
        for row in 0..geometry.row_count() {
            assert!(
                storage
                    .numeric_row(&geometry, row)
                    .unwrap()
                    .row
                    .coordinates
                    .parallactic_angles_rad
                    .is_none()
            );
        }
        assert_eq!(storage.parallactic_angle_cache_entries(), 0);
        blocks += 1;
    }
    assert_eq!(blocks, 4);
    assert!(
        storage.numeric_row(&geometry, 0).is_err(),
        "an exhausted block no longer matches the geometry of its last fill"
    );
    let observation = source.complete().unwrap();

    let mut source = observation.into_block_stream(&problem);
    let mut first = source.create_storage();
    assert!(source.fill_next(&mut first).unwrap());
    first
        .project_numeric_geometry(&problem, &mut geometry)
        .unwrap();
    assert!(source.fill_next(&mut first).unwrap());
    assert!(
        first.numeric_row(&geometry, 0).is_err(),
        "geometry projected from one fill is not read against the next"
    );
}

#[test]
fn numeric_geometry_coarse_chunks_match_serial_for_uneven_rows_and_window() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("numeric-chunks.ms");
    generate_fixture_with_rows(&path, 17);
    let problem = compiled_problem(&path, 17);
    let source = &problem.observation().sources()[0];
    let selected = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 17, 1),
    )
    .unwrap();
    let mut source = selected.into_block_stream(&problem);
    let mut storage = source.create_storage();
    assert!(source.fill_next(&mut storage).unwrap());
    let mut serial = super::SelectedObservationNumericGeometry::new(17, 3).unwrap();
    storage
        .project_numeric_geometry(&problem, &mut serial)
        .unwrap();
    for chunk_rows in [1, 3, 5, 6] {
        let mut parallel = super::SelectedObservationNumericGeometry::new(17, 3).unwrap();
        storage
            .project_numeric_geometry_with(&problem, &mut parallel, chunk_rows, |chunks| {
                std::thread::scope(|scope| {
                    let jobs = chunks
                        .iter_mut()
                        .map(|chunk| scope.spawn(move || chunk.project()))
                        .collect::<Vec<_>>();
                    for job in jobs {
                        job.join().unwrap()?;
                    }
                    Ok(())
                })
            })
            .unwrap();
        assert_eq!(parallel.row_count(), serial.row_count());
        assert_eq!(parallel.frequencies_hz(), serial.frequencies_hz());
        for row in 0..serial.row_count() {
            assert_eq!(
                storage.numeric_row(&parallel, row).unwrap().row,
                storage.numeric_row(&serial, row).unwrap().row
            );
        }
    }
    let mut incomplete = super::SelectedObservationNumericGeometry::new(17, 3).unwrap();
    assert!(
        storage
            .project_numeric_geometry_with(&problem, &mut incomplete, 3, |chunks| {
                chunks[0].project()?;
                Err(crate::BoundObservationSourceError::StoredSampleShapeMismatch)
            })
            .is_err()
    );
    assert!(storage.numeric_row(&incomplete, 0).is_err());

    while source.fill_next(&mut storage).unwrap() {}
    let retained = source.complete().unwrap();
    // The second selected channel (1.402 GHz) and its straddling partner.
    let mut window_source = retained.into_windowed_block_stream(&problem, [1.4015e9, 1.4025e9]);
    let mut window_storage = window_source.create_storage();
    assert!(window_source.fill_next(&mut window_storage).unwrap());
    let mut window_serial = super::SelectedObservationNumericGeometry::new(17, 2).unwrap();
    window_storage
        .project_numeric_geometry(&problem, &mut window_serial)
        .unwrap();
    let mut window_parallel = super::SelectedObservationNumericGeometry::new(17, 2).unwrap();
    window_storage
        .project_numeric_geometry_with(&problem, &mut window_parallel, 3, |chunks| {
            std::thread::scope(|scope| {
                let jobs = chunks
                    .iter_mut()
                    .map(|chunk| scope.spawn(move || chunk.project()))
                    .collect::<Vec<_>>();
                for job in jobs {
                    job.join().unwrap()?;
                }
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(
        window_serial.frequencies_hz(),
        window_parallel.frequencies_hz()
    );
    for row in 0..window_serial.row_count() {
        assert_eq!(
            window_storage
                .numeric_row(&window_parallel, row)
                .unwrap()
                .row,
            window_storage.numeric_row(&window_serial, row).unwrap().row
        );
    }
}

#[test]
fn refillable_block_stream_reads_whole_numeric_blocks_and_returns_the_owner() {
    let directory = tempfile::tempdir().expect("temporary block-stream fixture");
    let path = directory.path().join("block-stream.ms");
    generate_fixture_with_rows(&path, 4);
    let problem = compiled_problem(&path, 4);
    let source = &problem.observation().sources()[0];
    let observation = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind block traversal");
    let mut source = observation.into_block_stream(&problem);
    let mut storage = source.create_storage();
    let mut blocks = 0;
    while source
        .fill_next(&mut storage)
        .expect("fill canonical block")
    {
        blocks += 1;
        let numeric = storage.numeric_columns().expect("filled numeric block");
        let samples =
            numeric.physical_rows.len() * numeric.channel_range.count * numeric.correlation_count;
        assert_eq!(numeric.flags.len(), samples);
        assert_eq!(numeric.row_flags.len(), numeric.physical_rows.len());
        match numeric.visibility {
            crate::SelectedNumericVisibility::Float32(values) => {
                assert_eq!(values.len(), samples);
            }
            crate::SelectedNumericVisibility::Complex32(values) => {
                assert_eq!(values.len(), samples);
            }
        }
        match numeric.weights {
            crate::SelectedNumericWeights::PerRow(values) => {
                assert_eq!(
                    values.len(),
                    numeric.physical_rows.len() * numeric.correlation_count
                );
            }
            crate::SelectedNumericWeights::PerChannel(values) => {
                assert_eq!(values.len(), samples);
            }
        }
    }
    assert_eq!(blocks, 4);
    assert!(
        !source
            .fill_next(&mut storage)
            .expect("poll after exhaustion")
    );
    let observation = source.complete().expect("return the owner");

    let (_, replayed) = stream(&problem, observation).expect("stream the returned owner");
    assert_eq!(replayed.len(), 16);
    assert_eq!(replayed, stream_rows(&problem, 3));
}

#[test]
#[cfg(unix)]
fn windowed_block_streams_read_only_the_reached_channels() {
    let directory = tempfile::tempdir().expect("temporary window fixture");
    let path = directory.path().join("window.ms");
    generate_fixture_with_rows(&path, 6);
    let (problem, access) = owner_problem_and_access(owner_resolution_request_with_channels(
        &path,
        6,
        vec![0, 1, 2],
    ));
    let observation = access.open(&problem).expect("bind source");
    let (observation, full) = stream(&problem, observation).expect("stream every channel");
    assert_eq!(full.len(), 6 * 3 * 2);

    let window = |observation: BoundSelectedObservation, bounds| {
        let mut source = observation.into_windowed_block_stream(&problem, bounds);
        let mut storage = source.create_storage();
        let mut geometry =
            super::SelectedObservationNumericGeometry::new(source.maximum_rows_per_block(), 3)
                .unwrap();
        let mut channels = Vec::new();
        while source.fill_next(&mut storage).expect("fill window block") {
            let columns = storage.numeric_columns().unwrap();
            channels.push((columns.channel_range.start, columns.channel_range.count));
            storage
                .project_numeric_geometry(&problem, &mut geometry)
                .unwrap();
            assert_eq!(
                geometry.channels().len(),
                columns.channel_range.count,
                "the geometry covers exactly the channels read"
            );
        }
        (source.complete().expect("exhaust the window"), channels)
    };

    // 1.4001-1.4009 GHz lies between the first two channels: the straddling
    // pair is read, the third channel is not.
    let (observation, narrowed) = window(observation, [1.4001e9, 1.4009e9]);
    assert!(!narrowed.is_empty());
    assert!(narrowed.iter().all(|&range| range == (0, 2)));

    // A window that reaches no channel reads no payload at all.
    let (_, disjoint) = window(observation, [1.5e9, 1.6e9]);
    assert!(disjoint.is_empty());
}

#[test]
fn retained_selected_observation_owns_canonical_multi_source_order() {
    let directory = tempfile::tempdir().expect("temporary multi-source fixture");
    let first_path = directory.path().join("first.ms");
    let second_path = directory.path().join("second.ms");
    generate_fixture(&first_path);
    generate_fixture(&second_path);
    let problem = compiled_problem_with_sources(&[(&first_path, 2), (&second_path, 2)]);
    let sources = problem.observation().sources();
    let one_row_measures = test_measures();
    let one_row_bindings: Vec<_> = sources
        .iter()
        .enumerate()
        .map(|(source_index, source)| {
            ObservationSourceBinding::new(
                source_ordinal(source),
                content_budget_for_rows_with_shared_bytes(
                    &problem,
                    source,
                    if source_index == 0 {
                        selected_observation_shared_bytes(&one_row_measures)
                    } else {
                        super::content_plan::SelectedObservationSharedBytes::NONE
                    },
                    1,
                    1,
                ),
            )
        })
        .collect();
    let two_row_measures = test_measures();
    let mut two_row_bindings: Vec<_> = sources
        .iter()
        .enumerate()
        .map(|(source_index, source)| {
            ObservationSourceBinding::new(
                source_ordinal(source),
                content_budget_for_rows_with_shared_bytes(
                    &problem,
                    source,
                    if source_index == 0 {
                        selected_observation_shared_bytes(&two_row_measures)
                    } else {
                        super::content_plan::SelectedObservationSharedBytes::NONE
                    },
                    2,
                    1,
                ),
            )
        })
        .collect();
    two_row_bindings.reverse();
    let one_row = BoundSelectedObservation::open(&problem, one_row_measures, one_row_bindings)
        .expect("bind canonical multi-source observation");
    let two_rows = BoundSelectedObservation::open(&problem, two_row_measures, two_row_bindings)
        .expect("bind reordered source budgets by snapshot position");

    let shared_measures_bytes = test_measures().retained_bytes();
    for (source_index, source) in sources.iter().enumerate() {
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
            .expect("open multi-source fixture for uncharged comparison");
        let uncharged = super::content_plan::selected_content_plan(
            &measurement_set,
            &problem,
            source,
            super::content_plan::SelectedObservationSharedBytes::NONE,
            content_budget_for_rows(&problem, source, 1, 1),
        )
        .expect("plan source without the shared Measures owner");
        let bound = one_row
            .source_content_plan(source_index)
            .expect("bound canonical source plan");
        assert_eq!(
            bound.retained_bytes() - uncharged.retained_bytes(),
            if source_index == 0 {
                shared_measures_bytes
            } else {
                0
            },
            "the provider must be charged only to the first canonical source"
        );
    }

    let (one_row, one_row_samples) =
        stream(&problem, one_row).expect("stream canonical multi-source blocks");
    let (_, two_row_samples) =
        stream(&problem, two_rows).expect("stream repartitioned multi-source blocks");

    assert_eq!(one_row_samples.len(), 16);
    assert_eq!(
        one_row_samples, two_row_samples,
        "physical source and row blocking are absent from the selected samples"
    );
    let (_, repeated) = stream(&problem, one_row).expect("repeat the retained traversal");
    assert_eq!(repeated, one_row_samples);
}

#[test]
fn retained_selected_samples_evaluate_fixed_pointing_centres() {
    let directory = tempfile::tempdir().expect("temporary fixed-centre fixture");
    let path = directory.path().join("fixed.ms");
    generate_fixture(&path);
    let phase = SkyDirection::new(DirectionFrame::J2000, 0.7, -0.2);
    let delay = SkyDirection::new(DirectionFrame::J2000, 0.8, -0.25);
    let pointing = SkyDirection::new(DirectionFrame::J2000, 0.9, -0.3);
    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Fixed(phase),
            DelayCentreLaw::Fixed(delay),
            PointingCentreLaw::Fixed(pointing),
        ),
    );
    let samples = stream_rows(&problem, 2);

    assert_eq!(samples.len(), 8);
    for sample in &samples {
        let coordinates = &sample.row.coordinates;
        assert_eq!(coordinates.pointing_directions.antenna1, pointing);
        assert_eq!(coordinates.pointing_directions.antenna2, pointing);
    }
}

#[test]
fn retained_mosaic_projection_uses_girar_uvw_with_adjoint_phase_sign() {
    let directory = tempfile::tempdir().expect("temporary mosaic-projection fixture");
    let path = directory.path().join("mosaic-projection.ms");
    generate_fixture(&path);
    let phase = SkyDirection::new(DirectionFrame::J2000, 0.7, -0.2);
    let centres = CentreLaws::new(
        PhaseCentreLaw::Fixed(phase),
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::PhaseTrackingCentre,
    );
    let problem = compiled_problem_with_geometry(
        &path,
        2,
        geometry_with_centres_and_uvw(centres, UvwCoordinateLaw::MosaicPhaseTrackingCentre),
    );
    let source = &problem.observation().sources()[0];
    let bound = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind mosaic-projection source");
    let (bound, samples) = stream(&problem, bound).expect("evaluate mosaic-projection samples");
    let sample = &samples[0];
    let target_direction = problem.geometry().domains()[0].model_phase_centre();
    let target = MDirection::from_angles(
        target_direction.longitude_rad(),
        target_direction.latitude_rad(),
        DirectionRef::J2000,
    );
    let (expected_uvw_m, casa_dphase_m) = bound
        .source(0)
        .geometry_engine()
        .reproject_raw_uvw_for_mosaic_to_direction(
            sample.row.coordinates.raw_uvw_m,
            usize::try_from(sample.row.metadata.field_id).expect("field id"),
            &target,
        )
        .expect("CASA girarUVW projection");
    let projection = sample
        .row
        .domain_projections
        .iter()
        .next()
        .expect("primary projection")
        .model();

    assert_eq!(projection.transformed_uvw_m(), expected_uvw_m);
    assert_eq!(projection.phase_shift_m(), -casa_dphase_m);
}

#[test]
fn retained_selected_samples_evaluate_moving_centres_at_each_row_time() {
    let directory = tempfile::tempdir().expect("temporary moving-centre fixture");
    let path = directory.path().join("moving.ms");
    generate_fixture(&path);
    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Ephemeris("Mars".to_string()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
    );
    let source = &problem.observation().sources()[0];
    let budget = SelectedObservationContentBudget::new(64 << 20, 1, 4);
    let ephemeris =
        crate::SelectedObservationEphemeris::named("Mars", budget.reference_data_budget())
            .expect("admit the Mars ephemeris");
    let observation = BoundSelectedObservation::open(
        &problem,
        test_measures(),
        vec![
            ObservationSourceBinding::new(source_ordinal(source), budget)
                .with_ephemeris(Some(ephemeris)),
        ],
    )
    .expect("bind moving-centre source");
    let (_, samples) = stream(&problem, observation).expect("evaluate moving-centre samples");

    let first = &samples[0].row;
    let second_row = &samples
        .iter()
        .find(|sample| sample.row.physical_row == 1)
        .expect("second selected row")
        .row;
    // Pointing follows the phase-tracking centre, so the pointing direction
    // is the row's evaluated ephemeris direction.
    assert_ne!(
        first.coordinates.pointing_directions.antenna1,
        second_row.coordinates.pointing_directions.antenna1
    );
    assert_ne!(
        first.domain_projections.iter().next().unwrap().model(),
        second_row.domain_projections.iter().next().unwrap().model(),
        "moving rows must not retain one fixed primary-domain projection",
    );
    for sample in &samples {
        let primary = sample
            .row
            .domain_projections
            .iter()
            .next()
            .expect("primary image-domain projection")
            .model();
        assert_ne!(primary.phase_shift_m(), 0.0);
    }
}

#[test]
fn retained_selected_samples_preserve_bounded_per_antenna_pointing_directions() {
    let directory = tempfile::tempdir().expect("temporary POINTING fixture");
    let path = directory.path().join("pointing.ms");
    generate_fixture(&path);
    let antenna1_pointing = [0.91, -0.31];
    let antenna2_pointing = [0.93, -0.29];
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        pointing
            .set_array(0, "DIRECTION", direction_array(antenna1_pointing))
            .expect("set antenna-0 POINTING direction");
        pointing
            .set_array(1, "DIRECTION", direction_array(antenna2_pointing))
            .expect("set antenna-1 POINTING direction");
    }
    measurement_set.save().expect("save POINTING fixture");

    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::Nearest,
                PointingExtrapolation::HoldNearest,
                MissingPointingPolicy::Reject,
            )),
        ),
    );
    let source = &problem.observation().sources()[0];
    let one_row = BoundObservationSource::open(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind one-row POINTING stream");
    let two_rows = BoundObservationSource::open(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind two-row POINTING stream");
    assert_eq!(one_row.content_plan().rows_per_block(), 1);
    assert_eq!(two_rows.content_plan().rows_per_block(), 2);
    assert!(
        one_row.content_plan().preparation_bytes_per_block()
            > one_row.content_plan().bytes_per_block()
    );
    let one_row_samples = stream_rows(&problem, 1);
    let two_row_samples = stream_rows(&problem, 2);

    assert_eq!(one_row_samples, two_row_samples);
    for sample in &one_row_samples {
        assert_eq!(
            sample.row.coordinates.pointing_directions.antenna1,
            SkyDirection::new(
                DirectionFrame::J2000,
                antenna1_pointing[0],
                antenna1_pointing[1],
            )
        );
        assert_eq!(
            sample.row.coordinates.pointing_directions.antenna2,
            SkyDirection::new(
                DirectionFrame::J2000,
                antenna2_pointing[0],
                antenna2_pointing[1],
            )
        );
    }
}

#[test]
fn observation_pointing_missing_policy_is_explicit_and_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary missing-POINTING fixture");
    let path = directory.path().join("missing-pointing.ms");
    generate_fixture(&path);
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        pointing
            .set_i32(0, "ANTENNA_ID", 98)
            .expect("detach first POINTING antenna");
        pointing
            .set_i32(1, "ANTENNA_ID", 99)
            .expect("detach second POINTING antenna");
    }
    measurement_set.save().expect("save POINTING fixture");

    let fallback_problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            observation_pointing(MissingPointingPolicy::UsePhaseTrackingCentre),
        ),
    );
    // The phase-tracking centre is the observation direction, the same
    // direction the field-centre pointing law evaluates.
    let field_centre_problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::FieldCentre,
        ),
    );
    let field_centre = stream_rows(&field_centre_problem, 2);
    let fallback = stream_rows(&fallback_problem, 2);
    assert_eq!(fallback.len(), field_centre.len());
    for (sample, field_centre) in fallback.iter().zip(&field_centre) {
        assert_eq!(
            sample.row.coordinates.pointing_directions,
            field_centre.row.coordinates.pointing_directions
        );
    }

    let rejecting_problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            observation_pointing(MissingPointingPolicy::Reject),
        ),
    );
    let source = &rejecting_problem.observation().sources()[0];
    let observation = open_observation(
        &rejecting_problem,
        source,
        content_budget_for_rows(&rejecting_problem, source, 2, 1),
    )
    .expect("bind rejecting POINTING source");
    let error = stream(&rejecting_problem, observation)
        .err()
        .expect("missing required POINTING must fail closed");
    assert!(matches!(
        error,
        super::BoundObservationSourceError::MissingPointingDirection { .. }
    ));
}

#[test]
fn observation_pointing_interpolates_each_antenna_on_the_shortest_arc() {
    let directory = tempfile::tempdir().expect("temporary interpolated-POINTING fixture");
    let path = directory.path().join("interpolated-pointing.ms");
    generate_fixture(&path);
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    let first_time = match measurement_set
        .main_table()
        .cell_accessor(0, "TIME")
        .and_then(|cell| cell.scalar())
        .expect("MAIN.TIME row 0")
    {
        ScalarValue::Float64(value) => *value,
        other => panic!(
            "MAIN.TIME must be Float64, found {:?}",
            other.primitive_type()
        ),
    };
    let second_time = match measurement_set
        .main_table()
        .cell_accessor(1, "TIME")
        .and_then(|cell| cell.scalar())
        .expect("MAIN.TIME row 1")
    {
        ScalarValue::Float64(value) => *value,
        other => panic!(
            "MAIN.TIME must be Float64, found {:?}",
            other.primitive_type()
        ),
    };
    let before_time = first_time - 0.5;
    let after_time = second_time + 0.5;
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        for (row, antenna, direction) in [(0, 0, [0.0, 0.0]), (1, 1, [0.4, 0.0])] {
            pointing
                .set_i32(row, "ANTENNA_ID", antenna)
                .expect("set POINTING antenna");
            pointing
                .set_f64(row, "TIME", before_time)
                .expect("set POINTING time");
            pointing
                .set_f64(row, "TIME_ORIGIN", before_time)
                .expect("set POINTING origin");
            pointing
                .set_f64(row, "INTERVAL", -1.0)
                .expect("set POINTING timestamp semantics");
            pointing
                .set_array(row, "DIRECTION", direction_array(direction))
                .expect("set POINTING direction");
        }
        pointing
            .table_mut()
            .add_row(pointing_row(0, after_time, [0.2, 0.0]))
            .expect("append antenna-0 bracket");
        pointing
            .table_mut()
            .add_row(pointing_row(1, after_time, [0.6, 0.0]))
            .expect("append antenna-1 bracket");
    }
    measurement_set.save().expect("save POINTING fixture");

    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::GreatCircleShortestArc,
                PointingExtrapolation::Reject,
                MissingPointingPolicy::Reject,
            )),
        ),
    );
    let samples = stream_rows(&problem, 2);

    let longitudes = |sample: &Sample| {
        let pointing = sample.row.coordinates.pointing_directions;
        [
            pointing.antenna1.longitude_rad(),
            pointing.antenna2.longitude_rad(),
        ]
    };
    for sample in &samples[..4] {
        let [antenna1, antenna2] = longitudes(sample);
        assert!((antenna1 - 0.05).abs() < 1.0e-12);
        assert!((antenna2 - 0.45).abs() < 1.0e-12);
    }
    for sample in &samples[4..] {
        let [antenna1, antenna2] = longitudes(sample);
        assert!((antenna1 - 0.15).abs() < 1.0e-12);
        assert!((antenna2 - 0.55).abs() < 1.0e-12);
    }
}

#[test]
fn selected_rows_pair_owner_derived_heterogeneous_apertures_with_antenna_pointings() {
    let directory = tempfile::tempdir().expect("temporary heterogeneous response fixture");
    let path = directory.path().join("heterogeneous-response.ms");
    generate_fixture(&path);
    let mut measurement_set = MeasurementSet::open(&path).expect("open response fixture");
    {
        let mut observation = measurement_set
            .observation_mut()
            .expect("OBSERVATION subtable");
        observation
            .set_string(0, "TELESCOPE_NAME", "ALMA")
            .expect("set telescope");
    }
    {
        let mut antenna = measurement_set.antenna_mut().expect("ANTENNA subtable");
        antenna
            .put_dish_diameter(0, 12.0)
            .expect("set ALMA aperture");
        antenna.put_dish_diameter(1, 7.0).expect("set ACA aperture");
    }
    measurement_set.save().expect("save response fixture");

    let centres = CentreLaws::new(
        PhaseCentreLaw::Observation,
        DelayCentreLaw::PhaseTrackingCentre,
        observation_pointing(MissingPointingPolicy::Reject),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source_input(&path, 2)]))
        .expect("compile heterogeneous observation");
    let science = ScientificContract::new(
        SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products()),
    )
    .with_instrument_model(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    let problem = compile(ProblemInput::new(
        specification_with_science(
            science,
            ReconstructionBasis::Constant,
            vec![PolarizationCoordinate::StokesI],
        ),
        geometry_with_centres(centres),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile heterogeneous response problem");
    let samples = stream_rows(&problem, 2);

    assert!(!samples.is_empty());
    for sample in samples {
        let metadata = sample.row.metadata;
        let responses = metadata
            .antenna_responses
            .expect("direction-dependent rows carry response classes");
        assert_eq!(responses.antenna1, AntennaResponseClass::CasaAlma12m);
        assert_eq!(responses.antenna2, AntennaResponseClass::CasaAca7m);
        assert_eq!(responses.family_envelope, AntennaResponseClass::CasaAlma12m);
        assert_eq!(metadata.antenna1, 0);
        assert_eq!(metadata.antenna2, 1);
    }
}

#[test]
fn multi_spw_selection_is_block_invariant_across_prediction_and_residual_replays() {
    let directory = tempfile::tempdir().expect("temporary multi-SPW fixture");
    let path = directory.path().join("multi-spw.ms");
    generate_fixture(&path);
    extend_fixture_with_second_spw(&path);
    let snapshot =
        compile_observation(ObservationSnapshotInput::new(vec![multi_spw_source_input(
            &path,
        )]))
        .expect("compile multi-SPW observation");
    let problem = compile(ProblemInput::new(
        specification(),
        geometry(),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile multi-SPW problem");
    let source = &problem.observation().sources()[0];
    let one_row = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind one-row multi-SPW stream");
    let three_rows = open_observation(
        &problem,
        source,
        content_budget_for_rows(&problem, source, 3, 1),
    )
    .expect("bind three-row multi-SPW stream");
    assert_eq!(one_row.source_content_plan(0).unwrap().rows_per_block(), 1);
    assert_eq!(
        three_rows.source_content_plan(0).unwrap().rows_per_block(),
        3
    );

    let (one_row, prediction) = stream(&problem, one_row).expect("read prediction replay");
    let (_, residual) = stream(&problem, one_row).expect("read residual replay");
    let (_, repartitioned) = stream(&problem, three_rows).expect("read repartitioned replay");

    assert_eq!(prediction, residual);
    assert_eq!(prediction, repartitioned);
    assert_eq!(prediction.len(), 16);
    assert_eq!(
        prediction
            .iter()
            .map(|sample| {
                (
                    sample.row.physical_row,
                    sample.row.data_description_id,
                    sample.row.spectral_window_id,
                    sample.channel.channel_index,
                    sample.correlation.correlation_index(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (0, 0, 0, 0, 0),
            (0, 0, 0, 0, 1),
            (0, 0, 0, 2, 0),
            (0, 0, 0, 2, 1),
            (1, 0, 0, 0, 0),
            (1, 0, 0, 0, 1),
            (1, 0, 0, 2, 0),
            (1, 0, 0, 2, 1),
            (2, 1, 1, 1, 0),
            (2, 1, 1, 1, 1),
            (2, 1, 1, 2, 0),
            (2, 1, 1, 2, 1),
            (3, 1, 1, 1, 0),
            (3, 1, 1, 1, 1),
            (3, 1, 1, 2, 0),
            (3, 1, 1, 2, 1),
        ]
    );
    assert_eq!(prediction[8].channel.frequency_centre_hz, 1.501e9);
    assert_eq!(prediction[12].channel.frequency_centre_hz, 1.501e9);
}

/// A selected visibility in its MeasurementSet storage representation.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Visibility {
    Float32(f32),
    Complex32([f32; 2]),
}

/// One selected sample as the imaging source reads it: a numeric row of the
/// block stream, one of its channels and one of its correlations.
#[derive(Debug, Clone, PartialEq)]
struct Sample {
    row: SelectedObservationRunRow,
    channel: SelectedObservationRunChannel,
    correlation: CorrelationProduct,
    /// The channel centre in the compiled output frame.
    frequency_hz: f64,
    visibility: Visibility,
    flag: bool,
    weight: f32,
}

/// Open the one source of `problem` under `budget`, with the POINTING query
/// domain the resolver derives when the problem reads observed pointings.
fn open_observation(
    problem: &casa_imaging_model::CompiledProblem,
    source: &ObservationSource,
    budget: SelectedObservationContentBudget,
) -> Result<BoundSelectedObservation, super::BoundSelectedObservationError> {
    let mut binding = ObservationSourceBinding::new(source_ordinal(source), budget);
    if matches!(
        problem.geometry().centres().pointing(),
        PointingCentreLaw::Observation(_)
    ) {
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
            .expect("open fixture for its POINTING query domain");
        binding = binding.with_pointing_query_domain(
            crate::observation_owner::test_pointing_query_domain(
                &measurement_set,
                source.selection(),
                budget,
            )
            .expect("derive the POINTING query domain"),
        );
    }
    BoundSelectedObservation::open(problem, test_measures(), vec![binding])
}

/// Every selected sample in stream order, read through the production block
/// stream exactly as the imaging source reads it. Returns the observation for
/// the next traversal.
fn stream(
    problem: &casa_imaging_model::CompiledProblem,
    observation: BoundSelectedObservation,
) -> Result<(BoundSelectedObservation, Vec<Sample>), super::BoundObservationSourceError> {
    let mut source = observation.into_block_stream(problem);
    let mut block = source.create_storage();
    let channels = problem
        .observation_transaction()
        .read_set()
        .sources()
        .iter()
        .flat_map(|source| source.selection().spectral_windows())
        .map(|window| window.channel_indices().len())
        .max()
        .unwrap_or(0);
    let mut geometry =
        super::SelectedObservationNumericGeometry::new(source.maximum_rows_per_block(), channels)
            .expect("allocate numeric geometry");
    let mut samples = Vec::new();
    while source.fill_next(&mut block)? {
        block.project_numeric_geometry(problem, &mut geometry)?;
        for row in 0..geometry.row_count() {
            let numeric = block.numeric_row(&geometry, row)?;
            let frequencies = &geometry.frequencies_hz()
                [row * numeric.channels.len()..(row + 1) * numeric.channels.len()];
            for (channel, frequency_hz) in numeric.channels.iter().zip(frequencies) {
                let stored = (channel.channel_index - numeric.first_stored_channel) as usize;
                for correlation in numeric.correlations {
                    let index = stored * numeric.stored_correlations
                        + correlation.correlation_index() as usize;
                    samples.push(Sample {
                        row: numeric.row.clone(),
                        channel: *channel,
                        correlation: *correlation,
                        frequency_hz: *frequency_hz,
                        visibility: match numeric.visibility {
                            crate::SelectedNumericVisibility::Complex32(values) => {
                                Visibility::Complex32([values[index].re, values[index].im])
                            }
                            crate::SelectedNumericVisibility::Float32(values) => {
                                Visibility::Float32(values[index])
                            }
                        },
                        flag: numeric.flags[index],
                        weight: match numeric.weights {
                            crate::SelectedNumericWeights::PerRow(values) => {
                                values[correlation.correlation_index() as usize]
                            }
                            crate::SelectedNumericWeights::PerChannel(values) => values[index],
                        },
                    });
                }
            }
        }
    }
    Ok((source.complete()?, samples))
}

/// Open the one source of `problem` with blocks of `rows` rows and read
/// every selected sample.
fn stream_rows(problem: &casa_imaging_model::CompiledProblem, rows: usize) -> Vec<Sample> {
    let source = &problem.observation().sources()[0];
    let observation = open_observation(
        problem,
        source,
        content_budget_for_rows(problem, source, rows, 1),
    )
    .expect("open the selected observation");
    stream(problem, observation)
        .expect("stream the selected observation")
        .1
}

fn extend_fixture_with_second_spw(path: &std::path::Path) {
    let mut measurement_set = MeasurementSet::open(path).expect("open multi-SPW fixture");
    let mut spectral_window = measurement_set
        .spectral_window()
        .expect("SPECTRAL_WINDOW")
        .table()
        .rows()
        .expect("SPECTRAL_WINDOW rows")[0]
        .clone();
    spectral_window.upsert(
        "NAME",
        Value::Scalar(ScalarValue::String("second-spw".to_string())),
    );
    spectral_window.upsert(
        "CHAN_FREQ",
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![3], vec![1.5e9, 1.501e9, 1.502e9])
                .expect("second-SPW frequency shape"),
        )),
    );
    spectral_window.upsert(
        "REF_FREQUENCY",
        Value::Scalar(ScalarValue::Float64(1.501e9)),
    );
    measurement_set
        .spectral_window_mut()
        .expect("mutable SPECTRAL_WINDOW")
        .table_mut()
        .add_row(spectral_window)
        .expect("append second SPECTRAL_WINDOW");

    let mut data_description = measurement_set
        .data_description()
        .expect("DATA_DESCRIPTION")
        .table()
        .rows()
        .expect("DATA_DESCRIPTION rows")[0]
        .clone();
    data_description.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1)));
    measurement_set
        .data_description_mut()
        .expect("mutable DATA_DESCRIPTION")
        .table_mut()
        .add_row(data_description)
        .expect("append second DATA_DESCRIPTION");

    let original_rows = measurement_set
        .main_table()
        .rows()
        .expect("MAIN rows")
        .to_vec();
    for mut row in original_rows {
        row.upsert("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(1)));
        measurement_set
            .main_table_mut()
            .add_row(row)
            .expect("append second-SPW MAIN row");
    }
    measurement_set.save().expect("save multi-SPW fixture");
}

fn pointing_row(antenna_id: i32, time_mjd_seconds: f64, direction: [f64; 2]) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna_id))),
        RecordField::new("DIRECTION", Value::Array(direction_array(direction))),
        RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(-1.0))),
        RecordField::new("NAME", Value::Scalar(ScalarValue::String(String::new()))),
        RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("TARGET", Value::Array(direction_array(direction))),
        RecordField::new(
            "TIME",
            Value::Scalar(ScalarValue::Float64(time_mjd_seconds)),
        ),
        RecordField::new(
            "TIME_ORIGIN",
            Value::Scalar(ScalarValue::Float64(time_mjd_seconds)),
        ),
        RecordField::new("TRACKING", Value::Scalar(ScalarValue::Bool(true))),
    ])
}

fn observation_pointing(missing: MissingPointingPolicy) -> PointingCentreLaw {
    PointingCentreLaw::Observation(ObservationPointingLaw::new(
        PointingDirectionColumn::Direction,
        PointingDirectionSemantic::AntennaBoresight,
        PointingTimeSampling::VisibilityTime,
        PointingInterpolation::Nearest,
        PointingExtrapolation::HoldNearest,
        missing,
    ))
}

fn direction_array(direction: [f64; 2]) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], direction.to_vec())
            .expect("constant POINTING direction shape"),
    )
}

fn generate_fixture(path: &std::path::Path) {
    generate_fixture_with_rows(path, 2);
}

fn generate_fixture_with_rows(path: &std::path::Path, row_count: usize) {
    generate_fixture_with_channel_count(path, row_count, 3);
}

fn generate_fixture_with_channel_count(
    path: &std::path::Path,
    row_count: usize,
    channel_count: usize,
) {
    let mut antennas = tutorial_vla_a_antennas();
    antennas.truncate(2);
    let mut request = SyntheticObservationRequest::vla_ppdisk("unused.fits", path, antennas);
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = row_count as f64;
    request.integration_seconds = 1.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "three-channel".to_string(),
        start_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        channel_count,
    }];
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    generate_synthetic_observation_ms(&request).expect("generate bounded disk fixture");
    set_fixture_frequency_frame_topocentric(path);
}

fn generate_fixture_with_phase_center(
    path: &std::path::Path,
    row_count: usize,
    phase_center_rad: [f64; 2],
) {
    let mut antennas = tutorial_vla_a_antennas();
    antennas.truncate(6);
    let mut request = SyntheticObservationRequest::vla_ppdisk("unused.fits", path, antennas);
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = row_count as f64;
    request.integration_seconds = 1.0;
    request.phase_center_rad = phase_center_rad;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "three-channel".to_string(),
        start_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        channel_count: 3,
    }];
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    generate_synthetic_observation_ms(&request).expect("generate fixed-centre disk fixture");
    set_fixture_frequency_frame_topocentric(path);
}

fn set_fixture_frequency_frame_topocentric(path: &std::path::Path) {
    let mut spectral = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW")))
        .expect("open generated fixture spectral window");
    spectral
        .row_accessor_mut()
        .set_cell(
            0,
            "MEAS_FREQ_REF",
            Value::Scalar(ScalarValue::Int32(FrequencyRef::TOPO.casacore_code())),
        )
        .expect("set explicit TOPO fixture frame");
    spectral
        .flush()
        .expect("persist explicit TOPO fixture frame");
}

#[cfg(unix)]
fn owner_resolution_request(
    path: &std::path::Path,
    row_count: usize,
) -> SelectedObservationResolutionRequest {
    owner_resolution_request_with_channels(path, row_count, vec![0, 2])
}

#[cfg(unix)]
fn owner_resolution_request_with_channels(
    path: &std::path::Path,
    row_count: usize,
    channel_indices: Vec<u32>,
) -> SelectedObservationResolutionRequest {
    let selected_rows = SelectedRows::from_ordered_main_rows(
        row_count as u64,
        (0..row_count).map(|row| SelectedMainRow::new(row as u64, 0)),
    )
    .expect("owner selected-row manifest");
    SelectedObservationResolutionRequest::new(
        path.display().to_string(),
        fixture_selection_with_channels(
            selected_rows,
            RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
            channel_indices,
        ),
        VisibilityColumn::Data,
        WeightColumn::Weight,
        SelectedObservationContentBudget::new(64 << 20, 1, 4),
        Arc::new(AccountedTestMeasures::with_heap_bytes(0)),
    )
}

#[cfg(unix)]
fn owner_problem_and_access(
    request: SelectedObservationResolutionRequest,
) -> (
    casa_imaging_model::CompiledProblem,
    ResolvedSelectedObservationAccess,
) {
    let (snapshot_input, access) = resolve_selected_observation(request)
        .expect("resolve selected owner")
        .into_parts();
    let snapshot = compile_observation(snapshot_input).expect("compile owner snapshot");
    let problem = compile(ProblemInput::new(
        specification(),
        geometry(),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile owner problem");
    (problem, access)
}

/// The snapshot position a binding names for `source`.
fn source_ordinal(source: &ObservationSource) -> usize {
    source.input_ordinal()
}

fn test_measures() -> super::SelectedObservationMeasures {
    super::measures::test_selected_observation_measures()
        .expect("bind deterministic Measures provider")
}

fn content_budget_for_rows(
    problem: &casa_imaging_model::CompiledProblem,
    source: &ObservationSource,
    target_rows_per_block: usize,
    maximum_live_blocks: usize,
) -> SelectedObservationContentBudget {
    let measures = test_measures();
    content_budget_for_rows_with_shared_bytes(
        problem,
        source,
        selected_observation_shared_bytes(&measures),
        target_rows_per_block,
        maximum_live_blocks,
    )
}

fn selected_observation_shared_bytes(
    measures: &super::SelectedObservationMeasures,
) -> super::content_plan::SelectedObservationSharedBytes {
    super::content_plan::SelectedObservationSharedBytes::new(measures.retained_bytes(), 0)
}

fn content_budget_for_rows_with_shared_bytes(
    problem: &casa_imaging_model::CompiledProblem,
    source: &ObservationSource,
    shared_bytes: super::content_plan::SelectedObservationSharedBytes,
    target_rows_per_block: usize,
    maximum_live_blocks: usize,
) -> SelectedObservationContentBudget {
    assert!(target_rows_per_block > 0);
    let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
        .expect("open retained fixture while deriving its exact content budget");
    let admitted = |available_bytes| {
        let budget = SelectedObservationContentBudget::new(available_bytes, maximum_live_blocks, 4);
        let planned = (|| {
            let domain = if matches!(
                problem.geometry().centres().pointing(),
                PointingCentreLaw::Observation(_)
            ) {
                Some(
                    crate::observation_owner::test_pointing_query_domain(
                        &measurement_set,
                        source.selection(),
                        budget,
                    )
                    .ok()?,
                )
            } else {
                None
            };
            BoundObservationSource::requirements_for_locked_source(
                &measurement_set,
                problem,
                source,
                shared_bytes,
                budget.maximum_pointing_polynomial_terms(),
                domain.as_ref(),
            )
            .ok()?
            .plan(budget)
            .ok()
        })();
        planned.is_some_and(|plan| plan.rows_per_block() >= target_rows_per_block)
    };
    let mut upper = 1_usize;
    while !admitted(upper) {
        upper = upper
            .checked_mul(2)
            .expect("fixture content budget fits usize");
    }
    let mut lower = 0_usize;
    while lower + 1 < upper {
        let middle = lower + (upper - lower) / 2;
        if admitted(middle) {
            upper = middle;
        } else {
            lower = middle;
        }
    }
    SelectedObservationContentBudget::new(upper, maximum_live_blocks, 4)
}

fn compiled_problem(
    path: &std::path::Path,
    row_count: usize,
) -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_sources(&[(path, row_count)])
}

fn compiled_problem_with_polarization(
    path: &std::path::Path,
    row_count: usize,
    coordinates: Vec<PolarizationCoordinate>,
) -> casa_imaging_model::CompiledProblem {
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source_input(
        path, row_count,
    )]))
    .expect("compile polarized selected observation");
    compile(ProblemInput::new(
        specification_with_sampling_basis_and_polarization(
            SpectralSamplingLaw::IDENTITY,
            ReconstructionBasis::Constant,
            coordinates,
        ),
        geometry(),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile polarized selected-observation problem")
}

fn compiled_problem_with_sampling(
    path: &std::path::Path,
    row_count: usize,
    sampling: SpectralSamplingLaw,
    wcs: SpectralWcs,
) -> casa_imaging_model::CompiledProblem {
    let channels = match &wcs {
        SpectralWcs::Linear { channels, .. } => *channels,
        SpectralWcs::Tabular {
            channel_centres_hz, ..
        } => channel_centres_hz.len(),
    };
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source_input(
        path, row_count,
    )]))
    .expect("compile spectral-contribution observation");
    compile(ProblemInput::new(
        specification_with_sampling_and_basis(
            sampling,
            ReconstructionBasis::ChannelLocal { channels },
        ),
        geometry_with_spectral_wcs(wcs),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile spectral-contribution problem")
}

fn compiled_problem_with_transformed_sampling(
    path: &std::path::Path,
    row_count: usize,
    sampling: SpectralSamplingLaw,
    wcs: SpectralWcs,
) -> casa_imaging_model::CompiledProblem {
    let channels = match &wcs {
        SpectralWcs::Linear { channels, .. } => *channels,
        SpectralWcs::Tabular {
            channel_centres_hz, ..
        } => channel_centres_hz.len(),
    };
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source_input(
        path, row_count,
    )]))
    .expect("compile transformed spectral-contribution observation");
    let geometry = geometry_with_spectral_wcs(wcs);
    let transformed = geometry
        .spectral()
        .clone()
        .with_output_frame(FrequencyFrame::Lsrk)
        .with_anchor(SpectralFrameAnchor::Conversion {
            epoch: Epoch::new(59_000.25, TimeScale::Utc),
            direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            observatory_position: ItrfPosition::new(-1_601_188.0, -5_041_977.0, 3_554_875.0),
        });
    compile(ProblemInput::new(
        specification_with_sampling_and_basis(
            sampling,
            ReconstructionBasis::ChannelLocal { channels },
        ),
        geometry.with_spectral(transformed),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile transformed spectral-contribution problem")
}

fn compiled_problem_with_centres(
    path: &std::path::Path,
    row_count: usize,
    centres: CentreLaws,
) -> casa_imaging_model::CompiledProblem {
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source_input(
        path, row_count,
    )]))
    .expect("compile fixed-centre observation");
    compile(ProblemInput::new(
        specification(),
        geometry_with_centres(centres),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile fixed-centre problem")
}

fn compiled_problem_with_geometry(
    path: &std::path::Path,
    row_count: usize,
    geometry: GeometryInput,
) -> casa_imaging_model::CompiledProblem {
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source_input(
        path, row_count,
    )]))
    .expect("compile geometry fixture observation");
    compile(ProblemInput::new(
        specification(),
        geometry,
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile geometry fixture problem")
}

fn compiled_problem_with_sources(
    sources: &[(&std::path::Path, usize)],
) -> casa_imaging_model::CompiledProblem {
    let sources = sources
        .iter()
        .map(|(path, row_count)| source_input(path, *row_count))
        .collect();
    let snapshot = compile_observation(ObservationSnapshotInput::new(sources))
        .expect("compile selected observation");
    compile(ProblemInput::new(
        specification(),
        geometry(),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile selected-observation problem")
}

fn source_input(path: &std::path::Path, row_count: usize) -> ObservationSourceInput {
    let selected_rows = SelectedRows::from_ordered_main_rows(
        row_count as u64,
        (0..row_count).map(|row| SelectedMainRow::new(row as u64, 0)),
    )
    .expect("selected row manifest");
    fixture_source_input(
        path,
        fixture_selection(
            selected_rows,
            RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        ),
    )
}

/// A fixture source reading `DATA` and `WEIGHT`, with no `CORRECTED_DATA`.
fn fixture_source_input(
    path: &std::path::Path,
    selection: ObservationSelection,
) -> ObservationSourceInput {
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(path.display().to_string()),
        selection,
        SelectedColumns::new(
            VisibilityColumn::Data,
            FlagPolicy::FlagOrFlagRow,
            WeightColumn::Weight,
        ),
        false,
    )
}

fn fixture_selection(
    selected_rows: SelectedRows,
    rows_filter: RowSelection,
) -> ObservationSelection {
    fixture_selection_with_channels(selected_rows, rows_filter, vec![0, 2])
}

fn fixture_selection_with_channels(
    selected_rows: SelectedRows,
    rows_filter: RowSelection,
    channel_indices: Vec<u32>,
) -> ObservationSelection {
    ObservationSelection::new(
        selected_rows,
        rows_filter,
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, channel_indices)],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    )
}

fn multi_spw_source_input(path: &std::path::Path) -> ObservationSourceInput {
    let selected_rows = SelectedRows::from_ordered_main_rows(
        4,
        [
            SelectedMainRow::new(0, 0),
            SelectedMainRow::new(1, 0),
            SelectedMainRow::new(2, 1),
            SelectedMainRow::new(3, 1),
        ],
    )
    .expect("multi-SPW selected row manifest");
    let selection = ObservationSelection::new(
        selected_rows,
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![
            DataDescriptionSelection::new(0, 0, 0),
            DataDescriptionSelection::new(1, 1, 0),
        ],
        vec![
            SpectralWindowSelection::new(0, vec![0, 2]),
            SpectralWindowSelection::new(1, vec![1, 2]),
        ],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    );
    fixture_source_input(path, selection)
}

fn specification() -> ProblemSpecification {
    specification_with_sampling(SpectralSamplingLaw::IDENTITY)
}

fn specification_with_sampling(sampling: SpectralSamplingLaw) -> ProblemSpecification {
    specification_with_sampling_and_basis(sampling, ReconstructionBasis::Constant)
}

fn specification_with_sampling_and_basis(
    sampling: SpectralSamplingLaw,
    basis: ReconstructionBasis,
) -> ProblemSpecification {
    specification_with_sampling_basis_and_polarization(
        sampling,
        basis,
        vec![PolarizationCoordinate::StokesI],
    )
}

fn specification_with_sampling_basis_and_polarization(
    sampling: SpectralSamplingLaw,
    basis: ReconstructionBasis,
    coordinates: Vec<PolarizationCoordinate>,
) -> ProblemSpecification {
    specification_with_science(
        ScientificContract::new(
            SpectralContract::new(sampling, SpectralCoupling::Independent),
            MeasurementEquationContract::new(InstrumentResponse::Scalar, inner_products()),
        ),
        basis,
        coordinates,
    )
}

fn inner_products() -> DeclaredInnerProducts {
    DeclaredInnerProducts::new(
        ModelInnerProduct::HermitianEuclidean,
        VisibilityInnerProduct::HermitianEuclidean,
    )
}

fn specification_with_science(
    science: ScientificContract,
    basis: ReconstructionBasis,
    coordinates: Vec<PolarizationCoordinate>,
) -> ProblemSpecification {
    ProblemSpecification::new(
        science,
        ReconstructionContract::new(
            basis,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(10, 0.1, 0.0),
            PolarizationContract::new(coordinates),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    0.2,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::Zero,
                )
                .expect("valid PB policy"),
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::Zero,
                )
                .expect("valid Taylor policy"),
            ),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F32],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    )
}

fn geometry() -> GeometryInput {
    geometry_with_centres(CentreLaws::new(
        PhaseCentreLaw::Observation,
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::PhaseTrackingCentre,
    ))
}

fn geometry_with_spectral_wcs(wcs: SpectralWcs) -> GeometryInput {
    geometry().with_spectral(SpectralCoordinateSpec::new(
        FrequencyFrame::Topocentric,
        FrequencyFrame::Topocentric,
        SpectralFrameAnchor::NotApplicable,
        wcs,
        RestFrequency::NotApplicable,
        casa_imaging_model::DopplerConvention::NotApplicable,
    ))
}

fn geometry_with_centres(centres: CentreLaws) -> GeometryInput {
    geometry_with_centres_and_uvw(centres, UvwCoordinateLaw::PhaseTrackingCentre)
}

fn geometry_with_centres_and_uvw(centres: CentreLaws, uvw: UvwCoordinateLaw) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [15.0, 15.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(32, 32),
            direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
        centres,
        uvw,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 3,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            casa_imaging_model::DopplerConvention::NotApplicable,
        ),
    )
}

fn multidomain_geometry() -> GeometryInput {
    let domain = |role, direction, facets, psf_phase_centre| {
        ImageDomainSpec::new(
            role,
            ImageShape::new(512, 512),
            DirectionCoordinateSpec::new(
                Projection::Sin,
                direction,
                [255.0, 255.0],
                [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
                [[1.0, 0.0], [0.0, 1.0]],
                [180.0, 0.0],
            ),
            facets,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )
        .with_psf_phase_centre(psf_phase_centre)
    };
    let main = SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5);
    let alpha = SkyDirection::new(DirectionFrame::J2000, 1.01, -0.49);
    let zeta = SkyDirection::new(DirectionFrame::J2000, 1.02, -0.48);
    GeometryInput::new(
        // Deliberately unordered input proves compiler order reaches selected rows.
        vec![
            domain(
                ImageDomainRole::Outlier("zeta".to_string()),
                zeta,
                FacetLayout::Single,
                PsfPhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.025, -0.475)),
            ),
            domain(
                ImageDomainRole::Main,
                main,
                FacetLayout::Regular {
                    columns: 2,
                    rows: 2,
                },
                PsfPhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.005, -0.495)),
            ),
            domain(
                ImageDomainRole::Outlier("alpha".to_string()),
                alpha,
                FacetLayout::Single,
                PsfPhaseCentreLaw::ImageDomainReference,
            ),
        ],
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 3,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            casa_imaging_model::DopplerConvention::NotApplicable,
        ),
    )
}
